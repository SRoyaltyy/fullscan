# Factor mine action — `combo_jer_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_join_vol_green_h1/union_earn_react_h3 w=0.5,0.5 net=priority

Cash book **-8.93%** ($9,107) · signal-only (no cash/fees) was —. Starts YES **22/30**. Fills 348 · skips 286 · realized $+2954.01.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_join_vol_green_h1 50%, union_earn_react_h3 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_join_vol_green_h1 50%, union_earn_react_h3 50%.
- Member: union_join_vol_green_h1 (50% · long · hold 1).
- Member: union_earn_react_h3 (50% · long · hold 3).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $103.85.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 6172 | $0.81 | $68.51 | — | $4,932.17 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten; ⚪; ret5=+13.2; combo leftover $5000.00; owner union_earn_react_h3 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 223 | $22.01 | $2.88 | — | $21.06 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten; ⚪; ret5=+0.3; combo leftover $5000.00; owner union_earn_react_h3 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.06 | ▲ close $10,769.53 vs 09:30 $10,000.00 (session +840.92) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.06 | ▲ 09:30 equity $10,963.61 vs yday $10,769.53 (+194.08) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1 | $0.77 | $0.01 | — | $20.29 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+20.4; combo leftover $1.32; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `DEFT` | 2 | $0.47 | $0.02 | — | $19.33 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+11.1; combo leftover $1.32; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 1 | $1.50 | $0.02 | — | $17.81 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $2.42; owner union_join_vol_green_h1 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.81 | ▲ close $11,884.12 vs 09:30 $10,963.61 (session +920.56) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.81 | ▼ 09:30 equity $11,733.81 vs yday $11,884.12 (-150.31) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 1 | $1.52 | $0.04 | $-0.04 | $19.30 | ▼ -0.04 after sell → book $11,733.77; vs 09:30 mark -0.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.30 | ▲ close $12,249.78 vs 09:30 $11,733.81 (session +516.01) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.30 | ▼ 09:30 equity $12,145.63 vs yday $12,249.78 (-104.15) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 6172 | $1.14 | $80.70 | $+1887.55 | $6,974.68 | ▲ +1,887.55 after sell → book $12,064.93; vs 09:30 mark -80.70 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 223 | $22.82 | $2.95 | $+174.80 | $12,060.58 | ▲ +174.80 after sell → book $12,061.97; vs 09:30 mark -2.96 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,060.58 | ▲ close $12,062.01 vs 09:30 $12,145.63 (session +0.04) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,060.58 | ▲ 09:30 equity $12,062.02 vs yday $12,062.01 (+0.01) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `BZAI` | 1 | $0.57 | $0.03 | $-0.24 | $12,061.12 | ▼ -0.24 after sell → book $12,061.99; vs 09:30 mark -0.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DEFT` | 2 | $0.43 | $0.03 | $-0.12 | $12,061.96 | ▼ -0.12 after sell → book $12,061.96; vs 09:30 mark -0.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,061.96 | ▲ close $12,061.96 vs 09:30 $12,062.02 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,061.96 | ▲ 09:30 equity $12,061.96 vs yday $12,061.96 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 16 | $46.85 | $2.04 | — | $11,310.32 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+5.0; combo leftover $753.87; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 83 | $9.01 | $2.24 | — | $10,560.25 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $753.87; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 193 | $3.89 | $2.57 | — | $9,806.91 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.5; combo leftover $753.87; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 22 | $34.05 | $2.06 | — | $9,055.76 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+9.3; combo leftover $753.87; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 33 | $22.44 | $2.09 | — | $8,313.15 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.1; combo leftover $753.87; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 6 | $123.47 | $2.01 | — | $7,570.32 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+2.9; combo leftover $753.87; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `BILL` | 15 | $49.00 | $2.04 | — | $6,833.28 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-2.0; combo leftover $753.87; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 75 | $9.94 | $2.21 | — | $6,085.57 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+12.6; combo leftover $753.87; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 37 | $20.55 | $2.10 | — | $5,323.12 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; combo leftover $760.70; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 36 | $20.65 | $2.10 | — | $4,577.62 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; combo leftover $760.70; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 131 | $5.77 | $2.38 | — | $3,819.37 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; combo leftover $760.70; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 38 | $19.63 | $2.10 | — | $3,071.32 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; combo leftover $760.70; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 25 | $29.63 | $2.06 | — | $2,328.51 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; combo leftover $760.70; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 434 | $1.75 | $5.60 | — | $1,563.41 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; combo leftover $760.70; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 5 | $144.54 | $2.00 | — | $838.70 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; combo leftover $760.70; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 154 | $4.92 | $2.45 | — | $78.57 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $760.70; owner union_join_vol_green_h1 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.57 | ▲ close $12,053.88 vs 09:30 $12,061.96 (session +29.98) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.57 | ▲ 09:30 equity $12,263.42 vs yday $12,053.88 (+209.54) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 37 | $21.90 | $2.12 | $+45.73 | $886.75 | ▲ +45.73 after sell → book $12,261.30; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 36 | $21.75 | $2.12 | $+35.38 | $1,667.63 | ▲ +35.38 after sell → book $12,259.18; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 131 | $5.67 | $2.41 | $-17.90 | $2,407.99 | ▼ -17.90 after sell → book $12,256.77; vs 09:30 mark -2.41 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 38 | $21.17 | $2.12 | $+54.29 | $3,210.33 | ▲ +54.29 after sell → book $12,254.65; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 25 | $32.17 | $2.08 | $+59.35 | $4,012.49 | ▲ +59.35 after sell → book $12,252.56; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 434 | $1.79 | $5.68 | $+6.08 | $4,783.67 | ▲ +6.08 after sell → book $12,246.88; vs 09:30 mark -5.68 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 5 | $154.70 | $2.02 | $+46.77 | $5,555.14 | ▲ +46.77 after sell → book $12,244.85; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 154 | $5.20 | $2.49 | $+38.18 | $6,353.46 | ▲ +38.18 after sell → book $12,242.37; vs 09:30 mark -2.48 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 35 | $17.93 | $2.10 | — | $5,723.64 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $635.35; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 6 | $93.98 | $2.01 | — | $5,157.75 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.4; combo leftover $635.35; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 14 | $43.08 | $2.03 | — | $4,552.60 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.9; combo leftover $635.35; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 276 | $2.30 | $3.56 | — | $3,914.24 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.0; combo leftover $635.35; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `ROST` | 2 | $243.85 | $2.00 | — | $3,424.54 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-6.5; combo leftover $635.35; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 3 | $119.43 | $2.00 | — | $3,064.25 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $428.07; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 24 | $17.20 | $2.06 | — | $2,649.39 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; combo leftover $428.07; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 1 | $216.30 | $1.99 | — | $2,431.10 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; combo leftover $428.07; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 38 | $11.13 | $2.10 | — | $2,006.05 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; combo leftover $428.07; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 324 | $1.32 | $4.18 | — | $1,574.19 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; combo leftover $428.07; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 257 | $1.66 | $3.32 | — | $1,144.26 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; combo leftover $428.07; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 307 | $1.39 | $3.96 | — | $713.57 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; combo leftover $428.07; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 51 | $8.28 | $2.14 | — | $289.14 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; combo leftover $428.07; owner union_join_vol_green_h1 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $289.14 | ▲ close $12,275.83 vs 09:30 $12,263.42 (session +66.92) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $289.14 | ▲ 09:30 equity $12,459.31 vs yday $12,275.83 (+183.48) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 3 | $120.51 | $2.02 | $-0.78 | $648.66 | ▼ -0.78 after sell → book $12,457.29; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 24 | $16.57 | $2.08 | $-19.26 | $1,044.25 | ▼ -19.26 after sell → book $12,455.21; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 1 | $217.03 | $2.01 | $-3.28 | $1,259.27 | ▼ -3.28 after sell → book $12,453.20; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 38 | $13.33 | $2.12 | $+79.37 | $1,763.69 | ▲ +79.37 after sell → book $12,451.07; vs 09:30 mark -2.13 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 324 | $1.83 | $4.24 | $+156.82 | $2,352.36 | ▲ +156.82 after sell → book $12,446.83; vs 09:30 mark -4.24 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 257 | $1.55 | $3.37 | $-34.95 | $2,747.34 | ▼ -34.95 after sell → book $12,443.46; vs 09:30 mark -3.37 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 307 | $1.24 | $4.02 | $-54.03 | $3,124.00 | ▼ -54.03 after sell → book $12,439.44; vs 09:30 mark -4.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 51 | $8.59 | $2.16 | $+11.50 | $3,559.93 | ▲ +11.50 after sell → book $12,437.28; vs 09:30 mark -2.16 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,559.93 | ▲ close $12,548.44 vs 09:30 $12,459.31 (session +111.17) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,559.93 | ▼ 09:30 equity $12,531.38 vs yday $12,548.44 (-17.06) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 16 | $43.63 | $2.06 | $-55.62 | $4,255.95 | ▼ -55.62 after sell → book $12,529.32; vs 09:30 mark -2.06 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 83 | $9.23 | $2.26 | $+13.76 | $5,019.78 | ▲ +13.76 after sell → book $12,527.05; vs 09:30 mark -2.27 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 193 | $5.24 | $2.61 | $+255.37 | $6,028.49 | ▲ +255.37 after sell → book $12,524.44; vs 09:30 mark -2.61 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 22 | $34.72 | $2.08 | $+10.61 | $6,790.25 | ▲ +10.61 after sell → book $12,522.37; vs 09:30 mark -2.07 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 33 | $21.85 | $2.11 | $-23.67 | $7,509.19 | ▼ -23.67 after sell → book $12,520.26; vs 09:30 mark -2.11 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BABA` | 6 | $117.94 | $2.03 | $-37.22 | $8,214.81 | ▼ -37.22 after sell → book $12,518.23; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BILL` | 15 | $47.98 | $2.06 | $-19.31 | $8,932.53 | ▼ -19.31 after sell → book $12,516.18; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BULL` | 75 | $8.46 | $2.24 | $-115.45 | $9,564.79 | ▼ -115.45 after sell → book $12,513.94; vs 09:30 mark -2.24 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 3 | $175.01 | $2.00 | — | $9,037.76 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-7.0; combo leftover $597.80; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 6 | $88.94 | $2.01 | — | $8,502.11 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-4.9; combo leftover $597.80; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 39 | $15.28 | $2.11 | — | $7,904.08 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $597.80; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 4 | $142.36 | $2.00 | — | $7,332.64 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-8.6; combo leftover $597.80; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 117 | $5.10 | $2.34 | — | $6,733.60 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-8.9; combo leftover $597.80; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 12 | $47.89 | $2.03 | — | $6,156.90 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ⚪; ret5=+14.0; combo leftover $597.80; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 42 | $13.92 | $2.12 | — | $5,570.14 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+5.9; combo leftover $597.80; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 131 | $4.54 | $2.38 | — | $4,972.36 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-14.6; combo leftover $597.80; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 381 | $1.63 | $4.91 | — | $4,346.42 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; combo leftover $621.55; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 175 | $3.55 | $2.52 | — | $3,722.65 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+27.9; combo leftover $621.55; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 97 | $6.37 | $2.28 | — | $3,102.48 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; combo leftover $621.55; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 17 | $35.05 | $2.04 | — | $2,504.59 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $621.55; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 9 | $64.55 | $2.02 | — | $1,921.62 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+4.4; combo leftover $621.55; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 3 | $156.51 | $2.00 | — | $1,450.09 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+17.4; combo leftover $621.55; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 69 | $8.98 | $2.20 | — | $828.28 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; combo leftover $621.55; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `IAUX` | 327 | $1.90 | $4.22 | — | $202.76 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.4; combo leftover $621.55; owner union_join_vol_green_h1 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $202.76 | ▼ close $12,401.16 vs 09:30 $12,531.38 (session -73.62) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $202.76 | ▼ 09:30 equity $12,366.06 vs yday $12,401.16 (-35.10) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `BEKE` | 35 | $18.14 | $2.12 | $+2.97 | $835.54 | ▲ +2.97 after sell → book $12,363.95; vs 09:30 mark -2.11 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BJ` | 6 | $94.60 | $2.03 | $-0.32 | $1,401.12 | ▼ -0.32 after sell → book $12,361.92; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BKE` | 14 | $44.39 | $2.05 | $+14.26 | $2,020.52 | ▲ +14.26 after sell → book $12,359.87; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 276 | $2.35 | $3.62 | $+6.62 | $2,665.51 | ▲ +6.62 after sell → book $12,356.25; vs 09:30 mark -3.62 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ROST` | 2 | $242.50 | $2.02 | $-6.71 | $3,148.49 | ▼ -6.71 after sell → book $12,354.24; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 381 | $1.75 | $4.99 | $+37.72 | $3,812.16 | ▲ +37.72 after sell → book $12,349.25; vs 09:30 mark -4.99 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 175 | $3.77 | $2.55 | $+33.43 | $4,469.35 | ▲ +33.43 after sell → book $12,346.69; vs 09:30 mark -2.56 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 97 | $6.13 | $2.31 | $-27.87 | $5,061.66 | ▼ -27.87 after sell → book $12,344.39; vs 09:30 mark -2.30 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 17 | $35.70 | $2.06 | $+6.95 | $5,666.50 | ▲ +6.95 after sell → book $12,342.33; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 9 | $63.60 | $2.04 | $-12.60 | $6,236.86 | ▼ -12.60 after sell → book $12,340.29; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 3 | $160.93 | $2.02 | $+9.24 | $6,717.63 | ▲ +9.24 after sell → book $12,338.27; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 69 | $9.03 | $2.22 | $-0.97 | $7,338.48 | ▼ -0.97 after sell → book $12,336.05; vs 09:30 mark -2.22 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `IAUX` | 327 | $1.87 | $4.28 | $-18.31 | $7,945.69 | ▼ -18.31 after sell → book $12,331.77; vs 09:30 mark -4.28 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 95 | $5.21 | $2.27 | — | $7,448.46 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $496.61; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 3 | $131.37 | $2.00 | — | $7,052.35 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.3; combo leftover $496.61; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 27 | $18.26 | $2.07 | — | $6,557.26 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-11.4; combo leftover $496.61; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 14 | $34.30 | $2.03 | — | $6,075.03 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+1.7; combo leftover $496.61; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 1 | $326.91 | $1.99 | — | $5,746.13 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-15.2; combo leftover $496.61; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `FSCO` | 97 | $5.08 | $2.28 | — | $5,251.09 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-1.6; combo leftover $496.61; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `HEI` | 1 | $370.00 | $1.99 | — | $4,879.09 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.6; combo leftover $496.61; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `INTU` | 1 | $323.47 | $1.99 | — | $4,553.63 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+2.0; combo leftover $496.61; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 782 | $5.81 | $10.09 | — | $0.12 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $4553.63; owner union_join_vol_green_h1 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.12 | ▲ close $12,655.46 vs 09:30 $12,366.06 (session +350.42) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.12 | ▲ 09:30 equity $13,073.78 vs yday $12,655.46 (+418.32) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 782 | $6.50 | $10.26 | $+519.23 | $5,072.87 | ▲ +519.23 after sell → book $13,063.53; vs 09:30 mark -10.25 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 7 | $80.60 | $2.01 | — | $4,506.65 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.0; combo leftover $634.11; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 39 | $16.18 | $2.11 | — | $3,873.53 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-6.7; combo leftover $634.11; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 5 | $118.77 | $2.00 | — | $3,277.67 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.3; combo leftover $634.11; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 35 | $17.78 | $2.10 | — | $2,653.28 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-1.2; combo leftover $634.11; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 47 | $13.41 | $2.13 | — | $2,020.88 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.1; combo leftover $634.11; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 6 | $97.16 | $2.01 | — | $1,435.91 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.5; combo leftover $634.11; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 3 | $206.82 | $2.00 | — | $813.45 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-0.2; combo leftover $634.11; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 5 | $120.17 | $2.00 | — | $210.59 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.9; combo leftover $634.11; owner union_earn_react_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $210.59 | ▼ close $13,024.73 vs 09:30 $13,073.78 (session -22.43) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $210.59 | ▲ 09:30 equity $13,052.91 vs yday $13,024.73 (+28.18) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 3 | $172.76 | $2.02 | $-10.77 | $726.86 | ▼ -10.77 after sell → book $13,050.90; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 6 | $93.30 | $2.03 | $+22.12 | $1,284.63 | ▲ +22.12 after sell → book $13,048.87; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 39 | $18.15 | $2.13 | $+107.70 | $1,990.35 | ▲ +107.70 after sell → book $13,046.74; vs 09:30 mark -2.13 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 4 | $132.80 | $2.02 | $-42.26 | $2,519.53 | ▼ -42.26 after sell → book $13,044.72; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 117 | $4.58 | $2.37 | $-65.55 | $3,053.02 | ▼ -65.55 after sell → book $13,042.35; vs 09:30 mark -2.37 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 12 | $48.42 | $2.05 | $+2.29 | $3,632.01 | ▲ +2.29 after sell → book $13,040.30; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 42 | $15.66 | $2.14 | $+68.83 | $4,287.60 | ▲ +68.83 after sell → book $13,038.17; vs 09:30 mark -2.13 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 131 | $3.38 | $2.41 | $-157.41 | $4,727.96 | ▼ -157.41 after sell → book $13,035.75; vs 09:30 mark -2.42 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 11 | $24.69 | $2.02 | — | $4,454.35 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.8; combo leftover $295.50; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 1 | $261.16 | $1.99 | — | $4,191.20 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+7.8; combo leftover $295.50; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 19 | $15.01 | $2.05 | — | $3,903.96 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+3.7; combo leftover $295.50; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 2 | $103.89 | $2.00 | — | $3,694.18 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.5; combo leftover $295.50; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 76 | $3.88 | $2.22 | — | $3,397.08 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-8.6; combo leftover $295.50; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 6 | $44.40 | $2.01 | — | $3,128.68 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.4; combo leftover $295.50; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 35 | $8.35 | $2.10 | — | $2,834.33 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.1; combo leftover $295.50; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 7 | $37.65 | $2.01 | — | $2,568.81 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.9; combo leftover $295.50; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 55 | $23.30 | $2.15 | — | $1,285.15 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+14.5; combo leftover $1284.40; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 67 | $19.00 | $2.19 | — | $9.96 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+7.5; combo leftover $1284.40; owner union_join_vol_green_h1 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.96 | ▼ close $12,856.79 vs 09:30 $13,052.91 (session -158.22) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.96 | ▼ 09:30 equity $12,813.81 vs yday $12,856.79 (-42.98) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 95 | $5.00 | $2.30 | $-24.53 | $482.66 | ▼ -24.53 after sell → book $12,811.51; vs 09:30 mark -2.30 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 3 | $148.03 | $2.02 | $+45.96 | $924.73 | ▲ +45.96 after sell → book $12,809.49; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 27 | $19.25 | $2.09 | $+22.57 | $1,442.39 | ▲ +22.57 after sell → book $12,807.40; vs 09:30 mark -2.09 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BOX` | 14 | $34.72 | $2.05 | $+1.80 | $1,926.42 | ▲ +1.80 after sell → book $12,805.35; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 1 | $298.01 | $2.01 | $-32.91 | $2,222.41 | ▼ -32.91 after sell → book $12,803.34; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FSCO` | 97 | $5.20 | $2.31 | $+7.05 | $2,724.51 | ▲ +7.05 after sell → book $12,801.03; vs 09:30 mark -2.31 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `HEI` | 1 | $334.88 | $2.01 | $-39.13 | $3,057.37 | ▼ -39.13 after sell → book $12,799.02; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `INTU` | 1 | $356.05 | $2.01 | $+28.57 | $3,411.41 | ▲ +28.57 after sell → book $12,797.01; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 55 | $22.66 | $2.17 | $-39.53 | $4,655.54 | ▼ -39.53 after sell → book $12,794.83; vs 09:30 mark -2.18 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 67 | $18.12 | $2.21 | $-63.03 | $5,867.70 | ▼ -63.03 after sell → book $12,792.62; vs 09:30 mark -2.21 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,867.70 | ▼ close $12,725.64 vs 09:30 $12,813.81 (session -66.98) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,867.70 | ▼ 09:30 equity $12,683.55 vs yday $12,725.64 (-42.09) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BBY` | 7 | $79.83 | $2.03 | $-9.43 | $6,424.48 | ▼ -9.43 after sell → book $12,681.52; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BILI` | 39 | $15.97 | $2.13 | $-12.42 | $7,045.18 | ▼ -12.42 after sell → book $12,679.39; vs 09:30 mark -2.13 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CM` | 5 | $113.66 | $2.02 | $-29.58 | $7,611.46 | ▼ -29.58 after sell → book $12,677.37; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CMBT` | 35 | $18.28 | $2.12 | $+13.29 | $8,249.14 | ▲ +13.29 after sell → book $12,675.25; vs 09:30 mark -2.12 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CSIQ` | 47 | $12.18 | $2.15 | $-62.09 | $8,819.45 | ▼ -62.09 after sell → book $12,673.10; vs 09:30 mark -2.15 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `HQY` | 6 | $96.65 | $2.03 | $-7.10 | $9,397.32 | ▼ -7.10 after sell → book $12,671.07; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RY` | 3 | $203.78 | $2.02 | $-13.14 | $10,006.64 | ▼ -13.14 after sell → book $12,669.05; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TD` | 5 | $120.54 | $2.02 | $-2.18 | $10,607.32 | ▼ -2.18 after sell → book $12,667.03; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,607.32 | ▼ close $12,645.51 vs 09:30 $12,683.55 (session -21.52) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,607.32 | ▼ 09:30 equity $12,633.08 vs yday $12,645.51 (-12.43) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 11 | $21.97 | $2.04 | $-33.99 | $10,846.94 | ▼ -33.99 after sell → book $12,631.04; vs 09:30 mark -2.04 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 1 | $246.70 | $2.01 | $-18.47 | $11,091.63 | ▼ -18.47 after sell → book $12,629.03; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 19 | $15.01 | $2.07 | $-4.11 | $11,374.75 | ▼ -4.11 after sell → book $12,626.96; vs 09:30 mark -2.07 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 2 | $92.00 | $1.87 | $-27.64 | $11,556.89 | ▼ -27.64 after sell → book $12,625.09; vs 09:30 mark -1.87 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 76 | $3.32 | $2.24 | $-47.02 | $11,806.97 | ▼ -47.02 after sell → book $12,622.85; vs 09:30 mark -2.24 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 6 | $44.17 | $2.03 | $-5.42 | $12,069.96 | ▼ -5.42 after sell → book $12,620.82; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 35 | $8.58 | $2.12 | $+3.84 | $12,368.14 | ▲ +3.84 after sell → book $12,618.71; vs 09:30 mark -2.11 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 7 | $35.80 | $2.03 | $-16.99 | $12,616.68 | ▼ -16.99 after sell → book $12,616.68; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,616.68 | ▲ close $12,616.68 vs 09:30 $12,633.08 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,616.68 | ▲ 09:30 equity $12,616.68 vs yday $12,616.68 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 73 | $10.74 | $2.21 | — | $11,830.08 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+8.5; combo leftover $788.54; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 2 | $351.74 | $2.00 | — | $11,124.61 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+3.3; combo leftover $788.54; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 114 | $6.90 | $2.33 | — | $10,335.68 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.8; combo leftover $788.54; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 2 | $354.49 | $2.00 | — | $9,624.70 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.3; combo leftover $788.54; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 35 | $22.32 | $2.10 | — | $8,841.41 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+2.4; combo leftover $788.54; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 3 | $257.00 | $2.00 | — | $8,068.41 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-5.5; combo leftover $788.54; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 16 | $47.60 | $2.04 | — | $7,304.77 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-6.2; combo leftover $788.54; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 52 | $15.09 | $2.15 | — | $6,517.94 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+6.1; combo leftover $788.54; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 6 | $132.45 | $2.01 | — | $5,721.23 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; combo leftover $814.74; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 48 | $16.77 | $2.13 | — | $4,914.14 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; combo leftover $814.74; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 373 | $2.18 | $4.81 | — | $4,096.19 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; combo leftover $814.74; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 34 | $23.88 | $2.09 | — | $3,282.18 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $814.74; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 78 | $10.42 | $2.22 | — | $2,467.19 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; combo leftover $814.74; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 422 | $1.93 | $5.44 | — | $1,647.29 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $814.74; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 5 | $161.54 | $2.00 | — | $837.58 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+12.0; combo leftover $814.74; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 78 | $10.38 | $2.22 | — | $26.11 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-56.2; combo leftover $814.74; owner union_join_vol_green_h1 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.11 | ▲ close $12,807.90 vs 09:30 $12,616.68 (session +230.98) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.11 | ▲ 09:30 equity $12,822.36 vs yday $12,807.90 (+14.46) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 6 | $130.03 | $2.03 | $-18.56 | $804.26 | ▼ -18.56 after sell → book $12,820.33; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 48 | $15.61 | $2.15 | $-59.97 | $1,551.39 | ▼ -59.97 after sell → book $12,818.18; vs 09:30 mark -2.15 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 373 | $2.16 | $4.88 | $-17.16 | $2,352.18 | ▼ -17.16 after sell → book $12,813.29; vs 09:30 mark -4.89 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 34 | $23.84 | $2.11 | $-5.56 | $3,160.63 | ▼ -5.56 after sell → book $12,811.18; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 78 | $10.50 | $2.25 | $+1.77 | $3,977.39 | ▲ +1.77 after sell → book $12,808.94; vs 09:30 mark -2.24 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 422 | $1.90 | $5.52 | $-23.63 | $4,773.66 | ▼ -23.63 after sell → book $12,803.41; vs 09:30 mark -5.53 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DUOL` | 5 | $157.46 | $2.02 | $-24.43 | $5,558.94 | ▼ -24.43 after sell → book $12,801.39; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 78 | $11.23 | $2.25 | $+62.22 | $6,432.63 | ▲ +62.22 after sell → book $12,799.14; vs 09:30 mark -2.25 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 6 | $63.18 | $2.01 | — | $6,051.54 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-10.9; combo leftover $402.04; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 45 | $8.74 | $2.12 | — | $5,656.12 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.8; combo leftover $402.04; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 5 | $68.52 | $2.00 | — | $5,311.51 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+3.4; combo leftover $402.04; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 111 | $3.62 | $2.32 | — | $4,907.92 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-3.1; combo leftover $402.04; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 2 | $167.55 | $2.00 | — | $4,570.83 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.9; combo leftover $402.04; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 8 | $44.90 | $2.01 | — | $4,209.61 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-7.5; combo leftover $402.04; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 4 | $98.15 | $2.00 | — | $3,815.01 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.9; combo leftover $402.04; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 25 | $15.70 | $2.06 | — | $3,420.45 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-0.4; combo leftover $402.04; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 5 | $82.70 | $2.00 | — | $3,004.94 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; combo leftover $427.56; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 170 | $2.51 | $2.50 | — | $2,575.74 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; combo leftover $427.56; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MDB` | 1 | $378.34 | $1.99 | — | $2,195.41 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-12.7; combo leftover $427.56; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 16 | $25.18 | $2.04 | — | $1,790.49 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $427.56; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 73 | $5.79 | $2.21 | — | $1,365.61 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $427.56; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `TDS` | 11 | $37.44 | $2.02 | — | $951.75 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.1; combo leftover $427.56; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `AHCO` | 67 | $6.32 | $2.19 | — | $526.12 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+10.7; combo leftover $427.56; owner union_join_vol_green_h1 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $526.12 | ▲ close $12,890.69 vs 09:30 $12,822.36 (session +123.04) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $526.12 | ▼ 09:30 equity $12,855.10 vs yday $12,890.69 (-35.59) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 5 | $89.67 | $2.02 | $+30.82 | $972.44 | ▲ +30.82 after sell → book $12,853.07; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 170 | $2.66 | $2.54 | $+20.46 | $1,422.10 | ▲ +20.46 after sell → book $12,850.53; vs 09:30 mark -2.54 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MDB` | 1 | $360.75 | $2.01 | $-21.60 | $1,780.84 | ▼ -21.60 after sell → book $12,848.52; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 16 | $26.44 | $2.06 | $+16.06 | $2,201.82 | ▲ +16.06 after sell → book $12,846.46; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 73 | $5.81 | $2.23 | $-2.98 | $2,623.72 | ▼ -2.98 after sell → book $12,844.23; vs 09:30 mark -2.23 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TDS` | 11 | $37.75 | $2.04 | $-0.66 | $3,036.93 | ▼ -0.66 after sell → book $12,842.19; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AHCO` | 67 | $6.48 | $2.21 | $+6.32 | $3,468.88 | ▲ +6.32 after sell → book $12,839.98; vs 09:30 mark -2.21 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,468.88 | ▼ close $12,823.24 vs 09:30 $12,855.10 (session -16.74) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,468.88 | ▼ 09:30 equity $12,792.49 vs yday $12,823.24 (-30.75) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 73 | $10.51 | $2.23 | $-21.60 | $4,233.88 | ▼ -21.60 after sell → book $12,790.26; vs 09:30 mark -2.23 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 2 | $366.23 | $2.02 | $+24.97 | $4,964.32 | ▲ +24.97 after sell → book $12,788.24; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 114 | $9.39 | $2.36 | $+279.17 | $6,032.42 | ▲ +279.17 after sell → book $12,785.88; vs 09:30 mark -2.36 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 2 | $341.90 | $2.02 | $-29.19 | $6,714.20 | ▼ -29.19 after sell → book $12,783.86; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CPB` | 35 | $21.67 | $2.12 | $-26.96 | $7,470.54 | ▼ -26.96 after sell → book $12,781.75; vs 09:30 mark -2.11 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 3 | $252.92 | $2.02 | $-16.26 | $8,227.28 | ▼ -16.26 after sell → book $12,779.73; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 16 | $56.94 | $2.06 | $+145.34 | $9,136.26 | ▲ +145.34 after sell → book $12,777.67; vs 09:30 mark -2.06 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MEI` | 52 | $13.84 | $2.17 | $-69.31 | $9,853.78 | ▼ -69.31 after sell → book $12,775.51; vs 09:30 mark -2.16 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,853.78 | ▼ close $12,753.09 vs 09:30 $12,792.49 (session -22.42) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,853.78 | ▼ 09:30 equity $12,737.48 vs yday $12,753.09 (-15.61) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `AMBA` | 6 | $67.44 | $2.03 | $+21.52 | $10,256.39 | ▲ +21.52 after sell → book $12,735.45; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ASAN` | 45 | $8.26 | $2.15 | $-25.87 | $10,625.94 | ▼ -25.87 after sell → book $12,733.30; vs 09:30 mark -2.15 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOCU` | 5 | $64.60 | $2.02 | $-23.63 | $10,946.92 | ▼ -23.63 after sell → book $12,731.28; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 111 | $3.76 | $2.35 | $+11.42 | $11,361.93 | ▲ +11.42 after sell → book $12,728.93; vs 09:30 mark -2.35 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `GWRE` | 2 | $142.43 | $2.02 | $-54.25 | $11,644.77 | ▼ -54.25 after sell → book $12,726.91; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `IOT` | 8 | $38.23 | $2.03 | $-57.45 | $11,948.54 | ▼ -57.45 after sell → book $12,724.88; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `LULU` | 4 | $98.71 | $2.02 | $-1.78 | $12,341.35 | ▼ -1.78 after sell → book $12,722.85; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MAMA` | 25 | $15.26 | $2.08 | $-15.15 | $12,720.77 | ▼ -15.15 after sell → book $12,720.77; vs 09:30 mark -2.08 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,720.77 | ▲ close $12,720.77 vs 09:30 $12,737.48 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,720.77 | ▲ 09:30 equity $12,720.77 vs yday $12,720.77 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 4 | $164.43 | $2.00 | — | $12,061.05 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $795.05; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 3 | $242.17 | $2.00 | — | $11,332.54 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-11.1; combo leftover $795.05; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 24 | $32.01 | $2.06 | — | $10,562.24 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-4.4; combo leftover $795.05; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 11 | $71.71 | $2.02 | — | $9,771.40 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-9.1; combo leftover $795.05; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 14 | $56.02 | $2.03 | — | $8,985.09 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.2; combo leftover $795.05; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 84 | $9.37 | $2.24 | — | $8,195.77 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+1.5; combo leftover $795.05; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 60 | $13.10 | $2.17 | — | $7,407.60 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-6.9; combo leftover $795.05; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 5 | $135.71 | $2.00 | — | $6,727.04 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-9.2; combo leftover $795.05; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 35 | $23.63 | $2.10 | — | $5,897.90 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; ret5=-6.3; combo leftover $840.88; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 311 | $2.70 | $4.01 | — | $5,054.19 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $840.88; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 76 | $10.95 | $2.22 | — | $4,219.77 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+20.8; combo leftover $840.88; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 171 | $4.91 | $2.50 | — | $3,377.66 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $840.88; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 9 | $84.27 | $2.02 | — | $2,617.21 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+17.2; combo leftover $840.88; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 15 | $54.91 | $2.04 | — | $1,791.52 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+24.3; combo leftover $840.88; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 136 | $6.16 | $2.40 | — | $951.37 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+36.4; combo leftover $840.88; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 15 | $54.66 | $2.04 | — | $129.43 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-22.3; combo leftover $840.88; owner union_join_vol_green_h1 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.43 | ▼ close $12,644.13 vs 09:30 $12,720.77 (session -40.79) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.43 | ▲ 09:30 equity $12,710.76 vs yday $12,644.13 (+66.63) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 35 | $23.20 | $2.12 | $-19.26 | $939.32 | ▼ -19.26 after sell → book $12,708.65; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INDP` | 311 | $2.80 | $4.07 | $+23.01 | $1,806.04 | ▲ +23.01 after sell → book $12,704.57; vs 09:30 mark -4.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 76 | $10.29 | $2.24 | $-54.62 | $2,585.84 | ▼ -54.62 after sell → book $12,702.33; vs 09:30 mark -2.24 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BNC` | 171 | $5.03 | $2.54 | $+15.48 | $3,443.43 | ▲ +15.48 after sell → book $12,699.79; vs 09:30 mark -2.54 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 9 | $86.06 | $2.04 | $+12.06 | $4,215.93 | ▲ +12.06 after sell → book $12,697.75; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 15 | $54.75 | $2.06 | $-6.49 | $5,035.13 | ▼ -6.49 after sell → book $12,695.70; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 136 | $6.02 | $2.43 | $-23.87 | $5,851.42 | ▼ -23.87 after sell → book $12,693.27; vs 09:30 mark -2.43 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 15 | $54.78 | $2.06 | $-2.29 | $6,671.06 | ▼ -2.29 after sell → book $12,691.21; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,671.06 | ▲ close $12,848.19 vs 09:30 $12,710.76 (session +156.98) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,671.06 | ▼ 09:30 equity $12,827.96 vs yday $12,848.19 (-20.23) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,671.06 | ▲ close $12,858.29 vs 09:30 $12,827.96 (session +30.33) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,671.06 | ▼ 09:30 equity $12,813.57 vs yday $12,858.29 (-44.72) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 4 | $140.03 | $2.02 | $-101.62 | $7,229.16 | ▼ -101.62 after sell → book $12,811.55; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 3 | $253.34 | $2.02 | $+29.49 | $7,987.16 | ▲ +29.49 after sell → book $12,809.53; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CPRT` | 24 | $30.57 | $2.08 | $-38.70 | $8,718.76 | ▼ -38.70 after sell → book $12,807.45; vs 09:30 mark -2.08 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DSGX` | 11 | $78.12 | $2.04 | $+66.44 | $9,576.04 | ▲ +66.44 after sell → book $12,805.41; vs 09:30 mark -2.04 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `KR` | 14 | $61.93 | $2.05 | $+78.66 | $10,441.01 | ▲ +78.66 after sell → book $12,803.36; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `LPTH` | 84 | $9.40 | $2.27 | $-1.99 | $11,228.34 | ▼ -1.99 after sell → book $12,801.09; vs 09:30 mark -2.27 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `REF` | 60 | $15.75 | $2.19 | $+154.64 | $12,171.15 | ▲ +154.64 after sell → book $12,798.90; vs 09:30 mark -2.19 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RH` | 5 | $125.55 | $2.02 | $-54.83 | $12,796.87 | ▼ -54.83 after sell → book $12,796.87; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 156 | $40.93 | $2.46 | — | $6,409.34 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.1; combo leftover $6398.44; owner union_earn_react_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 10 | $77.12 | $2.02 | — | $5,636.12 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+7.2; combo leftover $801.17; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 136 | $5.87 | $2.40 | — | $4,835.40 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.1; combo leftover $801.17; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 9 | $87.40 | $2.02 | — | $4,046.78 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.2; combo leftover $801.17; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 29 | $27.09 | $2.08 | — | $3,259.09 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; combo leftover $801.17; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 8 | $89.38 | $2.01 | — | $2,542.04 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; combo leftover $801.17; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 34 | $23.29 | $2.09 | — | $1,748.09 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+16.1; combo leftover $801.17; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 24 | $33.14 | $2.06 | — | $950.67 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=-2.9; combo leftover $801.17; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 28 | $28.16 | $2.07 | — | $160.11 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+14.8; combo leftover $801.17; owner union_join_vol_green_h1 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.11 | ▼ close $12,647.92 vs 09:30 $12,813.57 (session -129.74) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.11 | ▲ 09:30 equity $12,810.14 vs yday $12,647.92 (+162.22) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 10 | $76.44 | $2.04 | $-10.86 | $922.47 | ▼ -10.86 after sell → book $12,808.10; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 136 | $5.58 | $2.43 | $-44.27 | $1,678.92 | ▼ -44.27 after sell → book $12,805.67; vs 09:30 mark -2.43 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 9 | $83.20 | $2.04 | $-41.85 | $2,425.68 | ▼ -41.85 after sell → book $12,803.63; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 29 | $28.23 | $2.10 | $+28.89 | $3,242.26 | ▲ +28.89 after sell → book $12,801.54; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 8 | $86.76 | $2.03 | $-25.01 | $3,934.30 | ▼ -25.01 after sell → book $12,799.50; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 34 | $24.09 | $2.11 | $+23.00 | $4,751.25 | ▲ +23.00 after sell → book $12,797.39; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CAI` | 28 | $28.59 | $2.09 | $+8.01 | $5,549.82 | ▲ +8.01 after sell → book $12,795.30; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 123 | $11.21 | $2.36 | — | $4,168.63 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+1.0; combo leftover $1387.45; owner union_earn_react_h3 | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 17 | $81.00 | $2.04 | — | $2,789.59 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-3.0; combo leftover $1387.45; owner union_earn_react_h3 | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 1 | $233.85 | $1.99 | — | $2,553.74 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+11.7; combo leftover $398.51; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 2 | $147.61 | $2.00 | — | $2,256.53 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+17.7; combo leftover $398.51; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 52 | $7.59 | $2.15 | — | $1,859.70 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; combo leftover $398.51; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 15 | $25.95 | $2.04 | — | $1,468.42 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; combo leftover $398.51; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 2 | $170.85 | $2.00 | — | $1,124.72 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; combo leftover $398.51; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 22 | $18.04 | $2.06 | — | $725.90 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; combo leftover $398.51; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRKR` | 6 | $61.90 | $2.01 | — | $352.49 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ret5=+11.5; combo leftover $398.51; owner union_join_vol_green_h1 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $352.49 | ▲ close $12,789.16 vs 09:30 $12,810.14 (session +12.49) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $352.49 | ▲ 09:30 equity $12,883.29 vs yday $12,789.16 (+94.13) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 24 | $39.50 | $2.08 | $+148.50 | $1,298.41 | ▲ +148.50 after sell → book $12,881.21; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 1 | $249.13 | $2.01 | $+11.27 | $1,545.52 | ▲ +11.27 after sell → book $12,879.19; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 2 | $146.50 | $2.02 | $-6.23 | $1,836.51 | ▼ -6.23 after sell → book $12,877.18; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 52 | $7.98 | $2.17 | $+15.97 | $2,249.30 | ▲ +15.97 after sell → book $12,875.01; vs 09:30 mark -2.17 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 15 | $26.14 | $2.06 | $-1.24 | $2,639.35 | ▼ -1.24 after sell → book $12,872.96; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 2 | $182.33 | $2.02 | $+18.95 | $3,001.99 | ▲ +18.95 after sell → book $12,870.94; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 22 | $17.80 | $2.08 | $-9.30 | $3,391.51 | ▼ -9.30 after sell → book $12,868.86; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRKR` | 6 | $63.37 | $2.03 | $+4.78 | $3,769.71 | ▲ +4.78 after sell → book $12,866.84; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 2 | $219.62 | $2.00 | — | $3,328.47 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; combo leftover $471.21; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 5 | $85.00 | $2.00 | — | $2,901.46 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+18.3; combo leftover $471.21; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 119 | $3.95 | $2.35 | — | $2,429.07 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; combo leftover $471.21; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 33 | $14.07 | $2.09 | — | $1,962.67 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; combo leftover $471.21; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 31 | $14.79 | $2.08 | — | $1,502.10 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; combo leftover $471.21; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 16 | $29.32 | $2.04 | — | $1,030.94 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $471.21; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 155 | $3.04 | $2.46 | — | $558.06 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $471.21; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `VITL` | 41 | $11.38 | $2.11 | — | $89.36 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+19.5; combo leftover $471.21; owner union_join_vol_green_h1 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $89.36 | ▲ close $13,041.45 vs 09:30 $12,883.29 (session +191.73) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $89.36 | ▲ 09:30 equity $13,239.19 vs yday $13,041.45 (+197.74) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 156 | $41.00 | $2.54 | $+5.93 | $6,482.83 | ▲ +5.93 after sell → book $13,236.65; vs 09:30 mark -2.54 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 2 | $230.25 | $2.02 | $+17.25 | $6,941.31 | ▲ +17.25 after sell → book $13,234.64; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 5 | $82.83 | $2.02 | $-14.88 | $7,353.44 | ▼ -14.88 after sell → book $13,232.61; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 119 | $3.87 | $2.38 | $-14.24 | $7,811.59 | ▼ -14.24 after sell → book $13,230.24; vs 09:30 mark -2.37 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 33 | $13.90 | $2.11 | $-9.81 | $8,268.18 | ▼ -9.81 after sell → book $13,228.13; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 31 | $14.58 | $2.10 | $-10.70 | $8,718.06 | ▼ -10.70 after sell → book $13,226.02; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 16 | $29.43 | $2.06 | $-2.34 | $9,186.88 | ▼ -2.34 after sell → book $13,223.97; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 155 | $4.00 | $2.49 | $+144.63 | $9,804.39 | ▲ +144.63 after sell → book $13,221.48; vs 09:30 mark -2.49 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VITL` | 41 | $12.05 | $2.13 | $+23.22 | $10,296.31 | ▲ +23.22 after sell → book $13,219.34; vs 09:30 mark -2.14 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 8 | $157.87 | $2.01 | — | $9,031.33 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+6.5; combo leftover $1287.04; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 14 | $88.83 | $2.03 | — | $7,785.68 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+7.6; combo leftover $1287.04; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 138 | $9.31 | $2.40 | — | $6,498.50 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.3; combo leftover $1287.04; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 95 | $13.47 | $2.27 | — | $5,216.10 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; combo leftover $1287.04; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 128 | $9.99 | $2.37 | — | $3,935.00 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+5.3; combo leftover $1287.04; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 76 | $16.91 | $2.22 | — | $2,647.63 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $1287.04; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 98 | $13.05 | $2.28 | — | $1,366.44 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+40.4; combo leftover $1287.04; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 223 | $5.75 | $2.88 | — | $80.20 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+30.3; combo leftover $1287.04; owner union_join_vol_green_h1 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $80.20 | ▲ close $13,319.20 vs 09:30 $13,239.19 (session +118.34) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.20 | ▲ 09:30 equity $13,362.96 vs yday $13,319.20 (+43.76) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 128 | $9.91 | $2.41 | $-15.02 | $1,346.27 | ▼ -15.02 after sell → book $13,360.55; vs 09:30 mark -2.41 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 98 | $12.99 | $2.31 | $-10.47 | $2,616.98 | ▼ -10.47 after sell → book $13,358.24; vs 09:30 mark -2.31 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `GEMI` | 223 | $6.05 | $2.92 | $+61.10 | $3,964.32 | ▲ +61.10 after sell → book $13,355.31; vs 09:30 mark -2.93 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `ARM` | 2 | $319.41 | $2.00 | — | $3,323.51 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+35.1; combo leftover $792.86; owner union_join_vol_green_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `FSLY` | 28 | $28.02 | $2.07 | — | $2,536.87 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; combo leftover $792.86; owner union_join_vol_green_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `META` | 1 | $731.40 | $1.99 | — | $1,803.48 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+11.4; combo leftover $792.86; owner union_join_vol_green_h1 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,803.48 | ▼ close $13,327.71 vs 09:30 $13,362.96 (session -21.54) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,803.48 | ▲ 09:30 equity $13,435.74 vs yday $13,327.71 (+108.03) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `ALMU` | 123 | $13.82 | $2.39 | $+316.28 | $3,500.95 | ▲ +316.28 after sell → book $13,433.35; vs 09:30 mark -2.39 | union_earn_react_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `LEN` | 17 | $82.00 | $2.06 | $+12.90 | $4,892.89 | ▲ +12.90 after sell → book $13,431.29; vs 09:30 mark -2.06 | union_earn_react_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 138 | $9.50 | $2.44 | $+21.38 | $6,201.45 | ▲ +21.38 after sell → book $13,428.85; vs 09:30 mark -2.44 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 95 | $12.84 | $2.30 | $-64.90 | $7,418.95 | ▼ -64.90 after sell → book $13,426.55; vs 09:30 mark -2.30 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 76 | $16.92 | $2.24 | $-3.70 | $8,702.63 | ▼ -3.70 after sell → book $13,424.31; vs 09:30 mark -2.24 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARM` | 2 | $331.78 | $2.02 | $+20.73 | $9,364.17 | ▲ +20.73 after sell → book $13,422.29; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FSLY` | 28 | $25.90 | $2.09 | $-63.53 | $10,087.28 | ▼ -63.53 after sell → book $13,420.20; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `META` | 1 | $747.60 | $2.01 | $+12.19 | $10,832.86 | ▲ +12.19 after sell → book $13,418.18; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 22 | $47.57 | $2.06 | — | $9,784.27 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-11.2; combo leftover $1083.29; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 5 | $196.78 | $2.00 | — | $8,798.36 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $1083.29; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 30 | $35.74 | $2.08 | — | $7,724.08 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.1; combo leftover $1083.29; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 22 | $47.15 | $2.06 | — | $6,684.73 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-1.9; combo leftover $1083.29; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 9 | $109.67 | $2.02 | — | $5,695.68 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $1083.29; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 45 | $20.65 | $2.12 | — | $4,764.31 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; combo leftover $949.28; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 60 | $15.72 | $2.17 | — | $3,818.94 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; combo leftover $949.28; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 37 | $25.40 | $2.10 | — | $2,877.03 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; combo leftover $949.28; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 1236 | $0.77 | $13.20 | — | $1,914.59 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; combo leftover $949.28; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 22 | $41.76 | $2.06 | — | $993.81 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $949.28; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 95 | $9.90 | $2.27 | — | $51.04 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; combo leftover $949.28; owner union_join_vol_green_h1 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.04 | ▼ close $13,064.78 vs 09:30 $13,435.74 (session -319.26) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.04 | ▼ 09:30 equity $12,902.89 vs yday $13,064.78 (-161.89) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 8 | $163.95 | $2.03 | $+44.59 | $1,360.60 | ▲ +44.59 after sell → book $12,900.85; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 14 | $87.67 | $2.05 | $-20.25 | $2,586.00 | ▼ -20.25 after sell → book $12,898.80; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 45 | $20.52 | $2.15 | $-10.12 | $3,507.25 | ▼ -10.12 after sell → book $12,896.66; vs 09:30 mark -2.14 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 60 | $14.38 | $2.19 | $-84.76 | $4,367.86 | ▼ -84.76 after sell → book $12,894.47; vs 09:30 mark -2.19 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `TNGX` | 37 | $23.99 | $2.12 | $-56.39 | $5,253.37 | ▼ -56.39 after sell → book $12,892.35; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `NMRA` | 1236 | $0.75 | $13.14 | $-53.54 | $6,162.28 | ▼ -53.54 after sell → book $12,879.20; vs 09:30 mark -13.15 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 22 | $36.02 | $2.08 | $-130.30 | $6,952.76 | ▼ -130.30 after sell → book $12,877.13; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BFLY` | 95 | $9.12 | $2.30 | $-78.68 | $7,816.86 | ▼ -78.68 after sell → book $12,874.83; vs 09:30 mark -2.30 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,816.86 | ▲ close $12,953.25 vs 09:30 $12,902.89 (session +78.42) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,417.18 | ▲ 09:30 equity $9,071.11 vs yday $9,065.61 (+5.50) | 09:30 open · cash $2,417.18 (unchanged overnight, no fees) · equity $9,071.11 vs prior close $9,065.61 (+5.50) | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 1 | $887.00 | $1.99 | — | $1,528.19 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+0.3; combo leftover $1208.59; owner union_earn_react_h3 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 7 | $26.27 | $1.86 | — | $1,342.44 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; combo leftover $191.02; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 49 | $3.86 | $2.04 | — | $1,151.26 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; combo leftover $191.02; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 1 | $184.00 | $1.84 | — | $965.42 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; combo leftover $191.02; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 11 | $16.21 | $1.82 | — | $785.29 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $191.02; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 1 | $123.50 | $1.24 | — | $660.55 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; combo leftover $191.02; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `QMCO` | 6 | $29.80 | $1.81 | — | $479.95 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+18.2; combo leftover $191.02; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 47 | $4.00 | $2.02 | — | $289.69 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; combo leftover $191.02; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CDNA` | 3 | $61.33 | $1.85 | — | $103.85 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+13.1; combo leftover $191.02; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $103.85 | ▲ close $9,106.97 vs 09:30 $9,071.11 (session +52.33) | 16:00 close · cash $103.85 · equity $9,106.97 vs 09:30 $9,071.11 (+35.86; session marks +52.33) · 18 name(s) marked open→close (per-name table). ABVX×11 09:30 $94.87 → close $94.87 +0.00; ANAB×21 09:30 $51.70 → close $51.70 +0.00; CBRL×10 09:30 $52.39 → close $51.81 -5.80; CTAS×2 09:30 $197.68 → close $197.68 -0.00; GIS×13 09:30 $34.83 → close $34.83 +0.00; KBH×10 09:30 $47.65 → close $47.65 +0.00; MLKN×57 09:30 $19.91 → close $19.91 -0.00; PAYX×4 09:30 $101.59 → close $101.59 -0.00; THO×16 09:30 $70.93 → close $70.93 +0.00; COST×1 09:30 $887.00 → close $922.76 +35.76; WRBY×7 09:30 $26.27 → close $26.71 +3.08; ZSQR×49 09:30 $3.86 → close $3.78 -3.92; TWST×1 09:30 $184.00 → close $182.83 -1.17; SECZ×11 09:30 $16.21 → close $15.96 -2.75; GRAL×1 09:30 $123.50 → close $126.89 +3.39; QMCO×6 09:30 $29.80 → close $31.68 +11.28; CYPH×47 09:30 $4.00 → close $4.12 +5.41; CDNA×3 09:30 $61.33 → close $63.68 +7.05 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `INO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `VOR` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `NMAX` | cash | leftover split 1.32 < 1 share @ 9.89 |
| 2026-08-14 | `AIRJ` | cash | leftover split 1.32 < 1 share @ 5.51 |
| 2026-08-14 | `AMAT` | cash | leftover split 1.32 < 1 share @ 499.40 |
| 2026-08-14 | `AMPG` | cash | leftover split 1.32 < 1 share @ 4.37 |
| 2026-08-14 | `BRUN` | cash | leftover split 1.32 < 1 share @ 26.25 |
| 2026-08-14 | `DGXX` | cash | leftover split 1.32 < 1 share @ 3.92 |
| 2026-08-14 | `BETR` | cash | leftover split 2.42 < 1 share @ 14.80 |
| 2026-08-14 | `ANGX` | cash | leftover split 2.42 < 1 share @ 4.31 |
| 2026-08-14 | `HYLN` | cash | leftover split 2.42 < 1 share @ 4.18 |
| 2026-08-14 | `ADUR` | cash | leftover split 2.42 < 1 share @ 16.50 |
| 2026-08-14 | `AIRO` | cash | leftover split 2.42 < 1 share @ 11.12 |
| 2026-08-14 | `NCMI` | cash | leftover split 2.42 < 1 share @ 2.69 |
| 2026-08-14 | `QMLS` | cash | leftover split 2.42 < 1 share @ 7.29 |
| 2026-08-17 | `INO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `VOR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `BZAI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `DEFT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `ABX` | cash | leftover split 3.86 < 1 share @ 9.12 |
| 2026-08-17 | `ALOY` | cash | leftover split 3.86 < 1 share @ 14.66 |
| 2026-08-17 | `BORR` | cash | leftover split 3.86 < 1 share @ 4.59 |
| 2026-08-17 | `XHG` | cash | leftover split 3.86 < 1 share @ 4.19 |
| 2026-08-17 | `MP` | cash | leftover split 3.86 < 1 share @ 58.01 |
| 2026-08-18 | `BZAI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `DEFT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `LOW` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `LZB` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `MRCY` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-21 | `AAP` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AEG` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ALVO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ATAT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ATHM` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `BABA` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `BILL` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `BULL` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AAP` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AEG` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ALVO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ATAT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ATHM` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BABA` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BILL` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BULL` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BEKE` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BJ` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BKE` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `PSEC` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `ROST` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_earn_react_h3 |
| 2026-08-25 | `BEKE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BJ` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BKE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `PSEC` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `ROST` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-26 | `BMO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BNS` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BZ` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `DKS` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `EH` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GFI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GRRR` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `SHMD` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BMO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BNS` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BZ` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `DKS` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `EH` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `GFI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `GRRR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `SHMD` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `TIGR` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `ANF` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BBWI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BOX` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `DY` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `FSCO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `HEI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `INTU` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TIGR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ANF` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBWI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BOX` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `DY` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `FSCO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `HEI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `INTU` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBY` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `BILI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CM` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CMBT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CSIQ` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `HQY` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `RY` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TD` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `BBY` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `BILI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CM` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CMBT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CSIQ` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `HQY` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `RY` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `TD` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `GAP` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `ADSK` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `BBAR` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `ESTC` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FINV` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FRO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `HAFN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `IREN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new long union_join_vol_green_h1 |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new long union_join_vol_green_h1 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new long union_earn_react_h3 |
| 2026-09-01 | `GAP` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `ADSK` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `BBAR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `ESTC` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FINV` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FRO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `HAFN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `IREN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-04 | `AI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `AVGO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CHPT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CIEN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CPB` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `FIVE` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `HPE` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `MEI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `DELL` | cash | leftover split 427.56 < 1 share @ 513.78 |
| 2026-09-08 | `AI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `AVGO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CHPT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CIEN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CPB` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `FIVE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `HPE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `MEI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `AMBA` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `ASAN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `DOCU` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `DOMO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `GWRE` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `IOT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `LULU` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `MAMA` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new long union_earn_react_h3 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new long union_earn_react_h3 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `AMBA` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `ASAN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `DOCU` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `DOMO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `GWRE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `IOT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `LULU` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `MAMA` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `ARBE` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `M` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `NAVN` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `NB` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `SHOE` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-14 | `ORCL` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `ADBE` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CPRT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `DSGX` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `KR` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `LPTH` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `REF` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `RH` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `GME` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `DHT` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `ORCL` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `ADBE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `CPRT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `DSGX` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `KR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `LPTH` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `REF` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `RH` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `IOT` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `SAFX` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-17 | `TCOM` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `TCOM` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `ALMU` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `LEN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-21 | `ALMU` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `LEN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-22 | `ALMU` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `GRPN` | no_price | no 09:30 open |
| 2026-09-22 | `XXI` | no_price | no 09:30 open |
| 2026-09-24 | `CBRL` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `CTAS` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `GIS` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `KBH` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `PAYX` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `GLND` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `KVYO` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CBRL` | 22 | 2026-09-23 @ $47.57 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-11.2; combo leftover $1083.29; owner union_earn_react_h3 |
| `CTAS` | 5 | 2026-09-23 @ $196.78 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $1083.29; owner union_earn_react_h3 |
| `GIS` | 30 | 2026-09-23 @ $35.74 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.1; combo leftover $1083.29; owner union_earn_react_h3 |
| `KBH` | 22 | 2026-09-23 @ $47.15 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-1.9; combo leftover $1083.29; owner union_earn_react_h3 |
| `PAYX` | 9 | 2026-09-23 @ $109.67 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $1083.29; owner union_earn_react_h3 |
