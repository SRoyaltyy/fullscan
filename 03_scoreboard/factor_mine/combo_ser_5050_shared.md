# Factor mine action — `combo_ser_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared short_news_r_h3/union_earn_react_h3 w=0.5,0.5 net=priority

Cash book **-12.68%** ($8,732) · signal-only (no cash/fees) was —. Starts YES **1/30**. Fills 245 · skips 327 · realized $+2456.60.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: short_news_r_h3 50%, union_earn_react_h3 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: short_news_r_h3 50%, union_earn_react_h3 50%.
- Member: short_news_r_h3 (50% · short · hold 3).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $4,800.59.

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
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 5 | $1.18 | $0.09 | — | $25.14 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $6.44; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.14 | ▲ close $11,883.83 vs 09:30 $10,963.61 (session +920.34) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.14 | ▼ 09:30 equity $11,733.56 vs yday $11,883.83 (-150.27) | — | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 4 | $1.15 | $0.08 | — | $29.66 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $5.03; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 1 | $3.56 | $0.06 | — | $33.16 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $5.03; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 1 | $3.01 | $0.05 | — | $36.12 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $5.03; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.12 | ▲ close $12,249.83 vs 09:30 $11,733.56 (session +516.46) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.12 | ▼ 09:30 equity $12,145.89 vs yday $12,249.83 (-103.94) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 6172 | $1.14 | $80.70 | $+1887.55 | $6,991.50 | ▲ +1,887.55 after sell → book $12,065.19; vs 09:30 mark -80.70 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 223 | $22.82 | $2.95 | $+174.80 | $12,077.40 | ▲ +174.80 after sell → book $12,062.23; vs 09:30 mark -2.96 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,077.40 | ▲ close $12,063.05 vs 09:30 $12,145.89 (session +0.81) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,077.40 | ▼ 09:30 equity $12,062.95 vs yday $12,063.05 (-0.10) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `BZAI` | 1 | $0.57 | $0.03 | $-0.24 | $12,077.95 | ▼ -0.24 after sell → book $12,062.93; vs 09:30 mark -0.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DEFT` | 2 | $0.43 | $0.03 | $-0.12 | $12,078.78 | ▼ -0.12 after sell → book $12,062.89; vs 09:30 mark -0.04 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 5 | $1.07 | $0.07 | $+0.39 | $12,073.36 | ▲ +0.39 after sell → book $12,062.82; vs 09:30 mark -0.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,073.36 | ▲ close $12,062.86 vs 09:30 $12,062.95 (session +0.04) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,073.36 | ▼ 09:30 equity $12,062.54 vs yday $12,062.86 (-0.32) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 4 | $0.96 | $0.05 | $+0.62 | $12,069.46 | ▲ +0.62 after sell → book $12,062.49; vs 09:30 mark -0.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 1 | $4.01 | $0.04 | $-0.56 | $12,065.40 | ▼ -0.56 after sell → book $12,062.45; vs 09:30 mark -0.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 1 | $2.95 | $0.03 | $-0.03 | $12,062.42 | ▼ -0.03 after sell → book $12,062.42; vs 09:30 mark -0.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 16 | $46.85 | $2.04 | — | $11,310.78 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+5.0; combo leftover $753.90; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 83 | $9.01 | $2.24 | — | $10,560.71 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $753.90; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 193 | $3.89 | $2.57 | — | $9,807.37 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.5; combo leftover $753.90; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 22 | $34.05 | $2.06 | — | $9,056.22 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+9.3; combo leftover $753.90; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 33 | $22.44 | $2.09 | — | $8,313.61 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.1; combo leftover $753.90; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 6 | $123.47 | $2.01 | — | $7,570.78 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+2.9; combo leftover $753.90; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `BILL` | 15 | $49.00 | $2.04 | — | $6,833.74 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-2.0; combo leftover $753.90; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 75 | $9.94 | $2.21 | — | $6,086.03 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+12.6; combo leftover $753.90; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 4 | $204.45 | $2.04 | — | $6,901.79 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $860.37; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 40 | $21.40 | $2.15 | — | $7,755.63 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $860.37; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 194 | $4.43 | $2.64 | — | $8,612.41 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; combo leftover $860.37; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 72 | $11.81 | $2.25 | — | $9,460.84 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $860.37; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 4 | $173.90 | $2.04 | — | $10,154.40 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $860.37; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 8 | $106.38 | $2.06 | — | $11,003.38 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $860.37; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 186 | $4.61 | $2.61 | — | $11,858.23 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $860.37; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,858.23 | ▼ close $11,979.86 vs 09:30 $12,062.54 (session -49.51) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,858.23 | ▼ 09:30 equity $11,908.54 vs yday $11,979.86 (-71.32) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 66 | $17.93 | $2.19 | — | $10,672.33 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $1185.82; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 12 | $93.98 | $2.03 | — | $9,542.55 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.4; combo leftover $1185.82; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 27 | $43.08 | $2.07 | — | $8,377.32 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.9; combo leftover $1185.82; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 515 | $2.30 | $6.64 | — | $7,186.17 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.0; combo leftover $1185.82; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `ROST` | 4 | $243.85 | $2.00 | — | $6,208.77 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-6.5; combo leftover $1185.82; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 318 | $3.11 | $4.19 | — | $7,193.56 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $991.13; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 7 | $133.11 | $2.06 | — | $8,123.27 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; combo leftover $991.13; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 11 | $89.10 | $2.07 | — | $9,101.30 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $991.13; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 25 | $38.40 | $2.11 | — | $10,059.19 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $991.13; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 47 | $20.90 | $2.18 | — | $11,039.31 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $991.13; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 36 | $27.00 | $2.14 | — | $12,009.17 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $991.13; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,009.17 | ▲ close $11,886.47 vs 09:30 $11,908.54 (session +7.61) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,009.17 | ▲ 09:30 equity $12,068.10 vs yday $11,886.47 (+181.63) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,009.17 | ▲ close $12,272.78 vs 09:30 $12,068.10 (session +204.68) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,009.17 | ▲ 09:30 equity $12,299.46 vs yday $12,272.78 (+26.68) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 16 | $43.63 | $2.06 | $-55.62 | $12,705.19 | ▼ -55.62 after sell → book $12,297.41; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 83 | $9.23 | $2.26 | $+13.76 | $13,469.02 | ▲ +13.76 after sell → book $12,295.14; vs 09:30 mark -2.27 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 193 | $5.24 | $2.61 | $+255.37 | $14,477.73 | ▲ +255.37 after sell → book $12,292.53; vs 09:30 mark -2.61 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 22 | $34.72 | $2.08 | $+10.61 | $15,239.49 | ▲ +10.61 after sell → book $12,290.46; vs 09:30 mark -2.07 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 33 | $21.85 | $2.11 | $-23.67 | $15,958.43 | ▼ -23.67 after sell → book $12,288.35; vs 09:30 mark -2.11 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BABA` | 6 | $117.94 | $2.03 | $-37.22 | $16,664.04 | ▼ -37.22 after sell → book $12,286.32; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BILL` | 15 | $47.98 | $2.06 | $-19.31 | $17,381.76 | ▼ -19.31 after sell → book $12,284.26; vs 09:30 mark -2.06 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BULL` | 75 | $8.46 | $2.24 | $-115.45 | $18,014.03 | ▼ -115.45 after sell → book $12,282.03; vs 09:30 mark -2.23 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 4 | $212.00 | $2.00 | $-34.25 | $17,164.03 | ▼ -34.25 after sell → book $12,280.03; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 40 | $20.90 | $2.11 | $+15.74 | $16,325.92 | ▲ +15.74 after sell → book $12,277.92; vs 09:30 mark -2.11 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 194 | $4.42 | $2.57 | $-3.27 | $15,465.86 | ▼ -3.27 after sell → book $12,275.34; vs 09:30 mark -2.58 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 72 | $11.00 | $2.21 | $+54.22 | $14,671.66 | ▲ +54.22 after sell → book $12,273.14; vs 09:30 mark -2.20 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 4 | $170.64 | $2.00 | $+9.00 | $13,987.10 | ▲ +9.00 after sell → book $12,271.14; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 8 | $105.58 | $2.01 | $+2.33 | $13,140.44 | ▲ +2.33 after sell → book $12,269.12; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 186 | $4.77 | $2.55 | $-34.92 | $12,250.67 | ▼ -34.92 after sell → book $12,266.57; vs 09:30 mark -2.55 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 4 | $175.01 | $2.00 | — | $11,548.63 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-7.0; combo leftover $765.67; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 8 | $88.94 | $2.01 | — | $10,835.10 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-4.9; combo leftover $765.67; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 50 | $15.28 | $2.14 | — | $10,068.96 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $765.67; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 5 | $142.36 | $2.00 | — | $9,355.15 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-8.6; combo leftover $765.67; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 150 | $5.10 | $2.44 | — | $8,587.71 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-8.9; combo leftover $765.67; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 15 | $47.89 | $2.04 | — | $7,867.33 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ⚪; ret5=+14.0; combo leftover $765.67; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 55 | $13.92 | $2.15 | — | $7,099.57 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+5.9; combo leftover $765.67; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 168 | $4.54 | $2.49 | — | $6,333.52 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-14.6; combo leftover $765.67; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 149 | $13.62 | $2.53 | — | $8,361.11 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $2041.55; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 37 | $54.51 | $2.18 | — | $10,375.80 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $2041.55; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 5 | $364.35 | $2.08 | — | $12,195.47 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $2041.55; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,195.47 | ▼ close $11,860.75 vs 09:30 $12,299.46 (session -381.74) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,195.47 | ▲ 09:30 equity $12,153.79 vs yday $11,860.75 (+293.04) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `BEKE` | 66 | $18.14 | $2.21 | $+9.13 | $13,390.50 | ▲ +9.13 after sell → book $12,151.58; vs 09:30 mark -2.21 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BJ` | 12 | $94.60 | $2.05 | $+3.37 | $14,523.65 | ▲ +3.37 after sell → book $12,149.53; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BKE` | 27 | $44.39 | $2.09 | $+31.21 | $15,720.09 | ▲ +31.21 after sell → book $12,147.44; vs 09:30 mark -2.09 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 515 | $2.35 | $6.74 | $+12.37 | $16,923.60 | ▲ +12.37 after sell → book $12,140.70; vs 09:30 mark -6.74 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ROST` | 4 | $242.50 | $2.02 | $-9.42 | $17,891.58 | ▼ -9.42 after sell → book $12,138.68; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 318 | $2.83 | $4.10 | $+80.75 | $16,987.54 | ▲ +80.75 after sell → book $12,134.58; vs 09:30 mark -4.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 7 | $154.20 | $2.01 | $-151.70 | $15,906.13 | ▼ -151.70 after sell → book $12,132.57; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 11 | $88.24 | $2.02 | $+5.37 | $14,933.46 | ▲ +5.37 after sell → book $12,130.54; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 25 | $38.41 | $2.06 | $-4.43 | $13,971.15 | ▼ -4.43 after sell → book $12,128.48; vs 09:30 mark -2.06 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 47 | $20.50 | $2.13 | $+14.49 | $13,005.52 | ▲ +14.49 after sell → book $12,126.35; vs 09:30 mark -2.13 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 36 | $26.00 | $2.10 | $+31.76 | $12,067.42 | ▲ +31.76 after sell → book $12,124.25; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 165 | $5.21 | $2.48 | — | $11,205.29 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $861.96; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 6 | $131.37 | $2.01 | — | $10,415.06 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.3; combo leftover $861.96; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 47 | $18.26 | $2.13 | — | $9,554.71 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-11.4; combo leftover $861.96; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 25 | $34.30 | $2.06 | — | $8,695.14 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+1.7; combo leftover $861.96; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 2 | $326.91 | $2.00 | — | $8,039.33 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-15.2; combo leftover $861.96; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `FSCO` | 169 | $5.08 | $2.50 | — | $7,178.31 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-1.6; combo leftover $861.96; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `HEI` | 2 | $370.00 | $2.00 | — | $6,436.31 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.6; combo leftover $861.96; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 5 | $213.94 | $2.05 | — | $7,503.96 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $1210.91; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 99 | $12.22 | $2.35 | — | $8,711.39 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $1210.91; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 238 | $5.08 | $3.15 | — | $9,917.28 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $1210.91; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 9 | $132.64 | $2.07 | — | $11,108.97 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $1210.91; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 6 | $199.94 | $2.06 | — | $12,306.55 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; combo leftover $1210.91; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,306.55 | ▲ close $12,132.46 vs 09:30 $12,153.79 (session +35.07) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,306.55 | ▼ 09:30 equity $11,929.32 vs yday $12,132.46 (-203.14) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 9 | $80.60 | $2.02 | — | $11,579.13 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.0; combo leftover $769.16; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 47 | $16.18 | $2.13 | — | $10,816.54 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-6.7; combo leftover $769.16; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 6 | $118.77 | $2.01 | — | $10,101.91 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.3; combo leftover $769.16; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 43 | $17.78 | $2.12 | — | $9,335.25 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-1.2; combo leftover $769.16; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 57 | $13.41 | $2.16 | — | $8,568.72 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.1; combo leftover $769.16; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 7 | $97.16 | $2.01 | — | $7,886.59 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.5; combo leftover $769.16; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 3 | $206.82 | $2.00 | — | $7,264.13 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-0.2; combo leftover $769.16; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 6 | $120.17 | $2.01 | — | $6,541.10 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.9; combo leftover $769.16; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 39 | $74.54 | $2.22 | — | $9,445.94 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $2978.22; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 53 | $55.25 | $2.26 | — | $12,371.93 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $2978.22; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,371.93 | ▼ close $11,779.00 vs 09:30 $11,929.32 (session -129.38) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,371.93 | ▲ 09:30 equity $11,801.06 vs yday $11,779.00 (+22.06) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 4 | $172.76 | $2.02 | $-13.02 | $13,060.95 | ▼ -13.02 after sell → book $11,799.04; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 8 | $93.30 | $2.03 | $+30.83 | $13,805.32 | ▲ +30.83 after sell → book $11,797.01; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 50 | $18.15 | $2.16 | $+139.20 | $14,710.66 | ▲ +139.20 after sell → book $11,794.85; vs 09:30 mark -2.16 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 5 | $132.80 | $2.02 | $-51.83 | $15,372.63 | ▼ -51.83 after sell → book $11,792.82; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 150 | $4.58 | $2.47 | $-82.91 | $16,057.16 | ▼ -82.91 after sell → book $11,790.35; vs 09:30 mark -2.47 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 15 | $48.42 | $2.06 | $+3.86 | $16,781.40 | ▲ +3.86 after sell → book $11,788.29; vs 09:30 mark -2.06 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 55 | $15.66 | $2.17 | $+91.37 | $17,640.53 | ▲ +91.37 after sell → book $11,786.12; vs 09:30 mark -2.17 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 168 | $3.38 | $2.53 | $-200.75 | $18,205.83 | ▼ -200.75 after sell → book $11,783.58; vs 09:30 mark -2.54 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 149 | $13.90 | $2.44 | $-45.95 | $16,132.30 | ▼ -45.95 after sell → book $11,781.15; vs 09:30 mark -2.43 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 37 | $52.49 | $2.10 | $+70.46 | $14,188.07 | ▲ +70.46 after sell → book $11,779.05; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 5 | $347.82 | $2.00 | $+78.57 | $12,446.96 | ▲ +78.57 after sell → book $11,777.04; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 2 | $261.16 | $2.00 | — | $11,922.64 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+7.8; combo leftover $777.94; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 51 | $15.01 | $2.14 | — | $11,154.99 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+3.7; combo leftover $777.94; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 7 | $103.89 | $2.01 | — | $10,425.75 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.5; combo leftover $777.94; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 200 | $3.88 | $2.59 | — | $9,647.16 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-8.6; combo leftover $777.94; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 17 | $44.40 | $2.04 | — | $8,890.32 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.4; combo leftover $777.94; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 31 | $24.69 | $2.08 | — | $8,122.85 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.8; combo leftover $777.94; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 93 | $8.35 | $2.27 | — | $7,344.03 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.1; combo leftover $777.94; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 20 | $37.65 | $2.05 | — | $6,589.08 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.9; combo leftover $777.94; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 11 | $252.24 | $2.13 | — | $9,361.59 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $2939.96; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 97 | $30.18 | $2.40 | — | $12,286.65 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $2939.96; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,286.65 | ▲ close $11,932.76 vs 09:30 $11,801.06 (session +177.45) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,286.65 | ▲ 09:30 equity $12,052.44 vs yday $11,932.76 (+119.68) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 165 | $5.00 | $2.52 | $-39.66 | $13,109.12 | ▼ -39.66 after sell → book $12,049.91; vs 09:30 mark -2.53 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 6 | $148.03 | $2.03 | $+95.92 | $13,995.28 | ▲ +95.92 after sell → book $12,047.89; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 47 | $19.25 | $2.15 | $+42.25 | $14,897.87 | ▲ +42.25 after sell → book $12,045.73; vs 09:30 mark -2.16 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BOX` | 25 | $34.72 | $2.08 | $+6.35 | $15,763.79 | ▲ +6.35 after sell → book $12,043.65; vs 09:30 mark -2.08 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 2 | $298.01 | $2.02 | $-61.81 | $16,357.79 | ▼ -61.81 after sell → book $12,041.63; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FSCO` | 169 | $5.20 | $2.54 | $+15.25 | $17,234.06 | ▲ +15.25 after sell → book $12,039.10; vs 09:30 mark -2.53 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `HEI` | 2 | $334.88 | $2.02 | $-74.25 | $17,901.80 | ▼ -74.25 after sell → book $12,037.08; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 5 | $208.88 | $2.00 | $+21.24 | $16,855.40 | ▲ +21.24 after sell → book $12,035.08; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 99 | $11.10 | $2.29 | $+106.25 | $15,754.21 | ▲ +106.25 after sell → book $12,032.79; vs 09:30 mark -2.29 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 238 | $4.97 | $3.07 | $+18.77 | $14,567.09 | ▲ +18.77 after sell → book $12,029.72; vs 09:30 mark -3.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 9 | $127.45 | $2.02 | $+42.62 | $13,418.02 | ▲ +42.62 after sell → book $12,027.70; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 6 | $254.39 | $2.01 | $-330.77 | $11,889.68 | ▼ -330.77 after sell → book $12,025.70; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,889.68 | ▲ close $12,026.72 vs 09:30 $12,052.44 (session +1.02) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,889.68 | ▲ 09:30 equity $12,113.42 vs yday $12,026.72 (+86.70) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BBY` | 9 | $79.83 | $2.04 | $-10.98 | $12,606.11 | ▼ -10.98 after sell → book $12,111.38; vs 09:30 mark -2.04 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BILI` | 47 | $15.97 | $2.15 | $-14.15 | $13,354.55 | ▼ -14.15 after sell → book $12,109.23; vs 09:30 mark -2.15 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CM` | 6 | $113.66 | $2.03 | $-34.70 | $14,034.48 | ▼ -34.70 after sell → book $12,107.20; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CMBT` | 43 | $18.28 | $2.14 | $+17.24 | $14,818.38 | ▲ +17.24 after sell → book $12,105.06; vs 09:30 mark -2.14 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CSIQ` | 57 | $12.18 | $2.18 | $-74.45 | $15,510.46 | ▼ -74.45 after sell → book $12,102.88; vs 09:30 mark -2.18 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `HQY` | 7 | $96.65 | $2.03 | $-7.61 | $16,184.98 | ▼ -7.61 after sell → book $12,100.85; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RY` | 3 | $203.78 | $2.02 | $-13.14 | $16,794.30 | ▼ -13.14 after sell → book $12,098.83; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TD` | 6 | $120.54 | $2.03 | $-1.82 | $17,515.51 | ▼ -1.82 after sell → book $12,096.80; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 39 | $73.22 | $2.11 | $+47.15 | $14,657.82 | ▲ +47.15 after sell → book $12,094.69; vs 09:30 mark -2.11 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 53 | $54.76 | $2.15 | $+21.56 | $11,753.40 | ▲ +21.56 after sell → book $12,092.55; vs 09:30 mark -2.14 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,753.40 | ▼ close $12,051.58 vs 09:30 $12,113.42 (session -40.97) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,753.40 | ▲ 09:30 equity $12,075.64 vs yday $12,051.58 (+24.06) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 2 | $246.70 | $2.02 | $-32.93 | $12,244.78 | ▼ -32.93 after sell → book $12,073.62; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 51 | $15.01 | $2.16 | $-4.31 | $13,008.13 | ▼ -4.31 after sell → book $12,071.46; vs 09:30 mark -2.16 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 7 | $92.00 | $2.03 | $-87.27 | $13,650.10 | ▼ -87.27 after sell → book $12,069.43; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 200 | $3.32 | $2.63 | $-117.22 | $14,311.46 | ▼ -117.22 after sell → book $12,066.79; vs 09:30 mark -2.64 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 17 | $44.17 | $2.06 | $-8.01 | $15,060.29 | ▼ -8.01 after sell → book $12,064.73; vs 09:30 mark -2.06 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 31 | $21.97 | $2.10 | $-88.51 | $15,739.26 | ▼ -88.51 after sell → book $12,062.63; vs 09:30 mark -2.10 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 93 | $8.58 | $2.29 | $+16.83 | $16,534.90 | ▲ +16.83 after sell → book $12,060.33; vs 09:30 mark -2.30 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 20 | $35.80 | $2.07 | $-41.12 | $17,248.73 | ▼ -41.12 after sell → book $12,058.26; vs 09:30 mark -2.07 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 11 | $235.71 | $2.02 | $+177.68 | $14,653.90 | ▲ +177.68 after sell → book $12,056.24; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 97 | $26.78 | $2.28 | $+325.12 | $12,053.96 | ▲ +325.12 after sell → book $12,053.96; vs 09:30 mark -2.28 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,053.96 | ▲ close $12,053.96 vs 09:30 $12,075.64 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,053.96 | ▲ 09:30 equity $12,053.96 vs yday $12,053.96 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 70 | $10.74 | $2.20 | — | $11,299.61 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+8.5; combo leftover $753.37; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 2 | $351.74 | $2.00 | — | $10,594.13 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+3.3; combo leftover $753.37; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 109 | $6.90 | $2.32 | — | $9,839.72 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.8; combo leftover $753.37; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 2 | $354.49 | $2.00 | — | $9,128.74 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.3; combo leftover $753.37; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 33 | $22.32 | $2.09 | — | $8,390.09 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+2.4; combo leftover $753.37; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 2 | $257.00 | $2.00 | — | $7,874.10 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-5.5; combo leftover $753.37; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 15 | $47.60 | $2.04 | — | $7,158.06 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-6.2; combo leftover $753.37; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 49 | $15.09 | $2.14 | — | $6,416.51 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+6.1; combo leftover $753.37; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 202 | $14.85 | $2.75 | — | $9,413.47 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $3009.30; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1759 | $1.71 | $23.09 | — | $12,398.27 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $3009.30; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,398.27 | ▲ close $12,458.15 vs 09:30 $12,053.96 (session +446.79) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,398.27 | ▲ 09:30 equity $12,550.63 vs yday $12,458.15 (+92.48) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 12 | $63.18 | $2.03 | — | $11,638.08 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-10.9; combo leftover $774.89; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 88 | $8.74 | $2.25 | — | $10,866.71 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.8; combo leftover $774.89; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 11 | $68.52 | $2.02 | — | $10,110.97 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+3.4; combo leftover $774.89; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 214 | $3.62 | $2.76 | — | $9,334.59 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-3.1; combo leftover $774.89; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 4 | $167.55 | $2.00 | — | $8,662.39 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.9; combo leftover $774.89; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 17 | $44.90 | $2.04 | — | $7,897.05 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-7.5; combo leftover $774.89; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 7 | $98.15 | $2.01 | — | $7,207.99 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.9; combo leftover $774.89; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 49 | $15.70 | $2.14 | — | $6,436.55 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-0.4; combo leftover $774.89; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 670 | $4.67 | $8.87 | — | $9,556.59 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $3133.34; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 40 | $76.55 | $2.23 | — | $12,616.36 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $3133.34; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,616.36 | ▼ close $12,393.66 vs 09:30 $12,550.63 (session -128.62) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,616.36 | ▲ 09:30 equity $12,421.86 vs yday $12,393.66 (+28.20) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,616.36 | ▲ close $12,698.51 vs 09:30 $12,421.86 (session +276.65) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,616.36 | ▼ 09:30 equity $12,682.95 vs yday $12,698.51 (-15.56) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 70 | $10.51 | $2.22 | $-20.87 | $13,349.84 | ▼ -20.87 after sell → book $12,680.73; vs 09:30 mark -2.22 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 2 | $366.23 | $2.02 | $+24.97 | $14,080.28 | ▲ +24.97 after sell → book $12,678.71; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 109 | $9.39 | $2.35 | $+266.75 | $15,101.45 | ▲ +266.75 after sell → book $12,676.37; vs 09:30 mark -2.34 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 2 | $341.90 | $2.02 | $-29.19 | $15,783.23 | ▼ -29.19 after sell → book $12,674.35; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CPB` | 33 | $21.67 | $2.11 | $-25.65 | $16,496.23 | ▼ -25.65 after sell → book $12,672.24; vs 09:30 mark -2.11 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 2 | $252.92 | $2.02 | $-12.17 | $17,000.06 | ▼ -12.17 after sell → book $12,670.23; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 15 | $56.94 | $2.06 | $+136.01 | $17,852.10 | ▲ +136.01 after sell → book $12,668.17; vs 09:30 mark -2.06 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MEI` | 49 | $13.84 | $2.16 | $-65.54 | $18,528.10 | ▼ -65.54 after sell → book $12,666.01; vs 09:30 mark -2.16 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 202 | $13.60 | $2.61 | $+247.15 | $15,778.30 | ▲ +247.15 after sell → book $12,663.41; vs 09:30 mark -2.60 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1759 | $1.58 | $22.69 | $+182.89 | $12,976.39 | ▲ +182.89 after sell → book $12,640.72; vs 09:30 mark -22.69 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,976.39 | ▼ close $12,629.72 vs 09:30 $12,682.95 (session -11.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,976.39 | ▲ 09:30 equity $12,693.24 vs yday $12,629.72 (+63.52) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `AMBA` | 12 | $67.44 | $2.05 | $+47.05 | $13,783.62 | ▲ +47.05 after sell → book $12,691.20; vs 09:30 mark -2.04 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ASAN` | 88 | $8.26 | $2.28 | $-46.77 | $14,508.22 | ▼ -46.77 after sell → book $12,688.92; vs 09:30 mark -2.28 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOCU` | 11 | $64.60 | $2.04 | $-47.19 | $15,216.78 | ▼ -47.19 after sell → book $12,686.87; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 214 | $3.76 | $2.81 | $+25.46 | $16,018.61 | ▲ +25.46 after sell → book $12,684.07; vs 09:30 mark -2.80 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `GWRE` | 4 | $142.43 | $2.02 | $-104.50 | $16,586.31 | ▼ -104.50 after sell → book $12,682.05; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `IOT` | 17 | $38.23 | $2.06 | $-117.58 | $17,234.08 | ▼ -117.58 after sell → book $12,679.99; vs 09:30 mark -2.06 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `LULU` | 7 | $98.71 | $2.03 | $-0.12 | $17,923.01 | ▼ -0.12 after sell → book $12,677.95; vs 09:30 mark -2.04 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MAMA` | 49 | $15.26 | $2.16 | $-25.85 | $18,668.60 | ▼ -25.85 after sell → book $12,675.80; vs 09:30 mark -2.15 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 670 | $4.36 | $8.64 | $+190.19 | $15,738.75 | ▲ +190.19 after sell → book $12,667.15; vs 09:30 mark -8.65 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 40 | $76.79 | $2.11 | $-13.94 | $12,665.04 | ▼ -13.94 after sell → book $12,665.04; vs 09:30 mark -2.11 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,665.04 | ▲ close $12,665.04 vs 09:30 $12,693.24 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,665.04 | ▲ 09:30 equity $12,665.04 vs yday $12,665.04 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 4 | $164.43 | $2.00 | — | $12,005.32 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $791.57; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 3 | $242.17 | $2.00 | — | $11,276.81 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-11.1; combo leftover $791.57; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 24 | $32.01 | $2.06 | — | $10,506.51 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-4.4; combo leftover $791.57; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 11 | $71.71 | $2.02 | — | $9,715.68 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-9.1; combo leftover $791.57; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 14 | $56.02 | $2.03 | — | $8,929.37 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.2; combo leftover $791.57; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 84 | $9.37 | $2.24 | — | $8,140.04 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+1.5; combo leftover $791.57; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 60 | $13.10 | $2.17 | — | $7,351.87 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-6.9; combo leftover $791.57; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 5 | $135.71 | $2.00 | — | $6,671.32 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-9.2; combo leftover $791.57; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 11 | $112.83 | $2.08 | — | $7,910.43 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $1264.85; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 359 | $3.52 | $4.74 | — | $9,169.37 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $1264.85; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 623 | $2.03 | $8.18 | — | $10,425.88 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $1264.85; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 50 | $24.97 | $2.19 | — | $11,672.18 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $1264.85; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 375 | $3.37 | $4.94 | — | $12,930.99 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $1264.85; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,930.99 | ▲ close $12,653.36 vs 09:30 $12,665.04 (session +26.98) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,930.99 | ▲ 09:30 equity $12,695.87 vs yday $12,653.36 (+42.51) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,930.99 | ▼ close $12,693.35 vs 09:30 $12,695.87 (session -2.52) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,930.99 | ▼ 09:30 equity $12,672.71 vs yday $12,693.35 (-20.64) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,930.99 | ▼ close $12,590.82 vs 09:30 $12,672.71 (session -81.89) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,930.99 | ▼ 09:30 equity $12,564.90 vs yday $12,590.82 (-25.92) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 4 | $140.03 | $2.02 | $-101.62 | $13,489.09 | ▼ -101.62 after sell → book $12,562.88; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 3 | $253.34 | $2.02 | $+29.49 | $14,247.09 | ▲ +29.49 after sell → book $12,560.86; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CPRT` | 24 | $30.57 | $2.08 | $-38.70 | $14,978.69 | ▼ -38.70 after sell → book $12,558.78; vs 09:30 mark -2.08 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DSGX` | 11 | $78.12 | $2.04 | $+66.44 | $15,835.96 | ▲ +66.44 after sell → book $12,556.73; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `KR` | 14 | $61.93 | $2.05 | $+78.66 | $16,700.93 | ▲ +78.66 after sell → book $12,554.68; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `LPTH` | 84 | $9.40 | $2.27 | $-1.99 | $17,488.26 | ▼ -1.99 after sell → book $12,552.41; vs 09:30 mark -2.27 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `REF` | 60 | $15.75 | $2.19 | $+154.64 | $18,431.07 | ▲ +154.64 after sell → book $12,550.22; vs 09:30 mark -2.19 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RH` | 5 | $125.55 | $2.02 | $-54.83 | $19,056.80 | ▼ -54.83 after sell → book $12,548.20; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 11 | $118.18 | $2.02 | $-62.90 | $17,754.80 | ▼ -62.90 after sell → book $12,546.18; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 359 | $3.98 | $4.63 | $-174.51 | $16,321.35 | ▼ -174.51 after sell → book $12,541.55; vs 09:30 mark -4.63 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 623 | $1.85 | $8.04 | $+95.92 | $15,160.76 | ▲ +95.92 after sell → book $12,533.51; vs 09:30 mark -8.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 50 | $24.42 | $2.14 | $+23.17 | $13,937.62 | ▲ +23.17 after sell → book $12,531.37; vs 09:30 mark -2.14 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 375 | $3.75 | $4.84 | $-152.28 | $12,526.53 | ▼ -152.28 after sell → book $12,526.53; vs 09:30 mark -4.84 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 153 | $40.93 | $2.45 | — | $6,261.79 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.1; combo leftover $6263.27; owner union_earn_react_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 168 | $18.61 | $2.63 | — | $9,385.64 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $3130.90; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 458 | $6.83 | $6.10 | — | $12,507.68 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $3130.90; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,507.68 | ▼ close $11,994.81 vs 09:30 $12,564.90 (session -520.54) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,507.68 | ▲ 09:30 equity $12,007.43 vs yday $11,994.81 (+12.62) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 38 | $81.00 | $2.10 | — | $9,427.58 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-3.0; combo leftover $3126.92; owner union_earn_react_h3 | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 278 | $11.21 | $3.59 | — | $6,307.61 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+1.0; combo leftover $3126.92; owner union_earn_react_h3 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 754 | $7.95 | $10.06 | — | $12,291.85 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $6000.87; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,291.85 | ▲ close $12,241.18 vs 09:30 $12,007.43 (session +249.50) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,291.85 | ▼ 09:30 equity $12,176.18 vs yday $12,241.18 (-65.00) | — | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 176 | $34.44 | $2.76 | — | $18,350.53 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $6088.09; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,350.53 | ▲ close $12,353.44 vs 09:30 $12,176.18 (session +180.02) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,350.53 | ▼ 09:30 equity $12,213.36 vs yday $12,353.44 (-140.08) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 153 | $41.00 | $2.52 | $+5.74 | $24,621.00 | ▲ +5.74 after sell → book $12,210.83; vs 09:30 mark -2.53 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 168 | $22.11 | $2.49 | $-593.13 | $20,904.03 | ▼ -593.13 after sell → book $12,208.34; vs 09:30 mark -2.49 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 458 | $6.55 | $5.91 | $+116.24 | $17,898.22 | ▲ +116.24 after sell → book $12,202.43; vs 09:30 mark -5.91 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 369 | $8.26 | $4.93 | — | $20,941.23 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3050.61; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 5 | $583.88 | $2.12 | — | $23,858.51 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $3050.61; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23,858.51 | ▲ close $12,243.79 vs 09:30 $12,213.36 (session +48.41) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23,858.51 | ▲ 09:30 equity $12,269.69 vs yday $12,243.79 (+25.90) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 754 | $8.28 | $9.73 | $-264.84 | $17,609.44 | ▼ -264.84 after sell → book $12,259.97; vs 09:30 mark -9.72 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 32 | $93.97 | $2.20 | — | $20,614.28 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3064.99; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,614.28 | ▼ close $12,173.05 vs 09:30 $12,269.69 (session -84.72) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,614.28 | ▼ 09:30 equity $11,798.64 vs yday $12,173.05 (-374.41) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `LEN` | 38 | $82.00 | $2.14 | $+33.76 | $23,728.14 | ▲ +33.76 after sell → book $11,796.50; vs 09:30 mark -2.14 | union_earn_react_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `ALMU` | 278 | $13.82 | $3.66 | $+718.33 | $27,566.43 | ▲ +718.33 after sell → book $11,792.83; vs 09:30 mark -3.67 | union_earn_react_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 57 | $47.57 | $2.16 | — | $24,852.78 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-11.2; combo leftover $2756.64; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 14 | $196.78 | $2.03 | — | $22,095.83 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $2756.64; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 77 | $35.74 | $2.22 | — | $19,341.63 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.1; combo leftover $2756.64; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 58 | $47.15 | $2.16 | — | $16,604.77 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-1.9; combo leftover $2756.64; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 25 | $109.67 | $2.06 | — | $13,860.95 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $2756.64; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 50 | $116.85 | $2.36 | — | $19,701.09 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $5891.10; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,701.09 | ▲ close $11,833.04 vs 09:30 $11,798.64 (session +53.21) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,701.09 | ▲ 09:30 equity $11,946.91 vs yday $11,833.04 (+113.87) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 5 | $600.27 | $2.00 | $-86.07 | $16,697.74 | ▼ -86.07 after sell → book $11,944.91; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,697.74 | ▼ close $11,883.86 vs 09:30 $11,946.91 (session -61.05) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,178.57 | ▼ 09:30 equity $8,728.42 vs yday $8,736.15 (-7.73) | 09:30 open · cash $4,178.57 (unchanged overnight, no fees) · equity $8,728.42 vs prior close $8,736.15 (-7.73) | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 2 | $887.00 | $2.00 | — | $2,402.57 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+0.3; combo leftover $2089.28; owner union_earn_react_h3 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 306 | $7.85 | $4.08 | — | $4,800.59 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $2402.57; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,800.59 | ▲ close $8,732.07 vs 09:30 $8,728.42 (session +9.73) | 16:00 close · cash $4,800.59 · equity $8,732.07 vs 09:30 $8,728.42 (+3.65; session marks +9.73) · 15 name(s) marked open→close (per-name table). ABVX×22 09:30 $94.87 → close $94.87 +0.00; AEHL×284 09:30 $9.05 → close $9.36 -88.04; ANAB×42 09:30 $51.70 → close $51.70 +0.00; BAND×37 09:30 $61.83 → close $61.83 -0.00; CBRL×37 09:30 $52.39 → close $51.81 -21.46; CTAS×8 09:30 $197.68 → close $197.68 -0.00; GIS×49 09:30 $34.83 → close $34.83 +0.00; HALO×18 09:30 $115.36 → close $113.90 +26.28; KBH×37 09:30 $47.65 → close $47.65 +0.00; MLKN×113 09:30 $19.91 → close $19.91 -0.00; PAYX×20 09:30 $101.59 → close $101.59 +0.00; THO×32 09:30 $70.93 → close $70.93 +0.00; USFD×24 09:30 $93.82 → close $93.82 +0.00; COST×2 09:30 $887.00 → close $922.76 +71.53; RSKD×306 09:30 $7.85 → close $7.78 +21.42 | — |

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
| 2026-08-14 | `LUNR` | cash | leftover split 6.44 < 1 share @ 19.17 |
| 2026-08-14 | `OWL` | cash | leftover split 6.44 < 1 share @ 12.70 |
| 2026-08-17 | `INO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `VOR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `BZAI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `DEFT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `EU` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `APMD` | cash | leftover split 5.03 < 1 share @ 31.70 |
| 2026-08-17 | `RNW` | cash | leftover split 5.03 < 1 share @ 6.80 |
| 2026-08-18 | `BZAI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `DEFT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `EU` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `RNW` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `LUNR` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
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
| 2026-08-21 | `AEM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WYFI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TOYO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AAP` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AEG` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ALVO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ATAT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ATHM` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BABA` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BILL` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BULL` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AEM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WYFI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TOYO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BEKE` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BJ` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BKE` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `PSEC` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `ROST` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
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
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_earn_react_h3 |
| 2026-08-25 | `BEKE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BJ` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BKE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `PSEC` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `ROST` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `QTRX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `MRNA` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `AUGO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `ARIS` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `NOG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-26 | `BMO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BNS` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BZ` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `DKS` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `EH` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GFI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GRRR` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `SHMD` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `INTU` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BMO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BNS` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BZ` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `DKS` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `EH` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `GFI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `GRRR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `SHMD` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `TIGR` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `ANF` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BBWI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BOX` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `DY` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `FSCO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `HEI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `NEM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `CRM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TIGR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ANF` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBWI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BOX` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `DY` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `FSCO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `HEI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `NEM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `CRM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBY` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `BILI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CM` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CMBT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CSIQ` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `HQY` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `RY` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TD` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `MT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TX` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `BBY` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `BILI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CM` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CMBT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CSIQ` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `HQY` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `RY` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `TD` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `MT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `TX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `ADSK` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `BBAR` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `ESTC` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FINV` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FRO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `GAP` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `HAFN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `IREN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new long union_earn_react_h3 |
| 2026-09-01 | `ADSK` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `BBAR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `ESTC` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FINV` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FRO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `GAP` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `HAFN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `IREN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
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
| 2026-09-04 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `AI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `AVGO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CHPT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CIEN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CPB` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `FIVE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `HPE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `MEI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `OPK` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `AMBA` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `ASAN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `DOCU` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `DOMO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `GWRE` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `IOT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `LULU` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `MAMA` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
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
| 2026-09-09 | `GSM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new short short_news_r_h3 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
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
| 2026-09-14 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-15 | `ORCL` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `ADBE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `CPRT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `DSGX` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `KR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `LPTH` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `REF` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `RH` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `MYGN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-17 | `TCOM` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-17 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `TCOM` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `BBNX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `LEN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `ALMU` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-21 | `LEN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `ALMU` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `FIVN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `LEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ALMU` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-23 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-23 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-24 | `CBRL` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `CTAS` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `GIS` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `KBH` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `PAYX` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `HALO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FIVN` | 176 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $6088.09; owner short_news_r_h3 |
| `AEHL` | 369 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3050.61; owner short_news_r_h3 |
| `USFD` | 32 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3064.99; owner short_news_r_h3 |
| `CBRL` | 57 | 2026-09-23 @ $47.57 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-11.2; combo leftover $2756.64; owner union_earn_react_h3 |
| `CTAS` | 14 | 2026-09-23 @ $196.78 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $2756.64; owner union_earn_react_h3 |
| `GIS` | 77 | 2026-09-23 @ $35.74 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.1; combo leftover $2756.64; owner union_earn_react_h3 |
| `KBH` | 58 | 2026-09-23 @ $47.15 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-1.9; combo leftover $2756.64; owner union_earn_react_h3 |
| `PAYX` | 25 | 2026-09-23 @ $109.67 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $2756.64; owner union_earn_react_h3 |
| `HALO` | 50 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $5891.10; owner short_news_r_h3 |
