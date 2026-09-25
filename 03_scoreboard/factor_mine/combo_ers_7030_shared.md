# Factor mine action — `combo_ers_7030_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_earn_react_h3/short_news_r_h3 w=0.7,0.3 net=priority

Cash book **-16.02%** ($8,398) · signal-only (no cash/fees) was —. Starts YES **6/30**. Fills 244 · skips 327 · realized $+2508.54.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_earn_react_h3 70%, short_news_r_h3 30%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_earn_react_h3 70%, short_news_r_h3 30%.
- Member: union_earn_react_h3 (70% · long · hold 3).
- Member: short_news_r_h3 (30% · short · hold 3).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $1,050.02.

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
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 2 | $0.77 | $0.02 | — | $19.51 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+20.4; combo leftover $1.84; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `DEFT` | 3 | $0.47 | $0.02 | — | $18.08 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+11.1; combo leftover $1.84; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 5 | $1.18 | $0.09 | — | $23.88 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $6.03; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.88 | ▲ close $11,883.66 vs 09:30 $10,963.61 (session +920.18) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.88 | ▼ 09:30 equity $11,733.33 vs yday $11,883.66 (-150.33) | — | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 4 | $1.15 | $0.08 | — | $28.41 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $4.78; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 1 | $3.56 | $0.06 | — | $31.91 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $4.78; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 1 | $3.01 | $0.05 | — | $34.86 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $4.78; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.86 | ▲ close $12,249.57 vs 09:30 $11,733.33 (session +516.42) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.86 | ▼ 09:30 equity $12,145.58 vs yday $12,249.57 (-103.99) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 6172 | $1.14 | $80.70 | $+1887.55 | $6,990.24 | ▲ +1,887.55 after sell → book $12,064.88; vs 09:30 mark -80.70 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 223 | $22.82 | $2.95 | $+174.80 | $12,076.15 | ▲ +174.80 after sell → book $12,061.92; vs 09:30 mark -2.96 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,076.15 | ▲ close $12,062.79 vs 09:30 $12,145.58 (session +0.86) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,076.15 | ▼ 09:30 equity $12,062.70 vs yday $12,062.79 (-0.09) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `BZAI` | 2 | $0.57 | $0.04 | $-0.45 | $12,077.25 | ▼ -0.45 after sell → book $12,062.67; vs 09:30 mark -0.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DEFT` | 3 | $0.43 | $0.04 | $-0.17 | $12,078.52 | ▼ -0.17 after sell → book $12,062.63; vs 09:30 mark -0.04 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 5 | $1.07 | $0.07 | $+0.39 | $12,073.10 | ▲ +0.39 after sell → book $12,062.56; vs 09:30 mark -0.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,073.10 | ▲ close $12,062.59 vs 09:30 $12,062.70 (session +0.04) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,073.10 | ▼ 09:30 equity $12,062.28 vs yday $12,062.59 (-0.31) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 4 | $0.96 | $0.05 | $+0.62 | $12,069.19 | ▲ +0.62 after sell → book $12,062.23; vs 09:30 mark -0.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 1 | $4.01 | $0.04 | $-0.56 | $12,065.14 | ▼ -0.56 after sell → book $12,062.19; vs 09:30 mark -0.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 1 | $2.95 | $0.03 | $-0.03 | $12,062.15 | ▼ -0.03 after sell → book $12,062.15; vs 09:30 mark -0.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 22 | $46.85 | $2.06 | — | $11,029.40 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+5.0; combo leftover $1055.44; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 117 | $9.01 | $2.34 | — | $9,972.89 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $1055.44; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 271 | $3.89 | $3.50 | — | $8,915.20 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.5; combo leftover $1055.44; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 30 | $34.05 | $2.08 | — | $7,891.62 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+9.3; combo leftover $1055.44; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 47 | $22.44 | $2.13 | — | $6,834.81 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.1; combo leftover $1055.44; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 8 | $123.47 | $2.01 | — | $5,845.04 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+2.9; combo leftover $1055.44; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `BILL` | 21 | $49.00 | $2.05 | — | $4,813.98 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-2.0; combo leftover $1055.44; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 106 | $9.94 | $2.31 | — | $3,758.03 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+12.6; combo leftover $1055.44; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 2 | $204.45 | $2.03 | — | $4,164.91 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $536.86; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 25 | $21.40 | $2.10 | — | $4,697.81 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $536.86; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 121 | $4.43 | $2.40 | — | $5,231.44 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; combo leftover $536.86; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 45 | $11.81 | $2.16 | — | $5,760.96 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $536.86; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $6,280.62 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $536.86; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 5 | $106.38 | $2.04 | — | $6,810.48 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $536.86; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 116 | $4.61 | $2.38 | — | $7,342.86 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $536.86; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,342.86 | ▼ close $11,946.27 vs 09:30 $12,062.28 (session -82.27) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,342.86 | ▼ 09:30 equity $11,904.44 vs yday $11,946.27 (-41.83) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 57 | $17.93 | $2.16 | — | $6,318.40 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $1028.00; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 10 | $93.98 | $2.02 | — | $5,376.58 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.4; combo leftover $1028.00; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 23 | $43.08 | $2.06 | — | $4,383.69 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.9; combo leftover $1028.00; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 446 | $2.30 | $5.75 | — | $3,352.13 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.0; combo leftover $1028.00; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `ROST` | 4 | $243.85 | $2.00 | — | $2,374.73 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-6.5; combo leftover $1028.00; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 127 | $3.11 | $2.41 | — | $2,767.29 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $395.79; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 2 | $133.11 | $2.02 | — | $3,031.48 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; combo leftover $395.79; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 4 | $89.10 | $2.03 | — | $3,385.85 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $395.79; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 10 | $38.40 | $2.05 | — | $3,767.80 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $395.79; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 18 | $20.90 | $2.07 | — | $4,141.93 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $395.79; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 14 | $27.00 | $2.06 | — | $4,517.86 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $395.79; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,517.86 | ▲ close $11,890.16 vs 09:30 $11,904.44 (session +12.38) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,517.86 | ▲ 09:30 equity $12,054.41 vs yday $11,890.16 (+164.25) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,517.86 | ▲ close $12,239.80 vs 09:30 $12,054.41 (session +185.40) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,517.86 | ▼ 09:30 equity $12,234.00 vs yday $12,239.80 (-5.80) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 22 | $43.63 | $2.08 | $-74.97 | $5,475.65 | ▼ -74.97 after sell → book $12,231.92; vs 09:30 mark -2.08 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 117 | $9.23 | $2.37 | $+21.03 | $6,553.19 | ▲ +21.03 after sell → book $12,229.55; vs 09:30 mark -2.37 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 271 | $5.24 | $3.55 | $+358.80 | $7,969.68 | ▲ +358.80 after sell → book $12,226.00; vs 09:30 mark -3.55 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 30 | $34.72 | $2.10 | $+15.92 | $9,009.18 | ▲ +15.92 after sell → book $12,223.90; vs 09:30 mark -2.10 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 47 | $21.85 | $2.15 | $-32.01 | $10,033.98 | ▼ -32.01 after sell → book $12,221.75; vs 09:30 mark -2.15 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BABA` | 8 | $117.94 | $2.03 | $-48.29 | $10,975.46 | ▼ -48.29 after sell → book $12,219.72; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BILL` | 21 | $47.98 | $2.07 | $-25.44 | $11,981.07 | ▼ -25.44 after sell → book $12,217.64; vs 09:30 mark -2.08 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BULL` | 106 | $8.46 | $2.34 | $-161.52 | $12,875.50 | ▼ -161.52 after sell → book $12,215.31; vs 09:30 mark -2.33 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 2 | $212.00 | $2.00 | $-19.12 | $12,449.50 | ▼ -19.12 after sell → book $12,213.31; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 25 | $20.90 | $2.06 | $+8.34 | $11,924.94 | ▲ +8.34 after sell → book $12,211.25; vs 09:30 mark -2.06 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 121 | $4.42 | $2.35 | $-3.54 | $11,387.76 | ▼ -3.54 after sell → book $12,208.89; vs 09:30 mark -2.36 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 45 | $11.00 | $2.12 | $+32.39 | $10,890.64 | ▲ +32.39 after sell → book $12,206.77; vs 09:30 mark -2.12 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 3 | $170.64 | $2.00 | $+5.75 | $10,376.72 | ▲ +5.75 after sell → book $12,204.77; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 5 | $105.58 | $2.00 | $-0.04 | $9,846.81 | ▼ -0.04 after sell → book $12,202.76; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 116 | $4.77 | $2.34 | $-23.28 | $9,291.16 | ▼ -23.28 after sell → book $12,200.43; vs 09:30 mark -2.33 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 4 | $175.01 | $2.00 | — | $8,589.11 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-7.0; combo leftover $812.98; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 9 | $88.94 | $2.02 | — | $7,786.64 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-4.9; combo leftover $812.98; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 53 | $15.28 | $2.15 | — | $6,974.65 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $812.98; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 5 | $142.36 | $2.00 | — | $6,260.84 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-8.6; combo leftover $812.98; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 159 | $5.10 | $2.47 | — | $5,447.48 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-8.9; combo leftover $812.98; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 16 | $47.89 | $2.04 | — | $4,679.20 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ⚪; ret5=+14.0; combo leftover $812.98; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 58 | $13.92 | $2.16 | — | $3,869.67 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+5.9; combo leftover $812.98; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 178 | $4.54 | $2.52 | — | $3,058.14 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-14.6; combo leftover $812.98; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 74 | $13.62 | $2.26 | — | $4,064.13 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $1019.38; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 18 | $54.51 | $2.09 | — | $5,043.22 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $1019.38; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 2 | $364.35 | $2.04 | — | $5,769.88 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $1019.38; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,769.88 | ▼ close $11,898.56 vs 09:30 $12,234.00 (session -278.11) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,769.88 | ▲ 09:30 equity $12,014.70 vs yday $11,898.56 (+116.14) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `BEKE` | 57 | $18.14 | $2.18 | $+7.34 | $6,801.68 | ▲ +7.34 after sell → book $12,012.52; vs 09:30 mark -2.18 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BJ` | 10 | $94.60 | $2.04 | $+2.14 | $7,745.64 | ▲ +2.14 after sell → book $12,010.48; vs 09:30 mark -2.04 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BKE` | 23 | $44.39 | $2.08 | $+25.99 | $8,764.53 | ▲ +25.99 after sell → book $12,008.40; vs 09:30 mark -2.08 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 446 | $2.35 | $5.84 | $+10.71 | $9,806.79 | ▲ +10.71 after sell → book $12,002.56; vs 09:30 mark -5.84 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ROST` | 4 | $242.50 | $2.02 | $-9.42 | $10,774.77 | ▼ -9.42 after sell → book $12,000.54; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 127 | $2.83 | $2.37 | $+30.78 | $10,412.99 | ▲ +30.78 after sell → book $11,998.17; vs 09:30 mark -2.37 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 2 | $154.20 | $2.00 | $-46.20 | $10,102.60 | ▼ -46.20 after sell → book $11,996.18; vs 09:30 mark -1.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 4 | $88.24 | $2.00 | $-0.59 | $9,747.63 | ▼ -0.59 after sell → book $11,994.17; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 10 | $38.41 | $2.02 | $-4.17 | $9,361.51 | ▼ -4.17 after sell → book $11,992.15; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 18 | $20.50 | $2.04 | $+3.08 | $8,990.47 | ▲ +3.08 after sell → book $11,990.11; vs 09:30 mark -2.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 14 | $26.00 | $2.03 | $+9.91 | $8,624.44 | ▲ +9.91 after sell → book $11,988.08; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 165 | $5.21 | $2.48 | — | $7,762.30 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $862.44; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 6 | $131.37 | $2.01 | — | $6,972.07 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.3; combo leftover $862.44; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 47 | $18.26 | $2.13 | — | $6,111.72 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-11.4; combo leftover $862.44; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 25 | $34.30 | $2.06 | — | $5,252.16 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+1.7; combo leftover $862.44; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 2 | $326.91 | $2.00 | — | $4,596.34 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-15.2; combo leftover $862.44; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `FSCO` | 169 | $5.08 | $2.50 | — | $3,735.33 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-1.6; combo leftover $862.44; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `HEI` | 2 | $370.00 | $2.00 | — | $2,993.33 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.6; combo leftover $862.44; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 2 | $213.94 | $2.03 | — | $3,419.18 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $598.67; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 48 | $12.22 | $2.17 | — | $4,003.57 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $598.67; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 117 | $5.08 | $2.39 | — | $4,595.54 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $598.67; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 4 | $132.64 | $2.04 | — | $5,124.07 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $598.67; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 2 | $199.94 | $2.03 | — | $5,521.92 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; combo leftover $598.67; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,521.92 | ▲ close $12,144.62 vs 09:30 $12,014.70 (session +182.37) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,521.92 | ▼ 09:30 equity $12,077.37 vs yday $12,144.62 (-67.25) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 5 | $80.60 | $2.00 | — | $5,116.92 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.0; combo leftover $483.17; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 29 | $16.18 | $2.08 | — | $4,645.62 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-6.7; combo leftover $483.17; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 4 | $118.77 | $2.00 | — | $4,168.54 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.3; combo leftover $483.17; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 27 | $17.78 | $2.07 | — | $3,686.41 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-1.2; combo leftover $483.17; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 36 | $13.41 | $2.10 | — | $3,201.55 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.1; combo leftover $483.17; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 4 | $97.16 | $2.00 | — | $2,810.91 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.5; combo leftover $483.17; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 2 | $206.82 | $2.00 | — | $2,395.27 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-0.2; combo leftover $483.17; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 4 | $120.17 | $2.00 | — | $1,912.59 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.9; combo leftover $483.17; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 12 | $74.54 | $2.07 | — | $2,805.00 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $956.29; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 17 | $55.25 | $2.09 | — | $3,742.16 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $956.29; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,742.16 | ▼ close $11,967.05 vs 09:30 $12,077.37 (session -89.91) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,742.16 | ▲ 09:30 equity $11,993.72 vs yday $11,967.05 (+26.67) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 4 | $172.76 | $2.02 | $-13.02 | $4,431.18 | ▼ -13.02 after sell → book $11,991.70; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 9 | $93.30 | $2.04 | $+35.19 | $5,268.84 | ▲ +35.19 after sell → book $11,989.66; vs 09:30 mark -2.04 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 53 | $18.15 | $2.17 | $+147.79 | $6,228.62 | ▲ +147.79 after sell → book $11,987.49; vs 09:30 mark -2.17 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 5 | $132.80 | $2.02 | $-51.83 | $6,890.60 | ▼ -51.83 after sell → book $11,985.47; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 159 | $4.58 | $2.50 | $-87.65 | $7,616.31 | ▼ -87.65 after sell → book $11,982.96; vs 09:30 mark -2.51 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 16 | $48.42 | $2.06 | $+4.38 | $8,388.98 | ▲ +4.38 after sell → book $11,980.91; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 58 | $15.66 | $2.18 | $+96.57 | $9,295.07 | ▲ +96.57 after sell → book $11,978.72; vs 09:30 mark -2.19 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 178 | $3.38 | $2.56 | $-212.46 | $9,894.15 | ▼ -212.46 after sell → book $11,976.16; vs 09:30 mark -2.56 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 74 | $13.90 | $2.21 | $-24.82 | $8,863.34 | ▼ -24.82 after sell → book $11,973.95; vs 09:30 mark -2.21 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 18 | $52.49 | $2.04 | $+32.23 | $7,916.47 | ▲ +32.23 after sell → book $11,971.90; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 2 | $347.82 | $2.00 | $+29.03 | $7,218.84 | ▲ +29.03 after sell → book $11,969.91; vs 09:30 mark -1.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 2 | $261.16 | $2.00 | — | $6,694.52 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+7.8; combo leftover $631.65; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 42 | $15.01 | $2.12 | — | $6,061.99 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+3.7; combo leftover $631.65; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 6 | $103.89 | $2.01 | — | $5,436.64 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.5; combo leftover $631.65; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 162 | $3.88 | $2.48 | — | $4,805.60 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-8.6; combo leftover $631.65; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 14 | $44.40 | $2.03 | — | $4,181.97 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.4; combo leftover $631.65; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 25 | $24.69 | $2.06 | — | $3,562.65 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.8; combo leftover $631.65; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 75 | $8.35 | $2.21 | — | $2,934.19 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.1; combo leftover $631.65; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 16 | $37.65 | $2.04 | — | $2,329.83 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.9; combo leftover $631.65; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 4 | $252.24 | $2.05 | — | $3,336.74 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $1164.92; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 38 | $30.18 | $2.16 | — | $4,481.43 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $1164.92; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,481.43 | ▼ close $11,924.19 vs 09:30 $11,993.72 (session -24.56) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,481.43 | ▲ 09:30 equity $11,979.00 vs yday $11,924.19 (+54.81) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 165 | $5.00 | $2.52 | $-39.66 | $5,303.90 | ▼ -39.66 after sell → book $11,976.48; vs 09:30 mark -2.52 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 6 | $148.03 | $2.03 | $+95.92 | $6,190.06 | ▲ +95.92 after sell → book $11,974.45; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 47 | $19.25 | $2.15 | $+42.25 | $7,092.65 | ▲ +42.25 after sell → book $11,972.30; vs 09:30 mark -2.15 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BOX` | 25 | $34.72 | $2.08 | $+6.35 | $7,958.57 | ▲ +6.35 after sell → book $11,970.21; vs 09:30 mark -2.09 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 2 | $298.01 | $2.02 | $-61.81 | $8,552.57 | ▼ -61.81 after sell → book $11,968.20; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FSCO` | 169 | $5.20 | $2.54 | $+15.25 | $9,428.84 | ▲ +15.25 after sell → book $11,965.66; vs 09:30 mark -2.54 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `HEI` | 2 | $334.88 | $2.02 | $-74.25 | $10,096.58 | ▼ -74.25 after sell → book $11,963.65; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 2 | $208.88 | $2.00 | $+6.10 | $9,676.83 | ▲ +6.10 after sell → book $11,961.65; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 48 | $11.10 | $2.13 | $+49.46 | $9,141.89 | ▲ +49.46 after sell → book $11,959.52; vs 09:30 mark -2.13 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 117 | $4.97 | $2.34 | $+7.56 | $8,557.48 | ▲ +7.56 after sell → book $11,957.18; vs 09:30 mark -2.34 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 4 | $127.45 | $2.00 | $+16.72 | $8,045.67 | ▲ +16.72 after sell → book $11,955.17; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 2 | $254.39 | $2.00 | $-112.92 | $7,534.90 | ▼ -112.92 after sell → book $11,953.18; vs 09:30 mark -1.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,534.90 | ▼ close $11,945.08 vs 09:30 $11,979.00 (session -8.10) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,534.90 | ▼ 09:30 equity $11,937.89 vs yday $11,945.08 (-7.19) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BBY` | 5 | $79.83 | $2.02 | $-7.88 | $7,932.02 | ▼ -7.88 after sell → book $11,935.86; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BILI` | 29 | $15.97 | $2.10 | $-10.26 | $8,393.06 | ▼ -10.26 after sell → book $11,933.77; vs 09:30 mark -2.09 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CM` | 4 | $113.66 | $2.02 | $-24.46 | $8,845.67 | ▼ -24.46 after sell → book $11,931.74; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CMBT` | 27 | $18.28 | $2.09 | $+9.34 | $9,337.14 | ▲ +9.34 after sell → book $11,929.65; vs 09:30 mark -2.09 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CSIQ` | 36 | $12.18 | $2.12 | $-48.50 | $9,773.51 | ▼ -48.50 after sell → book $11,927.54; vs 09:30 mark -2.11 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `HQY` | 4 | $96.65 | $2.02 | $-6.06 | $10,158.08 | ▼ -6.06 after sell → book $11,925.51; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RY` | 2 | $203.78 | $2.02 | $-10.09 | $10,563.63 | ▼ -10.09 after sell → book $11,923.50; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TD` | 4 | $120.54 | $2.02 | $-2.54 | $11,043.77 | ▼ -2.54 after sell → book $11,921.48; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 12 | $73.22 | $2.03 | $+11.74 | $10,163.10 | ▲ +11.74 after sell → book $11,919.45; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 17 | $54.76 | $2.04 | $+4.20 | $9,230.14 | ▲ +4.20 after sell → book $11,917.41; vs 09:30 mark -2.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,230.14 | ▼ close $11,873.17 vs 09:30 $11,937.89 (session -44.24) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,230.14 | ▼ 09:30 equity $11,867.17 vs yday $11,873.17 (-6.00) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 2 | $246.70 | $2.02 | $-32.93 | $9,721.52 | ▼ -32.93 after sell → book $11,865.15; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 42 | $15.01 | $2.14 | $-4.25 | $10,349.81 | ▼ -4.25 after sell → book $11,863.02; vs 09:30 mark -2.13 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 6 | $92.00 | $2.03 | $-75.38 | $10,899.78 | ▼ -75.38 after sell → book $11,860.99; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 162 | $3.32 | $2.51 | $-95.71 | $11,435.11 | ▼ -95.71 after sell → book $11,858.48; vs 09:30 mark -2.51 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 14 | $44.17 | $2.05 | $-7.30 | $12,051.43 | ▼ -7.30 after sell → book $11,856.42; vs 09:30 mark -2.06 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 25 | $21.97 | $2.08 | $-72.15 | $12,598.60 | ▼ -72.15 after sell → book $11,854.34; vs 09:30 mark -2.08 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 75 | $8.58 | $2.24 | $+12.80 | $13,239.86 | ▲ +12.80 after sell → book $11,852.10; vs 09:30 mark -2.24 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 16 | $35.80 | $2.06 | $-33.70 | $13,810.52 | ▼ -33.70 after sell → book $11,850.04; vs 09:30 mark -2.06 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 4 | $235.71 | $2.00 | $+62.07 | $12,865.68 | ▲ +62.07 after sell → book $11,848.04; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 38 | $26.78 | $2.10 | $+124.94 | $11,845.94 | ▲ +124.94 after sell → book $11,845.94; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,845.94 | ▲ close $11,845.94 vs 09:30 $11,867.17 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,845.94 | ▲ 09:30 equity $11,845.94 vs yday $11,845.94 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 96 | $10.74 | $2.28 | — | $10,812.14 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+8.5; combo leftover $1036.52; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 2 | $351.74 | $2.00 | — | $10,106.66 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+3.3; combo leftover $1036.52; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 150 | $6.90 | $2.44 | — | $9,069.22 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.8; combo leftover $1036.52; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 2 | $354.49 | $2.00 | — | $8,358.25 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.3; combo leftover $1036.52; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 46 | $22.32 | $2.13 | — | $7,329.40 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+2.4; combo leftover $1036.52; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 4 | $257.00 | $2.00 | — | $6,299.40 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-5.5; combo leftover $1036.52; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 21 | $47.60 | $2.05 | — | $5,297.74 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-6.2; combo leftover $1036.52; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 68 | $15.09 | $2.19 | — | $4,269.43 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+6.1; combo leftover $1036.52; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 143 | $14.85 | $2.52 | — | $6,390.46 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $2134.72; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1248 | $1.71 | $16.38 | — | $8,508.16 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $2134.72; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,508.16 | ▲ close $12,304.37 vs 09:30 $11,845.94 (session +494.42) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,508.16 | ▲ 09:30 equity $12,379.94 vs yday $12,304.37 (+75.57) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 11 | $63.18 | $2.02 | — | $7,811.16 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-10.9; combo leftover $744.46; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 85 | $8.74 | $2.25 | — | $7,066.01 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.8; combo leftover $744.46; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 10 | $68.52 | $2.02 | — | $6,378.79 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+3.4; combo leftover $744.46; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 205 | $3.62 | $2.64 | — | $5,635.07 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-3.1; combo leftover $744.46; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 4 | $167.55 | $2.00 | — | $4,962.87 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.9; combo leftover $744.46; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 16 | $44.90 | $2.04 | — | $4,242.43 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-7.5; combo leftover $744.46; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 7 | $98.15 | $2.01 | — | $3,553.37 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.9; combo leftover $744.46; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 47 | $15.70 | $2.13 | — | $2,813.34 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-0.4; combo leftover $744.46; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 301 | $4.67 | $3.98 | — | $4,215.03 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $1406.67; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 18 | $76.55 | $2.10 | — | $5,590.83 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $1406.67; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,590.83 | ▼ close $12,291.82 vs 09:30 $12,379.94 (session -64.93) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,590.83 | ▲ 09:30 equity $12,311.45 vs yday $12,291.82 (+19.63) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,590.83 | ▲ close $12,453.76 vs 09:30 $12,311.45 (session +142.31) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,590.83 | ▼ 09:30 equity $12,426.17 vs yday $12,453.76 (-27.59) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 96 | $10.51 | $2.30 | $-27.14 | $6,597.48 | ▼ -27.14 after sell → book $12,423.86; vs 09:30 mark -2.31 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 2 | $366.23 | $2.02 | $+24.97 | $7,327.93 | ▲ +24.97 after sell → book $12,421.85; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 150 | $9.39 | $2.48 | $+368.58 | $8,733.95 | ▲ +368.58 after sell → book $12,419.37; vs 09:30 mark -2.48 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 2 | $341.90 | $2.02 | $-29.19 | $9,415.73 | ▼ -29.19 after sell → book $12,417.35; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CPB` | 46 | $21.67 | $2.15 | $-34.18 | $10,410.41 | ▼ -34.18 after sell → book $12,415.21; vs 09:30 mark -2.14 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 4 | $252.92 | $2.02 | $-20.34 | $11,420.06 | ▼ -20.34 after sell → book $12,413.18; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 21 | $56.94 | $2.07 | $+192.01 | $12,613.73 | ▲ +192.01 after sell → book $12,411.11; vs 09:30 mark -2.07 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MEI` | 68 | $13.84 | $2.22 | $-89.41 | $13,552.64 | ▼ -89.41 after sell → book $12,408.90; vs 09:30 mark -2.21 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 143 | $13.60 | $2.42 | $+173.81 | $11,605.42 | ▲ +173.81 after sell → book $12,406.48; vs 09:30 mark -2.42 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1248 | $1.58 | $16.10 | $+129.76 | $9,617.48 | ▲ +129.76 after sell → book $12,390.38; vs 09:30 mark -16.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,617.48 | ▼ close $12,359.60 vs 09:30 $12,426.17 (session -30.78) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,617.48 | ▲ 09:30 equity $12,373.15 vs yday $12,359.60 (+13.55) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `AMBA` | 11 | $67.44 | $2.04 | $+42.79 | $10,357.27 | ▲ +42.79 after sell → book $12,371.10; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ASAN` | 85 | $8.26 | $2.27 | $-45.31 | $11,057.11 | ▼ -45.31 after sell → book $12,368.84; vs 09:30 mark -2.26 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOCU` | 10 | $64.60 | $2.04 | $-43.26 | $11,701.07 | ▼ -43.26 after sell → book $12,366.80; vs 09:30 mark -2.04 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 205 | $3.76 | $2.69 | $+24.39 | $12,469.18 | ▲ +24.39 after sell → book $12,364.11; vs 09:30 mark -2.69 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `GWRE` | 4 | $142.43 | $2.02 | $-104.50 | $13,036.87 | ▼ -104.50 after sell → book $12,362.08; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `IOT` | 16 | $38.23 | $2.06 | $-110.90 | $13,646.42 | ▼ -110.90 after sell → book $12,360.03; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `LULU` | 7 | $98.71 | $2.03 | $-0.12 | $14,335.36 | ▼ -0.12 after sell → book $12,358.00; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MAMA` | 47 | $15.26 | $2.15 | $-24.96 | $15,050.42 | ▼ -24.96 after sell → book $12,355.84; vs 09:30 mark -2.16 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 301 | $4.36 | $3.88 | $+85.44 | $13,734.18 | ▲ +85.44 after sell → book $12,351.96; vs 09:30 mark -3.88 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 18 | $76.79 | $2.04 | $-8.47 | $12,349.92 | ▼ -8.47 after sell → book $12,349.92; vs 09:30 mark -2.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,349.92 | ▲ close $12,349.92 vs 09:30 $12,373.15 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,349.92 | ▲ 09:30 equity $12,349.92 vs yday $12,349.92 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 6 | $164.43 | $2.01 | — | $11,361.33 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $1080.62; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 4 | $242.17 | $2.00 | — | $10,390.65 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-11.1; combo leftover $1080.62; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 33 | $32.01 | $2.09 | — | $9,332.23 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-4.4; combo leftover $1080.62; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 15 | $71.71 | $2.04 | — | $8,254.54 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-9.1; combo leftover $1080.62; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 19 | $56.02 | $2.05 | — | $7,188.12 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.2; combo leftover $1080.62; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 115 | $9.37 | $2.33 | — | $6,108.23 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+1.5; combo leftover $1080.62; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 82 | $13.10 | $2.24 | — | $5,031.80 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-6.9; combo leftover $1080.62; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 7 | $135.71 | $2.01 | — | $4,079.81 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-9.2; combo leftover $1080.62; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 7 | $112.83 | $2.05 | — | $4,867.61 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $815.96; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 231 | $3.52 | $3.05 | — | $5,677.68 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $815.96; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 401 | $2.03 | $5.27 | — | $6,486.43 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $815.96; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 32 | $24.97 | $2.13 | — | $7,283.35 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $815.96; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 242 | $3.37 | $3.19 | — | $8,095.69 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $815.96; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,095.69 | ▲ close $12,353.91 vs 09:30 $12,349.92 (session +36.44) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,095.69 | ▲ 09:30 equity $12,377.24 vs yday $12,353.91 (+23.33) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,095.69 | ▲ close $12,490.17 vs 09:30 $12,377.24 (session +112.93) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,095.69 | ▼ 09:30 equity $12,461.97 vs yday $12,490.17 (-28.20) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,095.69 | ▼ close $12,428.92 vs 09:30 $12,461.97 (session -33.05) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,095.69 | ▼ 09:30 equity $12,380.43 vs yday $12,428.92 (-48.49) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 6 | $140.03 | $2.03 | $-150.44 | $8,933.84 | ▼ -150.44 after sell → book $12,378.40; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 4 | $253.34 | $2.02 | $+40.66 | $9,945.18 | ▲ +40.66 after sell → book $12,376.38; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CPRT` | 33 | $30.57 | $2.11 | $-51.72 | $10,951.88 | ▼ -51.72 after sell → book $12,374.27; vs 09:30 mark -2.11 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DSGX` | 15 | $78.12 | $2.06 | $+92.06 | $12,121.63 | ▲ +92.06 after sell → book $12,372.22; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `KR` | 19 | $61.93 | $2.07 | $+108.18 | $13,296.23 | ▲ +108.18 after sell → book $12,370.15; vs 09:30 mark -2.07 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `LPTH` | 115 | $9.40 | $2.36 | $-1.25 | $14,374.87 | ▼ -1.25 after sell → book $12,367.79; vs 09:30 mark -2.36 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `REF` | 82 | $15.75 | $2.26 | $+212.80 | $15,664.11 | ▲ +212.80 after sell → book $12,365.53; vs 09:30 mark -2.26 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RH` | 7 | $125.55 | $2.03 | $-75.16 | $16,540.93 | ▼ -75.16 after sell → book $12,363.50; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 7 | $118.18 | $2.01 | $-41.48 | $15,711.66 | ▼ -41.48 after sell → book $12,361.49; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 231 | $3.98 | $2.98 | $-112.29 | $14,789.30 | ▼ -112.29 after sell → book $12,358.51; vs 09:30 mark -2.98 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 401 | $1.85 | $5.17 | $+61.74 | $14,042.27 | ▲ +61.74 after sell → book $12,353.33; vs 09:30 mark -5.18 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 32 | $24.42 | $2.09 | $+13.39 | $13,258.75 | ▲ +13.39 after sell → book $12,351.25; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 242 | $3.75 | $3.12 | $-98.28 | $12,348.13 | ▼ -98.28 after sell → book $12,348.13; vs 09:30 mark -3.12 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 211 | $40.93 | $2.72 | — | $3,709.17 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.1; combo leftover $8643.69; owner union_earn_react_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 99 | $18.61 | $2.37 | — | $5,549.19 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $1854.59; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 271 | $6.83 | $3.61 | — | $7,396.52 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $1854.59; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,396.52 | ▼ close $11,972.64 vs 09:30 $12,380.43 (session -366.79) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,396.52 | ▲ 09:30 equity $12,023.59 vs yday $11,972.64 (+50.95) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 230 | $11.21 | $2.97 | — | $4,815.25 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+1.0; combo leftover $2588.78; owner union_earn_react_h3 | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 31 | $81.00 | $2.08 | — | $2,302.17 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-3.0; combo leftover $2588.78; owner union_earn_react_h3 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 289 | $7.95 | $3.86 | — | $4,595.86 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $2302.17; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,595.86 | ▲ close $12,083.25 vs 09:30 $12,023.59 (session +68.57) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,595.86 | ▲ 09:30 equity $12,090.73 vs yday $12,083.25 (+7.48) | — | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 133 | $34.44 | $2.57 | — | $9,173.81 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $4595.86; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,173.81 | ▲ close $12,378.64 vs 09:30 $12,090.73 (session +290.48) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,173.81 | ▲ 09:30 equity $12,400.27 vs yday $12,378.64 (+21.63) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 211 | $41.00 | $2.83 | $+9.22 | $17,821.98 | ▲ +9.22 after sell → book $12,397.44; vs 09:30 mark -2.83 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 99 | $22.11 | $2.29 | $-351.16 | $15,630.80 | ▼ -351.16 after sell → book $12,395.15; vs 09:30 mark -2.29 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 271 | $6.55 | $3.50 | $+68.78 | $13,852.26 | ▲ +68.78 after sell → book $12,391.66; vs 09:30 mark -3.49 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 375 | $8.26 | $5.01 | — | $16,944.75 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3097.91; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 5 | $583.88 | $2.12 | — | $19,862.03 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $3097.91; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,862.03 | ▲ close $12,433.63 vs 09:30 $12,400.27 (session +49.10) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,862.03 | ▲ 09:30 equity $12,471.16 vs yday $12,433.63 (+37.53) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 289 | $8.28 | $3.73 | $-101.51 | $17,466.83 | ▼ -101.51 after sell → book $12,467.43; vs 09:30 mark -3.73 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 33 | $93.97 | $2.21 | — | $20,565.63 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3116.86; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,565.63 | ▼ close $12,380.54 vs 09:30 $12,471.16 (session -84.68) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,565.63 | ▼ 09:30 equity $12,045.85 vs yday $12,380.54 (-334.69) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `ALMU` | 230 | $13.82 | $3.03 | $+594.30 | $23,741.20 | ▲ +594.30 after sell → book $12,042.82; vs 09:30 mark -3.03 | union_earn_react_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `LEN` | 31 | $82.00 | $2.11 | $+26.80 | $26,281.08 | ▲ +26.80 after sell → book $12,040.71; vs 09:30 mark -2.11 | union_earn_react_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 77 | $47.57 | $2.22 | — | $22,615.97 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-11.2; combo leftover $3679.35; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 18 | $196.78 | $2.04 | — | $19,071.89 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $3679.35; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 102 | $35.74 | $2.30 | — | $15,424.11 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.1; combo leftover $3679.35; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 78 | $47.15 | $2.22 | — | $11,744.19 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-1.9; combo leftover $3679.35; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 33 | $109.67 | $2.09 | — | $8,122.99 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $3679.35; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 51 | $116.85 | $2.36 | — | $14,079.98 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $6014.92; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,079.98 | ▼ close $11,969.12 vs 09:30 $12,045.85 (session -58.35) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,079.98 | ▲ 09:30 equity $12,075.41 vs yday $11,969.12 (+106.29) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 5 | $600.27 | $2.00 | $-86.07 | $11,076.62 | ▼ -86.07 after sell → book $12,073.41; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,076.62 | ▼ close $12,040.31 vs 09:30 $12,075.41 (session -33.10) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $526.30 | ▼ 09:30 equity $8,483.88 vs yday $8,491.65 (-7.77) | 09:30 open · cash $526.30 (unchanged overnight, no fees) · equity $8,483.88 vs prior close $8,491.65 (-7.77) | — |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 67 | $7.85 | $2.23 | — | $1,050.02 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $526.30; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,050.02 | ▼ close $8,398.22 vs 09:30 $8,483.88 (session -83.43) | 16:00 close · cash $1,050.02 · equity $8,398.22 vs 09:30 $8,483.88 (-85.66; session marks -83.43) · 14 name(s) marked open→close (per-name table). ABVX×28 09:30 $94.87 → close $94.87 +0.00; AEHL×280 09:30 $9.05 → close $9.36 -86.80; ANAB×53 09:30 $51.70 → close $51.70 +0.00; BAND×26 09:30 $61.83 → close $61.83 -0.00; CBRL×35 09:30 $52.39 → close $51.81 -20.30; CTAS×8 09:30 $197.68 → close $197.68 -0.00; GIS×47 09:30 $34.83 → close $34.83 +0.00; HALO×13 09:30 $115.36 → close $113.90 +18.98; KBH×35 09:30 $47.65 → close $47.65 +0.00; MLKN×142 09:30 $19.91 → close $19.91 -0.00; PAYX×19 09:30 $101.59 → close $101.59 +0.00; THO×40 09:30 $70.93 → close $70.93 +0.00; USFD×24 09:30 $93.82 → close $93.82 +0.00; RSKD×67 09:30 $7.85 → close $7.78 +4.69 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `INO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `VOR` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `NMAX` | cash | leftover split 1.84 < 1 share @ 9.89 |
| 2026-08-14 | `AIRJ` | cash | leftover split 1.84 < 1 share @ 5.51 |
| 2026-08-14 | `AMAT` | cash | leftover split 1.84 < 1 share @ 499.40 |
| 2026-08-14 | `AMPG` | cash | leftover split 1.84 < 1 share @ 4.37 |
| 2026-08-14 | `BRUN` | cash | leftover split 1.84 < 1 share @ 26.25 |
| 2026-08-14 | `DGXX` | cash | leftover split 1.84 < 1 share @ 3.92 |
| 2026-08-14 | `LUNR` | cash | leftover split 6.03 < 1 share @ 19.17 |
| 2026-08-14 | `OWL` | cash | leftover split 6.03 < 1 share @ 12.70 |
| 2026-08-17 | `INO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `VOR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `BZAI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `DEFT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `EU` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `APMD` | cash | leftover split 4.78 < 1 share @ 31.70 |
| 2026-08-17 | `RNW` | cash | leftover split 4.78 < 1 share @ 6.80 |
| 2026-08-18 | `BZAI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `DEFT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `EU` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `RNW` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `LOW` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `LZB` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `MRCY` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `LUNR` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
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
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_earn_react_h3 |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
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
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
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
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new long union_earn_react_h3 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new long union_earn_react_h3 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new long union_earn_react_h3 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
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
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new short short_news_r_h3 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `M` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `NAVN` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `NB` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `SHOE` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
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
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-17 | `TCOM` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-17 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `TCOM` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `BBNX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `ALMU` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `LEN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-21 | `ALMU` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `LEN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
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
| 2026-09-24 | `CBRL` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `CTAS` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `GIS` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `KBH` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `PAYX` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `HALO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FIVN` | 133 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $4595.86; owner short_news_r_h3 |
| `AEHL` | 375 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3097.91; owner short_news_r_h3 |
| `USFD` | 33 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3116.86; owner short_news_r_h3 |
| `CBRL` | 77 | 2026-09-23 @ $47.57 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-11.2; combo leftover $3679.35; owner union_earn_react_h3 |
| `CTAS` | 18 | 2026-09-23 @ $196.78 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $3679.35; owner union_earn_react_h3 |
| `GIS` | 102 | 2026-09-23 @ $35.74 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.1; combo leftover $3679.35; owner union_earn_react_h3 |
| `KBH` | 78 | 2026-09-23 @ $47.15 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-1.9; combo leftover $3679.35; owner union_earn_react_h3 |
| `PAYX` | 33 | 2026-09-23 @ $109.67 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $3679.35; owner union_earn_react_h3 |
| `HALO` | 51 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $6014.92; owner short_news_r_h3 |
