# Factor mine action — `combo_se_5050_weather`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared short_news_r_h3/union_e_fresh_h3 w=0.5,0.5 net=weather

Cash book **-7.91%** ($9,209) · signal-only (no cash/fees) was —. Starts YES **16/30**. Fills 243 · skips 323 · realized $+3397.90.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: short_news_r_h3 50%, union_e_fresh_h3 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name on opposite sides, the short kid wins when morning S is below 0; otherwise the long kid wins. Same-side ties still go to the earlier claim. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: short_news_r_h3 50%, union_e_fresh_h3 50%.
- Member: short_news_r_h3 (50% · short · hold 3).
- Member: union_e_fresh_h3 (50% · long · hold 3).
- Each lot remembers the owner kid, so that kid’s min-hold and list-drop rule apply. A hold-3 fresh-E lot is not sold because the heat kid only holds 1 day.

### When it buys

- At 09:30, each member runs its own pick_day on its own list and gates. Nobody mashes the names into one ranked list first.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- Opposite-side fight: short wins if morning S < 0, else long. Same-side ties go to the earlier claim (fresh-E, then heat, then other longs, then shorts).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $5,288.25.

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
| 2026-08-14 09:30 ET | **BUY** | `EU` | 1 | $1.18 | $0.01 | — | $19.87 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $1.32; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 1 | $12.70 | $0.15 | — | $32.41 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $19.87; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.41 | ▲ close $11,884.57 vs 09:30 $10,963.61 (session +921.12) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.41 | ▼ 09:30 equity $11,734.47 vs yday $11,884.57 (-150.10) | — | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 5 | $1.15 | $0.09 | — | $38.07 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $6.48; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 1 | $3.56 | $0.06 | — | $41.57 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $6.48; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 2 | $3.01 | $0.09 | — | $47.51 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $6.48; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.51 | ▲ close $12,250.74 vs 09:30 $11,734.47 (session +516.49) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.51 | ▼ 09:30 equity $12,147.12 vs yday $12,250.74 (-103.62) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 6172 | $1.14 | $80.70 | $+1887.55 | $7,002.89 | ▲ +1,887.55 after sell → book $12,066.42; vs 09:30 mark -80.70 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 223 | $22.82 | $2.95 | $+174.80 | $12,088.79 | ▲ +174.80 after sell → book $12,063.46; vs 09:30 mark -2.96 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,088.79 | ▲ close $12,064.06 vs 09:30 $12,147.12 (session +0.60) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,088.79 | ▼ 09:30 equity $12,063.79 vs yday $12,064.06 (-0.27) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `EU` | 1 | $1.07 | $0.03 | $-0.16 | $12,089.83 | ▼ -0.16 after sell → book $12,063.76; vs 09:30 mark -0.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 1 | $11.75 | $0.12 | $+0.67 | $12,077.96 | ▲ +0.67 after sell → book $12,063.64; vs 09:30 mark -0.12 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,077.96 | ▲ close $12,063.67 vs 09:30 $12,063.79 (session +0.03) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,077.96 | ▼ 09:30 equity $12,063.23 vs yday $12,063.67 (-0.44) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 5 | $0.96 | $0.06 | $+0.78 | $12,073.08 | ▲ +0.78 after sell → book $12,063.17; vs 09:30 mark -0.06 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 1 | $4.01 | $0.04 | $-0.56 | $12,069.02 | ▼ -0.56 after sell → book $12,063.12; vs 09:30 mark -0.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 2 | $2.95 | $0.07 | $-0.03 | $12,063.06 | ▼ -0.03 after sell → book $12,063.06; vs 09:30 mark -0.06 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 170 | $4.43 | $2.50 | — | $11,307.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $753.94; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 16 | $46.85 | $2.04 | — | $10,555.82 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $753.94; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 7 | $97.43 | $2.01 | — | $9,871.80 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $753.94; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 2513 | $0.30 | $15.08 | — | $9,102.82 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $753.94; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 83 | $9.01 | $2.24 | — | $8,352.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $753.94; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 193 | $3.89 | $2.57 | — | $7,599.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $753.94; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 22 | $34.05 | $2.06 | — | $6,848.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $753.94; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 33 | $22.44 | $2.09 | — | $6,105.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $753.94; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 4 | $204.45 | $2.04 | — | $6,921.40 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $1002.71; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 46 | $21.40 | $2.17 | — | $7,903.63 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $1002.71; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 84 | $11.81 | $2.29 | — | $8,893.79 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1002.71; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 5 | $173.90 | $2.05 | — | $9,761.25 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $1002.71; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 9 | $106.38 | $2.06 | — | $10,716.60 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $1002.71; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 217 | $4.61 | $2.87 | — | $11,714.10 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $1002.71; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,714.10 | ▲ close $12,114.57 vs 09:30 $12,063.23 (session +95.60) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,714.10 | ▼ 09:30 equity $12,098.63 vs yday $12,114.57 (-15.94) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 8 | $115.18 | $2.01 | — | $10,790.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $976.17; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $10,165.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list probable,yday_gainer; 🔵; ret5=+1.4; combo leftover $976.17; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 54 | $17.93 | $2.15 | — | $9,194.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $976.17; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 10 | $93.98 | $2.02 | — | $8,252.93 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; combo leftover $976.17; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 22 | $43.08 | $2.06 | — | $7,303.11 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $976.17; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 424 | $2.30 | $5.47 | — | $6,322.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $976.17; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 323 | $3.11 | $4.26 | — | $7,322.72 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $1006.91; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 7 | $133.11 | $2.06 | — | $8,252.43 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; combo leftover $1006.91; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 11 | $89.10 | $2.07 | — | $9,230.46 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $1006.91; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 26 | $38.40 | $2.12 | — | $10,226.75 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $1006.91; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 48 | $20.90 | $2.18 | — | $11,227.76 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1006.91; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 37 | $27.00 | $2.15 | — | $12,224.62 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $1006.91; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,224.62 | ▲ close $12,344.76 vs 09:30 $12,098.63 (session +276.66) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,224.62 | ▲ 09:30 equity $12,435.74 vs yday $12,344.76 (+90.98) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,224.62 | ▲ close $12,505.01 vs 09:30 $12,435.74 (session +69.27) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,224.62 | ▲ 09:30 equity $12,578.79 vs yday $12,505.01 (+73.78) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `TOYO` | 170 | $4.42 | $2.54 | $-6.74 | $12,973.48 | ▼ -6.74 after sell → book $12,576.25; vs 09:30 mark -2.54 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 16 | $43.63 | $2.06 | $-55.62 | $13,669.50 | ▼ -55.62 after sell → book $12,574.19; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EL` | 7 | $104.00 | $2.03 | $+41.95 | $14,395.47 | ▲ +41.95 after sell → book $12,572.16; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DVLT` | 2513 | $0.31 | $15.76 | $-5.70 | $15,158.74 | ▼ -5.70 after sell → book $12,556.40; vs 09:30 mark -15.76 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 83 | $9.23 | $2.26 | $+13.76 | $15,922.57 | ▲ +13.76 after sell → book $12,554.14; vs 09:30 mark -2.26 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 193 | $5.24 | $2.61 | $+255.37 | $16,931.28 | ▲ +255.37 after sell → book $12,551.53; vs 09:30 mark -2.61 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 22 | $34.72 | $2.08 | $+10.61 | $17,693.04 | ▲ +10.61 after sell → book $12,549.45; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 33 | $21.85 | $2.11 | $-23.67 | $18,411.98 | ▼ -23.67 after sell → book $12,547.34; vs 09:30 mark -2.11 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 4 | $212.00 | $2.00 | $-34.25 | $17,561.98 | ▼ -34.25 after sell → book $12,545.34; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 46 | $20.90 | $2.13 | $+18.70 | $16,598.45 | ▲ +18.70 after sell → book $12,543.21; vs 09:30 mark -2.13 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 84 | $11.00 | $2.24 | $+63.92 | $15,672.21 | ▲ +63.92 after sell → book $12,540.97; vs 09:30 mark -2.24 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 5 | $170.64 | $2.00 | $+12.25 | $14,817.01 | ▲ +12.25 after sell → book $12,538.97; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 9 | $105.58 | $2.02 | $+3.12 | $13,864.77 | ▲ +3.12 after sell → book $12,536.95; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 217 | $4.77 | $2.80 | $-40.39 | $12,826.88 | ▼ -40.39 after sell → book $12,534.15; vs 09:30 mark -2.80 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 4 | $175.01 | $2.00 | — | $12,124.84 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; combo leftover $801.68; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 9 | $88.94 | $2.02 | — | $11,322.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $801.68; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 52 | $15.28 | $2.15 | — | $10,525.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $801.68; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 5 | $142.36 | $2.00 | — | $9,811.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $801.68; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 157 | $5.10 | $2.46 | — | $9,008.69 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $801.68; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 16 | $47.89 | $2.04 | — | $8,240.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $801.68; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 57 | $13.92 | $2.16 | — | $7,444.81 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $801.68; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 176 | $4.54 | $2.52 | — | $6,642.37 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $801.68; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 153 | $13.62 | $2.55 | — | $8,724.45 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $2086.13; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 38 | $54.51 | $2.19 | — | $10,793.64 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $2086.13; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 5 | $364.35 | $2.08 | — | $12,613.31 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $2086.13; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,613.31 | ▼ close $12,167.95 vs 09:30 $12,578.79 (session -342.03) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,613.31 | ▲ 09:30 equity $12,444.95 vs yday $12,167.95 (+277.00) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `FUTU` | 8 | $124.67 | $2.03 | $+71.87 | $13,608.64 | ▲ +71.87 after sell → book $12,442.92; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `DE` | 1 | $632.15 | $2.01 | $+4.88 | $14,238.77 | ▲ +4.88 after sell → book $12,440.90; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BEKE` | 54 | $18.14 | $2.17 | $+6.75 | $15,216.16 | ▲ +6.75 after sell → book $12,438.73; vs 09:30 mark -2.17 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BJ` | 10 | $94.60 | $2.04 | $+2.14 | $16,160.12 | ▲ +2.14 after sell → book $12,436.69; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BKE` | 22 | $44.39 | $2.08 | $+24.69 | $17,134.63 | ▲ +24.69 after sell → book $12,434.62; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 424 | $2.35 | $5.55 | $+10.18 | $18,125.48 | ▲ +10.18 after sell → book $12,429.07; vs 09:30 mark -5.55 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 323 | $2.83 | $4.17 | $+82.02 | $17,207.22 | ▲ +82.02 after sell → book $12,424.90; vs 09:30 mark -4.17 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 7 | $154.20 | $2.01 | $-151.70 | $16,125.81 | ▼ -151.70 after sell → book $12,422.89; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 11 | $88.24 | $2.02 | $+5.37 | $15,153.15 | ▲ +5.37 after sell → book $12,420.87; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 26 | $38.41 | $2.07 | $-4.44 | $14,152.42 | ▼ -4.44 after sell → book $12,418.80; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 48 | $20.50 | $2.13 | $+14.88 | $13,166.28 | ▲ +14.88 after sell → book $12,416.66; vs 09:30 mark -2.14 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 37 | $26.00 | $2.10 | $+32.75 | $12,202.18 | ▲ +32.75 after sell → book $12,414.56; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 1744 | $0.58 | $15.40 | — | $11,170.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $1016.85; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 195 | $5.21 | $2.58 | — | $10,151.51 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $1016.85; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 7 | $131.37 | $2.01 | — | $9,229.91 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; combo leftover $1016.85; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 55 | $18.26 | $2.15 | — | $8,223.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $1016.85; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 29 | $34.30 | $2.08 | — | $7,226.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; combo leftover $1016.85; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 3 | $326.91 | $2.00 | — | $6,243.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-15.2; combo leftover $1016.85; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 5 | $213.94 | $2.05 | — | $7,311.59 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $1238.83; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 101 | $12.22 | $2.35 | — | $8,543.46 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $1238.83; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 243 | $5.08 | $3.22 | — | $9,774.68 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $1238.83; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 9 | $132.64 | $2.07 | — | $10,966.37 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $1238.83; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 6 | $199.94 | $2.06 | — | $12,163.95 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; combo leftover $1238.83; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,163.95 | ▲ close $12,409.15 vs 09:30 $12,444.95 (session +32.57) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,163.95 | ▼ 09:30 equity $12,177.27 vs yday $12,409.15 (-231.88) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 9 | $80.60 | $2.02 | — | $11,436.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.0; combo leftover $760.25; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 46 | $16.18 | $2.13 | — | $10,690.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.7; combo leftover $760.25; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 6 | $118.77 | $2.01 | — | $9,975.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.3; combo leftover $760.25; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 42 | $17.78 | $2.12 | — | $9,226.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.2; combo leftover $760.25; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 56 | $13.41 | $2.16 | — | $8,473.50 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; combo leftover $760.25; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 7 | $97.16 | $2.01 | — | $7,791.37 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; combo leftover $760.25; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 3 | $206.82 | $2.00 | — | $7,168.91 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.2; combo leftover $760.25; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 6 | $120.17 | $2.01 | — | $6,445.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $760.25; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 40 | $74.54 | $2.23 | — | $9,425.26 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $3040.21; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 55 | $55.25 | $2.27 | — | $12,461.73 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $3040.21; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,461.73 | ▼ close $12,039.57 vs 09:30 $12,177.27 (session -116.75) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,461.73 | ▼ 09:30 equity $12,038.35 vs yday $12,039.57 (-1.22) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 4 | $172.76 | $2.02 | $-13.02 | $13,150.75 | ▼ -13.02 after sell → book $12,036.33; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 9 | $93.30 | $2.04 | $+35.19 | $13,988.42 | ▲ +35.19 after sell → book $12,034.29; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 52 | $18.15 | $2.17 | $+144.93 | $14,930.05 | ▲ +144.93 after sell → book $12,032.12; vs 09:30 mark -2.17 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 5 | $132.80 | $2.02 | $-51.83 | $15,592.02 | ▼ -51.83 after sell → book $12,030.10; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 157 | $4.58 | $2.50 | $-86.60 | $16,308.59 | ▼ -86.60 after sell → book $12,027.60; vs 09:30 mark -2.50 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 16 | $48.42 | $2.06 | $+4.38 | $17,081.25 | ▲ +4.38 after sell → book $12,025.54; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 57 | $15.66 | $2.18 | $+94.84 | $17,971.69 | ▲ +94.84 after sell → book $12,023.36; vs 09:30 mark -2.18 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 176 | $3.38 | $2.56 | $-210.12 | $18,564.01 | ▼ -210.12 after sell → book $12,020.80; vs 09:30 mark -2.56 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 153 | $13.90 | $2.45 | $-47.07 | $16,434.86 | ▼ -47.07 after sell → book $12,018.36; vs 09:30 mark -2.44 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 38 | $52.49 | $2.10 | $+72.47 | $14,438.14 | ▲ +72.47 after sell → book $12,016.25; vs 09:30 mark -2.11 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 5 | $347.82 | $2.00 | $+78.57 | $12,697.03 | ▲ +78.57 after sell → book $12,014.25; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 3 | $261.16 | $2.00 | — | $11,911.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; combo leftover $793.56; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 52 | $15.01 | $2.15 | — | $11,128.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $793.56; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 7 | $103.89 | $2.01 | — | $10,399.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $793.56; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 204 | $3.88 | $2.63 | — | $9,605.50 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $793.56; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 17 | $44.40 | $2.04 | — | $8,848.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $793.56; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 32 | $24.69 | $2.09 | — | $8,056.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $793.56; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 95 | $8.35 | $2.27 | — | $7,260.96 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $793.56; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 21 | $37.65 | $2.05 | — | $6,468.37 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $793.56; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 11 | $252.24 | $2.13 | — | $9,240.87 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $2999.25; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 99 | $30.18 | $2.41 | — | $12,226.28 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $2999.25; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,226.28 | ▲ close $12,145.59 vs 09:30 $12,038.35 (session +153.14) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,226.28 | ▲ 09:30 equity $12,247.45 vs yday $12,145.59 (+101.86) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 1744 | $0.51 | $14.43 | $-157.14 | $13,101.30 | ▼ -157.14 after sell → book $12,233.02; vs 09:30 mark -14.43 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 195 | $5.00 | $2.62 | $-46.14 | $14,073.68 | ▼ -46.14 after sell → book $12,230.41; vs 09:30 mark -2.61 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 7 | $148.03 | $2.03 | $+112.58 | $15,107.86 | ▲ +112.58 after sell → book $12,228.38; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 55 | $19.25 | $2.17 | $+50.12 | $16,164.44 | ▲ +50.12 after sell → book $12,226.20; vs 09:30 mark -2.18 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BOX` | 29 | $34.72 | $2.10 | $+8.01 | $17,169.22 | ▲ +8.01 after sell → book $12,224.10; vs 09:30 mark -2.10 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 3 | $298.01 | $2.02 | $-90.72 | $18,061.23 | ▼ -90.72 after sell → book $12,222.08; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 5 | $208.88 | $2.00 | $+21.24 | $17,014.82 | ▲ +21.24 after sell → book $12,220.08; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 101 | $11.10 | $2.29 | $+108.47 | $15,891.43 | ▲ +108.47 after sell → book $12,217.79; vs 09:30 mark -2.29 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 243 | $4.97 | $3.13 | $+19.16 | $14,679.37 | ▲ +19.16 after sell → book $12,214.65; vs 09:30 mark -3.14 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 9 | $127.45 | $2.02 | $+42.62 | $13,530.30 | ▲ +42.62 after sell → book $12,212.63; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 6 | $254.39 | $2.01 | $-330.77 | $12,001.96 | ▼ -330.77 after sell → book $12,210.63; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,001.96 | ▲ close $12,217.66 vs 09:30 $12,247.45 (session +7.03) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,001.96 | ▲ 09:30 equity $12,300.28 vs yday $12,217.66 (+82.62) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BBY` | 9 | $79.83 | $2.04 | $-10.98 | $12,718.39 | ▼ -10.98 after sell → book $12,298.24; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BILI` | 46 | $15.97 | $2.15 | $-13.94 | $13,450.86 | ▼ -13.94 after sell → book $12,296.09; vs 09:30 mark -2.15 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CM` | 6 | $113.66 | $2.03 | $-34.70 | $14,130.79 | ▼ -34.70 after sell → book $12,294.06; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CMBT` | 42 | $18.28 | $2.14 | $+16.75 | $14,896.42 | ▲ +16.75 after sell → book $12,291.93; vs 09:30 mark -2.13 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CSIQ` | 56 | $12.18 | $2.18 | $-73.22 | $15,576.32 | ▼ -73.22 after sell → book $12,289.75; vs 09:30 mark -2.18 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `HQY` | 7 | $96.65 | $2.03 | $-7.61 | $16,250.84 | ▼ -7.61 after sell → book $12,287.72; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RY` | 3 | $203.78 | $2.02 | $-13.14 | $16,860.16 | ▼ -13.14 after sell → book $12,285.70; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TD` | 6 | $120.54 | $2.03 | $-1.82 | $17,581.37 | ▼ -1.82 after sell → book $12,283.67; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 40 | $73.22 | $2.11 | $+48.46 | $14,650.46 | ▲ +48.46 after sell → book $12,281.56; vs 09:30 mark -2.11 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 55 | $54.76 | $2.15 | $+22.52 | $11,636.51 | ▲ +22.52 after sell → book $12,279.41; vs 09:30 mark -2.15 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,636.51 | ▼ close $12,232.37 vs 09:30 $12,300.28 (session -47.04) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,636.51 | ▲ 09:30 equity $12,255.10 vs yday $12,232.37 (+22.73) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 3 | $246.70 | $2.02 | $-47.40 | $12,374.59 | ▼ -47.40 after sell → book $12,253.08; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 52 | $15.01 | $2.17 | $-4.31 | $13,152.94 | ▼ -4.31 after sell → book $12,250.92; vs 09:30 mark -2.16 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 7 | $92.00 | $2.03 | $-87.27 | $13,794.91 | ▼ -87.27 after sell → book $12,248.89; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 204 | $3.32 | $2.68 | $-119.55 | $14,469.52 | ▼ -119.55 after sell → book $12,246.21; vs 09:30 mark -2.68 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 17 | $44.17 | $2.06 | $-8.01 | $15,218.34 | ▼ -8.01 after sell → book $12,244.15; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 32 | $21.97 | $2.11 | $-91.23 | $15,919.28 | ▼ -91.23 after sell → book $12,242.04; vs 09:30 mark -2.11 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 95 | $8.58 | $2.30 | $+17.27 | $16,732.08 | ▲ +17.27 after sell → book $12,239.74; vs 09:30 mark -2.30 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 21 | $35.80 | $2.07 | $-42.98 | $17,481.70 | ▼ -42.98 after sell → book $12,237.67; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 11 | $235.71 | $2.02 | $+177.68 | $14,886.87 | ▲ +177.68 after sell → book $12,235.65; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 99 | $26.78 | $2.29 | $+331.90 | $12,233.36 | ▲ +331.90 after sell → book $12,233.36; vs 09:30 mark -2.29 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,233.36 | ▲ close $12,233.36 vs 09:30 $12,255.10 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,233.36 | ▲ 09:30 equity $12,233.36 vs yday $12,233.36 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 71 | $10.74 | $2.20 | — | $11,468.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $764.58; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 2 | $351.74 | $2.00 | — | $10,762.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $764.58; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 110 | $6.90 | $2.32 | — | $10,001.47 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $764.58; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 2 | $354.49 | $2.00 | — | $9,290.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $764.58; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 34 | $22.32 | $2.09 | — | $8,529.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $764.58; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 2 | $257.00 | $2.00 | — | $8,013.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $764.58; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 16 | $47.60 | $2.04 | — | $7,249.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $764.58; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 50 | $15.09 | $2.14 | — | $6,493.24 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $764.58; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 205 | $14.85 | $2.79 | — | $9,534.71 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $3054.14; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1786 | $1.71 | $23.44 | — | $12,565.32 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $3054.14; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,565.32 | ▲ close $12,649.23 vs 09:30 $12,233.36 (session +458.89) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,565.32 | ▲ 09:30 equity $12,742.34 vs yday $12,649.23 (+93.11) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 12 | $63.18 | $2.03 | — | $11,805.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $785.33; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 89 | $8.74 | $2.26 | — | $11,025.02 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $785.33; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 11 | $68.52 | $2.02 | — | $10,269.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $785.33; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 217 | $3.62 | $2.80 | — | $9,482.02 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $785.33; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 4 | $167.55 | $2.00 | — | $8,809.82 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $785.33; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 17 | $44.90 | $2.04 | — | $8,044.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $785.33; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 8 | $98.15 | $2.01 | — | $7,257.27 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $785.33; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 50 | $15.70 | $2.14 | — | $6,470.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $785.33; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 681 | $4.67 | $9.01 | — | $9,641.39 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $3181.26; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 41 | $76.55 | $2.23 | — | $12,777.70 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $3181.26; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,777.70 | ▼ close $12,584.18 vs 09:30 $12,742.34 (session -129.62) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,777.70 | ▲ 09:30 equity $12,613.11 vs yday $12,584.18 (+28.93) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,777.70 | ▲ close $12,899.49 vs 09:30 $12,613.11 (session +286.38) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,777.70 | ▼ 09:30 equity $12,883.65 vs yday $12,899.49 (-15.84) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 71 | $10.51 | $2.22 | $-21.11 | $13,521.69 | ▼ -21.11 after sell → book $12,881.43; vs 09:30 mark -2.22 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 2 | $366.23 | $2.02 | $+24.97 | $14,252.13 | ▲ +24.97 after sell → book $12,879.41; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 110 | $9.39 | $2.35 | $+269.23 | $15,282.68 | ▲ +269.23 after sell → book $12,877.06; vs 09:30 mark -2.35 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 2 | $341.90 | $2.02 | $-29.19 | $15,964.47 | ▼ -29.19 after sell → book $12,875.05; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CPB` | 34 | $21.67 | $2.11 | $-26.30 | $16,699.13 | ▼ -26.30 after sell → book $12,872.93; vs 09:30 mark -2.12 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 2 | $252.92 | $2.02 | $-12.17 | $17,202.96 | ▼ -12.17 after sell → book $12,870.92; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 16 | $56.94 | $2.06 | $+145.34 | $18,111.94 | ▲ +145.34 after sell → book $12,868.86; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MEI` | 50 | $13.84 | $2.16 | $-66.80 | $18,801.78 | ▼ -66.80 after sell → book $12,866.70; vs 09:30 mark -2.16 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 205 | $13.60 | $2.64 | $+250.82 | $16,011.14 | ▲ +250.82 after sell → book $12,864.06; vs 09:30 mark -2.64 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1786 | $1.58 | $23.04 | $+185.70 | $13,166.22 | ▲ +185.70 after sell → book $12,841.02; vs 09:30 mark -23.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,166.22 | ▼ close $12,827.75 vs 09:30 $12,883.65 (session -13.27) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,166.22 | ▲ 09:30 equity $12,891.83 vs yday $12,827.75 (+64.08) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `AMBA` | 12 | $67.44 | $2.05 | $+47.05 | $13,973.45 | ▲ +47.05 after sell → book $12,889.79; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ASAN` | 89 | $8.26 | $2.28 | $-47.26 | $14,706.31 | ▼ -47.26 after sell → book $12,887.50; vs 09:30 mark -2.29 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOCU` | 11 | $64.60 | $2.04 | $-47.19 | $15,414.87 | ▼ -47.19 after sell → book $12,885.46; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 217 | $3.76 | $2.85 | $+25.82 | $16,227.94 | ▲ +25.82 after sell → book $12,882.62; vs 09:30 mark -2.84 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `GWRE` | 4 | $142.43 | $2.02 | $-104.50 | $16,795.64 | ▼ -104.50 after sell → book $12,880.59; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `IOT` | 17 | $38.23 | $2.06 | $-117.58 | $17,443.40 | ▼ -117.58 after sell → book $12,878.53; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `LULU` | 8 | $98.71 | $2.03 | $+0.43 | $18,231.05 | ▲ +0.43 after sell → book $12,876.50; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MAMA` | 50 | $15.26 | $2.16 | $-26.30 | $18,991.89 | ▼ -26.30 after sell → book $12,874.34; vs 09:30 mark -2.16 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 681 | $4.36 | $8.78 | $+193.31 | $16,013.94 | ▲ +193.31 after sell → book $12,865.55; vs 09:30 mark -8.79 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 41 | $76.79 | $2.11 | $-14.19 | $12,863.44 | ▼ -14.19 after sell → book $12,863.44; vs 09:30 mark -2.11 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,863.44 | ▲ close $12,863.44 vs 09:30 $12,891.83 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,863.44 | ▲ 09:30 equity $12,863.44 vs yday $12,863.44 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 4 | $164.43 | $2.00 | — | $12,203.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $803.97; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 136 | $5.91 | $2.40 | — | $11,397.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; combo leftover $803.97; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 3 | $242.17 | $2.00 | — | $10,669.05 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; combo leftover $803.97; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 25 | $32.01 | $2.06 | — | $9,866.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $803.97; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 11 | $71.71 | $2.02 | — | $9,075.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; combo leftover $803.97; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 14 | $56.02 | $2.03 | — | $8,289.59 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; combo leftover $803.97; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 85 | $9.37 | $2.25 | — | $7,490.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $803.97; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 61 | $13.10 | $2.17 | — | $6,689.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; combo leftover $803.97; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 11 | $112.83 | $2.08 | — | $7,928.73 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $1284.65; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 364 | $3.52 | $4.80 | — | $9,205.21 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $1284.65; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 632 | $2.03 | $8.30 | — | $10,479.87 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $1284.65; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 51 | $24.97 | $2.20 | — | $11,751.14 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $1284.65; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 381 | $3.37 | $5.02 | — | $13,030.09 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $1284.65; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,030.09 | ▲ close $12,854.52 vs 09:30 $12,863.44 (session +30.41) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,030.09 | ▲ 09:30 equity $12,908.26 vs yday $12,854.52 (+53.74) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,030.09 | ▲ close $12,981.41 vs 09:30 $12,908.26 (session +73.15) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,030.09 | ▼ 09:30 equity $12,959.74 vs yday $12,981.41 (-21.67) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,030.09 | ▼ close $12,888.17 vs 09:30 $12,959.74 (session -71.57) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,030.09 | ▼ 09:30 equity $12,858.50 vs yday $12,888.17 (-29.67) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 4 | $140.03 | $2.02 | $-101.62 | $13,588.18 | ▼ -101.62 after sell → book $12,856.47; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DBI` | 136 | $6.25 | $2.43 | $+41.41 | $14,435.75 | ▲ +41.41 after sell → book $12,854.04; vs 09:30 mark -2.43 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 3 | $253.34 | $2.02 | $+29.49 | $15,193.75 | ▲ +29.49 after sell → book $12,852.02; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CPRT` | 25 | $30.57 | $2.08 | $-40.15 | $15,955.92 | ▼ -40.15 after sell → book $12,849.94; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DSGX` | 11 | $78.12 | $2.04 | $+66.44 | $16,813.20 | ▲ +66.44 after sell → book $12,847.90; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `KR` | 14 | $61.93 | $2.05 | $+78.66 | $17,678.16 | ▲ +78.66 after sell → book $12,845.84; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `LPTH` | 85 | $9.40 | $2.27 | $-1.96 | $18,474.89 | ▼ -1.96 after sell → book $12,843.57; vs 09:30 mark -2.27 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `REF` | 61 | $15.75 | $2.19 | $+157.28 | $19,433.45 | ▲ +157.28 after sell → book $12,841.38; vs 09:30 mark -2.19 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 11 | $118.18 | $2.02 | $-62.90 | $18,131.45 | ▼ -62.90 after sell → book $12,839.36; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 364 | $3.98 | $4.70 | $-176.94 | $16,678.03 | ▼ -176.94 after sell → book $12,834.66; vs 09:30 mark -4.70 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 632 | $1.85 | $8.15 | $+97.30 | $15,500.68 | ▲ +97.30 after sell → book $12,826.51; vs 09:30 mark -8.15 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 51 | $24.42 | $2.14 | $+23.71 | $14,253.12 | ▲ +23.71 after sell → book $12,824.37; vs 09:30 mark -2.14 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 381 | $3.75 | $4.91 | $-154.72 | $12,819.45 | ▼ -154.72 after sell → book $12,819.45; vs 09:30 mark -4.92 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 96 | $33.14 | $2.28 | — | $9,635.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; combo leftover $3204.86; owner union_e_fresh_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 78 | $40.93 | $2.22 | — | $6,440.97 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $3204.86; owner union_e_fresh_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 172 | $18.61 | $2.65 | — | $9,639.24 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $3203.74; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 469 | $6.83 | $6.24 | — | $12,836.27 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $3203.74; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,836.27 | ▼ close $12,475.68 vs 09:30 $12,858.50 (session -330.38) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,836.27 | ▲ 09:30 equity $12,644.61 vs yday $12,475.68 (+168.93) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 39 | $81.00 | $2.11 | — | $9,675.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.0; combo leftover $3209.07; owner union_e_fresh_h3 | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 286 | $11.21 | $3.69 | — | $6,465.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.0; combo leftover $3209.07; owner union_e_fresh_h3 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 794 | $7.95 | $10.60 | — | $12,767.12 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $6319.41; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,767.12 | ▲ close $13,047.89 vs 09:30 $12,644.61 (session +419.67) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,767.12 | ▲ 09:30 equity $13,096.83 vs yday $13,047.89 (+48.94) | — | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 190 | $34.44 | $2.82 | — | $19,307.89 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $6548.41; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,307.89 | ▲ close $13,278.87 vs 09:30 $13,096.83 (session +184.87) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,307.89 | ▼ 09:30 equity $13,155.29 vs yday $13,278.87 (-123.58) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `FPS` | 96 | $40.03 | $2.32 | $+656.84 | $23,148.45 | ▲ +656.84 after sell → book $13,152.97; vs 09:30 mark -2.32 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 78 | $41.00 | $2.26 | $+0.97 | $26,344.19 | ▲ +0.97 after sell → book $13,150.71; vs 09:30 mark -2.26 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 172 | $22.11 | $2.51 | $-607.15 | $22,538.76 | ▼ -607.15 after sell → book $13,148.20; vs 09:30 mark -2.51 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 469 | $6.55 | $6.05 | $+119.03 | $19,460.76 | ▲ +119.03 after sell → book $13,142.15; vs 09:30 mark -6.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 397 | $8.26 | $5.30 | — | $22,734.68 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3285.54; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 5 | $583.88 | $2.12 | — | $25,651.96 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $3285.54; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25,651.96 | ▲ close $13,182.30 vs 09:30 $13,155.29 (session +47.57) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25,651.96 | ▲ 09:30 equity $13,207.20 vs yday $13,182.30 (+24.90) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 794 | $8.28 | $10.24 | $-278.89 | $19,071.37 | ▼ -278.89 after sell → book $13,196.96; vs 09:30 mark -10.24 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 35 | $93.97 | $2.22 | — | $22,358.09 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3299.24; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,358.09 | ▼ close $13,110.13 vs 09:30 $13,207.20 (session -84.60) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,358.09 | ▼ 09:30 equity $12,695.07 vs yday $13,110.13 (-415.06) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `LEN` | 39 | $82.00 | $2.14 | $+34.75 | $25,553.95 | ▲ +34.75 after sell → book $12,692.93; vs 09:30 mark -2.14 | union_e_fresh_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `ALMU` | 286 | $13.82 | $3.77 | $+739.00 | $29,502.70 | ▲ +739.00 after sell → book $12,689.16; vs 09:30 mark -3.77 | union_e_fresh_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 62 | $47.57 | $2.18 | — | $26,551.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $2950.27; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 14 | $196.78 | $2.03 | — | $23,794.24 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $2950.27; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 82 | $35.74 | $2.24 | — | $20,861.32 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $2950.27; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 62 | $47.15 | $2.18 | — | $17,935.84 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $2950.27; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 26 | $109.67 | $2.07 | — | $15,082.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $2950.27; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 54 | $116.85 | $2.39 | — | $21,389.87 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $6339.24; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,389.87 | ▲ close $12,742.10 vs 09:30 $12,695.07 (session +66.01) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,389.87 | ▲ 09:30 equity $12,857.41 vs yday $12,742.10 (+115.31) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 5 | $600.27 | $2.00 | $-86.07 | $18,386.51 | ▼ -86.07 after sell → book $12,855.41; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,386.51 | ▼ close $12,791.71 vs 09:30 $12,857.41 (session -63.69) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,423.29 | ▼ 09:30 equity $9,207.75 vs yday $9,216.51 (-8.76) | 09:30 open · cash $4,423.29 (unchanged overnight, no fees) · equity $9,207.75 vs prior close $9,216.51 (-8.76) | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 2 | $887.00 | $2.00 | — | $2,647.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+0.3; combo leftover $2211.64; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 337 | $7.85 | $4.50 | — | $5,288.25 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $2647.29; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,288.25 | ▲ close $9,209.08 vs 09:30 $9,207.75 (session +7.82) | 16:00 close · cash $5,288.25 · equity $9,209.08 vs 09:30 $9,207.75 (+1.33; session marks +7.82) · 15 name(s) marked open→close (per-name table). ABVX×24 09:30 $94.87 → close $94.87 +0.00; AEHL×300 09:30 $9.05 → close $9.36 -93.00; ANAB×44 09:30 $51.70 → close $51.70 +0.00; BAND×40 09:30 $61.83 → close $61.83 -0.00; CBRL×38 09:30 $52.39 → close $51.81 -22.04; CTAS×9 09:30 $197.68 → close $197.68 -0.00; GIS×51 09:30 $34.83 → close $34.83 +0.00; HALO×19 09:30 $115.36 → close $113.90 +27.74; KBH×39 09:30 $47.65 → close $47.65 +0.00; MLKN×119 09:30 $19.91 → close $19.91 -0.00; PAYX×21 09:30 $101.59 → close $101.59 +0.00; THO×34 09:30 $70.93 → close $70.93 +0.00; USFD×26 09:30 $93.82 → close $93.82 +0.00; COST×2 09:30 $887.00 → close $922.76 +71.53; RSKD×337 09:30 $7.85 → close $7.78 +23.59 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `INO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `VOR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `LUNR` | cash | leftover split 1.32 < 1 share @ 19.17 |
| 2026-08-14 | `BTBT` | cash | leftover split 1.32 < 1 share @ 1.50 |
| 2026-08-14 | `ARX` | cash | leftover split 1.32 < 1 share @ 19.57 |
| 2026-08-14 | `AIRO` | cash | leftover split 1.32 < 1 share @ 11.12 |
| 2026-08-14 | `MH` | cash | leftover split 1.32 < 1 share @ 13.55 |
| 2026-08-14 | `CLBT` | cash | leftover split 1.32 < 1 share @ 10.83 |
| 2026-08-14 | `NMAX` | cash | leftover split 1.32 < 1 share @ 9.89 |
| 2026-08-17 | `INO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `VOR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `EU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `APMD` | cash | leftover split 6.48 < 1 share @ 31.70 |
| 2026-08-17 | `RNW` | cash | leftover split 6.48 < 1 share @ 6.80 |
| 2026-08-18 | `EU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `RNW` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
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
| 2026-08-21 | `TOYO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `EL` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
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
| 2026-08-24 | `TOYO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AAP` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `EL` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `DVLT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
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
| 2026-08-24 | `DE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BEKE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BJ` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BKE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `PSEC` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
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
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_e_fresh_h3 |
| 2026-08-25 | `FUTU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `DE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BEKE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BJ` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BKE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `PSEC` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `QTRX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `MRNA` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
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
| 2026-08-27 | `BE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `NEM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `CRM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `SLQT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `TIGR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ANF` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBWI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BOX` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `DY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
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
| 2026-08-31 | `BBY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `BILI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CMBT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CSIQ` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `HQY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `RY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
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
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h3 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h3 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h3 |
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
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new short short_news_r_h3 |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
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
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-17 | `FPS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-17 | `TCOM` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-17 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `FPS` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `TCOM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `BBNX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `LEN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `ALMU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-21 | `LEN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `ALMU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
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
| 2026-09-24 | `CBRL` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `CTAS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `GIS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `KBH` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `PAYX` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `HALO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
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
| `FIVN` | 190 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $6548.41; owner short_news_r_h3 |
| `AEHL` | 397 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3285.54; owner short_news_r_h3 |
| `USFD` | 35 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3299.24; owner short_news_r_h3 |
| `CBRL` | 62 | 2026-09-23 @ $47.57 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $2950.27; owner union_e_fresh_h3 |
| `CTAS` | 14 | 2026-09-23 @ $196.78 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $2950.27; owner union_e_fresh_h3 |
| `GIS` | 82 | 2026-09-23 @ $35.74 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $2950.27; owner union_e_fresh_h3 |
| `KBH` | 62 | 2026-09-23 @ $47.15 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $2950.27; owner union_e_fresh_h3 |
| `PAYX` | 26 | 2026-09-23 @ $109.67 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $2950.27; owner union_e_fresh_h3 |
| `HALO` | 54 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $6339.24; owner short_news_r_h3 |
