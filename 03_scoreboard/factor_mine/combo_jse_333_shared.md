# Factor mine action — `combo_jse_333_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_join_vol_green_h1/short_news_r_h3/union_e_fresh_h3 w=0.33,0.33,0.33 net=priority

Cash book **-9.65%** ($9,035) · signal-only (no cash/fees) was —. Starts YES **7/30**. Fills 427 · skips 387 · realized $+3094.73.

## How this sleeve decides (like you are 10)

Imagine 3 kids at the same 09:30 school bell sharing one $10,000 book: union_join_vol_green_h1 33%, short_news_r_h3 33%, union_e_fresh_h3 33%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_join_vol_green_h1 33%, short_news_r_h3 33%, union_e_fresh_h3 33%.
- Member: union_join_vol_green_h1 (33% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6,125.64.

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
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 1 | $12.70 | $0.15 | — | $33.61 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $21.06; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.61 | ▲ close $11,884.56 vs 09:30 $10,963.61 (session +921.09) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.61 | ▼ 09:30 equity $11,734.46 vs yday $11,884.56 (-150.10) | — | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 5 | $1.15 | $0.09 | — | $39.27 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $6.72; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 1 | $3.56 | $0.06 | — | $42.77 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $6.72; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 2 | $3.01 | $0.09 | — | $48.70 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $6.72; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.70 | ▲ close $12,250.80 vs 09:30 $11,734.46 (session +516.57) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.70 | ▼ 09:30 equity $12,147.18 vs yday $12,250.80 (-103.62) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 6172 | $1.14 | $80.70 | $+1887.55 | $7,004.08 | ▲ +1,887.55 after sell → book $12,066.48; vs 09:30 mark -80.70 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 223 | $22.82 | $2.95 | $+174.80 | $12,089.99 | ▲ +174.80 after sell → book $12,063.53; vs 09:30 mark -2.95 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,089.99 | ▲ close $12,064.18 vs 09:30 $12,147.18 (session +0.66) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,089.99 | ▼ 09:30 equity $12,063.92 vs yday $12,064.18 (-0.26) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 1 | $11.75 | $0.12 | $+0.67 | $12,078.12 | ▲ +0.67 after sell → book $12,063.80; vs 09:30 mark -0.12 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,078.12 | ▲ close $12,063.83 vs 09:30 $12,063.92 (session +0.03) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,078.12 | ▼ 09:30 equity $12,063.39 vs yday $12,063.83 (-0.44) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 5 | $0.96 | $0.06 | $+0.78 | $12,073.24 | ▲ +0.78 after sell → book $12,063.32; vs 09:30 mark -0.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 1 | $4.01 | $0.04 | $-0.56 | $12,069.18 | ▼ -0.56 after sell → book $12,063.28; vs 09:30 mark -0.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 2 | $2.95 | $0.07 | $-0.03 | $12,063.22 | ▼ -0.03 after sell → book $12,063.22; vs 09:30 mark -0.06 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 113 | $4.43 | $2.33 | — | $11,560.30 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $502.63; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 10 | $46.85 | $2.02 | — | $11,089.78 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $502.63; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 5 | $97.43 | $2.00 | — | $10,600.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $502.63; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 1675 | $0.30 | $10.05 | — | $10,088.07 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $502.63; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 55 | $9.01 | $2.15 | — | $9,590.37 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $502.63; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 129 | $3.89 | $2.38 | — | $9,086.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $502.63; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 14 | $34.05 | $2.03 | — | $8,607.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $502.63; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 22 | $22.44 | $2.06 | — | $8,111.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $502.63; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 24 | $20.55 | $2.06 | — | $7,616.45 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; combo leftover $506.98; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 24 | $20.65 | $2.06 | — | $7,118.79 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; combo leftover $506.98; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 87 | $5.77 | $2.25 | — | $6,614.55 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; combo leftover $506.98; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 25 | $19.63 | $2.06 | — | $6,121.73 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; combo leftover $506.98; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 17 | $29.63 | $2.04 | — | $5,615.98 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; combo leftover $506.98; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 289 | $1.75 | $3.73 | — | $5,106.50 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; combo leftover $506.98; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 3 | $144.54 | $2.00 | — | $4,670.88 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; combo leftover $506.98; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 103 | $4.92 | $2.30 | — | $4,161.82 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $506.98; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $4,773.14 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $693.64; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 32 | $21.40 | $2.12 | — | $5,455.81 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $693.64; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 58 | $11.81 | $2.20 | — | $6,138.88 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $693.64; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $6,658.55 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $693.64; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 6 | $106.38 | $2.05 | — | $7,294.78 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $693.64; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 150 | $4.61 | $2.49 | — | $7,983.79 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $693.64; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,983.79 | ▲ close $12,135.24 vs 09:30 $12,063.39 (session +128.49) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,983.79 | ▲ 09:30 equity $12,261.95 vs yday $12,135.24 (+126.71) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 24 | $21.90 | $2.08 | $+28.26 | $8,507.31 | ▲ +28.26 after sell → book $12,259.87; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 24 | $21.75 | $2.08 | $+22.26 | $9,027.22 | ▲ +22.26 after sell → book $12,257.78; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 87 | $5.67 | $2.28 | $-13.23 | $9,518.24 | ▼ -13.23 after sell → book $12,255.51; vs 09:30 mark -2.27 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 25 | $21.17 | $2.08 | $+34.35 | $10,045.40 | ▲ +34.35 after sell → book $12,253.42; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 17 | $32.17 | $2.06 | $+39.08 | $10,590.23 | ▲ +39.08 after sell → book $12,251.36; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 289 | $1.79 | $3.79 | $+4.05 | $11,103.76 | ▲ +4.05 after sell → book $12,247.58; vs 09:30 mark -3.78 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 3 | $154.70 | $2.02 | $+26.46 | $11,565.84 | ▲ +26.46 after sell → book $12,245.56; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 103 | $5.20 | $2.33 | $+24.21 | $12,099.11 | ▲ +24.21 after sell → book $12,243.23; vs 09:30 mark -2.33 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 5 | $115.18 | $2.00 | — | $11,521.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $672.17; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $10,895.95 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list probable,yday_gainer; 🔵; ret5=+1.4; combo leftover $672.17; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 37 | $17.93 | $2.10 | — | $10,230.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $672.17; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 7 | $93.98 | $2.01 | — | $9,570.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; combo leftover $672.17; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 15 | $43.08 | $2.04 | — | $8,922.15 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $672.17; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 292 | $2.30 | $3.77 | — | $8,246.78 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $672.17; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 4 | $119.43 | $2.00 | — | $7,767.06 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $589.06; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 34 | $17.20 | $2.09 | — | $7,180.17 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; combo leftover $589.06; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 52 | $11.13 | $2.15 | — | $6,599.26 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; combo leftover $589.06; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 446 | $1.32 | $5.75 | — | $6,004.79 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; combo leftover $589.06; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 354 | $1.66 | $4.57 | — | $5,412.58 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; combo leftover $589.06; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 423 | $1.39 | $5.46 | — | $4,819.16 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; combo leftover $589.06; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 71 | $8.28 | $2.20 | — | $4,229.08 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; combo leftover $589.06; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 226 | $3.11 | $2.98 | — | $4,928.95 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $704.85; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 5 | $133.11 | $2.04 | — | $5,592.46 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; combo leftover $704.85; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 7 | $89.10 | $2.05 | — | $6,214.11 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $704.85; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 18 | $38.40 | $2.08 | — | $6,903.23 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $704.85; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 33 | $20.90 | $2.13 | — | $7,590.80 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $704.85; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 26 | $27.00 | $2.11 | — | $8,290.69 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $704.85; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,290.69 | ▲ close $12,471.71 vs 09:30 $12,261.95 (session +280.01) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,290.69 | ▲ 09:30 equity $12,690.64 vs yday $12,471.71 (+218.93) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 4 | $120.51 | $2.02 | $+0.30 | $8,770.71 | ▲ +0.30 after sell → book $12,688.62; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 34 | $16.57 | $2.11 | $-25.62 | $9,331.98 | ▼ -25.62 after sell → book $12,686.50; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 52 | $13.33 | $2.17 | $+110.09 | $10,022.97 | ▲ +110.09 after sell → book $12,684.34; vs 09:30 mark -2.16 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 446 | $1.83 | $5.84 | $+215.87 | $10,833.32 | ▲ +215.87 after sell → book $12,678.50; vs 09:30 mark -5.84 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 354 | $1.55 | $4.64 | $-48.14 | $11,377.38 | ▼ -48.14 after sell → book $12,673.87; vs 09:30 mark -4.63 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 423 | $1.24 | $5.54 | $-74.44 | $11,896.36 | ▼ -74.44 after sell → book $12,668.33; vs 09:30 mark -5.54 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 71 | $8.59 | $2.22 | $+17.58 | $12,504.03 | ▲ +17.58 after sell → book $12,666.10; vs 09:30 mark -2.23 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,504.03 | ▲ close $12,715.72 vs 09:30 $12,690.64 (session +49.62) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,504.03 | ▲ 09:30 equity $12,765.79 vs yday $12,715.72 (+50.07) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `TOYO` | 113 | $4.42 | $2.36 | $-5.82 | $13,001.13 | ▼ -5.82 after sell → book $12,763.43; vs 09:30 mark -2.36 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 10 | $43.63 | $2.04 | $-36.26 | $13,435.39 | ▼ -36.26 after sell → book $12,761.39; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EL` | 5 | $104.00 | $2.02 | $+28.82 | $13,953.37 | ▲ +28.82 after sell → book $12,759.37; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DVLT` | 1675 | $0.31 | $10.51 | $-3.81 | $14,462.11 | ▼ -3.81 after sell → book $12,748.86; vs 09:30 mark -10.51 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 55 | $9.23 | $2.17 | $+7.77 | $14,967.59 | ▲ +7.77 after sell → book $12,746.69; vs 09:30 mark -2.17 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 129 | $5.24 | $2.41 | $+169.36 | $15,641.14 | ▲ +169.36 after sell → book $12,744.28; vs 09:30 mark -2.41 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 14 | $34.72 | $2.05 | $+5.30 | $16,125.17 | ▲ +5.30 after sell → book $12,742.23; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 22 | $21.85 | $2.08 | $-17.11 | $16,603.79 | ▼ -17.11 after sell → book $12,740.15; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $212.00 | $2.00 | $-26.68 | $15,965.79 | ▼ -26.68 after sell → book $12,738.15; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 32 | $20.90 | $2.09 | $+11.79 | $15,294.90 | ▲ +11.79 after sell → book $12,736.06; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 58 | $11.00 | $2.16 | $+42.90 | $14,654.74 | ▲ +42.90 after sell → book $12,733.90; vs 09:30 mark -2.16 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 3 | $170.64 | $2.00 | $+5.75 | $14,140.82 | ▲ +5.75 after sell → book $12,731.90; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 6 | $105.58 | $2.01 | $+0.75 | $13,505.33 | ▲ +0.75 after sell → book $12,729.89; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 150 | $4.77 | $2.44 | $-28.93 | $12,787.39 | ▼ -28.93 after sell → book $12,727.45; vs 09:30 mark -2.44 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 3 | $175.01 | $2.00 | — | $12,260.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; combo leftover $532.81; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 5 | $88.94 | $2.00 | — | $11,813.66 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $532.81; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 34 | $15.28 | $2.09 | — | $11,292.05 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $532.81; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 3 | $142.36 | $2.00 | — | $10,862.97 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $532.81; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 104 | $5.10 | $2.30 | — | $10,330.27 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $532.81; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 11 | $47.89 | $2.02 | — | $9,801.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $532.81; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 38 | $13.92 | $2.10 | — | $9,270.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $532.81; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 117 | $4.54 | $2.34 | — | $8,736.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $532.81; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 334 | $1.63 | $4.31 | — | $8,187.55 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; combo leftover $546.02; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 153 | $3.55 | $2.45 | — | $7,641.96 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+27.9; combo leftover $546.02; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 85 | $6.37 | $2.25 | — | $7,098.26 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; combo leftover $546.02; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 15 | $35.05 | $2.04 | — | $6,570.48 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $546.02; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 8 | $64.55 | $2.01 | — | $6,052.06 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+4.4; combo leftover $546.02; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 3 | $156.51 | $2.00 | — | $5,580.53 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+17.4; combo leftover $546.02; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 60 | $8.98 | $2.17 | — | $5,039.56 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; combo leftover $546.02; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `IAUX` | 287 | $1.90 | $3.70 | — | $4,490.56 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.4; combo leftover $546.02; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 109 | $13.62 | $2.39 | — | $5,973.30 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $1496.85; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 27 | $54.51 | $2.13 | — | $7,442.93 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $1496.85; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 4 | $364.35 | $2.06 | — | $8,898.27 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $1496.85; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,898.27 | ▼ close $12,547.32 vs 09:30 $12,765.79 (session -135.75) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,898.27 | ▲ 09:30 equity $12,724.17 vs yday $12,547.32 (+176.85) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `FUTU` | 5 | $124.67 | $2.02 | $+43.42 | $9,519.60 | ▲ +43.42 after sell → book $12,722.15; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `DE` | 1 | $632.15 | $2.01 | $+4.88 | $10,149.73 | ▲ +4.88 after sell → book $12,720.13; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BEKE` | 37 | $18.14 | $2.12 | $+3.36 | $10,818.79 | ▲ +3.36 after sell → book $12,718.01; vs 09:30 mark -2.12 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BJ` | 7 | $94.60 | $2.03 | $+0.30 | $11,478.96 | ▲ +0.30 after sell → book $12,715.98; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BKE` | 15 | $44.39 | $2.06 | $+15.56 | $12,142.76 | ▲ +15.56 after sell → book $12,713.93; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 292 | $2.35 | $3.83 | $+7.01 | $12,825.13 | ▲ +7.01 after sell → book $12,710.10; vs 09:30 mark -3.83 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 226 | $2.83 | $2.92 | $+57.38 | $12,182.64 | ▲ +57.38 after sell → book $12,707.19; vs 09:30 mark -2.91 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 5 | $154.20 | $2.00 | $-109.50 | $11,409.63 | ▼ -109.50 after sell → book $12,705.18; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 7 | $88.24 | $2.01 | $+1.96 | $10,789.94 | ▲ +1.96 after sell → book $12,703.17; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 18 | $38.41 | $2.04 | $-4.31 | $10,096.52 | ▼ -4.31 after sell → book $12,701.13; vs 09:30 mark -2.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 33 | $20.50 | $2.09 | $+8.98 | $9,417.93 | ▲ +8.98 after sell → book $12,699.04; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 26 | $26.00 | $2.07 | $+21.82 | $8,739.86 | ▲ +21.82 after sell → book $12,696.97; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 334 | $1.75 | $4.37 | $+33.07 | $9,321.65 | ▲ +33.07 after sell → book $12,692.59; vs 09:30 mark -4.38 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 153 | $3.77 | $2.48 | $+28.73 | $9,895.98 | ▲ +28.73 after sell → book $12,690.11; vs 09:30 mark -2.48 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 85 | $6.13 | $2.27 | $-24.91 | $10,414.76 | ▼ -24.91 after sell → book $12,687.84; vs 09:30 mark -2.27 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 15 | $35.70 | $2.06 | $+5.66 | $10,948.21 | ▲ +5.66 after sell → book $12,685.79; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 8 | $63.60 | $2.03 | $-11.65 | $11,454.97 | ▼ -11.65 after sell → book $12,683.75; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 3 | $160.93 | $2.02 | $+9.24 | $11,935.74 | ▲ +9.24 after sell → book $12,681.73; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 60 | $9.03 | $2.19 | $-1.36 | $12,475.35 | ▼ -1.36 after sell → book $12,679.54; vs 09:30 mark -2.19 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `IAUX` | 287 | $1.87 | $3.76 | $-16.07 | $13,008.28 | ▼ -16.07 after sell → book $12,675.78; vs 09:30 mark -3.76 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 1239 | $0.58 | $10.94 | — | $12,275.01 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $722.68; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 138 | $5.21 | $2.40 | — | $11,553.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $722.68; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 5 | $131.37 | $2.00 | — | $10,894.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; combo leftover $722.68; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 39 | $18.26 | $2.11 | — | $10,180.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $722.68; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 21 | $34.30 | $2.05 | — | $9,458.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; combo leftover $722.68; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 2 | $326.91 | $2.00 | — | $8,802.35 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-15.2; combo leftover $722.68; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 757 | $5.81 | $9.77 | — | $4,394.42 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $4401.18; owner union_join_vol_green_h1 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 4 | $213.94 | $2.05 | — | $5,248.13 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $878.88; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 71 | $12.22 | $2.25 | — | $6,113.50 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $878.88; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 173 | $5.08 | $2.57 | — | $6,989.77 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $878.88; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 6 | $132.64 | $2.05 | — | $7,783.56 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $878.88; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 4 | $199.94 | $2.04 | — | $8,581.28 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; combo leftover $878.88; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,581.28 | ▲ close $12,762.33 vs 09:30 $12,724.17 (session +128.77) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,581.28 | ▲ 09:30 equity $12,989.65 vs yday $12,762.33 (+227.32) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 757 | $6.50 | $9.93 | $+502.63 | $13,491.85 | ▲ +502.63 after sell → book $12,979.72; vs 09:30 mark -9.93 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 10 | $80.60 | $2.02 | — | $12,683.83 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.0; combo leftover $843.24; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 52 | $16.18 | $2.15 | — | $11,840.32 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.7; combo leftover $843.24; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 7 | $118.77 | $2.01 | — | $11,006.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.3; combo leftover $843.24; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 47 | $17.78 | $2.13 | — | $10,169.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.2; combo leftover $843.24; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 62 | $13.41 | $2.18 | — | $9,335.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; combo leftover $843.24; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 8 | $97.16 | $2.01 | — | $8,556.24 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; combo leftover $843.24; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 4 | $206.82 | $2.00 | — | $7,726.96 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.2; combo leftover $843.24; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 7 | $120.17 | $2.01 | — | $6,883.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $843.24; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 43 | $74.54 | $2.24 | — | $10,086.73 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $3240.80; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 58 | $55.25 | $2.29 | — | $13,288.94 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $3240.80; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,288.94 | ▼ close $12,898.39 vs 09:30 $12,989.65 (session -60.28) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,288.94 | ▼ 09:30 equity $12,893.43 vs yday $12,898.39 (-4.96) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 3 | $172.76 | $2.02 | $-10.77 | $13,805.21 | ▼ -10.77 after sell → book $12,891.41; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 5 | $93.30 | $2.02 | $+17.77 | $14,269.68 | ▲ +17.77 after sell → book $12,889.38; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 34 | $18.15 | $2.11 | $+93.38 | $14,884.67 | ▲ +93.38 after sell → book $12,887.27; vs 09:30 mark -2.11 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 3 | $132.80 | $2.02 | $-32.70 | $15,281.05 | ▼ -32.70 after sell → book $12,885.25; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 104 | $4.58 | $2.33 | $-58.71 | $15,755.04 | ▼ -58.71 after sell → book $12,882.92; vs 09:30 mark -2.33 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 11 | $48.42 | $2.04 | $+1.76 | $16,285.62 | ▲ +1.76 after sell → book $12,880.88; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 38 | $15.66 | $2.12 | $+61.89 | $16,878.57 | ▲ +61.89 after sell → book $12,878.76; vs 09:30 mark -2.12 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 117 | $3.38 | $2.37 | $-141.02 | $17,271.66 | ▼ -141.02 after sell → book $12,876.39; vs 09:30 mark -2.37 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 109 | $13.90 | $2.32 | $-34.68 | $15,754.25 | ▼ -34.68 after sell → book $12,874.07; vs 09:30 mark -2.32 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 27 | $52.49 | $2.07 | $+50.34 | $14,334.94 | ▲ +50.34 after sell → book $12,872.00; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 4 | $347.82 | $2.00 | $+62.05 | $12,941.66 | ▲ +62.05 after sell → book $12,870.00; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 21 | $24.69 | $2.05 | — | $12,421.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $539.24; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 2 | $261.16 | $2.00 | — | $11,896.80 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; combo leftover $539.24; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 35 | $15.01 | $2.10 | — | $11,369.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $539.24; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 5 | $103.89 | $2.00 | — | $10,847.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $539.24; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 138 | $3.88 | $2.40 | — | $10,310.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $539.24; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 12 | $44.40 | $2.03 | — | $9,775.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $539.24; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 64 | $8.35 | $2.18 | — | $9,238.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $539.24; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 14 | $37.65 | $2.03 | — | $8,709.59 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $539.24; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 93 | $23.30 | $2.27 | — | $6,540.42 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+14.5; combo leftover $2177.40; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 114 | $19.00 | $2.33 | — | $4,372.09 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+7.5; combo leftover $2177.40; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 8 | $252.24 | $2.10 | — | $6,387.91 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $2186.04; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 72 | $30.18 | $2.29 | — | $8,558.58 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $2186.04; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,558.58 | ▲ close $12,896.43 vs 09:30 $12,893.43 (session +52.22) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,558.58 | ▼ 09:30 equity $12,874.73 vs yday $12,896.43 (-21.70) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 1239 | $0.51 | $10.25 | $-111.64 | $9,180.22 | ▼ -111.64 after sell → book $12,864.48; vs 09:30 mark -10.25 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 138 | $5.00 | $2.44 | $-33.82 | $9,867.78 | ▼ -33.82 after sell → book $12,862.05; vs 09:30 mark -2.43 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 5 | $148.03 | $2.02 | $+79.27 | $10,605.91 | ▲ +79.27 after sell → book $12,860.02; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 39 | $19.25 | $2.13 | $+34.38 | $11,354.53 | ▲ +34.38 after sell → book $12,857.89; vs 09:30 mark -2.13 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BOX` | 21 | $34.72 | $2.07 | $+4.69 | $12,081.58 | ▲ +4.69 after sell → book $12,855.82; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 2 | $298.01 | $2.02 | $-61.81 | $12,675.58 | ▼ -61.81 after sell → book $12,853.80; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 4 | $208.88 | $2.00 | $+16.19 | $11,838.06 | ▲ +16.19 after sell → book $12,851.80; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 71 | $11.10 | $2.20 | $+75.07 | $11,047.75 | ▲ +75.07 after sell → book $12,849.60; vs 09:30 mark -2.20 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 173 | $4.97 | $2.51 | $+13.08 | $10,184.57 | ▲ +13.08 after sell → book $12,847.09; vs 09:30 mark -2.51 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 6 | $127.45 | $2.01 | $+27.08 | $9,417.86 | ▲ +27.08 after sell → book $12,845.08; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 4 | $254.39 | $2.00 | $-221.85 | $8,398.30 | ▼ -221.85 after sell → book $12,843.08; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 93 | $22.66 | $2.30 | $-64.09 | $10,503.38 | ▼ -64.09 after sell → book $12,840.78; vs 09:30 mark -2.30 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 114 | $18.12 | $2.37 | $-104.45 | $12,567.26 | ▼ -104.45 after sell → book $12,838.41; vs 09:30 mark -2.37 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,567.26 | ▼ close $12,816.08 vs 09:30 $12,874.73 (session -22.33) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,567.26 | ▲ 09:30 equity $12,891.65 vs yday $12,816.08 (+75.57) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BBY` | 10 | $79.83 | $2.04 | $-11.76 | $13,363.52 | ▼ -11.76 after sell → book $12,889.61; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BILI` | 52 | $15.97 | $2.17 | $-15.23 | $14,191.80 | ▼ -15.23 after sell → book $12,887.45; vs 09:30 mark -2.16 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CM` | 7 | $113.66 | $2.03 | $-39.81 | $14,985.38 | ▼ -39.81 after sell → book $12,885.41; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CMBT` | 47 | $18.28 | $2.15 | $+19.22 | $15,842.39 | ▲ +19.22 after sell → book $12,883.26; vs 09:30 mark -2.15 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CSIQ` | 62 | $12.18 | $2.20 | $-80.63 | $16,595.36 | ▼ -80.63 after sell → book $12,881.07; vs 09:30 mark -2.19 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `HQY` | 8 | $96.65 | $2.03 | $-8.13 | $17,366.52 | ▼ -8.13 after sell → book $12,879.03; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RY` | 4 | $203.78 | $2.02 | $-16.18 | $18,179.62 | ▼ -16.18 after sell → book $12,877.01; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TD` | 7 | $120.54 | $2.03 | $-1.45 | $19,021.37 | ▼ -1.45 after sell → book $12,874.98; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 43 | $73.22 | $2.12 | $+52.40 | $15,870.79 | ▲ +52.40 after sell → book $12,872.86; vs 09:30 mark -2.12 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 58 | $54.76 | $2.16 | $+23.97 | $12,692.55 | ▲ +23.97 after sell → book $12,870.70; vs 09:30 mark -2.16 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,692.55 | ▼ close $12,838.77 vs 09:30 $12,891.65 (session -31.93) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,692.55 | ▲ 09:30 equity $12,857.28 vs yday $12,838.77 (+18.51) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 21 | $21.97 | $2.07 | $-61.25 | $13,151.84 | ▼ -61.25 after sell → book $12,855.20; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 2 | $246.70 | $2.02 | $-32.93 | $13,643.23 | ▼ -32.93 after sell → book $12,853.19; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 35 | $15.01 | $2.12 | $-4.21 | $14,166.46 | ▼ -4.21 after sell → book $12,851.07; vs 09:30 mark -2.12 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 5 | $92.00 | $2.02 | $-63.48 | $14,624.44 | ▼ -63.48 after sell → book $12,849.05; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 138 | $3.32 | $2.44 | $-82.12 | $15,080.16 | ▼ -82.12 after sell → book $12,846.61; vs 09:30 mark -2.44 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 12 | $44.17 | $2.05 | $-6.83 | $15,608.16 | ▼ -6.83 after sell → book $12,844.57; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 64 | $8.58 | $2.20 | $+10.34 | $16,155.07 | ▲ +10.34 after sell → book $12,842.36; vs 09:30 mark -2.21 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 14 | $35.80 | $2.05 | $-29.98 | $16,654.15 | ▼ -29.98 after sell → book $12,840.31; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 8 | $235.71 | $2.01 | $+128.13 | $14,766.46 | ▲ +128.13 after sell → book $12,838.30; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 72 | $26.78 | $2.21 | $+240.30 | $12,836.09 | ▲ +240.30 after sell → book $12,836.09; vs 09:30 mark -2.21 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,836.09 | ▲ close $12,836.09 vs 09:30 $12,857.28 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,836.09 | ▲ 09:30 equity $12,836.09 vs yday $12,836.09 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 49 | $10.74 | $2.14 | — | $12,307.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $534.84; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $11,953.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $534.84; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 77 | $6.90 | $2.22 | — | $11,420.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $534.84; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $11,063.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $534.84; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 23 | $22.32 | $2.06 | — | $10,548.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $534.84; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 2 | $257.00 | $2.00 | — | $10,032.30 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $534.84; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 11 | $47.60 | $2.02 | — | $9,506.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $534.84; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 35 | $15.09 | $2.10 | — | $8,976.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $534.84; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 4 | $132.45 | $2.00 | — | $8,444.63 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; combo leftover $561.03; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 33 | $16.77 | $2.09 | — | $7,889.13 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; combo leftover $561.03; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 257 | $2.18 | $3.32 | — | $7,325.55 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; combo leftover $561.03; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 23 | $23.88 | $2.06 | — | $6,774.25 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $561.03; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 53 | $10.42 | $2.15 | — | $6,219.84 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; combo leftover $561.03; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 290 | $1.93 | $3.74 | — | $5,656.40 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $561.03; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 3 | $161.54 | $2.00 | — | $5,169.78 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+12.0; combo leftover $561.03; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 54 | $10.38 | $2.15 | — | $4,607.38 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-56.2; combo leftover $561.03; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 155 | $14.85 | $2.56 | — | $6,906.57 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $2303.69; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1347 | $1.71 | $17.68 | — | $9,192.26 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $2303.69; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,192.26 | ▲ close $13,094.23 vs 09:30 $12,836.09 (session +314.41) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,192.26 | ▲ 09:30 equity $13,153.70 vs yday $13,094.23 (+59.47) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 4 | $130.03 | $2.02 | $-13.70 | $9,710.36 | ▼ -13.70 after sell → book $13,151.68; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 33 | $15.61 | $2.11 | $-42.48 | $10,223.38 | ▼ -42.48 after sell → book $13,149.57; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 257 | $2.16 | $3.37 | $-11.82 | $10,775.13 | ▼ -11.82 after sell → book $13,146.20; vs 09:30 mark -3.37 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 23 | $23.84 | $2.08 | $-5.06 | $11,321.37 | ▼ -5.06 after sell → book $13,144.12; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 53 | $10.50 | $2.17 | $-0.08 | $11,875.70 | ▼ -0.08 after sell → book $13,141.95; vs 09:30 mark -2.17 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 290 | $1.90 | $3.80 | $-16.24 | $12,422.90 | ▼ -16.24 after sell → book $13,138.15; vs 09:30 mark -3.80 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DUOL` | 3 | $157.46 | $2.02 | $-16.26 | $12,893.26 | ▼ -16.26 after sell → book $13,136.13; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 54 | $11.23 | $2.17 | $+41.85 | $13,497.51 | ▲ +41.85 after sell → book $13,133.96; vs 09:30 mark -2.17 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 8 | $63.18 | $2.01 | — | $12,990.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $562.40; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 64 | $8.74 | $2.18 | — | $12,428.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $562.40; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 8 | $68.52 | $2.01 | — | $11,878.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $562.40; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 155 | $3.62 | $2.46 | — | $11,315.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $562.40; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 3 | $167.55 | $2.00 | — | $10,810.91 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $562.40; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 12 | $44.90 | $2.03 | — | $10,270.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $562.40; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 5 | $98.15 | $2.00 | — | $9,777.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $562.40; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 35 | $15.70 | $2.10 | — | $9,225.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $562.40; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 1 | $513.78 | $1.99 | — | $8,709.96 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; combo leftover $576.61; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 6 | $82.70 | $2.01 | — | $8,211.76 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; combo leftover $576.61; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 229 | $2.51 | $2.95 | — | $7,634.01 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; combo leftover $576.61; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MDB` | 1 | $378.34 | $1.99 | — | $7,253.68 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-12.7; combo leftover $576.61; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 22 | $25.18 | $2.06 | — | $6,697.66 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $576.61; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 99 | $5.79 | $2.29 | — | $6,122.17 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $576.61; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `TDS` | 15 | $37.44 | $2.04 | — | $5,558.53 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.1; combo leftover $576.61; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `AHCO` | 91 | $6.32 | $2.26 | — | $4,981.15 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+10.7; combo leftover $576.61; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 533 | $4.67 | $7.05 | — | $7,463.21 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $2490.57; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 32 | $76.55 | $2.18 | — | $9,910.62 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $2490.57; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,910.62 | ▲ close $13,157.86 vs 09:30 $13,153.70 (session +67.51) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,910.62 | ▼ 09:30 equity $13,132.08 vs yday $13,157.86 (-25.78) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 1 | $521.15 | $2.01 | $+3.36 | $10,429.76 | ▲ +3.36 after sell → book $13,130.07; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 6 | $89.67 | $2.03 | $+37.78 | $10,965.75 | ▲ +37.78 after sell → book $13,128.04; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 229 | $2.66 | $3.00 | $+28.39 | $11,571.89 | ▲ +28.39 after sell → book $13,125.04; vs 09:30 mark -3.00 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MDB` | 1 | $360.75 | $2.01 | $-21.60 | $11,930.63 | ▼ -21.60 after sell → book $13,123.03; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 22 | $26.44 | $2.08 | $+23.59 | $12,510.23 | ▲ +23.59 after sell → book $13,120.95; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 99 | $5.81 | $2.31 | $-2.62 | $13,083.11 | ▼ -2.62 after sell → book $13,118.64; vs 09:30 mark -2.31 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TDS` | 15 | $37.75 | $2.06 | $+0.56 | $13,647.30 | ▲ +0.56 after sell → book $13,116.58; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AHCO` | 91 | $6.48 | $2.29 | $+10.01 | $14,234.69 | ▲ +10.01 after sell → book $13,114.29; vs 09:30 mark -2.29 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,234.69 | ▲ close $13,324.42 vs 09:30 $13,132.08 (session +210.13) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,234.69 | ▼ 09:30 equity $13,315.77 vs yday $13,324.42 (-8.65) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 49 | $10.51 | $2.16 | $-15.81 | $14,747.53 | ▼ -15.81 after sell → book $13,313.62; vs 09:30 mark -2.15 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 1 | $366.23 | $2.01 | $+10.48 | $15,111.74 | ▲ +10.48 after sell → book $13,311.60; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 77 | $9.39 | $2.24 | $+187.27 | $15,832.53 | ▲ +187.27 after sell → book $13,309.36; vs 09:30 mark -2.24 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 1 | $341.90 | $2.01 | $-16.60 | $16,172.42 | ▼ -16.60 after sell → book $13,307.35; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CPB` | 23 | $21.67 | $2.08 | $-19.09 | $16,668.75 | ▼ -19.09 after sell → book $13,305.27; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 2 | $252.92 | $2.02 | $-12.17 | $17,172.57 | ▼ -12.17 after sell → book $13,303.25; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 11 | $56.94 | $2.04 | $+98.67 | $17,796.87 | ▲ +98.67 after sell → book $13,301.21; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MEI` | 35 | $13.84 | $2.12 | $-47.96 | $18,279.15 | ▼ -47.96 after sell → book $13,299.09; vs 09:30 mark -2.12 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 155 | $13.60 | $2.46 | $+188.73 | $16,168.70 | ▲ +188.73 after sell → book $13,296.64; vs 09:30 mark -2.45 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1347 | $1.58 | $17.38 | $+140.05 | $14,023.06 | ▲ +140.05 after sell → book $13,279.26; vs 09:30 mark -17.38 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,023.06 | ▼ close $13,269.42 vs 09:30 $13,315.77 (session -9.85) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,023.06 | ▲ 09:30 equity $13,323.30 vs yday $13,269.42 (+53.88) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `AMBA` | 8 | $67.44 | $2.03 | $+30.03 | $14,560.55 | ▲ +30.03 after sell → book $13,321.27; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ASAN` | 64 | $8.26 | $2.20 | $-35.10 | $15,086.99 | ▼ -35.10 after sell → book $13,319.07; vs 09:30 mark -2.20 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOCU` | 8 | $64.60 | $2.03 | $-35.41 | $15,601.75 | ▼ -35.41 after sell → book $13,317.03; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 155 | $3.76 | $2.49 | $+17.53 | $16,182.06 | ▲ +17.53 after sell → book $13,314.54; vs 09:30 mark -2.49 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `GWRE` | 3 | $142.43 | $2.02 | $-79.38 | $16,607.33 | ▼ -79.38 after sell → book $13,312.52; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `IOT` | 12 | $38.23 | $2.05 | $-84.17 | $17,063.99 | ▼ -84.17 after sell → book $13,310.48; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `LULU` | 5 | $98.71 | $2.02 | $-1.23 | $17,555.51 | ▼ -1.23 after sell → book $13,308.45; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MAMA` | 35 | $15.26 | $2.12 | $-19.61 | $18,087.50 | ▼ -19.61 after sell → book $13,306.34; vs 09:30 mark -2.11 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 533 | $4.36 | $6.88 | $+151.30 | $15,756.74 | ▲ +151.30 after sell → book $13,299.46; vs 09:30 mark -6.88 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 32 | $76.79 | $2.09 | $-11.95 | $13,297.37 | ▼ -11.95 after sell → book $13,297.37; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,297.37 | ▲ close $13,297.37 vs 09:30 $13,323.30 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,297.37 | ▲ 09:30 equity $13,297.37 vs yday $13,297.37 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 3 | $164.43 | $2.00 | — | $12,802.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $554.06; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 93 | $5.91 | $2.27 | — | $12,250.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; combo leftover $554.06; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 2 | $242.17 | $2.00 | — | $11,763.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; combo leftover $554.06; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 17 | $32.01 | $2.04 | — | $11,217.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $554.06; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 7 | $71.71 | $2.01 | — | $10,713.66 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; combo leftover $554.06; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 9 | $56.02 | $2.02 | — | $10,207.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; combo leftover $554.06; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 59 | $9.37 | $2.17 | — | $9,652.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $554.06; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 42 | $13.10 | $2.12 | — | $9,100.15 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; combo leftover $554.06; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 24 | $23.63 | $2.06 | — | $8,530.97 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; ret5=-6.3; combo leftover $568.76; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 210 | $2.70 | $2.71 | — | $7,961.26 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $568.76; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 51 | $10.95 | $2.14 | — | $7,400.66 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+20.8; combo leftover $568.76; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 115 | $4.91 | $2.33 | — | $6,833.68 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $568.76; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 6 | $84.27 | $2.01 | — | $6,326.05 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+17.2; combo leftover $568.76; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 10 | $54.91 | $2.02 | — | $5,774.93 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+24.3; combo leftover $568.76; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 92 | $6.16 | $2.27 | — | $5,205.95 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+36.4; combo leftover $568.76; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 10 | $54.66 | $2.02 | — | $4,657.33 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-22.3; combo leftover $568.76; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 8 | $112.83 | $2.06 | — | $5,557.95 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $931.47; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 264 | $3.52 | $3.48 | — | $6,483.74 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $931.47; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 458 | $2.03 | $6.02 | — | $7,407.46 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $931.47; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 37 | $24.97 | $2.15 | — | $8,329.21 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $931.47; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 276 | $3.37 | $3.64 | — | $9,255.68 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $931.47; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,255.68 | ▼ close $13,203.46 vs 09:30 $13,297.37 (session -42.38) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,255.68 | ▲ 09:30 equity $13,280.67 vs yday $13,203.46 (+77.21) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 24 | $23.20 | $2.08 | $-14.46 | $9,810.40 | ▼ -14.46 after sell → book $13,278.59; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INDP` | 210 | $2.80 | $2.75 | $+15.54 | $10,395.65 | ▲ +15.54 after sell → book $13,275.84; vs 09:30 mark -2.75 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 51 | $10.29 | $2.16 | $-37.97 | $10,918.28 | ▼ -37.97 after sell → book $13,273.68; vs 09:30 mark -2.16 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BNC` | 115 | $5.03 | $2.36 | $+9.10 | $11,494.36 | ▲ +9.10 after sell → book $13,271.31; vs 09:30 mark -2.37 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 6 | $86.06 | $2.03 | $+6.70 | $12,008.69 | ▲ +6.70 after sell → book $13,269.28; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 10 | $54.75 | $2.04 | $-5.66 | $12,554.15 | ▼ -5.66 after sell → book $13,267.24; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 92 | $6.02 | $2.29 | $-17.44 | $13,105.70 | ▼ -17.44 after sell → book $13,264.95; vs 09:30 mark -2.29 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 10 | $54.78 | $2.04 | $-2.86 | $13,651.46 | ▼ -2.86 after sell → book $13,262.91; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,651.46 | ▲ close $13,305.21 vs 09:30 $13,280.67 (session +42.30) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,651.46 | ▼ 09:30 equity $13,291.00 vs yday $13,305.21 (-14.21) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,651.46 | ▼ close $13,235.73 vs 09:30 $13,291.00 (session -55.27) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,651.46 | ▼ 09:30 equity $13,217.48 vs yday $13,235.73 (-18.25) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 3 | $140.03 | $2.02 | $-77.22 | $14,069.53 | ▼ -77.22 after sell → book $13,215.46; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DBI` | 93 | $6.25 | $2.29 | $+27.06 | $14,648.49 | ▲ +27.06 after sell → book $13,213.17; vs 09:30 mark -2.29 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 2 | $253.34 | $2.02 | $+18.33 | $15,153.15 | ▲ +18.33 after sell → book $13,211.15; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CPRT` | 17 | $30.57 | $2.06 | $-28.58 | $15,670.78 | ▼ -28.58 after sell → book $13,209.09; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DSGX` | 7 | $78.12 | $2.03 | $+40.83 | $16,215.59 | ▲ +40.83 after sell → book $13,207.06; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `KR` | 9 | $61.93 | $2.04 | $+49.14 | $16,770.92 | ▲ +49.14 after sell → book $13,205.02; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `LPTH` | 59 | $9.40 | $2.19 | $-2.58 | $17,323.34 | ▼ -2.58 after sell → book $13,202.84; vs 09:30 mark -2.18 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `REF` | 42 | $15.75 | $2.14 | $+107.05 | $17,982.70 | ▲ +107.05 after sell → book $13,200.70; vs 09:30 mark -2.14 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 8 | $118.18 | $2.01 | $-46.83 | $17,035.25 | ▼ -46.83 after sell → book $13,198.69; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 264 | $3.98 | $3.41 | $-128.33 | $15,981.12 | ▼ -128.33 after sell → book $13,195.28; vs 09:30 mark -3.41 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 458 | $1.85 | $5.91 | $+70.51 | $15,127.91 | ▲ +70.51 after sell → book $13,189.37; vs 09:30 mark -5.91 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 37 | $24.42 | $2.10 | $+16.10 | $14,222.27 | ▲ +16.10 after sell → book $13,187.27; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 276 | $3.75 | $3.56 | $-112.08 | $13,183.71 | ▼ -112.08 after sell → book $13,183.71; vs 09:30 mark -3.56 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 66 | $33.14 | $2.19 | — | $10,994.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; combo leftover $2197.29; owner union_e_fresh_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 53 | $40.93 | $2.15 | — | $8,822.84 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $2197.29; owner union_e_fresh_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 8 | $77.12 | $2.01 | — | $8,203.87 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+7.2; combo leftover $630.20; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 107 | $5.87 | $2.31 | — | $7,573.47 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.1; combo leftover $630.20; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 7 | $87.40 | $2.01 | — | $6,959.66 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.2; combo leftover $630.20; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 23 | $27.09 | $2.06 | — | $6,334.53 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; combo leftover $630.20; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 7 | $89.38 | $2.01 | — | $5,706.86 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; combo leftover $630.20; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 27 | $23.29 | $2.07 | — | $5,075.96 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+16.1; combo leftover $630.20; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 22 | $28.16 | $2.06 | — | $4,454.38 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+14.8; combo leftover $630.20; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 119 | $18.61 | $2.45 | — | $6,666.53 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $2227.19; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 326 | $6.83 | $4.34 | — | $8,888.77 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $2227.19; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,888.77 | ▼ close $12,854.77 vs 09:30 $13,217.48 (session -303.29) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,888.77 | ▲ 09:30 equity $13,018.69 vs yday $12,854.77 (+163.92) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 8 | $76.44 | $2.03 | $-9.49 | $9,498.25 | ▼ -9.49 after sell → book $13,016.65; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 107 | $5.58 | $2.34 | $-35.68 | $10,092.98 | ▼ -35.68 after sell → book $13,014.32; vs 09:30 mark -2.33 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 7 | $83.20 | $2.03 | $-33.44 | $10,673.34 | ▼ -33.44 after sell → book $13,012.28; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 23 | $28.23 | $2.08 | $+22.08 | $11,320.56 | ▲ +22.08 after sell → book $13,010.21; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 7 | $86.76 | $2.03 | $-22.38 | $11,925.84 | ▼ -22.38 after sell → book $13,008.17; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 27 | $24.09 | $2.09 | $+17.44 | $12,574.18 | ▲ +17.44 after sell → book $13,006.08; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CAI` | 22 | $28.59 | $2.08 | $+5.44 | $13,201.20 | ▲ +5.44 after sell → book $13,004.01; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 27 | $81.00 | $2.07 | — | $11,012.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.0; combo leftover $2200.20; owner union_e_fresh_h3 | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 196 | $11.21 | $2.58 | — | $8,812.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.0; combo leftover $2200.20; owner union_e_fresh_h3 | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 2 | $233.85 | $2.00 | — | $8,342.69 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+11.7; combo leftover $629.46; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 4 | $147.61 | $2.00 | — | $7,750.25 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+17.7; combo leftover $629.46; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 82 | $7.59 | $2.24 | — | $7,125.63 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; combo leftover $629.46; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 24 | $25.95 | $2.06 | — | $6,500.77 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; combo leftover $629.46; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 3 | $170.85 | $2.00 | — | $5,986.22 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; combo leftover $629.46; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 34 | $18.04 | $2.09 | — | $5,370.94 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; combo leftover $629.46; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRKR` | 10 | $61.90 | $2.02 | — | $4,749.92 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ret5=+11.5; combo leftover $629.46; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 597 | $7.95 | $7.97 | — | $9,488.10 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $4749.92; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,488.10 | ▲ close $13,328.35 vs 09:30 $13,018.69 (session +351.37) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,488.10 | ▲ 09:30 equity $13,407.70 vs yday $13,328.35 (+79.35) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 2 | $249.13 | $2.02 | $+26.55 | $9,984.35 | ▲ +26.55 after sell → book $13,405.69; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 4 | $146.50 | $2.02 | $-8.46 | $10,568.32 | ▼ -8.46 after sell → book $13,403.66; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 82 | $7.98 | $2.26 | $+27.48 | $11,220.43 | ▲ +27.48 after sell → book $13,401.41; vs 09:30 mark -2.25 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 24 | $26.14 | $2.08 | $+0.42 | $11,845.70 | ▲ +0.42 after sell → book $13,399.32; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 3 | $182.33 | $2.02 | $+30.42 | $12,390.67 | ▲ +30.42 after sell → book $13,397.30; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 34 | $17.80 | $2.11 | $-12.19 | $12,993.76 | ▼ -12.19 after sell → book $13,395.19; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRKR` | 10 | $63.37 | $2.04 | $+10.64 | $13,625.42 | ▲ +10.64 after sell → book $13,393.15; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 3 | $219.62 | $2.00 | — | $12,964.56 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; combo leftover $851.59; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 10 | $85.00 | $2.02 | — | $12,112.54 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+18.3; combo leftover $851.59; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 215 | $3.95 | $2.77 | — | $11,260.52 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; combo leftover $851.59; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 60 | $14.07 | $2.17 | — | $10,414.15 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; combo leftover $851.59; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 57 | $14.79 | $2.16 | — | $9,568.96 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; combo leftover $851.59; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 29 | $29.32 | $2.08 | — | $8,716.60 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $851.59; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 280 | $3.04 | $3.61 | — | $7,863.19 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $851.59; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `VITL` | 74 | $11.38 | $2.21 | — | $7,018.86 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+19.5; combo leftover $851.59; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 194 | $34.44 | $2.84 | — | $13,697.38 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $6687.06; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,697.38 | ▲ close $13,740.19 vs 09:30 $13,407.70 (session +368.90) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,697.38 | ▲ 09:30 equity $13,759.97 vs yday $13,740.19 (+19.78) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `FPS` | 66 | $40.03 | $2.22 | $+450.33 | $16,337.14 | ▲ +450.33 after sell → book $13,757.75; vs 09:30 mark -2.22 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 53 | $41.00 | $2.18 | $-0.62 | $18,507.96 | ▼ -0.62 after sell → book $13,755.57; vs 09:30 mark -2.18 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 119 | $22.11 | $2.35 | $-421.29 | $15,874.52 | ▼ -421.29 after sell → book $13,753.22; vs 09:30 mark -2.35 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 326 | $6.55 | $4.21 | $+82.74 | $13,735.02 | ▲ +82.74 after sell → book $13,749.02; vs 09:30 mark -4.20 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 3 | $230.25 | $2.02 | $+27.87 | $14,423.75 | ▲ +27.87 after sell → book $13,747.00; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 10 | $82.83 | $2.04 | $-25.76 | $15,250.01 | ▼ -25.76 after sell → book $13,744.96; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 215 | $3.87 | $2.82 | $-22.79 | $16,079.24 | ▼ -22.79 after sell → book $13,742.14; vs 09:30 mark -2.82 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 60 | $13.90 | $2.19 | $-14.56 | $16,911.05 | ▼ -14.56 after sell → book $13,739.95; vs 09:30 mark -2.19 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 57 | $14.58 | $2.18 | $-16.31 | $17,739.93 | ▼ -16.31 after sell → book $13,737.77; vs 09:30 mark -2.18 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 29 | $29.43 | $2.10 | $-0.98 | $18,591.30 | ▼ -0.98 after sell → book $13,735.67; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 280 | $4.00 | $3.67 | $+262.92 | $19,707.63 | ▲ +262.92 after sell → book $13,732.00; vs 09:30 mark -3.67 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VITL` | 74 | $12.05 | $2.23 | $+45.13 | $20,597.10 | ▲ +45.13 after sell → book $13,729.77; vs 09:30 mark -2.23 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 8 | $157.87 | $2.01 | — | $19,332.13 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+6.5; combo leftover $1287.32; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 14 | $88.83 | $2.03 | — | $18,086.47 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+7.6; combo leftover $1287.32; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 138 | $9.31 | $2.40 | — | $16,799.29 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.3; combo leftover $1287.32; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 95 | $13.47 | $2.27 | — | $15,516.89 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; combo leftover $1287.32; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 128 | $9.99 | $2.37 | — | $14,235.80 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+5.3; combo leftover $1287.32; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 76 | $16.91 | $2.22 | — | $12,948.42 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $1287.32; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 98 | $13.05 | $2.28 | — | $11,667.23 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+40.4; combo leftover $1287.32; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 223 | $5.75 | $2.88 | — | $10,380.99 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+30.3; combo leftover $1287.32; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 414 | $8.26 | $5.53 | — | $13,795.10 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3427.82; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 5 | $583.88 | $2.12 | — | $16,712.38 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $3427.82; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,712.38 | ▼ close $13,678.04 vs 09:30 $13,759.97 (session -25.59) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,712.38 | ▲ 09:30 equity $13,751.62 vs yday $13,678.04 (+73.58) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 597 | $8.28 | $7.70 | $-209.69 | $11,764.51 | ▼ -209.69 after sell → book $13,743.92; vs 09:30 mark -7.70 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 128 | $9.91 | $2.41 | $-15.02 | $13,030.58 | ▼ -15.02 after sell → book $13,741.52; vs 09:30 mark -2.40 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 98 | $12.99 | $2.31 | $-10.47 | $14,301.29 | ▼ -10.47 after sell → book $13,739.21; vs 09:30 mark -2.31 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `GEMI` | 223 | $6.05 | $2.92 | $+61.10 | $15,648.63 | ▲ +61.10 after sell → book $13,736.28; vs 09:30 mark -2.93 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `ARM` | 4 | $319.41 | $2.00 | — | $14,368.99 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+35.1; combo leftover $1564.86; owner union_join_vol_green_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `FSLY` | 55 | $28.02 | $2.15 | — | $12,825.73 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; combo leftover $1564.86; owner union_join_vol_green_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `META` | 2 | $731.40 | $2.00 | — | $11,360.94 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+11.4; combo leftover $1564.86; owner union_join_vol_green_h1 | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 36 | $93.97 | $2.23 | — | $14,741.63 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3432.53; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,741.63 | ▼ close $13,602.20 vs 09:30 $13,751.62 (session -125.70) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,741.63 | ▼ 09:30 equity $13,120.54 vs yday $13,602.20 (-481.66) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `LEN` | 27 | $82.00 | $2.10 | $+22.83 | $16,953.53 | ▲ +22.83 after sell → book $13,118.44; vs 09:30 mark -2.10 | union_e_fresh_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `ALMU` | 196 | $13.82 | $2.63 | $+506.35 | $19,659.62 | ▲ +506.35 after sell → book $13,115.81; vs 09:30 mark -2.63 | union_e_fresh_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 138 | $9.50 | $2.44 | $+21.38 | $20,968.18 | ▲ +21.38 after sell → book $13,113.37; vs 09:30 mark -2.44 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 95 | $12.84 | $2.30 | $-64.90 | $22,185.68 | ▼ -64.90 after sell → book $13,111.07; vs 09:30 mark -2.30 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 76 | $16.92 | $2.24 | $-3.70 | $23,469.36 | ▼ -3.70 after sell → book $13,108.83; vs 09:30 mark -2.24 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARM` | 4 | $331.78 | $2.02 | $+45.46 | $24,794.46 | ▲ +45.46 after sell → book $13,106.81; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FSLY` | 55 | $25.90 | $2.18 | $-120.93 | $26,216.78 | ▼ -120.93 after sell → book $13,104.63; vs 09:30 mark -2.18 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `META` | 2 | $747.60 | $2.02 | $+28.39 | $27,709.96 | ▲ +28.39 after sell → book $13,102.61; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 38 | $47.57 | $2.10 | — | $25,900.20 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $1847.33; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 9 | $196.78 | $2.02 | — | $24,127.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $1847.33; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 51 | $35.74 | $2.14 | — | $22,302.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $1847.33; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 39 | $47.15 | $2.11 | — | $20,461.32 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $1847.33; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 16 | $109.67 | $2.04 | — | $18,704.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $1847.33; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 75 | $20.65 | $2.21 | — | $17,153.60 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; combo leftover $1558.71; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 99 | $15.72 | $2.29 | — | $15,595.03 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; combo leftover $1558.71; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 61 | $25.40 | $2.17 | — | $14,043.46 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; combo leftover $1558.71; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 2029 | $0.77 | $21.67 | — | $12,463.52 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; combo leftover $1558.71; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 37 | $41.76 | $2.10 | — | $10,916.30 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $1558.71; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 157 | $9.90 | $2.46 | — | $9,359.54 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; combo leftover $1558.71; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 55 | $116.85 | $2.39 | — | $15,783.89 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $6529.65; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,783.89 | ▼ close $12,806.84 vs 09:30 $13,120.54 (session -250.06) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,783.89 | ▼ 09:30 equity $12,658.53 vs yday $12,806.84 (-148.31) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 8 | $163.95 | $2.03 | $+44.59 | $17,093.46 | ▲ +44.59 after sell → book $12,656.50; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 14 | $87.67 | $2.05 | $-20.25 | $18,318.86 | ▼ -20.25 after sell → book $12,654.45; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 5 | $600.27 | $2.00 | $-86.07 | $15,315.50 | ▼ -86.07 after sell → book $12,652.44; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 75 | $20.52 | $2.24 | $-14.20 | $16,852.26 | ▼ -14.20 after sell → book $12,650.20; vs 09:30 mark -2.24 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 99 | $14.38 | $2.31 | $-137.26 | $18,273.57 | ▼ -137.26 after sell → book $12,647.89; vs 09:30 mark -2.31 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `TNGX` | 61 | $23.99 | $2.19 | $-90.38 | $19,734.76 | ▼ -90.38 after sell → book $12,645.69; vs 09:30 mark -2.20 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `NMRA` | 2029 | $0.75 | $21.57 | $-87.88 | $21,226.82 | ▼ -87.88 after sell → book $12,624.12; vs 09:30 mark -21.57 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 37 | $36.02 | $2.12 | $-216.42 | $22,557.63 | ▼ -216.42 after sell → book $12,622.00; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BFLY` | 157 | $9.12 | $2.50 | $-127.42 | $23,986.97 | ▼ -127.42 after sell → book $12,619.50; vs 09:30 mark -2.50 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23,986.97 | ▼ close $12,460.03 vs 09:30 $12,658.53 (session -159.47) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,704.60 | ▼ 09:30 equity $8,959.72 vs yday $8,972.51 (-12.79) | 09:30 open · cash $8,704.60 (unchanged overnight, no fees) · equity $8,959.72 vs prior close $8,972.51 (-12.79) | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 3 | $887.00 | $2.00 | — | $6,041.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+0.3; combo leftover $2901.53; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 14 | $26.27 | $2.03 | — | $5,671.79 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; combo leftover $377.60; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 97 | $3.86 | $2.28 | — | $5,295.09 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; combo leftover $377.60; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 2 | $184.00 | $2.00 | — | $4,925.09 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; combo leftover $377.60; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 23 | $16.21 | $2.06 | — | $4,550.20 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $377.60; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 3 | $123.50 | $2.00 | — | $4,177.70 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; combo leftover $377.60; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `QMCO` | 12 | $29.80 | $2.03 | — | $3,818.08 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+18.2; combo leftover $377.60; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 94 | $4.00 | $2.27 | — | $3,439.34 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; combo leftover $377.60; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CDNA` | 6 | $61.33 | $2.01 | — | $3,069.35 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+13.1; combo leftover $377.60; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 390 | $7.85 | $5.20 | — | $6,125.64 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $3069.35; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,125.64 | ▲ close $9,034.53 vs 09:30 $8,959.72 (session +98.68) | 16:00 close · cash $6,125.64 · equity $9,034.53 vs 09:30 $8,959.72 (+74.81; session marks +98.68) · 23 name(s) marked open→close (per-name table). ABVX×15 09:30 $94.87 → close $94.87 +0.00; AEHL×302 09:30 $9.05 → close $9.36 -93.62; ANAB×29 09:30 $51.70 → close $51.70 +0.00; BAND×39 09:30 $61.83 → close $61.83 -0.00; CBRL×31 09:30 $52.39 → close $51.81 -17.98; CTAS×7 09:30 $197.68 → close $197.68 -0.00; GIS×41 09:30 $34.83 → close $34.83 +0.00; HALO×19 09:30 $115.36 → close $113.90 +27.74; KBH×31 09:30 $47.65 → close $47.65 +0.00; MLKN×79 09:30 $19.91 → close $19.91 -0.00; PAYX×20 09:30 $101.59 → close $101.59 +0.00; THO×22 09:30 $70.93 → close $70.93 +0.00; USFD×25 09:30 $93.82 → close $93.82 +0.00; COST×3 09:30 $887.00 → close $922.76 +107.29; WRBY×14 09:30 $26.27 → close $26.71 +6.16; ZSQR×97 09:30 $3.86 → close $3.78 -7.76; TWST×2 09:30 $184.00 → close $182.83 -2.34; SECZ×23 09:30 $16.21 → close $15.96 -5.75; GRAL×3 09:30 $123.50 → close $126.89 +10.17; QMCO×12 09:30 $29.80 → close $31.68 +22.56; CYPH×94 09:30 $4.00 → close $4.12 +10.81; CDNA×6 09:30 $61.33 → close $63.68 +14.10; RSKD×390 09:30 $7.85 → close $7.78 +27.30 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `INO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `VOR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `BTBT` | cash | leftover split 0.88 < 1 share @ 1.50 |
| 2026-08-14 | `AIRO` | cash | leftover split 0.88 < 1 share @ 11.12 |
| 2026-08-14 | `EU` | cash | leftover split 0.88 < 1 share @ 1.18 |
| 2026-08-14 | `LUNR` | cash | leftover split 0.88 < 1 share @ 19.17 |
| 2026-08-14 | `ARX` | cash | leftover split 0.88 < 1 share @ 19.57 |
| 2026-08-14 | `MH` | cash | leftover split 0.88 < 1 share @ 13.55 |
| 2026-08-14 | `CLBT` | cash | leftover split 0.88 < 1 share @ 10.83 |
| 2026-08-14 | `NMAX` | cash | leftover split 0.88 < 1 share @ 9.89 |
| 2026-08-14 | `BETR` | cash | leftover split 1.76 < 1 share @ 14.80 |
| 2026-08-14 | `ANGX` | cash | leftover split 1.76 < 1 share @ 4.31 |
| 2026-08-14 | `HYLN` | cash | leftover split 1.76 < 1 share @ 4.18 |
| 2026-08-14 | `ADUR` | cash | leftover split 1.76 < 1 share @ 16.50 |
| 2026-08-14 | `NCMI` | cash | leftover split 1.76 < 1 share @ 2.69 |
| 2026-08-14 | `QMLS` | cash | leftover split 1.76 < 1 share @ 7.29 |
| 2026-08-17 | `INO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `VOR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `ABX` | cash | leftover split 3.36 < 1 share @ 9.12 |
| 2026-08-17 | `ALOY` | cash | leftover split 3.36 < 1 share @ 14.66 |
| 2026-08-17 | `BORR` | cash | leftover split 3.36 < 1 share @ 4.59 |
| 2026-08-17 | `XHG` | cash | leftover split 3.36 < 1 share @ 4.19 |
| 2026-08-17 | `MP` | cash | leftover split 3.36 < 1 share @ 58.01 |
| 2026-08-17 | `APMD` | cash | leftover split 6.72 < 1 share @ 31.70 |
| 2026-08-17 | `RNW` | cash | leftover split 6.72 < 1 share @ 6.80 |
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
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
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
| 2026-08-31 | `GAP` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `ADSK` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `BBAR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `ESTC` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FINV` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FRO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `HAFN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `IREN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new long union_join_vol_green_h1 |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new long union_join_vol_green_h1 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new long union_e_fresh_h3 |
| 2026-09-01 | `GAP` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `ADSK` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `BBAR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `ESTC` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FINV` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FRO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
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
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
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
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `ARBE` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
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
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `GME` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `DHT` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
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
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `IOT` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `SAFX` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
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
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `GRPN` | no_price | no 09:30 open |
| 2026-09-22 | `XXI` | no_price | no 09:30 open |
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
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `GLND` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `KVYO` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
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
| `FIVN` | 194 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $6687.06; owner short_news_r_h3 |
| `AEHL` | 414 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3427.82; owner short_news_r_h3 |
| `USFD` | 36 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3432.53; owner short_news_r_h3 |
| `CBRL` | 38 | 2026-09-23 @ $47.57 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $1847.33; owner union_e_fresh_h3 |
| `CTAS` | 9 | 2026-09-23 @ $196.78 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $1847.33; owner union_e_fresh_h3 |
| `GIS` | 51 | 2026-09-23 @ $35.74 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $1847.33; owner union_e_fresh_h3 |
| `KBH` | 39 | 2026-09-23 @ $47.15 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $1847.33; owner union_e_fresh_h3 |
| `PAYX` | 16 | 2026-09-23 @ $109.67 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $1847.33; owner union_e_fresh_h3 |
| `HALO` | 55 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $6529.65; owner short_news_r_h3 |
