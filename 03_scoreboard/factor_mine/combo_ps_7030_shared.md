# Factor mine action — `combo_ps_7030_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_news_pack_h1/short_news_r_h3 w=0.7,0.3 net=priority

Cash book **-13.29%** ($8,671) · signal-only (no cash/fees) was —. Starts YES **2/30**. Fills 176 · skips 146 · realized $+1121.47.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_news_pack_h1 70%, short_news_r_h3 30%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_news_pack_h1 70%, short_news_r_h3 30%.
- Member: union_news_pack_h1 (70% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,324.43.

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
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 19 | $120.00 | $2.05 | — | $7,717.95 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+0.6; combo leftover $2333.33; owner union_news_pack_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 6 | $359.83 | $2.01 | — | $5,556.97 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+5.9; combo leftover $2333.33; owner union_news_pack_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 15 | $146.90 | $2.04 | — | $3,351.43 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+3.6; combo leftover $2333.33; owner union_news_pack_h1 | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 946 | $1.18 | $12.40 | — | $4,455.31 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $1117.14; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 58 | $19.17 | $2.21 | — | $5,564.95 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $1117.14; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 87 | $12.70 | $2.31 | — | $6,667.11 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $1117.14; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,667.11 | ▲ close $10,153.68 vs 09:30 $10,000.00 (session +176.69) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,667.11 | ▲ 09:30 equity $10,161.94 vs yday $10,153.68 (+8.26) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 19 | $127.40 | $2.08 | $+136.48 | $9,085.64 | ▲ +136.48 after sell → book $10,159.87; vs 09:30 mark -2.07 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 6 | $367.88 | $2.04 | $+44.26 | $11,290.88 | ▲ +44.26 after sell → book $10,157.83; vs 09:30 mark -2.04 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 15 | $149.37 | $2.06 | $+32.95 | $13,529.37 | ▲ +32.95 after sell → book $10,155.77; vs 09:30 mark -2.06 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 68 | $46.18 | $2.19 | — | $10,386.93 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+6.7; combo leftover $3156.85; owner union_news_pack_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 22 | $142.77 | $2.06 | — | $7,243.94 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+5.8; combo leftover $3156.85; owner union_news_pack_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 15 | $202.70 | $2.04 | — | $4,201.40 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+8.3; combo leftover $3156.85; owner union_news_pack_h1 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 730 | $1.15 | $9.57 | — | $5,031.33 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $840.28; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 236 | $3.56 | $3.12 | — | $5,868.38 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $840.28; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 26 | $31.70 | $2.11 | — | $6,690.47 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; combo leftover $840.28; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 279 | $3.01 | $3.68 | — | $7,526.58 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $840.28; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 123 | $6.80 | $2.41 | — | $8,360.56 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; combo leftover $840.28; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,360.56 | ▲ close $10,431.41 vs 09:30 $10,161.94 (session +302.82) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,360.56 | ▲ 09:30 equity $10,650.92 vs yday $10,431.41 (+219.51) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 68 | $48.00 | $2.23 | $+119.33 | $11,622.33 | ▲ +119.33 after sell → book $10,648.69; vs 09:30 mark -2.23 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 22 | $148.04 | $2.09 | $+111.79 | $14,877.12 | ▲ +111.79 after sell → book $10,646.60; vs 09:30 mark -2.09 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 15 | $208.93 | $2.07 | $+89.34 | $18,009.00 | ▲ +89.34 after sell → book $10,644.53; vs 09:30 mark -2.07 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,009.00 | ▲ close $10,832.10 vs 09:30 $10,650.92 (session +187.57) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,009.00 | ▼ 09:30 equity $10,804.01 vs yday $10,832.10 (-28.09) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 946 | $1.07 | $12.20 | $+79.46 | $16,984.58 | ▲ +79.46 after sell → book $10,791.81; vs 09:30 mark -12.20 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 87 | $11.75 | $2.25 | $+77.66 | $15,960.08 | ▲ +77.66 after sell → book $10,789.56; vs 09:30 mark -2.25 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,960.08 | ▲ close $10,823.26 vs 09:30 $10,804.01 (session +33.70) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,960.08 | ▼ 09:30 equity $10,768.71 vs yday $10,823.26 (-54.55) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 58 | $18.13 | $2.16 | $+55.94 | $14,906.37 | ▲ +55.94 after sell → book $10,766.54; vs 09:30 mark -2.17 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 730 | $0.96 | $9.22 | $+117.72 | $14,194.16 | ▲ +117.72 after sell → book $10,757.32; vs 09:30 mark -9.22 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 236 | $4.01 | $3.04 | $-113.54 | $13,243.58 | ▼ -113.54 after sell → book $10,754.28; vs 09:30 mark -3.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 26 | $31.87 | $2.07 | $-8.60 | $12,412.89 | ▼ -8.60 after sell → book $10,752.21; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 279 | $2.95 | $3.60 | $+9.46 | $11,586.24 | ▲ +9.46 after sell → book $10,748.61; vs 09:30 mark -3.60 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 123 | $6.81 | $2.36 | $-6.00 | $10,746.25 | ▼ -6.00 after sell → book $10,746.25; vs 09:30 mark -2.36 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 168 | $44.76 | $2.49 | — | $3,224.08 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+8.7; combo leftover $7522.38; owner union_news_pack_h1 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 1 | $204.45 | $2.02 | — | $3,426.51 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $403.01; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 18 | $21.40 | $2.07 | — | $3,809.64 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $403.01; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 90 | $4.43 | $2.30 | — | $4,206.04 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; combo leftover $403.01; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 34 | $11.81 | $2.12 | — | $4,605.63 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $403.01; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 2 | $173.90 | $2.03 | — | $4,951.40 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $403.01; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 8 | $46.85 | $2.04 | — | $5,324.16 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; combo leftover $403.01; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 3 | $106.38 | $2.03 | — | $5,641.27 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $403.01; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 87 | $4.61 | $2.29 | — | $6,040.05 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $403.01; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,040.05 | ▼ close $10,713.40 vs 09:30 $10,768.71 (session -13.46) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,040.05 | ▼ 09:30 equity $10,707.04 vs yday $10,713.40 (-6.36) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 168 | $44.52 | $2.58 | $-45.40 | $13,516.83 | ▼ -45.40 after sell → book $10,704.46; vs 09:30 mark -2.58 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 39 | $119.43 | $2.11 | — | $8,856.95 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $4730.89; owner union_news_pack_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 7 | $623.26 | $2.01 | — | $4,492.12 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list probable,yday_gainer; 🔵; ret5=+1.4; combo leftover $4730.89; owner union_news_pack_h1 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 240 | $3.11 | $3.17 | — | $5,235.36 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $748.69; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 5 | $133.11 | $2.04 | — | $5,898.86 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; combo leftover $748.69; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 8 | $89.10 | $2.05 | — | $6,609.61 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $748.69; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 19 | $38.40 | $2.09 | — | $7,337.12 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $748.69; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 35 | $20.90 | $2.14 | — | $8,066.49 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $748.69; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 27 | $27.00 | $2.11 | — | $8,793.38 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $748.69; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,793.38 | ▲ close $10,917.53 vs 09:30 $10,707.04 (session +230.78) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,793.38 | ▲ 09:30 equity $10,965.93 vs yday $10,917.53 (+48.40) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 39 | $120.51 | $2.15 | $+37.86 | $13,491.11 | ▲ +37.86 after sell → book $10,963.77; vs 09:30 mark -2.16 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 7 | $653.04 | $2.06 | $+204.39 | $18,060.33 | ▲ +204.39 after sell → book $10,961.71; vs 09:30 mark -2.06 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,060.33 | ▲ close $11,032.66 vs 09:30 $10,965.93 (session +70.95) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,060.33 | ▲ 09:30 equity $11,065.92 vs yday $11,032.66 (+33.26) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 1 | $212.00 | $1.99 | $-11.56 | $17,846.34 | ▼ -11.56 after sell → book $11,063.93; vs 09:30 mark -1.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 18 | $20.90 | $2.04 | $+4.88 | $17,468.10 | ▲ +4.88 after sell → book $11,061.89; vs 09:30 mark -2.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 90 | $4.42 | $2.26 | $-3.66 | $17,068.04 | ▼ -3.66 after sell → book $11,059.63; vs 09:30 mark -2.26 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 34 | $11.00 | $2.09 | $+23.49 | $16,691.95 | ▲ +23.49 after sell → book $11,057.54; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 2 | $170.64 | $2.00 | $+2.50 | $16,348.67 | ▲ +2.50 after sell → book $11,055.54; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 8 | $43.63 | $2.01 | $+21.70 | $15,997.62 | ▲ +21.70 after sell → book $11,053.53; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 3 | $105.58 | $2.00 | $-1.63 | $15,678.88 | ▼ -1.63 after sell → book $11,051.53; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 87 | $4.77 | $2.25 | $-18.46 | $15,261.64 | ▼ -18.46 after sell → book $11,049.28; vs 09:30 mark -2.25 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 45 | $118.52 | $2.12 | — | $9,926.11 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; combo leftover $5341.57; owner union_news_pack_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 69 | $77.13 | $2.20 | — | $4,601.94 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; ⚪; ret5=+13.8; combo leftover $5341.57; owner union_news_pack_h1 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 84 | $13.62 | $2.30 | — | $5,744.15 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $1150.49; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 21 | $54.51 | $2.10 | — | $6,886.75 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $1150.49; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 6 | $175.01 | $2.06 | — | $7,934.76 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; combo leftover $1150.49; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 3 | $364.35 | $2.05 | — | $9,025.76 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $1150.49; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,025.76 | ▲ close $11,339.36 vs 09:30 $11,065.92 (session +302.91) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,025.76 | ▼ 09:30 equity $11,321.31 vs yday $11,339.36 (-18.05) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 240 | $2.83 | $3.10 | $+60.94 | $8,343.46 | ▲ +60.94 after sell → book $11,318.21; vs 09:30 mark -3.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 5 | $154.20 | $2.00 | $-109.50 | $7,570.46 | ▼ -109.50 after sell → book $11,316.21; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 8 | $88.24 | $2.01 | $+2.81 | $6,862.52 | ▲ +2.81 after sell → book $11,314.19; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 19 | $38.41 | $2.05 | $-4.32 | $6,130.68 | ▼ -4.32 after sell → book $11,312.14; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 35 | $20.50 | $2.10 | $+9.77 | $5,411.09 | ▲ +9.77 after sell → book $11,310.05; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 27 | $26.00 | $2.07 | $+22.82 | $4,707.02 | ▲ +22.82 after sell → book $11,307.98; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 45 | $119.80 | $2.18 | $+53.30 | $10,095.84 | ▲ +53.30 after sell → book $11,305.80; vs 09:30 mark -2.18 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 69 | $79.34 | $2.25 | $+148.04 | $15,568.05 | ▲ +148.04 after sell → book $11,303.55; vs 09:30 mark -2.25 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 20 | $267.02 | $2.05 | — | $10,225.60 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+16.7; combo leftover $5448.82; owner union_news_pack_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 45 | $118.50 | $2.12 | — | $4,890.97 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; combo leftover $5448.82; owner union_news_pack_h1 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 4 | $213.94 | $2.05 | — | $5,744.69 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $978.19; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 80 | $12.22 | $2.28 | — | $6,720.01 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $978.19; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 192 | $5.08 | $2.63 | — | $7,692.73 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $978.19; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 7 | $132.64 | $2.06 | — | $8,619.16 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $978.19; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 4 | $199.94 | $2.04 | — | $9,416.87 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; combo leftover $978.19; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,416.87 | ▼ close $11,121.18 vs 09:30 $11,321.31 (session -167.13) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,416.87 | ▼ 09:30 equity $11,002.25 vs yday $11,121.18 (-118.93) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 20 | $267.23 | $2.10 | $+0.05 | $14,759.37 | ▲ +0.05 after sell → book $11,000.15; vs 09:30 mark -2.10 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 18 | $81.65 | $2.04 | — | $13,287.63 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+2.0; combo leftover $1475.94; owner union_news_pack_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $12,318.62 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+0.1; combo leftover $1475.94; owner union_news_pack_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 4 | $318.88 | $2.00 | — | $11,041.10 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+1.9; combo leftover $1475.94; owner union_news_pack_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 6 | $222.86 | $2.01 | — | $9,701.93 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=-3.6; combo leftover $1475.94; owner union_news_pack_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 5 | $261.47 | $2.00 | — | $8,392.58 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list overnight,overnight_mega; 🔵; ret5=+1.4; combo leftover $1475.94; owner union_news_pack_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 20 | $70.30 | $2.05 | — | $6,984.53 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=-11.2; combo leftover $1475.94; owner union_news_pack_h1 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 36 | $74.54 | $2.20 | — | $9,665.76 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $2747.01; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 49 | $55.25 | $2.24 | — | $12,370.77 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $2747.01; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,370.77 | ▼ close $10,710.12 vs 09:30 $11,002.25 (session -273.48) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,370.77 | ▼ 09:30 equity $10,606.35 vs yday $10,710.12 (-103.77) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 84 | $13.90 | $2.24 | $-27.64 | $11,200.93 | ▼ -27.64 after sell → book $10,604.11; vs 09:30 mark -2.24 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 21 | $52.49 | $2.05 | $+38.26 | $10,096.59 | ▲ +38.26 after sell → book $10,602.06; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 6 | $172.76 | $2.01 | $+9.44 | $9,058.02 | ▲ +9.44 after sell → book $10,600.05; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 3 | $347.82 | $2.00 | $+45.54 | $8,012.56 | ▲ +45.54 after sell → book $10,598.05; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 45 | $115.66 | $2.18 | $-132.10 | $13,215.08 | ▼ -132.10 after sell → book $10,595.87; vs 09:30 mark -2.18 | union_news_pack_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 18 | $79.27 | $2.07 | $-46.95 | $14,639.88 | ▼ -46.95 after sell → book $10,593.81; vs 09:30 mark -2.06 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $15,557.15 | ▼ -51.73 after sell → book $10,591.79; vs 09:30 mark -2.02 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 4 | $318.03 | $2.02 | $-7.42 | $16,827.25 | ▼ -7.42 after sell → book $10,589.77; vs 09:30 mark -2.02 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 6 | $227.36 | $2.03 | $+22.96 | $18,189.38 | ▲ +22.96 after sell → book $10,587.74; vs 09:30 mark -2.03 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 20 | $65.29 | $2.07 | $-104.32 | $19,493.11 | ▼ -104.32 after sell → book $10,585.67; vs 09:30 mark -2.07 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 8 | $324.41 | $2.01 | — | $16,895.82 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; combo leftover $2729.04; owner union_news_pack_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 6 | $400.42 | $2.01 | — | $14,491.29 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+1.9; combo leftover $2729.04; owner union_news_pack_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 2 | $1306.03 | $2.00 | — | $11,877.24 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+0.0; combo leftover $2729.04; owner union_news_pack_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 11 | $240.22 | $2.02 | — | $9,232.79 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+4.5; combo leftover $2729.04; owner union_news_pack_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 65 | $41.74 | $2.19 | — | $6,517.51 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; ret5=+2.4; combo leftover $2729.04; owner union_news_pack_h1 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 10 | $252.24 | $2.12 | — | $9,037.79 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $2643.86; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 87 | $30.18 | $2.36 | — | $11,661.09 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $2643.86; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,661.09 | ▲ close $10,643.57 vs 09:30 $10,606.35 (session +72.60) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,661.09 | ▲ 09:30 equity $10,753.26 vs yday $10,643.57 (+109.69) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 4 | $208.88 | $2.00 | $+16.19 | $10,823.57 | ▲ +16.19 after sell → book $10,751.26; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 80 | $11.10 | $2.23 | $+85.09 | $9,933.34 | ▲ +85.09 after sell → book $10,749.03; vs 09:30 mark -2.23 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 192 | $4.97 | $2.57 | $+14.96 | $8,975.57 | ▲ +14.96 after sell → book $10,746.47; vs 09:30 mark -2.56 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 7 | $127.45 | $2.01 | $+32.26 | $8,081.41 | ▲ +32.26 after sell → book $10,744.46; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 4 | $254.39 | $2.00 | $-221.85 | $7,061.85 | ▼ -221.85 after sell → book $10,742.45; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 5 | $257.71 | $2.03 | $-22.83 | $8,348.37 | ▼ -22.83 after sell → book $10,740.43; vs 09:30 mark -2.02 | union_news_pack_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 8 | $322.49 | $2.04 | $-19.42 | $10,926.25 | ▼ -19.42 after sell → book $10,738.38; vs 09:30 mark -2.05 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 6 | $378.44 | $2.04 | $-135.92 | $13,194.85 | ▼ -135.92 after sell → book $10,736.35; vs 09:30 mark -2.03 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 2 | $1261.90 | $2.03 | $-92.28 | $15,716.63 | ▼ -92.28 after sell → book $10,734.32; vs 09:30 mark -2.03 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 11 | $233.97 | $2.05 | $-72.88 | $18,288.19 | ▼ -72.88 after sell → book $10,732.27; vs 09:30 mark -2.05 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 65 | $42.00 | $2.22 | $+12.50 | $21,015.97 | ▲ +12.50 after sell → book $10,730.05; vs 09:30 mark -2.22 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,015.97 | ▲ close $10,784.16 vs 09:30 $10,753.26 (session +54.11) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,015.97 | ▲ 09:30 equity $10,941.69 vs yday $10,784.16 (+157.53) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 36 | $73.22 | $2.10 | $+43.22 | $18,377.95 | ▲ +43.22 after sell → book $10,939.59; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 49 | $54.76 | $2.14 | $+19.63 | $15,692.58 | ▲ +19.63 after sell → book $10,937.46; vs 09:30 mark -2.13 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,692.58 | ▲ close $10,952.68 vs 09:30 $10,941.69 (session +15.22) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,692.58 | ▲ 09:30 equity $11,005.62 vs yday $10,952.68 (+52.94) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 10 | $235.71 | $2.02 | $+161.16 | $13,333.46 | ▲ +161.16 after sell → book $11,003.60; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 87 | $26.78 | $2.25 | $+291.19 | $11,001.34 | ▲ +291.19 after sell → book $11,001.34; vs 09:30 mark -2.26 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,001.34 | ▲ close $11,001.34 vs 09:30 $11,005.62 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,001.34 | ▲ 09:30 equity $11,001.34 vs yday $11,001.34 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 5 | $351.74 | $2.00 | — | $9,240.64 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=+3.3; combo leftover $1925.24; owner union_news_pack_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $7,779.71 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+6.1; combo leftover $1925.24; owner union_news_pack_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 40 | $47.60 | $2.11 | — | $5,873.60 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=-6.2; combo leftover $1925.24; owner union_news_pack_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 5 | $354.49 | $2.00 | — | $4,099.15 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=-12.3; combo leftover $1925.24; owner union_news_pack_h1 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 138 | $14.85 | $2.50 | — | $6,145.95 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $2049.57; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1198 | $1.71 | $15.73 | — | $8,178.80 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $2049.57; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,178.80 | ▲ close $11,308.87 vs 09:30 $11,001.34 (session +333.87) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,178.80 | ▲ 09:30 equity $11,357.23 vs yday $11,308.87 (+48.36) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 5 | $359.70 | $2.03 | $+35.77 | $9,975.27 | ▲ +35.77 after sell → book $11,355.20; vs 09:30 mark -2.03 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 3 | $513.78 | $2.02 | $+78.39 | $11,514.59 | ▲ +78.39 after sell → book $11,353.18; vs 09:30 mark -2.02 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 40 | $53.85 | $2.14 | $+245.75 | $13,666.45 | ▲ +245.75 after sell → book $11,351.04; vs 09:30 mark -2.14 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 5 | $321.67 | $2.03 | $-168.13 | $15,272.77 | ▼ -168.13 after sell → book $11,349.01; vs 09:30 mark -2.03 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 13 | $263.36 | $2.03 | — | $11,847.07 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; combo leftover $3563.65; owner union_news_pack_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 15 | $236.82 | $2.04 | — | $8,292.73 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; ret5=+8.1; combo leftover $3563.65; owner union_news_pack_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 25 | $137.35 | $2.06 | — | $4,856.92 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; ret5=+5.4; combo leftover $3563.65; owner union_news_pack_h1 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 520 | $4.67 | $6.88 | — | $7,278.43 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $2428.46; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 31 | $76.55 | $2.18 | — | $9,649.31 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $2428.46; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,649.31 | ▲ close $11,587.57 vs 09:30 $11,357.23 (session +253.74) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,649.31 | ▲ 09:30 equity $11,640.87 vs yday $11,587.57 (+53.30) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 13 | $253.72 | $2.07 | $-129.41 | $12,945.60 | ▼ -129.41 after sell → book $11,638.80; vs 09:30 mark -2.07 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 15 | $267.76 | $2.08 | $+459.99 | $16,959.93 | ▲ +459.99 after sell → book $11,636.73; vs 09:30 mark -2.07 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,959.93 | ▲ close $11,830.95 vs 09:30 $11,640.87 (session +194.22) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,959.93 | ▲ 09:30 equity $11,990.95 vs yday $11,830.95 (+160.00) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 138 | $13.60 | $2.40 | $+167.60 | $15,080.72 | ▲ +167.60 after sell → book $11,988.54; vs 09:30 mark -2.41 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1198 | $1.58 | $15.45 | $+124.56 | $13,172.43 | ▲ +124.56 after sell → book $11,973.09; vs 09:30 mark -15.45 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 25 | $141.82 | $2.10 | $+107.58 | $16,715.82 | ▲ +107.58 after sell → book $11,970.98; vs 09:30 mark -2.11 | union_news_pack_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,715.82 | ▲ close $11,995.26 vs 09:30 $11,990.95 (session +24.28) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,715.82 | ▲ 09:30 equity $12,068.13 vs yday $11,995.26 (+72.87) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 520 | $4.36 | $6.71 | $+147.61 | $14,441.92 | ▲ +147.61 after sell → book $12,061.43; vs 09:30 mark -6.70 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 31 | $76.79 | $2.08 | $-11.70 | $12,059.34 | ▼ -11.70 after sell → book $12,059.34; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,059.34 | ▲ close $12,059.34 vs 09:30 $12,068.13 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,059.34 | ▲ 09:30 equity $12,059.34 vs yday $12,059.34 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 34 | $242.17 | $2.09 | — | $3,823.47 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; ret5=-11.1; combo leftover $8441.54; owner union_news_pack_h1 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 6 | $112.83 | $2.05 | — | $4,498.43 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $764.69; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 217 | $3.52 | $2.87 | — | $5,259.41 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $764.69; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 376 | $2.03 | $4.94 | — | $6,017.74 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $764.69; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 30 | $24.97 | $2.12 | — | $6,764.72 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $764.69; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 226 | $3.37 | $2.98 | — | $7,523.36 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $764.69; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,523.36 | ▲ close $12,378.99 vs 09:30 $12,059.34 (session +336.70) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,523.36 | ▲ 09:30 equity $12,716.57 vs yday $12,378.99 (+337.58) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 34 | $261.51 | $2.17 | $+653.29 | $16,412.53 | ▲ +653.29 after sell → book $12,714.40; vs 09:30 mark -2.17 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,412.53 | ▼ close $12,614.16 vs 09:30 $12,716.57 (session -100.24) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,412.53 | ▲ 09:30 equity $12,614.27 vs yday $12,614.16 (+0.11) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,412.53 | ▼ close $12,552.72 vs 09:30 $12,614.27 (session -61.55) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,412.53 | ▲ 09:30 equity $12,564.09 vs yday $12,552.72 (+11.37) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 6 | $118.18 | $2.01 | $-36.12 | $15,701.44 | ▼ -36.12 after sell → book $12,562.08; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 217 | $3.98 | $2.80 | $-105.49 | $14,834.98 | ▼ -105.49 after sell → book $12,559.28; vs 09:30 mark -2.80 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 376 | $1.85 | $4.85 | $+57.89 | $14,134.53 | ▲ +57.89 after sell → book $12,554.43; vs 09:30 mark -4.85 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 30 | $24.42 | $2.08 | $+12.30 | $13,399.85 | ▲ +12.30 after sell → book $12,552.35; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 226 | $3.75 | $2.92 | $-91.78 | $12,549.43 | ▼ -91.78 after sell → book $12,549.43; vs 09:30 mark -2.92 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 23 | $189.17 | $2.06 | — | $8,196.46 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+7.9; combo leftover $4392.30; owner union_news_pack_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 109 | $39.99 | $2.32 | — | $3,835.24 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+9.3; combo leftover $4392.30; owner union_news_pack_h1 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 103 | $18.61 | $2.38 | — | $5,749.68 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $1917.62; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 280 | $6.83 | $3.73 | — | $7,658.36 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $1917.62; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,658.36 | ▼ close $11,967.38 vs 09:30 $12,564.09 (session -571.57) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,658.36 | ▲ 09:30 equity $12,003.76 vs yday $11,967.38 (+36.38) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 23 | $190.35 | $2.10 | $+22.98 | $12,034.30 | ▲ +22.98 after sell → book $12,001.65; vs 09:30 mark -2.11 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 109 | $37.57 | $2.37 | $-268.46 | $16,127.07 | ▼ -268.46 after sell → book $11,999.29; vs 09:30 mark -2.36 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 12 | $934.88 | $2.03 | — | $4,906.48 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list yday_gainer; ret5=-7.0; combo leftover $11288.95; owner union_news_pack_h1 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 308 | $7.95 | $4.11 | — | $7,350.97 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $2453.24; owner short_news_r_h3 | — |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 30 | $81.00 | $2.18 | — | $9,778.79 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-3.0; combo leftover $2453.24; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,778.79 | ▼ close $11,664.34 vs 09:30 $12,003.76 (session -326.63) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,778.79 | ▲ 09:30 equity $11,948.31 vs yday $11,664.34 (+283.97) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 12 | $915.66 | $2.12 | $-234.79 | $20,764.59 | ▼ -234.79 after sell → book $11,946.19; vs 09:30 mark -2.12 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 173 | $34.44 | $2.75 | — | $26,719.96 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5973.09; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26,719.96 | ▲ close $12,149.28 vs 09:30 $11,948.31 (session +205.84) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26,719.96 | ▼ 09:30 equity $11,950.67 vs yday $12,149.28 (-198.61) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 103 | $22.11 | $2.30 | $-365.18 | $24,440.33 | ▼ -365.18 after sell → book $11,948.37; vs 09:30 mark -2.30 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 280 | $6.55 | $3.61 | $+71.06 | $22,602.72 | ▲ +71.06 after sell → book $11,944.76; vs 09:30 mark -3.61 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 361 | $8.26 | $4.82 | — | $25,579.76 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $2986.19; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 5 | $583.88 | $2.12 | — | $28,497.04 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $2986.19; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28,497.04 | ▼ close $11,635.19 vs 09:30 $11,950.67 (session -302.63) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28,497.04 | ▲ 09:30 equity $11,672.24 vs yday $11,635.19 (+37.05) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 308 | $8.28 | $3.97 | $-108.18 | $25,944.37 | ▼ -108.18 after sell → book $11,668.27; vs 09:30 mark -3.97 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 31 | $93.97 | $2.20 | — | $28,855.24 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $2917.07; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28,855.24 | ▼ close $11,581.31 vs 09:30 $11,672.24 (session -84.76) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28,855.24 | ▼ 09:30 equity $10,893.28 vs yday $11,581.31 (-688.03) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `LEN` | 30 | $82.00 | $2.08 | $-34.26 | $26,393.16 | ▼ -34.26 after sell → book $10,891.20; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 93 | $196.78 | $2.27 | — | $8,090.35 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $18475.21; owner union_news_pack_h1 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 46 | $116.85 | $2.33 | — | $13,463.12 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $5444.47; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,463.12 | ▼ close $10,669.54 vs 09:30 $10,893.28 (session -217.06) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,463.12 | ▲ 09:30 equity $10,806.28 vs yday $10,669.54 (+136.74) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 5 | $600.27 | $2.00 | $-86.07 | $10,459.77 | ▼ -86.07 after sell → book $10,804.28; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 93 | $192.26 | $2.43 | $-425.06 | $28,337.52 | ▼ -425.06 after sell → book $10,801.85; vs 09:30 mark -2.43 | union_news_pack_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28,337.52 | ▼ close $10,552.24 vs 09:30 $10,806.28 (session -249.61) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,653.73 | ▼ 09:30 equity $8,201.45 vs yday $8,228.04 (-26.59) | 09:30 open · cash $18,653.73 (unchanged overnight, no fees) · equity $8,201.45 vs prior close $8,228.04 (-26.59) | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 14 | $887.00 | $2.03 | — | $6,233.70 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=+0.3; combo leftover $13057.61; owner union_news_pack_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 522 | $7.85 | $6.97 | — | $10,324.43 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $4099.71; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,324.43 | ▲ close $8,671.13 vs 09:30 $8,201.45 (session +478.68) | 16:00 close · cash $10,324.43 · equity $8,671.13 vs 09:30 $8,201.45 (+469.68; session marks +478.68) · 7 name(s) marked open→close (per-name table). AEHL×269 09:30 $9.05 → close $9.36 -83.39; BAND×35 09:30 $61.83 → close $61.83 -0.00; HALO×17 09:30 $115.36 → close $113.90 +24.82; PAYX×18 09:30 $101.59 → close $101.59 +0.00; USFD×22 09:30 $93.82 → close $93.82 +0.00; COST×14 09:30 $887.00 → close $922.76 +500.71; RSKD×522 09:30 $7.85 → close $7.78 +36.54 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `EU` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `LUNR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `EU` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `LUNR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new long union_news_pack_h1 |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new long union_news_pack_h1 |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new long union_news_pack_h1 |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `RNW` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-21 | `AEM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WYFI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TOYO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AAP` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AEM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WYFI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TOYO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AAP` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `QTRX` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `MRNA` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AUGO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `SSRM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `ARIS` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new long union_news_pack_h1 |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new long union_news_pack_h1 |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-25 | `QTRX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `MRNA` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `AUGO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `ARIS` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `NOG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-26 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BMO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `INTU` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BMO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `NEM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `CRM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `ASML` | cash | leftover split 1475.94 < 1 share @ 1746.53 |
| 2026-08-28 | `BE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `NEM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `CRM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `MT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TX` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `MT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `TX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new long union_news_pack_h1 |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new long union_news_pack_h1 |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new long union_news_pack_h1 |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new long union_news_pack_h1 |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new long union_news_pack_h1 |
| 2026-09-01 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new long union_news_pack_h1 |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new long union_news_pack_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new long union_news_pack_h1 |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new long union_news_pack_h1 |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new long union_news_pack_h1 |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-04 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `OPK` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new long union_news_pack_h1 |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new long union_news_pack_h1 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-09 | `GSM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new long union_news_pack_h1 |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new short short_news_r_h3 |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new long union_news_pack_h1 |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new long union_news_pack_h1 |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new long union_news_pack_h1 |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-14 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new long union_news_pack_h1 |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new long union_news_pack_h1 |
| 2026-09-15 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `MYGN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-17 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `BBNX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `LEN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-21 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `LEN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `FIVN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `LEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-23 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-23 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-24 | `HALO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new long union_news_pack_h1 |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new long union_news_pack_h1 |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new long union_news_pack_h1 |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new long union_news_pack_h1 |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FIVN` | 173 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5973.09; owner short_news_r_h3 |
| `AEHL` | 361 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $2986.19; owner short_news_r_h3 |
| `USFD` | 31 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $2917.07; owner short_news_r_h3 |
| `HALO` | 46 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $5444.47; owner short_news_r_h3 |
