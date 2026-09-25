# Factor mine action — `combo_sn_3070_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared short_news_r_h3/union_news_g_h1 w=0.3,0.7 net=priority

Cash book **-26.45%** ($7,355) · signal-only (no cash/fees) was —. Starts YES **0/30**. Fills 302 · skips 196 · realized $-422.24.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: short_news_r_h3 30%, union_news_g_h1 70%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: short_news_r_h3 30%, union_news_g_h1 70%.
- Member: short_news_r_h3 (30% · short · hold 3).
- Member: union_news_g_h1 (70% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,047.38.

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
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 2 | $359.83 | $2.00 | — | $9,278.34 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; combo leftover $875.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 5 | $146.90 | $2.00 | — | $8,541.84 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; combo leftover $875.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 7 | $120.00 | $2.01 | — | $7,699.83 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; combo leftover $875.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 203 | $4.31 | $2.62 | — | $6,822.28 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; combo leftover $875.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 44 | $19.57 | $2.12 | — | $5,959.08 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $875.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 64 | $13.55 | $2.18 | — | $5,089.70 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $875.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 66 | $13.18 | $2.19 | — | $4,217.63 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; combo leftover $875.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1191 | $1.18 | $15.61 | — | $5,607.40 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $1405.88; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 73 | $19.17 | $2.27 | — | $7,004.54 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $1405.88; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 110 | $12.70 | $2.39 | — | $8,398.60 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $1405.88; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,398.60 | ▲ close $10,081.12 vs 09:30 $10,000.00 (session +116.51) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,398.60 | ▼ 09:30 equity $10,071.01 vs yday $10,081.12 (-10.11) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 2 | $367.88 | $2.02 | $+12.09 | $9,132.34 | ▲ +12.09 after sell → book $10,068.99; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 5 | $149.37 | $2.02 | $+8.32 | $9,877.17 | ▲ +8.32 after sell → book $10,066.97; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 7 | $127.40 | $2.03 | $+47.76 | $10,766.94 | ▲ +47.76 after sell → book $10,064.94; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 203 | $4.60 | $2.66 | $+53.59 | $11,698.07 | ▲ +53.59 after sell → book $10,062.27; vs 09:30 mark -2.67 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 44 | $19.57 | $2.14 | $-4.26 | $12,557.01 | ▼ -4.26 after sell → book $10,060.13; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 64 | $13.16 | $2.20 | $-29.34 | $13,397.05 | ▼ -29.34 after sell → book $10,057.93; vs 09:30 mark -2.20 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 66 | $13.84 | $2.21 | $+39.16 | $14,308.28 | ▲ +39.16 after sell → book $10,055.72; vs 09:30 mark -2.21 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 43 | $46.18 | $2.12 | — | $12,320.42 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; combo leftover $2003.16; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 14 | $142.77 | $2.03 | — | $10,319.61 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; combo leftover $2003.16; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 9 | $202.70 | $2.02 | — | $8,493.29 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; combo leftover $2003.16; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 21 | $92.99 | $2.05 | — | $6,538.45 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; combo leftover $2003.16; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 40 | $49.00 | $2.11 | — | $4,576.34 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; combo leftover $2003.16; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 795 | $1.15 | $10.42 | — | $5,480.17 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $915.27; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 257 | $3.56 | $3.39 | — | $6,391.69 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $915.27; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 28 | $31.70 | $2.12 | — | $7,277.17 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; combo leftover $915.27; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 304 | $3.01 | $4.01 | — | $8,188.21 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $915.27; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 134 | $6.80 | $2.45 | — | $9,096.96 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; combo leftover $915.27; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,096.96 | ▲ close $10,222.13 vs 09:30 $10,071.01 (session +199.12) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,096.96 | ▲ 09:30 equity $10,299.25 vs yday $10,222.13 (+77.12) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 43 | $48.00 | $2.15 | $+74.00 | $11,158.81 | ▲ +74.00 after sell → book $10,297.10; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 14 | $148.04 | $2.06 | $+69.69 | $13,229.31 | ▲ +69.69 after sell → book $10,295.04; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 9 | $208.93 | $2.04 | $+52.01 | $15,107.64 | ▲ +52.01 after sell → book $10,293.00; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 21 | $92.38 | $2.08 | $-16.94 | $17,045.54 | ▼ -16.94 after sell → book $10,290.92; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 40 | $45.09 | $2.13 | $-160.64 | $18,847.01 | ▼ -160.64 after sell → book $10,288.79; vs 09:30 mark -2.13 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,847.01 | ▲ close $10,501.64 vs 09:30 $10,299.25 (session +212.86) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,847.01 | ▼ 09:30 equity $10,471.96 vs yday $10,501.64 (-29.68) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 1191 | $1.07 | $15.36 | $+100.03 | $17,557.28 | ▲ +100.03 after sell → book $10,456.60; vs 09:30 mark -15.36 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 110 | $11.75 | $2.32 | $+99.24 | $16,262.46 | ▲ +99.24 after sell → book $10,454.28; vs 09:30 mark -2.32 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,262.46 | ▲ close $10,495.47 vs 09:30 $10,471.96 (session +41.19) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,262.46 | ▼ 09:30 equity $10,439.83 vs yday $10,495.47 (-55.64) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 73 | $18.13 | $2.21 | $+71.44 | $14,936.76 | ▲ +71.44 after sell → book $10,437.62; vs 09:30 mark -2.21 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 795 | $0.96 | $10.04 | $+128.20 | $14,161.13 | ▲ +128.20 after sell → book $10,427.58; vs 09:30 mark -10.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 257 | $4.01 | $3.32 | $-123.64 | $13,125.96 | ▼ -123.64 after sell → book $10,424.26; vs 09:30 mark -3.32 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 28 | $31.87 | $2.07 | $-8.95 | $12,231.53 | ▼ -8.95 after sell → book $10,422.19; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 304 | $2.95 | $3.92 | $+10.31 | $11,330.80 | ▲ +10.31 after sell → book $10,418.26; vs 09:30 mark -3.93 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 134 | $6.81 | $2.39 | $-6.18 | $10,415.87 | ▼ -6.18 after sell → book $10,415.87; vs 09:30 mark -2.39 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 10 | $91.01 | $2.02 | — | $9,503.75 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; combo leftover $911.39; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 6 | $150.14 | $2.01 | — | $8,600.90 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $911.39; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1289 | $0.71 | $12.98 | — | $7,676.60 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; combo leftover $911.39; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 137 | $6.61 | $2.40 | — | $6,769.32 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; combo leftover $911.39; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 56 | $16.00 | $2.16 | — | $5,871.16 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; combo leftover $911.39; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 34 | $26.57 | $2.09 | — | $4,965.69 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; combo leftover $911.39; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 15 | $58.73 | $2.04 | — | $4,082.70 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; combo leftover $911.39; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 20 | $44.76 | $2.05 | — | $3,185.45 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; combo leftover $911.39; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 1 | $204.45 | $2.02 | — | $3,387.88 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $398.18; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 18 | $21.40 | $2.07 | — | $3,771.01 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $398.18; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 89 | $4.43 | $2.29 | — | $4,162.98 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; combo leftover $398.18; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 33 | $11.81 | $2.12 | — | $4,550.76 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $398.18; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 2 | $173.90 | $2.03 | — | $4,896.53 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $398.18; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 8 | $46.85 | $2.04 | — | $5,269.29 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; combo leftover $398.18; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 3 | $106.38 | $2.03 | — | $5,586.40 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $398.18; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 86 | $4.61 | $2.28 | — | $5,980.58 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $398.18; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,980.58 | ▼ close $10,282.77 vs 09:30 $10,439.83 (session -88.47) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,980.58 | ▲ 09:30 equity $10,433.46 vs yday $10,282.77 (+150.69) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 10 | $95.72 | $2.04 | $+43.04 | $6,935.74 | ▲ +43.04 after sell → book $10,431.42; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 6 | $133.11 | $2.03 | $-106.22 | $7,732.37 | ▼ -106.22 after sell → book $10,429.39; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1289 | $0.67 | $12.78 | $-68.30 | $8,588.38 | ▼ -68.30 after sell → book $10,416.61; vs 09:30 mark -12.78 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 137 | $6.95 | $2.43 | $+42.43 | $9,538.09 | ▲ +42.43 after sell → book $10,414.17; vs 09:30 mark -2.44 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 56 | $17.66 | $2.18 | $+88.62 | $10,524.88 | ▲ +88.62 after sell → book $10,412.00; vs 09:30 mark -2.17 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 34 | $26.25 | $2.11 | $-15.08 | $11,415.26 | ▼ -15.08 after sell → book $10,409.88; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 20 | $44.52 | $2.07 | $-8.92 | $12,303.59 | ▼ -8.92 after sell → book $10,407.81; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $11,107.27 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $1230.36; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 498 | $2.47 | $6.42 | — | $9,870.79 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; combo leftover $1230.36; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $8,716.97 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $1230.36; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $8,091.72 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.4; combo leftover $1230.36; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 105 | $11.70 | $2.31 | — | $6,860.91 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; combo leftover $1230.36; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 110 | $11.10 | $2.32 | — | $5,638.14 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; combo leftover $1230.36; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 379 | $3.24 | $4.89 | — | $4,405.29 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; combo leftover $1230.36; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 236 | $3.11 | $3.11 | — | $5,136.14 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $734.22; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 5 | $133.11 | $2.04 | — | $5,799.65 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; combo leftover $734.22; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 8 | $89.10 | $2.05 | — | $6,510.39 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $734.22; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 19 | $38.40 | $2.09 | — | $7,237.91 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $734.22; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 35 | $20.90 | $2.14 | — | $7,967.27 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $734.22; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 27 | $27.00 | $2.11 | — | $8,694.16 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $734.22; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,694.16 | ▼ close $10,361.32 vs 09:30 $10,433.46 (session -10.98) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,694.16 | ▼ 09:30 equity $10,341.17 vs yday $10,361.32 (-20.15) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $9,897.22 | ▲ +6.74 after sell → book $10,339.13; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 498 | $2.40 | $6.52 | $-47.80 | $11,085.90 | ▼ -47.80 after sell → book $10,332.61; vs 09:30 mark -6.52 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $121.00 | $2.04 | $+54.14 | $12,293.86 | ▲ +54.14 after sell → book $10,330.57; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 1 | $653.04 | $2.01 | $+25.77 | $12,944.89 | ▲ +25.77 after sell → book $10,328.56; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 105 | $11.17 | $2.33 | $-60.29 | $14,115.41 | ▼ -60.29 after sell → book $10,326.23; vs 09:30 mark -2.33 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 110 | $11.48 | $2.35 | $+37.68 | $15,375.86 | ▲ +37.68 after sell → book $10,323.88; vs 09:30 mark -2.35 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 379 | $2.99 | $4.96 | $-104.60 | $16,504.11 | ▼ -104.60 after sell → book $10,318.92; vs 09:30 mark -4.96 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,504.11 | ▲ close $10,363.55 vs 09:30 $10,341.17 (session +44.64) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,504.11 | ▲ 09:30 equity $10,410.04 vs yday $10,363.55 (+46.49) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 15 | $57.93 | $2.06 | $-16.09 | $17,371.00 | ▼ -16.09 after sell → book $10,407.98; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 1 | $212.00 | $1.99 | $-11.56 | $17,157.01 | ▼ -11.56 after sell → book $10,405.99; vs 09:30 mark -1.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 18 | $20.90 | $2.04 | $+4.88 | $16,778.77 | ▲ +4.88 after sell → book $10,403.95; vs 09:30 mark -2.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 89 | $4.42 | $2.26 | $-3.66 | $16,383.13 | ▼ -3.66 after sell → book $10,401.69; vs 09:30 mark -2.26 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 33 | $11.00 | $2.09 | $+22.69 | $16,018.04 | ▲ +22.69 after sell → book $10,399.60; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 2 | $170.64 | $2.00 | $+2.50 | $15,674.76 | ▲ +2.50 after sell → book $10,397.60; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 8 | $43.63 | $2.01 | $+21.70 | $15,323.71 | ▲ +21.70 after sell → book $10,395.59; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 3 | $105.58 | $2.00 | $-1.63 | $15,004.97 | ▼ -1.63 after sell → book $10,393.59; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 86 | $4.77 | $2.25 | $-18.29 | $14,592.50 | ▼ -18.29 after sell → book $10,391.34; vs 09:30 mark -2.25 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 154 | $9.42 | $2.45 | — | $13,139.37 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; combo leftover $1459.25; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 41 | $35.05 | $2.11 | — | $11,700.21 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $1459.25; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 60 | $24.11 | $2.17 | — | $10,251.44 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=+891.7; combo leftover $1459.25; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 50 | $28.86 | $2.14 | — | $8,806.30 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.7; combo leftover $1459.25; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 167 | $8.72 | $2.49 | — | $7,347.57 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.0; combo leftover $1459.25; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 12 | $118.52 | $2.03 | — | $5,923.30 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+21.7; combo leftover $1459.25; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 18 | $77.13 | $2.04 | — | $4,532.92 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; ⚪; ret5=+13.8; combo leftover $1459.25; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 83 | $13.62 | $2.29 | — | $5,661.50 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $1133.23; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 20 | $54.51 | $2.10 | — | $6,749.60 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $1133.23; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 6 | $175.01 | $2.06 | — | $7,797.60 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; combo leftover $1133.23; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 3 | $364.35 | $2.05 | — | $8,888.60 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $1133.23; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,888.60 | ▲ close $10,730.55 vs 09:30 $10,410.04 (session +363.14) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,888.60 | ▲ 09:30 equity $10,730.85 vs yday $10,730.55 (+0.30) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 236 | $2.83 | $3.04 | $+59.92 | $8,217.68 | ▲ +59.92 after sell → book $10,727.81; vs 09:30 mark -3.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 5 | $154.20 | $2.00 | $-109.50 | $7,444.67 | ▼ -109.50 after sell → book $10,725.80; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 8 | $88.24 | $2.01 | $+2.81 | $6,736.74 | ▲ +2.81 after sell → book $10,723.79; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 19 | $38.41 | $2.05 | $-4.32 | $6,004.90 | ▼ -4.32 after sell → book $10,721.74; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 35 | $20.50 | $2.10 | $+9.77 | $5,285.31 | ▲ +9.77 after sell → book $10,719.65; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 27 | $26.00 | $2.07 | $+22.82 | $4,581.24 | ▲ +22.82 after sell → book $10,717.58; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 154 | $10.07 | $2.49 | $+95.16 | $6,129.53 | ▲ +95.16 after sell → book $10,715.09; vs 09:30 mark -2.49 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 41 | $35.70 | $2.13 | $+22.40 | $7,591.09 | ▲ +22.40 after sell → book $10,712.95; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 60 | $26.61 | $2.19 | $+145.64 | $9,185.50 | ▲ +145.64 after sell → book $10,710.76; vs 09:30 mark -2.19 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 50 | $27.56 | $2.16 | $-69.30 | $10,561.34 | ▼ -69.30 after sell → book $10,708.60; vs 09:30 mark -2.16 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 167 | $8.86 | $2.53 | $+18.36 | $12,038.43 | ▲ +18.36 after sell → book $10,706.07; vs 09:30 mark -2.53 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 12 | $119.80 | $2.05 | $+11.29 | $13,473.98 | ▲ +11.29 after sell → book $10,704.02; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 18 | $79.34 | $2.07 | $+35.67 | $14,900.03 | ▲ +35.67 after sell → book $10,701.95; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 156 | $11.12 | $2.46 | — | $13,162.86 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; combo leftover $1738.34; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 209 | $8.29 | $2.70 | — | $11,427.55 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; combo leftover $1738.34; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 99 | $17.41 | $2.29 | — | $9,701.67 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-9.2; combo leftover $1738.34; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 154 | $11.22 | $2.45 | — | $7,971.34 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.8; combo leftover $1738.34; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 6 | $267.02 | $2.01 | — | $6,367.21 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.7; combo leftover $1738.34; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 14 | $118.50 | $2.03 | — | $4,706.18 | — | union ∩ news_g, no 🚨; gate news=good; list overnight,overnight_mega; 🔵; ret5=-2.7; combo leftover $1738.34; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 4 | $213.94 | $2.05 | — | $5,559.90 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $941.24; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 77 | $12.22 | $2.27 | — | $6,498.57 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $941.24; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 185 | $5.08 | $2.61 | — | $7,435.75 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $941.24; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 7 | $132.64 | $2.06 | — | $8,362.18 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $941.24; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 4 | $199.94 | $2.04 | — | $9,159.89 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; combo leftover $941.24; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,159.89 | ▲ close $10,788.17 vs 09:30 $10,730.85 (session +111.18) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,159.89 | ▼ 09:30 equity $10,675.48 vs yday $10,788.17 (-112.69) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 6 | $267.23 | $2.03 | $-2.78 | $10,761.24 | ▼ -2.78 after sell → book $10,673.45; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 60 | $41.44 | $2.17 | — | $8,272.67 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+3.1; combo leftover $2510.96; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 35 | $70.30 | $2.10 | — | $5,810.08 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=-11.2; combo leftover $2510.96; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `SRRK` | 41 | $60.00 | $2.11 | — | $3,347.97 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+6.2; combo leftover $2510.96; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 22 | $74.54 | $2.12 | — | $4,985.72 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $1673.98; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 30 | $55.25 | $2.15 | — | $6,641.07 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $1673.98; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,641.07 | ▼ close $10,554.48 vs 09:30 $10,675.48 (session -108.32) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,641.07 | ▼ 09:30 equity $10,391.42 vs yday $10,554.48 (-163.06) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 83 | $13.90 | $2.24 | $-27.36 | $5,485.13 | ▼ -27.36 after sell → book $10,389.18; vs 09:30 mark -2.24 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 20 | $52.49 | $2.05 | $+36.25 | $4,433.28 | ▲ +36.25 after sell → book $10,387.13; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 6 | $172.76 | $2.01 | $+9.44 | $3,394.72 | ▲ +9.44 after sell → book $10,385.12; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 3 | $347.82 | $2.00 | $+45.54 | $2,349.26 | ▲ +45.54 after sell → book $10,383.12; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 156 | $11.27 | $2.50 | $+18.44 | $4,104.88 | ▲ +18.44 after sell → book $10,380.62; vs 09:30 mark -2.50 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWRD` | 99 | $17.70 | $2.32 | $+24.11 | $5,854.86 | ▲ +24.11 after sell → book $10,378.31; vs 09:30 mark -2.31 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TRLV` | 154 | $11.00 | $2.49 | $-38.82 | $7,546.37 | ▼ -38.82 after sell → book $10,375.81; vs 09:30 mark -2.50 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 14 | $115.66 | $2.06 | $-43.85 | $9,163.55 | ▼ -43.85 after sell → book $10,373.76; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 35 | $65.29 | $2.12 | $-179.57 | $11,446.58 | ▼ -179.57 after sell → book $10,371.64; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SRRK` | 41 | $58.75 | $2.14 | $-55.51 | $13,853.19 | ▼ -55.51 after sell → book $10,369.49; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 49 | $32.90 | $2.14 | — | $12,238.95 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; combo leftover $1616.21; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 187 | $8.61 | $2.55 | — | $10,626.33 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.7; combo leftover $1616.21; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 11 | $141.76 | $2.02 | — | $9,064.95 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; combo leftover $1616.21; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 83 | $19.25 | $2.24 | — | $7,464.96 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+14.1; combo leftover $1616.21; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 86 | $18.75 | $2.25 | — | $5,850.21 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-5.0; combo leftover $1616.21; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 55 | $28.91 | $2.15 | — | $4,258.01 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+9.2; combo leftover $1616.21; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 8 | $252.24 | $2.10 | — | $6,273.83 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $2129.00; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 70 | $30.18 | $2.29 | — | $8,384.14 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $2129.00; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,384.14 | ▼ close $10,324.26 vs 09:30 $10,391.42 (session -27.49) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,384.14 | ▲ 09:30 equity $10,406.08 vs yday $10,324.26 (+81.82) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 209 | $9.50 | $2.75 | $+247.45 | $10,366.90 | ▲ +247.45 after sell → book $10,403.33; vs 09:30 mark -2.75 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 4 | $208.88 | $2.00 | $+16.19 | $9,529.38 | ▲ +16.19 after sell → book $10,401.33; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 77 | $11.10 | $2.22 | $+81.75 | $8,672.45 | ▲ +81.75 after sell → book $10,399.11; vs 09:30 mark -2.22 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 185 | $4.97 | $2.54 | $+14.27 | $7,749.53 | ▲ +14.27 after sell → book $10,396.56; vs 09:30 mark -2.55 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 7 | $127.45 | $2.01 | $+32.26 | $6,855.37 | ▲ +32.26 after sell → book $10,394.55; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 4 | $254.39 | $2.00 | $-221.85 | $5,835.81 | ▼ -221.85 after sell → book $10,392.55; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 60 | $42.00 | $2.20 | $+29.23 | $8,353.61 | ▲ +29.23 after sell → book $10,390.35; vs 09:30 mark -2.20 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 49 | $31.15 | $2.16 | $-90.05 | $9,877.80 | ▼ -90.05 after sell → book $10,388.19; vs 09:30 mark -2.16 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 187 | $8.52 | $2.59 | $-21.98 | $11,468.45 | ▼ -21.98 after sell → book $10,385.60; vs 09:30 mark -2.59 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 11 | $132.30 | $2.04 | $-108.13 | $12,921.70 | ▼ -108.13 after sell → book $10,383.55; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 83 | $17.87 | $2.26 | $-119.04 | $14,402.65 | ▼ -119.04 after sell → book $10,381.29; vs 09:30 mark -2.26 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 86 | $19.25 | $2.28 | $+38.48 | $16,055.87 | ▲ +38.48 after sell → book $10,379.01; vs 09:30 mark -2.28 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 55 | $28.06 | $2.18 | $-51.08 | $17,597.00 | ▼ -51.08 after sell → book $10,376.84; vs 09:30 mark -2.17 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,597.00 | ▲ close $10,412.18 vs 09:30 $10,406.08 (session +35.34) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,597.00 | ▲ 09:30 equity $10,528.44 vs yday $10,412.18 (+116.26) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 22 | $73.22 | $2.06 | $+24.86 | $15,984.10 | ▲ +24.86 after sell → book $10,526.38; vs 09:30 mark -2.06 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 30 | $54.76 | $2.08 | $+10.47 | $14,339.22 | ▲ +10.47 after sell → book $10,524.30; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,339.22 | ▲ close $10,536.42 vs 09:30 $10,528.44 (session +12.12) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,339.22 | ▲ 09:30 equity $10,578.94 vs yday $10,536.42 (+42.52) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 8 | $235.71 | $2.01 | $+128.13 | $12,451.53 | ▲ +128.13 after sell → book $10,576.93; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 70 | $26.78 | $2.20 | $+233.51 | $10,574.73 | ▲ +233.51 after sell → book $10,574.73; vs 09:30 mark -2.20 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,574.73 | ▲ close $10,574.73 vs 09:30 $10,578.94 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,574.73 | ▲ 09:30 equity $10,574.73 vs yday $10,574.73 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 38 | $23.88 | $2.10 | — | $9,665.18 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $925.29; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 28 | $32.88 | $2.07 | — | $8,742.47 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+16.2; combo leftover $925.29; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 121 | $7.59 | $2.35 | — | $7,821.72 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.5; combo leftover $925.29; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $7,116.48 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.1; combo leftover $925.29; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 58 | $15.87 | $2.16 | — | $6,193.86 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $925.29; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 2 | $351.74 | $2.00 | — | $5,488.38 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+3.3; combo leftover $925.29; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 2 | $354.49 | $2.00 | — | $4,777.41 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-12.3; combo leftover $925.29; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 19 | $47.60 | $2.05 | — | $3,870.96 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; combo leftover $925.29; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 130 | $14.85 | $2.47 | — | $5,798.99 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $1935.48; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1131 | $1.71 | $14.85 | — | $7,718.15 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $1935.48; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,718.15 | ▲ close $10,797.43 vs 09:30 $10,574.73 (session +256.75) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,718.15 | ▲ 09:30 equity $10,805.03 vs yday $10,797.43 (+7.60) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 38 | $23.84 | $2.12 | $-5.75 | $8,621.95 | ▼ -5.75 after sell → book $10,802.91; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 28 | $32.48 | $2.09 | $-15.37 | $9,529.29 | ▼ -15.37 after sell → book $10,800.81; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 121 | $7.79 | $2.38 | $+19.46 | $10,469.50 | ▲ +19.46 after sell → book $10,798.43; vs 09:30 mark -2.38 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $11,159.52 | ▼ -15.23 after sell → book $10,796.42; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 2 | $359.70 | $2.02 | $+11.91 | $11,876.90 | ▲ +11.91 after sell → book $10,794.40; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 2 | $321.67 | $2.02 | $-69.65 | $12,518.23 | ▼ -69.65 after sell → book $10,792.39; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 19 | $53.85 | $2.07 | $+114.64 | $13,539.31 | ▲ +114.64 after sell → book $10,790.32; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 7 | $263.36 | $2.01 | — | $11,693.78 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; combo leftover $1895.50; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 977 | $1.94 | $12.60 | — | $9,785.80 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+18.3; combo leftover $1895.50; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 13 | $137.35 | $2.03 | — | $7,998.22 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+5.4; combo leftover $1895.50; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 8 | $236.82 | $2.01 | — | $6,101.64 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.1; combo leftover $1895.50; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 25 | $75.65 | $2.06 | — | $4,208.33 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+6.0; combo leftover $1895.50; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 450 | $4.67 | $5.95 | — | $6,303.87 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $2104.16; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 27 | $76.55 | $2.15 | — | $8,368.57 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $2104.16; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,368.57 | ▲ close $10,878.68 vs 09:30 $10,805.03 (session +117.19) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,368.57 | ▲ 09:30 equity $11,011.54 vs yday $10,878.68 (+132.86) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 58 | $16.74 | $2.18 | $+46.11 | $9,337.30 | ▲ +46.11 after sell → book $11,009.35; vs 09:30 mark -2.19 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 7 | $253.72 | $2.04 | $-71.53 | $11,111.31 | ▼ -71.53 after sell → book $11,007.32; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 977 | $1.94 | $12.78 | $-25.38 | $12,993.91 | ▼ -25.38 after sell → book $10,994.54; vs 09:30 mark -12.78 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 8 | $267.76 | $2.04 | $+243.46 | $15,133.95 | ▲ +243.46 after sell → book $10,992.50; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,133.95 | ▲ close $11,126.29 vs 09:30 $11,011.54 (session +133.79) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,133.95 | ▲ 09:30 equity $11,218.15 vs yday $11,126.29 (+91.86) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 130 | $13.60 | $2.38 | $+157.65 | $13,363.57 | ▲ +157.65 after sell → book $11,215.77; vs 09:30 mark -2.38 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1131 | $1.58 | $14.59 | $+117.59 | $11,562.00 | ▲ +117.59 after sell → book $11,201.18; vs 09:30 mark -14.59 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 13 | $141.82 | $2.05 | $+54.03 | $13,403.60 | ▲ +54.03 after sell → book $11,199.12; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 25 | $76.60 | $2.09 | $+19.59 | $15,316.51 | ▲ +19.59 after sell → book $11,197.03; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,316.51 | ▲ close $11,218.09 vs 09:30 $11,218.15 (session +21.06) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,316.51 | ▲ 09:30 equity $11,281.18 vs yday $11,218.09 (+63.09) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 450 | $4.36 | $5.80 | $+127.74 | $13,348.71 | ▲ +127.74 after sell → book $11,275.38; vs 09:30 mark -5.80 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 27 | $76.79 | $2.07 | $-10.71 | $11,273.31 | ▼ -10.71 after sell → book $11,273.31; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,273.31 | ▲ close $11,273.31 vs 09:30 $11,281.18 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,273.31 | ▲ 09:30 equity $11,273.31 vs yday $11,273.31 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $10,120.29 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $1315.22; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 644 | $2.04 | $8.31 | — | $8,798.22 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; combo leftover $1315.22; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 620 | $2.12 | $8.00 | — | $7,475.82 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; combo leftover $1315.22; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 87 | $15.01 | $2.25 | — | $6,167.70 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $1315.22; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 5 | $242.17 | $2.00 | — | $4,954.85 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-11.1; combo leftover $1315.22; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 9 | $135.71 | $2.02 | — | $3,731.44 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-9.2; combo leftover $1315.22; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 6 | $112.83 | $2.05 | — | $4,406.40 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $746.29; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 212 | $3.52 | $2.80 | — | $5,149.84 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $746.29; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 367 | $2.03 | $4.83 | — | $5,890.03 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $746.29; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 29 | $24.97 | $2.12 | — | $6,612.04 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $746.29; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 221 | $3.37 | $2.92 | — | $7,353.89 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $746.29; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,353.89 | ▼ close $11,102.99 vs 09:30 $11,273.31 (session -131.03) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,353.89 | ▼ 09:30 equity $11,071.86 vs yday $11,102.99 (-31.13) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 7 | $141.42 | $2.03 | $-165.11 | $8,341.80 | ▼ -165.11 after sell → book $11,069.83; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 644 | $2.01 | $8.42 | $-36.05 | $9,627.82 | ▼ -36.05 after sell → book $11,061.41; vs 09:30 mark -8.42 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 620 | $2.05 | $8.11 | $-59.51 | $10,890.70 | ▼ -59.51 after sell → book $11,053.29; vs 09:30 mark -8.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 5 | $261.51 | $2.03 | $+92.67 | $12,196.23 | ▲ +92.67 after sell → book $11,051.27; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 9 | $131.40 | $2.04 | $-42.84 | $13,376.79 | ▼ -42.84 after sell → book $11,049.23; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,376.79 | ▼ close $10,977.17 vs 09:30 $11,071.86 (session -72.06) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,376.79 | ▲ 09:30 equity $10,982.56 vs yday $10,977.17 (+5.39) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,376.79 | ▼ close $10,940.21 vs 09:30 $10,982.56 (session -42.35) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,376.79 | ▲ 09:30 equity $10,959.18 vs yday $10,940.21 (+18.97) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 6 | $118.18 | $2.01 | $-36.12 | $12,665.70 | ▼ -36.12 after sell → book $10,957.17; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 212 | $3.98 | $2.73 | $-103.06 | $11,819.21 | ▼ -103.06 after sell → book $10,954.44; vs 09:30 mark -2.73 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 367 | $1.85 | $4.73 | $+56.50 | $11,135.52 | ▲ +56.50 after sell → book $10,949.70; vs 09:30 mark -4.74 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 29 | $24.42 | $2.08 | $+11.76 | $10,425.27 | ▲ +11.76 after sell → book $10,947.63; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 221 | $3.75 | $2.85 | $-89.75 | $9,593.67 | ▼ -89.75 after sell → book $10,944.78; vs 09:30 mark -2.85 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 63 | $26.27 | $2.18 | — | $7,936.48 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+10.0; combo leftover $1678.89; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 241 | $6.95 | $3.11 | — | $6,258.42 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-5.8; combo leftover $1678.89; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 41 | $39.99 | $2.11 | — | $4,616.72 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+9.3; combo leftover $1678.89; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 8 | $189.17 | $2.01 | — | $3,101.34 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+7.9; combo leftover $1678.89; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 83 | $18.61 | $2.31 | — | $4,643.66 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $1550.67; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 227 | $6.83 | $3.02 | — | $6,191.05 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $1550.67; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,191.05 | ▼ close $10,650.04 vs 09:30 $10,959.18 (session -279.99) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,191.05 | ▲ 09:30 equity $10,716.75 vs yday $10,650.04 (+66.71) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 63 | $26.51 | $2.20 | $+10.74 | $7,858.98 | ▲ +10.74 after sell → book $10,714.55; vs 09:30 mark -2.20 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 241 | $7.27 | $3.16 | $+70.85 | $9,607.89 | ▲ +70.85 after sell → book $10,711.39; vs 09:30 mark -3.16 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 41 | $37.57 | $2.14 | $-103.47 | $11,146.12 | ▼ -103.47 after sell → book $10,709.25; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 8 | $190.35 | $2.04 | $+5.39 | $12,666.89 | ▲ +5.39 after sell → book $10,707.22; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 8 | $170.85 | $2.01 | — | $11,298.07 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; combo leftover $1477.80; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 1 | $934.88 | $1.99 | — | $10,361.20 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-7.0; combo leftover $1477.80; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 83 | $17.72 | $2.24 | — | $8,888.20 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=-8.3; combo leftover $1477.80; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 834 | $1.77 | $10.76 | — | $7,401.26 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-10.2; combo leftover $1477.80; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 6 | $238.60 | $2.01 | — | $5,967.65 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.6; combo leftover $1477.80; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 66 | $22.12 | $2.19 | — | $4,505.55 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+10.5; combo leftover $1477.80; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 283 | $7.95 | $3.78 | — | $6,751.62 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $2252.77; owner short_news_r_h3 | — |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 27 | $81.00 | $2.16 | — | $8,936.46 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-3.0; combo leftover $2252.77; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,936.46 | ▲ close $10,857.64 vs 09:30 $10,716.75 (session +177.56) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,936.46 | ▲ 09:30 equity $10,912.14 vs yday $10,857.64 (+54.50) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 87 | $15.87 | $2.28 | $+70.29 | $10,314.87 | ▲ +70.29 after sell → book $10,909.86; vs 09:30 mark -2.28 | union_news_g_h1: dropped from list after 5 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 8 | $182.33 | $2.04 | $+87.79 | $11,771.48 | ▲ +87.79 after sell → book $10,907.83; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 1 | $915.66 | $2.01 | $-23.23 | $12,685.12 | ▼ -23.23 after sell → book $10,905.81; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 83 | $17.13 | $2.26 | $-53.47 | $14,104.65 | ▼ -53.47 after sell → book $10,903.55; vs 09:30 mark -2.26 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 834 | $1.77 | $10.91 | $-21.67 | $15,569.92 | ▼ -21.67 after sell → book $10,892.64; vs 09:30 mark -10.91 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 6 | $236.80 | $2.03 | $-14.84 | $16,988.69 | ▼ -14.84 after sell → book $10,890.61; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 211 | $14.07 | $2.72 | — | $14,017.20 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; combo leftover $2973.02; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 201 | $14.79 | $2.60 | — | $11,041.81 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; combo leftover $2973.02; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 394 | $7.54 | $5.08 | — | $8,067.94 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-20.9; combo leftover $2973.02; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 142 | $20.91 | $2.42 | — | $5,096.30 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+8.6; combo leftover $2973.02; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 147 | $34.44 | $2.63 | — | $10,156.35 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5096.30; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,156.35 | ▼ close $10,833.84 vs 09:30 $10,912.14 (session -41.32) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,156.35 | ▼ 09:30 equity $10,820.70 vs yday $10,833.84 (-13.14) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 83 | $22.11 | $2.24 | $-295.05 | $8,318.98 | ▼ -295.05 after sell → book $10,818.46; vs 09:30 mark -2.24 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 227 | $6.55 | $2.93 | $+57.61 | $6,829.20 | ▲ +57.61 after sell → book $10,815.53; vs 09:30 mark -2.93 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 66 | $22.78 | $2.21 | $+39.16 | $8,330.47 | ▲ +39.16 after sell → book $10,813.32; vs 09:30 mark -2.21 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 211 | $13.90 | $2.78 | $-41.37 | $11,260.59 | ▼ -41.37 after sell → book $10,810.54; vs 09:30 mark -2.78 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 201 | $14.58 | $2.65 | $-47.46 | $14,188.52 | ▼ -47.46 after sell → book $10,807.89; vs 09:30 mark -2.65 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 394 | $7.36 | $5.17 | $-79.20 | $17,083.18 | ▼ -79.20 after sell → book $10,802.71; vs 09:30 mark -5.18 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 142 | $21.65 | $2.46 | $+100.20 | $20,155.02 | ▲ +100.20 after sell → book $10,800.25; vs 09:30 mark -2.46 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 90 | $25.95 | $2.26 | — | $17,817.26 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.2; combo leftover $2351.42; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 1093 | $2.15 | $14.10 | — | $15,453.21 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+7.5; combo leftover $2351.42; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 168 | $13.94 | $2.49 | — | $13,108.80 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; combo leftover $2351.42; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 391 | $6.00 | $5.04 | — | $10,757.75 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-24.1; combo leftover $2351.42; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 12 | $190.30 | $2.03 | — | $8,472.13 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+10.6; combo leftover $2351.42; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 10 | $230.25 | $2.02 | — | $6,167.61 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+12.5; combo leftover $2351.42; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 326 | $8.26 | $4.35 | — | $8,856.01 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $2693.08; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 4 | $583.88 | $2.09 | — | $11,189.44 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $2693.08; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,189.44 | ▼ close $10,163.21 vs 09:30 $10,820.70 (session -602.65) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,189.44 | ▼ 09:30 equity $10,162.82 vs yday $10,163.21 (-0.39) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 283 | $8.28 | $3.65 | $-99.40 | $8,843.96 | ▼ -99.40 after sell → book $10,159.17; vs 09:30 mark -3.65 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `MARA` | 168 | $13.13 | $2.54 | $-141.11 | $11,047.26 | ▼ -141.11 after sell → book $10,156.63; vs 09:30 mark -2.54 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 391 | $5.99 | $5.13 | $-14.08 | $13,384.22 | ▼ -14.08 after sell → book $10,151.50; vs 09:30 mark -5.13 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 1546 | $1.01 | $19.94 | — | $11,802.82 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+14.3; combo leftover $1561.49; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `MRNA` | 9 | $168.50 | $2.02 | — | $10,284.30 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+17.9; combo leftover $1561.49; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 363 | $4.30 | $4.68 | — | $8,718.72 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; combo leftover $1561.49; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 26 | $93.97 | $2.16 | — | $11,159.78 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $2531.22; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,159.78 | ▼ close $10,081.32 vs 09:30 $10,162.82 (session -41.38) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,159.78 | ▼ 09:30 equity $9,929.22 vs yday $10,081.32 (-152.10) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `LEN` | 27 | $82.00 | $2.07 | $-31.23 | $8,943.71 | ▼ -31.23 after sell → book $9,927.15; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 90 | $26.58 | $2.29 | $+52.15 | $11,333.61 | ▲ +52.15 after sell → book $9,924.85; vs 09:30 mark -2.30 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMTX` | 1093 | $2.09 | $14.30 | $-93.98 | $13,603.68 | ▼ -93.98 after sell → book $9,910.55; vs 09:30 mark -14.30 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 12 | $174.50 | $2.05 | $-193.68 | $15,695.63 | ▼ -193.68 after sell → book $9,908.50; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 10 | $266.50 | $2.05 | $+358.43 | $18,358.58 | ▲ +358.43 after sell → book $9,906.45; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 1546 | $0.95 | $19.59 | $-132.30 | $19,807.68 | ▼ -132.30 after sell → book $9,886.85; vs 09:30 mark -19.60 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MRNA` | 9 | $183.41 | $2.04 | $+130.09 | $21,456.29 | ▲ +130.09 after sell → book $9,884.81; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 314 | $7.95 | $4.05 | — | $18,955.94 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ⚪; ret5=+12.4; combo leftover $2503.23; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 159 | $15.72 | $2.47 | — | $16,453.99 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; combo leftover $2503.23; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 2051 | $1.22 | $26.46 | — | $13,925.31 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-33.0; combo leftover $2503.23; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 1925 | $1.30 | $24.83 | — | $11,397.98 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; combo leftover $2503.23; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 62 | $40.00 | $2.18 | — | $8,915.81 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+6.7; combo leftover $2503.23; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 12 | $196.78 | $2.03 | — | $6,552.42 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $2503.23; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 42 | $116.85 | $2.30 | — | $11,457.82 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $4911.40; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,457.82 | ▼ close $9,469.22 vs 09:30 $9,929.22 (session -351.28) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,457.82 | ▼ 09:30 equity $9,389.19 vs yday $9,469.22 (-80.03) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 4 | $600.27 | $2.00 | $-69.66 | $9,054.74 | ▼ -69.66 after sell → book $9,387.19; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DGXX` | 363 | $4.12 | $4.75 | $-74.78 | $10,545.54 | ▼ -74.78 after sell → book $9,382.43; vs 09:30 mark -4.76 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 314 | $7.38 | $4.12 | $-187.15 | $12,858.74 | ▼ -187.15 after sell → book $9,378.31; vs 09:30 mark -4.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 159 | $14.38 | $2.51 | $-218.04 | $15,142.65 | ▼ -218.04 after sell → book $9,375.80; vs 09:30 mark -2.51 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CMPX` | 2051 | $1.17 | $26.82 | $-155.83 | $17,515.50 | ▼ -155.83 after sell → book $9,348.98; vs 09:30 mark -26.82 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 1925 | $1.27 | $25.17 | $-107.75 | $19,935.08 | ▼ -107.75 after sell → book $9,323.81; vs 09:30 mark -25.17 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 62 | $39.27 | $2.21 | $-49.64 | $22,367.61 | ▼ -49.64 after sell → book $9,321.60; vs 09:30 mark -2.21 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 12 | $192.26 | $2.05 | $-58.32 | $24,672.68 | ▼ -58.32 after sell → book $9,319.55; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,672.68 | ▼ close $9,084.14 vs 09:30 $9,389.19 (session -235.41) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,190.96 | ▼ 09:30 equity $7,452.89 vs yday $7,480.70 (-27.81) | 09:30 open · cash $18,190.96 (unchanged overnight, no fees) · equity $7,452.89 vs prior close $7,480.70 (-27.81) | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 659 | $3.86 | $8.50 | — | $15,638.72 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; combo leftover $2546.73; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 157 | $16.21 | $2.46 | — | $13,091.29 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $2546.73; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 9 | $272.16 | $2.02 | — | $10,639.83 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+11.7; combo leftover $2546.73; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 34 | $74.15 | $2.09 | — | $8,116.64 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.5; combo leftover $2546.73; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 2 | $887.00 | $2.00 | — | $6,340.64 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+0.3; combo leftover $2546.73; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 473 | $7.85 | $6.31 | — | $10,047.38 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $3717.91; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,047.38 | ▼ close $7,355.11 vs 09:30 $7,452.89 (session -74.40) | 16:00 close · cash $10,047.38 · equity $7,355.11 vs 09:30 $7,452.89 (-97.78; session marks -74.40) · 11 name(s) marked open→close (per-name table). AEHL×281 09:30 $9.05 → close $9.36 -87.11; BAND×36 09:30 $61.83 → close $61.83 -0.00; HALO×18 09:30 $115.36 → close $113.90 +26.28; PAYX×18 09:30 $101.59 → close $101.59 +0.00; USFD×22 09:30 $93.82 → close $93.82 +0.00; ZSQR×659 09:30 $3.86 → close $3.78 -52.72; SECZ×157 09:30 $16.21 → close $15.96 -39.25; ILMN×9 09:30 $272.16 → close $270.00 -19.44; RKLB×34 09:30 $74.15 → close $73.95 -6.80; COST×2 09:30 $887.00 → close $922.76 +71.53; RSKD×473 09:30 $7.85 → close $7.78 +33.11 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 875.00 < 1 share @ 1646.93 |
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
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `RNW` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
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
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
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
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-09-01 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-04 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `OPK` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-09 | `GSM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new short short_news_r_h3 |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new long union_news_g_h1 |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new long union_news_g_h1 |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new long union_news_g_h1 |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-14 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new long union_news_g_h1 |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new long union_news_g_h1 |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new long union_news_g_h1 |
| 2026-09-15 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `MYGN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new long union_news_g_h1 |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new long union_news_g_h1 |
| 2026-09-17 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `BBNX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `LEN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-21 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `LEN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `FIVN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `LEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMTX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `YSS` | no_price | no 09:30 open |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-23 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-23 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-24 | `HALO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `ZSQR` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FIVN` | 147 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5096.30; owner short_news_r_h3 |
| `AEHL` | 326 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $2693.08; owner short_news_r_h3 |
| `USFD` | 26 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $2531.22; owner short_news_r_h3 |
| `HALO` | 42 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $4911.40; owner short_news_r_h3 |
