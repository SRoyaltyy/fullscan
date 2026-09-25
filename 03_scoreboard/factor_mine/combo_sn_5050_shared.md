# Factor mine action — `combo_sn_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared short_news_r_h3/union_news_g_h1 w=0.5,0.5 net=priority

Cash book **-22.69%** ($7,731) · signal-only (no cash/fees) was —. Starts YES **0/30**. Fills 300 · skips 197 · realized $-335.14.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: short_news_r_h3 50%, union_news_g_h1 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: short_news_r_h3 50%, union_news_g_h1 50%.
- Member: short_news_r_h3 (50% · short · hold 3).
- Member: union_news_g_h1 (50% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $13,527.19.

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
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 1 | $359.83 | $1.99 | — | $9,638.18 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; combo leftover $625.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 4 | $146.90 | $2.00 | — | $9,048.57 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; combo leftover $625.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 5 | $120.00 | $2.00 | — | $8,446.57 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; combo leftover $625.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 145 | $4.31 | $2.42 | — | $7,819.19 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; combo leftover $625.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 31 | $19.57 | $2.08 | — | $7,210.44 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $625.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 46 | $13.55 | $2.13 | — | $6,585.01 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $625.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 47 | $13.18 | $2.13 | — | $5,963.42 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; combo leftover $625.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1410 | $1.18 | $18.48 | — | $7,608.74 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $1664.21; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 86 | $19.17 | $2.32 | — | $9,255.04 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $1664.21; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 131 | $12.70 | $2.46 | — | $10,915.62 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $1664.21; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,915.62 | ▲ close $10,057.77 vs 09:30 $10,000.00 (session +95.80) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,915.62 | ▼ 09:30 equity $10,012.17 vs yday $10,057.77 (-45.60) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 1 | $367.88 | $2.01 | $+4.04 | $11,281.49 | ▲ +4.04 after sell → book $10,010.16; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 4 | $149.37 | $2.02 | $+5.86 | $11,876.95 | ▲ +5.86 after sell → book $10,008.14; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 5 | $127.40 | $2.02 | $+32.97 | $12,511.92 | ▲ +32.97 after sell → book $10,006.11; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 145 | $4.60 | $2.46 | $+37.17 | $13,176.46 | ▲ +37.17 after sell → book $10,003.65; vs 09:30 mark -2.46 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 31 | $19.57 | $2.10 | $-4.19 | $13,781.03 | ▼ -4.19 after sell → book $10,001.55; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 46 | $13.16 | $2.15 | $-22.22 | $14,384.24 | ▼ -22.22 after sell → book $9,999.40; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 47 | $13.84 | $2.15 | $+26.74 | $15,032.57 | ▲ +26.74 after sell → book $9,997.25; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 32 | $46.18 | $2.09 | — | $13,552.72 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; combo leftover $1503.26; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 10 | $142.77 | $2.02 | — | $12,123.00 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; combo leftover $1503.26; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 7 | $202.70 | $2.01 | — | $10,702.09 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; combo leftover $1503.26; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 16 | $92.99 | $2.04 | — | $9,212.22 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; combo leftover $1503.26; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 30 | $49.00 | $2.08 | — | $7,740.14 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; combo leftover $1503.26; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 868 | $1.15 | $11.38 | — | $8,726.96 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $998.70; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 280 | $3.56 | $3.70 | — | $9,720.06 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $998.70; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 31 | $31.70 | $2.13 | — | $10,700.63 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; combo leftover $998.70; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 331 | $3.01 | $4.36 | — | $11,692.58 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $998.70; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 146 | $6.80 | $2.49 | — | $12,682.89 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; combo leftover $998.70; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,682.89 | ▲ close $10,160.04 vs 09:30 $10,012.17 (session +197.08) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,682.89 | ▲ 09:30 equity $10,270.19 vs yday $10,160.04 (+110.15) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 32 | $48.00 | $2.11 | $+54.05 | $14,216.78 | ▲ +54.05 after sell → book $10,268.08; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 10 | $148.04 | $2.04 | $+48.64 | $15,695.14 | ▲ +48.64 after sell → book $10,266.04; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 7 | $208.93 | $2.03 | $+39.57 | $17,155.62 | ▲ +39.57 after sell → book $10,264.01; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 16 | $92.38 | $2.06 | $-13.86 | $18,631.64 | ▼ -13.86 after sell → book $10,261.95; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 30 | $45.09 | $2.10 | $-121.48 | $19,982.24 | ▼ -121.48 after sell → book $10,259.85; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,982.24 | ▲ close $10,498.52 vs 09:30 $10,270.19 (session +238.67) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,982.24 | ▼ 09:30 equity $10,466.36 vs yday $10,498.52 (-32.16) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 1410 | $1.07 | $18.19 | $+118.43 | $18,455.35 | ▲ +118.43 after sell → book $10,448.17; vs 09:30 mark -18.19 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 131 | $11.75 | $2.38 | $+118.95 | $16,913.71 | ▲ +118.95 after sell → book $10,445.78; vs 09:30 mark -2.39 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,913.71 | ▲ close $10,493.76 vs 09:30 $10,466.36 (session +47.97) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,913.71 | ▼ 09:30 equity $10,435.77 vs yday $10,493.76 (-57.99) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 86 | $18.13 | $2.25 | $+84.87 | $15,352.29 | ▲ +84.87 after sell → book $10,433.52; vs 09:30 mark -2.25 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 868 | $0.96 | $10.96 | $+139.97 | $14,505.44 | ▲ +139.97 after sell → book $10,422.56; vs 09:30 mark -10.96 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 280 | $4.01 | $3.61 | $-134.71 | $13,377.63 | ▼ -134.71 after sell → book $10,418.95; vs 09:30 mark -3.61 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 31 | $31.87 | $2.08 | $-9.48 | $12,387.57 | ▼ -9.48 after sell → book $10,416.86; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 331 | $2.95 | $4.27 | $+11.23 | $11,406.85 | ▲ +11.23 after sell → book $10,412.59; vs 09:30 mark -4.27 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 146 | $6.81 | $2.43 | $-6.38 | $10,410.17 | ▼ -6.38 after sell → book $10,410.17; vs 09:30 mark -2.42 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 7 | $91.01 | $2.01 | — | $9,771.09 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; combo leftover $650.64; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 4 | $150.14 | $2.00 | — | $9,168.52 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $650.64; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 920 | $0.71 | $9.26 | — | $8,508.82 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; combo leftover $650.64; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 98 | $6.61 | $2.28 | — | $7,859.25 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; combo leftover $650.64; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 40 | $16.00 | $2.11 | — | $7,217.14 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; combo leftover $650.64; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 24 | $26.57 | $2.06 | — | $6,577.39 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; combo leftover $650.64; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 11 | $58.73 | $2.02 | — | $5,929.34 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; combo leftover $650.64; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 14 | $44.76 | $2.03 | — | $5,300.67 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; combo leftover $650.64; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $5,911.98 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $649.15; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 30 | $21.40 | $2.12 | — | $6,551.86 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $649.15; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 146 | $4.43 | $2.48 | — | $7,196.16 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; combo leftover $649.15; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 54 | $11.81 | $2.19 | — | $7,831.99 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $649.15; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $8,351.65 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $649.15; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $8,958.64 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; combo leftover $649.15; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 6 | $106.38 | $2.05 | — | $9,594.87 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $649.15; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 140 | $4.61 | $2.46 | — | $10,237.81 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $649.15; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,237.81 | ▼ close $10,347.25 vs 09:30 $10,435.77 (session -21.70) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,237.81 | ▲ 09:30 equity $10,423.18 vs yday $10,347.25 (+75.93) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 7 | $95.72 | $2.03 | $+28.93 | $10,905.82 | ▲ +28.93 after sell → book $10,421.15; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 4 | $133.11 | $2.02 | $-72.14 | $11,436.24 | ▼ -72.14 after sell → book $10,419.13; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 920 | $0.67 | $9.12 | $-48.75 | $12,047.19 | ▼ -48.75 after sell → book $10,410.00; vs 09:30 mark -9.13 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 98 | $6.95 | $2.31 | $+29.22 | $12,725.98 | ▲ +29.22 after sell → book $10,407.69; vs 09:30 mark -2.31 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 40 | $17.66 | $2.13 | $+62.16 | $13,430.25 | ▲ +62.16 after sell → book $10,405.56; vs 09:30 mark -2.13 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 24 | $26.25 | $2.08 | $-11.82 | $14,058.17 | ▼ -11.82 after sell → book $10,403.48; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 14 | $44.52 | $2.05 | $-7.44 | $14,679.40 | ▼ -7.44 after sell → book $10,401.43; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 8 | $119.43 | $2.01 | — | $13,721.95 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $1048.53; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 424 | $2.47 | $5.47 | — | $12,669.20 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; combo leftover $1048.53; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 9 | $115.18 | $2.02 | — | $11,630.56 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $1048.53; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $11,005.31 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.4; combo leftover $1048.53; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 89 | $11.70 | $2.26 | — | $9,961.75 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; combo leftover $1048.53; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 94 | $11.10 | $2.27 | — | $8,916.55 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; combo leftover $1048.53; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 323 | $3.24 | $4.17 | — | $7,865.86 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; combo leftover $1048.53; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 278 | $3.11 | $3.67 | — | $8,726.77 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $865.10; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 6 | $133.11 | $2.05 | — | $9,523.38 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; combo leftover $865.10; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 9 | $89.10 | $2.06 | — | $10,323.23 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $865.10; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 22 | $38.40 | $2.10 | — | $11,165.93 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $865.10; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 41 | $20.90 | $2.16 | — | $12,020.67 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $865.10; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 32 | $27.00 | $2.13 | — | $12,882.54 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $865.10; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,882.54 | ▼ close $10,361.71 vs 09:30 $10,423.18 (session -5.37) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,882.54 | ▲ 09:30 equity $10,377.01 vs yday $10,361.71 (+15.30) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 8 | $120.51 | $2.03 | $+4.59 | $13,844.59 | ▲ +4.59 after sell → book $10,374.98; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 424 | $2.40 | $5.55 | $-40.70 | $14,856.64 | ▼ -40.70 after sell → book $10,369.43; vs 09:30 mark -5.55 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 9 | $121.00 | $2.04 | $+48.33 | $15,943.60 | ▲ +48.33 after sell → book $10,367.39; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 1 | $653.04 | $2.01 | $+25.77 | $16,594.63 | ▲ +25.77 after sell → book $10,365.38; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 89 | $11.17 | $2.28 | $-51.71 | $17,586.47 | ▼ -51.71 after sell → book $10,363.09; vs 09:30 mark -2.29 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 94 | $11.48 | $2.30 | $+31.62 | $18,663.30 | ▲ +31.62 after sell → book $10,360.80; vs 09:30 mark -2.29 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 323 | $2.99 | $4.23 | $-89.15 | $19,624.84 | ▼ -89.15 after sell → book $10,356.57; vs 09:30 mark -4.23 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,624.84 | ▲ close $10,411.47 vs 09:30 $10,377.01 (session +54.91) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,624.84 | ▲ 09:30 equity $10,465.07 vs yday $10,411.47 (+53.60) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 11 | $57.93 | $2.04 | $-12.87 | $20,260.02 | ▼ -12.87 after sell → book $10,463.02; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $212.00 | $2.00 | $-26.68 | $19,622.02 | ▼ -26.68 after sell → book $10,461.02; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 30 | $20.90 | $2.08 | $+10.80 | $18,992.94 | ▲ +10.80 after sell → book $10,458.94; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 146 | $4.42 | $2.43 | $-3.45 | $18,345.20 | ▼ -3.45 after sell → book $10,456.52; vs 09:30 mark -2.42 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 54 | $11.00 | $2.15 | $+39.67 | $17,749.04 | ▲ +39.67 after sell → book $10,454.36; vs 09:30 mark -2.16 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 3 | $170.64 | $2.00 | $+5.75 | $17,235.13 | ▲ +5.75 after sell → book $10,452.37; vs 09:30 mark -1.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 13 | $43.63 | $2.03 | $+37.77 | $16,665.91 | ▲ +37.77 after sell → book $10,450.34; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 6 | $105.58 | $2.01 | $+0.75 | $16,030.42 | ▲ +0.75 after sell → book $10,448.33; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 140 | $4.77 | $2.41 | $-27.27 | $15,360.21 | ▼ -27.27 after sell → book $10,445.92; vs 09:30 mark -2.41 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 116 | $9.42 | $2.34 | — | $14,265.15 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; combo leftover $1097.16; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 31 | $35.05 | $2.08 | — | $13,176.52 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $1097.16; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 45 | $24.11 | $2.12 | — | $12,089.44 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=+891.7; combo leftover $1097.16; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 38 | $28.86 | $2.10 | — | $10,990.66 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.7; combo leftover $1097.16; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 125 | $8.72 | $2.37 | — | $9,898.29 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.0; combo leftover $1097.16; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 9 | $118.52 | $2.02 | — | $8,829.60 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+21.7; combo leftover $1097.16; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 14 | $77.13 | $2.03 | — | $7,747.74 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; ⚪; ret5=+13.8; combo leftover $1097.16; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 95 | $13.62 | $2.34 | — | $9,039.78 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $1303.86; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 23 | $54.51 | $2.11 | — | $10,291.40 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $1303.86; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 7 | $175.01 | $2.06 | — | $11,514.40 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; combo leftover $1303.86; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 3 | $364.35 | $2.05 | — | $12,605.41 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $1303.86; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,605.41 | ▲ close $10,644.93 vs 09:30 $10,465.07 (session +222.64) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,605.41 | ▲ 09:30 equity $10,704.39 vs yday $10,644.93 (+59.46) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 278 | $2.83 | $3.59 | $+70.59 | $11,815.08 | ▲ +70.59 after sell → book $10,700.80; vs 09:30 mark -3.59 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 6 | $154.20 | $2.01 | $-130.60 | $10,887.87 | ▼ -130.60 after sell → book $10,698.79; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 9 | $88.24 | $2.02 | $+3.66 | $10,091.69 | ▲ +3.66 after sell → book $10,696.77; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 22 | $38.41 | $2.06 | $-4.38 | $9,244.62 | ▼ -4.38 after sell → book $10,694.72; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 41 | $20.50 | $2.11 | $+12.13 | $8,402.01 | ▲ +12.13 after sell → book $10,692.61; vs 09:30 mark -2.11 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 32 | $26.00 | $2.09 | $+27.78 | $7,567.92 | ▲ +27.78 after sell → book $10,690.52; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 116 | $10.07 | $2.37 | $+70.69 | $8,733.67 | ▲ +70.69 after sell → book $10,688.15; vs 09:30 mark -2.37 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 31 | $35.70 | $2.10 | $+15.96 | $9,838.27 | ▲ +15.96 after sell → book $10,686.05; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 45 | $26.61 | $2.15 | $+108.23 | $11,033.57 | ▲ +108.23 after sell → book $10,683.90; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 38 | $27.56 | $2.12 | $-53.63 | $12,078.73 | ▼ -53.63 after sell → book $10,681.78; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 125 | $8.86 | $2.40 | $+12.74 | $13,183.83 | ▲ +12.74 after sell → book $10,679.38; vs 09:30 mark -2.40 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 9 | $119.80 | $2.04 | $+7.47 | $14,260.00 | ▲ +7.47 after sell → book $10,677.35; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 14 | $79.34 | $2.05 | $+26.86 | $15,368.71 | ▲ +26.86 after sell → book $10,675.30; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 115 | $11.12 | $2.33 | — | $14,087.57 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; combo leftover $1280.73; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 154 | $8.29 | $2.45 | — | $12,808.46 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; combo leftover $1280.73; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 73 | $17.41 | $2.21 | — | $11,535.32 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-9.2; combo leftover $1280.73; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 114 | $11.22 | $2.33 | — | $10,253.91 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.8; combo leftover $1280.73; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 4 | $267.02 | $2.00 | — | $9,183.83 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.7; combo leftover $1280.73; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 10 | $118.50 | $2.02 | — | $7,996.81 | — | union ∩ news_g, no 🚨; gate news=good; list overnight,overnight_mega; 🔵; ret5=-2.7; combo leftover $1280.73; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 4 | $213.94 | $2.05 | — | $8,850.52 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $1066.19; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 87 | $12.22 | $2.30 | — | $9,911.36 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $1066.19; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 209 | $5.08 | $2.77 | — | $10,970.31 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $1066.19; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 8 | $132.64 | $2.06 | — | $12,029.36 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $1066.19; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 5 | $199.94 | $2.05 | — | $13,027.01 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; combo leftover $1066.19; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,027.01 | ▲ close $10,678.86 vs 09:30 $10,704.39 (session +28.15) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,027.01 | ▼ 09:30 equity $10,535.92 vs yday $10,678.86 (-142.94) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 4 | $267.23 | $2.02 | $-3.18 | $14,093.91 | ▼ -3.18 after sell → book $10,533.90; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 56 | $41.44 | $2.16 | — | $11,771.11 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+3.1; combo leftover $2348.98; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 33 | $70.30 | $2.09 | — | $9,449.12 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=-11.2; combo leftover $2348.98; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `SRRK` | 39 | $60.00 | $2.11 | — | $7,107.01 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+6.2; combo leftover $2348.98; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 35 | $74.54 | $2.20 | — | $9,713.72 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $2631.89; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 47 | $55.25 | $2.23 | — | $12,308.23 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $2631.89; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,308.23 | ▼ close $10,373.74 vs 09:30 $10,535.92 (session -149.37) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,308.23 | ▼ 09:30 equity $10,226.38 vs yday $10,373.74 (-147.36) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 95 | $13.90 | $2.27 | $-30.74 | $10,985.46 | ▼ -30.74 after sell → book $10,224.10; vs 09:30 mark -2.28 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 23 | $52.49 | $2.06 | $+42.29 | $9,776.13 | ▲ +42.29 after sell → book $10,222.04; vs 09:30 mark -2.06 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 7 | $172.76 | $2.01 | $+11.67 | $8,564.80 | ▲ +11.67 after sell → book $10,220.03; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 3 | $347.82 | $2.00 | $+45.54 | $7,519.34 | ▲ +45.54 after sell → book $10,218.03; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 115 | $11.27 | $2.36 | $+12.55 | $8,813.03 | ▲ +12.55 after sell → book $10,215.67; vs 09:30 mark -2.36 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWRD` | 73 | $17.70 | $2.23 | $+16.73 | $10,102.89 | ▲ +16.73 after sell → book $10,213.44; vs 09:30 mark -2.23 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TRLV` | 114 | $11.00 | $2.36 | $-29.77 | $11,354.53 | ▼ -29.77 after sell → book $10,211.08; vs 09:30 mark -2.36 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 10 | $115.66 | $2.04 | $-32.46 | $12,509.09 | ▼ -32.46 after sell → book $10,209.04; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 33 | $65.29 | $2.12 | $-169.54 | $14,661.55 | ▼ -169.54 after sell → book $10,206.92; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SRRK` | 39 | $58.75 | $2.14 | $-52.99 | $16,950.66 | ▼ -52.99 after sell → book $10,204.79; vs 09:30 mark -2.13 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 42 | $32.90 | $2.12 | — | $15,566.75 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; combo leftover $1412.56; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 164 | $8.61 | $2.48 | — | $14,152.22 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.7; combo leftover $1412.56; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $12,874.37 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; combo leftover $1412.56; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 73 | $19.25 | $2.21 | — | $11,466.91 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+14.1; combo leftover $1412.56; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 75 | $18.75 | $2.21 | — | $10,058.44 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-5.0; combo leftover $1412.56; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 48 | $28.91 | $2.13 | — | $8,668.63 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+9.2; combo leftover $1412.56; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 10 | $252.24 | $2.12 | — | $11,188.91 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $2547.90; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 84 | $30.18 | $2.35 | — | $13,721.68 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $2547.90; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,721.68 | ▲ close $10,288.32 vs 09:30 $10,226.38 (session +101.18) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,721.68 | ▲ 09:30 equity $10,378.14 vs yday $10,288.32 (+89.82) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 154 | $9.50 | $2.49 | $+181.40 | $15,182.19 | ▲ +181.40 after sell → book $10,375.65; vs 09:30 mark -2.49 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 4 | $208.88 | $2.00 | $+16.19 | $14,344.67 | ▲ +16.19 after sell → book $10,373.65; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 87 | $11.10 | $2.25 | $+92.88 | $13,376.72 | ▲ +92.88 after sell → book $10,371.40; vs 09:30 mark -2.25 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 209 | $4.97 | $2.70 | $+16.48 | $12,334.25 | ▲ +16.48 after sell → book $10,368.70; vs 09:30 mark -2.70 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 8 | $127.45 | $2.01 | $+37.44 | $11,312.64 | ▲ +37.44 after sell → book $10,366.69; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 5 | $254.39 | $2.00 | $-276.31 | $10,038.68 | ▼ -276.31 after sell → book $10,364.68; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 56 | $42.00 | $2.19 | $+27.02 | $12,388.49 | ▲ +27.02 after sell → book $10,362.49; vs 09:30 mark -2.19 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 42 | $31.15 | $2.14 | $-77.75 | $13,694.66 | ▼ -77.75 after sell → book $10,360.36; vs 09:30 mark -2.13 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 164 | $8.52 | $2.52 | $-19.76 | $15,089.42 | ▼ -19.76 after sell → book $10,357.84; vs 09:30 mark -2.52 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $16,278.08 | ▼ -89.19 after sell → book $10,355.80; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 73 | $17.87 | $2.23 | $-105.18 | $17,580.36 | ▼ -105.18 after sell → book $10,353.57; vs 09:30 mark -2.23 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 75 | $19.25 | $2.24 | $+33.05 | $19,021.87 | ▲ +33.05 after sell → book $10,351.33; vs 09:30 mark -2.24 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 48 | $28.06 | $2.15 | $-45.09 | $20,366.59 | ▼ -45.09 after sell → book $10,349.17; vs 09:30 mark -2.16 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,366.59 | ▲ close $10,401.49 vs 09:30 $10,378.14 (session +52.32) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,366.59 | ▲ 09:30 equity $10,556.23 vs yday $10,401.49 (+154.74) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 35 | $73.22 | $2.10 | $+41.91 | $17,801.80 | ▲ +41.91 after sell → book $10,554.14; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 47 | $54.76 | $2.13 | $+18.67 | $15,225.95 | ▲ +18.67 after sell → book $10,552.01; vs 09:30 mark -2.13 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,225.95 | ▲ close $10,567.65 vs 09:30 $10,556.23 (session +15.64) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,225.95 | ▲ 09:30 equity $10,619.33 vs yday $10,567.65 (+51.68) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 10 | $235.71 | $2.02 | $+161.16 | $12,866.83 | ▲ +161.16 after sell → book $10,617.31; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 84 | $26.78 | $2.24 | $+281.01 | $10,615.07 | ▲ +281.01 after sell → book $10,615.07; vs 09:30 mark -2.24 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,615.07 | ▲ close $10,615.07 vs 09:30 $10,619.33 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,615.07 | ▲ 09:30 equity $10,615.07 vs yday $10,615.07 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 27 | $23.88 | $2.07 | — | $9,968.24 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $663.44; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 20 | $32.88 | $2.05 | — | $9,308.59 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+16.2; combo leftover $663.44; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 87 | $7.59 | $2.25 | — | $8,646.00 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.5; combo leftover $663.44; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 41 | $15.87 | $2.11 | — | $7,993.22 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $663.44; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $7,639.49 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+3.3; combo leftover $663.44; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $7,283.01 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-12.3; combo leftover $663.44; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 13 | $47.60 | $2.03 | — | $6,662.18 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; combo leftover $663.44; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 178 | $14.85 | $2.65 | — | $9,302.83 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $2650.14; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1549 | $1.71 | $20.33 | — | $11,931.29 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $2650.14; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,931.29 | ▲ close $10,855.82 vs 09:30 $10,615.07 (session +278.23) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,931.29 | ▲ 09:30 equity $10,889.07 vs yday $10,855.82 (+33.25) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 27 | $23.84 | $2.09 | $-5.24 | $12,572.88 | ▼ -5.24 after sell → book $10,886.98; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 20 | $32.48 | $2.07 | $-12.12 | $13,220.41 | ▼ -12.12 after sell → book $10,884.91; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 87 | $7.79 | $2.28 | $+12.87 | $13,895.86 | ▲ +12.87 after sell → book $10,882.63; vs 09:30 mark -2.28 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 1 | $359.70 | $2.01 | $+3.95 | $14,253.55 | ▲ +3.95 after sell → book $10,880.62; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 1 | $321.67 | $2.01 | $-36.83 | $14,573.20 | ▼ -36.83 after sell → book $10,878.60; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 13 | $53.85 | $2.05 | $+77.17 | $15,271.21 | ▲ +77.17 after sell → book $10,876.56; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 5 | $263.36 | $2.00 | — | $13,952.40 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; combo leftover $1527.12; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 787 | $1.94 | $10.15 | — | $12,415.47 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+18.3; combo leftover $1527.12; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 11 | $137.35 | $2.02 | — | $10,902.60 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+5.4; combo leftover $1527.12; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 6 | $236.82 | $2.01 | — | $9,479.67 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.1; combo leftover $1527.12; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 20 | $75.65 | $2.05 | — | $7,964.62 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+6.0; combo leftover $1527.12; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 581 | $4.67 | $7.69 | — | $10,670.20 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $2714.58; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 35 | $76.55 | $2.20 | — | $13,347.25 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $2714.58; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,347.25 | ▲ close $10,905.91 vs 09:30 $10,889.07 (session +57.48) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,347.25 | ▲ 09:30 equity $11,024.41 vs yday $10,905.91 (+118.50) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 41 | $16.74 | $2.13 | $+31.42 | $14,031.46 | ▲ +31.42 after sell → book $11,022.28; vs 09:30 mark -2.13 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 5 | $253.72 | $2.03 | $-52.23 | $15,298.03 | ▼ -52.23 after sell → book $11,020.25; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 787 | $1.94 | $10.30 | $-20.45 | $16,814.52 | ▼ -20.45 after sell → book $11,009.96; vs 09:30 mark -10.29 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 6 | $267.76 | $2.03 | $+181.60 | $18,419.05 | ▲ +181.60 after sell → book $11,007.93; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,419.05 | ▲ close $11,222.22 vs 09:30 $11,024.41 (session +214.29) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,419.05 | ▲ 09:30 equity $11,313.33 vs yday $11,222.22 (+91.11) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 178 | $13.60 | $2.52 | $+217.33 | $15,995.72 | ▲ +217.33 after sell → book $11,310.80; vs 09:30 mark -2.53 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1549 | $1.58 | $19.98 | $+161.05 | $13,528.32 | ▲ +161.05 after sell → book $11,290.82; vs 09:30 mark -19.98 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 11 | $141.82 | $2.05 | $+45.10 | $15,086.29 | ▲ +45.10 after sell → book $11,288.77; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 20 | $76.60 | $2.07 | $+14.88 | $16,616.22 | ▲ +14.88 after sell → book $11,286.70; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,616.22 | ▲ close $11,313.93 vs 09:30 $11,313.33 (session +27.23) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,616.22 | ▲ 09:30 equity $11,395.41 vs yday $11,313.93 (+81.48) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 581 | $4.36 | $7.49 | $+164.93 | $14,075.57 | ▲ +164.93 after sell → book $11,387.92; vs 09:30 mark -7.49 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 35 | $76.79 | $2.10 | $-12.69 | $11,385.82 | ▼ -12.69 after sell → book $11,385.82; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,385.82 | ▲ close $11,385.82 vs 09:30 $11,395.41 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,385.82 | ▲ 09:30 equity $11,385.82 vs yday $11,385.82 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 5 | $164.43 | $2.00 | — | $10,561.67 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $948.82; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 465 | $2.04 | $6.00 | — | $9,607.07 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; combo leftover $948.82; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 447 | $2.12 | $5.77 | — | $8,653.66 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; combo leftover $948.82; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 63 | $15.01 | $2.18 | — | $7,705.85 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $948.82; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 3 | $242.17 | $2.00 | — | $6,977.34 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-11.1; combo leftover $948.82; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 6 | $135.71 | $2.01 | — | $6,161.08 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-9.2; combo leftover $948.82; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 10 | $112.83 | $2.07 | — | $7,287.36 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $1136.59; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 322 | $3.52 | $4.25 | — | $8,416.55 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $1136.59; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 559 | $2.03 | $7.34 | — | $9,543.97 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $1136.59; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 45 | $24.97 | $2.18 | — | $10,665.45 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $1136.59; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 337 | $3.37 | $4.44 | — | $11,796.69 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $1136.59; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,796.69 | ▼ close $11,238.95 vs 09:30 $11,385.82 (session -106.64) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,796.69 | ▼ 09:30 equity $11,232.34 vs yday $11,238.95 (-6.61) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 5 | $141.42 | $2.02 | $-119.08 | $12,501.77 | ▼ -119.08 after sell → book $11,230.32; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 465 | $2.01 | $6.09 | $-26.03 | $13,430.33 | ▼ -26.03 after sell → book $11,224.23; vs 09:30 mark -6.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 447 | $2.05 | $5.85 | $-42.91 | $14,340.83 | ▼ -42.91 after sell → book $11,218.38; vs 09:30 mark -5.85 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 3 | $261.51 | $2.02 | $+54.00 | $15,123.34 | ▲ +54.00 after sell → book $11,216.36; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 6 | $131.40 | $2.03 | $-29.90 | $15,909.71 | ▼ -29.90 after sell → book $11,214.33; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,909.71 | ▼ close $11,090.19 vs 09:30 $11,232.34 (session -124.14) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,909.71 | ▲ 09:30 equity $11,093.48 vs yday $11,090.19 (+3.29) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,909.71 | ▼ close $11,005.35 vs 09:30 $11,093.48 (session -88.13) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,909.71 | ▲ 09:30 equity $11,027.94 vs yday $11,005.35 (+22.59) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 10 | $118.18 | $2.02 | $-57.54 | $14,725.89 | ▼ -57.54 after sell → book $11,025.92; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 322 | $3.98 | $4.15 | $-156.52 | $13,440.18 | ▼ -156.52 after sell → book $11,021.77; vs 09:30 mark -4.15 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 559 | $1.85 | $7.21 | $+86.06 | $12,398.82 | ▲ +86.06 after sell → book $11,014.56; vs 09:30 mark -7.21 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 45 | $24.42 | $2.12 | $+20.45 | $11,297.79 | ▲ +20.45 after sell → book $11,012.43; vs 09:30 mark -2.13 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 337 | $3.75 | $4.35 | $-136.85 | $10,029.70 | ▼ -136.85 after sell → book $11,008.09; vs 09:30 mark -4.34 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 47 | $26.27 | $2.13 | — | $8,792.88 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+10.0; combo leftover $1253.71; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 180 | $6.95 | $2.53 | — | $7,539.35 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-5.8; combo leftover $1253.71; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 31 | $39.99 | $2.08 | — | $6,297.57 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+9.3; combo leftover $1253.71; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 6 | $189.17 | $2.01 | — | $5,160.54 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+7.9; combo leftover $1253.71; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 138 | $18.61 | $2.52 | — | $7,726.21 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $2580.27; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 377 | $6.83 | $5.02 | — | $10,296.10 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $2580.27; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,296.10 | ▼ close $10,580.89 vs 09:30 $11,027.94 (session -410.91) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,296.10 | ▲ 09:30 equity $10,611.03 vs yday $10,580.89 (+30.14) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 47 | $26.51 | $2.15 | $+7.00 | $11,539.92 | ▲ +7.00 after sell → book $10,608.88; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 180 | $7.27 | $2.57 | $+52.50 | $12,845.95 | ▲ +52.50 after sell → book $10,606.31; vs 09:30 mark -2.57 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 31 | $37.57 | $2.10 | $-79.21 | $14,008.52 | ▼ -79.21 after sell → book $10,604.21; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 6 | $190.35 | $2.03 | $+3.04 | $15,148.59 | ▲ +3.04 after sell → book $10,602.18; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 7 | $170.85 | $2.01 | — | $13,950.63 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; combo leftover $1262.38; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 1 | $934.88 | $1.99 | — | $13,013.75 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-7.0; combo leftover $1262.38; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 71 | $17.72 | $2.20 | — | $11,753.43 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=-8.3; combo leftover $1262.38; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 713 | $1.77 | $9.20 | — | $10,482.22 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-10.2; combo leftover $1262.38; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 5 | $238.60 | $2.00 | — | $9,287.22 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.6; combo leftover $1262.38; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 57 | $22.12 | $2.16 | — | $8,024.22 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+10.5; combo leftover $1262.38; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 332 | $7.95 | $4.43 | — | $10,659.19 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $2645.65; owner short_news_r_h3 | — |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 32 | $81.00 | $2.19 | — | $13,249.00 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-3.0; combo leftover $2645.65; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,249.00 | ▲ close $10,792.33 vs 09:30 $10,611.03 (session +216.34) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,249.00 | ▲ 09:30 equity $10,855.44 vs yday $10,792.33 (+63.11) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 63 | $15.87 | $2.20 | $+49.80 | $14,246.61 | ▲ +49.80 after sell → book $10,853.24; vs 09:30 mark -2.20 | union_news_g_h1: dropped from list after 5 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 7 | $182.33 | $2.03 | $+76.32 | $15,520.89 | ▲ +76.32 after sell → book $10,851.21; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 1 | $915.66 | $2.01 | $-23.23 | $16,434.53 | ▼ -23.23 after sell → book $10,849.19; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 71 | $17.13 | $2.22 | $-46.32 | $17,648.54 | ▼ -46.32 after sell → book $10,846.97; vs 09:30 mark -2.22 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 713 | $1.77 | $9.33 | $-18.52 | $18,901.22 | ▼ -18.52 after sell → book $10,837.64; vs 09:30 mark -9.33 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 5 | $236.80 | $2.02 | $-13.03 | $20,083.20 | ▼ -13.03 after sell → book $10,835.62; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 178 | $14.07 | $2.52 | — | $17,576.21 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; combo leftover $2510.40; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 169 | $14.79 | $2.50 | — | $15,074.21 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; combo leftover $2510.40; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 333 | $7.54 | $4.30 | — | $12,560.76 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-20.9; combo leftover $2510.40; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 120 | $20.91 | $2.35 | — | $10,049.21 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+8.6; combo leftover $2510.40; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 157 | $34.44 | $2.68 | — | $15,453.61 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5411.98; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,453.61 | ▼ close $10,786.81 vs 09:30 $10,855.44 (session -34.46) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,453.61 | ▼ 09:30 equity $10,729.04 vs yday $10,786.81 (-57.77) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 138 | $22.11 | $2.40 | $-487.92 | $12,400.02 | ▼ -487.92 after sell → book $10,726.63; vs 09:30 mark -2.41 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 377 | $6.55 | $4.86 | $+95.68 | $9,925.81 | ▲ +95.68 after sell → book $10,721.77; vs 09:30 mark -4.86 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 57 | $22.78 | $2.18 | $+33.28 | $11,222.09 | ▲ +33.28 after sell → book $10,719.59; vs 09:30 mark -2.18 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 178 | $13.90 | $2.57 | $-35.36 | $13,693.72 | ▼ -35.36 after sell → book $10,717.02; vs 09:30 mark -2.57 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 169 | $14.58 | $2.54 | $-40.53 | $16,155.19 | ▼ -40.53 after sell → book $10,714.47; vs 09:30 mark -2.55 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 333 | $7.36 | $4.37 | $-66.94 | $18,601.70 | ▼ -66.94 after sell → book $10,710.10; vs 09:30 mark -4.37 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 120 | $21.65 | $2.39 | $+84.06 | $21,197.31 | ▲ +84.06 after sell → book $10,707.71; vs 09:30 mark -2.39 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 68 | $25.95 | $2.19 | — | $19,430.52 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.2; combo leftover $1766.44; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 821 | $2.15 | $10.59 | — | $17,654.77 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+7.5; combo leftover $1766.44; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 126 | $13.94 | $2.37 | — | $15,895.97 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; combo leftover $1766.44; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 294 | $6.00 | $3.79 | — | $14,128.17 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-24.1; combo leftover $1766.44; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 9 | $190.30 | $2.02 | — | $12,413.46 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+10.6; combo leftover $1766.44; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 7 | $230.25 | $2.01 | — | $10,799.70 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+12.5; combo leftover $1766.44; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 323 | $8.26 | $4.31 | — | $13,463.36 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $2671.18; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 4 | $583.88 | $2.09 | — | $15,796.79 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $2671.18; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,796.79 | ▼ close $10,140.98 vs 09:30 $10,729.04 (session -537.35) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,796.79 | ▲ 09:30 equity $10,146.64 vs yday $10,140.98 (+5.66) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 332 | $8.28 | $4.28 | $-116.61 | $13,045.20 | ▼ -116.61 after sell → book $10,142.35; vs 09:30 mark -4.29 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `MARA` | 126 | $13.13 | $2.40 | $-106.83 | $14,697.18 | ▼ -106.83 after sell → book $10,139.95; vs 09:30 mark -2.40 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 294 | $5.99 | $3.86 | $-10.59 | $16,454.39 | ▼ -10.59 after sell → book $10,136.10; vs 09:30 mark -3.85 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 1357 | $1.01 | $17.51 | — | $15,066.31 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+14.3; combo leftover $1371.20; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `MRNA` | 8 | $168.50 | $2.01 | — | $13,716.30 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+17.9; combo leftover $1371.20; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 318 | $4.30 | $4.10 | — | $12,344.79 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; combo leftover $1371.20; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 26 | $93.97 | $2.16 | — | $14,785.85 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $2528.12; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,785.85 | ▼ close $10,067.17 vs 09:30 $10,146.64 (session -43.14) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,785.85 | ▼ 09:30 equity $9,746.40 vs yday $10,067.17 (-320.77) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `LEN` | 32 | $82.00 | $2.09 | $-36.27 | $12,159.76 | ▼ -36.27 after sell → book $9,744.32; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 68 | $26.58 | $2.22 | $+38.43 | $13,964.98 | ▲ +38.43 after sell → book $9,742.10; vs 09:30 mark -2.22 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMTX` | 821 | $2.09 | $10.74 | $-70.59 | $15,670.13 | ▼ -70.59 after sell → book $9,731.36; vs 09:30 mark -10.74 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 9 | $174.50 | $2.04 | $-146.26 | $17,238.59 | ▼ -146.26 after sell → book $9,729.32; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 7 | $266.50 | $2.04 | $+249.70 | $19,102.06 | ▲ +249.70 after sell → book $9,727.28; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 1357 | $0.95 | $17.20 | $-116.12 | $20,374.01 | ▼ -116.12 after sell → book $9,710.08; vs 09:30 mark -17.20 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MRNA` | 8 | $183.41 | $2.04 | $+115.19 | $21,839.21 | ▲ +115.19 after sell → book $9,708.05; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 228 | $7.95 | $2.94 | — | $20,023.67 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ⚪; ret5=+12.4; combo leftover $1819.93; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 115 | $15.72 | $2.33 | — | $18,213.54 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; combo leftover $1819.93; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 1491 | $1.22 | $19.23 | — | $16,375.28 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-33.0; combo leftover $1819.93; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 1399 | $1.30 | $18.05 | — | $14,538.54 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; combo leftover $1819.93; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 45 | $40.00 | $2.12 | — | $12,736.41 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+6.7; combo leftover $1819.93; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 9 | $196.78 | $2.02 | — | $10,963.37 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $1819.93; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 41 | $116.85 | $2.29 | — | $15,751.93 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $4830.67; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,751.93 | ▼ close $9,461.76 vs 09:30 $9,746.40 (session -197.29) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,751.93 | ▼ 09:30 equity $9,424.66 vs yday $9,461.76 (-37.10) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 4 | $600.27 | $2.00 | $-69.66 | $13,348.85 | ▼ -69.66 after sell → book $9,422.66; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DGXX` | 318 | $4.12 | $4.17 | $-65.51 | $14,654.84 | ▼ -65.51 after sell → book $9,418.49; vs 09:30 mark -4.17 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 228 | $7.38 | $2.99 | $-135.89 | $16,334.49 | ▼ -135.89 after sell → book $9,415.50; vs 09:30 mark -2.99 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 115 | $14.38 | $2.37 | $-158.80 | $17,985.82 | ▼ -158.80 after sell → book $9,413.13; vs 09:30 mark -2.37 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CMPX` | 1491 | $1.17 | $19.50 | $-113.28 | $19,710.80 | ▼ -113.28 after sell → book $9,393.64; vs 09:30 mark -19.49 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 1399 | $1.27 | $18.29 | $-78.31 | $21,469.24 | ▼ -78.31 after sell → book $9,375.35; vs 09:30 mark -18.29 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 45 | $39.27 | $2.15 | $-37.12 | $23,234.24 | ▼ -37.12 after sell → book $9,373.20; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 9 | $192.26 | $2.04 | $-44.74 | $24,962.54 | ▼ -44.74 after sell → book $9,371.16; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,962.54 | ▼ close $9,149.50 vs 09:30 $9,424.66 (session -221.66) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,651.92 | ▼ 09:30 equity $7,792.88 vs yday $7,820.96 (-28.08) | 09:30 open · cash $18,651.92 (unchanged overnight, no fees) · equity $7,792.88 vs prior close $7,820.96 (-28.08) | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 483 | $3.86 | $6.23 | — | $16,781.31 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; combo leftover $1865.19; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 115 | $16.21 | $2.33 | — | $14,914.82 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $1865.19; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 6 | $272.16 | $2.01 | — | $13,279.86 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+11.7; combo leftover $1865.19; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 25 | $74.15 | $2.06 | — | $11,424.04 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.5; combo leftover $1865.19; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 2 | $887.00 | $2.00 | — | $9,648.05 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+0.3; combo leftover $1865.19; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 495 | $7.85 | $6.61 | — | $13,527.19 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $3889.12; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,527.19 | ▼ close $7,730.71 vs 09:30 $7,792.88 (session -40.93) | 16:00 close · cash $13,527.19 · equity $7,730.71 vs 09:30 $7,792.88 (-62.17; session marks -40.93) · 11 name(s) marked open→close (per-name table). AEHL×284 09:30 $9.05 → close $9.36 -88.04; BAND×36 09:30 $61.83 → close $61.83 -0.00; HALO×18 09:30 $115.36 → close $113.90 +26.28; PAYX×18 09:30 $101.59 → close $101.59 +0.00; USFD×23 09:30 $93.82 → close $93.82 +0.00; ZSQR×483 09:30 $3.86 → close $3.78 -38.64; SECZ×115 09:30 $16.21 → close $15.96 -28.75; ILMN×6 09:30 $272.16 → close $270.00 -12.96; RKLB×25 09:30 $74.15 → close $73.95 -5.00; COST×2 09:30 $887.00 → close $922.76 +71.53; RSKD×495 09:30 $7.85 → close $7.78 +34.65 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 625.00 < 1 share @ 1646.93 |
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
| 2026-09-03 | `DE` | cash | leftover split 663.44 < 1 share @ 703.25 |
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
| `FIVN` | 157 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5411.98; owner short_news_r_h3 |
| `AEHL` | 323 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $2671.18; owner short_news_r_h3 |
| `USFD` | 26 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $2528.12; owner short_news_r_h3 |
| `HALO` | 41 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $4830.67; owner short_news_r_h3 |
