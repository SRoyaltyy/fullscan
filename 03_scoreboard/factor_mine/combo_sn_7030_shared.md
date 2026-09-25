# Factor mine action — `combo_sn_7030_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared short_news_r_h3/union_news_g_h1 w=0.7,0.3 net=priority

Cash book **-17.29%** ($8,271) · signal-only (no cash/fees) was —. Starts YES **0/30**. Fills 298 · skips 198 · realized $-233.81.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: short_news_r_h3 70%, union_news_g_h1 30%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: short_news_r_h3 70%, union_news_g_h1 30%.
- Member: short_news_r_h3 (70% · short · hold 3).
- Member: union_news_g_h1 (30% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $18,280.53.

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
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 1 | $359.83 | $1.99 | — | $9,638.18 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; combo leftover $375.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 2 | $146.90 | $2.00 | — | $9,342.38 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; combo leftover $375.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 3 | $120.00 | $2.00 | — | $8,980.38 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; combo leftover $375.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 87 | $4.31 | $2.25 | — | $8,603.16 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; combo leftover $375.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 19 | $19.57 | $2.05 | — | $8,229.28 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $375.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 27 | $13.55 | $2.07 | — | $7,861.36 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $375.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 28 | $13.18 | $2.07 | — | $7,490.25 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; combo leftover $375.00; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1410 | $1.18 | $18.48 | — | $9,135.57 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $1664.26; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 86 | $19.17 | $2.32 | — | $10,781.87 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $1664.26; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 131 | $12.70 | $2.46 | — | $12,442.45 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $1664.26; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,442.45 | ▲ close $10,034.06 vs 09:30 $10,000.00 (session +71.75) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,442.45 | ▼ 09:30 equity $9,970.82 vs yday $10,034.06 (-63.24) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 1 | $367.88 | $2.01 | $+4.04 | $12,808.31 | ▲ +4.04 after sell → book $9,968.80; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 2 | $149.37 | $2.02 | $+0.93 | $13,105.04 | ▲ +0.93 after sell → book $9,966.79; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 3 | $127.40 | $2.02 | $+18.18 | $13,485.22 | ▲ +18.18 after sell → book $9,964.77; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 87 | $4.60 | $2.28 | $+20.70 | $13,883.14 | ▲ +20.70 after sell → book $9,962.49; vs 09:30 mark -2.28 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 19 | $19.57 | $2.07 | $-4.11 | $14,252.91 | ▼ -4.11 after sell → book $9,960.43; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 27 | $13.16 | $2.09 | $-14.69 | $14,606.14 | ▼ -14.69 after sell → book $9,958.34; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 28 | $13.84 | $2.09 | $+14.31 | $14,991.56 | ▲ +14.31 after sell → book $9,956.24; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 19 | $46.18 | $2.05 | — | $14,112.10 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; combo leftover $899.49; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 6 | $142.77 | $2.01 | — | $13,253.47 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; combo leftover $899.49; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 4 | $202.70 | $2.00 | — | $12,440.67 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; combo leftover $899.49; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 9 | $92.99 | $2.02 | — | $11,601.74 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; combo leftover $899.49; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 18 | $49.00 | $2.04 | — | $10,717.69 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; combo leftover $899.49; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 864 | $1.15 | $11.33 | — | $11,699.97 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $994.61; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 279 | $3.56 | $3.68 | — | $12,689.53 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $994.61; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 31 | $31.70 | $2.13 | — | $13,670.10 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; combo leftover $994.61; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 330 | $3.01 | $4.35 | — | $14,659.05 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $994.61; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 146 | $6.80 | $2.49 | — | $15,649.36 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; combo leftover $994.61; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,649.36 | ▲ close $10,091.10 vs 09:30 $9,970.82 (session +168.95) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,649.36 | ▲ 09:30 equity $10,216.88 vs yday $10,091.10 (+125.78) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 19 | $48.00 | $2.07 | $+30.47 | $16,559.29 | ▲ +30.47 after sell → book $10,214.81; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 6 | $148.04 | $2.03 | $+27.58 | $17,445.50 | ▲ +27.58 after sell → book $10,212.78; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 4 | $208.93 | $2.02 | $+20.90 | $18,279.20 | ▲ +20.90 after sell → book $10,210.76; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 9 | $92.38 | $2.04 | $-9.54 | $19,108.58 | ▼ -9.54 after sell → book $10,208.72; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 18 | $45.09 | $2.06 | $-74.49 | $19,918.14 | ▼ -74.49 after sell → book $10,206.66; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,918.14 | ▲ close $10,444.86 vs 09:30 $10,216.88 (session +238.20) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,918.14 | ▼ 09:30 equity $10,412.80 vs yday $10,444.86 (-32.06) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 1410 | $1.07 | $18.19 | $+118.43 | $18,391.25 | ▲ +118.43 after sell → book $10,394.61; vs 09:30 mark -18.19 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 131 | $11.75 | $2.38 | $+118.95 | $16,849.62 | ▲ +118.95 after sell → book $10,392.23; vs 09:30 mark -2.38 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,849.62 | ▲ close $10,440.16 vs 09:30 $10,412.80 (session +47.94) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,849.62 | ▼ 09:30 equity $10,382.49 vs yday $10,440.16 (-57.67) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 86 | $18.13 | $2.25 | $+84.87 | $15,288.19 | ▲ +84.87 after sell → book $10,380.24; vs 09:30 mark -2.25 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 864 | $0.96 | $10.91 | $+139.33 | $14,445.24 | ▲ +139.33 after sell → book $10,369.33; vs 09:30 mark -10.91 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 279 | $4.01 | $3.60 | $-134.23 | $13,321.46 | ▼ -134.23 after sell → book $10,365.73; vs 09:30 mark -3.60 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 31 | $31.87 | $2.08 | $-9.48 | $12,331.41 | ▼ -9.48 after sell → book $10,363.65; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 330 | $2.95 | $4.26 | $+11.19 | $11,353.65 | ▲ +11.19 after sell → book $10,359.39; vs 09:30 mark -4.26 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 146 | $6.81 | $2.43 | $-6.38 | $10,356.96 | ▼ -6.38 after sell → book $10,356.96; vs 09:30 mark -2.43 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 4 | $91.01 | $2.00 | — | $9,990.92 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; combo leftover $388.39; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 2 | $150.14 | $2.00 | — | $9,688.64 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $388.39; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 549 | $0.71 | $5.53 | — | $9,294.97 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; combo leftover $388.39; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 58 | $6.61 | $2.16 | — | $8,909.72 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; combo leftover $388.39; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 24 | $16.00 | $2.06 | — | $8,523.66 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; combo leftover $388.39; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 14 | $26.57 | $2.03 | — | $8,149.65 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; combo leftover $388.39; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 6 | $58.73 | $2.01 | — | $7,795.26 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; combo leftover $388.39; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 8 | $44.76 | $2.01 | — | $7,435.16 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; combo leftover $388.39; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $8,046.48 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $646.07; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 30 | $21.40 | $2.12 | — | $8,686.36 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $646.07; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 145 | $4.43 | $2.48 | — | $9,326.23 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; combo leftover $646.07; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 54 | $11.81 | $2.19 | — | $9,962.05 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $646.07; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $10,481.72 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $646.07; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $11,088.70 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; combo leftover $646.07; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 6 | $106.38 | $2.05 | — | $11,724.94 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $646.07; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 140 | $4.61 | $2.46 | — | $12,367.88 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $646.07; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,367.88 | ▲ close $10,342.43 vs 09:30 $10,382.49 (session +22.71) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,367.88 | ▲ 09:30 equity $10,365.71 vs yday $10,342.43 (+23.28) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 4 | $95.72 | $2.02 | $+14.82 | $12,748.74 | ▲ +14.82 after sell → book $10,363.69; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 2 | $133.11 | $2.02 | $-38.07 | $13,012.94 | ▼ -38.07 after sell → book $10,361.68; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 549 | $0.67 | $5.45 | $-29.09 | $13,377.52 | ▼ -29.09 after sell → book $10,356.23; vs 09:30 mark -5.45 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 58 | $6.95 | $2.18 | $+15.66 | $13,778.43 | ▲ +15.66 after sell → book $10,354.04; vs 09:30 mark -2.19 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 24 | $17.66 | $2.08 | $+35.70 | $14,200.19 | ▲ +35.70 after sell → book $10,351.96; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 14 | $26.25 | $2.05 | $-8.56 | $14,565.64 | ▼ -8.56 after sell → book $10,349.91; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 8 | $44.52 | $2.03 | $-5.97 | $14,919.77 | ▼ -5.97 after sell → book $10,347.88; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 5 | $119.43 | $2.00 | — | $14,320.61 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $639.42; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 258 | $2.47 | $3.33 | — | $13,680.02 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; combo leftover $639.42; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 5 | $115.18 | $2.00 | — | $13,102.12 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $639.42; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $12,476.86 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.4; combo leftover $639.42; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 54 | $11.70 | $2.15 | — | $11,842.91 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; combo leftover $639.42; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 57 | $11.10 | $2.16 | — | $11,208.34 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; combo leftover $639.42; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 197 | $3.24 | $2.58 | — | $10,567.48 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; combo leftover $639.42; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 276 | $3.11 | $3.64 | — | $11,422.20 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $860.97; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 6 | $133.11 | $2.05 | — | $12,218.81 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; combo leftover $860.97; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 9 | $89.10 | $2.06 | — | $13,018.65 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $860.97; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 22 | $38.40 | $2.10 | — | $13,861.35 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $860.97; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 41 | $20.90 | $2.16 | — | $14,716.09 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $860.97; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 31 | $27.00 | $2.13 | — | $15,550.97 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $860.97; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,550.97 | ▼ close $10,315.93 vs 09:30 $10,365.71 (session -1.60) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,550.97 | ▲ 09:30 equity $10,352.99 vs yday $10,315.93 (+37.06) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 5 | $120.51 | $2.02 | $+1.37 | $16,151.49 | ▲ +1.37 after sell → book $10,350.96; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 258 | $2.40 | $3.38 | $-24.77 | $16,767.31 | ▼ -24.77 after sell → book $10,347.58; vs 09:30 mark -3.38 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 5 | $121.00 | $2.02 | $+25.07 | $17,370.28 | ▲ +25.07 after sell → book $10,345.55; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 1 | $653.04 | $2.01 | $+25.77 | $18,021.31 | ▲ +25.77 after sell → book $10,343.54; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 54 | $11.17 | $2.17 | $-32.94 | $18,622.32 | ▼ -32.94 after sell → book $10,341.37; vs 09:30 mark -2.17 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 57 | $11.48 | $2.18 | $+17.60 | $19,274.50 | ▲ +17.60 after sell → book $10,339.19; vs 09:30 mark -2.18 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 197 | $2.99 | $2.62 | $-54.45 | $19,860.90 | ▼ -54.45 after sell → book $10,336.56; vs 09:30 mark -2.63 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,860.90 | ▲ close $10,398.98 vs 09:30 $10,352.99 (session +62.42) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,860.90 | ▲ 09:30 equity $10,447.56 vs yday $10,398.98 (+48.58) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 6 | $57.93 | $2.03 | $-8.84 | $20,206.46 | ▼ -8.84 after sell → book $10,445.54; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $212.00 | $2.00 | $-26.68 | $19,568.46 | ▼ -26.68 after sell → book $10,443.54; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 30 | $20.90 | $2.08 | $+10.80 | $18,939.38 | ▲ +10.80 after sell → book $10,441.46; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 145 | $4.42 | $2.42 | $-3.45 | $18,296.05 | ▼ -3.45 after sell → book $10,439.03; vs 09:30 mark -2.43 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 54 | $11.00 | $2.15 | $+39.67 | $17,699.90 | ▲ +39.67 after sell → book $10,436.88; vs 09:30 mark -2.15 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 3 | $170.64 | $2.00 | $+5.75 | $17,185.98 | ▲ +5.75 after sell → book $10,434.88; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 13 | $43.63 | $2.03 | $+37.77 | $16,616.76 | ▲ +37.77 after sell → book $10,432.85; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 6 | $105.58 | $2.01 | $+0.75 | $15,981.27 | ▲ +0.75 after sell → book $10,430.84; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 140 | $4.77 | $2.41 | $-27.27 | $15,311.06 | ▼ -27.27 after sell → book $10,428.43; vs 09:30 mark -2.41 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 69 | $9.42 | $2.20 | — | $14,658.89 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; combo leftover $656.19; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 18 | $35.05 | $2.04 | — | $14,025.94 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $656.19; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 27 | $24.11 | $2.07 | — | $13,372.90 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=+891.7; combo leftover $656.19; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 22 | $28.86 | $2.06 | — | $12,735.93 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.7; combo leftover $656.19; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 75 | $8.72 | $2.21 | — | $12,079.71 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.0; combo leftover $656.19; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 5 | $118.52 | $2.00 | — | $11,485.11 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+21.7; combo leftover $656.19; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 8 | $77.13 | $2.01 | — | $10,866.05 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; ⚪; ret5=+13.8; combo leftover $656.19; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 95 | $13.62 | $2.34 | — | $12,158.09 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $1301.73; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 23 | $54.51 | $2.11 | — | $13,409.71 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $1301.73; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 7 | $175.01 | $2.06 | — | $14,632.71 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; combo leftover $1301.73; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 3 | $364.35 | $2.05 | — | $15,723.71 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $1301.73; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,723.71 | ▲ close $10,483.41 vs 09:30 $10,447.56 (session +78.14) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,723.71 | ▲ 09:30 equity $10,598.78 vs yday $10,483.41 (+115.37) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 276 | $2.83 | $3.56 | $+70.08 | $14,939.07 | ▲ +70.08 after sell → book $10,595.22; vs 09:30 mark -3.56 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 6 | $154.20 | $2.01 | $-130.60 | $14,011.87 | ▼ -130.60 after sell → book $10,593.22; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 9 | $88.24 | $2.02 | $+3.66 | $13,215.69 | ▲ +3.66 after sell → book $10,591.20; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 22 | $38.41 | $2.06 | $-4.38 | $12,368.61 | ▼ -4.38 after sell → book $10,589.14; vs 09:30 mark -2.06 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 41 | $20.50 | $2.11 | $+12.13 | $11,526.00 | ▲ +12.13 after sell → book $10,587.03; vs 09:30 mark -2.11 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 31 | $26.00 | $2.08 | $+26.79 | $10,717.92 | ▲ +26.79 after sell → book $10,584.95; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 69 | $10.07 | $2.22 | $+40.43 | $11,410.53 | ▲ +40.43 after sell → book $10,582.73; vs 09:30 mark -2.22 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 18 | $35.70 | $2.06 | $+7.59 | $12,051.06 | ▲ +7.59 after sell → book $10,580.66; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 27 | $26.61 | $2.09 | $+63.34 | $12,767.44 | ▲ +63.34 after sell → book $10,578.57; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 22 | $27.56 | $2.08 | $-32.73 | $13,371.69 | ▼ -32.73 after sell → book $10,576.50; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 75 | $8.86 | $2.24 | $+6.05 | $14,033.95 | ▲ +6.05 after sell → book $10,574.26; vs 09:30 mark -2.24 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 5 | $119.80 | $2.02 | $+2.37 | $14,630.93 | ▲ +2.37 after sell → book $10,572.24; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 8 | $79.34 | $2.03 | $+13.63 | $15,263.61 | ▲ +13.63 after sell → book $10,570.20; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 68 | $11.12 | $2.19 | — | $14,505.26 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; combo leftover $763.18; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 92 | $8.29 | $2.27 | — | $13,740.31 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; combo leftover $763.18; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 43 | $17.41 | $2.12 | — | $12,989.56 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-9.2; combo leftover $763.18; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 68 | $11.22 | $2.19 | — | $12,224.41 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.8; combo leftover $763.18; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 2 | $267.02 | $2.00 | — | $11,688.37 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.7; combo leftover $763.18; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 6 | $118.50 | $2.01 | — | $10,975.36 | — | union ∩ news_g, no 🚨; gate news=good; list overnight,overnight_mega; 🔵; ret5=-2.7; combo leftover $763.18; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 4 | $213.94 | $2.05 | — | $11,829.08 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $1055.74; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 86 | $12.22 | $2.30 | — | $12,877.70 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $1055.74; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 207 | $5.08 | $2.74 | — | $13,926.51 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $1055.74; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 7 | $132.64 | $2.06 | — | $14,852.94 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $1055.74; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 5 | $199.94 | $2.05 | — | $15,850.59 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; combo leftover $1055.74; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,850.59 | ▼ close $10,493.76 vs 09:30 $10,598.78 (session -52.47) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,850.59 | ▼ 09:30 equity $10,341.31 vs yday $10,493.76 (-152.45) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 2 | $267.23 | $2.02 | $-3.59 | $16,383.03 | ▼ -3.59 after sell → book $10,339.29; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 39 | $41.44 | $2.11 | — | $14,764.76 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+3.1; combo leftover $1638.30; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 23 | $70.30 | $2.06 | — | $13,145.80 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=-11.2; combo leftover $1638.30; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `SRRK` | 27 | $60.00 | $2.07 | — | $11,523.73 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+6.2; combo leftover $1638.30; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 34 | $74.54 | $2.19 | — | $14,055.90 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $2583.26; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 46 | $55.25 | $2.23 | — | $16,595.17 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $2583.26; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,595.17 | ▼ close $10,199.28 vs 09:30 $10,341.31 (session -129.35) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,595.17 | ▼ 09:30 equity $10,099.48 vs yday $10,199.28 (-99.80) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 95 | $13.90 | $2.27 | $-30.74 | $15,272.40 | ▼ -30.74 after sell → book $10,097.21; vs 09:30 mark -2.27 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 23 | $52.49 | $2.06 | $+42.29 | $14,063.07 | ▲ +42.29 after sell → book $10,095.15; vs 09:30 mark -2.06 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 7 | $172.76 | $2.01 | $+11.67 | $12,851.74 | ▲ +11.67 after sell → book $10,093.14; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 3 | $347.82 | $2.00 | $+45.54 | $11,806.28 | ▲ +45.54 after sell → book $10,091.14; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 68 | $11.27 | $2.22 | $+5.79 | $12,570.42 | ▲ +5.79 after sell → book $10,088.92; vs 09:30 mark -2.22 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWRD` | 43 | $17.70 | $2.14 | $+8.21 | $13,329.38 | ▲ +8.21 after sell → book $10,086.78; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TRLV` | 68 | $11.00 | $2.22 | $-19.37 | $14,075.17 | ▼ -19.37 after sell → book $10,084.57; vs 09:30 mark -2.21 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 6 | $115.66 | $2.03 | $-21.08 | $14,767.10 | ▼ -21.08 after sell → book $10,082.54; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 23 | $65.29 | $2.08 | $-119.37 | $16,266.69 | ▼ -119.37 after sell → book $10,080.46; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SRRK` | 27 | $58.75 | $2.09 | $-37.91 | $17,850.85 | ▼ -37.91 after sell → book $10,078.37; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 27 | $32.90 | $2.07 | — | $16,960.48 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; combo leftover $892.54; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 103 | $8.61 | $2.30 | — | $16,071.35 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.7; combo leftover $892.54; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 6 | $141.76 | $2.01 | — | $15,218.78 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; combo leftover $892.54; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 46 | $19.25 | $2.13 | — | $14,331.15 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+14.1; combo leftover $892.54; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 47 | $18.75 | $2.13 | — | $13,447.77 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-5.0; combo leftover $892.54; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 30 | $28.91 | $2.08 | — | $12,578.39 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+9.2; combo leftover $892.54; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 9 | $252.24 | $2.11 | — | $14,846.44 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $2516.41; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 83 | $30.18 | $2.34 | — | $17,349.04 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $2516.41; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,349.04 | ▲ close $10,251.30 vs 09:30 $10,099.48 (session +190.10) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,349.04 | ▲ 09:30 equity $10,345.65 vs yday $10,251.30 (+94.35) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 92 | $9.50 | $2.29 | $+106.76 | $18,220.75 | ▲ +106.76 after sell → book $10,343.35; vs 09:30 mark -2.30 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 4 | $208.88 | $2.00 | $+16.19 | $17,383.23 | ▲ +16.19 after sell → book $10,341.35; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 86 | $11.10 | $2.25 | $+91.77 | $16,426.38 | ▲ +91.77 after sell → book $10,339.10; vs 09:30 mark -2.25 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 207 | $4.97 | $2.67 | $+16.32 | $15,393.88 | ▲ +16.32 after sell → book $10,336.43; vs 09:30 mark -2.67 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 7 | $127.45 | $2.01 | $+32.26 | $14,499.72 | ▲ +32.26 after sell → book $10,334.42; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 5 | $254.39 | $2.00 | $-276.31 | $13,225.77 | ▼ -276.31 after sell → book $10,332.42; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 39 | $42.00 | $2.13 | $+17.60 | $14,861.64 | ▲ +17.60 after sell → book $10,330.29; vs 09:30 mark -2.13 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 27 | $31.15 | $2.09 | $-51.41 | $15,700.60 | ▼ -51.41 after sell → book $10,328.20; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 103 | $8.52 | $2.33 | $-13.90 | $16,575.83 | ▼ -13.90 after sell → book $10,325.87; vs 09:30 mark -2.33 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 6 | $132.30 | $2.03 | $-60.80 | $17,367.60 | ▼ -60.80 after sell → book $10,323.84; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 46 | $17.87 | $2.15 | $-67.76 | $18,187.47 | ▼ -67.76 after sell → book $10,321.69; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 47 | $19.25 | $2.15 | $+19.22 | $19,090.07 | ▲ +19.22 after sell → book $10,319.54; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 30 | $28.06 | $2.10 | $-29.68 | $19,929.77 | ▼ -29.68 after sell → book $10,317.44; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,929.77 | ▲ close $10,368.42 vs 09:30 $10,345.65 (session +50.98) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,929.77 | ▲ 09:30 equity $10,514.54 vs yday $10,368.42 (+146.12) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 34 | $73.22 | $2.09 | $+40.60 | $17,438.20 | ▲ +40.60 after sell → book $10,512.45; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 46 | $54.76 | $2.13 | $+18.18 | $14,917.11 | ▲ +18.18 after sell → book $10,510.32; vs 09:30 mark -2.13 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,917.11 | ▲ close $10,523.36 vs 09:30 $10,514.54 (session +13.04) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,917.11 | ▲ 09:30 equity $10,572.98 vs yday $10,523.36 (+49.62) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 9 | $235.71 | $2.02 | $+144.65 | $12,793.71 | ▲ +144.65 after sell → book $10,570.97; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 83 | $26.78 | $2.24 | $+277.62 | $10,568.73 | ▲ +277.62 after sell → book $10,568.73; vs 09:30 mark -2.24 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,568.73 | ▲ close $10,568.73 vs 09:30 $10,572.98 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,568.73 | ▲ 09:30 equity $10,568.73 vs yday $10,568.73 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 16 | $23.88 | $2.04 | — | $10,184.61 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $396.33; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 12 | $32.88 | $2.03 | — | $9,788.02 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+16.2; combo leftover $396.33; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 52 | $7.59 | $2.15 | — | $9,391.20 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.5; combo leftover $396.33; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 24 | $15.87 | $2.06 | — | $9,008.26 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $396.33; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $8,654.52 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+3.3; combo leftover $396.33; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $8,298.04 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-12.3; combo leftover $396.33; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 8 | $47.60 | $2.01 | — | $7,915.23 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; combo leftover $396.33; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 177 | $14.85 | $2.64 | — | $10,541.03 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $2638.61; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1543 | $1.71 | $20.25 | — | $13,159.31 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $2638.61; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,159.31 | ▲ close $10,752.15 vs 09:30 $10,568.73 (session +220.59) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,159.31 | ▲ 09:30 equity $10,798.48 vs yday $10,752.15 (+46.33) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 16 | $23.84 | $2.06 | $-4.74 | $13,538.69 | ▼ -4.74 after sell → book $10,796.42; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 12 | $32.48 | $2.05 | $-8.87 | $13,926.40 | ▼ -8.87 after sell → book $10,794.37; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 52 | $7.79 | $2.17 | $+6.09 | $14,329.32 | ▲ +6.09 after sell → book $10,792.21; vs 09:30 mark -2.16 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 1 | $359.70 | $2.01 | $+3.95 | $14,687.01 | ▲ +3.95 after sell → book $10,790.20; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 1 | $321.67 | $2.01 | $-36.83 | $15,006.66 | ▼ -36.83 after sell → book $10,788.18; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 8 | $53.85 | $2.03 | $+45.95 | $15,435.43 | ▲ +45.95 after sell → book $10,786.15; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 3 | $263.36 | $2.00 | — | $14,643.35 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; combo leftover $926.13; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 477 | $1.94 | $6.15 | — | $13,711.82 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+18.3; combo leftover $926.13; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 6 | $137.35 | $2.01 | — | $12,885.71 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+5.4; combo leftover $926.13; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 3 | $236.82 | $2.00 | — | $12,173.25 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.1; combo leftover $926.13; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 12 | $75.65 | $2.03 | — | $11,263.42 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+6.0; combo leftover $926.13; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 576 | $4.67 | $7.62 | — | $13,945.72 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $2692.99; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 35 | $76.55 | $2.20 | — | $16,622.77 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $2692.99; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,622.77 | ▼ close $10,748.81 vs 09:30 $10,798.48 (session -13.33) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,622.77 | ▲ 09:30 equity $10,832.18 vs yday $10,748.81 (+83.37) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 24 | $16.74 | $2.08 | $+16.74 | $17,022.45 | ▲ +16.74 after sell → book $10,830.10; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 3 | $253.72 | $2.02 | $-32.94 | $17,781.59 | ▼ -32.94 after sell → book $10,828.08; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 477 | $1.94 | $6.24 | $-12.40 | $18,700.73 | ▼ -12.40 after sell → book $10,821.84; vs 09:30 mark -6.24 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 3 | $267.76 | $2.02 | $+88.80 | $19,501.99 | ▲ +88.80 after sell → book $10,819.82; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,501.99 | ▲ close $11,054.71 vs 09:30 $10,832.18 (session +234.89) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,501.99 | ▲ 09:30 equity $11,120.05 vs yday $11,054.71 (+65.34) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 177 | $13.60 | $2.52 | $+216.09 | $17,092.27 | ▲ +216.09 after sell → book $11,117.53; vs 09:30 mark -2.52 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1543 | $1.58 | $19.90 | $+160.43 | $14,634.42 | ▲ +160.43 after sell → book $11,097.62; vs 09:30 mark -19.91 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 6 | $141.82 | $2.03 | $+22.78 | $15,483.32 | ▲ +22.78 after sell → book $11,095.60; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 12 | $76.60 | $2.05 | $+7.33 | $16,400.47 | ▲ +7.33 after sell → book $11,093.55; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,400.47 | ▲ close $11,120.63 vs 09:30 $11,120.05 (session +27.08) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,400.47 | ▲ 09:30 equity $11,201.46 vs yday $11,120.63 (+80.83) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 576 | $4.36 | $7.43 | $+163.51 | $13,881.68 | ▲ +163.51 after sell → book $11,194.03; vs 09:30 mark -7.43 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 35 | $76.79 | $2.10 | $-12.69 | $11,191.93 | ▼ -12.69 after sell → book $11,191.93; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,191.93 | ▲ close $11,191.93 vs 09:30 $11,201.46 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,191.93 | ▲ 09:30 equity $11,191.93 vs yday $11,191.93 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 3 | $164.43 | $2.00 | — | $10,696.65 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $559.60; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 274 | $2.04 | $3.53 | — | $10,134.15 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; combo leftover $559.60; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 263 | $2.12 | $3.39 | — | $9,573.20 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; combo leftover $559.60; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 37 | $15.01 | $2.10 | — | $9,015.73 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $559.60; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 2 | $242.17 | $2.00 | — | $8,529.39 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-11.1; combo leftover $559.60; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 4 | $135.71 | $2.00 | — | $7,984.55 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-9.2; combo leftover $559.60; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 9 | $112.83 | $2.06 | — | $8,998.00 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $1117.69; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 317 | $3.52 | $4.18 | — | $10,109.66 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $1117.69; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 550 | $2.03 | $7.23 | — | $11,218.93 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $1117.69; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 44 | $24.97 | $2.17 | — | $12,315.44 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $1117.69; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 331 | $3.37 | $4.37 | — | $13,426.54 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $1117.69; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,426.54 | ▼ close $11,093.28 vs 09:30 $11,191.93 (session -63.62) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,426.54 | ▲ 09:30 equity $11,101.73 vs yday $11,093.28 (+8.45) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 3 | $141.42 | $2.02 | $-73.05 | $13,848.78 | ▼ -73.05 after sell → book $11,099.71; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 274 | $2.01 | $3.59 | $-15.34 | $14,395.93 | ▼ -15.34 after sell → book $11,096.12; vs 09:30 mark -3.59 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 263 | $2.05 | $3.45 | $-25.25 | $14,931.64 | ▼ -25.25 after sell → book $11,092.68; vs 09:30 mark -3.44 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 2 | $261.51 | $2.02 | $+34.67 | $15,452.64 | ▲ +34.67 after sell → book $11,090.66; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 4 | $131.40 | $2.02 | $-21.26 | $15,976.22 | ▼ -21.26 after sell → book $11,088.64; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,976.22 | ▼ close $10,954.14 vs 09:30 $11,101.73 (session -134.50) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,976.22 | ▲ 09:30 equity $10,956.36 vs yday $10,954.14 (+2.22) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,976.22 | ▼ close $10,872.31 vs 09:30 $10,956.36 (session -84.05) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,976.22 | ▲ 09:30 equity $10,892.32 vs yday $10,872.31 (+20.01) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 9 | $118.18 | $2.02 | $-52.19 | $14,910.58 | ▼ -52.19 after sell → book $10,890.30; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 317 | $3.98 | $4.09 | $-154.09 | $13,644.83 | ▼ -154.09 after sell → book $10,886.21; vs 09:30 mark -4.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 550 | $1.85 | $7.09 | $+84.68 | $12,620.24 | ▲ +84.68 after sell → book $10,879.12; vs 09:30 mark -7.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 44 | $24.42 | $2.12 | $+19.91 | $11,543.64 | ▲ +19.91 after sell → book $10,877.00; vs 09:30 mark -2.12 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 331 | $3.75 | $4.27 | $-134.42 | $10,298.12 | ▼ -134.42 after sell → book $10,872.73; vs 09:30 mark -4.27 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 29 | $26.27 | $2.08 | — | $9,534.21 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+10.0; combo leftover $772.36; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 111 | $6.95 | $2.32 | — | $8,760.44 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-5.8; combo leftover $772.36; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 19 | $39.99 | $2.05 | — | $7,998.58 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+9.3; combo leftover $772.36; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 4 | $189.17 | $2.00 | — | $7,239.90 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+7.9; combo leftover $772.36; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 145 | $18.61 | $2.54 | — | $9,935.80 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $2716.07; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 397 | $6.83 | $5.28 | — | $12,642.03 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $2716.07; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,642.03 | ▼ close $10,443.92 vs 09:30 $10,892.32 (session -412.53) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,642.03 | ▲ 09:30 equity $10,448.73 vs yday $10,443.92 (+4.81) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 29 | $26.51 | $2.10 | $+2.79 | $13,408.72 | ▲ +2.79 after sell → book $10,446.63; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 111 | $7.27 | $2.35 | $+30.85 | $14,213.34 | ▲ +30.85 after sell → book $10,444.28; vs 09:30 mark -2.35 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 19 | $37.57 | $2.07 | $-50.09 | $14,925.10 | ▼ -50.09 after sell → book $10,442.21; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 4 | $190.35 | $2.02 | $+0.70 | $15,684.48 | ▲ +0.70 after sell → book $10,440.19; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 4 | $170.85 | $2.00 | — | $14,999.08 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; combo leftover $784.22; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 44 | $17.72 | $2.12 | — | $14,217.28 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=-8.3; combo leftover $784.22; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 443 | $1.77 | $5.71 | — | $13,427.45 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-10.2; combo leftover $784.22; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 3 | $238.60 | $2.00 | — | $12,709.65 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.6; combo leftover $784.22; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 35 | $22.12 | $2.10 | — | $11,933.36 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+10.5; combo leftover $784.22; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 327 | $7.95 | $4.36 | — | $14,528.64 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $2606.56; owner short_news_r_h3 | — |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 32 | $81.00 | $2.19 | — | $17,118.46 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-3.0; combo leftover $2606.56; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,118.46 | ▲ close $10,653.54 vs 09:30 $10,448.73 (session +233.83) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,118.46 | ▲ 09:30 equity $10,689.17 vs yday $10,653.54 (+35.63) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 37 | $15.87 | $2.12 | $+27.60 | $17,703.53 | ▲ +27.60 after sell → book $10,687.05; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 5 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 4 | $182.33 | $2.02 | $+41.90 | $18,430.82 | ▲ +41.90 after sell → book $10,685.02; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 44 | $17.13 | $2.14 | $-30.22 | $19,182.40 | ▼ -30.22 after sell → book $10,682.88; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 443 | $1.77 | $5.80 | $-11.51 | $19,960.71 | ▼ -11.51 after sell → book $10,677.08; vs 09:30 mark -5.80 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 3 | $236.80 | $2.02 | $-9.42 | $20,669.09 | ▼ -9.42 after sell → book $10,675.06; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 110 | $14.07 | $2.32 | — | $19,119.07 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; combo leftover $1550.18; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 104 | $14.79 | $2.30 | — | $17,578.61 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; combo leftover $1550.18; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 205 | $7.54 | $2.64 | — | $16,031.29 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-20.9; combo leftover $1550.18; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 74 | $20.91 | $2.21 | — | $14,481.74 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+8.6; combo leftover $1550.18; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 154 | $34.44 | $2.67 | — | $19,782.84 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5332.79; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,782.84 | ▲ close $10,689.18 vs 09:30 $10,689.17 (session +26.26) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,782.84 | ▼ 09:30 equity $10,582.31 vs yday $10,689.18 (-106.87) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 145 | $22.11 | $2.42 | $-512.47 | $16,574.46 | ▼ -512.47 after sell → book $10,579.88; vs 09:30 mark -2.43 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 397 | $6.55 | $5.12 | $+100.76 | $13,968.99 | ▲ +100.76 after sell → book $10,574.76; vs 09:30 mark -5.12 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 35 | $22.78 | $2.12 | $+18.89 | $14,764.17 | ▲ +18.89 after sell → book $10,572.64; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 110 | $13.90 | $2.35 | $-23.37 | $16,290.82 | ▼ -23.37 after sell → book $10,570.29; vs 09:30 mark -2.35 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 104 | $14.58 | $2.33 | $-26.47 | $17,804.81 | ▼ -26.47 after sell → book $10,567.96; vs 09:30 mark -2.33 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 205 | $7.36 | $2.69 | $-41.21 | $19,310.92 | ▼ -41.21 after sell → book $10,565.27; vs 09:30 mark -2.69 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 74 | $21.65 | $2.24 | $+50.31 | $20,910.78 | ▲ +50.31 after sell → book $10,563.03; vs 09:30 mark -2.24 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 40 | $25.95 | $2.11 | — | $19,870.67 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.2; combo leftover $1045.54; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 486 | $2.15 | $6.27 | — | $18,819.51 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+7.5; combo leftover $1045.54; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 75 | $13.94 | $2.21 | — | $17,771.79 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; combo leftover $1045.54; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 174 | $6.00 | $2.51 | — | $16,725.28 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-24.1; combo leftover $1045.54; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 5 | $190.30 | $2.00 | — | $15,771.77 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+10.6; combo leftover $1045.54; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 4 | $230.25 | $2.00 | — | $14,848.77 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+12.5; combo leftover $1045.54; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 319 | $8.26 | $4.26 | — | $17,479.45 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $2636.48; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 4 | $583.88 | $2.09 | — | $19,812.88 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $2636.48; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,812.88 | ▼ close $10,128.46 vs 09:30 $10,582.31 (session -411.11) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,812.88 | ▲ 09:30 equity $10,143.09 vs yday $10,128.46 (+14.63) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 327 | $8.28 | $4.22 | $-114.86 | $17,102.73 | ▼ -114.86 after sell → book $10,138.87; vs 09:30 mark -4.22 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `MARA` | 75 | $13.13 | $2.24 | $-65.20 | $18,085.24 | ▼ -65.20 after sell → book $10,136.63; vs 09:30 mark -2.24 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 174 | $5.99 | $2.55 | $-6.80 | $19,124.95 | ▼ -6.80 after sell → book $10,134.08; vs 09:30 mark -2.55 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 946 | $1.01 | $12.20 | — | $18,157.29 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+14.3; combo leftover $956.25; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `MRNA` | 5 | $168.50 | $2.00 | — | $17,312.79 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+17.9; combo leftover $956.25; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 222 | $4.30 | $2.86 | — | $16,355.32 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; combo leftover $956.25; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 26 | $93.97 | $2.16 | — | $18,796.38 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $2529.25; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,796.38 | ▼ close $10,056.11 vs 09:30 $10,143.09 (session -58.74) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,796.38 | ▼ 09:30 equity $9,608.77 vs yday $10,056.11 (-447.34) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `LEN` | 32 | $82.00 | $2.09 | $-36.27 | $16,170.29 | ▼ -36.27 after sell → book $9,606.69; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 40 | $26.58 | $2.13 | $+20.96 | $17,231.36 | ▲ +20.96 after sell → book $9,604.56; vs 09:30 mark -2.13 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMTX` | 486 | $2.09 | $6.36 | $-41.79 | $18,240.74 | ▼ -41.79 after sell → book $9,598.20; vs 09:30 mark -6.36 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 5 | $174.50 | $2.02 | $-83.03 | $19,111.22 | ▼ -83.03 after sell → book $9,596.17; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 4 | $266.50 | $2.02 | $+140.98 | $20,175.19 | ▲ +140.98 after sell → book $9,594.15; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 946 | $0.95 | $11.99 | $-80.96 | $21,061.90 | ▼ -80.96 after sell → book $9,582.16; vs 09:30 mark -11.99 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MRNA` | 5 | $183.41 | $2.02 | $+70.50 | $21,976.90 | ▲ +70.50 after sell → book $9,580.13; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 138 | $7.95 | $2.40 | — | $20,877.40 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ⚪; ret5=+12.4; combo leftover $1098.85; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 69 | $15.72 | $2.20 | — | $19,790.52 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; combo leftover $1098.85; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 900 | $1.22 | $11.61 | — | $18,680.91 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-33.0; combo leftover $1098.85; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 845 | $1.30 | $10.90 | — | $17,571.51 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; combo leftover $1098.85; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 27 | $40.00 | $2.07 | — | $16,489.44 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+6.7; combo leftover $1098.85; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 5 | $196.78 | $2.00 | — | $15,503.53 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $1098.85; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 40 | $116.85 | $2.29 | — | $20,175.25 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $4774.47; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,175.25 | ▼ close $9,503.01 vs 09:30 $9,608.77 (session -43.65) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,175.25 | ▲ 09:30 equity $9,513.42 vs yday $9,503.01 (+10.41) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 4 | $600.27 | $2.00 | $-69.66 | $17,772.17 | ▼ -69.66 after sell → book $9,511.42; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DGXX` | 222 | $4.12 | $2.91 | $-45.73 | $18,683.90 | ▼ -45.73 after sell → book $9,508.51; vs 09:30 mark -2.91 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 138 | $7.38 | $2.44 | $-83.50 | $19,699.90 | ▼ -83.50 after sell → book $9,506.07; vs 09:30 mark -2.44 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 69 | $14.38 | $2.22 | $-96.88 | $20,689.90 | ▼ -96.88 after sell → book $9,503.85; vs 09:30 mark -2.22 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CMPX` | 900 | $1.17 | $11.77 | $-68.38 | $21,731.13 | ▼ -68.38 after sell → book $9,492.08; vs 09:30 mark -11.77 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 845 | $1.27 | $11.05 | $-47.30 | $22,793.23 | ▼ -47.30 after sell → book $9,481.03; vs 09:30 mark -11.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 27 | $39.27 | $2.09 | $-23.87 | $23,851.43 | ▼ -23.87 after sell → book $9,478.94; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 5 | $192.26 | $2.02 | $-26.63 | $24,810.70 | ▼ -26.63 after sell → book $9,476.91; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,810.70 | ▼ close $9,258.70 vs 09:30 $9,513.42 (session -218.21) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,567.71 | ▼ 09:30 equity $8,335.30 vs yday $8,363.92 (-28.62) | 09:30 open · cash $19,567.71 (unchanged overnight, no fees) · equity $8,335.30 vs prior close $8,363.92 (-28.62) | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 304 | $3.86 | $3.92 | — | $18,390.35 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; combo leftover $1174.06; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 72 | $16.21 | $2.21 | — | $17,221.02 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $1174.06; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 4 | $272.16 | $2.00 | — | $16,130.38 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+11.7; combo leftover $1174.06; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 15 | $74.15 | $2.04 | — | $15,016.10 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.5; combo leftover $1174.06; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 1 | $887.00 | $1.99 | — | $14,127.10 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+0.3; combo leftover $1174.06; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 530 | $7.85 | $7.07 | — | $18,280.53 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $4161.57; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,280.53 | ▼ close $8,271.36 vs 09:30 $8,335.30 (session -44.72) | 16:00 close · cash $18,280.53 · equity $8,271.36 vs 09:30 $8,335.30 (-63.94; session marks -44.72) · 11 name(s) marked open→close (per-name table). AEHL×290 09:30 $9.05 → close $9.36 -89.90; BAND×38 09:30 $61.83 → close $61.83 -0.00; HALO×18 09:30 $115.36 → close $113.90 +26.28; PAYX×19 09:30 $101.59 → close $101.59 +0.00; USFD×24 09:30 $93.82 → close $93.82 +0.00; ZSQR×304 09:30 $3.86 → close $3.78 -24.32; SECZ×72 09:30 $16.21 → close $15.96 -18.00; ILMN×4 09:30 $272.16 → close $270.00 -8.64; RKLB×15 09:30 $74.15 → close $73.95 -3.00; COST×1 09:30 $887.00 → close $922.76 +35.76; RSKD×530 09:30 $7.85 → close $7.78 +37.10 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 375.00 < 1 share @ 1646.93 |
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
| 2026-09-03 | `DE` | cash | leftover split 396.33 < 1 share @ 703.25 |
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
| 2026-09-17 | `LITE` | cash | leftover split 784.22 < 1 share @ 934.88 |
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
| `FIVN` | 154 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5332.79; owner short_news_r_h3 |
| `AEHL` | 319 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $2636.48; owner short_news_r_h3 |
| `USFD` | 26 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $2529.25; owner short_news_r_h3 |
| `HALO` | 40 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $4774.47; owner short_news_r_h3 |
