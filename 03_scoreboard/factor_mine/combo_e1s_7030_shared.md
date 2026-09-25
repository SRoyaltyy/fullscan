# Factor mine action — `combo_e1s_7030_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_e_fresh_h1/short_news_r_h3 w=0.7,0.3 net=priority

Cash book **-10.43%** ($8,957) · signal-only (no cash/fees) was —. Starts YES **1/30**. Fills 266 · skips 162 · realized $+1242.52.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_e_fresh_h1 70%, short_news_r_h3 30%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_e_fresh_h1 70%, short_news_r_h3 30%.
- Member: union_e_fresh_h1 (70% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,655.61.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 6172 | $0.81 | $68.51 | — | $4,932.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; combo leftover $5000.00; owner union_e_fresh_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 223 | $22.01 | $2.88 | — | $21.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; combo leftover $5000.00; owner union_e_fresh_h1 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.06 | ▲ close $10,769.53 vs 09:30 $10,000.00 (session +840.92) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.06 | ▲ 09:30 equity $10,963.61 vs yday $10,769.53 (+194.08) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 6172 | $0.93 | $76.99 | $+595.14 | $5,684.04 | ▲ +595.14 after sell → book $10,886.63; vs 09:30 mark -76.98 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 223 | $23.33 | $2.96 | $+288.53 | $10,883.67 | ▲ +288.53 after sell → book $10,883.67; vs 09:30 mark -2.96 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 634 | $1.50 | $8.18 | — | $9,924.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $952.32; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 48 | $19.57 | $2.13 | — | $8,983.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $952.32; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 85 | $11.12 | $2.25 | — | $8,035.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $952.32; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 70 | $13.55 | $2.20 | — | $7,084.86 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $952.32; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 87 | $10.83 | $2.25 | — | $6,140.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-30.1; combo leftover $952.32; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 807 | $1.18 | $10.41 | — | $5,177.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $952.32; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 49 | $19.17 | $2.14 | — | $4,236.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $952.32; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 96 | $9.89 | $2.28 | — | $3,284.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; combo leftover $952.32; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 258 | $12.70 | $3.49 | — | $6,555.88 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $3284.06; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,555.88 | ▲ close $10,989.45 vs 09:30 $10,963.61 (session +141.10) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,555.88 | ▲ 09:30 equity $11,061.98 vs yday $10,989.45 (+72.53) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 634 | $1.52 | $8.29 | $-3.79 | $7,511.27 | ▼ -3.79 after sell → book $11,053.69; vs 09:30 mark -8.29 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 48 | $19.57 | $2.15 | $-4.29 | $8,448.47 | ▼ -4.29 after sell → book $11,051.53; vs 09:30 mark -2.16 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 85 | $9.57 | $2.27 | $-136.26 | $9,259.65 | ▼ -136.26 after sell → book $11,049.26; vs 09:30 mark -2.27 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 70 | $13.16 | $2.22 | $-31.72 | $10,178.63 | ▼ -31.72 after sell → book $11,047.04; vs 09:30 mark -2.22 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 87 | $11.19 | $2.28 | $+26.79 | $11,149.89 | ▲ +26.79 after sell → book $11,044.77; vs 09:30 mark -2.27 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `EU` | 807 | $1.21 | $10.55 | $+3.25 | $12,115.80 | ▲ +3.25 after sell → book $11,034.21; vs 09:30 mark -10.56 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 49 | $20.25 | $2.16 | $+48.63 | $13,105.90 | ▲ +48.63 after sell → book $11,032.06; vs 09:30 mark -2.15 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 96 | $10.97 | $2.30 | $+98.62 | $14,156.71 | ▲ +98.62 after sell → book $11,029.75; vs 09:30 mark -2.31 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 959 | $1.15 | $12.57 | — | $15,246.99 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $1102.98; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 309 | $3.56 | $4.08 | — | $16,342.95 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $1102.98; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 34 | $31.70 | $2.14 | — | $17,418.61 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; combo leftover $1102.98; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 366 | $3.01 | $4.82 | — | $18,515.45 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $1102.98; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 162 | $6.80 | $2.54 | — | $19,614.51 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; combo leftover $1102.98; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,614.51 | ▲ close $11,085.71 vs 09:30 $11,061.98 (session +82.10) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,614.51 | ▲ 09:30 equity $11,164.77 vs yday $11,085.71 (+79.06) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,614.51 | ▲ close $11,329.10 vs 09:30 $11,164.77 (session +164.34) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,614.51 | ▼ 09:30 equity $11,244.19 vs yday $11,329.10 (-84.91) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 258 | $11.75 | $3.33 | $+236.99 | $16,579.68 | ▲ +236.99 after sell → book $11,240.86; vs 09:30 mark -3.33 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,579.68 | ▲ close $11,250.17 vs 09:30 $11,244.19 (session +9.31) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,579.68 | ▼ 09:30 equity $11,149.03 vs yday $11,250.17 (-101.14) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 959 | $0.96 | $12.11 | $+154.65 | $15,644.05 | ▲ +154.65 after sell → book $11,136.92; vs 09:30 mark -12.11 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 309 | $4.01 | $3.99 | $-148.66 | $14,399.43 | ▼ -148.66 after sell → book $11,132.93; vs 09:30 mark -3.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 34 | $31.87 | $2.09 | $-10.01 | $13,313.76 | ▼ -10.01 after sell → book $11,130.84; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 366 | $2.95 | $4.72 | $+12.42 | $12,229.34 | ▲ +12.42 after sell → book $11,126.12; vs 09:30 mark -4.72 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 162 | $6.81 | $2.48 | $-6.64 | $11,123.64 | ▼ -6.64 after sell → book $11,123.64; vs 09:30 mark -2.48 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 9 | $97.43 | $2.02 | — | $10,244.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $973.32; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 219 | $4.43 | $2.83 | — | $9,271.76 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $973.32; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 3244 | $0.30 | $19.46 | — | $8,279.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $973.32; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 20 | $46.85 | $2.05 | — | $7,340.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $973.32; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 108 | $9.01 | $2.31 | — | $6,364.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $973.32; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 250 | $3.89 | $3.23 | — | $5,388.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $973.32; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 28 | $34.05 | $2.07 | — | $4,433.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $973.32; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 43 | $22.44 | $2.12 | — | $3,466.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $973.32; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 2 | $204.45 | $2.03 | — | $3,873.28 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $577.74; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 26 | $21.40 | $2.10 | — | $4,427.58 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $577.74; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 48 | $11.81 | $2.17 | — | $4,992.53 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $577.74; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $5,512.20 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $577.74; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 5 | $106.38 | $2.04 | — | $6,042.06 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $577.74; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 125 | $4.61 | $2.41 | — | $6,615.90 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $577.74; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,615.90 | ▲ close $11,171.25 vs 09:30 $11,149.03 (session +96.48) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,615.90 | ▲ 09:30 equity $11,180.40 vs yday $11,171.25 (+9.15) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 9 | $96.75 | $2.04 | $-10.17 | $7,484.61 | ▼ -10.17 after sell → book $11,178.36; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TOYO` | 219 | $4.68 | $2.87 | $+49.05 | $8,506.66 | ▲ +49.05 after sell → book $11,175.49; vs 09:30 mark -2.87 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DVLT` | 3244 | $0.31 | $20.34 | $-7.36 | $9,491.96 | ▼ -7.36 after sell → book $11,155.15; vs 09:30 mark -20.34 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AEG` | 108 | $9.04 | $2.34 | $-1.42 | $10,465.94 | ▼ -1.42 after sell → book $11,152.81; vs 09:30 mark -2.34 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALVO` | 250 | $4.32 | $3.28 | $+101.00 | $11,542.66 | ▲ +101.00 after sell → book $11,149.53; vs 09:30 mark -3.28 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 28 | $34.31 | $2.09 | $+3.11 | $12,501.25 | ▲ +3.11 after sell → book $11,147.44; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 43 | $22.20 | $2.14 | $-14.58 | $13,453.71 | ▼ -14.58 after sell → book $11,145.30; vs 09:30 mark -2.14 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 13 | $115.18 | $2.03 | — | $11,954.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $1569.60; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $10,705.83 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list probable,yday_gainer; 🔵; ret5=+1.4; combo leftover $1569.60; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 87 | $17.93 | $2.25 | — | $9,143.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $1569.60; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 16 | $93.98 | $2.04 | — | $7,637.51 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; combo leftover $1569.60; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 36 | $43.08 | $2.10 | — | $6,084.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $1569.60; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 682 | $2.30 | $8.80 | — | $4,507.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $1569.60; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 241 | $3.11 | $3.18 | — | $5,253.47 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $751.19; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 5 | $133.11 | $2.04 | — | $5,916.97 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; combo leftover $751.19; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 8 | $89.10 | $2.05 | — | $6,627.72 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $751.19; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 19 | $38.40 | $2.09 | — | $7,355.23 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $751.19; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 35 | $20.90 | $2.14 | — | $8,084.60 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $751.19; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 27 | $27.00 | $2.11 | — | $8,811.49 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $751.19; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,811.49 | ▲ close $11,357.68 vs 09:30 $11,180.40 (session +245.20) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,811.49 | ▲ 09:30 equity $11,431.73 vs yday $11,357.68 (+74.05) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 20 | $43.05 | $2.07 | $-80.12 | $9,670.42 | ▼ -80.12 after sell → book $11,429.66; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 13 | $121.00 | $2.05 | $+71.58 | $11,241.36 | ▲ +71.58 after sell → book $11,427.61; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $12,545.43 | ▲ +55.55 after sell → book $11,425.59; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 87 | $18.05 | $2.28 | $+5.91 | $14,113.94 | ▲ +5.91 after sell → book $11,423.32; vs 09:30 mark -2.27 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 16 | $97.02 | $2.06 | $+44.54 | $15,664.20 | ▲ +44.54 after sell → book $11,421.26; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 36 | $44.22 | $2.12 | $+36.82 | $17,253.99 | ▲ +36.82 after sell → book $11,419.13; vs 09:30 mark -2.13 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 682 | $2.34 | $8.92 | $+9.56 | $18,840.95 | ▲ +9.56 after sell → book $11,410.21; vs 09:30 mark -8.92 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,840.95 | ▲ close $11,454.34 vs 09:30 $11,431.73 (session +44.13) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,840.95 | ▲ 09:30 equity $11,494.32 vs yday $11,454.34 (+39.98) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 2 | $212.00 | $2.00 | $-19.12 | $18,414.95 | ▼ -19.12 after sell → book $11,492.32; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 26 | $20.90 | $2.07 | $+8.83 | $17,869.49 | ▲ +8.83 after sell → book $11,490.26; vs 09:30 mark -2.06 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 48 | $11.00 | $2.13 | $+34.82 | $17,339.35 | ▲ +34.82 after sell → book $11,488.12; vs 09:30 mark -2.14 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 3 | $170.64 | $2.00 | $+5.75 | $16,825.43 | ▲ +5.75 after sell → book $11,486.12; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 5 | $105.58 | $2.00 | $-0.04 | $16,295.53 | ▼ -0.04 after sell → book $11,484.12; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 125 | $4.77 | $2.37 | $-24.78 | $15,696.91 | ▼ -24.78 after sell → book $11,481.75; vs 09:30 mark -2.37 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 7 | $175.01 | $2.01 | — | $14,469.83 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; combo leftover $1373.48; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 15 | $88.94 | $2.04 | — | $13,133.70 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $1373.48; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 89 | $15.28 | $2.26 | — | $11,771.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $1373.48; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 9 | $142.36 | $2.02 | — | $10,488.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $1373.48; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 269 | $5.10 | $3.47 | — | $9,112.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $1373.48; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 28 | $47.89 | $2.07 | — | $7,769.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $1373.48; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 98 | $13.92 | $2.28 | — | $6,403.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $1373.48; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 302 | $4.54 | $3.90 | — | $5,026.97 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $1373.48; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 122 | $13.62 | $2.44 | — | $6,686.78 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $1675.66; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 30 | $54.51 | $2.15 | — | $8,319.94 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $1675.66; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 4 | $364.35 | $2.06 | — | $9,775.27 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $1675.66; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,775.27 | ▼ close $10,966.23 vs 09:30 $11,494.32 (session -488.83) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,775.27 | ▲ 09:30 equity $11,145.95 vs yday $10,966.23 (+179.72) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 241 | $2.83 | $3.11 | $+61.19 | $9,090.13 | ▲ +61.19 after sell → book $11,142.84; vs 09:30 mark -3.11 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 5 | $154.20 | $2.00 | $-109.50 | $8,317.13 | ▼ -109.50 after sell → book $11,140.84; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 8 | $88.24 | $2.01 | $+2.81 | $7,609.20 | ▲ +2.81 after sell → book $11,138.83; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 19 | $38.41 | $2.05 | $-4.32 | $6,877.36 | ▼ -4.32 after sell → book $11,136.78; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 35 | $20.50 | $2.10 | $+9.77 | $6,157.76 | ▲ +9.77 after sell → book $11,134.68; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 27 | $26.00 | $2.07 | $+22.82 | $5,453.69 | ▲ +22.82 after sell → book $11,132.61; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMO` | 7 | $173.22 | $2.03 | $-16.57 | $6,664.20 | ▼ -16.57 after sell → book $11,130.58; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BNS` | 15 | $92.65 | $2.06 | $+51.56 | $8,051.90 | ▲ +51.56 after sell → book $11,128.53; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EH` | 269 | $4.77 | $3.52 | $-95.77 | $9,331.50 | ▼ -95.77 after sell → book $11,125.00; vs 09:30 mark -3.53 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GFI` | 28 | $48.24 | $2.09 | $+5.63 | $10,680.13 | ▲ +5.63 after sell → book $11,122.91; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 98 | $14.03 | $2.31 | $+6.18 | $12,052.75 | ▲ +6.18 after sell → book $11,120.59; vs 09:30 mark -2.32 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SHMD` | 302 | $3.38 | $3.96 | $-359.68 | $13,069.56 | ▼ -359.68 after sell → book $11,116.64; vs 09:30 mark -3.95 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 2615 | $0.58 | $23.09 | — | $11,521.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $1524.78; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 292 | $5.21 | $3.77 | — | $9,996.84 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $1524.78; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 11 | $131.37 | $2.02 | — | $8,549.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; combo leftover $1524.78; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 83 | $18.26 | $2.24 | — | $7,031.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $1524.78; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 44 | $34.30 | $2.12 | — | $5,520.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; combo leftover $1524.78; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 4 | $326.91 | $2.00 | — | $4,210.96 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-15.2; combo leftover $1524.78; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 3 | $213.94 | $2.04 | — | $4,850.74 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $842.19; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 68 | $12.22 | $2.24 | — | $5,679.47 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $842.19; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 165 | $5.08 | $2.55 | — | $6,515.12 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $842.19; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 6 | $132.64 | $2.05 | — | $7,308.91 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $842.19; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 4 | $199.94 | $2.04 | — | $8,106.63 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; combo leftover $842.19; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,106.63 | ▲ close $11,267.01 vs 09:30 $11,145.95 (session +196.53) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,106.63 | ▼ 09:30 equity $11,030.79 vs yday $11,267.01 (-236.22) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 89 | $18.50 | $2.28 | $+282.04 | $9,750.84 | ▲ +282.04 after sell → book $11,028.50; vs 09:30 mark -2.29 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 9 | $128.73 | $2.04 | $-126.72 | $10,907.37 | ▼ -126.72 after sell → book $11,026.46; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 2615 | $0.53 | $22.15 | $-183.84 | $12,271.17 | ▼ -183.84 after sell → book $11,004.31; vs 09:30 mark -22.15 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 292 | $5.49 | $3.83 | $+74.17 | $13,870.43 | ▲ +74.17 after sell → book $11,000.49; vs 09:30 mark -3.82 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ANF` | 11 | $144.70 | $2.05 | $+142.56 | $15,460.08 | ▲ +142.56 after sell → book $10,998.44; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BBWI` | 83 | $18.69 | $2.27 | $+31.19 | $17,009.09 | ▲ +31.19 after sell → book $10,996.18; vs 09:30 mark -2.26 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BOX` | 44 | $33.79 | $2.14 | $-26.71 | $18,493.70 | ▼ -26.71 after sell → book $10,994.03; vs 09:30 mark -2.15 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DY` | 4 | $314.90 | $2.02 | $-52.06 | $19,751.28 | ▼ -52.06 after sell → book $10,992.01; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 21 | $80.60 | $2.05 | — | $18,056.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.0; combo leftover $1728.24; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 106 | $16.18 | $2.31 | — | $16,339.24 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.7; combo leftover $1728.24; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 14 | $118.77 | $2.03 | — | $14,674.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.3; combo leftover $1728.24; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 97 | $17.78 | $2.28 | — | $12,947.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.2; combo leftover $1728.24; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 128 | $13.41 | $2.37 | — | $11,228.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; combo leftover $1728.24; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 17 | $97.16 | $2.04 | — | $9,574.87 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; combo leftover $1728.24; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 8 | $206.82 | $2.01 | — | $7,918.30 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.2; combo leftover $1728.24; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 14 | $120.17 | $2.03 | — | $6,233.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $1728.24; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 36 | $74.54 | $2.20 | — | $8,915.12 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $2743.72; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 49 | $55.25 | $2.24 | — | $11,620.13 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $2743.72; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,620.13 | ▲ close $11,022.73 vs 09:30 $11,030.79 (session +52.30) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,620.13 | ▲ 09:30 equity $11,048.50 vs yday $11,022.73 (+25.77) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 122 | $13.90 | $2.36 | $-38.34 | $9,921.97 | ▼ -38.34 after sell → book $11,046.15; vs 09:30 mark -2.35 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 30 | $52.49 | $2.08 | $+56.37 | $8,345.19 | ▲ +56.37 after sell → book $11,044.07; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 4 | $347.82 | $2.00 | $+62.05 | $6,951.91 | ▲ +62.05 after sell → book $11,042.07; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BBY` | 21 | $83.85 | $2.08 | $+64.12 | $8,710.68 | ▲ +64.12 after sell → book $11,039.99; vs 09:30 mark -2.08 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BILI` | 106 | $16.94 | $2.34 | $+75.91 | $10,503.98 | ▲ +75.91 after sell → book $11,037.65; vs 09:30 mark -2.34 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 14 | $115.66 | $2.06 | $-47.63 | $12,121.17 | ▼ -47.63 after sell → book $11,035.59; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CMBT` | 97 | $18.58 | $2.31 | $+73.01 | $13,921.12 | ▲ +73.01 after sell → book $11,033.28; vs 09:30 mark -2.31 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CSIQ` | 128 | $13.65 | $2.41 | $+25.94 | $15,665.91 | ▲ +25.94 after sell → book $11,030.87; vs 09:30 mark -2.41 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `HQY` | 17 | $93.62 | $2.06 | $-64.28 | $17,255.38 | ▼ -64.28 after sell → book $11,028.81; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RY` | 8 | $205.50 | $2.04 | $-14.61 | $18,897.35 | ▼ -14.61 after sell → book $11,026.77; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TD` | 14 | $122.07 | $2.06 | $+22.51 | $20,604.27 | ▲ +22.51 after sell → book $11,024.72; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 6 | $261.16 | $2.01 | — | $19,035.30 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; combo leftover $1802.87; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 120 | $15.01 | $2.35 | — | $17,231.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $1802.87; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 17 | $103.89 | $2.04 | — | $15,463.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $1802.87; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 464 | $3.88 | $5.99 | — | $13,657.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $1802.87; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 40 | $44.40 | $2.11 | — | $11,879.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $1802.87; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 73 | $24.69 | $2.21 | — | $10,074.59 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $1802.87; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 215 | $8.35 | $2.77 | — | $8,276.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $1802.87; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 47 | $37.65 | $2.13 | — | $6,505.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $1802.87; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 10 | $252.24 | $2.12 | — | $9,025.40 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $2750.78; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 91 | $30.18 | $2.38 | — | $11,769.40 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $2750.78; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,769.40 | ▼ close $10,844.46 vs 09:30 $11,048.50 (session -154.15) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,769.40 | ▲ 09:30 equity $10,947.89 vs yday $10,844.46 (+103.43) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 3 | $208.88 | $2.00 | $+11.14 | $11,140.76 | ▲ +11.14 after sell → book $10,945.89; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 68 | $11.10 | $2.19 | $+71.73 | $10,383.77 | ▲ +71.73 after sell → book $10,943.70; vs 09:30 mark -2.19 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 165 | $4.97 | $2.48 | $+12.29 | $9,560.41 | ▲ +12.29 after sell → book $10,941.21; vs 09:30 mark -2.49 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 6 | $127.45 | $2.01 | $+27.08 | $8,793.70 | ▲ +27.08 after sell → book $10,939.20; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 4 | $254.39 | $2.00 | $-221.85 | $7,774.14 | ▼ -221.85 after sell → book $10,937.20; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 6 | $257.71 | $2.03 | $-24.74 | $9,318.37 | ▼ -24.74 after sell → book $10,935.17; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBAR` | 120 | $14.88 | $2.38 | $-20.33 | $11,101.59 | ▼ -20.33 after sell → book $10,932.79; vs 09:30 mark -2.38 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 17 | $98.00 | $2.06 | $-104.24 | $12,765.52 | ▼ -104.24 after sell → book $10,930.72; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 464 | $3.39 | $6.08 | $-239.42 | $14,332.41 | ▼ -239.42 after sell → book $10,924.65; vs 09:30 mark -6.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 40 | $44.85 | $2.13 | $+13.76 | $16,124.27 | ▲ +13.76 after sell → book $10,922.51; vs 09:30 mark -2.14 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 73 | $22.98 | $2.23 | $-129.27 | $17,799.58 | ▼ -129.27 after sell → book $10,920.28; vs 09:30 mark -2.23 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 215 | $8.53 | $2.82 | $+33.10 | $19,630.70 | ▲ +33.10 after sell → book $10,917.45; vs 09:30 mark -2.83 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `IREN` | 47 | $35.81 | $2.15 | $-90.53 | $21,311.62 | ▼ -90.53 after sell → book $10,915.30; vs 09:30 mark -2.15 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,311.62 | ▲ close $10,969.85 vs 09:30 $10,947.89 (session +54.55) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,311.62 | ▲ 09:30 equity $11,129.10 vs yday $10,969.85 (+159.25) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 36 | $73.22 | $2.10 | $+43.22 | $18,673.60 | ▲ +43.22 after sell → book $11,127.00; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 49 | $54.76 | $2.14 | $+19.63 | $15,988.22 | ▲ +19.63 after sell → book $11,124.86; vs 09:30 mark -2.14 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,988.22 | ▲ close $11,139.52 vs 09:30 $11,129.10 (session +14.66) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,988.22 | ▲ 09:30 equity $11,194.14 vs yday $11,139.52 (+54.62) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 10 | $235.71 | $2.02 | $+161.16 | $13,629.10 | ▲ +161.16 after sell → book $11,192.12; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 91 | $26.78 | $2.26 | $+304.76 | $11,189.86 | ▲ +304.76 after sell → book $11,189.86; vs 09:30 mark -2.26 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,189.86 | ▲ close $11,189.86 vs 09:30 $11,194.14 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,189.86 | ▲ 09:30 equity $11,189.86 vs yday $11,189.86 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 91 | $10.74 | $2.26 | — | $10,209.80 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $979.11; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 2 | $351.74 | $2.00 | — | $9,504.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $979.11; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 141 | $6.90 | $2.41 | — | $8,529.01 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $979.11; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 2 | $354.49 | $2.00 | — | $7,818.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $979.11; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 43 | $22.32 | $2.12 | — | $6,856.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $979.11; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 3 | $257.00 | $2.00 | — | $6,083.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $979.11; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 20 | $47.60 | $2.05 | — | $5,129.11 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $979.11; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 64 | $15.09 | $2.18 | — | $4,161.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $979.11; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 140 | $14.85 | $2.51 | — | $6,237.66 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $2080.58; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1216 | $1.71 | $15.96 | — | $8,301.06 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $2080.58; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,301.06 | ▲ close $11,634.87 vs 09:30 $11,189.86 (session +480.50) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,301.06 | ▲ 09:30 equity $11,709.15 vs yday $11,634.87 (+74.28) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 91 | $10.91 | $2.29 | $+10.46 | $9,291.58 | ▲ +10.46 after sell → book $11,706.86; vs 09:30 mark -2.29 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 2 | $359.70 | $2.02 | $+11.91 | $10,008.97 | ▲ +11.91 after sell → book $11,704.85; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 141 | $9.28 | $2.45 | $+330.72 | $11,315.00 | ▲ +330.72 after sell → book $11,702.40; vs 09:30 mark -2.45 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 2 | $321.67 | $2.02 | $-69.65 | $11,956.32 | ▼ -69.65 after sell → book $11,700.38; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CPB` | 43 | $22.10 | $2.14 | $-13.72 | $12,904.48 | ▼ -13.72 after sell → book $11,698.24; vs 09:30 mark -2.14 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 3 | $238.88 | $2.02 | $-58.38 | $13,619.10 | ▼ -58.38 after sell → book $11,696.22; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 20 | $53.85 | $2.07 | $+120.88 | $14,694.03 | ▲ +120.88 after sell → book $11,694.15; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 64 | $15.34 | $2.20 | $+11.62 | $15,673.59 | ▲ +11.62 after sell → book $11,691.95; vs 09:30 mark -2.20 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 21 | $63.18 | $2.05 | — | $14,344.76 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $1371.44; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 156 | $8.74 | $2.46 | — | $12,978.86 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $1371.44; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 20 | $68.52 | $2.05 | — | $11,606.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $1371.44; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 379 | $3.62 | $4.89 | — | $10,231.44 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $1371.44; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 8 | $167.55 | $2.01 | — | $8,889.02 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $1371.44; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 30 | $44.90 | $2.08 | — | $7,539.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $1371.44; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 13 | $98.15 | $2.03 | — | $6,261.96 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $1371.44; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 87 | $15.70 | $2.25 | — | $4,893.81 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $1371.44; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 523 | $4.67 | $6.92 | — | $7,329.30 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $2446.91; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 31 | $76.55 | $2.18 | — | $9,700.18 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $2446.91; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,700.18 | ▼ close $11,498.67 vs 09:30 $11,709.15 (session -164.37) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,700.18 | ▼ 09:30 equity $11,463.98 vs yday $11,498.67 (-34.69) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `AMBA` | 21 | $63.83 | $2.07 | $+9.52 | $11,038.53 | ▲ +9.52 after sell → book $11,461.90; vs 09:30 mark -2.08 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASAN` | 156 | $8.73 | $2.49 | $-6.51 | $12,397.92 | ▼ -6.51 after sell → book $11,459.41; vs 09:30 mark -2.49 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 20 | $67.05 | $2.07 | $-33.52 | $13,736.85 | ▼ -33.52 after sell → book $11,457.34; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOMO` | 379 | $3.84 | $4.96 | $+75.42 | $15,187.24 | ▲ +75.42 after sell → book $11,452.37; vs 09:30 mark -4.97 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 8 | $160.52 | $2.03 | $-60.29 | $16,469.37 | ▼ -60.29 after sell → book $11,450.34; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 30 | $39.56 | $2.10 | $-164.38 | $17,654.07 | ▼ -164.38 after sell → book $11,448.24; vs 09:30 mark -2.10 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 13 | $100.58 | $2.05 | $+27.51 | $18,959.56 | ▲ +27.51 after sell → book $11,446.19; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MAMA` | 87 | $15.20 | $2.28 | $-48.03 | $20,279.68 | ▼ -48.03 after sell → book $11,443.91; vs 09:30 mark -2.28 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,279.68 | ▲ close $11,668.14 vs 09:30 $11,463.98 (session +224.23) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,279.68 | ▲ 09:30 equity $11,696.00 vs yday $11,668.14 (+27.86) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 140 | $13.60 | $2.41 | $+170.08 | $18,373.27 | ▲ +170.08 after sell → book $11,693.59; vs 09:30 mark -2.41 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1216 | $1.58 | $15.69 | $+126.43 | $16,436.31 | ▲ +126.43 after sell → book $11,677.91; vs 09:30 mark -15.68 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,436.31 | ▲ close $11,702.28 vs 09:30 $11,696.00 (session +24.37) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,436.31 | ▲ 09:30 equity $11,775.54 vs yday $11,702.28 (+73.26) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 523 | $4.36 | $6.75 | $+148.46 | $14,149.28 | ▲ +148.46 after sell → book $11,768.79; vs 09:30 mark -6.75 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 31 | $76.79 | $2.08 | $-11.70 | $11,766.71 | ▼ -11.70 after sell → book $11,766.71; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,766.71 | ▲ close $11,766.71 vs 09:30 $11,775.54 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,766.71 | ▲ 09:30 equity $11,766.71 vs yday $11,766.71 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 6 | $164.43 | $2.01 | — | $10,778.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $1029.59; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 174 | $5.91 | $2.51 | — | $9,747.27 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; combo leftover $1029.59; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 4 | $242.17 | $2.00 | — | $8,776.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; combo leftover $1029.59; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 32 | $32.01 | $2.09 | — | $7,750.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $1029.59; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 14 | $71.71 | $2.03 | — | $6,744.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; combo leftover $1029.59; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 18 | $56.02 | $2.04 | — | $5,733.80 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; combo leftover $1029.59; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 109 | $9.37 | $2.32 | — | $4,710.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $1029.59; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 78 | $13.10 | $2.22 | — | $3,686.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; combo leftover $1029.59; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 6 | $112.83 | $2.05 | — | $4,361.10 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $737.23; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 209 | $3.52 | $2.76 | — | $5,094.01 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $737.23; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 363 | $2.03 | $4.77 | — | $5,826.13 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $737.23; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 29 | $24.97 | $2.12 | — | $6,548.14 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $737.23; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 218 | $3.37 | $2.88 | — | $7,279.93 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $737.23; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,279.93 | ▲ close $11,772.30 vs 09:30 $11,766.71 (session +37.39) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,279.93 | ▲ 09:30 equity $11,806.07 vs yday $11,772.30 (+33.77) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 6 | $141.42 | $2.03 | $-142.10 | $8,126.42 | ▼ -142.10 after sell → book $11,804.04; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DBI` | 174 | $5.86 | $2.55 | $-13.76 | $9,143.51 | ▼ -13.76 after sell → book $11,801.49; vs 09:30 mark -2.55 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 4 | $261.51 | $2.02 | $+73.34 | $10,187.52 | ▲ +73.34 after sell → book $11,799.46; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CPRT` | 32 | $30.63 | $2.11 | $-48.35 | $11,165.58 | ▼ -48.35 after sell → book $11,797.36; vs 09:30 mark -2.10 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DSGX` | 14 | $77.68 | $2.05 | $+79.50 | $12,251.05 | ▲ +79.50 after sell → book $11,795.31; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `KR` | 18 | $59.31 | $2.06 | $+55.11 | $13,316.56 | ▲ +55.11 after sell → book $11,793.24; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `LPTH` | 109 | $8.85 | $2.35 | $-61.34 | $14,278.87 | ▼ -61.34 after sell → book $11,790.90; vs 09:30 mark -2.34 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `REF` | 78 | $14.16 | $2.25 | $+78.21 | $15,381.10 | ▲ +78.21 after sell → book $11,788.65; vs 09:30 mark -2.25 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,381.10 | ▼ close $11,693.38 vs 09:30 $11,806.07 (session -95.27) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,381.10 | ▼ 09:30 equity $11,693.37 vs yday $11,693.38 (-0.01) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,381.10 | ▼ close $11,632.01 vs 09:30 $11,693.37 (session -61.36) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,381.10 | ▲ 09:30 equity $11,642.97 vs yday $11,632.01 (+10.96) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 6 | $118.18 | $2.01 | $-36.12 | $14,670.01 | ▼ -36.12 after sell → book $11,640.96; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 209 | $3.98 | $2.70 | $-101.60 | $13,835.50 | ▼ -101.60 after sell → book $11,638.27; vs 09:30 mark -2.69 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 363 | $1.85 | $4.68 | $+55.88 | $13,159.26 | ▲ +55.88 after sell → book $11,633.58; vs 09:30 mark -4.69 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 29 | $24.42 | $2.08 | $+11.76 | $12,449.01 | ▲ +11.76 after sell → book $11,631.51; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 218 | $3.75 | $2.81 | $-88.53 | $11,628.69 | ▼ -88.53 after sell → book $11,628.69; vs 09:30 mark -2.82 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 122 | $33.14 | $2.36 | — | $7,583.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; combo leftover $4070.04; owner union_e_fresh_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 99 | $40.93 | $2.29 | — | $3,528.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $4070.04; owner union_e_fresh_h1 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 94 | $18.61 | $2.35 | — | $5,275.89 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $1764.45; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 258 | $6.83 | $3.43 | — | $7,034.60 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $1764.45; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,034.60 | ▼ close $11,528.31 vs 09:30 $11,642.97 (session -89.96) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,034.60 | ▲ 09:30 equity $11,774.45 vs yday $11,528.31 (+246.14) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `FPS` | 122 | $36.76 | $2.41 | $+436.87 | $11,516.91 | ▲ +436.87 after sell → book $11,772.04; vs 09:30 mark -2.41 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TCOM` | 99 | $40.79 | $2.34 | $-18.48 | $15,552.78 | ▼ -18.48 after sell → book $11,769.70; vs 09:30 mark -2.34 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 485 | $11.21 | $6.26 | — | $10,109.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.0; combo leftover $5443.47; owner union_e_fresh_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 67 | $81.00 | $2.19 | — | $4,680.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.0; combo leftover $5443.47; owner union_e_fresh_h1 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 588 | $7.95 | $7.85 | — | $9,347.24 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $4680.48; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,347.24 | ▲ close $12,020.28 vs 09:30 $11,774.45 (session +266.87) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,347.24 | ▼ 09:30 equity $11,904.27 vs yday $12,020.28 (-116.01) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ALMU` | 485 | $11.64 | $6.38 | $+195.91 | $14,986.25 | ▲ +195.91 after sell → book $11,897.88; vs 09:30 mark -6.39 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LEN` | 67 | $78.25 | $2.24 | $-188.69 | $20,226.76 | ▼ -188.69 after sell → book $11,895.64; vs 09:30 mark -2.24 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 172 | $34.44 | $2.74 | — | $26,147.69 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5947.82; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26,147.69 | ▲ close $11,935.91 vs 09:30 $11,904.27 (session +43.02) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26,147.69 | ▼ 09:30 equity $11,664.29 vs yday $11,935.91 (-271.62) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 94 | $22.11 | $2.27 | $-333.62 | $24,067.08 | ▼ -333.62 after sell → book $11,662.02; vs 09:30 mark -2.27 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 258 | $6.55 | $3.33 | $+65.48 | $22,373.85 | ▲ +65.48 after sell → book $11,658.69; vs 09:30 mark -3.33 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 352 | $8.26 | $4.70 | — | $25,276.67 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $2914.67; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 4 | $583.88 | $2.09 | — | $27,610.10 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $2914.67; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27,610.10 | ▼ close $11,495.46 vs 09:30 $11,664.29 (session -156.44) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27,610.10 | ▲ 09:30 equity $11,516.56 vs yday $11,495.46 (+21.10) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 588 | $8.28 | $7.59 | $-206.53 | $22,736.81 | ▼ -206.53 after sell → book $11,508.97; vs 09:30 mark -7.59 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 30 | $93.97 | $2.19 | — | $25,553.72 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $2877.24; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25,553.72 | ▼ close $11,439.18 vs 09:30 $11,516.56 (session -67.60) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25,553.72 | ▼ 09:30 equity $10,874.72 vs yday $11,439.18 (-564.46) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 75 | $47.57 | $2.21 | — | $21,983.76 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $3577.52; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 18 | $196.78 | $2.04 | — | $18,439.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $3577.52; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 100 | $35.74 | $2.29 | — | $14,863.38 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $3577.52; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 75 | $47.15 | $2.21 | — | $11,324.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $3577.52; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 32 | $109.67 | $2.09 | — | $7,813.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $3577.52; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 46 | $116.85 | $2.33 | — | $13,186.16 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $5431.94; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,186.16 | ▼ close $10,841.05 vs 09:30 $10,874.72 (session -20.49) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,186.16 | ▲ 09:30 equity $10,938.95 vs yday $10,841.05 (+97.90) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 4 | $600.27 | $2.00 | $-69.66 | $10,783.08 | ▼ -69.66 after sell → book $10,936.95; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `CBRL` | 75 | $46.88 | $2.26 | $-56.22 | $14,296.83 | ▼ -56.22 after sell → book $10,934.69; vs 09:30 mark -2.26 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 18 | $192.26 | $2.08 | $-85.49 | $17,755.43 | ▼ -85.49 after sell → book $10,932.61; vs 09:30 mark -2.08 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `GIS` | 100 | $35.96 | $2.34 | $+17.37 | $21,349.09 | ▲ +17.37 after sell → book $10,930.27; vs 09:30 mark -2.34 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `KBH` | 75 | $47.14 | $2.26 | $-5.22 | $24,882.33 | ▼ -5.22 after sell → book $10,928.02; vs 09:30 mark -2.25 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PAYX` | 32 | $105.49 | $2.12 | $-137.90 | $28,255.96 | ▼ -137.90 after sell → book $10,925.90; vs 09:30 mark -2.12 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28,255.96 | ▼ close $10,681.80 vs 09:30 $10,938.95 (session -244.10) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,746.52 | ▼ 09:30 equity $8,456.25 vs yday $8,485.05 (-28.80) | 09:30 open · cash $19,746.52 (unchanged overnight, no fees) · equity $8,456.25 vs prior close $8,485.05 (-28.80) | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 15 | $887.00 | $2.04 | — | $6,439.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+0.3; combo leftover $13822.56; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 538 | $7.85 | $7.18 | — | $10,655.61 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $4227.11; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,655.61 | ▲ close $8,956.93 vs 09:30 $8,456.25 (session +509.89) | 16:00 close · cash $10,655.61 · equity $8,956.93 vs 09:30 $8,456.25 (+500.68; session marks +509.89) · 7 name(s) marked open→close (per-name table). AEHL×292 09:30 $9.05 → close $9.36 -90.52; BAND×37 09:30 $61.83 → close $61.83 -0.00; HALO×18 09:30 $115.36 → close $113.90 +26.28; PAYX×20 09:30 $101.59 → close $101.59 +0.00; USFD×24 09:30 $93.82 → close $93.82 +0.00; COST×15 09:30 $887.00 → close $922.76 +536.47; RSKD×538 09:30 $7.85 → close $7.78 +37.66 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `RNW` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `LUNR` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-21 | `AEM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WYFI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AEM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WYFI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `QTRX` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `MRNA` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AUGO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `SSRM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `ARIS` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_e_fresh_h1 |
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
| 2026-08-26 | `INTU` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
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
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new long union_e_fresh_h1 |
| 2026-09-01 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-04 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `OPK` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-09 | `GSM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new short short_news_r_h3 |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-14 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-15 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `MYGN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-17 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `BBNX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-21 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `FIVN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-23 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-23 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-24 | `HALO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FIVN` | 172 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5947.82; owner short_news_r_h3 |
| `AEHL` | 352 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $2914.67; owner short_news_r_h3 |
| `USFD` | 30 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $2877.24; owner short_news_r_h3 |
| `HALO` | 46 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $5431.94; owner short_news_r_h3 |
