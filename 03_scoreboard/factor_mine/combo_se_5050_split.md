# Factor mine action — `combo_se_5050_split`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · split short_news_r_h3/union_e_fresh_h3 w=0.5,0.5 net=priority

Cash book **-8.87%** ($9,113) · signal-only (no cash/fees) was —. Starts YES **21/30**. Fills 218 · skips 315 · realized $+2068.42.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell, each with their own slice of $10,000: short_news_r_h3 50%, union_e_fresh_h3 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. Each kid gets their own slice of the $10,000 and keeps it — two (or three) tiny books added together. They do not share leftover cash, so the same name can appear in two slices. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

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
- One ticker, one side. Claim order: fresh-E, then heat, then the other longs, then shorts. A name already held cannot be opened on the other side.
- Split pile: each member is a normal leftover book at its weight × $10k. Unused cash in one slice stays in that slice.
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $12,723.53.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3086 | $0.81 | $34.25 | — | $2,466.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 111 | $22.01 | $2.32 | — | $20.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; leftover $2500.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,000.00 | ▲ close $5,000.00 vs 09:30 $5,000.00 (session +0.00) | — | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.65 | ▲ close $5,383.24 vs 09:30 $5,000.00 (session +419.82) | — | — |
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,000.00 | ▲ 09:30 equity $5,000.00 vs yday $5,000.00 (+0.00) | — | — |
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,000.00 | ▲ 09:30 equity $5,000.00 vs yday $5,000.00 (+0.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 1 | $1.50 | $0.02 | — | $19.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; leftover $2.58 | — |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 2 | $1.18 | $0.03 | — | $16.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $2.58 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,468.92 | ▲ close $5,002.93 vs 09:30 $5,000.00 (session +16.57) | — | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.74 | ▲ close $5,940.80 vs 09:30 $5,480.26 (session +460.59) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,000.00 | ▲ 09:30 equity $5,000.00 vs yday $5,000.00 (+0.00) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.65 | ▲ 09:30 equity $5,480.26 vs yday $5,383.24 (+97.02) | — | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 706 | $1.18 | $9.26 | — | $5,823.82 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $833.33 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 43 | $19.17 | $2.16 | — | $6,645.97 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $833.33 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 65 | $12.70 | $2.23 | — | $7,468.92 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; leftover $833.33 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,902.06 | ▲ close $5,005.72 vs 09:30 $4,956.11 (session +64.55) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.74 | ▲ close $6,123.61 vs 09:30 $5,865.71 (session +257.90) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,468.92 | ▼ 09:30 equity $4,956.11 vs yday $5,002.93 (-46.82) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.74 | ▼ 09:30 equity $5,865.71 vs yday $5,940.80 (-75.09) | — | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 15 | $31.70 | $2.07 | — | $8,923.59 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; leftover $495.61 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 164 | $3.01 | $2.53 | — | $9,414.70 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; leftover $495.61 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 72 | $6.80 | $2.24 | — | $9,902.06 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; leftover $495.61 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 430 | $1.15 | $5.64 | — | $7,957.78 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; leftover $495.61 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 139 | $3.56 | $2.45 | — | $8,450.16 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; leftover $495.61 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,902.06 | ▲ close $5,199.48 vs 09:30 $5,081.15 (session +118.33) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,025.09 | ▼ close $6,028.68 vs 09:30 $6,071.60 (session -0.21) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,902.06 | ▲ 09:30 equity $5,081.15 vs yday $5,005.72 (+75.43) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.74 | ▼ 09:30 equity $6,071.60 vs yday $6,123.61 (-52.01) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 3086 | $1.14 | $40.35 | $+943.78 | $3,494.43 | ▲ +943.78 after sell → book $6,031.25; vs 09:30 mark -40.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 111 | $22.82 | $2.36 | $+85.23 | $6,025.09 | ▲ +85.23 after sell → book $6,028.89; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,371.60 | ▲ close $5,196.35 vs 09:30 $5,183.76 (session +23.88) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,028.57 | ▲ close $6,028.57 vs 09:30 $6,028.65 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 706 | $1.07 | $9.11 | $+59.30 | $9,137.53 | ▲ +59.30 after sell → book $5,174.65; vs 09:30 mark -9.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 65 | $11.75 | $2.19 | $+57.01 | $8,371.60 | ▲ +57.01 after sell → book $5,172.47; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,902.06 | ▼ 09:30 equity $5,183.76 vs yday $5,199.48 (-15.72) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,025.09 | ▼ 09:30 equity $6,028.65 vs yday $6,028.68 (-0.03) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 1 | $1.42 | $0.04 | $-0.14 | $6,026.48 | ▼ -0.14 after sell → book $6,028.62; vs 09:30 mark -0.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `EU` | 2 | $1.07 | $0.05 | $-0.30 | $6,028.57 | ▼ -0.30 after sell → book $6,028.57; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 16 | $46.85 | $2.04 | — | $3,068.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; leftover $753.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 83 | $9.01 | $2.24 | — | $2,318.87 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $753.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 193 | $3.89 | $2.57 | — | $1,565.54 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; leftover $753.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 22 | $34.05 | $2.06 | — | $814.38 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; leftover $753.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 33 | $22.44 | $2.09 | — | $71.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; leftover $753.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 2511 | $0.30 | $15.07 | — | $3,820.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; leftover $753.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 7 | $97.43 | $2.01 | — | $5,344.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; leftover $753.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 170 | $4.43 | $2.50 | — | $4,588.95 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; leftover $753.57 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,390.15 | ▲ close $5,172.32 vs 09:30 $5,167.66 (session +37.81) | — | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.77 | ▲ close $6,049.53 vs 09:30 $6,028.57 (session +51.53) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 15 | $31.87 | $2.04 | $-6.65 | $6,129.79 | ▼ -6.65 after sell → book $5,155.67; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 164 | $2.95 | $2.48 | $+4.83 | $5,643.51 | ▲ +4.83 after sell → book $5,153.19; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 43 | $18.13 | $2.12 | $+40.44 | $7,589.89 | ▲ +40.44 after sell → book $5,165.54; vs 09:30 mark -2.12 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 72 | $6.81 | $2.21 | $-5.17 | $5,150.98 | ▼ -5.17 after sell → book $5,150.98; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 430 | $0.96 | $5.43 | $+69.34 | $7,170.37 | ▲ +69.34 after sell → book $5,160.11; vs 09:30 mark -5.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 139 | $4.01 | $2.41 | $-68.11 | $6,609.88 | ▼ -68.11 after sell → book $5,157.71; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,371.60 | ▼ 09:30 equity $5,167.66 vs yday $5,196.35 (-28.69) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,028.57 | ▲ 09:30 equity $6,028.57 vs yday $6,028.57 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 6 | $46.85 | $2.04 | — | $6,757.18 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; leftover $321.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 27 | $11.81 | $2.10 | — | $6,305.98 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $321.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 1 | $204.45 | $2.02 | — | $5,353.41 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $321.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 69 | $4.61 | $2.23 | — | $7,390.15 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; leftover $321.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 1 | $173.90 | $1.77 | — | $6,478.11 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; leftover $321.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 72 | $4.43 | $2.24 | — | $5,989.07 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; leftover $321.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 3 | $106.38 | $2.03 | — | $7,074.29 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; leftover $321.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 15 | $21.40 | $2.06 | — | $5,672.35 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; leftover $321.94 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 4 | $2.30 | $0.10 | — | $62.47 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; leftover $10.25 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,804.64 | ▼ close $5,128.34 vs 09:30 $5,148.39 (session -7.34) | — | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $62.47 | ▲ close $6,188.49 vs 09:30 $6,073.09 (session +115.50) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,390.15 | ▼ 09:30 equity $5,148.39 vs yday $5,172.32 (-23.93) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.77 | ▲ 09:30 equity $6,073.09 vs yday $6,049.53 (+23.56) | — | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 20 | $20.90 | $2.08 | — | $9,401.71 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $429.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 4 | $89.10 | $2.03 | — | $8,565.44 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $429.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 3 | $133.11 | $2.03 | — | $8,211.08 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; leftover $429.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 15 | $27.00 | $2.07 | — | $9,804.64 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; leftover $429.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 137 | $3.11 | $2.45 | — | $7,813.78 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $429.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 11 | $38.40 | $2.05 | — | $8,985.79 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $429.03 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,804.64 | ▲ close $5,196.18 vs 09:30 $5,159.21 (session +36.97) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $62.47 | ▲ close $6,283.32 vs 09:30 $6,204.40 (session +78.92) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,804.64 | ▲ 09:30 equity $5,159.21 vs yday $5,128.34 (+30.87) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.47 | ▲ 09:30 equity $6,204.40 vs yday $6,188.49 (+15.91) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 4 | $175.01 | $2.00 | — | $5,547.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; leftover $781.15 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 8 | $88.94 | $2.01 | — | $4,833.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; leftover $781.15 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 51 | $15.28 | $2.14 | — | $4,052.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; leftover $781.15 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 5 | $142.36 | $2.00 | — | $3,338.42 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; leftover $781.15 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 153 | $5.10 | $2.45 | — | $2,555.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; leftover $781.15 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 16 | $47.89 | $2.04 | — | $1,787.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; leftover $781.15 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 56 | $13.92 | $2.16 | — | $1,005.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; leftover $781.15 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 171 | $4.54 | $2.50 | — | $226.02 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; leftover $781.15 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,690.47 | ▼ close $5,120.19 vs 09:30 $5,213.24 (session -68.54) | — | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $226.02 | ▼ close $6,018.37 vs 09:30 $6,289.94 (session -222.83) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 6 | $43.63 | $2.01 | $+15.28 | $8,219.46 | ▲ +15.28 after sell → book $5,201.22; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 27 | $11.00 | $2.07 | $+17.83 | $8,655.60 | ▲ +17.83 after sell → book $5,204.94; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 1 | $212.00 | $1.99 | $-11.56 | $9,590.65 | ▼ -11.56 after sell → book $5,211.25; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 69 | $4.77 | $2.20 | $-15.46 | $7,569.39 | ▼ -15.46 after sell → book $5,197.02; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 1 | $170.64 | $1.71 | $-0.22 | $8,483.25 | ▼ -0.22 after sell → book $5,203.23; vs 09:30 mark -1.71 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 72 | $4.42 | $2.21 | $-3.72 | $8,954.67 | ▼ -3.72 after sell → book $5,207.01; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 3 | $105.58 | $2.00 | $-1.63 | $7,900.72 | ▼ -1.63 after sell → book $5,199.22; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 15 | $20.90 | $2.04 | $+3.40 | $9,275.11 | ▲ +3.40 after sell → book $5,209.21; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,804.64 | ▲ 09:30 equity $5,213.24 vs yday $5,196.18 (+17.06) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.47 | ▲ 09:30 equity $6,289.94 vs yday $6,283.32 (+6.62) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 16 | $43.63 | $2.06 | $-55.62 | $2,995.99 | ▼ -55.62 after sell → book $6,267.57; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 83 | $9.23 | $2.26 | $+13.76 | $3,759.81 | ▲ +13.76 after sell → book $6,265.30; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 193 | $5.24 | $2.61 | $+255.37 | $4,768.52 | ▲ +255.37 after sell → book $6,262.69; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 22 | $34.72 | $2.08 | $+10.61 | $5,530.29 | ▲ +10.61 after sell → book $6,260.62; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 33 | $21.85 | $2.11 | $-23.67 | $6,249.23 | ▼ -23.67 after sell → book $6,258.51; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DVLT` | 2511 | $0.31 | $15.74 | $-5.70 | $2,299.96 | ▼ -5.70 after sell → book $6,269.62; vs 09:30 mark -15.75 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EL` | 7 | $104.00 | $2.03 | $+41.95 | $788.44 | ▲ +41.95 after sell → book $6,287.91; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TOYO` | 170 | $4.42 | $2.54 | $-6.74 | $1,537.30 | ▼ -6.74 after sell → book $6,285.37; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 11 | $54.51 | $2.06 | — | $8,805.15 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; leftover $649.63 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 47 | $13.62 | $2.17 | — | $8,207.60 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $649.63 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 3 | $175.01 | $2.03 | — | $9,328.15 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; leftover $649.63 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 1 | $364.35 | $2.02 | — | $9,690.47 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $649.63 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 2 | $18.26 | $0.37 | — | $121.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; leftover $39.22 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 1 | $34.30 | $0.35 | — | $87.25 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; leftover $39.22 | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 67 | $0.58 | $0.59 | — | $195.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; leftover $39.22 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 7 | $5.21 | $0.39 | — | $158.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $39.22 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,498.69 | ▼ close $5,104.98 vs 09:30 $5,199.54 (session -71.48) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $87.25 | ▲ close $6,198.17 vs 09:30 $5,999.43 (session +200.57) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 20 | $20.50 | $2.05 | $+3.87 | $7,644.22 | ▲ +3.87 after sell → book $5,189.07; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 4 | $88.24 | $2.00 | $-0.59 | $8,480.80 | ▼ -0.59 after sell → book $5,193.14; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 3 | $154.20 | $2.00 | $-67.30 | $8,835.76 | ▼ -67.30 after sell → book $5,195.14; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 15 | $26.00 | $2.04 | $+10.90 | $7,252.18 | ▲ +10.90 after sell → book $5,187.03; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 137 | $2.83 | $2.40 | $+33.51 | $9,300.36 | ▲ +33.51 after sell → book $5,197.14; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 11 | $38.41 | $2.02 | $-4.19 | $8,056.27 | ▼ -4.19 after sell → book $5,191.12; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,690.47 | ▲ 09:30 equity $5,199.54 vs yday $5,120.19 (+79.35) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $226.02 | ▼ 09:30 equity $5,999.43 vs yday $6,018.37 (-18.94) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 4 | $2.35 | $0.13 | $-0.03 | $235.29 | ▼ -0.03 after sell → book $5,999.30; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 42 | $12.22 | $2.15 | — | $8,189.13 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; leftover $518.70 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 102 | $5.08 | $2.34 | — | $8,704.95 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; leftover $518.70 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 2 | $213.94 | $2.03 | — | $7,678.04 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $518.70 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 2 | $199.94 | $2.03 | — | $9,498.69 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; leftover $518.70 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 3 | $132.64 | $2.03 | — | $9,100.84 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; leftover $518.70 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,902.63 | ▼ close $5,007.53 vs 09:30 $5,037.87 (session -26.14) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $87.25 | ▼ close $6,179.04 vs 09:30 $6,208.65 (session -29.61) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,498.69 | ▼ 09:30 equity $5,037.87 vs yday $5,104.98 (-67.11) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.25 | ▲ 09:30 equity $6,208.65 vs yday $6,198.17 (+10.48) | — | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 16 | $74.54 | $2.09 | — | $10,689.24 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; leftover $1259.47 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 22 | $55.25 | $2.11 | — | $11,902.63 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; leftover $1259.47 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 2 | $261.16 | $2.00 | — | $5,502.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; leftover $753.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 50 | $15.01 | $2.14 | — | $4,750.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; leftover $753.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 7 | $103.89 | $2.01 | — | $4,021.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; leftover $753.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 194 | $3.88 | $2.57 | — | $3,265.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; leftover $753.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 16 | $44.40 | $2.04 | — | $2,553.31 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; leftover $753.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 30 | $24.69 | $2.08 | — | $1,810.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; leftover $753.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 90 | $8.35 | $2.26 | — | $1,056.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; leftover $753.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 20 | $37.65 | $2.05 | — | $301.82 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; leftover $753.40 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,039.82 | ▲ close $5,172.02 vs 09:30 $5,000.83 (session +183.55) | — | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $301.82 | ▼ close $5,931.92 vs 09:30 $6,187.92 (session -221.34) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 11 | $52.49 | $2.02 | $+18.14 | $10,667.79 | ▲ +18.14 after sell → book $4,996.68; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 47 | $13.90 | $2.13 | $-17.22 | $11,247.20 | ▼ -17.22 after sell → book $4,998.70; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 3 | $172.76 | $2.00 | $+2.72 | $10,147.51 | ▲ +2.72 after sell → book $4,994.68; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 1 | $347.82 | $1.99 | $+12.51 | $9,797.70 | ▲ +12.51 after sell → book $4,992.69; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,902.63 | ▼ 09:30 equity $5,000.83 vs yday $5,007.53 (-6.70) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.25 | ▲ 09:30 equity $6,187.92 vs yday $6,179.04 (+8.88) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 4 | $172.76 | $2.02 | $-13.02 | $776.27 | ▼ -13.02 after sell → book $6,185.89; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 8 | $93.30 | $2.03 | $+30.83 | $1,520.63 | ▲ +30.83 after sell → book $6,183.86; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 51 | $18.15 | $2.16 | $+142.06 | $2,444.12 | ▲ +142.06 after sell → book $6,181.70; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 5 | $132.80 | $2.02 | $-51.83 | $3,106.09 | ▼ -51.83 after sell → book $6,179.67; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 153 | $4.58 | $2.48 | $-84.49 | $3,804.35 | ▼ -84.49 after sell → book $6,177.19; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 16 | $48.42 | $2.06 | $+4.38 | $4,577.01 | ▲ +4.38 after sell → book $6,175.13; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 56 | $15.66 | $2.18 | $+93.10 | $5,451.79 | ▲ +93.10 after sell → book $6,172.95; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 171 | $3.38 | $2.54 | $-204.26 | $6,027.23 | ▼ -204.26 after sell → book $6,170.41; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 41 | $30.18 | $2.17 | — | $12,039.82 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; leftover $1248.17 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 4 | $252.24 | $2.05 | — | $10,804.61 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1248.17 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,746.88 | ▲ close $5,232.79 vs 09:30 $5,218.88 (session +24.31) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $442.47 | ▲ close $5,983.04 vs 09:30 $5,938.19 (session +46.59) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 42 | $11.10 | $2.12 | $+42.77 | $11,151.75 | ▲ +42.77 after sell → book $5,214.77; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 102 | $4.97 | $2.30 | $+6.08 | $10,642.00 | ▲ +6.08 after sell → book $5,212.47; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 2 | $208.88 | $2.00 | $+6.10 | $11,620.06 | ▲ +6.10 after sell → book $5,216.88; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 2 | $254.39 | $2.00 | $-112.92 | $9,746.88 | ▼ -112.92 after sell → book $5,208.48; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 3 | $127.45 | $2.00 | $+11.54 | $10,257.65 | ▲ +11.54 after sell → book $5,210.47; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,039.82 | ▲ 09:30 equity $5,218.88 vs yday $5,172.02 (+46.86) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $301.82 | ▲ 09:30 equity $5,938.19 vs yday $5,931.92 (+6.27) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 2 | $19.25 | $0.41 | $+1.20 | $408.12 | ▲ +1.20 after sell → book $5,936.82; vs 09:30 mark -0.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BOX` | 1 | $34.72 | $0.37 | $-0.30 | $442.47 | ▼ -0.30 after sell → book $5,936.45; vs 09:30 mark -0.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 67 | $0.51 | $0.56 | $-6.05 | $335.42 | ▼ -6.05 after sell → book $5,937.62; vs 09:30 mark -0.57 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 7 | $5.00 | $0.39 | $-2.25 | $370.03 | ▼ -2.25 after sell → book $5,937.23; vs 09:30 mark -0.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,366.54 | ▲ close $5,301.94 vs 09:30 $5,300.82 (session +5.22) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $442.47 | ▼ close $5,862.78 vs 09:30 $5,919.01 (session -56.23) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 16 | $73.22 | $2.04 | $+16.99 | $8,573.32 | ▲ +16.99 after sell → book $5,298.78; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 22 | $54.76 | $2.06 | $+6.61 | $7,366.54 | ▲ +6.61 after sell → book $5,296.72; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,746.88 | ▲ 09:30 equity $5,300.82 vs yday $5,232.79 (+68.03) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $442.47 | ▼ 09:30 equity $5,919.01 vs yday $5,983.04 (-64.03) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,321.61 | ▲ close $5,321.61 vs 09:30 $5,325.72 (session +0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,811.04 | ▲ close $5,811.04 vs 09:30 $5,828.37 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 41 | $26.78 | $2.11 | $+135.12 | $5,321.61 | ▲ +135.12 after sell → book $5,321.61; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 4 | $235.71 | $2.00 | $+62.07 | $6,421.70 | ▲ +62.07 after sell → book $5,323.72; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,366.54 | ▲ 09:30 equity $5,325.72 vs yday $5,301.94 (+23.78) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $442.47 | ▼ 09:30 equity $5,828.37 vs yday $5,862.78 (-34.41) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 2 | $246.70 | $2.02 | $-32.93 | $933.85 | ▼ -32.93 after sell → book $5,826.35; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 50 | $15.01 | $2.16 | $-4.30 | $1,682.19 | ▼ -4.30 after sell → book $5,824.19; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 7 | $92.00 | $2.03 | $-87.27 | $2,324.16 | ▼ -87.27 after sell → book $5,822.16; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 194 | $3.32 | $2.61 | $-113.83 | $2,965.63 | ▼ -113.83 after sell → book $5,819.55; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 16 | $44.17 | $2.06 | $-7.78 | $3,670.29 | ▼ -7.78 after sell → book $5,817.49; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 30 | $21.97 | $2.10 | $-85.78 | $4,327.29 | ▼ -85.78 after sell → book $5,815.39; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 90 | $8.58 | $2.28 | $+16.16 | $5,097.21 | ▲ +16.16 after sell → book $5,813.11; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 20 | $35.80 | $2.07 | $-41.12 | $5,811.04 | ▼ -41.12 after sell → book $5,811.04; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 67 | $10.74 | $2.19 | — | $5,088.93 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; leftover $726.38 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 2 | $351.74 | $2.00 | — | $4,383.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; leftover $726.38 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 105 | $6.90 | $2.31 | — | $3,656.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; leftover $726.38 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 2 | $354.49 | $2.00 | — | $2,945.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; leftover $726.38 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 32 | $22.32 | $2.09 | — | $2,229.35 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; leftover $726.38 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 2 | $257.00 | $2.00 | — | $1,713.35 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; leftover $726.38 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 15 | $47.60 | $2.04 | — | $997.32 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; leftover $726.38 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 48 | $15.09 | $2.13 | — | $270.86 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; leftover $726.38 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,961.11 | ▲ close $5,392.22 vs 09:30 $5,321.61 (session +83.14) | — | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $270.86 | ▲ close $6,043.84 vs 09:30 $5,811.04 (session +249.55) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,321.61 | ▲ 09:30 equity $5,321.61 vs yday $5,321.61 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,811.04 | ▲ 09:30 equity $5,811.04 vs yday $5,811.04 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 778 | $1.71 | $10.21 | — | $7,961.11 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; leftover $1330.40 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 89 | $14.85 | $2.32 | — | $6,640.94 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1330.40 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 3 | $8.74 | $0.27 | — | $244.37 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; leftover $33.86 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 9 | $3.62 | $0.35 | — | $211.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; leftover $33.86 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 2 | $15.70 | $0.32 | — | $179.76 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; leftover $33.86 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,610.82 | ▼ close $5,372.41 vs 09:30 $5,422.02 (session -43.67) | — | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $179.76 | ▲ close $6,090.18 vs 09:30 $6,068.00 (session +23.12) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,961.11 | ▲ 09:30 equity $5,422.02 vs yday $5,392.22 (+29.80) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $270.86 | ▲ 09:30 equity $6,068.00 vs yday $6,043.84 (+24.16) | — | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 290 | $4.67 | $3.84 | — | $9,311.57 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; leftover $1355.50 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 17 | $76.55 | $2.10 | — | $10,610.82 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; leftover $1355.50 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,610.82 | ▲ close $5,529.81 vs 09:30 $5,394.94 (session +134.87) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $179.76 | ▼ close $6,099.11 vs 09:30 $6,103.85 (session -4.74) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,610.82 | ▲ 09:30 equity $5,394.94 vs yday $5,372.41 (+22.53) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $179.76 | ▲ 09:30 equity $6,103.85 vs yday $6,090.18 (+13.67) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,158.89 | ▲ close $5,548.47 vs 09:30 $5,547.30 (session +13.46) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,986.94 | ▼ close $6,076.18 vs 09:30 $6,095.12 (session -2.04) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 778 | $1.58 | $10.04 | $+80.89 | $8,158.89 | ▲ +80.89 after sell → book $5,535.01; vs 09:30 mark -10.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 89 | $13.60 | $2.26 | $+106.67 | $9,398.17 | ▲ +106.67 after sell → book $5,545.05; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,610.82 | ▲ 09:30 equity $5,547.30 vs yday $5,529.81 (+17.49) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $179.76 | ▼ 09:30 equity $6,095.12 vs yday $6,099.11 (-3.99) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 67 | $10.51 | $2.21 | $-20.15 | $881.72 | ▼ -20.15 after sell → book $6,092.91; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 2 | $366.23 | $2.02 | $+24.97 | $1,612.16 | ▲ +24.97 after sell → book $6,090.89; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 105 | $9.39 | $2.33 | $+256.81 | $2,595.78 | ▲ +256.81 after sell → book $6,088.56; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 2 | $341.90 | $2.02 | $-29.19 | $3,277.57 | ▼ -29.19 after sell → book $6,086.55; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CPB` | 32 | $21.67 | $2.11 | $-24.99 | $3,968.90 | ▼ -24.99 after sell → book $6,084.44; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 2 | $252.92 | $2.02 | $-12.17 | $4,472.72 | ▼ -12.17 after sell → book $6,082.42; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 15 | $56.94 | $2.06 | $+136.01 | $5,324.77 | ▲ +136.01 after sell → book $6,080.37; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MEI` | 48 | $13.84 | $2.15 | $-64.29 | $5,986.94 | ▼ -64.29 after sell → book $6,078.22; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,583.28 | ▲ close $5,583.28 vs 09:30 $5,589.06 (session +0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,075.08 | ▲ close $6,075.08 vs 09:30 $6,076.08 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 290 | $4.36 | $3.74 | $+82.32 | $6,890.75 | ▲ +82.32 after sell → book $5,585.32; vs 09:30 mark -3.74 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 17 | $76.79 | $2.04 | $-8.22 | $5,583.28 | ▼ -8.22 after sell → book $5,583.28; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,158.89 | ▲ 09:30 equity $5,589.06 vs yday $5,548.47 (+40.59) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,986.94 | ▼ 09:30 equity $6,076.08 vs yday $6,076.18 (-0.10) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ASAN` | 3 | $8.26 | $0.28 | $-1.99 | $6,011.44 | ▼ -1.99 after sell → book $6,075.80; vs 09:30 mark -0.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 9 | $3.76 | $0.39 | $+0.57 | $6,044.89 | ▲ +0.57 after sell → book $6,075.41; vs 09:30 mark -0.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MAMA` | 2 | $15.26 | $0.33 | $-1.53 | $6,075.08 | ▼ -1.53 after sell → book $6,075.08; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 3 | $242.17 | $2.00 | — | $3,928.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; leftover $759.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 23 | $32.01 | $2.06 | — | $3,189.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; leftover $759.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 128 | $5.91 | $2.37 | — | $4,656.51 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; leftover $759.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 10 | $71.71 | $2.02 | — | $2,470.59 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; leftover $759.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 13 | $56.02 | $2.03 | — | $1,740.30 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; leftover $759.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 81 | $9.37 | $2.23 | — | $979.10 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; leftover $759.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 4 | $164.43 | $2.00 | — | $5,415.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; leftover $759.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 57 | $13.10 | $2.16 | — | $230.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; leftover $759.39 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,241.62 | ▼ close $5,568.14 vs 09:30 $5,583.28 (session -2.35) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $230.23 | ▲ close $6,095.21 vs 09:30 $6,075.08 (session +37.01) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,583.28 | ▲ 09:30 equity $5,583.28 vs yday $5,583.28 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,075.08 | ▲ 09:30 equity $6,075.08 vs yday $6,075.08 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 22 | $24.97 | $2.09 | — | $7,688.11 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; leftover $558.33 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 275 | $2.03 | $3.62 | — | $7,140.86 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; leftover $558.33 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 165 | $3.37 | $2.54 | — | $8,241.62 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; leftover $558.33 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 4 | $112.83 | $2.03 | — | $6,032.58 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $558.33 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 158 | $3.52 | $2.52 | — | $6,586.23 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; leftover $558.33 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,241.62 | ▼ close $5,507.92 vs 09:30 $5,583.27 (session -75.35) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $230.23 | ▲ close $6,328.83 vs 09:30 $6,106.81 (session +222.02) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,241.62 | ▲ 09:30 equity $5,583.27 vs yday $5,568.14 (+15.13) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $230.23 | ▲ 09:30 equity $6,106.81 vs yday $6,095.21 (+11.60) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,241.62 | ▼ close $5,466.95 vs 09:30 $5,508.12 (session -41.17) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $230.23 | ▲ close $6,344.67 vs 09:30 $6,308.33 (session +36.34) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,241.62 | ▲ 09:30 equity $5,508.12 vs yday $5,507.92 (+0.20) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $230.23 | ▼ 09:30 equity $6,308.33 vs yday $6,328.83 (-20.50) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 94 | $33.14 | $2.27 | — | $3,164.44 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; leftover $3140.94 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 76 | $40.93 | $2.22 | — | $51.54 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; leftover $3140.94 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,175.53 | ▼ close $5,264.88 vs 09:30 $5,475.32 (session -192.95) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.54 | ▲ close $6,399.18 vs 09:30 $6,298.92 (session +121.80) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 22 | $24.42 | $2.06 | $+7.95 | $6,084.00 | ▲ +7.95 after sell → book $5,465.25; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 275 | $1.85 | $3.55 | $+42.33 | $6,623.30 | ▲ +42.33 after sell → book $5,467.31; vs 09:30 mark -3.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 165 | $3.75 | $2.48 | $-67.72 | $5,462.77 | ▼ -67.72 after sell → book $5,462.77; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 4 | $118.18 | $2.00 | $-25.42 | $7,766.90 | ▼ -25.42 after sell → book $5,473.32; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 158 | $3.98 | $2.46 | $-77.66 | $7,135.59 | ▼ -77.66 after sell → book $5,470.85; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,241.62 | ▲ 09:30 equity $5,475.32 vs yday $5,466.95 (+8.37) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $230.23 | ▼ 09:30 equity $6,298.92 vs yday $6,344.67 (-45.75) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 3 | $253.34 | $2.02 | $+29.49 | $2,343.93 | ▲ +29.49 after sell → book $6,292.48; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CPRT` | 23 | $30.57 | $2.08 | $-37.26 | $3,044.96 | ▼ -37.26 after sell → book $6,290.40; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DBI` | 128 | $6.25 | $2.41 | $+38.74 | $1,585.93 | ▲ +38.74 after sell → book $6,294.50; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DSGX` | 10 | $78.12 | $2.04 | $+60.04 | $3,824.12 | ▲ +60.04 after sell → book $6,288.36; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `KR` | 13 | $61.93 | $2.05 | $+72.75 | $4,627.16 | ▲ +72.75 after sell → book $6,286.31; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `LPTH` | 81 | $9.40 | $2.26 | $-2.06 | $5,386.30 | ▼ -2.06 after sell → book $6,284.05; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 4 | $140.03 | $2.02 | $-101.62 | $788.33 | ▼ -101.62 after sell → book $6,296.90; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `REF` | 57 | $15.75 | $2.18 | $+146.71 | $6,281.87 | ▲ +146.71 after sell → book $6,281.87; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 73 | $18.61 | $2.27 | — | $6,819.03 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1365.69 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 199 | $6.83 | $2.67 | — | $8,175.53 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; leftover $1365.69 | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 2 | $11.21 | $0.23 | — | $28.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.0; leftover $25.77 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,770.68 | ▲ close $5,341.31 vs 09:30 $5,246.43 (session +99.53) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.89 | ▲ close $6,696.98 vs 09:30 $6,607.02 (session +90.19) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,175.53 | ▼ 09:30 equity $5,246.43 vs yday $5,264.88 (-18.45) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.54 | ▲ 09:30 equity $6,607.02 vs yday $6,399.18 (+207.84) | — | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 164 | $7.95 | $2.56 | — | $9,476.77 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $1311.61 | — |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 16 | $81.00 | $2.09 | — | $10,770.68 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-3.0; leftover $1311.61 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,420.23 | ▲ close $5,420.85 vs 09:30 $5,355.02 (session +68.16) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.89 | ▲ close $6,853.38 vs 09:30 $6,851.53 (session +1.85) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,770.68 | ▲ 09:30 equity $5,355.02 vs yday $5,341.31 (+13.71) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.89 | ▲ 09:30 equity $6,851.53 vs yday $6,696.98 (+154.55) | — | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 77 | $34.44 | $2.33 | — | $13,420.23 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; leftover $2677.51 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,990.98 | ▼ close $5,193.77 vs 09:30 $5,324.59 (session -121.43) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,903.14 | ▲ close $6,930.36 vs 09:30 $6,933.96 (session +0.97) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 73 | $22.11 | $2.21 | $-259.98 | $11,803.99 | ▼ -259.98 after sell → book $5,322.38; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 199 | $6.55 | $2.59 | $+50.46 | $10,497.96 | ▲ +50.46 after sell → book $5,319.80; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,420.23 | ▼ 09:30 equity $5,324.59 vs yday $5,420.85 (-96.26) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.89 | ▲ 09:30 equity $6,933.96 vs yday $6,853.38 (+80.58) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `FPS` | 94 | $40.03 | $2.32 | $+643.07 | $3,789.40 | ▲ +643.07 after sell → book $6,931.65; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 76 | $41.00 | $2.26 | $+0.85 | $6,903.14 | ▲ +0.85 after sell → book $6,929.39; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 161 | $8.26 | $2.55 | — | $11,825.27 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; leftover $1329.95 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 2 | $583.88 | $2.05 | — | $12,990.98 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; leftover $1329.95 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,850.93 | ▼ close $5,169.13 vs 09:30 $5,207.57 (session -33.88) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,903.14 | ▲ close $6,930.36 vs 09:30 $6,930.36 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 164 | $8.28 | $2.48 | $-58.34 | $11,631.40 | ▼ -58.34 after sell → book $5,205.09; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,990.98 | ▲ 09:30 equity $5,207.57 vs yday $5,193.77 (+13.80) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,903.14 | ▲ 09:30 equity $6,930.36 vs yday $6,930.36 (-0.00) | — | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 13 | $93.97 | $2.08 | — | $12,850.93 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; leftover $1301.27 | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 29 | $47.57 | $2.08 | — | $5,548.87 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; leftover $1386.10 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 7 | $196.78 | $2.01 | — | $4,169.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $1386.10 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 38 | $35.74 | $2.10 | — | $2,809.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; leftover $1386.10 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 29 | $47.15 | $2.08 | — | $1,439.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; leftover $1386.10 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 12 | $109.67 | $2.03 | — | $121.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; leftover $1386.10 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,871.75 | ▲ close $4,946.25 vs 09:30 $4,851.81 (session +98.62) | — | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $121.68 | ▼ close $6,825.94 vs 09:30 $6,930.78 (session -94.24) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `LEN` | 16 | $82.00 | $2.04 | $-20.13 | $11,536.89 | ▼ -20.13 after sell → book $4,849.77; vs 09:30 mark -2.04 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,850.93 | ▼ 09:30 equity $4,851.81 vs yday $5,169.13 (-317.32) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,903.14 | ▲ 09:30 equity $6,930.78 vs yday $6,930.36 (+0.42) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `ALMU` | 2 | $13.82 | $0.30 | $+4.69 | $6,930.48 | ▲ +4.69 after sell → book $6,930.48; vs 09:30 mark -0.30 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 20 | $116.85 | $2.14 | — | $13,871.75 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; leftover $2424.89 | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,669.21 | ▼ close $4,879.77 vs 09:30 $4,992.00 (session -110.23) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $121.68 | ▲ close $6,933.27 vs 09:30 $6,826.47 (session +106.81) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 2 | $600.27 | $2.00 | $-36.82 | $12,669.21 | ▼ -36.82 after sell → book $4,990.00; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,871.75 | ▲ 09:30 equity $4,992.00 vs yday $4,946.25 (+45.75) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $121.68 | ▲ 09:30 equity $6,826.47 vs yday $6,825.94 (+0.53) | — | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,576.49 | ▼ close $4,566.90 vs 09:30 $4,583.77 (session -12.99) | 16:00 close · cash $12,576.49 · equity $4,566.90 vs 09:30 $4,583.77 (-16.87; session marks -12.99) · 6 name(s) marked open→close (per-name table). AEHL×150 09:30 $9.05 → close $9.36 -46.50; BAND×19 09:30 $61.83 → close $61.83 -0.00; HALO×9 09:30 $115.36 → close $113.90 +13.14; PAYX×10 09:30 $101.59 → close $101.59 +0.00; USFD×12 09:30 $93.82 → close $93.82 +0.00; RSKD×291 09:30 $7.85 → close $7.78 +20.37 | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,296.02 | ▼ 09:30 equity $4,583.77 vs yday $4,598.53 (-14.76) | 09:30 open · cash $10,296.02 (unchanged overnight, no fees) · equity $4,583.77 vs prior close $4,598.53 (-14.76) · 5 name(s) re-marked at the open (per-name table). AEHL×150 yday $8.96 → 09:30 $9.05 -13.50; BAND×19 yday $61.83 → 09:30 $61.83 -0.00; HALO×9 yday $115.22 → 09:30 $115.36 -1.26; PAYX×10 yday $101.59 → 09:30 $101.59 -0.00; USFD×12 yday $93.82 → 09:30 $93.82 -0.00 | — |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 291 | $7.85 | $3.88 | — | $12,576.49 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; leftover $2291.89 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `EU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LUNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `OWL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LUNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OWL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ZNTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `VERI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ZNTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `APMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `RNW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WYFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TOYO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WMT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AQST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WYFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TOYO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TEAM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WMT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `SSRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARIS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARIS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `NOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `INTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `NEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `BE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `NEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `TX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `TX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PIPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `GSM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `PIPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `QRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RWT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RWT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `MYGN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `GFR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `GFR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BULL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `LEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BULL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `LEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `LEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-23 | `AMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `USFD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `USFD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `ARX` | cash | leftover split 2.58 < 1 share @ 19.57 |
| 2026-08-14 | `AIRO` | cash | leftover split 2.58 < 1 share @ 11.12 |
| 2026-08-14 | `MH` | cash | leftover split 2.58 < 1 share @ 13.55 |
| 2026-08-14 | `CLBT` | cash | leftover split 2.58 < 1 share @ 10.83 |
| 2026-08-14 | `LUNR` | cash | leftover split 2.58 < 1 share @ 19.17 |
| 2026-08-14 | `NMAX` | cash | leftover split 2.58 < 1 share @ 9.89 |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `EU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `EU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `EL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TOYO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AEG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ATAT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ATHM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `FUTU` | cash | leftover split 10.25 < 1 share @ 115.18 |
| 2026-08-21 | `DE` | cash | leftover split 10.25 < 1 share @ 623.26 |
| 2026-08-21 | `WMT` | cash | leftover split 10.25 < 1 share @ 103.69 |
| 2026-08-21 | `BEKE` | cash | leftover split 10.25 < 1 share @ 17.93 |
| 2026-08-21 | `BJ` | cash | leftover split 10.25 < 1 share @ 93.98 |
| 2026-08-21 | `BKE` | cash | leftover split 10.25 < 1 share @ 43.08 |
| 2026-08-24 | `EL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TOYO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AEG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ATAT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ATHM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `PSEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `PSEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `BMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BNS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SHMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ANF` | cash | leftover split 39.22 < 1 share @ 131.37 |
| 2026-08-26 | `DY` | cash | leftover split 39.22 < 1 share @ 326.91 |
| 2026-08-27 | `BMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BNS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SHMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SLQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `TIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BOX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BBY` | cash | leftover split 10.91 < 1 share @ 80.60 |
| 2026-08-27 | `BILI` | cash | leftover split 10.91 < 1 share @ 16.18 |
| 2026-08-27 | `CM` | cash | leftover split 10.91 < 1 share @ 118.77 |
| 2026-08-27 | `CMBT` | cash | leftover split 10.91 < 1 share @ 17.78 |
| 2026-08-27 | `CSIQ` | cash | leftover split 10.91 < 1 share @ 13.41 |
| 2026-08-27 | `HQY` | cash | leftover split 10.91 < 1 share @ 97.16 |
| 2026-08-27 | `RY` | cash | leftover split 10.91 < 1 share @ 206.82 |
| 2026-08-27 | `TD` | cash | leftover split 10.91 < 1 share @ 120.17 |
| 2026-08-28 | `SLQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `TIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BOX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBAR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ESTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FINV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBAR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ESTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FINV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CHPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CPB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HPE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MEI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AMBA` | cash | leftover split 33.86 < 1 share @ 63.18 |
| 2026-09-04 | `DOCU` | cash | leftover split 33.86 < 1 share @ 68.52 |
| 2026-09-04 | `GWRE` | cash | leftover split 33.86 < 1 share @ 167.55 |
| 2026-09-04 | `IOT` | cash | leftover split 33.86 < 1 share @ 44.90 |
| 2026-09-04 | `LULU` | cash | leftover split 33.86 < 1 share @ 98.15 |
| 2026-09-08 | `AI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CHPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CPB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HPE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MEI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ASAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `DOMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `MAMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ASAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `DOMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MAMA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `DBI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ADBE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CPRT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `DSGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `KR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `LPTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `REF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `DBI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ADBE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CPRT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `DSGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `KR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `LPTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `REF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `FPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `LEN` | cash | leftover split 25.77 < 1 share @ 81.00 |
| 2026-09-18 | `FPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ALMU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `ALMU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `ALMU` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-24 | `CBRL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CTAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `GIS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `KBH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `PAYX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FIVN` | 77 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; leftover $2677.51 |
| `AEHL` | 161 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; leftover $1329.95 |
| `USFD` | 13 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; leftover $1301.27 |
| `HALO` | 20 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; leftover $2424.89 |
| `CBRL` | 29 | 2026-09-23 @ $47.57 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; leftover $1386.10 |
| `CTAS` | 7 | 2026-09-23 @ $196.78 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $1386.10 |
| `GIS` | 38 | 2026-09-23 @ $35.74 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; leftover $1386.10 |
| `KBH` | 29 | 2026-09-23 @ $47.15 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; leftover $1386.10 |
| `PAYX` | 12 | 2026-09-23 @ $109.67 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; leftover $1386.10 |
