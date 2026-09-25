# Factor mine action — `combo_ne1_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_news_g_h1/union_e_fresh_h1 w=0.5,0.5 net=priority

Cash book **-14.01%** ($8,599) · signal-only (no cash/fees) was —. Starts YES **1/30**. Fills 351 · skips 135 · realized $+513.04.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_news_g_h1 50%, union_e_fresh_h1 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_news_g_h1 50%, union_e_fresh_h1 50%.
- Member: union_news_g_h1 (50% · long · hold 1).
- Member: union_e_fresh_h1 (50% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $214.82.

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
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 34 | $19.57 | $2.09 | — | $10,216.20 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 50 | $13.55 | $2.14 | — | $9,536.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 453 | $1.50 | $5.84 | — | $8,851.22 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 61 | $11.12 | $2.17 | — | $8,170.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 62 | $10.83 | $2.18 | — | $7,497.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-30.1; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 576 | $1.18 | $7.43 | — | $6,809.98 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 35 | $19.17 | $2.10 | — | $6,136.93 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 68 | $9.89 | $2.19 | — | $5,461.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 2 | $359.83 | $2.00 | — | $4,740.22 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; combo leftover $910.31; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 6 | $146.90 | $2.01 | — | $3,856.81 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; combo leftover $910.31; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 7 | $120.00 | $2.01 | — | $3,014.80 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; combo leftover $910.31; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 211 | $4.31 | $2.72 | — | $2,102.67 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; combo leftover $910.31; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 69 | $13.18 | $2.20 | — | $1,191.05 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; combo leftover $910.31; owner union_news_g_h1 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,191.05 | ▲ close $10,979.39 vs 09:30 $10,963.61 (session +132.80) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,191.05 | ▲ 09:30 equity $11,081.55 vs yday $10,979.39 (+102.16) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 34 | $19.57 | $2.11 | $-4.20 | $1,854.32 | ▼ -4.20 after sell → book $11,079.44; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 50 | $13.16 | $2.16 | $-23.80 | $2,510.16 | ▼ -23.80 after sell → book $11,077.28; vs 09:30 mark -2.16 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 453 | $1.52 | $5.93 | $-2.71 | $3,192.79 | ▼ -2.71 after sell → book $11,071.35; vs 09:30 mark -5.93 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 61 | $9.57 | $2.19 | $-98.92 | $3,774.37 | ▼ -98.92 after sell → book $11,069.16; vs 09:30 mark -2.19 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 62 | $11.19 | $2.20 | $+17.95 | $4,465.95 | ▲ +17.95 after sell → book $11,066.96; vs 09:30 mark -2.20 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `EU` | 576 | $1.21 | $7.54 | $+2.31 | $5,155.38 | ▲ +2.31 after sell → book $11,059.43; vs 09:30 mark -7.53 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 35 | $20.25 | $2.12 | $+33.59 | $5,862.01 | ▲ +33.59 after sell → book $11,057.31; vs 09:30 mark -2.12 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 68 | $10.97 | $2.22 | $+68.69 | $6,605.76 | ▲ +68.69 after sell → book $11,055.10; vs 09:30 mark -2.21 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 2 | $367.88 | $2.02 | $+12.09 | $7,339.50 | ▲ +12.09 after sell → book $11,053.08; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 6 | $149.37 | $2.03 | $+10.78 | $8,233.69 | ▲ +10.78 after sell → book $11,051.05; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 7 | $127.40 | $2.03 | $+47.76 | $9,123.46 | ▲ +47.76 after sell → book $11,049.02; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 211 | $4.60 | $2.77 | $+55.70 | $10,091.30 | ▲ +55.70 after sell → book $11,046.26; vs 09:30 mark -2.76 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 69 | $13.84 | $2.22 | $+41.12 | $11,044.04 | ▲ +41.12 after sell → book $11,044.04; vs 09:30 mark -2.22 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 47 | $46.18 | $2.13 | — | $8,871.45 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; combo leftover $2208.81; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 15 | $142.77 | $2.04 | — | $6,727.86 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; combo leftover $2208.81; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 10 | $202.70 | $2.02 | — | $4,698.84 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; combo leftover $2208.81; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 23 | $92.99 | $2.06 | — | $2,558.01 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; combo leftover $2208.81; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 45 | $49.00 | $2.12 | — | $350.89 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; combo leftover $2208.81; owner union_news_g_h1 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $350.89 | ▲ close $11,133.80 vs 09:30 $11,081.55 (session +100.13) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $350.89 | ▼ 09:30 equity $11,070.58 vs yday $11,133.80 (-63.22) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 47 | $48.00 | $2.16 | $+81.25 | $2,604.73 | ▲ +81.25 after sell → book $11,068.42; vs 09:30 mark -2.16 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 15 | $148.04 | $2.06 | $+74.95 | $4,823.27 | ▲ +74.95 after sell → book $11,066.36; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 10 | $208.93 | $2.05 | $+58.23 | $6,910.52 | ▲ +58.23 after sell → book $11,064.31; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 23 | $92.38 | $2.09 | $-18.18 | $9,033.17 | ▼ -18.18 after sell → book $11,062.22; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 45 | $45.09 | $2.15 | $-180.23 | $11,060.07 | ▼ -180.23 after sell → book $11,060.07; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,060.07 | ▲ close $11,060.07 vs 09:30 $11,070.58 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,060.07 | ▲ 09:30 equity $11,060.07 vs yday $11,060.07 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,060.07 | ▲ close $11,060.07 vs 09:30 $11,060.07 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,060.07 | ▲ 09:30 equity $11,060.07 vs yday $11,060.07 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 7 | $97.43 | $2.01 | — | $10,376.05 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $691.25; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 156 | $4.43 | $2.46 | — | $9,682.51 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $691.25; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 2304 | $0.30 | $13.82 | — | $8,977.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $691.25; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 14 | $46.85 | $2.03 | — | $8,319.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $691.25; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 76 | $9.01 | $2.22 | — | $7,632.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $691.25; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 177 | $3.89 | $2.52 | — | $6,941.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $691.25; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 20 | $34.05 | $2.05 | — | $6,258.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $691.25; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 30 | $22.44 | $2.08 | — | $5,583.20 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $691.25; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 7 | $91.01 | $2.01 | — | $4,944.12 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; combo leftover $697.90; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 4 | $150.14 | $2.00 | — | $4,341.56 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $697.90; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 987 | $0.71 | $9.94 | — | $3,633.81 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; combo leftover $697.90; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 105 | $6.61 | $2.31 | — | $2,937.98 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; combo leftover $697.90; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 43 | $16.00 | $2.12 | — | $2,247.86 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; combo leftover $697.90; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 26 | $26.57 | $2.07 | — | $1,554.97 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; combo leftover $697.90; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 11 | $58.73 | $2.02 | — | $906.92 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; combo leftover $697.90; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 15 | $44.76 | $2.04 | — | $233.48 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; combo leftover $697.90; owner union_news_g_h1 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $233.48 | ▼ close $10,959.88 vs 09:30 $11,060.07 (session -46.50) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $233.48 | ▲ 09:30 equity $11,116.74 vs yday $10,959.88 (+156.86) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 7 | $96.75 | $2.03 | $-8.80 | $908.70 | ▼ -8.80 after sell → book $11,114.71; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TOYO` | 156 | $4.68 | $2.49 | $+34.05 | $1,636.29 | ▲ +34.05 after sell → book $11,112.22; vs 09:30 mark -2.49 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DVLT` | 2304 | $0.31 | $14.45 | $-5.23 | $2,336.08 | ▼ -5.23 after sell → book $11,097.77; vs 09:30 mark -14.44 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AEG` | 76 | $9.04 | $2.24 | $-2.18 | $3,020.88 | ▼ -2.18 after sell → book $11,095.53; vs 09:30 mark -2.24 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALVO` | 177 | $4.32 | $2.56 | $+71.03 | $3,782.96 | ▲ +71.03 after sell → book $11,092.97; vs 09:30 mark -2.56 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 20 | $34.31 | $2.07 | $+1.08 | $4,467.09 | ▲ +1.08 after sell → book $11,090.90; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 30 | $22.20 | $2.10 | $-11.38 | $5,130.99 | ▼ -11.38 after sell → book $11,088.80; vs 09:30 mark -2.10 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 7 | $95.72 | $2.03 | $+28.93 | $5,799.00 | ▲ +28.93 after sell → book $11,086.77; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 4 | $133.11 | $2.02 | $-72.14 | $6,329.42 | ▼ -72.14 after sell → book $11,084.74; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 987 | $0.67 | $9.79 | $-52.30 | $6,984.87 | ▼ -52.30 after sell → book $11,074.96; vs 09:30 mark -9.78 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 105 | $6.95 | $2.33 | $+31.59 | $7,712.28 | ▲ +31.59 after sell → book $11,072.62; vs 09:30 mark -2.34 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 43 | $17.66 | $2.14 | $+67.12 | $8,469.53 | ▲ +67.12 after sell → book $11,070.49; vs 09:30 mark -2.13 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 26 | $26.25 | $2.09 | $-12.48 | $9,149.94 | ▼ -12.48 after sell → book $11,068.40; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 15 | $44.52 | $2.06 | $-7.69 | $9,815.68 | ▼ -7.69 after sell → book $11,066.34; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 6 | $115.18 | $2.01 | — | $9,122.59 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $701.12; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $8,497.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list probable,yday_gainer; 🔵; ret5=+1.4; combo leftover $701.12; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 6 | $103.69 | $2.01 | — | $7,873.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; ret5=-10.3; combo leftover $701.12; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 39 | $17.93 | $2.11 | — | $7,171.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $701.12; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 7 | $93.98 | $2.01 | — | $6,511.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; combo leftover $701.12; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 16 | $43.08 | $2.04 | — | $5,820.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $701.12; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 304 | $2.30 | $3.92 | — | $5,117.31 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $701.12; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 8 | $119.43 | $2.01 | — | $4,159.86 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $1023.46; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 414 | $2.47 | $5.34 | — | $3,131.94 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; combo leftover $1023.46; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 87 | $11.70 | $2.25 | — | $2,111.79 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; combo leftover $1023.46; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 92 | $11.10 | $2.27 | — | $1,088.78 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; combo leftover $1023.46; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 315 | $3.24 | $4.06 | — | $64.12 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; combo leftover $1023.46; owner union_news_g_h1 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $64.12 | ▲ close $11,050.33 vs 09:30 $11,116.74 (session +16.01) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.12 | ▼ 09:30 equity $11,046.59 vs yday $11,050.33 (-3.74) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 14 | $43.05 | $2.05 | $-57.28 | $664.76 | ▼ -57.28 after sell → book $11,044.54; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 6 | $121.00 | $2.03 | $+30.88 | $1,388.74 | ▲ +30.88 after sell → book $11,042.51; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 1 | $653.04 | $2.01 | $+25.77 | $2,039.76 | ▲ +25.77 after sell → book $11,040.50; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WMT` | 6 | $104.14 | $2.03 | $-1.34 | $2,662.57 | ▼ -1.34 after sell → book $11,038.47; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 39 | $18.05 | $2.13 | $+0.45 | $3,364.59 | ▲ +0.45 after sell → book $11,036.34; vs 09:30 mark -2.13 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 7 | $97.02 | $2.03 | $+17.24 | $4,041.70 | ▲ +17.24 after sell → book $11,034.31; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 16 | $44.22 | $2.06 | $+14.14 | $4,747.16 | ▲ +14.14 after sell → book $11,032.25; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 304 | $2.34 | $3.98 | $+4.26 | $5,454.54 | ▲ +4.26 after sell → book $11,028.27; vs 09:30 mark -3.98 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 8 | $120.51 | $2.03 | $+4.59 | $6,416.59 | ▲ +4.59 after sell → book $11,026.24; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 414 | $2.40 | $5.42 | $-39.74 | $7,404.77 | ▼ -39.74 after sell → book $11,020.82; vs 09:30 mark -5.42 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 87 | $11.17 | $2.28 | $-50.64 | $8,374.28 | ▼ -50.64 after sell → book $11,018.54; vs 09:30 mark -2.28 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 92 | $11.48 | $2.29 | $+30.86 | $9,428.15 | ▲ +30.86 after sell → book $11,016.25; vs 09:30 mark -2.29 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 315 | $2.99 | $4.13 | $-86.94 | $10,365.88 | ▼ -86.94 after sell → book $11,012.13; vs 09:30 mark -4.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,365.88 | ▼ close $10,993.70 vs 09:30 $11,046.59 (session -18.42) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,365.88 | ▲ 09:30 equity $11,003.11 vs yday $10,993.70 (+9.41) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 11 | $57.93 | $2.04 | $-12.87 | $11,001.06 | ▼ -12.87 after sell → book $11,001.06; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 3 | $175.01 | $2.00 | — | $10,474.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; combo leftover $687.57; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 7 | $88.94 | $2.01 | — | $9,849.44 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $687.57; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 44 | $15.28 | $2.12 | — | $9,175.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $687.57; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 4 | $142.36 | $2.00 | — | $8,603.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $687.57; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 134 | $5.10 | $2.39 | — | $7,917.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $687.57; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 14 | $47.89 | $2.03 | — | $7,245.27 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $687.57; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 49 | $13.92 | $2.14 | — | $6,561.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $687.57; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 151 | $4.54 | $2.44 | — | $5,872.32 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $687.57; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 89 | $9.42 | $2.26 | — | $5,031.68 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; combo leftover $838.90; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 23 | $35.05 | $2.06 | — | $4,223.47 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $838.90; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 34 | $24.11 | $2.09 | — | $3,401.64 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=+891.7; combo leftover $838.90; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 29 | $28.86 | $2.08 | — | $2,562.62 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.7; combo leftover $838.90; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 96 | $8.72 | $2.28 | — | $1,723.23 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.0; combo leftover $838.90; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 7 | $118.52 | $2.01 | — | $891.58 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+21.7; combo leftover $838.90; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 10 | $77.13 | $2.02 | — | $118.26 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; ⚪; ret5=+13.8; combo leftover $838.90; owner union_news_g_h1 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $118.26 | ▲ close $11,048.56 vs 09:30 $11,003.11 (session +79.42) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.26 | ▼ 09:30 equity $10,928.09 vs yday $11,048.56 (-120.47) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `BMO` | 3 | $173.22 | $2.02 | $-9.39 | $635.90 | ▼ -9.39 after sell → book $10,926.07; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BNS` | 7 | $92.65 | $2.03 | $+21.93 | $1,282.42 | ▲ +21.93 after sell → book $10,924.04; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EH` | 134 | $4.77 | $2.42 | $-49.04 | $1,919.17 | ▼ -49.04 after sell → book $10,921.61; vs 09:30 mark -2.43 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GFI` | 14 | $48.24 | $2.05 | $+0.82 | $2,592.48 | ▲ +0.82 after sell → book $10,919.56; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 49 | $14.03 | $2.16 | $+1.10 | $3,277.79 | ▲ +1.10 after sell → book $10,917.40; vs 09:30 mark -2.16 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SHMD` | 151 | $3.38 | $2.48 | $-180.84 | $3,785.69 | ▼ -180.84 after sell → book $10,914.92; vs 09:30 mark -2.48 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 89 | $10.07 | $2.28 | $+53.31 | $4,679.64 | ▲ +53.31 after sell → book $10,912.64; vs 09:30 mark -2.28 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 23 | $35.70 | $2.08 | $+10.81 | $5,498.66 | ▲ +10.81 after sell → book $10,910.56; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 34 | $26.61 | $2.11 | $+80.80 | $6,401.29 | ▲ +80.80 after sell → book $10,908.45; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 29 | $27.56 | $2.10 | $-41.87 | $7,198.43 | ▼ -41.87 after sell → book $10,906.35; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 96 | $8.86 | $2.30 | $+8.86 | $8,046.69 | ▲ +8.86 after sell → book $10,904.05; vs 09:30 mark -2.30 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 7 | $119.80 | $2.03 | $+4.92 | $8,883.26 | ▲ +4.92 after sell → book $10,902.02; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 10 | $79.34 | $2.04 | $+18.04 | $9,674.62 | ▲ +18.04 after sell → book $10,899.98; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 1382 | $0.58 | $12.20 | — | $8,856.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $806.22; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 154 | $5.21 | $2.45 | — | $8,051.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $806.22; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 6 | $131.37 | $2.01 | — | $7,261.69 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; combo leftover $806.22; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 44 | $18.26 | $2.12 | — | $6,456.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $806.22; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 23 | $34.30 | $2.06 | — | $5,665.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; combo leftover $806.22; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 2 | $326.91 | $2.00 | — | $5,009.35 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-15.2; combo leftover $806.22; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 75 | $11.12 | $2.21 | — | $4,173.14 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; combo leftover $834.89; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 100 | $8.29 | $2.29 | — | $3,341.85 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; combo leftover $834.89; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 47 | $17.41 | $2.13 | — | $2,521.45 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-9.2; combo leftover $834.89; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 74 | $11.22 | $2.21 | — | $1,688.96 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.8; combo leftover $834.89; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 3 | $267.02 | $2.00 | — | $885.90 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.7; combo leftover $834.89; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 7 | $118.50 | $2.01 | — | $54.39 | — | union ∩ news_g, no 🚨; gate news=good; list overnight,overnight_mega; 🔵; ret5=-2.7; combo leftover $834.89; owner union_news_g_h1 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.39 | ▲ close $11,181.76 vs 09:30 $10,928.09 (session +317.47) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.39 | ▼ 09:30 equity $11,144.16 vs yday $11,181.76 (-37.60) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 44 | $18.50 | $2.14 | $+137.42 | $866.24 | ▲ +137.42 after sell → book $11,142.01; vs 09:30 mark -2.15 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 4 | $128.73 | $2.02 | $-58.54 | $1,379.14 | ▼ -58.54 after sell → book $11,139.99; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 1382 | $0.53 | $11.71 | $-97.16 | $2,099.89 | ▼ -97.16 after sell → book $11,128.28; vs 09:30 mark -11.71 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 154 | $5.49 | $2.49 | $+38.18 | $2,942.86 | ▲ +38.18 after sell → book $11,125.79; vs 09:30 mark -2.49 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ANF` | 6 | $144.70 | $2.03 | $+75.94 | $3,809.04 | ▲ +75.94 after sell → book $11,123.77; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BBWI` | 44 | $18.69 | $2.14 | $+14.66 | $4,629.25 | ▲ +14.66 after sell → book $11,121.62; vs 09:30 mark -2.15 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BOX` | 23 | $33.79 | $2.08 | $-15.87 | $5,404.35 | ▼ -15.87 after sell → book $11,119.55; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DY` | 2 | $314.90 | $2.02 | $-28.03 | $6,032.13 | ▼ -28.03 after sell → book $11,117.53; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 3 | $267.23 | $2.02 | $-3.39 | $6,831.80 | ▼ -3.39 after sell → book $11,115.51; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 6 | $80.60 | $2.01 | — | $6,346.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.0; combo leftover $487.99; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 30 | $16.18 | $2.08 | — | $5,858.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.7; combo leftover $487.99; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 27 | $17.78 | $2.07 | — | $5,376.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.2; combo leftover $487.99; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 36 | $13.41 | $2.10 | — | $4,891.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; combo leftover $487.99; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 5 | $97.16 | $2.00 | — | $4,403.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; combo leftover $487.99; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 2 | $206.82 | $2.00 | — | $3,988.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.2; combo leftover $487.99; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 4 | $120.17 | $2.00 | — | $3,505.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $487.99; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 28 | $41.44 | $2.07 | — | $2,343.21 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+3.1; combo leftover $1168.53; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 16 | $70.30 | $2.04 | — | $1,216.37 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=-11.2; combo leftover $1168.53; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `SRRK` | 19 | $60.00 | $2.05 | — | $74.32 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+6.2; combo leftover $1168.53; owner union_news_g_h1 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $74.32 | ▲ close $11,113.69 vs 09:30 $11,144.16 (session +18.60) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.32 | ▼ 09:30 equity $11,049.44 vs yday $11,113.69 (-64.25) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 75 | $11.27 | $2.24 | $+6.80 | $917.33 | ▲ +6.80 after sell → book $11,047.20; vs 09:30 mark -2.24 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWRD` | 47 | $17.70 | $2.15 | $+9.35 | $1,747.08 | ▲ +9.35 after sell → book $11,045.05; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TRLV` | 74 | $11.00 | $2.23 | $-20.73 | $2,558.85 | ▼ -20.73 after sell → book $11,042.82; vs 09:30 mark -2.23 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 7 | $115.66 | $2.03 | $-23.92 | $3,366.44 | ▼ -23.92 after sell → book $11,040.79; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BBY` | 6 | $83.85 | $2.03 | $+15.46 | $3,867.51 | ▲ +15.46 after sell → book $11,038.76; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BILI` | 30 | $16.94 | $2.10 | $+18.62 | $4,373.61 | ▲ +18.62 after sell → book $11,036.66; vs 09:30 mark -2.10 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CMBT` | 27 | $18.58 | $2.09 | $+17.44 | $4,873.18 | ▲ +17.44 after sell → book $11,034.57; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CSIQ` | 36 | $13.65 | $2.12 | $+4.42 | $5,362.46 | ▲ +4.42 after sell → book $11,032.45; vs 09:30 mark -2.12 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `HQY` | 5 | $93.62 | $2.02 | $-21.73 | $5,828.54 | ▼ -21.73 after sell → book $11,030.43; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RY` | 2 | $205.50 | $2.02 | $-6.65 | $6,237.52 | ▼ -6.65 after sell → book $11,028.41; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TD` | 4 | $122.07 | $2.02 | $+3.58 | $6,723.78 | ▲ +3.58 after sell → book $11,026.39; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 16 | $65.29 | $2.06 | $-84.26 | $7,766.36 | ▼ -84.26 after sell → book $11,024.33; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SRRK` | 19 | $58.75 | $2.07 | $-27.86 | $8,880.54 | ▼ -27.86 after sell → book $11,022.26; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 2 | $261.16 | $2.00 | — | $8,356.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; combo leftover $555.03; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 36 | $15.01 | $2.10 | — | $7,813.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $555.03; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 5 | $103.89 | $2.00 | — | $7,292.31 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $555.03; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 143 | $3.88 | $2.42 | — | $6,735.05 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $555.03; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 12 | $44.40 | $2.03 | — | $6,200.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $555.03; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 22 | $24.69 | $2.06 | — | $5,654.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $555.03; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 66 | $8.35 | $2.19 | — | $5,101.70 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $555.03; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 14 | $37.65 | $2.03 | — | $4,572.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $555.03; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 23 | $32.90 | $2.06 | — | $3,813.88 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; combo leftover $762.11; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 88 | $8.61 | $2.25 | — | $3,053.95 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.7; combo leftover $762.11; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 5 | $141.76 | $2.00 | — | $2,343.14 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; combo leftover $762.11; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 39 | $19.25 | $2.11 | — | $1,590.29 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+14.1; combo leftover $762.11; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 40 | $18.75 | $2.11 | — | $838.18 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-5.0; combo leftover $762.11; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 26 | $28.91 | $2.07 | — | $84.45 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+9.2; combo leftover $762.11; owner union_news_g_h1 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $84.45 | ▼ close $10,669.51 vs 09:30 $11,049.44 (session -323.33) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $84.45 | ▼ 09:30 equity $10,668.60 vs yday $10,669.51 (-0.91) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 100 | $9.50 | $2.32 | $+116.39 | $1,032.13 | ▲ +116.39 after sell → book $10,666.28; vs 09:30 mark -2.32 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 28 | $42.00 | $2.09 | $+11.51 | $2,206.04 | ▲ +11.51 after sell → book $10,664.19; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 2 | $257.71 | $2.02 | $-10.91 | $2,719.44 | ▼ -10.91 after sell → book $10,662.17; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBAR` | 36 | $14.88 | $2.12 | $-8.90 | $3,253.00 | ▼ -8.90 after sell → book $10,660.05; vs 09:30 mark -2.12 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 5 | $98.00 | $2.02 | $-33.48 | $3,740.98 | ▼ -33.48 after sell → book $10,658.03; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 143 | $3.39 | $2.45 | $-74.94 | $4,223.30 | ▼ -74.94 after sell → book $10,655.58; vs 09:30 mark -2.45 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 12 | $44.85 | $2.05 | $+1.33 | $4,759.45 | ▲ +1.33 after sell → book $10,653.53; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 22 | $22.98 | $2.08 | $-41.75 | $5,262.94 | ▼ -41.75 after sell → book $10,651.46; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 66 | $8.53 | $2.21 | $+7.48 | $5,823.71 | ▲ +7.48 after sell → book $10,649.25; vs 09:30 mark -2.21 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `IREN` | 14 | $35.81 | $2.05 | $-29.77 | $6,322.99 | ▼ -29.77 after sell → book $10,647.19; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 23 | $31.15 | $2.08 | $-44.39 | $7,037.37 | ▼ -44.39 after sell → book $10,645.12; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 88 | $8.52 | $2.28 | $-12.45 | $7,784.85 | ▼ -12.45 after sell → book $10,642.84; vs 09:30 mark -2.28 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 5 | $132.30 | $2.02 | $-51.33 | $8,444.32 | ▼ -51.33 after sell → book $10,640.81; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 39 | $17.87 | $2.13 | $-58.05 | $9,139.12 | ▼ -58.05 after sell → book $10,638.68; vs 09:30 mark -2.13 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 40 | $19.25 | $2.13 | $+15.76 | $9,906.99 | ▲ +15.76 after sell → book $10,636.55; vs 09:30 mark -2.13 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 26 | $28.06 | $2.09 | $-26.26 | $10,634.47 | ▼ -26.26 after sell → book $10,634.47; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,634.47 | ▲ close $10,634.47 vs 09:30 $10,668.60 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,634.47 | ▲ 09:30 equity $10,634.47 vs yday $10,634.47 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,634.47 | ▲ close $10,634.47 vs 09:30 $10,634.47 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,634.47 | ▲ 09:30 equity $10,634.47 vs yday $10,634.47 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,634.47 | ▲ close $10,634.47 vs 09:30 $10,634.47 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,634.47 | ▲ 09:30 equity $10,634.47 vs yday $10,634.47 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $10,280.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $664.65; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $9,924.25 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $664.65; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 13 | $47.60 | $2.03 | — | $9,303.42 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $664.65; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 61 | $10.74 | $2.17 | — | $8,645.80 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $664.65; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 96 | $6.90 | $2.28 | — | $7,981.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $664.65; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 29 | $22.32 | $2.08 | — | $7,331.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $664.65; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 2 | $257.00 | $2.00 | — | $6,815.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $664.65; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 44 | $15.09 | $2.12 | — | $6,149.69 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $664.65; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 51 | $23.88 | $2.14 | — | $4,929.67 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $1229.94; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 37 | $32.88 | $2.10 | — | $3,711.01 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+16.2; combo leftover $1229.94; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 162 | $7.59 | $2.48 | — | $2,478.95 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.5; combo leftover $1229.94; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $1,773.71 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.1; combo leftover $1229.94; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 77 | $15.87 | $2.22 | — | $549.50 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $1229.94; owner union_news_g_h1 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $549.50 | ▲ close $10,948.31 vs 09:30 $10,634.47 (session +341.44) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $549.50 | ▼ 09:30 equity $10,915.34 vs yday $10,948.31 (-32.97) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 1 | $359.70 | $2.01 | $+3.95 | $907.18 | ▲ +3.95 after sell → book $10,913.32; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 1 | $321.67 | $2.01 | $-36.83 | $1,226.84 | ▼ -36.83 after sell → book $10,911.31; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 13 | $53.85 | $2.05 | $+77.17 | $1,924.84 | ▲ +77.17 after sell → book $10,909.26; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 61 | $10.91 | $2.19 | $+5.70 | $2,588.16 | ▲ +5.70 after sell → book $10,907.07; vs 09:30 mark -2.19 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 96 | $9.28 | $2.30 | $+223.90 | $3,476.73 | ▲ +223.90 after sell → book $10,904.76; vs 09:30 mark -2.31 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CPB` | 29 | $22.10 | $2.10 | $-10.55 | $4,115.54 | ▼ -10.55 after sell → book $10,902.67; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 2 | $238.88 | $2.02 | $-40.25 | $4,591.28 | ▼ -40.25 after sell → book $10,900.65; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 44 | $15.34 | $2.14 | $+6.74 | $5,264.10 | ▲ +6.74 after sell → book $10,898.51; vs 09:30 mark -2.14 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 51 | $23.84 | $2.16 | $-6.35 | $6,477.78 | ▼ -6.35 after sell → book $10,896.35; vs 09:30 mark -2.16 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 37 | $32.48 | $2.12 | $-19.02 | $7,677.42 | ▼ -19.02 after sell → book $10,894.23; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 162 | $7.79 | $2.51 | $+27.41 | $8,936.88 | ▲ +27.41 after sell → book $10,891.71; vs 09:30 mark -2.52 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $9,626.90 | ▼ -15.23 after sell → book $10,889.70; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 9 | $63.18 | $2.02 | — | $9,056.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $601.68; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 68 | $8.74 | $2.19 | — | $8,459.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $601.68; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 8 | $68.52 | $2.01 | — | $7,909.57 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $601.68; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 166 | $3.62 | $2.49 | — | $7,307.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $601.68; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 3 | $167.55 | $2.00 | — | $6,802.35 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $601.68; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 13 | $44.90 | $2.03 | — | $6,216.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $601.68; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 6 | $98.15 | $2.01 | — | $5,625.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $601.68; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 38 | $15.70 | $2.10 | — | $5,027.01 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $601.68; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 3 | $263.36 | $2.00 | — | $4,234.93 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; combo leftover $1005.40; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 518 | $1.94 | $6.68 | — | $3,223.33 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+18.3; combo leftover $1005.40; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 7 | $137.35 | $2.01 | — | $2,259.86 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+5.4; combo leftover $1005.40; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 4 | $236.82 | $2.00 | — | $1,310.58 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.1; combo leftover $1005.40; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 13 | $75.65 | $2.03 | — | $325.10 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+6.0; combo leftover $1005.40; owner union_news_g_h1 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $325.10 | ▲ close $10,912.32 vs 09:30 $10,915.34 (session +54.20) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $325.10 | ▲ 09:30 equity $10,958.33 vs yday $10,912.32 (+46.01) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 77 | $16.74 | $2.24 | $+62.52 | $1,611.84 | ▲ +62.52 after sell → book $10,956.09; vs 09:30 mark -2.24 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AMBA` | 9 | $63.83 | $2.04 | $+1.80 | $2,184.27 | ▲ +1.80 after sell → book $10,954.05; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASAN` | 68 | $8.73 | $2.22 | $-5.09 | $2,775.70 | ▼ -5.09 after sell → book $10,951.84; vs 09:30 mark -2.21 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 8 | $67.05 | $2.03 | $-15.81 | $3,310.06 | ▼ -15.81 after sell → book $10,949.80; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOMO` | 166 | $3.84 | $2.53 | $+32.34 | $3,944.98 | ▲ +32.34 after sell → book $10,947.28; vs 09:30 mark -2.52 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 3 | $160.52 | $2.02 | $-25.11 | $4,424.52 | ▼ -25.11 after sell → book $10,945.26; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 13 | $39.56 | $2.05 | $-73.50 | $4,936.75 | ▼ -73.50 after sell → book $10,943.21; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 6 | $100.58 | $2.03 | $+10.54 | $5,538.20 | ▲ +10.54 after sell → book $10,941.18; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MAMA` | 38 | $15.20 | $2.12 | $-23.23 | $6,113.68 | ▼ -23.23 after sell → book $10,939.06; vs 09:30 mark -2.12 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 3 | $253.72 | $2.02 | $-32.94 | $6,872.82 | ▼ -32.94 after sell → book $10,937.04; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 518 | $1.94 | $6.78 | $-13.46 | $7,870.96 | ▼ -13.46 after sell → book $10,930.26; vs 09:30 mark -6.78 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 4 | $267.76 | $2.02 | $+119.74 | $8,939.98 | ▲ +119.74 after sell → book $10,928.24; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,939.98 | ▼ close $10,892.85 vs 09:30 $10,958.33 (session -35.39) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,939.98 | ▲ 09:30 equity $10,928.52 vs yday $10,892.85 (+35.67) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 7 | $141.82 | $2.03 | $+27.25 | $9,930.69 | ▲ +27.25 after sell → book $10,926.49; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 13 | $76.60 | $2.05 | $+8.27 | $10,924.44 | ▲ +8.27 after sell → book $10,924.44; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,924.44 | ▲ close $10,924.44 vs 09:30 $10,928.52 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,924.44 | ▲ 09:30 equity $10,924.44 vs yday $10,924.44 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,924.44 | ▲ close $10,924.44 vs 09:30 $10,924.44 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,924.44 | ▲ 09:30 equity $10,924.44 vs yday $10,924.44 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 4 | $164.43 | $2.00 | — | $10,264.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $682.78; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 2 | $242.17 | $2.00 | — | $9,778.38 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; combo leftover $682.78; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 115 | $5.91 | $2.33 | — | $9,096.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; combo leftover $682.78; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 21 | $32.01 | $2.05 | — | $8,422.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $682.78; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 9 | $71.71 | $2.02 | — | $7,774.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; combo leftover $682.78; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 12 | $56.02 | $2.03 | — | $7,100.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; combo leftover $682.78; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 72 | $9.37 | $2.21 | — | $6,423.61 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $682.78; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 52 | $13.10 | $2.15 | — | $5,740.27 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; combo leftover $682.78; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 703 | $2.04 | $9.07 | — | $4,297.08 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; combo leftover $1435.07; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 676 | $2.12 | $8.72 | — | $2,855.24 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; combo leftover $1435.07; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 95 | $15.01 | $2.27 | — | $1,427.01 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $1435.07; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 10 | $135.71 | $2.02 | — | $67.89 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-9.2; combo leftover $1435.07; owner union_news_g_h1 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.89 | ▼ close $10,823.58 vs 09:30 $10,924.44 (session -61.99) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.89 | ▼ 09:30 equity $10,783.56 vs yday $10,823.58 (-40.02) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 4 | $141.42 | $2.02 | $-96.06 | $631.55 | ▼ -96.06 after sell → book $10,781.54; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 2 | $261.51 | $2.02 | $+34.67 | $1,152.56 | ▲ +34.67 after sell → book $10,779.53; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DBI` | 115 | $5.86 | $2.36 | $-10.45 | $1,824.09 | ▼ -10.45 after sell → book $10,777.16; vs 09:30 mark -2.37 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CPRT` | 21 | $30.63 | $2.07 | $-33.11 | $2,465.25 | ▼ -33.11 after sell → book $10,775.09; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DSGX` | 9 | $77.68 | $2.04 | $+49.68 | $3,162.33 | ▲ +49.68 after sell → book $10,773.05; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `KR` | 12 | $59.31 | $2.05 | $+35.41 | $3,872.00 | ▲ +35.41 after sell → book $10,771.00; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `LPTH` | 72 | $8.85 | $2.23 | $-41.87 | $4,506.98 | ▼ -41.87 after sell → book $10,768.78; vs 09:30 mark -2.22 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `REF` | 52 | $14.16 | $2.17 | $+50.81 | $5,241.13 | ▲ +50.81 after sell → book $10,766.61; vs 09:30 mark -2.17 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 703 | $2.01 | $9.20 | $-39.36 | $6,644.96 | ▼ -39.36 after sell → book $10,757.41; vs 09:30 mark -9.20 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 676 | $2.05 | $8.84 | $-64.88 | $8,021.92 | ▼ -64.88 after sell → book $10,748.57; vs 09:30 mark -8.84 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 10 | $131.40 | $2.04 | $-47.16 | $9,333.88 | ▼ -47.16 after sell → book $10,746.53; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,333.88 | ▲ close $10,774.08 vs 09:30 $10,783.56 (session +27.55) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,333.88 | ▲ 09:30 equity $10,779.78 vs yday $10,774.08 (+5.70) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,333.88 | ▲ close $10,800.68 vs 09:30 $10,779.78 (session +20.90) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,333.88 | ▲ 09:30 equity $10,809.23 vs yday $10,800.68 (+8.55) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 70 | $33.14 | $2.20 | — | $7,011.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; combo leftover $2333.47; owner union_e_fresh_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 57 | $40.93 | $2.16 | — | $4,676.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $2333.47; owner union_e_fresh_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 44 | $26.27 | $2.12 | — | $3,518.71 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+10.0; combo leftover $1169.18; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 168 | $6.95 | $2.49 | — | $2,348.61 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-5.8; combo leftover $1169.18; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 29 | $39.99 | $2.08 | — | $1,186.83 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+9.3; combo leftover $1169.18; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 6 | $189.17 | $2.01 | — | $49.80 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+7.9; combo leftover $1169.18; owner union_news_g_h1 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.80 | ▲ close $10,844.42 vs 09:30 $10,809.23 (session +48.25) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.80 | ▲ 09:30 equity $11,069.41 vs yday $10,844.42 (+224.99) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `FPS` | 70 | $36.76 | $2.23 | $+248.97 | $2,620.77 | ▲ +248.97 after sell → book $11,067.18; vs 09:30 mark -2.23 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TCOM` | 57 | $40.79 | $2.19 | $-12.33 | $4,943.61 | ▼ -12.33 after sell → book $11,064.99; vs 09:30 mark -2.19 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 44 | $26.51 | $2.14 | $+6.30 | $6,107.90 | ▲ +6.30 after sell → book $11,062.84; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 168 | $7.27 | $2.53 | $+48.73 | $7,326.73 | ▲ +48.73 after sell → book $11,060.31; vs 09:30 mark -2.53 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 29 | $37.57 | $2.10 | $-74.35 | $8,414.17 | ▼ -74.35 after sell → book $11,058.22; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 6 | $190.35 | $2.03 | $+3.04 | $9,554.24 | ▲ +3.04 after sell → book $11,056.19; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 213 | $11.21 | $2.75 | — | $7,163.76 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.0; combo leftover $2388.56; owner union_e_fresh_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 29 | $81.00 | $2.08 | — | $4,812.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.0; combo leftover $2388.56; owner union_e_fresh_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 4 | $170.85 | $2.00 | — | $4,127.28 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; combo leftover $802.11; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 45 | $17.72 | $2.12 | — | $3,327.76 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=-8.3; combo leftover $802.11; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 453 | $1.77 | $5.84 | — | $2,520.10 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-10.2; combo leftover $802.11; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 3 | $238.60 | $2.00 | — | $1,802.30 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.6; combo leftover $802.11; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 36 | $22.12 | $2.10 | — | $1,003.88 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+10.5; combo leftover $802.11; owner union_news_g_h1 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,003.88 | ▲ close $11,110.07 vs 09:30 $11,069.41 (session +72.77) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,003.88 | ▼ 09:30 equity $11,096.88 vs yday $11,110.07 (-13.19) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 95 | $15.87 | $2.30 | $+77.12 | $2,509.23 | ▲ +77.12 after sell → book $11,094.58; vs 09:30 mark -2.30 | union_news_g_h1: dropped from list after 5 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ALMU` | 213 | $11.64 | $2.80 | $+86.04 | $4,985.75 | ▲ +86.04 after sell → book $11,091.78; vs 09:30 mark -2.80 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LEN` | 29 | $78.25 | $2.11 | $-83.93 | $7,252.89 | ▼ -83.93 after sell → book $11,089.67; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 4 | $182.33 | $2.02 | $+41.90 | $7,980.19 | ▲ +41.90 after sell → book $11,087.65; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 45 | $17.13 | $2.15 | $-30.82 | $8,748.90 | ▼ -30.82 after sell → book $11,085.51; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 453 | $1.77 | $5.93 | $-11.77 | $9,544.78 | ▼ -11.77 after sell → book $11,079.58; vs 09:30 mark -5.93 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 3 | $236.80 | $2.02 | $-9.42 | $10,253.16 | ▼ -9.42 after sell → book $11,077.56; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 182 | $14.07 | $2.54 | — | $7,689.88 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; combo leftover $2563.29; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 173 | $14.79 | $2.51 | — | $5,128.70 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; combo leftover $2563.29; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 340 | $7.54 | $4.39 | — | $2,562.42 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-20.9; combo leftover $2563.29; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 122 | $20.91 | $2.36 | — | $9.04 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+8.6; combo leftover $2563.29; owner union_news_g_h1 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.04 | ▼ close $10,887.13 vs 09:30 $11,096.88 (session -178.64) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.04 | ▲ 09:30 equity $11,024.96 vs yday $10,887.13 (+137.83) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 36 | $22.78 | $2.12 | $+19.54 | $827.00 | ▲ +19.54 after sell → book $11,022.84; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 182 | $13.90 | $2.59 | $-36.06 | $3,354.22 | ▼ -36.06 after sell → book $11,020.26; vs 09:30 mark -2.58 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 173 | $14.58 | $2.56 | $-41.40 | $5,874.00 | ▼ -41.40 after sell → book $11,017.70; vs 09:30 mark -2.56 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 340 | $7.36 | $4.46 | $-68.35 | $8,371.94 | ▼ -68.35 after sell → book $11,013.24; vs 09:30 mark -4.46 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 122 | $21.65 | $2.40 | $+85.53 | $11,010.84 | ▲ +85.53 after sell → book $11,010.84; vs 09:30 mark -2.40 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 70 | $25.95 | $2.20 | — | $9,192.14 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.2; combo leftover $1835.14; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 853 | $2.15 | $11.00 | — | $7,347.19 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+7.5; combo leftover $1835.14; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 131 | $13.94 | $2.38 | — | $5,518.66 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; combo leftover $1835.14; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 305 | $6.00 | $3.93 | — | $3,684.73 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-24.1; combo leftover $1835.14; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 9 | $190.30 | $2.02 | — | $1,970.01 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+10.6; combo leftover $1835.14; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 7 | $230.25 | $2.01 | — | $356.25 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+12.5; combo leftover $1835.14; owner union_news_g_h1 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $356.25 | ▼ close $10,697.23 vs 09:30 $11,024.96 (session -290.06) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $356.25 | ▼ 09:30 equity $10,674.53 vs yday $10,697.23 (-22.70) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `MARA` | 131 | $13.13 | $2.42 | $-110.91 | $2,073.86 | ▼ -110.91 after sell → book $10,672.11; vs 09:30 mark -2.42 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 305 | $5.99 | $4.00 | $-10.98 | $3,896.81 | ▼ -10.98 after sell → book $10,668.11; vs 09:30 mark -4.00 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 643 | $1.01 | $8.29 | — | $3,239.09 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+14.3; combo leftover $649.47; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `MRNA` | 3 | $168.50 | $2.00 | — | $2,731.59 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+17.9; combo leftover $649.47; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 151 | $4.30 | $2.44 | — | $2,079.85 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; combo leftover $649.47; owner union_news_g_h1 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,079.85 | ▲ close $10,655.90 vs 09:30 $10,674.53 (session +0.52) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,079.85 | ▲ 09:30 equity $10,969.58 vs yday $10,655.90 (+313.68) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 70 | $26.58 | $2.23 | $+39.67 | $3,938.22 | ▲ +39.67 after sell → book $10,967.35; vs 09:30 mark -2.23 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMTX` | 853 | $2.09 | $11.16 | $-73.34 | $5,709.83 | ▼ -73.34 after sell → book $10,956.19; vs 09:30 mark -11.16 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 9 | $174.50 | $2.04 | $-146.26 | $7,278.29 | ▼ -146.26 after sell → book $10,954.16; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 7 | $266.50 | $2.04 | $+249.70 | $9,141.75 | ▲ +249.70 after sell → book $10,952.12; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 643 | $0.95 | $8.15 | $-55.03 | $9,744.45 | ▼ -55.03 after sell → book $10,943.96; vs 09:30 mark -8.16 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MRNA` | 3 | $183.41 | $2.02 | $+40.70 | $10,292.65 | ▲ +40.70 after sell → book $10,941.95; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 5 | $196.78 | $2.00 | — | $9,306.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $1029.26; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 21 | $47.57 | $2.05 | — | $8,305.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $1029.26; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 28 | $35.74 | $2.07 | — | $7,302.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $1029.26; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 21 | $47.15 | $2.05 | — | $6,310.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $1029.26; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 9 | $109.67 | $2.02 | — | $5,321.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $1029.26; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 133 | $7.95 | $2.39 | — | $4,261.93 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ⚪; ret5=+12.4; combo leftover $1064.33; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 67 | $15.72 | $2.19 | — | $3,206.50 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; combo leftover $1064.33; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 872 | $1.22 | $11.25 | — | $2,131.42 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-33.0; combo leftover $1064.33; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 818 | $1.30 | $10.55 | — | $1,057.46 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; combo leftover $1064.33; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 26 | $40.00 | $2.07 | — | $15.39 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+6.7; combo leftover $1064.33; owner union_news_g_h1 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.39 | ▼ close $10,627.26 vs 09:30 $10,969.58 (session -276.03) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.39 | ▼ 09:30 equity $10,554.66 vs yday $10,627.26 (-72.60) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `DGXX` | 151 | $4.12 | $2.48 | $-32.10 | $635.04 | ▼ -32.10 after sell → book $10,552.18; vs 09:30 mark -2.48 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 5 | $192.26 | $2.02 | $-26.63 | $1,594.31 | ▼ -26.63 after sell → book $10,550.16; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CBRL` | 21 | $46.88 | $2.07 | $-18.62 | $2,576.72 | ▼ -18.62 after sell → book $10,548.09; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `GIS` | 28 | $35.96 | $2.09 | $+1.99 | $3,581.50 | ▲ +1.99 after sell → book $10,545.99; vs 09:30 mark -2.10 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `KBH` | 21 | $47.14 | $2.07 | $-4.34 | $4,569.37 | ▼ -4.34 after sell → book $10,543.92; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PAYX` | 9 | $105.49 | $2.04 | $-41.66 | $5,516.76 | ▼ -41.66 after sell → book $10,541.88; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 133 | $7.38 | $2.42 | $-80.62 | $6,495.88 | ▼ -80.62 after sell → book $10,539.46; vs 09:30 mark -2.42 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 67 | $14.38 | $2.21 | $-94.18 | $7,457.13 | ▼ -94.18 after sell → book $10,537.25; vs 09:30 mark -2.21 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CMPX` | 872 | $1.17 | $11.40 | $-66.25 | $8,465.97 | ▼ -66.25 after sell → book $10,525.85; vs 09:30 mark -11.40 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 818 | $1.27 | $10.70 | $-45.79 | $9,494.13 | ▼ -45.79 after sell → book $10,515.15; vs 09:30 mark -10.70 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 26 | $39.27 | $2.09 | $-23.14 | $10,513.06 | ▼ -23.14 after sell → book $10,513.06; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,513.06 | ▲ close $10,513.06 vs 09:30 $10,554.66 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,525.15 | ▲ 09:30 equity $8,525.15 vs yday $8,525.15 (+0.00) | 09:30 open · cash $8,525.15 (unchanged overnight, no fees) · equity $8,525.15 vs prior close $8,525.15 (+0.00) | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 4 | $887.00 | $2.00 | — | $4,975.15 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+0.3; combo leftover $4262.57; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 322 | $3.86 | $4.15 | — | $3,728.07 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; combo leftover $1243.79; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 76 | $16.21 | $2.22 | — | $2,493.90 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $1243.79; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 4 | $272.16 | $2.00 | — | $1,403.25 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+11.7; combo leftover $1243.79; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 16 | $74.15 | $2.04 | — | $214.82 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.5; combo leftover $1243.79; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $214.82 | ▲ close $8,599.20 vs 09:30 $8,525.15 (session +86.46) | 16:00 close · cash $214.82 · equity $8,599.20 vs 09:30 $8,525.15 (+74.05; session marks +86.46) · 5 name(s) marked open→close (per-name table). COST×4 09:30 $887.00 → close $922.76 +143.06; ZSQR×322 09:30 $3.86 → close $3.78 -25.76; SECZ×76 09:30 $16.21 → close $15.96 -19.00; ILMN×4 09:30 $272.16 → close $270.00 -8.64; RKLB×16 09:30 $74.15 → close $73.95 -3.20 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 910.31 < 1 share @ 1646.93 |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_e_fresh_h1 |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new long union_e_fresh_h1 |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new long union_news_g_h1 |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new long union_news_g_h1 |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new long union_news_g_h1 |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new long union_news_g_h1 |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new long union_news_g_h1 |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new long union_news_g_h1 |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new long union_news_g_h1 |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new long union_news_g_h1 |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-17 | `LITE` | cash | leftover split 802.11 < 1 share @ 934.88 |
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMTX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `YSS` | no_price | no 09:30 open |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `ZSQR` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
