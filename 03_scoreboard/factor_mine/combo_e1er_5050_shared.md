# Factor mine action — `combo_e1er_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_e_fresh_h1/union_earn_react_h3 w=0.5,0.5 net=priority

Cash book **-4.84%** ($9,516) · signal-only (no cash/fees) was —. Starts YES **8/30**. Fills 205 · skips 137 · realized $-376.82.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_e_fresh_h1 50%, union_earn_react_h3 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_e_fresh_h1 50%, union_earn_react_h3 50%.
- Member: union_e_fresh_h1 (50% · long · hold 1).
- Member: union_earn_react_h3 (50% · long · hold 3).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $288.18.

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
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 453 | $1.50 | $5.84 | — | $10,198.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 34 | $19.57 | $2.09 | — | $9,530.86 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 61 | $11.12 | $2.17 | — | $8,850.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 50 | $13.55 | $2.14 | — | $8,170.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 62 | $10.83 | $2.18 | — | $7,497.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-30.1; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 576 | $1.18 | $7.43 | — | $6,809.98 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 35 | $19.17 | $2.10 | — | $6,136.93 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 68 | $9.89 | $2.19 | — | $5,461.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRJ` | 141 | $5.51 | $2.41 | — | $4,682.56 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+13.1; combo leftover $780.27; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `AMAT` | 1 | $499.40 | $1.99 | — | $4,181.16 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+1.3; combo leftover $780.27; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `AMPG` | 178 | $4.37 | $2.52 | — | $3,401.13 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+10.3; combo leftover $780.27; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 29 | $26.25 | $2.08 | — | $2,637.95 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+31.2; combo leftover $780.27; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1018 | $0.77 | $10.85 | — | $1,847.31 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+20.4; combo leftover $780.27; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `DEFT` | 1660 | $0.47 | $12.78 | — | $1,054.33 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+11.1; combo leftover $780.27; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `DGXX` | 199 | $3.92 | $2.59 | — | $271.66 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+10.1; combo leftover $780.27; owner union_earn_react_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $271.66 | ▼ close $10,619.86 vs 09:30 $10,963.61 (session -202.44) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $271.66 | ▲ 09:30 equity $10,640.79 vs yday $10,619.86 (+20.93) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 453 | $1.52 | $5.93 | $-2.71 | $954.29 | ▼ -2.71 after sell → book $10,634.86; vs 09:30 mark -5.93 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 34 | $19.57 | $2.11 | $-4.20 | $1,617.56 | ▼ -4.20 after sell → book $10,632.75; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 61 | $9.57 | $2.19 | $-98.92 | $2,199.14 | ▼ -98.92 after sell → book $10,630.55; vs 09:30 mark -2.20 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 50 | $13.16 | $2.16 | $-23.80 | $2,854.98 | ▼ -23.80 after sell → book $10,628.39; vs 09:30 mark -2.16 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 62 | $11.19 | $2.20 | $+17.95 | $3,546.56 | ▲ +17.95 after sell → book $10,626.20; vs 09:30 mark -2.19 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `EU` | 576 | $1.21 | $7.54 | $+2.31 | $4,235.99 | ▲ +2.31 after sell → book $10,618.66; vs 09:30 mark -7.54 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 35 | $20.25 | $2.12 | $+33.59 | $4,942.62 | ▲ +33.59 after sell → book $10,616.55; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 68 | $10.97 | $2.22 | $+68.69 | $5,686.37 | ▲ +68.69 after sell → book $10,614.33; vs 09:30 mark -2.22 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,686.37 | ▼ close $10,429.58 vs 09:30 $10,640.79 (session -184.75) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,686.37 | ▼ 09:30 equity $10,245.64 vs yday $10,429.58 (-183.94) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,686.37 | ▼ close $10,168.26 vs 09:30 $10,245.64 (session -77.37) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,686.37 | ▲ 09:30 equity $10,201.02 vs yday $10,168.26 (+32.76) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRJ` | 141 | $5.33 | $2.45 | $-30.24 | $6,435.45 | ▼ -30.24 after sell → book $10,198.57; vs 09:30 mark -2.45 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AMAT` | 1 | $507.87 | $2.01 | $+4.46 | $6,941.30 | ▲ +4.46 after sell → book $10,196.56; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AMPG` | 178 | $3.56 | $2.56 | $-148.91 | $7,572.42 | ▼ -148.91 after sell → book $10,194.00; vs 09:30 mark -2.56 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BRUN` | 29 | $20.38 | $2.10 | $-174.40 | $8,161.20 | ▼ -174.40 after sell → book $10,191.90; vs 09:30 mark -2.10 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BZAI` | 1018 | $0.57 | $9.04 | $-219.42 | $8,732.42 | ▼ -219.42 after sell → book $10,182.86; vs 09:30 mark -9.04 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DEFT` | 1660 | $0.43 | $12.49 | $-83.37 | $9,442.04 | ▼ -83.37 after sell → book $10,170.38; vs 09:30 mark -12.48 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DGXX` | 199 | $3.66 | $2.63 | $-56.96 | $10,167.75 | ▼ -56.96 after sell → book $10,167.75; vs 09:30 mark -2.63 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,167.75 | ▲ close $10,167.75 vs 09:30 $10,201.02 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,167.75 | ▲ 09:30 equity $10,167.75 vs yday $10,167.75 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 6 | $97.43 | $2.01 | — | $9,581.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $635.48; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 143 | $4.43 | $2.42 | — | $8,945.25 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $635.48; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 2118 | $0.30 | $12.71 | — | $8,297.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $635.48; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 13 | $46.85 | $2.03 | — | $7,686.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $635.48; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 70 | $9.01 | $2.20 | — | $7,053.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $635.48; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 163 | $3.89 | $2.48 | — | $6,416.61 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $635.48; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 18 | $34.05 | $2.04 | — | $5,801.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $635.48; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 28 | $22.44 | $2.07 | — | $5,171.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $635.48; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 13 | $123.47 | $2.03 | — | $3,564.14 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+2.9; combo leftover $1723.76; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `BILL` | 35 | $49.00 | $2.10 | — | $1,847.04 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-2.0; combo leftover $1723.76; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 173 | $9.94 | $2.51 | — | $124.91 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+12.6; combo leftover $1723.76; owner union_earn_react_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.91 | ▼ close $10,025.80 vs 09:30 $10,167.75 (session -107.36) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.91 | ▼ 09:30 equity $10,006.02 vs yday $10,025.80 (-19.78) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 6 | $96.75 | $2.03 | $-8.12 | $703.38 | ▼ -8.12 after sell → book $10,003.99; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TOYO` | 143 | $4.68 | $2.45 | $+30.88 | $1,370.17 | ▲ +30.88 after sell → book $10,001.54; vs 09:30 mark -2.45 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DVLT` | 2118 | $0.31 | $13.28 | $-4.81 | $2,013.47 | ▼ -4.81 after sell → book $9,988.26; vs 09:30 mark -13.28 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AEG` | 70 | $9.04 | $2.22 | $-2.32 | $2,644.05 | ▼ -2.32 after sell → book $9,986.04; vs 09:30 mark -2.22 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALVO` | 163 | $4.32 | $2.52 | $+65.09 | $3,345.69 | ▲ +65.09 after sell → book $9,983.52; vs 09:30 mark -2.52 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 18 | $34.31 | $2.06 | $+0.57 | $3,961.21 | ▲ +0.57 after sell → book $9,981.46; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 28 | $22.20 | $2.09 | $-10.89 | $4,580.72 | ▼ -10.89 after sell → book $9,979.37; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 2 | $115.18 | $2.00 | — | $4,348.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $327.19; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 3 | $103.69 | $2.00 | — | $4,035.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; ret5=-10.3; combo leftover $327.19; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 18 | $17.93 | $2.04 | — | $3,710.42 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $327.19; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 3 | $93.98 | $2.00 | — | $3,426.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; combo leftover $327.19; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 7 | $43.08 | $2.01 | — | $3,122.91 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $327.19; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 142 | $2.30 | $2.42 | — | $2,793.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $327.19; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `ROST` | 11 | $243.85 | $2.02 | — | $109.52 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-6.5; combo leftover $2793.89; owner union_earn_react_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $109.52 | ▼ close $9,841.23 vs 09:30 $10,006.02 (session -123.65) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.52 | ▼ 09:30 equity $9,773.18 vs yday $9,841.23 (-68.05) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 13 | $43.05 | $2.05 | $-53.48 | $667.12 | ▼ -53.48 after sell → book $9,771.13; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 2 | $121.00 | $2.02 | $+7.63 | $907.10 | ▲ +7.63 after sell → book $9,769.11; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WMT` | 3 | $104.14 | $2.02 | $-2.67 | $1,217.50 | ▼ -2.67 after sell → book $9,767.09; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 18 | $18.05 | $2.06 | $-1.95 | $1,540.43 | ▼ -1.95 after sell → book $9,765.03; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 3 | $97.02 | $2.02 | $+5.10 | $1,829.47 | ▲ +5.10 after sell → book $9,763.01; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 7 | $44.22 | $2.03 | $+3.94 | $2,136.98 | ▲ +3.94 after sell → book $9,760.98; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 142 | $2.34 | $2.45 | $+0.81 | $2,466.81 | ▲ +0.81 after sell → book $9,758.53; vs 09:30 mark -2.45 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,466.81 | ▲ close $9,824.93 vs 09:30 $9,773.18 (session +66.40) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,466.81 | ▼ 09:30 equity $9,799.58 vs yday $9,824.93 (-25.35) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `BABA` | 13 | $117.94 | $2.05 | $-75.97 | $3,997.98 | ▼ -75.97 after sell → book $9,797.53; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BILL` | 35 | $47.98 | $2.12 | $-39.74 | $5,675.33 | ▼ -39.74 after sell → book $9,795.41; vs 09:30 mark -2.12 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BULL` | 173 | $8.46 | $2.55 | $-261.10 | $7,136.37 | ▼ -261.10 after sell → book $9,792.87; vs 09:30 mark -2.54 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 5 | $175.01 | $2.00 | — | $6,259.31 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; combo leftover $892.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 10 | $88.94 | $2.02 | — | $5,367.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $892.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 58 | $15.28 | $2.16 | — | $4,479.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $892.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 6 | $142.36 | $2.01 | — | $3,623.32 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $892.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 174 | $5.10 | $2.51 | — | $2,733.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $892.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 18 | $47.89 | $2.04 | — | $1,869.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $892.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 64 | $13.92 | $2.18 | — | $976.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $892.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 196 | $4.54 | $2.58 | — | $82.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $892.05; owner union_e_fresh_h1 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.88 | ▼ close $9,513.91 vs 09:30 $9,799.58 (session -261.44) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.88 | ▼ 09:30 equity $9,505.56 vs yday $9,513.91 (-8.35) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ROST` | 11 | $242.50 | $2.05 | $-18.93 | $2,748.33 | ▼ -18.93 after sell → book $9,503.51; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMO` | 5 | $173.22 | $2.02 | $-12.98 | $3,612.40 | ▼ -12.98 after sell → book $9,501.48; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BNS` | 10 | $92.65 | $2.04 | $+33.04 | $4,536.86 | ▲ +33.04 after sell → book $9,499.44; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EH` | 174 | $4.77 | $2.55 | $-62.48 | $5,364.29 | ▼ -62.48 after sell → book $9,496.89; vs 09:30 mark -2.55 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GFI` | 18 | $48.24 | $2.06 | $+2.19 | $6,230.55 | ▲ +2.19 after sell → book $9,494.83; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 64 | $14.03 | $2.20 | $+2.66 | $7,126.27 | ▲ +2.66 after sell → book $9,492.63; vs 09:30 mark -2.20 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SHMD` | 196 | $3.38 | $2.62 | $-233.54 | $7,786.13 | ▼ -233.54 after sell → book $9,490.01; vs 09:30 mark -2.62 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 1112 | $0.58 | $9.82 | — | $7,128.01 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $648.84; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 124 | $5.21 | $2.36 | — | $6,479.61 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $648.84; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 4 | $131.37 | $2.00 | — | $5,952.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; combo leftover $648.84; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 35 | $18.26 | $2.10 | — | $5,310.93 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $648.84; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 18 | $34.30 | $2.04 | — | $4,691.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; combo leftover $648.84; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 1 | $326.91 | $1.99 | — | $4,362.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-15.2; combo leftover $648.84; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FSCO` | 286 | $5.08 | $3.69 | — | $2,906.01 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-1.6; combo leftover $1454.19; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `HEI` | 3 | $370.00 | $2.00 | — | $1,794.02 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.6; combo leftover $1454.19; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `INTU` | 4 | $323.47 | $2.00 | — | $498.13 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+2.0; combo leftover $1454.19; owner union_earn_react_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $498.13 | ▲ close $9,708.17 vs 09:30 $9,505.56 (session +246.17) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $498.13 | ▼ 09:30 equity $9,681.03 vs yday $9,708.17 (-27.14) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 58 | $18.50 | $2.18 | $+182.41 | $1,568.95 | ▲ +182.41 after sell → book $9,678.85; vs 09:30 mark -2.18 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 6 | $128.73 | $2.03 | $-85.82 | $2,339.30 | ▼ -85.82 after sell → book $9,676.82; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 1112 | $0.53 | $9.42 | $-78.18 | $2,919.24 | ▼ -78.18 after sell → book $9,667.40; vs 09:30 mark -9.42 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 124 | $5.49 | $2.39 | $+29.97 | $3,597.60 | ▲ +29.97 after sell → book $9,665.00; vs 09:30 mark -2.40 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ANF` | 4 | $144.70 | $2.02 | $+49.30 | $4,174.38 | ▲ +49.30 after sell → book $9,662.98; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BBWI` | 35 | $18.69 | $2.12 | $+10.84 | $4,826.42 | ▲ +10.84 after sell → book $9,660.87; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BOX` | 18 | $33.79 | $2.06 | $-13.29 | $5,432.57 | ▼ -13.29 after sell → book $9,658.80; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DY` | 1 | $314.90 | $2.01 | $-16.02 | $5,745.46 | ▼ -16.02 after sell → book $9,656.79; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 8 | $80.60 | $2.01 | — | $5,098.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.0; combo leftover $718.18; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 44 | $16.18 | $2.12 | — | $4,384.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.7; combo leftover $718.18; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 6 | $118.77 | $2.01 | — | $3,669.98 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.3; combo leftover $718.18; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 40 | $17.78 | $2.11 | — | $2,956.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.2; combo leftover $718.18; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 53 | $13.41 | $2.15 | — | $2,243.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; combo leftover $718.18; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 7 | $97.16 | $2.01 | — | $1,561.66 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; combo leftover $718.18; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 3 | $206.82 | $2.00 | — | $939.20 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.2; combo leftover $718.18; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 5 | $120.17 | $2.00 | — | $336.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $718.18; owner union_e_fresh_h1 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $336.34 | ▲ close $9,643.81 vs 09:30 $9,681.03 (session +3.44) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $336.34 | ▲ 09:30 equity $9,670.75 vs yday $9,643.81 (+26.94) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BBY` | 8 | $83.85 | $2.03 | $+21.95 | $1,005.11 | ▲ +21.95 after sell → book $9,668.72; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BILI` | 44 | $16.94 | $2.14 | $+29.18 | $1,748.33 | ▲ +29.18 after sell → book $9,666.58; vs 09:30 mark -2.14 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 6 | $115.66 | $2.03 | $-22.70 | $2,440.26 | ▼ -22.70 after sell → book $9,664.55; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CMBT` | 40 | $18.58 | $2.13 | $+27.76 | $3,181.33 | ▲ +27.76 after sell → book $9,662.42; vs 09:30 mark -2.13 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CSIQ` | 53 | $13.65 | $2.17 | $+8.40 | $3,902.61 | ▲ +8.40 after sell → book $9,660.25; vs 09:30 mark -2.17 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `HQY` | 7 | $93.62 | $2.03 | $-28.82 | $4,555.92 | ▼ -28.82 after sell → book $9,658.22; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RY` | 3 | $205.50 | $2.02 | $-7.98 | $5,170.40 | ▼ -7.98 after sell → book $9,656.20; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TD` | 5 | $122.07 | $2.02 | $+5.47 | $5,778.72 | ▲ +5.47 after sell → book $9,654.17; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 2 | $261.16 | $2.00 | — | $5,254.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; combo leftover $722.34; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 48 | $15.01 | $2.13 | — | $4,531.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $722.34; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 6 | $103.89 | $2.01 | — | $3,906.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $722.34; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 186 | $3.88 | $2.55 | — | $3,182.22 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $722.34; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 16 | $44.40 | $2.04 | — | $2,469.78 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $722.34; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 29 | $24.69 | $2.08 | — | $1,751.69 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $722.34; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 86 | $8.35 | $2.25 | — | $1,031.35 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $722.34; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 19 | $37.65 | $2.05 | — | $314.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $722.34; owner union_e_fresh_h1 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $314.04 | ▼ close $9,475.02 vs 09:30 $9,670.75 (session -162.06) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $314.04 | ▲ 09:30 equity $9,476.27 vs yday $9,475.02 (+1.25) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `FSCO` | 286 | $5.20 | $3.75 | $+26.88 | $1,797.50 | ▲ +26.88 after sell → book $9,472.53; vs 09:30 mark -3.74 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `HEI` | 3 | $334.88 | $2.02 | $-109.38 | $2,800.12 | ▼ -109.38 after sell → book $9,470.51; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `INTU` | 4 | $356.05 | $2.02 | $+126.29 | $4,222.29 | ▲ +126.29 after sell → book $9,468.48; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 2 | $257.71 | $2.02 | $-10.91 | $4,735.70 | ▼ -10.91 after sell → book $9,466.47; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBAR` | 48 | $14.88 | $2.15 | $-10.53 | $5,447.78 | ▼ -10.53 after sell → book $9,464.31; vs 09:30 mark -2.16 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 6 | $98.00 | $2.03 | $-39.38 | $6,033.75 | ▼ -39.38 after sell → book $9,462.28; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 186 | $3.39 | $2.59 | $-96.28 | $6,661.71 | ▼ -96.28 after sell → book $9,459.70; vs 09:30 mark -2.58 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 16 | $44.85 | $2.06 | $+3.10 | $7,377.25 | ▲ +3.10 after sell → book $9,457.64; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 29 | $22.98 | $2.10 | $-53.76 | $8,041.57 | ▼ -53.76 after sell → book $9,455.54; vs 09:30 mark -2.10 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 86 | $8.53 | $2.27 | $+10.96 | $8,772.88 | ▲ +10.96 after sell → book $9,453.27; vs 09:30 mark -2.27 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `IREN` | 19 | $35.81 | $2.07 | $-38.98 | $9,451.20 | ▼ -38.98 after sell → book $9,451.20; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,451.20 | ▲ close $9,451.20 vs 09:30 $9,476.27 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,451.20 | ▲ 09:30 equity $9,451.20 vs yday $9,451.20 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,451.20 | ▲ close $9,451.20 vs 09:30 $9,451.20 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,451.20 | ▲ 09:30 equity $9,451.20 vs yday $9,451.20 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,451.20 | ▲ close $9,451.20 vs 09:30 $9,451.20 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,451.20 | ▲ 09:30 equity $9,451.20 vs yday $9,451.20 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 109 | $10.74 | $2.32 | — | $8,277.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $1181.40; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $7,220.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $1181.40; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 171 | $6.90 | $2.50 | — | $6,038.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $1181.40; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 3 | $354.49 | $2.00 | — | $4,972.59 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $1181.40; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 52 | $22.32 | $2.15 | — | $3,809.80 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $1181.40; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 4 | $257.00 | $2.00 | — | $2,779.80 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $1181.40; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 24 | $47.60 | $2.06 | — | $1,635.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $1181.40; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 78 | $15.09 | $2.22 | — | $456.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $1181.40; owner union_e_fresh_h1 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $456.09 | ▲ close $9,832.85 vs 09:30 $9,451.20 (session +398.91) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $456.09 | ▲ 09:30 equity $9,869.91 vs yday $9,832.85 (+37.06) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 109 | $10.91 | $2.35 | $+13.32 | $1,642.94 | ▲ +13.32 after sell → book $9,867.57; vs 09:30 mark -2.34 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $2,720.02 | ▲ +19.86 after sell → book $9,865.55; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 171 | $9.28 | $2.54 | $+401.93 | $4,304.36 | ▲ +401.93 after sell → book $9,863.01; vs 09:30 mark -2.54 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 3 | $321.67 | $2.02 | $-102.48 | $5,267.35 | ▼ -102.48 after sell → book $9,860.99; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CPB` | 52 | $22.10 | $2.17 | $-15.75 | $6,414.38 | ▼ -15.75 after sell → book $9,858.82; vs 09:30 mark -2.17 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 4 | $238.88 | $2.02 | $-76.50 | $7,367.88 | ▼ -76.50 after sell → book $9,856.80; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 24 | $53.85 | $2.08 | $+145.86 | $8,658.20 | ▲ +145.86 after sell → book $9,854.72; vs 09:30 mark -2.08 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 78 | $15.34 | $2.25 | $+15.03 | $9,852.47 | ▲ +15.03 after sell → book $9,852.47; vs 09:30 mark -2.25 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 19 | $63.18 | $2.05 | — | $8,650.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $1231.56; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 140 | $8.74 | $2.41 | — | $7,423.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $1231.56; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 17 | $68.52 | $2.04 | — | $6,257.11 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $1231.56; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 340 | $3.62 | $4.39 | — | $5,023.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $1231.56; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 7 | $167.55 | $2.01 | — | $3,848.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $1231.56; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 27 | $44.90 | $2.07 | — | $2,634.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $1231.56; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 12 | $98.15 | $2.03 | — | $1,454.57 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $1231.56; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 78 | $15.70 | $2.22 | — | $227.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $1231.56; owner union_e_fresh_h1 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $227.74 | ▼ close $9,750.36 vs 09:30 $9,869.91 (session -82.89) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $227.74 | ▼ 09:30 equity $9,692.48 vs yday $9,750.36 (-57.88) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `AMBA` | 19 | $63.83 | $2.07 | $+8.24 | $1,438.45 | ▲ +8.24 after sell → book $9,690.42; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASAN` | 140 | $8.73 | $2.44 | $-6.25 | $2,658.20 | ▼ -6.25 after sell → book $9,687.97; vs 09:30 mark -2.45 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 17 | $67.05 | $2.06 | $-29.09 | $3,795.99 | ▼ -29.09 after sell → book $9,685.91; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOMO` | 340 | $3.84 | $4.45 | $+67.66 | $5,097.14 | ▲ +67.66 after sell → book $9,681.46; vs 09:30 mark -4.45 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 7 | $160.52 | $2.03 | $-53.25 | $6,218.75 | ▼ -53.25 after sell → book $9,679.43; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 27 | $39.56 | $2.09 | $-148.34 | $7,284.78 | ▼ -148.34 after sell → book $9,677.34; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 12 | $100.58 | $2.05 | $+25.09 | $8,489.69 | ▲ +25.09 after sell → book $9,675.29; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MAMA` | 78 | $15.20 | $2.25 | $-43.47 | $9,673.05 | ▼ -43.47 after sell → book $9,673.05; vs 09:30 mark -2.24 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,673.05 | ▲ close $9,673.05 vs 09:30 $9,692.48 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,673.05 | ▲ 09:30 equity $9,673.05 vs yday $9,673.05 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,673.05 | ▲ close $9,673.05 vs 09:30 $9,673.05 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,673.05 | ▲ 09:30 equity $9,673.05 vs yday $9,673.05 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,673.05 | ▲ close $9,673.05 vs 09:30 $9,673.05 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,673.05 | ▲ 09:30 equity $9,673.05 vs yday $9,673.05 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 3 | $164.43 | $2.00 | — | $9,177.76 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $604.57; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 102 | $5.91 | $2.30 | — | $8,572.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; combo leftover $604.57; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 2 | $242.17 | $2.00 | — | $8,086.30 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; combo leftover $604.57; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 18 | $32.01 | $2.04 | — | $7,508.08 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $604.57; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 8 | $71.71 | $2.01 | — | $6,932.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; combo leftover $604.57; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 10 | $56.02 | $2.02 | — | $6,370.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; combo leftover $604.57; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 64 | $9.37 | $2.18 | — | $5,768.30 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $604.57; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 46 | $13.10 | $2.13 | — | $5,163.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; combo leftover $604.57; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 38 | $135.71 | $2.10 | — | $4.49 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-9.2; combo leftover $5163.58; owner union_earn_react_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.49 | ▼ close $9,620.71 vs 09:30 $9,673.05 (session -33.55) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.49 | ▼ 09:30 equity $9,526.33 vs yday $9,620.71 (-94.38) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 3 | $141.42 | $2.02 | $-73.05 | $426.73 | ▼ -73.05 after sell → book $9,524.31; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DBI` | 102 | $5.86 | $2.32 | $-9.72 | $1,022.13 | ▼ -9.72 after sell → book $9,521.99; vs 09:30 mark -2.32 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 2 | $261.51 | $2.02 | $+34.67 | $1,543.13 | ▲ +34.67 after sell → book $9,519.97; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CPRT` | 18 | $30.63 | $2.06 | $-28.95 | $2,092.41 | ▼ -28.95 after sell → book $9,517.91; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DSGX` | 8 | $77.68 | $2.03 | $+43.71 | $2,711.82 | ▲ +43.71 after sell → book $9,515.88; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `KR` | 10 | $59.31 | $2.04 | $+28.84 | $3,302.88 | ▲ +28.84 after sell → book $9,513.84; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `LPTH` | 64 | $8.85 | $2.20 | $-37.66 | $3,867.07 | ▼ -37.66 after sell → book $9,511.63; vs 09:30 mark -2.21 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `REF` | 46 | $14.16 | $2.15 | $+44.48 | $4,516.29 | ▲ +44.48 after sell → book $9,509.49; vs 09:30 mark -2.14 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,516.29 | ▲ close $9,614.75 vs 09:30 $9,526.33 (session +105.26) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,516.29 | ▼ 09:30 equity $9,591.95 vs yday $9,614.75 (-22.80) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,516.29 | ▼ close $9,259.83 vs 09:30 $9,591.95 (session -332.12) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,516.29 | ▲ 09:30 equity $9,287.19 vs yday $9,259.83 (+27.36) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `RH` | 38 | $125.55 | $2.15 | $-390.34 | $9,285.03 | ▼ -390.34 after sell → book $9,285.03; vs 09:30 mark -2.16 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 140 | $33.14 | $2.41 | — | $4,643.02 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; combo leftover $4642.52; owner union_e_fresh_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 113 | $40.93 | $2.33 | — | $15.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $4642.52; owner union_e_fresh_h1 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.60 | ▲ close $9,461.79 vs 09:30 $9,287.19 (session +181.50) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.60 | ▲ 09:30 equity $9,771.27 vs yday $9,461.79 (+309.48) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `FPS` | 140 | $36.76 | $2.47 | $+501.92 | $5,159.53 | ▲ +501.92 after sell → book $9,768.80; vs 09:30 mark -2.47 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TCOM` | 113 | $40.79 | $2.38 | $-20.53 | $9,766.42 | ▼ -20.53 after sell → book $9,766.42; vs 09:30 mark -2.38 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 435 | $11.21 | $5.61 | — | $4,884.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.0; combo leftover $4883.21; owner union_e_fresh_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 60 | $81.00 | $2.17 | — | $22.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.0; combo leftover $4883.21; owner union_e_fresh_h1 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.28 | ▲ close $9,826.36 vs 09:30 $9,771.27 (session +67.72) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.28 | ▼ 09:30 equity $9,780.68 vs yday $9,826.36 (-45.68) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ALMU` | 435 | $11.64 | $5.72 | $+175.71 | $5,079.96 | ▲ +175.71 after sell → book $9,774.96; vs 09:30 mark -5.72 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LEN` | 60 | $78.25 | $2.22 | $-169.39 | $9,772.74 | ▼ -169.39 after sell → book $9,772.74; vs 09:30 mark -2.22 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,772.74 | ▲ close $9,772.74 vs 09:30 $9,780.68 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,772.74 | ▲ 09:30 equity $9,772.74 vs yday $9,772.74 (+0.00) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,772.74 | ▲ close $9,772.74 vs 09:30 $9,772.74 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,772.74 | ▲ 09:30 equity $9,772.74 vs yday $9,772.74 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,772.74 | ▲ close $9,772.74 vs 09:30 $9,772.74 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,772.74 | ▲ 09:30 equity $9,772.74 vs yday $9,772.74 (+0.00) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 41 | $47.57 | $2.11 | — | $7,820.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $1954.55; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 9 | $196.78 | $2.02 | — | $6,047.22 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $1954.55; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 54 | $35.74 | $2.15 | — | $4,115.11 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $1954.55; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 41 | $47.15 | $2.11 | — | $2,179.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $1954.55; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 17 | $109.67 | $2.04 | — | $313.42 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $1954.55; owner union_e_fresh_h1 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $313.42 | ▼ close $9,633.23 vs 09:30 $9,772.74 (session -129.08) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $313.42 | ▲ 09:30 equity $9,633.78 vs yday $9,633.23 (+0.55) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `CBRL` | 41 | $46.88 | $2.14 | $-32.54 | $2,233.36 | ▼ -32.54 after sell → book $9,631.64; vs 09:30 mark -2.14 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 9 | $192.26 | $2.04 | $-44.74 | $3,961.66 | ▼ -44.74 after sell → book $9,629.60; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `GIS` | 54 | $35.96 | $2.18 | $+7.55 | $5,901.32 | ▲ +7.55 after sell → book $9,627.42; vs 09:30 mark -2.18 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `KBH` | 41 | $47.14 | $2.14 | $-4.66 | $7,831.92 | ▼ -4.66 after sell → book $9,625.28; vs 09:30 mark -2.14 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PAYX` | 17 | $105.49 | $2.07 | $-75.13 | $9,623.22 | ▼ -75.13 after sell → book $9,623.22; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,623.22 | ▲ close $9,623.22 vs 09:30 $9,633.78 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,160.20 | ▲ 09:30 equity $9,160.20 vs yday $9,160.20 (+0.00) | 09:30 open · cash $9,160.20 (unchanged overnight, no fees) · equity $9,160.20 vs prior close $9,160.20 (+0.00) | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 10 | $887.00 | $2.02 | — | $288.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+0.3; combo leftover $9160.20; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $288.18 | ▲ close $9,515.83 vs 09:30 $9,160.20 (session +357.65) | 16:00 close · cash $288.18 · equity $9,515.83 vs 09:30 $9,160.20 (+355.63; session marks +357.65) · 1 name(s) marked open→close (per-name table). COST×10 09:30 $887.00 → close $922.76 +357.65 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `AIRJ` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `AMAT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `AMPG` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `BRUN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `BZAI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `DEFT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `DGXX` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `AIRJ` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `AMAT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `AMPG` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `BRUN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `BZAI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `DEFT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `DGXX` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `LOW` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `LZB` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `MRCY` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-21 | `BABA` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `BILL` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `BULL` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `DE` | cash | leftover split 327.19 < 1 share @ 623.26 |
| 2026-08-24 | `BABA` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BILL` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BULL` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ROST` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_e_fresh_h1 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_earn_react_h3 |
| 2026-08-25 | `ROST` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `FSCO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `HEI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `INTU` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `FSCO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `HEI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `INTU` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new long union_e_fresh_h1 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new long union_earn_react_h3 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new long union_e_fresh_h1 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new long union_earn_react_h3 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new long union_earn_react_h3 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `M` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `NAVN` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `NB` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `SHOE` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-14 | `RH` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-15 | `RH` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
