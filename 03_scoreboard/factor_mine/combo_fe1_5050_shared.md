# Factor mine action — `combo_fe1_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared flatten_h5/union_e_fresh_h1 w=0.5,0.5 net=priority

Cash book **-9.62%** ($9,038) · signal-only (no cash/fees) was —. Starts YES **10/30**. Fills 336 · skips 447 · realized $+55.90.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: flatten_h5 50%, union_e_fresh_h1 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: flatten_h5 50%, union_e_fresh_h1 50%.
- Member: flatten_h5 (50% · long · hold 5).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $810.82.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3086 | $0.81 | $34.25 | — | $7,466.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; combo leftover $2500.00; owner union_e_fresh_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 113 | $22.01 | $2.33 | — | $4,976.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; combo leftover $2500.00; owner union_e_fresh_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 11 | $59.80 | $2.02 | — | $4,316.80 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 15 | $45.98 | $2.04 | — | $3,625.07 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 14 | $50.62 | $2.03 | — | $2,914.31 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 14 | $49.70 | $2.03 | — | $2,216.48 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 60 | $11.70 | $2.17 | — | $1,512.31 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 23 | $29.74 | $2.06 | — | $826.23 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 30 | $23.33 | $2.08 | — | $124.25 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.25 | ▲ close $10,400.40 vs 09:30 $10,000.00 (session +451.42) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.25 | ▲ 09:30 equity $10,485.91 vs yday $10,400.40 (+85.51) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 3086 | $0.93 | $38.49 | $+297.57 | $2,955.74 | ▲ +297.57 after sell → book $10,447.42; vs 09:30 mark -38.49 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 113 | $23.33 | $2.37 | $+144.46 | $5,589.66 | ▲ +144.46 after sell → book $10,445.05; vs 09:30 mark -2.37 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 232 | $1.50 | $2.99 | — | $5,238.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $349.35; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 17 | $19.57 | $2.04 | — | $4,903.93 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $349.35; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 31 | $11.12 | $2.08 | — | $4,557.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $349.35; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 25 | $13.55 | $2.06 | — | $4,216.32 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $349.35; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 32 | $10.83 | $2.09 | — | $3,867.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-30.1; combo leftover $349.35; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 296 | $1.18 | $3.82 | — | $3,514.57 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $349.35; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 18 | $19.17 | $2.04 | — | $3,167.47 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $349.35; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 35 | $9.89 | $2.10 | — | $2,819.05 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; combo leftover $349.35; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 1 | $359.83 | $1.99 | — | $2,457.23 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+5.9; combo leftover $402.72; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 2 | $146.90 | $2.00 | — | $2,161.43 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+3.6; combo leftover $402.72; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 3 | $120.00 | $2.00 | — | $1,799.43 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+0.6; combo leftover $402.72; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 1 | $330.91 | $1.99 | — | $1,466.53 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-8.6; combo leftover $402.72; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 6 | $57.61 | $2.01 | — | $1,118.86 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.7; combo leftover $402.72; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 44 | $9.01 | $2.12 | — | $720.30 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; combo leftover $402.72; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 429 | $0.94 | $5.31 | — | $313.02 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; combo leftover $402.72; owner flatten_h5 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $313.02 | ▲ close $10,430.34 vs 09:30 $10,485.91 (session +21.93) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $313.02 | ▲ 09:30 equity $10,467.27 vs yday $10,430.34 (+36.93) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 232 | $1.52 | $3.04 | $-1.39 | $662.62 | ▼ -1.39 after sell → book $10,464.23; vs 09:30 mark -3.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 17 | $19.57 | $2.06 | $-4.10 | $993.25 | ▼ -4.10 after sell → book $10,462.17; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 31 | $9.57 | $2.10 | $-52.24 | $1,287.81 | ▼ -52.24 after sell → book $10,460.07; vs 09:30 mark -2.10 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 25 | $13.16 | $2.08 | $-13.90 | $1,614.73 | ▼ -13.90 after sell → book $10,457.98; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 32 | $11.19 | $2.11 | $+7.33 | $1,970.70 | ▲ +7.33 after sell → book $10,455.87; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `EU` | 296 | $1.21 | $3.88 | $+1.18 | $2,324.98 | ▲ +1.18 after sell → book $10,452.00; vs 09:30 mark -3.87 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 18 | $20.25 | $2.06 | $+15.33 | $2,687.42 | ▲ +15.33 after sell → book $10,449.93; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 35 | $10.97 | $2.12 | $+33.42 | $3,069.26 | ▲ +33.42 after sell → book $10,447.82; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 8 | $46.18 | $2.01 | — | $2,697.80 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+6.7; combo leftover $383.66; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 2 | $142.77 | $2.00 | — | $2,410.27 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+5.8; combo leftover $383.66; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $2,205.57 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+8.3; combo leftover $383.66; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 94 | $4.05 | $2.27 | — | $1,822.60 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; combo leftover $383.66; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 45 | $8.46 | $2.12 | — | $1,439.78 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; combo leftover $383.66; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 4 | $90.54 | $2.00 | — | $1,075.61 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-7.2; combo leftover $383.66; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 118 | $3.24 | $2.34 | — | $690.95 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; combo leftover $383.66; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 79 | $4.81 | $2.23 | — | $308.73 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; combo leftover $383.66; owner flatten_h5 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $308.73 | ▼ close $10,414.73 vs 09:30 $10,467.27 (session -16.12) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $308.73 | ▼ 09:30 equity $10,292.14 vs yday $10,414.73 (-122.59) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $308.73 | ▼ close $10,286.63 vs 09:30 $10,292.14 (session -5.51) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $308.73 | ▲ 09:30 equity $10,409.68 vs yday $10,286.63 (+123.05) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $308.73 | ▲ close $10,605.36 vs 09:30 $10,409.68 (session +195.68) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $308.73 | ▼ 09:30 equity $10,580.99 vs yday $10,605.36 (-24.37) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 11 | $58.64 | $2.04 | $-16.83 | $951.73 | ▼ -16.83 after sell → book $10,578.95; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 15 | $42.46 | $2.06 | $-56.89 | $1,586.57 | ▼ -56.89 after sell → book $10,576.89; vs 09:30 mark -2.06 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 14 | $53.06 | $2.05 | $+30.03 | $2,327.36 | ▲ +30.03 after sell → book $10,574.84; vs 09:30 mark -2.05 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 14 | $51.65 | $2.05 | $+23.22 | $3,048.41 | ▲ +23.22 after sell → book $10,572.79; vs 09:30 mark -2.05 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 60 | $13.84 | $2.19 | $+124.04 | $3,876.62 | ▲ +124.04 after sell → book $10,570.60; vs 09:30 mark -2.19 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 23 | $30.66 | $2.08 | $+17.02 | $4,579.72 | ▲ +17.02 after sell → book $10,568.52; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 30 | $23.11 | $2.10 | $-10.78 | $5,270.92 | ▼ -10.78 after sell → book $10,566.42; vs 09:30 mark -2.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 3 | $97.43 | $2.00 | — | $4,976.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $329.43; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 74 | $4.43 | $2.21 | — | $4,646.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $329.43; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 1098 | $0.30 | $6.59 | — | $4,310.61 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $329.43; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 7 | $46.85 | $2.01 | — | $3,980.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $329.43; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 36 | $9.01 | $2.10 | — | $3,654.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $329.43; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 84 | $3.89 | $2.24 | — | $3,325.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $329.43; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 9 | $34.05 | $2.02 | — | $3,016.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $329.43; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 14 | $22.44 | $2.03 | — | $2,700.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $329.43; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 16 | $20.55 | $2.04 | — | $2,369.69 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; combo leftover $337.57; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 3 | $91.01 | $2.00 | — | $2,094.67 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; combo leftover $337.57; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 16 | $20.65 | $2.04 | — | $1,762.23 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; combo leftover $337.57; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 58 | $5.77 | $2.16 | — | $1,425.40 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; combo leftover $337.57; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 17 | $19.63 | $2.04 | — | $1,089.65 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; combo leftover $337.57; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 11 | $29.63 | $2.02 | — | $761.70 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; combo leftover $337.57; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 192 | $1.75 | $2.57 | — | $423.13 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; combo leftover $337.57; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 2 | $144.54 | $2.00 | — | $132.06 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; combo leftover $337.57; owner flatten_h5 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.06 | ▲ close $10,633.78 vs 09:30 $10,580.99 (session +105.42) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.06 | ▲ 09:30 equity $10,793.38 vs yday $10,633.78 (+159.60) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `TLN` | 1 | $318.52 | $2.01 | $-45.32 | $448.56 | ▼ -45.32 after sell → book $10,791.37; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `VST` | 2 | $139.99 | $2.02 | $-17.83 | $726.53 | ▼ -17.83 after sell → book $10,789.35; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `NRG` | 3 | $116.58 | $2.02 | $-14.28 | $1,074.25 | ▼ -14.28 after sell → book $10,787.33; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `DAVE` | 1 | $339.30 | $2.01 | $+4.38 | $1,411.54 | ▲ +4.38 after sell → book $10,785.32; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `SLG` | 6 | $58.63 | $2.03 | $+2.08 | $1,761.29 | ▲ +2.08 after sell → book $10,783.29; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 44 | $11.70 | $2.14 | $+114.10 | $2,273.95 | ▲ +114.10 after sell → book $10,781.15; vs 09:30 mark -2.14 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 429 | $0.87 | $5.09 | $-40.42 | $2,640.80 | ▼ -40.42 after sell → book $10,776.06; vs 09:30 mark -5.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 3 | $96.75 | $2.02 | $-6.06 | $2,929.03 | ▼ -6.06 after sell → book $10,774.04; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TOYO` | 74 | $4.68 | $2.23 | $+14.05 | $3,273.12 | ▲ +14.05 after sell → book $10,771.81; vs 09:30 mark -2.23 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DVLT` | 1098 | $0.31 | $6.89 | $-2.50 | $3,606.61 | ▼ -2.50 after sell → book $10,764.92; vs 09:30 mark -6.89 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AEG` | 36 | $9.04 | $2.12 | $-3.14 | $3,929.93 | ▼ -3.14 after sell → book $10,762.80; vs 09:30 mark -2.12 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALVO` | 84 | $4.32 | $2.27 | $+31.61 | $4,290.54 | ▲ +31.61 after sell → book $10,760.53; vs 09:30 mark -2.27 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 9 | $34.31 | $2.04 | $-1.71 | $4,597.30 | ▼ -1.71 after sell → book $10,758.50; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 14 | $22.20 | $2.05 | $-7.44 | $4,906.05 | ▼ -7.44 after sell → book $10,756.45; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 3 | $115.18 | $2.00 | — | $4,558.51 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $350.43; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 3 | $103.69 | $2.00 | — | $4,245.44 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; ret5=-10.3; combo leftover $350.43; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 19 | $17.93 | $2.05 | — | $3,902.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $350.43; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 3 | $93.98 | $2.00 | — | $3,618.69 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; combo leftover $350.43; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 8 | $43.08 | $2.01 | — | $3,272.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $350.43; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 152 | $2.30 | $2.45 | — | $2,919.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $350.43; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 3 | $119.43 | $2.00 | — | $2,559.70 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+21.1; combo leftover $365.00; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 21 | $17.20 | $2.05 | — | $2,196.44 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; combo leftover $365.00; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 1 | $216.30 | $1.99 | — | $1,978.15 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; combo leftover $365.00; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 32 | $11.13 | $2.09 | — | $1,619.91 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; combo leftover $365.00; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 147 | $2.47 | $2.43 | — | $1,254.38 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; combo leftover $365.00; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 189 | $1.93 | $2.56 | — | $887.06 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; combo leftover $365.00; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 6 | $59.72 | $2.01 | — | $526.73 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; combo leftover $365.00; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 276 | $1.32 | $3.56 | — | $158.85 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; combo leftover $365.00; owner flatten_h5 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.85 | ▲ close $10,956.89 vs 09:30 $10,793.38 (session +231.64) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.85 | ▲ 09:30 equity $11,081.23 vs yday $10,956.89 (+124.34) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `DVN` | 8 | $48.89 | $2.03 | $+17.63 | $547.93 | ▲ +17.63 after sell → book $11,079.20; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `EOG` | 2 | $152.07 | $2.02 | $+14.59 | $850.06 | ▲ +14.59 after sell → book $11,077.18; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `FANG` | 1 | $210.00 | $2.01 | $+3.29 | $1,058.05 | ▲ +3.29 after sell → book $11,075.17; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 94 | $4.62 | $2.30 | $+49.48 | $1,490.50 | ▲ +49.48 after sell → book $11,072.87; vs 09:30 mark -2.30 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 45 | $9.26 | $2.15 | $+31.73 | $1,905.05 | ▲ +31.73 after sell → book $11,070.73; vs 09:30 mark -2.14 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `ELF` | 4 | $102.20 | $2.02 | $+42.62 | $2,311.83 | ▲ +42.62 after sell → book $11,068.71; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 118 | $3.50 | $2.37 | $+25.96 | $2,722.46 | ▲ +25.96 after sell → book $11,066.33; vs 09:30 mark -2.38 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 79 | $5.05 | $2.25 | $+14.48 | $3,119.16 | ▲ +14.48 after sell → book $11,064.08; vs 09:30 mark -2.25 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 7 | $43.05 | $2.03 | $-30.64 | $3,418.48 | ▼ -30.64 after sell → book $11,062.05; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 3 | $121.00 | $2.02 | $+13.44 | $3,779.46 | ▲ +13.44 after sell → book $11,060.03; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WMT` | 3 | $104.14 | $2.02 | $-2.67 | $4,089.86 | ▼ -2.67 after sell → book $11,058.01; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 19 | $18.05 | $2.07 | $-1.83 | $4,430.84 | ▼ -1.83 after sell → book $11,055.95; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 3 | $97.02 | $2.02 | $+5.10 | $4,719.88 | ▲ +5.10 after sell → book $11,053.93; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 8 | $44.22 | $2.03 | $+5.07 | $5,071.60 | ▲ +5.07 after sell → book $11,051.89; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 152 | $2.34 | $2.48 | $+1.15 | $5,424.80 | ▲ +1.15 after sell → book $11,049.41; vs 09:30 mark -2.48 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,424.80 | ▼ close $11,010.07 vs 09:30 $11,081.23 (session -39.34) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,424.80 | ▼ 09:30 equity $10,933.51 vs yday $11,010.07 (-76.56) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 1 | $175.01 | $1.75 | — | $5,248.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; combo leftover $339.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 3 | $88.94 | $2.00 | — | $4,979.22 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $339.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 22 | $15.28 | $2.06 | — | $4,641.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $339.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 2 | $142.36 | $2.00 | — | $4,354.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $339.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 66 | $5.10 | $2.19 | — | $4,015.50 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $339.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 7 | $47.89 | $2.01 | — | $3,678.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $339.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 24 | $13.92 | $2.06 | — | $3,342.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $339.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 74 | $4.54 | $2.21 | — | $3,003.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $339.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 21 | $23.77 | $2.05 | — | $2,502.35 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; combo leftover $500.60; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 45 | $10.98 | $2.12 | — | $2,006.13 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; combo leftover $500.60; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 8 | $61.19 | $2.01 | — | $1,514.59 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.4; combo leftover $500.60; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 59 | $8.35 | $2.17 | — | $1,019.78 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; combo leftover $500.60; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 101 | $4.94 | $2.29 | — | $518.54 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; combo leftover $500.60; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 1 | $426.97 | $1.99 | — | $89.58 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.0; combo leftover $500.60; owner flatten_h5 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $89.58 | ▲ close $11,085.62 vs 09:30 $10,933.51 (session +181.03) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $89.58 | ▼ 09:30 equity $10,993.02 vs yday $11,085.62 (-92.60) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `BMO` | 1 | $173.22 | $1.76 | $-5.30 | $261.04 | ▼ -5.30 after sell → book $10,991.26; vs 09:30 mark -1.76 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BNS` | 3 | $92.65 | $2.02 | $+7.11 | $536.98 | ▲ +7.11 after sell → book $10,989.25; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EH` | 66 | $4.77 | $2.21 | $-26.18 | $849.59 | ▼ -26.18 after sell → book $10,987.04; vs 09:30 mark -2.21 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GFI` | 7 | $48.24 | $2.03 | $-1.59 | $1,185.24 | ▼ -1.59 after sell → book $10,985.01; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 24 | $14.03 | $2.08 | $-1.50 | $1,519.87 | ▼ -1.50 after sell → book $10,982.92; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SHMD` | 74 | $3.38 | $2.23 | $-90.66 | $1,767.76 | ▼ -90.66 after sell → book $10,980.69; vs 09:30 mark -2.23 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 505 | $0.58 | $4.46 | — | $1,468.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $294.63; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 56 | $5.21 | $2.16 | — | $1,174.97 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $294.63; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 2 | $131.37 | $2.00 | — | $910.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; combo leftover $294.63; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 16 | $18.26 | $2.04 | — | $616.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $294.63; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 8 | $34.30 | $2.01 | — | $339.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; combo leftover $294.63; owner union_e_fresh_h1 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $339.62 | ▲ close $11,027.30 vs 09:30 $10,993.02 (session +59.28) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $339.62 | ▼ 09:30 equity $11,024.16 vs yday $11,027.30 (-3.14) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 16 | $20.93 | $2.06 | $+1.98 | $672.44 | ▲ +1.98 after sell → book $11,022.10; vs 09:30 mark -2.06 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 3 | $95.52 | $2.02 | $+9.51 | $956.98 | ▲ +9.51 after sell → book $11,020.08; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 16 | $21.31 | $2.06 | $+6.46 | $1,295.88 | ▲ +6.46 after sell → book $11,018.02; vs 09:30 mark -2.06 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 58 | $5.49 | $2.18 | $-20.59 | $1,612.12 | ▼ -20.59 after sell → book $11,015.84; vs 09:30 mark -2.18 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 17 | $21.47 | $2.06 | $+27.18 | $1,975.05 | ▲ +27.18 after sell → book $11,013.78; vs 09:30 mark -2.06 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 11 | $32.32 | $2.04 | $+25.52 | $2,328.53 | ▲ +25.52 after sell → book $11,011.74; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 192 | $1.91 | $2.61 | $+25.55 | $2,692.64 | ▲ +25.55 after sell → book $11,009.13; vs 09:30 mark -2.61 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 2 | $155.89 | $2.02 | $+18.69 | $3,002.40 | ▲ +18.69 after sell → book $11,007.11; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 22 | $18.50 | $2.08 | $+66.71 | $3,407.33 | ▲ +66.71 after sell → book $11,005.04; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 2 | $128.73 | $2.02 | $-31.27 | $3,662.77 | ▼ -31.27 after sell → book $11,003.02; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 505 | $0.53 | $4.29 | $-35.51 | $3,926.14 | ▼ -35.51 after sell → book $10,998.74; vs 09:30 mark -4.28 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 56 | $5.49 | $2.18 | $+11.34 | $4,231.40 | ▲ +11.34 after sell → book $10,996.56; vs 09:30 mark -2.18 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ANF` | 2 | $144.70 | $2.02 | $+22.65 | $4,518.78 | ▲ +22.65 after sell → book $10,994.54; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BBWI` | 16 | $18.69 | $2.06 | $+2.78 | $4,815.76 | ▲ +2.78 after sell → book $10,992.48; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BOX` | 8 | $33.79 | $2.03 | $-8.13 | $5,084.05 | ▼ -8.13 after sell → book $10,990.45; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 3 | $80.60 | $2.00 | — | $4,840.25 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.0; combo leftover $317.75; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 19 | $16.18 | $2.05 | — | $4,530.78 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.7; combo leftover $317.75; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 2 | $118.77 | $2.00 | — | $4,291.25 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.3; combo leftover $317.75; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 17 | $17.78 | $2.04 | — | $3,986.95 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.2; combo leftover $317.75; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 23 | $13.41 | $2.06 | — | $3,676.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; combo leftover $317.75; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 3 | $97.16 | $2.00 | — | $3,382.98 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; combo leftover $317.75; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 1 | $206.82 | $1.99 | — | $3,174.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.2; combo leftover $317.75; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 2 | $120.17 | $2.00 | — | $2,931.83 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $317.75; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 23 | $41.44 | $2.06 | — | $1,976.65 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; combo leftover $977.28; owner flatten_h5 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 67 | $14.42 | $2.19 | — | $1,008.32 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; combo leftover $977.28; owner flatten_h5 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 375 | $2.60 | $4.84 | — | $28.48 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; combo leftover $977.28; owner flatten_h5 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.48 | ▲ close $11,047.03 vs 09:30 $11,024.16 (session +81.81) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.48 | ▼ 09:30 equity $11,031.05 vs yday $11,047.03 (-15.98) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 3 | $119.19 | $2.02 | $-4.74 | $384.03 | ▼ -4.74 after sell → book $11,029.03; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 21 | $16.44 | $2.07 | $-20.09 | $727.20 | ▼ -20.09 after sell → book $11,026.96; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEM` | 1 | $216.31 | $2.01 | $-4.00 | $941.50 | ▼ -4.00 after sell → book $11,024.95; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 32 | $15.43 | $2.11 | $+133.41 | $1,433.15 | ▲ +133.41 after sell → book $11,022.84; vs 09:30 mark -2.11 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 147 | $2.35 | $2.47 | $-22.54 | $1,776.14 | ▼ -22.54 after sell → book $11,020.38; vs 09:30 mark -2.46 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 189 | $2.06 | $2.60 | $+19.41 | $2,162.88 | ▲ +19.41 after sell → book $11,017.78; vs 09:30 mark -2.60 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRSP` | 6 | $58.22 | $2.03 | $-13.04 | $2,510.17 | ▼ -13.04 after sell → book $11,015.75; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 276 | $1.82 | $3.62 | $+130.82 | $3,008.87 | ▲ +130.82 after sell → book $11,012.13; vs 09:30 mark -3.62 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `BBY` | 3 | $83.85 | $2.02 | $+5.73 | $3,258.40 | ▲ +5.73 after sell → book $11,010.11; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BILI` | 19 | $16.94 | $2.07 | $+10.33 | $3,578.20 | ▲ +10.33 after sell → book $11,008.05; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 2 | $115.66 | $2.02 | $-10.23 | $3,807.50 | ▼ -10.23 after sell → book $11,006.03; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CMBT` | 17 | $18.58 | $2.06 | $+9.50 | $4,121.30 | ▲ +9.50 after sell → book $11,003.97; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CSIQ` | 23 | $13.65 | $2.08 | $+1.38 | $4,433.17 | ▲ +1.38 after sell → book $11,001.89; vs 09:30 mark -2.08 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `HQY` | 3 | $93.62 | $2.02 | $-14.64 | $4,712.01 | ▼ -14.64 after sell → book $10,999.87; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RY` | 1 | $205.50 | $2.01 | $-5.33 | $4,915.50 | ▼ -5.33 after sell → book $10,997.86; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TD` | 2 | $122.07 | $2.02 | $-0.21 | $5,157.62 | ▼ -0.21 after sell → book $10,995.84; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 2 | $261.16 | $2.00 | — | $4,633.31 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; combo leftover $644.70; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 42 | $15.01 | $2.12 | — | $4,000.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $644.70; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 6 | $103.89 | $2.01 | — | $3,375.42 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $644.70; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 166 | $3.88 | $2.49 | — | $2,728.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $644.70; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 14 | $44.40 | $2.03 | — | $2,105.22 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $644.70; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 26 | $24.69 | $2.07 | — | $1,461.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $644.70; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 77 | $8.35 | $2.22 | — | $816.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $644.70; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 17 | $37.65 | $2.04 | — | $174.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $644.70; owner union_e_fresh_h1 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.04 | ▼ close $10,645.66 vs 09:30 $11,031.05 (session -333.22) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.04 | ▲ 09:30 equity $10,680.41 vs yday $10,645.66 (+34.75) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 2 | $257.71 | $2.02 | $-10.91 | $687.44 | ▼ -10.91 after sell → book $10,678.39; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBAR` | 42 | $14.88 | $2.14 | $-9.71 | $1,310.27 | ▼ -9.71 after sell → book $10,676.26; vs 09:30 mark -2.13 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 6 | $98.00 | $2.03 | $-39.38 | $1,896.24 | ▼ -39.38 after sell → book $10,674.23; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 166 | $3.39 | $2.53 | $-86.35 | $2,456.45 | ▼ -86.35 after sell → book $10,671.70; vs 09:30 mark -2.53 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 14 | $44.85 | $2.05 | $+2.22 | $3,082.30 | ▲ +2.22 after sell → book $10,669.65; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 26 | $22.98 | $2.09 | $-48.62 | $3,677.69 | ▼ -48.62 after sell → book $10,667.56; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 77 | $8.53 | $2.24 | $+9.40 | $4,332.26 | ▲ +9.40 after sell → book $10,665.32; vs 09:30 mark -2.24 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `IREN` | 17 | $35.81 | $2.06 | $-35.30 | $4,938.97 | ▼ -35.30 after sell → book $10,663.26; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,938.97 | ▲ close $10,715.36 vs 09:30 $10,680.41 (session +52.10) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,938.97 | ▲ 09:30 equity $10,814.24 vs yday $10,715.36 (+98.88) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 21 | $23.94 | $2.07 | $-0.56 | $5,439.63 | ▼ -0.56 after sell → book $10,812.16; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 45 | $10.42 | $2.15 | $-29.47 | $5,906.39 | ▼ -29.47 after sell → book $10,810.02; vs 09:30 mark -2.14 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `INSP` | 8 | $63.00 | $2.03 | $+10.43 | $6,408.36 | ▲ +10.43 after sell → book $10,807.99; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 59 | $8.25 | $2.19 | $-10.25 | $6,892.92 | ▼ -10.25 after sell → book $10,805.80; vs 09:30 mark -2.19 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 101 | $4.64 | $2.32 | $-34.91 | $7,359.24 | ▼ -34.91 after sell → book $10,803.48; vs 09:30 mark -2.32 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `HCA` | 1 | $418.43 | $2.01 | $-12.55 | $7,775.66 | ▼ -12.55 after sell → book $10,801.47; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,775.66 | ▼ close $10,757.95 vs 09:30 $10,814.24 (session -43.52) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,775.66 | ▼ 09:30 equity $10,729.61 vs yday $10,757.95 (-28.34) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,775.66 | ▼ close $10,697.63 vs 09:30 $10,729.61 (session -31.98) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,775.66 | ▲ 09:30 equity $10,720.45 vs yday $10,697.63 (+22.82) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 23 | $42.43 | $2.08 | $+18.63 | $8,749.47 | ▲ +18.63 after sell → book $10,718.37; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 67 | $15.45 | $2.21 | $+64.61 | $9,782.40 | ▲ +64.61 after sell → book $10,716.15; vs 09:30 mark -2.22 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 375 | $2.49 | $4.91 | $-51.00 | $10,711.24 | ▼ -51.00 after sell → book $10,711.24; vs 09:30 mark -4.91 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 62 | $10.74 | $2.18 | — | $10,042.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $669.45; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $9,689.15 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $669.45; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 97 | $6.90 | $2.28 | — | $9,017.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $669.45; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $8,661.08 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $669.45; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 29 | $22.32 | $2.08 | — | $8,011.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $669.45; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 2 | $257.00 | $2.00 | — | $7,495.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $669.45; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 14 | $47.60 | $2.03 | — | $6,827.30 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $669.45; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 44 | $15.09 | $2.12 | — | $6,161.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $669.45; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $4,942.92 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; combo leftover $1232.24; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 28 | $42.93 | $2.07 | — | $3,738.80 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; combo leftover $1232.24; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 339 | $3.63 | $4.37 | — | $2,503.86 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; combo leftover $1232.24; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 153 | $8.03 | $2.45 | — | $1,272.82 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; combo leftover $1232.24; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $78.75 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; combo leftover $1232.24; owner flatten_h5 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.75 | ▲ close $10,822.85 vs 09:30 $10,720.45 (session +141.25) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.75 | ▼ 09:30 equity $10,796.35 vs yday $10,822.85 (-26.50) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 62 | $10.91 | $2.20 | $+5.86 | $752.98 | ▲ +5.86 after sell → book $10,794.16; vs 09:30 mark -2.19 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 1 | $359.70 | $2.01 | $+3.95 | $1,110.66 | ▲ +3.95 after sell → book $10,792.14; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 97 | $9.28 | $2.31 | $+226.27 | $2,008.52 | ▲ +226.27 after sell → book $10,789.84; vs 09:30 mark -2.30 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 1 | $321.67 | $2.01 | $-36.83 | $2,328.17 | ▼ -36.83 after sell → book $10,787.82; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CPB` | 29 | $22.10 | $2.10 | $-10.55 | $2,966.98 | ▼ -10.55 after sell → book $10,785.73; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 2 | $238.88 | $2.02 | $-40.25 | $3,442.72 | ▼ -40.25 after sell → book $10,783.71; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 14 | $53.85 | $2.05 | $+83.42 | $4,194.57 | ▲ +83.42 after sell → book $10,781.66; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 44 | $15.34 | $2.14 | $+6.74 | $4,867.39 | ▲ +6.74 after sell → book $10,779.52; vs 09:30 mark -2.14 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 4 | $63.18 | $2.00 | — | $4,612.66 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $304.21; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 34 | $8.74 | $2.09 | — | $4,313.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $304.21; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 4 | $68.52 | $2.00 | — | $4,037.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $304.21; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 84 | $3.62 | $2.24 | — | $3,731.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $304.21; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 1 | $167.55 | $1.68 | — | $3,562.20 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $304.21; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 6 | $44.90 | $2.01 | — | $3,290.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $304.21; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 3 | $98.15 | $2.00 | — | $2,994.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $304.21; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 19 | $15.70 | $2.05 | — | $2,694.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $304.21; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 178 | $2.52 | $2.52 | — | $2,242.91 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; combo leftover $449.00; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 66 | $6.71 | $2.19 | — | $1,797.86 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; combo leftover $449.00; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 236 | $1.90 | $3.04 | — | $1,346.42 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; combo leftover $449.00; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 93 | $4.78 | $2.27 | — | $899.61 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; combo leftover $449.00; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 282 | $1.59 | $3.64 | — | $447.59 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; combo leftover $449.00; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 39 | $11.31 | $2.11 | — | $4.40 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; combo leftover $449.00; owner flatten_h5 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.40 | ▲ close $10,778.52 vs 09:30 $10,796.35 (session +30.84) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.40 | ▼ 09:30 equity $10,766.72 vs yday $10,778.52 (-11.80) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `AMBA` | 4 | $63.83 | $2.02 | $-1.42 | $257.69 | ▼ -1.42 after sell → book $10,764.69; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASAN` | 34 | $8.73 | $2.11 | $-4.54 | $552.40 | ▼ -4.54 after sell → book $10,762.58; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 4 | $67.05 | $2.02 | $-9.90 | $818.58 | ▼ -9.90 after sell → book $10,760.56; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOMO` | 84 | $3.84 | $2.27 | $+14.39 | $1,138.87 | ▲ +14.39 after sell → book $10,758.29; vs 09:30 mark -2.27 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 1 | $160.52 | $1.63 | $-10.34 | $1,297.77 | ▼ -10.34 after sell → book $10,756.67; vs 09:30 mark -1.62 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 6 | $39.56 | $2.03 | $-36.08 | $1,533.10 | ▼ -36.08 after sell → book $10,754.64; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 3 | $100.58 | $2.02 | $+3.27 | $1,832.82 | ▲ +3.27 after sell → book $10,752.62; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MAMA` | 19 | $15.20 | $2.07 | $-13.61 | $2,119.55 | ▼ -13.61 after sell → book $10,750.55; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,119.55 | ▼ close $10,620.03 vs 09:30 $10,766.72 (session -130.52) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,119.55 | ▼ 09:30 equity $10,574.12 vs yday $10,620.03 (-45.91) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,119.55 | ▼ close $10,295.87 vs 09:30 $10,574.12 (session -278.26) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,119.55 | ▼ 09:30 equity $10,198.01 vs yday $10,295.87 (-97.86) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,119.55 | ▼ close $10,054.50 vs 09:30 $10,198.01 (session -143.51) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,119.55 | ▲ 09:30 equity $10,139.25 vs yday $10,054.50 (+84.75) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 23 | $53.53 | $2.08 | $+10.81 | $3,348.66 | ▲ +10.81 after sell → book $10,137.17; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 28 | $41.30 | $2.09 | $-49.81 | $4,502.97 | ▼ -49.81 after sell → book $10,135.08; vs 09:30 mark -2.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 339 | $2.77 | $4.44 | $-300.35 | $5,437.56 | ▼ -300.35 after sell → book $10,130.64; vs 09:30 mark -4.44 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 153 | $7.70 | $2.48 | $-55.42 | $6,613.17 | ▼ -55.42 after sell → book $10,128.15; vs 09:30 mark -2.49 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 9 | $122.40 | $2.04 | $-94.50 | $7,712.74 | ▼ -94.50 after sell → book $10,126.12; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 2 | $164.43 | $2.00 | — | $7,381.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $482.05; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 81 | $5.91 | $2.23 | — | $6,900.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; combo leftover $482.05; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 1 | $242.17 | $1.99 | — | $6,656.78 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; combo leftover $482.05; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 15 | $32.01 | $2.04 | — | $6,174.59 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $482.05; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 6 | $71.71 | $2.01 | — | $5,742.32 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; combo leftover $482.05; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 8 | $56.02 | $2.01 | — | $5,292.15 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; combo leftover $482.05; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 51 | $9.37 | $2.14 | — | $4,812.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $482.05; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 36 | $13.10 | $2.10 | — | $4,338.44 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; combo leftover $482.05; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 53 | $16.28 | $2.15 | — | $3,473.45 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; combo leftover $867.69; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 317 | $2.73 | $4.09 | — | $2,603.95 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; combo leftover $867.69; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 4 | $206.84 | $2.00 | — | $1,774.59 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; combo leftover $867.69; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 5 | $157.78 | $2.00 | — | $983.68 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; combo leftover $867.69; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 15 | $56.09 | $2.04 | — | $140.30 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; combo leftover $867.69; owner flatten_h5 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $140.30 | ▲ close $10,168.94 vs 09:30 $10,139.25 (session +71.62) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $140.30 | ▼ 09:30 equity $10,032.56 vs yday $10,168.94 (-136.38) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 178 | $2.15 | $2.56 | $-70.95 | $520.43 | ▼ -70.95 after sell → book $10,030.00; vs 09:30 mark -2.56 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 66 | $5.93 | $2.21 | $-55.88 | $909.60 | ▼ -55.88 after sell → book $10,027.79; vs 09:30 mark -2.21 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 236 | $1.72 | $3.09 | $-49.80 | $1,311.25 | ▼ -49.80 after sell → book $10,024.70; vs 09:30 mark -3.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 93 | $4.13 | $2.29 | $-65.01 | $1,693.05 | ▼ -65.01 after sell → book $10,022.40; vs 09:30 mark -2.30 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 282 | $1.59 | $3.69 | $-7.33 | $2,137.73 | ▼ -7.33 after sell → book $10,018.71; vs 09:30 mark -3.69 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 39 | $10.73 | $2.13 | $-26.85 | $2,554.08 | ▼ -26.85 after sell → book $10,016.58; vs 09:30 mark -2.13 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 2 | $141.42 | $2.02 | $-50.03 | $2,834.90 | ▼ -50.03 after sell → book $10,014.56; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DBI` | 81 | $5.86 | $2.26 | $-8.54 | $3,307.30 | ▼ -8.54 after sell → book $10,012.31; vs 09:30 mark -2.25 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 1 | $261.51 | $2.01 | $+15.33 | $3,566.80 | ▲ +15.33 after sell → book $10,010.29; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CPRT` | 15 | $30.63 | $2.06 | $-24.79 | $4,024.19 | ▼ -24.79 after sell → book $10,008.24; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DSGX` | 6 | $77.68 | $2.03 | $+31.78 | $4,488.25 | ▲ +31.78 after sell → book $10,006.21; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `KR` | 8 | $59.31 | $2.03 | $+22.27 | $4,960.69 | ▲ +22.27 after sell → book $10,004.18; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `LPTH` | 51 | $8.85 | $2.16 | $-30.83 | $5,409.88 | ▼ -30.83 after sell → book $10,002.01; vs 09:30 mark -2.17 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `REF` | 36 | $14.16 | $2.12 | $+33.94 | $5,917.52 | ▲ +33.94 after sell → book $9,999.90; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,917.52 | ▼ close $9,922.74 vs 09:30 $10,032.56 (session -77.15) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,917.52 | ▲ 09:30 equity $9,964.41 vs yday $9,922.74 (+41.67) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,917.52 | ▼ close $9,881.47 vs 09:30 $9,964.41 (session -82.94) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,917.52 | ▲ 09:30 equity $9,923.90 vs yday $9,881.47 (+42.43) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 44 | $33.14 | $2.12 | — | $4,457.24 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; combo leftover $1479.38; owner union_e_fresh_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 36 | $40.93 | $2.10 | — | $2,981.66 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $1479.38; owner union_e_fresh_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 2 | $270.89 | $2.00 | — | $2,437.89 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.0; combo leftover $745.42; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 9 | $77.12 | $2.02 | — | $1,741.79 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+7.2; combo leftover $745.42; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 52 | $14.31 | $2.15 | — | $995.52 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; combo leftover $745.42; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 20 | $36.46 | $2.05 | — | $264.27 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.9; combo leftover $745.42; owner flatten_h5 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $264.27 | ▲ close $9,971.68 vs 09:30 $9,923.90 (session +60.21) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $264.27 | ▲ 09:30 equity $10,195.15 vs yday $9,971.68 (+223.47) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `FPS` | 44 | $36.76 | $2.14 | $+155.01 | $1,879.57 | ▲ +155.01 after sell → book $10,193.01; vs 09:30 mark -2.14 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TCOM` | 36 | $40.79 | $2.12 | $-9.26 | $3,345.89 | ▼ -9.26 after sell → book $10,190.89; vs 09:30 mark -2.12 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 74 | $11.21 | $2.21 | — | $2,514.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.0; combo leftover $836.47; owner union_e_fresh_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 10 | $81.00 | $2.02 | — | $1,702.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.0; combo leftover $836.47; owner union_e_fresh_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 1 | $233.85 | $1.99 | — | $1,466.27 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+11.7; combo leftover $283.69; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 1 | $151.43 | $1.52 | — | $1,313.33 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.0; combo leftover $283.69; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 1 | $147.61 | $1.48 | — | $1,164.24 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+17.7; combo leftover $283.69; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 27 | $10.25 | $2.07 | — | $885.42 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; combo leftover $283.69; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 37 | $7.59 | $2.10 | — | $602.48 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.4; combo leftover $283.69; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 8 | $34.93 | $2.01 | — | $321.03 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.6; combo leftover $283.69; owner flatten_h5 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $321.03 | ▲ close $10,180.97 vs 09:30 $10,195.15 (session +5.49) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $321.03 | ▲ 09:30 equity $10,196.90 vs yday $10,180.97 (+15.93) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 53 | $16.93 | $2.17 | $+30.13 | $1,216.15 | ▲ +30.13 after sell → book $10,194.73; vs 09:30 mark -2.17 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 317 | $2.68 | $4.15 | $-24.09 | $2,061.56 | ▼ -24.09 after sell → book $10,190.58; vs 09:30 mark -4.15 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 4 | $197.76 | $2.02 | $-40.34 | $2,850.58 | ▼ -40.34 after sell → book $10,188.56; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 5 | $152.71 | $2.02 | $-29.38 | $3,612.10 | ▼ -29.38 after sell → book $10,186.53; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 15 | $55.80 | $2.06 | $-8.44 | $4,447.05 | ▼ -8.44 after sell → book $10,184.48; vs 09:30 mark -2.05 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `ALMU` | 74 | $11.64 | $2.23 | $+27.37 | $5,306.17 | ▲ +27.37 after sell → book $10,182.24; vs 09:30 mark -2.24 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LEN` | 10 | $78.25 | $2.04 | $-31.56 | $6,086.63 | ▼ -31.56 after sell → book $10,180.20; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 9 | $108.55 | $2.02 | — | $5,107.67 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; combo leftover $1014.44; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 1 | $593.15 | $1.99 | — | $4,512.52 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; combo leftover $1014.44; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 4 | $209.52 | $2.00 | — | $3,672.44 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; combo leftover $1014.44; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 4 | $219.62 | $2.00 | — | $2,791.96 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; combo leftover $1014.44; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 11 | $85.00 | $2.02 | — | $1,854.94 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; combo leftover $1014.44; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 29 | $34.44 | $2.08 | — | $854.10 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; combo leftover $1014.44; owner flatten_h5 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $854.10 | ▼ close $10,029.50 vs 09:30 $10,196.90 (session -138.59) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $854.10 | ▲ 09:30 equity $10,103.77 vs yday $10,029.50 (+74.27) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 1 | $157.87 | $1.58 | — | $694.65 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; combo leftover $170.82; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 1 | $88.83 | $0.89 | — | $604.93 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; combo leftover $170.82; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 12 | $13.47 | $1.65 | — | $441.63 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; combo leftover $170.82; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 42 | $4.00 | $1.81 | — | $271.83 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; combo leftover $170.82; owner flatten_h5 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $271.83 | ▲ close $10,130.77 vs 09:30 $10,103.77 (session +32.93) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $271.83 | ▼ 09:30 equity $10,129.61 vs yday $10,130.77 (-1.16) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $271.83 | ▲ close $10,130.39 vs 09:30 $10,129.61 (session +0.78) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $271.83 | ▲ 09:30 equity $10,334.83 vs yday $10,130.39 (+204.44) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `IQV` | 2 | $270.66 | $2.02 | $-4.47 | $811.13 | ▼ -4.47 after sell → book $10,332.82; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 9 | $73.61 | $2.04 | $-35.64 | $1,471.59 | ▼ -35.64 after sell → book $10,330.78; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 52 | $13.12 | $2.17 | $-66.19 | $2,151.66 | ▼ -66.19 after sell → book $10,328.61; vs 09:30 mark -2.17 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 20 | $38.04 | $2.07 | $+27.48 | $2,910.39 | ▲ +27.48 after sell → book $10,326.54; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 6 | $47.57 | $2.01 | — | $2,622.96 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $291.04; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 1 | $196.78 | $1.97 | — | $2,424.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $291.04; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 8 | $35.74 | $2.01 | — | $2,136.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $291.04; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 6 | $47.15 | $2.01 | — | $1,851.37 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $291.04; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 2 | $109.67 | $2.00 | — | $1,630.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $291.04; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 2 | $116.85 | $2.00 | — | $1,394.34 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; combo leftover $326.01; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 11 | $27.79 | $2.02 | — | $1,086.62 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; combo leftover $326.01; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 33 | $9.81 | $2.09 | — | $760.80 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; combo leftover $326.01; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 16 | $20.25 | $2.04 | — | $434.77 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; combo leftover $326.01; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 15 | $20.65 | $2.04 | — | $122.98 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; combo leftover $326.01; owner flatten_h5 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.98 | ▼ close $10,231.77 vs 09:30 $10,334.83 (session -74.60) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.98 | ▼ 09:30 equity $10,116.15 vs yday $10,231.77 (-115.62) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `ILMN` | 1 | $253.79 | $2.01 | $+15.93 | $374.76 | ▲ +15.93 after sell → book $10,114.13; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `TWST` | 1 | $157.72 | $1.60 | $+3.17 | $530.88 | ▲ +3.17 after sell → book $10,112.53; vs 09:30 mark -1.60 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `RVTY` | 1 | $141.79 | $1.44 | $-8.74 | $671.23 | ▼ -8.74 after sell → book $10,111.09; vs 09:30 mark -1.44 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 27 | $10.39 | $2.09 | $-0.38 | $949.67 | ▼ -0.38 after sell → book $10,109.00; vs 09:30 mark -2.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 37 | $7.38 | $2.12 | $-11.99 | $1,220.61 | ▼ -11.99 after sell → book $10,106.88; vs 09:30 mark -2.12 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMN` | 8 | $33.82 | $2.03 | $-12.93 | $1,489.13 | ▼ -12.93 after sell → book $10,104.85; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `CBRL` | 6 | $46.88 | $2.03 | $-8.18 | $1,768.38 | ▼ -8.18 after sell → book $10,102.82; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 1 | $192.26 | $1.95 | $-8.44 | $1,958.70 | ▼ -8.44 after sell → book $10,100.87; vs 09:30 mark -1.95 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `GIS` | 8 | $35.96 | $2.03 | $-2.29 | $2,244.34 | ▼ -2.29 after sell → book $10,098.84; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `KBH` | 6 | $47.14 | $2.03 | $-4.10 | $2,525.16 | ▼ -4.10 after sell → book $10,096.81; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PAYX` | 2 | $105.49 | $2.02 | $-12.37 | $2,734.12 | ▼ -12.37 after sell → book $10,094.79; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,734.12 | ▲ close $10,134.45 vs 09:30 $10,116.15 (session +39.67) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,262.31 | ▲ 09:30 equity $9,020.16 vs yday $9,016.40 (+3.76) | 09:30 open · cash $3,262.31 (unchanged overnight, no fees) · equity $9,020.16 vs prior close $9,016.40 (+3.76) | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 1 | $887.00 | $1.99 | — | $2,373.32 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+0.3; combo leftover $1631.15; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 20 | $38.51 | $2.05 | — | $1,601.07 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.7; combo leftover $791.11; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 103 | $7.65 | $2.30 | — | $810.82 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; combo leftover $791.11; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $810.82 | ▲ close $9,037.74 vs 09:30 $9,020.16 (session +23.93) | 16:00 close · cash $810.82 · equity $9,037.74 vs 09:30 $9,020.16 (+17.58; session marks +23.93) · 20 name(s) marked open→close (per-name table). A×1 09:30 $171.98 → close $172.79 +0.81; ADMA×26 09:30 $9.52 → close $9.52 +0.00; ARQT×9 09:30 $26.27 → close $26.27 +0.00; CYPH×19 09:30 $4.00 → close $4.12 +2.19; DELL×1 09:30 $536.02 → close $536.02 +0.00; DLO×2 09:30 $13.88 → close $13.88 +0.00; DXCM×2 09:30 $87.47 → close $87.47 +0.00; ECO×8 09:30 $78.22 → close $78.22 +0.00; FIVN×20 09:30 $36.66 → close $36.66 -0.00; FTRE×13 09:30 $20.02 → close $20.02 +0.00; GNRC×3 09:30 $198.05 → close $198.05 +0.00; HALO×2 09:30 $115.36 → close $113.90 -2.92; MGTX×5 09:30 $11.05 → close $11.05 +0.00; OMER×12 09:30 $20.61 → close $20.08 -6.36; RBRK×6 09:30 $113.80 → close $113.80 +0.00; TDC×1 09:30 $29.46 → close $29.46 -0.00; VICR×3 09:30 $276.06 → close $276.06 -0.00; COST×1 09:30 $887.00 → close $922.76 +35.76; BLFS×20 09:30 $38.51 → close $38.49 -0.40; MRVI×103 09:30 $7.65 → close $7.60 -5.15 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `IREN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TPG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `SLS` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `IREN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `TPG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `SLS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `TLN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `VST` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `NRG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `DAVE` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `SLG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `MARA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `LDI` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `IREN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TPG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `SLS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TLN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `VST` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `NRG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `DAVE` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `SLG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `MARA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `LDI` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `DVN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `EOG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `FANG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `TMC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `TGB` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `ELF` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `DNN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `HNST` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `IREN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `TPG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `SLS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `TLN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `VST` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `NRG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `DAVE` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `SLG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `MARA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `LDI` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `DVN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `EOG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `FANG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `TMC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `TGB` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `ELF` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `DNN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `HNST` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-20 | `TLN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `VST` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `NRG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `DAVE` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `SLG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `MARA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `LDI` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `DVN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `EOG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `FANG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `TMC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `TGB` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `ELF` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `DNN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `HNST` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-21 | `DVN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `EOG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `FANG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `TMC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `TGB` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `ELF` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `DNN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `HNST` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `AG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `BHP` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `CDE` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `HDSN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `IAG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `KGC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `NFGC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `WPM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `DE` | cash | leftover split 350.43 < 1 share @ 623.26 |
| 2026-08-24 | `AG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `BHP` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `CDE` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `HDSN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `IAG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `KGC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `NFGC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `WPM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `AU` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `AEM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `CRSP` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_e_fresh_h1 |
| 2026-08-25 | `AG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `BHP` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `CDE` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `HDSN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `IAG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `KGC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `NFGC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `WPM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `AU` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `AEM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `CRSP` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-26 | `AG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `BHP` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `CDE` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `HDSN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `IAG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `KGC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `NFGC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `WPM` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `AU` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `AEM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `CRSP` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `DY` | cash | leftover split 294.63 < 1 share @ 326.91 |
| 2026-08-27 | `AU` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `AEM` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `CRSP` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `OCUL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `INSP` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `CRMD` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `RZLT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `HCA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-28 | `OCUL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `INSP` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `CRMD` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `RZLT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `HCA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-31 | `MOS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `OCUL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `INSP` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `CRMD` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `RZLT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `HCA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `RRC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `CRK` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `SLI` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new long union_e_fresh_h1 |
| 2026-09-01 | `RRC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `CRK` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `SLI` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `RRC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-02 | `CRK` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-02 | `SLI` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-04 | `HRMY` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-04 | `VSTM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-04 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `ATRC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `HRMY` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `CABA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `VSTM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `ALEC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `BHC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `BMEA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `OABI` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `OPK` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `VIR` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `ATRC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `HRMY` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `CABA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `VSTM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `ALEC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `BHC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `BMEA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `OABI` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `OPK` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `VIR` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `ATRC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-10 | `HRMY` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-10 | `CABA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-10 | `VSTM` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-10 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-10 | `ALEC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-10 | `BHC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-10 | `BMEA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-10 | `OABI` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-10 | `OPK` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-10 | `VIR` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new long flatten_h5 |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new long flatten_h5 |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new long flatten_h5 |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new long flatten_h5 |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-11 | `ALEC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `BHC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `BMEA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `OABI` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `OPK` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `VIR` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-14 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `OVID` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `SANM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `COHU` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-15 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `OVID` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `SANM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `NVT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `COHU` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-16 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-16 | `OVID` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-16 | `SANM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-16 | `NVT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-16 | `COHU` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-17 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `OVID` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `SANM` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `NVT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `COHU` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `IQV` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `IQV` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `ILMN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `TWST` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `PGEN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `AMN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `IQV` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `ILMN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-21 | `TWST` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-21 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-21 | `AMN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-21 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `DELL` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `VICR` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `ECO` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `HUM` | cash | leftover split 170.82 < 1 share @ 386.20 |
| 2026-09-22 | `IQV` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `ILMN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `TWST` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `PGEN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `AMN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `DELL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `VICR` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `ECO` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `A` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `DXCM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `MGTX` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 45.30 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-23 | `ILMN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `TWST` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `AMN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `DELL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `VICR` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `ECO` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `MGTX` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-23 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-24 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `DELL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `VICR` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `ECO` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `A` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `DXCM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `MGTX` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `HALO` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `ARQT` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `ADMA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `FTRE` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `OMER` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RBRK` | 9 | 2026-09-18 @ $108.55 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; combo leftover $1014.44; owner flatten_h5 |
| `DELL` | 1 | 2026-09-18 @ $593.15 | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; combo leftover $1014.44; owner flatten_h5 |
| `GNRC` | 4 | 2026-09-18 @ $209.52 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; combo leftover $1014.44; owner flatten_h5 |
| `VICR` | 4 | 2026-09-18 @ $219.62 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; combo leftover $1014.44; owner flatten_h5 |
| `ECO` | 11 | 2026-09-18 @ $85.00 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; combo leftover $1014.44; owner flatten_h5 |
| `FIVN` | 29 | 2026-09-18 @ $34.44 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; combo leftover $1014.44; owner flatten_h5 |
| `A` | 1 | 2026-09-21 @ $157.87 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; combo leftover $170.82; owner flatten_h5 |
| `DXCM` | 1 | 2026-09-21 @ $88.83 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; combo leftover $170.82; owner flatten_h5 |
| `MGTX` | 12 | 2026-09-21 @ $13.47 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; combo leftover $170.82; owner flatten_h5 |
| `CYPH` | 42 | 2026-09-21 @ $4.00 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; combo leftover $170.82; owner flatten_h5 |
| `HALO` | 2 | 2026-09-23 @ $116.85 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; combo leftover $326.01; owner flatten_h5 |
| `ARQT` | 11 | 2026-09-23 @ $27.79 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; combo leftover $326.01; owner flatten_h5 |
| `ADMA` | 33 | 2026-09-23 @ $9.81 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; combo leftover $326.01; owner flatten_h5 |
| `FTRE` | 16 | 2026-09-23 @ $20.25 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; combo leftover $326.01; owner flatten_h5 |
| `OMER` | 15 | 2026-09-23 @ $20.65 | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; combo leftover $326.01; owner flatten_h5 |
