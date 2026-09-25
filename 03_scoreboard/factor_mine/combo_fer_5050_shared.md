# Factor mine action — `combo_fer_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared flatten_h5/union_earn_react_h3 w=0.5,0.5 net=priority

Cash book **-14.28%** ($8,572) · signal-only (no cash/fees) was —. Starts YES **3/30**. Fills 264 · skips 550 · realized $+722.94.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: flatten_h5 50%, union_earn_react_h3 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: flatten_h5 50%, union_earn_react_h3 50%.
- Member: flatten_h5 (50% · long · hold 5).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $38.92.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3086 | $0.81 | $34.25 | — | $7,466.09 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten; ⚪; ret5=+13.2; combo leftover $2500.00; owner union_earn_react_h3 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 113 | $22.01 | $2.33 | — | $4,976.63 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten; ⚪; ret5=+0.3; combo leftover $2500.00; owner union_earn_react_h3 | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 11 | $59.80 | $2.02 | — | $4,316.80 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 15 | $45.98 | $2.04 | — | $3,625.07 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 14 | $50.62 | $2.03 | — | $2,914.31 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 14 | $49.70 | $2.03 | — | $2,216.48 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 60 | $11.70 | $2.17 | — | $1,512.31 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 23 | $29.74 | $2.06 | — | $826.23 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 30 | $23.33 | $2.08 | — | $124.25 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.25 | ▲ close $10,400.40 vs 09:30 $10,000.00 (session +451.42) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.25 | ▲ 09:30 equity $10,485.91 vs yday $10,400.40 (+85.51) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRJ` | 1 | $5.51 | $0.06 | — | $118.68 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+13.1; combo leftover $7.77; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `AMPG` | 1 | $4.37 | $0.05 | — | $114.27 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+10.3; combo leftover $7.77; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 10 | $0.77 | $0.11 | — | $106.50 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+20.4; combo leftover $7.77; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `DEFT` | 16 | $0.47 | $0.12 | — | $98.86 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+11.1; combo leftover $7.77; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `DGXX` | 1 | $3.92 | $0.04 | — | $94.90 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+10.1; combo leftover $7.77; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $85.79 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; combo leftover $11.86; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 12 | $0.94 | $0.15 | — | $74.40 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; combo leftover $11.86; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 7 | $1.50 | $0.13 | — | $63.77 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; combo leftover $11.86; owner flatten_h5 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.77 | ▲ close $10,948.96 vs 09:30 $10,485.91 (session +463.79) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.77 | ▼ 09:30 equity $10,878.56 vs yday $10,948.96 (-70.40) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $59.68 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; combo leftover $7.97; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 2 | $3.24 | $0.07 | — | $53.13 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; combo leftover $7.97; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 1 | $4.81 | $0.05 | — | $48.27 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; combo leftover $7.97; owner flatten_h5 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.27 | ▲ close $11,127.67 vs 09:30 $10,878.56 (session +249.28) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.27 | ▼ 09:30 equity $11,008.30 vs yday $11,127.67 (-119.37) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 3086 | $1.14 | $40.35 | $+943.78 | $3,525.96 | ▲ +943.78 after sell → book $10,967.95; vs 09:30 mark -40.35 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 113 | $22.82 | $2.37 | $+86.83 | $6,102.25 | ▲ +86.83 after sell → book $10,965.58; vs 09:30 mark -2.37 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,102.25 | ▲ close $11,015.92 vs 09:30 $11,008.30 (session +50.34) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,102.25 | ▲ 09:30 equity $11,076.43 vs yday $11,015.92 (+60.51) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRJ` | 1 | $5.33 | $0.08 | $-0.31 | $6,107.50 | ▼ -0.31 after sell → book $11,076.35; vs 09:30 mark -0.08 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AMPG` | 1 | $3.56 | $0.06 | $-0.91 | $6,111.01 | ▼ -0.91 after sell → book $11,076.29; vs 09:30 mark -0.06 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BZAI` | 10 | $0.57 | $0.11 | $-2.17 | $6,116.60 | ▼ -2.17 after sell → book $11,076.18; vs 09:30 mark -0.11 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DEFT` | 16 | $0.43 | $0.14 | $-0.82 | $6,123.42 | ▼ -0.82 after sell → book $11,076.05; vs 09:30 mark -0.13 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DGXX` | 1 | $3.66 | $0.06 | $-0.36 | $6,127.02 | ▼ -0.36 after sell → book $11,075.99; vs 09:30 mark -0.06 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,127.02 | ▲ close $11,185.69 vs 09:30 $11,076.43 (session +109.71) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,127.02 | ▼ 09:30 equity $11,149.93 vs yday $11,185.69 (-35.76) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 11 | $58.64 | $2.04 | $-16.83 | $6,770.02 | ▼ -16.83 after sell → book $11,147.89; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 15 | $42.46 | $2.06 | $-56.89 | $7,404.86 | ▼ -56.89 after sell → book $11,145.83; vs 09:30 mark -2.06 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 14 | $53.06 | $2.05 | $+30.03 | $8,145.65 | ▲ +30.03 after sell → book $11,143.78; vs 09:30 mark -2.05 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 14 | $51.65 | $2.05 | $+23.22 | $8,866.70 | ▲ +23.22 after sell → book $11,141.73; vs 09:30 mark -2.05 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 60 | $13.84 | $2.19 | $+124.04 | $9,694.91 | ▲ +124.04 after sell → book $11,139.54; vs 09:30 mark -2.19 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 23 | $30.66 | $2.08 | $+17.02 | $10,398.01 | ▲ +17.02 after sell → book $11,137.46; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 30 | $23.11 | $2.10 | $-10.78 | $11,089.21 | ▼ -10.78 after sell → book $11,135.36; vs 09:30 mark -2.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 14 | $46.85 | $2.03 | — | $10,431.28 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+5.0; combo leftover $693.08; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 76 | $9.01 | $2.22 | — | $9,744.30 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $693.08; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 178 | $3.89 | $2.52 | — | $9,049.36 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.5; combo leftover $693.08; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 20 | $34.05 | $2.05 | — | $8,366.31 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+9.3; combo leftover $693.08; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 30 | $22.44 | $2.08 | — | $7,691.03 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.1; combo leftover $693.08; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 5 | $123.47 | $2.00 | — | $7,071.67 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+2.9; combo leftover $693.08; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `BILL` | 14 | $49.00 | $2.03 | — | $6,383.64 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-2.0; combo leftover $693.08; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 69 | $9.94 | $2.20 | — | $5,695.58 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+12.6; combo leftover $693.08; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 34 | $20.55 | $2.09 | — | $4,994.79 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; combo leftover $711.95; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 7 | $91.01 | $2.01 | — | $4,355.71 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; combo leftover $711.95; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 34 | $20.65 | $2.09 | — | $3,651.52 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; combo leftover $711.95; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 123 | $5.77 | $2.36 | — | $2,939.45 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; combo leftover $711.95; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 36 | $19.63 | $2.10 | — | $2,230.67 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; combo leftover $711.95; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 24 | $29.63 | $2.06 | — | $1,517.49 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; combo leftover $711.95; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 406 | $1.75 | $5.24 | — | $801.75 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; combo leftover $711.95; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $221.59 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; combo leftover $711.95; owner flatten_h5 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $221.59 | ▲ close $11,165.87 vs 09:30 $11,149.93 (session +67.61) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $221.59 | ▲ 09:30 equity $11,316.13 vs yday $11,165.87 (+150.26) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $233.15 | ▲ +2.46 after sell → book $11,315.99; vs 09:30 mark -0.14 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 12 | $0.87 | $0.16 | $-1.15 | $243.39 | ▼ -1.15 after sell → book $11,315.83; vs 09:30 mark -0.16 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 7 | $1.66 | $0.16 | $+0.84 | $254.86 | ▲ +0.84 after sell → book $11,315.68; vs 09:30 mark -0.15 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 1 | $17.93 | $0.18 | — | $236.74 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $25.49; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 11 | $2.30 | $0.29 | — | $211.15 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.0; combo leftover $25.49; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $193.78 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; combo leftover $26.39; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $171.29 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; combo leftover $26.39; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $146.31 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; combo leftover $26.39; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $120.93 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; combo leftover $26.39; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 19 | $1.32 | $0.31 | — | $95.54 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; combo leftover $26.39; owner flatten_h5 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.54 | ▼ close $11,303.25 vs 09:30 $11,316.13 (session -10.67) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.54 | ▲ 09:30 equity $11,414.85 vs yday $11,303.25 (+111.60) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 1 | $4.62 | $0.07 | $+0.46 | $100.10 | ▲ +0.46 after sell → book $11,414.79; vs 09:30 mark -0.06 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 2 | $3.50 | $0.10 | $+0.35 | $107.00 | ▲ +0.35 after sell → book $11,414.69; vs 09:30 mark -0.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 1 | $5.05 | $0.07 | $+0.12 | $111.98 | ▲ +0.12 after sell → book $11,414.62; vs 09:30 mark -0.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.98 | ▲ close $11,494.87 vs 09:30 $11,414.85 (session +80.26) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.98 | ▼ 09:30 equity $11,392.91 vs yday $11,494.87 (-101.96) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 14 | $43.63 | $2.05 | $-49.16 | $720.75 | ▼ -49.16 after sell → book $11,390.86; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 76 | $9.23 | $2.24 | $+12.26 | $1,419.99 | ▲ +12.26 after sell → book $11,388.62; vs 09:30 mark -2.24 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 178 | $5.24 | $2.56 | $+235.21 | $2,350.14 | ▲ +235.21 after sell → book $11,386.05; vs 09:30 mark -2.57 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 20 | $34.72 | $2.07 | $+9.28 | $3,042.47 | ▲ +9.28 after sell → book $11,383.98; vs 09:30 mark -2.07 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 30 | $21.85 | $2.10 | $-21.88 | $3,695.87 | ▼ -21.88 after sell → book $11,381.88; vs 09:30 mark -2.10 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BABA` | 5 | $117.94 | $2.02 | $-31.68 | $4,283.55 | ▼ -31.68 after sell → book $11,379.86; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BILL` | 14 | $47.98 | $2.05 | $-18.29 | $4,953.29 | ▼ -18.29 after sell → book $11,377.81; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BULL` | 69 | $8.46 | $2.22 | $-106.54 | $5,534.81 | ▼ -106.54 after sell → book $11,375.59; vs 09:30 mark -2.22 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 1 | $175.01 | $1.75 | — | $5,358.05 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-7.0; combo leftover $345.93; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 3 | $88.94 | $2.00 | — | $5,089.23 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-4.9; combo leftover $345.93; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 22 | $15.28 | $2.06 | — | $4,751.01 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $345.93; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 2 | $142.36 | $2.00 | — | $4,464.29 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-8.6; combo leftover $345.93; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 67 | $5.10 | $2.19 | — | $4,120.40 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-8.9; combo leftover $345.93; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 7 | $47.89 | $2.01 | — | $3,783.16 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ⚪; ret5=+14.0; combo leftover $345.93; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 24 | $13.92 | $2.06 | — | $3,447.02 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+5.9; combo leftover $345.93; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 76 | $4.54 | $2.22 | — | $3,099.38 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-14.6; combo leftover $345.93; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 21 | $23.77 | $2.05 | — | $2,598.16 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; combo leftover $516.56; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 47 | $10.98 | $2.13 | — | $2,079.97 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; combo leftover $516.56; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 8 | $61.19 | $2.01 | — | $1,588.43 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.4; combo leftover $516.56; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 61 | $8.35 | $2.17 | — | $1,076.91 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; combo leftover $516.56; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 104 | $4.94 | $2.30 | — | $560.85 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; combo leftover $516.56; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 1 | $426.97 | $1.99 | — | $131.89 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.0; combo leftover $516.56; owner flatten_h5 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.89 | ▲ close $11,513.45 vs 09:30 $11,392.91 (session +166.81) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.89 | ▼ 09:30 equity $11,397.70 vs yday $11,513.45 (-115.75) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `BEKE` | 1 | $18.14 | $0.20 | $-0.18 | $149.82 | ▼ -0.18 after sell → book $11,397.49; vs 09:30 mark -0.21 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 11 | $2.35 | $0.31 | $-0.05 | $175.36 | ▼ -0.05 after sell → book $11,397.18; vs 09:30 mark -0.31 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 4 | $5.21 | $0.22 | — | $154.30 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $21.92; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 1 | $18.26 | $0.19 | — | $135.86 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-11.4; combo leftover $21.92; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `FSCO` | 4 | $5.08 | $0.22 | — | $115.32 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-1.6; combo leftover $21.92; owner union_earn_react_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $115.32 | ▼ close $11,391.30 vs 09:30 $11,397.70 (session -5.25) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $115.32 | ▲ 09:30 equity $11,410.22 vs yday $11,391.30 (+18.92) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 34 | $20.93 | $2.11 | $+8.72 | $824.83 | ▲ +8.72 after sell → book $11,408.11; vs 09:30 mark -2.11 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 7 | $95.52 | $2.03 | $+27.53 | $1,491.44 | ▲ +27.53 after sell → book $11,406.08; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 34 | $21.31 | $2.11 | $+18.24 | $2,213.86 | ▲ +18.24 after sell → book $11,403.96; vs 09:30 mark -2.12 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 123 | $5.49 | $2.39 | $-39.19 | $2,886.75 | ▼ -39.19 after sell → book $11,401.58; vs 09:30 mark -2.38 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 36 | $21.47 | $2.12 | $+62.02 | $3,657.55 | ▲ +62.02 after sell → book $11,399.46; vs 09:30 mark -2.12 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 24 | $32.32 | $2.08 | $+60.42 | $4,431.15 | ▲ +60.42 after sell → book $11,397.38; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 406 | $1.91 | $5.31 | $+54.41 | $5,201.29 | ▲ +54.41 after sell → book $11,392.06; vs 09:30 mark -5.32 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 4 | $155.89 | $2.02 | $+41.38 | $5,822.83 | ▲ +41.38 after sell → book $11,390.04; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 4 | $80.60 | $2.00 | — | $5,498.43 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.0; combo leftover $363.93; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 22 | $16.18 | $2.06 | — | $5,140.41 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-6.7; combo leftover $363.93; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 3 | $118.77 | $2.00 | — | $4,782.10 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.3; combo leftover $363.93; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 20 | $17.78 | $2.05 | — | $4,424.45 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-1.2; combo leftover $363.93; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 27 | $13.41 | $2.07 | — | $4,060.31 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.1; combo leftover $363.93; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 3 | $97.16 | $2.00 | — | $3,766.83 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.5; combo leftover $363.93; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 1 | $206.82 | $1.99 | — | $3,558.02 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-0.2; combo leftover $363.93; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 3 | $120.17 | $2.00 | — | $3,195.51 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.9; combo leftover $363.93; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 25 | $41.44 | $2.06 | — | $2,157.44 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; combo leftover $1065.17; owner flatten_h5 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 73 | $14.42 | $2.21 | — | $1,102.58 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; combo leftover $1065.17; owner flatten_h5 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 409 | $2.60 | $5.28 | — | $33.90 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; combo leftover $1065.17; owner flatten_h5 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.90 | ▲ close $11,386.17 vs 09:30 $11,410.22 (session +21.86) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.90 | ▲ 09:30 equity $11,423.30 vs yday $11,386.17 (+37.13) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.44 | $0.19 | $-1.12 | $50.15 | ▼ -1.12 after sell → book $11,423.11; vs 09:30 mark -0.19 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 2 | $15.43 | $0.33 | $+8.04 | $80.68 | ▲ +8.04 after sell → book $11,422.78; vs 09:30 mark -0.33 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 10 | $2.35 | $0.28 | $-1.76 | $103.89 | ▼ -1.76 after sell → book $11,422.49; vs 09:30 mark -0.29 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 13 | $2.06 | $0.33 | $+1.07 | $130.35 | ▲ +1.07 after sell → book $11,422.17; vs 09:30 mark -0.32 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 19 | $1.82 | $0.42 | $+8.77 | $164.50 | ▲ +8.77 after sell → book $11,421.74; vs 09:30 mark -0.43 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 1 | $172.76 | $1.75 | $-5.75 | $335.51 | ▼ -5.75 after sell → book $11,419.99; vs 09:30 mark -1.75 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 3 | $93.30 | $2.02 | $+9.06 | $613.39 | ▲ +9.06 after sell → book $11,417.97; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 22 | $18.15 | $2.08 | $+59.01 | $1,010.62 | ▲ +59.01 after sell → book $11,415.90; vs 09:30 mark -2.07 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 2 | $132.80 | $2.02 | $-23.13 | $1,274.20 | ▼ -23.13 after sell → book $11,413.88; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 67 | $4.58 | $2.21 | $-39.24 | $1,578.85 | ▼ -39.24 after sell → book $11,411.67; vs 09:30 mark -2.21 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 7 | $48.42 | $2.03 | $-0.33 | $1,915.76 | ▼ -0.33 after sell → book $11,409.64; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 24 | $15.66 | $2.08 | $+37.62 | $2,289.52 | ▲ +37.62 after sell → book $11,407.56; vs 09:30 mark -2.08 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 76 | $3.38 | $2.24 | $-93.00 | $2,544.16 | ▼ -93.00 after sell → book $11,405.32; vs 09:30 mark -2.24 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 1 | $261.16 | $1.99 | — | $2,281.00 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+7.8; combo leftover $318.02; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 21 | $15.01 | $2.05 | — | $1,963.74 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+3.7; combo leftover $318.02; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 3 | $103.89 | $2.00 | — | $1,650.07 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.5; combo leftover $318.02; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 81 | $3.88 | $2.23 | — | $1,333.56 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-8.6; combo leftover $318.02; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 7 | $44.40 | $2.01 | — | $1,020.75 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.4; combo leftover $318.02; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 12 | $24.69 | $2.03 | — | $722.44 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.8; combo leftover $318.02; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 38 | $8.35 | $2.10 | — | $403.04 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.1; combo leftover $318.02; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 8 | $37.65 | $2.01 | — | $99.86 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.9; combo leftover $318.02; owner union_earn_react_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.86 | ▼ close $11,114.27 vs 09:30 $11,423.30 (session -274.61) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.86 | ▲ 09:30 equity $11,154.07 vs yday $11,114.27 (+39.80) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 4 | $5.00 | $0.23 | $-1.29 | $119.63 | ▼ -1.29 after sell → book $11,153.84; vs 09:30 mark -0.23 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 1 | $19.25 | $0.22 | $+0.59 | $138.67 | ▲ +0.59 after sell → book $11,153.63; vs 09:30 mark -0.21 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FSCO` | 4 | $5.20 | $0.24 | $+0.02 | $159.23 | ▲ +0.02 after sell → book $11,153.39; vs 09:30 mark -0.24 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $159.23 | ▲ close $11,177.09 vs 09:30 $11,154.07 (session +23.70) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $159.23 | ▲ 09:30 equity $11,248.00 vs yday $11,177.09 (+70.91) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 21 | $23.94 | $2.07 | $-0.56 | $659.89 | ▼ -0.56 after sell → book $11,245.92; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 47 | $10.42 | $2.15 | $-30.60 | $1,147.48 | ▼ -30.60 after sell → book $11,243.77; vs 09:30 mark -2.15 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `INSP` | 8 | $63.00 | $2.03 | $+10.43 | $1,649.45 | ▲ +10.43 after sell → book $11,241.74; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 61 | $8.25 | $2.19 | $-10.47 | $2,150.50 | ▼ -10.47 after sell → book $11,239.54; vs 09:30 mark -2.20 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 104 | $4.64 | $2.33 | $-35.83 | $2,630.73 | ▼ -35.83 after sell → book $11,237.21; vs 09:30 mark -2.33 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `HCA` | 1 | $418.43 | $2.01 | $-12.55 | $3,047.15 | ▼ -12.55 after sell → book $11,235.20; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BBY` | 4 | $79.83 | $2.02 | $-7.10 | $3,364.45 | ▼ -7.10 after sell → book $11,233.18; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BILI` | 22 | $15.97 | $2.08 | $-8.75 | $3,713.71 | ▼ -8.75 after sell → book $11,231.10; vs 09:30 mark -2.08 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CM` | 3 | $113.66 | $2.02 | $-19.35 | $4,052.67 | ▼ -19.35 after sell → book $11,229.08; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CMBT` | 20 | $18.28 | $2.07 | $+5.88 | $4,416.20 | ▲ +5.88 after sell → book $11,227.01; vs 09:30 mark -2.07 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CSIQ` | 27 | $12.18 | $2.09 | $-37.37 | $4,742.97 | ▼ -37.37 after sell → book $11,224.92; vs 09:30 mark -2.09 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `HQY` | 3 | $96.65 | $2.02 | $-5.55 | $5,030.90 | ▼ -5.55 after sell → book $11,222.90; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RY` | 1 | $203.78 | $2.01 | $-7.05 | $5,232.67 | ▼ -7.05 after sell → book $11,220.89; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TD` | 3 | $120.54 | $2.02 | $-2.91 | $5,592.27 | ▼ -2.91 after sell → book $11,218.87; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,592.27 | ▼ close $11,146.45 vs 09:30 $11,248.00 (session -72.42) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,592.27 | ▼ 09:30 equity $11,101.34 vs yday $11,146.45 (-45.11) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 1 | $246.70 | $2.01 | $-18.47 | $5,836.96 | ▼ -18.47 after sell → book $11,099.33; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 21 | $15.01 | $2.07 | $-4.13 | $6,150.10 | ▼ -4.13 after sell → book $11,097.26; vs 09:30 mark -2.07 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 3 | $92.00 | $2.02 | $-39.69 | $6,424.08 | ▼ -39.69 after sell → book $11,095.24; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 81 | $3.32 | $2.26 | $-49.85 | $6,690.74 | ▼ -49.85 after sell → book $11,092.98; vs 09:30 mark -2.26 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 7 | $44.17 | $2.03 | $-5.65 | $6,997.90 | ▼ -5.65 after sell → book $11,090.95; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 12 | $21.97 | $2.05 | $-36.71 | $7,259.49 | ▼ -36.71 after sell → book $11,088.90; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 38 | $8.58 | $2.12 | $+4.51 | $7,583.41 | ▲ +4.51 after sell → book $11,086.78; vs 09:30 mark -2.12 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 8 | $35.80 | $2.03 | $-18.85 | $7,867.74 | ▼ -18.85 after sell → book $11,084.75; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,867.74 | ▼ close $11,049.85 vs 09:30 $11,101.34 (session -34.90) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,867.74 | ▲ 09:30 equity $11,074.75 vs yday $11,049.85 (+24.90) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 25 | $42.43 | $2.08 | $+20.60 | $8,926.40 | ▲ +20.60 after sell → book $11,072.66; vs 09:30 mark -2.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 73 | $15.45 | $2.23 | $+70.75 | $10,052.02 | ▲ +70.75 after sell → book $11,070.43; vs 09:30 mark -2.23 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 409 | $2.49 | $5.35 | $-55.62 | $11,065.08 | ▼ -55.62 after sell → book $11,065.08; vs 09:30 mark -5.35 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 64 | $10.74 | $2.18 | — | $10,375.21 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+8.5; combo leftover $691.57; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $10,021.48 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+3.3; combo leftover $691.57; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 100 | $6.90 | $2.29 | — | $9,329.19 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.8; combo leftover $691.57; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $8,972.71 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.3; combo leftover $691.57; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 30 | $22.32 | $2.08 | — | $8,301.03 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+2.4; combo leftover $691.57; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 2 | $257.00 | $2.00 | — | $7,785.03 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-5.5; combo leftover $691.57; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 14 | $47.60 | $2.03 | — | $7,116.60 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-6.2; combo leftover $691.57; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 45 | $15.09 | $2.12 | — | $6,435.43 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+6.1; combo leftover $691.57; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $52.88 | $2.06 | — | $5,164.24 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; combo leftover $1287.09; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $3,917.20 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; combo leftover $1287.09; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 354 | $3.63 | $4.57 | — | $2,627.61 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; combo leftover $1287.09; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 160 | $8.03 | $2.47 | — | $1,340.34 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; combo leftover $1287.09; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $146.27 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; combo leftover $1287.09; owner flatten_h5 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $146.27 | ▲ close $11,179.24 vs 09:30 $11,074.75 (session +144.05) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $146.27 | ▼ 09:30 equity $11,151.77 vs yday $11,179.24 (-27.47) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 1 | $8.74 | $0.09 | — | $137.44 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.8; combo leftover $9.14; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 2 | $3.62 | $0.08 | — | $130.13 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-3.1; combo leftover $9.14; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 8 | $2.52 | $0.23 | — | $109.75 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; combo leftover $21.69; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 3 | $6.71 | $0.21 | — | $89.41 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; combo leftover $21.69; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 11 | $1.90 | $0.24 | — | $68.27 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; combo leftover $21.69; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 4 | $4.78 | $0.20 | — | $48.94 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; combo leftover $21.69; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 13 | $1.59 | $0.25 | — | $28.03 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; combo leftover $21.69; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 1 | $11.31 | $0.12 | — | $16.60 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; combo leftover $21.69; owner flatten_h5 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.60 | ▲ close $11,236.17 vs 09:30 $11,151.77 (session +85.80) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.60 | ▲ 09:30 equity $11,272.73 vs yday $11,236.17 (+36.56) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.60 | ▼ close $11,143.34 vs 09:30 $11,272.73 (session -129.39) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.60 | ▼ 09:30 equity $11,106.07 vs yday $11,143.34 (-37.27) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 64 | $10.51 | $2.20 | $-19.42 | $687.04 | ▼ -19.42 after sell → book $11,103.87; vs 09:30 mark -2.20 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 1 | $366.23 | $2.01 | $+10.48 | $1,051.26 | ▲ +10.48 after sell → book $11,101.86; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 100 | $9.39 | $2.32 | $+244.39 | $1,987.94 | ▲ +244.39 after sell → book $11,099.54; vs 09:30 mark -2.32 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 1 | $341.90 | $2.01 | $-16.60 | $2,327.83 | ▼ -16.60 after sell → book $11,097.53; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CPB` | 30 | $21.67 | $2.10 | $-23.68 | $2,975.83 | ▼ -23.68 after sell → book $11,095.43; vs 09:30 mark -2.10 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 2 | $252.92 | $2.02 | $-12.17 | $3,479.65 | ▼ -12.17 after sell → book $11,093.41; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 14 | $56.94 | $2.05 | $+126.68 | $4,274.76 | ▲ +126.68 after sell → book $11,091.36; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MEI` | 45 | $13.84 | $2.15 | $-60.52 | $4,895.41 | ▼ -60.52 after sell → book $11,089.21; vs 09:30 mark -2.15 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,895.41 | ▼ close $10,909.94 vs 09:30 $11,106.07 (session -179.27) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,895.41 | ▼ 09:30 equity $10,844.97 vs yday $10,909.94 (-64.97) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ASAN` | 1 | $8.26 | $0.11 | $-0.68 | $4,903.57 | ▼ -0.68 after sell → book $10,844.87; vs 09:30 mark -0.10 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 2 | $3.76 | $0.10 | $+0.11 | $4,910.99 | ▲ +0.11 after sell → book $10,844.76; vs 09:30 mark -0.11 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,910.99 | ▼ close $10,751.33 vs 09:30 $10,844.97 (session -93.44) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,910.99 | ▲ 09:30 equity $10,808.60 vs yday $10,751.33 (+57.27) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 24 | $53.53 | $2.08 | $+11.46 | $6,193.62 | ▲ +11.46 after sell → book $10,806.51; vs 09:30 mark -2.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 29 | $41.30 | $2.10 | $-51.44 | $7,389.23 | ▼ -51.44 after sell → book $10,804.42; vs 09:30 mark -2.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 354 | $2.77 | $4.64 | $-313.64 | $8,365.17 | ▼ -313.64 after sell → book $10,799.78; vs 09:30 mark -4.64 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 160 | $7.70 | $2.51 | $-57.78 | $9,594.66 | ▼ -57.78 after sell → book $10,797.27; vs 09:30 mark -2.51 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 9 | $122.40 | $2.04 | $-94.50 | $10,694.23 | ▼ -94.50 after sell → book $10,795.24; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 4 | $164.43 | $2.00 | — | $10,034.51 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $668.39; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 2 | $242.17 | $2.00 | — | $9,548.17 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-11.1; combo leftover $668.39; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 20 | $32.01 | $2.05 | — | $8,905.92 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-4.4; combo leftover $668.39; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 9 | $71.71 | $2.02 | — | $8,258.51 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-9.1; combo leftover $668.39; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 11 | $56.02 | $2.02 | — | $7,640.27 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.2; combo leftover $668.39; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 71 | $9.37 | $2.20 | — | $6,972.80 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+1.5; combo leftover $668.39; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 51 | $13.10 | $2.14 | — | $6,302.55 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-6.9; combo leftover $668.39; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 4 | $135.71 | $2.00 | — | $5,757.71 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-9.2; combo leftover $668.39; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 70 | $16.28 | $2.20 | — | $4,615.91 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; combo leftover $1151.54; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 421 | $2.73 | $5.43 | — | $3,461.15 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; combo leftover $1151.54; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 5 | $206.84 | $2.00 | — | $2,424.95 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; combo leftover $1151.54; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 7 | $157.78 | $2.01 | — | $1,318.47 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; combo leftover $1151.54; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 20 | $56.09 | $2.05 | — | $194.62 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; combo leftover $1151.54; owner flatten_h5 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $194.62 | ▲ close $10,850.86 vs 09:30 $10,808.60 (session +85.76) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $194.62 | ▼ 09:30 equity $10,634.39 vs yday $10,850.86 (-216.47) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 8 | $2.15 | $0.22 | $-3.40 | $211.61 | ▼ -3.40 after sell → book $10,634.18; vs 09:30 mark -0.21 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 3 | $5.93 | $0.21 | $-2.76 | $229.19 | ▼ -2.76 after sell → book $10,633.97; vs 09:30 mark -0.21 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 11 | $1.72 | $0.24 | $-2.52 | $247.82 | ▼ -2.52 after sell → book $10,633.73; vs 09:30 mark -0.24 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 4 | $4.13 | $0.20 | $-3.00 | $264.14 | ▼ -3.00 after sell → book $10,633.53; vs 09:30 mark -0.20 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 13 | $1.59 | $0.27 | $-0.51 | $284.54 | ▼ -0.51 after sell → book $10,633.27; vs 09:30 mark -0.26 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 1 | $10.73 | $0.13 | $-0.83 | $295.14 | ▼ -0.83 after sell → book $10,633.14; vs 09:30 mark -0.13 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $295.14 | ▲ close $10,662.11 vs 09:30 $10,634.39 (session +28.98) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $295.14 | ▲ 09:30 equity $10,703.35 vs yday $10,662.11 (+41.24) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $295.14 | ▼ close $10,620.50 vs 09:30 $10,703.35 (session -82.85) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $295.14 | ▲ 09:30 equity $10,641.35 vs yday $10,620.50 (+20.85) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 4 | $140.03 | $2.02 | $-101.62 | $853.24 | ▼ -101.62 after sell → book $10,639.33; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 2 | $253.34 | $2.02 | $+18.33 | $1,357.90 | ▲ +18.33 after sell → book $10,637.31; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CPRT` | 20 | $30.57 | $2.07 | $-32.92 | $1,967.23 | ▼ -32.92 after sell → book $10,635.24; vs 09:30 mark -2.07 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DSGX` | 9 | $78.12 | $2.04 | $+53.64 | $2,668.28 | ▲ +53.64 after sell → book $10,633.21; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `KR` | 11 | $61.93 | $2.04 | $+60.94 | $3,347.46 | ▲ +60.94 after sell → book $10,631.16; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `LPTH` | 71 | $9.40 | $2.22 | $-2.30 | $4,012.64 | ▼ -2.30 after sell → book $10,628.94; vs 09:30 mark -2.22 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `REF` | 51 | $15.75 | $2.16 | $+130.84 | $4,813.73 | ▲ +130.84 after sell → book $10,626.78; vs 09:30 mark -2.16 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RH` | 4 | $125.55 | $2.02 | $-44.66 | $5,313.90 | ▼ -44.66 after sell → book $10,624.75; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 64 | $40.93 | $2.18 | — | $2,692.20 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.1; combo leftover $2656.95; owner union_earn_react_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 2 | $270.89 | $2.00 | — | $2,148.43 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.0; combo leftover $673.05; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 8 | $77.12 | $2.01 | — | $1,529.45 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+7.2; combo leftover $673.05; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 47 | $14.31 | $2.13 | — | $854.75 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; combo leftover $673.05; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 18 | $36.46 | $2.04 | — | $196.43 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.9; combo leftover $673.05; owner flatten_h5 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $196.43 | ▼ close $10,597.01 vs 09:30 $10,641.35 (session -17.38) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $196.43 | ▲ 09:30 equity $10,776.03 vs yday $10,597.01 (+179.02) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 4 | $11.21 | $0.46 | — | $151.13 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+1.0; combo leftover $49.11; owner union_earn_react_h3 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 2 | $10.25 | $0.21 | — | $130.42 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; combo leftover $25.19; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 3 | $7.59 | $0.24 | — | $107.41 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.4; combo leftover $25.19; owner flatten_h5 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.41 | ▼ close $10,724.80 vs 09:30 $10,776.03 (session -50.32) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.41 | ▲ 09:30 equity $10,758.96 vs yday $10,724.80 (+34.16) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 70 | $16.93 | $2.22 | $+41.08 | $1,290.29 | ▲ +41.08 after sell → book $10,756.74; vs 09:30 mark -2.22 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 421 | $2.68 | $5.51 | $-31.99 | $2,413.06 | ▼ -31.99 after sell → book $10,751.23; vs 09:30 mark -5.51 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 5 | $197.76 | $2.02 | $-49.43 | $3,399.83 | ▼ -49.43 after sell → book $10,749.20; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 7 | $152.71 | $2.03 | $-39.53 | $4,466.77 | ▼ -39.53 after sell → book $10,747.17; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 20 | $55.80 | $2.07 | $-9.92 | $5,580.70 | ▼ -9.92 after sell → book $10,745.10; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 8 | $108.55 | $2.01 | — | $4,710.29 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; combo leftover $930.12; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 1 | $593.15 | $1.99 | — | $4,115.14 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; combo leftover $930.12; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 4 | $209.52 | $2.00 | — | $3,275.06 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; combo leftover $930.12; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 4 | $219.62 | $2.00 | — | $2,394.58 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; combo leftover $930.12; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 10 | $85.00 | $2.02 | — | $1,542.56 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; combo leftover $930.12; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 27 | $34.44 | $2.07 | — | $610.61 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; combo leftover $930.12; owner flatten_h5 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $610.61 | ▼ close $10,621.19 vs 09:30 $10,758.96 (session -111.81) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $610.61 | ▲ 09:30 equity $10,704.95 vs yday $10,621.19 (+83.76) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 64 | $41.00 | $2.21 | $+0.08 | $3,232.40 | ▲ +0.08 after sell → book $10,702.74; vs 09:30 mark -2.21 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 4 | $157.87 | $2.00 | — | $2,598.91 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; combo leftover $646.48; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 1 | $386.20 | $1.99 | — | $2,210.72 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-5.8; combo leftover $646.48; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 7 | $88.83 | $2.01 | — | $1,586.90 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; combo leftover $646.48; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 47 | $13.47 | $2.13 | — | $951.68 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; combo leftover $646.48; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 161 | $4.00 | $2.47 | — | $305.21 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; combo leftover $646.48; owner flatten_h5 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $305.21 | ▼ close $10,657.86 vs 09:30 $10,704.95 (session -34.27) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $305.21 | ▲ 09:30 equity $10,670.04 vs yday $10,657.86 (+12.18) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $305.21 | ▲ close $10,685.50 vs 09:30 $10,670.04 (session +15.46) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $305.21 | ▲ 09:30 equity $10,859.35 vs yday $10,685.50 (+173.85) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `IQV` | 2 | $270.66 | $2.02 | $-4.47 | $844.51 | ▼ -4.47 after sell → book $10,857.33; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 8 | $73.61 | $2.03 | $-32.13 | $1,431.36 | ▼ -32.13 after sell → book $10,855.30; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 47 | $13.12 | $2.15 | $-60.21 | $2,045.84 | ▼ -60.21 after sell → book $10,853.15; vs 09:30 mark -2.15 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 18 | $38.04 | $2.06 | $+24.33 | $2,728.50 | ▲ +24.33 after sell → book $10,851.09; vs 09:30 mark -2.06 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `ALMU` | 4 | $13.82 | $0.58 | $+9.39 | $2,783.20 | ▲ +9.39 after sell → book $10,850.50; vs 09:30 mark -0.59 | union_earn_react_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 5 | $47.57 | $2.00 | — | $2,543.34 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-11.2; combo leftover $278.32; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 1 | $196.78 | $1.97 | — | $2,344.59 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $278.32; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 7 | $35.74 | $2.01 | — | $2,092.40 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.1; combo leftover $278.32; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 5 | $47.15 | $2.00 | — | $1,854.64 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-1.9; combo leftover $278.32; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 2 | $109.67 | $2.00 | — | $1,633.31 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $278.32; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 2 | $116.85 | $2.00 | — | $1,397.61 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; combo leftover $326.66; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 11 | $27.79 | $2.02 | — | $1,089.90 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; combo leftover $326.66; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 33 | $9.81 | $2.09 | — | $764.08 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; combo leftover $326.66; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 16 | $20.25 | $2.04 | — | $438.04 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; combo leftover $326.66; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 15 | $20.65 | $2.04 | — | $126.26 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; combo leftover $326.66; owner flatten_h5 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.26 | ▼ close $10,738.58 vs 09:30 $10,859.35 (session -91.76) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.26 | ▼ 09:30 equity $10,585.23 vs yday $10,738.58 (-153.35) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 2 | $10.39 | $0.23 | $-0.16 | $146.80 | ▼ -0.16 after sell → book $10,584.99; vs 09:30 mark -0.24 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 3 | $7.38 | $0.25 | $-1.12 | $168.69 | ▼ -1.12 after sell → book $10,584.74; vs 09:30 mark -0.25 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $168.69 | ▲ close $10,741.73 vs 09:30 $10,585.23 (session +157.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.38 | ▼ 09:30 equity $8,567.33 vs yday $8,570.53 (-3.20) | 09:30 open · cash $54.38 (unchanged overnight, no fees) · equity $8,567.33 vs prior close $8,570.53 (-3.20) | — |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 2 | $7.65 | $0.16 | — | $38.92 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; combo leftover $18.13; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.92 | ▲ close $8,571.70 vs 09:30 $8,567.33 (session +4.53) | 16:00 close · cash $38.92 · equity $8,571.70 vs 09:30 $8,567.33 (+4.37; session marks +4.53) · 27 name(s) marked open→close (per-name table). A×4 09:30 $171.98 → close $172.79 +3.24; ADMA×40 09:30 $9.52 → close $9.52 +0.00; ARQT×14 09:30 $26.27 → close $26.27 +0.00; CBRL×6 09:30 $52.39 → close $51.81 -3.48; CTAS×1 09:30 $197.68 → close $197.68 -0.00; CYPH×168 09:30 $4.00 → close $4.12 +19.32; DLO×4 09:30 $13.88 → close $13.88 +0.00; DXCM×7 09:30 $87.47 → close $87.47 +0.00; ECO×4 09:30 $78.22 → close $78.22 +0.00; FIVN×11 09:30 $36.66 → close $36.66 -0.00; FTRE×19 09:30 $20.02 → close $20.02 +0.00; GIS×9 09:30 $34.83 → close $34.83 +0.00; GNRC×1 09:30 $198.05 → close $198.05 +0.00; HALO×3 09:30 $115.36 → close $113.90 -4.38; HUM×1 09:30 $380.32 → close $380.32 +0.00; IOVA×64 09:30 $10.80 → close $10.80 +0.00; KBH×6 09:30 $47.65 → close $47.65 +0.00; MGTX×49 09:30 $11.05 → close $11.05 +0.00; MKC×1 09:30 $47.82 → close $47.82 -0.00; MLKN×2 09:30 $19.91 → close $19.91 -0.00; OMER×19 09:30 $20.61 → close $20.08 -10.07; PACS×1 09:30 $41.46 → close $41.46 -0.00; PAYX×2 09:30 $101.59 → close $101.59 -0.00; RBRK×3 09:30 $113.80 → close $113.80 +0.00; TDC×1 09:30 $29.46 → close $29.46 -0.00; VICR×1 09:30 $276.06 → close $276.06 -0.00; MRVI×2 09:30 $7.65 → close $7.60 -0.10 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `INO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `VOR` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `IREN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TPG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `SLS` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `NMAX` | cash | leftover split 7.77 < 1 share @ 9.89 |
| 2026-08-14 | `AMAT` | cash | leftover split 7.77 < 1 share @ 499.40 |
| 2026-08-14 | `BRUN` | cash | leftover split 7.77 < 1 share @ 26.25 |
| 2026-08-14 | `TLN` | cash | leftover split 11.86 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 11.86 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 11.86 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 11.86 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 11.86 < 1 share @ 57.61 |
| 2026-08-17 | `INO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `VOR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `IREN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `TPG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `SLS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `AIRJ` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `AMPG` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `BZAI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `DEFT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `DGXX` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `MARA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `LDI` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `BTBT` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `DVN` | cash | leftover split 7.97 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 7.97 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 7.97 < 1 share @ 202.70 |
| 2026-08-17 | `TGB` | cash | leftover split 7.97 < 1 share @ 8.46 |
| 2026-08-17 | `ELF` | cash | leftover split 7.97 < 1 share @ 90.54 |
| 2026-08-18 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `IREN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TPG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `SLS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `AIRJ` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `AMPG` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `BZAI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `DEFT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `DGXX` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `MARA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `LDI` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `BTBT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `TMC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
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
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `IREN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `TPG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `SLS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `MARA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `LDI` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `BTBT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `TMC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
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
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `LOW` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `LZB` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `MRCY` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-20 | `MARA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `LDI` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `BTBT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `TMC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `DNN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `HNST` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-21 | `TMC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `DNN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `HNST` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `AAP` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AEG` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ALVO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ATAT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ATHM` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `BABA` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `BILL` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `BULL` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `BHP` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `CDE` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `HDSN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `IAG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `KGC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `NFGC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `WPM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `BJ` | cash | leftover split 25.49 < 1 share @ 93.98 |
| 2026-08-21 | `BKE` | cash | leftover split 25.49 < 1 share @ 43.08 |
| 2026-08-21 | `ROST` | cash | leftover split 25.49 < 1 share @ 243.85 |
| 2026-08-21 | `AU` | cash | leftover split 26.39 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 26.39 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 26.39 < 1 share @ 59.72 |
| 2026-08-24 | `AAP` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AEG` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ALVO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ATAT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ATHM` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BABA` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BILL` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BULL` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `BHP` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `CDE` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `HDSN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `IAG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `KGC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `NFGC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `WPM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `BEKE` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `PSEC` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_earn_react_h3 |
| 2026-08-25 | `AG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `BHP` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `CDE` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `HDSN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `IAG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `KGC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `NFGC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `WPM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `BEKE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `PSEC` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-26 | `AG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `BHP` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `CDE` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `HDSN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `IAG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `KGC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `NFGC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `WPM` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `BMO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BNS` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BZ` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `DKS` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `EH` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GFI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GRRR` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `SHMD` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `ANF` | cash | leftover split 21.92 < 1 share @ 131.37 |
| 2026-08-26 | `BOX` | cash | leftover split 21.92 < 1 share @ 34.30 |
| 2026-08-26 | `DY` | cash | leftover split 21.92 < 1 share @ 326.91 |
| 2026-08-26 | `HEI` | cash | leftover split 21.92 < 1 share @ 370.00 |
| 2026-08-26 | `INTU` | cash | leftover split 21.92 < 1 share @ 323.47 |
| 2026-08-27 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `BMO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BNS` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BZ` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `DKS` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `EH` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `GFI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `GRRR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `SHMD` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `OCUL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `INSP` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `CRMD` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `RZLT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `HCA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `TIGR` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BBWI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `FSCO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `OCUL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `INSP` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `CRMD` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `RZLT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `HCA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `TIGR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBWI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `FSCO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBY` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `BILI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CM` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CMBT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CSIQ` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `HQY` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `RY` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TD` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `MOS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `OCUL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `INSP` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `CRMD` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `RZLT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `HCA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `BBY` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `BILI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CM` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CMBT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CSIQ` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `HQY` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `RY` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `TD` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `RRC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `CRK` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `SLI` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `ADSK` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `BBAR` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `ESTC` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FINV` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FRO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `GAP` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `HAFN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `IREN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new long union_earn_react_h3 |
| 2026-09-01 | `RRC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `CRK` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `SLI` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `ADSK` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `BBAR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `ESTC` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FINV` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FRO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `GAP` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `HAFN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `IREN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new long union_earn_react_h3 |
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
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-04 | `AI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `AVGO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CHPT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CIEN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CPB` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `FIVE` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `HPE` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `MEI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `HRMY` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-04 | `VSTM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-04 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-04 | `AMBA` | cash | leftover split 9.14 < 1 share @ 63.18 |
| 2026-09-04 | `DOCU` | cash | leftover split 9.14 < 1 share @ 68.52 |
| 2026-09-04 | `GWRE` | cash | leftover split 9.14 < 1 share @ 167.55 |
| 2026-09-04 | `IOT` | cash | leftover split 9.14 < 1 share @ 44.90 |
| 2026-09-04 | `LULU` | cash | leftover split 9.14 < 1 share @ 98.15 |
| 2026-09-04 | `MAMA` | cash | leftover split 9.14 < 1 share @ 15.70 |
| 2026-09-08 | `AI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `AVGO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CHPT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CIEN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CPB` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `FIVE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `HPE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `MEI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `ATRC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `HRMY` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `CABA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `VSTM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `ASAN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `DOMO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
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
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new long union_earn_react_h3 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new long union_earn_react_h3 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `ATRC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `HRMY` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `CABA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `VSTM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `ASAN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `DOMO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
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
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
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
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `M` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `NAVN` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `NB` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `SHOE` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-11 | `ALEC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `BHC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `BMEA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `OABI` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `OPK` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `VIR` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-14 | `ORCL` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `ADBE` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CPRT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `DSGX` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `KR` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `LPTH` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `REF` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `RH` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `OVID` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `SANM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `COHU` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-15 | `ORCL` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `ADBE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `CPRT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `DSGX` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `KR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `LPTH` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `REF` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `RH` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `OVID` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `SANM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `NVT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `COHU` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
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
| 2026-09-17 | `TCOM` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-17 | `IQV` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `LEN` | cash | leftover split 49.11 < 1 share @ 81.00 |
| 2026-09-17 | `ILMN` | cash | leftover split 25.19 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 25.19 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 25.19 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 25.19 < 1 share @ 34.93 |
| 2026-09-18 | `TCOM` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `IQV` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `ALMU` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `PGEN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `IQV` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `ALMU` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `DELL` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `VICR` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `ECO` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `IQV` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `ALMU` | no_price | no 09:30 open — carry |
| 2026-09-22 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `PGEN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `DELL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `VICR` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `ECO` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `A` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `HUM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
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
| 2026-09-22 | `USFD` | cash | leftover split 50.87 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-23 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `DELL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `VICR` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `ECO` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `HUM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-23 | `MGTX` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-23 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-24 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `DELL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `VICR` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `ECO` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `A` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `HUM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `DXCM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `MGTX` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `CBRL` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `CTAS` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `GIS` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `KBH` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `PAYX` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
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
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RBRK` | 8 | 2026-09-18 @ $108.55 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; combo leftover $930.12; owner flatten_h5 |
| `DELL` | 1 | 2026-09-18 @ $593.15 | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; combo leftover $930.12; owner flatten_h5 |
| `GNRC` | 4 | 2026-09-18 @ $209.52 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; combo leftover $930.12; owner flatten_h5 |
| `VICR` | 4 | 2026-09-18 @ $219.62 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; combo leftover $930.12; owner flatten_h5 |
| `ECO` | 10 | 2026-09-18 @ $85.00 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; combo leftover $930.12; owner flatten_h5 |
| `FIVN` | 27 | 2026-09-18 @ $34.44 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; combo leftover $930.12; owner flatten_h5 |
| `A` | 4 | 2026-09-21 @ $157.87 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; combo leftover $646.48; owner flatten_h5 |
| `HUM` | 1 | 2026-09-21 @ $386.20 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-5.8; combo leftover $646.48; owner flatten_h5 |
| `DXCM` | 7 | 2026-09-21 @ $88.83 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; combo leftover $646.48; owner flatten_h5 |
| `MGTX` | 47 | 2026-09-21 @ $13.47 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; combo leftover $646.48; owner flatten_h5 |
| `CYPH` | 161 | 2026-09-21 @ $4.00 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; combo leftover $646.48; owner flatten_h5 |
| `CBRL` | 5 | 2026-09-23 @ $47.57 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-11.2; combo leftover $278.32; owner union_earn_react_h3 |
| `CTAS` | 1 | 2026-09-23 @ $196.78 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $278.32; owner union_earn_react_h3 |
| `GIS` | 7 | 2026-09-23 @ $35.74 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.1; combo leftover $278.32; owner union_earn_react_h3 |
| `KBH` | 5 | 2026-09-23 @ $47.15 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-1.9; combo leftover $278.32; owner union_earn_react_h3 |
| `PAYX` | 2 | 2026-09-23 @ $109.67 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $278.32; owner union_earn_react_h3 |
| `HALO` | 2 | 2026-09-23 @ $116.85 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; combo leftover $326.66; owner flatten_h5 |
| `ARQT` | 11 | 2026-09-23 @ $27.79 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; combo leftover $326.66; owner flatten_h5 |
| `ADMA` | 33 | 2026-09-23 @ $9.81 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; combo leftover $326.66; owner flatten_h5 |
| `FTRE` | 16 | 2026-09-23 @ $20.25 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; combo leftover $326.66; owner flatten_h5 |
| `OMER` | 15 | 2026-09-23 @ $20.65 | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; combo leftover $326.66; owner flatten_h5 |
