# Factor mine action — `combo_hf_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_hot_n4_h1/flatten_h5 w=0.5,0.5 net=priority

Cash book **-1.12%** ($9,888) · signal-only (no cash/fees) was —. Starts YES **29/30**. Fills 271 · skips 415 · realized $+719.52.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_hot_n4_h1 50%, flatten_h5 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_hot_n4_h1 50%, flatten_h5 50%.
- Member: union_hot_n4_h1 (50% · long · hold 1).
- Member: flatten_h5 (50% · long · hold 5).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $414.29.

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
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $8,756.47 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; combo leftover $1250.00; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $7,517.83 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; combo leftover $1250.00; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; combo leftover $1250.00; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $5,033.85 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+13.2; combo leftover $1250.00; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 21 | $59.80 | $2.05 | — | $3,776.00 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; combo leftover $1258.46; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $2,531.44 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; combo leftover $1258.46; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 107 | $11.70 | $2.31 | — | $1,277.22 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; combo leftover $1258.46; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $26.03 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; combo leftover $1258.46; owner flatten_h5 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.03 | ▲ close $10,154.21 vs 09:30 $10,000.00 (session +186.16) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.03 | ▲ 09:30 equity $10,178.67 vs yday $10,154.21 (+24.46) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $1,214.37 | ▼ -55.19 after sell → book $10,176.58; vs 09:30 mark -2.09 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $2,426.96 | ▼ -26.05 after sell → book $10,174.41; vs 09:30 mark -2.17 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,751.84 | ▲ +107.86 after sell → book $10,172.33; vs 09:30 mark -2.08 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $5,167.58 | ▲ +148.79 after sell → book $10,153.08; vs 09:30 mark -19.25 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 26 | $24.68 | $2.07 | — | $4,523.83 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; combo leftover $645.95; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 33 | $19.57 | $2.09 | — | $3,875.93 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $645.95; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 293 | $2.20 | $3.78 | — | $3,227.55 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; combo leftover $645.95; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 58 | $11.12 | $2.16 | — | $2,580.43 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $645.95; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 2 | $146.90 | $2.00 | — | $2,284.63 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+3.6; combo leftover $322.55; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 2 | $120.00 | $2.00 | — | $2,042.64 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+0.6; combo leftover $322.55; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 5 | $57.61 | $2.00 | — | $1,752.58 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.7; combo leftover $322.55; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 35 | $9.01 | $2.10 | — | $1,435.14 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; combo leftover $322.55; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 344 | $0.94 | $4.26 | — | $1,108.55 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; combo leftover $322.55; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 215 | $1.50 | $2.77 | — | $783.28 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; combo leftover $322.55; owner flatten_h5 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $783.28 | ▲ close $10,151.84 vs 09:30 $10,178.67 (session +23.98) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $783.28 | ▼ 09:30 equity $10,095.86 vs yday $10,151.84 (-55.98) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 26 | $24.83 | $2.09 | $-0.26 | $1,426.77 | ▼ -0.26 after sell → book $10,093.78; vs 09:30 mark -2.08 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 33 | $19.57 | $2.11 | $-4.20 | $2,070.47 | ▼ -4.20 after sell → book $10,091.67; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 293 | $2.08 | $3.84 | $-41.31 | $2,677.54 | ▼ -41.31 after sell → book $10,087.83; vs 09:30 mark -3.84 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 58 | $9.57 | $2.18 | $-94.25 | $3,230.42 | ▼ -94.25 after sell → book $10,085.64; vs 09:30 mark -2.19 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 96 | $4.19 | $2.28 | — | $2,825.90 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $403.80; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 58 | $6.87 | $2.16 | — | $2,425.27 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $403.80; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 29 | $13.64 | $2.08 | — | $2,027.64 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $403.80; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 9 | $41.23 | $2.02 | — | $1,654.55 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $403.80; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 4 | $46.18 | $1.86 | — | $1,467.97 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+6.7; combo leftover $206.82; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 1 | $142.77 | $1.43 | — | $1,323.77 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+5.8; combo leftover $206.82; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $1,119.08 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+8.3; combo leftover $206.82; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 51 | $4.05 | $2.14 | — | $910.38 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; combo leftover $206.82; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 24 | $8.46 | $2.06 | — | $705.28 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; combo leftover $206.82; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 2 | $90.54 | $1.82 | — | $522.39 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-7.2; combo leftover $206.82; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 63 | $3.24 | $2.18 | — | $316.09 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; combo leftover $206.82; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 42 | $4.81 | $2.12 | — | $111.95 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; combo leftover $206.82; owner flatten_h5 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.95 | ▲ close $10,109.33 vs 09:30 $10,095.86 (session +47.82) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.95 | ▼ 09:30 equity $9,994.66 vs yday $10,109.33 (-114.67) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 96 | $3.94 | $2.30 | $-28.58 | $487.89 | ▼ -28.58 after sell → book $9,992.36; vs 09:30 mark -2.30 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 29 | $13.31 | $2.10 | $-13.74 | $871.78 | ▼ -13.74 after sell → book $9,990.26; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 9 | $41.50 | $2.04 | $-1.62 | $1,243.24 | ▼ -1.62 after sell → book $9,988.22; vs 09:30 mark -2.04 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,243.24 | ▼ close $9,962.39 vs 09:30 $9,994.66 (session -25.83) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,243.24 | ▲ 09:30 equity $10,089.86 vs yday $9,962.39 (+127.47) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 58 | $7.19 | $2.18 | $+14.21 | $1,658.08 | ▲ +14.21 after sell → book $10,087.68; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,658.08 | ▲ close $10,308.43 vs 09:30 $10,089.86 (session +220.75) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,658.08 | ▼ 09:30 equity $10,299.36 vs yday $10,308.43 (-9.07) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 21 | $58.64 | $2.07 | $-28.49 | $2,887.45 | ▼ -28.49 after sell → book $10,297.28; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 25 | $51.65 | $2.09 | $+44.60 | $4,176.61 | ▲ +44.60 after sell → book $10,295.20; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 107 | $13.84 | $2.34 | $+224.33 | $5,655.15 | ▲ +224.33 after sell → book $10,292.86; vs 09:30 mark -2.34 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 42 | $30.66 | $2.14 | $+34.39 | $6,940.73 | ▲ +34.39 after sell → book $10,290.72; vs 09:30 mark -2.14 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 5 | $150.14 | $2.00 | — | $6,188.03 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $867.59; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 754 | $1.15 | $9.73 | — | $5,311.20 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $867.59; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 73 | $11.81 | $2.21 | — | $4,446.50 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $867.59; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 633 | $1.37 | $8.17 | — | $3,571.12 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $867.59; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 21 | $20.55 | $2.05 | — | $3,137.52 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; combo leftover $446.39; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 4 | $91.01 | $2.00 | — | $2,771.48 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; combo leftover $446.39; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 21 | $20.65 | $2.05 | — | $2,335.77 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; combo leftover $446.39; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 77 | $5.77 | $2.22 | — | $1,889.26 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; combo leftover $446.39; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 22 | $19.63 | $2.06 | — | $1,455.35 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; combo leftover $446.39; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 15 | $29.63 | $2.04 | — | $1,008.86 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; combo leftover $446.39; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 255 | $1.75 | $3.29 | — | $559.32 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; combo leftover $446.39; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 3 | $144.54 | $2.00 | — | $123.70 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; combo leftover $446.39; owner flatten_h5 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.70 | ▲ close $10,360.91 vs 09:30 $10,299.36 (session +110.01) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.70 | ▲ 09:30 equity $10,626.52 vs yday $10,360.91 (+265.61) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `VST` | 2 | $139.99 | $2.02 | $-17.83 | $401.67 | ▼ -17.83 after sell → book $10,624.51; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `NRG` | 2 | $116.58 | $2.02 | $-10.85 | $632.81 | ▼ -10.85 after sell → book $10,622.49; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `SLG` | 5 | $58.63 | $2.02 | $+1.07 | $923.94 | ▲ +1.07 after sell → book $10,620.46; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 35 | $11.70 | $2.12 | $+89.94 | $1,331.32 | ▲ +89.94 after sell → book $10,618.35; vs 09:30 mark -2.11 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 344 | $0.87 | $4.08 | $-32.42 | $1,625.49 | ▼ -32.42 after sell → book $10,614.27; vs 09:30 mark -4.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 215 | $1.66 | $2.82 | $+28.81 | $1,979.57 | ▲ +28.81 after sell → book $10,611.45; vs 09:30 mark -2.82 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 73 | $11.57 | $2.23 | $-22.33 | $2,821.95 | ▼ -22.33 after sell → book $10,609.22; vs 09:30 mark -2.23 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 633 | $1.46 | $8.28 | $+40.52 | $3,737.85 | ▲ +40.52 after sell → book $10,600.94; vs 09:30 mark -8.28 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 208 | $4.49 | $2.68 | — | $2,801.24 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $934.46; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 137 | $6.81 | $2.40 | — | $1,865.87 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $934.46; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 2 | $119.43 | $2.00 | — | $1,625.02 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+21.1; combo leftover $266.55; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 15 | $17.20 | $2.04 | — | $1,364.98 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; combo leftover $266.55; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 1 | $216.30 | $1.99 | — | $1,146.69 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; combo leftover $266.55; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 23 | $11.13 | $2.06 | — | $888.64 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; combo leftover $266.55; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 107 | $2.47 | $2.31 | — | $622.04 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; combo leftover $266.55; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 138 | $1.93 | $2.40 | — | $353.29 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; combo leftover $266.55; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 4 | $59.72 | $2.00 | — | $112.41 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; combo leftover $266.55; owner flatten_h5 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $112.41 | ▲ close $10,723.31 vs 09:30 $10,626.52 (session +142.26) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $112.41 | ▲ 09:30 equity $11,261.32 vs yday $10,723.31 (+538.01) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `DVN` | 4 | $48.89 | $1.99 | $+6.99 | $305.98 | ▲ +6.99 after sell → book $11,259.33; vs 09:30 mark -1.99 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `EOG` | 1 | $152.07 | $1.54 | $+6.33 | $456.51 | ▲ +6.33 after sell → book $11,257.79; vs 09:30 mark -1.54 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `FANG` | 1 | $210.00 | $2.01 | $+3.29 | $664.50 | ▲ +3.29 after sell → book $11,255.77; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 51 | $4.62 | $2.16 | $+25.02 | $898.21 | ▲ +25.02 after sell → book $11,253.61; vs 09:30 mark -2.16 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 24 | $9.26 | $2.08 | $+15.06 | $1,118.37 | ▲ +15.06 after sell → book $11,251.53; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `ELF` | 2 | $102.20 | $2.02 | $+19.49 | $1,320.75 | ▲ +19.49 after sell → book $11,249.51; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 63 | $3.50 | $2.20 | $+12.00 | $1,539.05 | ▲ +12.00 after sell → book $11,247.31; vs 09:30 mark -2.20 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 42 | $5.05 | $2.14 | $+5.83 | $1,749.02 | ▲ +5.83 after sell → book $11,245.18; vs 09:30 mark -2.13 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 5 | $142.70 | $2.02 | $-41.23 | $2,460.49 | ▼ -41.23 after sell → book $11,243.15; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 754 | $1.83 | $9.86 | $+493.13 | $3,830.45 | ▲ +493.13 after sell → book $11,233.29; vs 09:30 mark -9.86 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 208 | $4.32 | $2.73 | $-40.77 | $4,726.28 | ▼ -40.77 after sell → book $11,230.56; vs 09:30 mark -2.73 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 137 | $8.03 | $2.43 | $+162.31 | $5,823.96 | ▲ +162.31 after sell → book $11,228.13; vs 09:30 mark -2.43 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,823.96 | ▼ close $11,225.36 vs 09:30 $11,261.32 (session -2.77) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,823.96 | ▼ 09:30 equity $11,165.70 vs yday $11,225.36 (-59.66) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 30 | $24.11 | $2.08 | — | $5,098.58 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $727.99; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 466 | $1.56 | $6.01 | — | $4,365.61 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $727.99; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 178 | $4.07 | $2.52 | — | $3,638.62 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $727.99; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 38 | $19.04 | $2.10 | — | $2,913.00 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $727.99; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 20 | $23.77 | $2.05 | — | $2,435.55 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; combo leftover $485.50; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 44 | $10.98 | $2.12 | — | $1,950.31 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; combo leftover $485.50; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 7 | $61.19 | $2.01 | — | $1,519.97 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.4; combo leftover $485.50; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 58 | $8.35 | $2.16 | — | $1,033.50 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; combo leftover $485.50; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 98 | $4.94 | $2.28 | — | $547.10 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; combo leftover $485.50; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 1 | $426.97 | $1.99 | — | $118.13 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.0; combo leftover $485.50; owner flatten_h5 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $118.13 | ▲ close $11,646.10 vs 09:30 $11,165.70 (session +505.75) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.13 | ▼ 09:30 equity $11,425.61 vs yday $11,646.10 (-220.49) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 30 | $26.61 | $2.10 | $+70.82 | $914.33 | ▲ +70.82 after sell → book $11,423.51; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 466 | $1.60 | $6.10 | $+6.53 | $1,653.84 | ▲ +6.53 after sell → book $11,417.42; vs 09:30 mark -6.09 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 38 | $20.72 | $2.12 | $+59.61 | $2,439.07 | ▲ +59.61 after sell → book $11,415.29; vs 09:30 mark -2.13 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 57 | $14.11 | $2.16 | — | $1,632.64 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $813.02; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 139 | $5.81 | $2.41 | — | $822.64 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $813.02; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 70 | $11.59 | $2.20 | — | $9.49 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; combo leftover $813.02; owner union_hot_n4_h1 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.49 | ▲ close $11,421.14 vs 09:30 $11,425.61 (session +12.62) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.49 | ▲ 09:30 equity $11,528.83 vs yday $11,421.14 (+107.69) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 21 | $20.93 | $2.07 | $+3.85 | $446.95 | ▲ +3.85 after sell → book $11,526.76; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 4 | $95.52 | $2.02 | $+14.02 | $827.01 | ▲ +14.02 after sell → book $11,524.74; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 21 | $21.31 | $2.07 | $+9.73 | $1,272.45 | ▲ +9.73 after sell → book $11,522.67; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 77 | $5.49 | $2.24 | $-26.02 | $1,692.93 | ▼ -26.02 after sell → book $11,520.42; vs 09:30 mark -2.25 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 22 | $21.47 | $2.08 | $+36.35 | $2,163.20 | ▲ +36.35 after sell → book $11,518.35; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 15 | $32.32 | $2.06 | $+36.26 | $2,645.94 | ▲ +36.26 after sell → book $11,516.29; vs 09:30 mark -2.06 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 255 | $1.91 | $3.34 | $+34.17 | $3,129.65 | ▲ +34.17 after sell → book $11,512.95; vs 09:30 mark -3.34 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 3 | $155.89 | $2.02 | $+30.03 | $3,595.30 | ▲ +30.03 after sell → book $11,510.93; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 57 | $14.20 | $2.18 | $+0.79 | $4,402.52 | ▲ +0.79 after sell → book $11,508.75; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 139 | $6.50 | $2.44 | $+91.06 | $5,303.58 | ▲ +91.06 after sell → book $11,506.31; vs 09:30 mark -2.44 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 70 | $12.18 | $2.22 | $+37.23 | $6,153.96 | ▲ +37.23 after sell → book $11,504.09; vs 09:30 mark -2.22 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 111 | $9.19 | $2.32 | — | $5,131.54 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; combo leftover $1025.66; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 7 | $144.18 | $2.01 | — | $4,120.27 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; combo leftover $1025.66; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 55 | $18.50 | $2.15 | — | $3,100.62 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; combo leftover $1025.66; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 24 | $41.44 | $2.06 | — | $2,104.00 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; combo leftover $1033.54; owner flatten_h5 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 71 | $14.42 | $2.20 | — | $1,077.97 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; combo leftover $1033.54; owner flatten_h5 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 397 | $2.60 | $5.12 | — | $40.65 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; combo leftover $1033.54; owner flatten_h5 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.65 | ▲ close $11,520.50 vs 09:30 $11,528.83 (session +32.29) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.65 | ▼ 09:30 equity $11,439.74 vs yday $11,520.50 (-80.76) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 2 | $119.19 | $2.02 | $-4.49 | $277.02 | ▼ -4.49 after sell → book $11,437.73; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 15 | $16.44 | $2.06 | $-15.49 | $521.56 | ▼ -15.49 after sell → book $11,435.67; vs 09:30 mark -2.06 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEM` | 1 | $216.31 | $2.01 | $-4.00 | $735.86 | ▼ -4.00 after sell → book $11,433.66; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 23 | $15.43 | $2.08 | $+94.76 | $1,088.67 | ▲ +94.76 after sell → book $11,431.58; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 107 | $2.35 | $2.34 | $-17.49 | $1,337.78 | ▼ -17.49 after sell → book $11,429.24; vs 09:30 mark -2.34 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 138 | $2.06 | $2.44 | $+13.10 | $1,619.62 | ▲ +13.10 after sell → book $11,426.80; vs 09:30 mark -2.44 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRSP` | 4 | $58.22 | $2.02 | $-10.02 | $1,850.48 | ▼ -10.02 after sell → book $11,424.78; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 178 | $3.69 | $2.56 | $-72.73 | $2,504.74 | ▼ -72.73 after sell → book $11,422.22; vs 09:30 mark -2.56 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 55 | $18.15 | $2.17 | $-23.58 | $3,500.81 | ▼ -23.58 after sell → book $11,420.04; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 125 | $14.00 | $2.37 | — | $1,748.45 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $1750.41; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 11 | $146.07 | $2.02 | — | $139.65 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $1750.41; owner union_hot_n4_h1 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $139.65 | ▼ close $11,267.07 vs 09:30 $11,439.74 (session -148.58) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $139.65 | ▼ 09:30 equity $11,252.02 vs yday $11,267.07 (-15.05) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 111 | $9.50 | $2.35 | $+29.74 | $1,191.80 | ▲ +29.74 after sell → book $11,249.67; vs 09:30 mark -2.35 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 7 | $134.10 | $2.03 | $-74.60 | $2,128.47 | ▼ -74.60 after sell → book $11,247.64; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 11 | $148.03 | $2.05 | $+17.49 | $3,754.76 | ▲ +17.49 after sell → book $11,245.60; vs 09:30 mark -2.04 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,754.76 | ▼ close $11,231.50 vs 09:30 $11,252.02 (session -14.10) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,754.76 | ▲ 09:30 equity $11,304.46 vs yday $11,231.50 (+72.96) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 20 | $23.94 | $2.07 | $-0.72 | $4,231.49 | ▼ -0.72 after sell → book $11,302.39; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 44 | $10.42 | $2.14 | $-28.90 | $4,687.82 | ▼ -28.90 after sell → book $11,300.24; vs 09:30 mark -2.15 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `INSP` | 7 | $63.00 | $2.03 | $+8.63 | $5,126.79 | ▲ +8.63 after sell → book $11,298.21; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 58 | $8.25 | $2.18 | $-10.15 | $5,603.11 | ▼ -10.15 after sell → book $11,296.03; vs 09:30 mark -2.18 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 98 | $4.64 | $2.31 | $-33.99 | $6,055.52 | ▼ -33.99 after sell → book $11,293.72; vs 09:30 mark -2.31 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `HCA` | 1 | $418.43 | $2.01 | $-12.55 | $6,471.94 | ▼ -12.55 after sell → book $11,291.71; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 125 | $13.04 | $2.40 | $-124.76 | $8,099.54 | ▼ -124.76 after sell → book $11,289.31; vs 09:30 mark -2.40 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,099.54 | ▼ close $11,243.09 vs 09:30 $11,304.46 (session -46.22) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,099.54 | ▼ 09:30 equity $11,213.17 vs yday $11,243.09 (-29.92) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,099.54 | ▼ close $11,179.17 vs 09:30 $11,213.17 (session -34.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,099.54 | ▲ 09:30 equity $11,203.34 vs yday $11,179.17 (+24.17) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 24 | $42.43 | $2.08 | $+19.62 | $9,115.78 | ▲ +19.62 after sell → book $11,201.26; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 71 | $15.45 | $2.22 | $+68.70 | $10,210.50 | ▲ +68.70 after sell → book $11,199.03; vs 09:30 mark -2.23 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 397 | $2.49 | $5.20 | $-53.99 | $11,193.83 | ▼ -53.99 after sell → book $11,193.83; vs 09:30 mark -5.20 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 786 | $1.78 | $10.14 | — | $9,784.61 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $1399.23; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 76 | $18.40 | $2.22 | — | $8,384.00 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $1399.23; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 102 | $13.71 | $2.30 | — | $6,983.28 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $1399.23; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 58 | $23.88 | $2.16 | — | $5,596.08 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $1399.23; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 21 | $52.88 | $2.05 | — | $4,483.54 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; combo leftover $1119.22; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 26 | $42.93 | $2.07 | — | $3,365.29 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; combo leftover $1119.22; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 308 | $3.63 | $3.97 | — | $2,243.28 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; combo leftover $1119.22; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 139 | $8.03 | $2.41 | — | $1,124.70 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; combo leftover $1119.22; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 8 | $132.45 | $2.01 | — | $63.09 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; combo leftover $1119.22; owner flatten_h5 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.09 | ▼ close $10,764.55 vs 09:30 $11,203.34 (session -399.95) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.09 | ▲ 09:30 equity $10,782.31 vs yday $10,764.55 (+17.76) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 76 | $18.15 | $2.24 | $-23.46 | $1,440.25 | ▼ -23.46 after sell → book $10,780.07; vs 09:30 mark -2.24 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 102 | $13.89 | $2.32 | $+13.74 | $2,854.70 | ▲ +13.74 after sell → book $10,777.74; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 58 | $23.84 | $2.19 | $-6.67 | $4,235.24 | ▼ -6.67 after sell → book $10,775.56; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 28 | $25.18 | $2.07 | — | $3,528.13 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $705.87; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 89 | $7.87 | $2.26 | — | $2,825.44 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $705.87; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 121 | $5.79 | $2.35 | — | $2,122.50 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $705.87; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 140 | $2.52 | $2.41 | — | $1,767.29 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; combo leftover $353.75; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 52 | $6.71 | $2.15 | — | $1,416.22 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; combo leftover $353.75; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 186 | $1.90 | $2.55 | — | $1,060.27 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; combo leftover $353.75; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 74 | $4.78 | $2.21 | — | $704.34 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; combo leftover $353.75; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 222 | $1.59 | $2.86 | — | $348.50 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; combo leftover $353.75; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 30 | $11.31 | $2.08 | — | $7.12 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; combo leftover $353.75; owner flatten_h5 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.12 | ▲ close $11,039.17 vs 09:30 $10,782.31 (session +284.55) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.12 | ▼ 09:30 equity $10,896.63 vs yday $11,039.17 (-142.54) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 786 | $1.56 | $10.28 | $-189.41 | $1,226.93 | ▼ -189.41 after sell → book $10,886.35; vs 09:30 mark -10.28 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 28 | $26.44 | $2.09 | $+31.11 | $1,965.15 | ▲ +31.11 after sell → book $10,884.25; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 89 | $7.76 | $2.28 | $-14.33 | $2,653.51 | ▼ -14.33 after sell → book $10,881.97; vs 09:30 mark -2.28 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 121 | $5.81 | $2.38 | $-2.32 | $3,354.14 | ▼ -2.32 after sell → book $10,879.59; vs 09:30 mark -2.38 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,354.14 | ▼ close $10,764.49 vs 09:30 $10,896.63 (session -115.10) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,354.14 | ▼ 09:30 equity $10,724.45 vs yday $10,764.49 (-40.04) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,354.14 | ▼ close $10,485.21 vs 09:30 $10,724.45 (session -239.24) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,354.14 | ▼ 09:30 equity $10,400.74 vs yday $10,485.21 (-84.47) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,354.14 | ▼ close $10,277.44 vs 09:30 $10,400.74 (session -123.31) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,354.14 | ▲ 09:30 equity $10,350.53 vs yday $10,277.44 (+73.09) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 21 | $53.53 | $2.07 | $+9.52 | $4,476.19 | ▲ +9.52 after sell → book $10,348.45; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 26 | $41.30 | $2.09 | $-46.54 | $5,547.91 | ▼ -46.54 after sell → book $10,346.37; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 308 | $2.77 | $4.03 | $-272.89 | $6,397.03 | ▼ -272.89 after sell → book $10,342.33; vs 09:30 mark -4.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 139 | $7.70 | $2.44 | $-50.72 | $7,464.89 | ▼ -50.72 after sell → book $10,339.89; vs 09:30 mark -2.44 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 8 | $122.40 | $2.03 | $-84.45 | $8,442.06 | ▼ -84.45 after sell → book $10,337.86; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 390 | $2.70 | $5.03 | — | $7,384.03 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $1055.26; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 214 | $4.91 | $2.76 | — | $6,330.53 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $1055.26; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 171 | $6.16 | $2.50 | — | $5,274.66 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; combo leftover $1055.26; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 337 | $3.13 | $4.35 | — | $4,215.51 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; combo leftover $1055.26; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 43 | $16.28 | $2.12 | — | $3,513.35 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; combo leftover $702.58; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 257 | $2.73 | $3.32 | — | $2,808.42 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; combo leftover $702.58; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 3 | $206.84 | $2.00 | — | $2,185.90 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; combo leftover $702.58; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 4 | $164.43 | $2.00 | — | $1,526.18 | — | baseline list, no extra gate; list flatten,earn_react; wish-list (live io HOLD — not a ticket); ⚪; ret5=+4.9; combo leftover $702.58; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 4 | $157.78 | $2.00 | — | $893.06 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; combo leftover $702.58; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 12 | $56.09 | $2.03 | — | $217.95 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; combo leftover $702.58; owner flatten_h5 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $217.95 | ▲ close $10,402.99 vs 09:30 $10,350.53 (session +93.24) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $217.95 | ▼ 09:30 equity $10,316.78 vs yday $10,402.99 (-86.21) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 140 | $2.15 | $2.44 | $-56.65 | $516.51 | ▼ -56.65 after sell → book $10,314.33; vs 09:30 mark -2.45 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 52 | $5.93 | $2.17 | $-44.87 | $822.70 | ▼ -44.87 after sell → book $10,312.17; vs 09:30 mark -2.16 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 186 | $1.72 | $2.59 | $-39.55 | $1,139.10 | ▼ -39.55 after sell → book $10,309.58; vs 09:30 mark -2.59 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 74 | $4.13 | $2.23 | $-52.55 | $1,442.49 | ▼ -52.55 after sell → book $10,307.34; vs 09:30 mark -2.24 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 222 | $1.59 | $2.91 | $-5.77 | $1,792.56 | ▼ -5.77 after sell → book $10,304.43; vs 09:30 mark -2.91 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 30 | $10.73 | $2.10 | $-21.58 | $2,112.36 | ▼ -21.58 after sell → book $10,302.33; vs 09:30 mark -2.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 171 | $6.02 | $2.54 | $-28.98 | $3,139.24 | ▼ -28.98 after sell → book $10,299.79; vs 09:30 mark -2.54 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,139.24 | ▲ close $10,481.59 vs 09:30 $10,316.78 (session +181.80) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,139.24 | ▲ 09:30 equity $10,576.24 vs yday $10,481.59 (+94.65) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 214 | $5.11 | $2.81 | $+37.23 | $4,229.97 | ▲ +37.23 after sell → book $10,573.43; vs 09:30 mark -2.81 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 337 | $3.64 | $4.41 | $+163.11 | $5,452.24 | ▲ +163.11 after sell → book $10,569.02; vs 09:30 mark -4.41 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,452.24 | ▲ close $10,584.97 vs 09:30 $10,576.24 (session +15.95) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,452.24 | ▲ 09:30 equity $10,624.84 vs yday $10,584.97 (+39.87) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 504 | $1.80 | $6.50 | — | $4,538.54 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; combo leftover $908.71; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 39 | $23.29 | $2.11 | — | $3,628.12 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; combo leftover $908.71; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 62 | $14.62 | $2.18 | — | $2,719.50 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; combo leftover $908.71; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 2 | $270.89 | $2.00 | — | $2,175.73 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.0; combo leftover $679.88; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 8 | $77.12 | $2.01 | — | $1,556.75 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+7.2; combo leftover $679.88; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 47 | $14.31 | $2.13 | — | $882.05 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; combo leftover $679.88; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 18 | $36.46 | $2.04 | — | $223.73 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.9; combo leftover $679.88; owner flatten_h5 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $223.73 | ▼ close $10,583.49 vs 09:30 $10,624.84 (session -22.38) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $223.73 | ▲ 09:30 equity $10,725.94 vs yday $10,583.49 (+142.45) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 39 | $24.09 | $2.13 | $+26.97 | $1,161.11 | ▲ +26.97 after sell → book $10,723.81; vs 09:30 mark -2.13 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 62 | $13.77 | $2.20 | $-57.07 | $2,012.66 | ▼ -57.07 after sell → book $10,721.62; vs 09:30 mark -2.19 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 22 | $22.46 | $2.06 | — | $1,516.48 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.3; combo leftover $503.16; owner union_hot_n4_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 13 | $36.76 | $2.03 | — | $1,036.57 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; combo leftover $503.16; owner union_hot_n4_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 1 | $151.43 | $1.52 | — | $883.62 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.0; combo leftover $172.76; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 1 | $147.61 | $1.48 | — | $734.53 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+17.7; combo leftover $172.76; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 16 | $10.25 | $1.69 | — | $568.85 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; combo leftover $172.76; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 22 | $7.59 | $1.74 | — | $400.13 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.4; combo leftover $172.76; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 4 | $34.93 | $1.41 | — | $259.00 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.6; combo leftover $172.76; owner flatten_h5 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $259.00 | ▲ close $10,910.02 vs 09:30 $10,725.94 (session +200.32) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $259.00 | ▼ 09:30 equity $10,877.51 vs yday $10,910.02 (-32.51) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 43 | $16.93 | $2.14 | $+23.69 | $984.85 | ▲ +23.69 after sell → book $10,875.37; vs 09:30 mark -2.14 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 257 | $2.68 | $3.37 | $-19.53 | $1,670.24 | ▼ -19.53 after sell → book $10,872.00; vs 09:30 mark -3.37 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 3 | $197.76 | $2.02 | $-31.26 | $2,261.51 | ▼ -31.26 after sell → book $10,869.99; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `ORCL` | 4 | $150.47 | $2.02 | $-59.86 | $2,861.36 | ▼ -59.86 after sell → book $10,867.96; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 4 | $152.71 | $2.02 | $-24.30 | $3,470.18 | ▼ -24.30 after sell → book $10,865.94; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 12 | $55.80 | $2.05 | $-7.55 | $4,137.74 | ▼ -7.55 after sell → book $10,863.90; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 504 | $1.96 | $6.60 | $+67.54 | $5,118.98 | ▲ +67.54 after sell → book $10,857.30; vs 09:30 mark -6.60 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 22 | $21.30 | $2.08 | $-29.65 | $5,585.50 | ▼ -29.65 after sell → book $10,855.22; vs 09:30 mark -2.08 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 13 | $39.50 | $2.05 | $+31.54 | $6,096.95 | ▲ +31.54 after sell → book $10,853.17; vs 09:30 mark -2.05 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 34 | $29.32 | $2.09 | — | $5,097.98 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $1016.16; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 334 | $3.04 | $4.31 | — | $4,079.98 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $1016.16; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 12 | $81.40 | $2.03 | — | $3,101.16 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; combo leftover $1016.16; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 4 | $108.55 | $2.00 | — | $2,664.96 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; combo leftover $516.86; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 2 | $209.52 | $2.00 | — | $2,243.92 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; combo leftover $516.86; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 2 | $219.62 | $2.00 | — | $1,802.68 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; combo leftover $516.86; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 6 | $85.00 | $2.01 | — | $1,290.68 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; combo leftover $516.86; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 15 | $34.44 | $2.04 | — | $772.04 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; combo leftover $516.86; owner flatten_h5 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $772.04 | ▼ close $10,790.00 vs 09:30 $10,877.51 (session -44.71) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $772.04 | ▲ 09:30 equity $10,980.88 vs yday $10,790.00 (+190.88) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 390 | $3.55 | $5.11 | $+321.36 | $2,151.43 | ▲ +321.36 after sell → book $10,975.77; vs 09:30 mark -5.11 | union_hot_n4_h1: dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 34 | $29.43 | $2.11 | $-0.46 | $3,149.94 | ▼ -0.46 after sell → book $10,973.66; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 334 | $4.00 | $4.37 | $+313.63 | $4,481.57 | ▲ +313.63 after sell → book $10,969.29; vs 09:30 mark -4.37 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 12 | $79.08 | $2.05 | $-31.91 | $5,428.48 | ▼ -31.91 after sell → book $10,967.24; vs 09:30 mark -2.05 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 274 | $2.47 | $3.53 | — | $4,748.17 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; combo leftover $678.56; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 40 | $16.91 | $2.11 | — | $4,069.66 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $678.56; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 411 | $1.65 | $5.30 | — | $3,386.21 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; combo leftover $678.56; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 58 | $11.67 | $2.16 | — | $2,707.18 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; combo leftover $678.56; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 3 | $157.87 | $2.00 | — | $2,231.57 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; combo leftover $541.44; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 1 | $386.20 | $1.99 | — | $1,843.38 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-5.8; combo leftover $541.44; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 6 | $88.83 | $2.01 | — | $1,308.39 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; combo leftover $541.44; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 40 | $13.47 | $2.11 | — | $767.48 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; combo leftover $541.44; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 135 | $4.00 | $2.40 | — | $225.09 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; combo leftover $541.44; owner flatten_h5 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $225.09 | ▲ close $10,962.61 vs 09:30 $10,980.88 (session +18.98) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $225.09 | ▼ 09:30 equity $10,945.98 vs yday $10,962.61 (-16.63) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 4 | $9.11 | $0.38 | — | $188.27 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+44.4; combo leftover $37.51; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 5 | $7.23 | $0.38 | — | $151.74 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+36.6; combo leftover $37.51; owner union_hot_n4_h1 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.74 | ▲ close $10,981.05 vs 09:30 $10,945.98 (session +35.82) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.74 | ▲ 09:30 equity $11,094.83 vs yday $10,981.05 (+113.78) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `IQV` | 2 | $270.66 | $2.02 | $-4.47 | $691.05 | ▼ -4.47 after sell → book $11,092.81; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 8 | $73.61 | $2.03 | $-32.13 | $1,277.89 | ▼ -32.13 after sell → book $11,090.78; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 47 | $13.12 | $2.15 | $-60.21 | $1,892.38 | ▼ -60.21 after sell → book $11,088.63; vs 09:30 mark -2.15 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 18 | $38.04 | $2.06 | $+24.33 | $2,575.04 | ▲ +24.33 after sell → book $11,086.56; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 40 | $16.92 | $2.13 | $-3.84 | $3,249.71 | ▼ -3.84 after sell → book $11,084.43; vs 09:30 mark -2.13 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 411 | $1.41 | $5.38 | $-109.32 | $3,823.84 | ▼ -109.32 after sell → book $11,079.05; vs 09:30 mark -5.38 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 58 | $12.80 | $2.18 | $+61.19 | $4,564.05 | ▲ +61.19 after sell → book $11,076.87; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 4 | $8.39 | $0.37 | $-3.62 | $4,597.25 | ▼ -3.62 after sell → book $11,076.50; vs 09:30 mark -0.37 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 5 | $6.83 | $0.38 | $-2.75 | $4,631.02 | ▼ -2.75 after sell → book $11,076.12; vs 09:30 mark -0.38 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 285 | $2.70 | $3.68 | — | $3,857.84 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $771.84; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 18 | $41.76 | $2.04 | — | $3,104.12 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $771.84; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 171 | $4.49 | $2.50 | — | $2,333.83 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; combo leftover $771.84; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 3 | $116.85 | $2.00 | — | $1,981.28 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; combo leftover $466.77; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 16 | $27.79 | $2.04 | — | $1,534.60 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; combo leftover $466.77; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 47 | $9.81 | $2.13 | — | $1,071.40 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; combo leftover $466.77; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 23 | $20.25 | $2.06 | — | $603.59 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; combo leftover $466.77; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 22 | $20.65 | $2.06 | — | $147.23 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; combo leftover $466.77; owner flatten_h5 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $147.23 | ▼ close $10,837.57 vs 09:30 $11,094.83 (session -220.05) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $147.23 | ▼ 09:30 equity $10,719.73 vs yday $10,837.57 (-117.84) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `TWST` | 1 | $157.72 | $1.60 | $+3.17 | $303.35 | ▲ +3.17 after sell → book $10,718.13; vs 09:30 mark -1.60 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `RVTY` | 1 | $141.79 | $1.44 | $-8.74 | $443.70 | ▼ -8.74 after sell → book $10,716.69; vs 09:30 mark -1.44 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 16 | $10.39 | $1.73 | $-1.18 | $608.21 | ▼ -1.18 after sell → book $10,714.96; vs 09:30 mark -1.73 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 22 | $7.38 | $1.71 | $-8.07 | $768.86 | ▼ -8.07 after sell → book $10,713.25; vs 09:30 mark -1.71 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMN` | 4 | $33.82 | $1.38 | $-7.23 | $902.76 | ▼ -7.23 after sell → book $10,711.86; vs 09:30 mark -1.39 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 274 | $2.68 | $3.59 | $+50.42 | $1,633.49 | ▲ +50.42 after sell → book $10,708.27; vs 09:30 mark -3.59 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 18 | $36.02 | $2.06 | $-107.34 | $2,279.87 | ▼ -107.34 after sell → book $10,706.21; vs 09:30 mark -2.06 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 171 | $3.92 | $2.54 | $-101.66 | $2,948.51 | ▼ -101.66 after sell → book $10,703.67; vs 09:30 mark -2.54 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,948.51 | ▲ close $11,429.10 vs 09:30 $10,719.73 (session +725.43) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,225.78 | ▲ 09:30 equity $10,084.05 vs yday $9,859.75 (+224.30) | 09:30 open · cash $2,225.78 (unchanged overnight, no fees) · equity $10,084.05 vs prior close $9,859.75 (+224.30) | — |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 12 | $29.76 | $2.03 | — | $1,866.63 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; combo leftover $370.96; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 22 | $16.21 | $2.06 | — | $1,507.96 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $370.96; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 23 | $15.58 | $2.06 | — | $1,147.53 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; combo leftover $370.96; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 9 | $38.51 | $2.02 | — | $798.93 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.7; combo leftover $382.51; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 50 | $7.65 | $2.14 | — | $414.29 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; combo leftover $382.51; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $414.29 | ▼ close $9,888.27 vs 09:30 $10,084.05 (session -185.49) | 16:00 close · cash $414.29 · equity $9,888.27 vs 09:30 $10,084.05 (-195.78; session marks -185.49) · 25 name(s) marked open→close (per-name table). A×2 09:30 $171.98 → close $172.79 +1.62; ADMA×50 09:30 $9.52 → close $9.52 +0.00; ARQT×17 09:30 $26.27 → close $26.27 +0.00; DLO×15 09:30 $13.88 → close $13.88 +0.00; DXCM×4 09:30 $87.47 → close $87.47 +0.00; ECO×4 09:30 $78.22 → close $78.22 +0.00; EL×2 09:30 $95.37 → close $95.37 +0.00; FIVN×10 09:30 $36.66 → close $36.66 -0.00; FTRE×24 09:30 $20.02 → close $20.02 +0.00; GLND×302 09:30 $6.06 → close $5.54 -157.04; GNRC×1 09:30 $198.05 → close $198.05 +0.00; HALO×4 09:30 $115.36 → close $113.90 -5.84; MGTX×28 09:30 $11.05 → close $11.05 +0.00; MKC×4 09:30 $47.82 → close $47.82 -0.00; OMER×23 09:30 $20.61 → close $20.08 -12.19; PACS×5 09:30 $41.46 → close $41.46 -0.00; RBRK×3 09:30 $113.80 → close $113.80 +0.00; TDC×7 09:30 $29.46 → close $29.46 -0.00; USFD×2 09:30 $93.82 → close $93.82 -0.00; VICR×1 09:30 $276.06 → close $276.06 -0.00; TJGC×12 09:30 $29.76 → close $26.24 -42.24; SECZ×22 09:30 $16.21 → close $15.96 -5.50; USDE×23 09:30 $15.58 → close $17.25 +38.38; BLFS×9 09:30 $38.51 → close $38.49 -0.18; MRVI×50 09:30 $7.65 → close $7.60 -2.50 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `SLS` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TLN` | cash | leftover split 322.55 < 1 share @ 359.83 |
| 2026-08-14 | `DAVE` | cash | leftover split 322.55 < 1 share @ 330.91 |
| 2026-08-17 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `SLS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `VST` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `NRG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `SLG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `MARA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `LDI` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `BTBT` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `SLS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `VST` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `NRG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `SLG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `MARA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `LDI` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `BTBT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `DVN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `EOG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `FANG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `TMC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `TGB` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `ELF` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `DNN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `HNST` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-19 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `SLS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `VST` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `NRG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `SLG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `MARA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `LDI` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `BTBT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `DVN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `EOG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `FANG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `TMC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `TGB` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `ELF` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `DNN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `HNST` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-20 | `VST` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `NRG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `SLG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `MARA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `LDI` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `BTBT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
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
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
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
| 2026-08-27 | `AU` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `AEM` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `CRSP` | min_hold | flatten_h5: dropped but min-hold 4/5 |
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
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-09-01 | `RRC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `CRK` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `SLI` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-02 | `RRC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-02 | `CRK` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-02 | `SLI` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
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
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
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
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
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
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new long flatten_h5 |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new long flatten_h5 |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new long flatten_h5 |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new long flatten_h5 |
| 2026-09-11 | `ALEC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `BHC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `BMEA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `OABI` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `OPK` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `VIR` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-14 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `OVID` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `SANM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `ORCL` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `COHU` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `GPRO` | hard_red | hard-red S=-11.00 sit; no new long union_hot_n4_h1 |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-15 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `OVID` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `SANM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `ORCL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `NVT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `COHU` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `GPRO` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-16 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-16 | `OVID` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-16 | `SANM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-16 | `ORCL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-16 | `NVT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-16 | `COHU` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-17 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `OVID` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `SANM` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `ORCL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `NVT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `COHU` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `IQV` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `ILMN` | cash | leftover split 172.76 < 1 share @ 233.85 |
| 2026-09-18 | `IQV` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `TWST` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `PGEN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `AMN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `DELL` | cash | leftover split 516.86 < 1 share @ 593.15 |
| 2026-09-21 | `IQV` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `TWST` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-21 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-21 | `AMN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-21 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `VICR` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `ECO` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `IQV` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `TWST` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `PGEN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `AMN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `VICR` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `ECO` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LVWR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `A` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `HUM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `DXCM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `MGTX` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 25.29 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-23 | `TWST` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `AMN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `VICR` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `ECO` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `HUM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-23 | `MGTX` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-23 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-24 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `VICR` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `ECO` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `A` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `HUM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `DXCM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `MGTX` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `HALO` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `ARQT` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `ADMA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `FTRE` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `OMER` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RBRK` | 4 | 2026-09-18 @ $108.55 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; combo leftover $516.86; owner flatten_h5 |
| `GNRC` | 2 | 2026-09-18 @ $209.52 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; combo leftover $516.86; owner flatten_h5 |
| `VICR` | 2 | 2026-09-18 @ $219.62 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; combo leftover $516.86; owner flatten_h5 |
| `ECO` | 6 | 2026-09-18 @ $85.00 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; combo leftover $516.86; owner flatten_h5 |
| `FIVN` | 15 | 2026-09-18 @ $34.44 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; combo leftover $516.86; owner flatten_h5 |
| `A` | 3 | 2026-09-21 @ $157.87 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; combo leftover $541.44; owner flatten_h5 |
| `HUM` | 1 | 2026-09-21 @ $386.20 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-5.8; combo leftover $541.44; owner flatten_h5 |
| `DXCM` | 6 | 2026-09-21 @ $88.83 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; combo leftover $541.44; owner flatten_h5 |
| `MGTX` | 40 | 2026-09-21 @ $13.47 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; combo leftover $541.44; owner flatten_h5 |
| `CYPH` | 135 | 2026-09-21 @ $4.00 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; combo leftover $541.44; owner flatten_h5 |
| `GLND` | 285 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $771.84; owner union_hot_n4_h1 |
| `HALO` | 3 | 2026-09-23 @ $116.85 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; combo leftover $466.77; owner flatten_h5 |
| `ARQT` | 16 | 2026-09-23 @ $27.79 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; combo leftover $466.77; owner flatten_h5 |
| `ADMA` | 47 | 2026-09-23 @ $9.81 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; combo leftover $466.77; owner flatten_h5 |
| `FTRE` | 23 | 2026-09-23 @ $20.25 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; combo leftover $466.77; owner flatten_h5 |
| `OMER` | 22 | 2026-09-23 @ $20.65 | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; combo leftover $466.77; owner flatten_h5 |
