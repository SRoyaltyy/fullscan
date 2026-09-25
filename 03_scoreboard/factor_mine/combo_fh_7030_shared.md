# Factor mine action — `combo_fh_7030_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared flatten_h5/union_hot_n4_h1 w=0.7,0.3 net=priority

Cash book **-6.50%** ($9,350) · signal-only (no cash/fees) was —. Starts YES **17/30**. Fills 266 · skips 411 · realized $+280.04.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: flatten_h5 70%, union_hot_n4_h1 30%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: flatten_h5 70%, union_hot_n4_h1 30%.
- Member: flatten_h5 (70% · long · hold 5).
- Member: union_hot_n4_h1 (30% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $418.54.

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
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 16 | $45.98 | $2.04 | — | $9,262.28 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; combo leftover $750.00; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 14 | $50.62 | $2.03 | — | $8,551.53 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; combo leftover $750.00; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 925 | $0.81 | $10.27 | — | $7,792.01 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+13.2; combo leftover $750.00; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 32 | $23.33 | $2.09 | — | $7,043.36 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; combo leftover $750.00; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 29 | $59.80 | $2.08 | — | $5,307.08 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; combo leftover $1760.84; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 35 | $49.70 | $2.10 | — | $3,565.49 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; combo leftover $1760.84; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 150 | $11.70 | $2.44 | — | $1,808.05 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; combo leftover $1760.84; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 59 | $29.74 | $2.17 | — | $51.22 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; combo leftover $1760.84; owner flatten_h5 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.22 | ▲ close $10,080.72 vs 09:30 $10,000.00 (session +105.93) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.22 | ▲ 09:30 equity $10,088.56 vs yday $10,080.72 (+7.84) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 16 | $44.09 | $2.06 | $-34.34 | $754.60 | ▼ -34.34 after sell → book $10,086.50; vs 09:30 mark -2.06 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 14 | $55.29 | $2.05 | $+61.25 | $1,526.61 | ▲ +61.25 after sell → book $10,084.45; vs 09:30 mark -2.05 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 925 | $0.93 | $11.54 | $+89.19 | $2,375.32 | ▲ +89.19 after sell → book $10,072.91; vs 09:30 mark -11.54 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 32 | $22.92 | $2.11 | $-17.31 | $3,106.66 | ▼ -17.31 after sell → book $10,070.81; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 9 | $24.68 | $2.02 | — | $2,882.52 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; combo leftover $233.00; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 11 | $19.57 | $2.02 | — | $2,665.23 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $233.00; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 105 | $2.20 | $2.31 | — | $2,431.92 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; combo leftover $233.00; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 20 | $11.12 | $2.05 | — | $2,207.47 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $233.00; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 1 | $146.90 | $1.47 | — | $2,059.10 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+3.6; combo leftover $275.93; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 2 | $120.00 | $2.00 | — | $1,817.10 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+0.6; combo leftover $275.93; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 4 | $57.61 | $2.00 | — | $1,584.66 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.7; combo leftover $275.93; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 30 | $9.01 | $2.08 | — | $1,312.28 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; combo leftover $275.93; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 294 | $0.94 | $3.64 | — | $1,033.17 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; combo leftover $275.93; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 183 | $1.50 | $2.54 | — | $756.13 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; combo leftover $275.93; owner flatten_h5 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $756.13 | ▲ close $10,147.93 vs 09:30 $10,088.56 (session +99.25) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $756.13 | ▼ 09:30 equity $10,124.43 vs yday $10,147.93 (-23.50) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 9 | $24.83 | $2.04 | $-2.70 | $977.56 | ▼ -2.70 after sell → book $10,122.39; vs 09:30 mark -2.04 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 11 | $19.57 | $2.04 | $-4.07 | $1,190.79 | ▼ -4.07 after sell → book $10,120.35; vs 09:30 mark -2.04 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 105 | $2.08 | $2.33 | $-16.71 | $1,407.38 | ▼ -16.71 after sell → book $10,118.02; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 20 | $9.57 | $1.99 | $-35.04 | $1,596.79 | ▼ -35.04 after sell → book $10,116.02; vs 09:30 mark -2.00 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 28 | $4.19 | $1.26 | — | $1,478.21 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $119.76; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 17 | $6.87 | $1.22 | — | $1,360.20 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $119.76; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 8 | $13.64 | $1.12 | — | $1,249.96 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $119.76; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 2 | $41.23 | $0.83 | — | $1,166.67 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $119.76; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 3 | $46.18 | $1.39 | — | $1,026.74 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+6.7; combo leftover $145.83; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 1 | $142.77 | $1.43 | — | $882.54 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+5.8; combo leftover $145.83; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 36 | $4.05 | $1.57 | — | $735.17 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; combo leftover $145.83; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 17 | $8.46 | $1.49 | — | $589.86 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; combo leftover $145.83; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 1 | $90.54 | $0.91 | — | $498.41 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-7.2; combo leftover $145.83; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 45 | $3.24 | $1.59 | — | $351.02 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; combo leftover $145.83; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 30 | $4.81 | $1.53 | — | $205.19 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; combo leftover $145.83; owner flatten_h5 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $205.19 | ▲ close $10,154.31 vs 09:30 $10,124.43 (session +52.62) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $205.19 | ▼ 09:30 equity $10,014.98 vs yday $10,154.31 (-139.33) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 28 | $3.94 | $1.21 | $-9.46 | $314.30 | ▼ -9.46 after sell → book $10,013.77; vs 09:30 mark -1.21 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 8 | $13.31 | $1.11 | $-4.86 | $419.67 | ▼ -4.86 after sell → book $10,012.66; vs 09:30 mark -1.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 2 | $41.50 | $0.86 | $-1.15 | $501.82 | ▼ -1.15 after sell → book $10,011.81; vs 09:30 mark -0.85 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $501.82 | ▲ close $10,025.38 vs 09:30 $10,014.98 (session +13.57) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $501.82 | ▲ 09:30 equity $10,176.02 vs yday $10,025.38 (+150.64) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 17 | $7.19 | $1.29 | $+2.93 | $622.75 | ▲ +2.93 after sell → book $10,174.72; vs 09:30 mark -1.30 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $622.75 | ▲ close $10,458.10 vs 09:30 $10,176.02 (session +283.37) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $622.75 | ▼ 09:30 equity $10,429.55 vs yday $10,458.10 (-28.55) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 29 | $58.64 | $2.10 | $-37.82 | $2,321.21 | ▼ -37.82 after sell → book $10,427.45; vs 09:30 mark -2.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 35 | $51.65 | $2.12 | $+64.04 | $4,126.84 | ▲ +64.04 after sell → book $10,425.33; vs 09:30 mark -2.12 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 150 | $13.84 | $2.48 | $+316.08 | $6,200.36 | ▲ +316.08 after sell → book $10,422.85; vs 09:30 mark -2.48 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 59 | $30.66 | $2.19 | $+49.92 | $8,007.11 | ▲ +49.92 after sell → book $10,420.66; vs 09:30 mark -2.19 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 3 | $150.14 | $2.00 | — | $7,554.69 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $600.53; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 522 | $1.15 | $6.73 | — | $6,947.66 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $600.53; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 50 | $11.81 | $2.14 | — | $6,354.77 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $600.53; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 438 | $1.37 | $5.65 | — | $5,749.06 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $600.53; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 34 | $20.55 | $2.09 | — | $5,048.27 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; combo leftover $718.63; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 7 | $91.01 | $2.01 | — | $4,409.18 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; combo leftover $718.63; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 34 | $20.65 | $2.09 | — | $3,704.99 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; combo leftover $718.63; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 124 | $5.77 | $2.36 | — | $2,987.15 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; combo leftover $718.63; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 36 | $19.63 | $2.10 | — | $2,278.37 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; combo leftover $718.63; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 24 | $29.63 | $2.06 | — | $1,565.19 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; combo leftover $718.63; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 410 | $1.75 | $5.29 | — | $842.40 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; combo leftover $718.63; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $262.24 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; combo leftover $718.63; owner flatten_h5 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $262.24 | ▲ close $10,551.23 vs 09:30 $10,429.55 (session +167.11) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $262.24 | ▲ 09:30 equity $10,824.77 vs yday $10,551.23 (+273.54) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `VST` | 1 | $139.99 | $1.42 | $-9.80 | $400.81 | ▼ -9.80 after sell → book $10,823.34; vs 09:30 mark -1.43 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `NRG` | 2 | $116.58 | $2.02 | $-10.85 | $631.95 | ▼ -10.85 after sell → book $10,821.33; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `SLG` | 4 | $58.63 | $2.02 | $+0.06 | $864.45 | ▲ +0.06 after sell → book $10,819.31; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 30 | $11.70 | $2.10 | $+76.52 | $1,213.35 | ▲ +76.52 after sell → book $10,817.21; vs 09:30 mark -2.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 294 | $0.87 | $3.49 | $-27.71 | $1,464.76 | ▼ -27.71 after sell → book $10,813.72; vs 09:30 mark -3.49 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 183 | $1.66 | $2.58 | $+24.16 | $1,765.96 | ▲ +24.16 after sell → book $10,811.14; vs 09:30 mark -2.58 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 50 | $11.57 | $2.16 | $-16.55 | $2,342.30 | ▼ -16.55 after sell → book $10,808.98; vs 09:30 mark -2.16 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 438 | $1.46 | $5.73 | $+28.04 | $2,976.04 | ▲ +28.04 after sell → book $10,803.24; vs 09:30 mark -5.74 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 99 | $4.49 | $2.29 | — | $2,529.25 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $446.41; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 65 | $6.81 | $2.19 | — | $2,084.41 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $446.41; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 2 | $119.43 | $2.00 | — | $1,843.56 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+21.1; combo leftover $297.77; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 17 | $17.20 | $2.04 | — | $1,549.12 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; combo leftover $297.77; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 1 | $216.30 | $1.99 | — | $1,330.82 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; combo leftover $297.77; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 26 | $11.13 | $2.07 | — | $1,039.37 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; combo leftover $297.77; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 120 | $2.47 | $2.35 | — | $740.62 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; combo leftover $297.77; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 154 | $1.93 | $2.45 | — | $440.95 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; combo leftover $297.77; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 4 | $59.72 | $2.00 | — | $200.07 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; combo leftover $297.77; owner flatten_h5 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $200.07 | ▲ close $10,907.16 vs 09:30 $10,824.77 (session +123.29) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $200.07 | ▲ 09:30 equity $11,262.53 vs yday $10,907.16 (+355.37) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `DVN` | 3 | $48.89 | $1.50 | $+5.24 | $345.24 | ▲ +5.24 after sell → book $11,261.03; vs 09:30 mark -1.50 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `EOG` | 1 | $152.07 | $1.54 | $+6.33 | $495.77 | ▲ +6.33 after sell → book $11,259.49; vs 09:30 mark -1.54 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 36 | $4.62 | $1.79 | $+17.34 | $660.48 | ▲ +17.34 after sell → book $11,257.70; vs 09:30 mark -1.79 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 17 | $9.26 | $1.65 | $+10.47 | $816.25 | ▲ +10.47 after sell → book $11,256.05; vs 09:30 mark -1.65 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `ELF` | 1 | $102.20 | $1.04 | $+9.71 | $917.41 | ▲ +9.71 after sell → book $11,255.01; vs 09:30 mark -1.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 45 | $3.50 | $1.73 | $+8.38 | $1,073.18 | ▲ +8.38 after sell → book $11,253.28; vs 09:30 mark -1.73 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 30 | $5.05 | $1.62 | $+4.04 | $1,223.05 | ▲ +4.04 after sell → book $11,251.65; vs 09:30 mark -1.63 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 3 | $142.70 | $2.02 | $-26.34 | $1,649.13 | ▼ -26.34 after sell → book $11,249.63; vs 09:30 mark -2.02 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 522 | $1.83 | $6.83 | $+341.40 | $2,597.56 | ▲ +341.40 after sell → book $11,242.80; vs 09:30 mark -6.83 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 99 | $4.32 | $2.31 | $-21.43 | $3,022.93 | ▼ -21.43 after sell → book $11,240.49; vs 09:30 mark -2.31 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 65 | $8.03 | $2.21 | $+74.91 | $3,542.67 | ▲ +74.91 after sell → book $11,238.28; vs 09:30 mark -2.21 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,542.67 | ▼ close $11,230.18 vs 09:30 $11,262.53 (session -8.10) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,542.67 | ▼ 09:30 equity $11,137.80 vs yday $11,230.18 (-92.38) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 11 | $24.11 | $2.02 | — | $3,275.44 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $265.70; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 170 | $1.56 | $2.50 | — | $3,007.74 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $265.70; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 65 | $4.07 | $2.19 | — | $2,741.01 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $265.70; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 13 | $19.04 | $2.03 | — | $2,491.46 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $265.70; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 17 | $23.77 | $2.04 | — | $2,085.33 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; combo leftover $415.24; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 37 | $10.98 | $2.10 | — | $1,676.97 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; combo leftover $415.24; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 6 | $61.19 | $2.01 | — | $1,307.82 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.4; combo leftover $415.24; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 49 | $8.35 | $2.14 | — | $896.53 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; combo leftover $415.24; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 84 | $4.94 | $2.24 | — | $479.33 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; combo leftover $415.24; owner flatten_h5 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $479.33 | ▲ close $11,551.81 vs 09:30 $11,137.80 (session +433.27) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $479.33 | ▼ 09:30 equity $11,378.11 vs yday $11,551.81 (-173.70) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 11 | $26.61 | $2.04 | $+23.43 | $770.00 | ▲ +23.43 after sell → book $11,376.07; vs 09:30 mark -2.04 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 170 | $1.60 | $2.54 | $+1.76 | $1,039.46 | ▲ +1.76 after sell → book $11,373.53; vs 09:30 mark -2.54 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 13 | $20.72 | $2.05 | $+17.76 | $1,306.77 | ▲ +17.76 after sell → book $11,371.48; vs 09:30 mark -2.05 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 9 | $14.11 | $1.30 | — | $1,178.48 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $130.68; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 22 | $5.81 | $1.34 | — | $1,049.32 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $130.68; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 11 | $11.59 | $1.31 | — | $920.57 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; combo leftover $130.68; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `HCA` | 2 | $427.50 | $2.00 | — | $63.58 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.1; combo leftover $920.57; owner flatten_h5 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.58 | ▼ close $11,299.44 vs 09:30 $11,378.11 (session -66.10) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.58 | ▲ 09:30 equity $11,313.00 vs yday $11,299.44 (+13.56) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 34 | $20.93 | $2.11 | $+8.72 | $773.09 | ▲ +8.72 after sell → book $11,310.89; vs 09:30 mark -2.11 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 7 | $95.52 | $2.03 | $+27.53 | $1,439.70 | ▲ +27.53 after sell → book $11,308.86; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 34 | $21.31 | $2.11 | $+18.24 | $2,162.12 | ▲ +18.24 after sell → book $11,306.74; vs 09:30 mark -2.12 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 124 | $5.49 | $2.39 | $-39.47 | $2,840.49 | ▼ -39.47 after sell → book $11,304.35; vs 09:30 mark -2.39 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 36 | $21.47 | $2.12 | $+62.02 | $3,611.29 | ▲ +62.02 after sell → book $11,302.23; vs 09:30 mark -2.12 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 24 | $32.32 | $2.08 | $+60.42 | $4,384.89 | ▲ +60.42 after sell → book $11,300.15; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 410 | $1.91 | $5.37 | $+54.94 | $5,162.62 | ▲ +54.94 after sell → book $11,294.78; vs 09:30 mark -5.37 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 4 | $155.89 | $2.02 | $+41.38 | $5,784.16 | ▲ +41.38 after sell → book $11,292.76; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 9 | $14.20 | $1.32 | $-1.81 | $5,910.64 | ▼ -1.81 after sell → book $11,291.44; vs 09:30 mark -1.32 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 22 | $6.50 | $1.52 | $+12.32 | $6,052.12 | ▲ +12.32 after sell → book $11,289.92; vs 09:30 mark -1.52 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 11 | $12.18 | $1.39 | $+3.84 | $6,184.71 | ▲ +3.84 after sell → book $11,288.53; vs 09:30 mark -1.39 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 67 | $9.19 | $2.19 | — | $5,566.79 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; combo leftover $618.47; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 4 | $144.18 | $2.00 | — | $4,988.06 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; combo leftover $618.47; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 33 | $18.50 | $2.09 | — | $4,375.48 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; combo leftover $618.47; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 35 | $41.44 | $2.10 | — | $2,922.98 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; combo leftover $1458.49; owner flatten_h5 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 101 | $14.42 | $2.29 | — | $1,464.27 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; combo leftover $1458.49; owner flatten_h5 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 560 | $2.60 | $7.22 | — | $1.04 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; combo leftover $1458.49; owner flatten_h5 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.04 | ▲ close $11,323.59 vs 09:30 $11,313.00 (session +52.96) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.04 | ▼ 09:30 equity $11,292.51 vs yday $11,323.59 (-31.08) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 2 | $119.19 | $2.02 | $-4.49 | $237.41 | ▼ -4.49 after sell → book $11,290.50; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 17 | $16.44 | $2.06 | $-17.02 | $514.83 | ▼ -17.02 after sell → book $11,288.44; vs 09:30 mark -2.06 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEM` | 1 | $216.31 | $2.01 | $-4.00 | $729.12 | ▼ -4.00 after sell → book $11,286.42; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 26 | $15.43 | $2.09 | $+107.64 | $1,128.22 | ▲ +107.64 after sell → book $11,284.34; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 120 | $2.35 | $2.38 | $-19.13 | $1,407.84 | ▼ -19.13 after sell → book $11,281.96; vs 09:30 mark -2.38 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 154 | $2.06 | $2.49 | $+15.08 | $1,722.59 | ▲ +15.08 after sell → book $11,279.47; vs 09:30 mark -2.49 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRSP` | 4 | $58.22 | $2.02 | $-10.02 | $1,953.45 | ▼ -10.02 after sell → book $11,277.45; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 65 | $3.69 | $2.21 | $-29.09 | $2,191.09 | ▼ -29.09 after sell → book $11,275.24; vs 09:30 mark -2.21 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 33 | $18.15 | $2.11 | $-15.75 | $2,787.93 | ▼ -15.75 after sell → book $11,273.13; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 99 | $14.00 | $2.29 | — | $1,399.64 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $1393.97; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $83.00 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $1393.97; owner union_hot_n4_h1 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.00 | ▼ close $11,091.38 vs 09:30 $11,292.51 (session -177.45) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.00 | ▲ 09:30 equity $11,111.08 vs yday $11,091.38 (+19.70) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 67 | $9.50 | $2.21 | $+16.37 | $717.29 | ▲ +16.37 after sell → book $11,108.87; vs 09:30 mark -2.21 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 4 | $134.10 | $2.02 | $-44.34 | $1,251.66 | ▼ -44.34 after sell → book $11,106.84; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $2,581.90 | ▲ +13.59 after sell → book $11,104.81; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,581.90 | ▼ close $11,101.14 vs 09:30 $11,111.08 (session -3.67) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,581.90 | ▲ 09:30 equity $11,235.17 vs yday $11,101.14 (+134.03) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 17 | $23.94 | $2.06 | $-1.21 | $2,986.81 | ▼ -1.21 after sell → book $11,233.10; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 37 | $10.42 | $2.12 | $-24.94 | $3,370.23 | ▼ -24.94 after sell → book $11,230.98; vs 09:30 mark -2.12 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `INSP` | 6 | $63.00 | $2.03 | $+6.82 | $3,746.21 | ▲ +6.82 after sell → book $11,228.96; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 49 | $8.25 | $2.16 | $-9.19 | $4,148.30 | ▼ -9.19 after sell → book $11,226.80; vs 09:30 mark -2.16 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 84 | $4.64 | $2.27 | $-29.71 | $4,535.79 | ▼ -29.71 after sell → book $11,224.53; vs 09:30 mark -2.27 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 99 | $13.04 | $2.31 | $-99.64 | $5,824.44 | ▼ -99.64 after sell → book $11,222.22; vs 09:30 mark -2.31 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,824.44 | ▼ close $11,148.02 vs 09:30 $11,235.17 (session -74.20) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,824.44 | ▼ 09:30 equity $11,102.96 vs yday $11,148.02 (-45.06) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `HCA` | 2 | $412.46 | $2.02 | $-34.09 | $6,647.34 | ▼ -34.09 after sell → book $11,100.94; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,647.34 | ▼ close $11,053.28 vs 09:30 $11,102.96 (session -47.66) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,647.34 | ▲ 09:30 equity $11,087.24 vs yday $11,053.28 (+33.96) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 35 | $42.43 | $2.12 | $+30.44 | $8,130.28 | ▲ +30.44 after sell → book $11,085.13; vs 09:30 mark -2.11 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 101 | $15.45 | $2.32 | $+99.41 | $9,688.40 | ▲ +99.41 after sell → book $11,082.80; vs 09:30 mark -2.33 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 560 | $2.49 | $7.33 | $-76.15 | $11,075.48 | ▼ -76.15 after sell → book $11,075.48; vs 09:30 mark -7.32 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 466 | $1.78 | $6.01 | — | $10,239.98 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $830.66; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 45 | $18.40 | $2.12 | — | $9,409.86 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $830.66; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 60 | $13.71 | $2.17 | — | $8,585.09 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $830.66; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 34 | $23.88 | $2.09 | — | $7,771.08 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $830.66; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 29 | $52.88 | $2.08 | — | $6,235.48 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; combo leftover $1554.22; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 36 | $42.93 | $2.10 | — | $4,687.90 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; combo leftover $1554.22; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 428 | $3.63 | $5.52 | — | $3,128.74 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; combo leftover $1554.22; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 193 | $8.03 | $2.57 | — | $1,576.38 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; combo leftover $1554.22; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 11 | $132.45 | $2.02 | — | $117.41 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; combo leftover $1554.22; owner flatten_h5 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $117.41 | ▼ close $10,728.92 vs 09:30 $11,087.24 (session -319.87) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $117.41 | ▼ 09:30 equity $10,708.51 vs yday $10,728.92 (-20.41) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 45 | $18.15 | $2.15 | $-15.52 | $932.01 | ▼ -15.52 after sell → book $10,706.36; vs 09:30 mark -2.15 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 60 | $13.89 | $2.19 | $+6.44 | $1,763.22 | ▲ +6.44 after sell → book $10,704.17; vs 09:30 mark -2.19 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 34 | $23.84 | $2.11 | $-5.56 | $2,571.67 | ▼ -5.56 after sell → book $10,702.06; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 10 | $25.18 | $2.02 | — | $2,317.85 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $257.17; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 32 | $7.87 | $2.09 | — | $2,063.93 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $257.17; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 44 | $5.79 | $2.12 | — | $1,807.04 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $257.17; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 119 | $2.52 | $2.35 | — | $1,504.82 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; combo leftover $301.17; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 44 | $6.71 | $2.12 | — | $1,207.46 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; combo leftover $301.17; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 158 | $1.90 | $2.46 | — | $904.79 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; combo leftover $301.17; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 63 | $4.78 | $2.18 | — | $601.47 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; combo leftover $301.17; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 189 | $1.59 | $2.56 | — | $298.41 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; combo leftover $301.17; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 26 | $11.31 | $2.07 | — | $2.28 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; combo leftover $301.17; owner flatten_h5 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.28 | ▲ close $10,874.06 vs 09:30 $10,708.51 (session +191.96) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.28 | ▼ 09:30 equity $10,817.25 vs yday $10,874.06 (-56.81) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 466 | $1.56 | $6.10 | $-112.30 | $725.47 | ▼ -112.30 after sell → book $10,811.15; vs 09:30 mark -6.10 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 10 | $26.44 | $2.04 | $+8.54 | $987.83 | ▲ +8.54 after sell → book $10,809.11; vs 09:30 mark -2.04 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 32 | $7.76 | $2.11 | $-7.71 | $1,234.04 | ▼ -7.71 after sell → book $10,807.00; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 44 | $5.81 | $2.14 | $-3.38 | $1,487.54 | ▼ -3.38 after sell → book $10,804.86; vs 09:30 mark -2.14 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,487.54 | ▼ close $10,657.21 vs 09:30 $10,817.25 (session -147.65) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,487.54 | ▼ 09:30 equity $10,606.84 vs yday $10,657.21 (-50.37) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,487.54 | ▼ close $10,321.73 vs 09:30 $10,606.84 (session -285.12) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,487.54 | ▼ 09:30 equity $10,220.03 vs yday $10,321.73 (-101.70) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,487.54 | ▼ close $10,072.58 vs 09:30 $10,220.03 (session -147.45) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,487.54 | ▲ 09:30 equity $10,161.04 vs yday $10,072.58 (+88.46) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 29 | $53.53 | $2.10 | $+14.67 | $3,037.81 | ▲ +14.67 after sell → book $10,158.94; vs 09:30 mark -2.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 36 | $41.30 | $2.12 | $-62.90 | $4,522.49 | ▼ -62.90 after sell → book $10,156.82; vs 09:30 mark -2.12 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 428 | $2.77 | $5.60 | $-379.20 | $5,702.45 | ▼ -379.20 after sell → book $10,151.22; vs 09:30 mark -5.60 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 193 | $7.70 | $2.61 | $-68.87 | $7,185.94 | ▼ -68.87 after sell → book $10,148.61; vs 09:30 mark -2.61 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 11 | $122.40 | $2.04 | $-114.62 | $8,530.29 | ▼ -114.62 after sell → book $10,146.56; vs 09:30 mark -2.05 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 236 | $2.70 | $3.04 | — | $7,890.05 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $639.77; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 130 | $4.91 | $2.38 | — | $7,249.37 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $639.77; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 103 | $6.16 | $2.30 | — | $6,612.59 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; combo leftover $639.77; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 204 | $3.13 | $2.63 | — | $5,971.44 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; combo leftover $639.77; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 61 | $16.28 | $2.17 | — | $4,976.18 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; combo leftover $995.24; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 364 | $2.73 | $4.70 | — | $3,977.77 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; combo leftover $995.24; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 4 | $206.84 | $2.00 | — | $3,148.41 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; combo leftover $995.24; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 6 | $164.43 | $2.01 | — | $2,159.82 | — | baseline list, no extra gate; list flatten,earn_react; wish-list (live io HOLD — not a ticket); ⚪; ret5=+4.9; combo leftover $995.24; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 6 | $157.78 | $2.01 | — | $1,211.13 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; combo leftover $995.24; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 17 | $56.09 | $2.04 | — | $255.56 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; combo leftover $995.24; owner flatten_h5 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $255.56 | ▲ close $10,158.68 vs 09:30 $10,161.04 (session +37.40) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $255.56 | ▼ 09:30 equity $9,976.86 vs yday $10,158.68 (-181.82) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 119 | $2.15 | $2.38 | $-48.75 | $509.03 | ▼ -48.75 after sell → book $9,974.48; vs 09:30 mark -2.38 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 44 | $5.93 | $2.14 | $-38.58 | $767.81 | ▼ -38.58 after sell → book $9,972.34; vs 09:30 mark -2.14 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 158 | $1.72 | $2.50 | $-34.19 | $1,036.28 | ▼ -34.19 after sell → book $9,969.84; vs 09:30 mark -2.50 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 63 | $4.13 | $2.20 | $-45.33 | $1,294.27 | ▼ -45.33 after sell → book $9,967.64; vs 09:30 mark -2.20 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 189 | $1.59 | $2.60 | $-5.16 | $1,592.18 | ▼ -5.16 after sell → book $9,965.04; vs 09:30 mark -2.60 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 26 | $10.73 | $2.09 | $-19.24 | $1,869.07 | ▼ -19.24 after sell → book $9,962.95; vs 09:30 mark -2.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 103 | $6.02 | $2.33 | $-19.05 | $2,486.81 | ▼ -19.05 after sell → book $9,960.63; vs 09:30 mark -2.32 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,486.81 | ▲ close $10,036.11 vs 09:30 $9,976.86 (session +75.48) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,486.81 | ▲ 09:30 equity $10,116.09 vs yday $10,036.11 (+79.98) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 130 | $5.11 | $2.41 | $+21.21 | $3,148.70 | ▲ +21.21 after sell → book $10,113.68; vs 09:30 mark -2.41 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 204 | $3.64 | $2.68 | $+98.73 | $3,888.58 | ▲ +98.73 after sell → book $10,111.00; vs 09:30 mark -2.68 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,888.58 | ▼ close $10,056.82 vs 09:30 $10,116.09 (session -54.18) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,888.58 | ▲ 09:30 equity $10,106.39 vs yday $10,056.82 (+49.57) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 216 | $1.80 | $2.79 | — | $3,497.00 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; combo leftover $388.86; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 16 | $23.29 | $2.04 | — | $3,122.32 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; combo leftover $388.86; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 26 | $14.62 | $2.07 | — | $2,740.13 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; combo leftover $388.86; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 2 | $270.89 | $2.00 | — | $2,196.35 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.0; combo leftover $685.03; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 8 | $77.12 | $2.01 | — | $1,577.38 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+7.2; combo leftover $685.03; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 47 | $14.31 | $2.13 | — | $902.68 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; combo leftover $685.03; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 18 | $36.46 | $2.04 | — | $244.35 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.9; combo leftover $685.03; owner flatten_h5 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $244.35 | ▼ close $10,073.49 vs 09:30 $10,106.39 (session -17.82) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $244.35 | ▲ 09:30 equity $10,247.99 vs yday $10,073.49 (+174.50) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 16 | $24.09 | $2.06 | $+8.70 | $627.74 | ▲ +8.70 after sell → book $10,245.94; vs 09:30 mark -2.05 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 26 | $13.77 | $2.09 | $-26.26 | $983.67 | ▼ -26.26 after sell → book $10,243.85; vs 09:30 mark -2.09 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 6 | $22.46 | $1.37 | — | $847.54 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.3; combo leftover $147.55; owner union_hot_n4_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 4 | $36.76 | $1.48 | — | $699.02 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; combo leftover $147.55; owner union_hot_n4_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 11 | $10.25 | $1.16 | — | $585.11 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; combo leftover $116.50; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 15 | $7.59 | $1.18 | — | $470.08 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.4; combo leftover $116.50; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 3 | $34.93 | $1.06 | — | $364.23 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.6; combo leftover $116.50; owner flatten_h5 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $364.23 | ▲ close $10,369.90 vs 09:30 $10,247.99 (session +132.30) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $364.23 | ▼ 09:30 equity $10,358.16 vs yday $10,369.90 (-11.74) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 61 | $16.93 | $2.19 | $+35.28 | $1,394.77 | ▲ +35.28 after sell → book $10,355.97; vs 09:30 mark -2.19 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 364 | $2.68 | $4.77 | $-27.66 | $2,365.52 | ▼ -27.66 after sell → book $10,351.20; vs 09:30 mark -4.77 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 4 | $197.76 | $2.02 | $-40.34 | $3,154.54 | ▼ -40.34 after sell → book $10,349.18; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `ORCL` | 6 | $150.47 | $2.03 | $-87.80 | $4,055.33 | ▼ -87.80 after sell → book $10,347.15; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 6 | $152.71 | $2.03 | $-34.46 | $4,969.56 | ▼ -34.46 after sell → book $10,345.12; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 17 | $55.80 | $2.06 | $-9.03 | $5,916.10 | ▼ -9.03 after sell → book $10,343.06; vs 09:30 mark -2.06 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 216 | $1.96 | $2.83 | $+28.94 | $6,336.63 | ▲ +28.94 after sell → book $10,340.23; vs 09:30 mark -2.83 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 6 | $21.30 | $1.32 | $-9.64 | $6,463.11 | ▼ -9.64 after sell → book $10,338.91; vs 09:30 mark -1.32 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 4 | $39.50 | $1.61 | $+7.87 | $6,619.50 | ▲ +7.87 after sell → book $10,337.30; vs 09:30 mark -1.61 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 22 | $29.32 | $2.06 | — | $5,972.40 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $661.95; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 218 | $3.04 | $2.81 | — | $5,307.96 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $661.95; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 8 | $81.40 | $2.01 | — | $4,654.75 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; combo leftover $661.95; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 7 | $108.55 | $2.01 | — | $3,892.89 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; combo leftover $775.79; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 1 | $593.15 | $1.99 | — | $3,297.74 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; combo leftover $775.79; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 3 | $209.52 | $2.00 | — | $2,667.19 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; combo leftover $775.79; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 3 | $219.62 | $2.00 | — | $2,006.33 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; combo leftover $775.79; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 9 | $85.00 | $2.02 | — | $1,239.31 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; combo leftover $775.79; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 22 | $34.44 | $2.06 | — | $479.57 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; combo leftover $775.79; owner flatten_h5 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $479.57 | ▼ close $10,223.71 vs 09:30 $10,358.16 (session -94.63) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $479.57 | ▲ 09:30 equity $10,383.02 vs yday $10,223.71 (+159.31) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 236 | $3.55 | $3.09 | $+194.46 | $1,314.28 | ▲ +194.46 after sell → book $10,379.93; vs 09:30 mark -3.09 | union_hot_n4_h1: dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 22 | $29.43 | $2.08 | $-1.71 | $1,959.66 | ▼ -1.71 after sell → book $10,377.85; vs 09:30 mark -2.08 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 218 | $4.00 | $2.86 | $+204.70 | $2,828.81 | ▲ +204.70 after sell → book $10,375.00; vs 09:30 mark -2.85 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 8 | $79.08 | $2.03 | $-22.61 | $3,459.41 | ▼ -22.61 after sell → book $10,372.96; vs 09:30 mark -2.04 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 105 | $2.47 | $2.31 | — | $3,197.76 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; combo leftover $259.46; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 15 | $16.91 | $2.04 | — | $2,942.07 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $259.46; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 157 | $1.65 | $2.46 | — | $2,680.56 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; combo leftover $259.46; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 22 | $11.67 | $2.06 | — | $2,421.76 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; combo leftover $259.46; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 3 | $157.87 | $2.00 | — | $1,946.16 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; combo leftover $484.35; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 1 | $386.20 | $1.99 | — | $1,557.96 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-5.8; combo leftover $484.35; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 5 | $88.83 | $2.00 | — | $1,111.81 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; combo leftover $484.35; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 35 | $13.47 | $2.10 | — | $638.26 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; combo leftover $484.35; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 121 | $4.00 | $2.35 | — | $151.91 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; combo leftover $484.35; owner flatten_h5 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.91 | ▲ close $10,355.52 vs 09:30 $10,383.02 (session +1.86) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.91 | ▼ 09:30 equity $10,351.33 vs yday $10,355.52 (-4.19) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 1 | $9.11 | $0.09 | — | $142.71 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+44.4; combo leftover $15.19; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 2 | $7.23 | $0.15 | — | $128.09 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+36.6; combo leftover $15.19; owner union_hot_n4_h1 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.09 | ▲ close $10,361.68 vs 09:30 $10,351.33 (session +10.60) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.09 | ▲ 09:30 equity $10,507.44 vs yday $10,361.68 (+145.76) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `IQV` | 2 | $270.66 | $2.02 | $-4.47 | $667.40 | ▼ -4.47 after sell → book $10,505.43; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 8 | $73.61 | $2.03 | $-32.13 | $1,254.24 | ▼ -32.13 after sell → book $10,503.40; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 47 | $13.12 | $2.15 | $-60.21 | $1,868.73 | ▼ -60.21 after sell → book $10,501.24; vs 09:30 mark -2.16 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 18 | $38.04 | $2.06 | $+24.33 | $2,551.39 | ▲ +24.33 after sell → book $10,499.18; vs 09:30 mark -2.06 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 15 | $16.92 | $2.06 | $-3.94 | $2,803.13 | ▼ -3.94 after sell → book $10,497.12; vs 09:30 mark -2.06 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 157 | $1.41 | $2.50 | $-42.64 | $3,022.01 | ▼ -42.64 after sell → book $10,494.63; vs 09:30 mark -2.50 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 22 | $12.80 | $2.08 | $+20.73 | $3,301.53 | ▲ +20.73 after sell → book $10,492.55; vs 09:30 mark -2.08 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 1 | $8.39 | $0.11 | $-0.92 | $3,309.81 | ▼ -0.92 after sell → book $10,492.44; vs 09:30 mark -0.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 2 | $6.83 | $0.16 | $-1.11 | $3,323.31 | ▼ -1.11 after sell → book $10,492.28; vs 09:30 mark -0.16 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 123 | $2.70 | $2.36 | — | $2,988.85 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $332.33; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 7 | $41.76 | $2.01 | — | $2,694.52 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $332.33; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 74 | $4.49 | $2.21 | — | $2,360.05 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; combo leftover $332.33; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 4 | $116.85 | $2.00 | — | $1,890.65 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; combo leftover $472.01; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 16 | $27.79 | $2.04 | — | $1,443.97 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; combo leftover $472.01; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 48 | $9.81 | $2.13 | — | $970.96 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; combo leftover $472.01; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 23 | $20.25 | $2.06 | — | $503.15 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; combo leftover $472.01; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 22 | $20.65 | $2.06 | — | $46.79 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; combo leftover $472.01; owner flatten_h5 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.79 | ▼ close $10,335.03 vs 09:30 $10,507.44 (session -140.38) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.79 | ▼ 09:30 equity $10,203.09 vs yday $10,335.03 (-131.94) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 11 | $10.39 | $1.20 | $-0.82 | $159.89 | ▼ -0.82 after sell → book $10,201.89; vs 09:30 mark -1.20 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 15 | $7.38 | $1.17 | $-5.51 | $269.41 | ▼ -5.51 after sell → book $10,200.72; vs 09:30 mark -1.17 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMN` | 3 | $33.82 | $1.04 | $-5.43 | $369.83 | ▼ -5.43 after sell → book $10,199.68; vs 09:30 mark -1.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 105 | $2.68 | $2.33 | $+17.41 | $648.90 | ▲ +17.41 after sell → book $10,197.35; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 7 | $36.02 | $2.03 | $-44.19 | $899.04 | ▼ -44.19 after sell → book $10,195.32; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 74 | $3.92 | $2.23 | $-46.26 | $1,187.26 | ▼ -46.26 after sell → book $10,193.08; vs 09:30 mark -2.24 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,187.26 | ▲ close $10,568.86 vs 09:30 $10,203.09 (session +375.79) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,677.06 | ▲ 09:30 equity $9,490.21 vs yday $9,338.69 (+151.52) | 09:30 open · cash $1,677.06 (unchanged overnight, no fees) · equity $9,490.21 vs prior close $9,338.69 (+151.52) | — |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 5 | $29.76 | $1.50 | — | $1,526.76 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; combo leftover $167.71; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 10 | $16.21 | $1.65 | — | $1,363.01 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $167.71; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 10 | $15.58 | $1.59 | — | $1,205.61 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; combo leftover $167.71; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 10 | $38.51 | $2.02 | — | $818.49 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.7; combo leftover $401.87; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 52 | $7.65 | $2.15 | — | $418.54 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; combo leftover $401.87; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $418.54 | ▼ close $9,350.08 vs 09:30 $9,490.21 (session -131.22) | 16:00 close · cash $418.54 · equity $9,350.08 vs 09:30 $9,490.21 (-140.13; session marks -131.22) · 25 name(s) marked open→close (per-name table). A×2 09:30 $171.98 → close $172.79 +1.62; ADMA×74 09:30 $9.52 → close $9.52 +0.00; ARQT×26 09:30 $26.27 → close $26.27 +0.00; DLO×11 09:30 $13.88 → close $13.88 +0.00; DXCM×3 09:30 $87.47 → close $87.47 +0.00; ECO×4 09:30 $78.22 → close $78.22 +0.00; EL×1 09:30 $95.37 → close $95.37 +0.00; FIVN×10 09:30 $36.66 → close $36.66 -0.00; FTRE×35 09:30 $20.02 → close $20.02 +0.00; GLND×191 09:30 $6.06 → close $5.54 -99.32; GNRC×1 09:30 $198.05 → close $198.05 +0.00; HALO×6 09:30 $115.36 → close $113.90 -8.76; MGTX×23 09:30 $11.05 → close $11.05 +0.00; MKC×3 09:30 $47.82 → close $47.82 -0.00; OMER×35 09:30 $20.61 → close $20.08 -18.55; PACS×4 09:30 $41.46 → close $41.46 -0.00; RBRK×3 09:30 $113.80 → close $113.80 +0.00; TDC×5 09:30 $29.46 → close $29.46 -0.00; USFD×1 09:30 $93.82 → close $93.82 -0.00; VICR×1 09:30 $276.06 → close $276.06 -0.00; TJGC×5 09:30 $29.76 → close $26.24 -17.60; SECZ×10 09:30 $16.21 → close $15.96 -2.50; USDE×10 09:30 $15.58 → close $17.25 +16.69; BLFS×10 09:30 $38.51 → close $38.49 -0.20; MRVI×52 09:30 $7.65 → close $7.60 -2.60 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `SLS` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TLN` | cash | leftover split 275.93 < 1 share @ 359.83 |
| 2026-08-14 | `DAVE` | cash | leftover split 275.93 < 1 share @ 330.91 |
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
| 2026-08-17 | `FANG` | cash | leftover split 145.83 < 1 share @ 202.70 |
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
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
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
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-20 | `VST` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `NRG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `SLG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `MARA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `LDI` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `BTBT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `DVN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `EOG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `TMC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `TGB` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `ELF` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `DNN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `HNST` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-21 | `DVN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `EOG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
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
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
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
| 2026-08-25 | `HCA` | cash | leftover split 415.24 < 1 share @ 426.97 |
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
| 2026-08-27 | `HCA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-28 | `OCUL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `INSP` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `CRMD` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `RZLT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `HCA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `MOS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `OCUL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `INSP` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `CRMD` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `RZLT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `HCA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-31 | `RRC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `CRK` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `SLI` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `HCA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
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
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
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
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
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
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
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
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
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
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
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
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-14 | `GPRO` | hard_red | hard-red S=-11.00 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `OVID` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `SANM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `ORCL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `NVT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `COHU` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `GPRO` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
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
| 2026-09-17 | `ILMN` | cash | leftover split 116.50 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 116.50 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 116.50 < 1 share @ 147.61 |
| 2026-09-18 | `IQV` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `PGEN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `AMN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `IQV` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `AMN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
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
| 2026-09-22 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `PGEN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `AMN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `DELL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
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
| 2026-09-22 | `USFD` | cash | leftover split 21.35 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-23 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `AMN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
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
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RBRK` | 7 | 2026-09-18 @ $108.55 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; combo leftover $775.79; owner flatten_h5 |
| `DELL` | 1 | 2026-09-18 @ $593.15 | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; combo leftover $775.79; owner flatten_h5 |
| `GNRC` | 3 | 2026-09-18 @ $209.52 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; combo leftover $775.79; owner flatten_h5 |
| `VICR` | 3 | 2026-09-18 @ $219.62 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; combo leftover $775.79; owner flatten_h5 |
| `ECO` | 9 | 2026-09-18 @ $85.00 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; combo leftover $775.79; owner flatten_h5 |
| `FIVN` | 22 | 2026-09-18 @ $34.44 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; combo leftover $775.79; owner flatten_h5 |
| `A` | 3 | 2026-09-21 @ $157.87 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; combo leftover $484.35; owner flatten_h5 |
| `HUM` | 1 | 2026-09-21 @ $386.20 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-5.8; combo leftover $484.35; owner flatten_h5 |
| `DXCM` | 5 | 2026-09-21 @ $88.83 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; combo leftover $484.35; owner flatten_h5 |
| `MGTX` | 35 | 2026-09-21 @ $13.47 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; combo leftover $484.35; owner flatten_h5 |
| `CYPH` | 121 | 2026-09-21 @ $4.00 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; combo leftover $484.35; owner flatten_h5 |
| `GLND` | 123 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $332.33; owner union_hot_n4_h1 |
| `HALO` | 4 | 2026-09-23 @ $116.85 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; combo leftover $472.01; owner flatten_h5 |
| `ARQT` | 16 | 2026-09-23 @ $27.79 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; combo leftover $472.01; owner flatten_h5 |
| `ADMA` | 48 | 2026-09-23 @ $9.81 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; combo leftover $472.01; owner flatten_h5 |
| `FTRE` | 23 | 2026-09-23 @ $20.25 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; combo leftover $472.01; owner flatten_h5 |
| `OMER` | 22 | 2026-09-23 @ $20.65 | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; combo leftover $472.01; owner flatten_h5 |
