# Factor mine action — `combo_nf_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_news_g_h1/flatten_h5 w=0.5,0.5 net=priority

Cash book **-17.87%** ($8,213) · signal-only (no cash/fees) was —. Starts YES **0/30**. Fills 392 · skips 441 · realized $+14.03.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_news_g_h1 50%, flatten_h5 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_news_g_h1 50%, flatten_h5 50%.
- Member: union_news_g_h1 (50% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $1,807.82.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; combo leftover $1250.00; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; combo leftover $1250.00; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; combo leftover $1250.00; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; combo leftover $1250.00; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; combo leftover $1250.00; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; combo leftover $1250.00; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; combo leftover $1250.00; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; combo leftover $1250.00; owner flatten_h5 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 1 | $4.31 | $0.05 | — | $93.18 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; combo leftover $6.10; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 2 | $9.01 | $0.19 | — | $74.97 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; combo leftover $18.64; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 19 | $0.94 | $0.23 | — | $56.93 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; combo leftover $18.64; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 12 | $1.50 | $0.22 | — | $38.72 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; combo leftover $18.64; owner flatten_h5 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.72 | ▲ close $10,435.42 vs 09:30 $10,178.12 (session +257.98) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.72 | ▼ 09:30 equity $10,414.89 vs yday $10,435.42 (-20.53) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 1 | $4.60 | $0.07 | $+0.17 | $43.25 | ▲ +0.17 after sell → book $10,414.82; vs 09:30 mark -0.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $35.06 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; combo leftover $8.65; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $26.52 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; combo leftover $8.65; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 2 | $3.24 | $0.07 | — | $19.96 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; combo leftover $8.65; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 1 | $4.81 | $0.05 | — | $15.10 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; combo leftover $8.65; owner flatten_h5 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.10 | ▲ close $10,525.72 vs 09:30 $10,414.89 (session +111.19) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.10 | ▼ 09:30 equity $10,391.47 vs yday $10,525.72 (-134.25) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.10 | ▲ close $10,571.23 vs 09:30 $10,391.47 (session +179.75) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.10 | ▲ 09:30 equity $10,709.29 vs yday $10,571.23 (+138.06) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.10 | ▲ close $11,030.73 vs 09:30 $10,709.29 (session +321.44) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.10 | ▼ 09:30 equity $10,966.50 vs yday $11,030.73 (-64.23) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 20 | $58.64 | $2.07 | $-27.32 | $1,185.83 | ▼ -27.32 after sell → book $10,964.43; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 27 | $42.46 | $2.09 | $-99.20 | $2,330.16 | ▼ -99.20 after sell → book $10,962.34; vs 09:30 mark -2.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 24 | $53.06 | $2.08 | $+54.34 | $3,601.52 | ▲ +54.34 after sell → book $10,960.26; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 25 | $51.65 | $2.09 | $+44.60 | $4,890.68 | ▲ +44.60 after sell → book $10,958.17; vs 09:30 mark -2.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 106 | $13.84 | $2.34 | $+222.19 | $6,355.39 | ▲ +222.19 after sell → book $10,955.84; vs 09:30 mark -2.33 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 42 | $30.66 | $2.14 | $+34.39 | $7,640.97 | ▲ +34.39 after sell → book $10,953.70; vs 09:30 mark -2.14 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 1543 | $1.30 | $20.18 | $+718.77 | $9,626.69 | ▲ +718.77 after sell → book $10,933.52; vs 09:30 mark -20.18 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 53 | $23.11 | $2.17 | $-15.98 | $10,849.36 | ▼ -15.98 after sell → book $10,931.35; vs 09:30 mark -2.17 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 7 | $91.01 | $2.01 | — | $10,210.27 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; combo leftover $678.08; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 4 | $150.14 | $2.00 | — | $9,607.71 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $678.08; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 959 | $0.71 | $9.66 | — | $8,920.04 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; combo leftover $678.08; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 102 | $6.61 | $2.30 | — | $8,244.04 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; combo leftover $678.08; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 42 | $16.00 | $2.12 | — | $7,569.92 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; combo leftover $678.08; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 25 | $26.57 | $2.06 | — | $6,903.61 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; combo leftover $678.08; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 11 | $58.73 | $2.02 | — | $6,255.55 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; combo leftover $678.08; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 15 | $44.76 | $2.04 | — | $5,582.12 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; combo leftover $678.08; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 38 | $20.55 | $2.10 | — | $4,799.11 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; combo leftover $797.45; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 38 | $20.65 | $2.10 | — | $4,012.31 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; combo leftover $797.45; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 138 | $5.77 | $2.40 | — | $3,213.65 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; combo leftover $797.45; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 40 | $19.63 | $2.11 | — | $2,426.34 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; combo leftover $797.45; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 26 | $29.63 | $2.07 | — | $1,653.89 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; combo leftover $797.45; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 455 | $1.75 | $5.87 | — | $851.77 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; combo leftover $797.45; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 5 | $144.54 | $2.00 | — | $127.06 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; combo leftover $797.45; owner flatten_h5 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.06 | ▲ close $10,921.74 vs 09:30 $10,966.50 (session +33.26) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.06 | ▲ 09:30 equity $11,208.25 vs yday $10,921.74 (+286.51) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 2 | $11.70 | $0.26 | $+4.93 | $150.20 | ▲ +4.93 after sell → book $11,207.99; vs 09:30 mark -0.26 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 19 | $0.87 | $0.24 | $-1.81 | $166.43 | ▼ -1.81 after sell → book $11,207.75; vs 09:30 mark -0.24 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 12 | $1.66 | $0.26 | $+1.45 | $186.10 | ▲ +1.45 after sell → book $11,207.49; vs 09:30 mark -0.26 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 7 | $95.72 | $2.03 | $+28.93 | $854.11 | ▲ +28.93 after sell → book $11,205.46; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 4 | $133.11 | $2.02 | $-72.14 | $1,384.53 | ▼ -72.14 after sell → book $11,203.44; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 959 | $0.67 | $9.51 | $-50.81 | $2,021.38 | ▼ -50.81 after sell → book $11,193.93; vs 09:30 mark -9.51 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 102 | $6.95 | $2.32 | $+30.57 | $2,727.96 | ▲ +30.57 after sell → book $11,191.61; vs 09:30 mark -2.32 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 42 | $17.66 | $2.14 | $+65.47 | $3,467.54 | ▲ +65.47 after sell → book $11,189.47; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 25 | $26.25 | $2.08 | $-12.15 | $4,121.71 | ▼ -12.15 after sell → book $11,187.39; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 15 | $44.52 | $2.06 | $-7.69 | $4,787.45 | ▼ -7.69 after sell → book $11,185.33; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 2 | $119.43 | $2.00 | — | $4,546.60 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $341.96; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 138 | $2.47 | $2.40 | — | $4,203.33 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; combo leftover $341.96; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 2 | $115.18 | $2.00 | — | $3,970.98 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $341.96; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 29 | $11.70 | $2.08 | — | $3,629.60 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; combo leftover $341.96; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 30 | $11.10 | $2.08 | — | $3,294.67 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; combo leftover $341.96; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 105 | $3.24 | $2.31 | — | $2,952.16 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; combo leftover $341.96; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 34 | $17.20 | $2.09 | — | $2,365.27 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; combo leftover $590.43; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 2 | $216.30 | $2.00 | — | $1,930.68 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; combo leftover $590.43; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 53 | $11.13 | $2.15 | — | $1,338.64 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; combo leftover $590.43; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 305 | $1.93 | $3.93 | — | $746.05 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; combo leftover $590.43; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 447 | $1.32 | $5.77 | — | $150.25 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; combo leftover $590.43; owner flatten_h5 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $150.25 | ▲ close $11,255.45 vs 09:30 $11,208.25 (session +98.91) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $150.25 | ▲ 09:30 equity $11,479.18 vs yday $11,255.45 (+223.73) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 2 | $4.62 | $0.12 | $+0.94 | $159.38 | ▲ +0.94 after sell → book $11,479.06; vs 09:30 mark -0.12 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 1 | $9.26 | $0.12 | $+0.60 | $168.52 | ▲ +0.60 after sell → book $11,478.94; vs 09:30 mark -0.12 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 2 | $3.50 | $0.10 | $+0.35 | $175.43 | ▲ +0.35 after sell → book $11,478.85; vs 09:30 mark -0.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 1 | $5.05 | $0.07 | $+0.12 | $180.40 | ▲ +0.12 after sell → book $11,478.77; vs 09:30 mark -0.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 2 | $120.51 | $2.02 | $-1.85 | $419.41 | ▼ -1.85 after sell → book $11,476.76; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 138 | $2.40 | $2.44 | $-14.50 | $748.17 | ▼ -14.50 after sell → book $11,474.32; vs 09:30 mark -2.44 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 2 | $121.00 | $2.02 | $+7.63 | $988.15 | ▲ +7.63 after sell → book $11,472.30; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 29 | $11.17 | $2.10 | $-19.54 | $1,309.99 | ▼ -19.54 after sell → book $11,470.21; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 30 | $11.48 | $2.10 | $+7.37 | $1,652.29 | ▲ +7.37 after sell → book $11,468.11; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 105 | $2.99 | $2.33 | $-30.89 | $1,963.91 | ▼ -30.89 after sell → book $11,465.78; vs 09:30 mark -2.33 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,963.91 | ▼ close $11,410.84 vs 09:30 $11,479.18 (session -54.93) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,963.91 | ▼ 09:30 equity $11,261.62 vs yday $11,410.84 (-149.22) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 11 | $57.93 | $2.04 | $-12.87 | $2,599.09 | ▼ -12.87 after sell → book $11,259.57; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 19 | $9.42 | $1.85 | — | $2,418.27 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; combo leftover $185.65; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 5 | $35.05 | $1.77 | — | $2,241.25 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $185.65; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 7 | $24.11 | $1.71 | — | $2,070.77 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=+891.7; combo leftover $185.65; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 6 | $28.86 | $1.75 | — | $1,895.86 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.7; combo leftover $185.65; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 21 | $8.72 | $1.89 | — | $1,710.85 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.0; combo leftover $185.65; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 1 | $118.52 | $1.19 | — | $1,591.14 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+21.7; combo leftover $185.65; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 2 | $77.13 | $1.55 | — | $1,435.33 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; ⚪; ret5=+13.8; combo leftover $185.65; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 10 | $23.77 | $2.02 | — | $1,195.61 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; combo leftover $239.22; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 21 | $10.98 | $2.05 | — | $962.98 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; combo leftover $239.22; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 3 | $61.19 | $1.84 | — | $777.56 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.4; combo leftover $239.22; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 28 | $8.35 | $2.07 | — | $541.69 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; combo leftover $239.22; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 48 | $4.94 | $2.13 | — | $302.43 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; combo leftover $239.22; owner flatten_h5 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $302.43 | ▲ close $11,708.62 vs 09:30 $11,261.62 (session +470.87) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $302.43 | ▼ 09:30 equity $11,554.18 vs yday $11,708.62 (-154.44) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 19 | $10.07 | $1.99 | $+8.51 | $491.77 | ▲ +8.51 after sell → book $11,552.19; vs 09:30 mark -1.99 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 5 | $35.70 | $1.82 | $-0.34 | $668.45 | ▼ -0.34 after sell → book $11,550.37; vs 09:30 mark -1.82 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 7 | $26.61 | $1.90 | $+13.89 | $852.82 | ▲ +13.89 after sell → book $11,548.47; vs 09:30 mark -1.90 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 6 | $27.56 | $1.69 | $-11.24 | $1,016.49 | ▼ -11.24 after sell → book $11,546.78; vs 09:30 mark -1.69 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 21 | $8.86 | $1.94 | $-0.90 | $1,200.60 | ▼ -0.90 after sell → book $11,544.83; vs 09:30 mark -1.95 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 1 | $119.80 | $1.22 | $-1.13 | $1,319.18 | ▼ -1.13 after sell → book $11,543.61; vs 09:30 mark -1.22 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 2 | $79.34 | $1.61 | $+1.26 | $1,476.25 | ▲ +1.26 after sell → book $11,542.00; vs 09:30 mark -1.61 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 11 | $11.12 | $1.26 | — | $1,352.67 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; combo leftover $123.02; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 14 | $8.29 | $1.20 | — | $1,235.41 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; combo leftover $123.02; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 7 | $17.41 | $1.24 | — | $1,112.30 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-9.2; combo leftover $123.02; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 10 | $11.22 | $1.15 | — | $998.95 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.8; combo leftover $123.02; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 1 | $118.50 | $1.19 | — | $879.26 | — | union ∩ news_g, no 🚨; gate news=good; list overnight,overnight_mega; 🔵; ret5=-2.7; combo leftover $123.02; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `HCA` | 2 | $427.50 | $2.00 | — | $22.27 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.1; combo leftover $879.26; owner flatten_h5 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.27 | ▼ close $11,515.05 vs 09:30 $11,554.18 (session -18.92) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.27 | ▲ 09:30 equity $11,565.73 vs yday $11,515.05 (+50.68) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 38 | $20.93 | $2.12 | $+10.21 | $815.48 | ▲ +10.21 after sell → book $11,563.60; vs 09:30 mark -2.13 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 38 | $21.31 | $2.12 | $+20.85 | $1,623.14 | ▲ +20.85 after sell → book $11,561.48; vs 09:30 mark -2.12 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 138 | $5.49 | $2.44 | $-43.48 | $2,378.32 | ▼ -43.48 after sell → book $11,559.04; vs 09:30 mark -2.44 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 40 | $21.47 | $2.13 | $+69.36 | $3,234.99 | ▲ +69.36 after sell → book $11,556.91; vs 09:30 mark -2.13 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 26 | $32.32 | $2.09 | $+65.78 | $4,073.22 | ▲ +65.78 after sell → book $11,554.82; vs 09:30 mark -2.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 455 | $1.91 | $5.96 | $+60.98 | $4,936.32 | ▲ +60.98 after sell → book $11,548.87; vs 09:30 mark -5.95 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 5 | $155.89 | $2.02 | $+52.72 | $5,713.74 | ▲ +52.72 after sell → book $11,546.84; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 22 | $41.44 | $2.06 | — | $4,800.01 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+3.1; combo leftover $952.29; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 13 | $70.30 | $2.03 | — | $3,884.08 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=-11.2; combo leftover $952.29; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `SRRK` | 15 | $60.00 | $2.04 | — | $2,982.04 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+6.2; combo leftover $952.29; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 103 | $14.42 | $2.30 | — | $1,494.48 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; combo leftover $1491.02; owner flatten_h5 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 571 | $2.60 | $7.37 | — | $2.52 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; combo leftover $1491.02; owner flatten_h5 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.52 | ▲ close $11,585.22 vs 09:30 $11,565.73 (session +54.16) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.52 | ▼ 09:30 equity $11,517.34 vs yday $11,585.22 (-67.88) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 34 | $16.44 | $2.11 | $-30.04 | $559.37 | ▼ -30.04 after sell → book $11,515.23; vs 09:30 mark -2.11 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEM` | 2 | $216.31 | $2.02 | $-3.99 | $989.97 | ▼ -3.99 after sell → book $11,513.21; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 53 | $15.43 | $2.17 | $+223.58 | $1,805.59 | ▲ +223.58 after sell → book $11,511.04; vs 09:30 mark -2.17 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 305 | $2.06 | $4.00 | $+31.72 | $2,429.90 | ▲ +31.72 after sell → book $11,507.05; vs 09:30 mark -3.99 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 447 | $1.82 | $5.85 | $+211.88 | $3,237.58 | ▲ +211.88 after sell → book $11,501.19; vs 09:30 mark -5.86 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 11 | $11.27 | $1.29 | $-0.90 | $3,360.26 | ▼ -0.90 after sell → book $11,499.90; vs 09:30 mark -1.29 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWRD` | 7 | $17.70 | $1.28 | $-0.49 | $3,482.88 | ▼ -0.49 after sell → book $11,498.62; vs 09:30 mark -1.28 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TRLV` | 10 | $11.00 | $1.15 | $-4.50 | $3,591.73 | ▼ -4.50 after sell → book $11,497.47; vs 09:30 mark -1.15 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 1 | $115.66 | $1.18 | $-5.21 | $3,706.21 | ▼ -5.21 after sell → book $11,496.29; vs 09:30 mark -1.18 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 13 | $65.29 | $2.05 | $-69.21 | $4,552.93 | ▼ -69.21 after sell → book $11,494.24; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SRRK` | 15 | $58.75 | $2.06 | $-22.84 | $5,432.13 | ▼ -22.84 after sell → book $11,492.19; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 27 | $32.90 | $2.07 | — | $4,541.76 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; combo leftover $905.35; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 105 | $8.61 | $2.31 | — | $3,635.40 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.7; combo leftover $905.35; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 6 | $141.76 | $2.01 | — | $2,782.83 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; combo leftover $905.35; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 47 | $19.25 | $2.13 | — | $1,875.95 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+14.1; combo leftover $905.35; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 48 | $18.75 | $2.13 | — | $973.82 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-5.0; combo leftover $905.35; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 31 | $28.91 | $2.08 | — | $75.53 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+9.2; combo leftover $905.35; owner union_news_g_h1 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.53 | ▼ close $11,153.54 vs 09:30 $11,517.34 (session -325.92) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.53 | ▲ 09:30 equity $11,186.20 vs yday $11,153.54 (+32.66) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 14 | $9.50 | $1.39 | $+14.35 | $207.13 | ▲ +14.35 after sell → book $11,184.80; vs 09:30 mark -1.40 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 22 | $42.00 | $2.08 | $+8.19 | $1,129.06 | ▲ +8.19 after sell → book $11,182.73; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 27 | $31.15 | $2.09 | $-51.41 | $1,968.02 | ▼ -51.41 after sell → book $11,180.64; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 105 | $8.52 | $2.33 | $-14.09 | $2,860.29 | ▼ -14.09 after sell → book $11,178.31; vs 09:30 mark -2.33 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 6 | $132.30 | $2.03 | $-60.80 | $3,652.06 | ▼ -60.80 after sell → book $11,176.28; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 47 | $17.87 | $2.15 | $-69.14 | $4,489.80 | ▼ -69.14 after sell → book $11,174.13; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 48 | $19.25 | $2.15 | $+19.71 | $5,411.64 | ▲ +19.71 after sell → book $11,171.97; vs 09:30 mark -2.16 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 31 | $28.06 | $2.10 | $-30.54 | $6,279.40 | ▼ -30.54 after sell → book $11,169.87; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,279.40 | ▲ close $11,226.02 vs 09:30 $11,186.20 (session +56.15) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,279.40 | ▲ 09:30 equity $11,371.23 vs yday $11,226.02 (+145.21) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 10 | $23.94 | $2.04 | $-2.36 | $6,516.76 | ▼ -2.36 after sell → book $11,369.19; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 21 | $10.42 | $2.07 | $-15.89 | $6,733.51 | ▼ -15.89 after sell → book $11,367.12; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `INSP` | 3 | $63.00 | $1.92 | $+1.67 | $6,920.59 | ▲ +1.67 after sell → book $11,365.20; vs 09:30 mark -1.92 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 28 | $8.25 | $2.09 | $-6.97 | $7,149.49 | ▼ -6.97 after sell → book $11,363.10; vs 09:30 mark -2.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 48 | $4.64 | $2.15 | $-18.69 | $7,370.06 | ▼ -18.69 after sell → book $11,360.95; vs 09:30 mark -2.15 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,370.06 | ▼ close $11,269.07 vs 09:30 $11,371.23 (session -91.88) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,370.06 | ▼ 09:30 equity $11,233.87 vs yday $11,269.07 (-35.20) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `HCA` | 2 | $412.46 | $2.02 | $-34.09 | $8,192.96 | ▼ -34.09 after sell → book $11,231.85; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,192.96 | ▼ close $11,169.69 vs 09:30 $11,233.87 (session -62.16) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,192.96 | ▲ 09:30 equity $11,206.10 vs yday $11,169.69 (+36.41) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 103 | $15.45 | $2.33 | $+101.46 | $9,781.98 | ▲ +101.46 after sell → book $11,203.77; vs 09:30 mark -2.33 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 571 | $2.49 | $7.47 | $-77.65 | $11,196.30 | ▼ -77.65 after sell → book $11,196.30; vs 09:30 mark -7.47 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 29 | $23.88 | $2.08 | — | $10,501.71 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $699.77; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 21 | $32.88 | $2.05 | — | $9,809.17 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+16.2; combo leftover $699.77; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 92 | $7.59 | $2.27 | — | $9,108.63 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.5; combo leftover $699.77; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 44 | $15.87 | $2.12 | — | $8,408.22 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $699.77; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $8,054.49 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+3.3; combo leftover $699.77; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $7,698.01 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-12.3; combo leftover $699.77; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 14 | $47.60 | $2.03 | — | $7,029.58 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; combo leftover $699.77; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 26 | $52.88 | $2.07 | — | $5,652.63 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; combo leftover $1405.92; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $42.93 | $2.09 | — | $4,276.78 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; combo leftover $1405.92; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 387 | $3.63 | $4.99 | — | $2,866.98 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; combo leftover $1405.92; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 175 | $8.03 | $2.52 | — | $1,459.22 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; combo leftover $1405.92; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $132.70 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; combo leftover $1405.92; owner flatten_h5 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.70 | ▼ close $11,161.25 vs 09:30 $11,206.10 (session -6.84) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.70 | ▼ 09:30 equity $11,084.04 vs yday $11,161.25 (-77.21) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 29 | $23.84 | $2.10 | $-5.33 | $821.96 | ▼ -5.33 after sell → book $11,081.94; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 21 | $32.48 | $2.07 | $-12.53 | $1,501.97 | ▼ -12.53 after sell → book $11,079.87; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 92 | $7.79 | $2.29 | $+13.84 | $2,216.35 | ▲ +13.84 after sell → book $11,077.57; vs 09:30 mark -2.30 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 1 | $359.70 | $2.01 | $+3.95 | $2,574.04 | ▲ +3.95 after sell → book $11,075.56; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 1 | $321.67 | $2.01 | $-36.83 | $2,893.70 | ▼ -36.83 after sell → book $11,073.55; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 14 | $53.85 | $2.05 | $+83.42 | $3,645.55 | ▲ +83.42 after sell → book $11,071.50; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 1 | $263.36 | $1.99 | — | $3,380.19 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; combo leftover $364.55; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 187 | $1.94 | $2.55 | — | $3,014.86 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+18.3; combo leftover $364.55; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 2 | $137.35 | $2.00 | — | $2,738.17 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+5.4; combo leftover $364.55; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 1 | $236.82 | $1.99 | — | $2,499.35 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.1; combo leftover $364.55; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 4 | $75.65 | $2.00 | — | $2,194.75 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+6.0; combo leftover $364.55; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 145 | $2.52 | $2.42 | — | $1,826.93 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; combo leftover $365.79; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 54 | $6.71 | $2.15 | — | $1,462.43 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; combo leftover $365.79; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 192 | $1.90 | $2.57 | — | $1,095.07 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; combo leftover $365.79; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 76 | $4.78 | $2.22 | — | $729.57 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; combo leftover $365.79; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 230 | $1.59 | $2.97 | — | $360.90 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; combo leftover $365.79; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 31 | $11.31 | $2.08 | — | $8.21 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; combo leftover $365.79; owner flatten_h5 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.21 | ▲ close $11,121.58 vs 09:30 $11,084.04 (session +75.03) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.21 | ▲ 09:30 equity $11,162.90 vs yday $11,121.58 (+41.32) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 44 | $16.74 | $2.14 | $+34.02 | $742.63 | ▲ +34.02 after sell → book $11,160.76; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 1 | $253.72 | $2.01 | $-13.65 | $994.33 | ▼ -13.65 after sell → book $11,158.74; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 187 | $1.94 | $2.59 | $-5.14 | $1,354.52 | ▼ -5.14 after sell → book $11,156.15; vs 09:30 mark -2.59 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 1 | $267.76 | $2.01 | $+26.93 | $1,620.27 | ▲ +26.93 after sell → book $11,154.14; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,620.27 | ▼ close $11,004.11 vs 09:30 $11,162.90 (session -150.03) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,620.27 | ▼ 09:30 equity $10,966.35 vs yday $11,004.11 (-37.76) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 2 | $141.82 | $2.02 | $+4.93 | $1,901.89 | ▲ +4.93 after sell → book $10,964.33; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 4 | $76.60 | $2.02 | $-0.22 | $2,206.27 | ▼ -0.22 after sell → book $10,962.31; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,206.27 | ▼ close $10,681.54 vs 09:30 $10,966.35 (session -280.77) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,206.27 | ▼ 09:30 equity $10,582.39 vs yday $10,681.54 (-99.15) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,206.27 | ▼ close $10,437.07 vs 09:30 $10,582.39 (session -145.32) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,206.27 | ▲ 09:30 equity $10,523.18 vs yday $10,437.07 (+86.11) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 26 | $53.53 | $2.09 | $+12.74 | $3,595.96 | ▲ +12.74 after sell → book $10,521.09; vs 09:30 mark -2.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 32 | $41.30 | $2.11 | $-56.35 | $4,915.46 | ▼ -56.35 after sell → book $10,518.99; vs 09:30 mark -2.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 387 | $2.77 | $5.07 | $-342.88 | $5,982.38 | ▼ -342.88 after sell → book $10,513.92; vs 09:30 mark -5.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 175 | $7.70 | $2.55 | $-62.82 | $7,327.32 | ▼ -62.82 after sell → book $10,511.36; vs 09:30 mark -2.56 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 10 | $122.40 | $2.04 | $-104.56 | $8,549.28 | ▼ -104.56 after sell → book $10,509.32; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 4 | $164.43 | $2.00 | — | $7,889.56 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $712.44; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 349 | $2.04 | $4.50 | — | $7,173.10 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; combo leftover $712.44; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 336 | $2.12 | $4.33 | — | $6,456.45 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; combo leftover $712.44; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 47 | $15.01 | $2.13 | — | $5,748.85 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $712.44; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 2 | $242.17 | $2.00 | — | $5,262.51 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-11.1; combo leftover $712.44; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 5 | $135.71 | $2.00 | — | $4,581.95 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-9.2; combo leftover $712.44; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 56 | $16.28 | $2.16 | — | $3,668.12 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; combo leftover $916.39; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 335 | $2.73 | $4.32 | — | $2,749.24 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; combo leftover $916.39; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 4 | $206.84 | $2.00 | — | $1,919.88 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; combo leftover $916.39; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 5 | $157.78 | $2.00 | — | $1,128.98 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; combo leftover $916.39; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 16 | $56.09 | $2.04 | — | $229.50 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; combo leftover $916.39; owner flatten_h5 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $229.50 | ▼ close $10,454.26 vs 09:30 $10,523.18 (session -25.57) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $229.50 | ▼ 09:30 equity $10,270.12 vs yday $10,454.26 (-184.14) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 145 | $2.15 | $2.46 | $-58.53 | $538.79 | ▼ -58.53 after sell → book $10,267.67; vs 09:30 mark -2.45 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 54 | $5.93 | $2.17 | $-46.44 | $856.84 | ▼ -46.44 after sell → book $10,265.49; vs 09:30 mark -2.18 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 192 | $1.72 | $2.61 | $-40.69 | $1,183.51 | ▼ -40.69 after sell → book $10,262.89; vs 09:30 mark -2.60 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 76 | $4.13 | $2.24 | $-53.86 | $1,495.15 | ▼ -53.86 after sell → book $10,260.65; vs 09:30 mark -2.24 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 230 | $1.59 | $3.02 | $-5.98 | $1,857.83 | ▼ -5.98 after sell → book $10,257.63; vs 09:30 mark -3.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 31 | $10.73 | $2.10 | $-22.17 | $2,188.36 | ▼ -22.17 after sell → book $10,255.53; vs 09:30 mark -2.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 4 | $141.42 | $2.02 | $-96.06 | $2,752.02 | ▼ -96.06 after sell → book $10,253.50; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 349 | $2.01 | $4.57 | $-19.54 | $3,448.94 | ▼ -19.54 after sell → book $10,248.93; vs 09:30 mark -4.57 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 336 | $2.05 | $4.40 | $-32.25 | $4,133.34 | ▼ -32.25 after sell → book $10,244.53; vs 09:30 mark -4.40 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 2 | $261.51 | $2.02 | $+34.67 | $4,654.34 | ▲ +34.67 after sell → book $10,242.52; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 5 | $131.40 | $2.02 | $-25.58 | $5,309.32 | ▼ -25.58 after sell → book $10,240.49; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,309.32 | ▼ close $10,175.58 vs 09:30 $10,270.12 (session -64.91) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,309.32 | ▲ 09:30 equity $10,220.81 vs yday $10,175.58 (+45.23) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,309.32 | ▼ close $10,146.09 vs 09:30 $10,220.81 (session -74.72) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,309.32 | ▲ 09:30 equity $10,194.34 vs yday $10,146.09 (+48.25) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 25 | $26.27 | $2.06 | — | $4,650.50 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+10.0; combo leftover $663.66; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 95 | $6.95 | $2.27 | — | $3,987.98 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-5.8; combo leftover $663.66; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 16 | $39.99 | $2.04 | — | $3,346.10 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+9.3; combo leftover $663.66; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 3 | $189.17 | $2.00 | — | $2,776.59 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+7.9; combo leftover $663.66; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 2 | $270.89 | $2.00 | — | $2,232.82 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.0; combo leftover $694.15; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 9 | $77.12 | $2.02 | — | $1,536.72 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+7.2; combo leftover $694.15; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 48 | $14.31 | $2.13 | — | $847.70 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; combo leftover $694.15; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 19 | $36.46 | $2.05 | — | $152.92 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.9; combo leftover $694.15; owner flatten_h5 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $152.92 | ▼ close $10,161.22 vs 09:30 $10,194.34 (session -16.55) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $152.92 | ▲ 09:30 equity $10,326.12 vs yday $10,161.22 (+164.90) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 25 | $26.51 | $2.08 | $+1.85 | $813.58 | ▲ +1.85 after sell → book $10,324.03; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 95 | $7.27 | $2.30 | $+25.82 | $1,501.93 | ▲ +25.82 after sell → book $10,321.73; vs 09:30 mark -2.30 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 16 | $37.57 | $2.06 | $-42.82 | $2,100.99 | ▼ -42.82 after sell → book $10,319.67; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 3 | $190.35 | $2.02 | $-0.48 | $2,670.02 | ▼ -0.48 after sell → book $10,317.65; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 1 | $170.85 | $1.71 | — | $2,497.46 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; combo leftover $222.50; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 12 | $17.72 | $2.03 | — | $2,282.80 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=-8.3; combo leftover $222.50; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 125 | $1.77 | $2.37 | — | $2,059.18 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-10.2; combo leftover $222.50; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 10 | $22.12 | $2.02 | — | $1,835.96 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+10.5; combo leftover $222.50; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 1 | $233.85 | $1.99 | — | $1,600.12 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+11.7; combo leftover $305.99; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 2 | $151.43 | $2.00 | — | $1,295.26 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.0; combo leftover $305.99; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 2 | $147.61 | $2.00 | — | $998.05 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+17.7; combo leftover $305.99; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 29 | $10.25 | $2.08 | — | $698.72 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; combo leftover $305.99; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 40 | $7.59 | $2.11 | — | $393.01 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.4; combo leftover $305.99; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 8 | $34.93 | $2.01 | — | $111.56 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.6; combo leftover $305.99; owner flatten_h5 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.56 | ▲ close $10,309.68 vs 09:30 $10,326.12 (session +12.33) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.56 | ▲ 09:30 equity $10,338.82 vs yday $10,309.68 (+29.14) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 47 | $15.87 | $2.15 | $+36.14 | $855.30 | ▲ +36.14 after sell → book $10,336.67; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 5 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 56 | $16.93 | $2.18 | $+32.06 | $1,801.20 | ▲ +32.06 after sell → book $10,334.49; vs 09:30 mark -2.18 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 335 | $2.68 | $4.39 | $-25.46 | $2,694.61 | ▼ -25.46 after sell → book $10,330.10; vs 09:30 mark -4.39 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 4 | $197.76 | $2.02 | $-40.34 | $3,483.63 | ▼ -40.34 after sell → book $10,328.08; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 5 | $152.71 | $2.02 | $-29.38 | $4,245.15 | ▼ -29.38 after sell → book $10,326.05; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 16 | $55.80 | $2.06 | $-8.74 | $5,135.90 | ▼ -8.74 after sell → book $10,324.00; vs 09:30 mark -2.05 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 1 | $182.33 | $1.85 | $+7.92 | $5,316.38 | ▲ +7.92 after sell → book $10,322.15; vs 09:30 mark -1.85 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 12 | $17.13 | $2.05 | $-11.15 | $5,519.89 | ▼ -11.15 after sell → book $10,320.10; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 125 | $1.77 | $2.40 | $-4.76 | $5,738.75 | ▼ -4.76 after sell → book $10,317.71; vs 09:30 mark -2.39 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 50 | $14.07 | $2.14 | — | $5,033.11 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; combo leftover $717.34; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 48 | $14.79 | $2.13 | — | $4,321.05 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; combo leftover $717.34; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 95 | $7.54 | $2.27 | — | $3,602.95 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-20.9; combo leftover $717.34; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 34 | $20.91 | $2.09 | — | $2,889.92 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+8.6; combo leftover $717.34; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 4 | $108.55 | $2.00 | — | $2,453.72 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; combo leftover $481.65; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 2 | $209.52 | $2.00 | — | $2,032.68 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; combo leftover $481.65; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 2 | $219.62 | $2.00 | — | $1,591.45 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; combo leftover $481.65; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 5 | $85.00 | $2.00 | — | $1,164.44 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; combo leftover $481.65; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 13 | $34.44 | $2.03 | — | $714.69 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; combo leftover $481.65; owner flatten_h5 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $714.69 | ▼ close $10,181.59 vs 09:30 $10,338.82 (session -117.44) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $714.69 | ▲ 09:30 equity $10,258.21 vs yday $10,181.59 (+76.62) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 10 | $22.78 | $2.04 | $+2.54 | $940.45 | ▲ +2.54 after sell → book $10,256.17; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 50 | $13.90 | $2.16 | $-12.80 | $1,633.29 | ▼ -12.80 after sell → book $10,254.01; vs 09:30 mark -2.16 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 48 | $14.58 | $2.15 | $-14.37 | $2,330.98 | ▼ -14.37 after sell → book $10,251.86; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 95 | $7.36 | $2.30 | $-21.20 | $3,027.88 | ▼ -21.20 after sell → book $10,249.56; vs 09:30 mark -2.30 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 34 | $21.65 | $2.11 | $+20.96 | $3,761.87 | ▲ +20.96 after sell → book $10,247.45; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 14 | $25.95 | $2.03 | — | $3,396.53 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.2; combo leftover $376.19; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 174 | $2.15 | $2.51 | — | $3,019.92 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+7.5; combo leftover $376.19; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 26 | $13.94 | $2.07 | — | $2,655.41 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; combo leftover $376.19; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 62 | $6.00 | $2.18 | — | $2,281.24 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-24.1; combo leftover $376.19; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 1 | $190.30 | $1.91 | — | $2,089.03 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+10.6; combo leftover $376.19; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 2 | $157.87 | $2.00 | — | $1,771.30 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; combo leftover $417.81; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 1 | $386.20 | $1.99 | — | $1,383.10 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-5.8; combo leftover $417.81; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 4 | $88.83 | $2.00 | — | $1,025.78 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; combo leftover $417.81; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 31 | $13.47 | $2.08 | — | $606.13 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; combo leftover $417.81; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 104 | $4.00 | $2.30 | — | $187.83 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; combo leftover $417.81; owner flatten_h5 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $187.83 | ▼ close $10,113.60 vs 09:30 $10,258.21 (session -112.78) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $187.83 | ▲ 09:30 equity $10,120.23 vs yday $10,113.60 (+6.63) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `MARA` | 26 | $13.13 | $2.09 | $-25.22 | $527.12 | ▼ -25.22 after sell → book $10,118.14; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 62 | $5.99 | $2.20 | $-4.99 | $896.30 | ▼ -4.99 after sell → book $10,115.94; vs 09:30 mark -2.20 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 73 | $1.01 | $0.96 | — | $821.62 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+14.3; combo leftover $74.69; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 17 | $4.30 | $0.78 | — | $747.73 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; combo leftover $74.69; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 1 | $93.97 | $0.94 | — | $652.82 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-0.6; combo leftover $124.62; owner flatten_h5 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $652.82 | ▲ close $10,144.72 vs 09:30 $10,120.23 (session +31.46) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $652.82 | ▲ 09:30 equity $10,247.95 vs yday $10,144.72 (+103.23) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `IQV` | 2 | $270.66 | $2.02 | $-4.47 | $1,192.13 | ▼ -4.47 after sell → book $10,245.93; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 9 | $73.61 | $2.04 | $-35.64 | $1,852.58 | ▼ -35.64 after sell → book $10,243.89; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 48 | $13.12 | $2.15 | $-61.41 | $2,480.18 | ▼ -61.41 after sell → book $10,241.74; vs 09:30 mark -2.15 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 19 | $38.04 | $2.07 | $+25.91 | $3,200.88 | ▲ +25.91 after sell → book $10,239.67; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 14 | $26.58 | $2.05 | $+4.74 | $3,570.95 | ▲ +4.74 after sell → book $10,237.62; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMTX` | 174 | $2.09 | $2.55 | $-15.50 | $3,932.05 | ▼ -15.50 after sell → book $10,235.07; vs 09:30 mark -2.55 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 1 | $174.50 | $1.77 | $-19.47 | $4,104.79 | ▼ -19.47 after sell → book $10,233.30; vs 09:30 mark -1.77 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 73 | $0.95 | $0.93 | $-6.27 | $4,173.20 | ▼ -6.27 after sell → book $10,232.37; vs 09:30 mark -0.93 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 26 | $15.72 | $2.07 | — | $3,762.41 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; combo leftover $417.32; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 342 | $1.22 | $4.41 | — | $3,340.76 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-33.0; combo leftover $417.32; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 321 | $1.30 | $4.14 | — | $2,919.32 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; combo leftover $417.32; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 10 | $40.00 | $2.02 | — | $2,517.30 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+6.7; combo leftover $417.32; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 2 | $196.78 | $2.00 | — | $2,121.74 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $417.32; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 3 | $116.85 | $2.00 | — | $1,769.20 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; combo leftover $424.35; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 15 | $27.79 | $2.04 | — | $1,350.31 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; combo leftover $424.35; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 43 | $9.81 | $2.12 | — | $926.36 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; combo leftover $424.35; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 20 | $20.25 | $2.05 | — | $519.31 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; combo leftover $424.35; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 20 | $20.65 | $2.05 | — | $104.26 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; combo leftover $424.35; owner flatten_h5 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $104.26 | ▼ close $10,027.53 vs 09:30 $10,247.95 (session -179.95) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.26 | ▼ 09:30 equity $9,921.80 vs yday $10,027.53 (-105.73) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `ILMN` | 1 | $253.79 | $2.01 | $+15.93 | $356.04 | ▲ +15.93 after sell → book $9,919.78; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `TWST` | 2 | $157.72 | $2.02 | $+8.57 | $669.46 | ▲ +8.57 after sell → book $9,917.77; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `RVTY` | 2 | $141.79 | $2.02 | $-15.65 | $951.03 | ▼ -15.65 after sell → book $9,915.75; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 29 | $10.39 | $2.10 | $-0.11 | $1,250.24 | ▼ -0.11 after sell → book $9,913.65; vs 09:30 mark -2.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 40 | $7.38 | $2.13 | $-12.64 | $1,543.31 | ▼ -12.64 after sell → book $9,911.52; vs 09:30 mark -2.13 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMN` | 8 | $33.82 | $2.03 | $-12.93 | $1,811.84 | ▼ -12.93 after sell → book $9,909.49; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `DGXX` | 17 | $4.12 | $0.77 | $-4.61 | $1,881.10 | ▼ -4.61 after sell → book $9,908.72; vs 09:30 mark -0.77 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 26 | $14.38 | $2.09 | $-39.00 | $2,252.90 | ▼ -39.00 after sell → book $9,906.63; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CMPX` | 342 | $1.17 | $4.48 | $-25.99 | $2,648.56 | ▼ -25.99 after sell → book $9,902.15; vs 09:30 mark -4.48 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 321 | $1.27 | $4.20 | $-17.98 | $3,052.02 | ▼ -17.98 after sell → book $9,897.95; vs 09:30 mark -4.20 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 10 | $39.27 | $2.04 | $-11.36 | $3,442.68 | ▼ -11.36 after sell → book $9,895.91; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 2 | $192.26 | $2.02 | $-13.05 | $3,825.19 | ▼ -13.05 after sell → book $9,893.89; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,825.19 | ▲ close $9,984.93 vs 09:30 $9,921.80 (session +91.04) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,857.25 | ▲ 09:30 equity $8,745.48 vs yday $8,742.35 (+3.13) | 09:30 open · cash $3,857.25 (unchanged overnight, no fees) · equity $8,745.48 vs prior close $8,742.35 (+3.13) | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 99 | $3.86 | $2.29 | — | $3,472.82 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; combo leftover $385.73; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 23 | $16.21 | $2.06 | — | $3,097.93 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $385.73; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 1 | $272.16 | $1.99 | — | $2,823.78 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+11.7; combo leftover $385.73; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 5 | $74.15 | $2.00 | — | $2,451.03 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.5; combo leftover $385.73; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 1 | $803.87 | $1.99 | — | $1,645.16 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+0.8; combo leftover $817.01; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 21 | $38.51 | $2.05 | — | $834.40 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.7; combo leftover $817.01; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 106 | $7.65 | $2.31 | — | $21.19 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; combo leftover $817.01; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.19 | ▼ close $8,687.27 vs 09:30 $8,745.48 (session -43.51) | 16:00 close · cash $21.19 · equity $8,687.27 vs 09:30 $8,745.48 (-58.21; session marks -43.51) · 27 name(s) marked open→close (per-name table). A×1 09:30 $171.98 → close $172.79 +0.81; ADMA×41 09:30 $9.52 → close $9.52 +0.00; ARQT×14 09:30 $26.27 → close $26.27 +0.00; CYPH×74 09:30 $4.00 → close $4.12 +8.51; DLO×8 09:30 $13.88 → close $13.88 +0.00; DXCM×3 09:30 $87.47 → close $87.47 +0.00; ECO×3 09:30 $78.22 → close $78.22 +0.00; EL×1 09:30 $95.37 → close $95.37 +0.00; FIVN×9 09:30 $36.66 → close $36.66 -0.00; FTRE×20 09:30 $20.02 → close $20.02 +0.00; GNRC×1 09:30 $198.05 → close $198.05 +0.00; HALO×3 09:30 $115.36 → close $113.90 -4.38; MGTX×22 09:30 $11.05 → close $11.05 +0.00; MKC×2 09:30 $47.82 → close $47.82 -0.00; OMER×19 09:30 $20.61 → close $20.08 -10.07; PACS×3 09:30 $41.46 → close $41.46 -0.00; RBRK×3 09:30 $113.80 → close $113.80 +0.00; TDC×4 09:30 $29.46 → close $29.46 -0.00; USFD×1 09:30 $93.82 → close $93.82 -0.00; VICR×1 09:30 $276.06 → close $276.06 -0.00; ZSQR×99 09:30 $3.86 → close $3.78 -7.92; SECZ×23 09:30 $16.21 → close $15.96 -5.75; ILMN×1 09:30 $272.16 → close $270.00 -2.16; RKLB×5 09:30 $74.15 → close $73.95 -1.00; REGN×1 09:30 $803.87 → close $788.04 -15.83; BLFS×21 09:30 $38.51 → close $38.49 -0.42; MRVI×106 09:30 $7.65 → close $7.60 -5.30 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.19 | ▼ 09:30 equity $8,576.96 vs yday $8,687.27 (-110.31) | 09:30 open · cash $21.19 (unchanged overnight, no fees) · equity $8,576.96 vs prior close $8,687.27 (-110.31) | — |
| 2026-09-28 09:30 ET | **SELL** | `A` | 1 | $170.00 | $1.72 | $+8.83 | $189.47 | ▲ +8.83 after sell → book $8,575.24; vs 09:30 mark -1.72 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-28 09:30 ET | **SELL** | `CYPH` | 74 | $4.03 | $2.23 | $-2.04 | $485.64 | ▼ -2.04 after sell → book $8,573.00; vs 09:30 mark -2.24 | flatten_h5: dropped from list after 5 sess (min 5) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-28 09:30 ET | **SELL** | `DXCM` | 3 | $86.87 | $2.02 | $-9.90 | $744.23 | ▼ -9.90 after sell → book $8,570.98; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-28 09:30 ET | **SELL** | `ECO` | 3 | $79.55 | $2.02 | $-20.37 | $980.86 | ▼ -20.37 after sell → book $8,568.97; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-09-28 09:30 ET | **SELL** | `FIVN` | 9 | $34.75 | $2.04 | $-1.26 | $1,291.57 | ▼ -1.26 after sell → book $8,566.93; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-09-28 09:30 ET | **SELL** | `GNRC` | 1 | $207.41 | $2.01 | $-6.12 | $1,496.97 | ▼ -6.12 after sell → book $8,564.92; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-09-28 09:30 ET | **SELL** | `ILMN` | 1 | $265.95 | $2.01 | $-10.22 | $1,760.91 | ▼ -10.22 after sell → book $8,562.90; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `MGTX` | 22 | $10.79 | $2.08 | $-63.09 | $1,996.21 | ▼ -63.09 after sell → book $8,560.83; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-28 09:30 ET | **SELL** | `RBRK` | 3 | $106.94 | $2.02 | $-8.85 | $2,315.01 | ▼ -8.85 after sell → book $8,558.81; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-09-28 09:30 ET | **SELL** | `RKLB` | 5 | $73.20 | $2.02 | $-8.78 | $2,678.99 | ▼ -8.78 after sell → book $8,556.78; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `SECZ` | 23 | $16.00 | $2.08 | $-8.97 | $3,044.91 | ▼ -8.97 after sell → book $8,554.70; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `VICR` | 1 | $280.00 | $2.01 | $+56.37 | $3,322.89 | ▲ +56.37 after sell → book $8,552.69; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-09-28 09:30 ET | **SELL** | `ZSQR` | 99 | $3.71 | $2.31 | $-19.45 | $3,687.87 | ▼ -19.45 after sell → book $8,550.38; vs 09:30 mark -2.31 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,687.87 | ▲ close $8,617.78 vs 09:30 $8,576.96 (session +67.40) | 16:00 close · cash $3,687.87 · equity $8,617.78 vs 09:30 $8,576.96 (+40.82; session marks +67.40) · 14 name(s) marked open→close (per-name table). ADMA×41 09:30 $9.38 → close $10.12 +30.34; ARQT×14 09:30 $26.70 → close $27.33 +8.82; BLFS×21 09:30 $38.13 → close $39.15 +21.42; DLO×8 09:30 $13.91 → close $13.82 -0.72; EL×1 09:30 $92.92 → close $95.57 +2.65; FTRE×20 09:30 $19.54 → close $20.06 +10.40; HALO×3 09:30 $113.34 → close $112.89 -1.35; MKC×2 09:30 $47.83 → close $48.46 +1.26; MRVI×106 09:30 $7.49 → close $7.63 +14.84; OMER×19 09:30 $19.83 → close $19.43 -7.60; PACS×3 09:30 $41.27 → close $43.06 +5.37; REGN×1 09:30 $771.84 → close $752.25 -19.59; TDC×4 09:30 $28.34 → close $28.55 +0.84; USFD×1 09:30 $93.00 → close $93.72 +0.72 | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,687.87 | ▼ 09:30 equity $8,585.31 vs yday $8,617.78 (-32.47) | 09:30 open · cash $3,687.87 (unchanged overnight, no fees) · equity $8,585.31 vs prior close $8,617.78 (-32.47) | — |
| 2026-09-29 09:30 ET | **SELL** | `DLO` | 8 | $13.82 | $1.15 | $-7.77 | $3,797.28 | ▼ -7.77 after sell → book $8,584.16; vs 09:30 mark -1.15 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-29 09:30 ET | **SELL** | `EL` | 1 | $95.31 | $0.98 | $-4.67 | $3,891.61 | ▼ -4.67 after sell → book $8,583.18; vs 09:30 mark -0.98 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-29 09:30 ET | **SELL** | `MKC` | 2 | $48.14 | $0.99 | $-4.85 | $3,986.91 | ▼ -4.85 after sell → book $8,582.20; vs 09:30 mark -0.98 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-29 09:30 ET | **SELL** | `PACS` | 3 | $42.75 | $1.31 | $-0.12 | $4,113.84 | ▼ -0.12 after sell → book $8,580.88; vs 09:30 mark -1.32 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-29 09:30 ET | **SELL** | `TDC` | 4 | $29.03 | $1.19 | $-5.40 | $4,228.77 | ▼ -5.40 after sell → book $8,579.69; vs 09:30 mark -1.19 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-29 09:30 ET | **SELL** | `USFD` | 1 | $92.92 | $0.95 | $-3.06 | $4,320.74 | ▼ -3.06 after sell → book $8,578.74; vs 09:30 mark -0.95 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-29 09:30 ET | **BUY** | `ANNX` | 77 | $3.99 | $2.22 | — | $4,011.29 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=-5.0; combo leftover $308.62; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SGMT` | 31 | $9.92 | $2.08 | — | $3,701.68 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-8.0; combo leftover $308.62; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `ZSQR` | 69 | $4.47 | $2.20 | — | $3,391.06 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ⚪; ret5=+72.0; combo leftover $308.62; owner union_news_g_h1 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `LQDA` | 4 | $72.49 | $2.00 | — | $3,099.10 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; ret5=+10.9; combo leftover $308.62; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `ALVO` | 50 | $6.13 | $2.14 | — | $2,790.46 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; ret5=+4.6; combo leftover $308.62; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `GLND` | 77 | $3.98 | $2.22 | — | $2,481.77 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=+55.9; combo leftover $308.62; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `BURL` | 1 | $268.37 | $1.99 | — | $2,211.41 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+5.8; combo leftover $308.62; owner union_news_g_h1 | join🟢 sector🟡 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SN` | 2 | $184.05 | $2.00 | — | $1,841.32 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.7; combo leftover $368.57; owner flatten_h5 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `DT` | 6 | $57.39 | $2.01 | — | $1,494.97 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+2.6; combo leftover $368.57; owner flatten_h5 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `TOST` | 12 | $30.40 | $2.03 | — | $1,128.14 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+1.9; combo leftover $368.57; owner flatten_h5 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SONO` | 20 | $17.76 | $2.05 | — | $770.89 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+9.8; combo leftover $368.57; owner flatten_h5 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-29 09:30 ET | **BUY** | `PDFS` | 7 | $50.25 | $2.01 | — | $417.13 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.5; combo leftover $368.57; owner flatten_h5 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SHOO` | 8 | $45.06 | $2.01 | — | $54.64 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.8; combo leftover $368.57; owner flatten_h5 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.64 | ▲ close $8,568.00 vs 09:30 $8,585.31 (session +16.22) | 16:00 close · cash $54.64 · equity $8,568.00 vs 09:30 $8,585.31 (-17.31; session marks +16.22) · 21 name(s) marked open→close (per-name table). ADMA×41 09:30 $10.04 → close $9.96 -3.28; ARQT×14 09:30 $27.21 → close $26.75 -6.44; BLFS×21 09:30 $39.04 → close $39.22 +3.78; FTRE×20 09:30 $19.90 → close $19.74 -3.20; HALO×3 09:30 $112.89 → close $111.56 -3.99; MRVI×106 09:30 $7.52 → close $7.65 +13.78; OMER×19 09:30 $19.26 → close $19.25 -0.19; REGN×1 09:30 $745.85 → close $750.51 +4.66; ANNX×77 09:30 $3.99 → close $4.13 +10.78; SGMT×31 09:30 $9.92 → close $9.33 -18.29; ZSQR×69 09:30 $4.47 → close $4.15 -22.08; LQDA×4 09:30 $72.49 → close $70.69 -7.20; ALVO×50 09:30 $6.13 → close $5.92 -10.50; GLND×77 09:30 $3.98 → close $4.75 +59.29; BURL×1 09:30 $268.37 → close $270.05 +1.68; SN×2 09:30 $184.05 → close $182.44 -3.22; DT×6 09:30 $57.39 → close $57.53 +0.84; TOST×12 09:30 $30.40 → close $30.46 +0.72; SONO×20 09:30 $17.76 → close $17.85 +1.80; PDFS×7 09:30 $50.25 → close $49.77 -3.36; SHOO×8 09:30 $45.06 → close $45.14 +0.64 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.64 | ▲ 09:30 equity $8,568.00 vs yday $8,568.00 (+0.00) | 09:30 open · cash $54.64 (unchanged overnight, no fees) · equity $8,568.00 vs prior close $8,568.00 (+0.00) | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.64 | ▲ close $8,568.00 vs 09:30 $8,568.00 (session +0.00) | 16:00 close · cash $54.64 · equity $8,568.00 vs 09:30 $8,568.00 (+0.00; session marks +0.00) · 21 name(s) marked open→close (per-name table). ADMA×41 09:30 $9.96 → close $9.96 +0.00; ALVO×50 09:30 $5.92 → close $5.92 +0.00; ANNX×77 09:30 $4.13 → close $4.13 +0.00; ARQT×14 09:30 $26.75 → close $26.75 +0.00; BLFS×21 09:30 $39.22 → close $39.22 +0.00; BURL×1 09:30 $270.05 → close $270.05 +0.00; DT×6 09:30 $57.53 → close $57.53 +0.00; FTRE×20 09:30 $19.74 → close $19.74 +0.00; GLND×77 09:30 $4.75 → close $4.75 +0.00; HALO×3 09:30 $111.56 → close $111.56 +0.00; LQDA×4 09:30 $70.69 → close $70.69 +0.00; MRVI×106 09:30 $7.65 → close $7.65 +0.00; OMER×19 09:30 $19.25 → close $19.25 +0.00; PDFS×7 09:30 $49.77 → close $49.77 +0.00; REGN×1 09:30 $750.51 → close $750.51 +0.00; SGMT×31 09:30 $9.33 → close $9.33 +0.00; SHOO×8 09:30 $45.14 → close $45.14 +0.00; SN×2 09:30 $182.44 → close $182.44 +0.00; SONO×20 09:30 $17.85 → close $17.85 +0.00; TOST×12 09:30 $30.46 → close $30.46 +0.00; ZSQR×69 09:30 $4.15 → close $4.15 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.64 | ▼ 09:30 equity $8,417.72 vs yday $8,568.00 (-150.28) | 09:30 open · cash $54.64 (unchanged overnight, no fees) · equity $8,417.72 vs prior close $8,568.00 (-150.28) | — |
| 2026-10-01 09:30 ET | **SELL** | `ADMA` | 41 | $10.07 | $2.13 | $+6.41 | $465.38 | ▲ +6.41 after sell → book $8,415.59; vs 09:30 mark -2.13 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-10-01 09:30 ET | **SELL** | `ALVO` | 50 | $5.81 | $2.16 | $-20.30 | $753.72 | ▼ -20.30 after sell → book $8,413.43; vs 09:30 mark -2.16 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-10-01 09:30 ET | **SELL** | `ANNX` | 77 | $4.18 | $2.24 | $+10.17 | $1,073.33 | ▲ +10.17 after sell → book $8,411.18; vs 09:30 mark -2.25 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-10-01 09:30 ET | **SELL** | `ARQT` | 14 | $26.10 | $2.05 | $-27.74 | $1,436.68 | ▼ -27.74 after sell → book $8,409.13; vs 09:30 mark -2.05 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-10-01 09:30 ET | **SELL** | `BURL` | 1 | $269.84 | $2.01 | $-2.54 | $1,704.51 | ▼ -2.54 after sell → book $8,407.12; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-10-01 09:30 ET | **SELL** | `FTRE` | 20 | $19.88 | $2.07 | $-11.52 | $2,100.04 | ▼ -11.52 after sell → book $8,405.05; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-10-01 09:30 ET | **SELL** | `HALO` | 3 | $109.93 | $2.02 | $-24.78 | $2,427.81 | ▼ -24.78 after sell → book $8,403.03; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-10-01 09:30 ET | **SELL** | `LQDA` | 4 | $23.96 | $0.99 | $-197.11 | $2,522.66 | ▼ -197.11 after sell → book $8,402.04; vs 09:30 mark -0.99 | union_news_g_h1: dropped from list after 2 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `OMER` | 19 | $18.87 | $2.07 | $-37.93 | $2,879.12 | ▼ -37.93 after sell → book $8,399.97; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-10-01 09:30 ET | **SELL** | `SGMT` | 31 | $9.63 | $2.10 | $-13.18 | $3,175.55 | ▼ -13.18 after sell → book $8,397.87; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-10-01 09:30 ET | **SELL** | `ZSQR` | 69 | $4.22 | $2.22 | $-21.67 | $3,464.51 | ▼ -21.67 after sell → book $8,395.65; vs 09:30 mark -2.22 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-10-01 09:30 ET | **BUY** | `TLSA` | 390 | $1.11 | $5.03 | — | $3,026.58 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+5.7; combo leftover $433.06; owner union_news_g_h1 | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `NTAP` | 2 | $211.75 | $2.00 | — | $2,601.08 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+7.0; combo leftover $433.06; owner union_news_g_h1 | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `PRGS` | 10 | $40.52 | $2.02 | — | $2,193.86 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-2.6; combo leftover $433.06; owner union_news_g_h1 | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `IT` | 2 | $196.19 | $2.00 | — | $1,799.49 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.5; combo leftover $438.77; owner flatten_h5 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `KSPI` | 4 | $92.93 | $2.00 | — | $1,425.77 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+0.2; combo leftover $438.77; owner flatten_h5 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `IOT` | 11 | $38.99 | $2.02 | — | $994.85 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-2.4; combo leftover $438.77; owner flatten_h5 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `AVPT` | 30 | $14.27 | $2.08 | — | $564.67 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.3; combo leftover $438.77; owner flatten_h5 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `RELY` | 20 | $21.28 | $2.05 | — | $137.02 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+5.7; combo leftover $438.77; owner flatten_h5 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $137.02 | ▼ close $8,230.08 vs 09:30 $8,417.72 (session -146.37) | 16:00 close · cash $137.02 · equity $8,230.08 vs 09:30 $8,417.72 (-187.64; session marks -146.37) · 18 name(s) marked open→close (per-name table). BLFS×21 09:30 $38.90 → close $36.78 -44.52; DT×6 09:30 $59.04 → close $59.26 +1.32; GLND×77 09:30 $5.06 → close $4.40 -50.82; MRVI×106 09:30 $7.67 → close $7.50 -18.02; PDFS×7 09:30 $51.41 → close $53.39 +13.86; REGN×1 09:30 $766.53 → close $734.81 -31.72; SHOO×8 09:30 $44.46 → close $45.69 +9.84; SN×2 09:30 $182.44 → close $182.44 +0.00; SONO×20 09:30 $18.09 → close $17.75 -6.80; TOST×12 09:30 $29.05 → close $29.23 +2.16; TLSA×390 09:30 $1.11 → close $1.14 +11.70; NTAP×2 09:30 $211.75 → close $215.05 +6.60; PRGS×10 09:30 $40.52 → close $36.58 -39.40; IT×2 09:30 $196.19 → close $192.80 -6.78; KSPI×4 09:30 $92.93 → close $92.02 -3.64; IOT×11 09:30 $38.99 → close $40.04 +11.55; AVPT×30 09:30 $14.27 → close $14.08 -5.70; RELY×20 09:30 $21.28 → close $21.48 +4.00 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $137.02 | ▲ 09:30 equity $8,296.12 vs yday $8,230.08 (+66.04) | 09:30 open · cash $137.02 (unchanged overnight, no fees) · equity $8,296.12 vs prior close $8,230.08 (+66.04) | — |
| 2026-10-02 09:30 ET | **SELL** | `MRVI` | 106 | $7.62 | $2.34 | $-7.82 | $942.40 | ▼ -7.82 after sell → book $8,293.78; vs 09:30 mark -2.34 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-10-02 09:30 ET | **SELL** | `NTAP` | 2 | $216.43 | $2.02 | $+5.35 | $1,373.25 | ▲ +5.35 after sell → book $8,291.77; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `PRGS` | 10 | $36.88 | $2.04 | $-40.46 | $1,740.01 | ▼ -40.46 after sell → book $8,289.73; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `REGN` | 1 | $740.50 | $2.01 | $-67.38 | $2,478.50 | ▼ -67.38 after sell → book $8,287.72; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-10-02 09:30 ET | **SELL** | `TLSA` | 390 | $1.16 | $5.11 | $+9.36 | $2,925.79 | ▲ +9.36 after sell → book $8,282.61; vs 09:30 mark -5.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **BUY** | `WRBY` | 7 | $27.63 | $1.96 | — | $2,730.42 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ⚪; ret5=+4.6; combo leftover $208.98; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `CMPX` | 222 | $0.94 | $2.75 | — | $2,518.97 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-20.6; combo leftover $208.98; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `VEEV` | 1 | $283.10 | $1.99 | — | $2,233.88 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.6; combo leftover $503.79; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `CORT` | 4 | $114.38 | $2.00 | — | $1,774.35 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.8; combo leftover $503.79; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `ILMN` | 1 | $265.91 | $1.99 | — | $1,506.45 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-3.0; combo leftover $503.79; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `CDNA` | 7 | $66.33 | $2.01 | — | $1,040.13 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.9; combo leftover $503.79; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `ETON` | 9 | $52.42 | $2.02 | — | $566.33 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.6; combo leftover $503.79; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $566.33 | ▼ close $8,240.24 vs 09:30 $8,296.12 (session -27.64) | 16:00 close · cash $566.33 · equity $8,240.24 vs 09:30 $8,296.12 (-55.88; session marks -27.64) · 20 name(s) marked open→close (per-name table). AVPT×30 09:30 $14.22 → close $14.07 -4.50; BLFS×21 09:30 $37.02 → close $37.30 +5.88; DT×6 09:30 $59.45 → close $59.32 -0.78; GLND×77 09:30 $4.25 → close $3.73 -40.04; IOT×11 09:30 $40.41 → close $41.12 +7.81; IT×2 09:30 $192.74 → close $184.80 -15.88; KSPI×4 09:30 $92.05 → close $94.05 +8.00; PDFS×7 09:30 $55.24 → close $55.50 +1.82; RELY×20 09:30 $21.80 → close $21.77 -0.60; SHOO×8 09:30 $46.59 → close $45.97 -4.96; SN×2 09:30 $183.57 → close $182.57 -2.00; SONO×20 09:30 $17.88 → close $17.70 -3.60; TOST×12 09:30 $29.21 → close $29.79 +6.96; WRBY×7 09:30 $27.63 → close $27.03 -4.20; CMPX×222 09:30 $0.94 → close $0.86 -18.51; VEEV×1 09:30 $283.10 → close $273.33 -9.77; CORT×4 09:30 $114.38 → close $116.23 +7.40; ILMN×1 09:30 $265.91 → close $273.04 +7.13; CDNA×7 09:30 $66.33 → close $67.15 +5.74; ETON×9 09:30 $52.42 → close $55.36 +26.46 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $566.33 | ▲ 09:30 equity $8,263.34 vs yday $8,240.24 (+23.10) | 09:30 open · cash $566.33 (unchanged overnight, no fees) · equity $8,263.34 vs prior close $8,240.24 (+23.10) | — |
| 2026-10-05 09:30 ET | **SELL** | `BLFS` | 21 | $37.16 | $2.07 | $-32.48 | $1,344.62 | ▼ -32.48 after sell → book $8,261.26; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-10-05 09:30 ET | **SELL** | `CMPX` | 222 | $0.90 | $2.71 | $-14.37 | $1,541.71 | ▼ -14.37 after sell → book $8,258.55; vs 09:30 mark -2.71 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `GLND` | 77 | $3.92 | $2.24 | $-8.70 | $1,841.69 | ▼ -8.70 after sell → book $8,256.31; vs 09:30 mark -2.24 | union_news_g_h1: dropped from list after 4 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-05 09:30 ET | **SELL** | `WRBY` | 7 | $27.02 | $1.93 | $-8.16 | $2,028.89 | ▼ -8.16 after sell → book $8,254.37; vs 09:30 mark -1.94 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **BUY** | `COP` | 2 | $127.00 | $2.00 | — | $1,772.90 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=-0.4; combo leftover $338.15; owner union_news_g_h1 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `RRC` | 8 | $38.10 | $2.01 | — | $1,466.08 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=-1.0; combo leftover $338.15; owner union_news_g_h1 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `ELMT` | 15 | $22.04 | $2.04 | — | $1,133.45 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+12.7; combo leftover $338.15; owner union_news_g_h1 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `DVN` | 3 | $47.64 | $1.44 | — | $989.09 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.3; combo leftover $188.91; owner flatten_h5 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `EXE` | 2 | $84.70 | $1.70 | — | $817.99 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-1.5; combo leftover $188.91; owner flatten_h5 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `SM` | 5 | $35.27 | $1.78 | — | $639.86 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.9; combo leftover $188.91; owner flatten_h5 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `MTDR` | 3 | $53.35 | $1.61 | — | $478.20 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.8; combo leftover $188.91; owner flatten_h5 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `GPRK` | 17 | $10.87 | $1.90 | — | $291.51 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-3.1; combo leftover $188.91; owner flatten_h5 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `OBE` | 18 | $10.26 | $1.90 | — | $104.93 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-2.7; combo leftover $188.91; owner flatten_h5 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $104.93 | ▲ close $8,396.72 vs 09:30 $8,263.34 (session +158.72) | 16:00 close · cash $104.93 · equity $8,396.72 vs 09:30 $8,263.34 (+133.38; session marks +158.72) · 25 name(s) marked open→close (per-name table). AVPT×30 09:30 $13.91 → close $14.66 +22.50; CDNA×7 09:30 $66.90 → close $69.85 +20.65; CORT×4 09:30 $115.59 → close $122.03 +25.76; DT×6 09:30 $59.42 → close $60.21 +4.74; ETON×9 09:30 $55.85 → close $55.62 -2.07; ILMN×1 09:30 $273.00 → close $293.69 +20.69; IOT×11 09:30 $42.00 → close $42.39 +4.29; IT×2 09:30 $185.07 → close $187.76 +5.38; KSPI×4 09:30 $94.50 → close $93.98 -2.08; PDFS×7 09:30 $55.71 → close $56.08 +2.59; RELY×20 09:30 $21.76 → close $22.91 +23.00; SHOO×8 09:30 $46.06 → close $45.68 -3.04; SN×2 09:30 $182.88 → close $182.69 -0.38; SONO×20 09:30 $17.52 → close $17.94 +8.40; TOST×12 09:30 $29.20 → close $30.04 +10.08; VEEV×1 09:30 $275.00 → close $283.45 +8.45; COP×2 09:30 $127.00 → close $128.40 +2.80; RRC×8 09:30 $38.10 → close $38.67 +4.56; ELMT×15 09:30 $22.04 → close $21.29 -11.25; DVN×3 09:30 $47.64 → close $47.98 +1.02; EXE×2 09:30 $84.70 → close $86.43 +3.46; SM×5 09:30 $35.27 → close $35.05 -1.10; MTDR×3 09:30 $53.35 → close $53.01 -1.02; GPRK×17 09:30 $10.87 → close $11.28 +6.97; OBE×18 09:30 $10.26 → close $10.50 +4.32 | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.93 | ▲ 09:30 equity $8,448.69 vs yday $8,396.72 (+51.98) | 09:30 open · cash $104.93 (unchanged overnight, no fees) · equity $8,448.69 vs prior close $8,396.72 (+51.98) | — |
| 2026-10-06 09:30 ET | **SELL** | `COP` | 2 | $128.19 | $2.02 | $-1.63 | $359.29 | ▼ -1.63 after sell → book $8,446.68; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `DT` | 6 | $60.84 | $2.03 | $+16.66 | $722.31 | ▲ +16.66 after sell → book $8,444.65; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-10-06 09:30 ET | **SELL** | `ELMT` | 15 | $21.63 | $2.06 | $-10.24 | $1,044.70 | ▼ -10.24 after sell → book $8,442.60; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `RRC` | 8 | $38.71 | $2.03 | $+0.83 | $1,352.35 | ▲ +0.83 after sell → book $8,440.56; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `SHOO` | 8 | $45.88 | $2.03 | $+2.51 | $1,717.35 | ▲ +2.51 after sell → book $8,438.53; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-10-06 09:30 ET | **SELL** | `SONO` | 20 | $18.00 | $2.07 | $+0.68 | $2,075.28 | ▲ +0.68 after sell → book $8,436.46; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-10-06 09:30 ET | **BUY** | `ALVO` | 56 | $6.07 | $2.16 | — | $1,733.20 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.7; combo leftover $345.88; owner union_news_g_h1 | join🔴 sector🟡 gen🟡 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `SIBN` | 17 | $20.00 | $2.04 | — | $1,391.16 | — | union ∩ news_g, no 🚨; gate news=good; list probable; ret5=+5.6; combo leftover $345.88; owner union_news_g_h1 | join🟡 sector🟡 gen🟡 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `TWST` | 1 | $209.55 | $1.99 | — | $1,179.62 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+14.1; combo leftover $345.88; owner union_news_g_h1 | join🔴 sector🟡 gen🟡 news🟢 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `NTAP` | 5 | $224.80 | $2.00 | — | $53.62 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+9.5; combo leftover $1179.62; owner flatten_h5 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.62 | ▼ close $8,271.04 vs 09:30 $8,448.69 (session -157.22) | 16:00 close · cash $53.62 · equity $8,271.04 vs 09:30 $8,448.69 (-177.65; session marks -157.22) · 23 name(s) marked open→close (per-name table). AVPT×30 09:30 $14.77 → close $14.59 -5.40; CDNA×7 09:30 $70.89 → close $63.79 -49.70; CORT×4 09:30 $122.20 → close $119.57 -10.52; DVN×3 09:30 $47.48 → close $48.02 +1.64; ETON×9 09:30 $56.15 → close $54.77 -12.42; EXE×2 09:30 $86.38 → close $88.23 +3.70; GPRK×17 09:30 $11.32 → close $11.44 +2.04; ILMN×1 09:30 $301.58 → close $273.54 -28.04; IOT×11 09:30 $42.70 → close $41.74 -10.56; IT×2 09:30 $188.47 → close $184.92 -7.10; KSPI×4 09:30 $94.50 → close $93.16 -5.36; MTDR×3 09:30 $52.56 → close $52.91 +1.05; OBE×18 09:30 $10.44 → close $10.62 +3.24; PDFS×7 09:30 $56.89 → close $54.44 -17.15; RELY×20 09:30 $23.12 → close $23.20 +1.60; SM×5 09:30 $34.90 → close $35.10 +1.00; SN×2 09:30 $183.47 → close $184.72 +2.50; TOST×12 09:30 $30.07 → close $30.25 +2.16; VEEV×1 09:30 $285.34 → close $283.50 -1.84; ALVO×56 09:30 $6.07 → close $6.14 +3.92; SIBN×17 09:30 $20.00 → close $19.55 -7.65; TWST×1 09:30 $209.55 → close $166.97 -42.58; NTAP×5 09:30 $224.80 → close $228.45 +18.25 | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.62 | ▼ 09:30 equity $8,226.12 vs yday $8,271.04 (-44.92) | 09:30 open · cash $53.62 (unchanged overnight, no fees) · equity $8,226.12 vs prior close $8,271.04 (-44.92) | — |
| 2026-10-07 09:30 ET | **SELL** | `ALVO` | 56 | $6.10 | $2.18 | $-2.66 | $393.04 | ▼ -2.66 after sell → book $8,223.95; vs 09:30 mark -2.17 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `PDFS` | 7 | $52.54 | $2.03 | $+11.99 | $758.79 | ▲ +11.99 after sell → book $8,221.92; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-10-07 09:30 ET | **SELL** | `SIBN` | 17 | $19.27 | $2.06 | $-16.51 | $1,084.32 | ▼ -16.51 after sell → book $8,219.85; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `SN` | 2 | $183.00 | $2.02 | $-6.11 | $1,448.30 | ▼ -6.11 after sell → book $8,217.84; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-10-07 09:30 ET | **SELL** | `TOST` | 12 | $30.13 | $2.05 | $-7.31 | $1,807.82 | ▼ -7.31 after sell → book $8,215.79; vs 09:30 mark -2.05 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,807.82 | ▼ close $8,212.70 vs 09:30 $8,226.12 (session -3.09) | 16:00 close · cash $1,807.82 · equity $8,212.70 vs 09:30 $8,226.12 (-13.42; session marks -3.09) · 18 name(s) marked open→close (per-name table). AVPT×30 09:30 $14.51 → close $14.28 -6.90; CDNA×7 09:30 $61.38 → close $63.04 +11.61; CORT×4 09:30 $118.60 → close $119.60 +4.00; DVN×3 09:30 $48.40 → close $47.88 -1.56; ETON×9 09:30 $53.84 → close $54.88 +9.36; EXE×2 09:30 $88.97 → close $88.12 -1.70; GPRK×17 09:30 $11.50 → close $11.11 -6.63; ILMN×1 09:30 $271.61 → close $267.76 -3.85; IOT×11 09:30 $41.60 → close $40.51 -11.99; IT×2 09:30 $186.51 → close $185.77 -1.48; KSPI×4 09:30 $92.70 → close $91.87 -3.32; MTDR×3 09:30 $53.44 → close $52.85 -1.77; NTAP×5 09:30 $232.00 → close $235.77 +18.85; OBE×18 09:30 $10.72 → close $10.63 -1.62; RELY×20 09:30 $22.96 → close $22.93 -0.70; SM×5 09:30 $35.46 → close $35.25 -1.05; TWST×1 09:30 $158.26 → close $155.65 -2.61; VEEV×1 09:30 $284.23 → close $282.50 -1.73 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `IREN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TPG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `SLS` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `INO` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TLN` | cash | leftover split 6.10 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 6.10 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 6.10 < 1 share @ 120.00 |
| 2026-08-14 | `ARX` | cash | leftover split 6.10 < 1 share @ 19.57 |
| 2026-08-14 | `SNDK` | cash | leftover split 6.10 < 1 share @ 1646.93 |
| 2026-08-14 | `MH` | cash | leftover split 6.10 < 1 share @ 13.55 |
| 2026-08-14 | `HLIT` | cash | leftover split 6.10 < 1 share @ 13.18 |
| 2026-08-14 | `DAVE` | cash | leftover split 18.64 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 18.64 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `IREN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `TPG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `SLS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `INO` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `MARA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `LDI` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `BTBT` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `DVN` | cash | leftover split 4.32 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 4.32 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 4.32 < 1 share @ 202.70 |
| 2026-08-17 | `CELC` | cash | leftover split 4.32 < 1 share @ 92.99 |
| 2026-08-17 | `OUST` | cash | leftover split 4.32 < 1 share @ 49.00 |
| 2026-08-17 | `ELF` | cash | leftover split 8.65 < 1 share @ 90.54 |
| 2026-08-18 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `IREN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TPG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `SLS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `INO` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `MARA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `LDI` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `BTBT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `TMC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `TGB` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `DNN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `HNST` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-19 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `IREN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `TPG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `SLS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `INO` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `MARA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `LDI` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `BTBT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `TMC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `TGB` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `DNN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `HNST` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-20 | `MARA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `LDI` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `BTBT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `TMC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `TGB` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `DNN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `HNST` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-21 | `TMC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `TGB` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `DNN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `HNST` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `AG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `CDE` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `HDSN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `IAG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `KGC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `NFGC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `WPM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `DE` | cash | leftover split 341.96 < 1 share @ 623.26 |
| 2026-08-24 | `AG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `CDE` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `HDSN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `IAG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `KGC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `NFGC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `WPM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `AEM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-25 | `AG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `CDE` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `HDSN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `IAG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `KGC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `NFGC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `WPM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `AEM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `HCA` | cash | leftover split 239.22 < 1 share @ 426.97 |
| 2026-08-26 | `AG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `CDE` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `HDSN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `IAG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `KGC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `NFGC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `WPM` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `AEM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `FNV` | cash | leftover split 123.02 < 1 share @ 267.02 |
| 2026-08-27 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `AEM` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
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
| 2026-08-31 | `CRK` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `SLI` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-09-01 | `HCA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-01 | `CRK` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `SLI` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-02 | `CRK` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-02 | `SLI` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-03 | `DE` | cash | leftover split 699.77 < 1 share @ 703.25 |
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
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
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
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new long union_news_g_h1 |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new long union_news_g_h1 |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new long union_news_g_h1 |
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
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
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
| 2026-09-14 | `COHU` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new long union_news_g_h1 |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new long union_news_g_h1 |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new long union_news_g_h1 |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-15 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `OVID` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `SANM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `NVT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `COHU` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new long union_news_g_h1 |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new long union_news_g_h1 |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
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
| 2026-09-17 | `LITE` | cash | leftover split 222.50 < 1 share @ 934.88 |
| 2026-09-17 | `JBHT` | cash | leftover split 222.50 < 1 share @ 238.60 |
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
| 2026-09-18 | `DELL` | cash | leftover split 481.65 < 1 share @ 593.15 |
| 2026-09-21 | `IQV` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `ILMN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
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
| 2026-09-22 | `ILMN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
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
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMTX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `A` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `HUM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `DXCM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `MGTX` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `YSS` | no_price | no 09:30 open |
| 2026-09-22 | `MRNA` | cash | leftover split 74.69 < 1 share @ 168.50 |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-23 | `ILMN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
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
| 2026-09-23 | `USFD` | min_hold | flatten_h5: dropped but min-hold 1/5 |
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
| 2026-09-24 | `USFD` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-24 | `HALO` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `ARQT` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `ADMA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `FTRE` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `OMER` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `ZSQR` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RBRK` | 4 | 2026-09-18 @ $108.55 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; combo leftover $481.65; owner flatten_h5 |
| `GNRC` | 2 | 2026-09-18 @ $209.52 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; combo leftover $481.65; owner flatten_h5 |
| `VICR` | 2 | 2026-09-18 @ $219.62 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; combo leftover $481.65; owner flatten_h5 |
| `ECO` | 5 | 2026-09-18 @ $85.00 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; combo leftover $481.65; owner flatten_h5 |
| `FIVN` | 13 | 2026-09-18 @ $34.44 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; combo leftover $481.65; owner flatten_h5 |
| `A` | 2 | 2026-09-21 @ $157.87 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; combo leftover $417.81; owner flatten_h5 |
| `HUM` | 1 | 2026-09-21 @ $386.20 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-5.8; combo leftover $417.81; owner flatten_h5 |
| `DXCM` | 4 | 2026-09-21 @ $88.83 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; combo leftover $417.81; owner flatten_h5 |
| `MGTX` | 31 | 2026-09-21 @ $13.47 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; combo leftover $417.81; owner flatten_h5 |
| `CYPH` | 104 | 2026-09-21 @ $4.00 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; combo leftover $417.81; owner flatten_h5 |
| `USFD` | 1 | 2026-09-22 @ $93.97 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-0.6; combo leftover $124.62; owner flatten_h5 |
| `HALO` | 3 | 2026-09-23 @ $116.85 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; combo leftover $424.35; owner flatten_h5 |
| `ARQT` | 15 | 2026-09-23 @ $27.79 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; combo leftover $424.35; owner flatten_h5 |
| `ADMA` | 43 | 2026-09-23 @ $9.81 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; combo leftover $424.35; owner flatten_h5 |
| `FTRE` | 20 | 2026-09-23 @ $20.25 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; combo leftover $424.35; owner flatten_h5 |
| `OMER` | 20 | 2026-09-23 @ $20.65 | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; combo leftover $424.35; owner flatten_h5 |
