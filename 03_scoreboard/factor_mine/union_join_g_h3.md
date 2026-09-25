# Factor mine action — `union_join_g_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ join_g, no 🚨

Cash book **-23.35%** ($7,665) · signal-only (no cash/fees) was +0.61%. Starts YES **2/30**. Fills 194 · skips 306 · realized $-674.41.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the join camera (do several factors agree?) is green.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `join=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,148.13.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=-13.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+0.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+9.2; leftover $12.19 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.95 | ▲ close $10,435.42 vs 09:30 $10,178.12 (session +257.69) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.95 | ▼ 09:30 equity $10,414.78 vs yday $10,435.42 (-20.64) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $59.85 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=-12.3; leftover $7.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 2 | $3.24 | $0.07 | — | $53.30 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+0.3; leftover $7.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 1 | $5.07 | $0.05 | — | $48.18 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=-4.7; leftover $7.99 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.18 | ▲ close $10,525.00 vs 09:30 $10,414.78 (session +110.38) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.18 | ▼ 09:30 equity $10,391.53 vs yday $10,525.00 (-133.47) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $1,246.11 | ▼ -0.12 after sell → book $10,389.46; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $2,420.14 | ▼ -69.50 after sell → book $10,387.37; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,660.54 | ▲ +23.38 after sell → book $10,385.29; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $4,890.45 | ▼ -14.65 after sell → book $10,383.20; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $6,230.07 | ▲ +97.12 after sell → book $10,380.86; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $7,397.64 | ▼ -83.63 after sell → book $10,378.73; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $9,136.48 | ▲ +471.89 after sell → book $10,358.55; vs 09:30 mark -20.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $10,308.79 | ▼ -66.33 after sell → book $10,356.38; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,308.79 | ▼ close $10,355.26 vs 09:30 $10,391.53 (session -1.13) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,308.79 | ▲ 09:30 equity $10,355.41 vs yday $10,355.26 (+0.15) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,317.59 | ▼ -0.31 after sell → book $10,355.30; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 13 | $0.88 | $0.17 | $-1.08 | $10,328.86 | ▼ -1.08 after sell → book $10,355.13; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 8 | $1.42 | $0.16 | $-0.94 | $10,340.06 | ▼ -0.94 after sell → book $10,354.97; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,340.06 | ▼ close $10,354.93 vs 09:30 $10,355.41 (session -0.04) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,340.06 | ▼ 09:30 equity $10,354.83 vs yday $10,354.93 (-0.10) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 1 | $3.92 | $0.06 | $-0.24 | $10,343.92 | ▼ -0.24 after sell → book $10,354.77; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 2 | $3.20 | $0.09 | $-0.24 | $10,350.23 | ▼ -0.24 after sell → book $10,354.68; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NB` | 1 | $4.45 | $0.07 | $-0.74 | $10,354.61 | ▼ -0.74 after sell → book $10,354.61; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,078.34 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1294.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,802.16 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1294.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 62 | $20.65 | $2.18 | — | $6,519.69 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1294.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 224 | $5.77 | $2.89 | — | $5,224.32 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1294.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,946.18 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1294.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,669.97 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1294.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 739 | $1.75 | $9.53 | — | $1,367.19 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1294.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $208.86 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1294.33 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $208.86 | ▲ close $10,569.20 vs 09:30 $10,354.83 (session +239.71) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $208.86 | ▲ 09:30 equity $10,845.09 vs yday $10,569.20 (+275.89) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $191.48 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $26.11 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $168.99 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $26.11 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $144.02 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $26.11 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $118.64 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $26.11 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 19 | $1.32 | $0.31 | — | $93.25 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $26.11 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $93.25 | ▲ close $10,844.10 vs 09:30 $10,845.09 (session +0.29) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.25 | ▲ 09:30 equity $10,955.84 vs yday $10,844.10 (+111.74) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $93.25 | ▼ close $10,921.08 vs 09:30 $10,955.84 (session -34.76) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.25 | ▼ 09:30 equity $10,750.46 vs yday $10,921.08 (-170.62) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.32 | $2.20 | $-18.63 | $1,350.89 | ▼ -18.63 after sell → book $10,748.26; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.86 | $2.05 | $+63.82 | $2,690.88 | ▲ +63.82 after sell → book $10,746.21; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 62 | $20.47 | $2.20 | $-15.53 | $3,957.82 | ▼ -15.53 after sell → book $10,744.01; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 224 | $5.53 | $2.94 | $-59.59 | $5,193.61 | ▼ -59.59 after sell → book $10,741.08; vs 09:30 mark -2.93 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.21 | $2.21 | $+98.31 | $6,570.05 | ▲ +98.31 after sell → book $10,738.87; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.32 | $2.14 | $+111.41 | $7,957.67 | ▲ +111.41 after sell → book $10,736.73; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 739 | $1.90 | $9.67 | $+91.65 | $9,352.10 | ▲ +91.65 after sell → book $10,727.06; vs 09:30 mark -9.67 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $10,602.15 | ▲ +91.71 after sell → book $10,725.03; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 63 | $23.77 | $2.18 | — | $9,102.46 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+13.0; leftover $1514.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 137 | $10.98 | $2.40 | — | $7,595.80 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+1.2; leftover $1514.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 24 | $61.19 | $2.06 | — | $6,125.18 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+7.4; leftover $1514.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 181 | $8.35 | $2.53 | — | $4,611.29 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1514.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 306 | $4.94 | $3.95 | — | $3,095.71 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+7.1; leftover $1514.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $1,812.80 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+6.0; leftover $1514.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 929 | $1.63 | $11.98 | — | $286.54 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1514.59 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $286.54 | ▲ close $10,876.85 vs 09:30 $10,750.46 (session +178.93) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $286.54 | ▲ 09:30 equity $10,902.12 vs yday $10,876.85 (+25.27) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $302.95 | ▼ -0.96 after sell → book $10,901.93; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $333.32 | ▲ +7.88 after sell → book $10,901.60; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $357.13 | ▼ -1.17 after sell → book $10,901.30; vs 09:30 mark -0.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 13 | $2.03 | $0.32 | $+0.69 | $383.20 | ▲ +0.69 after sell → book $10,900.98; vs 09:30 mark -0.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 19 | $1.60 | $0.38 | $+4.63 | $413.22 | ▲ +4.63 after sell → book $10,900.60; vs 09:30 mark -0.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 6 | $9.83 | $0.61 | — | $353.63 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $59.03 | — |
| 2026-08-26 09:30 ET | **BUY** | `AVEX` | 3 | $17.51 | $0.53 | — | $300.56 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $59.03 | — |
| 2026-08-26 09:30 ET | **BUY** | `BZ` | 3 | $16.77 | $0.51 | — | $249.74 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $59.03 | — |
| 2026-08-26 09:30 ET | **BUY** | `MAIR` | 2 | $27.59 | $0.56 | — | $194.00 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer; ret5=+2.0; leftover $59.03 | — |
| 2026-08-26 09:30 ET | **BUY** | `BRR` | 26 | $2.20 | $0.65 | — | $136.15 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer; 🔵; ret5=+17.8; leftover $59.03 | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 101 | $0.58 | $0.89 | — | $76.38 | — | union ∩ join_g, no 🚨; gate join=good; list yday_mover; 🔵; ret5=-27.5; leftover $59.03 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.38 | ▼ close $10,827.26 vs 09:30 $10,902.12 (session -69.58) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.38 | ▲ 09:30 equity $10,850.35 vs yday $10,827.26 (+23.09) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 1 | $14.42 | $0.15 | — | $61.81 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+7.1; leftover $19.09 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 7 | $2.60 | $0.20 | — | $43.41 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot; ret5=+13.0; leftover $19.09 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.41 | ▼ close $10,696.44 vs 09:30 $10,850.35 (session -153.56) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.41 | ▲ 09:30 equity $10,730.18 vs yday $10,696.44 (+33.74) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 63 | $23.95 | $2.20 | $+6.96 | $1,550.06 | ▲ +6.96 after sell → book $10,727.98; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 137 | $10.97 | $2.44 | $-6.21 | $3,050.51 | ▼ -6.21 after sell → book $10,725.54; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 24 | $60.52 | $2.08 | $-20.23 | $4,500.91 | ▼ -20.23 after sell → book $10,723.46; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 181 | $8.28 | $2.58 | $-17.78 | $5,997.01 | ▼ -17.78 after sell → book $10,720.88; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 306 | $4.95 | $4.01 | $-4.90 | $7,507.70 | ▼ -4.90 after sell → book $10,716.87; vs 09:30 mark -4.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $423.76 | $2.02 | $-13.65 | $8,776.96 | ▼ -13.65 after sell → book $10,714.85; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 929 | $1.69 | $12.15 | $+31.61 | $10,334.82 | ▲ +31.61 after sell → book $10,702.70; vs 09:30 mark -12.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 44 | $32.90 | $2.12 | — | $8,885.10 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1476.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 94 | $15.66 | $2.27 | — | $7,410.79 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1476.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 18 | $79.42 | $2.04 | — | $5,979.18 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1476.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $4,715.98 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1476.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 10 | $146.07 | $2.02 | — | $3,253.26 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1476.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 10 | $141.76 | $2.02 | — | $1,833.64 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1476.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 161 | $9.13 | $2.47 | — | $361.24 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer; 🔵; ret5=+20.0; leftover $1476.40 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $361.24 | ▼ close $10,356.10 vs 09:30 $10,730.18 (session -331.65) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $361.24 | ▼ 09:30 equity $10,329.01 vs yday $10,356.10 (-27.09) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ABX` | 6 | $9.74 | $0.62 | $-1.77 | $419.05 | ▼ -1.77 after sell → book $10,328.38; vs 09:30 mark -0.63 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVEX` | 3 | $17.63 | $0.56 | $-0.73 | $471.39 | ▼ -0.73 after sell → book $10,327.83; vs 09:30 mark -0.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 3 | $17.70 | $0.56 | $+1.72 | $523.93 | ▲ +1.72 after sell → book $10,327.27; vs 09:30 mark -0.56 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `MAIR` | 2 | $26.28 | $0.55 | $-3.73 | $575.93 | ▼ -3.73 after sell → book $10,326.71; vs 09:30 mark -0.56 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BRR` | 26 | $2.23 | $0.68 | $-0.55 | $633.24 | ▼ -0.55 after sell → book $10,326.04; vs 09:30 mark -0.67 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 101 | $0.51 | $0.84 | $-9.11 | $683.90 | ▼ -9.11 after sell → book $10,325.19; vs 09:30 mark -0.85 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $683.90 | ▲ close $10,343.78 vs 09:30 $10,329.01 (session +18.59) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $683.90 | ▼ 09:30 equity $10,164.40 vs yday $10,343.78 (-179.38) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 1 | $15.82 | $0.18 | $+1.07 | $699.54 | ▲ +1.07 after sell → book $10,164.22; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 7 | $2.67 | $0.23 | $+0.06 | $718.00 | ▲ +0.06 after sell → book $10,163.99; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $718.00 | ▲ close $10,222.63 vs 09:30 $10,164.40 (session +58.64) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $718.00 | ▼ 09:30 equity $10,182.66 vs yday $10,222.63 (-39.97) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 44 | $32.42 | $2.14 | $-25.39 | $2,142.34 | ▼ -25.39 after sell → book $10,180.52; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 94 | $13.92 | $2.30 | $-168.13 | $3,448.52 | ▼ -168.13 after sell → book $10,178.22; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 18 | $78.84 | $2.07 | $-14.55 | $4,865.58 | ▼ -14.55 after sell → book $10,176.16; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SIMO` | 5 | $235.71 | $2.02 | $-86.68 | $6,042.10 | ▼ -86.68 after sell → book $10,174.13; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 10 | $139.65 | $2.04 | $-68.26 | $7,436.56 | ▼ -68.26 after sell → book $10,172.09; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 10 | $133.00 | $2.04 | $-91.66 | $8,764.52 | ▼ -91.66 after sell → book $10,170.05; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `VYX` | 161 | $8.73 | $2.51 | $-69.38 | $10,167.54 | ▼ -69.38 after sell → book $10,167.54; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,167.54 | ▲ close $10,167.54 vs 09:30 $10,182.66 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,167.54 | ▲ 09:30 equity $10,167.54 vs yday $10,167.54 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $52.88 | $2.06 | — | $8,896.36 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1270.94 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $7,649.31 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1270.94 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 350 | $3.63 | $4.51 | — | $6,374.29 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1270.94 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 158 | $8.03 | $2.46 | — | $5,103.09 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1270.94 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $3,909.02 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1270.94 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 82 | $15.45 | $2.24 | — | $2,639.89 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1270.94 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $1,470.31 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1270.94 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 75 | $16.77 | $2.21 | — | $210.35 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1270.94 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $210.35 | ▼ close $9,921.70 vs 09:30 $10,167.54 (session -226.24) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $210.35 | ▲ 09:30 equity $9,923.33 vs yday $9,921.70 (+1.63) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 13 | $2.52 | $0.37 | — | $177.22 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $35.06 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 5 | $6.71 | $0.35 | — | $143.32 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $35.06 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 18 | $1.90 | $0.40 | — | $108.72 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $35.06 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 7 | $4.78 | $0.36 | — | $74.91 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $35.06 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 22 | $1.59 | $0.42 | — | $39.51 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $35.06 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 3 | $11.31 | $0.35 | — | $5.23 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $35.06 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.23 | ▲ close $9,953.13 vs 09:30 $9,923.33 (session +32.03) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.23 | ▲ 09:30 equity $9,983.57 vs yday $9,953.13 (+30.44) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.23 | ▼ close $9,814.85 vs 09:30 $9,983.57 (session -168.72) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.23 | ▼ 09:30 equity $9,766.13 vs yday $9,814.85 (-48.72) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 24 | $53.16 | $2.08 | $+2.58 | $1,278.99 | ▲ +2.58 after sell → book $9,764.05; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 29 | $42.01 | $2.10 | $-30.85 | $2,495.19 | ▼ -30.85 after sell → book $9,761.96; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 350 | $3.28 | $4.58 | $-131.60 | $3,638.60 | ▼ -131.60 after sell → book $9,757.37; vs 09:30 mark -4.59 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 158 | $8.01 | $2.50 | $-8.12 | $4,901.68 | ▼ -8.12 after sell → book $9,754.87; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $6,031.58 | ▼ -64.17 after sell → book $9,752.84; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 82 | $15.16 | $2.26 | $-28.28 | $7,272.44 | ▼ -28.28 after sell → book $9,750.58; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 8 | $140.29 | $2.03 | $-49.25 | $8,392.76 | ▼ -49.25 after sell → book $9,748.54; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 75 | $15.46 | $2.24 | $-102.70 | $9,550.02 | ▼ -102.70 after sell → book $9,746.30; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,550.02 | ▼ close $9,737.96 vs 09:30 $9,766.13 (session -8.35) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,550.02 | ▼ 09:30 equity $9,735.20 vs yday $9,737.96 (-2.76) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 13 | $2.22 | $0.35 | $-4.61 | $9,578.54 | ▼ -4.61 after sell → book $9,734.85; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 5 | $6.11 | $0.34 | $-3.69 | $9,608.75 | ▼ -3.69 after sell → book $9,734.51; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 18 | $1.83 | $0.40 | $-2.06 | $9,641.28 | ▼ -2.06 after sell → book $9,734.11; vs 09:30 mark -0.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 7 | $3.92 | $0.32 | $-6.68 | $9,668.42 | ▼ -6.68 after sell → book $9,733.79; vs 09:30 mark -0.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 22 | $1.53 | $0.42 | $-2.16 | $9,701.66 | ▼ -2.16 after sell → book $9,733.37; vs 09:30 mark -0.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 3 | $10.57 | $0.35 | $-2.91 | $9,733.02 | ▼ -2.91 after sell → book $9,733.02; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,733.02 | ▲ close $9,733.02 vs 09:30 $9,735.20 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,733.02 | ▲ 09:30 equity $9,733.02 vs yday $9,733.02 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 74 | $16.28 | $2.21 | — | $8,526.09 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=-1.1; leftover $1216.63 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 445 | $2.73 | $5.74 | — | $7,305.50 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=-3.0; leftover $1216.63 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 5 | $206.84 | $2.00 | — | $6,269.29 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+8.3; leftover $1216.63 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $5,116.27 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1216.63 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 7 | $157.78 | $2.01 | — | $4,009.80 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+4.7; leftover $1216.63 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 21 | $56.09 | $2.05 | — | $2,829.86 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+19.6; leftover $1216.63 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 256 | $4.75 | $3.30 | — | $1,610.56 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1216.63 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 51 | $23.63 | $2.14 | — | $403.28 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; ret5=-6.3; leftover $1216.63 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $403.28 | ▼ close $9,616.48 vs 09:30 $9,733.02 (session -95.06) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $403.28 | ▼ 09:30 equity $9,401.87 vs yday $9,616.48 (-214.61) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $403.28 | ▲ close $9,490.67 vs 09:30 $9,401.87 (session +88.81) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $403.28 | ▲ 09:30 equity $9,509.73 vs yday $9,490.67 (+19.06) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $403.28 | ▼ close $9,310.21 vs 09:30 $9,509.73 (session -199.52) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $403.28 | ▲ 09:30 equity $9,391.01 vs yday $9,310.21 (+80.80) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AUPH` | 74 | $16.16 | $2.23 | $-13.33 | $1,596.89 | ▼ -13.33 after sell → book $9,388.78; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `OVID` | 445 | $2.72 | $5.82 | $-16.01 | $2,801.47 | ▼ -16.01 after sell → book $9,382.96; vs 09:30 mark -5.82 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 5 | $194.84 | $2.02 | $-64.03 | $3,773.64 | ▼ -64.03 after sell → book $9,380.93; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 7 | $140.03 | $2.03 | $-174.84 | $4,751.82 | ▼ -174.84 after sell → book $9,378.90; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 7 | $147.79 | $2.03 | $-73.97 | $5,784.32 | ▼ -73.97 after sell → book $9,376.87; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COHU` | 21 | $51.29 | $2.07 | $-104.93 | $6,859.34 | ▼ -104.93 after sell → book $9,374.80; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 256 | $4.73 | $3.35 | $-11.78 | $8,066.86 | ▼ -11.78 after sell → book $9,371.44; vs 09:30 mark -3.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `TYRA` | 51 | $25.58 | $2.16 | $+95.14 | $9,369.28 | ▲ +95.14 after sell → book $9,369.28; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $8,283.72 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+4.0; leftover $1171.16 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 15 | $77.12 | $2.04 | — | $7,124.88 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot; ret5=+7.2; leftover $1171.16 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 81 | $14.31 | $2.23 | — | $5,963.54 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+4.8; leftover $1171.16 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 32 | $36.46 | $2.09 | — | $4,794.73 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+2.9; leftover $1171.16 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 62 | $18.61 | $2.18 | — | $3,638.74 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1171.16 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 64 | $18.21 | $2.18 | — | $2,471.11 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; ret5=-19.1; leftover $1171.16 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 17 | $68.79 | $2.04 | — | $1,299.64 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1171.16 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 199 | $5.87 | $2.59 | — | $128.93 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1171.16 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.93 | ▲ close $9,535.24 vs 09:30 $9,391.01 (session +183.30) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.93 | ▲ 09:30 equity $9,694.90 vs yday $9,535.24 (+159.66) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 1 | $10.25 | $0.11 | — | $118.57 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $16.12 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 2 | $7.59 | $0.16 | — | $103.23 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $16.12 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $103.23 | ▲ close $9,754.62 vs 09:30 $9,694.90 (session +59.99) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $103.23 | ▲ 09:30 equity $9,778.60 vs yday $9,754.62 (+23.98) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 3 | $3.95 | $0.13 | — | $91.25 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $12.90 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 2 | $5.83 | $0.12 | — | $79.47 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $12.90 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.47 | ▼ close $9,706.96 vs 09:30 $9,778.60 (session -71.39) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.47 | ▲ 09:30 equity $9,769.92 vs yday $9,706.96 (+62.96) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 4 | $266.76 | $2.02 | $-20.54 | $1,144.49 | ▼ -20.54 after sell → book $9,767.90; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 15 | $76.27 | $2.06 | $-16.84 | $2,286.49 | ▼ -16.84 after sell → book $9,765.85; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAH` | 81 | $13.65 | $2.26 | $-57.95 | $3,389.88 | ▼ -57.95 after sell → book $9,763.59; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 32 | $36.70 | $2.11 | $+3.49 | $4,562.17 | ▲ +3.49 after sell → book $9,761.48; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BBNX` | 62 | $22.11 | $2.20 | $+212.63 | $5,930.80 | ▲ +212.63 after sell → book $9,759.29; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARQQ` | 64 | $20.55 | $2.20 | $+145.37 | $7,243.79 | ▲ +145.37 after sell → book $9,757.08; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 17 | $79.08 | $2.06 | $+170.83 | $8,586.09 | ▲ +170.83 after sell → book $9,755.02; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 199 | $5.62 | $2.63 | $-54.97 | $9,701.84 | ▼ -54.97 after sell → book $9,752.39; vs 09:30 mark -2.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 7 | $157.87 | $2.01 | — | $8,594.74 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+6.5; leftover $1212.73 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 3 | $386.20 | $2.00 | — | $7,434.14 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=-5.8; leftover $1212.73 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 13 | $88.83 | $2.03 | — | $6,277.32 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+7.6; leftover $1212.73 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 130 | $9.31 | $2.38 | — | $5,064.64 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1212.73 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 89 | $13.47 | $2.26 | — | $3,863.11 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1212.73 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1092 | $1.11 | $14.09 | — | $2,636.90 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1212.73 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 121 | $9.99 | $2.35 | — | $1,425.76 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1212.73 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 664 | $1.82 | $8.57 | — | $205.39 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1212.73 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $205.39 | ▼ close $9,605.03 vs 09:30 $9,769.92 (session -111.67) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $205.39 | ▼ 09:30 equity $9,579.41 vs yday $9,605.03 (-25.62) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 1 | $10.18 | $0.12 | $-0.30 | $215.45 | ▼ -0.30 after sell → book $9,579.29; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 46 | $0.58 | $0.40 | — | $188.36 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $26.93 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $188.36 | ▲ close $9,789.58 vs 09:30 $9,579.41 (session +210.70) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $188.36 | ▼ 09:30 equity $9,780.45 vs yday $9,789.58 (-9.13) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `EYPT` | 3 | $4.10 | $0.15 | $+0.17 | $200.51 | ▲ +0.17 after sell → book $9,780.30; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BNC` | 2 | $6.29 | $0.15 | $+0.65 | $212.94 | ▲ +0.65 after sell → book $9,780.15; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 1 | $27.79 | $0.28 | — | $184.87 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+7.0; leftover $42.59 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 4 | $9.81 | $0.40 | — | $145.23 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+4.0; leftover $42.59 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 2 | $20.25 | $0.41 | — | $104.31 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+15.0; leftover $42.59 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 2 | $20.65 | $0.42 | — | $62.60 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $42.59 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $62.60 | ▼ close $9,430.51 vs 09:30 $9,780.45 (session -348.13) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.60 | ▼ 09:30 equity $9,353.12 vs yday $9,430.51 (-77.39) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 2 | $7.38 | $0.17 | $-0.75 | $77.18 | ▼ -0.75 after sell → book $9,352.95; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 7 | $163.95 | $2.03 | $+38.52 | $1,222.80 | ▲ +38.52 after sell → book $9,350.91; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `HUM` | 3 | $374.54 | $2.02 | $-39.00 | $2,344.40 | ▼ -39.00 after sell → book $9,348.90; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 13 | $87.67 | $2.05 | $-19.09 | $3,482.13 | ▼ -19.09 after sell → book $9,346.85; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 130 | $8.67 | $2.41 | $-87.99 | $4,606.82 | ▼ -87.99 after sell → book $9,344.43; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 89 | $12.26 | $2.28 | $-112.67 | $5,695.67 | ▼ -112.67 after sell → book $9,342.15; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ORBS` | 1092 | $1.05 | $14.28 | $-93.88 | $6,828.00 | ▼ -93.88 after sell → book $9,327.87; vs 09:30 mark -14.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SBET` | 121 | $9.80 | $2.38 | $-27.73 | $8,011.41 | ▼ -27.73 after sell → book $9,325.49; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTBT` | 664 | $1.73 | $8.69 | $-83.65 | $9,148.13 | ▼ -83.65 after sell → book $9,316.81; vs 09:30 mark -8.68 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,148.13 | ▲ close $9,317.25 vs 09:30 $9,353.12 (session +0.44) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,350.86 | ▲ 09:30 equity $7,806.46 vs yday $7,805.98 (+0.48) | 09:30 open · cash $7,350.86 (unchanged overnight, no fees) · equity $7,806.46 vs prior close $7,805.98 (+0.48) · 11 name(s) re-marked at the open (per-name table). ADMA×3 yday $9.52 → 09:30 $9.52 +0.00; APPS×5 yday $10.88 → 09:30 $10.88 +0.00; ARHS×6 yday $9.47 → 09:30 $9.47 +0.00; ARQT×1 yday $26.27 → 09:30 $26.27 +0.00; DEFT×105 yday $0.53 → 09:30 $0.53 +0.00; DLO×4 yday $13.88 → 09:30 $13.88 +0.00; FTRE×1 yday $20.02 → 09:30 $20.02 +0.00; MKC×1 yday $47.82 → 09:30 $47.82 +0.00; OMER×1 yday $20.13 → 09:30 $20.61 +0.48; PGEN×4 yday $7.70 → 09:30 $7.70 +0.00; TDC×2 yday $29.46 → 09:30 $29.46 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 1 | $803.87 | $1.99 | — | $6,545.00 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+0.8; leftover $1050.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 9 | $115.36 | $2.02 | — | $5,504.74 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1050.12 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 27 | $38.51 | $2.07 | — | $4,462.90 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+4.7; leftover $1050.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 137 | $7.65 | $2.40 | — | $3,412.45 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1050.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 39 | $26.27 | $2.11 | — | $2,385.81 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1050.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $1,378.66 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1050.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BRVE` | 44 | $23.58 | $2.12 | — | $339.02 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $1050.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $339.02 | ▼ close $7,665.15 vs 09:30 $7,806.46 (session -126.57) | 16:00 close · cash $339.02 · equity $7,665.15 vs 09:30 $7,806.46 (-141.31; session marks -126.57) · 18 name(s) marked open→close (per-name table). ADMA×3 09:30 $9.52 → close $9.52 +0.00; APPS×5 09:30 $10.88 → close $10.88 +0.00; ARHS×6 09:30 $9.47 → close $9.47 +0.00; ARQT×1 09:30 $26.27 → close $26.27 +0.00; DEFT×105 09:30 $0.53 → close $0.53 +0.00; DLO×4 09:30 $13.88 → close $13.88 +0.00; FTRE×1 09:30 $20.02 → close $20.02 +0.00; MKC×1 09:30 $47.82 → close $47.82 -0.00; OMER×1 09:30 $20.61 → close $20.08 -0.53; PGEN×4 09:30 $7.70 → close $7.70 -0.00; TDC×2 09:30 $29.46 → close $29.46 -0.00; REGN×1 09:30 $803.87 → close $788.04 -15.83; HALO×9 09:30 $115.36 → close $113.90 -13.14; BLFS×27 09:30 $38.51 → close $38.49 -0.54; MRVI×137 09:30 $7.65 → close $7.60 -6.85; WRBY×39 09:30 $26.27 → close $26.71 +17.16; TXG×12 09:30 $83.76 → close $85.71 +23.40; BRVE×44 09:30 $23.58 → close $20.62 -130.24 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 12.19 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 12.19 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 12.19 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 12.19 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 12.19 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 7.99 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 7.99 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 7.99 < 1 share @ 202.70 |
| 2026-08-17 | `TGB` | cash | leftover split 7.99 < 1 share @ 8.46 |
| 2026-08-17 | `ELF` | cash | leftover split 7.99 < 1 share @ 90.54 |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JKHY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ATHM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BABA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 26.11 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 26.11 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 26.11 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `OCUL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `INSP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RZLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `HCA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BE` | cash | leftover split 59.03 < 1 share @ 213.94 |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AVEX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `SLQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 19.09 < 1 share @ 41.44 |
| 2026-08-27 | `BE` | cash | leftover split 19.09 < 1 share @ 227.10 |
| 2026-08-28 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AVEX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MAIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `SLQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SLI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `VYX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `VYX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CNTB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `OVID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SANM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `COHU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `TYRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NTAP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CDW` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OVID` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `COHU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `TYRA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `IQV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BBNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ARQQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 16.12 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 16.12 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 16.12 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 16.12 < 1 share @ 34.93 |
| 2026-09-17 | `AXTI` | cash | leftover split 16.12 < 1 share @ 67.91 |
| 2026-09-17 | `ARQT` | cash | leftover split 16.12 < 1 share @ 25.95 |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ARQQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 12.90 < 1 share @ 108.55 |
| 2026-09-18 | `GNRC` | cash | leftover split 12.90 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 12.90 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 12.90 < 1 share @ 85.00 |
| 2026-09-18 | `FIVN` | cash | leftover split 12.90 < 1 share @ 34.44 |
| 2026-09-18 | `BHVN` | cash | leftover split 12.90 < 1 share @ 14.07 |
| 2026-09-21 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `HUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 26.93 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `MRAM` | no_price | no 09:30 open |
| 2026-09-23 | `HUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `HALO` | cash | leftover split 42.59 < 1 share @ 116.85 |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EGHT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TDTH` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DEFT` | 46 | 2026-09-22 @ $0.58 | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $26.93 |
| `ARQT` | 1 | 2026-09-23 @ $27.79 | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+7.0; leftover $42.59 |
| `ADMA` | 4 | 2026-09-23 @ $9.81 | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+4.0; leftover $42.59 |
| `FTRE` | 2 | 2026-09-23 @ $20.25 | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+15.0; leftover $42.59 |
| `OMER` | 2 | 2026-09-23 @ $20.65 | union ∩ join_g, no 🚨; gate join=good; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $42.59 |
