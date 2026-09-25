# Factor mine action — `union_clk_mom_break_peer_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `hot_score` · size `leftover` · sell `list` · S-boost `none` · Clock-B #1 mom+breakout+peer/sector (research; not KEEP)

Cash book **-5.33%** ($9,467) · signal-only (no cash/fees) was +1.42%. Starts YES **4/30**. Fills 237 · skips 96 · realized $-120.43.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how hot the prior tape looked.
- Must-have: Clock-B #1: moderate prior momentum, a completed 10-session breakout (or candle capture), and peer or sector camera green.
- Must-not: Clock-B #5 long veto: extreme prior extension plus diminishing progress or a failed breakout.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how hot the prior tape looked and keep the top 8.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `clk_mom_break_peer=True` · **rank** `hot_score` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,173.28.

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
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 86 | $16.50 | $2.25 | — | $8,578.75 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1428.57 | — |
| 2026-08-14 09:30 ET | **BUY** | `GEMI` | 366 | $3.90 | $4.72 | — | $7,146.63 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; 🔵; ⚪; ret5=+8.0; leftover $1428.57 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 24 | $57.61 | $2.06 | — | $5,761.93 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1428.57 | — |
| 2026-08-14 09:30 ET | **BUY** | `MRLN` | 344 | $4.15 | $4.44 | — | $4,329.89 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; 🔵; ret5=+4.0; leftover $1428.57 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 331 | $4.31 | $4.27 | — | $2,899.01 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1428.57 | — |
| 2026-08-14 09:30 ET | **BUY** | `YSS` | 142 | $10.06 | $2.42 | — | $1,468.08 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; 🔵; ⚪; ret5=+5.7; leftover $1428.57 | — |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $459.08 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable; 🔵; ⚪; ret5=+7.9; leftover $1428.57 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $459.08 | ▼ close $9,938.43 vs 09:30 $10,000.00 (session -39.42) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $459.08 | ▼ 09:30 equity $9,899.26 vs yday $9,938.43 (-39.17) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 86 | $15.73 | $2.27 | $-70.74 | $1,809.59 | ▼ -70.74 after sell → book $9,896.99; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `GEMI` | 366 | $3.89 | $4.79 | $-13.17 | $3,228.53 | ▼ -13.17 after sell → book $9,892.19; vs 09:30 mark -4.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 24 | $55.37 | $2.08 | $-57.90 | $4,555.33 | ▼ -57.90 after sell → book $9,890.11; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MRLN` | 344 | $3.75 | $4.50 | $-146.54 | $5,840.82 | ▼ -146.54 after sell → book $9,885.60; vs 09:30 mark -4.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 331 | $4.60 | $4.34 | $+87.38 | $7,359.09 | ▲ +87.38 after sell → book $9,881.27; vs 09:30 mark -4.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `YSS` | 142 | $10.36 | $2.45 | $+37.73 | $8,827.76 | ▲ +37.73 after sell → book $9,878.82; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $9,876.80 | ▲ +40.05 after sell → book $9,876.80; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 135 | $18.24 | $2.40 | — | $7,412.01 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $2469.20 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 152 | $16.20 | $2.45 | — | $4,947.16 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $2469.20 | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 53 | $46.18 | $2.15 | — | $2,497.47 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten; 🔵; ret5=+6.7; leftover $2469.20 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 12 | $202.70 | $2.03 | — | $63.04 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten; 🔵; ret5=+8.3; leftover $2469.20 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.04 | ▼ close $9,857.65 vs 09:30 $9,899.26 (session -10.13) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.04 | ▼ 09:30 equity $9,699.76 vs yday $9,857.65 (-157.89) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 135 | $16.20 | $2.43 | $-280.23 | $2,247.61 | ▼ -280.23 after sell → book $9,697.33; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 152 | $15.78 | $2.49 | $-68.78 | $4,643.68 | ▼ -68.78 after sell → book $9,694.84; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 53 | $48.00 | $2.18 | $+92.13 | $7,185.50 | ▲ +92.13 after sell → book $9,692.66; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 12 | $208.93 | $2.06 | $+70.68 | $9,690.60 | ▲ +70.68 after sell → book $9,690.60; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,690.60 | ▲ close $9,690.60 vs 09:30 $9,699.76 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,690.60 | ▲ 09:30 equity $9,690.60 vs yday $9,690.60 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,690.60 | ▲ close $9,690.60 vs 09:30 $9,690.60 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,690.60 | ▲ 09:30 equity $9,690.60 vs yday $9,690.60 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `CABA` | 398 | $3.04 | $5.13 | — | $8,475.55 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+8.8; leftover $1211.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $7,275.95 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1211.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $6,081.88 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1211.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 246 | $4.92 | $3.17 | — | $4,868.39 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1211.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $3,681.08 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1211.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `COTY` | 475 | $2.55 | $6.13 | — | $2,463.70 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; 🔵; ⚪; ret5=+9.8; leftover $1211.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 692 | $1.75 | $8.93 | — | $1,243.77 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1211.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 35 | $34.05 | $2.10 | — | $49.93 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; 🔵; ret5=+9.3; leftover $1211.33 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.93 | ▲ close $9,945.69 vs 09:30 $9,690.60 (session +286.99) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.93 | ▲ 09:30 equity $10,177.88 vs yday $9,945.69 (+232.19) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `CABA` | 398 | $3.20 | $5.21 | $+53.34 | $1,318.32 | ▲ +53.34 after sell → book $10,172.67; vs 09:30 mark -5.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 61 | $21.17 | $2.19 | $+89.57 | $2,607.50 | ▲ +89.57 after sell → book $10,170.48; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 58 | $21.90 | $2.18 | $+73.95 | $3,875.51 | ▲ +73.95 after sell → book $10,168.29; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 246 | $5.20 | $3.22 | $+62.48 | $5,151.49 | ▲ +62.48 after sell → book $10,165.07; vs 09:30 mark -3.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $6,436.16 | ▲ +97.36 after sell → book $10,162.94; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `COTY` | 475 | $2.71 | $6.22 | $+63.66 | $7,717.19 | ▲ +63.66 after sell → book $10,156.72; vs 09:30 mark -6.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 692 | $1.79 | $9.05 | $+9.70 | $8,946.82 | ▲ +9.70 after sell → book $10,147.67; vs 09:30 mark -9.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 35 | $34.31 | $2.12 | $+4.89 | $10,145.55 | ▲ +4.89 after sell → book $10,145.55; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `DXYZ` | 36 | $34.89 | $2.10 | — | $8,887.42 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.6; leftover $1268.19 | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1467 | $0.86 | $17.08 | — | $7,602.85 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1268.19 | — |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 407 | $3.11 | $5.25 | — | $6,331.83 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer; ret5=+7.1; leftover $1268.19 | — |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 9 | $127.43 | $2.02 | — | $5,182.94 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable; 🔵; ⚪; ret5=+7.9; leftover $1268.19 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 763 | $1.66 | $9.84 | — | $3,906.52 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1268.19 | — |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 38 | $33.36 | $2.10 | — | $2,636.74 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $1268.19 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,380.56 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1268.19 | — |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 521 | $2.43 | $6.72 | — | $107.81 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $1268.19 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.81 | ▼ close $10,067.42 vs 09:30 $10,177.88 (session -30.97) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.81 | ▼ 09:30 equity $10,017.36 vs yday $10,067.42 (-50.06) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `DXYZ` | 36 | $33.10 | $2.12 | $-68.66 | $1,297.30 | ▼ -68.66 after sell → book $10,015.25; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1467 | $0.89 | $17.71 | $+3.35 | $2,585.21 | ▲ +3.35 after sell → book $9,997.53; vs 09:30 mark -17.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GORO` | 407 | $3.20 | $5.33 | $+26.05 | $3,882.29 | ▲ +26.05 after sell → book $9,992.21; vs 09:30 mark -5.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 9 | $129.99 | $2.04 | $+18.99 | $5,050.16 | ▲ +18.99 after sell → book $9,990.17; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 763 | $1.55 | $9.98 | $-103.75 | $6,222.83 | ▼ -103.75 after sell → book $9,980.19; vs 09:30 mark -9.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 38 | $32.82 | $2.12 | $-24.75 | $7,467.87 | ▼ -24.75 after sell → book $9,978.07; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.75 | $2.07 | $-24.50 | $8,699.54 | ▼ -24.50 after sell → book $9,975.99; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HITI` | 521 | $2.45 | $6.82 | $-3.12 | $9,969.18 | ▼ -3.12 after sell → book $9,969.18; vs 09:30 mark -6.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,969.18 | ▲ close $9,969.18 vs 09:30 $10,017.36 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,969.18 | ▲ 09:30 equity $9,969.18 vs yday $9,969.18 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `VALE` | 94 | $15.01 | $2.27 | — | $8,555.96 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list mover_buy; ⚪; ret5=+9.4; leftover $1424.17 | — |
| 2026-08-25 09:30 ET | **BUY** | `SJM` | 11 | $123.87 | $2.02 | — | $7,191.37 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list overnight; ret5=+6.8; leftover $1424.17 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 104 | $13.59 | $2.30 | — | $5,775.71 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1424.17 | — |
| 2026-08-25 09:30 ET | **BUY** | `ANF` | 12 | $112.17 | $2.03 | — | $4,427.64 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list overnight; 🔵; ⚪; ret5=+6.8; leftover $1424.17 | — |
| 2026-08-25 09:30 ET | **BUY** | `TIGR` | 273 | $5.21 | $3.52 | — | $3,001.79 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list overnight; ret5=+7.4; leftover $1424.17 | — |
| 2026-08-25 09:30 ET | **BUY** | `BOX` | 42 | $33.33 | $2.12 | — | $1,599.81 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list overnight; 🔵; ret5=+3.7; leftover $1424.17 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $316.91 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten; ret5=+6.0; leftover $1424.17 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $316.91 | ▲ close $10,022.26 vs 09:30 $9,969.18 (session +69.34) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $316.91 | ▲ 09:30 equity $10,383.88 vs yday $10,022.26 (+361.62) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `VALE` | 94 | $15.37 | $2.30 | $+29.27 | $1,759.39 | ▲ +29.27 after sell → book $10,381.58; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 104 | $13.63 | $2.33 | $-0.47 | $3,174.58 | ▼ -0.47 after sell → book $10,379.25; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ANF` | 12 | $131.37 | $2.05 | $+226.33 | $4,748.97 | ▲ +226.33 after sell → book $10,377.20; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `TIGR` | 273 | $5.21 | $3.58 | $-7.10 | $6,167.72 | ▼ -7.10 after sell → book $10,373.62; vs 09:30 mark -3.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BOX` | 42 | $34.30 | $2.14 | $+36.49 | $7,606.18 | ▲ +36.49 after sell → book $10,371.48; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-2.43 | $8,886.66 | ▼ -2.43 after sell → book $10,369.46; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 150 | $9.83 | $2.44 | — | $7,409.72 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1481.11 | — |
| 2026-08-26 09:30 ET | **BUY** | `ACRS` | 226 | $6.53 | $2.92 | — | $5,931.03 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list yday_gainer; 🔵; ret5=+3.6; leftover $1481.11 | — |
| 2026-08-26 09:30 ET | **BUY** | `BZ` | 88 | $16.77 | $2.25 | — | $4,453.01 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $1481.11 | — |
| 2026-08-26 09:30 ET | **BUY** | `CRMD` | 172 | $8.60 | $2.51 | — | $2,971.31 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten; 🔵; ret5=+4.8; leftover $1481.11 | — |
| 2026-08-26 09:30 ET | **BUY** | `LI` | 122 | $12.14 | $2.36 | — | $1,487.87 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; ret5=+1.2; leftover $1481.11 | — |
| 2026-08-26 09:30 ET | **BUY** | `HQY` | 14 | $104.39 | $2.03 | — | $24.38 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list overnight; ret5=+2.5; leftover $1481.11 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.38 | ▲ close $10,374.18 vs 09:30 $10,383.88 (session +19.22) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.38 | ▼ 09:30 equity $10,254.69 vs yday $10,374.18 (-119.49) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `SJM` | 11 | $130.29 | $2.04 | $+66.55 | $1,455.52 | ▲ +66.55 after sell → book $10,252.64; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ABX` | 150 | $9.68 | $2.48 | $-27.42 | $2,905.05 | ▼ -27.42 after sell → book $10,250.17; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ACRS` | 226 | $6.15 | $2.96 | $-91.76 | $4,291.98 | ▼ -91.76 after sell → book $10,247.20; vs 09:30 mark -2.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 88 | $18.50 | $2.28 | $+147.70 | $5,917.70 | ▲ +147.70 after sell → book $10,244.92; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 172 | $8.49 | $2.55 | $-23.97 | $7,375.44 | ▼ -23.97 after sell → book $10,242.38; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `LI` | 122 | $12.35 | $2.39 | $+20.88 | $8,879.75 | ▲ +20.88 after sell → book $10,239.99; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HQY` | 14 | $97.16 | $2.05 | $-105.30 | $10,237.93 | ▼ -105.30 after sell → book $10,237.93; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `NCNO` | 58 | $22.03 | $2.16 | — | $8,958.03 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot,earn_react; 🔵; ret5=+4.0; leftover $1279.74 | — |
| 2026-08-27 09:30 ET | **BUY** | `NABL` | 330 | $3.87 | $4.26 | — | $7,676.67 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+9.8; leftover $1279.74 | — |
| 2026-08-27 09:30 ET | **BUY** | `INO` | 992 | $1.29 | $12.80 | — | $6,384.20 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+0.0; leftover $1279.74 | — |
| 2026-08-27 09:30 ET | **BUY** | `SRRK` | 21 | $60.00 | $2.05 | — | $5,122.14 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+6.2; leftover $1279.74 | — |
| 2026-08-27 09:30 ET | **BUY** | `PAGP` | 45 | $28.00 | $2.12 | — | $3,860.02 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+7.8; leftover $1279.74 | — |
| 2026-08-27 09:30 ET | **BUY** | `DASH` | 5 | $235.94 | $2.00 | — | $2,678.31 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+7.6; leftover $1279.74 | — |
| 2026-08-27 09:30 ET | **BUY** | `AEO` | 74 | $17.27 | $2.21 | — | $1,398.12 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+5.5; leftover $1279.74 | — |
| 2026-08-27 09:30 ET | **BUY** | `ULTA` | 2 | $536.07 | $2.00 | — | $323.99 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list overnight; ret5=+2.9; leftover $1279.74 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $323.99 | ▲ close $10,210.49 vs 09:30 $10,254.69 (session +2.16) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $323.99 | ▲ 09:30 equity $10,348.37 vs yday $10,210.49 (+137.88) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `NCNO` | 58 | $23.30 | $2.18 | $+69.31 | $1,673.20 | ▲ +69.31 after sell → book $10,346.18; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NABL` | 330 | $4.25 | $4.32 | $+116.82 | $3,071.38 | ▲ +116.82 after sell → book $10,341.86; vs 09:30 mark -4.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `INO` | 992 | $1.27 | $12.97 | $-45.61 | $4,318.25 | ▼ -45.61 after sell → book $10,328.89; vs 09:30 mark -12.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SRRK` | 21 | $58.75 | $2.07 | $-30.38 | $5,549.92 | ▼ -30.38 after sell → book $10,326.81; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PAGP` | 45 | $28.08 | $2.15 | $-0.67 | $6,811.38 | ▼ -0.67 after sell → book $10,324.67; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DASH` | 5 | $233.37 | $2.02 | $-16.88 | $7,976.20 | ▼ -16.88 after sell → book $10,322.64; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEO` | 74 | $17.06 | $2.23 | $-19.99 | $9,236.41 | ▼ -19.99 after sell → book $10,320.41; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 69 | $19.00 | $2.20 | — | $7,923.21 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+7.5; leftover $1319.49 | — |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 273 | $4.82 | $3.52 | — | $6,603.83 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.8; leftover $1319.49 | — |
| 2026-08-28 09:30 ET | **BUY** | `RBRK` | 13 | $98.95 | $2.03 | — | $5,315.45 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; ret5=+9.7; leftover $1319.49 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $4,007.65 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; ret5=+7.8; leftover $1319.49 | — |
| 2026-08-28 09:30 ET | **BUY** | `PD` | 100 | $13.09 | $2.29 | — | $2,696.36 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; ret5=+4.2; leftover $1319.49 | — |
| 2026-08-28 09:30 ET | **BUY** | `S` | 61 | $21.49 | $2.17 | — | $1,383.29 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; ret5=+8.5; leftover $1319.49 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 158 | $8.35 | $2.46 | — | $61.53 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list earn_react; ret5=+5.1; leftover $1319.49 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.53 | ▼ close $10,232.30 vs 09:30 $10,348.37 (session -71.43) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.53 | ▼ 09:30 equity $10,177.01 vs yday $10,232.30 (-55.29) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ULTA` | 2 | $521.10 | $2.02 | $-33.95 | $1,101.71 | ▼ -33.95 after sell → book $10,175.00; vs 09:30 mark -2.01 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 69 | $18.12 | $2.22 | $-64.79 | $2,350.12 | ▼ -64.79 after sell → book $10,172.78; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 273 | $4.81 | $3.58 | $-9.83 | $3,659.67 | ▼ -9.83 after sell → book $10,169.20; vs 09:30 mark -3.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RBRK` | 13 | $92.83 | $2.05 | $-83.64 | $4,864.41 | ▼ -83.64 after sell → book $10,167.15; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 5 | $257.71 | $2.03 | $-21.28 | $6,150.94 | ▼ -21.28 after sell → book $10,165.13; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PD` | 100 | $13.58 | $2.32 | $+44.39 | $7,506.62 | ▲ +44.39 after sell → book $10,162.81; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `S` | 61 | $21.45 | $2.19 | $-6.81 | $8,812.88 | ▼ -6.81 after sell → book $10,160.62; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 158 | $8.53 | $2.50 | $+23.48 | $10,158.12 | ▲ +23.48 after sell → book $10,158.12; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,158.12 | ▲ close $10,158.12 vs 09:30 $10,177.01 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,158.12 | ▲ 09:30 equity $10,158.12 vs yday $10,158.12 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,158.12 | ▲ close $10,158.12 vs 09:30 $10,158.12 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,158.12 | ▲ 09:30 equity $10,158.12 vs yday $10,158.12 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,158.12 | ▲ close $10,158.12 vs 09:30 $10,158.12 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,158.12 | ▲ 09:30 equity $10,158.12 vs yday $10,158.12 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `EBS` | 193 | $6.56 | $2.57 | — | $8,889.47 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ⚪; ret5=+8.2; leftover $1269.76 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,914.85 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list mover_buy; 🔵; ret5=+6.1; leftover $1269.76 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 349 | $3.63 | $4.50 | — | $6,643.48 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1269.76 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 158 | $8.03 | $2.46 | — | $5,372.27 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1269.76 | — |
| 2026-09-03 09:30 ET | **BUY** | `GALT` | 282 | $4.50 | $3.64 | — | $4,099.64 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.0; leftover $1269.76 | — |
| 2026-09-03 09:30 ET | **BUY** | `CTVA` | 14 | $90.24 | $2.03 | — | $2,834.24 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.6; leftover $1269.76 | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $52.88 | $2.06 | — | $1,563.06 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1269.76 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $316.02 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1269.76 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $316.02 | ▼ close $9,966.91 vs 09:30 $10,158.12 (session -169.87) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $316.02 | ▼ 09:30 equity $9,909.32 vs yday $9,966.91 (-57.59) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `EBS` | 193 | $6.26 | $2.61 | $-63.08 | $1,521.58 | ▼ -63.08 after sell → book $9,906.70; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CABA` | 349 | $3.46 | $4.57 | $-68.40 | $2,724.55 | ▼ -68.40 after sell → book $9,902.13; vs 09:30 mark -4.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 158 | $7.91 | $2.50 | $-23.92 | $3,971.83 | ▼ -23.92 after sell → book $9,899.63; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `GALT` | 282 | $4.33 | $3.69 | $-55.27 | $5,189.20 | ▼ -55.27 after sell → book $9,895.94; vs 09:30 mark -3.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTVA` | 14 | $87.64 | $2.05 | $-40.48 | $6,414.11 | ▼ -40.48 after sell → book $9,893.89; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 24 | $52.03 | $2.08 | $-24.54 | $7,660.75 | ▼ -24.54 after sell → book $9,891.81; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 29 | $41.50 | $2.10 | $-45.64 | $8,862.15 | ▼ -45.64 after sell → book $9,889.71; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `GORO` | 320 | $3.95 | $4.13 | — | $7,594.02 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+6.9; leftover $1266.02 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 9 | $137.35 | $2.02 | — | $6,355.85 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+5.4; leftover $1266.02 | — |
| 2026-09-04 09:30 ET | **BUY** | `BLSH` | 36 | $34.69 | $2.10 | — | $5,104.92 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+7.9; leftover $1266.02 | — |
| 2026-09-04 09:30 ET | **BUY** | `ZETA` | 38 | $32.65 | $2.10 | — | $3,862.11 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+8.1; leftover $1266.02 | — |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 141 | $8.94 | $2.41 | — | $2,599.16 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+7.7; leftover $1266.02 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 5 | $236.82 | $2.00 | — | $1,413.05 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+8.1; leftover $1266.02 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 16 | $75.65 | $2.04 | — | $200.62 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1266.02 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $200.62 | ▲ close $10,166.09 vs 09:30 $9,909.32 (session +293.18) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $200.62 | ▼ 09:30 equity $10,118.99 vs yday $10,166.09 (-47.10) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+65.67 | $1,240.90 | ▲ +65.67 after sell → book $10,116.97; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GORO` | 320 | $4.13 | $4.19 | $+49.28 | $2,558.31 | ▲ +49.28 after sell → book $10,112.78; vs 09:30 mark -4.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MSTR` | 9 | $137.62 | $2.04 | $-1.62 | $3,794.85 | ▼ -1.62 after sell → book $10,110.74; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BLSH` | 36 | $35.90 | $2.12 | $+39.34 | $5,085.13 | ▲ +39.34 after sell → book $10,108.62; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ZETA` | 38 | $31.08 | $2.12 | $-63.89 | $6,264.05 | ▼ -63.89 after sell → book $10,106.50; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 5 | $267.76 | $2.03 | $+150.67 | $7,600.82 | ▲ +150.67 after sell → book $10,104.47; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,600.82 | ▼ close $10,091.54 vs 09:30 $10,118.99 (session -12.93) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,600.82 | ▲ 09:30 equity $10,095.42 vs yday $10,091.54 (+3.88) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `HAFN` | 141 | $9.00 | $2.45 | $+3.60 | $8,867.38 | ▲ +3.60 after sell → book $10,092.98; vs 09:30 mark -2.44 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 16 | $76.60 | $2.06 | $+11.10 | $10,090.92 | ▲ +11.10 after sell → book $10,090.92; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,090.92 | ▲ close $10,090.92 vs 09:30 $10,095.42 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,090.92 | ▲ 09:30 equity $10,090.92 vs yday $10,090.92 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,090.92 | ▲ close $10,090.92 vs 09:30 $10,090.92 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,090.92 | ▲ 09:30 equity $10,090.92 vs yday $10,090.92 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 594 | $2.12 | $7.66 | — | $8,823.98 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1261.36 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 265 | $4.75 | $3.42 | — | $7,561.81 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1261.36 | — |
| 2026-09-11 09:30 ET | **BUY** | `INSP` | 18 | $69.88 | $2.04 | — | $6,301.92 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.0; leftover $1261.36 | — |
| 2026-09-11 09:30 ET | **BUY** | `GME` | 59 | $21.04 | $2.17 | — | $5,058.40 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+7.5; leftover $1261.36 | — |
| 2026-09-11 09:30 ET | **BUY** | `DHT` | 59 | $21.30 | $2.17 | — | $3,799.53 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+6.8; leftover $1261.36 | — |
| 2026-09-11 09:30 ET | **BUY** | `PBR` | 59 | $21.21 | $2.17 | — | $2,545.97 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+2.5; leftover $1261.36 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 618 | $2.04 | $7.97 | — | $1,277.28 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1261.36 | — |
| 2026-09-11 09:30 ET | **BUY** | `MYGN` | 374 | $3.37 | $4.82 | — | $12.08 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+4.0; leftover $1261.36 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.08 | ▲ close $10,156.81 vs 09:30 $10,090.92 (session +98.31) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.08 | ▼ 09:30 equity $10,128.43 vs yday $10,156.81 (-28.38) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 594 | $2.05 | $7.77 | $-57.01 | $1,222.00 | ▼ -57.01 after sell → book $10,120.65; vs 09:30 mark -7.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 265 | $4.82 | $3.47 | $+11.66 | $2,495.83 | ▲ +11.66 after sell → book $10,117.18; vs 09:30 mark -3.47 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INSP` | 18 | $72.14 | $2.06 | $+36.57 | $3,792.29 | ▲ +36.57 after sell → book $10,115.12; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PBR` | 59 | $21.23 | $2.19 | $-3.17 | $5,042.67 | ▼ -3.17 after sell → book $10,112.93; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 618 | $2.01 | $8.08 | $-34.60 | $6,276.77 | ▼ -34.60 after sell → book $10,104.85; vs 09:30 mark -8.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `MYGN` | 374 | $3.43 | $4.90 | $+12.72 | $7,554.69 | ▲ +12.72 after sell → book $10,099.95; vs 09:30 mark -4.90 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,554.69 | ▲ close $10,137.12 vs 09:30 $10,128.43 (session +37.17) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,554.69 | ▲ 09:30 equity $10,145.38 vs yday $10,137.12 (+8.26) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `GME` | 59 | $21.51 | $2.19 | $+23.38 | $8,821.59 | ▲ +23.38 after sell → book $10,143.19; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `DHT` | 59 | $22.40 | $2.19 | $+60.55 | $10,141.00 | ▲ +60.55 after sell → book $10,141.00; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,141.00 | ▲ close $10,141.00 vs 09:30 $10,145.38 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,141.00 | ▲ 09:30 equity $10,141.00 vs yday $10,141.00 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 46 | $27.09 | $2.13 | — | $8,892.74 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1267.63 | — |
| 2026-09-16 09:30 ET | **BUY** | `META` | 1 | $679.91 | $1.99 | — | $8,210.83 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+9.3; leftover $1267.63 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 31 | $39.99 | $2.08 | — | $6,969.06 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+9.3; leftover $1267.63 | — |
| 2026-09-16 09:30 ET | **BUY** | `TALO` | 70 | $17.87 | $2.20 | — | $5,715.96 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+6.8; leftover $1267.63 | — |
| 2026-09-16 09:30 ET | **BUY** | `KR` | 20 | $61.93 | $2.05 | — | $4,475.31 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.8; leftover $1267.63 | — |
| 2026-09-16 09:30 ET | **BUY** | `APA` | 27 | $46.44 | $2.07 | — | $3,219.36 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.9; leftover $1267.63 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 16 | $77.12 | $2.04 | — | $1,983.40 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,ohlc_hot; ret5=+7.2; leftover $1267.63 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 6 | $189.17 | $2.01 | — | $846.37 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+7.9; leftover $1267.63 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $846.37 | ▼ close $9,948.51 vs 09:30 $10,141.00 (session -175.92) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $846.37 | ▲ 09:30 equity $9,985.91 vs yday $9,948.51 (+37.40) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 46 | $28.23 | $2.15 | $+48.16 | $2,142.80 | ▲ +48.16 after sell → book $9,983.76; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `META` | 1 | $682.44 | $2.01 | $-1.48 | $2,823.23 | ▼ -1.48 after sell → book $9,981.75; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 31 | $37.57 | $2.10 | $-79.21 | $3,985.80 | ▼ -79.21 after sell → book $9,979.65; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TALO` | 70 | $17.19 | $2.22 | $-52.02 | $5,186.88 | ▼ -52.02 after sell → book $9,977.43; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `KR` | 20 | $61.02 | $2.07 | $-22.32 | $6,405.21 | ▼ -22.32 after sell → book $9,975.36; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `APA` | 27 | $44.63 | $2.09 | $-53.03 | $7,608.13 | ▼ -53.03 after sell → book $9,973.27; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 16 | $76.44 | $2.06 | $-14.98 | $8,829.11 | ▼ -14.98 after sell → book $9,971.21; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 6 | $190.35 | $2.03 | $+3.04 | $9,969.18 | ▲ +3.04 after sell → book $9,969.18; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 48 | $25.95 | $2.13 | — | $8,721.45 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1246.15 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 519 | $2.40 | $6.70 | — | $7,469.15 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1246.15 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 164 | $7.59 | $2.48 | — | $6,221.91 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1246.15 | — |
| 2026-09-17 09:30 ET | **BUY** | `QTRX` | 423 | $2.94 | $5.46 | — | $4,972.83 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list yday_gainer,ohlc_hot; 🔵; ret5=+9.8; leftover $1246.15 | — |
| 2026-09-17 09:30 ET | **BUY** | `SFL` | 91 | $13.55 | $2.26 | — | $3,737.52 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+7.8; leftover $1246.15 | — |
| 2026-09-17 09:30 ET | **BUY** | `FOSL` | 228 | $5.46 | $2.94 | — | $2,489.70 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+9.5; leftover $1246.15 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGNY` | 45 | $27.38 | $2.12 | — | $1,255.47 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+6.1; leftover $1246.15 | — |
| 2026-09-17 09:30 ET | **BUY** | `AVTR` | 78 | $15.81 | $2.22 | — | $20.07 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; leftover $1246.15 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.07 | ▲ close $10,092.09 vs 09:30 $9,985.91 (session +149.23) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.07 | ▼ 09:30 equity $10,079.07 vs yday $10,092.09 (-13.02) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 48 | $26.14 | $2.15 | $+4.83 | $1,272.63 | ▲ +4.83 after sell → book $10,076.91; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 519 | $2.29 | $6.79 | $-70.58 | $2,454.35 | ▼ -70.58 after sell → book $10,070.12; vs 09:30 mark -6.79 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 164 | $7.98 | $2.52 | $+58.96 | $3,760.55 | ▲ +58.96 after sell → book $10,067.60; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `QTRX` | 423 | $3.12 | $5.54 | $+65.15 | $5,074.78 | ▲ +65.15 after sell → book $10,062.07; vs 09:30 mark -5.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SFL` | 91 | $13.74 | $2.29 | $+12.74 | $6,322.83 | ▲ +12.74 after sell → book $10,059.78; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FOSL` | 228 | $5.63 | $2.99 | $+32.83 | $7,603.48 | ▲ +32.83 after sell → book $10,056.79; vs 09:30 mark -2.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGNY` | 45 | $27.01 | $2.15 | $-20.92 | $8,816.78 | ▼ -20.92 after sell → book $10,054.64; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 78 | $15.87 | $2.25 | $+0.21 | $10,052.40 | ▲ +0.21 after sell → book $10,052.40; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 215 | $5.83 | $2.77 | — | $8,796.17 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1256.55 | — |
| 2026-09-18 09:30 ET | **BUY** | `AMD` | 2 | $547.37 | $2.00 | — | $7,699.44 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+8.2; leftover $1256.55 | — |
| 2026-09-18 09:30 ET | **BUY** | `SYM` | 28 | $44.70 | $2.07 | — | $6,445.76 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.5; leftover $1256.55 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 60 | $20.91 | $2.17 | — | $5,188.99 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1256.55 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 350 | $3.58 | $4.51 | — | $3,931.48 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1256.55 | — |
| 2026-09-18 09:30 ET | **BUY** | `SHLS` | 164 | $7.64 | $2.48 | — | $2,676.04 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list yday_gainer; 🔵; ret5=+7.6; leftover $1256.55 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 89 | $14.07 | $2.26 | — | $1,421.55 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1256.55 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 84 | $14.79 | $2.24 | — | $176.95 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1256.55 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $176.95 | ▼ close $9,974.53 vs 09:30 $10,079.07 (session -57.36) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $176.95 | ▲ 09:30 equity $10,235.45 vs yday $9,974.53 (+260.92) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `BNC` | 215 | $6.42 | $2.82 | $+120.18 | $1,553.35 | ▲ +120.18 after sell → book $10,232.63; vs 09:30 mark -2.82 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SYM` | 28 | $42.42 | $2.09 | $-68.01 | $2,739.02 | ▼ -68.01 after sell → book $10,230.54; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 60 | $21.65 | $2.19 | $+40.04 | $4,035.83 | ▲ +40.04 after sell → book $10,228.35; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DDD` | 350 | $3.71 | $4.58 | $+36.40 | $5,329.74 | ▲ +36.40 after sell → book $10,223.76; vs 09:30 mark -4.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SHLS` | 164 | $7.71 | $2.52 | $+6.48 | $6,591.67 | ▲ +6.48 after sell → book $10,221.25; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 89 | $13.90 | $2.28 | $-19.67 | $7,826.48 | ▼ -19.67 after sell → book $10,218.96; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 84 | $14.58 | $2.27 | $-22.15 | $9,048.94 | ▼ -22.15 after sell → book $10,216.70; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `MXL` | 15 | $83.53 | $2.04 | — | $7,793.95 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+8.8; leftover $1292.71 | — |
| 2026-09-21 09:30 ET | **BUY** | `UMC` | 51 | $24.93 | $2.14 | — | $6,520.38 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+8.7; leftover $1292.71 | — |
| 2026-09-21 09:30 ET | **BUY** | `FORM` | 10 | $123.00 | $2.02 | — | $5,288.36 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+3.0; leftover $1292.71 | — |
| 2026-09-21 09:30 ET | **BUY** | `COHR` | 3 | $326.48 | $2.00 | — | $4,306.92 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+3.9; leftover $1292.71 | — |
| 2026-09-21 09:30 ET | **BUY** | `ASST` | 40 | $31.64 | $2.11 | — | $3,039.21 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.9; leftover $1292.71 | — |
| 2026-09-21 09:30 ET | **BUY** | `SHMD` | 344 | $3.75 | $4.44 | — | $1,744.77 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+9.2; leftover $1292.71 | — |
| 2026-09-21 09:30 ET | **BUY** | `ARM` | 4 | $294.36 | $2.00 | — | $565.33 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+4.1; leftover $1292.71 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $565.33 | ▲ close $10,259.78 vs 09:30 $10,235.45 (session +59.83) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $565.33 | ▼ 09:30 equity $10,144.36 vs yday $10,259.78 (-115.42) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `AMD` | 2 | $606.57 | $2.02 | $+114.39 | $1,776.45 | ▲ +114.39 after sell → book $10,142.34; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `UMC` | 51 | $25.26 | $2.16 | $+12.52 | $3,062.55 | ▲ +12.52 after sell → book $10,140.18; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `COHR` | 3 | $310.29 | $2.02 | $-52.59 | $3,991.40 | ▼ -52.59 after sell → book $10,138.16; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `ASST` | 40 | $29.30 | $2.13 | $-97.84 | $5,161.27 | ▼ -97.84 after sell → book $10,136.03; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `ARM` | 4 | $319.41 | $2.02 | $+96.18 | $6,436.89 | ▲ +96.18 after sell → book $10,134.01; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,436.89 | ▲ close $10,134.01 vs 09:30 $10,144.36 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,436.89 | ▲ 09:30 equity $10,231.18 vs yday $10,134.01 (+97.17) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `MXL` | 15 | $86.57 | $2.06 | $+41.51 | $7,733.39 | ▲ +41.51 after sell → book $10,229.13; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FORM` | 10 | $125.39 | $2.04 | $+19.84 | $8,985.25 | ▲ +19.84 after sell → book $10,227.09; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SHMD` | 344 | $3.61 | $4.50 | $-57.10 | $10,222.58 | ▼ -57.10 after sell → book $10,222.58; vs 09:30 mark -4.51 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 433 | $3.93 | $5.59 | — | $8,515.30 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1703.76 | — |
| 2026-09-23 09:30 ET | **BUY** | `ZS` | 7 | $213.00 | $2.01 | — | $7,022.29 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.0; leftover $1703.76 | — |
| 2026-09-23 09:30 ET | **BUY** | `HIMS` | 56 | $30.40 | $2.16 | — | $5,317.74 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ret5=+9.3; leftover $1703.76 | — |
| 2026-09-23 09:30 ET | **BUY** | `OPRT` | 207 | $8.23 | $2.67 | — | $3,611.46 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+8.7; leftover $1703.76 | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 19 | $89.50 | $2.05 | — | $1,908.91 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1703.76 | — |
| 2026-09-23 09:30 ET | **BUY** | `CLPT` | 109 | $15.55 | $2.32 | — | $211.64 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $1703.76 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $211.64 | ▼ close $9,989.00 vs 09:30 $10,231.18 (session -216.79) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $211.64 | ▼ 09:30 equity $9,940.87 vs yday $9,989.00 (-48.13) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `INDP` | 433 | $3.77 | $5.67 | $-80.54 | $1,838.38 | ▼ -80.54 after sell → book $9,935.20; vs 09:30 mark -5.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ZS` | 7 | $213.47 | $2.03 | $-0.72 | $3,330.67 | ▼ -0.72 after sell → book $9,933.17; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HIMS` | 56 | $28.00 | $2.18 | $-138.74 | $4,896.49 | ▼ -138.74 after sell → book $9,930.99; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 19 | $87.67 | $2.07 | $-38.79 | $6,560.25 | ▼ -38.79 after sell → book $9,928.92; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CLPT` | 109 | $14.82 | $2.35 | $-84.23 | $8,173.28 | ▼ -84.23 after sell → book $9,926.57; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,173.28 | ▼ close $9,891.38 vs 09:30 $9,940.87 (session -35.19) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,502.14 | ▲ 09:30 equity $9,502.14 vs yday $9,502.14 (+0.00) | 09:30 open · cash $9,502.14 · no holdings · equity $9,502.14 vs prior close $9,502.14 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `SAIL` | 53 | $22.05 | $2.15 | — | $8,331.34 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+9.6; leftover $1187.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 197 | $6.00 | $2.58 | — | $7,146.76 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $1187.77 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 45 | $26.27 | $2.12 | — | $5,962.49 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1187.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PRGO` | 80 | $14.81 | $2.23 | — | $4,775.46 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+8.9; leftover $1187.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SENS` | 115 | $10.28 | $2.33 | — | $3,590.92 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ⚪; ret5=+9.7; leftover $1187.77 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 16 | $74.15 | $2.04 | — | $2,402.48 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+8.5; leftover $1187.77 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RGEN` | 6 | $189.92 | $2.01 | — | $1,260.95 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $1187.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 155 | $7.65 | $2.46 | — | $72.75 | — | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1187.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.75 | ▼ close $9,466.99 vs 09:30 $9,502.14 (session -17.23) | 16:00 close · cash $72.75 · equity $9,466.99 vs 09:30 $9,502.14 (-35.15; session marks -17.23) · 8 name(s) marked open→close (per-name table). SAIL×53 09:30 $22.05 → close $20.64 -74.73; SATL×197 09:30 $6.00 → close $6.17 +33.49; WRBY×45 09:30 $26.27 → close $26.71 +19.80; PRGO×80 09:30 $14.81 → close $15.42 +48.80; SENS×115 09:30 $10.28 → close $10.00 -32.20; RKLB×16 09:30 $74.15 → close $73.95 -3.20; RGEN×6 09:30 $189.92 → close $189.68 -1.44; MRVI×155 09:30 $7.65 → close $7.60 -7.75 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MRCY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `ALEC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMTX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MTDR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SQM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `DK` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `VIPS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `RCKT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `GWRE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `YEXT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FNKO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HUBS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SLB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `AGCO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ZETA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TSLA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SIGA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `HAL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PANW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VIR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AGRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CSAN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `LAND` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FMC` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CF` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `LAND` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `XRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `STX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CDZI` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `KEP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `QRVO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `KGS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `PUMP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `PGNY` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `VLO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FRO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `KGS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `PUMP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VLO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `FRO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `META` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IOVA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `KGS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `MXL` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FORM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SHMD` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ZS` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `MRAM` | no_price | no 09:30 open |
| 2026-09-24 | `CRWD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RNG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RPD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AVT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DDOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `OPRT` | 207 | 2026-09-23 @ $8.23 | Clock-B #1 mom+breakout+peer/sector (research; not KEEP); gate clk_mom_break_peer=True; rank hot_score; list ohlc_hot; ret5=+8.7; leftover $1703.76 |
