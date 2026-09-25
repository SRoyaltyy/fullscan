# Factor mine action — `union_white_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ white, no 🚨

Cash book **-19.63%** ($8,037) · signal-only (no cash/fees) was +12.04%. Starts YES **3/30**. Fills 147 · skips 157 · realized $-1586.47.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: no morning camera is red (the 'white' / all-clear row).
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
- **Gate** `zero_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $168.42.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 2 | $4.31 | $0.09 | — | $55.23 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 2 | $4.18 | $0.09 | — | $46.78 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $12.19 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.78 | ▲ close $10,435.12 vs 09:30 $10,178.12 (session +257.57) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.78 | ▼ 09:30 equity $10,415.02 vs yday $10,435.12 (-20.10) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $42.69 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $5.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $39.42 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+0.3; leftover $5.85 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.42 | ▲ close $10,525.84 vs 09:30 $10,415.02 (session +110.89) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.42 | ▼ 09:30 equity $10,392.48 vs yday $10,525.84 (-133.36) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $1,237.35 | ▼ -0.12 after sell → book $10,390.41; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $2,411.37 | ▼ -69.50 after sell → book $10,388.31; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,651.77 | ▲ +23.38 after sell → book $10,386.23; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $4,881.69 | ▼ -14.65 after sell → book $10,384.15; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $6,221.31 | ▲ +97.12 after sell → book $10,381.81; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $7,388.88 | ▼ -83.63 after sell → book $10,379.68; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $9,127.72 | ▲ +471.89 after sell → book $10,359.50; vs 09:30 mark -20.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $10,300.03 | ▼ -66.33 after sell → book $10,357.33; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,300.03 | ▼ close $10,356.24 vs 09:30 $10,392.48 (session -1.10) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,300.03 | ▼ 09:30 equity $10,356.18 vs yday $10,356.24 (-0.06) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,308.83 | ▼ -0.31 after sell → book $10,356.07; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 13 | $0.88 | $0.17 | $-1.08 | $10,320.10 | ▼ -1.08 after sell → book $10,355.90; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 8 | $1.42 | $0.16 | $-0.94 | $10,331.30 | ▼ -0.94 after sell → book $10,355.74; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 2 | $4.79 | $0.12 | $+0.75 | $10,340.76 | ▲ +0.75 after sell → book $10,355.62; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 2 | $3.87 | $0.10 | $-0.81 | $10,348.39 | ▼ -0.81 after sell → book $10,355.51; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,348.39 | ▲ close $10,355.58 vs 09:30 $10,356.18 (session +0.07) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,348.39 | ▼ 09:30 equity $10,355.51 vs yday $10,355.58 (-0.07) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 1 | $3.92 | $0.06 | $-0.24 | $10,352.25 | ▼ -0.24 after sell → book $10,355.45; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 1 | $3.20 | $0.06 | $-0.13 | $10,355.40 | ▼ -0.13 after sell → book $10,355.40; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,079.12 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,802.95 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 62 | $20.65 | $2.18 | — | $6,520.47 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 224 | $5.77 | $2.89 | — | $5,225.10 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,946.97 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,670.76 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 739 | $1.75 | $9.53 | — | $1,367.97 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $209.64 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1294.42 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $209.64 | ▲ close $10,569.98 vs 09:30 $10,355.51 (session +239.71) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.64 | ▲ 09:30 equity $10,845.87 vs yday $10,569.98 (+275.89) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $192.27 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $26.21 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $169.78 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $26.21 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $144.80 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $26.21 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $119.42 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $26.21 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 19 | $1.32 | $0.31 | — | $94.03 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $26.21 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.03 | ▲ close $10,844.88 vs 09:30 $10,845.87 (session +0.29) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.03 | ▲ 09:30 equity $10,956.62 vs yday $10,844.88 (+111.74) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.03 | ▼ close $10,921.86 vs 09:30 $10,956.62 (session -34.76) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.03 | ▼ 09:30 equity $10,751.24 vs yday $10,921.86 (-170.62) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.32 | $2.20 | $-18.63 | $1,351.68 | ▼ -18.63 after sell → book $10,749.05; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.86 | $2.05 | $+63.82 | $2,691.66 | ▲ +63.82 after sell → book $10,746.99; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 62 | $20.47 | $2.20 | $-15.53 | $3,958.61 | ▼ -15.53 after sell → book $10,744.80; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 224 | $5.53 | $2.94 | $-59.59 | $5,194.39 | ▼ -59.59 after sell → book $10,741.86; vs 09:30 mark -2.94 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.21 | $2.21 | $+98.31 | $6,570.83 | ▲ +98.31 after sell → book $10,739.65; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.32 | $2.14 | $+111.41 | $7,958.45 | ▲ +111.41 after sell → book $10,737.51; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 739 | $1.90 | $9.67 | $+91.65 | $9,352.89 | ▲ +91.65 after sell → book $10,727.85; vs 09:30 mark -9.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $10,602.93 | ▲ +91.71 after sell → book $10,725.81; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 63 | $23.77 | $2.18 | — | $9,103.24 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+13.0; leftover $1514.70 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 181 | $8.35 | $2.53 | — | $7,589.36 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1514.70 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 929 | $1.63 | $11.98 | — | $6,063.11 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1514.70 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 289 | $5.24 | $3.73 | — | $4,545.02 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1514.70 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 172 | $8.79 | $2.51 | — | $3,030.63 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1514.70 | — |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2443 | $0.62 | $22.48 | — | $1,493.50 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $1514.70 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 233 | $6.37 | $3.01 | — | $6.28 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1514.70 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.28 | ▲ close $10,833.35 vs 09:30 $10,751.24 (session +155.95) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.28 | ▼ 09:30 equity $10,829.89 vs yday $10,833.35 (-3.46) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $22.69 | ▼ -0.96 after sell → book $10,829.70; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $53.06 | ▲ +7.88 after sell → book $10,829.37; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $76.87 | ▼ -1.17 after sell → book $10,829.08; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 13 | $2.03 | $0.32 | $+0.69 | $102.93 | ▲ +0.69 after sell → book $10,828.75; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 19 | $1.60 | $0.38 | $+4.63 | $132.95 | ▲ +4.63 after sell → book $10,828.37; vs 09:30 mark -0.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 22 | $5.81 | $1.34 | — | $3.79 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $132.95 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.79 | ▼ close $10,641.88 vs 09:30 $10,829.89 (session -185.15) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.79 | ▲ 09:30 equity $10,699.47 vs yday $10,641.88 (+57.59) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.79 | ▼ close $10,654.80 vs 09:30 $10,699.47 (session -44.66) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.79 | ▼ 09:30 equity $10,622.47 vs yday $10,654.80 (-32.33) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 63 | $23.95 | $2.20 | $+6.96 | $1,510.44 | ▲ +6.96 after sell → book $10,620.27; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 181 | $8.28 | $2.58 | $-17.78 | $3,006.54 | ▼ -17.78 after sell → book $10,617.70; vs 09:30 mark -2.57 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 929 | $1.69 | $12.15 | $+31.61 | $4,564.40 | ▲ +31.61 after sell → book $10,605.55; vs 09:30 mark -12.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 289 | $4.84 | $3.79 | $-123.12 | $5,959.38 | ▼ -123.12 after sell → book $10,601.76; vs 09:30 mark -3.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUJA` | 172 | $9.08 | $2.55 | $+44.83 | $7,518.59 | ▲ +44.83 after sell → book $10,599.21; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DEFT` | 2443 | $0.64 | $23.26 | $-9.09 | $9,046.63 | ▼ -9.09 after sell → book $10,575.95; vs 09:30 mark -23.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 233 | $5.88 | $3.06 | $-120.23 | $10,413.62 | ▼ -120.23 after sell → book $10,572.90; vs 09:30 mark -3.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $9,150.41 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1301.70 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $7,872.56 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1301.70 | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $122.81 | $2.02 | — | $6,642.44 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1301.70 | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $5,342.79 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1301.70 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.49 | $2.03 | — | $4,059.90 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1301.70 | — |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 20 | $62.82 | $2.05 | — | $2,801.45 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1301.70 | — |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $1,641.69 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1301.70 | — |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 10 | $119.76 | $2.02 | — | $442.07 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1301.70 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $442.07 | ▼ close $10,177.57 vs 09:30 $10,622.47 (session -379.18) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $442.07 | ▲ 09:30 equity $10,232.26 vs yday $10,177.57 (+54.69) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `USDE` | 22 | $6.76 | $1.57 | $+17.98 | $589.22 | ▲ +17.98 after sell → book $10,230.69; vs 09:30 mark -1.57 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $589.22 | ▲ close $10,237.82 vs 09:30 $10,232.26 (session +7.13) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $589.22 | ▼ 09:30 equity $10,043.54 vs yday $10,237.82 (-194.28) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $589.22 | ▲ close $10,054.76 vs 09:30 $10,043.54 (session +11.22) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $589.22 | ▼ 09:30 equity $10,031.37 vs yday $10,054.76 (-23.39) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `SIMO` | 5 | $235.71 | $2.02 | $-86.68 | $1,765.74 | ▼ -86.68 after sell → book $10,029.34; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 9 | $133.00 | $2.04 | $-82.89 | $2,960.70 | ▼ -82.89 after sell → book $10,027.30; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TTMI` | 10 | $114.22 | $2.04 | $-89.96 | $4,100.86 | ▼ -89.96 after sell → book $10,025.26; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 4 | $318.04 | $2.02 | $-29.50 | $5,371.00 | ▼ -29.50 after sell → book $10,023.24; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `AVT` | 14 | $89.39 | $2.05 | $-33.48 | $6,620.41 | ▼ -33.48 after sell → book $10,021.19; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CGNX` | 20 | $60.37 | $2.07 | $-53.12 | $7,825.74 | ▼ -53.12 after sell → book $10,019.12; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `COHR` | 4 | $268.12 | $2.02 | $-89.30 | $8,896.20 | ▼ -89.30 after sell → book $10,017.10; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LSCC` | 10 | $112.09 | $2.04 | $-80.76 | $10,015.06 | ▼ -80.76 after sell → book $10,015.06; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,015.06 | ▲ close $10,015.06 vs 09:30 $10,031.37 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,015.06 | ▲ 09:30 equity $10,015.06 vs yday $10,015.06 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $8,796.76 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1251.88 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $7,549.71 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1251.88 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 344 | $3.63 | $4.44 | — | $6,296.55 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1251.88 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 155 | $8.03 | $2.46 | — | $5,049.45 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1251.88 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $3,855.38 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1251.88 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 74 | $16.77 | $2.21 | — | $2,612.19 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1251.88 | — |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 84 | $14.85 | $2.24 | — | $1,362.55 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1251.88 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 574 | $2.18 | $7.40 | — | $103.82 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1251.88 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $103.82 | ▼ close $9,767.67 vs 09:30 $10,015.06 (session -222.48) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $103.82 | ▼ 09:30 equity $9,714.47 vs yday $9,767.67 (-53.20) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 6 | $2.52 | $0.17 | — | $88.54 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $17.30 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 2 | $6.71 | $0.14 | — | $74.97 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $17.30 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 9 | $1.90 | $0.20 | — | $57.68 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $17.30 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 3 | $4.78 | $0.15 | — | $43.18 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $17.30 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 10 | $1.59 | $0.19 | — | $27.10 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $17.30 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 1 | $11.31 | $0.12 | — | $15.67 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $17.30 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.67 | ▲ close $9,808.50 vs 09:30 $9,714.47 (session +94.99) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.67 | ▼ 09:30 equity $9,785.60 vs yday $9,808.50 (-22.90) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.67 | ▼ close $9,658.20 vs 09:30 $9,785.60 (session -127.40) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.67 | ▼ 09:30 equity $9,603.67 vs yday $9,658.20 (-54.53) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 23 | $53.16 | $2.08 | $+2.30 | $1,236.27 | ▲ +2.30 after sell → book $9,601.59; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 29 | $42.01 | $2.10 | $-30.85 | $2,452.46 | ▼ -30.85 after sell → book $9,599.49; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 344 | $3.28 | $4.50 | $-129.34 | $3,576.28 | ▼ -129.34 after sell → book $9,594.99; vs 09:30 mark -4.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 155 | $8.01 | $2.49 | $-8.05 | $4,815.34 | ▼ -8.05 after sell → book $9,592.50; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $5,945.23 | ▼ -64.17 after sell → book $9,590.46; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 74 | $15.46 | $2.23 | $-101.39 | $7,087.04 | ▼ -101.39 after sell → book $9,588.23; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SLN` | 84 | $13.60 | $2.27 | $-109.51 | $8,227.17 | ▼ -109.51 after sell → book $9,585.96; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 574 | $2.22 | $7.51 | $+8.05 | $9,493.94 | ▲ +8.05 after sell → book $9,578.45; vs 09:30 mark -7.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,493.94 | ▼ close $9,574.74 vs 09:30 $9,603.67 (session -3.71) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,493.94 | ▼ 09:30 equity $9,573.59 vs yday $9,574.74 (-1.15) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 6 | $2.22 | $0.17 | $-2.14 | $9,507.09 | ▼ -2.14 after sell → book $9,573.42; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 2 | $6.11 | $0.15 | $-1.49 | $9,519.16 | ▼ -1.49 after sell → book $9,573.27; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 9 | $1.83 | $0.21 | $-1.04 | $9,535.42 | ▼ -1.04 after sell → book $9,573.06; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 3 | $3.92 | $0.15 | $-2.87 | $9,547.04 | ▼ -2.87 after sell → book $9,572.91; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 10 | $1.53 | $0.20 | $-0.99 | $9,562.14 | ▼ -0.99 after sell → book $9,572.71; vs 09:30 mark -0.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 1 | $10.57 | $0.13 | $-0.98 | $9,572.58 | ▼ -0.98 after sell → book $9,572.58; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,572.58 | ▲ close $9,572.58 vs 09:30 $9,573.59 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,572.58 | ▲ 09:30 equity $9,572.58 vs yday $9,572.58 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 11 | $164.43 | $2.02 | — | $7,761.82 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1914.52 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 36 | $52.55 | $2.10 | — | $5,867.93 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1914.52 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 189 | $10.11 | $2.56 | — | $3,954.58 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $1914.52 | — |
| 2026-09-11 09:30 ET | **BUY** | `ZSQR` | 589 | $3.25 | $7.60 | — | $2,032.73 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+3.6; leftover $1914.52 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 104 | $18.30 | $2.30 | — | $127.23 | — | union ∩ white, no 🚨; gate zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1914.52 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.23 | ▼ close $9,467.34 vs 09:30 $9,572.58 (session -88.66) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.23 | ▼ 09:30 equity $9,324.71 vs yday $9,467.34 (-142.63) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.23 | ▼ close $8,925.02 vs 09:30 $9,324.71 (session -399.69) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.23 | ▼ 09:30 equity $8,895.15 vs yday $8,925.02 (-29.87) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.23 | ▼ close $8,506.01 vs 09:30 $8,895.15 (session -389.14) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.23 | ▼ 09:30 equity $8,414.05 vs yday $8,506.01 (-91.96) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 11 | $140.03 | $2.05 | $-272.47 | $1,665.51 | ▼ -272.47 after sell → book $8,412.00; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAND` | 36 | $48.60 | $2.12 | $-146.42 | $3,412.99 | ▼ -146.42 after sell → book $8,409.88; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `PAGS` | 189 | $9.39 | $2.60 | $-141.24 | $5,185.10 | ▼ -141.24 after sell → book $8,407.28; vs 09:30 mark -2.60 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ZSQR` | 589 | $2.34 | $7.71 | $-551.29 | $6,555.65 | ▼ -551.29 after sell → book $8,399.57; vs 09:30 mark -7.71 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `PAYP` | 104 | $17.73 | $2.33 | $-63.92 | $8,397.24 | ▼ -63.92 after sell → book $8,397.24; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 46 | $89.38 | $2.13 | — | $4,283.63 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $4198.62 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 35 | $118.18 | $2.10 | — | $145.24 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $4198.62 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $145.24 | ▼ close $8,071.33 vs 09:30 $8,414.05 (session -321.69) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $145.24 | ▲ 09:30 equity $8,157.70 vs yday $8,071.33 (+86.37) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `BULL` | 18 | $7.95 | $1.49 | — | $0.65 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $145.24 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.65 | ▲ close $8,523.00 vs 09:30 $8,157.70 (session +366.79) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.65 | ▲ 09:30 equity $8,602.85 vs yday $8,523.00 (+79.85) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.65 | ▼ close $8,333.41 vs 09:30 $8,602.85 (session -269.44) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.65 | ▲ 09:30 equity $8,424.67 vs yday $8,333.41 (+91.26) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `SWKS` | 46 | $89.66 | $2.17 | $+8.58 | $4,122.84 | ▲ +8.58 after sell → book $8,422.50; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QRVO` | 35 | $118.44 | $2.14 | $+4.87 | $8,266.10 | ▲ +4.87 after sell → book $8,420.36; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,266.10 | ▼ close $8,414.60 vs 09:30 $8,424.67 (session -5.76) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,266.10 | ▲ 09:30 equity $8,415.05 vs yday $8,414.60 (+0.45) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `BULL` | 18 | $8.28 | $1.56 | $+2.80 | $8,413.49 | ▲ +2.80 after sell → book $8,413.49; vs 09:30 mark -1.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,413.49 | ▲ close $8,413.49 vs 09:30 $8,415.05 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,413.49 | ▲ 09:30 equity $8,413.49 vs yday $8,413.49 (-0.00) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 11 | $89.50 | $2.02 | — | $7,426.96 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1051.69 | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 6 | $166.54 | $2.01 | — | $6,425.72 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1051.69 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 9 | $116.85 | $2.02 | — | $5,372.05 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1051.69 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 37 | $27.79 | $2.10 | — | $4,341.72 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1051.69 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 132 | $7.95 | $2.39 | — | $3,289.93 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1051.69 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 107 | $9.81 | $2.31 | — | $2,237.95 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1051.69 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 51 | $20.25 | $2.14 | — | $1,203.06 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+15.0; leftover $1051.69 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 50 | $20.65 | $2.14 | — | $168.42 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1051.69 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $168.42 | ▼ close $8,165.89 vs 09:30 $8,413.49 (session -230.47) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $168.42 | ▼ 09:30 equity $8,120.91 vs yday $8,165.89 (-44.98) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $168.42 | ▲ close $8,239.16 vs 09:30 $8,120.91 (session +118.25) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,454.62 | ▲ 09:30 equity $8,042.60 vs yday $8,039.58 (+3.02) | 09:30 open · cash $7,454.62 (unchanged overnight, no fees) · equity $8,042.60 vs prior close $8,039.58 (+3.02) · 5 name(s) re-marked at the open (per-name table). ADMA×13 yday $9.52 → 09:30 $9.52 +0.00; ARQT×4 yday $26.27 → 09:30 $26.27 +0.00; FTRE×6 yday $20.02 → 09:30 $20.02 +0.00; HALO×1 yday $115.22 → 09:30 $115.36 +0.14; OMER×6 yday $20.13 → 09:30 $20.61 +2.88 | — |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 139 | $7.65 | $2.41 | — | $6,388.86 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1064.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $5,381.72 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1064.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 12 | $83.69 | $2.03 | — | $4,375.35 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $1064.95 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 275 | $3.86 | $3.55 | — | $3,310.30 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1064.95 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 5 | $184.00 | $2.00 | — | $2,388.30 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $1064.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 65 | $16.21 | $2.19 | — | $1,332.46 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1064.95 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 8 | $123.50 | $2.01 | — | $342.45 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $1064.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $342.45 | ▲ close $8,037.00 vs 09:30 $8,042.60 (session +10.61) | 16:00 close · cash $342.45 · equity $8,037.00 vs 09:30 $8,042.60 (-5.60; session marks +10.61) · 12 name(s) marked open→close (per-name table). ADMA×13 09:30 $9.52 → close $9.52 +0.00; ARQT×4 09:30 $26.27 → close $26.27 +0.00; FTRE×6 09:30 $20.02 → close $20.02 +0.00; HALO×1 09:30 $115.36 → close $113.90 -1.46; OMER×6 09:30 $20.61 → close $20.08 -3.18; MRVI×139 09:30 $7.65 → close $7.60 -6.95; TXG×12 09:30 $83.76 → close $85.71 +23.40; TEM×12 09:30 $83.69 → close $85.01 +15.78; ZSQR×275 09:30 $3.86 → close $3.78 -22.00; TWST×5 09:30 $184.00 → close $182.83 -5.85; SECZ×65 09:30 $16.21 → close $15.96 -16.25; GRAL×8 09:30 $123.50 → close $126.89 +27.12 | — |

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
| 2026-08-14 | `DAVE` | cash | leftover split 12.19 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 12.19 < 1 share @ 57.61 |
| 2026-08-14 | `BETR` | cash | leftover split 12.19 < 1 share @ 14.80 |
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
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TGB` | cash | leftover split 5.85 < 1 share @ 8.46 |
| 2026-08-17 | `CDNL` | cash | leftover split 5.85 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 5.85 < 1 share @ 9.12 |
| 2026-08-17 | `OCC` | cash | leftover split 5.85 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 5.85 < 1 share @ 16.20 |
| 2026-08-17 | `UMAC` | cash | leftover split 5.85 < 1 share @ 32.55 |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 26.21 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 26.21 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 26.21 < 1 share @ 59.72 |
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
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `MOS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SUJA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `MOS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `AVT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CGNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `COHR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LSCC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `AVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CGNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `COHR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LSCC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `PAGS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ZSQR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `PAYP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `PAGS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ZSQR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `PAYP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-17 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BULL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 0.08 < 1 share @ 108.55 |
| 2026-09-18 | `ECO` | cash | leftover split 0.08 < 1 share @ 85.00 |
| 2026-09-18 | `FIVN` | cash | leftover split 0.08 < 1 share @ 34.44 |
| 2026-09-18 | `RARE` | cash | leftover split 0.08 < 1 share @ 14.79 |
| 2026-09-18 | `SDGR` | cash | leftover split 0.08 < 1 share @ 29.32 |
| 2026-09-18 | `CYPH` | cash | leftover split 0.08 < 1 share @ 3.04 |
| 2026-09-18 | `TEM` | cash | leftover split 0.08 < 1 share @ 81.40 |
| 2026-09-18 | `RXT` | cash | leftover split 0.08 < 1 share @ 3.94 |
| 2026-09-21 | `BULL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DXCM` | 11 | 2026-09-23 @ $89.50 | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1051.69 |
| `A` | 6 | 2026-09-23 @ $166.54 | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1051.69 |
| `HALO` | 9 | 2026-09-23 @ $116.85 | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1051.69 |
| `ARQT` | 37 | 2026-09-23 @ $27.79 | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1051.69 |
| `PGEN` | 132 | 2026-09-23 @ $7.95 | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1051.69 |
| `ADMA` | 107 | 2026-09-23 @ $9.81 | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1051.69 |
| `FTRE` | 51 | 2026-09-23 @ $20.25 | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+15.0; leftover $1051.69 |
| `OMER` | 50 | 2026-09-23 @ $20.65 | union ∩ white, no 🚨; gate zero_red=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1051.69 |
