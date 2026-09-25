# Factor mine action — `union_white_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ white hold 5, no 🚨

Cash book **-10.17%** ($8,983) · signal-only (no cash/fees) was +36.60%. Starts YES **2/30**. Fills 142 · skips 295 · realized $-24.31.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `zero_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $234.85.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 2 | $4.31 | $0.09 | — | $55.23 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 2 | $4.18 | $0.09 | — | $46.78 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $12.19 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.78 | ▲ close $10,435.12 vs 09:30 $10,178.12 (session +257.57) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.78 | ▼ 09:30 equity $10,415.02 vs yday $10,435.12 (-20.10) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $42.69 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $5.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $39.42 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+0.3; leftover $5.85 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.42 | ▲ close $10,525.84 vs 09:30 $10,415.02 (session +110.89) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.42 | ▼ 09:30 equity $10,392.48 vs yday $10,525.84 (-133.36) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.42 | ▲ close $10,572.87 vs 09:30 $10,392.48 (session +180.39) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.42 | ▲ 09:30 equity $10,710.43 vs yday $10,572.87 (+137.56) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.42 | ▲ close $11,030.39 vs 09:30 $10,710.43 (session +319.97) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.42 | ▼ 09:30 equity $10,965.47 vs yday $11,030.39 (-64.92) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 20 | $58.64 | $2.07 | $-27.32 | $1,210.15 | ▼ -27.32 after sell → book $10,963.40; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 27 | $42.46 | $2.09 | $-99.20 | $2,354.47 | ▼ -99.20 after sell → book $10,961.31; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 24 | $53.06 | $2.08 | $+54.34 | $3,625.83 | ▲ +54.34 after sell → book $10,959.23; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 25 | $51.65 | $2.09 | $+44.60 | $4,915.00 | ▲ +44.60 after sell → book $10,957.14; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 106 | $13.84 | $2.34 | $+222.19 | $6,379.70 | ▲ +222.19 after sell → book $10,954.80; vs 09:30 mark -2.34 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 42 | $30.66 | $2.14 | $+34.39 | $7,665.28 | ▲ +34.39 after sell → book $10,952.67; vs 09:30 mark -2.13 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 1543 | $1.30 | $20.18 | $+718.77 | $9,651.01 | ▲ +718.77 after sell → book $10,932.49; vs 09:30 mark -20.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 53 | $23.11 | $2.17 | $-15.98 | $10,873.67 | ▼ -15.98 after sell → book $10,930.32; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 66 | $20.55 | $2.19 | — | $9,515.18 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1359.21 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,239.01 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1359.21 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 65 | $20.65 | $2.19 | — | $6,894.57 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1359.21 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 235 | $5.77 | $3.03 | — | $5,535.59 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1359.21 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 69 | $19.63 | $2.20 | — | $4,178.92 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1359.21 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 45 | $29.63 | $2.12 | — | $2,843.45 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1359.21 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 776 | $1.75 | $10.01 | — | $1,475.44 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1359.21 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $172.56 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1359.21 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $172.56 | ▲ close $11,159.93 vs 09:30 $10,965.47 (session +255.40) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $172.56 | ▲ 09:30 equity $11,453.24 vs yday $11,159.93 (+293.31) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $184.12 | ▲ +2.46 after sell → book $11,453.10; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 13 | $0.87 | $0.17 | $-1.24 | $195.22 | ▼ -1.24 after sell → book $11,452.93; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 8 | $1.66 | $0.18 | $+0.96 | $208.32 | ▲ +0.96 after sell → book $11,452.75; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 2 | $4.43 | $0.11 | $+0.03 | $217.07 | ▲ +0.03 after sell → book $11,452.64; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 2 | $3.42 | $0.09 | $-1.70 | $223.81 | ▼ -1.70 after sell → book $11,452.54; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $206.44 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $27.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $183.95 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $27.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 11 | $2.47 | $0.30 | — | $156.48 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $27.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 14 | $1.93 | $0.31 | — | $129.14 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $27.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 21 | $1.32 | $0.34 | — | $101.08 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $27.98 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $101.08 | ▲ close $11,452.47 vs 09:30 $11,453.24 (session +1.29) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $101.08 | ▲ 09:30 equity $11,571.21 vs yday $11,452.47 (+118.74) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 1 | $4.62 | $0.07 | $+0.46 | $105.64 | ▲ +0.46 after sell → book $11,571.14; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 1 | $3.50 | $0.06 | $+0.17 | $109.08 | ▲ +0.17 after sell → book $11,571.08; vs 09:30 mark -0.06 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $109.08 | ▼ close $11,534.80 vs 09:30 $11,571.21 (session -36.28) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.08 | ▼ 09:30 equity $11,353.49 vs yday $11,534.80 (-181.31) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 1 | $8.35 | $0.09 | — | $100.65 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+8.0; leftover $15.58 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 9 | $1.63 | $0.17 | — | $85.80 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $15.58 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 2 | $5.24 | $0.11 | — | $75.21 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $15.58 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 1 | $8.79 | $0.09 | — | $66.33 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $15.58 | — |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 25 | $0.62 | $0.23 | — | $50.60 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $15.58 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 2 | $6.37 | $0.13 | — | $37.73 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $15.58 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.73 | ▲ close $11,811.09 vs 09:30 $11,353.49 (session +458.42) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.73 | ▼ 09:30 equity $11,597.83 vs yday $11,811.09 (-213.26) | — | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 6 | $5.81 | $0.37 | — | $2.50 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $37.73 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.50 | ▼ close $11,436.09 vs 09:30 $11,597.83 (session -161.37) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.50 | ▲ 09:30 equity $11,466.20 vs yday $11,436.09 (+30.11) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 66 | $20.93 | $2.21 | $+20.68 | $1,381.67 | ▲ +20.68 after sell → book $11,463.99; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 14 | $95.52 | $2.05 | $+59.06 | $2,716.90 | ▲ +59.06 after sell → book $11,461.94; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 65 | $21.31 | $2.21 | $+38.51 | $4,099.84 | ▲ +38.51 after sell → book $11,459.73; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 235 | $5.49 | $3.08 | $-71.91 | $5,386.91 | ▼ -71.91 after sell → book $11,456.65; vs 09:30 mark -3.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 69 | $21.47 | $2.22 | $+122.54 | $6,866.12 | ▲ +122.54 after sell → book $11,454.43; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 45 | $32.32 | $2.15 | $+116.78 | $8,318.37 | ▲ +116.78 after sell → book $11,452.28; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 776 | $1.91 | $10.15 | $+104.00 | $9,790.38 | ▲ +104.00 after sell → book $11,442.13; vs 09:30 mark -10.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 9 | $155.89 | $2.04 | $+98.09 | $11,191.35 | ▲ +98.09 after sell → book $11,440.09; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,191.35 | ▲ close $11,453.24 vs 09:30 $11,466.20 (session +13.16) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,191.35 | ▼ 09:30 equity $11,444.89 vs yday $11,453.24 (-8.35) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.44 | $0.19 | $-1.12 | $11,207.61 | ▼ -1.12 after sell → book $11,444.70; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 2 | $15.43 | $0.33 | $+8.04 | $11,238.13 | ▲ +8.04 after sell → book $11,444.37; vs 09:30 mark -0.33 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 11 | $2.35 | $0.31 | $-1.94 | $11,263.67 | ▼ -1.94 after sell → book $11,444.05; vs 09:30 mark -0.32 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 14 | $2.06 | $0.35 | $+1.16 | $11,292.16 | ▲ +1.16 after sell → book $11,443.70; vs 09:30 mark -0.35 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 21 | $1.82 | $0.47 | $+9.69 | $11,329.91 | ▲ +9.69 after sell → book $11,443.24; vs 09:30 mark -0.46 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $10,066.71 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1416.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $8,788.85 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1416.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 11 | $122.81 | $2.02 | — | $7,435.92 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1416.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $6,136.28 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1416.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 15 | $91.49 | $2.04 | — | $4,761.89 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1416.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 22 | $62.82 | $2.06 | — | $3,377.80 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1416.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $2,218.03 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1416.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 11 | $119.76 | $2.02 | — | $898.65 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1416.24 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $898.65 | ▼ close $11,038.65 vs 09:30 $11,444.89 (session -388.43) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $898.65 | ▲ 09:30 equity $11,095.41 vs yday $11,038.65 (+56.76) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $898.65 | ▲ close $11,116.20 vs 09:30 $11,095.41 (session +20.79) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $898.65 | ▼ 09:30 equity $10,910.47 vs yday $11,116.20 (-205.73) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 1 | $8.25 | $0.11 | $-0.29 | $906.80 | ▼ -0.29 after sell → book $10,910.37; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 9 | $1.68 | $0.20 | $+0.08 | $921.72 | ▲ +0.08 after sell → book $10,910.17; vs 09:30 mark -0.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ALVO` | 2 | $5.08 | $0.13 | $-0.56 | $931.75 | ▼ -0.56 after sell → book $10,910.04; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `SUJA` | 1 | $9.98 | $0.12 | $+0.98 | $941.61 | ▲ +0.98 after sell → book $10,909.92; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `DEFT` | 25 | $0.63 | $0.25 | $-0.23 | $957.10 | ▼ -0.23 after sell → book $10,909.66; vs 09:30 mark -0.26 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ZURA` | 2 | $5.52 | $0.14 | $-1.97 | $968.01 | ▼ -1.97 after sell → book $10,909.53; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $968.01 | ▲ close $10,920.27 vs 09:30 $10,910.47 (session +10.74) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $968.01 | ▼ 09:30 equity $10,895.02 vs yday $10,920.27 (-25.25) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `USDE` | 6 | $8.07 | $0.52 | $+12.67 | $1,015.91 | ▲ +12.67 after sell → book $10,894.50; vs 09:30 mark -0.52 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,015.91 | ▲ close $10,986.80 vs 09:30 $10,895.02 (session +92.30) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,015.91 | ▼ 09:30 equity $10,909.50 vs yday $10,986.80 (-77.30) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 2 | $52.88 | $1.06 | — | $909.08 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $126.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 2 | $42.93 | $0.86 | — | $822.36 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $126.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 34 | $3.63 | $1.34 | — | $697.60 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $126.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 15 | $8.03 | $1.25 | — | $575.90 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $126.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 7 | $16.77 | $1.19 | — | $457.32 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $126.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 8 | $14.85 | $1.21 | — | $337.31 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $126.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 58 | $2.18 | $1.44 | — | $209.43 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $126.99 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $209.43 | ▲ close $10,952.73 vs 09:30 $10,909.50 (session +51.59) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.43 | ▲ 09:30 equity $11,097.99 vs yday $10,952.73 (+145.26) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `SIMO` | 5 | $239.23 | $2.02 | $-69.08 | $1,403.55 | ▼ -69.08 after sell → book $11,095.96; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SMTC` | 9 | $138.71 | $2.04 | $-31.50 | $2,649.90 | ▼ -31.50 after sell → book $11,093.92; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `TTMI` | 11 | $118.58 | $2.04 | $-50.60 | $3,952.24 | ▼ -50.60 after sell → book $11,091.88; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `KEYS` | 4 | $326.10 | $2.02 | $+2.74 | $5,254.62 | ▲ +2.74 after sell → book $11,089.86; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVT` | 15 | $91.02 | $2.06 | $-11.14 | $6,617.86 | ▼ -11.14 after sell → book $11,087.80; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `CGNX` | 22 | $60.96 | $2.08 | $-45.05 | $7,956.91 | ▼ -45.05 after sell → book $11,085.73; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `COHR` | 4 | $269.69 | $2.02 | $-83.02 | $9,033.64 | ▼ -83.02 after sell → book $11,083.70; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `LSCC` | 11 | $115.92 | $2.04 | $-46.31 | $10,306.72 | ▼ -46.31 after sell → book $11,081.66; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 681 | $2.52 | $8.78 | — | $8,581.82 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1717.79 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 256 | $6.71 | $3.30 | — | $6,860.75 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1717.79 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 904 | $1.90 | $11.66 | — | $5,131.49 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1717.79 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 359 | $4.78 | $4.63 | — | $3,410.84 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1717.79 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 1080 | $1.59 | $13.93 | — | $1,679.71 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1717.79 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 148 | $11.31 | $2.43 | — | $3.40 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1717.79 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.40 | ▼ close $10,987.37 vs 09:30 $11,097.99 (session -49.55) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.40 | ▼ 09:30 equity $10,861.21 vs yday $10,987.37 (-126.16) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.40 | ▼ close $10,741.94 vs 09:30 $10,861.21 (session -119.27) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.40 | ▼ 09:30 equity $10,692.01 vs yday $10,741.94 (-49.93) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.40 | ▼ close $10,250.02 vs 09:30 $10,692.01 (session -441.99) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.40 | ▼ 09:30 equity $10,096.39 vs yday $10,250.02 (-153.63) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.40 | ▼ close $9,859.31 vs 09:30 $10,096.39 (session -237.09) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.40 | ▲ 09:30 equity $9,983.30 vs yday $9,859.31 (+123.99) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 2 | $53.53 | $1.10 | $-0.86 | $109.36 | ▼ -0.86 after sell → book $9,982.20; vs 09:30 mark -1.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 2 | $41.30 | $0.85 | $-4.98 | $191.11 | ▼ -4.98 after sell → book $9,981.35; vs 09:30 mark -0.85 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 34 | $2.77 | $1.06 | $-31.64 | $284.22 | ▼ -31.64 after sell → book $9,980.28; vs 09:30 mark -1.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 15 | $7.70 | $1.22 | $-7.42 | $398.50 | ▼ -7.42 after sell → book $9,979.06; vs 09:30 mark -1.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `ARCT` | 7 | $14.06 | $1.03 | $-21.19 | $495.90 | ▼ -21.19 after sell → book $9,978.04; vs 09:30 mark -1.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `SLN` | 8 | $13.32 | $1.11 | $-14.56 | $601.35 | ▼ -14.56 after sell → book $9,976.93; vs 09:30 mark -1.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CRDL` | 58 | $2.03 | $1.37 | $-11.51 | $717.72 | ▼ -11.51 after sell → book $9,975.56; vs 09:30 mark -1.37 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 2 | $52.55 | $1.06 | — | $611.56 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $143.54 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 14 | $10.11 | $1.46 | — | $468.56 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $143.54 | — |
| 2026-09-11 09:30 ET | **BUY** | `ZSQR` | 44 | $3.25 | $1.56 | — | $324.00 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+3.6; leftover $143.54 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 7 | $18.30 | $1.30 | — | $194.60 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $143.54 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $194.60 | ▲ close $9,973.20 vs 09:30 $9,983.30 (session +3.02) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $194.60 | ▲ 09:30 equity $10,031.50 vs yday $9,973.20 (+58.30) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 681 | $2.15 | $8.91 | $-269.66 | $1,649.84 | ▼ -269.66 after sell → book $10,022.59; vs 09:30 mark -8.91 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 256 | $5.93 | $3.36 | $-206.34 | $3,164.56 | ▼ -206.34 after sell → book $10,019.23; vs 09:30 mark -3.36 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 904 | $1.72 | $11.82 | $-190.73 | $4,703.10 | ▼ -190.73 after sell → book $10,007.41; vs 09:30 mark -11.82 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 359 | $4.13 | $4.70 | $-242.68 | $6,181.06 | ▼ -242.68 after sell → book $10,002.70; vs 09:30 mark -4.71 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 1080 | $1.59 | $14.12 | $-28.06 | $7,884.14 | ▼ -28.06 after sell → book $9,988.58; vs 09:30 mark -14.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 148 | $10.73 | $2.47 | $-90.75 | $9,469.71 | ▼ -90.75 after sell → book $9,986.11; vs 09:30 mark -2.47 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,469.71 | ▼ close $9,958.65 vs 09:30 $10,031.50 (session -27.46) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,469.71 | ▼ 09:30 equity $9,957.43 vs yday $9,958.65 (-1.22) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,469.71 | ▼ close $9,930.93 vs 09:30 $9,957.43 (session -26.50) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,469.71 | ▼ 09:30 equity $9,925.44 vs yday $9,930.93 (-5.49) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 52 | $89.38 | $2.15 | — | $4,819.80 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $4734.85 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 40 | $118.18 | $2.11 | — | $90.49 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $4734.85 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.49 | ▼ close $9,554.72 vs 09:30 $9,925.44 (session -366.46) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.49 | ▲ 09:30 equity $9,659.48 vs yday $9,554.72 (+104.76) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `BULL` | 11 | $7.95 | $0.91 | — | $2.14 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $90.49 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.14 | ▲ close $10,087.98 vs 09:30 $9,659.48 (session +429.40) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.14 | ▲ 09:30 equity $10,176.93 vs yday $10,087.98 (+88.95) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `BAND` | 2 | $51.19 | $1.05 | $-4.84 | $103.46 | ▼ -4.84 after sell → book $10,175.88; vs 09:30 mark -1.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `PAGS` | 14 | $9.55 | $1.40 | $-10.70 | $235.76 | ▼ -10.70 after sell → book $10,174.48; vs 09:30 mark -1.40 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `ZSQR` | 44 | $2.50 | $1.25 | $-35.81 | $344.50 | ▼ -35.81 after sell → book $10,173.22; vs 09:30 mark -1.26 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `PAYP` | 7 | $17.91 | $1.29 | $-5.33 | $468.58 | ▼ -5.33 after sell → book $10,171.93; vs 09:30 mark -1.29 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 1 | $34.44 | $0.35 | — | $433.79 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+14.0; leftover $58.57 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 3 | $14.79 | $0.45 | — | $388.97 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $58.57 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 1 | $29.32 | $0.30 | — | $359.35 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $58.57 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 19 | $3.04 | $0.63 | — | $301.05 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $58.57 | — |
| 2026-09-18 09:30 ET | **BUY** | `RXT` | 14 | $3.94 | $0.59 | — | $245.30 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $58.57 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $245.30 | ▼ close $9,865.39 vs 09:30 $10,176.93 (session -304.22) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $245.30 | ▲ 09:30 equity $9,976.26 vs yday $9,865.39 (+110.87) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $245.30 | ▼ close $9,871.29 vs 09:30 $9,976.26 (session -104.97) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $245.30 | ▲ 09:30 equity $9,874.05 vs yday $9,871.29 (+2.76) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $245.30 | ▲ close $9,878.70 vs 09:30 $9,874.05 (session +4.65) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $245.30 | ▲ 09:30 equity $10,006.87 vs yday $9,878.70 (+128.17) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `SWKS` | 52 | $90.21 | $2.19 | $+38.82 | $4,934.03 | ▲ +38.82 after sell → book $10,004.67; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `QRVO` | 40 | $118.44 | $2.16 | $+6.13 | $9,669.47 | ▲ +6.13 after sell → book $10,002.51; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 13 | $89.50 | $2.03 | — | $8,503.94 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1208.68 | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 7 | $166.54 | $2.01 | — | $7,336.15 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1208.68 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 10 | $116.85 | $2.02 | — | $6,165.63 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1208.68 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 43 | $27.79 | $2.12 | — | $4,968.54 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1208.68 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 152 | $7.95 | $2.45 | — | $3,757.69 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1208.68 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 123 | $9.81 | $2.36 | — | $2,548.71 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1208.68 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 59 | $20.25 | $2.17 | — | $1,351.79 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+15.0; leftover $1208.68 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 58 | $20.65 | $2.16 | — | $151.92 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1208.68 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.92 | ▼ close $9,707.96 vs 09:30 $10,006.87 (session -277.24) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.92 | ▼ 09:30 equity $9,649.90 vs yday $9,707.96 (-58.06) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `BULL` | 11 | $7.62 | $0.89 | $-5.43 | $234.85 | ▼ -5.43 after sell → book $9,649.01; vs 09:30 mark -0.89 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $234.85 | ▲ close $9,800.47 vs 09:30 $9,649.90 (session +151.47) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $411.09 | ▲ 09:30 equity $9,024.52 vs yday $9,002.02 (+22.50) | 09:30 open · cash $411.09 (unchanged overnight, no fees) · equity $9,024.52 vs prior close $9,002.02 (+22.50) · 8 name(s) re-marked at the open (per-name table). A×6 yday $172.84 → 09:30 $171.98 -5.16; ADMA×115 yday $9.52 → 09:30 $9.52 +0.00; ARQT×40 yday $26.27 → 09:30 $26.27 +0.00; DXCM×12 yday $87.47 → 09:30 $87.47 +0.00; FTRE×56 yday $20.02 → 09:30 $20.02 +0.00; HALO×9 yday $115.22 → 09:30 $115.36 +1.26; OMER×55 yday $20.13 → 09:30 $20.61 +26.40; PGEN×142 yday $7.70 → 09:30 $7.70 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 7 | $7.65 | $0.56 | — | $356.98 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+5.2; leftover $58.73 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 15 | $3.86 | $0.62 | — | $298.46 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $58.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 3 | $16.21 | $0.50 | — | $249.33 | — | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $58.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $249.33 | ▼ close $8,983.11 vs 09:30 $9,024.52 (session -39.73) | 16:00 close · cash $249.33 · equity $8,983.11 vs 09:30 $9,024.52 (-41.41; session marks -39.73) · 11 name(s) marked open→close (per-name table). A×6 09:30 $171.98 → close $172.79 +4.86; ADMA×115 09:30 $9.52 → close $9.52 +0.00; ARQT×40 09:30 $26.27 → close $26.27 +0.00; DXCM×12 09:30 $87.47 → close $87.47 +0.00; FTRE×56 09:30 $20.02 → close $20.02 +0.00; HALO×9 09:30 $115.36 → close $113.90 -13.14; OMER×55 09:30 $20.61 → close $20.08 -29.15; PGEN×142 09:30 $7.70 → close $7.70 -0.00; MRVI×7 09:30 $7.65 → close $7.60 -0.35; ZSQR×15 09:30 $3.86 → close $3.78 -1.20; SECZ×3 09:30 $16.21 → close $15.96 -0.75 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `DAVE` | cash | leftover split 12.19 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 12.19 < 1 share @ 57.61 |
| 2026-08-14 | `BETR` | cash | leftover split 12.19 < 1 share @ 14.80 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `TGB` | cash | leftover split 5.85 < 1 share @ 8.46 |
| 2026-08-17 | `CDNL` | cash | leftover split 5.85 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 5.85 < 1 share @ 9.12 |
| 2026-08-17 | `OCC` | cash | leftover split 5.85 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 5.85 < 1 share @ 16.20 |
| 2026-08-17 | `UMAC` | cash | leftover split 5.85 < 1 share @ 32.55 |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `SLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `HIMS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-19 | `BTSG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `IREN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TPG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TGTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `SLS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `HIMS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `INO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TNDM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ANGX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HYLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-20 | `MARA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `LDI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HYLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `TMC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `TMC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `DNN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 27.98 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 27.98 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 27.98 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-25 | `AG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BHP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `CDE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `HDSN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `IAG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `KGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NFGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `WPM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `MOS` | cash | leftover split 15.58 < 1 share @ 23.77 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRMD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `SUJA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `DEFT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `USDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ALVO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `SUJA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `DEFT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ZURA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `USDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ALVO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `SUJA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `DEFT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ZURA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `USDE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `AVT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `CGNX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `COHR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `LSCC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-01 | `USDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `AVT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `CGNX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `COHR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `LSCC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-02 | `SIMO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SMTC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `TTMI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `KEYS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `AVT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `CGNX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `COHR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `LSCC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-03 | `SIMO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SMTC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `TTMI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `KEYS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `AVT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `CGNX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `COHR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `LSCC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `RVTY` | cash | leftover split 126.99 < 1 share @ 132.45 |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `SLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-09 | `ATRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `HRMY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `VSTM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `SLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-10 | `ATRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `HRMY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CABA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `VSTM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `SLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ALEC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BHC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `OABI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `OPK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `VIR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-11 | `ALEC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `BHC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OABI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OPK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `VIR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `ORCL` | cash | leftover split 143.54 < 1 share @ 164.43 |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `PAGS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `ZSQR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `PAYP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-15 | `BAND` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `PAGS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `ZSQR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `PAYP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-16 | `BAND` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `PAGS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `ZSQR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `PAYP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-17 | `BAND` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `PAGS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `ZSQR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `PAYP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `SWKS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `QRVO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `SWKS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `QRVO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `BULL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 58.57 < 1 share @ 108.55 |
| 2026-09-18 | `ECO` | cash | leftover split 58.57 < 1 share @ 85.00 |
| 2026-09-18 | `TEM` | cash | leftover split 58.57 < 1 share @ 81.40 |
| 2026-09-21 | `SWKS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `QRVO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `BULL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `SDGR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `RXT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `SWKS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `QRVO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `BULL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `FIVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `SDGR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `RXT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `BULL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `FIVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `RARE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `SDGR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `RXT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `FIVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `RARE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `SDGR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `RXT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FIVN` | 1 | 2026-09-18 @ $34.44 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+14.0; leftover $58.57 |
| `RARE` | 3 | 2026-09-18 @ $14.79 | union ∩ white hold 5, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $58.57 |
| `SDGR` | 1 | 2026-09-18 @ $29.32 | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $58.57 |
| `CYPH` | 19 | 2026-09-18 @ $3.04 | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $58.57 |
| `RXT` | 14 | 2026-09-18 @ $3.94 | union ∩ white hold 5, no 🚨; gate zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $58.57 |
| `DXCM` | 13 | 2026-09-23 @ $89.50 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1208.68 |
| `A` | 7 | 2026-09-23 @ $166.54 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1208.68 |
| `HALO` | 10 | 2026-09-23 @ $116.85 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1208.68 |
| `ARQT` | 43 | 2026-09-23 @ $27.79 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1208.68 |
| `PGEN` | 152 | 2026-09-23 @ $7.95 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1208.68 |
| `ADMA` | 123 | 2026-09-23 @ $9.81 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1208.68 |
| `FTRE` | 59 | 2026-09-23 @ $20.25 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+15.0; leftover $1208.68 |
| `OMER` | 58 | 2026-09-23 @ $20.65 | union ∩ white hold 5, no 🚨; gate zero_red=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1208.68 |
