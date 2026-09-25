# Factor mine action — `union_coil_off_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ coil_off, no 🚨

Cash book **-12.12%** ($8,788) · signal-only (no cash/fees) was -17.20%. Starts YES **0/30**. Fills 253 · skips 105 · realized $-1593.31.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: prior 5-session return is at least 0%.
- Must-have: prior 5-session return is at most 10% (not already exploded).
- Must-have: prior relative volume is at least 0.7.
- Must-have: prior relative volume is at most 2.2 (not a blow-off).
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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,327.17.

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
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 98 | $50.62 | $2.28 | — | $5,036.64 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $5000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 227 | $22.01 | $2.93 | — | $37.44 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.44 | ▲ close $10,677.03 vs 09:30 $10,000.00 (session +682.25) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.44 | ▲ 09:30 equity $10,751.77 vs yday $10,677.03 (+74.74) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 98 | $55.29 | $2.34 | $+452.72 | $5,453.52 | ▲ +452.72 after sell → book $10,749.43; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 227 | $23.33 | $3.01 | $+293.70 | $10,746.42 | ▲ +293.70 after sell → book $10,746.42; vs 09:30 mark -3.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,664.93 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+5.9; leftover $1343.30 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 23 | $57.61 | $2.06 | — | $8,337.84 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1343.30 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1433 | $0.94 | $17.73 | — | $6,977.40 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1343.30 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 895 | $1.50 | $11.55 | — | $5,623.35 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1343.30 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 311 | $4.31 | $4.01 | — | $4,278.93 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1343.30 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 321 | $4.18 | $4.14 | — | $2,933.01 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1343.30 | — |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $1,924.01 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable; 🔵; ⚪; ret5=+7.9; leftover $1343.30 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 81 | $16.50 | $2.23 | — | $585.28 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1343.30 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $585.28 | ▼ close $10,643.82 vs 09:30 $10,751.77 (session -56.89) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $585.28 | ▲ 09:30 equity $10,694.45 vs yday $10,643.82 (+50.63) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,686.90 | ▲ +20.13 after sell → book $10,692.43; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 23 | $55.37 | $2.08 | $-55.66 | $2,958.33 | ▼ -55.66 after sell → book $10,690.35; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1433 | $0.91 | $17.54 | $-78.26 | $4,240.52 | ▼ -78.26 after sell → book $10,672.81; vs 09:30 mark -17.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 895 | $1.52 | $11.71 | $-5.35 | $5,589.21 | ▼ -5.35 after sell → book $10,661.10; vs 09:30 mark -11.71 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 311 | $4.60 | $4.08 | $+82.10 | $7,015.74 | ▲ +82.10 after sell → book $10,657.03; vs 09:30 mark -4.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 321 | $4.10 | $4.20 | $-34.03 | $8,327.63 | ▼ -34.03 after sell → book $10,652.82; vs 09:30 mark -4.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $9,376.68 | ▲ +40.05 after sell → book $10,650.81; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 81 | $15.73 | $2.26 | $-66.86 | $10,648.55 | ▼ -66.86 after sell → book $10,648.55; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 28 | $46.18 | $2.07 | — | $9,353.44 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+6.7; leftover $1331.07 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 410 | $3.24 | $5.29 | — | $8,019.75 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $1331.07 | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 72 | $18.24 | $2.21 | — | $6,704.26 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1331.07 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 82 | $16.20 | $2.24 | — | $5,373.63 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1331.07 | — |
| 2026-08-17 09:30 ET | **BUY** | `NEWP` | 191 | $6.94 | $2.56 | — | $4,045.52 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.1; leftover $1331.07 | — |
| 2026-08-17 09:30 ET | **BUY** | `IQ` | 985 | $1.35 | $12.71 | — | $2,703.07 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list overnight; ⚪; ret5=+1.5; leftover $1331.07 | — |
| 2026-08-17 09:30 ET | **BUY** | `KLAR` | 64 | $20.67 | $2.18 | — | $1,378.00 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list overnight; ret5=+4.5; leftover $1331.07 | — |
| 2026-08-17 09:30 ET | **BUY** | `VNET` | 171 | $7.75 | $2.50 | — | $50.25 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list overnight; ret5=+3.7; leftover $1331.07 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.25 | ▼ close $10,449.34 vs 09:30 $10,694.45 (session -167.45) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.25 | ▼ 09:30 equity $9,823.31 vs yday $10,449.34 (-626.03) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 28 | $48.00 | $2.09 | $+46.79 | $1,392.16 | ▲ +46.79 after sell → book $9,821.22; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 410 | $3.11 | $5.37 | $-63.96 | $2,661.89 | ▼ -63.96 after sell → book $9,815.85; vs 09:30 mark -5.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 72 | $16.20 | $2.23 | $-151.31 | $3,826.06 | ▼ -151.31 after sell → book $9,813.62; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 82 | $15.78 | $2.26 | $-38.94 | $5,117.76 | ▼ -38.94 after sell → book $9,811.36; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NEWP` | 191 | $6.51 | $2.60 | $-87.30 | $6,358.57 | ▼ -87.30 after sell → book $9,808.76; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `IQ` | 985 | $1.27 | $12.88 | $-104.39 | $7,596.64 | ▼ -104.39 after sell → book $9,795.88; vs 09:30 mark -12.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `KLAR` | 64 | $15.66 | $2.20 | $-325.02 | $8,596.67 | ▼ -325.02 after sell → book $9,793.67; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,596.67 | ▼ close $9,721.85 vs 09:30 $9,823.31 (session -71.82) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,596.67 | ▲ 09:30 equity $9,764.60 vs yday $9,721.85 (+42.75) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `VNET` | 171 | $6.83 | $2.54 | $-162.36 | $9,762.06 | ▼ -162.36 after sell → book $9,762.06; vs 09:30 mark -2.54 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,762.06 | ▲ close $9,762.06 vs 09:30 $9,764.60 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,762.06 | ▲ 09:30 equity $9,762.06 vs yday $9,762.06 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 59 | $20.55 | $2.17 | — | $8,547.45 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1220.26 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,362.29 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1220.26 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 211 | $5.77 | $2.72 | — | $6,142.09 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1220.26 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 62 | $19.63 | $2.18 | — | $4,922.86 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1220.26 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $3,705.92 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1220.26 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 697 | $1.75 | $8.99 | — | $2,477.17 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1220.26 | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 163 | $7.45 | $2.48 | — | $1,260.35 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1220.26 | — |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 113 | $10.77 | $2.33 | — | $41.01 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1220.26 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.01 | ▲ close $9,837.15 vs 09:30 $9,762.06 (session +100.09) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.01 | ▲ 09:30 equity $10,090.07 vs yday $9,837.15 (+252.92) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 59 | $21.90 | $2.19 | $+75.30 | $1,330.92 | ▲ +75.30 after sell → book $10,087.88; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,573.23 | ▲ +57.15 after sell → book $10,085.83; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 211 | $5.67 | $2.77 | $-26.59 | $3,766.83 | ▼ -26.59 after sell → book $10,083.06; vs 09:30 mark -2.77 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 62 | $21.17 | $2.20 | $+91.11 | $5,077.18 | ▲ +91.11 after sell → book $10,080.87; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $6,394.01 | ▲ +99.89 after sell → book $10,078.73; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 697 | $1.79 | $9.12 | $+9.77 | $7,632.53 | ▲ +9.77 after sell → book $10,069.62; vs 09:30 mark -9.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 163 | $7.09 | $2.52 | $-63.68 | $8,785.68 | ▼ -63.68 after sell → book $10,067.10; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `EXK` | 113 | $11.34 | $2.36 | $+59.72 | $10,064.74 | ▲ +59.72 after sell → book $10,064.74; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $8,808.57 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1258.09 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 757 | $1.66 | $9.77 | — | $7,542.18 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1258.09 | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1456 | $0.86 | $16.95 | — | $6,267.25 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1258.09 | — |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 9 | $127.43 | $2.02 | — | $5,118.36 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable; 🔵; ⚪; ret5=+7.9; leftover $1258.09 | — |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 231 | $5.43 | $2.98 | — | $3,861.05 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $1258.09 | — |
| 2026-08-21 09:30 ET | **BUY** | `TXG` | 19 | $64.39 | $2.05 | — | $2,635.60 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1258.09 | — |
| 2026-08-21 09:30 ET | **BUY** | `DXYZ` | 36 | $34.89 | $2.10 | — | $1,377.46 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.6; leftover $1258.09 | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 70 | $17.93 | $2.20 | — | $119.81 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $1258.09 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $119.81 | ▼ close $9,902.59 vs 09:30 $10,090.07 (session -122.04) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $119.81 | ▼ 09:30 equity $9,848.00 vs yday $9,902.59 (-54.59) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.75 | $2.07 | $-24.50 | $1,351.49 | ▼ -24.50 after sell → book $9,845.93; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 757 | $1.55 | $9.90 | $-102.94 | $2,514.94 | ▼ -102.94 after sell → book $9,836.03; vs 09:30 mark -9.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1456 | $0.89 | $17.58 | $+3.33 | $3,793.20 | ▲ +3.33 after sell → book $9,818.45; vs 09:30 mark -17.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 9 | $129.99 | $2.04 | $+18.99 | $4,961.07 | ▲ +18.99 after sell → book $9,816.41; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `EMBC` | 231 | $5.20 | $3.03 | $-60.29 | $6,158.09 | ▼ -60.29 after sell → book $9,813.39; vs 09:30 mark -3.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TXG` | 19 | $63.15 | $2.07 | $-27.67 | $7,355.87 | ▼ -27.67 after sell → book $9,811.32; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DXYZ` | 36 | $33.10 | $2.12 | $-68.66 | $8,545.35 | ▼ -68.66 after sell → book $9,809.20; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 70 | $18.05 | $2.22 | $+3.98 | $9,806.98 | ▲ +3.98 after sell → book $9,806.98; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,806.98 | ▲ close $9,806.98 vs 09:30 $9,848.00 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,806.98 | ▲ 09:30 equity $9,806.98 vs yday $9,806.98 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 111 | $10.98 | $2.32 | — | $8,585.88 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+1.2; leftover $1225.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 248 | $4.94 | $3.20 | — | $7,357.56 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+7.1; leftover $1225.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $426.97 | $2.00 | — | $6,501.62 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+6.0; leftover $1225.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 90 | $13.59 | $2.26 | — | $5,276.26 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1225.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 33 | $36.96 | $2.09 | — | $4,054.49 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1225.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `AMTX` | 645 | $1.90 | $8.32 | — | $2,820.67 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ⚪; ret5=+5.0; leftover $1225.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `AVAH` | 89 | $13.62 | $2.26 | — | $1,605.79 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $1225.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 18 | $64.55 | $2.04 | — | $441.85 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+4.4; leftover $1225.87 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $441.85 | ▲ close $9,821.47 vs 09:30 $9,806.98 (session +38.97) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $441.85 | ▼ 09:30 equity $9,817.24 vs yday $9,821.47 (-4.23) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `OCUL` | 111 | $10.79 | $2.35 | $-25.76 | $1,637.18 | ▼ -25.76 after sell → book $9,814.88; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `HCA` | 2 | $427.50 | $2.02 | $-2.95 | $2,490.17 | ▼ -2.95 after sell → book $9,812.87; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 90 | $13.63 | $2.28 | $-0.94 | $3,714.58 | ▼ -0.94 after sell → book $9,810.58; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 33 | $38.24 | $2.11 | $+38.04 | $4,974.39 | ▲ +38.04 after sell → book $9,808.47; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AMTX` | 645 | $1.91 | $8.44 | $-10.31 | $6,197.91 | ▼ -10.31 after sell → book $9,800.04; vs 09:30 mark -8.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AVAH` | 89 | $13.65 | $2.28 | $-2.31 | $7,410.48 | ▼ -2.31 after sell → book $9,797.76; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 18 | $63.60 | $2.06 | $-21.21 | $8,553.21 | ▼ -21.21 after sell → book $9,795.69; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `INSP` | 20 | $60.07 | $2.05 | — | $7,349.76 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+6.4; leftover $1221.89 | — |
| 2026-08-26 09:30 ET | **BUY** | `CRMD` | 142 | $8.60 | $2.42 | — | $6,126.15 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+4.8; leftover $1221.89 | — |
| 2026-08-26 09:30 ET | **BUY** | `SENS` | 128 | $9.48 | $2.37 | — | $4,910.33 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $1221.89 | — |
| 2026-08-26 09:30 ET | **BUY** | `BE` | 5 | $213.94 | $2.00 | — | $3,838.63 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1221.89 | — |
| 2026-08-26 09:30 ET | **BUY** | `ACRS` | 187 | $6.53 | $2.55 | — | $2,614.97 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.6; leftover $1221.89 | — |
| 2026-08-26 09:30 ET | **BUY** | `TMCI` | 255 | $4.78 | $3.29 | — | $1,392.78 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+8.1; leftover $1221.89 | — |
| 2026-08-26 09:30 ET | **BUY** | `CRDL` | 601 | $2.03 | $7.75 | — | $164.99 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+5.5; leftover $1221.89 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $164.99 | ▲ close $9,776.13 vs 09:30 $9,817.24 (session +2.88) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $164.99 | ▲ 09:30 equity $9,809.41 vs yday $9,776.13 (+33.28) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 20 | $62.10 | $2.07 | $+36.48 | $1,404.92 | ▲ +36.48 after sell → book $9,807.34; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 142 | $8.49 | $2.45 | $-20.49 | $2,608.05 | ▼ -20.49 after sell → book $9,804.89; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SENS` | 128 | $9.33 | $2.41 | $-23.98 | $3,799.89 | ▼ -23.98 after sell → book $9,802.49; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ACRS` | 187 | $6.15 | $2.59 | $-76.20 | $4,947.35 | ▼ -76.20 after sell → book $9,799.90; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TMCI` | 255 | $4.72 | $3.34 | $-21.93 | $6,147.60 | ▼ -21.93 after sell → book $9,796.55; vs 09:30 mark -3.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 601 | $2.09 | $7.86 | $+20.44 | $7,395.83 | ▲ +20.44 after sell → book $9,788.69; vs 09:30 mark -7.86 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 29 | $41.44 | $2.08 | — | $6,191.99 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+3.1; leftover $1232.64 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 85 | $14.42 | $2.25 | — | $4,964.05 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+7.1; leftover $1232.64 | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 51 | $24.00 | $2.14 | — | $3,737.91 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+8.7; leftover $1232.64 | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 94 | $12.98 | $2.27 | — | $2,515.51 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $1232.64 | — |
| 2026-08-27 09:30 ET | **BUY** | `AVBP` | 40 | $30.79 | $2.11 | — | $1,281.80 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+3.7; leftover $1232.64 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 127 | $9.68 | $2.37 | — | $50.07 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1232.64 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.07 | ▼ close $9,763.61 vs 09:30 $9,809.41 (session -11.86) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.07 | ▼ 09:30 equity $9,734.32 vs yday $9,763.61 (-29.29) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 248 | $4.95 | $3.25 | $-3.97 | $1,274.42 | ▼ -3.97 after sell → book $9,731.07; vs 09:30 mark -3.25 | dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BE` | 5 | $215.71 | $2.02 | $+4.79 | $2,350.92 | ▲ +4.79 after sell → book $9,729.04; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 29 | $41.74 | $2.10 | $+4.53 | $3,559.29 | ▲ +4.53 after sell → book $9,726.95; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 51 | $23.95 | $2.16 | $-6.86 | $4,778.57 | ▼ -6.86 after sell → book $9,724.78; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 94 | $13.05 | $2.30 | $+2.01 | $6,002.98 | ▲ +2.01 after sell → book $9,722.49; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AVBP` | 40 | $30.53 | $2.13 | $-14.64 | $7,222.05 | ▼ -14.64 after sell → book $9,720.36; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ABX` | 127 | $9.88 | $2.40 | $+20.63 | $8,474.40 | ▲ +20.63 after sell → book $9,717.95; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 77 | $15.66 | $2.22 | — | $7,266.36 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1210.63 | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 9 | $122.81 | $2.02 | — | $6,159.06 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1210.63 | — |
| 2026-08-28 09:30 ET | **BUY** | `EQ` | 492 | $2.46 | $6.35 | — | $4,942.39 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+7.9; leftover $1210.63 | — |
| 2026-08-28 09:30 ET | **BUY** | `BTSG` | 19 | $60.54 | $2.05 | — | $3,790.08 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+2.3; leftover $1210.63 | — |
| 2026-08-28 09:30 ET | **BUY** | `CRDL` | 587 | $2.06 | $7.57 | — | $2,573.29 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+9.3; leftover $1210.63 | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 41 | $28.91 | $2.11 | — | $1,385.87 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+9.2; leftover $1210.63 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADBT` | 242 | $4.99 | $3.12 | — | $175.16 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+0.0; leftover $1210.63 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $175.16 | ▼ close $9,372.01 vs 09:30 $9,734.32 (session -320.50) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $175.16 | ▼ 09:30 equity $9,357.71 vs yday $9,372.01 (-14.30) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 85 | $14.54 | $2.27 | $+5.69 | $1,408.80 | ▲ +5.69 after sell → book $9,355.45; vs 09:30 mark -2.26 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 77 | $14.44 | $2.24 | $-98.40 | $2,518.43 | ▼ -98.40 after sell → book $9,353.20; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 9 | $118.83 | $2.04 | $-39.87 | $3,585.86 | ▼ -39.87 after sell → book $9,351.16; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `EQ` | 492 | $2.39 | $6.44 | $-47.23 | $4,755.31 | ▼ -47.23 after sell → book $9,344.73; vs 09:30 mark -6.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BTSG` | 19 | $58.76 | $2.07 | $-37.93 | $5,869.68 | ▼ -37.93 after sell → book $9,342.66; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRDL` | 587 | $1.92 | $7.68 | $-97.43 | $6,989.04 | ▼ -97.43 after sell → book $9,334.98; vs 09:30 mark -7.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 41 | $28.06 | $2.13 | $-39.10 | $8,137.37 | ▼ -39.10 after sell → book $9,332.85; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADBT` | 242 | $4.94 | $3.17 | $-18.39 | $9,329.67 | ▼ -18.39 after sell → book $9,329.67; vs 09:30 mark -3.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,329.67 | ▲ close $9,329.67 vs 09:30 $9,357.71 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,329.67 | ▲ 09:30 equity $9,329.67 vs yday $9,329.67 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,329.67 | ▲ close $9,329.67 vs 09:30 $9,329.67 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,329.67 | ▲ 09:30 equity $9,329.67 vs yday $9,329.67 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,329.67 | ▲ close $9,329.67 vs 09:30 $9,329.67 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,329.67 | ▲ 09:30 equity $9,329.67 vs yday $9,329.67 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 22 | $52.88 | $2.06 | — | $8,164.26 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1166.21 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 27 | $42.93 | $2.07 | — | $7,003.08 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1166.21 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 321 | $3.63 | $4.14 | — | $5,833.71 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1166.21 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 8 | $132.45 | $2.01 | — | $4,772.09 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1166.21 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 75 | $15.45 | $2.21 | — | $3,611.13 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1166.21 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 69 | $16.77 | $2.20 | — | $2,451.80 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1166.21 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 534 | $2.18 | $6.89 | — | $1,280.79 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1166.21 | — |
| 2026-09-03 09:30 ET | **BUY** | `SDGR` | 55 | $21.03 | $2.15 | — | $121.99 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $1166.21 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $121.99 | ▼ close $9,055.83 vs 09:30 $9,329.67 (session -250.11) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $121.99 | ▼ 09:30 equity $9,025.48 vs yday $9,055.83 (-30.35) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 8 | $130.03 | $2.03 | $-23.41 | $1,160.19 | ▼ -23.41 after sell → book $9,023.44; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 75 | $15.00 | $2.24 | $-38.20 | $2,282.96 | ▼ -38.20 after sell → book $9,021.21; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 69 | $15.61 | $2.22 | $-84.46 | $3,357.83 | ▼ -84.46 after sell → book $9,018.99; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 534 | $2.16 | $6.99 | $-24.56 | $4,504.28 | ▼ -24.56 after sell → book $9,012.00; vs 09:30 mark -6.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SDGR` | 55 | $20.58 | $2.17 | $-29.08 | $5,634.01 | ▼ -29.08 after sell → book $9,009.83; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 447 | $2.52 | $5.77 | — | $4,501.80 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1126.80 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 167 | $6.71 | $2.49 | — | $3,378.74 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1126.80 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 235 | $4.78 | $3.03 | — | $2,252.41 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1126.80 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 99 | $11.31 | $2.29 | — | $1,130.43 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1126.80 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $74.99 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1126.80 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $74.99 | ▼ close $8,839.77 vs 09:30 $9,025.48 (session -154.48) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.99 | ▼ 09:30 equity $8,807.45 vs yday $8,839.77 (-32.32) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 22 | $54.31 | $2.08 | $+27.33 | $1,267.73 | ▲ +27.33 after sell → book $8,805.37; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HRMY` | 27 | $42.20 | $2.09 | $-23.87 | $2,405.04 | ▼ -23.87 after sell → book $8,803.28; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 321 | $3.43 | $4.20 | $-72.55 | $3,501.87 | ▼ -72.55 after sell → book $8,799.08; vs 09:30 mark -4.20 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 447 | $2.38 | $5.85 | $-74.20 | $4,559.88 | ▼ -74.20 after sell → book $8,793.23; vs 09:30 mark -5.85 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 167 | $6.57 | $2.53 | $-28.40 | $5,654.54 | ▼ -28.40 after sell → book $8,790.70; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 235 | $4.30 | $3.08 | $-118.91 | $6,661.96 | ▼ -118.91 after sell → book $8,787.62; vs 09:30 mark -3.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 99 | $11.22 | $2.31 | $-13.51 | $7,770.42 | ▼ -13.51 after sell → book $8,785.30; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $8,783.28 | ▼ -42.58 after sell → book $8,783.28; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,783.28 | ▲ close $8,783.28 vs 09:30 $8,807.45 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,783.28 | ▲ 09:30 equity $8,783.28 vs yday $8,783.28 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,783.28 | ▲ close $8,783.28 vs 09:30 $8,783.28 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,783.28 | ▲ 09:30 equity $8,783.28 vs yday $8,783.28 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,783.28 | ▲ close $8,783.28 vs 09:30 $8,783.28 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,783.28 | ▲ 09:30 equity $8,783.28 vs yday $8,783.28 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 6 | $157.78 | $2.01 | — | $7,834.59 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+4.7; leftover $1097.91 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 538 | $2.04 | $6.94 | — | $6,730.13 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1097.91 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 231 | $4.75 | $2.98 | — | $5,629.90 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1097.91 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 517 | $2.12 | $6.67 | — | $4,527.19 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1097.91 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 95 | $11.55 | $2.27 | — | $3,427.67 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1097.91 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 14 | $77.33 | $2.03 | — | $2,343.02 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+2.5; leftover $1097.91 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 20 | $52.55 | $2.05 | — | $1,289.97 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1097.91 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 108 | $10.11 | $2.31 | — | $195.77 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $1097.91 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $195.77 | ▲ close $8,833.70 vs 09:30 $8,783.28 (session +77.69) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $195.77 | ▼ 09:30 equity $8,746.02 vs yday $8,833.70 (-87.68) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `NVT` | 6 | $150.00 | $2.03 | $-50.72 | $1,093.74 | ▼ -50.72 after sell → book $8,743.99; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 538 | $2.01 | $7.04 | $-30.12 | $2,168.09 | ▼ -30.12 after sell → book $8,736.96; vs 09:30 mark -7.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 231 | $4.82 | $3.03 | $+10.16 | $3,278.48 | ▲ +10.16 after sell → book $8,733.93; vs 09:30 mark -3.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 517 | $2.05 | $6.77 | $-49.62 | $4,331.56 | ▼ -49.62 after sell → book $8,727.16; vs 09:30 mark -6.77 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `FUBO` | 95 | $11.56 | $2.30 | $-3.63 | $5,427.46 | ▼ -3.63 after sell → book $8,724.86; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIST` | 14 | $77.10 | $2.05 | $-7.30 | $6,504.81 | ▼ -7.30 after sell → book $8,722.81; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 20 | $56.90 | $2.07 | $+82.88 | $7,640.74 | ▲ +82.88 after sell → book $8,720.74; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAGS` | 108 | $10.00 | $2.34 | $-16.54 | $8,718.40 | ▼ -16.54 after sell → book $8,718.40; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,718.40 | ▲ close $8,718.40 vs 09:30 $8,746.02 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,718.40 | ▲ 09:30 equity $8,718.40 vs yday $8,718.40 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,718.40 | ▲ close $8,718.40 vs 09:30 $8,718.40 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,718.40 | ▲ 09:30 equity $8,718.40 vs yday $8,718.40 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $7,632.84 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+4.0; leftover $1089.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 76 | $14.31 | $2.22 | — | $6,543.06 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+4.8; leftover $1089.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 29 | $36.46 | $2.08 | — | $5,483.64 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+2.9; leftover $1089.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 15 | $68.79 | $2.04 | — | $4,449.76 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1089.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 185 | $5.87 | $2.54 | — | $3,361.26 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1089.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 12 | $87.40 | $2.03 | — | $2,310.43 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1089.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `MRCY` | 12 | $87.52 | $2.03 | — | $1,258.17 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+4.3; leftover $1089.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `QLYS` | 6 | $179.60 | $2.01 | — | $178.56 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+8.8; leftover $1089.80 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $178.56 | ▼ close $8,594.84 vs 09:30 $8,718.40 (session -106.62) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $178.56 | ▲ 09:30 equity $8,701.03 vs yday $8,594.84 (+106.19) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `IQV` | 4 | $273.15 | $2.02 | $+5.02 | $1,269.14 | ▲ +5.02 after sell → book $8,699.01; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AVAH` | 76 | $14.33 | $2.24 | $-2.94 | $2,355.98 | ▼ -2.94 after sell → book $8,696.77; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BLFS` | 29 | $36.67 | $2.10 | $+1.92 | $3,417.31 | ▲ +1.92 after sell → book $8,694.67; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 15 | $72.70 | $2.06 | $+54.56 | $4,505.76 | ▲ +54.56 after sell → book $8,692.62; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 185 | $5.58 | $2.59 | $-58.78 | $5,535.47 | ▼ -58.78 after sell → book $8,690.03; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 12 | $83.20 | $2.05 | $-54.47 | $6,531.82 | ▼ -54.47 after sell → book $8,687.98; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `MRCY` | 12 | $89.27 | $2.05 | $+16.93 | $7,601.02 | ▲ +16.93 after sell → book $8,685.94; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QLYS` | 6 | $180.82 | $2.03 | $+3.28 | $8,683.91 | ▲ +3.28 after sell → book $8,683.91; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 143 | $7.59 | $2.42 | — | $7,596.12 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1085.49 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 31 | $34.93 | $2.08 | — | $6,511.21 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+1.6; leftover $1085.49 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 6 | $170.85 | $2.01 | — | $5,484.10 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1085.49 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 452 | $2.40 | $5.83 | — | $4,393.47 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1085.49 | — |
| 2026-09-17 09:30 ET | **BUY** | `AIB` | 743 | $1.46 | $9.58 | — | $3,299.10 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+4.4; leftover $1085.49 | — |
| 2026-09-17 09:30 ET | **BUY** | `BYND` | 97 | $11.19 | $2.28 | — | $2,211.39 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+0.5; leftover $1085.49 | — |
| 2026-09-17 09:30 ET | **BUY** | `FTAI` | 5 | $196.50 | $2.00 | — | $1,226.89 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+2.5; leftover $1085.49 | — |
| 2026-09-17 09:30 ET | **BUY** | `QTRX` | 369 | $2.94 | $4.76 | — | $137.27 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,ohlc_hot; 🔵; ret5=+9.8; leftover $1085.49 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $137.27 | ▲ close $8,753.24 vs 09:30 $8,701.03 (session +100.30) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $137.27 | ▲ 09:30 equity $8,789.63 vs yday $8,753.24 (+36.39) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 143 | $7.98 | $2.45 | $+50.90 | $1,275.96 | ▲ +50.90 after sell → book $8,787.18; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMN` | 31 | $34.52 | $2.10 | $-16.90 | $2,343.97 | ▼ -16.90 after sell → book $8,785.08; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 6 | $182.33 | $2.03 | $+64.84 | $3,435.92 | ▲ +64.84 after sell → book $8,783.05; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 452 | $2.29 | $5.92 | $-61.47 | $4,465.09 | ▼ -61.47 after sell → book $8,777.13; vs 09:30 mark -5.92 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AIB` | 743 | $1.41 | $9.72 | $-56.45 | $5,503.00 | ▼ -56.45 after sell → book $8,767.42; vs 09:30 mark -9.71 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BYND` | 97 | $11.71 | $2.31 | $+45.37 | $6,636.08 | ▲ +45.37 after sell → book $8,765.11; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FTAI` | 5 | $195.55 | $2.02 | $-8.78 | $7,611.80 | ▼ -8.78 after sell → book $8,763.08; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `QTRX` | 369 | $3.12 | $4.83 | $+56.83 | $8,758.25 | ▲ +56.83 after sell → book $8,758.25; vs 09:30 mark -4.83 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 77 | $14.07 | $2.22 | — | $7,672.64 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1094.78 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 305 | $3.58 | $3.93 | — | $6,576.81 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1094.78 | — |
| 2026-09-18 09:30 ET | **BUY** | `RANI` | 1287 | $0.85 | $14.80 | — | $5,468.06 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+3.6; leftover $1094.78 | — |
| 2026-09-18 09:30 ET | **BUY** | `SHLS` | 143 | $7.64 | $2.42 | — | $4,373.12 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+7.6; leftover $1094.78 | — |
| 2026-09-18 09:30 ET | **BUY** | `XE` | 67 | $16.28 | $2.19 | — | $3,280.17 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.1; leftover $1094.78 | — |
| 2026-09-18 09:30 ET | **BUY** | `AMD` | 2 | $547.37 | $2.00 | — | $2,183.43 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+8.2; leftover $1094.78 | — |
| 2026-09-18 09:30 ET | **BUY** | `SYM` | 24 | $44.70 | $2.06 | — | $1,108.57 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.5; leftover $1094.78 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 52 | $20.91 | $2.15 | — | $19.10 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1094.78 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.10 | ▼ close $8,654.66 vs 09:30 $8,789.63 (session -71.83) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.10 | ▲ 09:30 equity $8,840.53 vs yday $8,654.66 (+185.87) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 77 | $13.90 | $2.24 | $-17.55 | $1,087.16 | ▼ -17.55 after sell → book $8,838.29; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DDD` | 305 | $3.71 | $4.00 | $+31.72 | $2,214.71 | ▲ +31.72 after sell → book $8,834.29; vs 09:30 mark -4.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RANI` | 1287 | $0.86 | $15.20 | $-11.99 | $3,311.48 | ▼ -11.99 after sell → book $8,819.09; vs 09:30 mark -15.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SHLS` | 143 | $7.71 | $2.45 | $+5.14 | $4,411.55 | ▲ +5.14 after sell → book $8,816.63; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `XE` | 67 | $16.32 | $2.21 | $-1.72 | $5,502.78 | ▼ -1.72 after sell → book $8,814.42; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `AMD` | 2 | $583.88 | $2.02 | $+69.01 | $6,668.53 | ▲ +69.01 after sell → book $8,812.41; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SYM` | 24 | $42.42 | $2.08 | $-58.86 | $7,684.52 | ▼ -58.86 after sell → book $8,810.32; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 52 | $21.65 | $2.17 | $+34.17 | $8,808.16 | ▲ +34.17 after sell → book $8,808.16; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 6 | $157.87 | $2.01 | — | $7,858.93 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+6.5; leftover $1101.02 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 81 | $13.47 | $2.23 | — | $6,765.22 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1101.02 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 991 | $1.11 | $12.78 | — | $5,652.43 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1101.02 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 110 | $9.99 | $2.32 | — | $4,551.21 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1101.02 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 603 | $1.82 | $7.78 | — | $3,442.95 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1101.02 | — |
| 2026-09-21 09:30 ET | **BUY** | `SGML` | 108 | $10.13 | $2.31 | — | $2,346.06 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+4.9; leftover $1101.02 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 42 | $25.95 | $2.12 | — | $1,254.04 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1101.02 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,254.04 | ▼ close $8,729.93 vs 09:30 $8,840.53 (session -46.66) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,254.04 | ▼ 09:30 equity $8,707.16 vs yday $8,729.93 (-22.77) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `ORBS` | 991 | $1.05 | $12.96 | $-85.20 | $2,281.64 | ▼ -85.20 after sell → book $8,694.20; vs 09:30 mark -12.96 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 110 | $9.91 | $2.35 | $-13.47 | $3,369.39 | ▼ -13.47 after sell → book $8,691.85; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `BTBT` | 603 | $1.79 | $7.89 | $-33.76 | $4,443.88 | ▼ -33.76 after sell → book $8,683.96; vs 09:30 mark -7.89 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `ALOY` | 67 | $9.40 | $2.19 | — | $3,811.89 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+9.5; leftover $634.84 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,811.89 | ▼ close $8,656.98 vs 09:30 $8,707.16 (session -24.79) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,811.89 | ▲ 09:30 equity $8,671.91 vs yday $8,656.98 (+14.93) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `A` | 6 | $166.54 | $2.03 | $+47.98 | $4,809.11 | ▲ +47.98 after sell → book $8,669.89; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 81 | $12.84 | $2.26 | $-55.92 | $5,846.89 | ▼ -55.92 after sell → book $8,667.63; vs 09:30 mark -2.26 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SGML` | 108 | $10.26 | $2.34 | $+8.84 | $6,952.63 | ▲ +8.84 after sell → book $8,665.29; vs 09:30 mark -2.34 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 42 | $26.58 | $2.14 | $+22.21 | $8,066.85 | ▲ +22.21 after sell → book $8,663.15; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ALOY` | 67 | $8.90 | $2.21 | $-37.90 | $8,660.94 | ▼ -37.90 after sell → book $8,660.94; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 12 | $89.50 | $2.03 | — | $7,584.91 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1082.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 9 | $116.85 | $2.02 | — | $6,531.25 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1082.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 110 | $9.81 | $2.32 | — | $5,449.83 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1082.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 52 | $20.65 | $2.15 | — | $4,373.88 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1082.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 275 | $3.93 | $3.55 | — | $3,289.58 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1082.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `MNRO` | 76 | $14.14 | $2.22 | — | $2,212.72 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+0.3; leftover $1082.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `NTSK` | 58 | $18.57 | $2.16 | — | $1,133.21 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.2; leftover $1082.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `HIMS` | 35 | $30.40 | $2.10 | — | $67.12 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.3; leftover $1082.62 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.12 | ▼ close $8,450.30 vs 09:30 $8,671.91 (session -192.11) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.12 | ▼ 09:30 equity $8,416.73 vs yday $8,450.30 (-33.57) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 12 | $87.67 | $2.05 | $-25.97 | $1,117.17 | ▼ -25.97 after sell → book $8,414.68; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HALO` | 9 | $112.22 | $2.04 | $-45.72 | $2,125.11 | ▼ -45.72 after sell → book $8,412.64; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 110 | $9.67 | $2.35 | $-20.07 | $3,186.46 | ▼ -20.07 after sell → book $8,410.29; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 52 | $20.52 | $2.17 | $-11.07 | $4,251.34 | ▼ -11.07 after sell → book $8,408.13; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INDP` | 275 | $3.77 | $3.60 | $-51.15 | $5,284.48 | ▼ -51.15 after sell → book $8,404.52; vs 09:30 mark -3.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `MNRO` | 76 | $14.04 | $2.24 | $-12.06 | $6,349.28 | ▼ -12.06 after sell → book $8,402.28; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HIMS` | 35 | $28.00 | $2.12 | $-88.21 | $7,327.17 | ▼ -88.21 after sell → book $8,400.17; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,327.17 | ▲ close $8,404.23 vs 09:30 $8,416.73 (session +4.06) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,814.64 | ▲ 09:30 equity $8,814.64 vs yday $8,814.64 (+0.00) | 09:30 open · cash $8,814.64 · no holdings · equity $8,814.64 vs prior close $8,814.64 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 1 | $803.87 | $1.99 | — | $8,008.78 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+0.8; leftover $1101.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 9 | $115.36 | $2.02 | — | $6,968.52 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1101.83 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 28 | $38.51 | $2.07 | — | $5,888.17 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+4.7; leftover $1101.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 144 | $7.65 | $2.42 | — | $4,784.14 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1101.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 13 | $83.76 | $2.03 | — | $3,693.23 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1101.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 500 | $2.20 | $6.45 | — | $2,586.78 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1101.83 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 183 | $6.00 | $2.54 | — | $1,486.25 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $1101.83 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 61 | $17.91 | $2.17 | — | $391.56 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable; 🔵; ret5=+3.7; leftover $1101.83 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $391.56 | ▼ close $8,788.39 vs 09:30 $8,814.64 (session -4.55) | 16:00 close · cash $391.56 · equity $8,788.39 vs 09:30 $8,814.64 (-26.25; session marks -4.55) · 8 name(s) marked open→close (per-name table). REGN×1 09:30 $803.87 → close $788.04 -15.83; HALO×9 09:30 $115.36 → close $113.90 -13.14; BLFS×28 09:30 $38.51 → close $38.49 -0.56; MRVI×144 09:30 $7.65 → close $7.60 -7.20; TXG×13 09:30 $83.76 → close $85.71 +25.35; HLP×500 09:30 $2.20 → close $2.21 +5.00; SATL×183 09:30 $6.00 → close $6.17 +31.11; PL×61 09:30 $17.91 → close $17.43 -29.28 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DVLT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MXL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABAT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BZ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `PURR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HAFN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `KEP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OBE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TJGC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NTAP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `XRX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `IMSR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IVVD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IOT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-21 | `SNDK` | cash | leftover split 1101.02 < 1 share @ 1826.00 |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SGML` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-22 | `EMAT` | no_price | no 09:30 open |
| 2026-09-22 | `NTSK` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CRWD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `PBLS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RNG` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `NTSK` | 58 | 2026-09-23 @ $18.57 | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.2; leftover $1082.62 |
