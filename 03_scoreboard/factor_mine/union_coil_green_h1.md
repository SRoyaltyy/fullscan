# Factor mine action — `union_coil_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-13.64%** ($8,636) · signal-only (no cash/fees) was -12.21%. Starts YES **0/30**. Fills 332 · skips 101 · realized $-768.45.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the last finished bar was green (closed up).
- Must-have: prior 5-session return is at least 0%.
- Must-have: prior 5-session return is at most 10% (not already exploded).
- Must-have: prior relative volume is at least 0.7.
- Must-have: prior relative volume is at most 2.2 (not a blow-off).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

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
- **Gate** `last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,231.52.

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
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 197 | $50.62 | $2.58 | — | $24.65 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $10000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.65 | ▲ close $10,784.79 vs 09:30 $10,000.00 (session +787.37) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.65 | ▲ 09:30 equity $10,916.78 vs yday $10,784.79 (+131.99) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 197 | $55.29 | $2.70 | $+914.08 | $10,914.08 | ▲ +914.08 after sell → book $10,914.08; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 23 | $57.61 | $2.06 | — | $9,586.99 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1364.26 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1455 | $0.94 | $18.00 | — | $8,205.66 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1364.26 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 909 | $1.50 | $11.73 | — | $6,830.43 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1364.26 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 316 | $4.31 | $4.08 | — | $5,464.39 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1364.26 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 326 | $4.18 | $4.21 | — | $4,097.51 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1364.26 | — |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $3,088.51 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable; 🔵; ⚪; ret5=+7.9; leftover $1364.26 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 82 | $16.50 | $2.24 | — | $1,733.28 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1364.26 | — |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 30 | $44.06 | $2.08 | — | $409.40 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable; 🔵; ret5=+3.9; leftover $1364.26 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $409.40 | ▼ close $10,811.45 vs 09:30 $10,916.78 (session -56.25) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $409.40 | ▲ 09:30 equity $10,874.99 vs yday $10,811.45 (+63.54) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 23 | $55.37 | $2.08 | $-55.66 | $1,680.83 | ▼ -55.66 after sell → book $10,872.91; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1455 | $0.91 | $17.81 | $-79.46 | $2,982.70 | ▼ -79.46 after sell → book $10,855.10; vs 09:30 mark -17.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 909 | $1.52 | $11.89 | $-5.43 | $4,352.49 | ▼ -5.43 after sell → book $10,843.21; vs 09:30 mark -11.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 316 | $4.60 | $4.14 | $+83.42 | $5,801.95 | ▲ +83.42 after sell → book $10,839.07; vs 09:30 mark -4.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 326 | $4.10 | $4.27 | $-34.56 | $7,134.28 | ▼ -34.56 after sell → book $10,834.80; vs 09:30 mark -4.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $8,183.32 | ▲ +40.05 after sell → book $10,832.78; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 82 | $15.73 | $2.26 | $-67.64 | $9,470.92 | ▼ -67.64 after sell → book $10,830.52; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ALGM` | 30 | $45.32 | $2.10 | $+33.62 | $10,828.42 | ▲ +33.62 after sell → book $10,828.42; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 39 | $46.18 | $2.11 | — | $9,025.29 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+6.7; leftover $1804.74 | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 98 | $18.24 | $2.28 | — | $7,235.49 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1804.74 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 111 | $16.20 | $2.32 | — | $5,434.97 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1804.74 | — |
| 2026-08-17 09:30 ET | **BUY** | `NEWP` | 260 | $6.94 | $3.35 | — | $3,627.21 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.1; leftover $1804.74 | — |
| 2026-08-17 09:30 ET | **BUY** | `IQ` | 1336 | $1.35 | $17.23 | — | $1,806.38 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list overnight; ⚪; ret5=+1.5; leftover $1804.74 | — |
| 2026-08-17 09:30 ET | **BUY** | `KLAR` | 87 | $20.67 | $2.25 | — | $5.84 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list overnight; ret5=+4.5; leftover $1804.74 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.84 | ▼ close $10,560.64 vs 09:30 $10,874.99 (session -238.23) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.84 | ▼ 09:30 equity $9,968.76 vs yday $10,560.64 (-591.88) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 39 | $48.00 | $2.13 | $+66.74 | $1,875.71 | ▲ +66.74 after sell → book $9,966.63; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 98 | $16.20 | $2.31 | $-204.52 | $3,460.99 | ▼ -204.52 after sell → book $9,964.31; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 111 | $15.78 | $2.36 | $-51.30 | $5,210.22 | ▼ -51.30 after sell → book $9,961.96; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NEWP` | 260 | $6.51 | $3.41 | $-118.56 | $6,899.41 | ▼ -118.56 after sell → book $9,958.55; vs 09:30 mark -3.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `IQ` | 1336 | $1.27 | $17.47 | $-141.58 | $8,578.66 | ▼ -141.58 after sell → book $9,941.08; vs 09:30 mark -17.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `KLAR` | 87 | $15.66 | $2.28 | $-440.40 | $9,938.80 | ▼ -440.40 after sell → book $9,938.80; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,938.80 | ▲ close $9,938.80 vs 09:30 $9,968.76 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,938.80 | ▲ 09:30 equity $9,938.80 vs yday $9,938.80 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,938.80 | ▲ close $9,938.80 vs 09:30 $9,938.80 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,938.80 | ▲ 09:30 equity $9,938.80 vs yday $9,938.80 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,703.63 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1242.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 215 | $5.77 | $2.77 | — | $7,460.31 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1242.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $6,221.44 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1242.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $5,004.50 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1242.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 709 | $1.75 | $9.15 | — | $3,754.60 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1242.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 166 | $7.45 | $2.49 | — | $2,515.41 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1242.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 115 | $10.77 | $2.33 | — | $1,274.53 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1242.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 131 | $9.46 | $2.38 | — | $32.88 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1242.35 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.88 | ▲ close $10,018.18 vs 09:30 $9,938.80 (session +104.97) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.88 | ▲ 09:30 equity $10,312.82 vs yday $10,018.18 (+294.64) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 60 | $21.90 | $2.19 | $+76.64 | $1,344.69 | ▲ +76.64 after sell → book $10,310.63; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 215 | $5.67 | $2.82 | $-27.09 | $2,560.92 | ▼ -27.09 after sell → book $10,307.81; vs 09:30 mark -2.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 63 | $21.17 | $2.20 | $+92.64 | $3,892.43 | ▲ +92.64 after sell → book $10,305.61; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $5,209.27 | ▲ +99.89 after sell → book $10,303.48; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 709 | $1.79 | $9.27 | $+9.94 | $6,469.11 | ▲ +9.94 after sell → book $10,294.21; vs 09:30 mark -9.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 166 | $7.09 | $2.53 | $-64.77 | $7,643.52 | ▼ -64.77 after sell → book $10,291.68; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `EXK` | 115 | $11.34 | $2.36 | $+60.85 | $8,945.26 | ▲ +60.85 after sell → book $10,289.32; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 131 | $10.26 | $2.42 | $+100.00 | $10,286.90 | ▲ +100.00 after sell → book $10,286.90; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 885 | $1.66 | $11.42 | — | $8,806.38 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1469.56 | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1700 | $0.86 | $19.79 | — | $7,317.80 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1469.56 | — |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 11 | $127.43 | $2.02 | — | $5,914.04 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable; 🔵; ⚪; ret5=+7.9; leftover $1469.56 | — |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 270 | $5.43 | $3.48 | — | $4,444.46 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $1469.56 | — |
| 2026-08-21 09:30 ET | **BUY** | `TXG` | 22 | $64.39 | $2.06 | — | $3,025.82 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1469.56 | — |
| 2026-08-21 09:30 ET | **BUY** | `DXYZ` | 42 | $34.89 | $2.12 | — | $1,558.33 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.6; leftover $1469.56 | — |
| 2026-08-21 09:30 ET | **BUY** | `PDD` | 16 | $90.03 | $2.04 | — | $115.81 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list overnight,overnight_mega; 🔵; ret5=+6.4; leftover $1469.56 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $115.81 | ▼ close $10,096.34 vs 09:30 $10,312.82 (session -147.64) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $115.81 | ▼ 09:30 equity $10,067.80 vs yday $10,096.34 (-28.54) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 885 | $1.55 | $11.57 | $-120.34 | $1,475.99 | ▼ -120.34 after sell → book $10,056.23; vs 09:30 mark -11.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1700 | $0.89 | $20.52 | $+3.89 | $2,968.46 | ▲ +3.89 after sell → book $10,035.70; vs 09:30 mark -20.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 11 | $129.99 | $2.04 | $+24.09 | $4,396.31 | ▲ +24.09 after sell → book $10,033.66; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `EMBC` | 270 | $5.20 | $3.54 | $-70.47 | $5,795.42 | ▼ -70.47 after sell → book $10,030.12; vs 09:30 mark -3.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TXG` | 22 | $63.15 | $2.08 | $-31.41 | $7,182.64 | ▼ -31.41 after sell → book $10,028.04; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DXYZ` | 42 | $33.10 | $2.14 | $-79.43 | $8,570.70 | ▼ -79.43 after sell → book $10,025.90; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PDD` | 16 | $90.95 | $2.06 | $+10.62 | $10,023.84 | ▲ +10.62 after sell → book $10,023.84; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,023.84 | ▲ close $10,023.84 vs 09:30 $10,067.80 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,023.84 | ▲ 09:30 equity $10,023.84 vs yday $10,023.84 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 92 | $13.59 | $2.27 | — | $8,771.30 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1252.98 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 33 | $36.96 | $2.09 | — | $7,549.53 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1252.98 | — |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 19 | $64.55 | $2.05 | — | $6,321.03 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+4.4; leftover $1252.98 | — |
| 2026-08-25 09:30 ET | **BUY** | `ANRO` | 34 | $36.52 | $2.09 | — | $5,077.26 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.9; leftover $1252.98 | — |
| 2026-08-25 09:30 ET | **BUY** | `ANF` | 11 | $112.17 | $2.02 | — | $3,841.37 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list overnight; 🔵; ⚪; ret5=+6.8; leftover $1252.98 | — |
| 2026-08-25 09:30 ET | **BUY** | `BOX` | 37 | $33.33 | $2.10 | — | $2,606.06 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list overnight; 🔵; ret5=+3.7; leftover $1252.98 | — |
| 2026-08-25 09:30 ET | **BUY** | `FSCO` | 245 | $5.10 | $3.16 | — | $1,353.40 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list overnight; ret5=+0.2; leftover $1252.98 | — |
| 2026-08-25 09:30 ET | **BUY** | `NCNO` | 60 | $20.76 | $2.17 | — | $105.63 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list overnight; 🔵; ret5=+3.6; leftover $1252.98 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $105.63 | ▼ close $9,970.53 vs 09:30 $10,023.84 (session -35.37) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $105.63 | ▲ 09:30 equity $10,165.68 vs yday $9,970.53 (+195.15) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 92 | $13.63 | $2.29 | $-0.88 | $1,357.29 | ▼ -0.88 after sell → book $10,163.38; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 33 | $38.24 | $2.11 | $+38.04 | $2,617.11 | ▲ +38.04 after sell → book $10,161.28; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 19 | $63.60 | $2.07 | $-22.16 | $3,823.44 | ▼ -22.16 after sell → book $10,159.21; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ANRO` | 34 | $35.80 | $2.11 | $-28.68 | $5,038.53 | ▼ -28.68 after sell → book $10,157.10; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ANF` | 11 | $131.37 | $2.04 | $+207.13 | $6,481.55 | ▲ +207.13 after sell → book $10,155.05; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BOX` | 37 | $34.30 | $2.12 | $+31.67 | $7,748.53 | ▲ +31.67 after sell → book $10,152.93; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FSCO` | 245 | $5.08 | $3.21 | $-11.27 | $8,989.92 | ▼ -11.27 after sell → book $10,149.72; vs 09:30 mark -3.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `CRMD` | 149 | $8.60 | $2.44 | — | $7,706.08 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+4.8; leftover $1284.27 | — |
| 2026-08-26 09:30 ET | **BUY** | `RZLT` | 256 | $5.01 | $3.30 | — | $6,420.22 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,yday_gainer; 🔵; ret5=+7.5; leftover $1284.27 | — |
| 2026-08-26 09:30 ET | **BUY** | `SENS` | 135 | $9.48 | $2.40 | — | $5,138.03 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $1284.27 | — |
| 2026-08-26 09:30 ET | **BUY** | `ACRS` | 196 | $6.53 | $2.58 | — | $3,855.57 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.6; leftover $1284.27 | — |
| 2026-08-26 09:30 ET | **BUY** | `TMCI` | 268 | $4.78 | $3.46 | — | $2,571.07 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+8.1; leftover $1284.27 | — |
| 2026-08-26 09:30 ET | **BUY** | `CRDL` | 632 | $2.03 | $8.15 | — | $1,279.96 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+5.5; leftover $1284.27 | — |
| 2026-08-26 09:30 ET | **BUY** | `LI` | 105 | $12.14 | $2.31 | — | $2.95 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+1.2; leftover $1284.27 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.95 | ▲ close $10,200.18 vs 09:30 $10,165.68 (session +75.09) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.95 | ▲ 09:30 equity $10,235.22 vs yday $10,200.18 (+35.04) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `NCNO` | 60 | $22.03 | $2.19 | $+71.84 | $1,322.56 | ▲ +71.84 after sell → book $10,233.03; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 149 | $8.49 | $2.47 | $-21.30 | $2,585.10 | ▼ -21.30 after sell → book $10,230.56; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SENS` | 135 | $9.33 | $2.43 | $-25.07 | $3,842.22 | ▼ -25.07 after sell → book $10,228.13; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ACRS` | 196 | $6.15 | $2.62 | $-79.68 | $5,045.00 | ▼ -79.68 after sell → book $10,225.51; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TMCI` | 268 | $4.72 | $3.51 | $-23.05 | $6,306.45 | ▼ -23.05 after sell → book $10,222.00; vs 09:30 mark -3.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 632 | $2.09 | $8.27 | $+21.50 | $7,619.06 | ▲ +21.50 after sell → book $10,213.73; vs 09:30 mark -8.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `LI` | 105 | $12.35 | $2.33 | $+17.41 | $8,913.48 | ▲ +17.41 after sell → book $10,211.40; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 30 | $41.44 | $2.08 | — | $7,668.20 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+3.1; leftover $1273.35 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 88 | $14.42 | $2.25 | — | $6,396.98 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+7.1; leftover $1273.35 | — |
| 2026-08-27 09:30 ET | **BUY** | `BE` | 5 | $227.10 | $2.00 | — | $5,259.48 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.6; leftover $1273.35 | — |
| 2026-08-27 09:30 ET | **BUY** | `MAIR` | 44 | $28.76 | $2.12 | — | $3,991.92 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+1.6; leftover $1273.35 | — |
| 2026-08-27 09:30 ET | **BUY** | `GRRR` | 79 | $15.94 | $2.23 | — | $2,730.43 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; 🔵; ret5=+5.9; leftover $1273.35 | — |
| 2026-08-27 09:30 ET | **BUY** | `GSM` | 316 | $4.02 | $4.08 | — | $1,456.03 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+0.5; leftover $1273.35 | — |
| 2026-08-27 09:30 ET | **BUY** | `NABL` | 329 | $3.87 | $4.24 | — | $178.56 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.8; leftover $1273.35 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $178.56 | ▼ close $10,090.32 vs 09:30 $10,235.22 (session -102.07) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $178.56 | ▲ 09:30 equity $10,192.44 vs yday $10,090.32 (+102.12) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 256 | $4.95 | $3.35 | $-22.02 | $1,442.41 | ▼ -22.02 after sell → book $10,189.08; vs 09:30 mark -3.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 30 | $41.74 | $2.10 | $+4.82 | $2,692.51 | ▲ +4.82 after sell → book $10,186.98; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BE` | 5 | $215.71 | $2.02 | $-61.01 | $3,769.01 | ▼ -61.01 after sell → book $10,184.96; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MAIR` | 44 | $27.36 | $2.14 | $-65.86 | $4,970.70 | ▼ -65.86 after sell → book $10,182.81; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 79 | $15.66 | $2.25 | $-26.60 | $6,205.59 | ▼ -26.60 after sell → book $10,180.56; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GSM` | 316 | $4.08 | $4.14 | $+10.74 | $7,490.73 | ▲ +10.74 after sell → book $10,176.42; vs 09:30 mark -4.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NABL` | 329 | $4.25 | $4.31 | $+116.47 | $8,884.67 | ▲ +116.47 after sell → book $10,172.11; vs 09:30 mark -4.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `EQ` | 515 | $2.46 | $6.64 | — | $7,611.13 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+7.9; leftover $1269.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 33 | $37.49 | $2.09 | — | $6,371.87 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+5.4; leftover $1269.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 66 | $19.00 | $2.19 | — | $5,115.68 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+7.5; leftover $1269.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `FSM` | 98 | $12.84 | $2.28 | — | $3,855.08 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+7.6; leftover $1269.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $2,808.44 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+7.8; leftover $1269.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 28 | $44.40 | $2.07 | — | $1,563.16 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+0.4; leftover $1269.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 152 | $8.35 | $2.45 | — | $291.52 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+5.1; leftover $1269.24 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $291.52 | ▼ close $9,962.72 vs 09:30 $10,192.44 (session -189.67) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $291.52 | ▲ 09:30 equity $9,963.23 vs yday $9,962.72 (+0.51) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 88 | $14.54 | $2.28 | $+6.03 | $1,568.76 | ▲ +6.03 after sell → book $9,960.95; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `EQ` | 515 | $2.39 | $6.74 | $-49.43 | $2,792.87 | ▼ -49.43 after sell → book $9,954.21; vs 09:30 mark -6.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 33 | $35.77 | $2.11 | $-60.96 | $3,971.17 | ▼ -60.96 after sell → book $9,952.10; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 66 | $18.12 | $2.21 | $-62.15 | $5,165.21 | ▼ -62.15 after sell → book $9,949.89; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FSM` | 98 | $12.26 | $2.31 | $-61.43 | $6,364.38 | ▼ -61.43 after sell → book $9,947.58; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $257.71 | $2.02 | $-17.82 | $7,393.20 | ▼ -17.82 after sell → book $9,945.56; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 28 | $44.85 | $2.09 | $+8.43 | $8,646.91 | ▲ +8.43 after sell → book $9,943.47; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 152 | $8.53 | $2.48 | $+22.43 | $9,940.98 | ▲ +22.43 after sell → book $9,940.98; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,940.98 | ▲ close $9,940.98 vs 09:30 $9,963.23 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,940.98 | ▲ 09:30 equity $9,940.98 vs yday $9,940.98 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,940.98 | ▲ close $9,940.98 vs 09:30 $9,940.98 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,940.98 | ▲ 09:30 equity $9,940.98 vs yday $9,940.98 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,940.98 | ▲ close $9,940.98 vs 09:30 $9,940.98 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,940.98 | ▲ 09:30 equity $9,940.98 vs yday $9,940.98 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $8,722.68 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1242.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 28 | $42.93 | $2.07 | — | $7,518.57 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1242.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 342 | $3.63 | $4.41 | — | $6,272.70 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1242.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $5,078.63 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1242.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 74 | $16.77 | $2.21 | — | $3,835.44 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1242.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 570 | $2.18 | $7.35 | — | $2,585.49 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1242.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `SDGR` | 59 | $21.03 | $2.17 | — | $1,342.55 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $1242.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `NEOV` | 329 | $3.77 | $4.24 | — | $97.98 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; 🔵; ⚪; ret5=+8.6; leftover $1242.62 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.98 | ▼ close $9,703.78 vs 09:30 $9,940.98 (session -210.67) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.98 | ▼ 09:30 equity $9,670.89 vs yday $9,703.78 (-32.89) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 23 | $52.03 | $2.08 | $-23.69 | $1,292.59 | ▼ -23.69 after sell → book $9,668.81; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 28 | $41.50 | $2.09 | $-44.21 | $2,452.49 | ▼ -44.21 after sell → book $9,666.71; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CABA` | 342 | $3.46 | $4.48 | $-67.03 | $3,631.33 | ▼ -67.03 after sell → book $9,662.23; vs 09:30 mark -4.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $4,799.57 | ▼ -25.83 after sell → book $9,660.20; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 74 | $15.61 | $2.23 | $-90.29 | $5,952.47 | ▼ -90.29 after sell → book $9,657.96; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 570 | $2.16 | $7.46 | $-26.21 | $7,176.22 | ▼ -26.21 after sell → book $9,650.51; vs 09:30 mark -7.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SDGR` | 59 | $20.58 | $2.19 | $-30.90 | $8,388.25 | ▼ -30.90 after sell → book $9,648.32; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NEOV` | 329 | $3.83 | $4.31 | $+11.19 | $9,644.01 | ▲ +11.19 after sell → book $9,644.01; vs 09:30 mark -4.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $8,588.57 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1205.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 153 | $7.87 | $2.45 | — | $7,382.01 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.7; leftover $1205.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `GORO` | 305 | $3.95 | $3.93 | — | $6,173.32 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+6.9; leftover $1205.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRCL` | 12 | $97.98 | $2.03 | — | $4,995.54 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.5; leftover $1205.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 8 | $137.35 | $2.01 | — | $3,894.72 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+5.4; leftover $1205.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `BLSH` | 34 | $34.69 | $2.09 | — | $2,713.17 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.9; leftover $1205.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `ZETA` | 36 | $32.65 | $2.10 | — | $1,535.67 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+8.1; leftover $1205.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 134 | $8.94 | $2.39 | — | $335.32 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+7.7; leftover $1205.50 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $335.32 | ▲ close $9,806.36 vs 09:30 $9,670.89 (session +181.36) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $335.32 | ▼ 09:30 equity $9,625.91 vs yday $9,806.36 (-180.45) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $1,348.18 | ▼ -42.58 after sell → book $9,623.89; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 153 | $7.76 | $2.48 | $-21.76 | $2,532.98 | ▼ -21.76 after sell → book $9,621.41; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GORO` | 305 | $4.13 | $4.00 | $+46.97 | $3,788.63 | ▲ +46.97 after sell → book $9,617.41; vs 09:30 mark -4.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRCL` | 12 | $100.65 | $2.05 | $+27.97 | $4,994.38 | ▲ +27.97 after sell → book $9,615.36; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MSTR` | 8 | $137.62 | $2.03 | $-1.89 | $6,093.31 | ▼ -1.89 after sell → book $9,613.33; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BLSH` | 34 | $35.90 | $2.11 | $+36.94 | $7,311.80 | ▲ +36.94 after sell → book $9,611.22; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ZETA` | 36 | $31.08 | $2.12 | $-60.74 | $8,428.56 | ▼ -60.74 after sell → book $9,609.10; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,428.56 | ▲ close $9,629.20 vs 09:30 $9,625.91 (session +20.10) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,428.56 | ▲ 09:30 equity $9,634.56 vs yday $9,629.20 (+5.36) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `HAFN` | 134 | $9.00 | $2.42 | $+3.22 | $9,632.14 | ▲ +3.22 after sell → book $9,632.14; vs 09:30 mark -2.42 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,632.14 | ▲ close $9,632.14 vs 09:30 $9,634.56 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,632.14 | ▲ 09:30 equity $9,632.14 vs yday $9,632.14 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,632.14 | ▲ close $9,632.14 vs 09:30 $9,632.14 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,632.14 | ▲ 09:30 equity $9,632.14 vs yday $9,632.14 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 590 | $2.04 | $7.61 | — | $8,420.93 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1204.02 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 253 | $4.75 | $3.26 | — | $7,215.91 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1204.02 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 567 | $2.12 | $7.31 | — | $6,006.56 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1204.02 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 104 | $11.55 | $2.30 | — | $4,803.06 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1204.02 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 15 | $77.33 | $2.04 | — | $3,641.07 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+2.5; leftover $1204.02 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 22 | $52.55 | $2.06 | — | $2,482.91 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1204.02 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 119 | $10.11 | $2.35 | — | $1,277.48 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $1204.02 | — |
| 2026-09-11 09:30 ET | **BUY** | `ZSQR` | 370 | $3.25 | $4.77 | — | $70.20 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+3.6; leftover $1204.02 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $70.20 | ▼ close $9,589.41 vs 09:30 $9,632.14 (session -11.02) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $70.20 | ▼ 09:30 equity $9,570.65 vs yday $9,589.41 (-18.76) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 590 | $2.01 | $7.72 | $-33.03 | $1,248.39 | ▼ -33.03 after sell → book $9,562.94; vs 09:30 mark -7.71 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 253 | $4.82 | $3.32 | $+11.13 | $2,464.53 | ▲ +11.13 after sell → book $9,559.62; vs 09:30 mark -3.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 567 | $2.05 | $7.42 | $-54.42 | $3,619.46 | ▼ -54.42 after sell → book $9,552.20; vs 09:30 mark -7.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `FUBO` | 104 | $11.56 | $2.33 | $-3.59 | $4,819.37 | ▼ -3.59 after sell → book $9,549.87; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIST` | 15 | $77.10 | $2.06 | $-7.54 | $5,973.82 | ▼ -7.54 after sell → book $9,547.82; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 22 | $56.90 | $2.08 | $+91.57 | $7,223.54 | ▲ +91.57 after sell → book $9,545.74; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAGS` | 119 | $10.00 | $2.38 | $-17.81 | $8,411.16 | ▼ -17.81 after sell → book $9,543.36; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ZSQR` | 370 | $3.06 | $4.84 | $-79.92 | $9,538.52 | ▼ -79.92 after sell → book $9,538.52; vs 09:30 mark -4.84 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,538.52 | ▲ close $9,538.52 vs 09:30 $9,570.65 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,538.52 | ▲ 09:30 equity $9,538.52 vs yday $9,538.52 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,538.52 | ▲ close $9,538.52 vs 09:30 $9,538.52 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,538.52 | ▲ 09:30 equity $9,538.52 vs yday $9,538.52 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $8,452.96 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+4.0; leftover $1192.31 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 32 | $36.46 | $2.09 | — | $7,284.15 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+2.9; leftover $1192.31 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 17 | $68.79 | $2.04 | — | $6,112.68 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1192.31 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 203 | $5.87 | $2.62 | — | $4,918.45 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1192.31 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 13 | $87.40 | $2.03 | — | $3,780.22 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1192.31 | — |
| 2026-09-16 09:30 ET | **BUY** | `MRCY` | 13 | $87.52 | $2.03 | — | $2,640.43 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+4.3; leftover $1192.31 | — |
| 2026-09-16 09:30 ET | **BUY** | `QLYS` | 6 | $179.60 | $2.01 | — | $1,560.83 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+8.8; leftover $1192.31 | — |
| 2026-09-16 09:30 ET | **BUY** | `ILMN` | 5 | $224.49 | $2.00 | — | $436.37 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+5.3; leftover $1192.31 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $436.37 | ▼ close $9,431.30 vs 09:30 $9,538.52 (session -90.40) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $436.37 | ▲ 09:30 equity $9,567.33 vs yday $9,431.30 (+136.03) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `IQV` | 4 | $273.15 | $2.02 | $+5.02 | $1,526.95 | ▲ +5.02 after sell → book $9,565.31; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BLFS` | 32 | $36.67 | $2.11 | $+2.53 | $2,698.28 | ▲ +2.53 after sell → book $9,563.20; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 17 | $72.70 | $2.06 | $+62.37 | $3,932.12 | ▲ +62.37 after sell → book $9,561.14; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 203 | $5.58 | $2.66 | $-64.15 | $5,062.20 | ▼ -64.15 after sell → book $9,558.48; vs 09:30 mark -2.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 13 | $83.20 | $2.05 | $-58.68 | $6,141.75 | ▼ -58.68 after sell → book $9,556.43; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `MRCY` | 13 | $89.27 | $2.05 | $+18.67 | $7,300.21 | ▲ +18.67 after sell → book $9,554.38; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QLYS` | 6 | $180.82 | $2.03 | $+3.28 | $8,383.10 | ▲ +3.28 after sell → book $9,552.35; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ILMN` | 5 | $233.85 | $2.02 | $+42.77 | $9,550.33 | ▲ +42.77 after sell → book $9,550.33; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 157 | $7.59 | $2.46 | — | $8,356.24 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1193.79 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 34 | $34.93 | $2.09 | — | $7,166.53 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+1.6; leftover $1193.79 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 6 | $170.85 | $2.01 | — | $6,139.42 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1193.79 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 497 | $2.40 | $6.41 | — | $4,940.21 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1193.79 | — |
| 2026-09-17 09:30 ET | **BUY** | `AIB` | 817 | $1.46 | $10.54 | — | $3,736.85 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+4.4; leftover $1193.79 | — |
| 2026-09-17 09:30 ET | **BUY** | `BYND` | 106 | $11.19 | $2.31 | — | $2,548.40 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+0.5; leftover $1193.79 | — |
| 2026-09-17 09:30 ET | **BUY** | `FTAI` | 6 | $196.50 | $2.01 | — | $1,367.39 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+2.5; leftover $1193.79 | — |
| 2026-09-17 09:30 ET | **BUY** | `QTRX` | 406 | $2.94 | $5.24 | — | $168.51 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,ohlc_hot; 🔵; ret5=+9.8; leftover $1193.79 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $168.51 | ▲ close $9,622.12 vs 09:30 $9,567.33 (session +104.86) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $168.51 | ▲ 09:30 equity $9,659.88 vs yday $9,622.12 (+37.76) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 157 | $7.98 | $2.50 | $+56.27 | $1,418.88 | ▲ +56.27 after sell → book $9,657.39; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMN` | 34 | $34.52 | $2.11 | $-18.14 | $2,590.44 | ▼ -18.14 after sell → book $9,655.27; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 6 | $182.33 | $2.03 | $+64.84 | $3,682.40 | ▲ +64.84 after sell → book $9,653.25; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 497 | $2.29 | $6.50 | $-67.59 | $4,814.02 | ▼ -67.59 after sell → book $9,646.74; vs 09:30 mark -6.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AIB` | 817 | $1.41 | $10.68 | $-62.07 | $5,955.31 | ▼ -62.07 after sell → book $9,636.06; vs 09:30 mark -10.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BYND` | 106 | $11.71 | $2.34 | $+49.95 | $7,193.70 | ▲ +49.95 after sell → book $9,633.72; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FTAI` | 6 | $195.55 | $2.03 | $-9.74 | $8,364.97 | ▼ -9.74 after sell → book $9,631.69; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `QTRX` | 406 | $3.12 | $5.31 | $+62.53 | $9,626.38 | ▲ +62.53 after sell → book $9,626.38; vs 09:30 mark -5.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 85 | $14.07 | $2.25 | — | $8,428.18 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1203.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 336 | $3.58 | $4.33 | — | $7,220.97 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1203.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `RANI` | 1415 | $0.85 | $16.27 | — | $6,001.95 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+3.6; leftover $1203.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `SHLS` | 157 | $7.64 | $2.46 | — | $4,800.01 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+7.6; leftover $1203.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `XE` | 73 | $16.28 | $2.21 | — | $3,609.36 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.1; leftover $1203.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `AMD` | 2 | $547.37 | $2.00 | — | $2,512.62 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+8.2; leftover $1203.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `SYM` | 26 | $44.70 | $2.07 | — | $1,348.35 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.5; leftover $1203.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 57 | $20.91 | $2.16 | — | $154.32 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1203.30 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $154.32 | ▼ close $9,512.45 vs 09:30 $9,659.88 (session -80.18) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.32 | ▲ 09:30 equity $9,711.50 vs yday $9,512.45 (+199.05) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 85 | $13.90 | $2.27 | $-18.96 | $1,333.55 | ▼ -18.96 after sell → book $9,709.23; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DDD` | 336 | $3.71 | $4.40 | $+34.95 | $2,575.71 | ▲ +34.95 after sell → book $9,704.83; vs 09:30 mark -4.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RANI` | 1415 | $0.86 | $16.72 | $-13.18 | $3,781.56 | ▼ -13.18 after sell → book $9,688.12; vs 09:30 mark -16.71 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SHLS` | 157 | $7.71 | $2.50 | $+6.03 | $4,989.53 | ▲ +6.03 after sell → book $9,685.62; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `XE` | 73 | $16.32 | $2.23 | $-1.52 | $6,178.66 | ▼ -1.52 after sell → book $9,683.39; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `AMD` | 2 | $583.88 | $2.02 | $+69.01 | $7,344.40 | ▲ +69.01 after sell → book $9,681.37; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SYM` | 26 | $42.42 | $2.09 | $-63.44 | $8,445.24 | ▼ -63.44 after sell → book $9,679.29; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 57 | $21.65 | $2.18 | $+37.84 | $9,677.10 | ▲ +37.84 after sell → book $9,677.10; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 7 | $157.87 | $2.01 | — | $8,570.00 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+6.5; leftover $1209.64 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 89 | $13.47 | $2.26 | — | $7,368.47 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1209.64 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1089 | $1.11 | $14.05 | — | $6,145.63 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1209.64 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 121 | $9.99 | $2.35 | — | $4,934.49 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1209.64 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 662 | $1.82 | $8.54 | — | $3,717.80 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1209.64 | — |
| 2026-09-21 09:30 ET | **BUY** | `SGML` | 119 | $10.13 | $2.35 | — | $2,509.39 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+4.9; leftover $1209.64 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 46 | $25.95 | $2.13 | — | $1,313.56 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1209.64 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,313.56 | ▼ close $9,593.82 vs 09:30 $9,711.50 (session -49.60) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,313.56 | ▼ 09:30 equity $9,568.80 vs yday $9,593.82 (-25.02) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `ORBS` | 1089 | $1.05 | $14.24 | $-93.63 | $2,442.77 | ▼ -93.63 after sell → book $9,554.56; vs 09:30 mark -14.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 121 | $9.91 | $2.38 | $-14.42 | $3,639.50 | ▼ -14.42 after sell → book $9,552.18; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `BTBT` | 662 | $1.79 | $8.66 | $-37.06 | $4,819.13 | ▼ -37.06 after sell → book $9,543.52; vs 09:30 mark -8.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `ALOY` | 128 | $9.40 | $2.37 | — | $3,613.56 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+9.5; leftover $1204.78 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,613.56 | ▼ close $9,493.79 vs 09:30 $9,568.80 (session -47.36) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,613.56 | ▲ 09:30 equity $9,504.92 vs yday $9,493.79 (+11.13) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `A` | 7 | $166.54 | $2.03 | $+56.65 | $4,777.30 | ▲ +56.65 after sell → book $9,502.88; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 89 | $12.84 | $2.28 | $-61.05 | $5,917.78 | ▼ -61.05 after sell → book $9,500.60; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SGML` | 119 | $10.26 | $2.38 | $+10.15 | $7,136.35 | ▲ +10.15 after sell → book $9,498.23; vs 09:30 mark -2.37 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 46 | $26.58 | $2.15 | $+24.70 | $8,356.88 | ▲ +24.70 after sell → book $9,496.08; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ALOY` | 128 | $8.90 | $2.41 | $-68.78 | $9,493.67 | ▼ -68.78 after sell → book $9,493.67; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 13 | $89.50 | $2.03 | — | $8,328.14 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1186.71 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 120 | $9.81 | $2.35 | — | $7,148.59 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1186.71 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 57 | $20.65 | $2.16 | — | $5,969.38 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1186.71 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 301 | $3.93 | $3.88 | — | $4,782.57 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1186.71 | — |
| 2026-09-23 09:30 ET | **BUY** | `MNRO` | 83 | $14.14 | $2.24 | — | $3,606.71 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+0.3; leftover $1186.71 | — |
| 2026-09-23 09:30 ET | **BUY** | `NTSK` | 63 | $18.57 | $2.18 | — | $2,434.31 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.2; leftover $1186.71 | — |
| 2026-09-23 09:30 ET | **BUY** | `HIMS` | 39 | $30.40 | $2.11 | — | $1,246.60 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.3; leftover $1186.71 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 29 | $40.00 | $2.08 | — | $84.52 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+6.7; leftover $1186.71 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $84.52 | ▼ close $9,309.93 vs 09:30 $9,504.92 (session -164.71) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $84.52 | ▼ 09:30 equity $9,250.76 vs yday $9,309.93 (-59.17) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 13 | $87.67 | $2.05 | $-27.80 | $1,222.25 | ▼ -27.80 after sell → book $9,248.71; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 120 | $9.67 | $2.38 | $-21.53 | $2,380.27 | ▼ -21.53 after sell → book $9,246.33; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 57 | $20.52 | $2.18 | $-11.75 | $3,547.73 | ▼ -11.75 after sell → book $9,244.15; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INDP` | 301 | $3.77 | $3.94 | $-55.99 | $4,678.55 | ▼ -55.99 after sell → book $9,240.20; vs 09:30 mark -3.95 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `MNRO` | 83 | $14.04 | $2.26 | $-12.80 | $5,841.61 | ▼ -12.80 after sell → book $9,237.94; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `NTSK` | 63 | $18.50 | $2.20 | $-9.10 | $7,004.91 | ▼ -9.10 after sell → book $9,235.74; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HIMS` | 39 | $28.00 | $2.13 | $-97.83 | $8,094.79 | ▼ -97.83 after sell → book $9,233.62; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 29 | $39.27 | $2.10 | $-25.34 | $9,231.52 | ▼ -25.34 after sell → book $9,231.52; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,231.52 | ▲ close $9,231.52 vs 09:30 $9,250.76 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,367.35 | ▲ 09:30 equity $9,367.35 vs yday $9,367.35 (+0.00) | 09:30 open · cash $9,367.35 · no holdings · equity $9,367.35 vs prior close $9,367.35 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 30 | $38.51 | $2.08 | — | $8,209.97 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+4.7; leftover $1170.92 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 153 | $7.65 | $2.45 | — | $7,037.07 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1170.92 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 13 | $83.76 | $2.03 | — | $5,946.16 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1170.92 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 532 | $2.20 | $6.86 | — | $4,768.90 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1170.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 195 | $6.00 | $2.58 | — | $3,596.32 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $1170.92 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 65 | $17.91 | $2.19 | — | $2,429.99 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable; 🔵; ret5=+3.7; leftover $1170.92 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 13 | $83.69 | $2.03 | — | $1,339.93 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $1170.92 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SAIL` | 53 | $22.05 | $2.15 | — | $169.13 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.6; leftover $1170.92 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $169.13 | ▼ close $9,311.73 vs 09:30 $9,367.35 (session -33.26) | 16:00 close · cash $169.13 · equity $9,311.73 vs 09:30 $9,367.35 (-55.62; session marks -33.26) · 8 name(s) marked open→close (per-name table). BLFS×30 09:30 $38.51 → close $38.49 -0.60; MRVI×153 09:30 $7.65 → close $7.60 -7.65; TXG×13 09:30 $83.76 → close $85.71 +25.35; HLP×532 09:30 $2.20 → close $2.21 +5.32; SATL×195 09:30 $6.00 → close $6.17 +33.15; PL×65 09:30 $17.91 → close $17.43 -31.20; TEM×13 09:30 $83.69 → close $85.01 +17.10; SAIL×53 09:30 $22.05 → close $20.64 -74.73 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $169.13 | ▼ 09:30 equity $9,227.06 vs yday $9,311.73 (-84.67) | 09:30 open · cash $169.13 (unchanged overnight, no fees) · equity $9,227.06 vs prior close $9,311.73 (-84.67) · 8 name(s) re-marked at the open (per-name table). BLFS×30 yday $38.49 → 09:30 $38.13 -10.80; HLP×532 yday $2.21 → 09:30 $2.22 +5.32; MRVI×153 yday $7.60 → 09:30 $7.49 -16.83; PL×65 yday $17.43 → 09:30 $17.17 -16.90; SAIL×53 yday $20.64 → 09:30 $20.03 -32.33; SATL×195 yday $6.17 → 09:30 $6.17 +0.00; TEM×13 yday $85.01 → 09:30 $83.57 -18.72; TXG×13 yday $85.71 → 09:30 $86.14 +5.59 | — |
| 2026-09-28 09:30 ET | **SELL** | `BLFS` | 30 | $38.13 | $2.10 | $-15.58 | $1,310.93 | ▼ -15.58 after sell → book $9,224.96; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `MRVI` | 153 | $7.49 | $2.48 | $-29.41 | $2,454.42 | ▼ -29.41 after sell → book $9,222.48; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `PL` | 65 | $17.17 | $2.21 | $-52.49 | $3,568.26 | ▼ -52.49 after sell → book $9,220.27; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `SAIL` | 53 | $20.03 | $2.17 | $-111.38 | $4,627.68 | ▼ -111.38 after sell → book $9,218.10; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `SATL` | 195 | $6.17 | $2.62 | $+27.96 | $5,828.21 | ▲ +27.96 after sell → book $9,215.48; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `TEM` | 13 | $83.57 | $2.05 | $-5.70 | $6,912.57 | ▼ -5.70 after sell → book $9,213.43; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-28 09:30 ET | **SELL** | `TXG` | 13 | $86.14 | $2.05 | $+26.86 | $8,030.35 | ▲ +26.86 after sell → book $9,211.39; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,030.35 | ▲ close $9,301.83 vs 09:30 $9,227.06 (session +90.44) | 16:00 close · cash $8,030.35 · equity $9,301.83 vs 09:30 $9,227.06 (+74.77; session marks +90.44) · 1 name(s) marked open→close (per-name table). HLP×532 09:30 $2.22 → close $2.39 +90.44 | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,030.35 | ▲ 09:30 equity $9,312.47 vs yday $9,301.83 (+10.64) | 09:30 open · cash $8,030.35 (unchanged overnight, no fees) · equity $9,312.47 vs prior close $9,301.83 (+10.64) · 1 name(s) re-marked at the open (per-name table). HLP×532 yday $2.39 → 09:30 $2.41 +10.64 | — |
| 2026-09-29 09:30 ET | **SELL** | `HLP` | 532 | $2.41 | $6.96 | $+97.90 | $9,305.51 | ▲ +97.90 after sell → book $9,305.51; vs 09:30 mark -6.96 | dropped from list after 2 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SN` | 6 | $184.05 | $2.01 | — | $8,199.20 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list flatten; 🔵; ⚪; ret5=+8.7; leftover $1163.19 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `DT` | 20 | $57.39 | $2.05 | — | $7,049.35 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list flatten; ⚪; ret5=+2.6; leftover $1163.19 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `TOST` | 38 | $30.40 | $2.10 | — | $5,892.05 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list flatten; ⚪; ret5=+1.9; leftover $1163.19 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SONO` | 65 | $17.76 | $2.19 | — | $4,735.46 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list flatten; ⚪; ret5=+9.8; leftover $1163.19 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-29 09:30 ET | **BUY** | `PDFS` | 23 | $50.25 | $2.06 | — | $3,577.65 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list flatten; ⚪; ret5=+6.5; leftover $1163.19 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SHOO` | 25 | $45.06 | $2.06 | — | $2,449.09 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list flatten; 🔵; ⚪; ret5=+4.8; leftover $1163.19 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `ABCL` | 82 | $14.12 | $2.24 | — | $1,289.01 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list probable,yday_gainer,ohlc_hot; ret5=+10.0; leftover $1163.19 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `ADMA` | 115 | $10.04 | $2.33 | — | $132.08 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list probable,yday_gainer,ohlc_hot; ⚪; ret5=+6.5; leftover $1163.19 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.08 | ▲ close $9,328.90 vs 09:30 $9,312.47 (session +40.43) | 16:00 close · cash $132.08 · equity $9,328.90 vs 09:30 $9,312.47 (+16.43; session marks +40.43) · 8 name(s) marked open→close (per-name table). SN×6 09:30 $184.05 → close $182.44 -9.66; DT×20 09:30 $57.39 → close $57.53 +2.80; TOST×38 09:30 $30.40 → close $30.46 +2.28; SONO×65 09:30 $17.76 → close $17.85 +5.85; PDFS×23 09:30 $50.25 → close $49.77 -11.04; SHOO×25 09:30 $45.06 → close $45.14 +2.00; ABCL×82 09:30 $14.12 → close $14.82 +57.40; ADMA×115 09:30 $10.04 → close $9.96 -9.20 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.08 | ▲ 09:30 equity $9,328.90 vs yday $9,328.90 (+0.00) | 09:30 open · cash $132.08 (unchanged overnight, no fees) · equity $9,328.90 vs prior close $9,328.90 (+0.00) · 8 name(s) re-marked at the open (per-name table). ABCL×82 yday $14.82 → 09:30 $14.82 +0.00; ADMA×115 yday $9.96 → 09:30 $9.96 +0.00; DT×20 yday $57.53 → 09:30 $57.53 +0.00; PDFS×23 yday $49.77 → 09:30 $49.77 +0.00; SHOO×25 yday $45.14 → 09:30 $45.14 +0.00; SN×6 yday $182.44 → 09:30 $182.44 +0.00; SONO×65 yday $17.85 → 09:30 $17.85 +0.00; TOST×38 yday $30.46 → 09:30 $30.46 +0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.08 | ▲ close $9,328.90 vs 09:30 $9,328.90 (session +0.00) | 16:00 close · cash $132.08 · equity $9,328.90 vs 09:30 $9,328.90 (+0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). ABCL×82 09:30 $14.82 → close $14.82 +0.00; ADMA×115 09:30 $9.96 → close $9.96 +0.00; DT×20 09:30 $57.53 → close $57.53 +0.00; PDFS×23 09:30 $49.77 → close $49.77 +0.00; SHOO×25 09:30 $45.14 → close $45.14 +0.00; SN×6 09:30 $182.44 → close $182.44 +0.00; SONO×65 09:30 $17.85 → close $17.85 +0.00; TOST×38 09:30 $30.46 → close $30.46 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.08 | ▲ 09:30 equity $9,335.22 vs yday $9,328.90 (+6.32) | 09:30 open · cash $132.08 (unchanged overnight, no fees) · equity $9,335.22 vs prior close $9,328.90 (+6.32) · 8 name(s) re-marked at the open (per-name table). ABCL×82 yday $14.82 → 09:30 $14.59 -19.27; ADMA×115 yday $9.96 → 09:30 $10.07 +12.65; DT×20 yday $57.53 → 09:30 $59.04 +30.20; PDFS×23 yday $49.77 → 09:30 $51.41 +37.72; SHOO×25 yday $45.14 → 09:30 $44.46 -17.00; SN×6 yday $182.44 → 09:30 $182.44 +0.00; SONO×65 yday $17.85 → 09:30 $18.09 +15.60; TOST×38 yday $30.46 → 09:30 $29.05 -53.58 | — |
| 2026-10-01 09:30 ET | **SELL** | `ABCL` | 82 | $14.59 | $2.26 | $+33.63 | $1,325.79 | ▲ +33.63 after sell → book $9,332.96; vs 09:30 mark -2.26 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `ADMA` | 115 | $10.07 | $2.36 | $-1.25 | $2,481.48 | ▼ -1.25 after sell → book $9,330.60; vs 09:30 mark -2.36 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `DT` | 20 | $59.04 | $2.07 | $+28.88 | $3,660.21 | ▲ +28.88 after sell → book $9,328.53; vs 09:30 mark -2.07 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `PDFS` | 23 | $51.41 | $2.08 | $+22.54 | $4,840.56 | ▲ +22.54 after sell → book $9,326.45; vs 09:30 mark -2.08 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `SHOO` | 25 | $44.46 | $2.08 | $-19.15 | $5,949.97 | ▼ -19.15 after sell → book $9,324.36; vs 09:30 mark -2.09 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `SONO` | 65 | $18.09 | $2.21 | $+17.06 | $7,123.62 | ▲ +17.06 after sell → book $9,322.16; vs 09:30 mark -2.20 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `TOST` | 38 | $29.05 | $2.12 | $-55.53 | $8,225.39 | ▼ -55.53 after sell → book $9,320.03; vs 09:30 mark -2.13 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **BUY** | `AVPT` | 72 | $14.27 | $2.21 | — | $7,195.75 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list flatten,ohlc_hot; 🔵; ret5=+7.3; leftover $1028.17 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `CDNL` | 33 | $30.30 | $2.09 | — | $6,193.76 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list probable,yday_gainer; ret5=+6.7; leftover $1028.17 | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `ZETA` | 32 | $32.03 | $2.09 | — | $5,166.71 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list probable,yday_gainer; 🔵; ret5=+6.0; leftover $1028.17 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `ENTX` | 355 | $2.89 | $4.58 | — | $4,136.18 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list yday_gainer; ret5=+3.6; leftover $1028.17 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `PUSA` | 262 | $3.92 | $3.38 | — | $3,105.76 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list yday_gainer; ret5=+4.9; leftover $1028.17 | join🔴 sector🔴 gen🔴 news🟡 digest🟡 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `OMDA` | 49 | $20.90 | $2.14 | — | $2,079.53 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list yday_gainer; ret5=+0.6; leftover $1028.17 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `FOSL` | 156 | $6.55 | $2.46 | — | $1,055.27 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list ohlc_hot; ret5=+6.5; leftover $1028.17 | join🔴 sector🔴 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `ONTO` | 3 | $315.94 | $2.00 | — | $105.45 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list ohlc_hot; 🔵; ret5=+9.0; leftover $1028.17 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $105.45 | ▼ close $9,101.95 vs 09:30 $9,335.22 (session -197.15) | 16:00 close · cash $105.45 · equity $9,101.95 vs 09:30 $9,335.22 (-233.27; session marks -197.15) · 9 name(s) marked open→close (per-name table). SN×6 09:30 $182.44 → close $182.44 +0.00; AVPT×72 09:30 $14.27 → close $14.08 -13.68; CDNL×33 09:30 $30.30 → close $29.74 -18.48; ZETA×32 09:30 $32.03 → close $32.40 +11.84; ENTX×355 09:30 $2.89 → close $2.89 +0.00; PUSA×262 09:30 $3.92 → close $3.20 -188.64; OMDA×49 09:30 $20.90 → close $20.41 -24.01; FOSL×156 09:30 $6.55 → close $6.80 +39.00; ONTO×3 09:30 $315.94 → close $314.88 -3.18 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $105.45 | ▲ 09:30 equity $9,240.65 vs yday $9,101.95 (+138.70) | 09:30 open · cash $105.45 (unchanged overnight, no fees) · equity $9,240.65 vs prior close $9,101.95 (+138.70) · 9 name(s) re-marked at the open (per-name table). AVPT×72 yday $14.08 → 09:30 $14.22 +10.08; CDNL×33 yday $29.74 → 09:30 $30.29 +18.15; ENTX×355 yday $2.89 → 09:30 $2.89 +0.00; FOSL×156 yday $6.80 → 09:30 $6.95 +23.40; OMDA×49 yday $20.41 → 09:30 $20.74 +16.17; ONTO×3 yday $314.88 → 09:30 $328.89 +42.03; PUSA×262 yday $3.20 → 09:30 $3.25 +14.41; SN×6 yday $182.44 → 09:30 $183.57 +6.78; ZETA×32 yday $32.40 → 09:30 $32.64 +7.68 | — |
| 2026-10-02 09:30 ET | **SELL** | `AVPT` | 72 | $14.22 | $2.23 | $-8.03 | $1,127.06 | ▼ -8.03 after sell → book $9,238.42; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `CDNL` | 33 | $30.29 | $2.11 | $-4.53 | $2,124.52 | ▼ -4.53 after sell → book $9,236.31; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `ENTX` | 355 | $2.89 | $4.65 | $-9.23 | $3,145.82 | ▼ -9.23 after sell → book $9,231.66; vs 09:30 mark -4.65 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `FOSL` | 156 | $6.95 | $2.49 | $+57.45 | $4,227.53 | ▲ +57.45 after sell → book $9,229.17; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **SELL** | `OMDA` | 49 | $20.74 | $2.16 | $-12.13 | $5,241.63 | ▼ -12.13 after sell → book $9,227.01; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `ONTO` | 3 | $328.89 | $2.02 | $+34.83 | $6,226.28 | ▲ +34.83 after sell → book $9,224.99; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-02 09:30 ET | **SELL** | `PUSA` | 262 | $3.25 | $3.43 | $-181.04 | $7,075.66 | ▼ -181.04 after sell → book $9,221.56; vs 09:30 mark -3.43 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `SN` | 6 | $183.57 | $2.03 | $-6.92 | $8,175.05 | ▼ -6.92 after sell → book $9,219.53; vs 09:30 mark -2.03 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-02 09:30 ET | **SELL** | `ZETA` | 32 | $32.64 | $2.11 | $+15.33 | $9,217.43 | ▲ +15.33 after sell → book $9,217.43; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **BUY** | `CDNA` | 17 | $66.33 | $2.04 | — | $8,087.78 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list flatten,ohlc_hot; 🔵; ⚪; ret5=+7.9; leftover $1152.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `COHR` | 3 | $316.56 | $2.00 | — | $7,136.08 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list probable,yday_gainer; 🔵; ret5=+9.8; leftover $1152.18 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `RR` | 643 | $1.79 | $8.29 | — | $5,976.82 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list probable,yday_gainer; ret5=+6.7; leftover $1152.18 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `GWRE` | 7 | $155.67 | $2.01 | — | $4,885.12 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list probable,yday_gainer; ret5=+2.9; leftover $1152.18 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `LWLG` | 203 | $5.66 | $2.62 | — | $3,733.52 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1152.18 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 judge🔴 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `AAOI` | 10 | $110.25 | $2.02 | — | $2,629.00 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list yday_gainer; ret5=+6.2; leftover $1152.18 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `CRDO` | 5 | $215.00 | $2.00 | — | $1,552.02 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list yday_gainer; 🔵; ret5=+7.2; leftover $1152.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `CIEN` | 2 | $385.00 | $2.00 | — | $780.02 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list yday_gainer; ret5=+5.8; leftover $1152.18 | join🔴 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $780.02 | ▲ close $9,266.86 vs 09:30 $9,240.65 (session +72.42) | 16:00 close · cash $780.02 · equity $9,266.86 vs 09:30 $9,240.65 (+26.21; session marks +72.42) · 8 name(s) marked open→close (per-name table). CDNA×17 09:30 $66.33 → close $67.15 +13.94; COHR×3 09:30 $316.56 → close $337.04 +61.43; RR×643 09:30 $1.79 → close $1.68 -70.73; GWRE×7 09:30 $155.67 → close $152.15 -24.64; LWLG×203 09:30 $5.66 → close $5.70 +8.12; AAOI×10 09:30 $110.25 → close $115.59 +53.40; CRDO×5 09:30 $215.00 → close $218.64 +18.22; CIEN×2 09:30 $385.00 → close $391.34 +12.68 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $780.02 | ▲ 09:30 equity $9,305.62 vs yday $9,266.86 (+38.76) | 09:30 open · cash $780.02 (unchanged overnight, no fees) · equity $9,305.62 vs prior close $9,266.86 (+38.76) · 8 name(s) re-marked at the open (per-name table). AAOI×10 yday $115.59 → 09:30 $118.64 +30.55; CDNA×17 yday $67.15 → 09:30 $66.90 -4.25; CIEN×2 yday $391.34 → 09:30 $393.77 +4.86; COHR×3 yday $337.04 → 09:30 $340.93 +11.65; CRDO×5 yday $218.64 → 09:30 $220.83 +10.95; GWRE×7 yday $152.15 → 09:30 $152.52 +2.59; LWLG×203 yday $5.70 → 09:30 $5.64 -11.17; RR×643 yday $1.68 → 09:30 $1.67 -6.43 | — |
| 2026-10-05 09:30 ET | **SELL** | `AAOI` | 10 | $118.64 | $2.04 | $+79.89 | $1,964.43 | ▲ +79.89 after sell → book $9,303.58; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **SELL** | `CDNA` | 17 | $66.90 | $2.06 | $+5.59 | $3,099.67 | ▲ +5.59 after sell → book $9,301.52; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `CIEN` | 2 | $393.77 | $2.02 | $+13.53 | $3,885.19 | ▲ +13.53 after sell → book $9,299.50; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `COHR` | 3 | $340.93 | $2.02 | $+69.06 | $4,905.95 | ▲ +69.06 after sell → book $9,297.48; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `CRDO` | 5 | $220.83 | $2.02 | $+25.14 | $6,008.07 | ▲ +25.14 after sell → book $9,295.46; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `GWRE` | 7 | $152.52 | $2.03 | $-26.09 | $7,073.68 | ▼ -26.09 after sell → book $9,293.43; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `LWLG` | 203 | $5.64 | $2.66 | $-8.33 | $8,216.96 | ▼ -8.33 after sell → book $9,290.77; vs 09:30 mark -2.66 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `RR` | 643 | $1.67 | $8.41 | $-93.87 | $9,282.35 | ▼ -93.87 after sell → book $9,282.35; vs 09:30 mark -8.42 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **BUY** | `WNC` | 165 | $14.02 | $2.48 | — | $6,966.57 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+7.8; leftover $2320.59 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `EVGO` | 1681 | $1.38 | $21.68 | — | $4,625.10 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list probable,yday_gainer; ret5=+0.0; leftover $2320.59 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `INIO` | 115 | $20.09 | $2.33 | — | $2,312.42 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list probable,yday_gainer; ret5=+4.6; leftover $2320.59 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `ON` | 27 | $85.00 | $2.07 | — | $15.35 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list ohlc_hot; ret5=+10.0; leftover $2320.59 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.35 | ▼ close $9,095.86 vs 09:30 $9,305.62 (session -157.92) | 16:00 close · cash $15.35 · equity $9,095.86 vs 09:30 $9,305.62 (-209.76; session marks -157.92) · 4 name(s) marked open→close (per-name table). WNC×165 09:30 $14.02 → close $13.99 -4.95; EVGO×1681 09:30 $1.38 → close $1.35 -50.43; INIO×115 09:30 $20.09 → close $18.98 -127.65; ON×27 09:30 $85.00 → close $85.93 +25.11 | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.35 | ▲ 09:30 equity $9,205.65 vs yday $9,095.86 (+109.79) | 09:30 open · cash $15.35 (unchanged overnight, no fees) · equity $9,205.65 vs prior close $9,095.86 (+109.79) · 4 name(s) re-marked at the open (per-name table). EVGO×1681 yday $1.35 → 09:30 $1.36 +15.30; INIO×115 yday $18.98 → 09:30 $19.33 +40.25; ON×27 yday $85.93 → 09:30 $86.90 +26.19; WNC×165 yday $13.99 → 09:30 $14.16 +28.05 | — |
| 2026-10-06 09:30 ET | **SELL** | `EVGO` | 1681 | $1.36 | $21.98 | $-78.80 | $2,278.01 | ▼ -78.80 after sell → book $9,183.66; vs 09:30 mark -21.99 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `INIO` | 115 | $19.33 | $2.37 | $-92.11 | $4,498.59 | ▼ -92.11 after sell → book $9,181.29; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `ON` | 27 | $86.90 | $2.10 | $+47.13 | $6,842.79 | ▲ +47.13 after sell → book $9,179.19; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `WNC` | 165 | $14.16 | $2.53 | $+18.08 | $9,176.66 | ▲ +18.08 after sell → book $9,176.66; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **BUY** | `AVPT` | 88 | $14.77 | $2.25 | — | $7,874.65 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list flatten; ⚪; ret5=+7.2; leftover $1310.95 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `EBS` | 183 | $7.16 | $2.54 | — | $6,561.83 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list probable,yday_gainer,yday_mover; ret5=+5.5; leftover $1310.95 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `ARCT` | 89 | $14.70 | $2.26 | — | $5,251.27 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list probable,yday_gainer; ret5=+1.4; leftover $1310.95 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `XERS` | 128 | $10.23 | $2.37 | — | $3,939.46 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list probable; ret5=+3.1; leftover $1310.95 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `ZNTL` | 466 | $2.81 | $6.01 | — | $2,623.99 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list probable; ret5=+6.9; leftover $1310.95 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `SIBN` | 65 | $20.00 | $2.19 | — | $1,321.80 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list probable; ret5=+5.6; leftover $1310.95 | join🟡 sector🟡 gen🟡 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `ILMN` | 4 | $301.58 | $2.00 | — | $113.48 | — | combo gate; gate last_green=True,ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2,rvol_min=0.7; list ohlc_hot; ret5=+8.0; leftover $1310.95 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.48 | ▼ close $8,724.34 vs 09:30 $9,205.65 (session -432.70) | 16:00 close · cash $113.48 · equity $8,724.34 vs 09:30 $9,205.65 (-481.31; session marks -432.70) · 7 name(s) marked open→close (per-name table). AVPT×88 09:30 $14.77 → close $14.59 -15.84; EBS×183 09:30 $7.16 → close $6.95 -38.43; ARCT×89 09:30 $14.70 → close $13.00 -151.30; XERS×128 09:30 $10.23 → close $10.07 -20.48; ZNTL×466 09:30 $2.81 → close $2.67 -65.24; SIBN×65 09:30 $20.00 → close $19.55 -29.25; ILMN×4 09:30 $301.58 → close $273.54 -112.16 | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.48 | ▼ 09:30 equity $8,656.27 vs yday $8,724.34 (-68.07) | 09:30 open · cash $113.48 (unchanged overnight, no fees) · equity $8,656.27 vs prior close $8,724.34 (-68.07) · 7 name(s) re-marked at the open (per-name table). ARCT×89 yday $13.00 → 09:30 $12.71 -25.81; AVPT×88 yday $14.59 → 09:30 $14.51 -7.04; EBS×183 yday $6.95 → 09:30 $7.05 +18.30; ILMN×4 yday $273.54 → 09:30 $271.61 -7.72; SIBN×65 yday $19.55 → 09:30 $19.27 -18.20; XERS×128 yday $10.07 → 09:30 $10.00 -8.96; ZNTL×466 yday $2.67 → 09:30 $2.63 -18.64 | — |
| 2026-10-07 09:30 ET | **SELL** | `ARCT` | 89 | $12.71 | $2.28 | $-181.65 | $1,242.39 | ▼ -181.65 after sell → book $8,653.99; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `AVPT` | 88 | $14.51 | $2.28 | $-27.41 | $2,516.99 | ▼ -27.41 after sell → book $8,651.71; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `EBS` | 183 | $7.05 | $2.58 | $-25.25 | $3,804.56 | ▼ -25.25 after sell → book $8,649.13; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `ILMN` | 4 | $271.61 | $2.02 | $-123.90 | $4,888.98 | ▼ -123.90 after sell → book $8,647.11; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `SIBN` | 65 | $19.27 | $2.21 | $-51.84 | $6,139.32 | ▼ -51.84 after sell → book $8,644.90; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `XERS` | 128 | $10.00 | $2.41 | $-34.22 | $7,416.92 | ▼ -34.22 after sell → book $8,642.50; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `ZNTL` | 466 | $2.63 | $6.10 | $-95.99 | $8,636.40 | ▼ -95.99 after sell → book $8,636.40; vs 09:30 mark -6.10 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,636.40 | ▲ close $8,636.40 vs 09:30 $8,656.27 (session +0.00) | 16:00 close · cash $8,636.40 · no lots left · equity $8,636.40. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MRCY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBNX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BETA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `U` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `VSTM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABAT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BZ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `VIPS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RCKT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `GWRE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TII` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `YEXT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FNKO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AGCO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VIR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LAND` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `XRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `WDS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HELP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AVXL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `FATE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `STX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TJGC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLMT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `QRVO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CRDL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `KGS` | hard_red | hard-red S=-13.28 sit; no new buys |
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
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IOT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INIO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-21 | `SNDK` | cash | leftover split 1209.64 < 1 share @ 1826.00 |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SGML` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-22 | `NTSK` | no_price | no 09:30 open |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CRWD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RNG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AVT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `OPRT` | hard_red | hard-red S=-7.66 sit; no new buys |
