# Factor mine action — `union_coil_green_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-18.32%** ($8,168) · signal-only (no cash/fees) was -21.07%. Starts YES **1/30**. Fills 174 · skips 304 · realized $-1242.34.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,503.55.

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
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 3 | $0.94 | $0.04 | — | $21.80 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $3.08 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 2 | $1.50 | $0.04 | — | $18.76 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $3.08 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.76 | ▼ close $10,471.51 vs 09:30 $10,916.78 (session -445.20) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.76 | ▼ 09:30 equity $10,400.52 vs yday $10,471.51 (-70.99) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `IQ` | 2 | $1.35 | $0.03 | — | $16.03 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list overnight; ⚪; ret5=+1.5; leftover $3.13 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.03 | ▼ close $10,223.21 vs 09:30 $10,400.52 (session -177.28) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.03 | ▼ 09:30 equity $10,222.95 vs yday $10,223.21 (-0.26) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 197 | $51.77 | $2.70 | $+220.64 | $10,212.03 | ▲ +220.64 after sell → book $10,220.26; vs 09:30 mark -2.69 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,212.03 | ▼ close $10,219.96 vs 09:30 $10,222.95 (session -0.30) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,212.03 | ▼ 09:30 equity $10,219.89 vs yday $10,219.96 (-0.07) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 3 | $0.88 | $0.06 | $-0.26 | $10,214.61 | ▼ -0.26 after sell → book $10,219.83; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 2 | $1.42 | $0.05 | $-0.25 | $10,217.40 | ▼ -0.25 after sell → book $10,219.78; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,217.40 | ▼ close $10,219.66 vs 09:30 $10,219.89 (session -0.12) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,217.40 | ▼ 09:30 equity $10,219.64 vs yday $10,219.66 (-0.02) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `IQ` | 2 | $1.12 | $0.05 | $-0.54 | $10,219.59 | ▼ -0.54 after sell → book $10,219.59; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $8,943.31 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1277.45 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 221 | $5.77 | $2.85 | — | $7,665.29 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1277.45 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $6,387.16 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1277.45 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $5,110.95 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1277.45 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 729 | $1.75 | $9.40 | — | $3,825.79 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1277.45 | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 171 | $7.45 | $2.50 | — | $2,549.34 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1277.45 | — |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 118 | $10.77 | $2.34 | — | $1,276.14 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1277.45 | — |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 134 | $9.46 | $2.39 | — | $6.10 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1277.45 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.10 | ▲ close $10,303.05 vs 09:30 $10,219.64 (session +109.44) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.10 | ▲ 09:30 equity $10,606.59 vs yday $10,303.05 (+303.54) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1 | $0.86 | $0.01 | — | $5.23 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $0.87 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.23 | ▼ close $10,497.07 vs 09:30 $10,606.59 (session -109.51) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.23 | ▲ 09:30 equity $10,591.52 vs yday $10,497.07 (+94.45) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.23 | ▼ close $10,449.13 vs 09:30 $10,591.52 (session -142.39) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.23 | ▼ 09:30 equity $10,326.50 vs yday $10,449.13 (-122.63) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.32 | $2.20 | $-18.63 | $1,262.87 | ▼ -18.63 after sell → book $10,324.30; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 221 | $5.53 | $2.90 | $-58.79 | $2,482.10 | ▼ -58.79 after sell → book $10,321.40; vs 09:30 mark -2.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.21 | $2.21 | $+98.31 | $3,858.55 | ▲ +98.31 after sell → book $10,319.20; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.32 | $2.14 | $+111.41 | $5,246.17 | ▲ +111.41 after sell → book $10,317.06; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 729 | $1.90 | $9.54 | $+90.41 | $6,621.73 | ▲ +90.41 after sell → book $10,307.52; vs 09:30 mark -9.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DNA` | 171 | $6.94 | $2.54 | $-92.25 | $7,805.93 | ▼ -92.25 after sell → book $10,304.98; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EXK` | 118 | $10.44 | $2.37 | $-43.66 | $9,035.48 | ▼ -43.66 after sell → book $10,302.61; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SCZM` | 134 | $9.45 | $2.42 | $-6.16 | $10,299.35 | ▼ -6.16 after sell → book $10,300.18; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 94 | $13.59 | $2.27 | — | $9,019.62 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1287.42 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 34 | $36.96 | $2.09 | — | $7,760.89 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1287.42 | — |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 19 | $64.55 | $2.05 | — | $6,532.39 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+4.4; leftover $1287.42 | — |
| 2026-08-25 09:30 ET | **BUY** | `ANRO` | 35 | $36.52 | $2.10 | — | $5,252.10 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.9; leftover $1287.42 | — |
| 2026-08-25 09:30 ET | **BUY** | `ANF` | 11 | $112.17 | $2.02 | — | $4,016.20 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list overnight; 🔵; ⚪; ret5=+6.8; leftover $1287.42 | — |
| 2026-08-25 09:30 ET | **BUY** | `BOX` | 38 | $33.33 | $2.10 | — | $2,747.56 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list overnight; 🔵; ret5=+3.7; leftover $1287.42 | — |
| 2026-08-25 09:30 ET | **BUY** | `FSCO` | 252 | $5.10 | $3.25 | — | $1,459.11 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list overnight; ret5=+0.2; leftover $1287.42 | — |
| 2026-08-25 09:30 ET | **BUY** | `NCNO` | 62 | $20.76 | $2.18 | — | $169.81 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list overnight; 🔵; ret5=+3.6; leftover $1287.42 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $169.81 | ▼ close $10,247.67 vs 09:30 $10,326.50 (session -34.45) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $169.81 | ▲ 09:30 equity $10,440.48 vs yday $10,247.67 (+192.81) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ORBS` | 1 | $0.80 | $0.03 | $-0.11 | $170.58 | ▼ -0.11 after sell → book $10,440.45; vs 09:30 mark -0.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `CRMD` | 2 | $8.60 | $0.18 | — | $153.20 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+4.8; leftover $24.37 | — |
| 2026-08-26 09:30 ET | **BUY** | `RZLT` | 4 | $5.01 | $0.21 | — | $132.95 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,yday_gainer; 🔵; ret5=+7.5; leftover $24.37 | — |
| 2026-08-26 09:30 ET | **BUY** | `SENS` | 2 | $9.48 | $0.20 | — | $113.79 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $24.37 | — |
| 2026-08-26 09:30 ET | **BUY** | `ACRS` | 3 | $6.53 | $0.20 | — | $94.00 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.6; leftover $24.37 | — |
| 2026-08-26 09:30 ET | **BUY** | `TMCI` | 5 | $4.78 | $0.25 | — | $69.84 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+8.1; leftover $24.37 | — |
| 2026-08-26 09:30 ET | **BUY** | `CRDL` | 12 | $2.03 | $0.28 | — | $45.20 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+5.5; leftover $24.37 | — |
| 2026-08-26 09:30 ET | **BUY** | `LI` | 2 | $12.14 | $0.25 | — | $20.67 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+1.2; leftover $24.37 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.67 | ▲ close $10,712.06 vs 09:30 $10,440.48 (session +273.19) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.67 | ▼ 09:30 equity $10,705.22 vs yday $10,712.06 (-6.84) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.67 | ▲ close $10,867.13 vs 09:30 $10,705.22 (session +161.91) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.67 | ▼ 09:30 equity $10,804.55 vs yday $10,867.13 (-62.58) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 94 | $13.05 | $2.30 | $-55.33 | $1,245.08 | ▼ -55.33 after sell → book $10,802.26; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 34 | $39.60 | $2.11 | $+85.56 | $2,589.36 | ▲ +85.56 after sell → book $10,800.14; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ETON` | 19 | $61.98 | $2.07 | $-52.94 | $3,764.92 | ▼ -52.94 after sell → book $10,798.08; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANRO` | 35 | $35.00 | $2.12 | $-57.41 | $4,987.80 | ▼ -57.41 after sell → book $10,795.96; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANF` | 11 | $146.07 | $2.05 | $+368.83 | $6,592.53 | ▲ +368.83 after sell → book $10,793.92; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BOX` | 38 | $34.75 | $2.12 | $+49.73 | $7,910.90 | ▲ +49.73 after sell → book $10,791.79; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FSCO` | 252 | $5.12 | $3.30 | $-1.51 | $9,197.84 | ▼ -1.51 after sell → book $10,788.49; vs 09:30 mark -3.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NCNO` | 62 | $23.30 | $2.20 | $+153.11 | $10,640.24 | ▲ +153.11 after sell → book $10,786.29; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 90 | $14.63 | $2.26 | — | $9,321.28 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+5.8; leftover $1330.03 | — |
| 2026-08-28 09:30 ET | **BUY** | `EQ` | 540 | $2.46 | $6.97 | — | $7,985.91 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+7.9; leftover $1330.03 | — |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 35 | $37.49 | $2.10 | — | $6,671.67 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+5.4; leftover $1330.03 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 70 | $19.00 | $2.20 | — | $5,339.47 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+7.5; leftover $1330.03 | — |
| 2026-08-28 09:30 ET | **BUY** | `FSM` | 103 | $12.84 | $2.30 | — | $4,014.65 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+7.6; leftover $1330.03 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $2,706.85 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+7.8; leftover $1330.03 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 29 | $44.40 | $2.08 | — | $1,417.17 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+0.4; leftover $1330.03 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 159 | $8.35 | $2.47 | — | $87.05 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+5.1; leftover $1330.03 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $87.05 | ▼ close $10,561.08 vs 09:30 $10,804.55 (session -202.84) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.05 | ▼ 09:30 equity $10,557.41 vs yday $10,561.08 (-3.67) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CRMD` | 2 | $8.26 | $0.19 | $-1.05 | $103.38 | ▼ -1.05 after sell → book $10,557.22; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `RZLT` | 4 | $4.65 | $0.22 | $-1.87 | $121.76 | ▼ -1.87 after sell → book $10,557.00; vs 09:30 mark -0.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `SENS` | 2 | $9.29 | $0.21 | $-0.79 | $140.13 | ▼ -0.79 after sell → book $10,556.79; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ACRS` | 3 | $5.97 | $0.21 | $-2.09 | $157.83 | ▼ -2.09 after sell → book $10,556.58; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TMCI` | 5 | $4.60 | $0.27 | $-1.42 | $180.57 | ▼ -1.42 after sell → book $10,556.32; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRDL` | 12 | $1.92 | $0.29 | $-1.89 | $203.32 | ▼ -1.89 after sell → book $10,556.03; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `LI` | 2 | $12.28 | $0.27 | $-0.24 | $227.61 | ▼ -0.24 after sell → book $10,555.76; vs 09:30 mark -0.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $227.61 | ▼ close $10,481.13 vs 09:30 $10,557.41 (session -74.63) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $227.61 | ▲ 09:30 equity $10,516.77 vs yday $10,481.13 (+35.64) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $227.61 | ▼ close $10,407.00 vs 09:30 $10,516.77 (session -109.77) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $227.61 | ▼ 09:30 equity $10,375.95 vs yday $10,407.00 (-31.05) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 90 | $15.70 | $2.29 | $+91.75 | $1,638.32 | ▲ +91.75 after sell → book $10,373.66; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `EQ` | 540 | $2.20 | $7.07 | $-154.43 | $2,819.26 | ▼ -154.43 after sell → book $10,366.60; vs 09:30 mark -7.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FIGR` | 35 | $33.31 | $2.12 | $-150.51 | $3,982.99 | ▼ -150.51 after sell → book $10,364.48; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TH` | 70 | $17.98 | $2.22 | $-75.82 | $5,239.37 | ▼ -75.82 after sell → book $10,362.26; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FSM` | 103 | $12.08 | $2.33 | $-82.91 | $6,481.28 | ▼ -82.91 after sell → book $10,359.93; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 5 | $246.70 | $2.02 | $-76.33 | $7,712.76 | ▼ -76.33 after sell → book $10,357.91; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 29 | $44.17 | $2.10 | $-10.84 | $8,991.59 | ▼ -10.84 after sell → book $10,355.81; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 159 | $8.58 | $2.50 | $+31.60 | $10,353.31 | ▲ +31.60 after sell → book $10,353.31; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,353.31 | ▲ close $10,353.31 vs 09:30 $10,375.95 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,353.31 | ▲ 09:30 equity $10,353.31 vs yday $10,353.31 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $52.88 | $2.06 | — | $9,082.13 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1294.16 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $42.93 | $2.08 | — | $7,792.15 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1294.16 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 356 | $3.63 | $4.59 | — | $6,495.27 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1294.16 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $5,301.21 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1294.16 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 77 | $16.77 | $2.22 | — | $4,007.70 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1294.16 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 593 | $2.18 | $7.65 | — | $2,707.31 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1294.16 | — |
| 2026-09-03 09:30 ET | **BUY** | `SDGR` | 61 | $21.03 | $2.17 | — | $1,422.30 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $1294.16 | — |
| 2026-09-03 09:30 ET | **BUY** | `NEOV` | 343 | $3.77 | $4.42 | — | $124.77 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; 🔵; ⚪; ret5=+8.6; leftover $1294.16 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.77 | ▼ close $10,106.73 vs 09:30 $10,353.31 (session -219.36) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.77 | ▼ 09:30 equity $10,072.44 vs yday $10,106.73 (-34.29) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 1 | $7.87 | $0.08 | — | $116.82 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.7; leftover $15.60 | — |
| 2026-09-04 09:30 ET | **BUY** | `GORO` | 3 | $3.95 | $0.13 | — | $104.84 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+6.9; leftover $15.60 | — |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 1 | $8.94 | $0.09 | — | $95.81 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+7.7; leftover $15.60 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.81 | ▲ close $10,108.90 vs 09:30 $10,072.44 (session +36.76) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.81 | ▼ 09:30 equity $10,103.63 vs yday $10,108.90 (-5.27) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.81 | ▼ close $10,069.72 vs 09:30 $10,103.63 (session -33.91) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.81 | ▼ 09:30 equity $9,996.98 vs yday $10,069.72 (-72.74) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 24 | $53.16 | $2.08 | $+2.58 | $1,369.56 | ▲ +2.58 after sell → book $9,994.89; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 30 | $42.01 | $2.10 | $-31.78 | $2,627.76 | ▼ -31.78 after sell → book $9,992.79; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 356 | $3.28 | $4.66 | $-133.85 | $3,790.78 | ▼ -133.85 after sell → book $9,988.13; vs 09:30 mark -4.66 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $4,920.68 | ▼ -64.17 after sell → book $9,986.10; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 77 | $15.46 | $2.24 | $-105.33 | $6,108.85 | ▼ -105.33 after sell → book $9,983.85; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 593 | $2.22 | $7.76 | $+8.31 | $7,417.55 | ▲ +8.31 after sell → book $9,976.09; vs 09:30 mark -7.76 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SDGR` | 61 | $19.88 | $2.19 | $-74.52 | $8,628.04 | ▼ -74.52 after sell → book $9,973.90; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `NEOV` | 343 | $3.84 | $4.49 | $+15.09 | $9,940.67 | ▲ +15.09 after sell → book $9,969.41; vs 09:30 mark -4.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,940.67 | ▼ close $9,967.93 vs 09:30 $9,996.98 (session -1.48) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,940.67 | ▼ 09:30 equity $9,967.16 vs yday $9,967.93 (-0.77) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `USDE` | 1 | $6.73 | $0.09 | $-1.31 | $9,947.31 | ▼ -1.31 after sell → book $9,967.07; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `GORO` | 3 | $3.56 | $0.14 | $-1.43 | $9,957.85 | ▼ -1.43 after sell → book $9,966.93; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `HAFN` | 1 | $9.08 | $0.11 | $-0.07 | $9,966.82 | ▼ -0.07 after sell → book $9,966.82; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,966.82 | ▲ close $9,966.82 vs 09:30 $9,967.16 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,966.82 | ▲ 09:30 equity $9,966.82 vs yday $9,966.82 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 610 | $2.04 | $7.87 | — | $8,714.55 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1245.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 262 | $4.75 | $3.38 | — | $7,466.67 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1245.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 587 | $2.12 | $7.57 | — | $6,214.66 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1245.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 107 | $11.55 | $2.31 | — | $4,976.50 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1245.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 16 | $77.33 | $2.04 | — | $3,737.18 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+2.5; leftover $1245.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 23 | $52.55 | $2.06 | — | $2,526.47 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1245.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 123 | $10.11 | $2.36 | — | $1,280.58 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $1245.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `ZSQR` | 383 | $3.25 | $4.94 | — | $30.89 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+3.6; leftover $1245.85 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.89 | ▼ close $9,923.40 vs 09:30 $9,966.82 (session -10.89) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.89 | ▼ 09:30 equity $9,904.38 vs yday $9,923.40 (-19.02) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.89 | ▼ close $9,631.97 vs 09:30 $9,904.38 (session -272.41) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.89 | ▼ 09:30 equity $9,617.82 vs yday $9,631.97 (-14.15) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.89 | ▼ close $9,290.57 vs 09:30 $9,617.82 (session -327.25) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.89 | ▼ 09:30 equity $9,050.37 vs yday $9,290.57 (-240.20) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 610 | $1.89 | $7.98 | $-107.35 | $1,175.81 | ▼ -107.35 after sell → book $9,042.39; vs 09:30 mark -7.98 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 262 | $4.73 | $3.43 | $-12.05 | $2,411.64 | ▼ -12.05 after sell → book $9,038.96; vs 09:30 mark -3.43 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAK` | 587 | $1.84 | $7.68 | $-179.61 | $3,484.04 | ▼ -179.61 after sell → book $9,031.28; vs 09:30 mark -7.68 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `FUBO` | 107 | $10.75 | $2.34 | $-90.25 | $4,631.95 | ▼ -90.25 after sell → book $9,028.94; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `VIST` | 16 | $76.75 | $2.06 | $-13.38 | $5,857.89 | ▼ -13.38 after sell → book $9,026.88; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAND` | 23 | $48.60 | $2.08 | $-94.99 | $6,973.61 | ▼ -94.99 after sell → book $9,024.80; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `PAGS` | 123 | $9.39 | $2.39 | $-93.31 | $8,126.19 | ▼ -93.31 after sell → book $9,022.41; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ZSQR` | 383 | $2.34 | $5.01 | $-358.49 | $9,017.40 | ▼ -358.49 after sell → book $9,017.40; vs 09:30 mark -5.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $7,931.83 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+4.0; leftover $1127.17 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 30 | $36.46 | $2.08 | — | $6,835.95 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+2.9; leftover $1127.17 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 16 | $68.79 | $2.04 | — | $5,733.28 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1127.17 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 192 | $5.87 | $2.57 | — | $4,603.67 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1127.17 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 12 | $87.40 | $2.03 | — | $3,552.84 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1127.17 | — |
| 2026-09-16 09:30 ET | **BUY** | `MRCY` | 12 | $87.52 | $2.03 | — | $2,500.58 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+4.3; leftover $1127.17 | — |
| 2026-09-16 09:30 ET | **BUY** | `QLYS` | 6 | $179.60 | $2.01 | — | $1,420.97 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+8.8; leftover $1127.17 | — |
| 2026-09-16 09:30 ET | **BUY** | `ILMN` | 5 | $224.49 | $2.00 | — | $296.52 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+5.3; leftover $1127.17 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $296.52 | ▼ close $8,918.55 vs 09:30 $9,050.37 (session -82.10) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $296.52 | ▲ 09:30 equity $9,047.59 vs yday $8,918.55 (+129.04) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 4 | $7.59 | $0.32 | — | $265.84 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $37.06 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 1 | $34.93 | $0.35 | — | $230.56 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+1.6; leftover $37.06 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 15 | $2.40 | $0.41 | — | $194.15 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $37.06 | — |
| 2026-09-17 09:30 ET | **BUY** | `AIB` | 25 | $1.46 | $0.44 | — | $157.21 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+4.4; leftover $37.06 | — |
| 2026-09-17 09:30 ET | **BUY** | `BYND` | 3 | $11.19 | $0.34 | — | $123.30 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+0.5; leftover $37.06 | — |
| 2026-09-17 09:30 ET | **BUY** | `QTRX` | 12 | $2.94 | $0.39 | — | $87.63 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,ohlc_hot; 🔵; ret5=+9.8; leftover $37.06 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $87.63 | ▲ close $9,188.88 vs 09:30 $9,047.59 (session +143.54) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.63 | ▲ 09:30 equity $9,239.33 vs yday $9,188.88 (+50.45) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 3 | $3.58 | $0.12 | — | $76.77 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $10.95 | — |
| 2026-09-18 09:30 ET | **BUY** | `RANI` | 12 | $0.85 | $0.14 | — | $66.43 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+3.6; leftover $10.95 | — |
| 2026-09-18 09:30 ET | **BUY** | `SHLS` | 1 | $7.64 | $0.08 | — | $58.72 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+7.6; leftover $10.95 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $58.72 | ▼ close $9,065.74 vs 09:30 $9,239.33 (session -173.27) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $58.72 | ▲ 09:30 equity $9,109.83 vs yday $9,065.74 (+44.09) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 4 | $266.76 | $2.02 | $-20.54 | $1,123.73 | ▼ -20.54 after sell → book $9,107.81; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 30 | $36.70 | $2.10 | $+3.02 | $2,222.63 | ▲ +3.02 after sell → book $9,105.71; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 16 | $79.08 | $2.06 | $+160.54 | $3,485.86 | ▲ +160.54 after sell → book $9,103.65; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 192 | $5.62 | $2.61 | $-53.17 | $4,562.29 | ▼ -53.17 after sell → book $9,101.05; vs 09:30 mark -2.60 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `VAL` | 12 | $83.46 | $2.05 | $-51.35 | $5,561.76 | ▼ -51.35 after sell → book $9,099.00; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `MRCY` | 12 | $86.52 | $2.05 | $-16.07 | $6,597.96 | ▼ -16.07 after sell → book $9,096.95; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QLYS` | 6 | $175.63 | $2.03 | $-27.86 | $7,649.71 | ▼ -27.86 after sell → book $9,094.93; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ILMN` | 5 | $241.51 | $2.02 | $+81.07 | $8,855.23 | ▲ +81.07 after sell → book $9,092.90; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 7 | $157.87 | $2.01 | — | $7,748.13 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+6.5; leftover $1106.90 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 82 | $13.47 | $2.24 | — | $6,640.95 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1106.90 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 997 | $1.11 | $12.86 | — | $5,521.41 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1106.90 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 110 | $9.99 | $2.32 | — | $4,420.19 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1106.90 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 606 | $1.82 | $7.82 | — | $3,306.43 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1106.90 | — |
| 2026-09-21 09:30 ET | **BUY** | `SGML` | 109 | $10.13 | $2.32 | — | $2,199.39 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+4.9; leftover $1106.90 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 42 | $25.95 | $2.12 | — | $1,107.38 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1106.90 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,107.38 | ▼ close $9,016.10 vs 09:30 $9,109.83 (session -45.13) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,107.38 | ▼ 09:30 equity $8,993.25 vs yday $9,016.10 (-22.85) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `ALOY` | 29 | $9.40 | $2.08 | — | $832.70 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+9.5; leftover $276.84 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $832.70 | ▲ close $9,174.67 vs 09:30 $8,993.25 (session +183.50) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $832.70 | ▼ 09:30 equity $9,158.32 vs yday $9,174.67 (-16.35) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `PGEN` | 4 | $7.95 | $0.35 | $+0.77 | $864.15 | ▲ +0.77 after sell → book $9,157.97; vs 09:30 mark -0.35 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMN` | 1 | $34.78 | $0.37 | $-0.87 | $898.56 | ▼ -0.87 after sell → book $9,157.60; vs 09:30 mark -0.37 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SABR` | 15 | $2.24 | $0.40 | $-3.21 | $931.76 | ▼ -3.21 after sell → book $9,157.20; vs 09:30 mark -0.40 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `AIB` | 25 | $1.37 | $0.44 | $-3.13 | $965.57 | ▼ -3.13 after sell → book $9,156.76; vs 09:30 mark -0.44 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BYND` | 3 | $11.01 | $0.36 | $-1.24 | $998.24 | ▼ -1.24 after sell → book $9,156.40; vs 09:30 mark -0.36 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `QTRX` | 12 | $3.17 | $0.44 | $+1.93 | $1,035.85 | ▲ +1.93 after sell → book $9,155.97; vs 09:30 mark -0.43 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `DDD` | 3 | $3.59 | $0.14 | $-0.22 | $1,046.48 | ▼ -0.22 after sell → book $9,155.83; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RANI` | 12 | $0.81 | $0.15 | $-0.77 | $1,056.05 | ▼ -0.77 after sell → book $9,155.68; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SHLS` | 1 | $8.15 | $0.10 | $+0.33 | $1,064.09 | ▲ +0.33 after sell → book $9,155.57; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 1 | $89.50 | $0.90 | — | $973.69 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.3; leftover $133.01 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 13 | $9.81 | $1.31 | — | $844.85 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+4.0; leftover $133.01 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 6 | $20.65 | $1.26 | — | $719.69 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $133.01 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 33 | $3.93 | $1.40 | — | $588.61 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $133.01 | — |
| 2026-09-23 09:30 ET | **BUY** | `MNRO` | 9 | $14.14 | $1.30 | — | $460.05 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+0.3; leftover $133.01 | — |
| 2026-09-23 09:30 ET | **BUY** | `NTSK` | 7 | $18.57 | $1.32 | — | $328.70 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.2; leftover $133.01 | — |
| 2026-09-23 09:30 ET | **BUY** | `HIMS` | 4 | $30.40 | $1.23 | — | $205.87 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.3; leftover $133.01 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 3 | $40.00 | $1.21 | — | $84.66 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+6.7; leftover $133.01 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $84.66 | ▼ close $8,852.47 vs 09:30 $9,158.32 (session -293.17) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $84.66 | ▼ 09:30 equity $8,725.24 vs yday $8,852.47 (-127.23) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 7 | $163.95 | $2.03 | $+38.52 | $1,230.28 | ▲ +38.52 after sell → book $8,723.21; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 82 | $12.26 | $2.26 | $-104.13 | $2,233.34 | ▼ -104.13 after sell → book $8,720.95; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ORBS` | 997 | $1.05 | $13.04 | $-85.72 | $3,267.16 | ▼ -85.72 after sell → book $8,707.91; vs 09:30 mark -13.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SBET` | 110 | $9.80 | $2.35 | $-25.57 | $4,342.81 | ▼ -25.57 after sell → book $8,705.56; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTBT` | 606 | $1.73 | $7.93 | $-76.35 | $5,380.23 | ▼ -76.35 after sell → book $8,697.64; vs 09:30 mark -7.92 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGML` | 109 | $9.89 | $2.35 | $-31.37 | $6,455.90 | ▼ -31.37 after sell → book $8,695.29; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GLXY` | 42 | $25.00 | $2.14 | $-44.36 | $7,503.55 | ▼ -44.36 after sell → book $8,693.15; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,503.55 | ▲ close $8,706.64 vs 09:30 $8,725.24 (session +13.49) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,551.89 | ▼ 09:30 equity $8,155.80 vs yday $8,156.01 (-0.21) | 09:30 open · cash $6,551.89 (unchanged overnight, no fees) · equity $8,155.80 vs prior close $8,156.01 (-0.21) · 7 name(s) re-marked at the open (per-name table). APPS×25 yday $10.88 → 09:30 $10.88 +0.00; ARHS×34 yday $9.47 → 09:30 $9.47 +0.00; BTQ×109 yday $2.79 → 09:30 $2.79 +0.00; INDP×17 yday $4.00 → 09:30 $4.00 +0.00; NN×19 yday $14.45 → 09:30 $14.45 +0.00; NTSK×16 yday $18.57 → 09:30 $18.57 +0.00; SAIL×3 yday $22.12 → 09:30 $22.05 -0.21 | — |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 24 | $38.51 | $2.06 | — | $5,625.59 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+4.7; leftover $935.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 122 | $7.65 | $2.36 | — | $4,689.93 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.2; leftover $935.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 11 | $83.76 | $2.02 | — | $3,766.55 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $935.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 425 | $2.20 | $5.48 | — | $2,826.07 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $935.98 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 155 | $6.00 | $2.46 | — | $1,893.61 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $935.98 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 52 | $17.91 | $2.15 | — | $960.15 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable; 🔵; ret5=+3.7; leftover $935.98 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 11 | $83.69 | $2.02 | — | $37.48 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $935.98 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.48 | ▲ close $8,168.00 vs 09:30 $8,155.80 (session +30.75) | 16:00 close · cash $37.48 · equity $8,168.00 vs 09:30 $8,155.80 (+12.20; session marks +30.75) · 14 name(s) marked open→close (per-name table). APPS×25 09:30 $10.88 → close $10.88 +0.00; ARHS×34 09:30 $9.47 → close $9.47 +0.00; BTQ×109 09:30 $2.79 → close $2.79 -0.00; INDP×17 09:30 $4.00 → close $4.00 +0.00; NN×19 09:30 $14.45 → close $14.45 -0.00; NTSK×16 09:30 $18.57 → close $18.57 -0.00; SAIL×3 09:30 $22.05 → close $20.64 -4.23; BLFS×24 09:30 $38.51 → close $38.49 -0.48; MRVI×122 09:30 $7.65 → close $7.60 -6.10; TXG×11 09:30 $83.76 → close $85.71 +21.45; HLP×425 09:30 $2.20 → close $2.21 +4.25; SATL×155 09:30 $6.00 → close $6.17 +26.35; PL×52 09:30 $17.91 → close $17.43 -24.96; TEM×11 09:30 $83.69 → close $85.01 +14.47 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLG` | cash | leftover split 3.08 < 1 share @ 57.61 |
| 2026-08-14 | `ANGX` | cash | leftover split 3.08 < 1 share @ 4.31 |
| 2026-08-14 | `HYLN` | cash | leftover split 3.08 < 1 share @ 4.18 |
| 2026-08-14 | `WDC` | cash | leftover split 3.08 < 1 share @ 503.50 |
| 2026-08-14 | `ADUR` | cash | leftover split 3.08 < 1 share @ 16.50 |
| 2026-08-14 | `ALGM` | cash | leftover split 3.08 < 1 share @ 44.06 |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 3.13 < 1 share @ 46.18 |
| 2026-08-17 | `OCC` | cash | leftover split 3.13 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 3.13 < 1 share @ 16.20 |
| 2026-08-17 | `NEWP` | cash | leftover split 3.13 < 1 share @ 6.94 |
| 2026-08-17 | `KLAR` | cash | leftover split 3.13 < 1 share @ 20.67 |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `IQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MRCY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `IQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBNX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BETA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `U` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `VSTM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BTBT` | cash | leftover split 0.87 < 1 share @ 1.66 |
| 2026-08-21 | `CF` | cash | leftover split 0.87 < 1 share @ 127.43 |
| 2026-08-21 | `EMBC` | cash | leftover split 0.87 < 1 share @ 5.43 |
| 2026-08-21 | `TXG` | cash | leftover split 0.87 < 1 share @ 64.39 |
| 2026-08-21 | `DXYZ` | cash | leftover split 0.87 < 1 share @ 34.89 |
| 2026-08-21 | `PDD` | cash | leftover split 0.87 < 1 share @ 90.03 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABAT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BZ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `VIPS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ETON` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ANRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BOX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FSCO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ETON` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ANRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BOX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FSCO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NCNO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `SENS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ACRS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `TMCI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `LI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 2.95 < 1 share @ 41.44 |
| 2026-08-27 | `CRK` | cash | leftover split 2.95 < 1 share @ 14.42 |
| 2026-08-27 | `BE` | cash | leftover split 2.95 < 1 share @ 227.10 |
| 2026-08-27 | `MAIR` | cash | leftover split 2.95 < 1 share @ 28.76 |
| 2026-08-27 | `GRRR` | cash | leftover split 2.95 < 1 share @ 15.94 |
| 2026-08-27 | `GSM` | cash | leftover split 2.95 < 1 share @ 4.02 |
| 2026-08-27 | `NABL` | cash | leftover split 2.95 < 1 share @ 3.87 |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ACRS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `TMCI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `LI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `EQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FSM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RCKT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `GWRE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TII` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `YEXT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FNKO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `EQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FSM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SDGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NEOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 15.60 < 1 share @ 263.36 |
| 2026-09-04 | `CRCL` | cash | leftover split 15.60 < 1 share @ 97.98 |
| 2026-09-04 | `MSTR` | cash | leftover split 15.60 < 1 share @ 137.35 |
| 2026-09-04 | `BLSH` | cash | leftover split 15.60 < 1 share @ 34.69 |
| 2026-09-04 | `ZETA` | cash | leftover split 15.60 < 1 share @ 32.65 |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SDGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `NEOV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LAND` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `XRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `FUBO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `VIST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `PAGS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ZSQR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NTAP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `XRX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `IMSR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `FUBO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `VIST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `PAGS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ZSQR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IVVD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IOT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INIO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `IQV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `VAL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `MRCY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QLYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ILMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 37.06 < 1 share @ 170.85 |
| 2026-09-17 | `FTAI` | cash | leftover split 37.06 < 1 share @ 196.50 |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `VAL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `MRCY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QLYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ILMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `AMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `AIB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BYND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BHVN` | cash | leftover split 10.95 < 1 share @ 14.07 |
| 2026-09-18 | `XE` | cash | leftover split 10.95 < 1 share @ 16.28 |
| 2026-09-18 | `AMD` | cash | leftover split 10.95 < 1 share @ 547.37 |
| 2026-09-18 | `SYM` | cash | leftover split 10.95 < 1 share @ 44.70 |
| 2026-09-18 | `TH` | cash | leftover split 10.95 < 1 share @ 20.91 |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `AMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `AIB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BYND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `DDD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RANI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SHLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SNDK` | cash | leftover split 1106.90 < 1 share @ 1826.00 |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SABR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AIB` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BYND` | no_price | no 09:30 open — carry |
| 2026-09-22 | `QTRX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DDD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RANI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `SHLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SGML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GLXY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-22 | `NTSK` | no_price | no 09:30 open |
| 2026-09-23 | `A` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SGML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GLXY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ALOY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ALOY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `MNRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `NTSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BLSH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CRWD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RNG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AVT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `OPRT` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ALOY` | 29 | 2026-09-22 @ $9.40 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+9.5; leftover $276.84 |
| `DXCM` | 1 | 2026-09-23 @ $89.50 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.3; leftover $133.01 |
| `ADMA` | 13 | 2026-09-23 @ $9.81 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+4.0; leftover $133.01 |
| `OMER` | 6 | 2026-09-23 @ $20.65 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $133.01 |
| `INDP` | 33 | 2026-09-23 @ $3.93 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $133.01 |
| `MNRO` | 9 | 2026-09-23 @ $14.14 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+0.3; leftover $133.01 |
| `NTSK` | 7 | 2026-09-23 @ $18.57 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.2; leftover $133.01 |
| `HIMS` | 4 | 2026-09-23 @ $30.40 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.3; leftover $133.01 |
| `BLSH` | 3 | 2026-09-23 @ $40.00 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+6.7; leftover $133.01 |
