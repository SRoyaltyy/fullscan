# Factor mine action — `probable_probable_ok_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-21.62%** ($7,838) · signal-only (no cash/fees) was +1.46%. Starts YES **0/30**. Fills 140 · skips 201 · realized $-2103.38.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at yesterday's 'likely to keep moving' list and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: yesterday's 'likely to keep moving' list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the last finished bar was green (closed up).
- Must-have: prior 5-session return is at most 10% (not already exploded).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

### When it buys

- At 09:30, take names on yesterday's 'likely to keep moving' list that pass the must-haves.
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

- **Universe** `probable` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True,ret_5_max=10.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6,751.08.

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
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $7,994.17 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 478 | $4.18 | $6.17 | — | $5,989.97 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $2000.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 3 | $503.50 | $2.00 | — | $4,477.47 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ⚪; ret5=+7.9; leftover $2000.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 121 | $16.50 | $2.35 | — | $2,478.62 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $2000.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 45 | $44.06 | $2.12 | — | $493.79 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+3.9; leftover $2000.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $493.79 | ▼ close $9,942.67 vs 09:30 $10,000.00 (session -38.70) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $493.79 | ▲ 09:30 equity $10,107.31 vs yday $9,942.67 (+164.64) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 2 | $39.85 | $0.80 | — | $413.29 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $82.30 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 9 | $9.12 | $0.85 | — | $330.36 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $82.30 | — |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 2 | $31.30 | $0.63 | — | $267.13 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-3.8; leftover $82.30 | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 4 | $18.24 | $0.74 | — | $193.43 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $82.30 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 5 | $16.20 | $0.82 | — | $111.60 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $82.30 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.60 | ▲ close $10,143.27 vs 09:30 $10,107.31 (session +39.81) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.60 | ▼ 09:30 equity $9,860.11 vs yday $10,143.27 (-283.16) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.60 | ▼ close $9,738.07 vs 09:30 $9,860.11 (session -122.04) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.60 | ▲ 09:30 equity $9,742.74 vs yday $9,738.07 (+4.67) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 464 | $4.79 | $6.08 | $+210.65 | $2,328.08 | ▲ +210.65 after sell → book $9,736.66; vs 09:30 mark -6.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 478 | $3.87 | $6.26 | $-160.61 | $4,171.68 | ▼ -160.61 after sell → book $9,730.40; vs 09:30 mark -6.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `WDC` | 3 | $494.28 | $2.02 | $-31.68 | $5,652.50 | ▼ -31.68 after sell → book $9,728.38; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ADUR` | 121 | $15.65 | $2.39 | $-107.59 | $7,543.76 | ▼ -107.59 after sell → book $9,725.99; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ALGM` | 45 | $40.00 | $2.15 | $-186.97 | $9,341.61 | ▼ -186.97 after sell → book $9,723.84; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,341.61 | ▼ close $9,713.51 vs 09:30 $9,742.74 (session -10.33) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,341.61 | ▼ 09:30 equity $9,710.08 vs yday $9,713.51 (-3.43) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `CDNL` | 2 | $43.13 | $0.89 | $+4.87 | $9,426.98 | ▲ +4.87 after sell → book $9,709.19; vs 09:30 mark -0.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ABX` | 9 | $9.13 | $0.87 | $-1.63 | $9,508.29 | ▼ -1.63 after sell → book $9,708.33; vs 09:30 mark -0.86 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `VERA` | 2 | $32.30 | $0.67 | $+0.69 | $9,572.20 | ▲ +0.69 after sell → book $9,707.65; vs 09:30 mark -0.68 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `OCC` | 4 | $14.10 | $0.60 | $-17.90 | $9,628.01 | ▼ -17.90 after sell → book $9,707.06; vs 09:30 mark -0.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ALM` | 5 | $15.81 | $0.83 | $-3.60 | $9,706.23 | ▼ -3.60 after sell → book $9,706.23; vs 09:30 mark -0.83 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 217 | $7.45 | $2.80 | — | $8,086.78 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1617.71 | — |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 14 | $113.23 | $2.03 | — | $6,499.53 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1617.71 | — |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 150 | $10.77 | $2.44 | — | $4,881.59 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1617.71 | — |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 171 | $9.46 | $2.50 | — | $3,261.43 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1617.71 | — |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 193 | $8.38 | $2.57 | — | $1,641.52 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1617.71 | — |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 55 | $29.20 | $2.15 | — | $33.36 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1617.71 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.36 | ▼ close $9,667.18 vs 09:30 $9,710.08 (session -24.55) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.36 | ▲ 09:30 equity $10,080.12 vs yday $9,667.18 (+412.94) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 3 | $1.66 | $0.06 | — | $28.32 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $5.56 | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 6 | $0.86 | $0.07 | — | $23.07 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $5.56 | — |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 1 | $3.11 | $0.03 | — | $19.93 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+7.1; leftover $5.56 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.93 | ▼ close $9,911.22 vs 09:30 $10,080.12 (session -168.73) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.93 | ▲ 09:30 equity $10,001.40 vs yday $9,911.22 (+90.18) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.93 | ▼ close $9,959.68 vs 09:30 $10,001.40 (session -41.73) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.93 | ▼ 09:30 equity $9,835.29 vs yday $9,959.68 (-124.39) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `DNA` | 217 | $6.94 | $2.85 | $-116.32 | $1,523.06 | ▼ -116.32 after sell → book $9,832.44; vs 09:30 mark -2.85 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MSTR` | 14 | $119.11 | $2.06 | $+78.23 | $3,188.54 | ▲ +78.23 after sell → book $9,830.38; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EXK` | 150 | $10.44 | $2.48 | $-54.42 | $4,752.07 | ▼ -54.42 after sell → book $9,827.91; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SCZM` | 171 | $9.45 | $2.54 | $-6.76 | $6,365.47 | ▼ -6.76 after sell → book $9,825.36; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NG` | 193 | $9.31 | $2.62 | $+174.31 | $8,159.69 | ▲ +174.31 after sell → book $9,822.75; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BLSH` | 55 | $30.00 | $2.18 | $+39.67 | $9,807.51 | ▲ +39.67 after sell → book $9,820.57; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3913 | $0.36 | $25.75 | — | $8,380.91 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-15.6; leftover $1401.07 | — |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 125 | $11.12 | $2.37 | — | $6,988.54 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.7; leftover $1401.07 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 103 | $13.59 | $2.30 | — | $5,586.47 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1401.07 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 147 | $9.49 | $2.43 | — | $4,189.01 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1401.07 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 37 | $36.96 | $2.10 | — | $2,819.39 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1401.07 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 307 | $4.55 | $3.96 | — | $1,418.58 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1401.07 | — |
| 2026-08-25 09:30 ET | **BUY** | `ADIG` | 64 | $21.79 | $2.18 | — | $21.84 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+3.1; leftover $1401.07 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.84 | ▲ close $9,848.78 vs 09:30 $9,835.29 (session +69.30) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.84 | ▼ 09:30 equity $9,784.70 vs yday $9,848.78 (-64.08) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `BTBT` | 3 | $1.53 | $0.07 | $-0.52 | $26.35 | ▼ -0.52 after sell → book $9,784.63; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ORBS` | 6 | $0.80 | $0.09 | $-0.56 | $31.04 | ▼ -0.56 after sell → book $9,784.54; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 1 | $3.77 | $0.06 | $+0.57 | $34.75 | ▲ +0.57 after sell → book $9,784.48; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.75 | ▲ close $9,887.20 vs 09:30 $9,784.70 (session +102.72) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.75 | ▲ 09:30 equity $9,897.72 vs yday $9,887.20 (+10.52) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `ITG` | 1 | $12.36 | $0.13 | — | $22.27 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=-3.0; leftover $17.38 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.27 | ▼ close $9,739.69 vs 09:30 $9,897.72 (session -157.90) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.27 | ▼ 09:30 equity $9,714.17 vs yday $9,739.69 (-25.52) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `SAFX` | 3913 | $0.36 | $26.68 | $-25.04 | $1,423.83 | ▼ -25.04 after sell → book $9,687.49; vs 09:30 mark -26.68 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `VITL` | 125 | $10.47 | $2.40 | $-86.01 | $2,730.18 | ▼ -86.01 after sell → book $9,685.09; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 103 | $13.05 | $2.33 | $-60.25 | $4,072.01 | ▼ -60.25 after sell → book $9,682.77; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CCOI` | 147 | $9.70 | $2.47 | $+25.97 | $5,495.44 | ▲ +25.97 after sell → book $9,680.30; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 37 | $39.60 | $2.12 | $+93.46 | $6,958.52 | ▲ +93.46 after sell → book $9,678.18; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZIP` | 307 | $4.21 | $4.02 | $-112.36 | $8,246.97 | ▼ -112.36 after sell → book $9,674.16; vs 09:30 mark -4.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ADIG` | 64 | $22.10 | $2.20 | $+15.45 | $9,659.16 | ▲ +15.45 after sell → book $9,671.95; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 1120 | $8.61 | $14.45 | — | $1.51 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.7; leftover $9659.16 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.51 | ▼ close $9,556.21 vs 09:30 $9,714.17 (session -101.29) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.51 | ▲ 09:30 equity $9,556.21 vs yday $9,556.21 (+0.00) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.51 | ▼ close $9,186.48 vs 09:30 $9,556.21 (session -369.73) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.51 | ▼ 09:30 equity $8,906.43 vs yday $9,186.48 (-280.05) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `ITG` | 1 | $12.12 | $0.14 | $-0.51 | $13.49 | ▼ -0.51 after sell → book $8,906.29; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.49 | ▼ close $8,167.09 vs 09:30 $8,906.43 (session -739.20) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.49 | ▼ 09:30 equity $8,133.49 vs yday $8,167.09 (-33.60) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `OPTX` | 1120 | $7.25 | $14.70 | $-1552.35 | $8,118.79 | ▼ -1,552.35 after sell → book $8,118.79; vs 09:30 mark -14.70 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,118.79 | ▲ close $8,118.79 vs 09:30 $8,133.49 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,118.79 | ▲ 09:30 equity $8,118.79 vs yday $8,118.79 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 161 | $16.77 | $2.47 | — | $5,416.35 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $2706.26 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 1241 | $2.18 | $16.01 | — | $2,694.96 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $2706.26 | — |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 192 | $13.96 | $2.57 | — | $12.07 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-6.4; leftover $2706.26 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.07 | ▼ close $7,999.07 vs 09:30 $8,118.79 (session -98.67) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.07 | ▼ 09:30 equity $7,987.92 vs yday $7,999.07 (-11.15) | — | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.07 | ▲ close $8,269.13 vs 09:30 $7,987.92 (session +281.21) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.07 | ▼ 09:30 equity $8,231.98 vs yday $8,269.13 (-37.15) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.07 | ▲ close $8,336.32 vs 09:30 $8,231.98 (session +104.34) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.07 | ▼ 09:30 equity $8,293.59 vs yday $8,336.32 (-42.73) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 161 | $15.46 | $2.52 | $-215.90 | $2,498.61 | ▼ -215.90 after sell → book $8,291.07; vs 09:30 mark -2.52 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 1241 | $2.22 | $16.24 | $+17.39 | $5,237.40 | ▲ +17.39 after sell → book $8,274.84; vs 09:30 mark -16.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CLYM` | 192 | $15.82 | $2.62 | $+351.93 | $8,272.21 | ▲ +351.93 after sell → book $8,272.21; vs 09:30 mark -2.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,272.21 | ▲ close $8,272.21 vs 09:30 $8,293.59 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,272.21 | ▲ 09:30 equity $8,272.21 vs yday $8,272.21 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,272.21 | ▲ close $8,272.21 vs 09:30 $8,272.21 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,272.21 | ▲ 09:30 equity $8,272.21 vs yday $8,272.21 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 506 | $2.04 | $6.53 | — | $7,233.45 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1034.03 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 217 | $4.75 | $2.80 | — | $6,199.90 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1034.03 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 487 | $2.12 | $6.28 | — | $5,161.17 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1034.03 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 43 | $23.63 | $2.12 | — | $4,142.97 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-6.3; leftover $1034.03 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 89 | $11.55 | $2.26 | — | $3,112.76 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1034.03 | — |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 6 | $157.55 | $2.01 | — | $2,165.45 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $1034.03 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 13 | $77.33 | $2.03 | — | $1,158.13 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+2.5; leftover $1034.03 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 19 | $52.55 | $2.05 | — | $157.63 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1034.03 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $157.63 | ▼ close $8,225.71 vs 09:30 $8,272.21 (session -20.43) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.63 | ▲ 09:30 equity $8,288.82 vs yday $8,225.71 (+63.11) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $157.63 | ▼ close $8,262.38 vs 09:30 $8,288.82 (session -26.44) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.63 | ▼ 09:30 equity $8,205.51 vs yday $8,262.38 (-56.87) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $157.63 | ▼ close $8,150.47 vs 09:30 $8,205.51 (session -55.04) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.63 | ▼ 09:30 equity $7,978.02 vs yday $8,150.47 (-172.45) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 506 | $1.89 | $6.62 | $-89.05 | $1,107.35 | ▼ -89.05 after sell → book $7,971.40; vs 09:30 mark -6.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 217 | $4.73 | $2.85 | $-9.98 | $2,130.92 | ▼ -9.98 after sell → book $7,968.56; vs 09:30 mark -2.84 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAK` | 487 | $1.84 | $6.37 | $-149.02 | $3,020.63 | ▼ -149.02 after sell → book $7,962.19; vs 09:30 mark -6.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `TYRA` | 43 | $25.58 | $2.14 | $+79.59 | $4,118.43 | ▲ +79.59 after sell → book $7,960.05; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `FUBO` | 89 | $10.75 | $2.28 | $-75.74 | $5,072.89 | ▼ -75.74 after sell → book $7,957.76; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RDDT` | 6 | $160.62 | $2.03 | $+14.38 | $6,034.59 | ▲ +14.38 after sell → book $7,955.74; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `VIST` | 13 | $76.75 | $2.05 | $-11.62 | $7,030.29 | ▼ -11.62 after sell → book $7,953.69; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAND` | 19 | $48.60 | $2.07 | $-79.16 | $7,951.62 | ▼ -79.16 after sell → book $7,951.62; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 62 | $18.21 | $2.18 | — | $6,820.42 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-19.1; leftover $1135.95 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 16 | $68.79 | $2.04 | — | $5,717.75 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1135.95 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 193 | $5.87 | $2.57 | — | $4,582.27 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1135.95 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 417 | $2.72 | $5.38 | — | $3,442.65 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.4; leftover $1135.95 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 12 | $87.40 | $2.03 | — | $2,391.82 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1135.95 | — |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 29 | $38.01 | $2.08 | — | $1,287.46 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $1135.95 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 41 | $27.09 | $2.11 | — | $174.65 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1135.95 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.65 | ▲ close $7,951.45 vs 09:30 $7,978.02 (session +18.21) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.65 | ▲ 09:30 equity $8,109.99 vs yday $7,951.45 (+158.54) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 128 | $0.17 | $0.60 | — | $152.29 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $21.83 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 1 | $15.87 | $0.16 | — | $136.26 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $21.83 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 9 | $2.40 | $0.24 | — | $114.42 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $21.83 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 1 | $18.04 | $0.18 | — | $96.20 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $21.83 | — |
| 2026-09-17 09:30 ET | **BUY** | `EROC` | 1 | $12.64 | $0.13 | — | $83.43 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-3.6; leftover $21.83 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.43 | ▲ close $8,262.13 vs 09:30 $8,109.99 (session +153.46) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.43 | ▲ 09:30 equity $8,336.87 vs yday $8,262.13 (+74.74) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 12 | $0.97 | $0.15 | — | $71.64 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $11.92 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 3 | $3.95 | $0.13 | — | $59.66 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $11.92 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 2 | $5.83 | $0.12 | — | $47.88 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $11.92 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 3 | $3.58 | $0.12 | — | $37.02 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $11.92 | — |
| 2026-09-18 09:30 ET | **BUY** | `RANI` | 14 | $0.85 | $0.16 | — | $24.96 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+3.6; leftover $11.92 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.96 | ▼ close $8,247.68 vs 09:30 $8,336.87 (session -88.51) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.96 | ▲ 09:30 equity $8,334.20 vs yday $8,247.68 (+86.52) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `ARQQ` | 62 | $20.55 | $2.20 | $+140.71 | $1,296.86 | ▲ +140.71 after sell → book $8,332.00; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 16 | $79.08 | $2.06 | $+160.54 | $2,560.08 | ▲ +160.54 after sell → book $8,329.94; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 193 | $5.62 | $2.61 | $-53.43 | $3,642.13 | ▼ -53.43 after sell → book $8,327.33; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QTRX` | 417 | $3.13 | $5.46 | $+160.13 | $4,941.88 | ▲ +160.13 after sell → book $8,321.87; vs 09:30 mark -5.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `VAL` | 12 | $83.46 | $2.05 | $-51.35 | $5,941.36 | ▼ -51.35 after sell → book $8,319.83; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `KRMN` | 29 | $36.30 | $2.10 | $-53.76 | $6,991.96 | ▼ -53.76 after sell → book $8,317.73; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ADPT` | 41 | $28.69 | $2.13 | $+61.35 | $8,166.12 | ▲ +61.35 after sell → book $8,315.60; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 109 | $9.31 | $2.32 | — | $7,149.01 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1020.76 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 75 | $13.47 | $2.21 | — | $6,136.17 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1020.76 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 919 | $1.11 | $11.86 | — | $5,104.23 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1020.76 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 102 | $9.99 | $2.30 | — | $4,082.95 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1020.76 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 559 | $1.82 | $7.21 | — | $3,055.56 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1020.76 | — |
| 2026-09-21 09:30 ET | **BUY** | `SGML` | 100 | $10.13 | $2.29 | — | $2,039.77 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+4.9; leftover $1020.76 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 39 | $25.95 | $2.11 | — | $1,025.62 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1020.76 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,025.62 | ▼ close $8,199.78 vs 09:30 $8,334.20 (session -85.52) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,025.62 | ▼ 09:30 equity $8,177.74 vs yday $8,199.78 (-22.04) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `DVLT` | 128 | $0.16 | $0.62 | $-2.50 | $1,045.48 | ▼ -2.50 after sell → book $8,177.12; vs 09:30 mark -0.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `CIFR` | 1 | $18.51 | $0.21 | $+0.08 | $1,063.78 | ▲ +0.08 after sell → book $8,176.91; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `ALOY` | 22 | $9.40 | $2.06 | — | $854.92 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+9.5; leftover $212.76 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $854.92 | ▲ close $8,346.05 vs 09:30 $8,177.74 (session +171.20) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $854.92 | ▼ 09:30 equity $8,342.08 vs yday $8,346.05 (-3.97) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BRUN` | 1 | $17.10 | $0.19 | $+0.87 | $871.83 | ▲ +0.87 after sell → book $8,341.88; vs 09:30 mark -0.20 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SABR` | 9 | $2.24 | $0.25 | $-1.93 | $891.74 | ▼ -1.93 after sell → book $8,341.63; vs 09:30 mark -0.25 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `EROC` | 1 | $12.82 | $0.15 | $-0.10 | $904.41 | ▼ -0.10 after sell → book $8,341.48; vs 09:30 mark -0.15 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TLSA` | 12 | $0.89 | $0.16 | $-1.28 | $914.93 | ▼ -1.28 after sell → book $8,341.32; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `EYPT` | 3 | $4.10 | $0.15 | $+0.17 | $927.07 | ▲ +0.17 after sell → book $8,341.17; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BNC` | 2 | $6.29 | $0.15 | $+0.65 | $939.50 | ▲ +0.65 after sell → book $8,341.02; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `DDD` | 3 | $3.59 | $0.14 | $-0.22 | $950.14 | ▼ -0.22 after sell → book $8,340.88; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RANI` | 14 | $0.81 | $0.18 | $-0.90 | $961.30 | ▼ -0.90 after sell → book $8,340.70; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 7 | $20.65 | $1.47 | — | $815.28 | — | combo gate; gate last_green=True,ret_5_max=10.0; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $160.22 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 40 | $3.93 | $1.69 | — | $656.39 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $160.22 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 10 | $15.72 | $1.60 | — | $497.59 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $160.22 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 6 | $25.40 | $1.54 | — | $343.65 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $160.22 | — |
| 2026-09-23 09:30 ET | **BUY** | `CLPT` | 10 | $15.55 | $1.58 | — | $186.56 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $160.22 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 208 | $0.77 | $2.22 | — | $24.60 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $160.22 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.60 | ▼ close $7,972.90 vs 09:30 $8,342.08 (session -357.69) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.60 | ▼ 09:30 equity $7,851.66 vs yday $7,972.90 (-121.24) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 109 | $8.67 | $2.35 | $-74.42 | $967.28 | ▼ -74.42 after sell → book $7,849.32; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 75 | $12.26 | $2.24 | $-95.58 | $1,884.54 | ▼ -95.58 after sell → book $7,847.08; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ORBS` | 919 | $1.05 | $12.02 | $-79.01 | $2,837.48 | ▼ -79.01 after sell → book $7,835.06; vs 09:30 mark -12.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SBET` | 102 | $9.80 | $2.32 | $-24.00 | $3,834.75 | ▼ -24.00 after sell → book $7,832.74; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTBT` | 559 | $1.73 | $7.31 | $-70.42 | $4,791.71 | ▼ -70.42 after sell → book $7,825.43; vs 09:30 mark -7.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGML` | 100 | $9.89 | $2.32 | $-29.11 | $5,778.40 | ▼ -29.11 after sell → book $7,823.11; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GLXY` | 39 | $25.00 | $2.13 | $-41.48 | $6,751.08 | ▼ -41.48 after sell → book $7,820.98; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,751.08 | ▲ close $7,821.55 vs 09:30 $7,851.66 (session +0.57) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,771.30 | ▲ 09:30 equity $7,919.87 vs yday $7,919.87 (+0.00) | 09:30 open · cash $6,771.30 (unchanged overnight, no fees) · equity $7,919.87 vs prior close $7,919.87 (+0.00) · 9 name(s) re-marked at the open (per-name table). APPS×19 yday $10.88 → 09:30 $10.88 +0.00; ARHS×27 yday $9.47 → 09:30 $9.47 +0.00; BTQ×85 yday $2.79 → 09:30 $2.79 +0.00; GT×1 yday $5.07 → 09:30 $5.07 +0.00; HELP×17 yday $12.59 → 09:30 $12.59 +0.00; INDP×1 yday $4.00 → 09:30 $4.00 +0.00; NMRA×7 yday $0.70 → 09:30 $0.70 +0.00; NN×15 yday $14.45 → 09:30 $14.45 +0.00; TLYS×1 yday $4.24 → 09:30 $4.24 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 36 | $26.27 | $2.10 | — | $5,823.48 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $967.33 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 11 | $83.76 | $2.02 | — | $4,900.10 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $967.33 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BRVE` | 41 | $23.58 | $2.11 | — | $3,931.21 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $967.33 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 439 | $2.20 | $5.66 | — | $2,959.74 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $967.33 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 161 | $6.00 | $2.47 | — | $1,991.27 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $967.33 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 54 | $17.91 | $2.15 | — | $1,021.98 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+3.7; leftover $967.33 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 11 | $83.69 | $2.02 | — | $99.31 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $967.33 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.31 | ▼ close $7,837.56 vs 09:30 $7,919.87 (session -63.76) | 16:00 close · cash $99.31 · equity $7,837.56 vs 09:30 $7,919.87 (-82.31; session marks -63.76) · 16 name(s) marked open→close (per-name table). APPS×19 09:30 $10.88 → close $10.88 +0.00; ARHS×27 09:30 $9.47 → close $9.47 +0.00; BTQ×85 09:30 $2.79 → close $2.79 -0.00; GT×1 09:30 $5.07 → close $5.07 +0.00; HELP×17 09:30 $12.59 → close $12.59 +0.00; INDP×1 09:30 $4.00 → close $4.00 +0.00; NMRA×7 09:30 $0.70 → close $0.70 +0.00; NN×15 09:30 $14.45 → close $14.45 -0.00; TLYS×1 09:30 $4.24 → close $4.24 -0.00; WRBY×36 09:30 $26.27 → close $26.71 +15.84; TXG×11 09:30 $83.76 → close $85.71 +21.45; BRVE×41 09:30 $23.58 → close $20.62 -121.36; HLP×439 09:30 $2.20 → close $2.21 +4.39; SATL×161 09:30 $6.00 → close $6.17 +27.37; PL×54 09:30 $17.91 → close $17.43 -25.92; TEM×11 09:30 $83.69 → close $85.01 +14.47 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `WDC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ALGM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CELC` | cash | leftover split 82.30 < 1 share @ 92.99 |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `WDC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ALGM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CDNL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ABX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `VERA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OCC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `CDNL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `VERA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OCC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ALM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MSTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BLSH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DE` | cash | leftover split 5.56 < 1 share @ 623.26 |
| 2026-08-21 | `QDEL` | cash | leftover split 5.56 < 1 share @ 14.96 |
| 2026-08-21 | `CF` | cash | leftover split 5.56 < 1 share @ 127.43 |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BLSH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `VITL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CCOI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZIP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ADIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AVBP` | cash | leftover split 8.69 < 1 share @ 31.21 |
| 2026-08-26 | `ABX` | cash | leftover split 8.69 < 1 share @ 9.83 |
| 2026-08-26 | `ITG` | cash | leftover split 8.69 < 1 share @ 12.04 |
| 2026-08-26 | `SENS` | cash | leftover split 8.69 < 1 share @ 9.48 |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `VITL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CCOI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZIP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ADIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BE` | cash | leftover split 17.38 < 1 share @ 227.10 |
| 2026-08-28 | `ITG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ITG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `OPTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `OPTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `IRD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 12.07 < 1 share @ 513.78 |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CLYM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `TYRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `FUBO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RDDT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `VIST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CAN` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `USDE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `TYRA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `FUBO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RDDT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `VIST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IVVD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `ARQQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `VAL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `KRMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ADPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AXTI` | cash | leftover split 21.83 < 1 share @ 67.91 |
| 2026-09-17 | `ARQT` | cash | leftover split 21.83 < 1 share @ 25.95 |
| 2026-09-17 | `SMTC` | cash | leftover split 21.83 < 1 share @ 170.85 |
| 2026-09-18 | `ARQQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `VAL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `KRMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `CIFR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `EROC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BHVN` | cash | leftover split 11.92 < 1 share @ 14.07 |
| 2026-09-18 | `RARE` | cash | leftover split 11.92 < 1 share @ 14.79 |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `CIFR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `EROC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `DDD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RANI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SNDK` | cash | leftover split 1020.76 < 1 share @ 1826.00 |
| 2026-09-22 | `BRUN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SABR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `EROC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `DDD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RANI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SGML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GLXY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `MRAM` | no_price | no 09:30 open |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SGML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GLXY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ALOY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ALOY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `TNGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CLPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `NMRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TDTH` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ALOY` | 22 | 2026-09-22 @ $9.40 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+9.5; leftover $212.76 |
| `OMER` | 7 | 2026-09-23 @ $20.65 | combo gate; gate last_green=True,ret_5_max=10.0; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $160.22 |
| `INDP` | 40 | 2026-09-23 @ $3.93 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $160.22 |
| `SGRY` | 10 | 2026-09-23 @ $15.72 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $160.22 |
| `TNGX` | 6 | 2026-09-23 @ $25.40 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $160.22 |
| `CLPT` | 10 | 2026-09-23 @ $15.55 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $160.22 |
| `NMRA` | 208 | 2026-09-23 @ $0.77 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $160.22 |
