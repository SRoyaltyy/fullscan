# Factor mine action — `probable_probable_ok_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-4.57%** ($9,543) · signal-only (no cash/fees) was -9.22%. Starts YES **0/30**. Fills 175 · skips 53 · realized $-864.75.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at yesterday's 'likely to keep moving' list and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `probable` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True,ret_5_max=10.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,135.24.

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
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 464 | $4.60 | $6.08 | $+122.49 | $2,622.11 | ▲ +122.49 after sell → book $10,101.23; vs 09:30 mark -6.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 478 | $4.10 | $6.26 | $-50.67 | $4,575.65 | ▼ -50.67 after sell → book $10,094.97; vs 09:30 mark -6.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 3 | $525.53 | $2.02 | $+62.07 | $6,150.22 | ▲ +62.07 after sell → book $10,092.95; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 121 | $15.73 | $2.39 | $-97.91 | $8,051.16 | ▼ -97.91 after sell → book $10,090.56; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ALGM` | 45 | $45.32 | $2.15 | $+52.42 | $10,088.41 | ▲ +52.42 after sell → book $10,088.41; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 42 | $39.85 | $2.12 | — | $8,412.59 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1681.40 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 184 | $9.12 | $2.54 | — | $6,731.97 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1681.40 | — |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 53 | $31.30 | $2.15 | — | $5,070.92 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-3.8; leftover $1681.40 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 18 | $92.99 | $2.04 | — | $3,395.06 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.8; leftover $1681.40 | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 92 | $18.24 | $2.27 | — | $1,714.71 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1681.40 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 103 | $16.20 | $2.30 | — | $43.81 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1681.40 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.81 | ▼ close $9,969.98 vs 09:30 $10,107.31 (session -105.01) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.81 | ▼ 09:30 equity $9,889.28 vs yday $9,969.98 (-80.70) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 42 | $41.57 | $2.14 | $+67.98 | $1,787.61 | ▲ +67.98 after sell → book $9,887.14; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 184 | $9.03 | $2.59 | $-21.69 | $3,446.55 | ▼ -21.69 after sell → book $9,884.56; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 53 | $31.31 | $2.17 | $-3.79 | $5,103.81 | ▼ -3.79 after sell → book $9,882.39; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 18 | $92.38 | $2.07 | $-15.09 | $6,764.58 | ▼ -15.09 after sell → book $9,880.32; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 92 | $16.20 | $2.29 | $-192.24 | $8,252.68 | ▼ -192.24 after sell → book $9,878.02; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 103 | $15.78 | $2.33 | $-47.89 | $9,875.70 | ▼ -47.89 after sell → book $9,875.70; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,875.70 | ▲ close $9,875.70 vs 09:30 $9,889.28 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,875.70 | ▲ 09:30 equity $9,875.70 vs yday $9,875.70 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,875.70 | ▲ close $9,875.70 vs 09:30 $9,875.70 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,875.70 | ▲ 09:30 equity $9,875.70 vs yday $9,875.70 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 220 | $7.45 | $2.84 | — | $8,233.86 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1645.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 14 | $113.23 | $2.03 | — | $6,646.61 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1645.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 152 | $10.77 | $2.45 | — | $5,007.12 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1645.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 173 | $9.46 | $2.51 | — | $3,368.03 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1645.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 196 | $8.38 | $2.58 | — | $1,722.97 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1645.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 56 | $29.20 | $2.16 | — | $85.61 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1645.95 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.61 | ▼ close $9,836.19 vs 09:30 $9,875.70 (session -24.94) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.61 | ▲ 09:30 equity $10,253.65 vs yday $9,836.19 (+417.46) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 220 | $7.09 | $2.89 | $-84.92 | $1,642.53 | ▼ -84.92 after sell → book $10,250.77; vs 09:30 mark -2.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MSTR` | 14 | $119.69 | $2.06 | $+86.35 | $3,316.13 | ▲ +86.35 after sell → book $10,248.71; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `EXK` | 152 | $11.34 | $2.48 | $+81.71 | $5,037.33 | ▲ +81.71 after sell → book $10,246.23; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 173 | $10.26 | $2.55 | $+133.34 | $6,809.76 | ▲ +133.34 after sell → book $10,243.68; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NG` | 196 | $9.02 | $2.62 | $+120.24 | $8,575.05 | ▲ +120.24 after sell → book $10,241.05; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BLSH` | 56 | $29.75 | $2.18 | $+26.46 | $10,238.87 | ▲ +26.46 after sell → book $10,238.87; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 1027 | $1.66 | $13.25 | — | $8,520.80 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1706.48 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $7,272.28 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1706.48 | — |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 114 | $14.96 | $2.33 | — | $5,564.51 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-1.6; leftover $1706.48 | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1975 | $0.86 | $22.99 | — | $3,835.12 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1706.48 | — |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 548 | $3.11 | $7.07 | — | $2,123.77 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+7.1; leftover $1706.48 | — |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 13 | $127.43 | $2.03 | — | $465.16 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ⚪; ret5=+7.9; leftover $1706.48 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $465.16 | ▼ close $10,182.69 vs 09:30 $10,253.65 (session -6.52) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $465.16 | ▲ 09:30 equity $10,244.67 vs yday $10,182.69 (+61.98) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 1027 | $1.55 | $13.43 | $-139.65 | $2,043.57 | ▼ -139.65 after sell → book $10,231.23; vs 09:30 mark -13.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $3,347.64 | ▲ +55.55 after sell → book $10,229.22; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 114 | $14.74 | $2.36 | $-29.78 | $5,025.63 | ▼ -29.78 after sell → book $10,226.85; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1975 | $0.89 | $23.84 | $+4.52 | $6,759.54 | ▲ +4.52 after sell → book $10,203.01; vs 09:30 mark -23.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GORO` | 548 | $3.20 | $7.17 | $+35.08 | $8,505.96 | ▲ +35.08 after sell → book $10,195.83; vs 09:30 mark -7.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 13 | $129.99 | $2.05 | $+29.20 | $10,193.78 | ▲ +29.20 after sell → book $10,193.78; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,193.78 | ▲ close $10,193.78 vs 09:30 $10,244.67 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,193.78 | ▲ 09:30 equity $10,193.78 vs yday $10,193.78 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 4067 | $0.36 | $26.76 | — | $8,711.04 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-15.6; leftover $1456.25 | — |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 130 | $11.12 | $2.38 | — | $7,263.06 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.7; leftover $1456.25 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 107 | $13.59 | $2.31 | — | $5,806.61 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1456.25 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 153 | $9.49 | $2.45 | — | $4,352.20 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1456.25 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 39 | $36.96 | $2.11 | — | $2,908.65 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1456.25 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 320 | $4.55 | $4.13 | — | $1,448.52 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1456.25 | — |
| 2026-08-25 09:30 ET | **BUY** | `ADIG` | 66 | $21.79 | $2.19 | — | $8.19 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+3.1; leftover $1456.25 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.19 | ▲ close $10,223.64 vs 09:30 $10,193.78 (session +72.18) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.19 | ▼ 09:30 equity $10,157.36 vs yday $10,223.64 (-66.28) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 4067 | $0.35 | $27.24 | $-74.34 | $1,416.60 | ▼ -74.34 after sell → book $10,130.12; vs 09:30 mark -27.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `VITL` | 130 | $11.03 | $2.41 | $-16.49 | $2,848.09 | ▼ -16.49 after sell → book $10,127.71; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 107 | $13.63 | $2.34 | $-0.37 | $4,304.16 | ▼ -0.37 after sell → book $10,125.37; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 153 | $9.89 | $2.49 | $+56.26 | $5,814.84 | ▲ +56.26 after sell → book $10,122.88; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 39 | $38.24 | $2.13 | $+45.68 | $7,304.07 | ▲ +45.68 after sell → book $10,120.75; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 320 | $4.31 | $4.19 | $-85.12 | $8,679.08 | ▼ -85.12 after sell → book $10,116.56; vs 09:30 mark -4.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ADIG` | 66 | $21.78 | $2.21 | $-5.06 | $10,114.35 | ▼ -5.06 after sell → book $10,114.35; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 81 | $31.21 | $2.23 | — | $7,584.10 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $2528.59 | — |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 257 | $9.83 | $3.32 | — | $5,054.48 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $2528.59 | — |
| 2026-08-26 09:30 ET | **BUY** | `ITG` | 210 | $12.04 | $2.71 | — | $2,523.37 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=-5.1; leftover $2528.59 | — |
| 2026-08-26 09:30 ET | **BUY** | `SENS` | 265 | $9.48 | $3.42 | — | $7.75 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $2528.59 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.75 | ▲ close $10,133.15 vs 09:30 $10,157.36 (session +30.48) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.75 | ▼ 09:30 equity $10,057.55 vs yday $10,133.15 (-75.60) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 81 | $30.79 | $2.27 | $-38.52 | $2,499.48 | ▼ -38.52 after sell → book $10,055.29; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ABX` | 257 | $9.68 | $3.38 | $-45.24 | $4,983.86 | ▼ -45.24 after sell → book $10,051.91; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SENS` | 265 | $9.33 | $3.48 | $-46.65 | $7,452.83 | ▼ -46.65 after sell → book $10,048.43; vs 09:30 mark -3.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BE` | 32 | $227.10 | $2.09 | — | $183.54 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.6; leftover $7452.83 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $183.54 | ▼ close $9,856.80 vs 09:30 $10,057.55 (session -189.54) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $183.54 | ▼ 09:30 equity $9,772.00 vs yday $9,856.80 (-84.80) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `ITG` | 210 | $12.79 | $2.77 | $+152.03 | $2,866.67 | ▲ +152.03 after sell → book $9,769.23; vs 09:30 mark -2.77 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BE` | 32 | $215.71 | $2.15 | $-368.88 | $9,767.08 | ▼ -368.88 after sell → book $9,767.08; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 1132 | $8.61 | $14.60 | — | $5.96 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.7; leftover $9767.08 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.96 | ▼ close $9,650.60 vs 09:30 $9,772.00 (session -101.88) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.96 | ▲ 09:30 equity $9,650.60 vs yday $9,650.60 (-0.00) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 1132 | $8.52 | $14.87 | $-131.35 | $9,635.73 | ▼ -131.35 after sell → book $9,635.73; vs 09:30 mark -14.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,635.73 | ▲ close $9,635.73 vs 09:30 $9,650.60 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,635.73 | ▲ 09:30 equity $9,635.73 vs yday $9,635.73 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,635.73 | ▲ close $9,635.73 vs 09:30 $9,635.73 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,635.73 | ▲ 09:30 equity $9,635.73 vs yday $9,635.73 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,635.73 | ▲ close $9,635.73 vs 09:30 $9,635.73 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,635.73 | ▲ 09:30 equity $9,635.73 vs yday $9,635.73 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 191 | $16.77 | $2.56 | — | $6,430.10 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $3211.91 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 1473 | $2.18 | $19.00 | — | $3,199.96 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $3211.91 | — |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 229 | $13.96 | $2.95 | — | $0.16 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-6.4; leftover $3211.91 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.16 | ▼ close $9,494.91 vs 09:30 $9,635.73 (session -116.30) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.16 | ▼ 09:30 equity $9,481.56 vs yday $9,494.91 (-13.35) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 191 | $15.61 | $2.62 | $-226.74 | $2,979.05 | ▼ -226.74 after sell → book $9,478.94; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 1473 | $2.16 | $19.27 | $-67.73 | $6,141.46 | ▼ -67.73 after sell → book $9,459.67; vs 09:30 mark -19.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CLYM` | 229 | $14.49 | $3.02 | $+115.40 | $9,456.65 | ▲ +115.40 after sell → book $9,456.65; vs 09:30 mark -3.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 18 | $513.78 | $2.04 | — | $206.57 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $9456.65 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $206.57 | ▲ close $9,641.09 vs 09:30 $9,481.56 (session +186.48) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $206.57 | ▼ 09:30 equity $9,587.27 vs yday $9,641.09 (-53.82) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 18 | $521.15 | $2.13 | $+128.49 | $9,585.14 | ▲ +128.49 after sell → book $9,585.14; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,585.14 | ▲ close $9,585.14 vs 09:30 $9,587.27 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,585.14 | ▲ 09:30 equity $9,585.14 vs yday $9,585.14 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,585.14 | ▲ close $9,585.14 vs 09:30 $9,585.14 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,585.14 | ▲ 09:30 equity $9,585.14 vs yday $9,585.14 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,585.14 | ▲ close $9,585.14 vs 09:30 $9,585.14 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,585.14 | ▲ 09:30 equity $9,585.14 vs yday $9,585.14 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 587 | $2.04 | $7.57 | — | $8,380.09 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1198.14 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 252 | $4.75 | $3.25 | — | $7,179.84 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1198.14 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 565 | $2.12 | $7.29 | — | $5,974.75 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1198.14 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 50 | $23.63 | $2.14 | — | $4,791.11 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-6.3; leftover $1198.14 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 103 | $11.55 | $2.30 | — | $3,599.16 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1198.14 | — |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 7 | $157.55 | $2.01 | — | $2,494.30 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $1198.14 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 15 | $77.33 | $2.04 | — | $1,332.31 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+2.5; leftover $1198.14 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 22 | $52.55 | $2.06 | — | $174.16 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1198.14 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.16 | ▼ close $9,532.54 vs 09:30 $9,585.14 (session -23.95) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.16 | ▲ 09:30 equity $9,605.90 vs yday $9,532.54 (+73.36) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 587 | $2.01 | $7.68 | $-32.86 | $1,346.35 | ▼ -32.86 after sell → book $9,598.22; vs 09:30 mark -7.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 252 | $4.82 | $3.30 | $+11.09 | $2,557.69 | ▲ +11.09 after sell → book $9,594.92; vs 09:30 mark -3.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 565 | $2.05 | $7.39 | $-54.23 | $3,708.54 | ▼ -54.23 after sell → book $9,587.52; vs 09:30 mark -7.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 50 | $23.20 | $2.16 | $-25.80 | $4,866.38 | ▼ -25.80 after sell → book $9,585.36; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `FUBO` | 103 | $11.56 | $2.33 | $-3.60 | $6,054.74 | ▼ -3.60 after sell → book $9,583.04; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RDDT` | 7 | $160.00 | $2.03 | $+13.11 | $7,172.71 | ▲ +13.11 after sell → book $9,581.01; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIST` | 15 | $77.10 | $2.06 | $-7.54 | $8,327.15 | ▼ -7.54 after sell → book $9,578.95; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 22 | $56.90 | $2.08 | $+91.57 | $9,576.88 | ▲ +91.57 after sell → book $9,576.88; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,576.88 | ▲ close $9,576.88 vs 09:30 $9,605.90 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,576.88 | ▲ 09:30 equity $9,576.88 vs yday $9,576.88 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,576.88 | ▲ close $9,576.88 vs 09:30 $9,576.88 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,576.88 | ▲ 09:30 equity $9,576.88 vs yday $9,576.88 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 75 | $18.21 | $2.21 | — | $8,208.91 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-19.1; leftover $1368.13 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 19 | $68.79 | $2.05 | — | $6,899.85 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1368.13 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 233 | $5.87 | $3.01 | — | $5,529.14 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1368.13 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 502 | $2.72 | $6.48 | — | $4,157.22 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.4; leftover $1368.13 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 15 | $87.40 | $2.04 | — | $2,844.19 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1368.13 | — |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 35 | $38.01 | $2.10 | — | $1,511.74 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $1368.13 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 50 | $27.09 | $2.14 | — | $155.10 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1368.13 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $155.10 | ▲ close $9,576.12 vs 09:30 $9,576.88 (session +19.26) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $155.10 | ▲ 09:30 equity $9,767.32 vs yday $9,576.12 (+191.20) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `ARQQ` | 75 | $19.59 | $2.24 | $+99.05 | $1,622.11 | ▲ +99.05 after sell → book $9,765.08; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 19 | $72.70 | $2.07 | $+70.17 | $3,001.35 | ▲ +70.17 after sell → book $9,763.02; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 233 | $5.58 | $3.05 | $-73.63 | $4,298.43 | ▼ -73.63 after sell → book $9,759.96; vs 09:30 mark -3.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QTRX` | 502 | $2.94 | $6.57 | $+97.39 | $5,767.74 | ▲ +97.39 after sell → book $9,753.39; vs 09:30 mark -6.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 15 | $83.20 | $2.06 | $-67.09 | $7,013.68 | ▼ -67.09 after sell → book $9,751.33; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `KRMN` | 35 | $37.89 | $2.12 | $-8.41 | $8,337.72 | ▼ -8.41 after sell → book $9,749.22; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 50 | $28.23 | $2.16 | $+52.70 | $9,747.06 | ▲ +52.70 after sell → book $9,747.06; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 7166 | $0.17 | $33.68 | — | $8,495.16 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $1218.38 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 76 | $15.87 | $2.22 | — | $7,286.82 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $1218.38 | — |
| 2026-09-17 09:30 ET | **BUY** | `AXTI` | 17 | $67.91 | $2.04 | — | $6,130.31 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.7; leftover $1218.38 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 46 | $25.95 | $2.13 | — | $4,934.48 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1218.38 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 7 | $170.85 | $2.01 | — | $3,736.52 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1218.38 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 507 | $2.40 | $6.54 | — | $2,513.18 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1218.38 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 67 | $18.04 | $2.19 | — | $1,302.64 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $1218.38 | — |
| 2026-09-17 09:30 ET | **BUY** | `EROC` | 96 | $12.64 | $2.28 | — | $86.93 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-3.6; leftover $1218.38 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.93 | ▼ close $9,667.03 vs 09:30 $9,767.32 (session -26.94) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.93 | ▲ 09:30 equity $9,896.21 vs yday $9,667.03 (+229.18) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `DVLT` | 7166 | $0.17 | $34.88 | $-68.56 | $1,270.27 | ▼ -68.56 after sell → book $9,861.33; vs 09:30 mark -34.88 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRUN` | 76 | $17.44 | $2.24 | $+114.86 | $2,593.46 | ▲ +114.86 after sell → book $9,859.08; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AXTI` | 17 | $69.72 | $2.06 | $+26.67 | $3,776.64 | ▲ +26.67 after sell → book $9,857.02; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 46 | $26.14 | $2.15 | $+4.46 | $4,976.94 | ▲ +4.46 after sell → book $9,854.88; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 7 | $182.33 | $2.03 | $+76.32 | $6,251.21 | ▲ +76.32 after sell → book $9,852.84; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 507 | $2.29 | $6.63 | $-68.94 | $7,405.61 | ▼ -68.94 after sell → book $9,846.21; vs 09:30 mark -6.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 67 | $17.80 | $2.21 | $-20.15 | $8,596.00 | ▼ -20.15 after sell → book $9,844.00; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `EROC` | 96 | $13.00 | $2.30 | $+29.98 | $9,841.69 | ▲ +29.98 after sell → book $9,841.69; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 1449 | $0.97 | $18.40 | — | $8,417.76 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1405.96 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 355 | $3.95 | $4.58 | — | $7,010.93 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1405.96 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 99 | $14.07 | $2.29 | — | $5,615.71 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1405.96 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 241 | $5.83 | $3.11 | — | $4,207.58 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1405.96 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 392 | $3.58 | $5.06 | — | $2,799.16 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1405.96 | — |
| 2026-09-18 09:30 ET | **BUY** | `RANI` | 1654 | $0.85 | $19.02 | — | $1,374.24 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+3.6; leftover $1405.96 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 92 | $14.79 | $2.27 | — | $11.29 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1405.96 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.29 | ▼ close $9,669.82 vs 09:30 $9,896.21 (session -117.15) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.29 | ▲ 09:30 equity $9,894.05 vs yday $9,669.82 (+224.23) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `TLSA` | 1449 | $0.94 | $18.22 | $-80.09 | $1,355.13 | ▼ -80.09 after sell → book $9,875.83; vs 09:30 mark -18.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 355 | $3.87 | $4.65 | $-37.63 | $2,724.33 | ▼ -37.63 after sell → book $9,871.18; vs 09:30 mark -4.65 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 99 | $13.90 | $2.31 | $-21.43 | $4,098.12 | ▼ -21.43 after sell → book $9,868.87; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BNC` | 241 | $6.42 | $3.16 | $+134.71 | $5,640.97 | ▲ +134.71 after sell → book $9,865.71; vs 09:30 mark -3.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DDD` | 392 | $3.71 | $5.13 | $+40.77 | $7,090.16 | ▲ +40.77 after sell → book $9,860.58; vs 09:30 mark -5.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RANI` | 1654 | $0.86 | $19.54 | $-15.40 | $8,499.68 | ▼ -15.40 after sell → book $9,841.04; vs 09:30 mark -19.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 92 | $14.58 | $2.29 | $-23.88 | $9,838.74 | ▼ -23.88 after sell → book $9,838.74; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 132 | $9.31 | $2.39 | — | $8,607.44 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1229.84 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 91 | $13.47 | $2.26 | — | $7,378.95 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1229.84 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1107 | $1.11 | $14.28 | — | $6,135.90 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1229.84 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 123 | $9.99 | $2.36 | — | $4,904.77 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1229.84 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 673 | $1.82 | $8.68 | — | $3,667.86 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1229.84 | — |
| 2026-09-21 09:30 ET | **BUY** | `SGML` | 121 | $10.13 | $2.35 | — | $2,439.18 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+4.9; leftover $1229.84 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 47 | $25.95 | $2.13 | — | $1,217.40 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1229.84 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,217.40 | ▼ close $9,702.33 vs 09:30 $9,894.05 (session -101.96) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,217.40 | ▼ 09:30 equity $9,676.89 vs yday $9,702.33 (-25.44) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `ORBS` | 1107 | $1.05 | $14.47 | $-95.17 | $2,365.27 | ▼ -95.17 after sell → book $9,662.42; vs 09:30 mark -14.47 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 123 | $9.91 | $2.39 | $-14.59 | $3,581.81 | ▼ -14.59 after sell → book $9,660.03; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `BTBT` | 673 | $1.79 | $8.80 | $-37.68 | $4,781.04 | ▼ -37.68 after sell → book $9,651.22; vs 09:30 mark -8.81 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `ALOY` | 101 | $9.40 | $2.29 | — | $3,829.35 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+9.5; leftover $956.21 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,829.35 | ▼ close $9,611.56 vs 09:30 $9,676.89 (session -37.37) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,829.35 | ▲ 09:30 equity $9,641.41 vs yday $9,611.56 (+29.85) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 132 | $9.50 | $2.42 | $+20.28 | $5,080.93 | ▲ +20.28 after sell → book $9,638.99; vs 09:30 mark -2.42 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 91 | $12.84 | $2.29 | $-62.34 | $6,247.08 | ▼ -62.34 after sell → book $9,636.70; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SGML` | 121 | $10.26 | $2.38 | $+10.39 | $7,486.16 | ▲ +10.39 after sell → book $9,634.32; vs 09:30 mark -2.38 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 47 | $26.58 | $2.15 | $+25.33 | $8,733.27 | ▲ +25.33 after sell → book $9,632.17; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ALOY` | 101 | $8.90 | $2.32 | $-55.11 | $9,629.85 | ▼ -55.11 after sell → book $9,629.85; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 77 | $20.65 | $2.22 | — | $8,037.58 | — | combo gate; gate last_green=True,ret_5_max=10.0; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1604.98 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 408 | $3.93 | $5.26 | — | $6,428.88 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1604.98 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 102 | $15.72 | $2.30 | — | $4,823.14 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1604.98 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 63 | $25.40 | $2.18 | — | $3,220.76 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $1604.98 | — |
| 2026-09-23 09:30 ET | **BUY** | `CLPT` | 103 | $15.55 | $2.30 | — | $1,616.81 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $1604.98 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 2076 | $0.77 | $22.17 | — | $0.27 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $1604.98 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.27 | ▼ close $9,220.35 vs 09:30 $9,641.41 (session -373.07) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.27 | ▼ 09:30 equity $9,171.76 vs yday $9,220.35 (-48.59) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 77 | $20.52 | $2.25 | $-14.48 | $1,578.07 | ▼ -14.48 after sell → book $9,169.51; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INDP` | 408 | $3.77 | $5.34 | $-75.89 | $3,110.88 | ▼ -75.89 after sell → book $9,164.17; vs 09:30 mark -5.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 102 | $14.38 | $2.32 | $-141.30 | $4,575.32 | ▼ -141.30 after sell → book $9,161.84; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `TNGX` | 63 | $23.99 | $2.20 | $-93.21 | $6,084.49 | ▼ -93.21 after sell → book $9,159.64; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CLPT` | 103 | $14.82 | $2.33 | $-79.82 | $7,608.62 | ▼ -79.82 after sell → book $9,157.31; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `NMRA` | 2076 | $0.75 | $22.07 | $-89.92 | $9,135.24 | ▼ -89.92 after sell → book $9,135.24; vs 09:30 mark -22.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,135.24 | ▲ close $9,135.24 vs 09:30 $9,171.76 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,652.38 | ▲ 09:30 equity $9,652.38 vs yday $9,652.38 (+0.00) | 09:30 open · cash $9,652.38 · no holdings · equity $9,652.38 vs prior close $9,652.38 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 52 | $26.27 | $2.15 | — | $8,284.19 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1378.91 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 16 | $83.76 | $2.04 | — | $6,942.00 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1378.91 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BRVE` | 58 | $23.58 | $2.16 | — | $5,572.19 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $1378.91 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 626 | $2.20 | $8.08 | — | $4,186.92 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1378.91 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 229 | $6.00 | $2.95 | — | $2,809.96 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $1378.91 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 76 | $17.91 | $2.22 | — | $1,446.58 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+3.7; leftover $1378.91 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 16 | $83.69 | $2.04 | — | $105.43 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $1378.91 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $105.43 | ▼ close $9,542.90 vs 09:30 $9,652.38 (session -87.85) | 16:00 close · cash $105.43 · equity $9,542.90 vs 09:30 $9,652.38 (-109.48; session marks -87.85) · 7 name(s) marked open→close (per-name table). WRBY×52 09:30 $26.27 → close $26.71 +22.88; TXG×16 09:30 $83.76 → close $85.71 +31.20; BRVE×58 09:30 $23.58 → close $20.62 -171.68; HLP×626 09:30 $2.20 → close $2.21 +6.26; SATL×229 09:30 $6.00 → close $6.17 +38.93; PL×76 09:30 $17.91 → close $17.43 -36.48; TEM×16 09:30 $83.69 → close $85.01 +21.04 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `IRD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CAN` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `USDE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IVVD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-21 | `SNDK` | cash | leftover split 1229.84 < 1 share @ 1826.00 |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SGML` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `MRAM` | no_price | no 09:30 open |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TDTH` | hard_red | hard-red S=-7.66 sit; no new buys |
