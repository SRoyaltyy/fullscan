# Factor mine action — `short_alarm_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · alarm

Cash book **-0.03%** ($9,997) · signal-only (no cash/fees) was +3.57%. Starts YES **5/30**. Fills 124 · skips 96 · realized $+54.59.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a SHORT sleeve: it borrows the name and profits if the price falls. Equity treats the short as a liability (must keep enough to cover).

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `alarm=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,054.61.

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
| 2026-08-14 09:30 ET | **SHORT** | `WWW` | 30 | $20.60 | $2.12 | — | $10,615.88 | — | alarm; gate alarm=True; list probable,yday_gainer; ret5=+4.4; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `FOSL` | 110 | $5.64 | $2.37 | — | $11,233.92 | — | alarm; gate alarm=True; list probable; 🔵; ret5=-4.1; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `AIRS` | 185 | $3.37 | $2.60 | — | $11,854.76 | — | alarm; gate alarm=True; list probable; ret5=-29.1; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OMER` | 36 | $17.35 | $2.14 | — | $12,477.23 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `MXCT` | 449 | $1.39 | $5.89 | — | $13,095.45 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+25.2; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `AVAH` | 52 | $11.91 | $2.18 | — | $13,712.58 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+21.3; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `CRMD` | 77 | $8.05 | $2.26 | — | $14,330.17 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LVWR` | 500 | $1.25 | $6.56 | — | $14,948.61 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+12.6; leftover $625.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,948.61 | ▲ close $10,037.72 vs 09:30 $10,000.00 (session +63.84) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,948.61 | ▲ 09:30 equity $10,069.07 vs yday $10,037.72 (+31.35) | — | — |
| 2026-08-17 09:30 ET | **COVER** | `WWW` | 30 | $20.98 | $2.08 | $-15.60 | $14,317.13 | ▼ -15.60 after sell → book $10,066.99; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `FOSL` | 110 | $5.50 | $2.32 | $+10.71 | $13,709.81 | ▲ +10.71 after sell → book $10,064.67; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AIRS` | 185 | $3.40 | $2.54 | $-9.77 | $13,079.19 | ▼ -9.77 after sell → book $10,062.12; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `OMER` | 36 | $17.17 | $2.10 | $+2.25 | $12,458.97 | ▲ +2.25 after sell → book $10,060.02; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `MXCT` | 449 | $1.32 | $5.79 | $+19.74 | $11,860.50 | ▲ +19.74 after sell → book $10,054.23; vs 09:30 mark -5.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AVAH` | 52 | $12.21 | $2.15 | $-19.93 | $11,223.44 | ▼ -19.93 after sell → book $10,052.09; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `CRMD` | 77 | $7.55 | $2.22 | $+34.02 | $10,639.87 | ▲ +34.02 after sell → book $10,049.87; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `LVWR` | 500 | $1.18 | $6.45 | $+21.99 | $10,043.42 | ▲ +21.99 after sell → book $10,043.42; vs 09:30 mark -6.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `HNST` | 130 | $4.81 | $2.43 | — | $10,666.29 | — | alarm; gate alarm=True; list flatten; ⚪; ret5=-11.4; leftover $627.71 | — |
| 2026-08-17 09:30 ET | **SHORT** | `FCEL` | 28 | $22.37 | $2.11 | — | $11,290.54 | — | alarm; gate alarm=True; list probable,yday_gainer; ⚪; ret5=+9.5; leftover $627.71 | — |
| 2026-08-17 09:30 ET | **SHORT** | `BW` | 60 | $10.35 | $2.21 | — | $11,909.33 | — | alarm; gate alarm=True; list probable; ⚪; ret5=+9.8; leftover $627.71 | — |
| 2026-08-17 09:30 ET | **SHORT** | `INO` | 586 | $1.07 | $7.68 | — | $12,528.66 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+62.7; leftover $627.71 | — |
| 2026-08-17 09:30 ET | **SHORT** | `BYND` | 48 | $12.83 | $2.17 | — | $13,142.33 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ⚪; ret5=-34.1; leftover $627.71 | — |
| 2026-08-17 09:30 ET | **SHORT** | `AEHR` | 4 | $132.79 | $2.04 | — | $13,671.46 | — | alarm; gate alarm=True; list yday_gainer; ⚪; ret5=+30.1; leftover $627.71 | — |
| 2026-08-17 09:30 ET | **SHORT** | `LUNR` | 30 | $20.25 | $2.12 | — | $14,276.84 | — | alarm; gate alarm=True; list yday_gainer,ohlc_hot; ⚪; ret5=+15.9; leftover $627.71 | — |
| 2026-08-17 09:30 ET | **SHORT** | `IOVA` | 91 | $6.84 | $2.31 | — | $14,896.98 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.1; leftover $627.71 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,896.98 | ▼ close $9,992.62 vs 09:30 $10,069.07 (session -27.74) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,896.98 | ▲ 09:30 equity $10,160.42 vs yday $9,992.62 (+167.80) | — | — |
| 2026-08-18 09:30 ET | **COVER** | `HNST` | 130 | $4.67 | $2.38 | $+13.39 | $14,287.50 | ▲ +13.39 after sell → book $10,158.04; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `FCEL` | 28 | $21.18 | $2.07 | $+29.13 | $13,692.38 | ▲ +29.13 after sell → book $10,155.96; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `BW` | 60 | $9.60 | $2.17 | $+40.62 | $13,114.21 | ▲ +40.62 after sell → book $10,153.79; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `INO` | 586 | $1.14 | $7.56 | $-56.26 | $12,438.61 | ▼ -56.26 after sell → book $10,146.23; vs 09:30 mark -7.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `BYND` | 48 | $11.12 | $2.13 | $+77.78 | $11,902.72 | ▲ +77.78 after sell → book $10,144.10; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `AEHR` | 4 | $135.58 | $2.00 | $-15.20 | $11,358.40 | ▼ -15.20 after sell → book $10,142.10; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `LUNR` | 30 | $19.31 | $2.08 | $+24.00 | $10,777.02 | ▲ +24.00 after sell → book $10,140.02; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `IOVA` | 91 | $7.00 | $2.26 | $-19.13 | $10,137.75 | ▼ -19.13 after sell → book $10,137.75; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,137.75 | ▲ close $10,137.75 vs 09:30 $10,160.42 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,137.75 | ▲ 09:30 equity $10,137.75 vs yday $10,137.75 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,137.75 | ▲ close $10,137.75 vs 09:30 $10,137.75 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,137.75 | ▲ 09:30 equity $10,137.75 vs yday $10,137.75 (+0.00) | — | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,137.75 | ▲ close $10,137.75 vs 09:30 $10,137.75 (session +0.00) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,137.75 | ▲ 09:30 equity $10,137.75 vs yday $10,137.75 (+0.00) | — | — |
| 2026-08-21 09:30 ET | **SHORT** | `YSS` | 109 | $9.26 | $2.37 | — | $11,144.72 | — | alarm; gate alarm=True; list yday_mover; ret5=-20.1; leftover $1013.78 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SMJF` | 89 | $11.35 | $2.31 | — | $12,152.56 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+13.4; leftover $1013.78 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 37 | $27.00 | $2.15 | — | $13,149.41 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.1; leftover $1013.78 | — |
| 2026-08-21 09:30 ET | **SHORT** | `CPRT` | 29 | $34.48 | $2.12 | — | $14,147.21 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+16.8; leftover $1013.78 | — |
| 2026-08-21 09:30 ET | **SHORT** | `FLO` | 146 | $6.90 | $2.49 | — | $15,152.12 | — | alarm; gate alarm=True; list earn_react; ret5=-5.7; leftover $1013.78 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,152.12 | ▼ close $10,114.27 vs 09:30 $10,137.75 (session -12.04) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,152.12 | ▲ 09:30 equity $10,139.13 vs yday $10,114.27 (+24.86) | — | — |
| 2026-08-24 09:30 ET | **COVER** | `YSS` | 109 | $9.22 | $2.32 | $-0.33 | $14,144.82 | ▼ -0.33 after sell → book $10,136.81; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `SMJF` | 89 | $11.25 | $2.26 | $+4.33 | $13,141.31 | ▲ +4.33 after sell → book $10,134.55; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `NOG` | 37 | $27.12 | $2.10 | $-8.69 | $12,135.77 | ▼ -8.69 after sell → book $10,132.45; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `CPRT` | 29 | $34.04 | $2.08 | $+8.56 | $11,146.54 | ▲ +8.56 after sell → book $10,130.38; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `FLO` | 146 | $6.96 | $2.43 | $-13.68 | $10,127.95 | ▼ -13.68 after sell → book $10,127.95; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,127.95 | ▲ close $10,127.95 vs 09:30 $10,139.13 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,127.95 | ▲ 09:30 equity $10,127.95 vs yday $10,127.95 (-0.00) | — | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,127.95 | ▲ close $10,127.95 vs 09:30 $10,127.95 (session +0.00) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,127.95 | ▲ 09:30 equity $10,127.95 vs yday $10,127.95 (-0.00) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,127.95 | ▲ close $10,127.95 vs 09:30 $10,127.95 (session +0.00) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,127.95 | ▲ 09:30 equity $10,127.95 vs yday $10,127.95 (-0.00) | — | — |
| 2026-08-27 09:30 ET | **SHORT** | `AVEX` | 34 | $18.43 | $2.13 | — | $10,752.44 | — | alarm; gate alarm=True; list probable,yday_gainer,yday_mover; ret5=-3.0; leftover $633.00 | — |
| 2026-08-27 09:30 ET | **SHORT** | `BKSY` | 25 | $25.29 | $2.10 | — | $11,382.59 | — | alarm; gate alarm=True; list yday_gainer; ret5=-11.5; leftover $633.00 | — |
| 2026-08-27 09:30 ET | **SHORT** | `BRR` | 289 | $2.19 | $3.80 | — | $12,011.69 | — | alarm; gate alarm=True; list yday_gainer; ret5=+3.3; leftover $633.00 | — |
| 2026-08-27 09:30 ET | **SHORT** | `USDE` | 97 | $6.50 | $2.32 | — | $12,639.87 | — | alarm; gate alarm=True; list yday_mover; ⚪; ret5=+93.5; leftover $633.00 | — |
| 2026-08-27 09:30 ET | **SHORT** | `SUJA` | 67 | $9.41 | $2.23 | — | $13,268.11 | — | alarm; gate alarm=True; list yday_mover; ret5=+27.7; leftover $633.00 | — |
| 2026-08-27 09:30 ET | **SHORT** | `BYND` | 44 | $14.20 | $2.16 | — | $13,890.75 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+1.2; leftover $633.00 | — |
| 2026-08-27 09:30 ET | **SHORT** | `FUTU` | 4 | $128.00 | $2.04 | — | $14,400.71 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+16.4; leftover $633.00 | — |
| 2026-08-27 09:30 ET | **SHORT** | `HNST` | 111 | $5.67 | $2.37 | — | $15,027.16 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+13.9; leftover $633.00 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,027.16 | ▼ close $10,006.65 vs 09:30 $10,127.95 (session -102.14) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,027.16 | ▲ 09:30 equity $10,090.23 vs yday $10,006.65 (+83.58) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `AVEX` | 34 | $18.75 | $2.09 | $-15.10 | $14,387.57 | ▼ -15.10 after sell → book $10,088.14; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `BKSY` | 25 | $24.44 | $2.06 | $+17.08 | $13,774.50 | ▲ +17.08 after sell → book $10,086.07; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `BRR` | 289 | $2.16 | $3.73 | $+1.14 | $13,146.54 | ▲ +1.14 after sell → book $10,082.35; vs 09:30 mark -3.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `USDE` | 97 | $7.24 | $2.28 | $-76.39 | $12,441.97 | ▼ -76.39 after sell → book $10,080.06; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `SUJA` | 67 | $9.08 | $2.19 | $+17.69 | $11,831.42 | ▲ +17.69 after sell → book $10,077.87; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `BYND` | 44 | $14.00 | $2.12 | $+4.52 | $11,213.30 | ▲ +4.52 after sell → book $10,075.75; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `FUTU` | 4 | $124.27 | $2.00 | $+10.88 | $10,714.22 | ▲ +10.88 after sell → book $10,073.75; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `HNST` | 111 | $5.77 | $2.32 | $-16.35 | $10,071.43 | ▼ -16.35 after sell → book $10,071.43; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SHORT** | `PYXS` | 189 | $3.32 | $2.62 | — | $10,696.29 | — | alarm; gate alarm=True; list probable,yday_gainer; ret5=+6.4; leftover $629.46 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SAFX` | 1724 | $0.36 | $11.78 | — | $11,313.77 | — | alarm; gate alarm=True; list probable,yday_gainer; ret5=+7.6; leftover $629.46 | — |
| 2026-08-28 09:30 ET | **SHORT** | `XPOF` | 117 | $5.38 | $2.39 | — | $11,940.85 | — | alarm; gate alarm=True; list probable,yday_gainer; ret5=+6.5; leftover $629.46 | — |
| 2026-08-28 09:30 ET | **SHORT** | `APMD` | 21 | $29.01 | $2.09 | — | $12,547.97 | — | alarm; gate alarm=True; list yday_gainer; ret5=+0.6; leftover $629.46 | — |
| 2026-08-28 09:30 ET | **SHORT** | `OPTU` | 629 | $1.00 | $8.25 | — | $13,168.72 | — | alarm; gate alarm=True; list yday_gainer; ret5=+16.8; leftover $629.46 | — |
| 2026-08-28 09:30 ET | **SHORT** | `ABTC` | 73 | $8.61 | $2.25 | — | $13,795.00 | — | alarm; gate alarm=True; list yday_mover; ret5=+3.4; leftover $629.46 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SBET` | 72 | $8.65 | $2.25 | — | $14,415.56 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+17.0; leftover $629.46 | — |
| 2026-08-28 09:30 ET | **SHORT** | `CRCL` | 6 | $92.61 | $2.04 | — | $14,969.17 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+12.6; leftover $629.46 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,969.17 | ▲ close $10,164.38 vs 09:30 $10,090.23 (session +126.60) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,969.17 | ▼ 09:30 equity $10,147.01 vs yday $10,164.38 (-17.37) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `PYXS` | 189 | $3.20 | $2.56 | $+17.51 | $14,361.82 | ▲ +17.51 after sell → book $10,144.45; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SAFX` | 1724 | $0.36 | $11.41 | $-18.02 | $13,726.32 | ▼ -18.02 after sell → book $10,133.04; vs 09:30 mark -11.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `XPOF` | 117 | $5.37 | $2.34 | $-3.56 | $13,095.68 | ▼ -3.56 after sell → book $10,130.70; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `APMD` | 21 | $29.71 | $2.05 | $-18.84 | $12,469.72 | ▼ -18.84 after sell → book $10,128.65; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `OPTU` | 629 | $1.06 | $8.11 | $-54.10 | $11,794.87 | ▼ -54.10 after sell → book $10,120.53; vs 09:30 mark -8.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABTC` | 73 | $7.66 | $2.21 | $+65.26 | $11,233.84 | ▲ +65.26 after sell → book $10,118.32; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SBET` | 72 | $8.24 | $2.21 | $+25.07 | $10,638.36 | ▲ +25.07 after sell → book $10,116.12; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRCL` | 6 | $87.04 | $2.01 | $+29.37 | $10,114.11 | ▲ +29.37 after sell → book $10,114.11; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,147.01 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | — | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | — | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,114.11 | ▲ close $10,114.11 vs 09:30 $10,114.11 (session +0.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,114.11 | ▲ 09:30 equity $10,114.11 vs yday $10,114.11 (-0.00) | — | — |
| 2026-09-17 09:30 ET | **SHORT** | `FRO` | 15 | $54.31 | $2.08 | — | $10,926.68 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+13.7; leftover $842.84 | — |
| 2026-09-17 09:30 ET | **SHORT** | `CVI` | 16 | $51.88 | $2.08 | — | $11,754.68 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+11.1; leftover $842.84 | — |
| 2026-09-17 09:30 ET | **SHORT** | `DHT` | 36 | $22.97 | $2.14 | — | $12,579.46 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+8.7; leftover $842.84 | — |
| 2026-09-17 09:30 ET | **SHORT** | `ATRC` | 14 | $57.96 | $2.07 | — | $13,388.83 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+7.8; leftover $842.84 | — |
| 2026-09-17 09:30 ET | **SHORT** | `HAFN` | 86 | $9.75 | $2.30 | — | $14,225.03 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+6.0; leftover $842.84 | — |
| 2026-09-17 09:30 ET | **SHORT** | `VLO` | 2 | $398.45 | $2.04 | — | $15,019.89 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+6.7; leftover $842.84 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,019.89 | ▼ close $10,033.02 vs 09:30 $10,114.11 (session -68.38) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,019.89 | ▲ 09:30 equity $10,062.44 vs yday $10,033.02 (+29.42) | — | — |
| 2026-09-18 09:30 ET | **COVER** | `FRO` | 15 | $51.19 | $2.04 | $+42.69 | $14,250.01 | ▲ +42.69 after sell → book $10,060.41; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `CVI` | 16 | $54.10 | $2.04 | $-39.64 | $13,382.37 | ▼ -39.64 after sell → book $10,058.37; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `DHT` | 36 | $23.16 | $2.10 | $-11.08 | $12,546.51 | ▼ -11.08 after sell → book $10,056.27; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `ATRC` | 14 | $58.51 | $2.03 | $-11.81 | $11,725.34 | ▼ -11.81 after sell → book $10,054.24; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `HAFN` | 86 | $9.85 | $2.25 | $-13.14 | $10,875.99 | ▼ -13.14 after sell → book $10,051.99; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `VLO` | 2 | $412.00 | $2.00 | $-31.13 | $10,050.00 | ▼ -31.13 after sell → book $10,050.00; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SHORT** | `DELL` | 1 | $593.15 | $2.03 | — | $10,641.12 | — | alarm; gate alarm=True; list flatten,ohlc_hot; ret5=+16.1; leftover $1005.00 | — |
| 2026-09-18 09:30 ET | **SHORT** | `SWRD` | 483 | $2.08 | $6.35 | — | $11,639.41 | — | alarm; gate alarm=True; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $1005.00 | — |
| 2026-09-18 09:30 ET | **SHORT** | `SMTC` | 5 | $182.33 | $2.05 | — | $12,549.01 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+12.0; leftover $1005.00 | — |
| 2026-09-18 09:30 ET | **SHORT** | `IQ` | 897 | $1.12 | $11.76 | — | $13,541.89 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+17.4; leftover $1005.00 | — |
| 2026-09-18 09:30 ET | **SHORT** | `BKV` | 43 | $22.92 | $2.17 | — | $14,525.28 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.8; leftover $1005.00 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,525.28 | ▲ close $10,100.09 vs 09:30 $10,062.44 (session +74.45) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,525.28 | ▼ 09:30 equity $10,067.35 vs yday $10,100.09 (-32.74) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `DELL` | 1 | $586.77 | $1.99 | $+2.36 | $13,936.52 | ▲ +2.36 after sell → book $10,065.36; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `SWRD` | 483 | $2.15 | $6.23 | $-46.39 | $12,891.84 | ▼ -46.39 after sell → book $10,059.13; vs 09:30 mark -6.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `SMTC` | 5 | $190.30 | $2.00 | $-43.90 | $11,938.34 | ▼ -43.90 after sell → book $10,057.13; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `IQ` | 897 | $1.01 | $11.57 | $+75.34 | $11,020.79 | ▲ +75.34 after sell → book $10,045.55; vs 09:30 mark -11.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `BKV` | 43 | $22.68 | $2.12 | $+6.03 | $10,043.44 | ▲ +6.03 after sell → book $10,043.44; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SHORT** | `PGEN` | 80 | $7.84 | $2.27 | — | $10,668.37 | — | alarm; gate alarm=True; list flatten; ret5=+13.6; leftover $627.71 | — |
| 2026-09-21 09:30 ET | **SHORT** | `IOVA` | 60 | $10.43 | $2.21 | — | $11,291.96 | — | alarm; gate alarm=True; list flatten; ret5=+19.2; leftover $627.71 | — |
| 2026-09-21 09:30 ET | **SHORT** | `MGTX` | 46 | $13.47 | $2.16 | — | $11,909.41 | — | alarm; gate alarm=True; list flatten; ret5=+3.6; leftover $627.71 | — |
| 2026-09-21 09:30 ET | **SHORT** | `CYPH` | 156 | $4.00 | $2.51 | — | $12,530.90 | — | alarm; gate alarm=True; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $627.71 | — |
| 2026-09-21 09:30 ET | **SHORT** | `CTKB` | 117 | $5.32 | $2.39 | — | $13,150.95 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+15.0; leftover $627.71 | — |
| 2026-09-21 09:30 ET | **SHORT** | `ALVO` | 106 | $5.92 | $2.35 | — | $13,776.12 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+11.0; leftover $627.71 | — |
| 2026-09-21 09:30 ET | **SHORT** | `TH` | 28 | $21.65 | $2.11 | — | $14,380.21 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.1; leftover $627.71 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AKBA` | 677 | $0.93 | $8.45 | — | $14,999.34 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+12.1; leftover $627.71 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,999.34 | ▲ close $10,167.28 vs 09:30 $10,067.35 (session +148.30) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,999.34 | ▼ 09:30 equity $10,143.27 vs yday $10,167.28 (-24.01) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `IOVA` | 60 | $10.18 | $2.17 | $+10.62 | $14,386.37 | ▲ +10.62 after sell → book $10,141.10; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **COVER** | `CYPH` | 156 | $3.51 | $2.46 | $+71.47 | $13,836.36 | ▲ +71.47 after sell → book $10,138.65; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **COVER** | `AKBA` | 677 | $0.95 | $8.46 | $-32.48 | $13,184.74 | ▼ -32.48 after sell → book $10,130.18; vs 09:30 mark -8.47 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SHORT** | `GLND` | 215 | $2.94 | $2.84 | — | $13,814.01 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+136.1; leftover $633.14 | — |
| 2026-09-22 09:30 ET | **SHORT** | `USDE` | 48 | $12.99 | $2.17 | — | $14,435.36 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+69.4; leftover $633.14 | — |
| 2026-09-22 09:30 ET | **SHORT** | `VGZ` | 238 | $2.65 | $3.14 | — | $15,062.92 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+26.3; leftover $633.14 | — |
| 2026-09-22 09:30 ET | **SHORT** | `AXTI` | 8 | $76.64 | $2.05 | — | $15,673.99 | — | alarm; gate alarm=True; list yday_gainer; ret5=+40.1; leftover $633.14 | — |
| 2026-09-22 09:30 ET | **SHORT** | `UMC` | 25 | $25.26 | $2.10 | — | $16,303.39 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+16.1; leftover $633.14 | — |
| 2026-09-22 09:30 ET | **SHORT** | `MRVL` | 2 | $255.46 | $2.03 | — | $16,812.28 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+17.6; leftover $633.14 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,812.28 | ▼ close $10,105.70 vs 09:30 $10,143.27 (session -10.16) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,812.28 | ▼ 09:30 equity $10,079.75 vs yday $10,105.70 (-25.95) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `PGEN` | 80 | $7.95 | $2.23 | $-13.30 | $16,174.05 | ▼ -13.30 after sell → book $10,077.52; vs 09:30 mark -2.23 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `MGTX` | 46 | $12.26 | $2.13 | $+51.37 | $15,607.96 | ▲ +51.37 after sell → book $10,075.39; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `CTKB` | 117 | $5.73 | $2.34 | $-52.70 | $14,935.21 | ▼ -52.70 after sell → book $10,073.05; vs 09:30 mark -2.34 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `ALVO` | 106 | $5.86 | $2.31 | $+1.70 | $14,311.74 | ▲ +1.70 after sell → book $10,070.74; vs 09:30 mark -2.31 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `TH` | 28 | $21.15 | $2.07 | $+9.82 | $13,717.46 | ▲ +9.82 after sell → book $10,068.66; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `GLND` | 215 | $2.70 | $2.77 | $+45.99 | $13,134.19 | ▲ +45.99 after sell → book $10,065.89; vs 09:30 mark -2.77 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `USDE` | 48 | $13.22 | $2.13 | $-15.35 | $12,497.50 | ▼ -15.35 after sell → book $10,063.76; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `VGZ` | 238 | $2.73 | $3.07 | $-25.25 | $11,844.69 | ▼ -25.25 after sell → book $10,060.69; vs 09:30 mark -3.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `AXTI` | 8 | $78.41 | $2.01 | $-18.22 | $11,215.39 | ▼ -18.22 after sell → book $10,058.67; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `UMC` | 25 | $25.28 | $2.06 | $-4.67 | $10,581.33 | ▼ -4.67 after sell → book $10,056.61; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `MRVL` | 2 | $262.36 | $2.00 | $-17.83 | $10,054.61 | ▼ -17.83 after sell → book $10,054.61; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,054.61 | ▲ close $10,054.61 vs 09:30 $10,079.75 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,054.61 | ▲ 09:30 equity $10,054.61 vs yday $10,054.61 (+0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,054.61 | ▲ close $10,054.61 vs 09:30 $10,054.61 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,996.57 | ▲ 09:30 equity $9,996.57 vs yday $9,996.57 (+0.00) | 09:30 open · cash $9,996.57 · no holdings · equity $9,996.57 vs prior close $9,996.57 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,996.57 | ▲ close $9,996.57 vs 09:30 $9,996.57 (session +0.00) | 16:00 close · cash $9,996.57 · no lots left · equity $9,996.57. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `CBRS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COHR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDTH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SNDK` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `LITE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WDC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENHA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INV` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `INMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WFF` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `EYPT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OABI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USDE` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CAN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ARCT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ASST` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SLS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `ARCT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CNXC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NABL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `AREC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SNAP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `STT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PURR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PTRN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PCG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MNSO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ED` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DUOL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DEFT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CNH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VIST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DINO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `XLAB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `DFDV` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CIFR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BRR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `DPRO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ADBT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `RARE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CABA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SARO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ORBS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SWIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TGB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DRTS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TRBG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `XHG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TRBG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BNC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `WYHG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AIAI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GPRO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ADBE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CPRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `XHLD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BAND` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CLOV` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ECO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `PGNY` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ECHO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `HLP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `DBI` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TJGC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `BBY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `VOD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MGTX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `CTKB` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TH` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `HYLN` | no_price | no 09:30 open |
| 2026-09-24 | `AEHL` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AIRS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SVIA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CAI` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ILMN` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BETA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SG` | hard_red | hard-red S=-7.66 sit; no new buys |
