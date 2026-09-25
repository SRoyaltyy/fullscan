# Factor mine action — `short_alarm_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · alarm

Cash book **-1.98%** ($9,802) · signal-only (no cash/fees) was -2.80%. Starts YES **2/30**. Fills 121 · skips 221 · realized $+639.78.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `alarm=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $14,371.26.

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
| 2026-08-17 09:30 ET | **SHORT** | `HNST` | 130 | $4.81 | $2.43 | — | $15,571.48 | — | alarm; gate alarm=True; list flatten; ⚪; ret5=-11.4; leftover $629.32 | — |
| 2026-08-17 09:30 ET | **SHORT** | `FCEL` | 28 | $22.37 | $2.11 | — | $16,195.73 | — | alarm; gate alarm=True; list probable,yday_gainer; ⚪; ret5=+9.5; leftover $629.32 | — |
| 2026-08-17 09:30 ET | **SHORT** | `BW` | 60 | $10.35 | $2.21 | — | $16,814.53 | — | alarm; gate alarm=True; list probable; ⚪; ret5=+9.8; leftover $629.32 | — |
| 2026-08-17 09:30 ET | **SHORT** | `INO` | 588 | $1.07 | $7.71 | — | $17,435.98 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+62.7; leftover $629.32 | — |
| 2026-08-17 09:30 ET | **SHORT** | `BYND` | 49 | $12.83 | $2.17 | — | $18,062.47 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ⚪; ret5=-34.1; leftover $629.32 | — |
| 2026-08-17 09:30 ET | **SHORT** | `AEHR` | 4 | $132.79 | $2.04 | — | $18,591.59 | — | alarm; gate alarm=True; list yday_gainer; ⚪; ret5=+30.1; leftover $629.32 | — |
| 2026-08-17 09:30 ET | **SHORT** | `LUNR` | 31 | $20.25 | $2.12 | — | $19,217.22 | — | alarm; gate alarm=True; list yday_gainer,ohlc_hot; ⚪; ret5=+15.9; leftover $629.32 | — |
| 2026-08-17 09:30 ET | **SHORT** | `IOVA` | 92 | $6.84 | $2.31 | — | $19,844.20 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.1; leftover $629.32 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,844.20 | ▲ close $10,058.29 vs 09:30 $10,069.07 (session +12.32) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,844.20 | ▲ 09:30 equity $10,276.97 vs yday $10,058.29 (+218.68) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,844.20 | ▼ close $10,202.19 vs 09:30 $10,276.97 (session -74.78) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,844.20 | ▼ 09:30 equity $10,183.81 vs yday $10,202.19 (-18.38) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `WWW` | 30 | $20.08 | $2.08 | $+11.40 | $19,239.72 | ▲ +11.40 after sell → book $10,181.73; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `FOSL` | 110 | $5.54 | $2.32 | $+6.31 | $18,628.00 | ▲ +6.31 after sell → book $10,179.41; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `AIRS` | 185 | $2.71 | $2.54 | $+116.95 | $18,124.10 | ▲ +116.95 after sell → book $10,176.86; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OMER` | 36 | $17.13 | $2.10 | $+3.69 | $17,505.32 | ▲ +3.69 after sell → book $10,174.76; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `MXCT` | 449 | $1.29 | $5.79 | $+33.21 | $16,920.32 | ▲ +33.21 after sell → book $10,168.97; vs 09:30 mark -5.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `AVAH` | 52 | $12.92 | $2.15 | $-56.85 | $16,246.33 | ▼ -56.85 after sell → book $10,166.82; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `CRMD` | 77 | $8.30 | $2.22 | $-23.73 | $15,605.01 | ▼ -23.73 after sell → book $10,164.60; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `LVWR` | 500 | $1.17 | $6.45 | $+26.99 | $15,013.56 | ▲ +26.99 after sell → book $10,158.15; vs 09:30 mark -6.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,013.56 | ▼ close $10,050.60 vs 09:30 $10,183.81 (session -107.55) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,013.56 | ▲ 09:30 equity $10,097.97 vs yday $10,050.60 (+47.37) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `HNST` | 130 | $4.98 | $2.38 | $-26.91 | $14,363.78 | ▼ -26.91 after sell → book $10,095.59; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `FCEL` | 28 | $20.21 | $2.07 | $+56.29 | $13,795.83 | ▲ +56.29 after sell → book $10,093.52; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `BW` | 60 | $9.05 | $2.17 | $+73.62 | $13,250.66 | ▲ +73.62 after sell → book $10,091.35; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `INO` | 588 | $1.30 | $7.59 | $-150.54 | $12,478.67 | ▼ -150.54 after sell → book $10,083.76; vs 09:30 mark -7.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `BYND` | 49 | $13.60 | $2.14 | $-42.04 | $11,810.14 | ▼ -42.04 after sell → book $10,081.63; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `AEHR` | 4 | $106.01 | $2.00 | $+103.08 | $11,384.10 | ▲ +103.08 after sell → book $10,079.63; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 31 | $18.13 | $2.08 | $+61.52 | $10,819.98 | ▲ +61.52 after sell → book $10,077.54; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `IOVA` | 92 | $8.07 | $2.27 | $-117.73 | $10,075.28 | ▼ -117.73 after sell → book $10,075.28; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,075.28 | ▲ close $10,075.28 vs 09:30 $10,097.97 (session +0.00) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,075.28 | ▲ 09:30 equity $10,075.28 vs yday $10,075.28 (-0.00) | — | — |
| 2026-08-21 09:30 ET | **SHORT** | `YSS` | 108 | $9.26 | $2.37 | — | $11,072.99 | — | alarm; gate alarm=True; list yday_mover; ret5=-20.1; leftover $1007.53 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SMJF` | 88 | $11.35 | $2.31 | — | $12,069.48 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+13.4; leftover $1007.53 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 37 | $27.00 | $2.15 | — | $13,066.33 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.1; leftover $1007.53 | — |
| 2026-08-21 09:30 ET | **SHORT** | `CPRT` | 29 | $34.48 | $2.12 | — | $14,064.13 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+16.8; leftover $1007.53 | — |
| 2026-08-21 09:30 ET | **SHORT** | `FLO` | 146 | $6.90 | $2.49 | — | $15,069.04 | — | alarm; gate alarm=True; list earn_react; ret5=-5.7; leftover $1007.53 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,069.04 | ▼ close $10,051.92 vs 09:30 $10,075.28 (session -11.92) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,069.04 | ▲ 09:30 equity $10,076.52 vs yday $10,051.92 (+24.60) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,069.04 | ▼ close $10,004.46 vs 09:30 $10,076.52 (session -72.06) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,069.04 | ▲ 09:30 equity $10,032.16 vs yday $10,004.46 (+27.70) | — | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,069.04 | ▲ close $10,099.11 vs 09:30 $10,032.16 (session +66.95) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,069.04 | ▲ 09:30 equity $10,134.26 vs yday $10,099.11 (+35.15) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `YSS` | 108 | $9.20 | $2.31 | $+1.80 | $14,073.12 | ▲ +1.80 after sell → book $10,131.94; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SMJF` | 88 | $11.15 | $2.25 | $+13.04 | $13,089.67 | ▲ +13.04 after sell → book $10,129.69; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 37 | $26.00 | $2.10 | $+32.75 | $12,125.57 | ▲ +32.75 after sell → book $10,127.59; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `CPRT` | 29 | $33.00 | $2.08 | $+38.72 | $11,166.49 | ▲ +38.72 after sell → book $10,125.51; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `FLO` | 146 | $7.13 | $2.43 | $-38.50 | $10,123.08 | ▼ -38.50 after sell → book $10,123.08; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,123.08 | ▲ close $10,123.08 vs 09:30 $10,134.26 (session +0.00) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,123.08 | ▲ 09:30 equity $10,123.08 vs yday $10,123.08 (+0.00) | — | — |
| 2026-08-27 09:30 ET | **SHORT** | `AVEX` | 34 | $18.43 | $2.13 | — | $10,747.58 | — | alarm; gate alarm=True; list probable,yday_gainer,yday_mover; ret5=-3.0; leftover $632.69 | — |
| 2026-08-27 09:30 ET | **SHORT** | `BKSY` | 25 | $25.29 | $2.10 | — | $11,377.72 | — | alarm; gate alarm=True; list yday_gainer; ret5=-11.5; leftover $632.69 | — |
| 2026-08-27 09:30 ET | **SHORT** | `BRR` | 288 | $2.19 | $3.79 | — | $12,004.65 | — | alarm; gate alarm=True; list yday_gainer; ret5=+3.3; leftover $632.69 | — |
| 2026-08-27 09:30 ET | **SHORT** | `USDE` | 97 | $6.50 | $2.32 | — | $12,632.83 | — | alarm; gate alarm=True; list yday_mover; ⚪; ret5=+93.5; leftover $632.69 | — |
| 2026-08-27 09:30 ET | **SHORT** | `SUJA` | 67 | $9.41 | $2.23 | — | $13,261.07 | — | alarm; gate alarm=True; list yday_mover; ret5=+27.7; leftover $632.69 | — |
| 2026-08-27 09:30 ET | **SHORT** | `BYND` | 44 | $14.20 | $2.16 | — | $13,883.71 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+1.2; leftover $632.69 | — |
| 2026-08-27 09:30 ET | **SHORT** | `FUTU` | 4 | $128.00 | $2.04 | — | $14,393.67 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+16.4; leftover $632.69 | — |
| 2026-08-27 09:30 ET | **SHORT** | `HNST` | 111 | $5.67 | $2.37 | — | $15,020.12 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+13.9; leftover $632.69 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,020.12 | ▼ close $10,001.77 vs 09:30 $10,123.08 (session -102.17) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,020.12 | ▲ 09:30 equity $10,085.35 vs yday $10,001.77 (+83.58) | — | — |
| 2026-08-28 09:30 ET | **SHORT** | `PYXS` | 189 | $3.32 | $2.62 | — | $15,644.98 | — | alarm; gate alarm=True; list probable,yday_gainer; ret5=+6.4; leftover $630.33 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SAFX` | 1726 | $0.36 | $11.79 | — | $16,263.18 | — | alarm; gate alarm=True; list probable,yday_gainer; ret5=+7.6; leftover $630.33 | — |
| 2026-08-28 09:30 ET | **SHORT** | `XPOF` | 117 | $5.38 | $2.39 | — | $16,890.26 | — | alarm; gate alarm=True; list probable,yday_gainer; ret5=+6.5; leftover $630.33 | — |
| 2026-08-28 09:30 ET | **SHORT** | `APMD` | 21 | $29.01 | $2.09 | — | $17,497.38 | — | alarm; gate alarm=True; list yday_gainer; ret5=+0.6; leftover $630.33 | — |
| 2026-08-28 09:30 ET | **SHORT** | `OPTU` | 630 | $1.00 | $8.26 | — | $18,119.12 | — | alarm; gate alarm=True; list yday_gainer; ret5=+16.8; leftover $630.33 | — |
| 2026-08-28 09:30 ET | **SHORT** | `ABTC` | 73 | $8.61 | $2.25 | — | $18,745.40 | — | alarm; gate alarm=True; list yday_mover; ret5=+3.4; leftover $630.33 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SBET` | 72 | $8.65 | $2.25 | — | $19,365.95 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+17.0; leftover $630.33 | — |
| 2026-08-28 09:30 ET | **SHORT** | `CRCL` | 6 | $92.61 | $2.04 | — | $19,919.57 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+12.6; leftover $630.33 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,919.57 | ▲ close $10,257.92 vs 09:30 $10,085.35 (session +206.25) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,919.57 | ▼ 09:30 equity $10,249.19 vs yday $10,257.92 (-8.73) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,919.57 | ▼ close $10,065.35 vs 09:30 $10,249.19 (session -183.84) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,919.57 | ▲ 09:30 equity $10,233.78 vs yday $10,065.35 (+168.43) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `AVEX` | 34 | $17.32 | $2.09 | $+33.52 | $19,328.60 | ▲ +33.52 after sell → book $10,231.68; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `BKSY` | 25 | $23.02 | $2.06 | $+52.46 | $18,750.91 | ▲ +52.46 after sell → book $10,229.62; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `BRR` | 288 | $2.14 | $3.72 | $+6.89 | $18,130.87 | ▲ +6.89 after sell → book $10,225.90; vs 09:30 mark -3.72 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `USDE` | 97 | $8.15 | $2.28 | $-164.66 | $17,338.04 | ▼ -164.66 after sell → book $10,223.62; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `SUJA` | 67 | $9.98 | $2.19 | $-42.61 | $16,667.19 | ▼ -42.61 after sell → book $10,221.43; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `BYND` | 44 | $13.04 | $2.12 | $+46.76 | $16,091.31 | ▲ +46.76 after sell → book $10,219.31; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `FUTU` | 4 | $119.82 | $2.00 | $+28.68 | $15,610.03 | ▲ +28.68 after sell → book $10,217.31; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `HNST` | 111 | $5.62 | $2.32 | $+0.30 | $14,983.88 | ▲ +0.30 after sell → book $10,214.98; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,983.88 | ▲ close $10,246.14 vs 09:30 $10,233.78 (session +31.16) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,983.88 | ▲ 09:30 equity $10,252.96 vs yday $10,246.14 (+6.82) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `PYXS` | 189 | $3.45 | $2.56 | $-29.74 | $14,329.28 | ▼ -29.74 after sell → book $10,250.41; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SAFX` | 1726 | $0.39 | $11.91 | $-66.85 | $13,644.23 | ▼ -66.85 after sell → book $10,238.50; vs 09:30 mark -11.91 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `XPOF` | 117 | $5.03 | $2.34 | $+36.22 | $13,053.38 | ▲ +36.22 after sell → book $10,236.16; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `APMD` | 21 | $27.00 | $2.05 | $+38.07 | $12,484.32 | ▲ +38.07 after sell → book $10,234.10; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `OPTU` | 630 | $0.93 | $7.77 | $+26.18 | $11,888.77 | ▲ +26.18 after sell → book $10,226.34; vs 09:30 mark -7.76 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `ABTC` | 73 | $7.66 | $2.21 | $+64.89 | $11,327.38 | ▲ +64.89 after sell → book $10,224.13; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SBET` | 72 | $8.01 | $2.21 | $+41.63 | $10,748.45 | ▲ +41.63 after sell → book $10,221.92; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `CRCL` | 6 | $87.75 | $2.01 | $+25.08 | $10,219.91 | ▲ +25.08 after sell → book $10,219.91; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,219.91 | ▲ close $10,219.91 vs 09:30 $10,252.96 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,219.91 | ▲ 09:30 equity $10,219.91 vs yday $10,219.91 (+0.00) | — | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,219.91 | ▲ close $10,219.91 vs 09:30 $10,219.91 (session +0.00) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,219.91 | ▲ 09:30 equity $10,219.91 vs yday $10,219.91 (+0.00) | — | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,219.91 | ▲ close $10,219.91 vs 09:30 $10,219.91 (session +0.00) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,219.91 | ▲ 09:30 equity $10,219.91 vs yday $10,219.91 (+0.00) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,219.91 | ▲ close $10,219.91 vs 09:30 $10,219.91 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,219.91 | ▲ 09:30 equity $10,219.91 vs yday $10,219.91 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,219.91 | ▲ close $10,219.91 vs 09:30 $10,219.91 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,219.91 | ▲ 09:30 equity $10,219.91 vs yday $10,219.91 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,219.91 | ▲ close $10,219.91 vs 09:30 $10,219.91 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,219.91 | ▲ 09:30 equity $10,219.91 vs yday $10,219.91 (+0.00) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,219.91 | ▲ close $10,219.91 vs 09:30 $10,219.91 (session +0.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,219.91 | ▲ 09:30 equity $10,219.91 vs yday $10,219.91 (+0.00) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,219.91 | ▲ close $10,219.91 vs 09:30 $10,219.91 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,219.91 | ▲ 09:30 equity $10,219.91 vs yday $10,219.91 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,219.91 | ▲ close $10,219.91 vs 09:30 $10,219.91 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,219.91 | ▲ 09:30 equity $10,219.91 vs yday $10,219.91 (+0.00) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,219.91 | ▲ close $10,219.91 vs 09:30 $10,219.91 (session +0.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,219.91 | ▲ 09:30 equity $10,219.91 vs yday $10,219.91 (+0.00) | — | — |
| 2026-09-17 09:30 ET | **SHORT** | `FRO` | 15 | $54.31 | $2.08 | — | $11,032.49 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+13.7; leftover $851.66 | — |
| 2026-09-17 09:30 ET | **SHORT** | `CVI` | 16 | $51.88 | $2.08 | — | $11,860.48 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+11.1; leftover $851.66 | — |
| 2026-09-17 09:30 ET | **SHORT** | `DHT` | 37 | $22.97 | $2.14 | — | $12,708.23 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+8.7; leftover $851.66 | — |
| 2026-09-17 09:30 ET | **SHORT** | `ATRC` | 14 | $57.96 | $2.07 | — | $13,517.60 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+7.8; leftover $851.66 | — |
| 2026-09-17 09:30 ET | **SHORT** | `HAFN` | 87 | $9.75 | $2.30 | — | $14,363.55 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+6.0; leftover $851.66 | — |
| 2026-09-17 09:30 ET | **SHORT** | `VLO` | 2 | $398.45 | $2.04 | — | $15,158.41 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+6.7; leftover $851.66 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,158.41 | ▼ close $10,139.00 vs 09:30 $10,219.91 (session -68.20) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,158.41 | ▲ 09:30 equity $10,167.95 vs yday $10,139.00 (+28.95) | — | — |
| 2026-09-18 09:30 ET | **SHORT** | `DELL` | 1 | $593.15 | $2.03 | — | $15,749.53 | — | alarm; gate alarm=True; list flatten,ohlc_hot; ret5=+16.1; leftover $1016.79 | — |
| 2026-09-18 09:30 ET | **SHORT** | `SWRD` | 488 | $2.08 | $6.41 | — | $16,758.16 | — | alarm; gate alarm=True; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $1016.79 | — |
| 2026-09-18 09:30 ET | **SHORT** | `SMTC` | 5 | $182.33 | $2.05 | — | $17,667.76 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+12.0; leftover $1016.79 | — |
| 2026-09-18 09:30 ET | **SHORT** | `IQ` | 907 | $1.12 | $11.89 | — | $18,671.71 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+17.4; leftover $1016.79 | — |
| 2026-09-18 09:30 ET | **SHORT** | `BKV` | 44 | $22.92 | $2.17 | — | $19,678.02 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.8; leftover $1016.79 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,678.02 | ▲ close $10,197.28 vs 09:30 $10,167.95 (session +53.88) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,678.02 | ▲ 09:30 equity $10,244.42 vs yday $10,197.28 (+47.14) | — | — |
| 2026-09-21 09:30 ET | **SHORT** | `PGEN` | 81 | $7.84 | $2.27 | — | $20,310.78 | — | alarm; gate alarm=True; list flatten; ret5=+13.6; leftover $640.28 | — |
| 2026-09-21 09:30 ET | **SHORT** | `IOVA` | 61 | $10.43 | $2.21 | — | $20,944.80 | — | alarm; gate alarm=True; list flatten; ret5=+19.2; leftover $640.28 | — |
| 2026-09-21 09:30 ET | **SHORT** | `MGTX` | 47 | $13.47 | $2.17 | — | $21,575.73 | — | alarm; gate alarm=True; list flatten; ret5=+3.6; leftover $640.28 | — |
| 2026-09-21 09:30 ET | **SHORT** | `CYPH` | 160 | $4.00 | $2.52 | — | $22,213.20 | — | alarm; gate alarm=True; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $640.28 | — |
| 2026-09-21 09:30 ET | **SHORT** | `CTKB` | 120 | $5.32 | $2.40 | — | $22,849.20 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+15.0; leftover $640.28 | — |
| 2026-09-21 09:30 ET | **SHORT** | `ALVO` | 108 | $5.92 | $2.36 | — | $23,486.20 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+11.0; leftover $640.28 | — |
| 2026-09-21 09:30 ET | **SHORT** | `TH` | 29 | $21.65 | $2.11 | — | $24,111.94 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.1; leftover $640.28 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AKBA` | 690 | $0.93 | $8.61 | — | $24,742.96 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+12.1; leftover $640.28 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,742.96 | ▲ close $10,570.10 vs 09:30 $10,244.42 (session +350.34) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,742.96 | ▼ 09:30 equity $10,551.03 vs yday $10,570.10 (-19.07) | — | — |
| 2026-09-22 09:30 ET | **SHORT** | `GLND` | 224 | $2.94 | $2.95 | — | $25,398.57 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+136.1; leftover $659.44 | — |
| 2026-09-22 09:30 ET | **SHORT** | `USDE` | 50 | $12.99 | $2.18 | — | $26,045.89 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+69.4; leftover $659.44 | — |
| 2026-09-22 09:30 ET | **SHORT** | `VGZ` | 248 | $2.65 | $3.27 | — | $26,699.82 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+26.3; leftover $659.44 | — |
| 2026-09-22 09:30 ET | **SHORT** | `AXTI` | 8 | $76.64 | $2.05 | — | $27,310.89 | — | alarm; gate alarm=True; list yday_gainer; ret5=+40.1; leftover $659.44 | — |
| 2026-09-22 09:30 ET | **SHORT** | `UMC` | 26 | $25.26 | $2.11 | — | $27,965.54 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+16.1; leftover $659.44 | — |
| 2026-09-22 09:30 ET | **SHORT** | `MRVL` | 2 | $255.46 | $2.03 | — | $28,474.43 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+17.6; leftover $659.44 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28,474.43 | ▼ close $10,463.76 vs 09:30 $10,551.03 (session -72.68) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28,474.43 | ▼ 09:30 equity $10,434.60 vs yday $10,463.76 (-29.16) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `FRO` | 15 | $47.92 | $2.04 | $+91.74 | $27,753.60 | ▲ +91.74 after sell → book $10,432.56; vs 09:30 mark -2.04 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `CVI` | 16 | $52.66 | $2.04 | $-16.60 | $26,909.00 | ▼ -16.60 after sell → book $10,430.53; vs 09:30 mark -2.03 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `DHT` | 37 | $21.35 | $2.10 | $+55.69 | $26,116.95 | ▲ +55.69 after sell → book $10,428.43; vs 09:30 mark -2.10 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `ATRC` | 14 | $58.92 | $2.03 | $-17.55 | $25,290.04 | ▼ -17.55 after sell → book $10,426.39; vs 09:30 mark -2.04 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `HAFN` | 87 | $9.29 | $2.25 | $+35.91 | $24,479.99 | ▲ +35.91 after sell → book $10,424.14; vs 09:30 mark -2.25 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `VLO` | 2 | $380.43 | $2.00 | $+32.01 | $23,717.14 | ▲ +32.01 after sell → book $10,422.15; vs 09:30 mark -1.99 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `DELL` | 1 | $559.00 | $1.99 | $+30.13 | $23,156.14 | ▲ +30.13 after sell → book $10,420.15; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `SWRD` | 488 | $2.16 | $6.30 | $-51.75 | $22,095.77 | ▼ -51.75 after sell → book $10,413.86; vs 09:30 mark -6.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `SMTC` | 5 | $174.50 | $2.00 | $+35.10 | $21,221.26 | ▲ +35.10 after sell → book $10,411.85; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `IQ` | 907 | $1.03 | $11.70 | $+58.04 | $20,275.35 | ▲ +58.04 after sell → book $10,400.15; vs 09:30 mark -11.70 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `BKV` | 44 | $23.29 | $2.12 | $-20.57 | $19,248.47 | ▼ -20.57 after sell → book $10,398.03; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,248.47 | ▲ close $10,604.56 vs 09:30 $10,434.60 (session +206.53) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,248.47 | ▲ 09:30 equity $10,658.18 vs yday $10,604.56 (+53.62) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `PGEN` | 81 | $7.38 | $2.23 | $+32.75 | $18,648.46 | ▲ +32.75 after sell → book $10,655.95; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `IOVA` | 61 | $10.39 | $2.17 | $-1.94 | $18,012.49 | ▼ -1.94 after sell → book $10,653.77; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `MGTX` | 47 | $11.42 | $2.13 | $+92.05 | $17,473.62 | ▲ +92.05 after sell → book $10,651.64; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `CYPH` | 160 | $3.40 | $2.47 | $+91.01 | $16,927.15 | ▲ +91.01 after sell → book $10,649.17; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `CTKB` | 120 | $5.64 | $2.35 | $-43.15 | $16,248.00 | ▼ -43.15 after sell → book $10,646.82; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `ALVO` | 108 | $5.75 | $2.31 | $+13.69 | $15,624.69 | ▲ +13.69 after sell → book $10,644.51; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `TH` | 29 | $20.97 | $2.08 | $+15.53 | $15,014.48 | ▲ +15.53 after sell → book $10,642.43; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `AKBA` | 690 | $0.92 | $8.42 | $-12.20 | $14,371.26 | ▼ -12.20 after sell → book $10,634.01; vs 09:30 mark -8.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,371.26 | ▼ close $10,038.08 vs 09:30 $10,658.18 (session -595.93) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,690.36 | ▼ 09:30 equity $9,809.31 vs yday $10,045.79 (-236.48) | 09:30 open · cash $16,690.36 (unchanged overnight, no fees) · equity $9,809.31 vs prior close $10,045.79 (-236.48) · 9 name(s) re-marked at the open (per-name table). AEHL×120 yday $8.96 → 09:30 $9.05 -10.80; ALOY×69 yday $8.52 → 09:30 $8.52 -0.00; AXTI×8 yday $75.90 → 09:30 $75.90 -0.00; CRML×71 yday $8.17 → 09:30 $8.17 -0.00; FJET×323 yday $1.80 → 09:30 $1.80 -0.00; GLND×222 yday $5.35 → 09:30 $6.06 -157.62; USDE×50 yday $14.22 → 09:30 $15.58 -68.05; VGZ×246 yday $2.71 → 09:30 $2.71 -0.00; YSS×66 yday $9.81 → 09:30 $9.81 -0.00 | — |
| 2026-09-25 09:30 ET | **COVER** | `AEHL` | 120 | $9.05 | $2.35 | $-99.56 | $15,602.01 | ▼ -99.56 after sell → book $9,806.96; vs 09:30 mark -2.35 | dropped from list after 4 sess (min 3) | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **COVER** | `GLND` | 222 | $6.06 | $2.86 | $-698.43 | $14,253.83 | ▼ -698.43 after sell → book $9,804.10; vs 09:30 mark -2.86 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **COVER** | `USDE` | 50 | $15.58 | $2.14 | $-133.87 | $13,472.63 | ▼ -133.87 after sell → book $9,801.96; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,472.63 | ▲ close $9,801.96 vs 09:30 $9,809.31 (session +0.00) | 16:00 close · cash $13,472.63 · equity $9,801.96 vs 09:30 $9,809.31 (-7.35; session marks +0.00) · 6 name(s) marked open→close (per-name table). ALOY×69 09:30 $8.52 → close $8.52 -0.00; AXTI×8 09:30 $75.90 → close $75.90 -0.00; CRML×71 09:30 $8.17 → close $8.17 -0.00; FJET×323 09:30 $1.80 → close $1.80 +0.00; VGZ×246 09:30 $2.71 → close $2.71 -0.00; YSS×66 09:30 $9.81 → close $9.81 -0.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `WWW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `FOSL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MXCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LVWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `WWW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `FOSL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OMER` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MXCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `FCEL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BYND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `AEHR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `LUNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CBRS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COHR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDTH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SNDK` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `LITE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WDC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENHA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INV` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `FCEL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BYND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `AEHR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `LUNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `INMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WFF` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `EYPT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OABI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `YSS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `SMJF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `NOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CPRT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USDE` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CAN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ARCT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ASST` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SLS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `YSS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `SMJF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `NOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CPRT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `FLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AVEX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `BKSY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `BRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SUJA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `BYND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `FUTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `HNST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `AVEX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `BKSY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `BRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SUJA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `BYND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `FUTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `HNST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `PYXS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `XPOF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `APMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OPTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ARCT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CNXC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NABL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `PYXS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `XPOF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `APMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `OPTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-18 | `FRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `CVI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `DHT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `VLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `FRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `CVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `DHT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `VLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SWRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `IQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BKV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FRO` | no_price | no 09:30 open — carry |
| 2026-09-22 | `CVI` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DHT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ATRC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `HAFN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `VLO` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `SWRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `IQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BKV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `CTKB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `AKBA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `HYLN` | no_price | no 09:30 open |
| 2026-09-23 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CTKB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `AKBA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GLND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `VGZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `AXTI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `UMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `MRVL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `GLND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `VGZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `AXTI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `UMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `MRVL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `AEHL` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AIRS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SVIA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CAI` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ILMN` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BETA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SG` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GLND` | 224 | 2026-09-22 @ $2.94 | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+136.1; leftover $659.44 |
| `USDE` | 50 | 2026-09-22 @ $12.99 | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+69.4; leftover $659.44 |
| `VGZ` | 248 | 2026-09-22 @ $2.65 | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+26.3; leftover $659.44 |
| `AXTI` | 8 | 2026-09-22 @ $76.64 | alarm; gate alarm=True; list yday_gainer; ret5=+40.1; leftover $659.44 |
| `UMC` | 26 | 2026-09-22 @ $25.26 | alarm; gate alarm=True; list ohlc_hot; ret5=+16.1; leftover $659.44 |
| `MRVL` | 2 | 2026-09-22 @ $255.46 | alarm; gate alarm=True; list ohlc_hot; ret5=+17.6; leftover $659.44 |
