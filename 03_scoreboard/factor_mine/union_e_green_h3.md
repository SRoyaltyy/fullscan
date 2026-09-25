# Factor mine action — `union_e_green_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-5.05%** ($9,495) · signal-only (no cash/fees) was -18.73%. Starts YES **20/30**. Fills 67 · skips 104 · realized $-87.73.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the name is in an earnings-reaction window (just reported, we are trading the reaction — not today's print).
- Must-have: the last finished bar was green (closed up).
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
- **Gate** `earn_react=True,last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $71.10.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 12176 | $0.81 | $135.15 | — | $2.29 | — | combo gate; gate earn_react=True,last_green=True; list flatten; ⚪; ret5=+13.2; leftover $10000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.29 | ▲ close $10,960.69 vs 09:30 $10,000.00 (session +1,095.84) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.29 | ▲ 09:30 equity $11,325.97 vs yday $10,960.69 (+365.28) | — | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.29 | ▲ close $13,274.13 vs 09:30 $11,325.97 (session +1,948.16) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.29 | ▼ 09:30 equity $13,030.61 vs yday $13,274.13 (-243.52) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.29 | ▲ close $14,004.69 vs 09:30 $13,030.61 (session +974.08) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.29 | ▼ 09:30 equity $13,882.93 vs yday $14,004.69 (-121.76) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 12176 | $1.14 | $159.20 | $+3723.72 | $13,723.72 | ▲ +3,723.72 after sell → book $13,723.72; vs 09:30 mark -159.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,723.72 | ▲ close $13,723.72 vs 09:30 $13,882.93 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,723.72 | ▲ 09:30 equity $13,723.72 vs yday $13,723.72 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,723.72 | ▲ close $13,723.72 vs 09:30 $13,723.72 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,723.72 | ▲ 09:30 equity $13,723.72 vs yday $13,723.72 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 50 | $34.05 | $2.14 | — | $12,019.08 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+9.3; leftover $1715.47 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 76 | $22.44 | $2.22 | — | $10,311.43 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.1; leftover $1715.47 | — |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 13 | $123.47 | $2.03 | — | $8,704.29 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+2.9; leftover $1715.47 | — |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 172 | $9.94 | $2.51 | — | $6,992.10 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+12.6; leftover $1715.47 | — |
| 2026-08-20 09:30 ET | **BUY** | `COTY` | 672 | $2.55 | $8.67 | — | $5,269.83 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+9.8; leftover $1715.47 | — |
| 2026-08-20 09:30 ET | **BUY** | `DQ` | 118 | $14.44 | $2.34 | — | $3,563.57 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-3.8; leftover $1715.47 | — |
| 2026-08-20 09:30 ET | **BUY** | `FUTU` | 14 | $117.65 | $2.03 | — | $1,914.44 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+4.1; leftover $1715.47 | — |
| 2026-08-20 09:30 ET | **BUY** | `IOND` | 26 | $65.60 | $2.07 | — | $206.77 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+3.7; leftover $1715.47 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $206.77 | ▲ close $13,801.36 vs 09:30 $13,723.72 (session +101.64) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $206.77 | ▼ 09:30 equity $13,767.60 vs yday $13,801.36 (-33.76) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 1 | $43.08 | $0.43 | — | $163.25 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-4.9; leftover $68.92 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 29 | $2.30 | $0.75 | — | $95.80 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-3.0; leftover $68.92 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.80 | ▼ close $13,656.64 vs 09:30 $13,767.60 (session -109.77) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.80 | ▼ 09:30 equity $13,497.90 vs yday $13,656.64 (-158.74) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.80 | ▲ close $13,620.12 vs 09:30 $13,497.90 (session +122.22) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.80 | ▼ 09:30 equity $13,511.38 vs yday $13,620.12 (-108.74) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 50 | $34.72 | $2.16 | $+29.20 | $1,829.64 | ▲ +29.20 after sell → book $13,509.22; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 76 | $21.85 | $2.24 | $-49.30 | $3,487.99 | ▼ -49.30 after sell → book $13,506.97; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BABA` | 13 | $117.94 | $2.05 | $-75.97 | $5,019.16 | ▼ -75.97 after sell → book $13,504.92; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BULL` | 172 | $8.46 | $2.55 | $-259.61 | $6,471.73 | ▼ -259.61 after sell → book $13,502.37; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `COTY` | 672 | $2.75 | $8.80 | $+116.94 | $8,310.94 | ▲ +116.94 after sell → book $13,493.58; vs 09:30 mark -8.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DQ` | 118 | $13.77 | $2.38 | $-83.78 | $9,933.42 | ▼ -83.78 after sell → book $13,491.20; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `FUTU` | 14 | $118.00 | $2.06 | $+0.81 | $11,583.37 | ▲ +0.81 after sell → book $13,489.15; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IOND` | 26 | $69.00 | $2.09 | $+84.24 | $13,375.28 | ▲ +84.24 after sell → book $13,487.06; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 2934 | $4.54 | $37.85 | — | $2.40 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-14.6; leftover $13375.28 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.40 | ▼ close $10,148.56 vs 09:30 $13,511.38 (session -3,300.65) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.40 | ▼ 09:30 equity $10,031.86 vs yday $10,148.56 (-116.70) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `BKE` | 1 | $44.39 | $0.47 | $+0.41 | $46.32 | ▲ +0.41 after sell → book $10,031.39; vs 09:30 mark -0.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 29 | $2.35 | $0.79 | $-0.09 | $113.68 | ▼ -0.09 after sell → book $10,030.60; vs 09:30 mark -0.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 2 | $5.21 | $0.11 | — | $103.15 | — | combo gate; gate earn_react=True,last_green=True; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $14.21 | — |
| 2026-08-26 09:30 ET | **BUY** | `LI` | 1 | $12.14 | $0.12 | — | $90.89 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+1.2; leftover $14.21 | — |
| 2026-08-26 09:30 ET | **BUY** | `QFIN` | 1 | $9.76 | $0.10 | — | $81.03 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-4.4; leftover $14.21 | — |
| 2026-08-26 09:30 ET | **BUY** | `QMLS` | 2 | $6.47 | $0.14 | — | $67.95 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-7.0; leftover $14.21 | — |
| 2026-08-26 09:30 ET | **BUY** | `SFL` | 1 | $12.35 | $0.13 | — | $55.47 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-1.7; leftover $14.21 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.47 | ▼ close $9,412.89 vs 09:30 $10,031.86 (session -617.11) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.47 | ▼ 09:30 equity $9,384.35 vs yday $9,412.89 (-28.54) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.47 | ▲ close $10,087.83 vs 09:30 $9,384.35 (session +703.48) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.47 | ▼ 09:30 equity $10,028.87 vs yday $10,087.83 (-58.96) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 2934 | $3.38 | $38.41 | $-3494.37 | $9,933.98 | ▼ -3,494.37 after sell → book $9,990.46; vs 09:30 mark -38.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $8,887.34 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+7.8; leftover $1241.75 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 11 | $103.89 | $2.02 | — | $7,742.52 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.5; leftover $1241.75 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 27 | $44.40 | $2.07 | — | $6,541.65 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.4; leftover $1241.75 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 50 | $24.69 | $2.14 | — | $5,305.01 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+5.8; leftover $1241.75 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 148 | $8.35 | $2.43 | — | $4,066.78 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+5.1; leftover $1241.75 | — |
| 2026-08-28 09:30 ET | **BUY** | `PD` | 94 | $13.09 | $2.27 | — | $2,834.05 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+4.2; leftover $1241.75 | — |
| 2026-08-28 09:30 ET | **BUY** | `RBRK` | 12 | $98.95 | $2.03 | — | $1,644.62 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+9.7; leftover $1241.75 | — |
| 2026-08-28 09:30 ET | **BUY** | `S` | 57 | $21.49 | $2.16 | — | $417.53 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+8.5; leftover $1241.75 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $417.53 | ▼ close $9,879.98 vs 09:30 $10,028.87 (session -93.35) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $417.53 | ▼ 09:30 equity $9,817.28 vs yday $9,879.98 (-62.70) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 2 | $5.00 | $0.13 | $-0.66 | $427.40 | ▼ -0.66 after sell → book $9,817.15; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `LI` | 1 | $12.28 | $0.15 | $-0.13 | $439.54 | ▼ -0.13 after sell → book $9,817.01; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `QFIN` | 1 | $8.70 | $0.11 | $-1.27 | $448.13 | ▼ -1.27 after sell → book $9,816.90; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `QMLS` | 2 | $5.95 | $0.14 | $-1.32 | $459.88 | ▼ -1.32 after sell → book $9,816.75; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `SFL` | 1 | $12.51 | $0.15 | $-0.11 | $472.25 | ▼ -0.11 after sell → book $9,816.61; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $472.25 | ▲ close $9,831.44 vs 09:30 $9,817.28 (session +14.83) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $472.25 | ▼ 09:30 equity $9,753.42 vs yday $9,831.44 (-78.02) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $472.25 | ▼ close $9,603.73 vs 09:30 $9,753.42 (session -149.69) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $472.25 | ▼ 09:30 equity $9,587.90 vs yday $9,603.73 (-15.83) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 4 | $246.70 | $2.02 | $-61.86 | $1,457.02 | ▼ -61.86 after sell → book $9,585.87; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 11 | $92.00 | $2.04 | $-134.86 | $2,466.98 | ▼ -134.86 after sell → book $9,583.83; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 27 | $44.17 | $2.09 | $-10.37 | $3,657.48 | ▼ -10.37 after sell → book $9,581.74; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 50 | $21.97 | $2.16 | $-140.30 | $4,753.82 | ▼ -140.30 after sell → book $9,579.58; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 148 | $8.58 | $2.47 | $+29.14 | $6,021.19 | ▲ +29.14 after sell → book $9,577.11; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `PD` | 94 | $14.00 | $2.30 | $+80.97 | $7,334.89 | ▲ +80.97 after sell → book $9,574.81; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `RBRK` | 12 | $89.00 | $2.05 | $-123.47 | $8,400.85 | ▼ -123.47 after sell → book $9,572.77; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `S` | 57 | $20.56 | $2.18 | $-57.35 | $9,570.59 | ▼ -57.35 after sell → book $9,570.59; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,570.59 | ▲ close $9,570.59 vs 09:30 $9,587.90 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,570.59 | ▲ 09:30 equity $9,570.59 vs yday $9,570.59 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 148 | $10.74 | $2.43 | — | $7,977.89 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+8.5; leftover $1595.10 | — |
| 2026-09-03 09:30 ET | **BUY** | `MOMO` | 290 | $5.50 | $3.74 | — | $6,379.15 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-4.8; leftover $1595.10 | — |
| 2026-09-03 09:30 ET | **BUY** | `PHR` | 144 | $11.02 | $2.42 | — | $4,789.85 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+2.2; leftover $1595.10 | — |
| 2026-09-03 09:30 ET | **BUY** | `TTC` | 16 | $99.00 | $2.04 | — | $3,203.81 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=-1.2; leftover $1595.10 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 20 | $76.86 | $2.05 | — | $1,664.56 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-6.6; leftover $1595.10 | — |
| 2026-09-03 09:30 ET | **BUY** | `WOOF` | 511 | $3.12 | $6.59 | — | $63.65 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-5.1; leftover $1595.10 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.65 | ▼ close $8,992.69 vs 09:30 $9,570.59 (session -558.62) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.65 | ▼ 09:30 equity $8,991.00 vs yday $8,992.69 (-1.69) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 3 | $3.62 | $0.12 | — | $52.69 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-3.1; leftover $12.73 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.69 | ▲ close $9,129.25 vs 09:30 $8,991.00 (session +138.36) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.69 | ▼ 09:30 equity $8,943.74 vs yday $9,129.25 (-185.51) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.69 | ▲ close $9,030.74 vs 09:30 $8,943.74 (session +87.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.69 | ▼ 09:30 equity $8,991.68 vs yday $9,030.74 (-39.06) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 148 | $10.51 | $2.47 | $-39.68 | $1,605.70 | ▼ -39.68 after sell → book $8,989.21; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MOMO` | 290 | $5.26 | $3.80 | $-77.14 | $3,127.29 | ▼ -77.14 after sell → book $8,985.40; vs 09:30 mark -3.81 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `PHR` | 144 | $10.24 | $2.46 | $-117.20 | $4,599.40 | ▼ -117.20 after sell → book $8,982.95; vs 09:30 mark -2.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `TTC` | 16 | $94.08 | $2.06 | $-82.82 | $6,102.62 | ▼ -82.82 after sell → book $8,980.89; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSXY` | 20 | $77.16 | $2.07 | $+1.88 | $7,643.74 | ▲ +1.88 after sell → book $8,978.81; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `WOOF` | 511 | $2.59 | $6.69 | $-284.11 | $8,960.55 | ▼ -284.11 after sell → book $8,972.13; vs 09:30 mark -6.68 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,960.55 | ▼ close $8,971.89 vs 09:30 $8,991.68 (session -0.24) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,960.55 | ▼ 09:30 equity $8,971.83 vs yday $8,971.89 (-0.06) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 3 | $3.76 | $0.14 | $+0.18 | $8,971.68 | ▲ +0.18 after sell → book $8,971.68; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,971.68 | ▲ close $8,971.68 vs 09:30 $8,971.83 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,971.68 | ▲ 09:30 equity $8,971.68 vs yday $8,971.68 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 160 | $56.02 | $2.47 | — | $6.01 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.2; leftover $8971.68 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.01 | ▲ close $9,364.41 vs 09:30 $8,971.68 (session +395.20) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.01 | ▲ 09:30 equity $9,495.61 vs yday $9,364.41 (+131.20) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.01 | ▲ close $9,751.61 vs 09:30 $9,495.61 (session +256.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.01 | ▼ 09:30 equity $9,729.21 vs yday $9,751.61 (-22.40) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.01 | ▲ close $9,966.01 vs 09:30 $9,729.21 (session +236.80) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.01 | ▼ 09:30 equity $9,914.81 vs yday $9,966.01 (-51.20) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `KR` | 160 | $61.93 | $2.58 | $+940.55 | $9,912.24 | ▲ +940.55 after sell → book $9,912.24; vs 09:30 mark -2.57 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,912.24 | ▲ close $9,912.24 vs 09:30 $9,914.81 (session +0.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,912.24 | ▲ 09:30 equity $9,912.24 vs yday $9,912.24 (-0.00) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,912.24 | ▲ close $9,912.24 vs 09:30 $9,912.24 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,912.24 | ▲ 09:30 equity $9,912.24 vs yday $9,912.24 (-0.00) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,912.24 | ▲ close $9,912.24 vs 09:30 $9,912.24 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,912.24 | ▲ 09:30 equity $9,912.24 vs yday $9,912.24 (-0.00) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,912.24 | ▲ close $9,912.24 vs 09:30 $9,912.24 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,912.24 | ▲ 09:30 equity $9,912.24 vs yday $9,912.24 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,912.24 | ▲ close $9,912.24 vs 09:30 $9,912.24 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,912.24 | ▲ 09:30 equity $9,912.24 vs yday $9,912.24 (-0.00) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 50 | $196.78 | $2.14 | — | $71.10 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $9912.24 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.10 | ▼ close $9,669.60 vs 09:30 $9,912.24 (session -240.50) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.10 | ▲ 09:30 equity $9,684.10 vs yday $9,669.60 (+14.50) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.10 | ▲ close $9,955.10 vs 09:30 $9,684.10 (session +271.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.61 | ▲ 09:30 equity $9,495.30 vs yday $9,495.30 (+0.00) | 09:30 open · cash $61.61 (unchanged overnight, no fees) · equity $9,495.30 vs prior close $9,495.30 (+0.00) · 1 name(s) re-marked at the open (per-name table). THO×133 yday $70.93 → 09:30 $70.93 +0.00 | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.61 | ▲ close $9,495.30 vs 09:30 $9,495.30 (session +0.00) | 16:00 close · cash $61.61 · equity $9,495.30 vs 09:30 $9,495.30 (+0.00; session marks +0.00) · 1 name(s) marked open→close (per-name table). THO×133 09:30 $70.93 → close $70.93 +0.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `NMAX` | cash | leftover split 0.29 < 1 share @ 9.89 |
| 2026-08-14 | `AIRJ` | cash | leftover split 0.29 < 1 share @ 5.51 |
| 2026-08-14 | `BRUN` | cash | leftover split 0.29 < 1 share @ 26.25 |
| 2026-08-14 | `BZAI` | cash | leftover split 0.29 < 1 share @ 0.77 |
| 2026-08-14 | `DLO` | cash | leftover split 0.29 < 1 share @ 15.28 |
| 2026-08-14 | `ENHA` | cash | leftover split 0.29 < 1 share @ 2.31 |
| 2026-08-14 | `FIRY` | cash | leftover split 0.29 < 1 share @ 9.74 |
| 2026-08-14 | `GEMI` | cash | leftover split 0.29 < 1 share @ 3.90 |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SQM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `YMM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `ATAT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ATHM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BULL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `COTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `FUTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IOND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BJ` | cash | leftover split 68.92 < 1 share @ 93.98 |
| 2026-08-24 | `ATAT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ATHM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BULL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `COTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `FUTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IOND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BKE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `PSEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `BKE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `PSEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `SHMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NCNO` | cash | leftover split 14.21 < 1 share @ 19.33 |
| 2026-08-26 | `SJM` | cash | leftover split 14.21 < 1 share @ 134.80 |
| 2026-08-26 | `SMTC` | cash | leftover split 14.21 < 1 share @ 130.90 |
| 2026-08-27 | `SHMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `TIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `LI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `QFIN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `QMLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `SFL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BBY` | cash | leftover split 13.87 < 1 share @ 80.60 |
| 2026-08-27 | `HQY` | cash | leftover split 13.87 < 1 share @ 97.16 |
| 2026-08-27 | `RY` | cash | leftover split 13.87 < 1 share @ 206.82 |
| 2026-08-27 | `TD` | cash | leftover split 13.87 < 1 share @ 120.17 |
| 2026-08-28 | `TIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `LI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `QFIN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `QMLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `SFL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ESTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RBRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `S` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ESTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `PD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `RBRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `S` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MOMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PHR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `TTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSXY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `WOOF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `GWRE` | cash | leftover split 12.73 < 1 share @ 167.55 |
| 2026-09-04 | `IOT` | cash | leftover split 12.73 < 1 share @ 44.90 |
| 2026-09-04 | `LULU` | cash | leftover split 12.73 < 1 share @ 98.15 |
| 2026-09-04 | `MAMA` | cash | leftover split 12.73 < 1 share @ 15.70 |
| 2026-09-08 | `AI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MOMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PHR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `TTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSXY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `WOOF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DOMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `DOMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `KR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `KR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-24 | `CTAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CTAS` | 50 | 2026-09-23 @ $196.78 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $9912.24 |
