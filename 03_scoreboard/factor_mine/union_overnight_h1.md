# Factor mine action — `union_overnight_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ overnight, no 🚨

Cash book **-21.79%** ($7,821) · signal-only (no cash/fees) was -26.59%. Starts YES **0/30**. Fills 126 · skips 73 · realized $-2767.20.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the prior Finviz calendar said this name reports AMC today or BMO next session (the print is still ahead; we buy today 09:30 to own the next open).
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
- **Gate** `overnight=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,232.75.

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
| 2026-08-14 09:30 ET | **BUY** | `DUOT` | 353 | $9.43 | $4.55 | — | $6,666.66 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ⚪; ret5=+7.7; leftover $3333.33 | — |
| 2026-08-14 09:30 ET | **BUY** | `NUAI` | 658 | $5.06 | $8.49 | — | $3,328.69 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ⚪; ret5=-3.2; leftover $3333.33 | — |
| 2026-08-14 09:30 ET | **BUY** | `SIDU` | 1298 | $2.55 | $16.74 | — | $2.04 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+21.5; leftover $3333.33 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.04 | ▼ close $9,928.73 vs 09:30 $10,000.00 (session -41.48) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.04 | ▲ 09:30 equity $10,192.39 vs yday $9,928.73 (+263.66) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `DUOT` | 353 | $10.35 | $4.64 | $+315.56 | $3,650.95 | ▲ +315.56 after sell → book $10,187.75; vs 09:30 mark -4.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NUAI` | 658 | $5.20 | $8.62 | $+75.01 | $7,063.93 | ▲ +75.01 after sell → book $10,179.13; vs 09:30 mark -8.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SIDU` | 1298 | $2.40 | $16.98 | $-228.43 | $10,162.14 | ▼ -228.43 after sell → book $10,162.14; vs 09:30 mark -16.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `HSAI` | 92 | $18.32 | $2.27 | — | $8,474.44 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-5.3; leftover $1693.69 | — |
| 2026-08-17 09:30 ET | **BUY** | `IQ` | 1254 | $1.35 | $16.18 | — | $6,765.36 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ⚪; ret5=+1.5; leftover $1693.69 | — |
| 2026-08-17 09:30 ET | **BUY** | `KLAR` | 81 | $20.67 | $2.23 | — | $5,088.86 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+4.5; leftover $1693.69 | — |
| 2026-08-17 09:30 ET | **BUY** | `PONY` | 207 | $8.16 | $2.67 | — | $3,397.07 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ⚪; ret5=-0.1; leftover $1693.69 | — |
| 2026-08-17 09:30 ET | **BUY** | `VNET` | 218 | $7.75 | $2.81 | — | $1,704.75 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+3.7; leftover $1693.69 | — |
| 2026-08-17 09:30 ET | **BUY** | `XP` | 106 | $15.93 | $2.31 | — | $13.87 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ⚪; ret5=-2.6; leftover $1693.69 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.87 | ▼ close $9,967.06 vs 09:30 $10,192.39 (session -166.62) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.87 | ▼ 09:30 equity $9,074.20 vs yday $9,967.06 (-892.86) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `HSAI` | 92 | $15.77 | $2.29 | $-239.62 | $1,461.95 | ▼ -239.62 after sell → book $9,071.90; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `IQ` | 1254 | $1.27 | $16.40 | $-132.89 | $3,038.14 | ▼ -132.89 after sell → book $9,055.51; vs 09:30 mark -16.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `KLAR` | 81 | $15.66 | $2.26 | $-410.30 | $4,304.34 | ▼ -410.30 after sell → book $9,053.25; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `PONY` | 207 | $7.53 | $2.72 | $-135.80 | $5,860.33 | ▼ -135.80 after sell → book $9,050.53; vs 09:30 mark -2.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VNET` | 218 | $7.00 | $2.86 | $-169.17 | $7,383.47 | ▼ -169.17 after sell → book $9,047.67; vs 09:30 mark -2.86 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `XP` | 106 | $15.70 | $2.34 | $-29.03 | $9,045.33 | ▼ -29.03 after sell → book $9,045.33; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,045.33 | ▲ close $9,045.33 vs 09:30 $9,074.20 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,045.33 | ▲ 09:30 equity $9,045.33 vs yday $9,045.33 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,045.33 | ▲ close $9,045.33 vs 09:30 $9,045.33 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,045.33 | ▲ 09:30 equity $9,045.33 vs yday $9,045.33 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BEKE` | 106 | $17.04 | $2.31 | — | $7,236.79 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-0.2; leftover $1809.07 | — |
| 2026-08-20 09:30 ET | **BUY** | `BJ` | 20 | $88.91 | $2.05 | — | $5,456.54 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-1.0; leftover $1809.07 | — |
| 2026-08-20 09:30 ET | **BUY** | `BKE` | 42 | $42.60 | $2.12 | — | $3,665.22 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-4.6; leftover $1809.07 | — |
| 2026-08-20 09:30 ET | **BUY** | `FLO` | 243 | $7.43 | $3.13 | — | $1,856.59 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+4.0; leftover $1809.07 | — |
| 2026-08-20 09:30 ET | **BUY** | `ROST` | 7 | $229.55 | $2.01 | — | $247.73 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight,overnight_mega; 🔵; ret5=-5.5; leftover $1809.07 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $247.73 | ▼ close $8,993.78 vs 09:30 $9,045.33 (session -39.93) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $247.73 | ▲ 09:30 equity $9,221.45 vs yday $8,993.78 (+227.67) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BEKE` | 106 | $17.93 | $2.34 | $+90.22 | $2,146.50 | ▲ +90.22 after sell → book $9,219.11; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BJ` | 20 | $93.98 | $2.08 | $+97.28 | $4,024.03 | ▲ +97.28 after sell → book $9,217.04; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BKE` | 42 | $43.08 | $2.14 | $+15.90 | $5,831.25 | ▲ +15.90 after sell → book $9,214.90; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `FLO` | 243 | $6.90 | $3.19 | $-135.11 | $7,504.76 | ▼ -135.11 after sell → book $9,211.71; vs 09:30 mark -3.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ROST` | 7 | $243.85 | $2.03 | $+96.05 | $9,209.67 | ▲ +96.05 after sell → book $9,209.67; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `PDD` | 51 | $90.03 | $2.14 | — | $4,616.00 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight,overnight_mega; 🔵; ret5=+6.4; leftover $4604.84 | — |
| 2026-08-21 09:30 ET | **BUY** | `XPEV` | 374 | $12.29 | $4.82 | — | $14.72 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+1.9; leftover $4604.84 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.72 | ▼ close $9,081.16 vs 09:30 $9,221.45 (session -121.55) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.72 | ▼ 09:30 equity $9,077.59 vs yday $9,081.16 (-3.57) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `PDD` | 51 | $90.95 | $2.19 | $+42.59 | $4,650.98 | ▲ +42.59 after sell → book $9,075.40; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XPEV` | 374 | $11.83 | $4.92 | $-181.79 | $9,070.47 | ▼ -181.79 after sell → book $9,070.47; vs 09:30 mark -4.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,070.47 | ▲ close $9,070.47 vs 09:30 $9,077.59 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,070.47 | ▲ 09:30 equity $9,070.47 vs yday $9,070.47 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `ANF` | 10 | $112.17 | $2.02 | — | $7,946.75 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ⚪; ret5=+6.8; leftover $1133.81 | — |
| 2026-08-25 09:30 ET | **BUY** | `BBWI` | 59 | $19.16 | $2.17 | — | $6,814.15 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+3.0; leftover $1133.81 | — |
| 2026-08-25 09:30 ET | **BUY** | `BOX` | 34 | $33.33 | $2.09 | — | $5,678.84 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+3.7; leftover $1133.81 | — |
| 2026-08-25 09:30 ET | **BUY** | `DCI` | 12 | $93.64 | $2.03 | — | $4,553.13 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-2.4; leftover $1133.81 | — |
| 2026-08-25 09:30 ET | **BUY** | `DY` | 2 | $390.22 | $2.00 | — | $3,770.69 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-12.0; leftover $1133.81 | — |
| 2026-08-25 09:30 ET | **BUY** | `FSCO` | 222 | $5.10 | $2.86 | — | $2,635.63 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+0.2; leftover $1133.81 | — |
| 2026-08-25 09:30 ET | **BUY** | `HEI` | 3 | $357.15 | $2.00 | — | $1,562.18 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ⚪; ret5=-5.0; leftover $1133.81 | — |
| 2026-08-25 09:30 ET | **BUY** | `INTU` | 3 | $364.35 | $2.00 | — | $467.13 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $1133.81 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $467.13 | ▼ close $8,789.14 vs 09:30 $9,070.47 (session -264.17) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $467.13 | ▲ 09:30 equity $9,027.92 vs yday $8,789.14 (+238.78) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ANF` | 10 | $131.37 | $2.04 | $+187.94 | $1,778.79 | ▲ +187.94 after sell → book $9,025.88; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BBWI` | 59 | $18.26 | $2.19 | $-57.45 | $2,853.94 | ▼ -57.45 after sell → book $9,023.69; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BOX` | 34 | $34.30 | $2.11 | $+28.78 | $4,018.03 | ▲ +28.78 after sell → book $9,021.58; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DCI` | 12 | $95.13 | $2.05 | $+13.81 | $5,157.55 | ▲ +13.81 after sell → book $9,019.54; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DY` | 2 | $326.91 | $2.02 | $-130.63 | $5,809.35 | ▼ -130.63 after sell → book $9,017.52; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FSCO` | 222 | $5.08 | $2.91 | $-10.21 | $6,934.20 | ▼ -10.21 after sell → book $9,014.61; vs 09:30 mark -2.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `HEI` | 3 | $370.00 | $2.02 | $+34.53 | $8,042.18 | ▲ +34.53 after sell → book $9,012.59; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `INTU` | 3 | $323.47 | $2.02 | $-126.66 | $9,010.57 | ▼ -126.66 after sell → book $9,010.57; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `STDN` | 80 | $13.95 | $2.23 | — | $7,892.34 | — | union ∩ overnight, no 🚨; gate overnight=True; list ohlc_hot,overnight; 🔵; ret5=+14.3; leftover $1126.32 | — |
| 2026-08-26 09:30 ET | **BUY** | `A` | 7 | $152.45 | $2.01 | — | $6,823.18 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+4.3; leftover $1126.32 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBY` | 13 | $85.19 | $2.03 | — | $5,713.68 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-2.3; leftover $1126.32 | — |
| 2026-08-26 09:30 ET | **BUY** | `BILI` | 69 | $16.22 | $2.20 | — | $4,592.30 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-2.5; leftover $1126.32 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 9 | $118.50 | $2.02 | — | $3,523.79 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $1126.32 | — |
| 2026-08-26 09:30 ET | **BUY** | `CMBT` | 62 | $17.91 | $2.18 | — | $2,411.19 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+3.9; leftover $1126.32 | — |
| 2026-08-26 09:30 ET | **BUY** | `CRM` | 5 | $199.94 | $2.00 | — | $1,409.49 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight,overnight_mega; ret5=+2.1; leftover $1126.32 | — |
| 2026-08-26 09:30 ET | **BUY** | `CRWD` | 6 | $182.75 | $2.01 | — | $310.98 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight,overnight_mega; 🔵; ret5=-12.9; leftover $1126.32 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $310.98 | ▲ close $9,064.89 vs 09:30 $9,027.92 (session +70.99) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $310.98 | ▲ 09:30 equity $9,268.89 vs yday $9,064.89 (+204.00) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `STDN` | 80 | $13.84 | $2.25 | $-13.28 | $1,415.92 | ▼ -13.28 after sell → book $9,266.64; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `A` | 7 | $159.35 | $2.03 | $+44.26 | $2,529.34 | ▲ +44.26 after sell → book $9,264.60; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BBY` | 13 | $80.60 | $2.05 | $-63.75 | $3,575.09 | ▼ -63.75 after sell → book $9,262.56; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BILI` | 69 | $16.18 | $2.22 | $-7.18 | $4,689.30 | ▼ -7.18 after sell → book $9,260.34; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CM` | 9 | $118.77 | $2.04 | $-1.62 | $5,756.19 | ▼ -1.62 after sell → book $9,258.30; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CMBT` | 62 | $17.78 | $2.20 | $-12.43 | $6,856.35 | ▼ -12.43 after sell → book $9,256.10; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRM` | 5 | $230.05 | $2.02 | $+146.52 | $8,004.58 | ▲ +146.52 after sell → book $9,254.08; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRWD` | 6 | $208.25 | $2.03 | $+148.96 | $9,252.05 | ▲ +148.96 after sell → book $9,252.05; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `GAP` | 55 | $20.75 | $2.15 | — | $8,108.65 | — | union ∩ overnight, no 🚨; gate overnight=True; list ohlc_hot,overnight; ret5=+5.2; leftover $1156.51 | — |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 4 | $261.47 | $2.00 | — | $7,060.76 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight,overnight_mega; 🔵; ret5=+1.4; leftover $1156.51 | — |
| 2026-08-27 09:30 ET | **BUY** | `AFRM` | 15 | $76.90 | $2.04 | — | $5,905.23 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-1.1; leftover $1156.51 | — |
| 2026-08-27 09:30 ET | **BUY** | `BBAR` | 77 | $14.96 | $2.22 | — | $4,751.09 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+3.0; leftover $1156.51 | — |
| 2026-08-27 09:30 ET | **BUY** | `CHA` | 109 | $10.54 | $2.32 | — | $3,599.91 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+2.5; leftover $1156.51 | — |
| 2026-08-27 09:30 ET | **BUY** | `ESTC` | 13 | $82.65 | $2.03 | — | $2,523.43 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-9.3; leftover $1156.51 | — |
| 2026-08-27 09:30 ET | **BUY** | `HAFN` | 146 | $7.91 | $2.43 | — | $1,366.14 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-1.8; leftover $1156.51 | — |
| 2026-08-27 09:30 ET | **BUY** | `MNSO` | 106 | $10.89 | $2.31 | — | $209.50 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+2.7; leftover $1156.51 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $209.50 | ▲ close $9,294.79 vs 09:30 $9,268.89 (session +60.23) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.50 | ▲ 09:30 equity $9,855.81 vs yday $9,294.79 (+561.02) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `GAP` | 55 | $24.69 | $2.18 | $+212.37 | $1,565.27 | ▲ +212.37 after sell → book $9,853.63; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ADSK` | 4 | $261.16 | $2.02 | $-5.26 | $2,607.89 | ▼ -5.26 after sell → book $9,851.61; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AFRM` | 15 | $86.00 | $2.06 | $+132.41 | $3,895.83 | ▲ +132.41 after sell → book $9,849.55; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BBAR` | 77 | $15.01 | $2.24 | $-0.61 | $5,049.36 | ▼ -0.61 after sell → book $9,847.31; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CHA` | 109 | $10.30 | $2.35 | $-30.82 | $6,169.71 | ▼ -30.82 after sell → book $9,844.96; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ESTC` | 13 | $103.89 | $2.05 | $+272.04 | $7,518.23 | ▲ +272.04 after sell → book $9,842.91; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `HAFN` | 146 | $8.35 | $2.46 | $+59.35 | $8,734.87 | ▲ +59.35 after sell → book $9,840.45; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MNSO` | 106 | $10.43 | $2.34 | $-53.40 | $9,838.12 | ▼ -53.40 after sell → book $9,838.12; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `LX` | 4240 | $1.16 | $54.70 | — | $4,865.02 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-13.8; leftover $4919.06 | — |
| 2026-08-28 09:30 ET | **BUY** | `SAIC` | 37 | $129.46 | $2.10 | — | $72.90 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+2.1; leftover $4919.06 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.90 | ▼ close $9,736.62 vs 09:30 $9,855.81 (session -44.70) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.90 | ▼ 09:30 equity $9,549.73 vs yday $9,736.62 (-186.89) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `LX` | 4240 | $1.01 | $55.43 | $-746.13 | $4,299.86 | ▼ -746.13 after sell → book $9,494.29; vs 09:30 mark -55.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SAIC` | 37 | $140.39 | $2.15 | $+400.16 | $9,492.14 | ▲ +400.16 after sell → book $9,492.14; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,492.14 | ▲ close $9,492.14 vs 09:30 $9,549.73 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,492.14 | ▲ 09:30 equity $9,492.14 vs yday $9,492.14 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,492.14 | ▲ close $9,492.14 vs 09:30 $9,492.14 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,492.14 | ▲ 09:30 equity $9,492.14 vs yday $9,492.14 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,492.14 | ▲ close $9,492.14 vs 09:30 $9,492.14 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,492.14 | ▲ 09:30 equity $9,492.14 vs yday $9,492.14 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AMBA` | 17 | $66.61 | $2.04 | — | $8,357.73 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-3.6; leftover $1186.52 | — |
| 2026-09-03 09:30 ET | **BUY** | `ASAN` | 116 | $10.16 | $2.34 | — | $7,176.83 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+3.8; leftover $1186.52 | — |
| 2026-09-03 09:30 ET | **BUY** | `DOCU` | 17 | $67.06 | $2.04 | — | $6,034.77 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+10.2; leftover $1186.52 | — |
| 2026-09-03 09:30 ET | **BUY** | `DOMO` | 313 | $3.78 | $4.04 | — | $4,847.59 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-2.4; leftover $1186.52 | — |
| 2026-09-03 09:30 ET | **BUY** | `GWRE` | 5 | $198.00 | $2.00 | — | $3,855.59 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+0.9; leftover $1186.52 | — |
| 2026-09-03 09:30 ET | **BUY** | `IOT` | 31 | $37.69 | $2.08 | — | $2,685.12 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-7.7; leftover $1186.52 | — |
| 2026-09-03 09:30 ET | **BUY** | `LULU` | 9 | $121.15 | $2.02 | — | $1,592.75 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+3.2; leftover $1186.52 | — |
| 2026-09-03 09:30 ET | **BUY** | `MAMA` | 75 | $15.62 | $2.21 | — | $419.03 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-6.7; leftover $1186.52 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $419.03 | ▲ close $9,483.17 vs 09:30 $9,492.14 (session +9.81) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $419.03 | ▼ 09:30 equity $9,093.77 vs yday $9,483.17 (-389.40) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AMBA` | 17 | $63.18 | $2.06 | $-62.41 | $1,491.03 | ▼ -62.41 after sell → book $9,091.71; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ASAN` | 116 | $8.74 | $2.37 | $-169.43 | $2,502.51 | ▼ -169.43 after sell → book $9,089.34; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DOCU` | 17 | $68.52 | $2.06 | $+20.72 | $3,665.28 | ▲ +20.72 after sell → book $9,087.28; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DOMO` | 313 | $3.62 | $4.10 | $-59.78 | $4,792.68 | ▼ -59.78 after sell → book $9,083.18; vs 09:30 mark -4.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `GWRE` | 5 | $167.55 | $2.02 | $-156.28 | $5,628.41 | ▼ -156.28 after sell → book $9,081.16; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `IOT` | 31 | $44.90 | $2.10 | $+219.32 | $7,018.20 | ▲ +219.32 after sell → book $9,079.05; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `LULU` | 9 | $98.15 | $2.04 | $-211.05 | $7,899.51 | ▼ -211.05 after sell → book $9,077.01; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MAMA` | 75 | $15.70 | $2.24 | $+1.55 | $9,074.78 | ▲ +1.55 after sell → book $9,074.78; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ABM` | 96 | $46.79 | $2.28 | — | $4,580.66 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+0.2; leftover $4537.39 | — |
| 2026-09-04 09:30 ET | **BUY** | `UNFI` | 103 | $43.80 | $2.30 | — | $66.96 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-7.7; leftover $4537.39 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $66.96 | ▲ close $9,108.55 vs 09:30 $9,093.77 (session +38.35) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $66.96 | ▲ 09:30 equity $9,121.35 vs yday $9,108.55 (+12.80) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ABM` | 96 | $45.81 | $2.33 | $-98.69 | $4,462.39 | ▼ -98.69 after sell → book $9,119.02; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `UNFI` | 103 | $45.21 | $2.35 | $+140.58 | $9,116.67 | ▲ +140.58 after sell → book $9,116.67; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,116.67 | ▲ close $9,116.67 vs 09:30 $9,121.35 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,116.67 | ▲ 09:30 equity $9,116.67 vs yday $9,116.67 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,116.67 | ▲ close $9,116.67 vs 09:30 $9,116.67 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,116.67 | ▲ 09:30 equity $9,116.67 vs yday $9,116.67 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,116.67 | ▲ close $9,116.67 vs 09:30 $9,116.67 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,116.67 | ▲ 09:30 equity $9,116.67 vs yday $9,116.67 (-0.00) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,116.67 | ▲ close $9,116.67 vs 09:30 $9,116.67 (session +0.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,116.67 | ▲ 09:30 equity $9,116.67 vs yday $9,116.67 (-0.00) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,116.67 | ▲ close $9,116.67 vs 09:30 $9,116.67 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,116.67 | ▲ 09:30 equity $9,116.67 vs yday $9,116.67 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,116.67 | ▲ close $9,116.67 vs 09:30 $9,116.67 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,116.67 | ▲ 09:30 equity $9,116.67 vs yday $9,116.67 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `ALMU` | 331 | $13.75 | $4.27 | — | $4,561.15 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-1.4; leftover $4558.33 | — |
| 2026-09-16 09:30 ET | **BUY** | `LEN` | 56 | $80.63 | $2.16 | — | $43.71 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-0.4; leftover $4558.33 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.71 | ▼ close $8,877.20 vs 09:30 $9,116.67 (session -233.04) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.71 | ▼ 09:30 equity $8,290.22 vs yday $8,877.20 (-586.98) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `ALMU` | 331 | $11.21 | $4.35 | $-849.36 | $3,749.86 | ▼ -849.36 after sell → book $8,285.86; vs 09:30 mark -4.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `LEN` | 56 | $81.00 | $2.20 | $+16.36 | $8,283.66 | ▲ +16.36 after sell → book $8,283.66; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,283.66 | ▲ close $8,283.66 vs 09:30 $8,290.22 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,283.66 | ▲ 09:30 equity $8,283.66 vs yday $8,283.66 (+0.00) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,283.66 | ▲ close $8,283.66 vs 09:30 $8,283.66 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,283.66 | ▲ 09:30 equity $8,283.66 vs yday $8,283.66 (+0.00) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `ABVX` | 26 | $105.72 | $2.07 | — | $5,532.87 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-11.5; leftover $2761.22 | — |
| 2026-09-21 09:30 ET | **BUY** | `MLKN` | 132 | $20.85 | $2.39 | — | $2,778.29 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-2.5; leftover $2761.22 | — |
| 2026-09-21 09:30 ET | **BUY** | `THO` | 40 | $68.39 | $2.11 | — | $40.58 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-7.0; leftover $2761.22 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.58 | ▼ close $8,210.12 vs 09:30 $8,283.66 (session -66.98) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.58 | ▲ 09:30 equity $8,210.12 vs yday $8,210.12 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.58 | ▲ close $8,210.12 vs 09:30 $8,210.12 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.58 | ▼ 09:30 equity $8,061.10 vs yday $8,210.12 (-149.02) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `ABVX` | 26 | $98.30 | $2.10 | $-197.09 | $2,594.28 | ▼ -197.09 after sell → book $8,059.00; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MLKN` | 132 | $19.76 | $2.43 | $-148.69 | $5,200.17 | ▼ -148.69 after sell → book $8,056.57; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `THO` | 40 | $71.41 | $2.14 | $+116.55 | $8,054.43 | ▲ +116.55 after sell → book $8,054.43; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `BB` | 156 | $8.60 | $2.46 | — | $6,710.37 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-0.4; leftover $1342.40 | — |
| 2026-09-23 09:30 ET | **BUY** | `DRI` | 6 | $215.10 | $2.01 | — | $5,417.76 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-0.6; leftover $1342.40 | — |
| 2026-09-23 09:30 ET | **BUY** | `FUL` | 26 | $50.51 | $2.07 | — | $4,102.43 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-2.6; leftover $1342.40 | — |
| 2026-09-23 09:30 ET | **BUY** | `NEOV` | 394 | $3.40 | $5.08 | — | $2,757.75 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-4.8; leftover $1342.40 | — |
| 2026-09-23 09:30 ET | **BUY** | `SFIX` | 448 | $2.99 | $5.78 | — | $1,412.45 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+1.7; leftover $1342.40 | — |
| 2026-09-23 09:30 ET | **BUY** | `SNX` | 4 | $283.46 | $2.00 | — | $276.61 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ⚪; ret5=+2.3; leftover $1342.40 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $276.61 | ▼ close $7,835.39 vs 09:30 $8,061.10 (session -199.64) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $276.61 | ▼ 09:30 equity $7,252.41 vs yday $7,835.39 (-582.98) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `BB` | 156 | $8.42 | $2.49 | $-33.03 | $1,587.63 | ▼ -33.03 after sell → book $7,249.91; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DRI` | 6 | $208.88 | $2.03 | $-41.36 | $2,838.89 | ▼ -41.36 after sell → book $7,247.89; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FUL` | 26 | $49.29 | $2.09 | $-35.88 | $4,118.34 | ▼ -35.88 after sell → book $7,245.80; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `NEOV` | 394 | $2.65 | $5.16 | $-305.74 | $5,157.28 | ▼ -305.74 after sell → book $7,240.64; vs 09:30 mark -5.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SFIX` | 448 | $2.31 | $5.86 | $-316.28 | $6,186.30 | ▼ -316.28 after sell → book $7,234.78; vs 09:30 mark -5.86 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SNX` | 4 | $262.12 | $2.02 | $-89.38 | $7,232.75 | ▼ -89.38 after sell → book $7,232.75; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,232.75 | ▲ close $7,232.75 vs 09:30 $7,252.41 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,820.74 | ▲ 09:30 equity $7,820.74 vs yday $7,820.74 (+0.00) | 09:30 open · cash $7,820.74 · no holdings · equity $7,820.74 vs prior close $7,820.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,820.74 | ▲ close $7,820.74 vs 09:30 $7,820.74 (session +0.00) | 16:00 close · cash $7,820.74 · no lots left · equity $7,820.74. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `DVLT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `EL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JKHY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `LOW` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MRCY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TGT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEG` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ALVO` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ATAT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ATHM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BABA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BILL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BULL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `BNS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BZ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SHMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SLQT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TUYA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `VIPS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `YEXT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MDT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MMED` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NIO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RZLV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SSL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `GTLB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BF-B` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CRDO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DELL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FCEL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PANW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `AI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CHPT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CPB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FIVE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MOMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NTSK` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PHR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BRZE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `KFY` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ODD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUNB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AVAV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `COO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `M` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAVN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `DSGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `KR` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LPTH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `REF` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `RH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `PLAY` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `TCOM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `THO` | no_price | no 09:30 open — carry |
| 2026-09-22 | `CBRL` | no_price | no 09:30 open |
| 2026-09-22 | `GIS` | cash | leftover split 10.14 < 1 share @ 35.96 |
| 2026-09-22 | `KBH` | cash | leftover split 10.14 < 1 share @ 49.39 |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-24 | `COST` | hard_red | hard-red S=-7.66 sit; no new buys |
