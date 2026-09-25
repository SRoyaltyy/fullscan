# Factor mine action — `short_news_or_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · short packet🔴 OR headline🔴

Cash book **-8.14%** ($9,186) · signal-only (no cash/fees) was -1.39%. Starts YES **0/30**. Fills 105 · skips 120 · realized $+434.23.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the morning news packet OR the prior-export headline is red.

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
- **Gate** `news_or_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $26,061.42.

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
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1412 | $1.18 | $18.51 | — | $11,647.65 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1666.67 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 86 | $19.17 | $2.32 | — | $13,293.95 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1666.67 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 131 | $12.70 | $2.46 | — | $14,954.53 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list ohlc_hot; 🔵; ret5=+12.6; leftover $1666.67 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,954.53 | ▲ close $10,010.33 vs 09:30 $10,000.00 (session +33.62) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,954.53 | ▼ 09:30 equity $9,916.79 vs yday $10,010.33 (-93.54) | — | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 862 | $1.15 | $11.30 | — | $15,934.53 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list yday_mover; ⚪; ret5=-12.2; leftover $991.68 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 278 | $3.56 | $3.67 | — | $16,920.54 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list yday_mover; ret5=-15.6; leftover $991.68 | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 31 | $31.70 | $2.13 | — | $17,901.11 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list ohlc_hot; ret5=+17.6; leftover $991.68 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 329 | $3.01 | $4.34 | — | $18,887.07 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list earn_react; ⚪; ret5=-5.3; leftover $991.68 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 145 | $6.80 | $2.49 | — | $19,870.58 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list overnight; ⚪; ret5=+10.4; leftover $991.68 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,870.58 | ▲ close $10,021.64 vs 09:30 $9,916.79 (session +128.77) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,870.58 | ▲ 09:30 equity $10,172.48 vs yday $10,021.64 (+150.84) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,870.58 | ▲ close $10,410.43 vs 09:30 $10,172.48 (session +237.95) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,870.58 | ▼ 09:30 equity $10,378.48 vs yday $10,410.43 (-31.95) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 1412 | $1.07 | $18.21 | $+118.60 | $18,341.53 | ▲ +118.60 after sell → book $10,360.27; vs 09:30 mark -18.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 131 | $11.75 | $2.38 | $+118.95 | $16,799.89 | ▲ +118.95 after sell → book $10,357.88; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,799.89 | ▲ close $10,405.81 vs 09:30 $10,378.48 (session +47.93) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,799.89 | ▼ 09:30 equity $10,348.47 vs yday $10,405.81 (-57.34) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 86 | $18.13 | $2.25 | $+84.87 | $15,238.47 | ▲ +84.87 after sell → book $10,346.22; vs 09:30 mark -2.25 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 862 | $0.96 | $10.89 | $+139.01 | $14,397.47 | ▲ +139.01 after sell → book $10,335.33; vs 09:30 mark -10.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 278 | $4.01 | $3.59 | $-133.75 | $13,277.72 | ▼ -133.75 after sell → book $10,331.75; vs 09:30 mark -3.58 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 31 | $31.87 | $2.08 | $-9.48 | $12,287.66 | ▼ -9.48 after sell → book $10,329.66; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 329 | $2.95 | $4.24 | $+11.16 | $11,312.87 | ▲ +11.16 after sell → book $10,325.42; vs 09:30 mark -4.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 145 | $6.81 | $2.42 | $-6.36 | $10,322.99 | ▼ -6.36 after sell → book $10,322.99; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $10,934.31 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $645.19 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 30 | $21.40 | $2.12 | — | $11,574.19 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list yday_mover; 🔵; ret5=-25.2; leftover $645.19 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 145 | $4.43 | $2.48 | — | $12,214.06 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list yday_mover; 🔵; ret5=-23.1; leftover $645.19 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 54 | $11.81 | $2.19 | — | $12,849.88 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $645.19 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $13,369.55 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list ohlc_hot; 🔵; ret5=+12.2; leftover $645.19 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $13,976.54 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list earn_react; 🔵; ret5=+5.0; leftover $645.19 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 6 | $106.38 | $2.05 | — | $14,612.77 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list earn_react; 🔵; ret5=-1.7; leftover $645.19 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 139 | $4.61 | $2.46 | — | $15,251.10 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list mover_buy; 🔵; ⚪; ret5=+8.5; leftover $645.19 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,251.10 | ▲ close $10,377.08 vs 09:30 $10,348.47 (session +71.52) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,251.10 | ▼ 09:30 equity $10,325.43 vs yday $10,377.08 (-51.65) | — | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 276 | $3.11 | $3.64 | — | $16,105.82 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $860.45 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 6 | $133.11 | $2.05 | — | $16,902.43 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list yday_mover; 🔵; ⚪; ret5=+109.5; leftover $860.45 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 9 | $89.10 | $2.06 | — | $17,702.27 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list ohlc_hot; 🔵; ret5=+15.8; leftover $860.45 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 22 | $38.40 | $2.10 | — | $18,544.97 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list ohlc_hot; 🔵; ret5=+15.8; leftover $860.45 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 41 | $20.90 | $2.16 | — | $19,399.72 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $860.45 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 31 | $27.00 | $2.13 | — | $20,234.59 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list ohlc_hot; ret5=+10.1; leftover $860.45 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,234.59 | ▼ close $10,300.62 vs 09:30 $10,325.43 (session -10.68) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,234.59 | ▲ 09:30 equity $10,362.42 vs yday $10,300.62 (+61.80) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,234.59 | ▲ close $10,435.02 vs 09:30 $10,362.42 (session +72.60) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,234.59 | ▲ 09:30 equity $10,478.44 vs yday $10,435.02 (+43.42) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $212.00 | $2.00 | $-26.68 | $19,596.59 | ▼ -26.68 after sell → book $10,476.44; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 30 | $20.90 | $2.08 | $+10.80 | $18,967.51 | ▲ +10.80 after sell → book $10,474.36; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 145 | $4.42 | $2.42 | $-3.45 | $18,324.19 | ▼ -3.45 after sell → book $10,471.94; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 54 | $11.00 | $2.15 | $+39.67 | $17,728.04 | ▲ +39.67 after sell → book $10,469.79; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 3 | $170.64 | $2.00 | $+5.75 | $17,214.12 | ▲ +5.75 after sell → book $10,467.79; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 13 | $43.63 | $2.03 | $+37.77 | $16,644.90 | ▲ +37.77 after sell → book $10,465.76; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 6 | $105.58 | $2.01 | $+0.75 | $16,009.41 | ▲ +0.75 after sell → book $10,463.75; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 139 | $4.77 | $2.41 | $-27.10 | $15,343.97 | ▼ -27.10 after sell → book $10,461.34; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 95 | $13.62 | $2.34 | — | $16,636.01 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $1307.67 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 23 | $54.51 | $2.11 | — | $17,887.63 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list ohlc_hot; ret5=+15.1; leftover $1307.67 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 7 | $175.01 | $2.06 | — | $19,110.63 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list earn_react; ret5=-7.0; leftover $1307.67 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 3 | $364.35 | $2.05 | — | $20,201.63 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $1307.67 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,201.63 | ▼ close $10,320.01 vs 09:30 $10,478.44 (session -132.76) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,201.63 | ▲ 09:30 equity $10,516.26 vs yday $10,320.01 (+196.25) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 276 | $2.83 | $3.56 | $+70.08 | $19,416.99 | ▲ +70.08 after sell → book $10,512.70; vs 09:30 mark -3.56 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 6 | $154.20 | $2.01 | $-130.60 | $18,489.79 | ▼ -130.60 after sell → book $10,510.70; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 9 | $88.24 | $2.02 | $+3.66 | $17,693.61 | ▲ +3.66 after sell → book $10,508.68; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 22 | $38.41 | $2.06 | $-4.38 | $16,846.53 | ▼ -4.38 after sell → book $10,506.62; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 41 | $20.50 | $2.11 | $+12.13 | $16,003.92 | ▲ +12.13 after sell → book $10,504.51; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 31 | $26.00 | $2.08 | $+26.79 | $15,195.84 | ▲ +26.79 after sell → book $10,502.43; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 4 | $213.94 | $2.05 | — | $16,049.55 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1050.24 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 85 | $12.22 | $2.30 | — | $17,085.95 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list ohlc_hot; 🔵; ret5=+12.4; leftover $1050.24 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 206 | $5.08 | $2.73 | — | $18,129.70 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list ohlc_hot; 🔵; ret5=+17.6; leftover $1050.24 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 7 | $132.64 | $2.06 | — | $19,056.13 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list ohlc_hot; 🔵; ret5=+16.5; leftover $1050.24 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 5 | $199.94 | $2.05 | — | $20,053.78 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list overnight,overnight_mega; ret5=+2.1; leftover $1050.24 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,053.78 | ▼ close $10,320.75 vs 09:30 $10,516.26 (session -170.50) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,053.78 | ▼ 09:30 equity $10,155.58 vs yday $10,320.75 (-165.17) | — | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 22 | $74.54 | $2.12 | — | $21,691.53 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list mover_buy; 🔵; ret5=-0.1; leftover $1692.60 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MU` | 1 | $967.01 | $2.04 | — | $22,656.50 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list mover_buy; 🔵; ret5=+0.1; leftover $1692.60 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 30 | $55.25 | $2.15 | — | $24,311.85 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list mover_buy; 🔵; ret5=+2.1; leftover $1692.60 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,311.85 | ▼ close $10,120.89 vs 09:30 $10,155.58 (session -28.37) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,311.85 | ▲ 09:30 equity $10,134.13 vs yday $10,120.89 (+13.24) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 95 | $13.90 | $2.27 | $-30.74 | $22,989.08 | ▼ -30.74 after sell → book $10,131.85; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 23 | $52.49 | $2.06 | $+42.29 | $21,779.75 | ▲ +42.29 after sell → book $10,129.79; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 7 | $172.76 | $2.01 | $+11.67 | $20,568.42 | ▲ +11.67 after sell → book $10,127.78; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 3 | $347.82 | $2.00 | $+45.54 | $19,522.96 | ▲ +45.54 after sell → book $10,125.78; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 10 | $252.24 | $2.12 | — | $22,043.24 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $2531.45 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 83 | $30.18 | $2.34 | — | $24,545.84 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list ohlc_hot; ret5=+12.1; leftover $2531.45 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,545.84 | ▲ close $10,463.23 vs 09:30 $10,134.13 (session +341.91) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,545.84 | ▲ 09:30 equity $10,573.42 vs yday $10,463.23 (+110.19) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 4 | $208.88 | $2.00 | $+16.19 | $23,708.32 | ▲ +16.19 after sell → book $10,571.42; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 85 | $11.10 | $2.25 | $+90.66 | $22,762.57 | ▲ +90.66 after sell → book $10,569.17; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 206 | $4.97 | $2.66 | $+16.24 | $21,735.06 | ▲ +16.24 after sell → book $10,566.51; vs 09:30 mark -2.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 7 | $127.45 | $2.01 | $+32.26 | $20,840.90 | ▲ +32.26 after sell → book $10,564.50; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 5 | $254.39 | $2.00 | $-276.31 | $19,566.95 | ▼ -276.31 after sell → book $10,562.50; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,566.95 | ▲ close $10,572.35 vs 09:30 $10,573.42 (session +9.85) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,566.95 | ▲ 09:30 equity $10,725.30 vs yday $10,572.35 (+152.95) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 22 | $73.22 | $2.06 | $+24.86 | $17,954.05 | ▲ +24.86 after sell → book $10,723.24; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `MU` | 1 | $941.13 | $1.99 | $+21.85 | $17,010.93 | ▲ +21.85 after sell → book $10,721.25; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 30 | $54.76 | $2.08 | $+10.47 | $15,366.05 | ▲ +10.47 after sell → book $10,719.17; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,366.05 | ▲ close $10,734.95 vs 09:30 $10,725.30 (session +15.78) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,366.05 | ▲ 09:30 equity $10,786.21 vs yday $10,734.95 (+51.26) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 10 | $235.71 | $2.02 | $+161.16 | $13,006.93 | ▲ +161.16 after sell → book $10,784.19; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 83 | $26.78 | $2.24 | $+277.62 | $10,781.95 | ▲ +277.62 after sell → book $10,781.95; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,781.95 | ▲ close $10,781.95 vs 09:30 $10,786.21 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,781.95 | ▲ 09:30 equity $10,781.95 vs yday $10,781.95 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 181 | $14.85 | $2.66 | — | $13,467.14 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $2695.49 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1576 | $1.71 | $20.69 | — | $16,141.41 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list ohlc_hot; 🔵; ⚪; ret5=+11.9; leftover $2695.49 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,141.41 | ▲ close $10,927.06 vs 09:30 $10,781.95 (session +168.46) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,141.41 | ▲ 09:30 equity $10,987.54 vs yday $10,927.06 (+60.48) | — | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 588 | $4.67 | $7.78 | — | $18,879.59 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list yday_gainer; ret5=+11.9; leftover $2746.89 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 35 | $76.55 | $2.20 | — | $21,556.64 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list mover_buy; 🔵; ⚪; ret5=+4.2; leftover $2746.89 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,556.64 | ▼ close $10,888.85 vs 09:30 $10,987.54 (session -88.71) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,556.64 | ▲ 09:30 equity $10,934.92 vs yday $10,888.85 (+46.07) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,556.64 | ▲ close $11,208.25 vs 09:30 $10,934.92 (session +273.33) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,556.64 | ▲ 09:30 equity $11,243.80 vs yday $11,208.25 (+35.55) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 181 | $13.60 | $2.53 | $+221.06 | $19,092.51 | ▲ +221.06 after sell → book $11,241.27; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1576 | $1.58 | $20.33 | $+163.86 | $16,582.10 | ▲ +163.86 after sell → book $11,220.94; vs 09:30 mark -20.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,582.10 | ▲ close $11,248.38 vs 09:30 $11,243.80 (session +27.44) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,582.10 | ▲ 09:30 equity $11,330.77 vs yday $11,248.38 (+82.39) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 588 | $4.36 | $7.59 | $+166.91 | $14,010.84 | ▲ +166.91 after sell → book $11,323.19; vs 09:30 mark -7.58 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 35 | $76.79 | $2.10 | $-12.69 | $11,321.09 | ▼ -12.69 after sell → book $11,321.09; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,321.09 | ▲ close $11,321.09 vs 09:30 $11,330.77 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,321.09 | ▲ 09:30 equity $11,321.09 vs yday $11,321.09 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 10 | $112.83 | $2.07 | — | $12,447.37 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $1132.11 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 321 | $3.52 | $4.24 | — | $13,573.05 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list yday_mover; 🔵; ret5=-19.2; leftover $1132.11 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 557 | $2.03 | $7.32 | — | $14,696.45 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list yday_mover; ret5=-8.8; leftover $1132.11 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 45 | $24.97 | $2.18 | — | $15,817.92 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list ohlc_hot; ret5=+10.8; leftover $1132.11 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 335 | $3.37 | $4.42 | — | $16,942.45 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list ohlc_hot; 🔵; ret5=+4.0; leftover $1132.11 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,942.45 | ▼ close $11,289.14 vs 09:30 $11,321.09 (session -11.74) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,942.45 | ▲ 09:30 equity $11,324.61 vs yday $11,289.14 (+35.47) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,942.45 | ▼ close $11,183.24 vs 09:30 $11,324.61 (session -141.37) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,942.45 | ▼ 09:30 equity $11,182.68 vs yday $11,183.24 (-0.56) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,942.45 | ▼ close $11,080.74 vs 09:30 $11,182.68 (session -101.94) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,942.45 | ▲ 09:30 equity $11,097.47 vs yday $11,080.74 (+16.73) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 10 | $118.18 | $2.02 | $-57.54 | $15,758.63 | ▼ -57.54 after sell → book $11,095.45; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 321 | $3.98 | $4.14 | $-156.04 | $14,476.91 | ▼ -156.04 after sell → book $11,091.31; vs 09:30 mark -4.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 557 | $1.85 | $7.19 | $+85.76 | $13,439.28 | ▲ +85.76 after sell → book $11,084.13; vs 09:30 mark -7.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 45 | $24.42 | $2.12 | $+20.45 | $12,338.25 | ▲ +20.45 after sell → book $11,082.00; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 335 | $3.75 | $4.32 | $-136.04 | $11,077.68 | ▼ -136.04 after sell → book $11,077.68; vs 09:30 mark -4.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 148 | $18.61 | $2.56 | — | $13,829.40 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $2769.42 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 405 | $6.83 | $5.39 | — | $16,590.16 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list ohlc_hot; ret5=+11.2; leftover $2769.42 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,590.16 | ▼ close $10,679.07 vs 09:30 $11,097.47 (session -390.66) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,590.16 | ▼ 09:30 equity $10,641.68 vs yday $10,679.07 (-37.39) | — | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 334 | $7.95 | $4.46 | — | $19,241.01 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $2660.42 | — |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 32 | $81.00 | $2.19 | — | $21,830.82 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list earn_react; 🔵; ret5=-3.0; leftover $2660.42 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,830.82 | ▲ close $10,836.34 vs 09:30 $10,641.68 (session +201.30) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,830.82 | ▲ 09:30 equity $10,863.32 vs yday $10,836.34 (+26.98) | — | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 157 | $34.44 | $2.68 | — | $27,235.22 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list flatten; 🔵; ⚪; ret5=+14.0; leftover $5431.66 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27,235.22 | ▲ close $10,999.15 vs 09:30 $10,863.32 (session +138.51) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27,235.22 | ▼ 09:30 equity $10,803.45 vs yday $10,999.15 (-195.70) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 148 | $22.11 | $2.43 | $-522.99 | $23,960.51 | ▼ -522.99 after sell → book $10,801.02; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 405 | $6.55 | $5.22 | $+102.79 | $21,302.53 | ▲ +102.79 after sell → book $10,795.79; vs 09:30 mark -5.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 326 | $8.26 | $4.35 | — | $23,990.94 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list yday_mover; ret5=+7.7; leftover $2698.95 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 4 | $583.88 | $2.09 | — | $26,324.36 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list ohlc_hot; ret5=+8.5; leftover $2698.95 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26,324.36 | ▼ close $10,541.73 vs 09:30 $10,803.45 (session -247.61) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26,324.36 | ▲ 09:30 equity $10,569.18 vs yday $10,541.73 (+27.45) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 334 | $8.28 | $4.31 | $-117.32 | $23,556.20 | ▼ -117.32 after sell → book $10,564.87; vs 09:30 mark -4.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 28 | $93.97 | $2.18 | — | $26,185.19 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list flatten; ret5=-0.6; leftover $2641.22 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26,185.19 | ▼ close $10,495.02 vs 09:30 $10,569.18 (session -67.68) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26,185.19 | ▼ 09:30 equity $9,851.82 vs yday $10,495.02 (-643.20) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `LEN` | 32 | $82.00 | $2.09 | $-36.27 | $23,559.10 | ▼ -36.27 after sell → book $9,849.73; vs 09:30 mark -2.09 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 42 | $116.85 | $2.30 | — | $28,464.50 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list flatten; 🔵; ⚪; ret5=+3.3; leftover $4924.87 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28,464.50 | ▲ close $10,054.80 vs 09:30 $9,851.82 (session +207.37) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28,464.50 | ▲ 09:30 equity $10,146.77 vs yday $10,054.80 (+91.97) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 4 | $600.27 | $2.00 | $-69.66 | $26,061.42 | ▼ -69.66 after sell → book $10,144.77; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26,061.42 | ▼ close $9,918.64 vs 09:30 $10,146.77 (session -226.13) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,984.90 | ▼ 09:30 equity $9,218.51 vs yday $9,248.26 (-29.75) | 09:30 open · cash $20,984.90 (unchanged overnight, no fees) · equity $9,218.51 vs prior close $9,248.26 (-29.75) · 5 name(s) re-marked at the open (per-name table). AEHL×301 yday $8.96 → 09:30 $9.05 -27.09; BAND×40 yday $61.83 → 09:30 $61.83 -0.00; HALO×19 yday $115.22 → 09:30 $115.36 -2.66; PAYX×20 yday $101.59 → 09:30 $101.59 -0.00; USFD×25 yday $93.82 → 09:30 $93.82 -0.00 | — |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 587 | $7.85 | $7.83 | — | $25,585.02 | — | short packet🔴 OR headline🔴; gate news_or_red=True; list yday_gainer; 🔵; ⚪; ret5=+25.4; leftover $4609.26 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25,585.02 | ▼ close $9,186.20 vs 09:30 $9,218.51 (session -24.48) | 16:00 close · cash $25,585.02 · equity $9,186.20 vs 09:30 $9,218.51 (-32.31; session marks -24.48) · 6 name(s) marked open→close (per-name table). AEHL×301 09:30 $9.05 → close $9.36 -93.31; BAND×40 09:30 $61.83 → close $61.83 -0.00; HALO×19 09:30 $115.36 → close $113.90 +27.74; PAYX×20 09:30 $101.59 → close $101.59 +0.00; USFD×25 09:30 $93.82 → close $93.82 +0.00; RSKD×587 09:30 $7.85 → close $7.78 +41.09 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `EU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LUNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `OWL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LUNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OWL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ZNTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `VERI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ZNTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `APMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `RNW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WYFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TOYO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WMT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AQST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WYFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TOYO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TEAM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WMT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `SSRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARIS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARIS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `NOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `INTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `NEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `BE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `NEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `TX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `TX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PIPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `GSM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `PIPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `QRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RWT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RWT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `MYGN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `GFR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `GFR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BULL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `LEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BULL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `LEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `LEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-23 | `AMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `USFD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `USFD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FIVN` | 157 | 2026-09-18 @ $34.44 | short packet🔴 OR headline🔴; gate news_or_red=True; list flatten; 🔵; ⚪; ret5=+14.0; leftover $5431.66 |
| `AEHL` | 326 | 2026-09-21 @ $8.26 | short packet🔴 OR headline🔴; gate news_or_red=True; list yday_mover; ret5=+7.7; leftover $2698.95 |
| `USFD` | 28 | 2026-09-22 @ $93.97 | short packet🔴 OR headline🔴; gate news_or_red=True; list flatten; ret5=-0.6; leftover $2641.22 |
| `HALO` | 42 | 2026-09-23 @ $116.85 | short packet🔴 OR headline🔴; gate news_or_red=True; list flatten; 🔵; ⚪; ret5=+3.3; leftover $4924.87 |
