# Factor mine action — `short_news_r_macd_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · short news🔴 ∩ prior MACD histogram > 0

Cash book **-1.74%** ($9,826) · signal-only (no cash/fees) was +37.55%. Starts YES **29/30**. Fills 79 · skips 81 · realized $+1277.69.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the news camera (does the morning packet like the headline?) is red.
- Must-have: prior MACD histogram is above zero (momentum still up).

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
- **Gate** `news=bad,macd_up=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $16,955.15.

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
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1412 | $1.18 | $18.51 | — | $11,647.65 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1666.67 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 86 | $19.17 | $2.32 | — | $13,293.95 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1666.67 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 131 | $12.70 | $2.46 | — | $14,954.53 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+12.6; leftover $1666.67 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,954.53 | ▲ close $10,010.33 vs 09:30 $10,000.00 (session +33.62) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,954.53 | ▼ 09:30 equity $9,916.79 vs yday $10,010.33 (-93.54) | — | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 1437 | $1.15 | $18.83 | — | $16,588.25 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; ⚪; ret5=-12.2; leftover $1652.80 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 549 | $3.01 | $7.23 | — | $18,233.51 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list earn_react; ⚪; ret5=-5.3; leftover $1652.80 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 243 | $6.80 | $3.23 | — | $19,882.67 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list overnight; ⚪; ret5=+10.4; leftover $1652.80 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,882.67 | ▲ close $10,105.14 vs 09:30 $9,916.79 (session +217.64) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,882.67 | ▲ 09:30 equity $10,321.13 vs yday $10,105.14 (+215.99) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,882.67 | ▲ close $10,579.59 vs 09:30 $10,321.13 (session +258.46) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,882.67 | ▼ 09:30 equity $10,574.96 vs yday $10,579.59 (-4.63) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 1412 | $1.07 | $18.21 | $+118.60 | $18,353.62 | ▲ +118.60 after sell → book $10,556.75; vs 09:30 mark -18.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 131 | $11.75 | $2.38 | $+118.95 | $16,811.99 | ▲ +118.95 after sell → book $10,554.37; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,811.99 | ▲ close $10,630.54 vs 09:30 $10,574.96 (session +76.18) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,811.99 | ▼ 09:30 equity $10,594.59 vs yday $10,630.54 (-35.95) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 86 | $18.13 | $2.25 | $+84.87 | $15,250.56 | ▲ +84.87 after sell → book $10,592.35; vs 09:30 mark -2.24 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 1437 | $0.96 | $18.15 | $+231.74 | $13,848.58 | ▲ +231.74 after sell → book $10,574.20; vs 09:30 mark -18.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 549 | $2.95 | $7.08 | $+18.63 | $12,221.94 | ▲ +18.63 after sell → book $10,567.11; vs 09:30 mark -7.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 243 | $6.81 | $3.13 | $-8.80 | $10,563.98 | ▼ -8.80 after sell → book $10,563.98; vs 09:30 mark -3.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $11,175.29 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $660.25 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 30 | $21.40 | $2.12 | — | $11,815.18 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; 🔵; ret5=-25.2; leftover $660.25 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 149 | $4.43 | $2.49 | — | $12,472.76 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; 🔵; ret5=-23.1; leftover $660.25 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 55 | $11.81 | $2.19 | — | $13,120.39 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $660.25 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $13,640.06 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+12.2; leftover $660.25 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 14 | $46.85 | $2.07 | — | $14,293.89 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list earn_react; 🔵; ret5=+5.0; leftover $660.25 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 6 | $106.38 | $2.05 | — | $14,930.12 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list earn_react; 🔵; ret5=-1.7; leftover $660.25 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 143 | $4.61 | $2.47 | — | $15,586.88 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list mover_buy; 🔵; ⚪; ret5=+8.5; leftover $660.25 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,586.88 | ▲ close $10,622.86 vs 09:30 $10,594.59 (session +76.34) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,586.88 | ▼ 09:30 equity $10,570.35 vs yday $10,622.86 (-52.51) | — | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 7 | $133.11 | $2.06 | — | $16,516.59 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; 🔵; ⚪; ret5=+109.5; leftover $1057.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 11 | $89.10 | $2.07 | — | $17,494.62 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+15.8; leftover $1057.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 27 | $38.40 | $2.12 | — | $18,529.30 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+15.8; leftover $1057.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 50 | $20.90 | $2.19 | — | $19,572.11 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1057.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 39 | $27.00 | $2.16 | — | $20,622.96 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+10.1; leftover $1057.03 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,622.96 | ▼ close $10,507.45 vs 09:30 $10,570.35 (session -52.31) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,622.96 | ▲ 09:30 equity $10,567.73 vs yday $10,507.45 (+60.28) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,622.96 | ▲ close $10,596.54 vs 09:30 $10,567.73 (session +28.81) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,622.96 | ▲ 09:30 equity $10,651.88 vs yday $10,596.54 (+55.34) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $212.00 | $2.00 | $-26.68 | $19,984.96 | ▼ -26.68 after sell → book $10,649.88; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 30 | $20.90 | $2.08 | $+10.80 | $19,355.88 | ▲ +10.80 after sell → book $10,647.80; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 149 | $4.42 | $2.44 | $-3.44 | $18,694.86 | ▼ -3.44 after sell → book $10,645.36; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 55 | $11.00 | $2.15 | $+40.48 | $18,087.71 | ▲ +40.48 after sell → book $10,643.21; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 3 | $170.64 | $2.00 | $+5.75 | $17,573.79 | ▲ +5.75 after sell → book $10,641.21; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 14 | $43.63 | $2.03 | $+40.98 | $16,960.94 | ▲ +40.98 after sell → book $10,639.18; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 6 | $105.58 | $2.01 | $+0.75 | $16,325.45 | ▲ +0.75 after sell → book $10,637.17; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 143 | $4.77 | $2.42 | $-27.77 | $15,640.92 | ▼ -27.77 after sell → book $10,634.75; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 130 | $13.62 | $2.46 | — | $17,409.71 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $1772.46 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 32 | $54.51 | $2.16 | — | $19,151.87 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+15.1; leftover $1772.46 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 4 | $364.35 | $2.06 | — | $20,607.20 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $1772.46 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,607.20 | ▼ close $10,462.83 vs 09:30 $10,651.88 (session -165.23) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,607.20 | ▲ 09:30 equity $10,724.07 vs yday $10,462.83 (+261.24) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 7 | $154.20 | $2.01 | $-151.70 | $19,525.79 | ▼ -151.70 after sell → book $10,722.06; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 11 | $88.24 | $2.02 | $+5.37 | $18,553.13 | ▲ +5.37 after sell → book $10,720.04; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 27 | $38.41 | $2.07 | $-4.46 | $17,513.99 | ▼ -4.46 after sell → book $10,717.97; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 50 | $20.50 | $2.14 | $+15.67 | $16,486.85 | ▲ +15.67 after sell → book $10,715.83; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 39 | $26.00 | $2.11 | $+34.74 | $15,470.74 | ▲ +34.74 after sell → book $10,713.72; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 5 | $213.94 | $2.05 | — | $16,538.39 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1071.37 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 87 | $12.22 | $2.30 | — | $17,599.22 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+12.4; leftover $1071.37 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 210 | $5.08 | $2.78 | — | $18,663.24 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+17.6; leftover $1071.37 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 8 | $132.64 | $2.06 | — | $19,722.30 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+16.5; leftover $1071.37 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 5 | $199.94 | $2.05 | — | $20,719.94 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list overnight,overnight_mega; ret5=+2.1; leftover $1071.37 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,719.94 | ▼ close $10,502.05 vs 09:30 $10,724.07 (session -200.41) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,719.94 | ▼ 09:30 equity $10,325.22 vs yday $10,502.05 (-176.83) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,719.94 | ▼ close $10,288.26 vs 09:30 $10,325.22 (session -36.96) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,719.94 | ▲ 09:30 equity $10,308.67 vs yday $10,288.26 (+20.41) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 130 | $13.90 | $2.38 | $-40.59 | $18,910.56 | ▼ -40.59 after sell → book $10,306.29; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 32 | $52.49 | $2.09 | $+60.40 | $17,228.80 | ▲ +60.40 after sell → book $10,304.21; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 4 | $347.82 | $2.00 | $+62.05 | $15,835.52 | ▲ +62.05 after sell → book $10,302.21; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 10 | $252.24 | $2.12 | — | $18,355.80 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $2575.55 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 85 | $30.18 | $2.35 | — | $20,918.75 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+12.1; leftover $2575.55 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,918.75 | ▲ close $10,611.61 vs 09:30 $10,308.67 (session +313.88) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,918.75 | ▲ 09:30 equity $10,755.85 vs yday $10,611.61 (+144.24) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 5 | $208.88 | $2.00 | $+21.24 | $19,872.34 | ▲ +21.24 after sell → book $10,753.84; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 87 | $11.10 | $2.25 | $+92.88 | $18,904.39 | ▲ +92.88 after sell → book $10,751.59; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 210 | $4.97 | $2.71 | $+16.56 | $17,856.93 | ▲ +16.56 after sell → book $10,748.88; vs 09:30 mark -2.71 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 8 | $127.45 | $2.01 | $+37.44 | $16,835.32 | ▲ +37.44 after sell → book $10,746.87; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 5 | $254.39 | $2.00 | $-276.31 | $15,561.36 | ▼ -276.31 after sell → book $10,744.86; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,561.36 | ▲ close $10,756.31 vs 09:30 $10,755.85 (session +11.45) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,561.36 | ▲ 09:30 equity $10,860.36 vs yday $10,756.31 (+104.05) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,561.36 | ▲ close $10,875.86 vs 09:30 $10,860.36 (session +15.50) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,561.36 | ▲ 09:30 equity $10,927.96 vs yday $10,875.86 (+52.10) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 10 | $235.71 | $2.02 | $+161.16 | $13,202.24 | ▲ +161.16 after sell → book $10,925.94; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 85 | $26.78 | $2.25 | $+284.41 | $10,923.70 | ▲ +284.41 after sell → book $10,923.70; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,923.70 | ▲ close $10,923.70 vs 09:30 $10,927.96 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,923.70 | ▲ 09:30 equity $10,923.70 vs yday $10,923.70 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 3194 | $1.71 | $41.93 | — | $16,343.51 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ⚪; ret5=+11.9; leftover $5461.85 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,343.51 | ▲ close $11,201.17 vs 09:30 $10,923.70 (session +319.40) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,343.51 | ▲ 09:30 equity $11,265.05 vs yday $11,201.17 (+63.88) | — | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 603 | $4.67 | $7.98 | — | $19,151.54 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_gainer; ret5=+11.9; leftover $2816.26 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 36 | $76.55 | $2.21 | — | $21,905.14 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list mover_buy; 🔵; ⚪; ret5=+4.2; leftover $2816.26 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,905.14 | ▼ close $11,077.53 vs 09:30 $11,265.05 (session -177.34) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,905.14 | ▼ 09:30 equity $11,075.63 vs yday $11,077.53 (-1.90) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,905.14 | ▲ close $11,316.88 vs 09:30 $11,075.63 (session +241.25) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,905.14 | ▲ 09:30 equity $11,352.42 vs yday $11,316.88 (+35.54) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 3194 | $1.58 | $41.20 | $+332.09 | $16,817.42 | ▲ +332.09 after sell → book $11,311.22; vs 09:30 mark -41.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,817.42 | ▲ close $11,339.39 vs 09:30 $11,352.42 (session +28.17) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,817.42 | ▲ 09:30 equity $11,423.90 vs yday $11,339.39 (+84.51) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 603 | $4.36 | $7.78 | $+171.17 | $14,180.56 | ▲ +171.17 after sell → book $11,416.12; vs 09:30 mark -7.78 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 36 | $76.79 | $2.10 | $-12.94 | $11,414.02 | ▼ -12.94 after sell → book $11,414.02; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,414.02 | ▲ close $11,414.02 vs 09:30 $11,423.90 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,414.02 | ▲ 09:30 equity $11,414.02 vs yday $11,414.02 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 16 | $112.83 | $2.11 | — | $13,217.27 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $1902.34 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 76 | $24.97 | $2.30 | — | $15,112.69 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+10.8; leftover $1902.34 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 564 | $3.37 | $7.44 | — | $17,005.93 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+4.0; leftover $1902.34 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,005.93 | ▼ close $11,369.17 vs 09:30 $11,414.02 (session -33.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,005.93 | ▲ 09:30 equity $11,401.89 vs yday $11,369.17 (+32.72) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,005.93 | ▼ close $11,330.37 vs 09:30 $11,401.89 (session -71.52) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,005.93 | ▼ 09:30 equity $11,285.33 vs yday $11,330.37 (-45.04) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,005.93 | ▼ close $11,078.05 vs 09:30 $11,285.33 (session -207.28) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,005.93 | ▲ 09:30 equity $11,144.13 vs yday $11,078.05 (+66.08) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 16 | $118.18 | $2.04 | $-89.67 | $15,113.02 | ▼ -89.67 after sell → book $11,142.10; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 76 | $24.42 | $2.22 | $+37.28 | $13,254.88 | ▲ +37.28 after sell → book $11,139.88; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 564 | $3.75 | $7.28 | $-229.03 | $11,132.60 | ▼ -229.03 after sell → book $11,132.60; vs 09:30 mark -7.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 814 | $6.83 | $10.83 | — | $16,681.39 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+11.2; leftover $5566.30 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,681.39 | ▲ close $11,398.53 vs 09:30 $11,144.13 (session +276.76) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,681.39 | ▲ 09:30 equity $11,406.67 vs yday $11,398.53 (+8.14) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,681.39 | ▼ close $11,260.15 vs 09:30 $11,406.67 (session -146.52) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,681.39 | ▲ 09:30 equity $11,276.43 vs yday $11,260.15 (+16.28) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,681.39 | ▼ close $11,260.15 vs 09:30 $11,276.43 (session -16.28) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,681.39 | ▲ 09:30 equity $11,349.69 vs yday $11,260.15 (+89.54) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 814 | $6.55 | $10.50 | $+206.59 | $11,339.19 | ▲ +206.59 after sell → book $11,339.19; vs 09:30 mark -10.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 343 | $8.26 | $4.58 | — | $14,167.79 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; ret5=+7.7; leftover $2834.80 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 4 | $583.88 | $2.09 | — | $16,501.21 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+8.5; leftover $2834.80 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,501.21 | ▲ close $11,665.57 vs 09:30 $11,349.69 (session +333.06) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,501.21 | ▲ 09:30 equity $11,701.37 vs yday $11,665.57 (+35.80) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,501.21 | ▼ close $11,632.57 vs 09:30 $11,701.37 (session -68.80) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,501.21 | ▼ 09:30 equity $11,401.55 vs yday $11,632.57 (-231.02) | — | — |
| 2026-09-23 09:30 ET | **SHORT** | `FIVN` | 146 | $38.91 | $2.65 | — | $22,178.69 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+16.6; leftover $5700.78 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,178.69 | ▼ close $11,396.08 vs 09:30 $11,401.55 (session -2.82) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,178.69 | ▲ 09:30 equity $11,485.12 vs yday $11,396.08 (+89.04) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `AEHL` | 343 | $8.21 | $4.42 | $+8.14 | $19,358.23 | ▲ +8.14 after sell → book $11,480.69; vs 09:30 mark -4.43 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 4 | $600.27 | $2.00 | $-69.66 | $16,955.15 | ▼ -69.66 after sell → book $11,478.69; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,955.15 | ▲ close $11,602.79 vs 09:30 $11,485.12 (session +124.10) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23,148.72 | ▼ 09:30 equity $9,794.95 vs yday $9,822.85 (-27.90) | 09:30 open · cash $23,148.72 (unchanged overnight, no fees) · equity $9,794.95 vs prior close $9,822.85 (-27.90) · 3 name(s) re-marked at the open (per-name table). AEHL×310 yday $8.96 → 09:30 $9.05 -27.90; BAND×87 yday $61.83 → 09:30 $61.83 -0.00; FIVN×141 yday $36.66 → 09:30 $36.66 -0.00 | — |
| 2026-09-25 09:30 ET | **COVER** | `AEHL` | 310 | $9.05 | $4.00 | $-253.04 | $20,339.22 | ▼ -253.04 after sell → book $9,790.95; vs 09:30 mark -4.00 | dropped from list after 4 sess (min 3) | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 623 | $7.85 | $8.31 | — | $25,221.46 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_gainer; 🔵; ⚪; ret5=+25.4; leftover $4895.48 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25,221.46 | ▲ close $9,826.25 vs 09:30 $9,794.95 (session +43.61) | 16:00 close · cash $25,221.46 · equity $9,826.25 vs 09:30 $9,794.95 (+31.30; session marks +43.61) · 3 name(s) marked open→close (per-name table). BAND×87 09:30 $61.83 → close $61.83 -0.00; FIVN×141 09:30 $36.66 → close $36.66 +0.00; RSKD×623 09:30 $7.85 → close $7.78 +43.61 | — |

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
| 2026-08-18 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `VERI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `RNW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
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
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `SSRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARIS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARIS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `NOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `INTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `NEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `BE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `NEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PIPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `GSM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `PIPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `QRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `MYGN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `GFR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `GFR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `AEHL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `AMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FIVN` | no_price | no 09:30 open |
| 2026-09-23 | `AEHL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `AMD` | min_hold | dropped but min-hold 2/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FIVN` | 146 | 2026-09-23 @ $38.91 | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+16.6; leftover $5700.78 |
