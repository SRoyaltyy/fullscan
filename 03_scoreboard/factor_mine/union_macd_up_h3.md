# Factor mine action — `union_macd_up_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ macd_up, no 🚨

Cash book **-5.75%** ($9,425) · signal-only (no cash/fees) was +63.53%. Starts YES **26/30**. Fills 184 · skips 288 · realized $-45.00.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: prior MACD histogram is above zero (momentum still up).
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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `macd_up=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6,012.05.

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
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 43 | $45.98 | $2.12 | — | $8,020.74 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ⚪; ret5=+12.3; leftover $2000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 39 | $50.62 | $2.11 | — | $6,044.33 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ⚪; ret5=+6.2; leftover $2000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 67 | $29.74 | $2.19 | — | $4,049.56 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ⚪; ret5=-5.3; leftover $2000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 85 | $23.33 | $2.25 | — | $2,064.26 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ⚪; ret5=+19.7; leftover $2000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 90 | $22.01 | $2.26 | — | $81.10 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ⚪; ret5=+0.3; leftover $2000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.10 | ▲ close $10,125.70 vs 09:30 $10,000.00 (session +136.63) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.10 | ▲ 09:30 equity $10,134.23 vs yday $10,125.70 (+8.53) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 6 | $1.50 | $0.11 | — | $72.00 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $10.14 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 2 | $4.18 | $0.09 | — | $63.55 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $10.14 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.55 | ▼ close $9,933.79 vs 09:30 $10,134.23 (session -200.25) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.55 | ▲ 09:30 equity $9,939.67 vs yday $9,933.79 (+5.88) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $59.45 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $7.94 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 2 | $3.24 | $0.07 | — | $52.90 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ⚪; ret5=+0.3; leftover $7.94 | — |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 1 | $5.07 | $0.05 | — | $47.78 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=-4.7; leftover $7.94 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.78 | ▼ close $9,909.69 vs 09:30 $9,939.67 (session -29.81) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.78 | ▼ 09:30 equity $9,774.98 vs yday $9,909.69 (-134.71) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 43 | $43.56 | $2.14 | $-108.32 | $1,918.71 | ▼ -108.32 after sell → book $9,772.83; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 39 | $51.77 | $2.13 | $+40.49 | $3,935.61 | ▲ +40.49 after sell → book $9,770.70; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 67 | $27.85 | $2.22 | $-131.04 | $5,799.34 | ▼ -131.04 after sell → book $9,768.48; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 85 | $22.16 | $2.27 | $-103.97 | $7,680.67 | ▼ -103.97 after sell → book $9,766.21; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 90 | $22.82 | $2.29 | $+68.35 | $9,732.18 | ▲ +68.35 after sell → book $9,763.92; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,732.18 | ▼ close $9,763.35 vs 09:30 $9,774.98 (session -0.57) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,732.18 | ▲ 09:30 equity $9,763.35 vs yday $9,763.35 (-0.00) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 6 | $1.42 | $0.12 | $-0.71 | $9,740.57 | ▼ -0.71 after sell → book $9,763.22; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 2 | $3.87 | $0.10 | $-0.81 | $9,748.21 | ▼ -0.81 after sell → book $9,763.12; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,748.21 | ▼ close $9,763.08 vs 09:30 $9,763.35 (session -0.04) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,748.21 | ▼ 09:30 equity $9,762.98 vs yday $9,763.08 (-0.10) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 1 | $3.92 | $0.06 | $-0.24 | $9,752.07 | ▼ -0.24 after sell → book $9,762.92; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 2 | $3.20 | $0.09 | $-0.24 | $9,758.38 | ▼ -0.24 after sell → book $9,762.83; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NB` | 1 | $4.45 | $0.07 | $-0.74 | $9,762.76 | ▼ -0.74 after sell → book $9,762.76; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 59 | $20.55 | $2.17 | — | $8,548.14 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1220.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,362.99 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1220.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 59 | $20.65 | $2.17 | — | $6,142.47 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1220.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 62 | $19.63 | $2.18 | — | $4,923.23 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1220.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $3,706.29 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1220.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 697 | $1.75 | $8.99 | — | $2,477.55 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1220.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,319.21 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1220.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 248 | $4.92 | $3.20 | — | $95.86 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1220.35 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.86 | ▲ close $9,973.09 vs 09:30 $9,762.98 (session +235.18) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.86 | ▲ 09:30 equity $10,321.91 vs yday $9,973.09 (+348.82) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $84.61 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $11.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 4 | $2.47 | $0.11 | — | $74.62 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $11.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 6 | $1.93 | $0.13 | — | $62.91 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $11.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 9 | $1.32 | $0.15 | — | $50.88 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $11.98 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.88 | ▲ close $10,331.48 vs 09:30 $10,321.91 (session +10.08) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.88 | ▲ 09:30 equity $10,414.48 vs yday $10,331.48 (+83.00) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.88 | ▲ close $10,423.64 vs 09:30 $10,414.48 (session +9.16) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.88 | ▼ 09:30 equity $10,271.21 vs yday $10,423.64 (-152.43) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 59 | $20.32 | $2.19 | $-17.92 | $1,247.57 | ▼ -17.92 after sell → book $10,269.02; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.86 | $2.05 | $+58.97 | $2,491.70 | ▲ +58.97 after sell → book $10,266.97; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 59 | $20.47 | $2.19 | $-14.97 | $3,697.25 | ▼ -14.97 after sell → book $10,264.79; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 62 | $21.21 | $2.20 | $+93.59 | $5,010.07 | ▲ +93.59 after sell → book $10,262.59; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 41 | $32.32 | $2.13 | $+106.04 | $6,333.06 | ▲ +106.04 after sell → book $10,260.46; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 697 | $1.90 | $9.12 | $+86.44 | $7,648.24 | ▲ +86.44 after sell → book $10,251.34; vs 09:30 mark -9.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $8,898.29 | ▲ +91.71 after sell → book $10,249.31; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 248 | $5.25 | $3.25 | $+75.39 | $10,197.03 | ▲ +75.39 after sell → book $10,246.05; vs 09:30 mark -3.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 53 | $23.77 | $2.15 | — | $8,935.08 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ⚪; ret5=+13.0; leftover $1274.63 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 116 | $10.98 | $2.34 | — | $7,659.06 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+1.2; leftover $1274.63 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 20 | $61.19 | $2.05 | — | $6,433.21 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+7.4; leftover $1274.63 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 152 | $8.35 | $2.45 | — | $5,161.56 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1274.63 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 258 | $4.94 | $3.33 | — | $3,883.71 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+7.1; leftover $1274.63 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $426.97 | $2.00 | — | $3,027.78 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+6.0; leftover $1274.63 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 175 | $7.25 | $2.52 | — | $1,756.51 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1274.63 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 93 | $13.59 | $2.27 | — | $490.37 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1274.63 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $490.37 | ▲ close $10,477.96 vs 09:30 $10,271.21 (session +251.00) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $490.37 | ▲ 09:30 equity $10,484.62 vs yday $10,477.96 (+6.66) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $505.55 | ▲ +3.93 after sell → book $10,484.45; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 4 | $2.41 | $0.13 | $-0.48 | $515.06 | ▼ -0.48 after sell → book $10,484.32; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 6 | $2.03 | $0.16 | $+0.31 | $527.08 | ▲ +0.31 after sell → book $10,484.16; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 9 | $1.60 | $0.19 | $+2.18 | $541.29 | ▲ +2.18 after sell → book $10,483.97; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 5 | $31.21 | $1.58 | — | $383.66 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $180.43 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 16 | $11.12 | $1.83 | — | $203.91 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $180.43 | — |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 18 | $9.83 | $1.82 | — | $25.15 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $180.43 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.15 | ▲ close $10,582.47 vs 09:30 $10,484.62 (session +103.73) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.15 | ▼ 09:30 equity $10,547.89 vs yday $10,582.47 (-34.58) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 3 | $2.60 | $0.09 | — | $17.26 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,ohlc_hot; ret5=+13.0; leftover $8.38 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.26 | ▲ close $10,643.68 vs 09:30 $10,547.89 (session +95.88) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.26 | ▼ 09:30 equity $10,587.96 vs yday $10,643.68 (-55.72) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 116 | $10.97 | $2.37 | $-5.87 | $1,287.42 | ▼ -5.87 after sell → book $10,585.60; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 20 | $60.52 | $2.07 | $-17.52 | $2,495.75 | ▼ -17.52 after sell → book $10,583.53; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 152 | $8.28 | $2.48 | $-15.57 | $3,751.83 | ▼ -15.57 after sell → book $10,581.05; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 258 | $4.95 | $3.38 | $-4.13 | $5,025.54 | ▼ -4.13 after sell → book $10,577.66; vs 09:30 mark -3.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 2 | $423.76 | $2.02 | $-10.43 | $5,871.05 | ▼ -10.43 after sell → book $10,575.65; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 175 | $9.73 | $2.56 | $+428.93 | $7,571.24 | ▲ +428.93 after sell → book $10,573.09; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 93 | $13.05 | $2.29 | $-54.78 | $8,782.60 | ▼ -54.78 after sell → book $10,570.80; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 35 | $41.74 | $2.10 | — | $7,319.60 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+2.4; leftover $1463.77 | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 100 | $14.63 | $2.29 | — | $5,854.31 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+5.8; leftover $1463.77 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 44 | $32.90 | $2.12 | — | $4,404.59 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1463.77 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 93 | $15.66 | $2.27 | — | $2,945.94 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1463.77 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 18 | $79.42 | $2.04 | — | $1,514.34 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1463.77 | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $251.13 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1463.77 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $251.13 | ▼ close $10,300.77 vs 09:30 $10,587.96 (session -257.20) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $251.13 | ▲ 09:30 equity $10,332.74 vs yday $10,300.77 (+31.97) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 53 | $23.68 | $2.17 | $-9.09 | $1,504.00 | ▼ -9.09 after sell → book $10,330.57; vs 09:30 mark -2.17 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVBP` | 5 | $29.94 | $1.53 | $-9.46 | $1,652.17 | ▼ -9.46 after sell → book $10,329.04; vs 09:30 mark -1.53 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FLNC` | 16 | $10.82 | $1.80 | $-8.43 | $1,823.49 | ▼ -8.43 after sell → book $10,327.24; vs 09:30 mark -1.80 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ABX` | 18 | $9.74 | $1.83 | $-5.27 | $1,996.98 | ▼ -5.27 after sell → book $10,325.41; vs 09:30 mark -1.83 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,996.98 | ▲ close $10,396.33 vs 09:30 $10,332.74 (session +70.92) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,996.98 | ▲ 09:30 equity $10,453.53 vs yday $10,396.33 (+57.20) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 3 | $2.67 | $0.11 | $+0.01 | $2,004.89 | ▲ +0.01 after sell → book $10,453.43; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,004.89 | ▼ close $10,444.79 vs 09:30 $10,453.53 (session -8.64) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,004.89 | ▼ 09:30 equity $10,367.10 vs yday $10,444.79 (-77.69) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 35 | $42.10 | $2.12 | $+8.39 | $3,476.27 | ▲ +8.39 after sell → book $10,364.98; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 100 | $15.70 | $2.32 | $+102.39 | $5,043.95 | ▲ +102.39 after sell → book $10,362.66; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 44 | $32.42 | $2.14 | $-25.39 | $6,468.29 | ▼ -25.39 after sell → book $10,360.52; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 93 | $13.92 | $2.29 | $-166.38 | $7,760.55 | ▼ -166.38 after sell → book $10,358.22; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 18 | $78.84 | $2.07 | $-14.55 | $9,177.61 | ▼ -14.55 after sell → book $10,356.16; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SIMO` | 5 | $235.71 | $2.02 | $-86.68 | $10,354.13 | ▼ -86.68 after sell → book $10,354.13; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,354.13 | ▲ close $10,354.13 vs 09:30 $10,367.10 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,354.13 | ▲ 09:30 equity $10,354.13 vs yday $10,354.13 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $52.88 | $2.06 | — | $9,082.95 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1294.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $42.93 | $2.08 | — | $7,792.97 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1294.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 356 | $3.63 | $4.59 | — | $6,496.10 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1294.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 161 | $8.03 | $2.47 | — | $5,200.79 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1294.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $4,006.73 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1294.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 83 | $15.45 | $2.24 | — | $2,722.14 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1294.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $1,552.56 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1294.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 77 | $16.77 | $2.22 | — | $259.05 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1294.27 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $259.05 | ▼ close $10,103.15 vs 09:30 $10,354.13 (session -231.28) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $259.05 | ▲ 09:30 equity $10,104.24 vs yday $10,103.15 (+1.09) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 17 | $2.52 | $0.48 | — | $215.73 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $43.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 22 | $1.90 | $0.48 | — | $173.45 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $43.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 9 | $4.78 | $0.46 | — | $129.97 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $43.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 27 | $1.59 | $0.51 | — | $86.53 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $43.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 3 | $11.31 | $0.35 | — | $52.25 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $43.18 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.25 | ▲ close $10,136.74 vs 09:30 $10,104.24 (session +34.77) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.25 | ▲ 09:30 equity $10,165.83 vs yday $10,136.74 (+29.09) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.25 | ▼ close $9,996.10 vs 09:30 $10,165.83 (session -169.73) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.25 | ▼ 09:30 equity $9,947.01 vs yday $9,996.10 (-49.09) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 24 | $53.16 | $2.08 | $+2.58 | $1,326.01 | ▲ +2.58 after sell → book $9,944.93; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 30 | $42.01 | $2.10 | $-31.78 | $2,584.21 | ▼ -31.78 after sell → book $9,942.83; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 356 | $3.28 | $4.66 | $-133.85 | $3,747.23 | ▼ -133.85 after sell → book $9,938.17; vs 09:30 mark -4.66 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 161 | $8.01 | $2.51 | $-8.20 | $5,034.33 | ▼ -8.20 after sell → book $9,935.66; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $6,164.22 | ▼ -64.17 after sell → book $9,933.62; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 83 | $15.16 | $2.26 | $-28.57 | $7,420.24 | ▼ -28.57 after sell → book $9,931.36; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 8 | $140.29 | $2.03 | $-49.25 | $8,540.57 | ▼ -49.25 after sell → book $9,929.33; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 77 | $15.46 | $2.24 | $-105.33 | $9,728.74 | ▼ -105.33 after sell → book $9,927.08; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,728.74 | ▼ close $9,918.07 vs 09:30 $9,947.01 (session -9.02) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,728.74 | ▼ 09:30 equity $9,915.06 vs yday $9,918.07 (-3.01) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 17 | $2.22 | $0.45 | $-6.03 | $9,766.03 | ▼ -6.03 after sell → book $9,914.61; vs 09:30 mark -0.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 22 | $1.83 | $0.49 | $-2.51 | $9,805.80 | ▼ -2.51 after sell → book $9,914.12; vs 09:30 mark -0.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 9 | $3.92 | $0.40 | $-8.58 | $9,840.70 | ▼ -8.58 after sell → book $9,913.72; vs 09:30 mark -0.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 27 | $1.53 | $0.51 | $-2.64 | $9,881.50 | ▼ -2.64 after sell → book $9,913.21; vs 09:30 mark -0.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 3 | $10.57 | $0.35 | $-2.91 | $9,912.86 | ▼ -2.91 after sell → book $9,912.86; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,912.86 | ▲ close $9,912.86 vs 09:30 $9,915.06 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,912.86 | ▲ 09:30 equity $9,912.86 vs yday $9,912.86 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 5 | $206.84 | $2.00 | — | $8,876.66 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+8.3; leftover $1239.11 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $7,723.64 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1239.11 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 7 | $157.78 | $2.01 | — | $6,617.17 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+4.7; leftover $1239.11 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 22 | $56.09 | $2.06 | — | $5,381.13 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+19.6; leftover $1239.11 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 260 | $4.75 | $3.35 | — | $4,142.78 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1239.11 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 584 | $2.12 | $7.53 | — | $2,897.16 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1239.11 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 107 | $11.55 | $2.31 | — | $1,659.00 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1239.11 | — |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 7 | $157.55 | $2.01 | — | $554.14 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $1239.11 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $554.14 | ▼ close $9,884.54 vs 09:30 $9,912.86 (session -5.03) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $554.14 | ▼ 09:30 equity $9,582.96 vs yday $9,884.54 (-301.58) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $554.14 | ▼ close $9,580.28 vs 09:30 $9,582.96 (session -2.68) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $554.14 | ▲ 09:30 equity $9,583.82 vs yday $9,580.28 (+3.54) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $554.14 | ▼ close $9,344.36 vs 09:30 $9,583.82 (session -239.46) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $554.14 | ▼ 09:30 equity $9,250.41 vs yday $9,344.36 (-93.95) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 5 | $194.84 | $2.02 | $-64.03 | $1,526.31 | ▼ -64.03 after sell → book $9,248.38; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 7 | $140.03 | $2.03 | $-174.84 | $2,504.49 | ▼ -174.84 after sell → book $9,246.35; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 7 | $147.79 | $2.03 | $-73.97 | $3,536.99 | ▼ -73.97 after sell → book $9,244.32; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COHU` | 22 | $51.29 | $2.08 | $-109.73 | $4,663.30 | ▼ -109.73 after sell → book $9,242.25; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 260 | $4.73 | $3.41 | $-11.96 | $5,889.69 | ▼ -11.96 after sell → book $9,238.84; vs 09:30 mark -3.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAK` | 584 | $1.84 | $7.64 | $-178.69 | $6,956.61 | ▼ -178.69 after sell → book $9,231.20; vs 09:30 mark -7.64 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `FUBO` | 107 | $10.75 | $2.34 | $-90.25 | $8,104.52 | ▼ -90.25 after sell → book $9,228.86; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RDDT` | 7 | $160.62 | $2.03 | $+17.45 | $9,226.83 | ▲ +17.45 after sell → book $9,226.83; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 424 | $2.72 | $5.47 | — | $8,068.08 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; ret5=-0.4; leftover $1153.35 | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 640 | $1.80 | $8.26 | — | $6,907.82 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $1153.35 | — |
| 2026-09-16 09:30 ET | **BUY** | `INDP` | 315 | $3.66 | $4.06 | — | $5,750.86 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; ret5=+96.8; leftover $1153.35 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 12 | $89.38 | $2.03 | — | $4,676.27 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1153.35 | — |
| 2026-09-16 09:30 ET | **BUY** | `FTRE` | 58 | $19.75 | $2.16 | — | $3,528.61 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+15.7; leftover $1153.35 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 49 | $23.29 | $2.14 | — | $2,385.26 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer; 🔵; ret5=+16.1; leftover $1153.35 | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 34 | $33.14 | $2.09 | — | $1,256.41 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer; 🔵; ret5=-2.9; leftover $1153.35 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 9 | $118.18 | $2.02 | — | $190.77 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $1153.35 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $190.77 | ▲ close $9,328.76 vs 09:30 $9,250.41 (session +130.16) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $190.77 | ▲ 09:30 equity $9,504.28 vs yday $9,328.76 (+175.52) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 2 | $10.25 | $0.21 | — | $170.06 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $23.85 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 9 | $2.40 | $0.24 | — | $148.22 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $23.85 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 1 | $18.04 | $0.18 | — | $130.00 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $23.85 | — |
| 2026-09-17 09:30 ET | **BUY** | `EMAT` | 6 | $3.86 | $0.25 | — | $106.59 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; 🔵; ret5=+18.7; leftover $23.85 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.59 | ▲ close $10,165.67 vs 09:30 $9,504.28 (session +662.28) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.59 | ▼ 09:30 equity $10,116.01 vs yday $10,165.67 (-49.66) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 3 | $3.95 | $0.13 | — | $94.61 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $15.23 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 2 | $5.83 | $0.12 | — | $82.83 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $15.23 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 5 | $3.04 | $0.17 | — | $67.49 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $15.23 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.49 | ▼ close $9,943.65 vs 09:30 $10,116.01 (session -171.95) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.49 | ▲ 09:30 equity $10,094.82 vs yday $9,943.65 (+151.17) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `QTRX` | 424 | $3.13 | $5.55 | $+162.82 | $1,389.06 | ▲ +162.82 after sell → book $10,089.26; vs 09:30 mark -5.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `HLP` | 640 | $2.08 | $8.37 | $+162.57 | $2,711.89 | ▲ +162.57 after sell → book $10,080.89; vs 09:30 mark -8.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 315 | $3.55 | $4.13 | $-42.84 | $3,826.01 | ▼ -42.84 after sell → book $10,076.77; vs 09:30 mark -4.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SWKS` | 12 | $89.66 | $2.05 | $-0.71 | $4,899.88 | ▼ -0.71 after sell → book $10,074.72; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `FTRE` | 58 | $20.29 | $2.18 | $+26.97 | $6,074.52 | ▲ +26.97 after sell → book $10,072.54; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 49 | $29.43 | $2.16 | $+296.56 | $7,514.43 | ▲ +296.56 after sell → book $10,070.38; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `FPS` | 34 | $40.03 | $2.11 | $+230.06 | $8,873.34 | ▲ +230.06 after sell → book $10,068.26; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QRVO` | 9 | $118.44 | $2.04 | $-1.71 | $9,937.26 | ▼ -1.71 after sell → book $10,066.23; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 7 | $157.87 | $2.01 | — | $8,830.16 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+6.5; leftover $1242.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 92 | $13.47 | $2.27 | — | $7,588.20 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1242.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 680 | $1.82 | $8.77 | — | $6,338.42 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1242.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 73 | $16.91 | $2.21 | — | $5,101.78 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; ret5=+50.5; leftover $1242.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 215 | $5.75 | $2.77 | — | $3,861.69 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; 🔵; ret5=+30.3; leftover $1242.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 106 | $11.67 | $2.31 | — | $2,622.36 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; ret5=+31.3; leftover $1242.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 502 | $2.47 | $6.48 | — | $1,375.94 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; ret5=+73.6; leftover $1242.16 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,375.94 | ▲ close $10,326.18 vs 09:30 $10,094.82 (session +286.77) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,375.94 | ▼ 09:30 equity $10,265.52 vs yday $10,326.18 (-60.66) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 2 | $10.18 | $0.23 | $-0.58 | $1,396.07 | ▼ -0.58 after sell → book $10,265.29; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `CIFR` | 1 | $18.51 | $0.21 | $+0.08 | $1,414.37 | ▲ +0.08 after sell → book $10,265.08; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 22 | $9.11 | $2.06 | — | $1,211.90 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; ret5=+44.4; leftover $202.05 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 200 | $1.01 | $2.59 | — | $1,007.31 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; ret5=+14.3; leftover $202.05 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 27 | $7.23 | $2.03 | — | $810.07 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; ret5=+36.6; leftover $202.05 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $810.07 | ▼ close $10,256.82 vs 09:30 $10,265.52 (session -1.58) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $810.07 | ▲ 09:30 equity $10,385.21 vs yday $10,256.82 (+128.39) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `SABR` | 9 | $2.24 | $0.25 | $-1.93 | $829.98 | ▼ -1.93 after sell → book $10,384.96; vs 09:30 mark -0.25 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `EMAT` | 6 | $3.65 | $0.26 | $-1.77 | $851.62 | ▼ -1.77 after sell → book $10,384.70; vs 09:30 mark -0.26 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `EYPT` | 3 | $4.10 | $0.15 | $+0.17 | $863.77 | ▲ +0.17 after sell → book $10,384.55; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BNC` | 2 | $6.29 | $0.15 | $+0.65 | $876.20 | ▲ +0.65 after sell → book $10,384.40; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `CYPH` | 5 | $3.82 | $0.23 | $+3.53 | $895.07 | ▲ +3.53 after sell → book $10,384.17; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 4 | $27.79 | $1.12 | — | $782.79 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+7.0; leftover $127.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 16 | $7.95 | $1.32 | — | $654.27 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+12.4; leftover $127.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 6 | $20.25 | $1.23 | — | $531.53 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+15.0; leftover $127.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 32 | $3.93 | $1.35 | — | $404.42 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $127.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 5 | $25.40 | $1.28 | — | $276.13 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $127.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLLN` | 1 | $116.00 | $1.16 | — | $158.97 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $127.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 3 | $41.76 | $1.26 | — | $32.43 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $127.87 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.43 | ▲ close $10,650.23 vs 09:30 $10,385.21 (session +274.80) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.43 | ▲ 09:30 equity $10,684.10 vs yday $10,650.23 (+33.87) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 7 | $163.95 | $2.03 | $+38.52 | $1,178.05 | ▲ +38.52 after sell → book $10,682.07; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 92 | $12.26 | $2.29 | $-116.34 | $2,303.68 | ▼ -116.34 after sell → book $10,679.78; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTBT` | 680 | $1.73 | $8.89 | $-85.67 | $3,467.78 | ▼ -85.67 after sell → book $10,670.89; vs 09:30 mark -8.89 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GEMI` | 215 | $5.62 | $2.82 | $-34.62 | $4,673.26 | ▼ -34.62 after sell → book $10,668.07; vs 09:30 mark -2.82 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 502 | $2.68 | $6.57 | $+92.37 | $6,012.05 | ▲ +92.37 after sell → book $10,661.50; vs 09:30 mark -6.57 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,012.05 | ▲ close $11,207.31 vs 09:30 $10,684.10 (session +545.82) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,016.83 | ▲ 09:30 equity $9,436.43 vs yday $9,321.14 (+115.29) | 09:30 open · cash $6,016.83 (unchanged overnight, no fees) · equity $9,436.43 vs prior close $9,321.14 (+115.29) · 13 name(s) re-marked at the open (per-name table). ADMA×4 yday $9.52 → 09:30 $9.52 +0.00; APPS×13 yday $10.88 → 09:30 $10.88 +0.00; ARQQ×6 yday $23.12 → 09:30 $23.12 +0.00; ARQT×1 yday $26.27 → 09:30 $26.27 +0.00; BTQ×57 yday $2.79 → 09:30 $2.79 +0.00; FTRE×2 yday $20.02 → 09:30 $20.02 +0.00; GRAL×1 yday $125.21 → 09:30 $123.50 -1.71; INDP×12 yday $4.00 → 09:30 $4.00 +0.00; IVVD×162 yday $0.91 → 09:30 $0.91 +0.00; NUAI×22 yday $6.94 → 09:30 $6.94 +0.00; TDC×5 yday $29.46 → 09:30 $29.46 +0.00; TJGC×75 yday $28.20 → 09:30 $29.76 +117.00; TNGX×1 yday $24.63 → 09:30 $24.63 +0.00 | — |
| 2026-09-25 09:30 ET | **SELL** | `GRAL` | 1 | $123.50 | $1.26 | $+14.42 | $6,139.07 | ▲ +14.42 after sell → book $9,435.17; vs 09:30 mark -1.26 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SELL** | `TJGC` | 75 | $29.76 | $2.25 | $+959.29 | $8,368.83 | ▲ +959.29 after sell → book $9,432.92; vs 09:30 mark -2.25 | dropped from list after 4 sess (min 3) | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 50 | $20.61 | $2.14 | — | $7,336.19 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+9.1; leftover $1046.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 27 | $38.51 | $2.07 | — | $6,294.35 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+4.7; leftover $1046.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 136 | $7.65 | $2.40 | — | $5,251.55 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1046.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 39 | $26.27 | $2.11 | — | $4,224.91 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1046.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $3,217.76 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1046.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 475 | $2.20 | $6.13 | — | $2,166.64 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1046.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 174 | $6.00 | $2.51 | — | $1,120.13 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $1046.10 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 58 | $17.91 | $2.16 | — | $79.18 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable; 🔵; ret5=+3.7; leftover $1046.10 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.18 | ▲ close $9,424.59 vs 09:30 $9,436.43 (session +13.21) | 16:00 close · cash $79.18 · equity $9,424.59 vs 09:30 $9,436.43 (-11.84; session marks +13.21) · 19 name(s) marked open→close (per-name table). ADMA×4 09:30 $9.52 → close $9.52 +0.00; APPS×13 09:30 $10.88 → close $10.88 +0.00; ARQQ×6 09:30 $23.12 → close $23.12 +0.00; ARQT×1 09:30 $26.27 → close $26.27 +0.00; BTQ×57 09:30 $2.79 → close $2.79 -0.00; FTRE×2 09:30 $20.02 → close $20.02 +0.00; INDP×12 09:30 $4.00 → close $4.00 +0.00; IVVD×162 09:30 $0.91 → close $0.91 -0.00; NUAI×22 09:30 $6.94 → close $6.94 +0.00; TDC×5 09:30 $29.46 → close $29.46 -0.00; TNGX×1 09:30 $24.63 → close $24.63 -0.00; OMER×50 09:30 $20.61 → close $20.08 -26.50; BLFS×27 09:30 $38.51 → close $38.49 -0.54; MRVI×136 09:30 $7.65 → close $7.60 -6.80; WRBY×39 09:30 $26.27 → close $26.71 +17.16; TXG×12 09:30 $83.76 → close $85.71 +23.40; HLP×475 09:30 $2.20 → close $2.21 +4.75; SATL×174 09:30 $6.00 → close $6.17 +29.58; PL×58 09:30 $17.91 → close $17.43 -27.84 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 10.14 < 1 share @ 359.83 |
| 2026-08-14 | `SLG` | cash | leftover split 10.14 < 1 share @ 57.61 |
| 2026-08-14 | `ADUR` | cash | leftover split 10.14 < 1 share @ 16.50 |
| 2026-08-14 | `ALGM` | cash | leftover split 10.14 < 1 share @ 44.06 |
| 2026-08-14 | `ARX` | cash | leftover split 10.14 < 1 share @ 19.57 |
| 2026-08-14 | `AIRO` | cash | leftover split 10.14 < 1 share @ 11.12 |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 7.94 < 1 share @ 46.18 |
| 2026-08-17 | `FANG` | cash | leftover split 7.94 < 1 share @ 202.70 |
| 2026-08-17 | `TGB` | cash | leftover split 7.94 < 1 share @ 8.46 |
| 2026-08-17 | `ELF` | cash | leftover split 7.94 < 1 share @ 90.54 |
| 2026-08-17 | `CELC` | cash | leftover split 7.94 < 1 share @ 92.99 |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 11.98 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 11.98 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 11.98 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 11.98 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `INSP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 8.38 < 1 share @ 41.44 |
| 2026-08-27 | `CRK` | cash | leftover split 8.38 < 1 share @ 14.42 |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 43.18 < 1 share @ 263.36 |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SSL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `KEP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `SANM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `COHU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `FUBO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RDDT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VERI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CMRC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `COHU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `FUBO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RDDT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INDP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `HLP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `FTRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SDGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `FPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RVTY` | cash | leftover split 23.85 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 23.85 < 1 share @ 34.93 |
| 2026-09-17 | `ARQT` | cash | leftover split 23.85 < 1 share @ 25.95 |
| 2026-09-17 | `SMTC` | cash | leftover split 23.85 < 1 share @ 170.85 |
| 2026-09-18 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `HLP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `FTRE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `FPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `CIFR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `EMAT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 15.23 < 1 share @ 108.55 |
| 2026-09-18 | `GNRC` | cash | leftover split 15.23 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 15.23 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 15.23 < 1 share @ 85.00 |
| 2026-09-21 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `CIFR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `EMAT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SNDK` | cash | leftover split 1242.16 < 1 share @ 1826.00 |
| 2026-09-22 | `SABR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `EMAT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TJGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GEMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `TJGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GEMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SECZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `FEAM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CRML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `IVVD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `NUAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CRML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `IVVD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `NUAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `TNGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BLLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VKTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `GLND` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `TJGC` | 73 | 2026-09-21 @ $16.91 | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; ret5=+50.5; leftover $1242.16 |
| `SECZ` | 106 | 2026-09-21 @ $11.67 | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; ret5=+31.3; leftover $1242.16 |
| `CRML` | 22 | 2026-09-22 @ $9.11 | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; ret5=+44.4; leftover $202.05 |
| `IVVD` | 200 | 2026-09-22 @ $1.01 | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; ret5=+14.3; leftover $202.05 |
| `NUAI` | 27 | 2026-09-22 @ $7.23 | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; ret5=+36.6; leftover $202.05 |
| `ARQT` | 4 | 2026-09-23 @ $27.79 | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+7.0; leftover $127.87 |
| `PGEN` | 16 | 2026-09-23 @ $7.95 | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+12.4; leftover $127.87 |
| `FTRE` | 6 | 2026-09-23 @ $20.25 | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+15.0; leftover $127.87 |
| `INDP` | 32 | 2026-09-23 @ $3.93 | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $127.87 |
| `TNGX` | 5 | 2026-09-23 @ $25.40 | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $127.87 |
| `BLLN` | 1 | 2026-09-23 @ $116.00 | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $127.87 |
| `VKTX` | 3 | 2026-09-23 @ $41.76 | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $127.87 |
