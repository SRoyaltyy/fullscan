# Factor mine action — `union_break10_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ break10, no 🚨

Cash book **+6.81%** ($10,681) · signal-only (no cash/fees) was +49.44%. Starts YES **25/30**. Fills 170 · skips 298 · realized $+167.97.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the name broke its prior 10-session range.
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
- **Gate** `break_10=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6,636.07.

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
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 217 | $45.98 | $2.80 | — | $19.54 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ⚪; ret5=+12.3; leftover $10000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.54 | ▼ close $9,732.46 vs 09:30 $10,000.00 (session -264.74) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.54 | ▼ 09:30 equity $9,587.07 vs yday $9,732.46 (-145.39) | — | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.54 | ▼ close $9,580.56 vs 09:30 $9,587.07 (session -6.51) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.54 | ▲ 09:30 equity $9,834.45 vs yday $9,580.56 (+253.89) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 1 | $1.92 | $0.02 | — | $17.60 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $2.44 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.60 | ▼ close $9,762.63 vs 09:30 $9,834.45 (session -71.80) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.60 | ▼ 09:30 equity $9,471.82 vs yday $9,762.63 (-290.81) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 217 | $43.56 | $2.91 | $-530.85 | $9,467.21 | ▼ -530.85 after sell → book $9,468.91; vs 09:30 mark -2.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,467.21 | ▼ close $9,468.86 vs 09:30 $9,471.82 (session -0.05) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,467.21 | ▲ 09:30 equity $9,468.91 vs yday $9,468.86 (+0.05) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,467.21 | ▼ close $9,468.88 vs 09:30 $9,468.91 (session -0.03) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,467.21 | ▼ 09:30 equity $9,468.85 vs yday $9,468.88 (-0.03) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `NPWR` | 1 | $1.64 | $0.04 | $-0.34 | $9,468.81 | ▼ -0.34 after sell → book $9,468.81; vs 09:30 mark -0.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 57 | $20.55 | $2.16 | — | $8,295.30 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1183.60 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,110.14 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1183.60 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $5,930.93 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1183.60 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $4,750.96 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1183.60 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $3,593.28 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1183.60 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 676 | $1.75 | $8.72 | — | $2,401.56 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1183.60 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,243.23 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1183.60 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 240 | $4.92 | $3.10 | — | $59.33 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1183.60 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.33 | ▲ close $9,673.19 vs 09:30 $9,468.85 (session +228.84) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.33 | ▲ 09:30 equity $10,012.21 vs yday $9,673.19 (+339.02) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 5 | $1.32 | $0.08 | — | $52.65 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $7.42 | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 8 | $0.86 | $0.09 | — | $45.64 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $7.42 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 25 | $0.29 | $0.15 | — | $38.15 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $7.42 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.15 | ▲ close $10,022.49 vs 09:30 $10,012.21 (session +10.60) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.15 | ▲ 09:30 equity $10,102.47 vs yday $10,022.49 (+79.98) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.15 | ▲ close $10,110.57 vs 09:30 $10,102.47 (session +8.09) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.15 | ▼ 09:30 equity $9,962.36 vs yday $10,110.57 (-148.21) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 57 | $20.32 | $2.18 | $-17.45 | $1,194.20 | ▼ -17.45 after sell → book $9,960.17; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.86 | $2.05 | $+58.97 | $2,438.34 | ▲ +58.97 after sell → book $9,958.13; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 57 | $20.47 | $2.18 | $-14.60 | $3,602.94 | ▼ -14.60 after sell → book $9,955.94; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 60 | $21.21 | $2.19 | $+90.44 | $4,873.35 | ▲ +90.44 after sell → book $9,953.75; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 39 | $32.32 | $2.13 | $+100.68 | $6,131.71 | ▲ +100.68 after sell → book $9,951.63; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 676 | $1.90 | $8.84 | $+83.84 | $7,407.26 | ▲ +83.84 after sell → book $9,942.78; vs 09:30 mark -8.85 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $8,657.31 | ▲ +91.71 after sell → book $9,940.75; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 240 | $5.25 | $3.15 | $+72.96 | $9,914.16 | ▲ +72.96 after sell → book $9,937.60; vs 09:30 mark -3.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 38 | $36.96 | $2.10 | — | $8,507.58 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1416.31 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 868 | $1.63 | $11.20 | — | $7,081.54 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1416.31 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 270 | $5.24 | $3.48 | — | $5,663.26 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1416.31 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 161 | $8.79 | $2.47 | — | $4,245.60 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1416.31 | — |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 248 | $5.71 | $3.20 | — | $2,826.32 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $1416.31 | — |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 398 | $3.55 | $5.13 | — | $1,408.28 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+27.9; leftover $1416.31 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 73 | $19.04 | $2.21 | — | $16.15 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+49.5; leftover $1416.31 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.15 | ▲ close $10,475.86 vs 09:30 $9,962.36 (session +568.05) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.15 | ▼ 09:30 equity $10,366.88 vs yday $10,475.86 (-108.98) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 5 | $1.60 | $0.12 | $+1.20 | $24.04 | ▲ +1.20 after sell → book $10,366.76; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ORBS` | 8 | $0.80 | $0.11 | $-0.74 | $30.30 | ▼ -0.74 after sell → book $10,366.65; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CAN` | 25 | $0.40 | $0.19 | $+2.23 | $40.03 | ▲ +2.23 after sell → book $10,366.46; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `CNTN` | 2 | $2.29 | $0.05 | — | $35.40 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot; 🔵; ret5=+14.9; leftover $5.72 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.40 | ▼ close $10,312.88 vs 09:30 $10,366.88 (session -53.53) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.40 | ▲ 09:30 equity $10,532.34 vs yday $10,312.88 (+219.46) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1 | $2.60 | $0.03 | — | $32.77 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot; ret5=+13.0; leftover $4.42 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.77 | ▲ close $10,611.53 vs 09:30 $10,532.34 (session +79.22) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.77 | ▼ 09:30 equity $10,604.47 vs yday $10,611.53 (-7.06) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 38 | $39.60 | $2.13 | $+96.09 | $1,535.44 | ▲ +96.09 after sell → book $10,602.34; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 868 | $1.69 | $11.35 | $+29.53 | $2,991.01 | ▲ +29.53 after sell → book $10,590.99; vs 09:30 mark -11.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 270 | $4.84 | $3.54 | $-115.02 | $4,294.27 | ▼ -115.02 after sell → book $10,587.45; vs 09:30 mark -3.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUJA` | 161 | $9.08 | $2.51 | $+41.71 | $5,753.64 | ▲ +41.71 after sell → book $10,584.94; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWDI` | 248 | $6.73 | $3.25 | $+246.51 | $7,419.43 | ▲ +246.51 after sell → book $10,581.69; vs 09:30 mark -3.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GORO` | 398 | $3.80 | $5.21 | $+89.15 | $8,926.61 | ▲ +89.15 after sell → book $10,576.47; vs 09:30 mark -5.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ASST` | 73 | $22.50 | $2.23 | $+248.14 | $10,566.88 | ▲ +248.14 after sell → book $10,574.24; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 155 | $9.73 | $2.46 | — | $9,056.28 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+47.1; leftover $1509.55 | — |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 165 | $9.13 | $2.48 | — | $7,547.34 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+20.0; leftover $1509.55 | — |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 3 | $461.85 | $2.00 | — | $6,159.79 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+16.8; leftover $1509.55 | — |
| 2026-08-28 09:30 ET | **BUY** | `SRPT` | 70 | $21.49 | $2.20 | — | $4,653.29 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+12.3; leftover $1509.55 | — |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 82 | $18.36 | $2.24 | — | $3,145.54 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+12.8; leftover $1509.55 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 64 | $23.30 | $2.18 | — | $1,652.15 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ret5=+14.5; leftover $1509.55 | — |
| 2026-08-28 09:30 ET | **BUY** | `DJT` | 155 | $9.72 | $2.46 | — | $143.10 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+14.8; leftover $1509.55 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $143.10 | ▼ close $10,318.15 vs 09:30 $10,604.47 (session -240.08) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $143.10 | ▼ 09:30 equity $10,192.22 vs yday $10,318.15 (-125.93) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CNTN` | 2 | $2.23 | $0.07 | $-0.24 | $147.49 | ▼ -0.24 after sell → book $10,192.15; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $147.49 | ▲ close $10,289.15 vs 09:30 $10,192.22 (session +97.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $147.49 | ▼ 09:30 equity $10,285.13 vs yday $10,289.15 (-4.02) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 1 | $2.67 | $0.05 | $-0.01 | $150.11 | ▼ -0.01 after sell → book $10,285.08; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $150.11 | ▼ close $10,143.72 vs 09:30 $10,285.13 (session -141.36) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $150.11 | ▼ 09:30 equity $10,134.65 vs yday $10,143.72 (-9.07) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 155 | $10.07 | $2.49 | $+47.75 | $1,708.47 | ▲ +47.75 after sell → book $10,132.16; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `VYX` | 165 | $8.73 | $2.52 | $-71.01 | $3,146.39 | ▼ -71.01 after sell → book $10,129.63; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SNPS` | 3 | $413.78 | $2.02 | $-148.23 | $4,385.71 | ▼ -148.23 after sell → book $10,127.61; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SRPT` | 70 | $21.33 | $2.22 | $-15.62 | $5,876.59 | ▼ -15.62 after sell → book $10,125.39; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NEO` | 82 | $17.40 | $2.26 | $-83.22 | $7,301.13 | ▼ -83.22 after sell → book $10,123.13; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NCNO` | 64 | $22.20 | $2.20 | $-74.79 | $8,719.72 | ▼ -74.79 after sell → book $10,120.92; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DJT` | 155 | $9.04 | $2.49 | $-110.35 | $10,118.43 | ▼ -110.35 after sell → book $10,118.43; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,118.43 | ▲ close $10,118.43 vs 09:30 $10,134.65 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,118.43 | ▲ 09:30 equity $10,118.43 vs yday $10,118.43 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $8,900.13 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1264.80 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $7,653.09 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1264.80 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 348 | $3.63 | $4.49 | — | $6,385.36 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1264.80 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 157 | $8.03 | $2.46 | — | $5,122.19 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1264.80 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $3,928.12 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1264.80 | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 710 | $1.78 | $9.16 | — | $2,655.16 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+183.1; leftover $1264.80 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 52 | $23.88 | $2.15 | — | $1,411.25 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1264.80 | — |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 930 | $1.36 | $12.00 | — | $134.46 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1264.80 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $134.46 | ▼ close $9,592.93 vs 09:30 $10,118.43 (session -489.10) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $134.46 | ▼ 09:30 equity $9,585.25 vs yday $9,592.93 (-7.68) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 3 | $4.53 | $0.14 | — | $120.72 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $16.81 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 6 | $2.51 | $0.17 | — | $105.49 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $16.81 | — |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 2 | $5.75 | $0.12 | — | $93.87 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $16.81 | — |
| 2026-09-04 09:30 ET | **BUY** | `SCZM` | 1 | $10.03 | $0.10 | — | $83.74 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; ret5=+4.0; leftover $16.81 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 2 | $5.79 | $0.12 | — | $72.04 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $16.81 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.04 | ▲ close $9,765.44 vs 09:30 $9,585.25 (session +180.85) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.04 | ▼ 09:30 equity $9,751.35 vs yday $9,765.44 (-14.09) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.04 | ▼ close $9,539.00 vs 09:30 $9,751.35 (session -212.35) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.04 | ▼ 09:30 equity $9,536.50 vs yday $9,539.00 (-2.50) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 23 | $53.16 | $2.08 | $+2.30 | $1,292.64 | ▲ +2.30 after sell → book $9,534.42; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 29 | $42.01 | $2.10 | $-30.85 | $2,508.83 | ▼ -30.85 after sell → book $9,532.32; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 348 | $3.28 | $4.56 | $-130.85 | $3,645.71 | ▼ -130.85 after sell → book $9,527.76; vs 09:30 mark -4.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 157 | $8.01 | $2.50 | $-8.10 | $4,900.79 | ▼ -8.10 after sell → book $9,525.27; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $6,030.68 | ▼ -64.17 after sell → book $9,523.23; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `GPRO` | 710 | $1.45 | $9.29 | $-252.75 | $7,050.89 | ▼ -252.75 after sell → book $9,513.94; vs 09:30 mark -9.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 52 | $23.22 | $2.17 | $-38.63 | $8,256.17 | ▼ -38.63 after sell → book $9,511.78; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SID` | 930 | $1.28 | $12.16 | $-98.56 | $9,434.41 | ▼ -98.56 after sell → book $9,499.62; vs 09:30 mark -12.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,434.41 | ▲ close $9,499.89 vs 09:30 $9,536.50 (session +0.27) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,434.41 | ▼ 09:30 equity $9,499.31 vs yday $9,499.89 (-0.58) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `BRR` | 6 | $2.87 | $0.21 | $+1.78 | $9,451.42 | ▲ +1.78 after sell → book $9,499.10; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `LENZ` | 2 | $4.85 | $0.12 | $-2.04 | $9,460.99 | ▼ -2.04 after sell → book $9,498.97; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `SCZM` | 1 | $9.93 | $0.12 | $-0.33 | $9,470.80 | ▼ -0.33 after sell → book $9,498.85; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DFDV` | 2 | $5.22 | $0.13 | $-1.39 | $9,481.11 | ▼ -1.39 after sell → book $9,498.72; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,481.11 | ▲ close $9,499.32 vs 09:30 $9,499.31 (session +0.60) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,481.11 | ▲ 09:30 equity $9,499.59 vs yday $9,499.32 (+0.27) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `IRD` | 3 | $6.16 | $0.21 | $+4.53 | $9,499.38 | ▲ +4.53 after sell → book $9,499.38; vs 09:30 mark -0.21 | dropped from list after 4 sess (min 3) | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 582 | $2.04 | $7.51 | — | $8,304.59 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1187.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 249 | $4.75 | $3.21 | — | $7,118.63 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1187.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 560 | $2.12 | $7.22 | — | $5,924.20 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1187.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 439 | $2.70 | $5.66 | — | $4,733.24 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1187.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 379 | $3.13 | $4.89 | — | $3,542.08 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+24.2; leftover $1187.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 108 | $10.95 | $2.31 | — | $2,357.17 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1187.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 200 | $5.91 | $2.59 | — | $1,172.58 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; leftover $1187.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 13 | $84.27 | $2.03 | — | $75.04 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1187.42 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.04 | ▲ close $9,599.85 vs 09:30 $9,499.59 (session +135.90) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.04 | ▼ 09:30 equity $9,554.63 vs yday $9,599.85 (-45.22) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.04 | ▲ close $9,850.04 vs 09:30 $9,554.63 (session +295.41) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.04 | ▲ 09:30 equity $9,948.02 vs yday $9,850.04 (+97.98) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.04 | ▲ close $10,071.74 vs 09:30 $9,948.02 (session +123.72) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.04 | ▼ 09:30 equity $9,889.35 vs yday $10,071.74 (-182.39) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 582 | $1.89 | $7.61 | $-102.42 | $1,167.40 | ▼ -102.42 after sell → book $9,881.73; vs 09:30 mark -7.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 249 | $4.73 | $3.26 | $-11.46 | $2,341.91 | ▼ -11.46 after sell → book $9,878.47; vs 09:30 mark -3.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAK` | 560 | $1.84 | $7.33 | $-171.35 | $3,364.98 | ▼ -171.35 after sell → book $9,871.14; vs 09:30 mark -7.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CMRC` | 379 | $3.48 | $4.96 | $+122.80 | $4,678.94 | ▲ +122.80 after sell → book $9,866.18; vs 09:30 mark -4.96 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `WLTH` | 108 | $10.82 | $2.34 | $-18.70 | $5,845.16 | ▼ -18.70 after sell → book $9,863.84; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DBI` | 200 | $6.25 | $2.63 | $+62.78 | $7,092.52 | ▲ +62.78 after sell → book $9,861.20; vs 09:30 mark -2.64 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SWKS` | 13 | $89.38 | $2.05 | $+62.35 | $8,252.42 | ▲ +62.35 after sell → book $9,859.16; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 15 | $77.12 | $2.04 | — | $7,093.58 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot; ret5=+7.2; leftover $1178.92 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 17 | $68.79 | $2.04 | — | $5,922.11 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1178.92 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 43 | $27.09 | $2.12 | — | $4,755.12 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1178.92 | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 654 | $1.80 | $8.44 | — | $3,569.48 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $1178.92 | — |
| 2026-09-16 09:30 ET | **BUY** | `FTRE` | 59 | $19.75 | $2.17 | — | $2,402.07 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+15.7; leftover $1178.92 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 50 | $23.29 | $2.14 | — | $1,235.43 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+16.1; leftover $1178.92 | — |
| 2026-09-16 09:30 ET | **BUY** | `REF` | 74 | $15.75 | $2.21 | — | $67.71 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot; ret5=+17.3; leftover $1178.92 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.71 | ▲ close $9,885.43 vs 09:30 $9,889.35 (session +47.43) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.71 | ▲ 09:30 equity $10,061.89 vs yday $9,885.43 (+176.46) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `INDP` | 439 | $3.30 | $5.75 | $+251.99 | $1,510.67 | ▲ +251.99 after sell → book $10,056.15; vs 09:30 mark -5.74 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 1 | $151.43 | $1.52 | — | $1,357.72 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $188.83 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 1 | $147.61 | $1.48 | — | $1,208.63 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot; ret5=+17.7; leftover $188.83 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 18 | $10.25 | $1.90 | — | $1,022.23 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $188.83 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 24 | $7.59 | $1.89 | — | $838.18 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $188.83 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 5 | $34.93 | $1.76 | — | $661.77 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ret5=+1.6; leftover $188.83 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 7 | $25.95 | $1.84 | — | $478.28 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $188.83 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 78 | $2.40 | $2.11 | — | $288.97 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $188.83 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $288.97 | ▲ close $10,422.44 vs 09:30 $10,061.89 (session +378.79) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $288.97 | ▼ 09:30 equity $10,337.02 vs yday $10,422.44 (-85.42) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 11 | $3.58 | $0.43 | — | $249.17 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $41.28 | — |
| 2026-09-18 09:30 ET | **BUY** | `INDP` | 10 | $3.85 | $0.41 | — | $210.25 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+53.5; leftover $41.28 | — |
| 2026-09-18 09:30 ET | **BUY** | `LVWR` | 27 | $1.49 | $0.48 | — | $169.54 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+25.7; leftover $41.28 | — |
| 2026-09-18 09:30 ET | **BUY** | `VITL` | 3 | $11.38 | $0.35 | — | $135.05 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+19.5; leftover $41.28 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $135.05 | ▼ close $10,221.44 vs 09:30 $10,337.02 (session -113.91) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $135.05 | ▲ 09:30 equity $10,353.98 vs yday $10,221.44 (+132.54) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 15 | $76.27 | $2.06 | $-16.84 | $1,277.04 | ▼ -16.84 after sell → book $10,351.92; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 17 | $79.08 | $2.06 | $+170.83 | $2,619.34 | ▲ +170.83 after sell → book $10,349.86; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ADPT` | 43 | $28.69 | $2.14 | $+64.54 | $3,850.87 | ▲ +64.54 after sell → book $10,347.72; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `HLP` | 654 | $2.08 | $8.56 | $+166.13 | $5,202.64 | ▲ +166.13 after sell → book $10,339.17; vs 09:30 mark -8.55 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `FTRE` | 59 | $20.29 | $2.19 | $+27.51 | $6,397.56 | ▲ +27.51 after sell → book $10,336.98; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 50 | $29.43 | $2.16 | $+302.70 | $7,866.90 | ▲ +302.70 after sell → book $10,334.82; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `REF` | 74 | $14.79 | $2.23 | $-75.49 | $8,959.12 | ▼ -75.49 after sell → book $10,332.58; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 83 | $13.47 | $2.24 | — | $7,838.46 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1119.89 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 66 | $16.91 | $2.19 | — | $6,720.21 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+50.5; leftover $1119.89 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 85 | $13.05 | $2.25 | — | $5,608.72 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $1119.89 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 194 | $5.75 | $2.57 | — | $4,489.67 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+30.3; leftover $1119.89 | — |
| 2026-09-21 09:30 ET | **BUY** | `FWDI` | 136 | $8.22 | $2.40 | — | $3,369.36 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1119.89 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 95 | $11.67 | $2.27 | — | $2,258.43 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+31.3; leftover $1119.89 | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 453 | $2.47 | $5.84 | — | $1,133.68 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+73.6; leftover $1119.89 | — |
| 2026-09-21 09:30 ET | **BUY** | `MSTR` | 6 | $164.58 | $2.01 | — | $144.19 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.5; leftover $1119.89 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $144.19 | ▲ close $10,515.69 vs 09:30 $10,353.98 (session +204.88) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $144.19 | ▼ 09:30 equity $10,504.37 vs yday $10,515.69 (-11.32) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 18 | $10.18 | $1.91 | $-5.07 | $325.52 | ▼ -5.07 after sell → book $10,502.46; vs 09:30 mark -1.91 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 5 | $9.11 | $0.47 | — | $279.50 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+44.4; leftover $46.50 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 46 | $1.01 | $0.60 | — | $232.44 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+14.3; leftover $46.50 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 6 | $7.23 | $0.45 | — | $188.61 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+36.6; leftover $46.50 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $188.61 | ▲ close $10,532.20 vs 09:30 $10,504.37 (session +31.26) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $188.61 | ▲ 09:30 equity $10,616.33 vs yday $10,532.20 (+84.13) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TWST` | 1 | $164.35 | $1.67 | $+9.74 | $351.29 | ▲ +9.74 after sell → book $10,614.66; vs 09:30 mark -1.67 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RVTY` | 1 | $142.40 | $1.45 | $-8.14 | $492.24 | ▼ -8.14 after sell → book $10,613.21; vs 09:30 mark -1.45 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `PGEN` | 24 | $7.95 | $2.00 | $+4.75 | $681.04 | ▲ +4.75 after sell → book $10,611.21; vs 09:30 mark -2.00 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMN` | 5 | $34.78 | $1.77 | $-4.29 | $853.17 | ▼ -4.29 after sell → book $10,609.44; vs 09:30 mark -1.77 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARQT` | 7 | $27.79 | $1.99 | $+9.06 | $1,045.71 | ▲ +9.06 after sell → book $10,607.45; vs 09:30 mark -1.99 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SABR` | 78 | $2.24 | $2.00 | $-16.59 | $1,218.43 | ▼ -16.59 after sell → book $10,605.45; vs 09:30 mark -2.00 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `DDD` | 11 | $3.59 | $0.45 | $-0.76 | $1,257.47 | ▼ -0.76 after sell → book $10,605.00; vs 09:30 mark -0.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `INDP` | 10 | $3.93 | $0.44 | $-0.06 | $1,296.33 | ▼ -0.06 after sell → book $10,604.56; vs 09:30 mark -0.44 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 27 | $1.41 | $0.48 | $-3.12 | $1,333.92 | ▼ -3.12 after sell → book $10,604.08; vs 09:30 mark -0.48 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `VITL` | 3 | $11.17 | $0.36 | $-1.34 | $1,367.06 | ▼ -1.34 after sell → book $10,603.71; vs 09:30 mark -0.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 1 | $166.54 | $1.67 | — | $1,198.85 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ⚪; ret5=+10.3; leftover $170.88 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 4 | $41.76 | $1.68 | — | $1,030.13 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $170.88 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 17 | $9.90 | $1.73 | — | $860.10 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $170.88 | — |
| 2026-09-23 09:30 ET | **BUY** | `EVTL` | 232 | $0.73 | $2.40 | — | $687.41 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+27.5; leftover $170.88 | — |
| 2026-09-23 09:30 ET | **BUY** | `INOD` | 2 | $70.84 | $1.42 | — | $544.31 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $170.88 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 38 | $4.49 | $1.82 | — | $371.87 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+26.4; leftover $170.88 | — |
| 2026-09-23 09:30 ET | **BUY** | `THM` | 59 | $2.86 | $1.86 | — | $201.26 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+25.5; leftover $170.88 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $201.26 | ▲ close $10,807.81 vs 09:30 $10,616.33 (session +216.69) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $201.26 | ▲ 09:30 equity $10,826.61 vs yday $10,807.81 (+18.80) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 83 | $12.26 | $2.26 | $-105.35 | $1,216.58 | ▼ -105.35 after sell → book $10,824.34; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `USDE` | 85 | $12.76 | $2.27 | $-29.16 | $2,298.91 | ▼ -29.16 after sell → book $10,822.07; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GEMI` | 194 | $5.62 | $2.61 | $-31.38 | $3,386.58 | ▼ -31.38 after sell → book $10,819.46; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `FWDI` | 136 | $7.94 | $2.43 | $-42.91 | $4,463.99 | ▼ -42.91 after sell → book $10,817.03; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 453 | $2.68 | $5.93 | $+83.36 | $5,672.10 | ▲ +83.36 after sell → book $10,811.10; vs 09:30 mark -5.93 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MSTR` | 6 | $161.00 | $2.03 | $-25.52 | $6,636.07 | ▼ -25.52 after sell → book $10,809.07; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,636.07 | ▲ close $11,289.01 vs 09:30 $10,826.61 (session +479.93) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,485.50 | ▲ 09:30 equity $10,714.29 vs yday $10,613.64 (+100.65) | 09:30 open · cash $7,485.50 (unchanged overnight, no fees) · equity $10,714.29 vs prior close $10,613.64 (+100.65) · 14 name(s) re-marked at the open (per-name table). ADMA×4 yday $9.52 → 09:30 $9.52 +0.00; AIBZ×35 yday $4.31 → 09:30 $4.31 +0.00; ARQQ×7 yday $23.12 → 09:30 $23.12 +0.00; ARQT×1 yday $26.27 → 09:30 $26.27 +0.00; FSLY×6 yday $26.68 → 09:30 $26.68 +0.00; GRAL×1 yday $125.21 → 09:30 $123.50 -1.71; GRPN×8 yday $20.89 → 09:30 $20.89 +0.00; IVVD×180 yday $0.91 → 09:30 $0.91 +0.00; MAZE×1 yday $26.21 → 09:30 $26.21 +0.00; NUAI×25 yday $6.94 → 09:30 $6.94 +0.00; OMER×2 yday $20.13 → 09:30 $20.61 +0.96; TJGC×65 yday $28.20 → 09:30 $29.76 +101.40; TNGX×1 yday $24.63 → 09:30 $24.63 +0.00; VKTX×1 yday $36.75 → 09:30 $36.75 +0.00 | — |
| 2026-09-25 09:30 ET | **SELL** | `GRAL` | 1 | $123.50 | $1.26 | $+14.42 | $7,607.74 | ▲ +14.42 after sell → book $10,713.03; vs 09:30 mark -1.26 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SELL** | `TJGC` | 65 | $29.76 | $2.21 | $+830.85 | $9,539.93 | ▲ +830.85 after sell → book $10,710.82; vs 09:30 mark -2.21 | dropped from list after 4 sess (min 3) | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 30 | $38.51 | $2.08 | — | $8,382.55 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ret5=+4.7; leftover $1192.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 155 | $7.65 | $2.46 | — | $7,194.35 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1192.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 45 | $26.27 | $2.12 | — | $6,010.07 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1192.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 14 | $83.76 | $2.03 | — | $4,835.40 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1192.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 542 | $2.20 | $6.99 | — | $3,636.01 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1192.49 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 198 | $6.00 | $2.58 | — | $2,445.42 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $1192.49 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 14 | $83.69 | $2.03 | — | $1,271.66 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $1192.49 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GLND` | 196 | $6.06 | $2.58 | — | $81.32 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+342.1; leftover $1192.49 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.32 | ▼ close $10,681.20 vs 09:30 $10,714.29 (session -6.74) | 16:00 close · cash $81.32 · equity $10,681.20 vs 09:30 $10,714.29 (-33.09; session marks -6.74) · 20 name(s) marked open→close (per-name table). ADMA×4 09:30 $9.52 → close $9.52 +0.00; AIBZ×35 09:30 $4.31 → close $4.31 -0.00; ARQQ×7 09:30 $23.12 → close $23.12 +0.00; ARQT×1 09:30 $26.27 → close $26.27 +0.00; FSLY×6 09:30 $26.68 → close $26.68 +0.00; GRPN×8 09:30 $20.89 → close $20.89 -0.00; IVVD×180 09:30 $0.91 → close $0.91 -0.00; MAZE×1 09:30 $26.21 → close $26.21 -0.00; NUAI×25 09:30 $6.94 → close $6.94 +0.00; OMER×2 09:30 $20.61 → close $20.08 -1.06; TNGX×1 09:30 $24.63 → close $24.63 -0.00; VKTX×1 09:30 $36.75 → close $36.75 +0.00; BLFS×30 09:30 $38.51 → close $38.49 -0.60; MRVI×155 09:30 $7.65 → close $7.60 -7.75; WRBY×45 09:30 $26.27 → close $26.71 +19.80; TXG×14 09:30 $83.76 → close $85.71 +27.30; HLP×542 09:30 $2.20 → close $2.21 +5.42; SATL×198 09:30 $6.00 → close $6.17 +33.66; TEM×14 09:30 $83.69 → close $85.01 +18.41; GLND×196 09:30 $6.06 → close $5.54 -101.92 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLG` | cash | leftover split 2.44 < 1 share @ 57.61 |
| 2026-08-14 | `ADUR` | cash | leftover split 2.44 < 1 share @ 16.50 |
| 2026-08-14 | `ARX` | cash | leftover split 2.44 < 1 share @ 19.57 |
| 2026-08-14 | `AIRO` | cash | leftover split 2.44 < 1 share @ 11.12 |
| 2026-08-14 | `TBBB` | cash | leftover split 2.44 < 1 share @ 48.82 |
| 2026-08-14 | `AMPY` | cash | leftover split 2.44 < 1 share @ 4.94 |
| 2026-08-14 | `SNDK` | cash | leftover split 2.44 < 1 share @ 1646.93 |
| 2026-08-14 | `MH` | cash | leftover split 2.44 < 1 share @ 13.55 |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 2.44 < 1 share @ 46.18 |
| 2026-08-17 | `OCC` | cash | leftover split 2.44 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 2.44 < 1 share @ 16.20 |
| 2026-08-17 | `CAPR` | cash | leftover split 2.44 < 1 share @ 6.87 |
| 2026-08-17 | `HTFL` | cash | leftover split 2.44 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 2.44 < 1 share @ 32.55 |
| 2026-08-17 | `LPTH` | cash | leftover split 2.44 < 1 share @ 14.94 |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ALEC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 7.42 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 7.42 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 7.42 < 1 share @ 216.30 |
| 2026-08-21 | `CF` | cash | leftover split 7.42 < 1 share @ 127.43 |
| 2026-08-21 | `MRVI` | cash | leftover split 7.42 < 1 share @ 8.28 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FWDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ABX` | cash | leftover split 5.72 < 1 share @ 9.83 |
| 2026-08-26 | `KURA` | cash | leftover split 5.72 < 1 share @ 13.63 |
| 2026-08-26 | `ACRS` | cash | leftover split 5.72 < 1 share @ 6.53 |
| 2026-08-26 | `FIGR` | cash | leftover split 5.72 < 1 share @ 40.50 |
| 2026-08-26 | `MNRO` | cash | leftover split 5.72 < 1 share @ 14.00 |
| 2026-08-26 | `FUTU` | cash | leftover split 5.72 < 1 share @ 124.67 |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FWDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CNTN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 4.42 < 1 share @ 41.44 |
| 2026-08-27 | `CAPR` | cash | leftover split 4.42 < 1 share @ 9.19 |
| 2026-08-27 | `BZ` | cash | leftover split 4.42 < 1 share @ 18.50 |
| 2026-08-27 | `OABI` | cash | leftover split 4.42 < 1 share @ 4.81 |
| 2026-08-27 | `AQST` | cash | leftover split 4.42 < 1 share @ 5.39 |
| 2026-08-27 | `VERA` | cash | leftover split 4.42 < 1 share @ 36.70 |
| 2026-08-27 | `VYX` | cash | leftover split 4.42 < 1 share @ 8.95 |
| 2026-08-28 | `CNTN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `VYX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SNPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SRPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NEO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NCNO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DJT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `APPN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CXM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CMRC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RCKT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `GWRE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `VYX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SNPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SRPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NEO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NCNO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DJT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SKYX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `METC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SID` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `GPRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 16.81 < 1 share @ 513.78 |
| 2026-09-04 | `TARS` | cash | leftover split 16.81 < 1 share @ 82.70 |
| 2026-09-04 | `ASST` | cash | leftover split 16.81 < 1 share @ 25.18 |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `GPRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SID` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `IRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `LENZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SECZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SKYX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HAFN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GSM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `IRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LENZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SSL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `KEP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SLDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `WLTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `DBI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VERI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HUT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NTAP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CMRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `WLTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `DBI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ADPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `HLP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `FTRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SDGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `REF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 188.83 < 1 share @ 233.85 |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `HLP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `FTRE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `REF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TWST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `AMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 41.28 < 1 share @ 108.55 |
| 2026-09-18 | `GNRC` | cash | leftover split 41.28 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 41.28 < 1 share @ 219.62 |
| 2026-09-21 | `TWST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `AMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `ARQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `DDD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `LVWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `VITL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TWST` | no_price | no 09:30 open — carry |
| 2026-09-22 | `RVTY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ARQT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SABR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DDD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `VITL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TJGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GEMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FWDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MSTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `ARM` | cash | leftover split 46.50 < 1 share @ 319.41 |
| 2026-09-22 | `ARQQ` | no_price | no 09:30 open |
| 2026-09-22 | `GRPN` | no_price | no 09:30 open |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `TJGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GEMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `FWDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SECZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `FEAM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CRML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `IVVD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `NUAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `VICR` | cash | leftover split 170.88 < 1 share @ 266.50 |
| 2026-09-24 | `CRML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `IVVD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `NUAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VKTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BFLY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `EVTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INOD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SVIA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `THM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CRWD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ASPN` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `TJGC` | 66 | 2026-09-21 @ $16.91 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+50.5; leftover $1119.89 |
| `SECZ` | 95 | 2026-09-21 @ $11.67 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+31.3; leftover $1119.89 |
| `CRML` | 5 | 2026-09-22 @ $9.11 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+44.4; leftover $46.50 |
| `IVVD` | 46 | 2026-09-22 @ $1.01 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+14.3; leftover $46.50 |
| `NUAI` | 6 | 2026-09-22 @ $7.23 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+36.6; leftover $46.50 |
| `A` | 1 | 2026-09-23 @ $166.54 | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ⚪; ret5=+10.3; leftover $170.88 |
| `VKTX` | 4 | 2026-09-23 @ $41.76 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $170.88 |
| `BFLY` | 17 | 2026-09-23 @ $9.90 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $170.88 |
| `EVTL` | 232 | 2026-09-23 @ $0.73 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+27.5; leftover $170.88 |
| `INOD` | 2 | 2026-09-23 @ $70.84 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $170.88 |
| `SVIA` | 38 | 2026-09-23 @ $4.49 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+26.4; leftover $170.88 |
| `THM` | 59 | 2026-09-23 @ $2.86 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+25.5; leftover $170.88 |
