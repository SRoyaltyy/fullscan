# Factor mine action — `union_macd_hist_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `macd_hist` · size `leftover` · sell `list` · S-boost `none` · rank by macd_hist

Cash book **-10.25%** ($8,975) · signal-only (no cash/fees) was -17.17%. Starts YES **5/30**. Fills 170 · skips 257 · realized $-743.63.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how positive the prior MACD histogram is.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by how positive the prior MACD histogram is and keep the top 8.
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
- **Gate** `none (list as ranked)` · **rank** `macd_hist` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $4,061.79.

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
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $8,756.47 | — | rank by macd_hist; rank macd_hist; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $7,539.45 | — | rank by macd_hist; rank macd_hist; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $6,300.81 | — | rank by macd_hist; rank macd_hist; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $5,066.09 | — | rank by macd_hist; rank macd_hist; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $3,814.90 | — | rank by macd_hist; rank macd_hist; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $2,547.94 | — | rank by macd_hist; rank macd_hist; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $1,305.43 | — | rank by macd_hist; rank macd_hist; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $60.87 | — | rank by macd_hist; rank macd_hist; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.87 | ▲ close $10,216.10 vs 09:30 $10,000.00 (session +248.15) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.87 | ▲ 09:30 equity $10,254.94 vs yday $10,216.10 (+38.84) | — | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.87 | ▲ close $10,454.40 vs 09:30 $10,254.94 (session +199.46) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.87 | ▼ 09:30 equity $10,427.69 vs yday $10,454.40 (-26.71) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.87 | ▲ close $10,569.79 vs 09:30 $10,427.69 (session +142.10) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.87 | ▼ 09:30 equity $10,434.55 vs yday $10,569.79 (-135.24) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $1,234.90 | ▼ -69.50 after sell → book $10,432.46; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $2,475.29 | ▲ +23.38 after sell → book $10,430.37; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $3,647.60 | ▼ -66.33 after sell → book $10,428.20; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 56 | $22.82 | $2.18 | $+41.02 | $4,923.35 | ▲ +41.02 after sell → book $10,426.03; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $6,090.91 | ▼ -83.63 after sell → book $10,423.89; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $7,829.76 | ▲ +471.89 after sell → book $10,403.72; vs 09:30 mark -20.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $9,169.38 | ▲ +97.12 after sell → book $10,401.38; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $10,399.29 | ▼ -14.65 after sell → book $10,399.29; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,399.29 | ▲ close $10,399.29 vs 09:30 $10,434.55 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,399.29 | ▲ 09:30 equity $10,399.29 vs yday $10,399.29 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,399.29 | ▲ close $10,399.29 vs 09:30 $10,399.29 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,399.29 | ▲ 09:30 equity $10,399.29 vs yday $10,399.29 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $9,196.16 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1299.91 | — |
| 2026-08-20 09:30 ET | **BUY** | `TEAM` | 7 | $173.90 | $2.01 | — | $7,976.85 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+12.2; leftover $1299.91 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEM` | 6 | $204.45 | $2.01 | — | $6,748.14 | — | rank by macd_hist; rank macd_hist; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $1299.91 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $5,589.81 | — | rank by macd_hist; rank macd_hist; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1299.91 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUGO` | 15 | $83.58 | $2.04 | — | $4,334.07 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ⚪; ret5=+9.6; leftover $1299.91 | — |
| 2026-08-20 09:30 ET | **BUY** | `TWST` | 9 | $136.84 | $2.02 | — | $3,100.50 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+13.7; leftover $1299.91 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRCL` | 15 | $82.99 | $2.04 | — | $1,853.61 | — | rank by macd_hist; rank macd_hist; list probable; 🔵; ⚪; ret5=+7.4; leftover $1299.91 | — |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 11 | $109.06 | $2.02 | — | $651.93 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $1299.91 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $651.93 | ▲ close $10,419.11 vs 09:30 $10,399.29 (session +35.97) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $651.93 | ▲ 09:30 equity $10,593.94 vs yday $10,419.11 (+174.83) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 1 | $119.43 | $1.20 | — | $531.30 | — | rank by macd_hist; rank macd_hist; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $162.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `MSTR` | 1 | $119.69 | $1.20 | — | $410.41 | — | rank by macd_hist; rank macd_hist; list yday_gainer,ohlc_hot; ret5=+15.7; leftover $162.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `TEM` | 2 | $65.60 | $1.32 | — | $277.89 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+22.8; leftover $162.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `SHAZ` | 2 | $61.46 | $1.24 | — | $153.74 | — | rank by macd_hist; rank macd_hist; list yday_mover; ret5=-15.5; leftover $162.98 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $153.74 | ▲ close $10,801.22 vs 09:30 $10,593.94 (session +212.23) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $153.74 | ▼ 09:30 equity $10,799.69 vs yday $10,801.22 (-1.53) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $153.74 | ▼ close $10,679.80 vs 09:30 $10,799.69 (session -119.89) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $153.74 | ▼ 09:30 equity $10,631.17 vs yday $10,679.80 (-48.63) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $143.50 | $2.03 | $-57.17 | $1,299.70 | ▼ -57.17 after sell → book $10,629.13; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TEAM` | 7 | $170.64 | $2.03 | $-26.86 | $2,492.15 | ▼ -26.86 after sell → book $10,627.10; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEM` | 6 | $212.00 | $2.03 | $+41.26 | $3,762.12 | ▲ +41.26 after sell → book $10,625.07; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUGO` | 15 | $85.78 | $2.06 | $+28.91 | $5,046.77 | ▲ +28.91 after sell → book $10,623.02; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TWST` | 9 | $145.73 | $2.04 | $+75.96 | $6,356.30 | ▲ +75.96 after sell → book $10,620.98; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRCL` | 15 | $84.73 | $2.06 | $+22.01 | $7,625.20 | ▲ +22.01 after sell → book $10,618.93; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BNTX` | 11 | $113.88 | $2.04 | $+48.95 | $8,875.83 | ▲ +48.95 after sell → book $10,616.88; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 61 | $24.11 | $2.17 | — | $7,402.95 | — | rank by macd_hist; rank macd_hist; list yday_mover; ret5=+891.7; leftover $1479.31 | — |
| 2026-08-25 09:30 ET | **BUY** | `INTU` | 4 | $364.35 | $2.00 | — | $5,943.55 | — | rank by macd_hist; rank macd_hist; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $1479.31 | — |
| 2026-08-25 09:30 ET | **BUY** | `ILMN` | 6 | $224.00 | $2.01 | — | $4,597.54 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+15.6; leftover $1479.31 | — |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 22 | $64.55 | $2.06 | — | $3,175.38 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+4.4; leftover $1479.31 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $1,892.47 | — | rank by macd_hist; rank macd_hist; list flatten; ret5=+6.0; leftover $1479.31 | — |
| 2026-08-25 09:30 ET | **BUY** | `WIX` | 17 | $83.15 | $2.04 | — | $476.88 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+14.5; leftover $1479.31 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $476.88 | ▲ close $10,946.59 vs 09:30 $10,631.17 (session +341.99) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $476.88 | ▼ 09:30 equity $10,612.53 vs yday $10,946.59 (-334.06) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 8 | $160.93 | $2.03 | $+127.07 | $1,762.29 | ▲ +127.07 after sell → book $10,610.50; vs 09:30 mark -2.03 | dropped from list after 4 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 1 | $119.80 | $1.22 | $-2.05 | $1,880.87 | ▼ -2.05 after sell → book $10,609.28; vs 09:30 mark -1.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MSTR` | 1 | $123.26 | $1.26 | $+1.11 | $2,002.87 | ▲ +1.11 after sell → book $10,608.02; vs 09:30 mark -1.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `TEM` | 2 | $67.48 | $1.38 | $+1.07 | $2,136.46 | ▲ +1.07 after sell → book $10,606.65; vs 09:30 mark -1.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `SHAZ` | 2 | $59.19 | $1.21 | $-6.99 | $2,253.63 | ▼ -6.99 after sell → book $10,605.44; vs 09:30 mark -1.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `RGLD` | 1 | $264.00 | $1.99 | — | $1,987.63 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+16.2; leftover $375.60 | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 1 | $267.02 | $1.99 | — | $1,718.62 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+16.7; leftover $375.60 | — |
| 2026-08-26 09:30 ET | **BUY** | `MRK` | 2 | $154.35 | $2.00 | — | $1,407.93 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+15.7; leftover $375.60 | — |
| 2026-08-26 09:30 ET | **BUY** | `NEM` | 2 | $132.64 | $2.00 | — | $1,140.65 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+16.5; leftover $375.60 | — |
| 2026-08-26 09:30 ET | **BUY** | `HTFL` | 7 | $50.02 | $2.01 | — | $788.50 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+10.3; leftover $375.60 | — |
| 2026-08-26 09:30 ET | **BUY** | `FUTU` | 3 | $124.67 | $2.00 | — | $412.49 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+15.7; leftover $375.60 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $412.49 | ▲ close $10,652.60 vs 09:30 $10,612.53 (session +59.15) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $412.49 | ▼ 09:30 equity $10,650.30 vs yday $10,652.60 (-2.30) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 6 | $9.19 | $0.57 | — | $356.78 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; leftover $58.93 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $356.78 | ▼ close $10,525.73 vs 09:30 $10,650.30 (session -124.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $356.78 | ▼ 09:30 equity $10,480.03 vs yday $10,525.73 (-45.70) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `REAX` | 61 | $23.40 | $2.19 | $-47.68 | $1,781.99 | ▼ -47.68 after sell → book $10,477.84; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INTU` | 4 | $347.82 | $2.02 | $-70.15 | $3,171.24 | ▼ -70.15 after sell → book $10,475.81; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ILMN` | 6 | $224.73 | $2.03 | $+0.34 | $4,517.59 | ▲ +0.34 after sell → book $10,473.78; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ETON` | 22 | $61.98 | $2.08 | $-60.67 | $5,879.08 | ▼ -60.67 after sell → book $10,471.71; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $423.76 | $2.02 | $-13.65 | $7,148.34 | ▼ -13.65 after sell → book $10,469.69; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `WIX` | 17 | $85.32 | $2.06 | $+32.79 | $8,596.72 | ▲ +32.79 after sell → book $10,467.63; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 8 | $137.19 | $2.01 | — | $7,497.18 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+7.1; leftover $1228.10 | — |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 2 | $461.85 | $2.00 | — | $6,571.49 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+16.8; leftover $1228.10 | — |
| 2026-08-28 09:30 ET | **BUY** | `MRVL` | 5 | $225.26 | $2.00 | — | $5,443.18 | — | rank by macd_hist; rank macd_hist; list earn_react; ret5=-3.8; leftover $1228.10 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $4,272.61 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1228.10 | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 4 | $252.24 | $2.00 | — | $3,261.64 | — | rank by macd_hist; rank macd_hist; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1228.10 | — |
| 2026-08-28 09:30 ET | **BUY** | `EL` | 11 | $106.99 | $2.02 | — | $2,082.73 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+10.5; leftover $1228.10 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $946.64 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1228.10 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $946.64 | ▼ close $10,222.58 vs 09:30 $10,480.03 (session -230.98) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $946.64 | ▼ 09:30 equity $10,176.55 vs yday $10,222.58 (-46.03) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `RGLD` | 1 | $262.72 | $2.01 | $-5.29 | $1,207.34 | ▼ -5.29 after sell → book $10,174.53; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FNV` | 1 | $265.78 | $2.01 | $-5.25 | $1,471.11 | ▼ -5.25 after sell → book $10,172.52; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRK` | 2 | $147.00 | $2.02 | $-18.71 | $1,763.10 | ▼ -18.71 after sell → book $10,170.51; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `NEM` | 2 | $127.45 | $2.02 | $-14.39 | $2,015.98 | ▼ -14.39 after sell → book $10,168.49; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `HTFL` | 7 | $47.68 | $2.03 | $-20.42 | $2,347.71 | ▼ -20.42 after sell → book $10,166.46; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FUTU` | 3 | $123.67 | $2.02 | $-7.02 | $2,716.70 | ▼ -7.02 after sell → book $10,164.44; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,716.70 | ▼ close $10,157.34 vs 09:30 $10,176.55 (session -7.10) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,716.70 | ▼ 09:30 equity $10,017.21 vs yday $10,157.34 (-140.13) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `CAPR` | 6 | $10.77 | $0.68 | $+8.23 | $2,780.63 | ▲ +8.23 after sell → book $10,016.52; vs 09:30 mark -0.69 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,780.63 | ▲ close $10,129.38 vs 09:30 $10,017.21 (session +112.86) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,780.63 | ▼ 09:30 equity $10,069.68 vs yday $10,129.38 (-59.70) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `SNPS` | 2 | $413.78 | $2.02 | $-100.15 | $3,606.18 | ▼ -100.15 after sell → book $10,067.67; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MRVL` | 5 | $205.25 | $2.02 | $-104.08 | $4,630.40 | ▼ -104.08 after sell → book $10,065.64; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 8 | $139.65 | $2.03 | $-55.41 | $5,745.57 | ▼ -55.41 after sell → book $10,063.61; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SIMO` | 4 | $235.71 | $2.02 | $-70.14 | $6,686.39 | ▼ -70.14 after sell → book $10,061.59; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `EL` | 11 | $100.00 | $2.04 | $-80.96 | $7,784.34 | ▼ -80.96 after sell → book $10,059.54; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $133.00 | $2.03 | $-74.13 | $8,846.31 | ▼ -74.13 after sell → book $10,057.51; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,846.31 | ▼ close $10,052.79 vs 09:30 $10,069.68 (session -4.72) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,846.31 | ▼ 09:30 equity $10,013.87 vs yday $10,052.79 (-38.92) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $8,141.07 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1263.76 | — |
| 2026-09-03 09:30 ET | **BUY** | `ALNY` | 4 | $265.94 | $2.00 | — | $7,075.31 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+11.9; leftover $1263.76 | — |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 9 | $127.91 | $2.02 | — | $5,922.10 | — | rank by macd_hist; rank macd_hist; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1263.76 | — |
| 2026-09-03 09:30 ET | **BUY** | `CF` | 9 | $138.60 | $2.02 | — | $4,672.68 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+10.8; leftover $1263.76 | — |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 7 | $161.54 | $2.01 | — | $3,539.89 | — | rank by macd_hist; rank macd_hist; list yday_gainer; 🔵; ret5=+12.0; leftover $1263.76 | — |
| 2026-09-03 09:30 ET | **BUY** | `CTVA` | 14 | $90.24 | $2.03 | — | $2,274.50 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+8.6; leftover $1263.76 | — |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 172 | $7.31 | $2.51 | — | $1,014.67 | — | rank by macd_hist; rank macd_hist; list yday_gainer; 🔵; ret5=+18.5; leftover $1263.76 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,014.67 | ▼ close $9,844.13 vs 09:30 $10,013.87 (session -155.16) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,014.67 | ▼ 09:30 equity $9,793.97 vs yday $9,844.13 (-50.16) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $153.62 | $2.03 | $+127.39 | $2,241.60 | ▲ +127.39 after sell → book $9,791.94; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 3) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 1 | $263.36 | $1.99 | — | $1,976.25 | — | rank by macd_hist; rank macd_hist; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $320.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 2 | $137.35 | $2.00 | — | $1,699.55 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+5.4; leftover $320.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 1 | $236.82 | $1.99 | — | $1,460.74 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+8.1; leftover $320.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 1 | $267.96 | $1.99 | — | $1,190.78 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+8.7; leftover $320.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `HOOD` | 2 | $120.47 | $2.00 | — | $947.84 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+13.6; leftover $320.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRCL` | 3 | $97.98 | $2.00 | — | $651.90 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+9.5; leftover $320.23 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $651.90 | ▲ close $9,975.47 vs 09:30 $9,793.97 (session +195.50) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $651.90 | ▼ 09:30 equity $9,921.70 vs yday $9,975.47 (-53.77) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $651.90 | ▼ close $9,803.41 vs 09:30 $9,921.70 (session -118.29) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $651.90 | ▲ 09:30 equity $9,846.28 vs yday $9,803.41 (+42.87) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `DE` | 1 | $681.32 | $2.01 | $-25.94 | $1,331.21 | ▼ -25.94 after sell → book $9,844.27; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ALNY` | 4 | $257.07 | $2.02 | $-39.50 | $2,357.46 | ▼ -39.50 after sell → book $9,842.25; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AGCO` | 9 | $127.69 | $2.04 | $-6.03 | $3,504.64 | ▼ -6.03 after sell → book $9,840.21; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CF` | 9 | $137.88 | $2.04 | $-10.53 | $4,743.52 | ▼ -10.53 after sell → book $9,838.17; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DUOL` | 7 | $145.58 | $2.03 | $-115.76 | $5,760.55 | ▼ -115.76 after sell → book $9,836.14; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CTVA` | 14 | $86.40 | $2.05 | $-57.84 | $6,968.10 | ▼ -57.84 after sell → book $9,834.09; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SION` | 172 | $7.27 | $2.54 | $-11.93 | $8,215.99 | ▼ -11.93 after sell → book $9,831.55; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,215.99 | ▼ close $9,765.17 vs 09:30 $9,846.28 (session -66.38) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,215.99 | ▼ 09:30 equity $9,734.31 vs yday $9,765.17 (-30.86) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `CRM` | 1 | $245.35 | $2.01 | $-22.02 | $8,459.33 | ▼ -22.02 after sell → book $9,732.30; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MSTR` | 2 | $128.44 | $2.02 | $-21.83 | $8,714.19 | ▼ -21.83 after sell → book $9,730.28; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BE` | 1 | $260.71 | $2.01 | $+19.88 | $8,972.89 | ▲ +19.88 after sell → book $9,728.27; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ASND` | 1 | $260.00 | $2.01 | $-11.97 | $9,230.88 | ▼ -11.97 after sell → book $9,726.26; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `HOOD` | 2 | $112.69 | $2.02 | $-19.58 | $9,454.24 | ▼ -19.58 after sell → book $9,724.24; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `CRCL` | 3 | $90.00 | $2.02 | $-27.96 | $9,722.22 | ▼ -27.96 after sell → book $9,722.22; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,722.22 | ▲ close $9,722.22 vs 09:30 $9,734.31 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,722.22 | ▲ 09:30 equity $9,722.22 vs yday $9,722.22 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 5 | $206.84 | $2.00 | — | $8,686.02 | — | rank by macd_hist; rank macd_hist; list flatten; ret5=+8.3; leftover $1215.28 | — |
| 2026-09-11 09:30 ET | **BUY** | `VLO` | 3 | $388.00 | $2.00 | — | $7,520.02 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+6.7; leftover $1215.28 | — |
| 2026-09-11 09:30 ET | **BUY** | `SION` | 156 | $7.79 | $2.46 | — | $6,302.32 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+4.2; leftover $1215.28 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 14 | $84.27 | $2.03 | — | $5,120.51 | — | rank by macd_hist; rank macd_hist; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1215.28 | — |
| 2026-09-11 09:30 ET | **BUY** | `QRVO` | 10 | $112.83 | $2.02 | — | $3,990.14 | — | rank by macd_hist; rank macd_hist; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $1215.28 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 22 | $54.91 | $2.06 | — | $2,780.06 | — | rank by macd_hist; rank macd_hist; list yday_gainer; 🔵; ret5=+24.3; leftover $1215.28 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $1,627.04 | — | rank by macd_hist; rank macd_hist; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1215.28 | — |
| 2026-09-11 09:30 ET | **BUY** | `CVI` | 25 | $48.36 | $2.06 | — | $415.97 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+13.2; leftover $1215.28 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $415.97 | ▲ close $9,806.61 vs 09:30 $9,722.22 (session +101.04) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $415.97 | ▼ 09:30 equity $9,653.92 vs yday $9,806.61 (-152.69) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $415.97 | ▼ close $9,445.80 vs 09:30 $9,653.92 (session -208.12) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $415.97 | ▼ 09:30 equity $9,402.55 vs yday $9,445.80 (-43.25) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $415.97 | ▲ close $9,475.40 vs 09:30 $9,402.55 (session +72.85) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $415.97 | ▼ 09:30 equity $9,454.17 vs yday $9,475.40 (-21.23) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 5 | $194.84 | $2.02 | $-64.03 | $1,388.15 | ▼ -64.03 after sell → book $9,452.15; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ASO` | 22 | $50.69 | $2.08 | $-96.97 | $2,501.25 | ▼ -96.97 after sell → book $9,450.07; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 7 | $140.03 | $2.03 | $-174.84 | $3,479.43 | ▼ -174.84 after sell → book $9,448.04; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CVI` | 25 | $51.05 | $2.09 | $+63.10 | $4,753.60 | ▲ +63.10 after sell → book $9,445.96; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `META` | 1 | $679.91 | $1.99 | — | $4,071.69 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+9.3; leftover $1188.40 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 6 | $189.17 | $2.01 | — | $2,934.67 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+7.9; leftover $1188.40 | — |
| 2026-09-16 09:30 ET | **BUY** | `OKTA` | 6 | $186.52 | $2.01 | — | $1,813.54 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+13.6; leftover $1188.40 | — |
| 2026-09-16 09:30 ET | **BUY** | `CRWD` | 5 | $236.92 | $2.00 | — | $626.93 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+15.5; leftover $1188.40 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $626.93 | ▼ close $9,390.84 vs 09:30 $9,454.17 (session -47.10) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $626.93 | ▲ 09:30 equity $9,422.78 vs yday $9,390.84 (+31.94) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `VLO` | 3 | $398.45 | $2.02 | $+27.33 | $1,820.26 | ▲ +27.33 after sell → book $9,420.76; vs 09:30 mark -2.02 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 156 | $7.27 | $2.49 | $-86.07 | $2,951.89 | ▼ -86.07 after sell → book $9,418.27; vs 09:30 mark -2.49 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 14 | $86.76 | $2.05 | $+30.78 | $4,164.48 | ▲ +30.78 after sell → book $9,416.22; vs 09:30 mark -2.05 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **SELL** | `QRVO` | 10 | $114.90 | $2.04 | $+16.59 | $5,311.44 | ▲ +16.59 after sell → book $9,414.18; vs 09:30 mark -2.04 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 3 | $170.85 | $2.00 | — | $4,796.89 | — | rank by macd_hist; rank macd_hist; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $663.93 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 4 | $147.61 | $2.00 | — | $4,204.45 | — | rank by macd_hist; rank macd_hist; list flatten,ohlc_hot; ret5=+17.7; leftover $663.93 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 18 | $36.76 | $2.04 | — | $3,540.72 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; leftover $663.93 | — |
| 2026-09-17 09:30 ET | **BUY** | `KGS` | 11 | $58.91 | $2.02 | — | $2,890.69 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+9.1; leftover $663.93 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 30 | $22.12 | $2.08 | — | $2,225.01 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+10.5; leftover $663.93 | — |
| 2026-09-17 09:30 ET | **BUY** | `INDP` | 201 | $3.30 | $2.60 | — | $1,559.11 | — | rank by macd_hist; rank macd_hist; list yday_mover; 🔵; ret5=+54.3; leftover $663.93 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGNY` | 24 | $27.38 | $2.06 | — | $899.93 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+6.1; leftover $663.93 | — |
| 2026-09-17 09:30 ET | **BUY** | `BKV` | 29 | $22.75 | $2.08 | — | $238.10 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+10.8; leftover $663.93 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $238.10 | ▲ close $9,651.12 vs 09:30 $9,422.78 (session +253.83) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $238.10 | ▲ 09:30 equity $9,708.34 vs yday $9,651.12 (+57.22) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $238.10 | ▼ close $9,447.39 vs 09:30 $9,708.34 (session -260.95) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $238.10 | ▲ 09:30 equity $9,490.29 vs yday $9,447.39 (+42.90) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `META` | 1 | $680.30 | $2.01 | $-3.62 | $916.39 | ▼ -3.62 after sell → book $9,488.28; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QCOM` | 6 | $180.61 | $2.03 | $-55.40 | $1,998.02 | ▼ -55.40 after sell → book $9,486.25; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `OKTA` | 6 | $183.90 | $2.03 | $-19.76 | $3,099.39 | ▼ -19.76 after sell → book $9,484.22; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `CRWD` | 5 | $231.62 | $2.02 | $-30.53 | $4,255.47 | ▼ -30.53 after sell → book $9,482.20; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `AMD` | 1 | $583.88 | $1.99 | — | $3,669.60 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+8.5; leftover $607.92 | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 2 | $230.25 | $2.00 | — | $3,207.10 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+12.5; leftover $607.92 | — |
| 2026-09-21 09:30 ET | **BUY** | `COHR` | 1 | $326.48 | $1.99 | — | $2,878.63 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+3.9; leftover $607.92 | — |
| 2026-09-21 09:30 ET | **BUY** | `ARM` | 2 | $294.36 | $2.00 | — | $2,287.91 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+4.1; leftover $607.92 | — |
| 2026-09-21 09:30 ET | **BUY** | `MXL` | 7 | $83.53 | $2.01 | — | $1,701.19 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+8.8; leftover $607.92 | — |
| 2026-09-21 09:30 ET | **BUY** | `FORM` | 4 | $123.00 | $2.00 | — | $1,207.19 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+3.0; leftover $607.92 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,207.19 | ▼ close $9,335.62 vs 09:30 $9,490.29 (session -134.59) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,207.19 | ▼ 09:30 equity $9,326.55 vs yday $9,335.62 (-9.07) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `FPS` | 18 | $37.41 | $2.06 | $+7.59 | $1,878.50 | ▲ +7.59 after sell → book $9,324.48; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `GME` | 30 | $23.50 | $2.10 | $+37.22 | $2,581.40 | ▲ +37.22 after sell → book $9,322.38; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `INDP` | 201 | $3.10 | $2.64 | $-45.44 | $3,201.86 | ▼ -45.44 after sell → book $9,319.74; vs 09:30 mark -2.64 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `INOD` | 7 | $61.78 | $2.01 | — | $2,767.39 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+10.5; leftover $457.41 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,767.39 | ▲ close $9,419.94 vs 09:30 $9,326.55 (session +102.21) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,767.39 | ▲ 09:30 equity $9,494.09 vs yday $9,419.94 (+74.15) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 3 | $174.50 | $2.02 | $+6.93 | $3,288.87 | ▲ +6.93 after sell → book $9,492.07; vs 09:30 mark -2.02 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RVTY` | 4 | $142.40 | $2.02 | $-24.86 | $3,856.45 | ▼ -24.86 after sell → book $9,490.05; vs 09:30 mark -2.02 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `KGS` | 11 | $54.38 | $2.04 | $-53.90 | $4,452.59 | ▼ -53.90 after sell → book $9,488.01; vs 09:30 mark -2.04 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `PGNY` | 24 | $26.10 | $2.08 | $-34.86 | $5,076.91 | ▼ -34.86 after sell → book $9,485.93; vs 09:30 mark -2.08 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BKV` | 29 | $23.29 | $2.10 | $+11.49 | $5,750.22 | ▲ +11.49 after sell → book $9,483.83; vs 09:30 mark -2.10 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `ZS` | 5 | $213.00 | $2.00 | — | $4,683.21 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+8.0; leftover $1150.04 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLLN` | 9 | $116.00 | $2.02 | — | $3,637.20 | — | rank by macd_hist; rank macd_hist; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $1150.04 | — |
| 2026-09-23 09:30 ET | **BUY** | `RBRK` | 10 | $112.46 | $2.02 | — | $2,510.58 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+12.7; leftover $1150.04 | — |
| 2026-09-23 09:30 ET | **BUY** | `AKAM` | 9 | $117.33 | $2.02 | — | $1,452.59 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+11.2; leftover $1150.04 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,452.59 | ▲ close $9,542.14 vs 09:30 $9,494.09 (session +66.37) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,452.59 | ▼ 09:30 equity $9,388.71 vs yday $9,542.14 (-153.43) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `AMD` | 1 | $600.27 | $2.01 | $+12.38 | $2,050.85 | ▲ +12.38 after sell → book $9,386.70; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `COHR` | 1 | $294.90 | $2.01 | $-35.59 | $2,343.73 | ▼ -35.59 after sell → book $9,384.68; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARM` | 2 | $319.23 | $2.02 | $+45.73 | $2,980.18 | ▲ +45.73 after sell → book $9,382.67; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MXL` | 7 | $82.53 | $2.03 | $-11.01 | $3,555.89 | ▼ -11.01 after sell → book $9,380.64; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `FORM` | 4 | $126.98 | $2.02 | $+11.90 | $4,061.79 | ▲ +11.90 after sell → book $9,378.61; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,061.79 | ▲ close $9,445.99 vs 09:30 $9,388.71 (session +67.37) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,103.87 | ▲ 09:30 equity $9,038.41 vs yday $9,038.39 (+0.02) | 09:30 open · cash $5,103.87 (unchanged overnight, no fees) · equity $9,038.41 vs prior close $9,038.39 (+0.02) · 10 name(s) re-marked at the open (per-name table). AEHL×54 yday $8.96 → 09:30 $9.05 +4.86; AIP×17 yday $24.19 → 09:30 $24.19 +0.00; ARM×1 yday $306.34 → 09:30 $306.34 +0.00; BLLN×1 yday $124.90 → 09:30 $125.19 +0.29; GRAL×3 yday $125.21 → 09:30 $123.50 -5.13; MANE×1 yday $115.72 → 09:30 $115.72 +0.00; PENG×7 yday $53.08 → 09:30 $53.08 +0.00; RBRK×3 yday $113.80 → 09:30 $113.80 +0.00; TWLO×1 yday $299.66 → 09:30 $299.66 +0.00; VICR×4 yday $276.06 → 09:30 $276.06 +0.00 | — |
| 2026-09-25 09:30 ET | **SELL** | `AEHL` | 54 | $9.05 | $2.17 | $+72.36 | $5,590.40 | ▲ +72.36 after sell → book $9,036.24; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AMD` | 1 | $634.53 | $1.99 | — | $4,953.87 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+15.4; leftover $798.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `META` | 1 | $768.85 | $1.99 | — | $4,183.03 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+14.0; leftover $798.63 | join🟢 sector🟡 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CLS` | 2 | $380.51 | $2.00 | — | $3,420.02 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+13.2; leftover $798.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `NTRA` | 1 | $410.00 | $1.99 | — | $3,008.02 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ⚪; ret5=+11.6; leftover $798.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 2 | $272.16 | $2.00 | — | $2,461.71 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $798.63 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CDNS` | 2 | $324.97 | $2.00 | — | $1,809.77 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ⚪; ret5=+14.7; leftover $798.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 4 | $184.00 | $2.00 | — | $1,071.77 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $798.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,071.77 | ▼ close $8,974.98 vs 09:30 $9,038.41 (session -47.29) | 16:00 close · cash $1,071.77 · equity $8,974.98 vs 09:30 $9,038.41 (-63.43; session marks -47.29) · 16 name(s) marked open→close (per-name table). AIP×17 09:30 $24.19 → close $24.19 +0.00; ARM×1 09:30 $306.34 → close $306.34 -0.00; BLLN×1 09:30 $125.19 → close $123.08 -2.11; GRAL×3 09:30 $123.50 → close $126.89 +10.17; MANE×1 09:30 $115.72 → close $115.72 +0.00; PENG×7 09:30 $53.08 → close $53.08 +0.00; RBRK×3 09:30 $113.80 → close $113.80 +0.00; TWLO×1 09:30 $299.66 → close $299.66 +0.00; VICR×4 09:30 $276.06 → close $276.06 -0.00; AMD×1 09:30 $634.53 → close $630.63 -3.90; META×1 09:30 $768.85 → close $751.66 -17.19; CLS×2 09:30 $380.51 → close $365.44 -30.14; NTRA×1 09:30 $410.00 → close $412.56 +2.56; ILMN×2 09:30 $272.16 → close $270.00 -4.32; CDNS×2 09:30 $324.97 → close $326.13 +2.32; TWST×4 09:30 $184.00 → close $182.83 -4.68 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SNDK` | cash | leftover split 7.61 < 1 share @ 1646.93 |
| 2026-08-14 | `ENTG` | cash | leftover split 7.61 < 1 share @ 162.45 |
| 2026-08-14 | `FORM` | cash | leftover split 7.61 < 1 share @ 129.48 |
| 2026-08-14 | `SPHR` | cash | leftover split 7.61 < 1 share @ 176.68 |
| 2026-08-14 | `AMAT` | cash | leftover split 7.61 < 1 share @ 499.40 |
| 2026-08-14 | `ZS` | cash | leftover split 7.61 < 1 share @ 190.00 |
| 2026-08-14 | `TLN` | cash | leftover split 7.61 < 1 share @ 359.83 |
| 2026-08-14 | `VOYG` | cash | leftover split 7.61 < 1 share @ 44.49 |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `AAOI` | cash | leftover split 7.61 < 1 share @ 152.64 |
| 2026-08-17 | `MXL` | cash | leftover split 7.61 < 1 share @ 86.67 |
| 2026-08-17 | `MP` | cash | leftover split 7.61 < 1 share @ 58.01 |
| 2026-08-17 | `HTFL` | cash | leftover split 7.61 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 7.61 < 1 share @ 32.55 |
| 2026-08-17 | `OUST` | cash | leftover split 7.61 < 1 share @ 49.00 |
| 2026-08-17 | `CELC` | cash | leftover split 7.61 < 1 share @ 92.99 |
| 2026-08-17 | `RDDT` | cash | leftover split 7.61 < 1 share @ 177.51 |
| 2026-08-18 | `MU` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MRVL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AMKR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `FN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AAOI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `COHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AXTI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PSX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRGP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `TEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TWST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CRCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BNTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TEAM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TWST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CRCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BNTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MSTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `SHAZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SQM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `SHAZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `REAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ILMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ETON` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `WIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ILMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ETON` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `WIX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RGLD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `FNV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `MRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `NEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `FUTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `MRNA` | cash | leftover split 58.93 < 1 share @ 144.18 |
| 2026-08-27 | `MU` | cash | leftover split 58.93 < 1 share @ 967.01 |
| 2026-08-27 | `MRVL` | cash | leftover split 58.93 < 1 share @ 253.44 |
| 2026-08-27 | `CBOE` | cash | leftover split 58.93 < 1 share @ 312.34 |
| 2026-08-27 | `DASH` | cash | leftover split 58.93 < 1 share @ 235.94 |
| 2026-08-27 | `BE` | cash | leftover split 58.93 < 1 share @ 227.10 |
| 2026-08-28 | `RGLD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FNV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `NEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `HTFL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FUTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MRVL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `EL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MSTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HUBS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SNPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MRVL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `EL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TSLA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AGCO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALAB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ARCT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CF` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASND` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `DE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ALNY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AGCO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DUOL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CTVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 320.23 < 1 share @ 513.78 |
| 2026-09-08 | `DE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALNY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DUOL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CTVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ASND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `RBLX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ASND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `HOOD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `CRCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `VRT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `STX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SKHY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VST` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GLW` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SEDG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DOCN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIMO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SKHY` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `VLO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `MXL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ASO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CVI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HUT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VICR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ASO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `META` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SIMO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBLX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INSP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `META` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `OKTA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CRWD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `META` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `OKTA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `FPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `KGS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `GME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGNY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BKV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `AMD` | cash | leftover split 34.01 < 1 share @ 547.37 |
| 2026-09-18 | `VICR` | cash | leftover split 34.01 < 1 share @ 219.62 |
| 2026-09-18 | `BLLN` | cash | leftover split 34.01 < 1 share @ 112.16 |
| 2026-09-18 | `GNRC` | cash | leftover split 34.01 < 1 share @ 209.52 |
| 2026-09-18 | `AVAV` | cash | leftover split 34.01 < 1 share @ 165.57 |
| 2026-09-18 | `ECO` | cash | leftover split 34.01 < 1 share @ 85.00 |
| 2026-09-18 | `RBRK` | cash | leftover split 34.01 < 1 share @ 108.55 |
| 2026-09-21 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `FPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `KGS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `GME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `PGNY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BKV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SNDK` | cash | leftover split 607.92 < 1 share @ 1826.00 |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `RVTY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `KGS` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PGNY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKV` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `COHR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MXL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FORM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `META` | cash | leftover split 457.41 < 1 share @ 731.40 |
| 2026-09-22 | `MPWR` | no_price | no 09:30 open |
| 2026-09-22 | `ZS` | no_price | no 09:30 open |
| 2026-09-22 | `RBRK` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `AKAM` | no_price | no 09:30 open |
| 2026-09-23 | `AMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `COHR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ARM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MXL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MPWR` | cash | leftover split 1150.04 < 1 share @ 1367.08 |
| 2026-09-24 | `INOD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `BLLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `RBRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `AKAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CLS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CRWD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DDOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `KEYS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `MDB` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `VICR` | 2 | 2026-09-21 @ $230.25 | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+12.5; leftover $607.92 |
| `INOD` | 7 | 2026-09-22 @ $61.78 | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+10.5; leftover $457.41 |
| `ZS` | 5 | 2026-09-23 @ $213.00 | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+8.0; leftover $1150.04 |
| `BLLN` | 9 | 2026-09-23 @ $116.00 | rank by macd_hist; rank macd_hist; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $1150.04 |
| `RBRK` | 10 | 2026-09-23 @ $112.46 | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+12.7; leftover $1150.04 |
| `AKAM` | 9 | 2026-09-23 @ $117.33 | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+11.2; leftover $1150.04 |
