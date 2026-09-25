# Factor mine action — `short_rsi_ob_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · RSI overbought

Cash book **-14.89%** ($8,511) · signal-only (no cash/fees) was -115.85%. Starts YES **1/30**. Fills 216 · skips 279 · realized $-808.98.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: prior RSI is overbought (≥70).

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
- **Gate** `rsi_ob=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $17,479.52.

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
| 2026-08-13 09:30 ET | **SHORT** | `TPG` | 49 | $50.62 | $2.23 | — | $12,478.30 | — | RSI overbought; gate rsi_ob=True; list flatten; ⚪; ret5=+6.2; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **SHORT** | `TNDM` | 107 | $23.33 | $2.42 | — | $14,972.19 | — | RSI overbought; gate rsi_ob=True; list flatten; ⚪; ret5=+19.7; leftover $2500.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,972.19 | ▼ close $9,820.90 vs 09:30 $10,000.00 (session -174.44) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,972.19 | ▼ 09:30 equity $9,810.54 vs yday $9,820.90 (-10.36) | — | — |
| 2026-08-14 09:30 ET | **SHORT** | `ARX` | 31 | $19.57 | $2.12 | — | $15,576.75 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $613.16 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OMER` | 35 | $17.35 | $2.13 | — | $16,181.86 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $613.16 | — |
| 2026-08-14 09:30 ET | **SHORT** | `AIRO` | 55 | $11.12 | $2.19 | — | $16,791.27 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $613.16 | — |
| 2026-08-14 09:30 ET | **SHORT** | `MXCT` | 441 | $1.39 | $5.79 | — | $17,398.47 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+25.2; leftover $613.16 | — |
| 2026-08-14 09:30 ET | **SHORT** | `TBBB` | 12 | $48.82 | $2.06 | — | $17,982.25 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $613.16 | — |
| 2026-08-14 09:30 ET | **SHORT** | `AMPY` | 124 | $4.94 | $2.41 | — | $18,592.40 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.4; leftover $613.16 | — |
| 2026-08-14 09:30 ET | **SHORT** | `MH` | 45 | $13.55 | $2.16 | — | $19,199.99 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $613.16 | — |
| 2026-08-14 09:30 ET | **SHORT** | `CRDL` | 354 | $1.73 | $4.65 | — | $19,807.76 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ret5=+25.7; leftover $613.16 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,807.76 | ▲ close $10,068.25 vs 09:30 $9,810.54 (session +281.22) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,807.76 | ▲ 09:30 equity $10,127.40 vs yday $10,068.25 (+59.15) | — | — |
| 2026-08-17 09:30 ET | **SHORT** | `HTFL` | 15 | $41.23 | $2.07 | — | $20,424.14 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+46.0; leftover $632.96 | — |
| 2026-08-17 09:30 ET | **SHORT** | `UMAC` | 19 | $32.55 | $2.08 | — | $21,040.50 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $632.96 | — |
| 2026-08-17 09:30 ET | **SHORT** | `NPWR` | 329 | $1.92 | $4.33 | — | $21,667.86 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $632.96 | — |
| 2026-08-17 09:30 ET | **SHORT** | `NMAX` | 57 | $10.97 | $2.20 | — | $22,290.95 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $632.96 | — |
| 2026-08-17 09:30 ET | **SHORT** | `CLYM` | 38 | $16.25 | $2.14 | — | $22,906.31 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,ohlc_hot; ret5=+16.6; leftover $632.96 | — |
| 2026-08-17 09:30 ET | **SHORT** | `XHG` | 151 | $4.19 | $2.50 | — | $23,536.50 | — | RSI overbought; gate rsi_ob=True; list yday_mover; ⚪; ret5=+291.8; leftover $632.96 | — |
| 2026-08-17 09:30 ET | **SHORT** | `SGMT` | 60 | $10.45 | $2.21 | — | $24,161.29 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+16.4; leftover $632.96 | — |
| 2026-08-17 09:30 ET | **SHORT** | `U` | 13 | $46.21 | $2.07 | — | $24,759.96 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ⚪; ret5=+7.6; leftover $632.96 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,759.96 | ▲ close $10,306.58 vs 09:30 $10,127.40 (session +198.78) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,759.96 | ▲ 09:30 equity $10,393.30 vs yday $10,306.58 (+86.72) | — | — |
| 2026-08-18 09:30 ET | **COVER** | `TPG` | 49 | $51.77 | $2.14 | $-60.56 | $22,221.09 | ▼ -60.56 after sell → book $10,391.16; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **COVER** | `TNDM` | 107 | $22.16 | $2.31 | $+120.46 | $19,847.66 | ▲ +120.46 after sell → book $10,388.85; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,847.66 | ▼ close $10,146.70 vs 09:30 $10,393.30 (session -242.15) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,847.66 | ▼ 09:30 equity $10,047.77 vs yday $10,146.70 (-98.93) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `ARX` | 31 | $19.58 | $2.08 | $-4.51 | $19,238.60 | ▼ -4.51 after sell → book $10,045.69; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OMER` | 35 | $17.13 | $2.10 | $+3.47 | $18,636.95 | ▲ +3.47 after sell → book $10,043.59; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `AIRO` | 55 | $9.10 | $2.15 | $+106.75 | $18,134.30 | ▲ +106.75 after sell → book $10,041.44; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `MXCT` | 441 | $1.29 | $5.69 | $+32.62 | $17,559.72 | ▲ +32.62 after sell → book $10,035.75; vs 09:30 mark -5.69 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `TBBB` | 12 | $48.62 | $2.03 | $-1.69 | $16,974.25 | ▼ -1.69 after sell → book $10,033.72; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `AMPY` | 124 | $4.88 | $2.36 | $+2.67 | $16,366.77 | ▲ +2.67 after sell → book $10,031.36; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `MH` | 45 | $13.01 | $2.12 | $+20.01 | $15,779.20 | ▲ +20.01 after sell → book $10,029.24; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `CRDL` | 354 | $1.92 | $4.57 | $-76.48 | $15,094.95 | ▼ -76.48 after sell → book $10,024.67; vs 09:30 mark -4.57 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,094.95 | ▲ close $10,143.02 vs 09:30 $10,047.77 (session +118.35) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,094.95 | ▲ 09:30 equity $10,193.85 vs yday $10,143.02 (+50.83) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `HTFL` | 15 | $45.90 | $2.04 | $-74.16 | $14,404.41 | ▼ -74.16 after sell → book $10,191.81; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `UMAC` | 19 | $28.32 | $2.05 | $+76.24 | $13,864.29 | ▲ +76.24 after sell → book $10,189.77; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `NPWR` | 329 | $1.64 | $4.24 | $+83.55 | $13,320.48 | ▲ +83.55 after sell → book $10,185.52; vs 09:30 mark -4.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `NMAX` | 57 | $10.89 | $2.16 | $+0.20 | $12,697.59 | ▲ +0.20 after sell → book $10,183.36; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `CLYM` | 38 | $17.16 | $2.10 | $-38.82 | $12,043.41 | ▼ -38.82 after sell → book $10,181.26; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `XHG` | 151 | $4.10 | $2.44 | $+8.65 | $11,421.87 | ▲ +8.65 after sell → book $10,178.82; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `SGMT` | 60 | $10.48 | $2.17 | $-6.18 | $10,790.90 | ▼ -6.18 after sell → book $10,176.65; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `U` | 13 | $47.25 | $2.03 | $-17.61 | $10,174.62 | ▼ -17.61 after sell → book $10,174.62; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `KGC` | 21 | $29.63 | $2.09 | — | $10,794.76 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $635.91 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WPM` | 4 | $144.54 | $2.04 | — | $11,370.88 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $635.91 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $11,982.19 | — | RSI overbought; gate rsi_ob=True; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $635.91 | — |
| 2026-08-20 09:30 ET | **SHORT** | `SCZM` | 67 | $9.46 | $2.23 | — | $12,613.78 | — | RSI overbought; gate rsi_ob=True; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $635.91 | — |
| 2026-08-20 09:30 ET | **SHORT** | `MRNA` | 4 | $150.14 | $2.04 | — | $13,212.30 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $635.91 | — |
| 2026-08-20 09:30 ET | **SHORT** | `CYPH` | 552 | $1.15 | $7.24 | — | $13,839.86 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $635.91 | — |
| 2026-08-20 09:30 ET | **SHORT** | `BNTX` | 5 | $109.06 | $2.04 | — | $14,383.12 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $635.91 | — |
| 2026-08-20 09:30 ET | **SHORT** | `EL` | 6 | $97.43 | $2.04 | — | $14,965.66 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; leftover $635.91 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,965.66 | ▼ close $10,093.08 vs 09:30 $10,193.85 (session -59.78) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,965.66 | ▼ 09:30 equity $9,938.79 vs yday $10,093.08 (-154.29) | — | — |
| 2026-08-21 09:30 ET | **SHORT** | `AU` | 6 | $119.43 | $2.05 | — | $15,680.19 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $828.23 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARCT` | 74 | $11.13 | $2.26 | — | $16,501.56 | — | RSI overbought; gate rsi_ob=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $828.23 | — |
| 2026-08-21 09:30 ET | **SHORT** | `CRDL` | 429 | $1.93 | $5.64 | — | $17,323.89 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $828.23 | — |
| 2026-08-21 09:30 ET | **SHORT** | `GMAB` | 24 | $33.36 | $2.10 | — | $18,122.42 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $828.23 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRVI` | 100 | $8.28 | $2.34 | — | $18,948.09 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $828.23 | — |
| 2026-08-21 09:30 ET | **SHORT** | `DFDV` | 205 | $4.04 | $2.71 | — | $19,773.57 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $828.23 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,773.57 | ▼ close $9,604.18 vs 09:30 $9,938.79 (session -317.52) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,773.57 | ▼ 09:30 equity $9,359.16 vs yday $9,604.18 (-245.02) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,773.57 | ▲ close $9,417.72 vs 09:30 $9,359.16 (session +58.57) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,773.57 | ▲ 09:30 equity $9,507.59 vs yday $9,417.72 (+89.87) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `KGC` | 21 | $32.32 | $2.05 | $-60.63 | $19,092.80 | ▼ -60.63 after sell → book $9,505.54; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WPM` | 4 | $156.51 | $2.00 | $-51.92 | $18,464.76 | ▼ -51.92 after sell → book $9,503.54; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $212.00 | $2.00 | $-26.68 | $17,826.76 | ▼ -26.68 after sell → book $9,501.54; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `SCZM` | 67 | $9.45 | $2.19 | $-3.75 | $17,191.42 | ▼ -3.75 after sell → book $9,499.35; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `MRNA` | 4 | $143.50 | $2.00 | $+22.52 | $16,615.42 | ▲ +22.52 after sell → book $9,497.35; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `BNTX` | 5 | $113.88 | $2.00 | $-28.14 | $16,044.01 | ▼ -28.14 after sell → book $9,495.34; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `EL` | 6 | $104.00 | $2.01 | $-43.47 | $15,418.00 | ▼ -43.47 after sell → book $9,493.33; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `LIFE` | 18 | $36.96 | $2.08 | — | $16,081.20 | — | RSI overbought; gate rsi_ob=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $678.10 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMEA` | 416 | $1.63 | $5.46 | — | $16,753.82 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $678.10 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ALVO` | 129 | $5.24 | $2.43 | — | $17,427.35 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $678.10 | — |
| 2026-08-25 09:30 ET | **SHORT** | `FWDI` | 118 | $5.71 | $2.39 | — | $18,098.74 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $678.10 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ASST` | 35 | $19.04 | $2.13 | — | $18,763.01 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ret5=+49.5; leftover $678.10 | — |
| 2026-08-25 09:30 ET | **SHORT** | `JANX` | 36 | $18.72 | $2.14 | — | $19,434.79 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,ohlc_hot; 🔵; ret5=+14.4; leftover $678.10 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMNR` | 28 | $23.80 | $2.11 | — | $20,099.08 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; ret5=+28.9; leftover $678.10 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,099.08 | ▼ close $8,894.16 vs 09:30 $9,507.59 (session -580.43) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,099.08 | ▲ 09:30 equity $9,064.40 vs yday $8,894.16 (+170.24) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `CYPH` | 552 | $1.60 | $7.12 | $-262.76 | $19,208.76 | ▼ -262.76 after sell → book $9,057.28; vs 09:30 mark -7.12 | dropped from list after 4 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AU` | 6 | $119.80 | $2.01 | $-6.28 | $18,487.95 | ▼ -6.28 after sell → book $9,055.27; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARCT` | 74 | $15.35 | $2.21 | $-316.75 | $17,349.84 | ▼ -316.75 after sell → book $9,053.06; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `GMAB` | 24 | $33.78 | $2.06 | $-14.25 | $16,537.05 | ▼ -14.25 after sell → book $9,050.99; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRVI` | 100 | $8.85 | $2.29 | $-61.63 | $15,649.76 | ▼ -61.63 after sell → book $9,048.70; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `DFDV` | 205 | $4.35 | $2.64 | $-68.91 | $14,755.37 | ▼ -68.91 after sell → book $9,046.06; vs 09:30 mark -2.64 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `SENS` | 68 | $9.48 | $2.23 | — | $15,397.78 | — | RSI overbought; gate rsi_ob=True; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $646.15 | — |
| 2026-08-26 09:30 ET | **SHORT** | `KURA` | 47 | $13.63 | $2.17 | — | $16,036.22 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+10.2; leftover $646.15 | — |
| 2026-08-26 09:30 ET | **SHORT** | `XHG` | 169 | $3.81 | $2.55 | — | $16,677.56 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ret5=-6.1; leftover $646.15 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BTG` | 112 | $5.75 | $2.37 | — | $17,319.18 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ret5=+17.9; leftover $646.15 | — |
| 2026-08-26 09:30 ET | **SHORT** | `MRK` | 4 | $154.35 | $2.04 | — | $17,934.54 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+15.7; leftover $646.15 | — |
| 2026-08-26 09:30 ET | **SHORT** | `B` | 13 | $48.18 | $2.07 | — | $18,558.82 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ret5=+16.1; leftover $646.15 | — |
| 2026-08-26 09:30 ET | **SHORT** | `PEPG` | 189 | $3.41 | $2.62 | — | $19,200.69 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ret5=+17.7; leftover $646.15 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,200.69 | ▼ close $8,994.20 vs 09:30 $9,064.40 (session -35.81) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,200.69 | ▼ 09:30 equity $8,901.26 vs yday $8,994.20 (-92.94) | — | — |
| 2026-08-27 09:30 ET | **COVER** | `CRDL` | 429 | $2.09 | $5.53 | $-79.81 | $18,298.55 | ▼ -79.81 after sell → book $8,895.73; vs 09:30 mark -5.53 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SHORT** | `OABI` | 184 | $4.81 | $2.61 | — | $19,180.98 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+14.8; leftover $889.57 | — |
| 2026-08-27 09:30 ET | **SHORT** | `HTFL` | 18 | $48.92 | $2.09 | — | $20,059.45 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+7.5; leftover $889.57 | — |
| 2026-08-27 09:30 ET | **SHORT** | `SBET` | 105 | $8.45 | $2.36 | — | $20,944.35 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+17.5; leftover $889.57 | — |
| 2026-08-27 09:30 ET | **SHORT** | `EL` | 8 | $104.49 | $2.06 | — | $21,778.21 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+7.2; leftover $889.57 | — |
| 2026-08-27 09:30 ET | **SHORT** | `DASH` | 3 | $235.94 | $2.04 | — | $22,483.99 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+7.6; leftover $889.57 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,483.99 | ▲ close $8,917.71 vs 09:30 $8,901.26 (session +33.13) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,483.99 | ▲ 09:30 equity $9,005.56 vs yday $8,917.71 (+87.85) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `LIFE` | 18 | $39.60 | $2.04 | $-51.65 | $21,769.15 | ▼ -51.65 after sell → book $9,003.52; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMEA` | 416 | $1.69 | $5.37 | $-35.79 | $21,060.74 | ▼ -35.79 after sell → book $8,998.15; vs 09:30 mark -5.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ALVO` | 129 | $4.84 | $2.38 | $+46.80 | $20,434.00 | ▲ +46.80 after sell → book $8,995.77; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `FWDI` | 118 | $6.73 | $2.34 | $-125.10 | $19,637.52 | ▼ -125.10 after sell → book $8,993.43; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ASST` | 35 | $22.50 | $2.10 | $-125.33 | $18,847.92 | ▼ -125.33 after sell → book $8,991.33; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `JANX` | 36 | $18.57 | $2.10 | $+1.17 | $18,177.31 | ▲ +1.17 after sell → book $8,989.24; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMNR` | 28 | $25.10 | $2.07 | $-40.59 | $17,472.43 | ▼ -40.59 after sell → book $8,987.16; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `ANF` | 5 | $146.07 | $2.05 | — | $18,200.74 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $748.93 | — |
| 2026-08-28 09:30 ET | **SHORT** | `BZ` | 41 | $18.15 | $2.15 | — | $18,942.73 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+14.1; leftover $748.93 | — |
| 2026-08-28 09:30 ET | **SHORT** | `CRDL` | 363 | $2.06 | $4.77 | — | $19,685.74 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; ret5=+9.3; leftover $748.93 | — |
| 2026-08-28 09:30 ET | **SHORT** | `CXM` | 95 | $7.88 | $2.32 | — | $20,432.02 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ret5=+10.3; leftover $748.93 | — |
| 2026-08-28 09:30 ET | **SHORT** | `PATH` | 41 | $18.12 | $2.15 | — | $21,172.99 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ret5=+15.1; leftover $748.93 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FSM` | 58 | $12.84 | $2.20 | — | $21,915.51 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+7.6; leftover $748.93 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,915.51 | ▲ close $9,219.46 vs 09:30 $9,005.56 (session +247.95) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,915.51 | ▲ 09:30 equity $9,255.80 vs yday $9,219.46 (+36.34) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `SENS` | 68 | $9.29 | $2.19 | $+8.49 | $21,281.59 | ▲ +8.49 after sell → book $9,253.60; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `KURA` | 47 | $12.71 | $2.13 | $+38.94 | $20,682.09 | ▲ +38.94 after sell → book $9,251.47; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `XHG` | 169 | $3.45 | $2.50 | $+55.79 | $20,096.54 | ▲ +55.79 after sell → book $9,248.97; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `BTG` | 112 | $5.63 | $2.33 | $+8.74 | $19,463.66 | ▲ +8.74 after sell → book $9,246.65; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `MRK` | 4 | $147.00 | $2.00 | $+25.36 | $18,873.66 | ▲ +25.36 after sell → book $9,244.65; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `B` | 13 | $45.64 | $2.03 | $+28.92 | $18,278.31 | ▲ +28.92 after sell → book $9,242.62; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `PEPG` | 189 | $3.04 | $2.56 | $+64.76 | $17,701.19 | ▲ +64.76 after sell → book $9,240.06; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,701.19 | ▼ close $9,178.34 vs 09:30 $9,255.80 (session -61.73) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,701.19 | ▲ 09:30 equity $9,302.91 vs yday $9,178.34 (+124.57) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `OABI` | 184 | $4.35 | $2.54 | $+79.49 | $16,898.25 | ▲ +79.49 after sell → book $9,300.37; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `HTFL` | 18 | $50.28 | $2.04 | $-28.61 | $15,991.16 | ▼ -28.61 after sell → book $9,298.32; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `SBET` | 105 | $8.29 | $2.31 | $+12.14 | $15,118.41 | ▲ +12.14 after sell → book $9,296.02; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `EL` | 8 | $101.32 | $2.01 | $+21.29 | $14,305.84 | ▲ +21.29 after sell → book $9,294.01; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `DASH` | 3 | $230.09 | $2.00 | $+13.51 | $13,613.57 | ▲ +13.51 after sell → book $9,292.01; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,613.57 | ▼ close $9,262.93 vs 09:30 $9,302.91 (session -29.08) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,613.57 | ▼ 09:30 equity $9,258.16 vs yday $9,262.93 (-4.77) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `ANF` | 5 | $139.65 | $2.00 | $+28.05 | $12,913.31 | ▲ +28.05 after sell → book $9,256.15; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `BZ` | 41 | $17.65 | $2.11 | $+16.23 | $12,187.55 | ▲ +16.23 after sell → book $9,254.04; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `CRDL` | 363 | $2.16 | $4.68 | $-45.76 | $11,398.79 | ▼ -45.76 after sell → book $9,249.36; vs 09:30 mark -4.68 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `CXM` | 95 | $7.40 | $2.27 | $+41.00 | $10,693.51 | ▲ +41.00 after sell → book $9,247.08; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `PATH` | 41 | $18.19 | $2.11 | $-6.93 | $9,945.61 | ▼ -6.93 after sell → book $9,244.97; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FSM` | 58 | $12.08 | $2.16 | $+39.71 | $9,242.80 | ▲ +39.71 after sell → book $9,242.80; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,242.80 | ▲ close $9,242.80 vs 09:30 $9,258.16 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,242.80 | ▲ 09:30 equity $9,242.80 vs yday $9,242.80 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **SHORT** | `ATRC` | 10 | $52.88 | $2.05 | — | $9,769.55 | — | RSI overbought; gate rsi_ob=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $577.68 | — |
| 2026-09-03 09:30 ET | **SHORT** | `CRK` | 37 | $15.45 | $2.14 | — | $10,339.06 | — | RSI overbought; gate rsi_ob=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $577.68 | — |
| 2026-09-03 09:30 ET | **SHORT** | `ARCT` | 34 | $16.77 | $2.13 | — | $10,907.11 | — | RSI overbought; gate rsi_ob=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $577.68 | — |
| 2026-09-03 09:30 ET | **SHORT** | `CRDL` | 264 | $2.18 | $3.48 | — | $11,479.16 | — | RSI overbought; gate rsi_ob=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $577.68 | — |
| 2026-09-03 09:30 ET | **SHORT** | `GPRO` | 324 | $1.78 | $4.26 | — | $12,051.62 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+183.1; leftover $577.68 | — |
| 2026-09-03 09:30 ET | **SHORT** | `MMED` | 24 | $23.88 | $2.10 | — | $12,622.64 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $577.68 | — |
| 2026-09-03 09:30 ET | **SHORT** | `NVAX` | 55 | $10.42 | $2.19 | — | $13,193.55 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $577.68 | — |
| 2026-09-03 09:30 ET | **SHORT** | `CNXC` | 17 | $32.88 | $2.08 | — | $13,750.44 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ret5=+16.2; leftover $577.68 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,750.44 | ▲ close $9,423.74 vs 09:30 $9,242.80 (session +201.35) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,750.44 | ▼ 09:30 equity $9,392.82 vs yday $9,423.74 (-30.92) | — | — |
| 2026-09-04 09:30 ET | **SHORT** | `BMEA` | 411 | $1.90 | $5.40 | — | $14,525.93 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $782.73 | — |
| 2026-09-04 09:30 ET | **SHORT** | `OABI` | 163 | $4.78 | $2.54 | — | $15,302.54 | — | RSI overbought; gate rsi_ob=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $782.73 | — |
| 2026-09-04 09:30 ET | **SHORT** | `OPK` | 492 | $1.59 | $6.46 | — | $16,078.36 | — | RSI overbought; gate rsi_ob=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $782.73 | — |
| 2026-09-04 09:30 ET | **SHORT** | `CRM` | 2 | $263.36 | $2.03 | — | $16,603.05 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $782.73 | — |
| 2026-09-04 09:30 ET | **SHORT** | `HRMY` | 18 | $41.50 | $2.08 | — | $17,347.96 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $782.73 | — |
| 2026-09-04 09:30 ET | **SHORT** | `FMC` | 60 | $12.95 | $2.21 | — | $18,122.75 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+21.8; leftover $782.73 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,122.75 | ▼ close $9,301.51 vs 09:30 $9,392.82 (session -70.58) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,122.75 | ▲ 09:30 equity $9,362.38 vs yday $9,301.51 (+60.87) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,122.75 | ▲ close $9,544.16 vs 09:30 $9,362.38 (session +181.78) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,122.75 | ▲ 09:30 equity $9,580.45 vs yday $9,544.16 (+36.29) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `CRK` | 37 | $15.16 | $2.10 | $+6.49 | $17,559.73 | ▲ +6.49 after sell → book $9,578.35; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `ARCT` | 34 | $15.46 | $2.09 | $+40.32 | $17,032.00 | ▲ +40.32 after sell → book $9,576.26; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `CRDL` | 264 | $2.22 | $3.41 | $-17.44 | $16,442.51 | ▼ -17.44 after sell → book $9,572.85; vs 09:30 mark -3.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `GPRO` | 324 | $1.45 | $4.18 | $+98.48 | $15,968.53 | ▲ +98.48 after sell → book $9,568.67; vs 09:30 mark -4.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `MMED` | 24 | $23.22 | $2.06 | $+11.68 | $15,409.19 | ▲ +11.68 after sell → book $9,566.61; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `NVAX` | 55 | $10.02 | $2.15 | $+17.65 | $14,855.94 | ▲ +17.65 after sell → book $9,564.46; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `CNXC` | 17 | $28.13 | $2.04 | $+76.63 | $14,375.68 | ▲ +76.63 after sell → book $9,562.41; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,375.68 | ▲ close $9,700.08 vs 09:30 $9,580.45 (session +137.67) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,375.68 | ▲ 09:30 equity $9,750.83 vs yday $9,700.08 (+50.75) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `ATRC` | 10 | $52.31 | $2.02 | $+1.63 | $13,850.56 | ▲ +1.63 after sell → book $9,748.81; vs 09:30 mark -2.02 | dropped from list after 4 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `BMEA` | 411 | $1.83 | $5.30 | $+18.07 | $13,093.13 | ▲ +18.07 after sell → book $9,743.51; vs 09:30 mark -5.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `OABI` | 163 | $3.92 | $2.48 | $+134.84 | $12,451.37 | ▲ +134.84 after sell → book $9,741.03; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `OPK` | 492 | $1.53 | $6.35 | $+16.71 | $11,692.26 | ▲ +16.71 after sell → book $9,734.68; vs 09:30 mark -6.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `CRM` | 2 | $245.35 | $2.00 | $+31.99 | $11,199.56 | ▲ +31.99 after sell → book $9,732.68; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `HRMY` | 18 | $41.26 | $2.04 | $+0.19 | $10,454.84 | ▲ +0.19 after sell → book $9,730.64; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `FMC` | 60 | $12.07 | $2.17 | $+48.42 | $9,728.47 | ▲ +48.42 after sell → book $9,728.47; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,728.47 | ▲ close $9,728.47 vs 09:30 $9,750.83 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,728.47 | ▲ 09:30 equity $9,728.47 vs yday $9,728.47 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **SHORT** | `INDP` | 225 | $2.70 | $2.97 | — | $10,333.00 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $608.03 | — |
| 2026-09-11 09:30 ET | **SHORT** | `WLTH` | 55 | $10.95 | $2.19 | — | $10,933.06 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $608.03 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BNC` | 123 | $4.91 | $2.41 | — | $11,534.59 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $608.03 | — |
| 2026-09-11 09:30 ET | **SHORT** | `SWKS` | 7 | $84.27 | $2.05 | — | $12,122.43 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $608.03 | — |
| 2026-09-11 09:30 ET | **SHORT** | `ANGX` | 113 | $5.38 | $2.37 | — | $12,728.00 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ret5=+19.8; leftover $608.03 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 5 | $112.83 | $2.04 | — | $13,290.13 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $608.03 | — |
| 2026-09-11 09:30 ET | **SHORT** | `ASO` | 11 | $54.91 | $2.06 | — | $13,892.08 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ret5=+24.3; leftover $608.03 | — |
| 2026-09-11 09:30 ET | **SHORT** | `IRD` | 98 | $6.16 | $2.33 | — | $14,493.43 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ret5=+36.4; leftover $608.03 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,493.43 | ▼ close $9,690.45 vs 09:30 $9,728.47 (session -19.61) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,493.43 | ▼ 09:30 equity $9,684.20 vs yday $9,690.45 (-6.25) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,493.43 | ▼ close $9,657.20 vs 09:30 $9,684.20 (session -27.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,493.43 | ▼ 09:30 equity $9,624.04 vs yday $9,657.20 (-33.16) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,493.43 | ▼ close $9,531.64 vs 09:30 $9,624.04 (session -92.40) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,493.43 | ▲ 09:30 equity $9,546.67 vs yday $9,531.64 (+15.03) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `WLTH` | 55 | $10.82 | $2.15 | $+2.80 | $13,896.18 | ▲ +2.80 after sell → book $9,544.52; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BNC` | 123 | $4.77 | $2.36 | $+12.46 | $13,307.11 | ▲ +12.46 after sell → book $9,542.16; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `ANGX` | 113 | $5.30 | $2.33 | $+4.34 | $12,705.88 | ▲ +4.34 after sell → book $9,539.83; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `ASO` | 11 | $50.69 | $2.02 | $+42.34 | $12,146.27 | ▲ +42.34 after sell → book $9,537.81; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `IRD` | 98 | $5.80 | $2.28 | $+30.67 | $11,575.58 | ▲ +30.67 after sell → book $9,535.52; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SHORT** | `AVAH` | 66 | $14.31 | $2.23 | — | $12,517.81 | — | RSI overbought; gate rsi_ob=True; list flatten; ret5=+4.8; leftover $953.55 | — |
| 2026-09-16 09:30 ET | **SHORT** | `HLP` | 529 | $1.80 | $6.95 | — | $13,463.06 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $953.55 | — |
| 2026-09-16 09:30 ET | **SHORT** | `SDGR` | 40 | $23.29 | $2.16 | — | $14,392.51 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ret5=+16.1; leftover $953.55 | — |
| 2026-09-16 09:30 ET | **SHORT** | `CAI` | 33 | $28.16 | $2.13 | — | $15,319.65 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,ohlc_hot; ret5=+14.8; leftover $953.55 | — |
| 2026-09-16 09:30 ET | **SHORT** | `RVTY` | 6 | $140.88 | $2.05 | — | $16,162.88 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,ohlc_hot; 🔵; ret5=+10.3; leftover $953.55 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,162.88 | ▼ close $9,472.95 vs 09:30 $9,546.67 (session -47.05) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,162.88 | ▼ 09:30 equity $9,388.98 vs yday $9,472.95 (-83.97) | — | — |
| 2026-09-17 09:30 ET | **COVER** | `SWKS` | 7 | $86.76 | $2.01 | $-21.49 | $15,553.55 | ▼ -21.49 after sell → book $9,386.97; vs 09:30 mark -2.01 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **COVER** | `QRVO` | 5 | $114.90 | $2.00 | $-14.37 | $14,977.04 | ▼ -14.37 after sell → book $9,384.97; vs 09:30 mark -2.00 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **SHORT** | `IOVA` | 91 | $10.25 | $2.31 | — | $15,907.48 | — | RSI overbought; gate rsi_ob=True; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $938.50 | — |
| 2026-09-17 09:30 ET | **SHORT** | `ADPT` | 33 | $28.23 | $2.13 | — | $16,836.94 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+13.3; leftover $938.50 | — |
| 2026-09-17 09:30 ET | **SHORT** | `FRO` | 17 | $54.31 | $2.09 | — | $17,758.12 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+13.7; leftover $938.50 | — |
| 2026-09-17 09:30 ET | **SHORT** | `CVI` | 18 | $51.88 | $2.09 | — | $18,689.87 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+11.1; leftover $938.50 | — |
| 2026-09-17 09:30 ET | **SHORT** | `DHT` | 40 | $22.97 | $2.16 | — | $19,606.51 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+8.7; leftover $938.50 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,606.51 | ▼ close $8,961.52 vs 09:30 $9,388.98 (session -412.67) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,606.51 | ▲ 09:30 equity $9,101.15 vs yday $8,961.52 (+139.63) | — | — |
| 2026-09-18 09:30 ET | **SHORT** | `ECO` | 10 | $85.00 | $2.06 | — | $20,454.45 | — | RSI overbought; gate rsi_ob=True; list flatten; 🔵; ⚪; ret5=+18.3; leftover $910.12 | — |
| 2026-09-18 09:30 ET | **SHORT** | `CYPH` | 299 | $3.04 | $3.94 | — | $21,357.97 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $910.12 | — |
| 2026-09-18 09:30 ET | **SHORT** | `TEM` | 11 | $81.40 | $2.07 | — | $22,251.31 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; leftover $910.12 | — |
| 2026-09-18 09:30 ET | **SHORT** | `CHPT` | 91 | $10.00 | $2.31 | — | $23,158.99 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ⚪; ret5=+11.1; leftover $910.12 | — |
| 2026-09-18 09:30 ET | **SHORT** | `GME` | 39 | $22.90 | $2.15 | — | $24,049.94 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $910.12 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,049.94 | ▼ close $9,017.09 vs 09:30 $9,101.15 (session -71.52) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,049.94 | ▼ 09:30 equity $8,865.85 vs yday $9,017.09 (-151.24) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `INDP` | 225 | $3.55 | $2.90 | $-197.12 | $23,248.29 | ▼ -197.12 after sell → book $8,862.95; vs 09:30 mark -2.90 | dropped from list after 6 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `AVAH` | 66 | $13.65 | $2.19 | $+39.14 | $22,345.20 | ▲ +39.14 after sell → book $8,860.76; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `HLP` | 529 | $2.08 | $6.82 | $-161.89 | $21,238.06 | ▼ -161.89 after sell → book $8,853.94; vs 09:30 mark -6.82 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `SDGR` | 40 | $29.43 | $2.11 | $-249.87 | $20,058.75 | ▼ -249.87 after sell → book $8,851.83; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `CAI` | 33 | $30.23 | $2.09 | $-72.53 | $19,059.07 | ▼ -72.53 after sell → book $8,849.74; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `RVTY` | 6 | $144.53 | $2.01 | $-25.96 | $18,189.88 | ▼ -25.96 after sell → book $8,847.73; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SHORT** | `TJGC` | 43 | $16.91 | $2.16 | — | $18,914.85 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+50.5; leftover $737.31 | — |
| 2026-09-21 09:30 ET | **SHORT** | `SECZ` | 63 | $11.67 | $2.22 | — | $19,647.84 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+31.3; leftover $737.31 | — |
| 2026-09-21 09:30 ET | **SHORT** | `FEAM` | 298 | $2.47 | $3.92 | — | $20,379.98 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+73.6; leftover $737.31 | — |
| 2026-09-21 09:30 ET | **SHORT** | `NEO` | 37 | $19.92 | $2.14 | — | $21,114.88 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+15.0; leftover $737.31 | — |
| 2026-09-21 09:30 ET | **SHORT** | `ALVO` | 124 | $5.92 | $2.41 | — | $21,846.54 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+11.0; leftover $737.31 | — |
| 2026-09-21 09:30 ET | **SHORT** | `ASST` | 23 | $31.64 | $2.10 | — | $22,572.17 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ret5=+8.9; leftover $737.31 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,572.17 | ▲ close $9,088.49 vs 09:30 $8,865.85 (session +255.71) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,572.17 | ▼ 09:30 equity $9,085.80 vs yday $9,088.49 (-2.69) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `IOVA` | 91 | $10.18 | $2.26 | $+1.79 | $21,643.52 | ▲ +1.79 after sell → book $9,083.53; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SHORT** | `GLND` | 220 | $2.94 | $2.90 | — | $22,287.42 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+136.1; leftover $648.82 | — |
| 2026-09-22 09:30 ET | **SHORT** | `NUAI` | 89 | $7.23 | $2.30 | — | $22,928.59 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+36.6; leftover $648.82 | — |
| 2026-09-22 09:30 ET | **SHORT** | `USDE` | 49 | $12.99 | $2.17 | — | $23,562.93 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+69.4; leftover $648.82 | — |
| 2026-09-22 09:30 ET | **SHORT** | `VGZ` | 244 | $2.65 | $3.22 | — | $24,206.31 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+26.3; leftover $648.82 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,206.31 | ▼ close $9,040.49 vs 09:30 $9,085.80 (session -32.45) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,206.31 | ▲ 09:30 equity $9,096.46 vs yday $9,040.49 (+55.97) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `ADPT` | 33 | $27.74 | $2.09 | $+11.95 | $23,288.80 | ▲ +11.95 after sell → book $9,094.37; vs 09:30 mark -2.09 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `FRO` | 17 | $47.92 | $2.04 | $+104.50 | $22,472.12 | ▲ +104.50 after sell → book $9,092.33; vs 09:30 mark -2.04 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `CVI` | 18 | $52.66 | $2.04 | $-18.17 | $21,522.20 | ▼ -18.17 after sell → book $9,090.29; vs 09:30 mark -2.04 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `DHT` | 40 | $21.35 | $2.11 | $+60.53 | $20,666.09 | ▲ +60.53 after sell → book $9,088.18; vs 09:30 mark -2.11 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `ECO` | 10 | $77.55 | $2.02 | $+70.42 | $19,888.57 | ▲ +70.42 after sell → book $9,086.16; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `CYPH` | 299 | $3.82 | $3.86 | $-242.51 | $18,742.53 | ▼ -242.51 after sell → book $9,082.30; vs 09:30 mark -3.86 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `TEM` | 11 | $76.47 | $2.02 | $+50.14 | $17,899.34 | ▲ +50.14 after sell → book $9,080.28; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `CHPT` | 91 | $9.76 | $2.26 | $+17.26 | $17,008.91 | ▲ +17.26 after sell → book $9,078.01; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `GME` | 39 | $23.94 | $2.11 | $-44.82 | $16,073.15 | ▼ -44.82 after sell → book $9,075.91; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SHORT** | `A` | 3 | $166.54 | $2.03 | — | $16,570.73 | — | RSI overbought; gate rsi_ob=True; list flatten; 🔵; ⚪; ret5=+10.3; leftover $648.28 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 5 | $116.85 | $2.04 | — | $17,152.94 | — | RSI overbought; gate rsi_ob=True; list flatten; 🔵; ⚪; ret5=+3.3; leftover $648.28 | — |
| 2026-09-23 09:30 ET | **SHORT** | `OMER` | 31 | $20.65 | $2.12 | — | $17,790.97 | — | RSI overbought; gate rsi_ob=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $648.28 | — |
| 2026-09-23 09:30 ET | **SHORT** | `VKTX` | 15 | $41.76 | $2.07 | — | $18,415.30 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $648.28 | — |
| 2026-09-23 09:30 ET | **SHORT** | `DNA` | 71 | $9.13 | $2.24 | — | $19,061.29 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+13.8; leftover $648.28 | — |
| 2026-09-23 09:30 ET | **SHORT** | `INOD` | 9 | $70.84 | $2.05 | — | $19,696.79 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $648.28 | — |
| 2026-09-23 09:30 ET | **SHORT** | `SVIA` | 144 | $4.49 | $2.47 | — | $20,340.88 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+26.4; leftover $648.28 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,340.88 | ▼ close $8,857.15 vs 09:30 $9,096.46 (session -203.72) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,340.88 | ▲ 09:30 equity $8,911.24 vs yday $8,857.15 (+54.09) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `FEAM` | 298 | $2.68 | $3.84 | $-70.35 | $19,538.40 | ▼ -70.35 after sell → book $8,907.40; vs 09:30 mark -3.84 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `NEO` | 37 | $18.47 | $2.10 | $+49.41 | $18,852.90 | ▲ +49.41 after sell → book $8,905.30; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `ALVO` | 124 | $5.75 | $2.36 | $+16.31 | $18,137.54 | ▲ +16.31 after sell → book $8,902.94; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `ASST` | 23 | $28.52 | $2.06 | $+67.60 | $17,479.52 | ▲ +67.60 after sell → book $8,900.88; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,479.52 | ▼ close $7,858.13 vs 09:30 $8,911.24 (session -1,042.75) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,510.00 | ▼ 09:30 equity $8,254.54 vs yday $8,554.21 (-299.67) | 09:30 open · cash $19,510.00 (unchanged overnight, no fees) · equity $8,254.54 vs prior close $8,554.21 (-299.67) · 15 name(s) re-marked at the open (per-name table). A×4 yday $172.84 → 09:30 $171.98 +3.44; AMRX×30 yday $19.81 → 09:30 $19.81 -0.00; DNA×75 yday $10.25 → 09:30 $10.20 +3.75; FWDI×75 yday $8.35 → 09:30 $8.35 -0.00; GLND×210 yday $5.35 → 09:30 $6.06 -149.10; GRAL×5 yday $125.21 → 09:30 $123.50 +8.55; HALO×5 yday $115.22 → 09:30 $115.36 -0.70; INOD×9 yday $71.87 → 09:30 $71.87 -0.00; NUAI×85 yday $6.94 → 09:30 $6.94 -0.00; OMER×33 yday $20.13 → 09:30 $20.61 -15.84; SVIA×154 yday $3.96 → 09:30 $3.96 -0.00; TJGC×55 yday $28.20 → 09:30 $29.76 -85.80; USDE×47 yday $14.22 → 09:30 $15.58 -63.97; VGZ×233 yday $2.71 → 09:30 $2.71 -0.00; VKTX×16 yday $36.75 → 09:30 $36.75 -0.00 | — |
| 2026-09-25 09:30 ET | **COVER** | `GRAL` | 5 | $123.50 | $2.00 | $-87.79 | $18,890.49 | ▼ -87.79 after sell → book $8,252.53; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **COVER** | `USDE` | 47 | $15.58 | $2.13 | $-126.08 | $18,156.05 | ▼ -126.08 after sell → book $8,250.40; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `TXG` | 9 | $83.76 | $2.06 | — | $18,907.83 | — | RSI overbought; gate rsi_ob=True; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $825.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `HLP` | 375 | $2.20 | $4.93 | — | $19,727.90 | — | RSI overbought; gate rsi_ob=True; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $825.04 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `TEM` | 9 | $83.69 | $2.06 | — | $20,479.10 | — | RSI overbought; gate rsi_ob=True; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $825.04 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `TWST` | 4 | $184.00 | $2.04 | — | $21,213.06 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $825.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `SECZ` | 50 | $16.21 | $2.18 | — | $22,021.38 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $825.04 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,021.38 | ▲ close $8,511.03 vs 09:30 $8,254.54 (session +273.89) | 16:00 close · cash $22,021.38 · equity $8,511.03 vs 09:30 $8,254.54 (+256.49; session marks +273.89) · 18 name(s) marked open→close (per-name table). A×4 09:30 $171.98 → close $172.79 -3.24; AMRX×30 09:30 $19.80 → close $19.80 -0.00; DNA×75 09:30 $10.20 → close $10.66 -34.50; FWDI×75 09:30 $8.35 → close $8.35 -0.00; GLND×210 09:30 $6.06 → close $5.54 +109.20; HALO×5 09:30 $115.36 → close $113.90 +7.30; INOD×9 09:30 $71.87 → close $71.87 -0.00; NUAI×85 09:30 $6.94 → close $6.94 -0.00; OMER×33 09:30 $20.61 → close $20.08 +17.49; SVIA×154 09:30 $3.96 → close $3.96 -0.00; TJGC×55 09:30 $29.76 → close $26.24 +193.60; VGZ×233 09:30 $2.71 → close $2.71 -0.00; VKTX×16 09:30 $36.75 → close $36.75 -0.00; TXG×9 09:30 $83.76 → close $85.71 -17.55; HLP×375 09:30 $2.20 → close $2.21 -3.75; TEM×9 09:30 $83.69 → close $85.01 -11.84; TWST×4 09:30 $184.00 → close $182.83 +4.68; SECZ×50 09:30 $16.21 → close $15.96 +12.50 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MXCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TBBB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AMPY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OMER` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MXCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TBBB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AMPY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HTFL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `UMAC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NMAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `SGMT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `U` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `WFF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DSX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `REAX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `HTFL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `UMAC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CLYM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `XHG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `SGMT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `U` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BNTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `EL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BNTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `GMAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `USDE` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CAN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ASST` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `XHG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NIQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `GMAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FWDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `JANX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FWDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `JANX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BTG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `MRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `B` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `PEPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `XHG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BTG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `B` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `PEPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `HTFL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `DASH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `HTFL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `EL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `DASH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CXM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PATH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FSM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CXM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FSM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `STT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PURR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ARCT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `LAND` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DINO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NVAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CNXC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `GPRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `NVAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CNXC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `FMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `DFDV` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BRR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `DPRO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `PURR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `CRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `FMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CABA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LIFE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XHLD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SID` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CVI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `WLTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ASO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `IRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `XHLD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CMRC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TJGC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `WLTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ASO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `IRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBLX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GME` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SDGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `HLP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `FRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `DHT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `ADPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `FRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `CVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `DHT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `CHPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `GME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ADPT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FRO` | no_price | no 09:30 open — carry |
| 2026-09-22 | `CVI` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DHT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ECO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `CHPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `GME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `TJGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `NEO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `AMRX` | no_price | no 09:30 open |
| 2026-09-23 | `TJGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SECZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `NEO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GLND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `NUAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `VGZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `NUAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `VGZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VKTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INOD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CTKB` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CAI` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `TJGC` | 43 | 2026-09-21 @ $16.91 | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+50.5; leftover $737.31 |
| `SECZ` | 63 | 2026-09-21 @ $11.67 | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+31.3; leftover $737.31 |
| `GLND` | 220 | 2026-09-22 @ $2.94 | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+136.1; leftover $648.82 |
| `NUAI` | 89 | 2026-09-22 @ $7.23 | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+36.6; leftover $648.82 |
| `USDE` | 49 | 2026-09-22 @ $12.99 | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+69.4; leftover $648.82 |
| `VGZ` | 244 | 2026-09-22 @ $2.65 | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+26.3; leftover $648.82 |
| `A` | 3 | 2026-09-23 @ $166.54 | RSI overbought; gate rsi_ob=True; list flatten; 🔵; ⚪; ret5=+10.3; leftover $648.28 |
| `HALO` | 5 | 2026-09-23 @ $116.85 | RSI overbought; gate rsi_ob=True; list flatten; 🔵; ⚪; ret5=+3.3; leftover $648.28 |
| `OMER` | 31 | 2026-09-23 @ $20.65 | RSI overbought; gate rsi_ob=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $648.28 |
| `VKTX` | 15 | 2026-09-23 @ $41.76 | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $648.28 |
| `DNA` | 71 | 2026-09-23 @ $9.13 | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+13.8; leftover $648.28 |
| `INOD` | 9 | 2026-09-23 @ $70.84 | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $648.28 |
| `SVIA` | 144 | 2026-09-23 @ $4.49 | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+26.4; leftover $648.28 |
