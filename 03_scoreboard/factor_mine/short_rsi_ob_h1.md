# Factor mine action — `short_rsi_ob_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · RSI overbought

Cash book **-0.76%** ($9,924) · signal-only (no cash/fees) was -20.71%. Starts YES **6/30**. Fills 241 · skips 95 · realized $-976.57.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `rsi_ob=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,649.58.

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
| 2026-08-14 09:30 ET | **COVER** | `TPG` | 49 | $55.29 | $2.14 | $-233.04 | $12,260.85 | ▼ -233.04 after sell → book $9,808.41; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **COVER** | `TNDM` | 107 | $22.92 | $2.31 | $+39.14 | $9,806.10 | ▲ +39.14 after sell → book $9,806.10; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SHORT** | `ARX` | 31 | $19.57 | $2.12 | — | $10,410.65 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $612.88 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OMER` | 35 | $17.35 | $2.13 | — | $11,015.77 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $612.88 | — |
| 2026-08-14 09:30 ET | **SHORT** | `AIRO` | 55 | $11.12 | $2.19 | — | $11,625.17 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $612.88 | — |
| 2026-08-14 09:30 ET | **SHORT** | `MXCT` | 440 | $1.39 | $5.78 | — | $12,231.00 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+25.2; leftover $612.88 | — |
| 2026-08-14 09:30 ET | **SHORT** | `TBBB` | 12 | $48.82 | $2.06 | — | $12,814.78 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $612.88 | — |
| 2026-08-14 09:30 ET | **SHORT** | `AMPY` | 124 | $4.94 | $2.41 | — | $13,424.93 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.4; leftover $612.88 | — |
| 2026-08-14 09:30 ET | **SHORT** | `MH` | 45 | $13.55 | $2.16 | — | $14,032.52 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $612.88 | — |
| 2026-08-14 09:30 ET | **SHORT** | `CRDL` | 354 | $1.73 | $4.65 | — | $14,640.28 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ret5=+25.7; leftover $612.88 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,640.28 | ▲ close $9,931.60 vs 09:30 $9,810.54 (session +149.01) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,640.28 | ▲ 09:30 equity $9,949.57 vs yday $9,931.60 (+17.97) | — | — |
| 2026-08-17 09:30 ET | **COVER** | `ARX` | 31 | $19.57 | $2.08 | $-4.20 | $14,031.53 | ▼ -4.20 after sell → book $9,947.49; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `OMER` | 35 | $17.17 | $2.10 | $+2.07 | $13,428.48 | ▲ +2.07 after sell → book $9,945.39; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AIRO` | 55 | $9.57 | $2.15 | $+80.90 | $12,899.98 | ▲ +80.90 after sell → book $9,943.24; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `MXCT` | 440 | $1.32 | $5.68 | $+19.35 | $12,313.50 | ▲ +19.35 after sell → book $9,937.56; vs 09:30 mark -5.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `TBBB` | 12 | $47.39 | $2.03 | $+13.07 | $11,742.80 | ▲ +13.07 after sell → book $9,935.54; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AMPY` | 124 | $4.86 | $2.36 | $+5.15 | $11,137.80 | ▲ +5.15 after sell → book $9,933.18; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `MH` | 45 | $13.16 | $2.12 | $+13.26 | $10,543.47 | ▲ +13.26 after sell → book $9,931.05; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `CRDL` | 354 | $1.73 | $4.57 | $-9.22 | $9,926.48 | ▼ -9.22 after sell → book $9,926.48; vs 09:30 mark -4.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `HTFL` | 15 | $41.23 | $2.07 | — | $10,542.86 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+46.0; leftover $620.41 | — |
| 2026-08-17 09:30 ET | **SHORT** | `UMAC` | 19 | $32.55 | $2.08 | — | $11,159.23 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $620.41 | — |
| 2026-08-17 09:30 ET | **SHORT** | `NPWR` | 323 | $1.92 | $4.25 | — | $11,775.14 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $620.41 | — |
| 2026-08-17 09:30 ET | **SHORT** | `NMAX` | 56 | $10.97 | $2.19 | — | $12,387.27 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $620.41 | — |
| 2026-08-17 09:30 ET | **SHORT** | `CLYM` | 38 | $16.25 | $2.14 | — | $13,002.63 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,ohlc_hot; ret5=+16.6; leftover $620.41 | — |
| 2026-08-17 09:30 ET | **SHORT** | `XHG` | 148 | $4.19 | $2.49 | — | $13,620.26 | — | RSI overbought; gate rsi_ob=True; list yday_mover; ⚪; ret5=+291.8; leftover $620.41 | — |
| 2026-08-17 09:30 ET | **SHORT** | `SGMT` | 59 | $10.45 | $2.20 | — | $14,234.61 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+16.4; leftover $620.41 | — |
| 2026-08-17 09:30 ET | **SHORT** | `U` | 13 | $46.21 | $2.07 | — | $14,833.27 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ⚪; ret5=+7.6; leftover $620.41 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,833.27 | ▲ close $10,044.75 vs 09:30 $9,949.57 (session +137.76) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,833.27 | ▲ 09:30 equity $10,110.09 vs yday $10,044.75 (+65.34) | — | — |
| 2026-08-18 09:30 ET | **COVER** | `HTFL` | 15 | $41.50 | $2.04 | $-8.16 | $14,208.74 | ▼ -8.16 after sell → book $10,108.06; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `UMAC` | 19 | $28.59 | $2.05 | $+71.11 | $13,663.48 | ▲ +71.11 after sell → book $10,106.01; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `NPWR` | 323 | $1.70 | $4.17 | $+62.65 | $13,110.21 | ▲ +62.65 after sell → book $10,101.84; vs 09:30 mark -4.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `NMAX` | 56 | $10.31 | $2.16 | $+32.61 | $12,530.69 | ▲ +32.61 after sell → book $10,099.68; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `CLYM` | 38 | $16.90 | $2.10 | $-28.94 | $11,886.39 | ▼ -28.94 after sell → book $10,097.58; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `XHG` | 148 | $3.94 | $2.43 | $+32.08 | $11,300.84 | ▲ +32.08 after sell → book $10,095.15; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `SGMT` | 59 | $10.41 | $2.17 | $-2.01 | $10,684.48 | ▼ -2.01 after sell → book $10,092.98; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `U` | 13 | $45.50 | $2.03 | $+5.14 | $10,090.95 | ▲ +5.14 after sell → book $10,090.95; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,090.95 | ▲ close $10,090.95 vs 09:30 $10,110.09 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,090.95 | ▲ 09:30 equity $10,090.95 vs yday $10,090.95 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,090.95 | ▲ close $10,090.95 vs 09:30 $10,090.95 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,090.95 | ▲ 09:30 equity $10,090.95 vs yday $10,090.95 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **SHORT** | `KGC` | 21 | $29.63 | $2.09 | — | $10,711.09 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $630.68 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WPM` | 4 | $144.54 | $2.04 | — | $11,287.21 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $630.68 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $11,898.53 | — | RSI overbought; gate rsi_ob=True; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $630.68 | — |
| 2026-08-20 09:30 ET | **SHORT** | `SCZM` | 66 | $9.46 | $2.23 | — | $12,520.66 | — | RSI overbought; gate rsi_ob=True; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $630.68 | — |
| 2026-08-20 09:30 ET | **SHORT** | `MRNA` | 4 | $150.14 | $2.04 | — | $13,119.18 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $630.68 | — |
| 2026-08-20 09:30 ET | **SHORT** | `CYPH` | 548 | $1.15 | $7.19 | — | $13,742.19 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $630.68 | — |
| 2026-08-20 09:30 ET | **SHORT** | `BNTX` | 5 | $109.06 | $2.04 | — | $14,285.45 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $630.68 | — |
| 2026-08-20 09:30 ET | **SHORT** | `EL` | 6 | $97.43 | $2.04 | — | $14,867.99 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; leftover $630.68 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,867.99 | ▼ close $10,009.93 vs 09:30 $10,090.95 (session -59.32) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,867.99 | ▼ 09:30 equity $9,856.66 vs yday $10,009.93 (-153.27) | — | — |
| 2026-08-21 09:30 ET | **COVER** | `KGC` | 21 | $32.17 | $2.05 | $-57.48 | $14,190.37 | ▼ -57.48 after sell → book $9,854.61; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `WPM` | 4 | $154.70 | $2.00 | $-44.68 | $13,569.57 | ▼ -44.68 after sell → book $9,852.61; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `SCZM` | 66 | $10.26 | $2.19 | $-57.21 | $12,890.22 | ▼ -57.21 after sell → book $9,850.42; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `MRNA` | 4 | $133.11 | $2.00 | $+64.08 | $12,355.78 | ▲ +64.08 after sell → book $9,848.42; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `BNTX` | 5 | $110.92 | $2.00 | $-13.34 | $11,799.17 | ▼ -13.34 after sell → book $9,846.41; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `EL` | 6 | $96.75 | $2.01 | $+0.03 | $11,216.66 | ▲ +0.03 after sell → book $9,844.40; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SHORT** | `AU` | 6 | $119.43 | $2.05 | — | $11,931.19 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $820.37 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARCT` | 73 | $11.13 | $2.25 | — | $12,741.43 | — | RSI overbought; gate rsi_ob=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $820.37 | — |
| 2026-08-21 09:30 ET | **SHORT** | `CRDL` | 425 | $1.93 | $5.59 | — | $13,556.10 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $820.37 | — |
| 2026-08-21 09:30 ET | **SHORT** | `GMAB` | 24 | $33.36 | $2.10 | — | $14,354.63 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $820.37 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRVI` | 99 | $8.28 | $2.34 | — | $15,172.02 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $820.37 | — |
| 2026-08-21 09:30 ET | **SHORT** | `DFDV` | 203 | $4.04 | $2.68 | — | $15,989.45 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $820.37 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,989.45 | ▼ close $9,605.46 vs 09:30 $9,856.66 (session -221.93) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,989.45 | ▼ 09:30 equity $9,357.80 vs yday $9,605.46 (-247.66) | — | — |
| 2026-08-24 09:30 ET | **COVER** | `AEM` | 3 | $217.03 | $2.00 | $-41.77 | $15,336.36 | ▼ -41.77 after sell → book $9,355.80; vs 09:30 mark -2.00 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `CYPH` | 548 | $1.83 | $7.07 | $-386.90 | $14,326.45 | ▼ -386.90 after sell → book $9,348.73; vs 09:30 mark -7.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AU` | 6 | $120.51 | $2.01 | $-10.54 | $13,601.38 | ▼ -10.54 after sell → book $9,346.72; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `CRDL` | 425 | $1.88 | $5.48 | $+10.18 | $12,796.90 | ▲ +10.18 after sell → book $9,341.24; vs 09:30 mark -5.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `GMAB` | 24 | $32.82 | $2.06 | $+8.79 | $12,007.16 | ▲ +8.79 after sell → book $9,339.18; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `MRVI` | 99 | $8.59 | $2.29 | $-35.31 | $11,154.46 | ▼ -35.31 after sell → book $9,336.89; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `DFDV` | 203 | $4.16 | $2.62 | $-29.66 | $10,307.36 | ▼ -29.66 after sell → book $9,334.27; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,307.36 | ▼ close $9,260.54 vs 09:30 $9,357.80 (session -73.73) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,307.36 | ▲ 09:30 equity $9,276.60 vs yday $9,260.54 (+16.06) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `ARCT` | 73 | $14.12 | $2.21 | $-222.73 | $9,274.40 | ▼ -222.73 after sell → book $9,274.40; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **SHORT** | `LIFE` | 15 | $36.96 | $2.07 | — | $9,826.73 | — | RSI overbought; gate rsi_ob=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $579.65 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMEA` | 355 | $1.63 | $4.66 | — | $10,400.71 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $579.65 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ALVO` | 110 | $5.24 | $2.36 | — | $10,974.75 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $579.65 | — |
| 2026-08-25 09:30 ET | **SHORT** | `CYPH` | 371 | $1.56 | $4.87 | — | $11,548.63 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $579.65 | — |
| 2026-08-25 09:30 ET | **SHORT** | `FWDI` | 101 | $5.71 | $2.34 | — | $12,123.01 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $579.65 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ASST` | 30 | $19.04 | $2.12 | — | $12,692.09 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ret5=+49.5; leftover $579.65 | — |
| 2026-08-25 09:30 ET | **SHORT** | `JANX` | 30 | $18.72 | $2.12 | — | $13,251.58 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,ohlc_hot; 🔵; ret5=+14.4; leftover $579.65 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMNR` | 24 | $23.80 | $2.10 | — | $13,820.68 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; ret5=+28.9; leftover $579.65 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,820.68 | ▼ close $9,055.36 vs 09:30 $9,276.60 (session -196.40) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,820.68 | ▲ 09:30 equity $9,118.62 vs yday $9,055.36 (+63.26) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `LIFE` | 15 | $38.24 | $2.04 | $-23.31 | $13,245.04 | ▼ -23.31 after sell → book $9,116.59; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `BMEA` | 355 | $1.75 | $4.58 | $-53.62 | $12,617.44 | ▼ -53.62 after sell → book $9,112.01; vs 09:30 mark -4.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `ALVO` | 110 | $4.98 | $2.32 | $+23.92 | $12,067.32 | ▲ +23.92 after sell → book $9,109.69; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `CYPH` | 371 | $1.60 | $4.79 | $-24.50 | $11,468.93 | ▼ -24.50 after sell → book $9,104.90; vs 09:30 mark -4.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `FWDI` | 101 | $5.97 | $2.29 | $-30.89 | $10,863.67 | ▼ -30.89 after sell → book $9,102.61; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `ASST` | 30 | $20.72 | $2.08 | $-54.60 | $10,239.99 | ▼ -54.60 after sell → book $9,100.53; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `JANX` | 30 | $18.59 | $2.08 | $-0.30 | $9,680.21 | ▼ -0.30 after sell → book $9,098.45; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `BMNR` | 24 | $24.24 | $2.06 | $-14.72 | $9,096.39 | ▼ -14.72 after sell → book $9,096.39; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SHORT** | `SENS` | 59 | $9.48 | $2.20 | — | $9,653.51 | — | RSI overbought; gate rsi_ob=True; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $568.52 | — |
| 2026-08-26 09:30 ET | **SHORT** | `KURA` | 41 | $13.63 | $2.15 | — | $10,210.19 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+10.2; leftover $568.52 | — |
| 2026-08-26 09:30 ET | **SHORT** | `XHG` | 149 | $3.81 | $2.49 | — | $10,775.39 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ret5=-6.1; leftover $568.52 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BTG` | 98 | $5.75 | $2.33 | — | $11,336.57 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ret5=+17.9; leftover $568.52 | — |
| 2026-08-26 09:30 ET | **SHORT** | `MRK` | 3 | $154.35 | $2.03 | — | $11,797.58 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+15.7; leftover $568.52 | — |
| 2026-08-26 09:30 ET | **SHORT** | `B` | 11 | $48.18 | $2.06 | — | $12,325.51 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ret5=+16.1; leftover $568.52 | — |
| 2026-08-26 09:30 ET | **SHORT** | `PEPG` | 166 | $3.41 | $2.54 | — | $12,889.02 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ret5=+17.7; leftover $568.52 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRDL` | 280 | $2.03 | $3.68 | — | $13,453.74 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+5.5; leftover $568.52 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,453.74 | ▲ close $9,089.74 vs 09:30 $9,118.62 (session +12.83) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,453.74 | ▲ 09:30 equity $9,118.53 vs yday $9,089.74 (+28.79) | — | — |
| 2026-08-27 09:30 ET | **COVER** | `BTG` | 98 | $5.73 | $2.28 | $-2.65 | $12,889.92 | ▼ -2.65 after sell → book $9,116.25; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `MRK` | 3 | $149.53 | $2.00 | $+10.43 | $12,439.33 | ▲ +10.43 after sell → book $9,114.25; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `B` | 11 | $47.07 | $2.02 | $+8.13 | $11,919.53 | ▲ +8.13 after sell → book $9,112.22; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `PEPG` | 166 | $3.22 | $2.49 | $+26.51 | $11,382.53 | ▲ +26.51 after sell → book $9,109.74; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `CRDL` | 280 | $2.09 | $3.61 | $-24.10 | $10,793.71 | ▼ -24.10 after sell → book $9,106.12; vs 09:30 mark -3.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SHORT** | `OABI` | 189 | $4.81 | $2.62 | — | $11,700.18 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+14.8; leftover $910.61 | — |
| 2026-08-27 09:30 ET | **SHORT** | `HTFL` | 18 | $48.92 | $2.09 | — | $12,578.65 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+7.5; leftover $910.61 | — |
| 2026-08-27 09:30 ET | **SHORT** | `SBET` | 107 | $8.45 | $2.36 | — | $13,480.44 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+17.5; leftover $910.61 | — |
| 2026-08-27 09:30 ET | **SHORT** | `EL` | 8 | $104.49 | $2.06 | — | $14,314.30 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+7.2; leftover $910.61 | — |
| 2026-08-27 09:30 ET | **SHORT** | `DASH` | 3 | $235.94 | $2.04 | — | $15,020.08 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+7.6; leftover $910.61 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,020.08 | ▲ close $9,126.30 vs 09:30 $9,118.53 (session +31.35) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,020.08 | ▲ 09:30 equity $9,168.57 vs yday $9,126.30 (+42.27) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `SENS` | 59 | $9.39 | $2.17 | $+0.94 | $14,463.91 | ▲ +0.94 after sell → book $9,166.41; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `KURA` | 41 | $13.05 | $2.11 | $+19.52 | $13,926.74 | ▲ +19.52 after sell → book $9,164.29; vs 09:30 mark -2.12 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `XHG` | 149 | $3.69 | $2.44 | $+12.96 | $13,374.50 | ▲ +12.96 after sell → book $9,161.86; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `OABI` | 189 | $4.54 | $2.56 | $+45.85 | $12,513.88 | ▲ +45.85 after sell → book $9,159.30; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `HTFL` | 18 | $48.50 | $2.04 | $+3.43 | $11,638.84 | ▲ +3.43 after sell → book $9,157.26; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `DASH` | 3 | $233.37 | $2.00 | $+3.67 | $10,936.73 | ▲ +3.67 after sell → book $9,155.26; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SHORT** | `ANF` | 5 | $146.07 | $2.05 | — | $11,665.03 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $762.94 | — |
| 2026-08-28 09:30 ET | **SHORT** | `BZ` | 42 | $18.15 | $2.16 | — | $12,425.18 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+14.1; leftover $762.94 | — |
| 2026-08-28 09:30 ET | **SHORT** | `CRDL` | 370 | $2.06 | $4.87 | — | $13,182.51 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; ret5=+9.3; leftover $762.94 | — |
| 2026-08-28 09:30 ET | **SHORT** | `CXM` | 96 | $7.88 | $2.32 | — | $13,936.67 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ret5=+10.3; leftover $762.94 | — |
| 2026-08-28 09:30 ET | **SHORT** | `PATH` | 42 | $18.12 | $2.16 | — | $14,695.76 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ret5=+15.1; leftover $762.94 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FSM` | 59 | $12.84 | $2.21 | — | $15,451.11 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+7.6; leftover $762.94 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,451.11 | ▲ close $9,270.09 vs 09:30 $9,168.57 (session +130.59) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,451.11 | ▲ 09:30 equity $9,286.44 vs yday $9,270.09 (+16.35) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `SBET` | 107 | $8.24 | $2.31 | $+17.80 | $14,567.12 | ▲ +17.80 after sell → book $9,284.13; vs 09:30 mark -2.31 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `EL` | 8 | $102.70 | $2.01 | $+10.25 | $13,743.51 | ▲ +10.25 after sell → book $9,282.12; vs 09:30 mark -2.01 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `ANF` | 5 | $148.03 | $2.00 | $-13.85 | $13,001.35 | ▼ -13.85 after sell → book $9,280.11; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `BZ` | 42 | $17.70 | $2.12 | $+14.63 | $12,255.84 | ▲ +14.63 after sell → book $9,278.00; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRDL` | 370 | $1.92 | $4.77 | $+42.16 | $11,540.66 | ▲ +42.16 after sell → book $9,273.22; vs 09:30 mark -4.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `CXM` | 96 | $8.17 | $2.28 | $-32.44 | $10,754.06 | ▼ -32.44 after sell → book $9,270.94; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `PATH` | 42 | $18.09 | $2.12 | $-2.80 | $9,992.17 | ▼ -2.80 after sell → book $9,268.83; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `FSM` | 59 | $12.26 | $2.17 | $+29.85 | $9,266.66 | ▲ +29.85 after sell → book $9,266.66; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,266.66 | ▲ close $9,266.66 vs 09:30 $9,286.44 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,266.66 | ▲ 09:30 equity $9,266.66 vs yday $9,266.66 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,266.66 | ▲ close $9,266.66 vs 09:30 $9,266.66 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,266.66 | ▲ 09:30 equity $9,266.66 vs yday $9,266.66 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,266.66 | ▲ close $9,266.66 vs 09:30 $9,266.66 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,266.66 | ▲ 09:30 equity $9,266.66 vs yday $9,266.66 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **SHORT** | `ATRC` | 10 | $52.88 | $2.05 | — | $9,793.41 | — | RSI overbought; gate rsi_ob=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $579.17 | — |
| 2026-09-03 09:30 ET | **SHORT** | `CRK` | 37 | $15.45 | $2.14 | — | $10,362.92 | — | RSI overbought; gate rsi_ob=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $579.17 | — |
| 2026-09-03 09:30 ET | **SHORT** | `ARCT` | 34 | $16.77 | $2.13 | — | $10,930.97 | — | RSI overbought; gate rsi_ob=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $579.17 | — |
| 2026-09-03 09:30 ET | **SHORT** | `CRDL` | 265 | $2.18 | $3.49 | — | $11,505.18 | — | RSI overbought; gate rsi_ob=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $579.17 | — |
| 2026-09-03 09:30 ET | **SHORT** | `GPRO` | 325 | $1.78 | $4.27 | — | $12,079.41 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+183.1; leftover $579.17 | — |
| 2026-09-03 09:30 ET | **SHORT** | `MMED` | 24 | $23.88 | $2.10 | — | $12,650.43 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $579.17 | — |
| 2026-09-03 09:30 ET | **SHORT** | `NVAX` | 55 | $10.42 | $2.19 | — | $13,221.34 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $579.17 | — |
| 2026-09-03 09:30 ET | **SHORT** | `CNXC` | 17 | $32.88 | $2.08 | — | $13,778.23 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ret5=+16.2; leftover $579.17 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,778.23 | ▲ close $9,447.98 vs 09:30 $9,266.66 (session +201.76) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,778.23 | ▼ 09:30 equity $9,416.97 vs yday $9,447.98 (-31.01) | — | — |
| 2026-09-04 09:30 ET | **COVER** | `CRK` | 37 | $15.00 | $2.10 | $+12.41 | $13,221.13 | ▲ +12.41 after sell → book $9,414.87; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `ARCT` | 34 | $15.61 | $2.09 | $+35.22 | $12,688.29 | ▲ +35.22 after sell → book $9,412.77; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `CRDL` | 265 | $2.16 | $3.42 | $-1.61 | $12,112.48 | ▼ -1.61 after sell → book $9,409.36; vs 09:30 mark -3.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `MMED` | 24 | $23.84 | $2.06 | $-3.20 | $11,538.25 | ▼ -3.20 after sell → book $9,407.29; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `NVAX` | 55 | $10.50 | $2.15 | $-8.75 | $10,958.60 | ▼ -8.75 after sell → book $9,405.14; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `CNXC` | 17 | $32.48 | $2.04 | $+2.68 | $10,404.40 | ▲ +2.68 after sell → book $9,403.10; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SHORT** | `BMEA` | 412 | $1.90 | $5.41 | — | $11,181.78 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $783.59 | — |
| 2026-09-04 09:30 ET | **SHORT** | `OABI` | 163 | $4.78 | $2.54 | — | $11,958.39 | — | RSI overbought; gate rsi_ob=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $783.59 | — |
| 2026-09-04 09:30 ET | **SHORT** | `OPK` | 492 | $1.59 | $6.46 | — | $12,734.21 | — | RSI overbought; gate rsi_ob=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $783.59 | — |
| 2026-09-04 09:30 ET | **SHORT** | `CRM` | 2 | $263.36 | $2.03 | — | $13,258.90 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $783.59 | — |
| 2026-09-04 09:30 ET | **SHORT** | `HRMY` | 18 | $41.50 | $2.08 | — | $14,003.81 | — | RSI overbought; gate rsi_ob=True; list flatten,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $783.59 | — |
| 2026-09-04 09:30 ET | **SHORT** | `FMC` | 60 | $12.95 | $2.21 | — | $14,778.60 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+21.8; leftover $783.59 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,778.60 | ▼ close $9,304.71 vs 09:30 $9,416.97 (session -77.65) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,778.60 | ▲ 09:30 equity $9,346.37 vs yday $9,304.71 (+41.66) | — | — |
| 2026-09-08 09:30 ET | **COVER** | `ATRC` | 10 | $54.31 | $2.02 | $-18.37 | $14,233.48 | ▼ -18.37 after sell → book $9,344.35; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `GPRO` | 325 | $1.56 | $4.19 | $+61.41 | $13,720.66 | ▲ +61.41 after sell → book $9,340.16; vs 09:30 mark -4.19 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `BMEA` | 412 | $2.00 | $5.31 | $-51.93 | $12,891.35 | ▼ -51.93 after sell → book $9,334.85; vs 09:30 mark -5.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `OABI` | 163 | $4.30 | $2.48 | $+73.22 | $12,187.97 | ▲ +73.22 after sell → book $9,332.37; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `OPK` | 492 | $1.63 | $6.35 | $-32.49 | $11,379.66 | ▼ -32.49 after sell → book $9,326.02; vs 09:30 mark -6.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `CRM` | 2 | $253.72 | $2.00 | $+15.25 | $10,870.23 | ▲ +15.25 after sell → book $9,324.03; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `HRMY` | 18 | $42.20 | $2.04 | $-16.73 | $10,108.58 | ▼ -16.73 after sell → book $9,321.98; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `FMC` | 60 | $13.11 | $2.17 | $-13.98 | $9,319.81 | ▼ -13.98 after sell → book $9,319.81; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,319.81 | ▲ close $9,319.81 vs 09:30 $9,346.37 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,319.81 | ▲ 09:30 equity $9,319.81 vs yday $9,319.81 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,319.81 | ▲ close $9,319.81 vs 09:30 $9,319.81 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,319.81 | ▲ 09:30 equity $9,319.81 vs yday $9,319.81 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,319.81 | ▲ close $9,319.81 vs 09:30 $9,319.81 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,319.81 | ▲ 09:30 equity $9,319.81 vs yday $9,319.81 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **SHORT** | `INDP` | 215 | $2.70 | $2.84 | — | $9,897.48 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $582.49 | — |
| 2026-09-11 09:30 ET | **SHORT** | `WLTH` | 53 | $10.95 | $2.18 | — | $10,475.64 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $582.49 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BNC` | 118 | $4.91 | $2.39 | — | $11,052.63 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $582.49 | — |
| 2026-09-11 09:30 ET | **SHORT** | `SWKS` | 6 | $84.27 | $2.04 | — | $11,556.21 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $582.49 | — |
| 2026-09-11 09:30 ET | **SHORT** | `ANGX` | 108 | $5.38 | $2.36 | — | $12,134.89 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ret5=+19.8; leftover $582.49 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 5 | $112.83 | $2.04 | — | $12,697.03 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $582.49 | — |
| 2026-09-11 09:30 ET | **SHORT** | `ASO` | 10 | $54.91 | $2.06 | — | $13,244.07 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ret5=+24.3; leftover $582.49 | — |
| 2026-09-11 09:30 ET | **SHORT** | `IRD` | 94 | $6.16 | $2.31 | — | $13,820.80 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ret5=+36.4; leftover $582.49 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,820.80 | ▼ close $9,285.40 vs 09:30 $9,319.81 (session -16.20) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,820.80 | ▼ 09:30 equity $9,278.04 vs yday $9,285.40 (-7.36) | — | — |
| 2026-09-14 09:30 ET | **COVER** | `WLTH` | 53 | $10.29 | $2.15 | $+30.65 | $13,273.28 | ▲ +30.65 after sell → book $9,275.89; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `BNC` | 118 | $5.03 | $2.34 | $-18.89 | $12,677.40 | ▼ -18.89 after sell → book $9,273.55; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `SWKS` | 6 | $86.06 | $2.01 | $-14.79 | $12,159.03 | ▼ -14.79 after sell → book $9,271.54; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `ANGX` | 108 | $5.57 | $2.31 | $-25.19 | $11,555.15 | ▼ -25.19 after sell → book $9,269.22; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `ASO` | 10 | $54.75 | $2.02 | $-2.48 | $11,005.63 | ▼ -2.48 after sell → book $9,267.20; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `IRD` | 94 | $6.02 | $2.27 | $+8.57 | $10,437.48 | ▲ +8.57 after sell → book $9,264.93; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,437.48 | ▼ close $9,222.48 vs 09:30 $9,278.04 (session -42.45) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,437.48 | ▼ 09:30 equity $9,164.48 vs yday $9,222.48 (-58.00) | — | — |
| 2026-09-15 09:30 ET | **COVER** | `QRVO` | 5 | $108.40 | $2.00 | $+18.13 | $9,893.48 | ▲ +18.13 after sell → book $9,162.48; vs 09:30 mark -2.00 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,893.48 | ▼ close $9,110.88 vs 09:30 $9,164.48 (session -51.60) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,893.48 | ▼ 09:30 equity $9,106.58 vs yday $9,110.88 (-4.30) | — | — |
| 2026-09-16 09:30 ET | **SHORT** | `AVAH` | 45 | $14.31 | $2.16 | — | $10,535.26 | — | RSI overbought; gate rsi_ob=True; list flatten; ret5=+4.8; leftover $650.47 | — |
| 2026-09-16 09:30 ET | **SHORT** | `HLP` | 361 | $1.80 | $4.74 | — | $11,180.32 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $650.47 | — |
| 2026-09-16 09:30 ET | **SHORT** | `SWKS` | 7 | $89.38 | $2.05 | — | $11,803.93 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $650.47 | — |
| 2026-09-16 09:30 ET | **SHORT** | `SDGR` | 27 | $23.29 | $2.11 | — | $12,430.65 | — | RSI overbought; gate rsi_ob=True; list yday_gainer; 🔵; ret5=+16.1; leftover $650.47 | — |
| 2026-09-16 09:30 ET | **SHORT** | `CAI` | 23 | $28.16 | $2.10 | — | $13,076.24 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,ohlc_hot; ret5=+14.8; leftover $650.47 | — |
| 2026-09-16 09:30 ET | **SHORT** | `QRVO` | 5 | $118.18 | $2.04 | — | $13,665.09 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $650.47 | — |
| 2026-09-16 09:30 ET | **SHORT** | `RVTY` | 4 | $140.88 | $2.04 | — | $14,226.58 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,ohlc_hot; 🔵; ret5=+10.3; leftover $650.47 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,226.58 | ▲ close $9,100.62 vs 09:30 $9,106.58 (session +11.28) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,226.58 | ▼ 09:30 equity $9,033.75 vs yday $9,100.62 (-66.87) | — | — |
| 2026-09-17 09:30 ET | **COVER** | `AVAH` | 45 | $14.33 | $2.12 | $-5.19 | $13,579.60 | ▼ -5.19 after sell → book $9,031.63; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **COVER** | `SWKS` | 7 | $86.76 | $2.01 | $+14.28 | $12,970.27 | ▲ +14.28 after sell → book $9,029.62; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **COVER** | `SDGR` | 27 | $24.09 | $2.07 | $-25.78 | $12,317.77 | ▼ -25.78 after sell → book $9,027.55; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **COVER** | `CAI` | 23 | $28.59 | $2.06 | $-14.16 | $11,658.03 | ▼ -14.16 after sell → book $9,025.49; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **COVER** | `QRVO` | 5 | $114.90 | $2.00 | $+12.35 | $11,081.52 | ▲ +12.35 after sell → book $9,023.48; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SHORT** | `IOVA` | 88 | $10.25 | $2.30 | — | $11,981.22 | — | RSI overbought; gate rsi_ob=True; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $902.35 | — |
| 2026-09-17 09:30 ET | **SHORT** | `ADPT` | 31 | $28.23 | $2.13 | — | $12,854.22 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+13.3; leftover $902.35 | — |
| 2026-09-17 09:30 ET | **SHORT** | `FRO` | 16 | $54.31 | $2.08 | — | $13,721.10 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+13.7; leftover $902.35 | — |
| 2026-09-17 09:30 ET | **SHORT** | `CVI` | 17 | $51.88 | $2.09 | — | $14,600.97 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+11.1; leftover $902.35 | — |
| 2026-09-17 09:30 ET | **SHORT** | `DHT` | 39 | $22.97 | $2.15 | — | $15,494.65 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+8.7; leftover $902.35 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,494.65 | ▼ close $8,891.91 vs 09:30 $9,033.75 (session -120.82) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,494.65 | ▲ 09:30 equity $8,955.75 vs yday $8,891.91 (+63.84) | — | — |
| 2026-09-18 09:30 ET | **COVER** | `HLP` | 361 | $1.96 | $4.66 | $-67.16 | $14,782.44 | ▼ -67.16 after sell → book $8,951.10; vs 09:30 mark -4.65 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `RVTY` | 4 | $146.50 | $2.00 | $-26.52 | $14,194.43 | ▼ -26.52 after sell → book $8,949.09; vs 09:30 mark -2.01 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `IOVA` | 88 | $10.12 | $2.25 | $+6.88 | $13,301.62 | ▲ +6.88 after sell → book $8,946.84; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `ADPT` | 31 | $28.55 | $2.08 | $-14.13 | $12,414.49 | ▼ -14.13 after sell → book $8,944.76; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `FRO` | 16 | $51.19 | $2.04 | $+45.80 | $11,593.41 | ▲ +45.80 after sell → book $8,942.72; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `DHT` | 39 | $23.16 | $2.11 | $-11.67 | $10,688.06 | ▼ -11.67 after sell → book $8,940.61; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SHORT** | `ECO` | 8 | $85.00 | $2.05 | — | $11,366.01 | — | RSI overbought; gate rsi_ob=True; list flatten; 🔵; ⚪; ret5=+18.3; leftover $745.05 | — |
| 2026-09-18 09:30 ET | **SHORT** | `SDGR` | 25 | $29.32 | $2.11 | — | $12,096.90 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $745.05 | — |
| 2026-09-18 09:30 ET | **SHORT** | `CYPH` | 245 | $3.04 | $3.23 | — | $12,837.25 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $745.05 | — |
| 2026-09-18 09:30 ET | **SHORT** | `TEM` | 9 | $81.40 | $2.06 | — | $13,567.79 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; leftover $745.05 | — |
| 2026-09-18 09:30 ET | **SHORT** | `CHPT` | 74 | $10.00 | $2.25 | — | $14,305.54 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ⚪; ret5=+11.1; leftover $745.05 | — |
| 2026-09-18 09:30 ET | **SHORT** | `GME` | 32 | $22.90 | $2.13 | — | $15,036.21 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $745.05 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,036.21 | ▼ close $8,884.41 vs 09:30 $8,955.75 (session -42.37) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,036.21 | ▼ 09:30 equity $8,785.98 vs yday $8,884.41 (-98.43) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `INDP` | 215 | $3.55 | $2.77 | $-188.36 | $14,270.19 | ▼ -188.36 after sell → book $8,783.21; vs 09:30 mark -2.77 | dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `CVI` | 17 | $53.19 | $2.04 | $-26.40 | $13,363.91 | ▼ -26.40 after sell → book $8,781.16; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `ECO` | 8 | $82.83 | $2.01 | $+13.29 | $12,699.26 | ▲ +13.29 after sell → book $8,779.15; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `SDGR` | 25 | $29.43 | $2.06 | $-6.92 | $11,961.45 | ▼ -6.92 after sell → book $8,777.09; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `TEM` | 9 | $79.08 | $2.02 | $+16.81 | $11,247.71 | ▲ +16.81 after sell → book $8,775.07; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `CHPT` | 74 | $10.32 | $2.21 | $-28.15 | $10,481.82 | ▼ -28.15 after sell → book $8,772.86; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `GME` | 32 | $22.78 | $2.09 | $-0.37 | $9,750.77 | ▼ -0.37 after sell → book $8,770.77; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SHORT** | `IOVA` | 60 | $10.43 | $2.21 | — | $10,374.36 | — | RSI overbought; gate rsi_ob=True; list flatten; ret5=+19.2; leftover $626.48 | — |
| 2026-09-21 09:30 ET | **SHORT** | `TJGC` | 37 | $16.91 | $2.14 | — | $10,997.90 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+50.5; leftover $626.48 | — |
| 2026-09-21 09:30 ET | **SHORT** | `SECZ` | 53 | $11.67 | $2.19 | — | $11,614.22 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+31.3; leftover $626.48 | — |
| 2026-09-21 09:30 ET | **SHORT** | `FEAM` | 253 | $2.47 | $3.33 | — | $12,235.80 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+73.6; leftover $626.48 | — |
| 2026-09-21 09:30 ET | **SHORT** | `NEO` | 31 | $19.92 | $2.12 | — | $12,851.20 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+15.0; leftover $626.48 | — |
| 2026-09-21 09:30 ET | **SHORT** | `ALVO` | 105 | $5.92 | $2.35 | — | $13,470.45 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; ret5=+11.0; leftover $626.48 | — |
| 2026-09-21 09:30 ET | **SHORT** | `ASST` | 19 | $31.64 | $2.08 | — | $14,069.52 | — | RSI overbought; gate rsi_ob=True; list ohlc_hot; 🔵; ret5=+8.9; leftover $626.48 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,069.52 | ▲ close $8,836.34 vs 09:30 $8,785.98 (session +81.99) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,069.52 | ▲ 09:30 equity $8,858.18 vs yday $8,836.34 (+21.84) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `CYPH` | 245 | $3.51 | $3.16 | $-122.77 | $13,206.41 | ▼ -122.77 after sell → book $8,855.02; vs 09:30 mark -3.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-22 09:30 ET | **COVER** | `IOVA` | 60 | $10.18 | $2.17 | $+10.62 | $12,593.44 | ▲ +10.62 after sell → book $8,852.85; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **COVER** | `ASST` | 19 | $29.30 | $2.05 | $+40.33 | $12,034.70 | ▲ +40.33 after sell → book $8,850.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SHORT** | `GLND` | 215 | $2.94 | $2.84 | — | $12,663.96 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+136.1; leftover $632.20 | — |
| 2026-09-22 09:30 ET | **SHORT** | `NUAI` | 87 | $7.23 | $2.29 | — | $13,290.68 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+36.6; leftover $632.20 | — |
| 2026-09-22 09:30 ET | **SHORT** | `USDE` | 48 | $12.99 | $2.17 | — | $13,912.03 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+69.4; leftover $632.20 | — |
| 2026-09-22 09:30 ET | **SHORT** | `VGZ` | 238 | $2.65 | $3.14 | — | $14,539.59 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+26.3; leftover $632.20 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,539.59 | ▲ close $8,886.02 vs 09:30 $8,858.18 (session +45.66) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,539.59 | ▼ 09:30 equity $8,842.69 vs yday $8,886.02 (-43.33) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `TJGC` | 37 | $16.92 | $2.10 | $-4.61 | $13,911.45 | ▼ -4.61 after sell → book $8,840.59; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `SECZ` | 53 | $12.80 | $2.15 | $-64.22 | $13,230.90 | ▼ -64.22 after sell → book $8,838.44; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `NEO` | 31 | $18.69 | $2.08 | $+33.93 | $12,649.43 | ▲ +33.93 after sell → book $8,836.36; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `ALVO` | 105 | $5.86 | $2.31 | $+1.65 | $12,031.82 | ▲ +1.65 after sell → book $8,834.05; vs 09:30 mark -2.31 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `GLND` | 215 | $2.70 | $2.77 | $+45.99 | $11,448.55 | ▲ +45.99 after sell → book $8,831.28; vs 09:30 mark -2.77 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `NUAI` | 87 | $6.83 | $2.25 | $+30.26 | $10,852.09 | ▲ +30.26 after sell → book $8,829.03; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `USDE` | 48 | $13.22 | $2.13 | $-15.35 | $10,215.39 | ▼ -15.35 after sell → book $8,826.89; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `VGZ` | 238 | $2.73 | $3.07 | $-25.25 | $9,562.58 | ▼ -25.25 after sell → book $8,823.82; vs 09:30 mark -3.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SHORT** | `A` | 3 | $166.54 | $2.03 | — | $10,060.17 | — | RSI overbought; gate rsi_ob=True; list flatten; 🔵; ⚪; ret5=+10.3; leftover $630.27 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 5 | $116.85 | $2.04 | — | $10,642.38 | — | RSI overbought; gate rsi_ob=True; list flatten; 🔵; ⚪; ret5=+3.3; leftover $630.27 | — |
| 2026-09-23 09:30 ET | **SHORT** | `OMER` | 30 | $20.65 | $2.12 | — | $11,259.76 | — | RSI overbought; gate rsi_ob=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $630.27 | — |
| 2026-09-23 09:30 ET | **SHORT** | `VKTX` | 15 | $41.76 | $2.07 | — | $11,884.09 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $630.27 | — |
| 2026-09-23 09:30 ET | **SHORT** | `DNA` | 69 | $9.13 | $2.24 | — | $12,511.82 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+13.8; leftover $630.27 | — |
| 2026-09-23 09:30 ET | **SHORT** | `INOD` | 8 | $70.84 | $2.05 | — | $13,076.49 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $630.27 | — |
| 2026-09-23 09:30 ET | **SHORT** | `SVIA` | 140 | $4.49 | $2.46 | — | $13,702.63 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+26.4; leftover $630.27 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,702.63 | ▲ close $8,997.46 vs 09:30 $8,842.69 (session +188.65) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,702.63 | ▲ 09:30 equity $9,115.67 vs yday $8,997.46 (+118.21) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `FEAM` | 253 | $2.68 | $3.26 | $-59.73 | $13,021.33 | ▼ -59.73 after sell → book $9,112.41; vs 09:30 mark -3.26 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `A` | 3 | $163.95 | $2.00 | $+3.74 | $12,527.48 | ▲ +3.74 after sell → book $9,110.41; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `HALO` | 5 | $112.22 | $2.00 | $+19.10 | $11,964.38 | ▲ +19.10 after sell → book $9,108.40; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `OMER` | 30 | $20.52 | $2.08 | $-0.30 | $11,346.70 | ▼ -0.30 after sell → book $9,106.32; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `VKTX` | 15 | $36.02 | $2.04 | $+81.92 | $10,804.29 | ▲ +81.92 after sell → book $9,104.29; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `DNA` | 69 | $8.50 | $2.20 | $+39.04 | $10,215.59 | ▲ +39.04 after sell → book $9,102.09; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `INOD` | 8 | $70.50 | $2.01 | $-1.34 | $9,649.58 | ▼ -1.34 after sell → book $9,100.08; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,649.58 | ▼ close $9,095.18 vs 09:30 $9,115.67 (session -4.90) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,386.73 | ▲ 09:30 equity $9,860.05 vs yday $9,860.05 (-0.00) | 09:30 open · cash $10,386.73 (unchanged overnight, no fees) · equity $9,860.05 vs prior close $9,860.05 (-0.00) · 1 name(s) re-marked at the open (per-name table). SVIA×133 yday $3.96 → 09:30 $3.96 -0.00 | — |
| 2026-09-25 09:30 ET | **SHORT** | `TXG` | 7 | $83.76 | $2.05 | — | $10,971.00 | — | RSI overbought; gate rsi_ob=True; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $616.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `HLP` | 280 | $2.20 | $3.69 | — | $11,583.32 | — | RSI overbought; gate rsi_ob=True; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $616.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `TEM` | 7 | $83.69 | $2.05 | — | $12,167.14 | — | RSI overbought; gate rsi_ob=True; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $616.25 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `GLND` | 101 | $6.06 | $2.34 | — | $12,776.86 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+342.1; leftover $616.25 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `TJGC` | 20 | $29.76 | $2.09 | — | $13,369.97 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+156.1; leftover $616.25 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `DNA` | 60 | $10.20 | $2.21 | — | $13,979.77 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $616.25 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `TWST` | 3 | $184.00 | $2.03 | — | $14,529.73 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $616.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `SECZ` | 38 | $16.21 | $2.14 | — | $15,143.57 | — | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $616.25 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,143.57 | ▲ close $9,924.14 vs 09:30 $9,860.05 (session +82.67) | 16:00 close · cash $15,143.57 · equity $9,924.14 vs 09:30 $9,860.05 (+64.09; session marks +82.67) · 9 name(s) marked open→close (per-name table). SVIA×133 09:30 $3.96 → close $3.96 -0.00; TXG×7 09:30 $83.76 → close $85.71 -13.65; HLP×280 09:30 $2.20 → close $2.21 -2.80; TEM×7 09:30 $83.69 → close $85.01 -9.21; GLND×101 09:30 $6.06 → close $5.54 +52.52; TJGC×20 09:30 $29.76 → close $26.24 +70.40; DNA×60 09:30 $10.20 → close $10.66 -27.60; TWST×3 09:30 $184.00 → close $182.83 +3.51; SECZ×38 09:30 $16.21 → close $15.96 +9.50 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `WFF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DSX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `REAX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `USDE` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CAN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ASST` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `XHG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NIQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `STT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PURR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ARCT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PATH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `LAND` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DINO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `DFDV` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BRR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `DPRO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `PURR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CABA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LIFE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ATRC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XHLD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SID` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CVI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `XHLD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CMRC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TJGC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBLX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GME` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `NEO` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `AMRX` | no_price | no 09:30 open |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `GLND` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CTKB` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CAI` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `SVIA` | 140 | 2026-09-23 @ $4.49 | RSI overbought; gate rsi_ob=True; list yday_gainer,yday_mover; ret5=+26.4; leftover $630.27 |
