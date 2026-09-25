# Factor mine action — `union_macd_hist_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `macd_hist` · size `leftover` · sell `list` · S-boost `none` · rank by macd_hist

Cash book **+0.22%** ($10,022) · signal-only (no cash/fees) was +3.37%. Starts YES **20/30**. Fills 236 · skips 94 · realized $-657.32.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `macd_hist` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6,962.58.

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
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $1,249.21 | ▼ -55.19 after sell → book $10,252.85; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $2,574.08 | ▲ +107.86 after sell → book $10,250.76; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $3,786.67 | ▼ -26.05 after sell → book $10,248.59; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $5,090.98 | ▲ +69.58 after sell → book $10,246.42; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $6,313.14 | ▼ -29.03 after sell → book $10,244.28; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $7,728.88 | ▲ +148.79 after sell → book $10,225.03; vs 09:30 mark -19.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $9,040.95 | ▲ +69.56 after sell → book $10,222.70; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $10,220.61 | ▼ -64.90 after sell → book $10,220.61; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `ENTG` | 7 | $162.45 | $2.01 | — | $9,081.45 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+14.8; leftover $1277.58 | — |
| 2026-08-14 09:30 ET | **BUY** | `FORM` | 9 | $129.48 | $2.02 | — | $7,914.11 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+14.3; leftover $1277.58 | — |
| 2026-08-14 09:30 ET | **BUY** | `SPHR` | 7 | $176.68 | $2.01 | — | $6,675.34 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ⚪; ret5=+14.4; leftover $1277.58 | — |
| 2026-08-14 09:30 ET | **BUY** | `AMAT` | 2 | $499.40 | $2.00 | — | $5,674.55 | — | rank by macd_hist; rank macd_hist; list earn_react; 🔵; ret5=+1.3; leftover $1277.58 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZS` | 6 | $190.00 | $2.01 | — | $4,532.54 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+15.7; leftover $1277.58 | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $3,451.05 | — | rank by macd_hist; rank macd_hist; list flatten; 🔵; ret5=+5.9; leftover $1277.58 | — |
| 2026-08-14 09:30 ET | **BUY** | `VOYG` | 28 | $44.49 | $2.07 | — | $2,203.26 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+15.6; leftover $1277.58 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,203.26 | ▼ close $10,103.60 vs 09:30 $10,254.94 (session -102.90) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,203.26 | ▲ 09:30 equity $10,168.83 vs yday $10,103.60 (+65.23) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `ENTG` | 7 | $162.04 | $2.03 | $-6.91 | $3,335.51 | ▼ -6.91 after sell → book $10,166.80; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `FORM` | 9 | $134.05 | $2.04 | $+37.08 | $4,539.92 | ▲ +37.08 after sell → book $10,164.76; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SPHR` | 7 | $168.10 | $2.03 | $-64.10 | $5,714.59 | ▼ -64.10 after sell → book $10,162.73; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AMAT` | 2 | $517.45 | $2.02 | $+32.08 | $6,747.46 | ▲ +32.08 after sell → book $10,160.71; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZS` | 6 | $188.38 | $2.03 | $-13.79 | $7,875.69 | ▼ -13.79 after sell → book $10,158.69; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $8,977.31 | ▲ +20.13 after sell → book $10,156.67; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VOYG` | 28 | $42.12 | $2.09 | $-70.53 | $10,154.57 | ▼ -70.53 after sell → book $10,154.57; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `AAOI` | 8 | $152.64 | $2.01 | — | $8,931.44 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+10.8; leftover $1269.32 | — |
| 2026-08-17 09:30 ET | **BUY** | `MXL` | 14 | $86.67 | $2.03 | — | $7,716.03 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ⚪; ret5=+13.2; leftover $1269.32 | — |
| 2026-08-17 09:30 ET | **BUY** | `MP` | 21 | $58.01 | $2.05 | — | $6,495.76 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1269.32 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 30 | $41.23 | $2.08 | — | $5,256.78 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover; ret5=+46.0; leftover $1269.32 | — |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 38 | $32.55 | $2.10 | — | $4,017.78 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1269.32 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 25 | $49.00 | $2.06 | — | $2,790.72 | — | rank by macd_hist; rank macd_hist; list yday_gainer; ⚪; ret5=+12.2; leftover $1269.32 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 13 | $92.99 | $2.03 | — | $1,579.82 | — | rank by macd_hist; rank macd_hist; list probable,yday_gainer; ret5=-0.8; leftover $1269.32 | — |
| 2026-08-17 09:30 ET | **BUY** | `RDDT` | 7 | $177.51 | $2.01 | — | $335.24 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover; ⚪; ret5=+10.1; leftover $1269.32 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $335.24 | ▼ close $9,950.08 vs 09:30 $10,168.83 (session -188.11) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $335.24 | ▼ 09:30 equity $9,617.76 vs yday $9,950.08 (-332.32) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `AAOI` | 8 | $146.20 | $2.03 | $-55.57 | $1,502.80 | ▼ -55.57 after sell → book $9,615.72; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `MXL` | 14 | $79.09 | $2.05 | $-110.20 | $2,608.01 | ▼ -110.20 after sell → book $9,613.67; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `MP` | 21 | $56.35 | $2.07 | $-38.99 | $3,789.29 | ▼ -38.99 after sell → book $9,611.60; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 30 | $41.50 | $2.10 | $+3.92 | $5,032.19 | ▲ +3.92 after sell → book $9,609.50; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 38 | $28.59 | $2.12 | $-154.71 | $6,116.48 | ▼ -154.71 after sell → book $9,607.37; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 25 | $45.09 | $2.08 | $-101.90 | $7,241.65 | ▼ -101.90 after sell → book $9,605.29; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 13 | $92.38 | $2.05 | $-12.01 | $8,440.54 | ▼ -12.01 after sell → book $9,603.24; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `RDDT` | 7 | $166.10 | $2.03 | $-83.91 | $9,601.21 | ▼ -83.91 after sell → book $9,601.21; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,601.21 | ▲ close $9,601.21 vs 09:30 $9,617.76 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,601.21 | ▲ 09:30 equity $9,601.21 vs yday $9,601.21 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,601.21 | ▲ close $9,601.21 vs 09:30 $9,601.21 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,601.21 | ▲ 09:30 equity $9,601.21 vs yday $9,601.21 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $8,548.22 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1200.15 | — |
| 2026-08-20 09:30 ET | **BUY** | `TEAM` | 6 | $173.90 | $2.01 | — | $7,502.81 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+12.2; leftover $1200.15 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEM` | 5 | $204.45 | $2.00 | — | $6,478.55 | — | rank by macd_hist; rank macd_hist; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $1200.15 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $5,320.22 | — | rank by macd_hist; rank macd_hist; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1200.15 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUGO` | 14 | $83.58 | $2.03 | — | $4,148.07 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ⚪; ret5=+9.6; leftover $1200.15 | — |
| 2026-08-20 09:30 ET | **BUY** | `TWST` | 8 | $136.84 | $2.01 | — | $3,051.33 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+13.7; leftover $1200.15 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRCL` | 14 | $82.99 | $2.03 | — | $1,887.44 | — | rank by macd_hist; rank macd_hist; list probable; 🔵; ⚪; ret5=+7.4; leftover $1200.15 | — |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 11 | $109.06 | $2.02 | — | $685.76 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $1200.15 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $685.76 | ▲ close $9,625.99 vs 09:30 $9,601.21 (session +40.92) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $685.76 | ▲ 09:30 equity $9,788.63 vs yday $9,625.99 (+162.64) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `TEAM` | 6 | $174.22 | $2.03 | $-2.12 | $1,729.05 | ▼ -2.12 after sell → book $9,786.60; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TWST` | 8 | $138.43 | $2.03 | $+8.67 | $2,834.46 | ▲ +8.67 after sell → book $9,784.57; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CRCL` | 14 | $87.98 | $2.05 | $+65.78 | $4,064.12 | ▲ +65.78 after sell → book $9,782.51; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BNTX` | 11 | $110.92 | $2.04 | $+16.39 | $5,282.20 | ▲ +16.39 after sell → book $9,780.47; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 11 | $119.43 | $2.02 | — | $3,966.45 | — | rank by macd_hist; rank macd_hist; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1320.55 | — |
| 2026-08-21 09:30 ET | **BUY** | `MSTR` | 11 | $119.69 | $2.02 | — | $2,647.84 | — | rank by macd_hist; rank macd_hist; list yday_gainer,ohlc_hot; ret5=+15.7; leftover $1320.55 | — |
| 2026-08-21 09:30 ET | **BUY** | `TEM` | 20 | $65.60 | $2.05 | — | $1,333.79 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+22.8; leftover $1320.55 | — |
| 2026-08-21 09:30 ET | **BUY** | `SHAZ` | 21 | $61.46 | $2.05 | — | $41.07 | — | rank by macd_hist; rank macd_hist; list yday_mover; ret5=-15.5; leftover $1320.55 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.07 | ▲ close $9,960.60 vs 09:30 $9,788.63 (session +188.28) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.07 | ▼ 09:30 equity $9,947.45 vs yday $9,960.60 (-13.15) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $1,037.94 | ▼ -56.12 after sell → book $9,945.42; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $+58.87 | $2,121.07 | ▲ +58.87 after sell → book $9,943.40; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WPM` | 8 | $159.50 | $2.03 | $+115.63 | $3,395.03 | ▲ +115.63 after sell → book $9,941.36; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUGO` | 14 | $88.60 | $2.05 | $+66.20 | $4,633.38 | ▲ +66.20 after sell → book $9,939.31; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 11 | $120.51 | $2.04 | $+7.81 | $5,956.95 | ▲ +7.81 after sell → book $9,937.27; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MSTR` | 11 | $121.84 | $2.04 | $+19.58 | $7,295.14 | ▲ +19.58 after sell → book $9,935.22; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TEM` | 20 | $70.08 | $2.07 | $+85.38 | $8,694.57 | ▲ +85.38 after sell → book $9,933.15; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `SHAZ` | 21 | $58.98 | $2.07 | $-56.21 | $9,931.08 | ▼ -56.21 after sell → book $9,931.08; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,931.08 | ▲ close $9,931.08 vs 09:30 $9,947.45 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,931.08 | ▲ 09:30 equity $9,931.08 vs yday $9,931.08 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 7 | $156.51 | $2.01 | — | $8,833.50 | — | rank by macd_hist; rank macd_hist; list ohlc_hot,mover_buy; ⚪; ret5=+17.4; leftover $1241.38 | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 10 | $118.52 | $2.02 | — | $7,646.28 | — | rank by macd_hist; rank macd_hist; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1241.38 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 51 | $24.11 | $2.14 | — | $6,414.52 | — | rank by macd_hist; rank macd_hist; list yday_mover; ret5=+891.7; leftover $1241.38 | — |
| 2026-08-25 09:30 ET | **BUY** | `INTU` | 3 | $364.35 | $2.00 | — | $5,319.48 | — | rank by macd_hist; rank macd_hist; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $1241.38 | — |
| 2026-08-25 09:30 ET | **BUY** | `ILMN` | 5 | $224.00 | $2.00 | — | $4,197.47 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+15.6; leftover $1241.38 | — |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 19 | $64.55 | $2.05 | — | $2,968.97 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+4.4; leftover $1241.38 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $426.97 | $2.00 | — | $2,113.04 | — | rank by macd_hist; rank macd_hist; list flatten; ret5=+6.0; leftover $1241.38 | — |
| 2026-08-25 09:30 ET | **BUY** | `WIX` | 14 | $83.15 | $2.03 | — | $946.91 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+14.5; leftover $1241.38 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $946.91 | ▲ close $10,234.35 vs 09:30 $9,931.08 (session +319.52) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $946.91 | ▼ 09:30 equity $9,942.52 vs yday $10,234.35 (-291.83) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 7 | $160.93 | $2.03 | $+26.90 | $2,071.38 | ▲ +26.90 after sell → book $9,940.48; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 10 | $119.80 | $2.04 | $+8.74 | $3,267.34 | ▲ +8.74 after sell → book $9,938.44; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 51 | $26.61 | $2.16 | $+123.19 | $4,622.29 | ▲ +123.19 after sell → book $9,936.28; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ILMN` | 5 | $220.78 | $2.02 | $-20.13 | $5,724.17 | ▼ -20.13 after sell → book $9,934.26; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 19 | $63.60 | $2.07 | $-22.16 | $6,930.50 | ▼ -22.16 after sell → book $9,932.19; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WIX` | 14 | $84.02 | $2.05 | $+8.10 | $8,104.73 | ▲ +8.10 after sell → book $9,930.14; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `RGLD` | 5 | $264.00 | $2.00 | — | $6,782.72 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+16.2; leftover $1350.79 | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 5 | $267.02 | $2.00 | — | $5,445.62 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+16.7; leftover $1350.79 | — |
| 2026-08-26 09:30 ET | **BUY** | `MRK` | 8 | $154.35 | $2.01 | — | $4,208.80 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+15.7; leftover $1350.79 | — |
| 2026-08-26 09:30 ET | **BUY** | `NEM` | 10 | $132.64 | $2.02 | — | $2,880.38 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+16.5; leftover $1350.79 | — |
| 2026-08-26 09:30 ET | **BUY** | `HTFL` | 27 | $50.02 | $2.07 | — | $1,527.77 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+10.3; leftover $1350.79 | — |
| 2026-08-26 09:30 ET | **BUY** | `FUTU` | 10 | $124.67 | $2.02 | — | $279.05 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+15.7; leftover $1350.79 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $279.05 | ▲ close $9,974.14 vs 09:30 $9,942.52 (session +56.14) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $279.05 | ▼ 09:30 equity $9,956.77 vs yday $9,974.14 (-17.37) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `INTU` | 3 | $353.54 | $2.02 | $-36.45 | $1,337.65 | ▼ -36.45 after sell → book $9,954.75; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 2 | $424.61 | $2.02 | $-8.73 | $2,184.86 | ▼ -8.73 after sell → book $9,952.74; vs 09:30 mark -2.01 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RGLD` | 5 | $264.89 | $2.03 | $+0.42 | $3,507.28 | ▲ +0.42 after sell → book $9,950.71; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 5 | $267.23 | $2.03 | $-2.98 | $4,841.41 | ▼ -2.98 after sell → book $9,948.69; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MRK` | 8 | $149.53 | $2.03 | $-42.61 | $6,035.61 | ▼ -42.61 after sell → book $9,946.65; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NEM` | 10 | $131.02 | $2.04 | $-20.26 | $7,343.77 | ▼ -20.26 after sell → book $9,944.61; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FUTU` | 10 | $128.00 | $2.04 | $+29.24 | $8,621.73 | ▲ +29.24 after sell → book $9,942.57; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 8 | $144.18 | $2.01 | — | $7,466.28 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=-14.2; leftover $1231.68 | — |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $6,497.27 | — | rank by macd_hist; rank macd_hist; list mover_buy; 🔵; ret5=+0.1; leftover $1231.68 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 4 | $253.44 | $2.00 | — | $5,481.51 | — | rank by macd_hist; rank macd_hist; list overnight,overnight_mega,mover_buy; 🔵; ret5=+3.3; leftover $1231.68 | — |
| 2026-08-27 09:30 ET | **BUY** | `CBOE` | 3 | $312.34 | $2.00 | — | $4,542.49 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+11.2; leftover $1231.68 | — |
| 2026-08-27 09:30 ET | **BUY** | `DASH` | 5 | $235.94 | $2.00 | — | $3,360.79 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+7.6; leftover $1231.68 | — |
| 2026-08-27 09:30 ET | **BUY** | `BE` | 5 | $227.10 | $2.00 | — | $2,223.28 | — | rank by macd_hist; rank macd_hist; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.6; leftover $1231.68 | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 134 | $9.19 | $2.39 | — | $989.43 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; leftover $1231.68 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $989.43 | ▼ close $9,869.97 vs 09:30 $9,956.77 (session -58.19) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $989.43 | ▼ 09:30 equity $9,710.98 vs yday $9,869.97 (-158.99) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `HTFL` | 27 | $48.50 | $2.09 | $-45.20 | $2,296.84 | ▼ -45.20 after sell → book $9,708.88; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $3,214.12 | ▼ -51.73 after sell → book $9,706.87; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CBOE` | 3 | $315.00 | $2.02 | $+3.96 | $4,157.10 | ▲ +3.96 after sell → book $9,704.85; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DASH` | 5 | $233.37 | $2.02 | $-16.88 | $5,321.92 | ▼ -16.88 after sell → book $9,702.83; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BE` | 5 | $215.71 | $2.02 | $-61.01 | $6,398.42 | ▼ -61.01 after sell → book $9,700.80; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 2 | $461.85 | $2.00 | — | $5,472.73 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+16.8; leftover $1279.68 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $4,302.15 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1279.68 | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $3,038.95 | — | rank by macd_hist; rank macd_hist; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1279.68 | — |
| 2026-08-28 09:30 ET | **BUY** | `EL` | 11 | $106.99 | $2.02 | — | $1,860.03 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+10.5; leftover $1279.68 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $582.18 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1279.68 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $582.18 | ▼ close $9,457.09 vs 09:30 $9,710.98 (session -233.66) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $582.18 | ▼ 09:30 equity $9,408.97 vs yday $9,457.09 (-48.12) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MRVL` | 4 | $216.30 | $2.02 | $-152.58 | $1,445.36 | ▼ -152.58 after sell → book $9,406.95; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 134 | $9.50 | $2.42 | $+36.72 | $2,715.93 | ▲ +36.72 after sell → book $9,404.52; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.03 | $2.03 | $+11.63 | $3,898.14 | ▲ +11.63 after sell → book $9,402.49; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $5,131.36 | ▼ -29.98 after sell → book $9,400.46; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `EL` | 11 | $102.70 | $2.04 | $-51.26 | $6,259.02 | ▼ -51.26 after sell → book $9,398.42; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $7,447.68 | ▼ -89.19 after sell → book $9,396.38; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,447.68 | ▲ close $9,449.58 vs 09:30 $9,408.97 (session +53.20) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,447.68 | ▼ 09:30 equity $9,426.10 vs yday $9,449.58 (-23.48) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `SNPS` | 2 | $428.21 | $2.02 | $-71.29 | $8,302.09 | ▼ -71.29 after sell → book $9,424.09; vs 09:30 mark -2.01 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,302.09 | ▲ close $9,536.25 vs 09:30 $9,426.10 (session +112.16) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,302.09 | ▼ 09:30 equity $9,513.29 vs yday $9,536.25 (-22.96) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,302.09 | ▼ close $9,508.57 vs 09:30 $9,513.29 (session -4.72) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,302.09 | ▼ 09:30 equity $9,469.65 vs yday $9,508.57 (-38.92) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $7,596.84 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1186.01 | — |
| 2026-09-03 09:30 ET | **BUY** | `ALNY` | 4 | $265.94 | $2.00 | — | $6,531.08 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+11.9; leftover $1186.01 | — |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 9 | $127.91 | $2.02 | — | $5,377.87 | — | rank by macd_hist; rank macd_hist; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1186.01 | — |
| 2026-09-03 09:30 ET | **BUY** | `CF` | 8 | $138.60 | $2.01 | — | $4,267.06 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+10.8; leftover $1186.01 | — |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 7 | $161.54 | $2.01 | — | $3,134.27 | — | rank by macd_hist; rank macd_hist; list yday_gainer; 🔵; ret5=+12.0; leftover $1186.01 | — |
| 2026-09-03 09:30 ET | **BUY** | `CTVA` | 13 | $90.24 | $2.03 | — | $1,959.12 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+8.6; leftover $1186.01 | — |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 162 | $7.31 | $2.48 | — | $772.42 | — | rank by macd_hist; rank macd_hist; list yday_gainer; 🔵; ret5=+18.5; leftover $1186.01 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $772.42 | ▼ close $9,307.95 vs 09:30 $9,469.65 (session -147.15) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $772.42 | ▼ 09:30 equity $9,261.85 vs yday $9,307.95 (-46.10) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $153.62 | $2.03 | $+71.47 | $1,999.35 | ▲ +71.47 after sell → book $9,259.82; vs 09:30 mark -2.03 | dropped from list after 6 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $2,689.37 | ▼ -15.23 after sell → book $9,257.81; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ALNY` | 4 | $258.58 | $2.02 | $-33.46 | $3,721.67 | ▼ -33.46 after sell → book $9,255.79; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AGCO` | 9 | $125.22 | $2.04 | $-28.26 | $4,846.61 | ▼ -28.26 after sell → book $9,253.75; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CF` | 8 | $135.43 | $2.03 | $-29.41 | $5,928.01 | ▼ -29.41 after sell → book $9,251.71; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DUOL` | 7 | $157.46 | $2.03 | $-32.60 | $7,028.20 | ▼ -32.60 after sell → book $9,249.68; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTVA` | 13 | $87.64 | $2.05 | $-37.88 | $8,165.47 | ▼ -37.88 after sell → book $9,247.63; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $7,110.03 | — | rank by macd_hist; rank macd_hist; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1166.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $6,080.48 | — | rank by macd_hist; rank macd_hist; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1166.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 8 | $137.35 | $2.01 | — | $4,979.66 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+5.4; leftover $1166.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 4 | $236.82 | $2.00 | — | $4,030.38 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+8.1; leftover $1166.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 4 | $267.96 | $2.00 | — | $2,956.54 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+8.7; leftover $1166.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `HOOD` | 9 | $120.47 | $2.02 | — | $1,870.25 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+13.6; leftover $1166.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRCL` | 11 | $97.98 | $2.02 | — | $790.44 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+9.5; leftover $1166.50 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $790.44 | ▲ close $9,496.46 vs 09:30 $9,261.85 (session +262.89) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $790.44 | ▼ 09:30 equity $9,461.10 vs yday $9,496.46 (-35.36) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `SION` | 162 | $7.13 | $2.51 | $-34.15 | $1,942.99 | ▼ -34.15 after sell → book $9,458.59; vs 09:30 mark -2.51 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $2,955.85 | ▼ -42.58 after sell → book $9,456.57; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $3,996.13 | ▲ +10.73 after sell → book $9,454.55; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 4 | $267.76 | $2.02 | $+119.74 | $5,065.15 | ▲ +119.74 after sell → book $9,452.53; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASND` | 4 | $263.41 | $2.02 | $-22.22 | $6,116.77 | ▼ -22.22 after sell → book $9,450.51; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,116.77 | ▼ close $9,322.97 vs 09:30 $9,461.10 (session -127.54) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,116.77 | ▲ 09:30 equity $9,426.21 vs yday $9,322.97 (+103.24) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 8 | $141.82 | $2.03 | $+31.71 | $7,249.29 | ▲ +31.71 after sell → book $9,424.18; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `HOOD` | 9 | $120.77 | $2.04 | $-1.40 | $8,334.19 | ▼ -1.40 after sell → book $9,422.14; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRCL` | 11 | $98.91 | $2.04 | $+6.11 | $9,420.10 | ▲ +6.11 after sell → book $9,420.10; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,420.10 | ▲ close $9,420.10 vs 09:30 $9,426.21 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,420.10 | ▲ 09:30 equity $9,420.10 vs yday $9,420.10 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,420.10 | ▲ close $9,420.10 vs 09:30 $9,420.10 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,420.10 | ▲ 09:30 equity $9,420.10 vs yday $9,420.10 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 5 | $206.84 | $2.00 | — | $8,383.89 | — | rank by macd_hist; rank macd_hist; list flatten; ret5=+8.3; leftover $1177.51 | — |
| 2026-09-11 09:30 ET | **BUY** | `VLO` | 3 | $388.00 | $2.00 | — | $7,217.90 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+6.7; leftover $1177.51 | — |
| 2026-09-11 09:30 ET | **BUY** | `SION` | 151 | $7.79 | $2.44 | — | $6,039.16 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+4.2; leftover $1177.51 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 13 | $84.27 | $2.03 | — | $4,941.62 | — | rank by macd_hist; rank macd_hist; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1177.51 | — |
| 2026-09-11 09:30 ET | **BUY** | `QRVO` | 10 | $112.83 | $2.02 | — | $3,811.25 | — | rank by macd_hist; rank macd_hist; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $1177.51 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 21 | $54.91 | $2.05 | — | $2,656.09 | — | rank by macd_hist; rank macd_hist; list yday_gainer; 🔵; ret5=+24.3; leftover $1177.51 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $1,503.07 | — | rank by macd_hist; rank macd_hist; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1177.51 | — |
| 2026-09-11 09:30 ET | **BUY** | `CVI` | 24 | $48.36 | $2.06 | — | $340.37 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+13.2; leftover $1177.51 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $340.37 | ▲ close $9,498.39 vs 09:30 $9,420.10 (session +94.91) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $340.37 | ▼ 09:30 equity $9,348.06 vs yday $9,498.39 (-150.33) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 13 | $86.06 | $2.05 | $+19.19 | $1,457.10 | ▲ +19.19 after sell → book $9,346.01; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 21 | $54.75 | $2.07 | $-7.49 | $2,604.78 | ▼ -7.49 after sell → book $9,343.94; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 7 | $141.42 | $2.03 | $-165.11 | $3,592.68 | ▼ -165.11 after sell → book $9,341.90; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CVI` | 24 | $50.25 | $2.08 | $+41.22 | $4,796.60 | ▲ +41.22 after sell → book $9,339.82; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,796.60 | ▼ close $9,252.71 vs 09:30 $9,348.06 (session -87.11) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,796.60 | ▼ 09:30 equity $9,211.10 vs yday $9,252.71 (-41.61) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `SANM` | 5 | $198.32 | $2.02 | $-46.63 | $5,786.18 | ▼ -46.63 after sell → book $9,209.08; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `QRVO` | 10 | $108.40 | $2.04 | $-48.41 | $6,868.14 | ▼ -48.41 after sell → book $9,207.04; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,868.14 | ▼ close $9,098.14 vs 09:30 $9,211.10 (session -108.90) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,868.14 | ▼ 09:30 equity $9,092.63 vs yday $9,098.14 (-5.51) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `META` | 1 | $679.91 | $1.99 | — | $6,186.23 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+9.3; leftover $1144.69 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 6 | $189.17 | $2.01 | — | $5,049.21 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+7.9; leftover $1144.69 | — |
| 2026-09-16 09:30 ET | **BUY** | `OKTA` | 6 | $186.52 | $2.01 | — | $3,928.08 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+13.6; leftover $1144.69 | — |
| 2026-09-16 09:30 ET | **BUY** | `CRWD` | 4 | $236.92 | $2.00 | — | $2,978.40 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+15.5; leftover $1144.69 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 12 | $89.38 | $2.03 | — | $1,903.81 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1144.69 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 9 | $118.18 | $2.02 | — | $838.17 | — | rank by macd_hist; rank macd_hist; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $1144.69 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $838.17 | ▼ close $9,040.37 vs 09:30 $9,092.63 (session -40.20) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $838.17 | ▲ 09:30 equity $9,073.21 vs yday $9,040.37 (+32.84) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `VLO` | 3 | $398.45 | $2.02 | $+27.33 | $2,031.50 | ▲ +27.33 after sell → book $9,071.19; vs 09:30 mark -2.02 | dropped from list after 4 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 151 | $7.27 | $2.48 | $-83.44 | $3,126.80 | ▼ -83.44 after sell → book $9,068.72; vs 09:30 mark -2.47 | dropped from list after 4 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `META` | 1 | $682.44 | $2.01 | $-1.48 | $3,807.22 | ▼ -1.48 after sell → book $9,066.70; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 6 | $190.35 | $2.03 | $+3.04 | $4,947.30 | ▲ +3.04 after sell → book $9,064.68; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `OKTA` | 6 | $183.00 | $2.03 | $-25.16 | $6,043.27 | ▼ -25.16 after sell → book $9,062.65; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CRWD` | 4 | $236.04 | $2.02 | $-7.54 | $6,985.41 | ▼ -7.54 after sell → book $9,060.63; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 12 | $86.76 | $2.05 | $-35.51 | $8,024.48 | ▼ -35.51 after sell → book $9,058.58; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QRVO` | 9 | $114.90 | $2.04 | $-33.57 | $9,056.54 | ▼ -33.57 after sell → book $9,056.54; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 6 | $170.85 | $2.01 | — | $8,029.43 | — | rank by macd_hist; rank macd_hist; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1132.07 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 7 | $147.61 | $2.01 | — | $6,994.15 | — | rank by macd_hist; rank macd_hist; list flatten,ohlc_hot; ret5=+17.7; leftover $1132.07 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 30 | $36.76 | $2.08 | — | $5,889.27 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; leftover $1132.07 | — |
| 2026-09-17 09:30 ET | **BUY** | `KGS` | 19 | $58.91 | $2.05 | — | $4,767.94 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+9.1; leftover $1132.07 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 51 | $22.12 | $2.14 | — | $3,637.67 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+10.5; leftover $1132.07 | — |
| 2026-09-17 09:30 ET | **BUY** | `INDP` | 343 | $3.30 | $4.42 | — | $2,501.35 | — | rank by macd_hist; rank macd_hist; list yday_mover; 🔵; ret5=+54.3; leftover $1132.07 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGNY` | 41 | $27.38 | $2.11 | — | $1,376.66 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+6.1; leftover $1132.07 | — |
| 2026-09-17 09:30 ET | **BUY** | `BKV` | 49 | $22.75 | $2.14 | — | $259.77 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+10.8; leftover $1132.07 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $259.77 | ▲ close $9,338.52 vs 09:30 $9,073.21 (session +300.94) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $259.77 | ▲ 09:30 equity $9,392.41 vs yday $9,338.52 (+53.89) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 6 | $182.33 | $2.03 | $+64.84 | $1,351.72 | ▲ +64.84 after sell → book $9,390.38; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 7 | $146.50 | $2.03 | $-11.81 | $2,375.19 | ▼ -11.81 after sell → book $9,388.35; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 30 | $39.50 | $2.10 | $+78.02 | $3,558.09 | ▲ +78.02 after sell → book $9,386.25; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `KGS` | 19 | $58.38 | $2.07 | $-14.18 | $4,665.24 | ▼ -14.18 after sell → book $9,384.18; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `GME` | 51 | $22.90 | $2.16 | $+35.47 | $5,830.98 | ▲ +35.47 after sell → book $9,382.02; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `INDP` | 343 | $3.85 | $4.49 | $+179.73 | $7,147.04 | ▲ +179.73 after sell → book $9,377.53; vs 09:30 mark -4.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGNY` | 41 | $27.01 | $2.13 | $-19.42 | $8,252.31 | ▼ -19.42 after sell → book $9,375.39; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BKV` | 49 | $22.92 | $2.16 | $+4.04 | $9,373.24 | ▲ +4.04 after sell → book $9,373.24; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `AMD` | 2 | $547.37 | $2.00 | — | $8,276.50 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+8.2; leftover $1171.65 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 5 | $219.62 | $2.00 | — | $7,176.40 | — | rank by macd_hist; rank macd_hist; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1171.65 | — |
| 2026-09-18 09:30 ET | **BUY** | `CRWD` | 4 | $246.98 | $2.00 | — | $6,186.47 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1171.65 | — |
| 2026-09-18 09:30 ET | **BUY** | `BLLN` | 10 | $112.16 | $2.02 | — | $5,062.85 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ⚪; ret5=+12.3; leftover $1171.65 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 5 | $209.52 | $2.00 | — | $4,013.25 | — | rank by macd_hist; rank macd_hist; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1171.65 | — |
| 2026-09-18 09:30 ET | **BUY** | `AVAV` | 7 | $165.57 | $2.01 | — | $2,852.25 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+11.1; leftover $1171.65 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 13 | $85.00 | $2.03 | — | $1,745.22 | — | rank by macd_hist; rank macd_hist; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1171.65 | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 10 | $108.55 | $2.02 | — | $657.70 | — | rank by macd_hist; rank macd_hist; list flatten; ⚪; ret5=+21.3; leftover $1171.65 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $657.70 | ▼ close $9,229.04 vs 09:30 $9,392.41 (session -128.11) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $657.70 | ▲ 09:30 equity $9,300.74 vs yday $9,229.04 (+71.70) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `CRWD` | 4 | $231.62 | $2.02 | $-65.46 | $1,582.16 | ▼ -65.46 after sell → book $9,298.72; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLLN` | 10 | $106.61 | $2.04 | $-59.56 | $2,646.22 | ▼ -59.56 after sell → book $9,296.68; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 5 | $210.00 | $2.02 | $-1.63 | $3,694.19 | ▼ -1.63 after sell → book $9,294.65; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAV` | 7 | $161.28 | $2.03 | $-34.07 | $4,821.12 | ▼ -34.07 after sell → book $9,292.62; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 13 | $82.83 | $2.05 | $-32.29 | $5,895.86 | ▼ -32.29 after sell → book $9,290.57; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 10 | $107.57 | $2.04 | $-13.86 | $6,969.52 | ▼ -13.86 after sell → book $9,288.53; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 6 | $190.30 | $2.01 | — | $5,825.71 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+10.6; leftover $1161.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `COHR` | 3 | $326.48 | $2.00 | — | $4,844.28 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+3.9; leftover $1161.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `ARM` | 3 | $294.36 | $2.00 | — | $3,959.20 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+4.1; leftover $1161.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `MXL` | 13 | $83.53 | $2.03 | — | $2,871.28 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+8.8; leftover $1161.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `FORM` | 9 | $123.00 | $2.02 | — | $1,762.26 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+3.0; leftover $1161.59 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,762.26 | ▲ close $9,301.81 vs 09:30 $9,300.74 (session +23.33) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,762.26 | ▼ 09:30 equity $9,239.75 vs yday $9,301.81 (-62.06) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `AMD` | 2 | $606.57 | $2.02 | $+114.39 | $2,973.38 | ▲ +114.39 after sell → book $9,237.73; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `COHR` | 3 | $310.29 | $2.02 | $-52.59 | $3,902.24 | ▼ -52.59 after sell → book $9,235.72; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `INOD` | 9 | $61.78 | $2.02 | — | $3,344.20 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; ret5=+10.5; leftover $557.46 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,344.20 | ▲ close $9,348.78 vs 09:30 $9,239.75 (session +115.08) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,344.20 | ▲ 09:30 equity $9,610.52 vs yday $9,348.78 (+261.74) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 6 | $174.50 | $2.03 | $-98.84 | $4,389.17 | ▼ -98.84 after sell → book $9,608.49; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARM` | 3 | $331.78 | $2.02 | $+108.24 | $5,382.49 | ▲ +108.24 after sell → book $9,606.47; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MXL` | 13 | $86.57 | $2.05 | $+35.44 | $6,505.85 | ▲ +35.44 after sell → book $9,604.42; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `ZS` | 6 | $213.00 | $2.01 | — | $5,225.84 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+8.0; leftover $1301.17 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLLN` | 11 | $116.00 | $2.02 | — | $3,947.82 | — | rank by macd_hist; rank macd_hist; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $1301.17 | — |
| 2026-09-23 09:30 ET | **BUY** | `RBRK` | 11 | $112.46 | $2.02 | — | $2,708.74 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+12.7; leftover $1301.17 | — |
| 2026-09-23 09:30 ET | **BUY** | `AKAM` | 11 | $117.33 | $2.02 | — | $1,416.09 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+11.2; leftover $1301.17 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,416.09 | ▲ close $9,780.57 vs 09:30 $9,610.52 (session +184.22) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,416.09 | ▼ 09:30 equity $9,626.69 vs yday $9,780.57 (-153.88) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FORM` | 9 | $126.98 | $2.04 | $+31.77 | $2,556.87 | ▲ +31.77 after sell → book $9,624.65; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INOD` | 9 | $70.50 | $2.04 | $+74.43 | $3,189.33 | ▲ +74.43 after sell → book $9,622.61; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLLN` | 11 | $112.33 | $2.04 | $-44.44 | $4,422.92 | ▼ -44.44 after sell → book $9,620.57; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `RBRK` | 11 | $114.51 | $2.04 | $+18.48 | $5,680.49 | ▲ +18.48 after sell → book $9,618.53; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `AKAM` | 11 | $116.74 | $2.04 | $-10.56 | $6,962.58 | ▼ -10.56 after sell → book $9,616.48; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,962.58 | ▲ close $9,630.72 vs 09:30 $9,626.69 (session +14.24) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,048.66 | ▲ 09:30 equity $10,068.90 vs yday $10,068.90 (-0.00) | 09:30 open · cash $7,048.66 (unchanged overnight, no fees) · equity $10,068.90 vs prior close $10,068.90 (-0.00) · 3 name(s) re-marked at the open (per-name table). MDB×2 yday $421.40 → 09:30 $421.40 +0.00; VICR×4 yday $276.06 → 09:30 $276.06 +0.00; ZS×5 yday $214.64 → 09:30 $214.64 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `AMD` | 1 | $634.53 | $1.99 | — | $6,412.13 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+15.4; leftover $881.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `META` | 1 | $768.85 | $1.99 | — | $5,641.29 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+14.0; leftover $881.08 | join🟢 sector🟡 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CLS` | 2 | $380.51 | $2.00 | — | $4,878.28 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+13.2; leftover $881.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `NTRA` | 2 | $410.00 | $2.00 | — | $4,056.28 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ⚪; ret5=+11.6; leftover $881.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 3 | $272.16 | $2.00 | — | $3,237.80 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $881.08 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CDNS` | 2 | $324.97 | $2.00 | — | $2,585.87 | — | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ⚪; ret5=+14.7; leftover $881.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 7 | $123.50 | $2.01 | — | $1,719.36 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $881.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 4 | $184.00 | $2.00 | — | $981.35 | — | rank by macd_hist; rank macd_hist; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $881.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $981.35 | ▼ close $10,021.69 vs 09:30 $10,068.90 (session -31.22) | 16:00 close · cash $981.35 · equity $10,021.69 vs 09:30 $10,068.90 (-47.21; session marks -31.22) · 11 name(s) marked open→close (per-name table). MDB×2 09:30 $421.40 → close $421.40 -0.00; VICR×4 09:30 $276.06 → close $276.06 -0.00; ZS×5 09:30 $214.64 → close $214.64 -0.00; AMD×1 09:30 $634.53 → close $630.63 -3.90; META×1 09:30 $768.85 → close $751.66 -17.19; CLS×2 09:30 $380.51 → close $365.44 -30.14; NTRA×2 09:30 $410.00 → close $412.56 +5.12; ILMN×3 09:30 $272.16 → close $270.00 -6.48; CDNS×2 09:30 $324.97 → close $326.13 +2.32; GRAL×7 09:30 $123.50 → close $126.89 +23.73; TWST×4 09:30 $184.00 → close $182.83 -4.68 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1277.58 < 1 share @ 1646.93 |
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
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SQM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MSTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HUBS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `RBLX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
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
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HUT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VICR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `META` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SIMO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBLX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INSP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-21 | `SNDK` | cash | leftover split 1161.59 < 1 share @ 1826.00 |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MXL` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FORM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `META` | cash | leftover split 557.46 < 1 share @ 731.40 |
| 2026-09-22 | `MPWR` | no_price | no 09:30 open |
| 2026-09-22 | `ZS` | no_price | no 09:30 open |
| 2026-09-22 | `RBRK` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `AKAM` | no_price | no 09:30 open |
| 2026-09-23 | `MPWR` | cash | leftover split 1301.17 < 1 share @ 1367.08 |
| 2026-09-24 | `CLS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CRWD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DDOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `KEYS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `MDB` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `VICR` | 5 | 2026-09-18 @ $219.62 | rank by macd_hist; rank macd_hist; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1171.65 |
| `ZS` | 6 | 2026-09-23 @ $213.00 | rank by macd_hist; rank macd_hist; list ohlc_hot; 🔵; ret5=+8.0; leftover $1301.17 |
