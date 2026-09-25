# Factor mine action — `union_macd_xup_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ macd_xup, no 🚨

Cash book **-5.83%** ($9,417) · signal-only (no cash/fees) was -55.44%. Starts YES **5/30**. Fills 123 · skips 209 · realized $-2117.42.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: MACD histogram just crossed from ≤0 to >0 on the last finished bar.
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
- **Gate** `macd_cross_up=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6,799.85.

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
| 2026-08-14 09:30 ET | **BUY** | `BCAR` | 1638 | $6.09 | $21.13 | — | $3.45 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ⚪; ret5=+27.6; leftover $10000.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.45 | ▼ close $9,552.99 vs 09:30 $10,000.00 (session -425.88) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.45 | ▲ 09:30 equity $9,815.07 vs yday $9,552.99 (+262.08) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.45 | ▼ close $9,536.61 vs 09:30 $9,815.07 (session -278.46) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.45 | ▼ 09:30 equity $9,045.21 vs yday $9,536.61 (-491.40) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.45 | ▼ close $8,733.99 vs 09:30 $9,045.21 (session -311.22) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.45 | ▼ 09:30 equity $8,717.61 vs yday $8,733.99 (-16.38) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `BCAR` | 1638 | $5.32 | $21.47 | $-1303.86 | $8,696.14 | ▼ -1,303.86 after sell → book $8,696.14; vs 09:30 mark -21.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,696.14 | ▲ close $8,696.14 vs 09:30 $8,717.61 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,696.14 | ▲ 09:30 equity $8,696.14 vs yday $8,696.14 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 15 | $109.06 | $2.04 | — | $7,058.20 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $1739.23 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 2460 | $0.71 | $24.77 | — | $5,294.21 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1739.23 | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 17 | $97.43 | $2.04 | — | $3,635.86 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; leftover $1739.23 | — |
| 2026-08-20 09:30 ET | **BUY** | `SBET` | 230 | $7.55 | $2.97 | — | $1,896.39 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+14.6; leftover $1739.23 | — |
| 2026-08-20 09:30 ET | **BUY** | `BMNR` | 81 | $21.46 | $2.23 | — | $155.90 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ⚪; ret5=+13.1; leftover $1739.23 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $155.90 | ▼ close $8,621.93 vs 09:30 $8,696.14 (session -40.16) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $155.90 | ▲ 09:30 equity $8,734.84 vs yday $8,621.93 (+112.91) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 22 | $1.39 | $0.37 | — | $124.95 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $31.18 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 3 | $8.28 | $0.26 | — | $99.85 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $31.18 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 2 | $11.70 | $0.24 | — | $76.21 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $31.18 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.21 | ▲ close $8,882.41 vs 09:30 $8,734.84 (session +148.44) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.21 | ▲ 09:30 equity $9,020.59 vs yday $8,882.41 (+138.18) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.21 | ▲ close $9,097.85 vs 09:30 $9,020.59 (session +77.26) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.21 | ▼ 09:30 equity $9,035.72 vs yday $9,097.85 (-62.13) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `BNTX` | 15 | $113.88 | $2.06 | $+68.21 | $1,782.35 | ▲ +68.21 after sell → book $9,033.66; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HUMA` | 2460 | $0.66 | $24.11 | $-157.12 | $3,389.22 | ▼ -157.12 after sell → book $9,009.55; vs 09:30 mark -24.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EL` | 17 | $104.00 | $2.07 | $+107.58 | $5,155.16 | ▲ +107.58 after sell → book $9,007.49; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SBET` | 230 | $8.05 | $3.02 | $+109.01 | $7,003.64 | ▲ +109.01 after sell → book $9,004.47; vs 09:30 mark -3.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BMNR` | 81 | $23.80 | $2.26 | $+185.05 | $8,929.17 | ▲ +185.05 after sell → book $9,002.20; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 700 | $6.37 | $9.03 | — | $4,461.14 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $4464.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 101 | $43.76 | $2.29 | — | $39.09 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $4464.59 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.09 | ▲ close $9,072.74 vs 09:30 $9,035.72 (session +81.86) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.09 | ▼ 09:30 equity $8,881.07 vs yday $9,072.74 (-191.67) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `INDP` | 22 | $1.09 | $0.33 | $-7.30 | $62.74 | ▼ -7.30 after sell → book $8,880.74; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MRVI` | 3 | $8.85 | $0.29 | $+1.16 | $89.00 | ▲ +1.16 after sell → book $8,880.45; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MARA` | 2 | $11.56 | $0.26 | $-0.78 | $111.86 | ▼ -0.78 after sell → book $8,880.19; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 1 | $31.21 | $0.32 | — | $80.34 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $37.29 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 3 | $11.12 | $0.34 | — | $46.64 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $37.29 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.64 | ▼ close $8,802.56 vs 09:30 $8,881.07 (session -76.98) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.64 | ▲ 09:30 equity $8,811.40 vs yday $8,802.56 (+8.84) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.64 | ▼ close $8,748.90 vs 09:30 $8,811.40 (session -62.50) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.64 | ▼ 09:30 equity $8,722.49 vs yday $8,748.90 (-26.41) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 700 | $5.88 | $9.18 | $-361.21 | $4,153.46 | ▼ -361.21 after sell → book $8,713.31; vs 09:30 mark -9.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RHI` | 101 | $44.51 | $2.35 | $+71.11 | $8,646.62 | ▲ +71.11 after sell → book $8,710.96; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 777 | $1.39 | $10.02 | — | $7,556.57 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ret5=+20.4; leftover $1080.83 | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 8 | $122.81 | $2.01 | — | $6,572.07 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1080.83 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 56 | $19.25 | $2.16 | — | $5,491.92 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; ret5=+14.1; leftover $1080.83 | — |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 58 | $18.36 | $2.16 | — | $4,424.87 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+12.8; leftover $1080.83 | — |
| 2026-08-28 09:30 ET | **BUY** | `FTNT` | 6 | $172.58 | $2.01 | — | $3,387.38 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+14.6; leftover $1080.83 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $2,340.74 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; ret5=+7.8; leftover $1080.83 | — |
| 2026-08-28 09:30 ET | **BUY** | `RBRK` | 10 | $98.95 | $2.02 | — | $1,349.22 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; ret5=+9.7; leftover $1080.83 | — |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 1 | $542.00 | $1.99 | — | $805.23 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; ret5=+4.8; leftover $1080.83 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $805.23 | ▼ close $8,409.41 vs 09:30 $8,722.49 (session -277.17) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $805.23 | ▼ 09:30 equity $8,339.59 vs yday $8,409.41 (-69.82) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `AVBP` | 1 | $29.94 | $0.32 | $-1.91 | $834.85 | ▼ -1.91 after sell → book $8,339.27; vs 09:30 mark -0.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FLNC` | 3 | $10.82 | $0.35 | $-1.60 | $866.95 | ▼ -1.60 after sell → book $8,338.91; vs 09:30 mark -0.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $866.95 | ▼ close $8,333.51 vs 09:30 $8,339.59 (session -5.40) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $866.95 | ▼ 09:30 equity $8,197.64 vs yday $8,333.51 (-135.87) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $866.95 | ▼ close $8,017.55 vs 09:30 $8,197.64 (session -180.09) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $866.95 | ▲ 09:30 equity $8,044.94 vs yday $8,017.55 (+27.39) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `LVWR` | 777 | $1.17 | $10.16 | $-191.13 | $1,765.88 | ▼ -191.13 after sell → book $8,034.78; vs 09:30 mark -10.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TTMI` | 8 | $114.22 | $2.03 | $-72.77 | $2,677.61 | ▼ -72.77 after sell → book $8,032.75; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERAS` | 56 | $16.97 | $2.18 | $-132.02 | $3,625.75 | ▼ -132.02 after sell → book $8,030.57; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NEO` | 58 | $17.40 | $2.18 | $-60.03 | $4,632.76 | ▼ -60.03 after sell → book $8,028.38; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FTNT` | 6 | $162.19 | $2.03 | $-66.38 | $5,603.88 | ▼ -66.38 after sell → book $8,026.36; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 4 | $246.70 | $2.02 | $-61.86 | $6,588.65 | ▼ -61.86 after sell → book $8,024.33; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `RBRK` | 10 | $89.00 | $2.04 | $-103.56 | $7,476.61 | ▼ -103.56 after sell → book $8,022.29; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ULTA` | 1 | $545.68 | $2.01 | $-0.33 | $8,020.28 | ▼ -0.33 after sell → book $8,020.28; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,020.28 | ▲ close $8,020.28 vs 09:30 $8,044.94 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,020.28 | ▲ 09:30 equity $8,020.28 vs yday $8,020.28 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $6,696.22 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1336.71 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 166 | $8.03 | $2.49 | — | $5,360.75 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1336.71 | — |
| 2026-09-03 09:30 ET | **BUY** | `PYXS` | 360 | $3.71 | $4.64 | — | $4,020.50 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+12.3; leftover $1336.71 | — |
| 2026-09-03 09:30 ET | **BUY** | `MLYS` | 45 | $29.15 | $2.12 | — | $2,706.63 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+12.2; leftover $1336.71 | — |
| 2026-09-03 09:30 ET | **BUY** | `HP` | 27 | $47.74 | $2.07 | — | $1,415.58 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+15.1; leftover $1336.71 | — |
| 2026-09-03 09:30 ET | **BUY** | `RSKD` | 200 | $6.68 | $2.59 | — | $76.99 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+11.4; leftover $1336.71 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.99 | ▼ close $7,870.26 vs 09:30 $8,020.28 (session -134.04) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.99 | ▼ 09:30 equity $7,793.53 vs yday $7,870.26 (-76.73) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 4 | $3.52 | $0.15 | — | $62.76 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $15.40 | — |
| 2026-09-04 09:30 ET | **BUY** | `GSM` | 3 | $4.67 | $0.15 | — | $48.60 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; ret5=+11.9; leftover $15.40 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.60 | ▲ close $7,896.90 vs 09:30 $7,793.53 (session +103.67) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.60 | ▲ 09:30 equity $8,004.49 vs yday $7,896.90 (+107.59) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.60 | ▼ close $7,976.86 vs 09:30 $8,004.49 (session -27.63) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.60 | ▼ 09:30 equity $7,819.68 vs yday $7,976.86 (-157.18) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 25 | $53.16 | $2.09 | $+2.85 | $1,375.51 | ▲ +2.85 after sell → book $7,817.59; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 166 | $8.01 | $2.53 | $-8.33 | $2,702.64 | ▼ -8.33 after sell → book $7,815.06; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `PYXS` | 360 | $3.48 | $4.71 | $-92.16 | $3,950.73 | ▼ -92.16 after sell → book $7,810.35; vs 09:30 mark -4.71 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MLYS` | 45 | $30.87 | $2.15 | $+73.13 | $5,337.73 | ▲ +73.13 after sell → book $7,808.20; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HP` | 27 | $44.97 | $2.09 | $-78.95 | $6,549.83 | ▼ -78.95 after sell → book $7,806.11; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RSKD` | 200 | $6.13 | $2.63 | $-115.22 | $7,773.20 | ▼ -115.22 after sell → book $7,803.48; vs 09:30 mark -2.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,773.20 | ▼ close $7,803.27 vs 09:30 $7,819.68 (session -0.21) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,773.20 | ▼ 09:30 equity $7,802.14 vs yday $7,803.27 (-1.13) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `EOSE` | 4 | $3.96 | $0.19 | $+1.44 | $7,788.87 | ▲ +1.44 after sell → book $7,801.95; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `GSM` | 3 | $4.36 | $0.16 | $-1.24 | $7,801.79 | ▼ -1.24 after sell → book $7,801.79; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,801.79 | ▲ close $7,801.79 vs 09:30 $7,802.14 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,801.79 | ▲ 09:30 equity $7,801.79 vs yday $7,801.79 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 440 | $5.91 | $5.68 | — | $5,195.71 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; leftover $2600.60 | — |
| 2026-09-11 09:30 ET | **BUY** | `APPS` | 218 | $11.88 | $2.81 | — | $2,603.06 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+20.7; leftover $2600.60 | — |
| 2026-09-11 09:30 ET | **BUY** | `INSP` | 37 | $69.88 | $2.10 | — | $15.40 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+8.0; leftover $2600.60 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.40 | ▲ close $7,878.18 vs 09:30 $7,801.79 (session +86.98) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.40 | ▼ 09:30 equity $7,824.48 vs yday $7,878.18 (-53.70) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.40 | ▲ close $8,195.58 vs 09:30 $7,824.48 (session +371.10) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.40 | ▼ 09:30 equity $8,115.18 vs yday $8,195.58 (-80.40) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.40 | ▼ close $8,006.44 vs 09:30 $8,115.18 (session -108.74) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.40 | ▼ 09:30 equity $7,936.09 vs yday $8,006.44 (-70.35) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `DBI` | 440 | $6.25 | $5.77 | $+138.15 | $2,759.63 | ▲ +138.15 after sell → book $7,930.32; vs 09:30 mark -5.77 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `APPS` | 218 | $11.30 | $2.87 | $-132.12 | $5,220.16 | ▼ -132.12 after sell → book $7,927.45; vs 09:30 mark -2.87 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `INSP` | 37 | $73.17 | $2.13 | $+117.50 | $7,925.32 | ▲ +117.50 after sell → book $7,925.32; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 550 | $1.80 | $7.09 | — | $6,928.22 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $990.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `FTRE` | 50 | $19.75 | $2.14 | — | $5,938.58 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+15.7; leftover $990.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 42 | $23.29 | $2.12 | — | $4,958.29 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+16.1; leftover $990.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `RVTY` | 7 | $140.88 | $2.01 | — | $3,970.12 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,ohlc_hot; 🔵; ret5=+10.3; leftover $990.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `TENB` | 26 | $36.86 | $2.07 | — | $3,009.69 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; ret5=+13.0; leftover $990.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `MRCY` | 11 | $87.52 | $2.02 | — | $2,044.95 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+4.3; leftover $990.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `RBRK` | 9 | $101.97 | $2.02 | — | $1,125.20 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+13.0; leftover $990.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `DOCU` | 14 | $70.60 | $2.03 | — | $134.77 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+10.4; leftover $990.66 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $134.77 | ▲ close $8,132.73 vs 09:30 $7,936.09 (session +228.91) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $134.77 | ▲ 09:30 equity $8,156.71 vs yday $8,132.73 (+23.98) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 2 | $10.25 | $0.21 | — | $114.06 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $26.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 11 | $2.40 | $0.30 | — | $87.36 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $26.95 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $87.36 | ▲ close $8,400.08 vs 09:30 $8,156.71 (session +243.88) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.36 | ▼ 09:30 equity $8,337.88 vs yday $8,400.08 (-62.20) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `LVWR` | 7 | $1.49 | $0.13 | — | $76.80 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+25.7; leftover $10.92 | — |
| 2026-09-18 09:30 ET | **BUY** | `PGEN` | 1 | $7.98 | $0.08 | — | $68.74 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ⚪; ret5=+17.8; leftover $10.92 | — |
| 2026-09-18 09:30 ET | **BUY** | `SATL` | 1 | $5.49 | $0.06 | — | $63.20 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+17.2; leftover $10.92 | — |
| 2026-09-18 09:30 ET | **BUY** | `LTRX` | 1 | $5.89 | $0.06 | — | $57.25 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+15.0; leftover $10.92 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.25 | ▼ close $8,249.88 vs 09:30 $8,337.88 (session -87.68) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.25 | ▲ 09:30 equity $8,353.34 vs yday $8,249.88 (+103.46) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `HLP` | 550 | $2.08 | $7.20 | $+139.71 | $1,194.05 | ▲ +139.71 after sell → book $8,346.14; vs 09:30 mark -7.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `FTRE` | 50 | $20.29 | $2.16 | $+22.70 | $2,206.39 | ▲ +22.70 after sell → book $8,343.98; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 42 | $29.43 | $2.14 | $+253.63 | $3,440.31 | ▲ +253.63 after sell → book $8,341.84; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RVTY` | 7 | $144.53 | $2.03 | $+21.51 | $4,449.99 | ▲ +21.51 after sell → book $8,339.81; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TENB` | 26 | $35.59 | $2.09 | $-37.18 | $5,373.24 | ▼ -37.18 after sell → book $8,337.72; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `MRCY` | 11 | $86.52 | $2.04 | $-15.07 | $6,322.92 | ▼ -15.07 after sell → book $8,335.68; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 9 | $107.57 | $2.04 | $+46.35 | $7,289.01 | ▲ +46.35 after sell → book $8,333.64; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `DOCU` | 14 | $69.17 | $2.05 | $-24.10 | $8,255.34 | ▼ -24.10 after sell → book $8,331.59; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 6 | $157.87 | $2.01 | — | $7,306.11 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten; ret5=+6.5; leftover $1031.92 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 565 | $1.82 | $7.29 | — | $6,267.70 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1031.92 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 179 | $5.75 | $2.53 | — | $5,235.03 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+30.3; leftover $1031.92 | — |
| 2026-09-21 09:30 ET | **BUY** | `MSTR` | 6 | $164.58 | $2.01 | — | $4,245.54 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.5; leftover $1031.92 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 479 | $2.15 | $6.18 | — | $3,209.51 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $1031.92 | — |
| 2026-09-21 09:30 ET | **BUY** | `ABTC` | 96 | $10.71 | $2.28 | — | $2,179.07 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+21.4; leftover $1031.92 | — |
| 2026-09-21 09:30 ET | **BUY** | `ASST` | 32 | $31.64 | $2.09 | — | $1,164.51 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+8.9; leftover $1031.92 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,164.51 | ▼ close $8,284.31 vs 09:30 $8,353.34 (session -22.90) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,164.51 | ▼ 09:30 equity $8,243.13 vs yday $8,284.31 (-41.18) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 2 | $10.18 | $0.23 | $-0.58 | $1,184.64 | ▼ -0.58 after sell → book $8,242.90; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 16 | $9.11 | $1.51 | — | $1,037.37 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ret5=+44.4; leftover $148.08 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 146 | $1.01 | $1.91 | — | $888.00 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ret5=+14.3; leftover $148.08 | — |
| 2026-09-22 09:30 ET | **BUY** | `MX` | 46 | $3.18 | $1.60 | — | $740.12 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ret5=+11.1; leftover $148.08 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $740.12 | ▲ close $8,241.34 vs 09:30 $8,243.13 (session +3.47) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $740.12 | ▼ 09:30 equity $8,205.97 vs yday $8,241.34 (-35.37) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `SABR` | 11 | $2.24 | $0.30 | $-2.36 | $764.46 | ▼ -2.36 after sell → book $8,205.68; vs 09:30 mark -0.29 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 7 | $1.41 | $0.14 | $-0.82 | $774.19 | ▼ -0.82 after sell → book $8,205.54; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `PGEN` | 1 | $7.95 | $0.10 | $-0.22 | $782.04 | ▼ -0.22 after sell → book $8,205.43; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SATL` | 1 | $5.74 | $0.08 | $+0.12 | $787.70 | ▲ +0.12 after sell → book $8,205.35; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `LTRX` | 1 | $6.25 | $0.09 | $+0.21 | $793.86 | ▲ +0.21 after sell → book $8,205.27; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 2 | $41.76 | $0.84 | — | $709.50 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $99.23 | — |
| 2026-09-23 09:30 ET | **BUY** | `THM` | 34 | $2.86 | $1.07 | — | $611.19 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+25.5; leftover $99.23 | — |
| 2026-09-23 09:30 ET | **BUY** | `FWDI` | 12 | $8.20 | $1.02 | — | $511.77 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+17.6; leftover $99.23 | — |
| 2026-09-23 09:30 ET | **BUY** | `FIVN` | 2 | $38.91 | $0.78 | — | $433.17 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+16.6; leftover $99.23 | — |
| 2026-09-23 09:30 ET | **BUY** | `XXI` | 14 | $6.93 | $1.01 | — | $335.14 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+14.2; leftover $99.23 | — |
| 2026-09-23 09:30 ET | **BUY** | `CNTN` | 40 | $2.44 | $1.10 | — | $236.44 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+16.2; leftover $99.23 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 2 | $40.00 | $0.81 | — | $155.64 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+6.7; leftover $99.23 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $155.64 | ▼ close $7,940.06 vs 09:30 $8,205.97 (session -258.57) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $155.64 | ▼ 09:30 equity $7,832.67 vs yday $7,940.06 (-107.39) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 6 | $163.95 | $2.03 | $+32.44 | $1,137.31 | ▲ +32.44 after sell → book $7,830.65; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTBT` | 565 | $1.73 | $7.39 | $-71.18 | $2,104.54 | ▼ -71.18 after sell → book $7,823.25; vs 09:30 mark -7.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GEMI` | 179 | $5.62 | $2.57 | $-29.26 | $3,107.96 | ▼ -29.26 after sell → book $7,820.69; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MSTR` | 6 | $161.00 | $2.03 | $-25.52 | $4,071.93 | ▼ -25.52 after sell → book $7,818.66; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMTX` | 479 | $1.88 | $6.27 | $-141.78 | $4,966.18 | ▼ -141.78 after sell → book $7,812.39; vs 09:30 mark -6.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ABTC` | 96 | $9.64 | $2.30 | $-107.30 | $5,889.32 | ▼ -107.30 after sell → book $7,810.09; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ASST` | 32 | $28.52 | $2.11 | $-104.03 | $6,799.85 | ▼ -104.03 after sell → book $7,807.98; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,799.85 | ▲ close $7,817.35 vs 09:30 $7,832.67 (session +9.37) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,919.55 | ▼ 09:30 equity $9,420.16 vs yday $9,421.86 (-1.70) | 09:30 open · cash $7,919.55 (unchanged overnight, no fees) · equity $9,420.16 vs prior close $9,421.86 (-1.70) · 12 name(s) re-marked at the open (per-name table). ADMA×2 yday $9.52 → 09:30 $9.52 +0.00; AIBZ×40 yday $4.31 → 09:30 $4.31 +0.00; APPS×16 yday $10.88 → 09:30 $10.88 +0.00; ARQQ×8 yday $23.12 → 09:30 $23.12 +0.00; BTQ×72 yday $2.79 → 09:30 $2.79 +0.00; FDMT×1 yday $14.71 → 09:30 $14.71 +0.00; FIVN×5 yday $36.66 → 09:30 $36.66 +0.00; GRAL×1 yday $125.21 → 09:30 $123.50 -1.71; IVVD×205 yday $0.91 → 09:30 $0.91 +0.00; MX×65 yday $3.18 → 09:30 $3.18 +0.00; SGRY×1 yday $14.20 → 09:30 $14.20 +0.00; THM×7 yday $2.81 → 09:30 $2.81 +0.00 | — |
| 2026-09-25 09:30 ET | **SELL** | `GRAL` | 1 | $123.50 | $1.26 | $+14.42 | $8,041.79 | ▲ +14.42 after sell → book $9,418.90; vs 09:30 mark -1.26 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 65 | $20.61 | $2.19 | — | $6,699.96 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten; 🔵; ret5=+9.1; leftover $1340.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 175 | $7.65 | $2.52 | — | $5,358.69 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1340.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 51 | $26.27 | $2.14 | — | $4,016.78 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1340.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `QMCO` | 44 | $29.80 | $2.12 | — | $2,703.46 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+18.2; leftover $1340.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CBRL` | 25 | $52.39 | $2.06 | — | $1,391.64 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+18.5; leftover $1340.30 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SENS` | 130 | $10.28 | $2.38 | — | $52.86 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ⚪; ret5=+9.7; leftover $1340.30 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.86 | ▲ close $9,416.55 vs 09:30 $9,420.16 (session +11.06) | 16:00 close · cash $52.86 · equity $9,416.55 vs 09:30 $9,420.16 (-3.61; session marks +11.06) · 17 name(s) marked open→close (per-name table). ADMA×2 09:30 $9.52 → close $9.52 +0.00; AIBZ×40 09:30 $4.31 → close $4.31 -0.00; APPS×16 09:30 $10.88 → close $10.88 +0.00; ARQQ×8 09:30 $23.12 → close $23.12 +0.00; BTQ×72 09:30 $2.79 → close $2.79 -0.00; FDMT×1 09:30 $14.71 → close $14.71 +0.00; FIVN×5 09:30 $36.66 → close $36.66 -0.00; IVVD×205 09:30 $0.91 → close $0.91 -0.00; MX×65 09:30 $3.18 → close $3.18 +0.00; SGRY×1 09:30 $14.20 → close $14.20 -0.00; THM×7 09:30 $2.81 → close $2.81 -0.00; OMER×65 09:30 $20.61 → close $20.08 -34.45; MRVI×175 09:30 $7.65 → close $7.60 -8.75; WRBY×51 09:30 $26.27 → close $26.71 +22.44; QMCO×44 09:30 $29.80 → close $31.68 +82.72; CBRL×25 09:30 $52.39 → close $51.81 -14.50; SENS×130 09:30 $10.28 → close $10.00 -36.40 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `BCAR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `RDDT` | cash | leftover split 3.45 < 1 share @ 177.51 |
| 2026-08-18 | `BCAR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DVLT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ATAT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `BNTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HUMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `EL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BMNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CF` | cash | leftover split 31.18 < 1 share @ 127.43 |
| 2026-08-21 | `ILMN` | cash | leftover split 31.18 < 1 share @ 212.40 |
| 2026-08-24 | `BNTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HUMA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BMNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DK` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RHI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BE` | cash | leftover split 37.29 < 1 share @ 213.94 |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RHI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AVBP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `FLNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BZ` | cash | leftover split 6.66 < 1 share @ 18.50 |
| 2026-08-27 | `VYX` | cash | leftover split 6.66 < 1 share @ 8.95 |
| 2026-08-27 | `GAP` | cash | leftover split 6.66 < 1 share @ 20.75 |
| 2026-08-27 | `AEO` | cash | leftover split 6.66 < 1 share @ 17.27 |
| 2026-08-27 | `SMTC` | cash | leftover split 6.66 < 1 share @ 149.40 |
| 2026-08-27 | `GEN` | cash | leftover split 6.66 < 1 share @ 29.83 |
| 2026-08-27 | `PGY` | cash | leftover split 6.66 < 1 share @ 22.93 |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `LVWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NEO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FTNT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RBRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ULTA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ERAS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NEO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FTNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `RBRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ULTA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `XRX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `WFRD` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ZETA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PYXS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MLYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RSKD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 15.40 < 1 share @ 513.78 |
| 2026-09-04 | `RNG` | cash | leftover split 15.40 < 1 share @ 75.35 |
| 2026-09-04 | `LULU` | cash | leftover split 15.40 < 1 share @ 98.15 |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PYXS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MLYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RSKD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `EOSE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `GSM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `EOSE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `GSM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `EYPT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DOCN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ANGX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VSAT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ARQQ` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XHLD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ARBE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COHU` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `DBI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `APPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `INSP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `IMSR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `WCC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `DBI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `APPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INIO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `HLP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `FTRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SDGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TENB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `MRCY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RBRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `DOCU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AMN` | cash | leftover split 26.95 < 1 share @ 34.93 |
| 2026-09-17 | `BRKR` | cash | leftover split 26.95 < 1 share @ 61.90 |
| 2026-09-17 | `ADPT` | cash | leftover split 26.95 < 1 share @ 28.23 |
| 2026-09-18 | `HLP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `FTRE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SDGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TENB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `MRCY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RBRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `DOCU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `GNRC` | cash | leftover split 10.92 < 1 share @ 209.52 |
| 2026-09-18 | `TEM` | cash | leftover split 10.92 < 1 share @ 81.40 |
| 2026-09-18 | `FCEL` | cash | leftover split 10.92 < 1 share @ 17.80 |
| 2026-09-18 | `VOYG` | cash | leftover split 10.92 < 1 share @ 37.16 |
| 2026-09-21 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `LVWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SATL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `LTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SNDK` | cash | leftover split 1031.92 < 1 share @ 1826.00 |
| 2026-09-22 | `SABR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `SATL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `LTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GEMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MSTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `ARQQ` | no_price | no 09:30 open |
| 2026-09-22 | `AIBZ` | no_price | no 09:30 open |
| 2026-09-23 | `A` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GEMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CRML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `IVVD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `MX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `MPWR` | cash | leftover split 99.23 < 1 share @ 1367.08 |
| 2026-09-24 | `CRML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `IVVD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `MX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `VKTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `THM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `FWDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `FIVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `XXI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CNTN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BLSH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RNG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `MDB` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SNPS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CRML` | 16 | 2026-09-22 @ $9.11 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ret5=+44.4; leftover $148.08 |
| `IVVD` | 146 | 2026-09-22 @ $1.01 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ret5=+14.3; leftover $148.08 |
| `MX` | 46 | 2026-09-22 @ $3.18 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ret5=+11.1; leftover $148.08 |
| `VKTX` | 2 | 2026-09-23 @ $41.76 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $99.23 |
| `THM` | 34 | 2026-09-23 @ $2.86 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+25.5; leftover $99.23 |
| `FWDI` | 12 | 2026-09-23 @ $8.20 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+17.6; leftover $99.23 |
| `FIVN` | 2 | 2026-09-23 @ $38.91 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+16.6; leftover $99.23 |
| `XXI` | 14 | 2026-09-23 @ $6.93 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+14.2; leftover $99.23 |
| `CNTN` | 40 | 2026-09-23 @ $2.44 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+16.2; leftover $99.23 |
| `BLSH` | 2 | 2026-09-23 @ $40.00 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+6.7; leftover $99.23 |
