# Factor mine action — `union_clk_hold_vs_sector_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · Clock-B #6 stock holds while sector camera is red

Cash book **-9.96%** ($9,004) · signal-only (no cash/fees) was -2.27%. Starts YES **25/30**. Fills 202 · skips 96 · realized $+387.41.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: Clock-B #6: the stock held up (yesterday up or last bar green) while the sector camera is red.
- Must-not: Clock-B #5 long veto: extreme prior extension plus diminishing progress or a failed breakout.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 8.
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
- **Gate** `clk_hold_vs_sector=True` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,757.95.

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
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 48 | $41.23 | $2.13 | — | $8,018.83 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover; ret5=+46.0; leftover $2000.00 | — |
| 2026-08-17 09:30 ET | **BUY** | `JBIO` | 81 | $24.60 | $2.23 | — | $6,023.99 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+12.5; leftover $2000.00 | — |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 63 | $31.30 | $2.18 | — | $4,049.91 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer; ret5=-3.8; leftover $2000.00 | — |
| 2026-08-17 09:30 ET | **BUY** | `ZNTL` | 561 | $3.56 | $7.24 | — | $2,045.52 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_mover; ret5=-15.6; leftover $2000.00 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 21 | $92.99 | $2.05 | — | $90.67 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer; ret5=-0.8; leftover $2000.00 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.67 | ▲ close $10,015.68 vs 09:30 $10,000.00 (session +31.52) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.67 | ▼ 09:30 equity $9,967.60 vs yday $10,015.68 (-48.08) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 48 | $41.50 | $2.16 | $+8.67 | $2,080.51 | ▲ +8.67 after sell → book $9,965.44; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `JBIO` | 81 | $23.07 | $2.26 | $-128.42 | $3,946.92 | ▼ -128.42 after sell → book $9,963.18; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 63 | $31.31 | $2.21 | $-3.75 | $5,917.25 | ▼ -3.75 after sell → book $9,960.98; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ZNTL` | 561 | $3.75 | $7.35 | $+92.01 | $8,013.65 | ▲ +92.01 after sell → book $9,953.63; vs 09:30 mark -7.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 21 | $92.38 | $2.08 | $-16.94 | $9,951.55 | ▼ -16.94 after sell → book $9,951.55; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,951.55 | ▲ close $9,951.55 vs 09:30 $9,967.60 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,951.55 | ▲ 09:30 equity $9,951.55 vs yday $9,951.55 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,951.55 | ▲ close $9,951.55 vs 09:30 $9,951.55 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,951.55 | ▲ 09:30 equity $9,951.55 vs yday $9,951.55 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `HTHT` | 25 | $48.39 | $2.06 | — | $8,739.74 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+17.8; leftover $1243.94 | — |
| 2026-08-20 09:30 ET | **BUY** | `RERE` | 296 | $4.20 | $3.82 | — | $7,492.72 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; 🔵; ret5=+2.9; leftover $1243.94 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 36 | $34.05 | $2.10 | — | $6,264.82 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; 🔵; ret5=+9.3; leftover $1243.94 | — |
| 2026-08-20 09:30 ET | **BUY** | `SG` | 193 | $6.43 | $2.57 | — | $5,021.26 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; 🔵; ret5=+10.3; leftover $1243.94 | — |
| 2026-08-20 09:30 ET | **BUY** | `BEKE` | 73 | $17.04 | $2.21 | — | $3,775.13 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight; 🔵; ret5=-0.2; leftover $1243.94 | — |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 10 | $123.47 | $2.02 | — | $2,538.41 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; 🔵; ret5=+2.9; leftover $1243.94 | — |
| 2026-08-20 09:30 ET | **BUY** | `ROST` | 5 | $229.55 | $2.00 | — | $1,388.66 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight,overnight_mega; 🔵; ret5=-5.5; leftover $1243.94 | — |
| 2026-08-20 09:30 ET | **BUY** | `BKE` | 29 | $42.60 | $2.08 | — | $151.18 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight; 🔵; ret5=-4.6; leftover $1243.94 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.18 | ▲ close $10,027.38 vs 09:30 $9,951.55 (session +94.69) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.18 | ▲ 09:30 equity $10,167.22 vs yday $10,027.38 (+139.84) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `HTHT` | 25 | $49.58 | $2.08 | $+25.60 | $1,388.60 | ▲ +25.60 after sell → book $10,165.13; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `RERE` | 296 | $4.17 | $3.88 | $-16.58 | $2,619.04 | ▼ -16.58 after sell → book $10,161.25; vs 09:30 mark -3.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 36 | $34.31 | $2.12 | $+5.14 | $3,852.08 | ▲ +5.14 after sell → book $10,159.14; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SG` | 193 | $6.61 | $2.61 | $+29.56 | $5,125.20 | ▲ +29.56 after sell → book $10,156.52; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BEKE` | 73 | $17.93 | $2.23 | $+60.89 | $6,432.22 | ▲ +60.89 after sell → book $10,154.29; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BABA` | 10 | $125.35 | $2.04 | $+14.74 | $7,683.68 | ▲ +14.74 after sell → book $10,152.25; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ROST` | 5 | $243.85 | $2.02 | $+67.47 | $8,900.91 | ▲ +67.47 after sell → book $10,150.23; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `SM` | 33 | $37.81 | $2.09 | — | $7,651.09 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+16.1; leftover $1271.56 | — |
| 2026-08-21 09:30 ET | **BUY** | `TALO` | 71 | $17.88 | $2.20 | — | $6,379.41 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+14.9; leftover $1271.56 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $5,130.89 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1271.56 | — |
| 2026-08-21 09:30 ET | **BUY** | `TRON` | 655 | $1.94 | $8.45 | — | $3,851.74 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,ohlc_hot; ret5=+15.4; leftover $1271.56 | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1471 | $0.86 | $17.12 | — | $2,563.67 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1271.56 | — |
| 2026-08-21 09:30 ET | **BUY** | `PDD` | 14 | $90.03 | $2.03 | — | $1,301.22 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight,overnight_mega; 🔵; ret5=+6.4; leftover $1271.56 | — |
| 2026-08-21 09:30 ET | **BUY** | `XPEV` | 103 | $12.29 | $2.30 | — | $33.05 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight; ret5=+1.9; leftover $1271.56 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.05 | ▲ close $10,170.37 vs 09:30 $10,167.22 (session +56.34) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.05 | ▲ 09:30 equity $10,177.76 vs yday $10,170.37 (+7.39) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 29 | $44.22 | $2.10 | $+42.81 | $1,313.34 | ▲ +42.81 after sell → book $10,175.67; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `SM` | 33 | $36.61 | $2.11 | $-43.80 | $2,519.36 | ▼ -43.80 after sell → book $10,173.56; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TALO` | 71 | $17.24 | $2.22 | $-49.87 | $3,741.17 | ▼ -49.87 after sell → book $10,171.33; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $5,045.24 | ▲ +55.55 after sell → book $10,169.32; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TRON` | 655 | $2.02 | $8.57 | $+35.38 | $6,359.77 | ▲ +35.38 after sell → book $10,160.75; vs 09:30 mark -8.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1471 | $0.89 | $17.76 | $+3.36 | $7,651.20 | ▲ +3.36 after sell → book $10,142.99; vs 09:30 mark -17.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PDD` | 14 | $90.95 | $2.05 | $+8.80 | $8,922.44 | ▲ +8.80 after sell → book $10,140.93; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XPEV` | 103 | $11.83 | $2.33 | $-52.01 | $10,138.61 | ▼ -52.01 after sell → book $10,138.61; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,138.61 | ▲ close $10,138.61 vs 09:30 $10,177.76 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,138.61 | ▲ 09:30 equity $10,138.61 vs yday $10,138.61 (-0.00) | — | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,138.61 | ▲ close $10,138.61 vs 09:30 $10,138.61 (session +0.00) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,138.61 | ▲ 09:30 equity $10,138.61 vs yday $10,138.61 (-0.00) | — | — |
| 2026-08-26 09:30 ET | **BUY** | `NVDA` | 5 | $212.64 | $2.00 | — | $9,073.40 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight,overnight_mega; 🔵; ret5=-3.0; leftover $1267.33 | — |
| 2026-08-26 09:30 ET | **BUY** | `P` | 12 | $103.16 | $2.03 | — | $7,833.46 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight; 🔵; ret5=-12.2; leftover $1267.33 | — |
| 2026-08-26 09:30 ET | **BUY** | `HPQ` | 43 | $29.42 | $2.12 | — | $6,566.28 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight; ret5=-1.5; leftover $1267.33 | — |
| 2026-08-26 09:30 ET | **BUY** | `LI` | 104 | $12.14 | $2.30 | — | $5,301.42 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; ret5=+1.2; leftover $1267.33 | — |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 90 | $14.00 | $2.26 | — | $4,039.16 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+17.8; leftover $1267.33 | — |
| 2026-08-26 09:30 ET | **BUY** | `NTNX` | 19 | $65.00 | $2.05 | — | $2,802.07 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight; ret5=+0.9; leftover $1267.33 | — |
| 2026-08-26 09:30 ET | **BUY** | `SFL` | 102 | $12.35 | $2.30 | — | $1,540.08 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; ret5=-1.7; leftover $1267.33 | — |
| 2026-08-26 09:30 ET | **BUY** | `AXTI` | 19 | $65.34 | $2.05 | — | $296.57 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1267.33 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $296.57 | ▼ close $10,069.38 vs 09:30 $10,138.61 (session -52.13) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $296.57 | ▲ 09:30 equity $10,267.89 vs yday $10,069.38 (+198.51) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `NVDA` | 5 | $222.86 | $2.02 | $+47.07 | $1,408.84 | ▲ +47.07 after sell → book $10,265.86; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `P` | 12 | $110.66 | $2.05 | $+85.93 | $2,734.72 | ▲ +85.93 after sell → book $10,263.82; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HPQ` | 43 | $27.86 | $2.14 | $-71.34 | $3,930.56 | ▼ -71.34 after sell → book $10,261.68; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `LI` | 104 | $12.35 | $2.33 | $+17.21 | $5,212.63 | ▲ +17.21 after sell → book $10,259.35; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MNRO` | 90 | $12.56 | $2.28 | $-134.14 | $6,340.74 | ▼ -134.14 after sell → book $10,257.06; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NTNX` | 19 | $71.24 | $2.07 | $+114.41 | $7,692.24 | ▲ +114.41 after sell → book $10,255.00; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SFL` | 102 | $12.03 | $2.32 | $-37.26 | $8,916.97 | ▼ -37.26 after sell → book $10,252.67; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AXTI` | 19 | $70.30 | $2.07 | $+90.13 | $10,250.60 | ▲ +90.13 after sell → book $10,250.60; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BE` | 5 | $227.10 | $2.00 | — | $9,113.10 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.6; leftover $1281.33 | — |
| 2026-08-27 09:30 ET | **BUY** | `DASH` | 5 | $235.94 | $2.00 | — | $7,931.39 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+7.6; leftover $1281.33 | — |
| 2026-08-27 09:30 ET | **BUY** | `AEO` | 74 | $17.27 | $2.21 | — | $6,651.20 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+5.5; leftover $1281.33 | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 15 | $80.60 | $2.04 | — | $5,440.17 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; ret5=-2.0; leftover $1281.33 | — |
| 2026-08-27 09:30 ET | **BUY** | `DKS` | 9 | $128.73 | $2.02 | — | $4,279.58 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_mover; ret5=-32.2; leftover $1281.33 | — |
| 2026-08-27 09:30 ET | **BUY** | `ULTA` | 2 | $536.07 | $2.00 | — | $3,205.44 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight; ret5=+2.9; leftover $1281.33 | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 30 | $41.44 | $2.08 | — | $1,960.16 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list flatten; ret5=+3.1; leftover $1281.33 | — |
| 2026-08-27 09:30 ET | **BUY** | `GAP` | 61 | $20.75 | $2.17 | — | $692.24 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot,overnight; ret5=+5.2; leftover $1281.33 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $692.24 | ▼ close $10,212.82 vs 09:30 $10,267.89 (session -21.26) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $692.24 | ▲ 09:30 equity $10,495.30 vs yday $10,212.82 (+282.48) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BE` | 5 | $215.71 | $2.02 | $-61.01 | $1,768.74 | ▼ -61.01 after sell → book $10,493.27; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DASH` | 5 | $233.37 | $2.02 | $-16.88 | $2,933.57 | ▼ -16.88 after sell → book $10,491.25; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEO` | 74 | $17.06 | $2.23 | $-19.99 | $4,193.77 | ▼ -19.99 after sell → book $10,489.01; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BBY` | 15 | $83.85 | $2.06 | $+44.66 | $5,449.47 | ▲ +44.66 after sell → book $10,486.96; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 9 | $132.80 | $2.04 | $+32.58 | $6,642.63 | ▲ +32.58 after sell → book $10,484.92; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ULTA` | 2 | $542.00 | $2.02 | $+7.85 | $7,724.61 | ▲ +7.85 after sell → book $10,482.90; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $6,554.04 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1287.44 | — |
| 2026-08-28 09:30 ET | **BUY** | `TGB` | 132 | $9.75 | $2.39 | — | $5,264.65 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+11.4; leftover $1287.44 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 67 | $19.00 | $2.19 | — | $3,989.46 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+7.5; leftover $1287.44 | — |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 34 | $37.49 | $2.09 | — | $2,712.71 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_mover; ret5=+5.4; leftover $1287.44 | — |
| 2026-08-28 09:30 ET | **BUY** | `ABAT` | 483 | $2.66 | $6.23 | — | $1,421.70 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1287.44 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 154 | $8.35 | $2.45 | — | $133.35 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; ret5=+5.1; leftover $1287.44 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.35 | ▼ close $10,222.79 vs 09:30 $10,495.30 (session -242.75) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.35 | ▼ 09:30 equity $10,167.82 vs yday $10,222.79 (-54.97) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 30 | $42.00 | $2.10 | $+12.62 | $1,391.25 | ▲ +12.62 after sell → book $10,165.72; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 61 | $22.98 | $2.19 | $+131.66 | $2,790.83 | ▲ +131.66 after sell → book $10,163.53; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.03 | $2.03 | $+11.63 | $3,973.04 | ▲ +11.63 after sell → book $10,161.50; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TGB` | 132 | $9.15 | $2.42 | $-84.00 | $5,178.42 | ▼ -84.00 after sell → book $10,159.08; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 67 | $18.12 | $2.21 | $-63.03 | $6,390.59 | ▼ -63.03 after sell → book $10,156.87; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 34 | $35.77 | $2.11 | $-62.68 | $7,604.65 | ▼ -62.68 after sell → book $10,154.75; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ABAT` | 483 | $2.56 | $6.32 | $-60.85 | $8,834.81 | ▼ -60.85 after sell → book $10,148.43; vs 09:30 mark -6.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 154 | $8.53 | $2.49 | $+22.78 | $10,145.94 | ▲ +22.78 after sell → book $10,145.94; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,145.94 | ▲ close $10,145.94 vs 09:30 $10,167.82 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,145.94 | ▲ 09:30 equity $10,145.94 vs yday $10,145.94 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,145.94 | ▲ close $10,145.94 vs 09:30 $10,145.94 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,145.94 | ▲ 09:30 equity $10,145.94 vs yday $10,145.94 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,145.94 | ▲ close $10,145.94 vs 09:30 $10,145.94 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,145.94 | ▲ 09:30 equity $10,145.94 vs yday $10,145.94 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `PBF` | 16 | $74.75 | $2.04 | — | $8,947.91 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list mover_buy; 🔵; ret5=+8.2; leftover $1268.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `PBR` | 59 | $21.18 | $2.17 | — | $7,696.12 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+17.5; leftover $1268.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `XP` | 61 | $20.74 | $2.17 | — | $6,428.81 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+12.2; leftover $1268.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `HP` | 26 | $47.74 | $2.07 | — | $5,185.50 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+15.1; leftover $1268.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `PBR-A` | 66 | $19.16 | $2.19 | — | $3,918.75 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+17.4; leftover $1268.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `VIST` | 16 | $77.14 | $2.04 | — | $2,682.47 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+12.2; leftover $1268.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `LULU` | 10 | $121.15 | $2.02 | — | $1,468.95 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight; 🔵; ret5=+3.2; leftover $1268.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 16 | $76.86 | $2.04 | — | $237.15 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list earn_react; 🔵; ret5=-6.6; leftover $1268.24 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $237.15 | ▼ close $9,859.02 vs 09:30 $10,145.94 (session -270.19) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $237.15 | ▼ 09:30 equity $9,537.97 vs yday $9,859.02 (-321.05) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `PBF` | 16 | $74.50 | $2.06 | $-8.10 | $1,427.10 | ▼ -8.10 after sell → book $9,535.92; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBR` | 59 | $20.25 | $2.19 | $-59.22 | $2,619.66 | ▼ -59.22 after sell → book $9,533.73; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `XP` | 61 | $19.67 | $2.19 | $-69.64 | $3,817.34 | ▼ -69.64 after sell → book $9,531.54; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HP` | 26 | $44.59 | $2.09 | $-86.06 | $4,974.59 | ▼ -86.06 after sell → book $9,529.45; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBR-A` | 66 | $18.36 | $2.21 | $-57.20 | $6,184.14 | ▼ -57.20 after sell → book $9,527.24; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VIST` | 16 | $73.97 | $2.06 | $-54.82 | $7,365.60 | ▼ -54.82 after sell → book $9,525.18; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `LULU` | 10 | $98.15 | $2.04 | $-234.06 | $8,345.06 | ▼ -234.06 after sell → book $9,523.14; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSXY` | 16 | $73.63 | $2.06 | $-55.78 | $9,521.08 | ▼ -55.78 after sell → book $9,521.08; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 5 | $236.82 | $2.00 | — | $8,334.98 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+8.1; leftover $1190.14 | — |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 133 | $8.94 | $2.39 | — | $7,143.57 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+7.7; leftover $1190.14 | — |
| 2026-09-04 09:30 ET | **BUY** | `MIR` | 71 | $16.60 | $2.20 | — | $5,962.77 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+12.9; leftover $1190.14 | — |
| 2026-09-04 09:30 ET | **BUY** | `GORO` | 301 | $3.95 | $3.88 | — | $4,769.93 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+6.9; leftover $1190.14 | — |
| 2026-09-04 09:30 ET | **BUY** | `GSM` | 254 | $4.67 | $3.28 | — | $3,580.48 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; ret5=+11.9; leftover $1190.14 | — |
| 2026-09-04 09:30 ET | **BUY** | `WNC` | 83 | $14.17 | $2.24 | — | $2,402.13 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; 🔵; ret5=+7.9; leftover $1190.14 | — |
| 2026-09-04 09:30 ET | **BUY** | `XRX` | 359 | $3.31 | $4.63 | — | $1,209.21 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+11.1; leftover $1190.14 | — |
| 2026-09-04 09:30 ET | **BUY** | `ABM` | 25 | $46.79 | $2.06 | — | $37.39 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list overnight; ret5=+0.2; leftover $1190.14 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.39 | ▲ close $9,721.22 vs 09:30 $9,537.97 (session +222.83) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.39 | ▲ 09:30 equity $9,791.53 vs yday $9,721.22 (+70.31) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 5 | $267.76 | $2.03 | $+150.67 | $1,374.17 | ▲ +150.67 after sell → book $9,789.51; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HAFN` | 133 | $8.81 | $2.42 | $-22.10 | $2,543.47 | ▼ -22.10 after sell → book $9,787.08; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MIR` | 71 | $17.07 | $2.22 | $+28.94 | $3,753.22 | ▲ +28.94 after sell → book $9,784.86; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GORO` | 301 | $4.13 | $3.94 | $+46.35 | $4,992.41 | ▲ +46.35 after sell → book $9,780.92; vs 09:30 mark -3.94 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GSM` | 254 | $4.75 | $3.33 | $+13.71 | $6,195.58 | ▲ +13.71 after sell → book $9,777.59; vs 09:30 mark -3.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `WNC` | 83 | $14.22 | $2.26 | $-0.35 | $7,373.58 | ▼ -0.35 after sell → book $9,775.33; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `XRX` | 359 | $3.50 | $4.70 | $+58.88 | $8,625.37 | ▲ +58.88 after sell → book $9,770.62; vs 09:30 mark -4.71 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ABM` | 25 | $45.81 | $2.08 | $-28.65 | $9,768.54 | ▼ -28.65 after sell → book $9,768.54; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,768.54 | ▲ close $9,768.54 vs 09:30 $9,791.53 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,768.54 | ▲ 09:30 equity $9,768.54 vs yday $9,768.54 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,768.54 | ▲ close $9,768.54 vs 09:30 $9,768.54 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,768.54 | ▲ 09:30 equity $9,768.54 vs yday $9,768.54 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,768.54 | ▲ close $9,768.54 vs 09:30 $9,768.54 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,768.54 | ▲ 09:30 equity $9,768.54 vs yday $9,768.54 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 22 | $54.91 | $2.06 | — | $8,558.46 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; 🔵; ret5=+24.3; leftover $1221.07 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 248 | $4.91 | $3.20 | — | $7,337.58 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1221.07 | — |
| 2026-09-11 09:30 ET | **BUY** | `PBR` | 57 | $21.21 | $2.16 | — | $6,126.45 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+2.5; leftover $1221.07 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 15 | $77.33 | $2.04 | — | $4,964.47 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer; ret5=+2.5; leftover $1221.07 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 575 | $2.12 | $7.42 | — | $3,738.05 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1221.07 | — |
| 2026-09-11 09:30 ET | **BUY** | `BKV` | 48 | $24.97 | $2.13 | — | $2,537.36 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+10.8; leftover $1221.07 | — |
| 2026-09-11 09:30 ET | **BUY** | `SSL` | 85 | $14.35 | $2.25 | — | $1,315.36 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+15.5; leftover $1221.07 | — |
| 2026-09-11 09:30 ET | **BUY** | `VLO` | 3 | $388.00 | $2.00 | — | $149.36 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+6.7; leftover $1221.07 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $149.36 | ▼ close $9,680.58 vs 09:30 $9,768.54 (session -64.71) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.36 | ▲ 09:30 equity $9,745.57 vs yday $9,680.58 (+64.99) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 22 | $54.75 | $2.08 | $-7.65 | $1,351.79 | ▼ -7.65 after sell → book $9,743.50; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BNC` | 248 | $5.03 | $3.25 | $+23.31 | $2,595.98 | ▲ +23.31 after sell → book $9,740.25; vs 09:30 mark -3.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PBR` | 57 | $21.23 | $2.18 | $-3.20 | $3,803.91 | ▼ -3.20 after sell → book $9,738.07; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIST` | 15 | $77.10 | $2.06 | $-7.54 | $4,958.35 | ▼ -7.54 after sell → book $9,736.01; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 575 | $2.05 | $7.52 | $-55.19 | $6,129.58 | ▼ -55.19 after sell → book $9,728.49; vs 09:30 mark -7.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BKV` | 48 | $24.26 | $2.15 | $-38.37 | $7,291.90 | ▼ -38.37 after sell → book $9,726.33; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SSL` | 85 | $14.69 | $2.27 | $+24.39 | $8,538.28 | ▲ +24.39 after sell → book $9,724.06; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `VLO` | 3 | $395.26 | $2.02 | $+17.76 | $9,722.05 | ▲ +17.76 after sell → book $9,722.05; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,722.05 | ▲ close $9,722.05 vs 09:30 $9,745.57 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,722.05 | ▲ 09:30 equity $9,722.05 vs yday $9,722.05 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,722.05 | ▲ close $9,722.05 vs 09:30 $9,722.05 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,722.05 | ▲ 09:30 equity $9,722.05 vs yday $9,722.05 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 30 | $39.99 | $2.08 | — | $8,520.27 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $1215.26 | — |
| 2026-09-16 09:30 ET | **BUY** | `TALO` | 68 | $17.87 | $2.19 | — | $7,302.91 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+6.8; leftover $1215.26 | — |
| 2026-09-16 09:30 ET | **BUY** | `APA` | 26 | $46.44 | $2.07 | — | $6,093.40 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+8.9; leftover $1215.26 | — |
| 2026-09-16 09:30 ET | **BUY** | `CVI` | 23 | $51.05 | $2.06 | — | $4,917.19 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; ret5=+13.1; leftover $1215.26 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 207 | $5.87 | $2.67 | — | $3,699.43 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1215.26 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 13 | $87.40 | $2.03 | — | $2,561.21 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1215.26 | — |
| 2026-09-16 09:30 ET | **BUY** | `VLO` | 3 | $391.68 | $2.00 | — | $1,384.17 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+6.7; leftover $1215.26 | — |
| 2026-09-16 09:30 ET | **BUY** | `FRO` | 23 | $52.52 | $2.06 | — | $174.15 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+10.7; leftover $1215.26 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.15 | ▼ close $9,551.99 vs 09:30 $9,722.05 (session -152.90) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.15 | ▼ 09:30 equity $9,504.93 vs yday $9,551.99 (-47.06) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 30 | $37.57 | $2.10 | $-76.78 | $1,299.15 | ▼ -76.78 after sell → book $9,502.83; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TALO` | 68 | $17.19 | $2.22 | $-50.65 | $2,465.85 | ▼ -50.65 after sell → book $9,500.61; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `APA` | 26 | $44.63 | $2.09 | $-51.22 | $3,624.14 | ▼ -51.22 after sell → book $9,498.52; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CVI` | 23 | $51.88 | $2.08 | $+14.95 | $4,815.31 | ▲ +14.95 after sell → book $9,496.45; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 207 | $5.58 | $2.71 | $-65.41 | $5,967.65 | ▼ -65.41 after sell → book $9,493.73; vs 09:30 mark -2.72 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 13 | $83.20 | $2.05 | $-58.68 | $7,047.20 | ▼ -58.68 after sell → book $9,491.68; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VLO` | 3 | $398.45 | $2.02 | $+16.29 | $8,240.53 | ▲ +16.29 after sell → book $9,489.66; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `FRO` | 23 | $54.31 | $2.08 | $+37.03 | $9,487.58 | ▲ +37.03 after sell → book $9,487.58; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SFL` | 87 | $13.55 | $2.25 | — | $8,306.48 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+7.8; leftover $1185.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `FTAI` | 6 | $196.50 | $2.01 | — | $7,125.47 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; 🔵; ret5=+2.5; leftover $1185.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 32 | $36.76 | $2.09 | — | $5,947.07 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; leftover $1185.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `EROC` | 93 | $12.64 | $2.27 | — | $4,769.28 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer; ret5=-3.6; leftover $1185.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `BKV` | 52 | $22.75 | $2.15 | — | $3,584.13 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+10.8; leftover $1185.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `TK` | 82 | $14.41 | $2.24 | — | $2,400.28 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+6.8; leftover $1185.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `KGS` | 20 | $58.91 | $2.05 | — | $1,220.03 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+9.1; leftover $1185.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `PUMP` | 115 | $10.31 | $2.33 | — | $32.04 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+5.9; leftover $1185.95 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.04 | ▲ close $9,572.85 vs 09:30 $9,504.93 (session +102.65) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.04 | ▲ 09:30 equity $9,635.56 vs yday $9,572.85 (+62.71) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `SFL` | 87 | $13.74 | $2.28 | $+12.00 | $1,225.15 | ▲ +12.00 after sell → book $9,633.29; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FTAI` | 6 | $195.55 | $2.03 | $-9.74 | $2,396.42 | ▼ -9.74 after sell → book $9,631.26; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 32 | $39.50 | $2.11 | $+83.49 | $3,658.31 | ▲ +83.49 after sell → book $9,629.15; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `EROC` | 93 | $13.00 | $2.29 | $+28.92 | $4,865.02 | ▲ +28.92 after sell → book $9,626.86; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BKV` | 52 | $22.92 | $2.17 | $+4.53 | $6,054.69 | ▲ +4.53 after sell → book $9,624.69; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TK` | 82 | $14.60 | $2.26 | $+11.08 | $7,249.63 | ▲ +11.08 after sell → book $9,622.43; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `KGS` | 20 | $58.38 | $2.07 | $-14.72 | $8,415.16 | ▼ -14.72 after sell → book $9,620.36; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PUMP` | 115 | $10.48 | $2.36 | $+14.85 | $9,618.00 | ▲ +14.85 after sell → book $9,618.00; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `PURR` | 173 | $13.82 | $2.51 | — | $7,224.63 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; ret5=+16.5; leftover $2404.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `ARE` | 42 | $56.70 | $2.12 | — | $4,841.11 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+13.7; leftover $2404.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `USDE` | 252 | $9.54 | $3.25 | — | $2,433.78 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover; ret5=+15.8; leftover $2404.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 319 | $7.54 | $4.12 | — | $26.00 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_mover; 🔵; ret5=-20.9; leftover $2404.50 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.00 | ▼ close $9,605.13 vs 09:30 $9,635.56 (session -0.87) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.00 | ▲ 09:30 equity $10,439.27 vs yday $9,605.13 (+834.14) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `PURR` | 173 | $14.65 | $2.56 | $+138.52 | $2,557.89 | ▲ +138.52 after sell → book $10,436.71; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARE` | 42 | $53.39 | $2.14 | $-143.28 | $4,798.13 | ▼ -143.28 after sell → book $10,434.57; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `USDE` | 252 | $13.05 | $3.32 | $+877.95 | $8,083.41 | ▲ +877.95 after sell → book $10,431.25; vs 09:30 mark -3.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 319 | $7.36 | $4.19 | $-64.13 | $10,427.07 | ▼ -64.13 after sell → book $10,427.07; vs 09:30 mark -4.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 5 | $230.25 | $2.00 | — | $9,273.81 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+12.5; leftover $1303.38 | — |
| 2026-09-21 09:30 ET | **BUY** | `COHR` | 3 | $326.48 | $2.00 | — | $8,292.37 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+3.9; leftover $1303.38 | — |
| 2026-09-21 09:30 ET | **BUY** | `FORM` | 10 | $123.00 | $2.02 | — | $7,060.35 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+3.0; leftover $1303.38 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 6 | $190.30 | $2.01 | — | $5,916.54 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+10.6; leftover $1303.38 | — |
| 2026-09-21 09:30 ET | **BUY** | `UMC` | 52 | $24.93 | $2.15 | — | $4,618.04 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+8.7; leftover $1303.38 | — |
| 2026-09-21 09:30 ET | **BUY** | `ABTC` | 121 | $10.71 | $2.35 | — | $3,319.77 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.4; leftover $1303.38 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMD` | 2 | $583.88 | $2.00 | — | $2,150.02 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+8.5; leftover $1303.38 | — |
| 2026-09-21 09:30 ET | **BUY** | `ARM` | 4 | $294.36 | $2.00 | — | $970.58 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+4.1; leftover $1303.38 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $970.58 | ▼ close $10,403.26 vs 09:30 $10,439.27 (session -7.28) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $970.58 | ▼ 09:30 equity $10,328.87 vs yday $10,403.26 (-74.39) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `COHR` | 3 | $310.29 | $2.02 | $-52.59 | $1,899.43 | ▼ -52.59 after sell → book $10,326.85; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `UMC` | 52 | $25.26 | $2.17 | $+12.85 | $3,210.78 | ▲ +12.85 after sell → book $10,324.68; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `AMD` | 2 | $606.57 | $2.02 | $+41.37 | $4,421.90 | ▲ +41.37 after sell → book $10,322.66; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 1089 | $0.58 | $9.58 | — | $3,780.70 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $631.70 | — |
| 2026-09-22 09:30 ET | **BUY** | `FSLY` | 22 | $28.02 | $2.06 | — | $3,162.21 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; leftover $631.70 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,162.21 | ▼ close $10,269.06 vs 09:30 $10,328.87 (session -41.97) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,162.21 | ▲ 09:30 equity $10,542.01 vs yday $10,269.06 (+272.95) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 5 | $266.50 | $2.03 | $+177.22 | $4,492.68 | ▲ +177.22 after sell → book $10,539.98; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FORM` | 10 | $125.39 | $2.04 | $+19.84 | $5,744.54 | ▲ +19.84 after sell → book $10,537.94; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 6 | $174.50 | $2.03 | $-98.84 | $6,789.51 | ▼ -98.84 after sell → book $10,535.92; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ABTC` | 121 | $10.11 | $2.38 | $-77.34 | $8,010.44 | ▼ -77.34 after sell → book $10,533.53; vs 09:30 mark -2.39 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARM` | 4 | $331.78 | $2.02 | $+145.66 | $9,335.54 | ▲ +145.66 after sell → book $10,531.51; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DEFT` | 1089 | $0.57 | $9.72 | $-24.75 | $9,951.99 | ▼ -24.75 after sell → book $10,521.79; vs 09:30 mark -9.72 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FSLY` | 22 | $25.90 | $2.08 | $-50.77 | $10,519.72 | ▼ -50.77 after sell → book $10,519.72; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `OPRT` | 319 | $8.23 | $4.12 | — | $7,890.23 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+8.7; leftover $2629.93 | — |
| 2026-09-23 09:30 ET | **BUY** | `EU` | 2191 | $1.20 | $28.26 | — | $5,232.77 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+12.9; leftover $2629.93 | — |
| 2026-09-23 09:30 ET | **BUY** | `BKV` | 112 | $23.29 | $2.33 | — | $2,621.96 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+10.8; leftover $2629.93 | — |
| 2026-09-23 09:30 ET | **BUY** | `BTGO` | 322 | $8.11 | $4.15 | — | $6.39 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+14.2; leftover $2629.93 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.39 | ▲ close $10,598.97 vs 09:30 $10,542.01 (session +118.11) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.39 | ▼ 09:30 equity $10,495.12 vs yday $10,598.97 (-103.85) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `EU` | 2191 | $1.20 | $28.65 | $-56.91 | $2,606.94 | ▼ -56.91 after sell → book $10,466.47; vs 09:30 mark -28.65 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKV` | 112 | $23.28 | $2.37 | $-5.81 | $5,211.93 | ▼ -5.81 after sell → book $10,464.10; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTGO` | 322 | $7.92 | $4.23 | $-69.56 | $7,757.95 | ▼ -69.56 after sell → book $10,459.88; vs 09:30 mark -4.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,757.95 | ▼ close $10,405.65 vs 09:30 $10,495.12 (session -54.23) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,450.87 | ▲ 09:30 equity $9,450.87 vs yday $9,450.87 (+0.00) | 09:30 open · cash $9,450.87 · no holdings · equity $9,450.87 vs prior close $9,450.87 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `CBRL` | 60 | $52.39 | $2.17 | — | $6,305.30 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer; 🔵; ret5=+18.5; leftover $3150.29 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GLND` | 519 | $6.06 | $6.70 | — | $3,153.46 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list yday_gainer,yday_mover; ret5=+342.1; leftover $3150.29 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DGXX` | 657 | $4.78 | $8.48 | — | $4.53 | — | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; 🔵; ret5=+18.0; leftover $3150.29 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.53 | ▼ close $9,004.02 vs 09:30 $9,450.87 (session -429.51) | 16:00 close · cash $4.53 · equity $9,004.02 vs 09:30 $9,450.87 (-446.85; session marks -429.51) · 3 name(s) marked open→close (per-name table). CBRL×60 09:30 $52.39 → close $51.81 -34.80; GLND×519 09:30 $6.06 → close $5.54 -269.88; DGXX×657 09:30 $4.78 → close $4.59 -124.83 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `MU` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AMKR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-24 | `BZ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `VIPS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DK` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `UEC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TUYA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `MMED` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MDT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SSL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FNKO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TII` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RCKT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PROK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `BMRN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `LENZ` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RANI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `XRX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AGCO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `CF` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FLR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FMC` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `XP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FIVE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PVH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `UGP` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OBE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `WDS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HELP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SID` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `EQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AKBA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SANM` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `QRVO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `WCC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `VLO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `FRO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `KGS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PUMP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `BKV` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FORM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ABTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-22 | `FIVN` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-22 | `AKAM` | no_price | no 09:30 open |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AKBA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NEWP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ASPN` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SWRD` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `OPRT` | 319 | 2026-09-23 @ $8.23 | Clock-B #6 stock holds while sector camera is red; gate clk_hold_vs_sector=True; rank cond; list ohlc_hot; ret5=+8.7; leftover $2629.93 |
