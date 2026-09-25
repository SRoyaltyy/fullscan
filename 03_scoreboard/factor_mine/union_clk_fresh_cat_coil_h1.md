# Factor mine action — `union_clk_fresh_cat_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · Clock-B #2 fresh catalyst + limited extension (research; not KEEP)

Cash book **-2.92%** ($9,709) · signal-only (no cash/fees) was -4.91%. Starts YES **13/30**. Fills 240 · skips 106 · realized $-137.13.

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
- Must-have: Clock-B #2: a fresh good catalyst (catal / EPS beat / packet or headline green) and the prior tape is not already exploded.
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
- **Gate** `clk_fresh_cat_coil=True` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,862.87.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 167 | $59.80 | $2.49 | — | $10.91 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; ⚪; ret5=-5.3; leftover $10000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.91 | ▲ close $10,069.32 vs 09:30 $10,000.00 (session +71.81) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.91 | ▼ 09:30 equity $9,972.46 vs yday $10,069.32 (-96.86) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 167 | $59.65 | $2.60 | $-30.14 | $9,969.86 | ▼ -30.14 after sell → book $9,969.86; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $8,730.15 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1246.23 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 289 | $4.31 | $3.73 | — | $7,480.83 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1246.23 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $6,235.39 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1246.23 | — |
| 2026-08-14 09:30 ET | **BUY** | `KULR` | 498 | $2.50 | $6.42 | — | $4,983.96 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; 🔵; ⚪; ret5=+7.6; leftover $1246.23 | — |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $3,974.97 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable; 🔵; ⚪; ret5=+7.9; leftover $1246.23 | — |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 28 | $44.06 | $2.07 | — | $2,739.21 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable; 🔵; ret5=+3.9; leftover $1246.23 | — |
| 2026-08-14 09:30 ET | **BUY** | `AMAT` | 2 | $499.40 | $2.00 | — | $1,738.42 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; 🔵; ret5=+1.3; leftover $1246.23 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $536.40 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ret5=+0.6; leftover $1246.23 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $536.40 | ▲ close $10,017.40 vs 09:30 $9,972.46 (session +70.23) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $536.40 | ▲ 09:30 equity $10,132.48 vs yday $10,017.40 (+115.08) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $1,713.91 | ▼ -62.20 after sell → book $10,130.24; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 289 | $4.60 | $3.79 | $+76.30 | $3,039.52 | ▲ +76.30 after sell → book $10,126.45; vs 09:30 mark -3.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 84 | $13.67 | $2.27 | $-99.43 | $4,185.54 | ▼ -99.43 after sell → book $10,124.19; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `KULR` | 498 | $2.63 | $6.52 | $+51.80 | $5,488.76 | ▲ +51.80 after sell → book $10,117.67; vs 09:30 mark -6.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $6,537.80 | ▲ +40.05 after sell → book $10,115.66; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ALGM` | 28 | $45.32 | $2.09 | $+31.11 | $7,804.67 | ▲ +31.11 after sell → book $10,113.56; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AMAT` | 2 | $517.45 | $2.02 | $+32.08 | $8,837.55 | ▲ +32.08 after sell → book $10,111.55; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $10,109.50 | ▲ +69.94 after sell → book $10,109.50; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,860.57 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ret5=+6.7; leftover $1263.69 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,716.40 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ret5=+5.8; leftover $1263.69 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,498.19 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ret5=+8.3; leftover $1263.69 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 138 | $9.12 | $2.40 | — | $5,237.23 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1263.69 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 78 | $16.20 | $2.22 | — | $3,971.40 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1263.69 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 390 | $3.24 | $5.03 | — | $2,702.77 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; ⚪; ret5=+0.3; leftover $1263.69 | — |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 13 | $90.54 | $2.03 | — | $1,523.72 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; ret5=-7.2; leftover $1263.69 | — |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 249 | $5.07 | $3.21 | — | $258.08 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; ret5=-4.7; leftover $1263.69 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $258.08 | ▲ close $10,143.42 vs 09:30 $10,132.48 (session +54.91) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $258.08 | ▼ 09:30 equity $10,056.92 vs yday $10,143.42 (-86.50) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,551.99 | ▲ +44.98 after sell → book $10,054.83; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,734.28 | ▲ +38.11 after sell → book $10,052.80; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,985.83 | ▲ +33.34 after sell → book $10,050.77; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 138 | $9.03 | $2.44 | $-17.26 | $5,229.53 | ▼ -17.26 after sell → book $10,048.33; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 78 | $15.78 | $2.25 | $-37.23 | $6,458.12 | ▼ -37.23 after sell → book $10,046.08; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 13 | $93.44 | $2.05 | $+33.62 | $7,670.80 | ▲ +33.62 after sell → book $10,044.04; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NB` | 249 | $4.66 | $3.26 | $-108.57 | $8,827.87 | ▼ -108.57 after sell → book $10,040.77; vs 09:30 mark -3.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,827.87 | ▲ close $10,056.37 vs 09:30 $10,056.92 (session +15.60) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,827.87 | ▲ 09:30 equity $10,071.97 vs yday $10,056.37 (+15.60) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `DNN` | 390 | $3.19 | $5.11 | $-29.64 | $10,066.87 | ▼ -29.64 after sell → book $10,066.87; vs 09:30 mark -5.10 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,066.87 | ▲ close $10,066.87 vs 09:30 $10,071.97 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,066.87 | ▲ 09:30 equity $10,066.87 vs yday $10,066.87 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,881.71 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1258.36 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $7,635.13 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1258.36 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 169 | $7.44 | $2.50 | — | $6,375.27 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1258.36 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRCL` | 15 | $82.99 | $2.04 | — | $5,128.39 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable; 🔵; ⚪; ret5=+7.4; leftover $1258.36 | — |
| 2026-08-20 09:30 ET | **BUY** | `FUTU` | 10 | $117.65 | $2.02 | — | $3,949.87 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; 🔵; ret5=+4.1; leftover $1258.36 | — |
| 2026-08-20 09:30 ET | **BUY** | `IOND` | 19 | $65.60 | $2.05 | — | $2,701.42 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; 🔵; ⚪; ret5=+3.7; leftover $1258.36 | — |
| 2026-08-20 09:30 ET | **BUY** | `RERE` | 299 | $4.20 | $3.86 | — | $1,441.77 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; 🔵; ret5=+2.9; leftover $1258.36 | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 168 | $7.45 | $2.49 | — | $187.67 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1258.36 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $187.67 | ▲ close $10,203.96 vs 09:30 $10,066.87 (session +156.19) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $187.67 | ▲ 09:30 equity $10,391.73 vs yday $10,203.96 (+187.77) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,429.98 | ▲ +57.15 after sell → book $10,389.68; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $2,778.99 | ▲ +102.43 after sell → book $10,387.55; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRVI` | 169 | $8.28 | $2.54 | $+136.93 | $4,175.77 | ▲ +136.93 after sell → book $10,385.01; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CRCL` | 15 | $87.98 | $2.06 | $+70.76 | $5,493.41 | ▲ +70.76 after sell → book $10,382.95; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `FUTU` | 10 | $115.18 | $2.04 | $-28.76 | $6,643.17 | ▼ -28.76 after sell → book $10,380.91; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IOND` | 19 | $68.41 | $2.07 | $+49.28 | $7,940.90 | ▲ +49.28 after sell → book $10,378.85; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `RERE` | 299 | $4.17 | $3.92 | $-16.74 | $9,183.81 | ▼ -16.74 after sell → book $10,374.93; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 168 | $7.09 | $2.53 | $-65.51 | $10,372.40 | ▼ -65.51 after sell → book $10,372.40; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 24 | $59.72 | $2.06 | — | $8,937.06 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1481.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 609 | $2.43 | $7.86 | — | $7,449.33 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $1481.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 82 | $17.93 | $2.24 | — | $5,976.42 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $1481.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 99 | $14.96 | $2.29 | — | $4,493.10 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer; ret5=-1.6; leftover $1481.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 644 | $2.30 | $8.31 | — | $3,003.59 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; ret5=-3.0; leftover $1481.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `WOLF` | 55 | $26.86 | $2.15 | — | $1,524.13 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list yday_mover; 🔵; ret5=-16.4; leftover $1481.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 34 | $43.08 | $2.09 | — | $57.32 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; ret5=-4.9; leftover $1481.77 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.32 | ▼ close $10,298.99 vs 09:30 $10,391.73 (session -46.41) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.32 | ▼ 09:30 equity $10,284.58 vs yday $10,298.99 (-14.41) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 24 | $58.75 | $2.08 | $-27.43 | $1,465.24 | ▼ -27.43 after sell → book $10,282.50; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HITI` | 609 | $2.45 | $7.97 | $-3.65 | $2,949.32 | ▼ -3.65 after sell → book $10,274.53; vs 09:30 mark -7.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 82 | $18.05 | $2.26 | $+5.34 | $4,427.57 | ▲ +5.34 after sell → book $10,272.27; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 99 | $14.74 | $2.32 | $-26.38 | $5,884.51 | ▼ -26.38 after sell → book $10,269.95; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 644 | $2.34 | $8.43 | $+9.03 | $7,383.05 | ▲ +9.03 after sell → book $10,261.53; vs 09:30 mark -8.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WOLF` | 55 | $25.00 | $2.18 | $-106.63 | $8,755.87 | ▼ -106.63 after sell → book $10,259.35; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 34 | $44.22 | $2.11 | $+34.55 | $10,257.24 | ▲ +34.55 after sell → book $10,257.24; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,257.24 | ▲ close $10,257.24 vs 09:30 $10,284.58 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,257.24 | ▲ 09:30 equity $10,257.24 vs yday $10,257.24 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 29 | $43.76 | $2.08 | — | $8,986.12 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $1282.15 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 20 | $61.19 | $2.05 | — | $7,760.27 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ret5=+7.4; leftover $1282.15 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 116 | $10.98 | $2.34 | — | $6,484.25 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ret5=+1.2; leftover $1282.15 | — |
| 2026-08-25 09:30 ET | **BUY** | `AMTX` | 674 | $1.90 | $8.69 | — | $5,194.96 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list yday_mover; ⚪; ret5=+5.0; leftover $1282.15 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 83 | $15.28 | $2.24 | — | $3,924.48 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; 🔵; ⚪; ret5=-0.7; leftover $1282.15 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 153 | $8.35 | $2.45 | — | $2,644.48 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1282.15 | — |
| 2026-08-25 09:30 ET | **BUY** | `ELMT` | 71 | $17.89 | $2.20 | — | $1,372.09 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list yday_mover; ⚪; ret5=-7.5; leftover $1282.15 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 14 | $88.94 | $2.03 | — | $124.89 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; 🔵; ret5=-4.9; leftover $1282.15 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.89 | ▲ close $10,423.21 vs 09:30 $10,257.24 (session +190.06) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.89 | ▼ 09:30 equity $10,420.87 vs yday $10,423.21 (-2.34) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 29 | $44.33 | $2.10 | $+12.36 | $1,408.37 | ▲ +12.36 after sell → book $10,418.78; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `INSP` | 20 | $60.07 | $2.07 | $-26.52 | $2,607.70 | ▼ -26.52 after sell → book $10,416.71; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AMTX` | 674 | $1.91 | $8.82 | $-10.77 | $3,886.22 | ▼ -10.77 after sell → book $10,407.89; vs 09:30 mark -8.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BZ` | 83 | $16.77 | $2.26 | $+119.17 | $5,275.87 | ▲ +119.17 after sell → book $10,405.63; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ELMT` | 71 | $17.82 | $2.22 | $-9.40 | $6,538.86 | ▼ -9.40 after sell → book $10,403.40; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BNS` | 14 | $92.65 | $2.05 | $+47.86 | $7,833.91 | ▲ +47.86 after sell → book $10,401.35; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 11 | $118.50 | $2.02 | — | $6,528.39 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $1305.65 | — |
| 2026-08-26 09:30 ET | **BUY** | `GSM` | 326 | $4.00 | $4.21 | — | $5,220.18 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list yday_mover; 🔵; ret5=-3.6; leftover $1305.65 | — |
| 2026-08-26 09:30 ET | **BUY** | `HEI` | 3 | $370.00 | $2.00 | — | $4,108.18 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; ret5=-4.6; leftover $1305.65 | — |
| 2026-08-26 09:30 ET | **BUY** | `DLTR` | 9 | $133.93 | $2.02 | — | $2,900.79 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list overnight; 🔵; ret5=+3.1; leftover $1305.65 | — |
| 2026-08-26 09:30 ET | **BUY** | `NVDA` | 6 | $212.64 | $2.01 | — | $1,622.95 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list overnight,overnight_mega; 🔵; ret5=-3.0; leftover $1305.65 | — |
| 2026-08-26 09:30 ET | **BUY** | `P` | 12 | $103.16 | $2.03 | — | $383.00 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list overnight; 🔵; ret5=-12.2; leftover $1305.65 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $383.00 | ▼ close $10,335.84 vs 09:30 $10,420.87 (session -51.23) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $383.00 | ▼ 09:30 equity $10,315.69 vs yday $10,335.84 (-20.15) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 116 | $10.63 | $2.37 | $-45.31 | $1,613.71 | ▼ -45.31 after sell → book $10,313.32; vs 09:30 mark -2.37 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 153 | $8.49 | $2.48 | $+16.49 | $2,910.20 | ▲ +16.49 after sell → book $10,310.84; vs 09:30 mark -2.48 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GSM` | 326 | $4.02 | $4.27 | $-1.96 | $4,216.45 | ▼ -1.96 after sell → book $10,306.57; vs 09:30 mark -4.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HEI` | 3 | $346.19 | $2.02 | $-75.45 | $5,253.00 | ▼ -75.45 after sell → book $10,304.55; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DLTR` | 9 | $120.00 | $2.04 | $-129.42 | $6,330.96 | ▼ -129.42 after sell → book $10,302.51; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NVDA` | 6 | $222.86 | $2.03 | $+57.28 | $7,666.09 | ▲ +57.28 after sell → book $10,300.48; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `P` | 12 | $110.66 | $2.05 | $+85.93 | $8,991.97 | ▲ +85.93 after sell → book $10,298.44; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 15 | $81.65 | $2.04 | — | $7,765.18 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1284.57 | — |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 17 | $74.54 | $2.04 | — | $6,495.96 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ret5=-0.1; leftover $1284.57 | — |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $5,526.96 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1284.57 | — |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 23 | $55.25 | $2.06 | — | $4,254.15 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ret5=+2.1; leftover $1284.57 | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 53 | $24.00 | $2.15 | — | $2,980.00 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; ret5=+8.7; leftover $1284.57 | — |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $205.90 | $2.01 | — | $1,742.59 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ret5=+8.5; leftover $1284.57 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,742.59 | ▼ close $10,167.19 vs 09:30 $10,315.69 (session -118.96) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,742.59 | ▼ 09:30 equity $10,161.48 vs yday $10,167.19 (-5.71) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 11 | $115.66 | $2.04 | $-35.31 | $3,012.81 | ▼ -35.31 after sell → book $10,159.44; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 15 | $79.27 | $2.06 | $-39.79 | $4,199.80 | ▼ -39.79 after sell → book $10,157.38; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 17 | $75.39 | $2.06 | $+10.35 | $5,479.37 | ▲ +10.35 after sell → book $10,155.32; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $6,396.65 | ▼ -51.73 after sell → book $10,153.31; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 23 | $55.97 | $2.08 | $+12.42 | $7,681.88 | ▲ +12.42 after sell → book $10,151.23; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 53 | $23.95 | $2.17 | $-6.97 | $8,949.06 | ▼ -6.97 after sell → book $10,149.06; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $200.00 | $2.03 | $-39.44 | $10,147.03 | ▼ -39.44 after sell → book $10,147.03; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $9,171.80 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1268.38 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $7,968.54 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1268.38 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 13 | $91.49 | $2.03 | — | $6,777.15 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1268.38 | — |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 20 | $62.82 | $2.05 | — | $5,518.70 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1268.38 | — |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $4,358.93 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1268.38 | — |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 10 | $119.76 | $2.02 | — | $3,159.31 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1268.38 | — |
| 2026-08-28 09:30 ET | **BUY** | `MTSI` | 4 | $275.20 | $2.00 | — | $2,056.51 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+4.1; leftover $1268.38 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,056.51 | ▼ close $9,835.01 vs 09:30 $10,161.48 (session -297.92) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,056.51 | ▲ 09:30 equity $9,875.01 vs yday $9,835.01 (+40.00) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 3 | $322.49 | $2.02 | $-9.78 | $3,021.96 | ▼ -9.78 after sell → book $9,872.99; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $4,155.26 | ▼ -69.96 after sell → book $9,870.97; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 13 | $89.39 | $2.05 | $-31.38 | $5,315.28 | ▼ -31.38 after sell → book $9,868.92; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 20 | $60.46 | $2.07 | $-51.32 | $6,522.41 | ▼ -51.32 after sell → book $9,866.85; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $280.25 | $2.02 | $-40.78 | $7,641.39 | ▼ -40.78 after sell → book $9,864.83; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 10 | $115.56 | $2.04 | $-46.06 | $8,794.95 | ▼ -46.06 after sell → book $9,862.79; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MTSI` | 4 | $266.96 | $2.02 | $-36.98 | $9,860.77 | ▼ -36.98 after sell → book $9,860.77; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,860.77 | ▲ close $9,860.77 vs 09:30 $9,875.01 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,860.77 | ▲ 09:30 equity $9,860.77 vs yday $9,860.77 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,860.77 | ▲ close $9,860.77 vs 09:30 $9,860.77 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,860.77 | ▲ 09:30 equity $9,860.77 vs yday $9,860.77 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,860.77 | ▲ close $9,860.77 vs 09:30 $9,860.77 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,860.77 | ▲ 09:30 equity $9,860.77 vs yday $9,860.77 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 28 | $42.93 | $2.07 | — | $8,656.66 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1232.60 | — |
| 2026-09-03 09:30 ET | **BUY** | `PBH` | 23 | $53.45 | $2.06 | — | $7,425.25 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.3; leftover $1232.60 | — |
| 2026-09-03 09:30 ET | **BUY** | `PCRX` | 46 | $26.74 | $2.13 | — | $6,193.08 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.6; leftover $1232.60 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $4,999.01 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1232.60 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $3,941.79 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1232.60 | — |
| 2026-09-03 09:30 ET | **BUY** | `PBF` | 16 | $74.75 | $2.04 | — | $2,743.76 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ret5=+8.2; leftover $1232.60 | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $1,525.46 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1232.60 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 339 | $3.63 | $4.37 | — | $290.51 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1232.60 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $290.51 | ▼ close $9,733.80 vs 09:30 $9,860.77 (session -108.22) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $290.51 | ▼ 09:30 equity $9,668.39 vs yday $9,733.80 (-65.41) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `PBH` | 23 | $51.80 | $2.08 | $-42.09 | $1,479.83 | ▼ -42.09 after sell → book $9,666.31; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PCRX` | 46 | $26.38 | $2.15 | $-20.84 | $2,691.17 | ▼ -20.84 after sell → book $9,664.17; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $3,768.25 | ▲ +19.86 after sell → book $9,662.15; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBF` | 16 | $74.50 | $2.06 | $-8.10 | $4,958.19 | ▼ -8.10 after sell → book $9,660.09; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 491 | $2.52 | $6.33 | — | $3,714.54 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1239.55 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 184 | $6.71 | $2.54 | — | $2,477.35 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1239.55 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 109 | $11.31 | $2.32 | — | $1,242.25 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1239.55 | — |
| 2026-09-04 09:30 ET | **BUY** | `PIPR` | 16 | $76.55 | $2.04 | — | $15.41 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+4.2; leftover $1239.55 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.41 | ▼ close $9,620.18 vs 09:30 $9,668.39 (session -26.68) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.41 | ▼ 09:30 equity $9,592.09 vs yday $9,620.18 (-28.09) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `HRMY` | 28 | $42.20 | $2.09 | $-24.61 | $1,194.91 | ▼ -24.61 after sell → book $9,589.99; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `RVTY` | 9 | $128.50 | $2.04 | $-39.60 | $2,349.38 | ▼ -39.60 after sell → book $9,587.96; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 23 | $54.31 | $2.08 | $+28.75 | $3,596.43 | ▲ +28.75 after sell → book $9,585.88; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 339 | $3.43 | $4.44 | $-76.61 | $4,754.76 | ▼ -76.61 after sell → book $9,581.44; vs 09:30 mark -4.44 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 491 | $2.38 | $6.43 | $-81.50 | $5,916.91 | ▼ -81.50 after sell → book $9,575.01; vs 09:30 mark -6.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 184 | $6.57 | $2.58 | $-30.88 | $7,123.21 | ▼ -30.88 after sell → book $9,572.43; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 109 | $11.22 | $2.35 | $-14.47 | $8,343.85 | ▼ -14.47 after sell → book $9,570.09; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `PIPR` | 16 | $76.64 | $2.06 | $-2.66 | $9,568.03 | ▼ -2.66 after sell → book $9,568.03; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,568.03 | ▲ close $9,568.03 vs 09:30 $9,592.09 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,568.03 | ▲ 09:30 equity $9,568.03 vs yday $9,568.03 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,568.03 | ▲ close $9,568.03 vs 09:30 $9,568.03 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,568.03 | ▲ 09:30 equity $9,568.03 vs yday $9,568.03 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,568.03 | ▲ close $9,568.03 vs 09:30 $9,568.03 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,568.03 | ▲ 09:30 equity $9,568.03 vs yday $9,568.03 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 22 | $52.55 | $2.06 | — | $8,409.87 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1196.00 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 5 | $206.84 | $2.00 | — | $7,373.67 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; ret5=+8.3; leftover $1196.00 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 251 | $4.75 | $3.24 | — | $6,178.18 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1196.00 | — |
| 2026-09-11 09:30 ET | **BUY** | `PGNY` | 43 | $27.45 | $2.12 | — | $4,995.71 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; 🔵; ret5=+6.1; leftover $1196.00 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 16 | $71.71 | $2.04 | — | $3,846.31 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; ret5=-9.1; leftover $1196.00 | — |
| 2026-09-11 09:30 ET | **BUY** | `PBR` | 56 | $21.21 | $2.16 | — | $2,656.39 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; ret5=+2.5; leftover $1196.00 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 103 | $11.55 | $2.30 | — | $1,464.45 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1196.00 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 79 | $15.01 | $2.23 | — | $276.43 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $1196.00 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $276.43 | ▲ close $9,754.86 vs 09:30 $9,568.03 (session +204.97) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $276.43 | ▲ 09:30 equity $9,758.39 vs yday $9,754.86 (+3.53) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 22 | $56.90 | $2.08 | $+91.57 | $1,526.15 | ▲ +91.57 after sell → book $9,756.31; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SANM` | 5 | $206.50 | $2.02 | $-5.73 | $2,556.63 | ▼ -5.73 after sell → book $9,754.29; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 251 | $4.82 | $3.29 | $+11.04 | $3,763.16 | ▲ +11.04 after sell → book $9,751.00; vs 09:30 mark -3.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PGNY` | 43 | $27.69 | $2.14 | $+6.06 | $4,951.69 | ▲ +6.06 after sell → book $9,748.86; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DSGX` | 16 | $77.68 | $2.06 | $+91.42 | $6,192.51 | ▲ +91.42 after sell → book $9,746.80; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PBR` | 56 | $21.23 | $2.18 | $-3.22 | $7,379.21 | ▼ -3.22 after sell → book $9,744.62; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `FUBO` | 103 | $11.56 | $2.33 | $-3.60 | $8,567.57 | ▼ -3.60 after sell → book $9,742.30; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AVTR` | 79 | $14.87 | $2.25 | $-15.54 | $9,740.05 | ▼ -15.54 after sell → book $9,740.05; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,740.05 | ▲ close $9,740.05 vs 09:30 $9,758.39 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,740.05 | ▲ 09:30 equity $9,740.05 vs yday $9,740.05 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,740.05 | ▲ close $9,740.05 vs 09:30 $9,740.05 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,740.05 | ▲ 09:30 equity $9,740.05 vs yday $9,740.05 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 85 | $14.31 | $2.25 | — | $8,521.45 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; ret5=+4.8; leftover $1217.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `ILMN` | 5 | $224.49 | $2.00 | — | $7,397.00 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list yday_gainer; 🔵; ret5=+5.3; leftover $1217.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 6 | $189.17 | $2.01 | — | $6,259.97 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $1217.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 30 | $39.99 | $2.08 | — | $5,058.19 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $1217.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 33 | $36.46 | $2.09 | — | $3,852.92 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ret5=+2.9; leftover $1217.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $2,767.36 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; ret5=+4.0; leftover $1217.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 207 | $5.87 | $2.67 | — | $1,549.60 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1217.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 17 | $68.79 | $2.04 | — | $378.13 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1217.51 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $378.13 | ▼ close $9,591.90 vs 09:30 $9,740.05 (session -131.01) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $378.13 | ▲ 09:30 equity $9,728.30 vs yday $9,591.90 (+136.40) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `AVAH` | 85 | $14.33 | $2.27 | $-2.81 | $1,593.91 | ▼ -2.81 after sell → book $9,726.03; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ILMN` | 5 | $233.85 | $2.02 | $+42.77 | $2,761.13 | ▲ +42.77 after sell → book $9,724.00; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 6 | $190.35 | $2.03 | $+3.04 | $3,901.20 | ▲ +3.04 after sell → book $9,721.97; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 30 | $37.57 | $2.10 | $-76.78 | $5,026.20 | ▼ -76.78 after sell → book $9,719.87; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BLFS` | 33 | $36.67 | $2.11 | $+2.73 | $6,234.21 | ▲ +2.73 after sell → book $9,717.77; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `IQV` | 4 | $273.15 | $2.02 | $+5.02 | $7,324.78 | ▲ +5.02 after sell → book $9,715.74; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 207 | $5.58 | $2.71 | $-65.41 | $8,477.13 | ▼ -65.41 after sell → book $9,713.03; vs 09:30 mark -2.71 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 17 | $72.70 | $2.06 | $+62.37 | $9,710.97 | ▲ +62.37 after sell → book $9,710.97; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BULL` | 152 | $7.95 | $2.45 | — | $8,500.12 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $1213.87 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 7 | $170.85 | $2.01 | — | $7,302.16 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1213.87 | — |
| 2026-09-17 09:30 ET | **BUY** | `AXTI` | 17 | $67.91 | $2.04 | — | $6,145.65 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.7; leftover $1213.87 | — |
| 2026-09-17 09:30 ET | **BUY** | `CYPH` | 453 | $2.67 | $5.84 | — | $4,928.03 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list yday_gainer; 🔵; ret5=-0.4; leftover $1213.87 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 159 | $7.59 | $2.47 | — | $3,718.75 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1213.87 | — |
| 2026-09-17 09:30 ET | **BUY** | `AVTR` | 76 | $15.81 | $2.22 | — | $2,514.98 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $1213.87 | — |
| 2026-09-17 09:30 ET | **BUY** | `SFL` | 89 | $13.55 | $2.26 | — | $1,306.77 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; ret5=+7.8; leftover $1213.87 | — |
| 2026-09-17 09:30 ET | **BUY** | `CRDO` | 7 | $168.65 | $2.01 | — | $124.21 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list yday_gainer; ret5=-3.8; leftover $1213.87 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.21 | ▲ close $9,944.11 vs 09:30 $9,728.30 (session +254.44) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.21 | ▲ 09:30 equity $10,049.38 vs yday $9,944.11 (+105.27) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `BULL` | 152 | $7.85 | $2.48 | $-20.13 | $1,314.93 | ▼ -20.13 after sell → book $10,046.90; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 7 | $182.33 | $2.03 | $+76.32 | $2,589.21 | ▲ +76.32 after sell → book $10,044.87; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AXTI` | 17 | $69.72 | $2.06 | $+26.67 | $3,772.38 | ▲ +26.67 after sell → book $10,042.81; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CYPH` | 453 | $3.04 | $5.93 | $+151.31 | $5,141.31 | ▲ +151.31 after sell → book $10,036.88; vs 09:30 mark -5.93 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 159 | $7.98 | $2.50 | $+57.04 | $6,407.63 | ▲ +57.04 after sell → book $10,034.38; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 76 | $15.87 | $2.24 | $+0.10 | $7,611.51 | ▲ +0.10 after sell → book $10,032.14; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SFL` | 89 | $13.74 | $2.28 | $+12.37 | $8,832.08 | ▲ +12.37 after sell → book $10,029.85; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CRDO` | 7 | $171.11 | $2.03 | $+13.18 | $10,027.82 | ▲ +13.18 after sell → book $10,027.82; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 79 | $20.91 | $2.23 | — | $8,373.71 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1671.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `AMD` | 3 | $547.37 | $2.00 | — | $6,729.60 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; ret5=+8.2; leftover $1671.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 466 | $3.58 | $6.01 | — | $5,055.31 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1671.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `SHLS` | 218 | $7.64 | $2.81 | — | $3,386.97 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list yday_gainer; 🔵; ret5=+7.6; leftover $1671.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 118 | $14.07 | $2.34 | — | $1,724.37 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1671.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `RANI` | 1966 | $0.85 | $22.61 | — | $30.66 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer; ret5=+3.6; leftover $1671.30 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.66 | ▲ close $10,034.36 vs 09:30 $10,049.38 (session +44.54) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.66 | ▲ 09:30 equity $10,241.11 vs yday $10,034.36 (+206.75) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 79 | $21.65 | $2.25 | $+53.98 | $1,738.76 | ▲ +53.98 after sell → book $10,238.86; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DDD` | 466 | $3.71 | $6.10 | $+48.47 | $3,461.51 | ▲ +48.47 after sell → book $10,232.76; vs 09:30 mark -6.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SHLS` | 218 | $7.71 | $2.86 | $+9.59 | $5,139.43 | ▲ +9.59 after sell → book $10,229.90; vs 09:30 mark -2.86 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 118 | $13.90 | $2.38 | $-24.78 | $6,777.26 | ▼ -24.78 after sell → book $10,227.52; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RANI` | 1966 | $0.86 | $23.22 | $-18.31 | $8,452.66 | ▼ -18.31 after sell → book $10,204.30; vs 09:30 mark -23.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `COHR` | 3 | $326.48 | $2.00 | — | $7,471.22 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; ret5=+3.9; leftover $1207.52 | — |
| 2026-09-21 09:30 ET | **BUY** | `FORM` | 9 | $123.00 | $2.02 | — | $6,362.20 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; ret5=+3.0; leftover $1207.52 | — |
| 2026-09-21 09:30 ET | **BUY** | `ARM` | 4 | $294.36 | $2.00 | — | $5,182.76 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; ret5=+4.1; leftover $1207.52 | — |
| 2026-09-21 09:30 ET | **BUY** | `MXL` | 14 | $83.53 | $2.03 | — | $4,011.31 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; ret5=+8.8; leftover $1207.52 | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 7 | $157.87 | $2.01 | — | $2,904.20 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; ret5=+6.5; leftover $1207.52 | — |
| 2026-09-21 09:30 ET | **BUY** | `PGNY` | 44 | $27.22 | $2.12 | — | $1,704.40 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; ret5=+6.1; leftover $1207.52 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,704.40 | ▲ close $10,400.05 vs 09:30 $10,241.11 (session +207.94) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,704.40 | ▼ 09:30 equity $10,325.55 vs yday $10,400.05 (-74.50) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `AMD` | 3 | $606.57 | $2.02 | $+173.58 | $3,522.09 | ▲ +173.58 after sell → book $10,323.53; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `COHR` | 3 | $310.29 | $2.02 | $-52.59 | $4,450.94 | ▼ -52.59 after sell → book $10,321.51; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `ARM` | 4 | $319.41 | $2.02 | $+96.18 | $5,726.56 | ▲ +96.18 after sell → book $10,319.49; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 7 | $93.97 | $2.01 | — | $5,066.76 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; ret5=-0.6; leftover $715.82 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,066.76 | ▼ close $10,317.20 vs 09:30 $10,325.55 (session -0.28) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,066.76 | ▲ 09:30 equity $10,379.22 vs yday $10,317.20 (+62.02) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `FORM` | 9 | $125.39 | $2.04 | $+17.46 | $6,193.23 | ▲ +17.46 after sell → book $10,377.18; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MXL` | 14 | $86.57 | $2.05 | $+38.48 | $7,403.16 | ▲ +38.48 after sell → book $10,375.13; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `A` | 7 | $166.54 | $2.03 | $+56.65 | $8,566.91 | ▲ +56.65 after sell → book $10,373.10; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `PGNY` | 44 | $26.10 | $2.14 | $-53.54 | $9,713.16 | ▼ -53.54 after sell → book $10,370.95; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `USFD` | 7 | $93.97 | $2.03 | $-4.04 | $10,368.92 | ▼ -4.04 after sell → book $10,368.92; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 14 | $89.50 | $2.03 | — | $9,113.89 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1296.12 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 11 | $116.85 | $2.02 | — | $7,826.52 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1296.12 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 62 | $20.65 | $2.18 | — | $6,544.04 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1296.12 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 132 | $9.81 | $2.39 | — | $5,246.74 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1296.12 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 46 | $27.79 | $2.13 | — | $3,966.27 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1296.12 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 6 | $196.78 | $2.01 | — | $2,783.58 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $1296.12 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 82 | $15.72 | $2.24 | — | $1,492.30 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1296.12 | — |
| 2026-09-23 09:30 ET | **BUY** | `PRME` | 367 | $3.53 | $4.73 | — | $192.06 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list yday_gainer; 🔵; ret5=-15.6; leftover $1296.12 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $192.06 | ▼ close $9,931.33 vs 09:30 $10,379.22 (session -417.87) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $192.06 | ▼ 09:30 equity $9,882.82 vs yday $9,931.33 (-48.51) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 14 | $87.67 | $2.05 | $-29.63 | $1,417.46 | ▼ -29.63 after sell → book $9,880.77; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HALO` | 11 | $112.22 | $2.04 | $-55.00 | $2,649.84 | ▼ -55.00 after sell → book $9,878.73; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 62 | $20.52 | $2.20 | $-12.43 | $3,919.88 | ▼ -12.43 after sell → book $9,876.53; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 132 | $9.67 | $2.42 | $-23.28 | $5,193.90 | ▼ -23.28 after sell → book $9,874.11; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 46 | $26.22 | $2.15 | $-76.50 | $6,397.87 | ▼ -76.50 after sell → book $9,871.96; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 6 | $192.26 | $2.03 | $-31.16 | $7,549.40 | ▼ -31.16 after sell → book $9,869.93; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 82 | $14.38 | $2.26 | $-114.38 | $8,726.30 | ▼ -114.38 after sell → book $9,867.67; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PRME` | 367 | $3.11 | $4.81 | $-163.68 | $9,862.87 | ▼ -163.68 after sell → book $9,862.87; vs 09:30 mark -4.80 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,862.87 | ▲ close $9,862.87 vs 09:30 $9,882.82 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,810.58 | ▲ 09:30 equity $9,810.58 vs yday $9,810.58 (+0.00) | 09:30 open · cash $9,810.58 · no holdings · equity $9,810.58 vs prior close $9,810.58 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 10 | $115.36 | $2.02 | — | $8,654.96 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1226.32 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SAIL` | 55 | $22.05 | $2.15 | — | $7,440.05 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+9.6; leftover $1226.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SMWB` | 163 | $7.50 | $2.48 | — | $6,215.08 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list yday_mover; 🔵; ret5=-9.7; leftover $1226.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 160 | $7.65 | $2.47 | — | $4,988.61 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1226.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RGEN` | 6 | $189.92 | $2.01 | — | $3,847.08 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $1226.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 1 | $887.00 | $1.99 | — | $2,958.08 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $1226.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 59 | $20.61 | $2.17 | — | $1,739.93 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; 🔵; ret5=+9.1; leftover $1226.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 31 | $38.51 | $2.08 | — | $544.03 | — | Clock-B #2 fresh catalyst + limited extension (research; not KEEP); gate clk_fresh_cat_coil=True; rank cond; list flatten; ret5=+4.7; leftover $1226.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $544.03 | ▼ close $9,708.53 vs 09:30 $9,810.58 (session -84.68) | 16:00 close · cash $544.03 · equity $9,708.53 vs 09:30 $9,810.58 (-102.05; session marks -84.68) · 8 name(s) marked open→close (per-name table). HALO×10 09:30 $115.36 → close $113.90 -14.60; SAIL×55 09:30 $22.05 → close $20.64 -77.55; SMWB×163 09:30 $7.50 → close $7.58 +13.04; MRVI×160 09:30 $7.65 → close $7.60 -8.00; RGEN×6 09:30 $189.92 → close $189.68 -1.44; COST×1 09:30 $887.00 → close $922.76 +35.76; OMER×59 09:30 $20.61 → close $20.08 -31.27; BLFS×31 09:30 $38.51 → close $38.49 -0.62 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PLX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BHP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `EBAY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TME` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EROC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1284.57 < 1 share @ 1746.53 |
| 2026-08-28 | `MPWR` | cash | leftover split 1268.38 < 1 share @ 1306.03 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACIW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `AVPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CHKP` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DINO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALAB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ANET` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CRK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DHT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `ADM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASND` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASTH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CHEF` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `COUR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CRDL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `VNT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `STX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HAS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OBE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `WDS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `QRVO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `VLO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `M` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VLO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DHT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VOD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `WCC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PANW` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `VLO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `FRO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IOT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CNTB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-21 | `SNDK` | cash | leftover split 1207.52 < 1 share @ 1826.00 |
| 2026-09-22 | `FORM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MXL` | no_price | no 09:30 open — carry |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PGNY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `ZS` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-24 | `EGHT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RNG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RPD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CRWD` | hard_red | hard-red S=-7.66 sit; no new buys |
