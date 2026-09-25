# Factor mine action — `union_clk_fresh_cat_coil_opp_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `opp_rvol` · size `leftover` · sell `list` · S-boost `none` · Clock-B #2 ∩ Theme Radar T−1 oppset

Cash book **+4.76%** ($10,476) · signal-only (no cash/fees) was +2.25%. Starts YES **25/30**. Fills 160 · skips 61 · realized $+223.91.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: Theme Radar T−1 relative volume (Clock-B opportunity-set; not same-day RelVol).
- Must-have: Clock-B #2: a fresh good catalyst (catal / EPS beat / packet or headline green) and the prior tape is not already exploded.
- Must-have: Theme Radar Clock-B opportunity-set: T−1 gap or RelVol (or week move) flagged — not today's Gap/RelVol.
- Must-not: Clock-B #5 long veto: extreme prior extension plus diminishing progress or a failed breakout.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by Theme Radar T−1 relative volume (Clock-B opportunity-set; not same-day RelVol) and keep the top 8.
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
- **Gate** `clk_fresh_cat_coil=True,oppset=True` · **rank** `opp_rvol` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,223.90.

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
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 151 | $16.50 | $2.44 | — | $7,506.06 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $2500.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `AMAT` | 5 | $499.40 | $2.00 | — | $5,007.05 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+1.3; leftover $2500.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 4 | $503.50 | $2.00 | — | $2,991.05 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable; 🔵; ⚪; ret5=+7.9; leftover $2500.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NU` | 158 | $15.74 | $2.46 | — | $501.67 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-1.3; leftover $2500.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $501.67 | ▼ close $9,920.78 vs 09:30 $10,000.00 (session -70.31) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $501.67 | ▲ 09:30 equity $9,999.45 vs yday $9,920.78 (+78.67) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 151 | $15.73 | $2.49 | $-121.20 | $2,874.41 | ▼ -121.20 after sell → book $9,996.96; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AMAT` | 5 | $517.45 | $2.04 | $+86.19 | $5,459.61 | ▲ +86.19 after sell → book $9,994.93; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 4 | $525.53 | $2.03 | $+84.09 | $7,559.70 | ▲ +84.09 after sell → book $9,992.90; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NU` | 158 | $15.40 | $2.51 | $-58.69 | $9,990.39 | ▼ -58.69 after sell → book $9,990.39; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 365 | $9.12 | $4.71 | — | $6,656.88 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $3330.13 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 205 | $16.20 | $2.64 | — | $3,333.24 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $3330.13 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 35 | $92.99 | $2.10 | — | $76.49 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-0.8; leftover $3330.13 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.49 | ▲ close $9,994.49 vs 09:30 $9,999.45 (session +13.55) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.49 | ▼ 09:30 equity $9,840.64 vs yday $9,994.49 (-153.85) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 365 | $9.03 | $4.80 | $-42.35 | $3,367.64 | ▼ -42.35 after sell → book $9,835.84; vs 09:30 mark -4.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 205 | $15.78 | $2.70 | $-91.45 | $6,599.84 | ▼ -91.45 after sell → book $9,833.14; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 35 | $92.38 | $2.13 | $-25.58 | $9,831.01 | ▼ -25.58 after sell → book $9,831.01; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,831.01 | ▲ close $9,831.01 vs 09:30 $9,840.64 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,831.01 | ▲ 09:30 equity $9,831.01 vs yday $9,831.01 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,831.01 | ▲ close $9,831.01 vs 09:30 $9,831.01 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,831.01 | ▲ 09:30 equity $9,831.01 vs yday $9,831.01 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 165 | $7.44 | $2.48 | — | $8,600.92 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1228.88 | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 164 | $7.45 | $2.48 | — | $7,376.64 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1228.88 | — |
| 2026-08-20 09:30 ET | **BUY** | `BILL` | 25 | $49.00 | $2.06 | — | $6,149.58 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-2.0; leftover $1228.88 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRCL` | 14 | $82.99 | $2.03 | — | $4,985.69 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable; 🔵; ⚪; ret5=+7.4; leftover $1228.88 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $3,800.53 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1228.88 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $2,583.58 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1228.88 | — |
| 2026-08-20 09:30 ET | **BUY** | `WOLF` | 46 | $26.50 | $2.13 | — | $1,362.46 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-8.3; leftover $1228.88 | — |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 42 | $29.20 | $2.12 | — | $133.94 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1228.88 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.94 | ▲ close $9,911.87 vs 09:30 $9,831.01 (session +98.31) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.94 | ▲ 09:30 equity $10,130.51 vs yday $9,911.87 (+218.64) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `MRVI` | 165 | $8.28 | $2.52 | $+133.59 | $1,497.62 | ▲ +133.59 after sell → book $10,127.99; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 164 | $7.09 | $2.52 | $-64.04 | $2,657.86 | ▼ -64.04 after sell → book $10,125.47; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BILL` | 25 | $47.50 | $2.08 | $-41.65 | $3,843.27 | ▼ -41.65 after sell → book $10,123.38; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CRCL` | 14 | $87.98 | $2.05 | $+65.78 | $5,072.94 | ▲ +65.78 after sell → book $10,121.33; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $6,315.25 | ▲ +57.15 after sell → book $10,119.28; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $7,632.09 | ▲ +99.89 after sell → book $10,117.15; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BLSH` | 42 | $29.75 | $2.14 | $+18.85 | $8,879.45 | ▲ +18.85 after sell → book $10,115.01; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 593 | $14.96 | $7.65 | — | $0.52 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-1.6; leftover $8879.45 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.52 | ▼ close $9,926.30 vs 09:30 $10,130.51 (session -181.06) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.52 | ▼ 09:30 equity $9,891.34 vs yday $9,926.30 (-34.96) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `WOLF` | 46 | $25.00 | $2.15 | $-73.28 | $1,148.37 | ▼ -73.28 after sell → book $9,889.19; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 593 | $14.74 | $7.82 | $-145.93 | $9,881.38 | ▼ -145.93 after sell → book $9,881.38; vs 09:30 mark -7.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,881.38 | ▲ close $9,881.38 vs 09:30 $9,891.34 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,881.38 | ▲ 09:30 equity $9,881.38 vs yday $9,881.38 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `AAOI` | 14 | $111.78 | $2.03 | — | $8,314.49 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-30.5; leftover $1646.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `ELMT` | 92 | $17.89 | $2.27 | — | $6,666.35 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; ⚪; ret5=-7.5; leftover $1646.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 118 | $13.92 | $2.34 | — | $5,021.44 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+5.9; leftover $1646.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `AXTI` | 24 | $68.20 | $2.06 | — | $3,382.58 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-31.9; leftover $1646.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 322 | $5.10 | $4.15 | — | $1,736.23 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-8.9; leftover $1646.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `QMCO` | 75 | $21.90 | $2.21 | — | $91.51 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=-13.5; leftover $1646.90 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $91.51 | ▼ close $9,818.64 vs 09:30 $9,881.38 (session -47.66) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $91.51 | ▼ 09:30 equity $9,712.10 vs yday $9,818.64 (-106.54) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AAOI` | 14 | $110.59 | $2.05 | $-20.68 | $1,637.72 | ▼ -20.68 after sell → book $9,710.05; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ELMT` | 92 | $17.82 | $2.29 | $-11.00 | $3,274.86 | ▼ -11.00 after sell → book $9,707.75; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 118 | $14.03 | $2.38 | $+8.26 | $4,928.03 | ▲ +8.26 after sell → book $9,705.38; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AXTI` | 24 | $65.34 | $2.08 | $-72.79 | $6,494.10 | ▼ -72.79 after sell → book $9,703.29; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EH` | 322 | $4.77 | $4.22 | $-114.63 | $8,025.82 | ▼ -114.63 after sell → book $9,699.07; vs 09:30 mark -4.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `QMCO` | 75 | $22.31 | $2.24 | $+26.29 | $9,696.83 | ▲ +26.29 after sell → book $9,696.83; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 66 | $18.26 | $2.19 | — | $8,489.48 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; ret5=-11.4; leftover $1212.10 | — |
| 2026-08-26 09:30 ET | **BUY** | `INTU` | 3 | $323.47 | $2.00 | — | $7,517.08 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; ret5=+2.0; leftover $1212.10 | — |
| 2026-08-26 09:30 ET | **BUY** | `NCNO` | 62 | $19.33 | $2.18 | — | $6,316.44 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; ret5=+3.0; leftover $1212.10 | — |
| 2026-08-26 09:30 ET | **BUY** | `HEI` | 3 | $370.00 | $2.00 | — | $5,204.44 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; ret5=-4.6; leftover $1212.10 | — |
| 2026-08-26 09:30 ET | **BUY** | `BE` | 5 | $213.94 | $2.00 | — | $4,132.74 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1212.10 | — |
| 2026-08-26 09:30 ET | **BUY** | `QMLS` | 187 | $6.47 | $2.55 | — | $2,920.29 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-7.0; leftover $1212.10 | — |
| 2026-08-26 09:30 ET | **BUY** | `NVTS` | 96 | $12.60 | $2.28 | — | $1,708.42 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=-5.5; leftover $1212.10 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 109 | $11.12 | $2.32 | — | $494.02 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1212.10 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $494.02 | ▲ close $9,806.92 vs 09:30 $9,712.10 (session +127.60) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $494.02 | ▲ 09:30 equity $10,032.78 vs yday $9,806.92 (+225.86) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BBWI` | 66 | $18.69 | $2.21 | $+23.98 | $1,725.35 | ▲ +23.98 after sell → book $10,030.57; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INTU` | 3 | $353.54 | $2.02 | $+86.19 | $2,783.95 | ▲ +86.19 after sell → book $10,028.55; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NCNO` | 62 | $22.03 | $2.20 | $+163.03 | $4,147.61 | ▲ +163.03 after sell → book $10,026.35; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HEI` | 3 | $346.19 | $2.02 | $-75.45 | $5,184.17 | ▼ -75.45 after sell → book $10,024.34; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BE` | 5 | $227.10 | $2.02 | $+61.77 | $6,317.64 | ▲ +61.77 after sell → book $10,022.31; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `QMLS` | 187 | $6.33 | $2.59 | $-31.32 | $7,498.76 | ▼ -31.32 after sell → book $10,019.72; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NVTS` | 96 | $13.18 | $2.30 | $+51.10 | $8,761.73 | ▲ +51.10 after sell → book $10,017.41; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 109 | $11.52 | $2.35 | $+38.94 | $10,015.07 | ▲ +38.94 after sell → book $10,015.07; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,015.07 | ▲ close $10,015.07 vs 09:30 $10,032.78 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,015.07 | ▲ 09:30 equity $10,015.07 vs yday $10,015.07 (-0.00) | — | — |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 4 | $306.34 | $2.00 | — | $8,787.71 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-23.0; leftover $1251.88 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 66 | $18.75 | $2.19 | — | $7,548.02 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=-5.0; leftover $1251.88 | — |
| 2026-08-28 09:30 ET | **BUY** | `SYRE` | 13 | $91.75 | $2.03 | — | $6,353.24 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-13.2; leftover $1251.88 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 38 | $32.90 | $2.10 | — | $5,100.94 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1251.88 | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 43 | $28.91 | $2.12 | — | $3,855.69 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=+9.2; leftover $1251.88 | — |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 9 | $137.19 | $2.02 | — | $2,618.96 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+7.1; leftover $1251.88 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 65 | $19.00 | $2.19 | — | $1,381.77 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+7.5; leftover $1251.88 | — |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 33 | $37.49 | $2.09 | — | $142.52 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; ret5=+5.4; leftover $1251.88 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.52 | ▼ close $9,809.58 vs 09:30 $10,015.07 (session -188.76) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.52 | ▼ 09:30 equity $9,719.72 vs yday $9,809.58 (-89.86) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 4 | $298.01 | $2.02 | $-37.34 | $1,332.53 | ▼ -37.34 after sell → book $9,717.70; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 66 | $19.25 | $2.21 | $+28.60 | $2,600.82 | ▲ +28.60 after sell → book $9,715.49; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SYRE` | 13 | $89.15 | $2.05 | $-37.88 | $3,757.73 | ▼ -37.88 after sell → book $9,713.44; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 38 | $31.15 | $2.12 | $-70.73 | $4,939.30 | ▼ -70.73 after sell → book $9,711.32; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 43 | $28.06 | $2.14 | $-40.81 | $6,143.74 | ▼ -40.81 after sell → book $9,709.18; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 65 | $18.12 | $2.21 | $-61.27 | $7,319.66 | ▼ -61.27 after sell → book $9,706.97; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 33 | $35.77 | $2.11 | $-60.96 | $8,497.96 | ▼ -60.96 after sell → book $9,704.86; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,497.96 | ▲ close $9,761.02 vs 09:30 $9,719.72 (session +56.16) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,497.96 | ▼ 09:30 equity $9,760.21 vs yday $9,761.02 (-0.81) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `MRNA` | 9 | $140.25 | $2.04 | $+23.49 | $9,758.18 | ▲ +23.49 after sell → book $9,758.18; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,758.18 | ▲ close $9,758.18 vs 09:30 $9,760.21 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,758.18 | ▲ 09:30 equity $9,758.18 vs yday $9,758.18 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,758.18 | ▲ close $9,758.18 vs 09:30 $9,758.18 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,758.18 | ▲ 09:30 equity $9,758.18 vs yday $9,758.18 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $55.42 | $2.06 | — | $8,536.88 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-25.9; leftover $1219.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `ENOV` | 60 | $20.28 | $2.17 | — | $7,317.91 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-20.0; leftover $1219.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 4 | $257.00 | $2.00 | — | $6,287.91 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-5.5; leftover $1219.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `NTSK` | 78 | $15.51 | $2.22 | — | $5,075.90 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-1.2; leftover $1219.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `NTAP` | 7 | $161.95 | $2.01 | — | $3,940.24 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-6.7; leftover $1219.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $2,721.94 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1219.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 15 | $76.86 | $2.04 | — | $1,567.01 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-6.6; leftover $1219.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `RPD` | 107 | $11.39 | $2.31 | — | $345.97 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-6.1; leftover $1219.77 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $345.97 | ▼ close $9,663.06 vs 09:30 $9,758.18 (session -78.25) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $345.97 | ▼ 09:30 equity $9,554.05 vs yday $9,663.06 (-109.01) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 22 | $55.79 | $2.08 | $+4.01 | $1,571.27 | ▲ +4.01 after sell → book $9,551.97; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ENOV` | 60 | $19.01 | $2.19 | $-80.56 | $2,709.68 | ▼ -80.56 after sell → book $9,549.78; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 4 | $238.88 | $2.02 | $-76.50 | $3,663.18 | ▼ -76.50 after sell → book $9,547.76; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NTSK` | 78 | $14.15 | $2.25 | $-110.55 | $4,764.63 | ▼ -110.55 after sell → book $9,545.51; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NTAP` | 7 | $182.59 | $2.03 | $+140.44 | $6,040.73 | ▲ +140.44 after sell → book $9,543.48; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 23 | $52.03 | $2.08 | $-23.69 | $7,235.34 | ▼ -23.69 after sell → book $9,541.40; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSXY` | 15 | $73.63 | $2.06 | $-52.54 | $8,337.74 | ▼ -52.54 after sell → book $9,539.35; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RPD` | 107 | $11.23 | $2.34 | $-21.77 | $9,537.01 | ▼ -21.77 after sell → book $9,537.01; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 136 | $8.74 | $2.40 | — | $8,345.97 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-0.8; leftover $1192.13 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 8 | $137.35 | $2.01 | — | $7,245.16 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+5.4; leftover $1192.13 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRCL` | 12 | $97.98 | $2.03 | — | $6,067.37 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+9.5; leftover $1192.13 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 17 | $68.52 | $2.04 | — | $4,900.49 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+3.4; leftover $1192.13 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 7 | $167.55 | $2.01 | — | $3,725.63 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list earn_react; ret5=+0.9; leftover $1192.13 | — |
| 2026-09-04 09:30 ET | **BUY** | `BLSH` | 34 | $34.69 | $2.09 | — | $2,544.08 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+7.9; leftover $1192.13 | — |
| 2026-09-04 09:30 ET | **BUY** | `ZETA` | 36 | $32.65 | $2.10 | — | $1,366.58 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+8.1; leftover $1192.13 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 5 | $236.82 | $2.00 | — | $180.47 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+8.1; leftover $1192.13 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $180.47 | ▲ close $9,662.49 vs 09:30 $9,554.05 (session +142.17) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $180.47 | ▼ 09:30 equity $9,618.28 vs yday $9,662.49 (-44.21) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ASAN` | 136 | $8.73 | $2.43 | $-6.19 | $1,365.32 | ▼ -6.19 after sell → book $9,615.85; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MSTR` | 8 | $137.62 | $2.03 | $-1.89 | $2,464.25 | ▼ -1.89 after sell → book $9,613.82; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRCL` | 12 | $100.65 | $2.05 | $+27.97 | $3,670.00 | ▲ +27.97 after sell → book $9,611.77; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 17 | $67.05 | $2.06 | $-29.09 | $4,807.79 | ▼ -29.09 after sell → book $9,609.71; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 7 | $160.52 | $2.03 | $-53.25 | $5,929.40 | ▼ -53.25 after sell → book $9,607.68; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BLSH` | 34 | $35.90 | $2.11 | $+36.94 | $7,147.89 | ▲ +36.94 after sell → book $9,605.57; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ZETA` | 36 | $31.08 | $2.12 | $-60.74 | $8,264.65 | ▼ -60.74 after sell → book $9,603.45; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 5 | $267.76 | $2.03 | $+150.67 | $9,601.43 | ▲ +150.67 after sell → book $9,601.43; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,601.43 | ▲ close $9,601.43 vs 09:30 $9,618.28 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,601.43 | ▲ 09:30 equity $9,601.43 vs yday $9,601.43 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,601.43 | ▲ close $9,601.43 vs 09:30 $9,601.43 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,601.43 | ▲ 09:30 equity $9,601.43 vs yday $9,601.43 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,601.43 | ▲ close $9,601.43 vs 09:30 $9,601.43 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,601.43 | ▲ 09:30 equity $9,601.43 vs yday $9,601.43 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `PBR` | 75 | $21.21 | $2.21 | — | $8,008.46 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+2.5; leftover $1600.24 | — |
| 2026-09-11 09:30 ET | **BUY** | `INTR` | 276 | $5.78 | $3.56 | — | $6,409.62 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=-1.7; leftover $1600.24 | — |
| 2026-09-11 09:30 ET | **BUY** | `INSP` | 22 | $69.88 | $2.06 | — | $4,870.20 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+8.0; leftover $1600.24 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 30 | $52.55 | $2.08 | — | $3,291.62 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1600.24 | — |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 10 | $157.55 | $2.02 | — | $1,714.10 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $1600.24 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 138 | $11.55 | $2.40 | — | $117.80 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1600.24 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $117.80 | ▲ close $9,764.70 vs 09:30 $9,601.43 (session +177.61) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $117.80 | ▼ 09:30 equity $9,714.65 vs yday $9,764.70 (-50.05) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `PBR` | 75 | $21.23 | $2.24 | $-2.96 | $1,707.81 | ▼ -2.96 after sell → book $9,712.41; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INTR` | 276 | $5.49 | $3.62 | $-87.22 | $3,219.43 | ▼ -87.22 after sell → book $9,708.79; vs 09:30 mark -3.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INSP` | 22 | $72.14 | $2.08 | $+45.59 | $4,804.43 | ▲ +45.59 after sell → book $9,706.71; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 30 | $56.90 | $2.10 | $+126.32 | $6,509.33 | ▲ +126.32 after sell → book $9,704.61; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RDDT` | 10 | $160.00 | $2.04 | $+20.44 | $8,107.29 | ▲ +20.44 after sell → book $9,702.57; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `FUBO` | 138 | $11.56 | $2.44 | $-3.46 | $9,700.13 | ▼ -3.46 after sell → book $9,700.13; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,700.13 | ▲ close $9,700.13 vs 09:30 $9,714.65 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,700.13 | ▲ 09:30 equity $9,700.13 vs yday $9,700.13 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,700.13 | ▲ close $9,700.13 vs 09:30 $9,700.13 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,700.13 | ▲ 09:30 equity $9,700.13 vs yday $9,700.13 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 13 | $87.40 | $2.03 | — | $8,561.90 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1212.52 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 206 | $5.87 | $2.66 | — | $7,350.02 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1212.52 | — |
| 2026-09-16 09:30 ET | **BUY** | `ILMN` | 5 | $224.49 | $2.00 | — | $6,225.57 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+5.3; leftover $1212.52 | — |
| 2026-09-16 09:30 ET | **BUY** | `QLYS` | 6 | $179.60 | $2.01 | — | $5,145.96 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=+8.8; leftover $1212.52 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 65 | $18.61 | $2.19 | — | $3,934.12 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1212.52 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 17 | $68.79 | $2.04 | — | $2,762.65 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1212.52 | — |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 31 | $38.01 | $2.08 | — | $1,582.26 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $1212.52 | — |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 174 | $6.95 | $2.51 | — | $370.45 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-5.8; leftover $1212.52 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $370.45 | ▲ close $9,828.75 vs 09:30 $9,700.13 (session +146.14) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $370.45 | ▲ 09:30 equity $9,991.07 vs yday $9,828.75 (+162.32) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 13 | $83.20 | $2.05 | $-58.68 | $1,450.00 | ▼ -58.68 after sell → book $9,989.02; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 206 | $5.58 | $2.70 | $-65.10 | $2,596.78 | ▼ -65.10 after sell → book $9,986.32; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ILMN` | 5 | $233.85 | $2.02 | $+42.77 | $3,764.00 | ▲ +42.77 after sell → book $9,984.29; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QLYS` | 6 | $180.82 | $2.03 | $+3.28 | $4,846.89 | ▲ +3.28 after sell → book $9,982.26; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BBNX` | 65 | $22.46 | $2.21 | $+245.86 | $6,304.59 | ▲ +245.86 after sell → book $9,980.06; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 17 | $72.70 | $2.06 | $+62.37 | $7,538.42 | ▲ +62.37 after sell → book $9,977.99; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `KRMN` | 31 | $37.89 | $2.10 | $-7.91 | $8,710.91 | ▼ -7.91 after sell → book $9,975.89; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 174 | $7.27 | $2.55 | $+50.62 | $9,973.34 | ▲ +50.62 after sell → book $9,973.34; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BULL` | 156 | $7.95 | $2.46 | — | $8,730.68 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $1246.67 | — |
| 2026-09-17 09:30 ET | **BUY** | `MAMA` | 87 | $14.21 | $2.25 | — | $7,492.16 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=-6.5; leftover $1246.67 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 164 | $7.59 | $2.48 | — | $6,244.92 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1246.67 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 7 | $170.85 | $2.01 | — | $5,046.96 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1246.67 | — |
| 2026-09-17 09:30 ET | **BUY** | `EROC` | 98 | $12.64 | $2.28 | — | $3,805.95 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-3.6; leftover $1246.67 | — |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 1 | $934.88 | $1.99 | — | $2,869.08 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=-7.0; leftover $1246.67 | — |
| 2026-09-17 09:30 ET | **BUY** | `CRDO` | 7 | $168.65 | $2.01 | — | $1,686.52 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=-3.8; leftover $1246.67 | — |
| 2026-09-17 09:30 ET | **BUY** | `AXTI` | 18 | $67.91 | $2.04 | — | $462.10 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.7; leftover $1246.67 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $462.10 | ▼ close $9,898.50 vs 09:30 $9,991.07 (session -57.31) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $462.10 | ▲ 09:30 equity $10,037.29 vs yday $9,898.50 (+138.79) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `BULL` | 156 | $7.85 | $2.49 | $-20.55 | $1,684.20 | ▼ -20.55 after sell → book $10,034.79; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `MAMA` | 87 | $12.91 | $2.28 | $-117.63 | $2,805.10 | ▼ -117.63 after sell → book $10,032.52; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 164 | $7.98 | $2.52 | $+58.96 | $4,111.30 | ▲ +58.96 after sell → book $10,030.00; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 7 | $182.33 | $2.03 | $+76.32 | $5,385.58 | ▲ +76.32 after sell → book $10,027.97; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `EROC` | 98 | $13.00 | $2.31 | $+30.69 | $6,657.27 | ▲ +30.69 after sell → book $10,025.66; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 1 | $915.66 | $2.01 | $-23.23 | $7,570.91 | ▼ -23.23 after sell → book $10,023.64; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CRDO` | 7 | $171.11 | $2.03 | $+13.18 | $8,766.65 | ▲ +13.18 after sell → book $10,021.61; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AXTI` | 18 | $69.72 | $2.06 | $+28.47 | $10,019.55 | ▲ +28.47 after sell → book $10,019.55; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 178 | $14.07 | $2.52 | — | $7,512.56 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2504.89 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 119 | $20.91 | $2.35 | — | $5,021.93 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2504.89 | — |
| 2026-09-18 09:30 ET | **BUY** | `AMD` | 4 | $547.37 | $2.00 | — | $2,830.44 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+8.2; leftover $2504.89 | — |
| 2026-09-18 09:30 ET | **BUY** | `SHLS` | 327 | $7.64 | $4.22 | — | $327.95 | — | Clock-B #2 ∩ Theme Radar T−1 oppset; gate clk_fresh_cat_coil=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+7.6; leftover $2504.89 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $327.95 | ▼ close $9,998.40 vs 09:30 $10,037.29 (session -10.06) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $327.95 | ▲ 09:30 equity $10,235.19 vs yday $9,998.40 (+236.79) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 178 | $13.90 | $2.57 | $-35.36 | $2,799.57 | ▼ -35.36 after sell → book $10,232.61; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 119 | $21.65 | $2.39 | $+83.33 | $5,373.54 | ▲ +83.33 after sell → book $10,230.23; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `AMD` | 4 | $583.88 | $2.03 | $+142.01 | $7,707.03 | ▲ +142.01 after sell → book $10,228.20; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SHLS` | 327 | $7.71 | $4.29 | $+14.38 | $10,223.90 | ▲ +14.38 after sell → book $10,223.90; vs 09:30 mark -4.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,223.90 | ▲ close $10,223.90 vs 09:30 $10,235.19 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,223.90 | ▲ 09:30 equity $10,223.90 vs yday $10,223.90 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,223.90 | ▲ close $10,223.90 vs 09:30 $10,223.90 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,223.90 | ▲ 09:30 equity $10,223.90 vs yday $10,223.90 (+0.00) | — | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,223.90 | ▲ close $10,223.90 vs 09:30 $10,223.90 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,223.90 | ▲ 09:30 equity $10,223.90 vs yday $10,223.90 (+0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,223.90 | ▲ close $10,223.90 vs 09:30 $10,223.90 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,475.92 | ▲ 09:30 equity $10,475.92 vs yday $10,475.92 (+0.00) | 09:30 open · cash $10,475.92 · no holdings · equity $10,475.92 vs prior close $10,475.92 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,475.92 | ▲ close $10,475.92 vs 09:30 $10,475.92 (session +0.00) | 16:00 close · cash $10,475.92 · no lots left · equity $10,475.92. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `CBRS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBNX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `COHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AAOI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEVA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EROC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `FNKO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WEN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `HAL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KRMN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OBE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VIR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SDGR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SIBN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `STX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NMAX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ON` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NTAP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PANW` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IOT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SIMO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
