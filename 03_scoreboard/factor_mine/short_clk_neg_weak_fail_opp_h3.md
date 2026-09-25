# Factor mine action — `short_clk_neg_weak_fail_opp_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `opp_rvol` · size `leftover` · sell `list` · S-boost `none` · Clock-B #4 ∩ Theme Radar T−1 oppset

Cash book **-5.80%** ($9,420) · signal-only (no cash/fees) was -53.45%. Starts YES **0/30**. Fills 104 · skips 135 · realized $-2597.52.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: Theme Radar T−1 relative volume (Clock-B opportunity-set; not same-day RelVol).
- Must-have: Clock-B #4: a red catalyst, peer/sector (or tape) weakness, and a failed recovery (last bar red or MACD down).
- Must-have: Theme Radar Clock-B opportunity-set: T−1 gap or RelVol (or week move) flagged — not today's Gap/RelVol.

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- Sort the keepers by Theme Radar T−1 relative volume (Clock-B opportunity-set; not same-day RelVol) and keep the top 8.
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
- **Gate** `clk_neg_weak_fail=True,oppset=True` · **rank** `opp_rvol` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,402.51.

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
| 2026-08-14 09:30 ET | **SHORT** | `CLBT` | 57 | $10.83 | $2.20 | — | $10,615.11 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; 🔵; ⚪; ret5=-30.1; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `SECZ` | 107 | $5.84 | $2.36 | — | $11,237.64 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; 🔵; ⚪; ret5=-20.7; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `STUB` | 81 | $7.66 | $2.27 | — | $11,855.82 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; 🔵; ⚪; ret5=-13.5; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `BIRK` | 15 | $39.75 | $2.07 | — | $12,450.00 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+10.2; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `STNE` | 63 | $9.89 | $2.22 | — | $13,070.85 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ⚪; ret5=-7.7; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `DLO` | 40 | $15.28 | $2.15 | — | $13,679.91 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ⚪; ret5=-0.1; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `DAVE` | 1 | $330.91 | $2.02 | — | $14,008.80 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list flatten; 🔵; ⚪; ret5=-8.6; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `CELC` | 7 | $83.04 | $2.05 | — | $14,588.03 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list earn_react; ret5=-7.1; leftover $625.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,588.03 | ▼ close $9,961.03 vs 09:30 $10,000.00 (session -21.64) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,588.03 | ▲ 09:30 equity $9,970.38 vs yday $9,961.03 (+9.35) | — | — |
| 2026-08-17 09:30 ET | **SHORT** | `CAPR` | 362 | $6.87 | $4.82 | — | $17,070.15 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+62.6; leftover $2492.59 | — |
| 2026-08-17 09:30 ET | **SHORT** | `CDNL` | 62 | $39.85 | $2.27 | — | $19,538.58 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $2492.59 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,538.58 | ▲ close $9,964.07 vs 09:30 $9,970.38 (session +0.78) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,538.58 | ▼ 09:30 equity $9,835.70 vs yday $9,964.07 (-128.37) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,538.58 | ▼ close $9,677.75 vs 09:30 $9,835.70 (session -157.95) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,538.58 | ▼ 09:30 equity $9,643.38 vs yday $9,677.75 (-34.37) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `CLBT` | 57 | $10.85 | $2.16 | $-5.50 | $18,917.97 | ▼ -5.50 after sell → book $9,641.22; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `SECZ` | 107 | $5.83 | $2.31 | $-3.60 | $18,291.85 | ▼ -3.60 after sell → book $9,638.91; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `STUB` | 81 | $7.12 | $2.23 | $+39.23 | $17,712.89 | ▲ +39.23 after sell → book $9,636.67; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `BIRK` | 15 | $37.50 | $2.04 | $+29.64 | $17,148.36 | ▲ +29.64 after sell → book $9,634.64; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `STNE` | 63 | $9.26 | $2.18 | $+35.29 | $16,562.80 | ▲ +35.29 after sell → book $9,632.46; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `DLO` | 40 | $13.64 | $2.11 | $+61.34 | $16,015.09 | ▲ +61.34 after sell → book $9,630.35; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `DAVE` | 1 | $334.00 | $1.99 | $-7.11 | $15,679.10 | ▼ -7.11 after sell → book $9,628.36; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `CELC` | 7 | $95.50 | $2.01 | $-91.28 | $15,008.58 | ▼ -91.28 after sell → book $9,626.34; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,008.58 | ▼ close $9,433.36 vs 09:30 $9,643.38 (session -192.98) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,008.58 | ▲ 09:30 equity $9,561.60 vs yday $9,433.36 (+128.24) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `CAPR` | 362 | $7.66 | $4.67 | $-295.47 | $12,230.99 | ▼ -295.47 after sell → book $9,556.93; vs 09:30 mark -4.67 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `CDNL` | 62 | $43.13 | $2.18 | $-207.81 | $9,554.76 | ▼ -207.81 after sell → book $9,554.76; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 111 | $21.40 | $2.43 | — | $11,927.73 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-25.2; leftover $2388.69 | — |
| 2026-08-20 09:30 ET | **SHORT** | `LZB` | 71 | $33.61 | $2.30 | — | $14,311.74 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-17.4; leftover $2388.69 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,311.74 | ▲ close $9,573.83 vs 09:30 $9,561.60 (session +23.80) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,311.74 | ▼ 09:30 equity $9,533.07 vs yday $9,573.83 (-40.76) | — | — |
| 2026-08-21 09:30 ET | **SHORT** | `EYPT` | 173 | $5.48 | $2.57 | — | $15,257.21 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-59.9; leftover $953.31 | — |
| 2026-08-21 09:30 ET | **SHORT** | `YSS` | 102 | $9.26 | $2.35 | — | $16,199.38 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-20.1; leftover $953.31 | — |
| 2026-08-21 09:30 ET | **SHORT** | `FLO` | 138 | $6.90 | $2.46 | — | $17,149.12 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list earn_react; ret5=-5.7; leftover $953.31 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SHAZ` | 15 | $61.46 | $2.08 | — | $18,068.94 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-15.5; leftover $953.31 | — |
| 2026-08-21 09:30 ET | **SHORT** | `EVMN` | 69 | $13.74 | $2.24 | — | $19,014.75 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-3.6; leftover $953.31 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,014.75 | ▲ close $9,637.89 vs 09:30 $9,533.07 (session +116.53) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,014.75 | ▲ 09:30 equity $9,771.77 vs yday $9,637.89 (+133.88) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,014.75 | ▼ close $9,766.05 vs 09:30 $9,771.77 (session -5.72) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,014.75 | ▼ 09:30 equity $9,679.52 vs yday $9,766.05 (-86.53) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 111 | $20.90 | $2.32 | $+50.75 | $16,692.53 | ▲ +50.75 after sell → book $9,677.20; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `LZB` | 71 | $32.33 | $2.20 | $+86.38 | $14,394.90 | ▲ +86.38 after sell → book $9,675.00; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `CAPR` | 667 | $7.25 | $8.89 | — | $19,221.76 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $4837.50 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,221.76 | ▼ close $9,027.72 vs 09:30 $9,679.52 (session -638.39) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,221.76 | ▲ 09:30 equity $9,034.22 vs yday $9,027.72 (+6.50) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `EYPT` | 173 | $5.03 | $2.51 | $+72.77 | $18,349.06 | ▲ +72.77 after sell → book $9,031.71; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `YSS` | 102 | $9.20 | $2.30 | $+1.48 | $17,408.37 | ▲ +1.48 after sell → book $9,029.42; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `FLO` | 138 | $7.13 | $2.40 | $-36.61 | $16,422.02 | ▼ -36.61 after sell → book $9,027.01; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SHAZ` | 15 | $59.19 | $2.04 | $+29.93 | $15,532.14 | ▲ +29.93 after sell → book $9,024.98; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `EVMN` | 69 | $14.17 | $2.20 | $-34.11 | $14,552.21 | ▼ -34.11 after sell → book $9,022.78; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `AVEX` | 128 | $17.51 | $2.47 | — | $16,791.02 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $2255.70 | — |
| 2026-08-26 09:30 ET | **SHORT** | `FLNC` | 202 | $11.12 | $2.72 | — | $19,034.54 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $2255.70 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,034.54 | ▼ close $8,205.74 vs 09:30 $9,034.22 (session -811.85) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,034.54 | ▲ 09:30 equity $8,218.73 vs yday $8,205.74 (+12.99) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,034.54 | ▼ close $7,614.38 vs 09:30 $8,218.73 (session -604.35) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,034.54 | ▲ 09:30 equity $7,868.09 vs yday $7,614.38 (+253.71) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `CAPR` | 667 | $9.73 | $8.60 | $-1671.65 | $12,536.02 | ▼ -1,671.65 after sell → book $7,859.48; vs 09:30 mark -8.61 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `QFIN` | 53 | $9.15 | $2.18 | — | $13,018.79 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-19.9; leftover $491.22 | — |
| 2026-08-28 09:30 ET | **SHORT** | `BHVN` | 30 | $15.88 | $2.11 | — | $13,493.08 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+19.4; leftover $491.22 | — |
| 2026-08-28 09:30 ET | **SHORT** | `GENB` | 31 | $15.77 | $2.12 | — | $13,979.83 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-1.4; leftover $491.22 | — |
| 2026-08-28 09:30 ET | **SHORT** | `JKS` | 36 | $13.37 | $2.13 | — | $14,459.02 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-14.9; leftover $491.22 | — |
| 2026-08-28 09:30 ET | **SHORT** | `ZYME` | 16 | $28.91 | $2.07 | — | $14,919.51 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=+9.2; leftover $491.22 | — |
| 2026-08-28 09:30 ET | **SHORT** | `QBTS` | 27 | $17.56 | $2.10 | — | $15,391.52 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-4.8; leftover $491.22 | — |
| 2026-08-28 09:30 ET | **SHORT** | `MNRO` | 39 | $12.38 | $2.14 | — | $15,872.20 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; ret5=+2.4; leftover $491.22 | — |
| 2026-08-28 09:30 ET | **SHORT** | `IREN` | 13 | $37.65 | $2.06 | — | $16,359.53 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list earn_react; ret5=-4.9; leftover $491.22 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,359.53 | ▲ close $8,121.06 vs 09:30 $7,868.09 (session +278.50) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,359.53 | ▲ 09:30 equity $8,168.52 vs yday $8,121.06 (+47.46) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `AVEX` | 128 | $17.63 | $2.37 | $-20.21 | $14,100.51 | ▼ -20.21 after sell → book $8,166.15; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `FLNC` | 202 | $10.82 | $2.61 | $+55.27 | $11,912.27 | ▲ +55.27 after sell → book $8,163.54; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,912.27 | ▲ close $8,165.54 vs 09:30 $8,168.52 (session +2.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,912.27 | ▲ 09:30 equity $8,209.47 vs yday $8,165.54 (+43.93) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,912.27 | ▼ close $8,173.51 vs 09:30 $8,209.47 (session -35.96) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,912.27 | ▲ 09:30 equity $8,178.65 vs yday $8,173.51 (+5.14) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `QFIN` | 53 | $8.38 | $2.15 | $+36.48 | $11,465.98 | ▲ +36.48 after sell → book $8,176.50; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `BHVN` | 30 | $15.97 | $2.08 | $-6.89 | $10,984.80 | ▼ -6.89 after sell → book $8,174.42; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `GENB` | 31 | $15.75 | $2.08 | $-3.58 | $10,494.47 | ▼ -3.58 after sell → book $8,172.34; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `JKS` | 36 | $12.45 | $2.10 | $+28.89 | $10,044.17 | ▲ +28.89 after sell → book $8,170.24; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `ZYME` | 16 | $30.00 | $2.04 | $-21.55 | $9,562.13 | ▼ -21.55 after sell → book $8,168.20; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `QBTS` | 27 | $16.28 | $2.07 | $+30.41 | $9,120.53 | ▲ +30.41 after sell → book $8,166.13; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `MNRO` | 39 | $12.54 | $2.11 | $-10.49 | $8,629.36 | ▼ -10.49 after sell → book $8,164.02; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `IREN` | 13 | $35.80 | $2.03 | $+19.96 | $8,161.99 | ▲ +19.96 after sell → book $8,161.99; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,161.99 | ▲ close $8,161.99 vs 09:30 $8,178.65 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,161.99 | ▲ 09:30 equity $8,161.99 vs yday $8,161.99 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **SHORT** | `ALMS` | 196 | $10.38 | $2.68 | — | $10,192.81 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-56.2; leftover $2040.50 | — |
| 2026-09-03 09:30 ET | **SHORT** | `GMRS` | 159 | $12.83 | $2.57 | — | $12,230.22 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=-0.2; leftover $2040.50 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,230.22 | ▼ close $7,871.47 vs 09:30 $8,161.99 (session -285.28) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,230.22 | ▲ 09:30 equity $7,916.03 vs yday $7,871.47 (+44.56) | — | — |
| 2026-09-04 09:30 ET | **SHORT** | `AUR` | 210 | $6.26 | $2.79 | — | $13,543.08 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+11.1; leftover $1319.34 | — |
| 2026-09-04 09:30 ET | **SHORT** | `SCZM` | 131 | $10.03 | $2.45 | — | $14,854.55 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=+4.0; leftover $1319.34 | — |
| 2026-09-04 09:30 ET | **SHORT** | `HQ` | 82 | $15.90 | $2.30 | — | $16,156.06 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; ret5=-4.1; leftover $1319.34 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,156.06 | ▲ close $7,930.86 vs 09:30 $7,916.03 (session +22.37) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,156.06 | ▲ 09:30 equity $7,943.12 vs yday $7,930.86 (+12.26) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,156.06 | ▼ close $7,891.53 vs 09:30 $7,943.12 (session -51.59) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,156.06 | ▲ 09:30 equity $7,934.46 vs yday $7,891.53 (+42.93) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `ALMS` | 196 | $10.49 | $2.58 | $-27.80 | $14,097.44 | ▼ -27.80 after sell → book $7,931.88; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `GMRS` | 159 | $13.80 | $2.47 | $-159.26 | $11,900.77 | ▼ -159.26 after sell → book $7,929.41; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,900.77 | ▼ close $7,851.70 vs 09:30 $7,934.46 (session -77.71) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,900.77 | ▲ 09:30 equity $7,992.98 vs yday $7,851.70 (+141.28) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `AUR` | 210 | $6.35 | $2.71 | $-23.35 | $10,564.56 | ▼ -23.35 after sell → book $7,990.27; vs 09:30 mark -2.71 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `SCZM` | 131 | $9.93 | $2.38 | $+8.27 | $9,261.35 | ▲ +8.27 after sell → book $7,987.89; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `HQ` | 82 | $15.53 | $2.24 | $+25.81 | $7,985.66 | ▲ +25.81 after sell → book $7,985.66; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,985.66 | ▲ close $7,985.66 vs 09:30 $7,992.98 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,985.66 | ▲ 09:30 equity $7,985.66 vs yday $7,985.66 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **SHORT** | `AXGN` | 13 | $42.48 | $2.06 | — | $8,535.83 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-13.4; leftover $570.40 | — |
| 2026-09-11 09:30 ET | **SHORT** | `TYRA` | 24 | $23.63 | $2.10 | — | $9,100.85 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-6.3; leftover $570.40 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BHVN` | 43 | $13.03 | $2.15 | — | $9,658.99 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-18.5; leftover $570.40 | — |
| 2026-09-11 09:30 ET | **SHORT** | `REAX` | 30 | $18.56 | $2.12 | — | $10,213.67 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; ret5=+2.8; leftover $570.40 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CPRT` | 17 | $32.01 | $2.08 | — | $10,755.77 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-4.4; leftover $570.40 | — |
| 2026-09-11 09:30 ET | **SHORT** | `LPTH` | 60 | $9.37 | $2.21 | — | $11,315.76 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+1.5; leftover $570.40 | — |
| 2026-09-11 09:30 ET | **SHORT** | `TGB` | 71 | $8.02 | $2.24 | — | $11,882.94 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-3.6; leftover $570.40 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,882.94 | ▲ close $8,012.12 vs 09:30 $7,985.66 (session +41.42) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,882.94 | ▲ 09:30 equity $8,056.23 vs yday $8,012.12 (+44.11) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,882.94 | ▼ close $7,931.65 vs 09:30 $8,056.23 (session -124.58) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,882.94 | ▲ 09:30 equity $7,942.69 vs yday $7,931.65 (+11.04) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,882.94 | ▼ close $7,927.63 vs 09:30 $7,942.69 (session -15.06) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,882.94 | ▲ 09:30 equity $7,931.08 vs yday $7,927.63 (+3.45) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `AXGN` | 13 | $44.87 | $2.03 | $-35.16 | $11,297.60 | ▼ -35.16 after sell → book $7,929.05; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `TYRA` | 24 | $25.58 | $2.06 | $-50.96 | $10,681.62 | ▼ -50.96 after sell → book $7,926.99; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BHVN` | 43 | $12.34 | $2.12 | $+25.40 | $10,148.88 | ▲ +25.40 after sell → book $7,924.87; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `REAX` | 30 | $19.03 | $2.08 | $-18.30 | $9,575.90 | ▼ -18.30 after sell → book $7,922.79; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CPRT` | 17 | $30.57 | $2.04 | $+20.36 | $9,054.17 | ▲ +20.36 after sell → book $7,920.75; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `LPTH` | 60 | $9.40 | $2.17 | $-6.18 | $8,488.00 | ▼ -6.18 after sell → book $7,918.58; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `TGB` | 71 | $8.02 | $2.20 | $-4.44 | $7,916.38 | ▼ -4.44 after sell → book $7,916.38; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SHORT** | `PLAY` | 115 | $6.86 | $2.39 | — | $8,702.89 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-22.4; leftover $791.64 | — |
| 2026-09-16 09:30 ET | **SHORT** | `HQ` | 61 | $12.89 | $2.21 | — | $9,486.97 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=-18.2; leftover $791.64 | — |
| 2026-09-16 09:30 ET | **SHORT** | `FWDI` | 128 | $6.15 | $2.43 | — | $10,271.74 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-11.4; leftover $791.64 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 42 | $18.61 | $2.16 | — | $11,051.20 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $791.64 | — |
| 2026-09-16 09:30 ET | **SHORT** | `SION` | 113 | $6.95 | $2.38 | — | $11,834.17 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-5.8; leftover $791.64 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,834.17 | ▼ close $7,721.75 vs 09:30 $7,931.08 (session -183.06) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,834.17 | ▼ 09:30 equity $7,628.98 vs yday $7,721.75 (-92.77) | — | — |
| 2026-09-17 09:30 ET | **SHORT** | `BTGO` | 96 | $6.56 | $2.32 | — | $12,461.61 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-14.6; leftover $635.75 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 79 | $7.95 | $2.27 | — | $13,087.40 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $635.75 | — |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 7 | $81.00 | $2.05 | — | $13,652.35 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-3.0; leftover $635.75 | — |
| 2026-09-17 09:30 ET | **SHORT** | `RCAT` | 85 | $7.39 | $2.29 | — | $14,278.64 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; ret5=-12.7; leftover $635.75 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BRUN` | 40 | $15.87 | $2.15 | — | $14,911.29 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $635.75 | — |
| 2026-09-17 09:30 ET | **SHORT** | `FTAI` | 3 | $196.50 | $2.04 | — | $15,498.76 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+2.5; leftover $635.75 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,498.76 | ▲ close $7,746.11 vs 09:30 $7,628.98 (session +130.23) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,498.76 | ▼ 09:30 equity $7,628.16 vs yday $7,746.11 (-117.95) | — | — |
| 2026-09-18 09:30 ET | **SHORT** | `FLNC` | 506 | $7.54 | $6.75 | — | $19,304.72 | — | Clock-B #4 ∩ Theme Radar T−1 oppset; gate clk_neg_weak_fail=True,oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-20.9; leftover $3814.08 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,304.72 | ▲ close $7,673.16 vs 09:30 $7,628.16 (session +51.75) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,304.72 | ▼ 09:30 equity $7,494.78 vs yday $7,673.16 (-178.38) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `PLAY` | 115 | $6.68 | $2.33 | $+15.98 | $18,534.18 | ▲ +15.98 after sell → book $7,492.44; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `HQ` | 61 | $13.41 | $2.17 | $-36.11 | $17,714.00 | ▼ -36.11 after sell → book $7,490.27; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `FWDI` | 128 | $8.22 | $2.37 | $-269.76 | $16,659.47 | ▼ -269.76 after sell → book $7,487.90; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 42 | $22.11 | $2.12 | $-151.27 | $15,728.73 | ▼ -151.27 after sell → book $7,485.78; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `SION` | 113 | $6.00 | $2.33 | $+102.64 | $15,048.40 | ▲ +102.64 after sell → book $7,483.45; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,048.40 | ▲ close $7,500.42 vs 09:30 $7,494.78 (session +16.97) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,048.40 | ▼ 09:30 equity $7,446.70 vs yday $7,500.42 (-53.72) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BTGO` | 96 | $7.81 | $2.28 | $-124.60 | $14,296.36 | ▼ -124.60 after sell → book $7,444.42; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 79 | $8.28 | $2.23 | $-30.17 | $13,640.41 | ▼ -30.17 after sell → book $7,442.19; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **COVER** | `RCAT` | 85 | $7.02 | $2.25 | $+27.34 | $13,041.47 | ▲ +27.34 after sell → book $7,439.95; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,041.47 | ▲ close $7,551.27 vs 09:30 $7,446.70 (session +111.32) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,041.47 | ▼ 09:30 equity $7,415.16 vs yday $7,551.27 (-136.11) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `LEN` | 7 | $82.00 | $2.01 | $-11.06 | $12,465.46 | ▼ -11.06 after sell → book $7,413.15; vs 09:30 mark -2.01 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `BRUN` | 40 | $17.10 | $2.11 | $-53.46 | $11,779.35 | ▼ -53.46 after sell → book $7,411.04; vs 09:30 mark -2.11 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `FTAI` | 3 | $187.73 | $2.00 | $+22.28 | $11,214.16 | ▲ +22.28 after sell → book $7,409.04; vs 09:30 mark -2.00 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `FLNC` | 506 | $7.52 | $6.53 | $-5.68 | $7,402.51 | ▼ -5.68 after sell → book $7,402.51; vs 09:30 mark -6.53 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,402.51 | ▲ close $7,402.51 vs 09:30 $7,415.16 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,402.51 | ▲ 09:30 equity $7,402.51 vs yday $7,402.51 (+0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,402.51 | ▲ close $7,402.51 vs 09:30 $7,402.51 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,420.24 | ▲ 09:30 equity $9,420.24 vs yday $9,420.24 (+0.00) | 09:30 open · cash $9,420.24 · no holdings · equity $9,420.24 vs prior close $9,420.24 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,420.24 | ▲ close $9,420.24 vs 09:30 $9,420.24 (session +0.00) | 16:00 close · cash $9,420.24 · no lots left · equity $9,420.24. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `CLBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SECZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `STUB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BIRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `STNE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DAVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CELC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CLBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SECZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BIRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `STNE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DAVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CELC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CDNL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALOY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CDNL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `EYPT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MRCY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BSBR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `LZB` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `WYFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `LZB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `WYFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `LZB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `YSS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `SHAZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `EVMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `YSS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `FLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `SHAZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `EVMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AVEX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `FLNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `AVEX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `QFIN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GENB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `JKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `QBTS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MNRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `QFIN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GENB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `JKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `QBTS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MNRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MNSO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `REAX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MMED` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `SCZM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `ALMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `GMRS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ALMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `GMRS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `AUR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `HQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `HQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `DYN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SGML` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TGB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `EVMN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SHOE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HELP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AXGN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `TYRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CPRT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `LPTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AXGN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CPRT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `LPTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RUM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `HQ` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `PLAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `HQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `FWDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BBNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SION` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PLAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `HQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `FWDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BTGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BULL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `LEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RCAT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `FTAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BTGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BULL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `LEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `RCAT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `FTAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `FLNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `LEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BRUN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FTAI` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
