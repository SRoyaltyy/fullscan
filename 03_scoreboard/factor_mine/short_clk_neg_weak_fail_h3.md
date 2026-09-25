# Factor mine action — `short_clk_neg_weak_fail_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · Clock-B #4 neg catalyst + weakness + failed recovery

Cash book **+0.73%** ($10,073) · signal-only (no cash/fees) was -21.45%. Starts YES **8/30**. Fills 209 · skips 270 · realized $-967.53.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: Clock-B #4: a red catalyst, peer/sector (or tape) weakness, and a failed recovery (last bar red or MACD down).

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
- **Gate** `clk_neg_weak_fail=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $15,621.66.

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
| 2026-08-13 09:30 ET | **SHORT** | `TGTX` | 33 | $49.70 | $2.16 | — | $11,637.94 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **SHORT** | `SLS` | 142 | $11.70 | $2.50 | — | $13,296.84 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **SHORT** | `HIMS` | 56 | $29.74 | $2.23 | — | $14,960.06 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,960.06 | ▲ close $10,011.80 vs 09:30 $10,000.00 (session +18.68) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,960.06 | ▼ 09:30 equity $10,006.95 vs yday $10,011.80 (-4.85) | — | — |
| 2026-08-14 09:30 ET | **SHORT** | `DAVE` | 1 | $330.91 | $2.02 | — | $15,288.95 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $625.43 | — |
| 2026-08-14 09:30 ET | **SHORT** | `MARA` | 69 | $9.01 | $2.24 | — | $15,908.40 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $625.43 | — |
| 2026-08-14 09:30 ET | **SHORT** | `AIRS` | 185 | $3.37 | $2.60 | — | $16,529.25 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable; ret5=-29.1; leftover $625.43 | — |
| 2026-08-14 09:30 ET | **SHORT** | `NCMI` | 232 | $2.69 | $3.06 | — | $17,150.27 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $625.43 | — |
| 2026-08-14 09:30 ET | **SHORT** | `BIRK` | 15 | $39.75 | $2.07 | — | $17,744.45 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer; 🔵; ret5=+10.2; leftover $625.43 | — |
| 2026-08-14 09:30 ET | **SHORT** | `CLBT` | 57 | $10.83 | $2.20 | — | $18,359.56 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ⚪; ret5=-30.1; leftover $625.43 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OTLK` | 741 | $0.84 | $8.63 | — | $18,976.34 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover,earn_react; 🔵; ret5=-8.6; leftover $625.43 | — |
| 2026-08-14 09:30 ET | **SHORT** | `SECZ` | 107 | $5.84 | $2.36 | — | $19,598.86 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ⚪; ret5=-20.7; leftover $625.43 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,598.86 | ▼ close $9,946.14 vs 09:30 $10,006.95 (session -35.63) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,598.86 | ▲ 09:30 equity $9,946.77 vs yday $9,946.14 (+0.63) | — | — |
| 2026-08-17 09:30 ET | **SHORT** | `TMC` | 153 | $4.05 | $2.50 | — | $20,216.01 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $621.67 | — |
| 2026-08-17 09:30 ET | **SHORT** | `CDNL` | 15 | $39.85 | $2.07 | — | $20,811.69 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $621.67 | — |
| 2026-08-17 09:30 ET | **SHORT** | `CAPR` | 90 | $6.87 | $2.30 | — | $21,427.69 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer,yday_mover; ret5=+62.6; leftover $621.67 | — |
| 2026-08-17 09:30 ET | **SHORT** | `INV` | 383 | $1.62 | $5.03 | — | $22,043.11 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $621.67 | — |
| 2026-08-17 09:30 ET | **SHORT** | `KLC` | 237 | $2.62 | $3.12 | — | $22,660.93 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ⚪; ret5=-49.7; leftover $621.67 | — |
| 2026-08-17 09:30 ET | **SHORT** | `AMPG` | 151 | $4.09 | $2.50 | — | $23,276.03 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ⚪; ret5=-31.1; leftover $621.67 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ENHA` | 309 | $2.01 | $4.06 | — | $23,893.05 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ⚪; ret5=-26.0; leftover $621.67 | — |
| 2026-08-17 09:30 ET | **SHORT** | `MRLN` | 165 | $3.75 | $2.54 | — | $24,509.26 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ⚪; ret5=-15.4; leftover $621.67 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,509.26 | ▲ close $10,337.71 vs 09:30 $9,946.77 (session +415.06) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,509.26 | ▲ 09:30 equity $10,518.15 vs yday $10,337.71 (+180.44) | — | — |
| 2026-08-18 09:30 ET | **COVER** | `TGTX` | 33 | $49.28 | $2.09 | $+9.61 | $22,880.93 | ▲ +9.61 after sell → book $10,516.06; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **COVER** | `SLS` | 142 | $12.66 | $2.42 | $-141.23 | $21,080.80 | ▼ -141.23 after sell → book $10,513.64; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **COVER** | `HIMS` | 56 | $27.85 | $2.16 | $+101.46 | $19,519.04 | ▲ +101.46 after sell → book $10,511.48; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,519.04 | ▼ close $10,501.04 vs 09:30 $10,518.15 (session -10.45) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,519.04 | ▼ 09:30 equity $10,435.73 vs yday $10,501.04 (-65.31) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `DAVE` | 1 | $334.00 | $1.99 | $-7.11 | $19,183.05 | ▼ -7.11 after sell → book $10,433.74; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `MARA` | 69 | $8.91 | $2.20 | $+2.47 | $18,566.06 | ▲ +2.47 after sell → book $10,431.54; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `AIRS` | 185 | $2.71 | $2.54 | $+116.95 | $18,062.16 | ▲ +116.95 after sell → book $10,429.00; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `NCMI` | 232 | $2.56 | $2.99 | $+24.11 | $17,465.25 | ▲ +24.11 after sell → book $10,426.00; vs 09:30 mark -3.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `BIRK` | 15 | $37.50 | $2.04 | $+29.64 | $16,900.72 | ▲ +29.64 after sell → book $10,423.97; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `CLBT` | 57 | $10.85 | $2.16 | $-5.50 | $16,280.11 | ▼ -5.50 after sell → book $10,421.81; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OTLK` | 741 | $0.71 | $7.47 | $+84.68 | $15,748.01 | ▲ +84.68 after sell → book $10,414.34; vs 09:30 mark -7.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `SECZ` | 107 | $5.83 | $2.31 | $-3.60 | $15,121.89 | ▼ -3.60 after sell → book $10,412.03; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,121.89 | ▼ close $10,283.30 vs 09:30 $10,435.73 (session -128.73) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,121.89 | ▼ 09:30 equity $10,280.51 vs yday $10,283.30 (-2.79) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `TMC` | 153 | $3.92 | $2.45 | $+14.94 | $14,519.68 | ▲ +14.94 after sell → book $10,278.06; vs 09:30 mark -2.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `CDNL` | 15 | $43.13 | $2.04 | $-53.31 | $13,870.69 | ▼ -53.31 after sell → book $10,276.02; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `CAPR` | 90 | $7.66 | $2.26 | $-75.66 | $13,179.03 | ▼ -75.66 after sell → book $10,273.76; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `INV` | 383 | $1.55 | $4.94 | $+16.84 | $12,580.44 | ▲ +16.84 after sell → book $10,268.82; vs 09:30 mark -4.94 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `KLC` | 237 | $2.88 | $3.06 | $-67.80 | $11,894.83 | ▼ -67.80 after sell → book $10,265.77; vs 09:30 mark -3.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `AMPG` | 151 | $3.54 | $2.44 | $+78.11 | $11,357.84 | ▲ +78.11 after sell → book $10,263.32; vs 09:30 mark -2.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ENHA` | 309 | $1.78 | $3.99 | $+63.02 | $10,803.84 | ▲ +63.02 after sell → book $10,259.34; vs 09:30 mark -3.98 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `MRLN` | 165 | $3.30 | $2.48 | $+69.23 | $10,256.85 | ▲ +69.23 after sell → book $10,256.85; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 29 | $21.40 | $2.11 | — | $10,875.34 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ret5=-25.2; leftover $641.05 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 144 | $4.43 | $2.47 | — | $11,510.78 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ret5=-23.1; leftover $641.05 | — |
| 2026-08-20 09:30 ET | **SHORT** | `DVLT` | 2136 | $0.30 | $13.20 | — | $12,138.39 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ret5=-3.2; leftover $641.05 | — |
| 2026-08-20 09:30 ET | **SHORT** | `LZB` | 19 | $33.61 | $2.08 | — | $12,774.89 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ret5=-17.4; leftover $641.05 | — |
| 2026-08-20 09:30 ET | **SHORT** | `SAFX` | 1810 | $0.35 | $12.17 | — | $13,403.47 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ret5=-29.4; leftover $641.05 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $14,010.45 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list earn_react; 🔵; ret5=+5.0; leftover $641.05 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEG` | 71 | $9.01 | $2.24 | — | $14,647.92 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $641.05 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ALVO` | 164 | $3.89 | $2.54 | — | $15,283.34 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list earn_react; 🔵; ret5=-0.5; leftover $641.05 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,283.34 | ▼ close $10,184.78 vs 09:30 $10,280.51 (session -33.19) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,283.34 | ▼ 09:30 equity $10,148.48 vs yday $10,184.78 (-36.30) | — | — |
| 2026-08-21 09:30 ET | **SHORT** | `ENHA` | 423 | $1.71 | $5.56 | — | $16,001.11 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $724.89 | — |
| 2026-08-21 09:30 ET | **SHORT** | `YSS` | 78 | $9.26 | $2.27 | — | $16,721.13 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=-20.1; leftover $724.89 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SHAZ` | 11 | $61.46 | $2.06 | — | $17,395.13 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=-15.5; leftover $724.89 | — |
| 2026-08-21 09:30 ET | **SHORT** | `EYPT` | 132 | $5.48 | $2.44 | — | $18,116.05 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ret5=-59.9; leftover $724.89 | — |
| 2026-08-21 09:30 ET | **SHORT** | `EVMN` | 52 | $13.74 | $2.19 | — | $18,828.34 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ret5=-3.6; leftover $724.89 | — |
| 2026-08-21 09:30 ET | **SHORT** | `EOSE` | 204 | $3.54 | $2.70 | — | $19,547.81 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ret5=-16.8; leftover $724.89 | — |
| 2026-08-21 09:30 ET | **SHORT** | `FLO` | 105 | $6.90 | $2.35 | — | $20,269.96 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list earn_react; ret5=-5.7; leftover $724.89 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,269.96 | ▼ close $10,101.83 vs 09:30 $10,148.48 (session -27.10) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,269.96 | ▲ 09:30 equity $10,112.52 vs yday $10,101.83 (+10.69) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,269.96 | ▼ close $10,098.13 vs 09:30 $10,112.52 (session -14.39) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,269.96 | ▼ 09:30 equity $10,035.45 vs yday $10,098.13 (-62.68) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 29 | $20.90 | $2.08 | $+10.31 | $19,661.78 | ▲ +10.31 after sell → book $10,033.37; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 144 | $4.42 | $2.42 | $-3.46 | $19,022.88 | ▼ -3.46 after sell → book $10,030.95; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `DVLT` | 2136 | $0.31 | $13.03 | $-47.59 | $18,347.69 | ▼ -47.59 after sell → book $10,017.92; vs 09:30 mark -13.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `LZB` | 19 | $32.33 | $2.05 | $+20.19 | $17,731.37 | ▲ +20.19 after sell → book $10,015.87; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 13 | $43.63 | $2.03 | $+37.77 | $17,162.15 | ▲ +37.77 after sell → book $10,013.84; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEG` | 71 | $9.23 | $2.20 | $-20.07 | $16,504.62 | ▼ -20.07 after sell → book $10,011.64; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ALVO` | 164 | $5.24 | $2.48 | $-226.42 | $15,642.78 | ▼ -226.42 after sell → book $10,009.16; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `CAPR` | 345 | $7.25 | $4.60 | — | $18,139.43 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $2502.29 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 14 | $175.01 | $2.13 | — | $20,587.44 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list earn_react; ret5=-7.0; leftover $2502.29 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,587.44 | ▼ close $9,723.98 vs 09:30 $10,035.45 (session -278.45) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,587.44 | ▲ 09:30 equity $9,742.77 vs yday $9,723.98 (+18.79) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `SAFX` | 1810 | $0.35 | $11.82 | $-22.17 | $19,936.69 | ▼ -22.17 after sell → book $9,730.95; vs 09:30 mark -11.82 | dropped from list after 4 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ENHA` | 423 | $1.63 | $5.46 | $+22.83 | $19,241.74 | ▲ +22.83 after sell → book $9,725.49; vs 09:30 mark -5.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `YSS` | 78 | $9.20 | $2.22 | $+0.19 | $18,521.92 | ▲ +0.19 after sell → book $9,723.27; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SHAZ` | 11 | $59.19 | $2.02 | $+20.89 | $17,868.81 | ▲ +20.89 after sell → book $9,721.25; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `EYPT` | 132 | $5.03 | $2.39 | $+54.58 | $17,202.46 | ▲ +54.58 after sell → book $9,718.86; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `EVMN` | 52 | $14.17 | $2.15 | $-26.69 | $16,463.48 | ▼ -26.69 after sell → book $9,716.72; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `EOSE` | 204 | $3.50 | $2.63 | $+3.85 | $15,747.86 | ▲ +3.85 after sell → book $9,714.08; vs 09:30 mark -2.64 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `FLO` | 105 | $7.13 | $2.31 | $-28.81 | $14,996.91 | ▼ -28.81 after sell → book $9,711.78; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `FLNC` | 54 | $11.12 | $2.19 | — | $15,595.20 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $606.99 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AVEX` | 34 | $17.51 | $2.13 | — | $16,188.41 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $606.99 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ASPN` | 116 | $5.20 | $2.38 | — | $16,789.23 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer; 🔵; ret5=-6.3; leftover $606.99 | — |
| 2026-08-26 09:30 ET | **SHORT** | `IRDM` | 12 | $46.96 | $2.06 | — | $17,350.69 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer; 🔵; ret5=-3.9; leftover $606.99 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BKSY` | 24 | $24.94 | $2.10 | — | $17,947.15 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer; ret5=-12.9; leftover $606.99 | — |
| 2026-08-26 09:30 ET | **SHORT** | `FWRD` | 34 | $17.41 | $2.13 | — | $18,536.96 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=-9.2; leftover $606.99 | — |
| 2026-08-26 09:30 ET | **SHORT** | `LI` | 49 | $12.14 | $2.17 | — | $19,129.65 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list earn_react; ret5=+1.2; leftover $606.99 | — |
| 2026-08-26 09:30 ET | **SHORT** | `PLAB` | 16 | $37.26 | $2.07 | — | $19,723.73 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list earn_react; 🔵; ret5=-8.0; leftover $606.99 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,723.73 | ▼ close $9,413.01 vs 09:30 $9,742.77 (session -281.53) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,723.73 | ▼ 09:30 equity $9,407.83 vs yday $9,413.01 (-5.18) | — | — |
| 2026-08-27 09:30 ET | **SHORT** | `AVBP` | 50 | $30.79 | $2.20 | — | $21,261.03 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer,yday_mover; ret5=+3.7; leftover $1567.97 | — |
| 2026-08-27 09:30 ET | **SHORT** | `ALOY` | 135 | $11.53 | $2.47 | — | $22,815.11 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer; ret5=-7.7; leftover $1567.97 | — |
| 2026-08-27 09:30 ET | **SHORT** | `SLQT` | 2958 | $0.53 | $25.10 | — | $24,357.75 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=-29.2; leftover $1567.97 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,357.75 | ▼ close $9,077.77 vs 09:30 $9,407.83 (session -300.29) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,357.75 | ▲ 09:30 equity $9,274.46 vs yday $9,077.77 (+196.69) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `CAPR` | 345 | $9.73 | $4.45 | $-864.65 | $20,996.45 | ▼ -864.65 after sell → book $9,270.01; vs 09:30 mark -4.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 14 | $172.76 | $2.03 | $+27.34 | $18,575.78 | ▲ +27.34 after sell → book $9,267.98; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `PYXS` | 174 | $3.32 | $2.57 | — | $19,150.89 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer; ret5=+6.4; leftover $579.25 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SAFX` | 1586 | $0.36 | $10.84 | — | $19,718.94 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer; ret5=+7.6; leftover $579.25 | — |
| 2026-08-28 09:30 ET | **SHORT** | `XPOF` | 107 | $5.38 | $2.35 | — | $20,292.25 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer; ret5=+6.5; leftover $579.25 | — |
| 2026-08-28 09:30 ET | **SHORT** | `BHVN` | 36 | $15.88 | $2.13 | — | $20,861.79 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer,yday_mover; ret5=+19.4; leftover $579.25 | — |
| 2026-08-28 09:30 ET | **SHORT** | `OPTU` | 579 | $1.00 | $7.59 | — | $21,433.20 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer; ret5=+16.8; leftover $579.25 | — |
| 2026-08-28 09:30 ET | **SHORT** | `ZYME` | 20 | $28.91 | $2.09 | — | $22,009.32 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer; ret5=+9.2; leftover $579.25 | — |
| 2026-08-28 09:30 ET | **SHORT** | `GENB` | 36 | $15.77 | $2.13 | — | $22,574.90 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=-1.4; leftover $579.25 | — |
| 2026-08-28 09:30 ET | **SHORT** | `QFIN` | 63 | $9.15 | $2.22 | — | $23,149.14 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=-19.9; leftover $579.25 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23,149.14 | ▲ close $9,657.54 vs 09:30 $9,274.46 (session +421.49) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23,149.14 | ▲ 09:30 equity $9,703.40 vs yday $9,657.54 (+45.86) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `FLNC` | 54 | $10.82 | $2.15 | $+11.86 | $22,562.71 | ▲ +11.86 after sell → book $9,701.24; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AVEX` | 34 | $17.63 | $2.09 | $-8.30 | $21,961.19 | ▼ -8.30 after sell → book $9,699.15; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ASPN` | 116 | $4.96 | $2.34 | $+23.12 | $21,383.50 | ▲ +23.12 after sell → book $9,696.81; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `IRDM` | 12 | $46.64 | $2.03 | $-0.25 | $20,821.79 | ▼ -0.25 after sell → book $9,694.79; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `BKSY` | 24 | $23.50 | $2.06 | $+30.40 | $20,255.73 | ▲ +30.40 after sell → book $9,692.73; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `FWRD` | 34 | $17.03 | $2.09 | $+8.70 | $19,674.62 | ▲ +8.70 after sell → book $9,690.63; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `LI` | 49 | $12.28 | $2.14 | $-11.17 | $19,070.76 | ▼ -11.17 after sell → book $9,688.50; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `PLAB` | 16 | $28.04 | $2.04 | $+143.41 | $18,620.08 | ▲ +143.41 after sell → book $9,686.46; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,620.08 | ▼ close $9,555.74 vs 09:30 $9,703.40 (session -130.72) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,620.08 | ▲ 09:30 equity $9,630.61 vs yday $9,555.74 (+74.87) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `AVBP` | 50 | $30.10 | $2.14 | $+30.16 | $17,112.94 | ▲ +30.16 after sell → book $9,628.47; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `ALOY` | 135 | $10.00 | $2.40 | $+201.68 | $15,760.55 | ▲ +201.68 after sell → book $9,626.07; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `SLQT` | 2958 | $0.55 | $25.02 | $-97.45 | $14,120.45 | ▼ -97.45 after sell → book $9,601.05; vs 09:30 mark -25.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,120.45 | ▼ close $9,579.74 vs 09:30 $9,630.61 (session -21.30) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,120.45 | ▼ 09:30 equity $9,553.34 vs yday $9,579.74 (-26.40) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `PYXS` | 174 | $3.45 | $2.51 | $-27.70 | $13,517.64 | ▼ -27.70 after sell → book $9,550.82; vs 09:30 mark -2.52 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SAFX` | 1586 | $0.39 | $10.94 | $-61.43 | $12,888.16 | ▼ -61.43 after sell → book $9,539.88; vs 09:30 mark -10.94 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `XPOF` | 107 | $5.03 | $2.31 | $+32.78 | $12,347.64 | ▲ +32.78 after sell → book $9,537.57; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `BHVN` | 36 | $15.97 | $2.10 | $-7.47 | $11,770.62 | ▼ -7.47 after sell → book $9,535.47; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `OPTU` | 579 | $0.93 | $7.14 | $+24.06 | $11,223.27 | ▲ +24.06 after sell → book $9,528.33; vs 09:30 mark -7.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `ZYME` | 20 | $30.00 | $2.05 | $-25.94 | $10,621.22 | ▼ -25.94 after sell → book $9,526.28; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `GENB` | 36 | $15.75 | $2.10 | $-3.51 | $10,052.13 | ▼ -3.51 after sell → book $9,524.19; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `QFIN` | 63 | $8.38 | $2.18 | $+44.12 | $9,522.01 | ▲ +44.12 after sell → book $9,522.01; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,522.01 | ▲ close $9,522.01 vs 09:30 $9,553.34 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,522.01 | ▲ 09:30 equity $9,522.01 vs yday $9,522.01 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **SHORT** | `SAFX` | 1804 | $0.38 | $12.54 | — | $10,189.57 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer; ret5=-2.3; leftover $680.14 | — |
| 2026-09-03 09:30 ET | **SHORT** | `GMRS` | 53 | $12.83 | $2.19 | — | $10,867.38 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer; ret5=-0.2; leftover $680.14 | — |
| 2026-09-03 09:30 ET | **SHORT** | `KLRA` | 42 | $15.95 | $2.15 | — | $11,535.12 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer; 🔵; ret5=-14.0; leftover $680.14 | — |
| 2026-09-03 09:30 ET | **SHORT** | `ALMS` | 65 | $10.38 | $2.22 | — | $12,207.27 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ret5=-56.2; leftover $680.14 | — |
| 2026-09-03 09:30 ET | **SHORT** | `RZLV` | 290 | $2.34 | $3.82 | — | $12,882.05 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ret5=-19.9; leftover $680.14 | — |
| 2026-09-03 09:30 ET | **SHORT** | `EVTL` | 1062 | $0.64 | $10.19 | — | $13,551.55 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=-22.0; leftover $680.14 | — |
| 2026-09-03 09:30 ET | **SHORT** | `REAX` | 36 | $18.40 | $2.14 | — | $14,211.81 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list ohlc_hot; 🔵; ret5=-32.2; leftover $680.14 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,211.81 | ▼ close $9,480.28 vs 09:30 $9,522.01 (session -6.47) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,211.81 | ▲ 09:30 equity $9,511.78 vs yday $9,480.28 (+31.50) | — | — |
| 2026-09-04 09:30 ET | **SHORT** | `HQ` | 42 | $15.90 | $2.15 | — | $14,877.46 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer,yday_mover; ret5=-4.1; leftover $679.41 | — |
| 2026-09-04 09:30 ET | **SHORT** | `EOSE` | 193 | $3.52 | $2.63 | — | $15,554.19 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $679.41 | — |
| 2026-09-04 09:30 ET | **SHORT** | `UAMY` | 129 | $5.25 | $2.43 | — | $16,229.01 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer,yday_mover; ret5=-0.4; leftover $679.41 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 145 | $4.67 | $2.48 | — | $16,903.68 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer; ret5=+11.9; leftover $679.41 | — |
| 2026-09-04 09:30 ET | **SHORT** | `SCZM` | 67 | $10.03 | $2.23 | — | $17,573.46 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer; ret5=+4.0; leftover $679.41 | — |
| 2026-09-04 09:30 ET | **SHORT** | `FCEL` | 46 | $14.52 | $2.17 | — | $18,239.21 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=-24.1; leftover $679.41 | — |
| 2026-09-04 09:30 ET | **SHORT** | `AUR` | 108 | $6.26 | $2.36 | — | $18,913.47 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list ohlc_hot; 🔵; ret5=+11.1; leftover $679.41 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,913.47 | ▼ close $9,351.16 vs 09:30 $9,511.78 (session -144.18) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,913.47 | ▼ 09:30 equity $9,284.25 vs yday $9,351.16 (-66.91) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,913.47 | ▼ close $8,970.20 vs 09:30 $9,284.25 (session -314.04) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,913.47 | ▲ 09:30 equity $9,027.71 vs yday $8,970.20 (+57.51) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `SAFX` | 1804 | $0.40 | $12.63 | $-66.66 | $18,179.25 | ▼ -66.66 after sell → book $9,015.08; vs 09:30 mark -12.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `GMRS` | 53 | $13.80 | $2.15 | $-55.75 | $17,445.70 | ▼ -55.75 after sell → book $9,012.93; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `KLRA` | 42 | $16.27 | $2.12 | $-17.71 | $16,760.24 | ▼ -17.71 after sell → book $9,010.81; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `ALMS` | 65 | $10.49 | $2.19 | $-11.88 | $16,076.21 | ▼ -11.88 after sell → book $9,008.63; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `RZLV` | 290 | $2.30 | $3.74 | $+4.04 | $15,405.47 | ▲ +4.04 after sell → book $9,004.89; vs 09:30 mark -3.74 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `EVTL` | 1062 | $0.59 | $9.49 | $+29.17 | $14,765.14 | ▲ +29.17 after sell → book $8,995.39; vs 09:30 mark -9.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `REAX` | 36 | $20.70 | $2.10 | $-87.03 | $14,017.85 | ▼ -87.03 after sell → book $8,993.30; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,017.85 | ▲ close $9,042.89 vs 09:30 $9,027.71 (session +49.59) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,017.85 | ▲ 09:30 equity $9,219.91 vs yday $9,042.89 (+177.02) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `HQ` | 42 | $15.53 | $2.12 | $+11.27 | $13,363.47 | ▲ +11.27 after sell → book $9,217.79; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `EOSE` | 193 | $3.96 | $2.57 | $-91.08 | $12,595.66 | ▼ -91.08 after sell → book $9,215.23; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `UAMY` | 129 | $5.10 | $2.38 | $+14.55 | $11,935.38 | ▲ +14.55 after sell → book $9,212.85; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 145 | $4.36 | $2.42 | $+40.05 | $11,300.75 | ▲ +40.05 after sell → book $9,210.42; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `SCZM` | 67 | $9.93 | $2.19 | $+2.28 | $10,633.25 | ▲ +2.28 after sell → book $9,208.23; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `FCEL` | 46 | $16.07 | $2.13 | $-75.59 | $9,891.90 | ▼ -75.59 after sell → book $9,206.10; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `AUR` | 108 | $6.35 | $2.31 | $-13.85 | $9,203.79 | ▼ -13.85 after sell → book $9,203.79; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,203.79 | ▲ close $9,203.79 vs 09:30 $9,219.91 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,203.79 | ▲ 09:30 equity $9,203.79 vs yday $9,203.79 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **SHORT** | `TYRA` | 24 | $23.63 | $2.10 | — | $9,768.81 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer; ret5=-6.3; leftover $575.24 | — |
| 2026-09-11 09:30 ET | **SHORT** | `LDI` | 676 | $0.85 | $7.91 | — | $10,335.50 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer; 🔵; ret5=-7.8; leftover $575.24 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 163 | $3.52 | $2.53 | — | $10,906.73 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ret5=-19.2; leftover $575.24 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BHVN` | 44 | $13.03 | $2.16 | — | $11,477.89 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=-18.5; leftover $575.24 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 283 | $2.03 | $3.72 | — | $12,048.66 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=-8.8; leftover $575.24 | — |
| 2026-09-11 09:30 ET | **SHORT** | `AXGN` | 13 | $42.48 | $2.06 | — | $12,598.83 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ret5=-13.4; leftover $575.24 | — |
| 2026-09-11 09:30 ET | **SHORT** | `REAX` | 30 | $18.56 | $2.12 | — | $13,153.52 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=+2.8; leftover $575.24 | — |
| 2026-09-11 09:30 ET | **SHORT** | `TGB` | 71 | $8.02 | $2.24 | — | $13,720.70 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=-3.6; leftover $575.24 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,720.70 | ▲ close $9,190.62 vs 09:30 $9,203.79 (session +11.67) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,720.70 | ▲ 09:30 equity $9,230.96 vs yday $9,190.62 (+40.34) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,720.70 | ▼ close $9,092.21 vs 09:30 $9,230.96 (session -138.75) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,720.70 | ▲ 09:30 equity $9,124.23 vs yday $9,092.21 (+32.02) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,720.70 | ▲ close $9,182.42 vs 09:30 $9,124.23 (session +58.19) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,720.70 | ▼ 09:30 equity $9,173.74 vs yday $9,182.42 (-8.68) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `TYRA` | 24 | $25.58 | $2.06 | $-50.96 | $13,104.72 | ▼ -50.96 after sell → book $9,171.68; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `LDI` | 676 | $0.73 | $6.97 | $+65.56 | $12,603.59 | ▲ +65.56 after sell → book $9,164.71; vs 09:30 mark -6.97 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 163 | $3.98 | $2.48 | $-79.99 | $11,952.37 | ▼ -79.99 after sell → book $9,162.23; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BHVN` | 44 | $12.34 | $2.12 | $+26.08 | $11,407.29 | ▲ +26.08 after sell → book $9,160.11; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 283 | $1.85 | $3.65 | $+43.57 | $10,880.09 | ▲ +43.57 after sell → book $9,156.46; vs 09:30 mark -3.65 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `AXGN` | 13 | $44.87 | $2.03 | $-35.16 | $10,294.75 | ▼ -35.16 after sell → book $9,154.43; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `REAX` | 30 | $19.03 | $2.08 | $-18.30 | $9,721.77 | ▼ -18.30 after sell → book $9,152.35; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `TGB` | 71 | $8.02 | $2.20 | $-4.44 | $9,150.15 | ▼ -4.44 after sell → book $9,150.15; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 30 | $18.61 | $2.12 | — | $9,706.33 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $571.88 | — |
| 2026-09-16 09:30 ET | **SHORT** | `HQ` | 44 | $12.89 | $2.16 | — | $10,271.33 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer,yday_mover; ret5=-18.2; leftover $571.88 | — |
| 2026-09-16 09:30 ET | **SHORT** | `PLAY` | 83 | $6.86 | $2.28 | — | $10,838.44 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=-22.4; leftover $571.88 | — |
| 2026-09-16 09:30 ET | **SHORT** | `DVLT` | 3574 | $0.16 | $17.06 | — | $11,393.22 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ret5=-23.8; leftover $571.88 | — |
| 2026-09-16 09:30 ET | **SHORT** | `SION` | 82 | $6.95 | $2.28 | — | $11,960.84 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=-5.8; leftover $571.88 | — |
| 2026-09-16 09:30 ET | **SHORT** | `CTMX` | 210 | $2.72 | $2.77 | — | $12,529.27 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=-26.3; leftover $571.88 | — |
| 2026-09-16 09:30 ET | **SHORT** | `CRBP` | 83 | $6.86 | $2.28 | — | $13,096.37 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=-34.7; leftover $571.88 | — |
| 2026-09-16 09:30 ET | **SHORT** | `EYPT` | 156 | $3.66 | $2.51 | — | $13,664.82 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=-19.7; leftover $571.88 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,664.82 | ▼ close $8,891.37 vs 09:30 $9,173.74 (session -225.33) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,664.82 | ▼ 09:30 equity $8,859.18 vs yday $8,891.37 (-32.19) | — | — |
| 2026-09-17 09:30 ET | **SHORT** | `BRUN` | 39 | $15.87 | $2.14 | — | $14,281.61 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $632.80 | — |
| 2026-09-17 09:30 ET | **SHORT** | `FTAI` | 3 | $196.50 | $2.04 | — | $14,869.07 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer; 🔵; ret5=+2.5; leftover $632.80 | — |
| 2026-09-17 09:30 ET | **SHORT** | `MRLN` | 278 | $2.27 | $3.66 | — | $15,496.47 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ret5=-29.6; leftover $632.80 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BTGO` | 96 | $6.56 | $2.32 | — | $16,123.91 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=-14.6; leftover $632.80 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 79 | $7.95 | $2.27 | — | $16,749.70 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $632.80 | — |
| 2026-09-17 09:30 ET | **SHORT** | `RCAT` | 85 | $7.39 | $2.29 | — | $17,375.98 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=-12.7; leftover $632.80 | — |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 7 | $81.00 | $2.05 | — | $17,940.94 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list earn_react; 🔵; ret5=-3.0; leftover $632.80 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,940.94 | ▲ close $8,955.52 vs 09:30 $8,859.18 (session +113.10) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,940.94 | ▼ 09:30 equity $8,868.49 vs yday $8,955.52 (-87.03) | — | — |
| 2026-09-18 09:30 ET | **SHORT** | `FLNC` | 588 | $7.54 | $7.84 | — | $22,363.68 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ret5=-20.9; leftover $4434.24 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,363.68 | ▲ close $9,148.49 vs 09:30 $8,868.49 (session +287.84) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,363.68 | ▼ 09:30 equity $8,970.14 vs yday $9,148.49 (-178.35) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 30 | $22.11 | $2.08 | $-109.20 | $21,698.30 | ▼ -109.20 after sell → book $8,968.06; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `HQ` | 44 | $13.41 | $2.12 | $-27.16 | $21,106.14 | ▼ -27.16 after sell → book $8,965.94; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `PLAY` | 83 | $6.68 | $2.24 | $+10.42 | $20,549.46 | ▲ +10.42 after sell → book $8,963.70; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `DVLT` | 3574 | $0.16 | $16.44 | $-33.50 | $19,961.18 | ▼ -33.50 after sell → book $8,947.26; vs 09:30 mark -16.44 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `SION` | 82 | $6.00 | $2.24 | $+73.39 | $19,466.94 | ▲ +73.39 after sell → book $8,945.02; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `CTMX` | 210 | $2.80 | $2.71 | $-22.28 | $18,876.23 | ▼ -22.28 after sell → book $8,942.31; vs 09:30 mark -2.71 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `CRBP` | 83 | $7.51 | $2.24 | $-58.47 | $18,250.66 | ▼ -58.47 after sell → book $8,940.07; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `EYPT` | 156 | $3.87 | $2.46 | $-37.73 | $17,644.49 | ▼ -37.73 after sell → book $8,937.62; vs 09:30 mark -2.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SHORT** | `SBET` | 63 | $9.99 | $2.22 | — | $18,271.64 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $638.40 | — |
| 2026-09-21 09:30 ET | **SHORT** | `SGML` | 62 | $10.13 | $2.21 | — | $18,897.79 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer; ret5=+4.9; leftover $638.40 | — |
| 2026-09-21 09:30 ET | **SHORT** | `FWDI` | 77 | $8.22 | $2.26 | — | $19,528.47 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $638.40 | — |
| 2026-09-21 09:30 ET | **SHORT** | `DFDV` | 98 | $6.51 | $2.33 | — | $20,164.13 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer,yday_mover; 🔵; ret5=+19.3; leftover $638.40 | — |
| 2026-09-21 09:30 ET | **SHORT** | `CAN` | 1527 | $0.42 | $11.24 | — | $20,791.17 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer,yday_mover; 🔵; ret5=+10.7; leftover $638.40 | — |
| 2026-09-21 09:30 ET | **SHORT** | `COIN` | 3 | $205.19 | $2.04 | — | $21,404.70 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer; 🔵; ret5=+10.8; leftover $638.40 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 77 | $8.26 | $2.26 | — | $22,038.46 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=+7.7; leftover $638.40 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,038.46 | ▲ close $9,079.29 vs 09:30 $8,970.14 (session +166.24) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,038.46 | ▼ 09:30 equity $9,042.27 vs yday $9,079.29 (-37.02) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `MRLN` | 278 | $1.87 | $3.59 | $+103.95 | $21,515.01 | ▲ +103.95 after sell → book $9,038.68; vs 09:30 mark -3.59 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **COVER** | `BTGO` | 96 | $7.81 | $2.28 | $-124.60 | $20,762.98 | ▼ -124.60 after sell → book $9,036.41; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 79 | $8.28 | $2.23 | $-30.17 | $20,107.02 | ▼ -30.17 after sell → book $9,034.18; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **COVER** | `RCAT` | 85 | $7.02 | $2.25 | $+27.34 | $19,508.08 | ▲ +27.34 after sell → book $9,031.93; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 8 | $93.97 | $2.05 | — | $20,257.78 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list flatten; ret5=-0.6; leftover $752.66 | — |
| 2026-09-22 09:30 ET | **SHORT** | `ALOY` | 80 | $9.40 | $2.27 | — | $21,007.51 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer; ret5=+9.5; leftover $752.66 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,007.51 | ▲ close $9,151.03 vs 09:30 $9,042.27 (session +123.42) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,007.51 | ▼ 09:30 equity $9,032.36 vs yday $9,151.03 (-118.67) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `BRUN` | 39 | $17.10 | $2.11 | $-52.22 | $20,338.50 | ▼ -52.22 after sell → book $9,030.26; vs 09:30 mark -2.10 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `FTAI` | 3 | $187.73 | $2.00 | $+22.28 | $19,773.31 | ▲ +22.28 after sell → book $9,028.26; vs 09:30 mark -2.00 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `LEN` | 7 | $82.00 | $2.01 | $-11.06 | $19,197.30 | ▼ -11.06 after sell → book $9,026.25; vs 09:30 mark -2.01 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `FLNC` | 588 | $7.52 | $7.59 | $-6.60 | $14,767.96 | ▼ -6.60 after sell → book $9,018.66; vs 09:30 mark -7.59 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SHORT** | `MAZE` | 39 | $28.30 | $2.16 | — | $15,869.50 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $1127.33 | — |
| 2026-09-23 09:30 ET | **SHORT** | `EU` | 939 | $1.20 | $12.31 | — | $16,983.99 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer,yday_mover; 🔵; ret5=+12.9; leftover $1127.33 | — |
| 2026-09-23 09:30 ET | **SHORT** | `CMPX` | 924 | $1.22 | $12.11 | — | $18,099.16 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ret5=-33.0; leftover $1127.33 | — |
| 2026-09-23 09:30 ET | **SHORT** | `BTGO` | 139 | $8.11 | $2.47 | — | $19,223.98 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list ohlc_hot; ret5=+14.2; leftover $1127.33 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,223.98 | ▲ close $9,080.55 vs 09:30 $9,032.36 (session +90.95) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,223.98 | ▲ 09:30 equity $9,180.07 vs yday $9,080.55 (+99.52) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `SBET` | 63 | $9.80 | $2.18 | $+7.57 | $18,604.40 | ▲ +7.57 after sell → book $9,177.89; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `SGML` | 62 | $9.89 | $2.18 | $+10.80 | $17,989.04 | ▲ +10.80 after sell → book $9,175.72; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `FWDI` | 77 | $7.94 | $2.22 | $+17.08 | $17,375.44 | ▲ +17.08 after sell → book $9,173.50; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `DFDV` | 98 | $5.77 | $2.28 | $+67.91 | $16,807.70 | ▲ +67.91 after sell → book $9,171.21; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `CAN` | 1527 | $0.38 | $10.41 | $+33.31 | $16,213.97 | ▲ +33.31 after sell → book $9,160.80; vs 09:30 mark -10.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `COIN` | 3 | $196.77 | $2.00 | $+21.23 | $15,621.66 | ▲ +21.23 after sell → book $9,158.80; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,621.66 | ▲ close $9,175.24 vs 09:30 $9,180.07 (session +16.44) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,057.38 | ▼ 09:30 equity $9,910.13 vs yday $9,918.14 (-8.01) | 09:30 open · cash $19,057.38 (unchanged overnight, no fees) · equity $9,910.13 vs prior close $9,918.14 (-8.01) · 8 name(s) re-marked at the open (per-name table). AEHL×89 yday $8.96 → 09:30 $9.05 -8.01; ALOY×72 yday $8.52 → 09:30 $8.52 -0.00; CMPX×3898 yday $1.13 → 09:30 $1.13 -0.00; DLO×46 yday $13.88 → 09:30 $13.88 -0.00; EU×642 yday $1.22 → 09:30 $1.22 -0.00; HELP×48 yday $12.59 → 09:30 $12.59 -0.00; NN×43 yday $14.45 → 09:30 $14.45 -0.00; USFD×7 yday $93.82 → 09:30 $93.82 -0.00 | — |
| 2026-09-25 09:30 ET | **SHORT** | `BRVE` | 52 | $23.58 | $2.20 | — | $20,281.34 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $1238.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `NEOV` | 518 | $2.39 | $6.81 | — | $21,512.55 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=-31.4; leftover $1238.77 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `SGMT` | 135 | $9.11 | $2.46 | — | $22,739.94 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $1238.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `COST` | 1 | $887.00 | $2.04 | — | $23,624.90 | — | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list earn_react; 🔵; ret5=+0.3; leftover $1238.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23,624.90 | ▲ close $10,073.23 vs 09:30 $9,910.13 (session +176.62) | 16:00 close · cash $23,624.90 · equity $10,073.23 vs 09:30 $9,910.13 (+163.10; session marks +176.62) · 12 name(s) marked open→close (per-name table). AEHL×89 09:30 $9.05 → close $9.36 -27.59; ALOY×72 09:30 $8.52 → close $8.52 -0.00; CMPX×3898 09:30 $1.14 → close $1.14 +0.00; DLO×46 09:30 $13.88 → close $13.88 -0.00; EU×642 09:30 $1.22 → close $1.22 -0.00; HELP×48 09:30 $12.59 → close $12.59 -0.00; NN×43 09:30 $14.45 → close $14.45 +0.00; USFD×7 09:30 $93.82 → close $93.82 +0.00; BRVE×52 09:30 $23.58 → close $20.62 +153.92; NEOV×518 09:30 $2.39 → close $2.19 +103.60; SGMT×135 09:30 $9.11 → close $9.24 -17.55; COST×1 09:30 $887.00 → close $922.76 -35.76 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `DAVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BIRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CLBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `OTLK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SECZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DAVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BIRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CLBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OTLK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SECZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CDNL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `KLC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `AMPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `MRLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `EYPT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STUB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALOY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GEMI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CDNL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `INV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `KLC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `AMPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ENHA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `MRLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `EYPT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `WYFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TOYO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `LZB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AEG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `WYFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TOYO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `LZB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AEG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ENHA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `YSS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `SHAZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `EVMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `EOSE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `XPEV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ENHA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `YSS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `SHAZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `EVMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `EOSE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `FLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FWRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `LI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `PLAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AVEX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ASPN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `IRDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BKSY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FWRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `LI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `PLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `ALOY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SLQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `AVBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ALOY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `PYXS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `XPOF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OPTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GENB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `QFIN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SRPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DLO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `PYXS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `XPOF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `OPTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GENB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `QFIN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `HDSN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MNSO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `REAX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MMED` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MRLN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HELP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SCZM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `GMRS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `KLRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ALMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RZLV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EVTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `REAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `GMRS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `KLRA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RZLV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `EVTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `EOSE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `UAMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `GSM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `FCEL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `AUR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ARDT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `HQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `EOSE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `UAMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `GSM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `FCEL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `AUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SWIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `EYPT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TGB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HELP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DRTS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `HELP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `EVMN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XPOF` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `PYXS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SHOE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `TYRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RWT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AXGN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CAN` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RWT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `AXGN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `TRX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RUM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `HQ` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `EU` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `BBNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `HQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `PLAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SION` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CTMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CRBP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `HQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `PLAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CTMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CRBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `FTAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `MRLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BTGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BULL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RCAT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `LEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `FTAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `MRLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BULL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `RCAT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `LEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `FLNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BRUN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FTAI` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SGML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `COIN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `MRAM` | no_price | no 09:30 open |
| 2026-09-22 | `EU` | no_price | no 09:30 open |
| 2026-09-22 | `BRVE` | no_price | no 09:30 open |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SGML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `COIN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `AEHL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `USFD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `ALOY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `USFD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `ALOY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `MAZE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `EU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CMPX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BTGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SRFM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `LU` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CGEM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `AEHL` | 77 | 2026-09-21 @ $8.26 | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; ret5=+7.7; leftover $638.40 |
| `USFD` | 8 | 2026-09-22 @ $93.97 | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list flatten; ret5=-0.6; leftover $752.66 |
| `ALOY` | 80 | 2026-09-22 @ $9.40 | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer; ret5=+9.5; leftover $752.66 |
| `MAZE` | 39 | 2026-09-23 @ $28.30 | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $1127.33 |
| `EU` | 939 | 2026-09-23 @ $1.20 | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_gainer,yday_mover; 🔵; ret5=+12.9; leftover $1127.33 |
| `CMPX` | 924 | 2026-09-23 @ $1.22 | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list yday_mover; 🔵; ret5=-33.0; leftover $1127.33 |
| `BTGO` | 139 | 2026-09-23 @ $8.11 | Clock-B #4 neg catalyst + weakness + failed recovery; gate clk_neg_weak_fail=True; list ohlc_hot; ret5=+14.2; leftover $1127.33 |
