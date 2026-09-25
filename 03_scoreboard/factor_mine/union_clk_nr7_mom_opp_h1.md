# Factor mine action — `union_clk_nr7_mom_opp_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `opp_rvol` · size `leftover` · sell `list` · S-boost `none` · Clock-B #10 ∩ Theme Radar T−1 oppset

Cash book **+0.31%** ($10,031) · signal-only (no cash/fees) was +0.01%. Starts YES **12/30**. Fills 8 · skips 0 · realized $+1045.40.

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
- Must-have: Clock-B #10: prior NR7 compression plus moderate momentum.
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
- **Gate** `clk_nr7_mom=True,oppset=True` · **rank** `opp_rvol` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,045.39.

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
| 2026-08-14 09:30 ET | **BUY** | `DUOT` | 1058 | $9.43 | $13.65 | — | $9.41 | — | Clock-B #10 ∩ Theme Radar T−1 oppset; gate clk_nr7_mom=True,oppset=True; rank opp_rvol; list overnight; 🔵; ⚪; ret5=+7.7; leftover $10000.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.41 | ▼ close $9,647.79 vs 09:30 $10,000.00 (session -338.56) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.41 | ▲ 09:30 equity $10,959.71 vs yday $9,647.79 (+1,311.92) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `DUOT` | 1058 | $10.35 | $13.91 | $+945.80 | $10,945.80 | ▲ +945.80 after sell → book $10,945.80; vs 09:30 mark -13.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,945.80 | ▲ close $10,945.80 vs 09:30 $10,959.71 (session +0.00) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,945.80 | ▲ 09:30 equity $10,945.80 vs yday $10,945.80 (+0.00) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,945.80 | ▲ close $10,945.80 vs 09:30 $10,945.80 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,945.80 | ▲ 09:30 equity $10,945.80 vs yday $10,945.80 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,945.80 | ▲ close $10,945.80 vs 09:30 $10,945.80 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,945.80 | ▲ 09:30 equity $10,945.80 vs yday $10,945.80 (+0.00) | — | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,945.80 | ▲ close $10,945.80 vs 09:30 $10,945.80 (session +0.00) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,945.80 | ▲ 09:30 equity $10,945.80 vs yday $10,945.80 (+0.00) | — | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,945.80 | ▲ close $10,945.80 vs 09:30 $10,945.80 (session +0.00) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,945.80 | ▲ 09:30 equity $10,945.80 vs yday $10,945.80 (+0.00) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,945.80 | ▲ close $10,945.80 vs 09:30 $10,945.80 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,945.80 | ▲ 09:30 equity $10,945.80 vs yday $10,945.80 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 785 | $13.92 | $10.13 | — | $8.47 | — | Clock-B #10 ∩ Theme Radar T−1 oppset; gate clk_nr7_mom=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+5.9; leftover $10945.80 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.47 | ▲ close $11,029.87 vs 09:30 $10,945.80 (session +94.20) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.47 | ▼ 09:30 equity $11,022.02 vs yday $11,029.87 (-7.85) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 785 | $14.03 | $10.34 | $+65.88 | $11,011.68 | ▲ +65.88 after sell → book $11,011.68; vs 09:30 mark -10.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,011.68 | ▲ close $11,011.68 vs 09:30 $11,022.02 (session +0.00) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,011.68 | ▲ 09:30 equity $11,011.68 vs yday $11,011.68 (-0.00) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,011.68 | ▲ close $11,011.68 vs 09:30 $11,011.68 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,011.68 | ▲ 09:30 equity $11,011.68 vs yday $11,011.68 (-0.00) | — | — |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 40 | $137.19 | $2.11 | — | $5,521.97 | — | Clock-B #10 ∩ Theme Radar T−1 oppset; gate clk_nr7_mom=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+7.1; leftover $5505.84 | — |
| 2026-08-28 09:30 ET | **BUY** | `MNRO` | 444 | $12.38 | $5.73 | — | $19.52 | — | Clock-B #10 ∩ Theme Radar T−1 oppset; gate clk_nr7_mom=True,oppset=True; rank opp_rvol; list yday_mover; ret5=+2.4; leftover $5505.84 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.52 | ▲ close $11,293.36 vs 09:30 $11,011.68 (session +289.52) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.52 | ▼ 09:30 equity $11,053.40 vs yday $11,293.36 (-239.96) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 40 | $134.10 | $2.16 | $-127.87 | $5,381.36 | ▼ -127.87 after sell → book $11,051.24; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MNRO` | 444 | $12.77 | $5.85 | $+161.59 | $11,045.39 | ▲ +161.59 after sell → book $11,045.39; vs 09:30 mark -5.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,053.40 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | — | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | — | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | — | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,031.18 | ▲ 09:30 equity $10,031.18 vs yday $10,031.18 (+0.00) | 09:30 open · cash $10,031.18 · no holdings · equity $10,031.18 vs prior close $10,031.18 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,031.18 | ▲ close $10,031.18 vs 09:30 $10,031.18 (session +0.00) | 16:00 close · cash $10,031.18 · no lots left · equity $10,031.18. | — |
