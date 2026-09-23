# Factor mine action — `union_clk_nr7_mom_opp_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `opp_rvol` · size `leftover` · sell `list` · S-boost `none` · Clock-B #10 ∩ Theme Radar T−1 oppset

Cash book **+10.45%** ($11,045) · signal-only (no cash/fees) was +0.01%. Starts YES **12/29**. Fills 8 · skips 0 · realized $+1045.40.

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

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `DUOT` | 1058 | — | $9.43 | +0.00 | $9.11 | -338.56 | -338.56 | +0.00 | -338.56 |
| 2026-08-17 | `DUOT` | 1058 | $9.11 | $10.35 | +1311.92 | — | +0.00 | +1311.92 | +973.36 | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-21 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-24 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-25 | `GRRR` | 785 | — | $13.92 | +0.00 | $14.04 | +94.20 | +94.20 | +0.00 | +94.20 |
| 2026-08-26 | `GRRR` | 785 | $14.04 | $14.03 | -7.85 | — | +0.00 | -7.85 | +86.35 | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | `MRNA` | 40 | — | $137.19 | +0.00 | $137.99 | +32.00 | +32.00 | +0.00 | +32.00 |
| 2026-08-28 | `MNRO` | 444 | — | $12.38 | +0.00 | $12.96 | +257.52 | +257.52 | +0.00 | +257.52 |
| 2026-08-31 | `MRNA` | 40 | $137.99 | $134.10 | -155.60 | — | +0.00 | -155.60 | -123.60 | — |
| 2026-08-31 | `MNRO` | 444 | $12.96 | $12.77 | -84.36 | — | +0.00 | -84.36 | +173.16 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-08 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-21 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-22 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-23 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -338.56 | DUOT | — | $9.41 | $9,647.79 | DUOT×1058 |
| 2026-08-17 | +2.25 | $9.41 | DUOT×1058 | $10,959.71 | +1,311.92 | +0.00 | — | DUOT | $10,945.80 | $10,945.80 | — |
| 2026-08-18 | -6.20 | $10,945.80 | — | $10,945.80 | +0.00 | +0.00 | — | — | $10,945.80 | $10,945.80 | — |
| 2026-08-19 | -7.20 | $10,945.80 | — | $10,945.80 | +0.00 | +0.00 | — | — | $10,945.80 | $10,945.80 | — |
| 2026-08-20 | +1.12 | $10,945.80 | — | $10,945.80 | +0.00 | +0.00 | — | — | $10,945.80 | $10,945.80 | — |
| 2026-08-21 | +3.25 | $10,945.80 | — | $10,945.80 | +0.00 | +0.00 | — | — | $10,945.80 | $10,945.80 | — |
| 2026-08-24 | -5.17 | $10,945.80 | — | $10,945.80 | +0.00 | +0.00 | — | — | $10,945.80 | $10,945.80 | — |
| 2026-08-25 | +1.80 | $10,945.80 | — | $10,945.80 | +0.00 | +94.20 | GRRR | — | $8.47 | $11,029.87 | GRRR×785 |
| 2026-08-26 | +2.02 | $8.47 | GRRR×785 | $11,022.02 | -7.85 | +0.00 | — | GRRR | $11,011.68 | $11,011.68 | — |
| 2026-08-27 | — | $11,011.68 | — | $11,011.68 | -0.00 | +0.00 | — | — | $11,011.68 | $11,011.68 | — |
| 2026-08-28 | +0.75 | $11,011.68 | — | $11,011.68 | -0.00 | +289.52 | MRNA, MNRO | — | $19.52 | $11,293.36 | MRNA×40, MNRO×444 |
| 2026-08-31 | -5.85 | $19.52 | MRNA×40, MNRO×444 | $11,053.40 | -239.96 | +0.00 | — | MRNA, MNRO | $11,045.39 | $11,045.39 | — |
| 2026-09-01 | -6.30 | $11,045.39 | — | $11,045.39 | +0.00 | +0.00 | — | — | $11,045.39 | $11,045.39 | — |
| 2026-09-02 | -3.83 | $11,045.39 | — | $11,045.39 | +0.00 | +0.00 | — | — | $11,045.39 | $11,045.39 | — |
| 2026-09-03 | -0.90 | $11,045.39 | — | $11,045.39 | +0.00 | +0.00 | — | — | $11,045.39 | $11,045.39 | — |
| 2026-09-04 | +2.25 | $11,045.39 | — | $11,045.39 | +0.00 | +0.00 | — | — | $11,045.39 | $11,045.39 | — |
| 2026-09-08 | -11.47 | $11,045.39 | — | $11,045.39 | +0.00 | +0.00 | — | — | $11,045.39 | $11,045.39 | — |
| 2026-09-09 | -13.95 | $11,045.39 | — | $11,045.39 | +0.00 | +0.00 | — | — | $11,045.39 | $11,045.39 | — |
| 2026-09-10 | -13.28 | $11,045.39 | — | $11,045.39 | +0.00 | +0.00 | — | — | $11,045.39 | $11,045.39 | — |
| 2026-09-11 | +0.50 | $11,045.39 | — | $11,045.39 | +0.00 | +0.00 | — | — | $11,045.39 | $11,045.39 | — |
| 2026-09-14 | -11.00 | $11,045.39 | — | $11,045.39 | +0.00 | +0.00 | — | — | $11,045.39 | $11,045.39 | — |
| 2026-09-15 | -3.84 | $11,045.39 | — | $11,045.39 | +0.00 | +0.00 | — | — | $11,045.39 | $11,045.39 | — |
| 2026-09-16 | +5.30 | $11,045.39 | — | $11,045.39 | +0.00 | +0.00 | — | — | $11,045.39 | $11,045.39 | — |
| 2026-09-17 | +7.38 | $11,045.39 | — | $11,045.39 | +0.00 | +0.00 | — | — | $11,045.39 | $11,045.39 | — |
| 2026-09-18 | +4.86 | $11,045.39 | — | $11,045.39 | +0.00 | +0.00 | — | — | $11,045.39 | $11,045.39 | — |
| 2026-09-21 | +12.87 | $11,045.39 | — | $11,045.39 | +0.00 | +0.00 | — | — | $11,045.39 | $11,045.39 | — |
| 2026-09-22 | -0.50 | $11,045.39 | — | $11,045.39 | +0.00 | +0.00 | — | — | $11,045.39 | $11,045.39 | — |
| 2026-09-23 | +2.29 | $11,045.39 | — | $11,045.39 | +0.00 | +0.00 | — | — | $11,045.39 | $11,045.39 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `DUOT` | 1058 | $9.43 | $13.65 | — | $9.41 | — | Clock-B #10 ∩ Theme Radar T−1 oppset; gate clk_nr7_mom=True,oppset=True; rank opp_rvol; list overnight; 🔵; ⚪; ret5=+7.7; leftover $10000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.41 | ▼ close $9,647.79 vs 09:30 $10,000.00 (session -338.56) | 16:00 close · cash $9.41 · equity $9,647.79 vs 09:30 $10,000.00 (-352.21; session marks -338.56) · 1 name(s) marked open→close (per-name table). DUOT×1058 09:30 $9.43 → close $9.11 -338.56 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.41 | ▲ 09:30 equity $10,959.71 vs yday $9,647.79 (+1,311.92) | 09:30 open · cash $9.41 (unchanged overnight, no fees) · equity $10,959.71 vs prior close $9,647.79 (+1311.92) · 1 name(s) re-marked at the open (per-name table). DUOT×1058 yday $9.11 → 09:30 $10.35 +1311.92 | — |
| 2026-08-17 09:30 ET | **SELL** | `DUOT` | 1058 | $10.35 | $13.91 | $+945.80 | $10,945.80 | ▲ +945.80 after sell → book $10,945.80; vs 09:30 mark -13.91 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,945.80 | ▲ close $10,945.80 vs 09:30 $10,959.71 (session +0.00) | 16:00 close · cash $10,945.80 · no lots left · equity $10,945.80. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,945.80 | ▲ 09:30 equity $10,945.80 vs yday $10,945.80 (+0.00) | 09:30 open · cash $10,945.80 · no holdings · equity $10,945.80 vs prior close $10,945.80 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,945.80 | ▲ close $10,945.80 vs 09:30 $10,945.80 (session +0.00) | 16:00 close · cash $10,945.80 · no lots left · equity $10,945.80. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,945.80 | ▲ 09:30 equity $10,945.80 vs yday $10,945.80 (+0.00) | 09:30 open · cash $10,945.80 · no holdings · equity $10,945.80 vs prior close $10,945.80 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,945.80 | ▲ close $10,945.80 vs 09:30 $10,945.80 (session +0.00) | 16:00 close · cash $10,945.80 · no lots left · equity $10,945.80. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,945.80 | ▲ 09:30 equity $10,945.80 vs yday $10,945.80 (+0.00) | 09:30 open · cash $10,945.80 · no holdings · equity $10,945.80 vs prior close $10,945.80 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,945.80 | ▲ close $10,945.80 vs 09:30 $10,945.80 (session +0.00) | 16:00 close · cash $10,945.80 · no lots left · equity $10,945.80. | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,945.80 | ▲ 09:30 equity $10,945.80 vs yday $10,945.80 (+0.00) | 09:30 open · cash $10,945.80 · no holdings · equity $10,945.80 vs prior close $10,945.80 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,945.80 | ▲ close $10,945.80 vs 09:30 $10,945.80 (session +0.00) | 16:00 close · cash $10,945.80 · no lots left · equity $10,945.80. | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,945.80 | ▲ 09:30 equity $10,945.80 vs yday $10,945.80 (+0.00) | 09:30 open · cash $10,945.80 · no holdings · equity $10,945.80 vs prior close $10,945.80 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,945.80 | ▲ close $10,945.80 vs 09:30 $10,945.80 (session +0.00) | 16:00 close · cash $10,945.80 · no lots left · equity $10,945.80. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,945.80 | ▲ 09:30 equity $10,945.80 vs yday $10,945.80 (+0.00) | 09:30 open · cash $10,945.80 · no holdings · equity $10,945.80 vs prior close $10,945.80 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 785 | $13.92 | $10.13 | — | $8.47 | — | Clock-B #10 ∩ Theme Radar T−1 oppset; gate clk_nr7_mom=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+5.9; leftover $10945.80 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.47 | ▲ close $11,029.87 vs 09:30 $10,945.80 (session +94.20) | 16:00 close · cash $8.47 · equity $11,029.87 vs 09:30 $10,945.80 (+84.07; session marks +94.20) · 1 name(s) marked open→close (per-name table). GRRR×785 09:30 $13.92 → close $14.04 +94.20 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.47 | ▼ 09:30 equity $11,022.02 vs yday $11,029.87 (-7.85) | 09:30 open · cash $8.47 (unchanged overnight, no fees) · equity $11,022.02 vs prior close $11,029.87 (-7.85) · 1 name(s) re-marked at the open (per-name table). GRRR×785 yday $14.04 → 09:30 $14.03 -7.85 | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 785 | $14.03 | $10.34 | $+65.88 | $11,011.68 | ▲ +65.88 after sell → book $11,011.68; vs 09:30 mark -10.34 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,011.68 | ▲ close $11,011.68 vs 09:30 $11,022.02 (session +0.00) | 16:00 close · cash $11,011.68 · no lots left · equity $11,011.68. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,011.68 | ▲ 09:30 equity $11,011.68 vs yday $11,011.68 (-0.00) | 09:30 open · cash $11,011.68 · no holdings · equity $11,011.68 vs prior close $11,011.68 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,011.68 | ▲ close $11,011.68 vs 09:30 $11,011.68 (session +0.00) | 16:00 close · cash $11,011.68 · no lots left · equity $11,011.68. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,011.68 | ▲ 09:30 equity $11,011.68 vs yday $11,011.68 (-0.00) | 09:30 open · cash $11,011.68 · no holdings · equity $11,011.68 vs prior close $11,011.68 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 40 | $137.19 | $2.11 | — | $5,521.97 | — | Clock-B #10 ∩ Theme Radar T−1 oppset; gate clk_nr7_mom=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+7.1; leftover $5505.84 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MNRO` | 444 | $12.38 | $5.73 | — | $19.52 | — | Clock-B #10 ∩ Theme Radar T−1 oppset; gate clk_nr7_mom=True,oppset=True; rank opp_rvol; list yday_mover; ret5=+2.4; leftover $5505.84 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.52 | ▲ close $11,293.36 vs 09:30 $11,011.68 (session +289.52) | 16:00 close · cash $19.52 · equity $11,293.36 vs 09:30 $11,011.68 (+281.68; session marks +289.52) · 2 name(s) marked open→close (per-name table). MRNA×40 09:30 $137.19 → close $137.99 +32.00; MNRO×444 09:30 $12.38 → close $12.96 +257.52 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.52 | ▼ 09:30 equity $11,053.40 vs yday $11,293.36 (-239.96) | 09:30 open · cash $19.52 (unchanged overnight, no fees) · equity $11,053.40 vs prior close $11,293.36 (-239.96) · 2 name(s) re-marked at the open (per-name table). MRNA×40 yday $137.99 → 09:30 $134.10 -155.60; MNRO×444 yday $12.96 → 09:30 $12.77 -84.36 | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 40 | $134.10 | $2.16 | $-127.87 | $5,381.36 | ▼ -127.87 after sell → book $11,051.24; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `MNRO` | 444 | $12.77 | $5.85 | $+161.59 | $11,045.39 | ▲ +161.59 after sell → book $11,045.39; vs 09:30 mark -5.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,053.40 (session +0.00) | 16:00 close · cash $11,045.39 · no lots left · equity $11,045.39. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | 09:30 open · cash $11,045.39 · no holdings · equity $11,045.39 vs prior close $11,045.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | 16:00 close · cash $11,045.39 · no lots left · equity $11,045.39. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | 09:30 open · cash $11,045.39 · no holdings · equity $11,045.39 vs prior close $11,045.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | 16:00 close · cash $11,045.39 · no lots left · equity $11,045.39. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | 09:30 open · cash $11,045.39 · no holdings · equity $11,045.39 vs prior close $11,045.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | 16:00 close · cash $11,045.39 · no lots left · equity $11,045.39. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | 09:30 open · cash $11,045.39 · no holdings · equity $11,045.39 vs prior close $11,045.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | 16:00 close · cash $11,045.39 · no lots left · equity $11,045.39. | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | 09:30 open · cash $11,045.39 · no holdings · equity $11,045.39 vs prior close $11,045.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | 16:00 close · cash $11,045.39 · no lots left · equity $11,045.39. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | 09:30 open · cash $11,045.39 · no holdings · equity $11,045.39 vs prior close $11,045.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | 16:00 close · cash $11,045.39 · no lots left · equity $11,045.39. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | 09:30 open · cash $11,045.39 · no holdings · equity $11,045.39 vs prior close $11,045.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | 16:00 close · cash $11,045.39 · no lots left · equity $11,045.39. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | 09:30 open · cash $11,045.39 · no holdings · equity $11,045.39 vs prior close $11,045.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | 16:00 close · cash $11,045.39 · no lots left · equity $11,045.39. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | 09:30 open · cash $11,045.39 · no holdings · equity $11,045.39 vs prior close $11,045.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | 16:00 close · cash $11,045.39 · no lots left · equity $11,045.39. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | 09:30 open · cash $11,045.39 · no holdings · equity $11,045.39 vs prior close $11,045.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | 16:00 close · cash $11,045.39 · no lots left · equity $11,045.39. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | 09:30 open · cash $11,045.39 · no holdings · equity $11,045.39 vs prior close $11,045.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | 16:00 close · cash $11,045.39 · no lots left · equity $11,045.39. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | 09:30 open · cash $11,045.39 · no holdings · equity $11,045.39 vs prior close $11,045.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | 16:00 close · cash $11,045.39 · no lots left · equity $11,045.39. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | 09:30 open · cash $11,045.39 · no holdings · equity $11,045.39 vs prior close $11,045.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | 16:00 close · cash $11,045.39 · no lots left · equity $11,045.39. | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | 09:30 open · cash $11,045.39 · no holdings · equity $11,045.39 vs prior close $11,045.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | 16:00 close · cash $11,045.39 · no lots left · equity $11,045.39. | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | 09:30 open · cash $11,045.39 · no holdings · equity $11,045.39 vs prior close $11,045.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | 16:00 close · cash $11,045.39 · no lots left · equity $11,045.39. | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,045.39 | ▲ 09:30 equity $11,045.39 vs yday $11,045.39 (+0.00) | 09:30 open · cash $11,045.39 · no holdings · equity $11,045.39 vs prior close $11,045.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,045.39 | ▲ close $11,045.39 vs 09:30 $11,045.39 (session +0.00) | 16:00 close · cash $11,045.39 · no lots left · equity $11,045.39. | — |
