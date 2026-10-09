# Factor mine action — `union_flow_in_white_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-13.15%** ($8,685) · signal-only (no cash/fees) was +4.80%. Starts YES **2/30**. Fills 17 · skips 18 · realized $+210.43.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: money came in (prior rel vol ≥ 1.5) but price barely moved (|1-day| ≤ 1.2%).
- Must-have: no morning camera is red (the 'white' / all-clear row).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

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
- **Gate** `flow_in=True,zero_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,210.43.

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
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 197 | $50.62 | $2.58 | — | $24.65 | — | combo gate; gate flow_in=True,zero_red=True; list flatten; ⚪; ret5=+6.2; leftover $10000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.65 | ▲ close $10,784.79 vs 09:30 $10,000.00 (session +787.37) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.65 | ▲ 09:30 equity $10,916.78 vs yday $10,784.79 (+131.99) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `KULR` | 3 | $2.50 | $0.08 | — | $17.06 | — | combo gate; gate flow_in=True,zero_red=True; list earn_react; 🔵; ⚪; ret5=+7.6; leftover $8.22 | — |
| 2026-08-14 09:30 ET | **BUY** | `RLX` | 4 | $1.85 | $0.09 | — | $9.58 | — | combo gate; gate flow_in=True,zero_red=True; list earn_react; 🔵; ⚪; ret5=+0.5; leftover $8.22 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.58 | ▼ close $10,472.17 vs 09:30 $10,916.78 (session -444.44) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.58 | ▼ 09:30 equity $10,401.14 vs yday $10,472.17 (-71.03) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.58 | ▼ close $10,223.61 vs 09:30 $10,401.14 (session -177.53) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.58 | ▼ 09:30 equity $10,223.30 vs yday $10,223.61 (-0.31) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 197 | $51.77 | $2.70 | $+220.64 | $10,205.57 | ▲ +220.64 after sell → book $10,220.60; vs 09:30 mark -2.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,205.57 | ▲ close $10,220.60 vs 09:30 $10,223.30 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,205.57 | ▼ 09:30 equity $10,220.58 vs yday $10,220.60 (-0.02) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `KULR` | 3 | $2.55 | $0.11 | $-0.04 | $10,213.12 | ▼ -0.04 after sell → book $10,220.48; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `RLX` | 4 | $1.84 | $0.11 | $-0.23 | $10,220.37 | ▼ -0.23 after sell → book $10,220.37; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,220.37 | ▲ close $10,220.37 vs 09:30 $10,220.58 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,220.37 | ▲ 09:30 equity $10,220.37 vs yday $10,220.37 (+0.00) | — | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,220.37 | ▲ close $10,220.37 vs 09:30 $10,220.37 (session +0.00) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,220.37 | ▲ 09:30 equity $10,220.37 vs yday $10,220.37 (+0.00) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 4183 | $2.43 | $53.96 | — | $1.72 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $10220.37 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.72 | ▲ close $10,250.07 vs 09:30 $10,220.37 (session +83.66) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.72 | ▲ 09:30 equity $10,250.07 vs yday $10,250.07 (+0.00) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.72 | ▲ close $10,291.90 vs 09:30 $10,250.07 (session +41.83) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.72 | ▲ 09:30 equity $10,375.56 vs yday $10,291.90 (+83.66) | — | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.72 | ▲ close $10,752.03 vs 09:30 $10,375.56 (session +376.47) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.72 | ▲ 09:30 equity $10,752.03 vs yday $10,752.03 (+0.00) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `HITI` | 4183 | $2.57 | $54.74 | $+476.92 | $10,697.29 | ▲ +476.92 after sell → book $10,697.29; vs 09:30 mark -54.74 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,697.29 | ▲ close $10,697.29 vs 09:30 $10,752.03 (session +0.00) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,697.29 | ▲ 09:30 equity $10,697.29 vs yday $10,697.29 (+0.00) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,697.29 | ▲ close $10,697.29 vs 09:30 $10,697.29 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,697.29 | ▲ 09:30 equity $10,697.29 vs yday $10,697.29 (+0.00) | — | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,697.29 | ▲ close $10,697.29 vs 09:30 $10,697.29 (session +0.00) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,697.29 | ▲ 09:30 equity $10,697.29 vs yday $10,697.29 (+0.00) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,697.29 | ▲ close $10,697.29 vs 09:30 $10,697.29 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,697.29 | ▲ 09:30 equity $10,697.29 vs yday $10,697.29 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,697.29 | ▲ close $10,697.29 vs 09:30 $10,697.29 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,697.29 | ▲ 09:30 equity $10,697.29 vs yday $10,697.29 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,697.29 | ▲ close $10,697.29 vs 09:30 $10,697.29 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,697.29 | ▲ 09:30 equity $10,697.29 vs yday $10,697.29 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 925 | $11.54 | $11.93 | — | $10.86 | — | combo gate; gate flow_in=True,zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $10697.29 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.86 | ▼ close $10,602.11 vs 09:30 $10,697.29 (session -83.25) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.86 | ▼ 09:30 equity $10,472.61 vs yday $10,602.11 (-129.50) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ADCT` | 2 | $1.30 | $0.03 | — | $8.23 | — | combo gate; gate flow_in=True,zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+17.9; leftover $3.62 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.23 | ▲ close $10,542.07 vs 09:30 $10,472.61 (session +69.49) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.23 | ▼ 09:30 equity $10,389.39 vs yday $10,542.07 (-152.68) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.23 | ▼ close $10,352.33 vs 09:30 $10,389.39 (session -37.06) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.23 | ▼ 09:30 equity $10,222.79 vs yday $10,352.33 (-129.54) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `VIR` | 925 | $11.04 | $12.17 | $-486.60 | $10,208.06 | ▼ -486.60 after sell → book $10,210.62; vs 09:30 mark -12.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,208.06 | ▼ close $10,210.50 vs 09:30 $10,222.79 (session -0.12) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,208.06 | ▼ 09:30 equity $10,210.48 vs yday $10,210.50 (-0.02) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ADCT` | 2 | $1.21 | $0.05 | $-0.26 | $10,210.43 | ▼ -0.26 after sell → book $10,210.43; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.48 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,210.43 | ▲ 09:30 equity $10,210.43 vs yday $10,210.43 (-0.00) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.43 (session +0.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,210.43 | ▲ 09:30 equity $10,210.43 vs yday $10,210.43 (-0.00) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.43 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,210.43 | ▲ 09:30 equity $10,210.43 vs yday $10,210.43 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.43 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,210.43 | ▲ 09:30 equity $10,210.43 vs yday $10,210.43 (-0.00) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.43 (session +0.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,210.43 | ▲ 09:30 equity $10,210.43 vs yday $10,210.43 (-0.00) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.43 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,210.43 | ▲ 09:30 equity $10,210.43 vs yday $10,210.43 (-0.00) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.43 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,210.43 | ▲ 09:30 equity $10,210.43 vs yday $10,210.43 (-0.00) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.43 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,210.43 | ▲ 09:30 equity $10,210.43 vs yday $10,210.43 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.43 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,210.43 | ▲ 09:30 equity $10,210.43 vs yday $10,210.43 (-0.00) | — | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.43 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,210.43 | ▲ 09:30 equity $10,210.43 vs yday $10,210.43 (-0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.43 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,228.37 | ▲ 09:30 equity $9,254.56 vs yday $9,254.56 (-0.00) | 09:30 open · cash $9,228.37 (unchanged overnight, no fees) · equity $9,254.56 vs prior close $9,254.56 (-0.00) · 1 name(s) re-marked at the open (per-name table). BB×3 yday $8.73 → 09:30 $8.73 +0.00 | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,228.37 | ▲ close $9,254.56 vs 09:30 $9,254.56 (session +0.00) | 16:00 close · cash $9,228.37 · equity $9,254.56 vs 09:30 $9,254.56 (-0.00; session marks +0.00) · 1 name(s) marked open→close (per-name table). BB×3 09:30 $8.73 → close $8.73 -0.00 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,228.37 | ▼ 09:30 equity $9,252.19 vs yday $9,254.56 (-2.37) | 09:30 open · cash $9,228.37 (unchanged overnight, no fees) · equity $9,252.19 vs prior close $9,254.56 (-2.37) · 1 name(s) re-marked at the open (per-name table). BB×3 yday $8.73 → 09:30 $7.94 -2.37 | — |
| 2026-09-28 09:30 ET | **SELL** | `BB` | 3 | $7.94 | $0.27 | $-2.51 | $9,251.92 | ▼ -2.51 after sell → book $9,251.92; vs 09:30 mark -0.27 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,251.92 | ▲ close $9,251.92 vs 09:30 $9,252.19 (session +0.00) | 16:00 close · cash $9,251.92 · no lots left · equity $9,251.92. | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,251.92 | ▲ 09:30 equity $9,251.92 vs yday $9,251.92 (+0.00) | 09:30 open · cash $9,251.92 · no holdings · equity $9,251.92 vs prior close $9,251.92 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-29 09:30 ET | **BUY** | `KMX` | 153 | $60.41 | $2.45 | — | $7.51 | — | combo gate; gate flow_in=True,zero_red=True; list earn_react; 🔵; ⚪; ret5=-2.3; leftover $9251.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.51 | ▼ close $9,069.70 vs 09:30 $9,251.92 (session -179.78) | 16:00 close · cash $7.51 · equity $9,069.70 vs 09:30 $9,251.92 (-182.22; session marks -179.78) · 1 name(s) marked open→close (per-name table). KMX×153 09:30 $60.41 → close $59.23 -179.78 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.51 | ▲ 09:30 equity $9,069.70 vs yday $9,069.70 (-0.00) | 09:30 open · cash $7.51 (unchanged overnight, no fees) · equity $9,069.70 vs prior close $9,069.70 (-0.00) · 1 name(s) re-marked at the open (per-name table). KMX×153 yday $59.23 → 09:30 $59.23 +0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.51 | ▲ close $9,069.70 vs 09:30 $9,069.70 (session +0.00) | 16:00 close · cash $7.51 · equity $9,069.70 vs 09:30 $9,069.70 (-0.00; session marks +0.00) · 1 name(s) marked open→close (per-name table). KMX×153 09:30 $59.23 → close $59.23 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.51 | ▼ 09:30 equity $8,463.82 vs yday $9,069.70 (-605.88) | 09:30 open · cash $7.51 (unchanged overnight, no fees) · equity $8,463.82 vs prior close $9,069.70 (-605.88) · 1 name(s) re-marked at the open (per-name table). KMX×153 yday $59.23 → 09:30 $55.27 -605.88 | — |
| 2026-10-01 09:30 ET | **SELL** | `KMX` | 153 | $55.27 | $2.54 | $-790.65 | $8,461.28 | ▼ -790.65 after sell → book $8,461.28; vs 09:30 mark -2.54 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,461.28 | ▲ close $8,461.28 vs 09:30 $8,463.82 (session +0.00) | 16:00 close · cash $8,461.28 · no lots left · equity $8,461.28. | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,461.28 | ▲ 09:30 equity $8,461.28 vs yday $8,461.28 (+0.00) | 09:30 open · cash $8,461.28 · no holdings · equity $8,461.28 vs prior close $8,461.28 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-02 09:30 ET | **BUY** | `ETON` | 161 | $52.42 | $2.47 | — | $19.19 | — | combo gate; gate flow_in=True,zero_red=True; list flatten; 🔵; ⚪; ret5=-12.6; leftover $8461.28 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.19 | ▲ close $8,932.15 vs 09:30 $8,461.28 (session +473.34) | 16:00 close · cash $19.19 · equity $8,932.15 vs 09:30 $8,461.28 (+470.87; session marks +473.34) · 1 name(s) marked open→close (per-name table). ETON×161 09:30 $52.42 → close $55.36 +473.34 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.19 | ▲ 09:30 equity $9,011.04 vs yday $8,932.15 (+78.89) | 09:30 open · cash $19.19 (unchanged overnight, no fees) · equity $9,011.04 vs prior close $8,932.15 (+78.89) · 1 name(s) re-marked at the open (per-name table). ETON×161 yday $55.36 → 09:30 $55.85 +78.89 | — |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.19 | ▼ close $8,974.01 vs 09:30 $9,011.04 (session -37.03) | 16:00 close · cash $19.19 · equity $8,974.01 vs 09:30 $9,011.04 (-37.03; session marks -37.03) · 1 name(s) marked open→close (per-name table). ETON×161 09:30 $55.85 → close $55.62 -37.03 | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.19 | ▲ 09:30 equity $9,059.34 vs yday $8,974.01 (+85.33) | 09:30 open · cash $19.19 (unchanged overnight, no fees) · equity $9,059.34 vs prior close $8,974.01 (+85.33) · 1 name(s) re-marked at the open (per-name table). ETON×161 yday $55.62 → 09:30 $56.15 +85.33 | — |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.19 | ▼ close $8,837.16 vs 09:30 $9,059.34 (session -222.18) | 16:00 close · cash $19.19 · equity $8,837.16 vs 09:30 $9,059.34 (-222.18; session marks -222.18) · 1 name(s) marked open→close (per-name table). ETON×161 09:30 $56.15 → close $54.77 -222.18 | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.19 | ▼ 09:30 equity $8,687.43 vs yday $8,837.16 (-149.73) | 09:30 open · cash $19.19 (unchanged overnight, no fees) · equity $8,687.43 vs prior close $8,837.16 (-149.73) · 1 name(s) re-marked at the open (per-name table). ETON×161 yday $54.77 → 09:30 $53.84 -149.73 | — |
| 2026-10-07 09:30 ET | **SELL** | `ETON` | 161 | $53.84 | $2.57 | $+223.58 | $8,684.86 | ▲ +223.58 after sell → book $8,684.86; vs 09:30 mark -2.57 | dropped from list after 3 sess (min 3) | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,684.86 | ▲ close $8,684.86 vs 09:30 $8,687.43 (session +0.00) | 16:00 close · cash $8,684.86 · no lots left · equity $8,684.86. | — |
| 2026-10-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,684.86 | ▲ 09:30 equity $8,684.86 vs yday $8,684.86 (+0.00) | 09:30 open · cash $8,684.86 · no holdings · equity $8,684.86 vs prior close $8,684.86 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,684.86 | ▲ close $8,684.86 vs 09:30 $8,684.86 (session +0.00) | 16:00 close · cash $8,684.86 · no lots left · equity $8,684.86. | — |
| 2026-10-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,684.86 | ▲ 09:30 equity $8,684.86 vs yday $8,684.86 (+0.00) | 09:30 open · cash $8,684.86 · no holdings · equity $8,684.86 vs prior close $8,684.86 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,684.86 | ▲ close $8,684.86 vs 09:30 $8,684.86 (session +0.00) | 16:00 close · cash $8,684.86 · no lots left · equity $8,684.86. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SPHR` | cash | leftover split 8.22 < 1 share @ 176.68 |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `KULR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `RLX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `XP` | cash | leftover split 9.58 < 1 share @ 15.93 |
| 2026-08-18 | `KULR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `RLX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HITI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `HITI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `RHI` | cash | leftover split 0.86 < 1 share @ 43.76 |
| 2026-08-25 | `ABUS` | cash | leftover split 0.86 < 1 share @ 5.25 |
| 2026-09-04 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ATRC` | cash | leftover split 3.62 < 1 share @ 52.03 |
| 2026-09-04 | `XP` | cash | leftover split 3.62 < 1 share @ 19.67 |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ADCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-09 | `ADCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
