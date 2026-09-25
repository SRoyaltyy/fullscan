# Factor mine action — `union_clk_earn_guide_react_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · Clock-B #3 knowable E + guidance proxy + react (research; not KEEP)

Cash book **-4.37%** ($9,563) · signal-only (no cash/fees) was +8.19%. Starts YES **0/30**. Fills 22 · skips 1 · realized $-764.31.

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
- Must-have: Clock-B #3: knowable earnings improvement, a raised-guidance headline proxy, and a favorable reaction window — all public by 09:30 (same-day E after the open does not count).
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
- **Gate** `clk_earn_guide_react=True` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,235.69.

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
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `WMT` | 93 | $106.38 | $2.27 | — | $104.39 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list earn_react; 🔵; ret5=-1.7; leftover $10000.00 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $104.39 | ▼ close $9,761.51 vs 09:30 $10,000.00 (session -236.22) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.39 | ▼ 09:30 equity $9,747.56 vs yday $9,761.51 (-13.95) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `WMT` | 93 | $103.69 | $2.36 | $-254.80 | $9,745.20 | ▼ -254.80 after sell → book $9,745.20; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,745.20 | ▲ close $9,745.20 vs 09:30 $9,747.56 (session +0.00) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,745.20 | ▲ 09:30 equity $9,745.20 vs yday $9,745.20 (-0.00) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,745.20 | ▲ close $9,745.20 vs 09:30 $9,745.20 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,745.20 | ▲ 09:30 equity $9,745.20 vs yday $9,745.20 (-0.00) | — | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,745.20 | ▲ close $9,745.20 vs 09:30 $9,745.20 (session +0.00) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,745.20 | ▲ 09:30 equity $9,745.20 vs yday $9,745.20 (-0.00) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,745.20 | ▲ close $9,745.20 vs 09:30 $9,745.20 (session +0.00) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,745.20 | ▲ 09:30 equity $9,745.20 vs yday $9,745.20 (-0.00) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 41 | $118.77 | $2.11 | — | $4,873.52 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list earn_react; ret5=+0.3; leftover $4872.60 | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 21 | $222.86 | $2.05 | — | $191.40 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $4872.60 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $191.40 | ▼ close $9,687.42 vs 09:30 $9,745.20 (session -53.61) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $191.40 | ▲ 09:30 equity $9,708.02 vs yday $9,687.42 (+20.60) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 41 | $115.66 | $2.16 | $-131.78 | $4,931.30 | ▼ -131.78 after sell → book $9,705.86; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 21 | $227.36 | $2.10 | $+90.35 | $9,703.76 | ▲ +90.35 after sell → book $9,703.76; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 37 | $261.16 | $2.10 | — | $38.74 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list earn_react; ret5=+7.8; leftover $9703.76 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.74 | ▼ close $9,683.16 vs 09:30 $9,708.02 (session -18.50) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.74 | ▼ 09:30 equity $9,574.01 vs yday $9,683.16 (-109.15) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 37 | $257.71 | $2.19 | $-131.94 | $9,571.82 | ▼ -131.94 after sell → book $9,571.82; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,571.82 | ▲ close $9,571.82 vs 09:30 $9,574.01 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,571.82 | ▲ 09:30 equity $9,571.82 vs yday $9,571.82 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,571.82 | ▲ close $9,571.82 vs 09:30 $9,571.82 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,571.82 | ▲ 09:30 equity $9,571.82 vs yday $9,571.82 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,571.82 | ▲ close $9,571.82 vs 09:30 $9,571.82 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,571.82 | ▲ 09:30 equity $9,571.82 vs yday $9,571.82 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 9 | $351.74 | $2.02 | — | $6,404.15 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $3190.61 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 67 | $47.60 | $2.19 | — | $3,212.75 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $3190.61 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 9 | $354.49 | $2.02 | — | $20.33 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list earn_react; 🔵; ret5=-12.3; leftover $3190.61 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.33 | ▲ close $9,739.39 vs 09:30 $9,571.82 (session +173.79) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.33 | ▲ 09:30 equity $9,760.61 vs yday $9,739.39 (+21.22) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 9 | $359.70 | $2.05 | $+67.57 | $3,255.58 | ▲ +67.57 after sell → book $9,758.56; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 67 | $53.85 | $2.23 | $+414.33 | $6,861.29 | ▲ +414.33 after sell → book $9,756.32; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 9 | $321.67 | $2.05 | $-299.45 | $9,754.27 | ▼ -299.45 after sell → book $9,754.27; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,754.27 | ▲ close $9,754.27 vs 09:30 $9,760.61 (session +0.00) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,754.27 | ▲ 09:30 equity $9,754.27 vs yday $9,754.27 (+0.00) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,754.27 | ▲ close $9,754.27 vs 09:30 $9,754.27 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,754.27 | ▲ 09:30 equity $9,754.27 vs yday $9,754.27 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,754.27 | ▲ close $9,754.27 vs 09:30 $9,754.27 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,754.27 | ▲ 09:30 equity $9,754.27 vs yday $9,754.27 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,754.27 | ▲ close $9,754.27 vs 09:30 $9,754.27 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,754.27 | ▲ 09:30 equity $9,754.27 vs yday $9,754.27 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 19 | $164.43 | $2.05 | — | $6,628.06 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $3251.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 13 | $242.17 | $2.03 | — | $3,477.82 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list earn_react; ret5=-11.1; leftover $3251.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 23 | $135.71 | $2.06 | — | $354.43 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list earn_react; ret5=-9.2; leftover $3251.42 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $354.43 | ▼ close $9,572.35 vs 09:30 $9,754.27 (session -175.79) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $354.43 | ▼ 09:30 equity $9,463.24 vs yday $9,572.35 (-109.11) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 19 | $141.42 | $2.08 | $-441.32 | $3,039.33 | ▼ -441.32 after sell → book $9,461.16; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 13 | $261.51 | $2.07 | $+247.32 | $6,436.89 | ▲ +247.32 after sell → book $9,459.09; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 23 | $131.40 | $2.09 | $-103.28 | $9,457.00 | ▼ -103.28 after sell → book $9,457.00; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,457.00 | ▲ close $9,457.00 vs 09:30 $9,463.24 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,457.00 | ▲ 09:30 equity $9,457.00 vs yday $9,457.00 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,457.00 | ▲ close $9,457.00 vs 09:30 $9,457.00 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,457.00 | ▲ 09:30 equity $9,457.00 vs yday $9,457.00 (+0.00) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,457.00 | ▲ close $9,457.00 vs 09:30 $9,457.00 (session +0.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,457.00 | ▲ 09:30 equity $9,457.00 vs yday $9,457.00 (+0.00) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,457.00 | ▲ close $9,457.00 vs 09:30 $9,457.00 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,457.00 | ▲ 09:30 equity $9,457.00 vs yday $9,457.00 (+0.00) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,457.00 | ▲ close $9,457.00 vs 09:30 $9,457.00 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,457.00 | ▲ 09:30 equity $9,457.00 vs yday $9,457.00 (+0.00) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,457.00 | ▲ close $9,457.00 vs 09:30 $9,457.00 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,457.00 | ▲ 09:30 equity $9,457.00 vs yday $9,457.00 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,457.00 | ▲ close $9,457.00 vs 09:30 $9,457.00 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,457.00 | ▲ 09:30 equity $9,457.00 vs yday $9,457.00 (+0.00) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 48 | $196.78 | $2.13 | — | $9.43 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $9457.00 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.43 | ▼ close $9,223.99 vs 09:30 $9,457.00 (session -230.88) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.43 | ▲ 09:30 equity $9,237.91 vs yday $9,223.99 (+13.92) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 48 | $192.26 | $2.22 | $-221.31 | $9,235.69 | ▼ -221.31 after sell → book $9,235.69; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,235.69 | ▲ close $9,235.69 vs 09:30 $9,237.91 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,563.02 | ▲ 09:30 equity $9,563.02 vs yday $9,563.02 (+0.00) | 09:30 open · cash $9,563.02 · no holdings · equity $9,563.02 vs prior close $9,563.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,563.02 | ▲ close $9,563.02 vs 09:30 $9,563.02 (session +0.00) | 16:00 close · cash $9,563.02 · no lots left · equity $9,563.02. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-09-10 | `WLTH` | hard_red | hard-red S=-13.28 sit; no new buys |
