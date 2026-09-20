# Factor mine action — `union_clk_earn_guide_react_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · Clock-B #3 knowable E + guidance proxy + react (research; not KEEP)

Cash book **-5.43%** ($9,457) · signal-only (no cash/fees) was +10.90%. Starts YES **0/26**. Fills 20 · skips 1 · realized $-543.00.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,457.00.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `WMT` | 93 | — | $106.38 | +0.00 | $103.84 | -236.22 | -236.22 | +0.00 | -236.22 |
| 2026-08-21 | `WMT` | 93 | $103.84 | $103.69 | -13.95 | — | +0.00 | -13.95 | -250.17 | — |
| 2026-08-24 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-25 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-26 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-27 | `CM` | 41 | — | $118.77 | +0.00 | $114.84 | -161.13 | -161.13 | +0.00 | -161.13 |
| 2026-08-27 | `NVDA` | 21 | — | $222.86 | +0.00 | $227.98 | +107.52 | +107.52 | +0.00 | +107.52 |
| 2026-08-28 | `CM` | 41 | $114.84 | $115.66 | +33.62 | — | +0.00 | +33.62 | -127.51 | — |
| 2026-08-28 | `NVDA` | 21 | $227.98 | $227.36 | -13.02 | — | +0.00 | -13.02 | +94.50 | — |
| 2026-08-28 | `ADSK` | 37 | — | $261.16 | +0.00 | $260.66 | -18.50 | -18.50 | +0.00 | -18.50 |
| 2026-08-31 | `ADSK` | 37 | $260.66 | $257.71 | -109.15 | — | +0.00 | -109.15 | -127.65 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 9 | — | $351.74 | +0.00 | $357.16 | +48.78 | +48.78 | +0.00 | +48.78 |
| 2026-09-03 | `HPE` | 67 | — | $47.60 | +0.00 | $54.44 | +458.28 | +458.28 | +0.00 | +458.28 |
| 2026-09-03 | `CIEN` | 9 | — | $354.49 | +0.00 | $317.46 | -333.27 | -333.27 | +0.00 | -333.27 |
| 2026-09-04 | `AVGO` | 9 | $357.16 | $359.70 | +22.86 | — | +0.00 | +22.86 | +71.64 | — |
| 2026-09-04 | `HPE` | 67 | $54.44 | $53.85 | -39.53 | — | +0.00 | -39.53 | +418.75 | — |
| 2026-09-04 | `CIEN` | 9 | $317.46 | $321.67 | +37.89 | — | +0.00 | +37.89 | -295.38 | — |
| 2026-09-08 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 19 | — | $164.43 | +0.00 | $150.28 | -268.85 | -268.85 | +0.00 | -268.85 |
| 2026-09-11 | `ADBE` | 13 | — | $242.17 | +0.00 | $252.23 | +130.78 | +130.78 | +0.00 | +130.78 |
| 2026-09-11 | `RH` | 23 | — | $135.71 | +0.00 | $134.07 | -37.72 | -37.72 | +0.00 | -37.72 |
| 2026-09-14 | `ORCL` | 19 | $150.28 | $141.42 | -168.34 | — | +0.00 | -168.34 | -437.19 | — |
| 2026-09-14 | `ADBE` | 13 | $252.23 | $261.51 | +120.64 | — | +0.00 | +120.64 | +251.42 | — |
| 2026-09-14 | `RH` | 23 | $134.07 | $131.40 | -61.41 | — | +0.00 | -61.41 | -99.13 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | -236.22 | WMT | — | $104.39 | $9,761.51 | WMT×93 |
| 2026-08-21 | +3.25 | $104.39 | WMT×93 | $9,747.56 | -13.95 | +0.00 | — | WMT | $9,745.20 | $9,745.20 | — |
| 2026-08-24 | -5.17 | $9,745.20 | — | $9,745.20 | -0.00 | +0.00 | — | — | $9,745.20 | $9,745.20 | — |
| 2026-08-25 | +1.80 | $9,745.20 | — | $9,745.20 | -0.00 | +0.00 | — | — | $9,745.20 | $9,745.20 | — |
| 2026-08-26 | +2.02 | $9,745.20 | — | $9,745.20 | -0.00 | +0.00 | — | — | $9,745.20 | $9,745.20 | — |
| 2026-08-27 | — | $9,745.20 | — | $9,745.20 | -0.00 | -53.61 | CM, NVDA | — | $191.40 | $9,687.42 | CM×41, NVDA×21 |
| 2026-08-28 | +0.75 | $191.40 | CM×41, NVDA×21 | $9,708.02 | +20.60 | -18.50 | ADSK | CM, NVDA | $38.74 | $9,683.16 | ADSK×37 |
| 2026-08-31 | -5.85 | $38.74 | ADSK×37 | $9,574.01 | -109.15 | +0.00 | — | ADSK | $9,571.82 | $9,571.82 | — |
| 2026-09-01 | -6.30 | $9,571.82 | — | $9,571.82 | +0.00 | +0.00 | — | — | $9,571.82 | $9,571.82 | — |
| 2026-09-02 | -3.83 | $9,571.82 | — | $9,571.82 | +0.00 | +0.00 | — | — | $9,571.82 | $9,571.82 | — |
| 2026-09-03 | -0.90 | $9,571.82 | — | $9,571.82 | +0.00 | +173.79 | AVGO, HPE, CIEN | — | $20.33 | $9,739.39 | AVGO×9, HPE×67, CIEN×9 |
| 2026-09-04 | +2.25 | $20.33 | AVGO×9, HPE×67, CIEN×9 | $9,760.61 | +21.22 | +0.00 | — | AVGO, HPE, CIEN | $9,754.27 | $9,754.27 | — |
| 2026-09-08 | -11.47 | $9,754.27 | — | $9,754.27 | +0.00 | +0.00 | — | — | $9,754.27 | $9,754.27 | — |
| 2026-09-09 | -13.95 | $9,754.27 | — | $9,754.27 | +0.00 | +0.00 | — | — | $9,754.27 | $9,754.27 | — |
| 2026-09-10 | -13.28 | $9,754.27 | — | $9,754.27 | +0.00 | +0.00 | — | — | $9,754.27 | $9,754.27 | — |
| 2026-09-11 | +0.50 | $9,754.27 | — | $9,754.27 | +0.00 | -175.79 | ORCL, ADBE, RH | — | $354.43 | $9,572.35 | ORCL×19, ADBE×13, RH×23 |
| 2026-09-14 | -11.00 | $354.43 | ORCL×19, ADBE×13, RH×23 | $9,463.24 | -109.11 | +0.00 | — | ORCL, ADBE, RH | $9,457.00 | $9,457.00 | — |
| 2026-09-15 | -3.84 | $9,457.00 | — | $9,457.00 | +0.00 | +0.00 | — | — | $9,457.00 | $9,457.00 | — |
| 2026-09-16 | +5.30 | $9,457.00 | — | $9,457.00 | +0.00 | +0.00 | — | — | $9,457.00 | $9,457.00 | — |
| 2026-09-17 | +7.38 | $9,457.00 | — | $9,457.00 | +0.00 | +0.00 | — | — | $9,457.00 | $9,457.00 | — |
| 2026-09-18 | +4.86 | $9,457.00 | — | $9,457.00 | +0.00 | +0.00 | — | — | $9,457.00 | $9,457.00 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `WMT` | 93 | $106.38 | $2.27 | — | $104.39 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list earn_react; 🔵; ret5=-1.7; leftover $10000.00 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $104.39 | ▼ close $9,761.51 vs 09:30 $10,000.00 (session -236.22) | 16:00 close · cash $104.39 · equity $9,761.51 vs 09:30 $10,000.00 (-238.49; session marks -236.22) · 1 name(s) marked open→close (per-name table). WMT×93 09:30 $106.38 → close $103.84 -236.22 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.39 | ▼ 09:30 equity $9,747.56 vs yday $9,761.51 (-13.95) | 09:30 open · cash $104.39 (unchanged overnight, no fees) · equity $9,747.56 vs prior close $9,761.51 (-13.95) · 1 name(s) re-marked at the open (per-name table). WMT×93 yday $103.84 → 09:30 $103.69 -13.95 | — |
| 2026-08-21 09:30 ET | **SELL** | `WMT` | 93 | $103.69 | $2.36 | $-254.80 | $9,745.20 | ▼ -254.80 after sell → book $9,745.20; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,745.20 | ▲ close $9,745.20 vs 09:30 $9,747.56 (session +0.00) | 16:00 close · cash $9,745.20 · no lots left · equity $9,745.20. | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,745.20 | ▲ 09:30 equity $9,745.20 vs yday $9,745.20 (-0.00) | 09:30 open · cash $9,745.20 · no holdings · equity $9,745.20 vs prior close $9,745.20 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,745.20 | ▲ close $9,745.20 vs 09:30 $9,745.20 (session +0.00) | 16:00 close · cash $9,745.20 · no lots left · equity $9,745.20. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,745.20 | ▲ 09:30 equity $9,745.20 vs yday $9,745.20 (-0.00) | 09:30 open · cash $9,745.20 · no holdings · equity $9,745.20 vs prior close $9,745.20 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,745.20 | ▲ close $9,745.20 vs 09:30 $9,745.20 (session +0.00) | 16:00 close · cash $9,745.20 · no lots left · equity $9,745.20. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,745.20 | ▲ 09:30 equity $9,745.20 vs yday $9,745.20 (-0.00) | 09:30 open · cash $9,745.20 · no holdings · equity $9,745.20 vs prior close $9,745.20 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,745.20 | ▲ close $9,745.20 vs 09:30 $9,745.20 (session +0.00) | 16:00 close · cash $9,745.20 · no lots left · equity $9,745.20. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,745.20 | ▲ 09:30 equity $9,745.20 vs yday $9,745.20 (-0.00) | 09:30 open · cash $9,745.20 · no holdings · equity $9,745.20 vs prior close $9,745.20 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 41 | $118.77 | $2.11 | — | $4,873.52 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list earn_react; ret5=+0.3; leftover $4872.60 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 21 | $222.86 | $2.05 | — | $191.40 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $4872.60 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $191.40 | ▼ close $9,687.42 vs 09:30 $9,745.20 (session -53.61) | 16:00 close · cash $191.40 · equity $9,687.42 vs 09:30 $9,745.20 (-57.78; session marks -53.61) · 2 name(s) marked open→close (per-name table). CM×41 09:30 $118.77 → close $114.84 -161.13; NVDA×21 09:30 $222.86 → close $227.98 +107.52 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $191.40 | ▲ 09:30 equity $9,708.02 vs yday $9,687.42 (+20.60) | 09:30 open · cash $191.40 (unchanged overnight, no fees) · equity $9,708.02 vs prior close $9,687.42 (+20.60) · 2 name(s) re-marked at the open (per-name table). CM×41 yday $114.84 → 09:30 $115.66 +33.62; NVDA×21 yday $227.98 → 09:30 $227.36 -13.02 | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 41 | $115.66 | $2.16 | $-131.78 | $4,931.30 | ▼ -131.78 after sell → book $9,705.86; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 21 | $227.36 | $2.10 | $+90.35 | $9,703.76 | ▲ +90.35 after sell → book $9,703.76; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 37 | $261.16 | $2.10 | — | $38.74 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list earn_react; ret5=+7.8; leftover $9703.76 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.74 | ▼ close $9,683.16 vs 09:30 $9,708.02 (session -18.50) | 16:00 close · cash $38.74 · equity $9,683.16 vs 09:30 $9,708.02 (-24.86; session marks -18.50) · 1 name(s) marked open→close (per-name table). ADSK×37 09:30 $261.16 → close $260.66 -18.50 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.74 | ▼ 09:30 equity $9,574.01 vs yday $9,683.16 (-109.15) | 09:30 open · cash $38.74 (unchanged overnight, no fees) · equity $9,574.01 vs prior close $9,683.16 (-109.15) · 1 name(s) re-marked at the open (per-name table). ADSK×37 yday $260.66 → 09:30 $257.71 -109.15 | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 37 | $257.71 | $2.19 | $-131.94 | $9,571.82 | ▼ -131.94 after sell → book $9,571.82; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,571.82 | ▲ close $9,571.82 vs 09:30 $9,574.01 (session +0.00) | 16:00 close · cash $9,571.82 · no lots left · equity $9,571.82. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,571.82 | ▲ 09:30 equity $9,571.82 vs yday $9,571.82 (+0.00) | 09:30 open · cash $9,571.82 · no holdings · equity $9,571.82 vs prior close $9,571.82 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,571.82 | ▲ close $9,571.82 vs 09:30 $9,571.82 (session +0.00) | 16:00 close · cash $9,571.82 · no lots left · equity $9,571.82. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,571.82 | ▲ 09:30 equity $9,571.82 vs yday $9,571.82 (+0.00) | 09:30 open · cash $9,571.82 · no holdings · equity $9,571.82 vs prior close $9,571.82 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,571.82 | ▲ close $9,571.82 vs 09:30 $9,571.82 (session +0.00) | 16:00 close · cash $9,571.82 · no lots left · equity $9,571.82. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,571.82 | ▲ 09:30 equity $9,571.82 vs yday $9,571.82 (+0.00) | 09:30 open · cash $9,571.82 · no holdings · equity $9,571.82 vs prior close $9,571.82 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 9 | $351.74 | $2.02 | — | $6,404.15 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $3190.61 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 67 | $47.60 | $2.19 | — | $3,212.75 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list earn_react,oppset; 🔵; ret5=-6.2; leftover $3190.61 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 9 | $354.49 | $2.02 | — | $20.33 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list earn_react; 🔵; ret5=-12.3; leftover $3190.61 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.33 | ▲ close $9,739.39 vs 09:30 $9,571.82 (session +173.79) | 16:00 close · cash $20.33 · equity $9,739.39 vs 09:30 $9,571.82 (+167.57; session marks +173.79) · 3 name(s) marked open→close (per-name table). AVGO×9 09:30 $351.74 → close $357.16 +48.78; HPE×67 09:30 $47.60 → close $54.44 +458.28; CIEN×9 09:30 $354.49 → close $317.46 -333.27 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.33 | ▲ 09:30 equity $9,760.61 vs yday $9,739.39 (+21.22) | 09:30 open · cash $20.33 (unchanged overnight, no fees) · equity $9,760.61 vs prior close $9,739.39 (+21.22) · 3 name(s) re-marked at the open (per-name table). AVGO×9 yday $357.16 → 09:30 $359.70 +22.86; HPE×67 yday $54.44 → 09:30 $53.85 -39.53; CIEN×9 yday $317.46 → 09:30 $321.67 +37.89 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 9 | $359.70 | $2.05 | $+67.57 | $3,255.58 | ▲ +67.57 after sell → book $9,758.56; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 67 | $53.85 | $2.23 | $+414.33 | $6,861.29 | ▲ +414.33 after sell → book $9,756.32; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟡 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 9 | $321.67 | $2.05 | $-299.45 | $9,754.27 | ▼ -299.45 after sell → book $9,754.27; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,754.27 | ▲ close $9,754.27 vs 09:30 $9,760.61 (session +0.00) | 16:00 close · cash $9,754.27 · no lots left · equity $9,754.27. | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,754.27 | ▲ 09:30 equity $9,754.27 vs yday $9,754.27 (+0.00) | 09:30 open · cash $9,754.27 · no holdings · equity $9,754.27 vs prior close $9,754.27 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,754.27 | ▲ close $9,754.27 vs 09:30 $9,754.27 (session +0.00) | 16:00 close · cash $9,754.27 · no lots left · equity $9,754.27. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,754.27 | ▲ 09:30 equity $9,754.27 vs yday $9,754.27 (+0.00) | 09:30 open · cash $9,754.27 · no holdings · equity $9,754.27 vs prior close $9,754.27 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,754.27 | ▲ close $9,754.27 vs 09:30 $9,754.27 (session +0.00) | 16:00 close · cash $9,754.27 · no lots left · equity $9,754.27. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,754.27 | ▲ 09:30 equity $9,754.27 vs yday $9,754.27 (+0.00) | 09:30 open · cash $9,754.27 · no holdings · equity $9,754.27 vs prior close $9,754.27 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,754.27 | ▲ close $9,754.27 vs 09:30 $9,754.27 (session +0.00) | 16:00 close · cash $9,754.27 · no lots left · equity $9,754.27. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,754.27 | ▲ 09:30 equity $9,754.27 vs yday $9,754.27 (+0.00) | 09:30 open · cash $9,754.27 · no holdings · equity $9,754.27 vs prior close $9,754.27 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 19 | $164.43 | $2.05 | — | $6,628.06 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $3251.42 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 13 | $242.17 | $2.03 | — | $3,477.82 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list earn_react; ret5=-11.1; leftover $3251.42 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 23 | $135.71 | $2.06 | — | $354.43 | — | Clock-B #3 knowable E + guidance proxy + react (research; not KEEP); gate clk_earn_guide_react=True; rank cond; list earn_react,oppset; ret5=-9.2; leftover $3251.42 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $354.43 | ▼ close $9,572.35 vs 09:30 $9,754.27 (session -175.79) | 16:00 close · cash $354.43 · equity $9,572.35 vs 09:30 $9,754.27 (-181.92; session marks -175.79) · 3 name(s) marked open→close (per-name table). ORCL×19 09:30 $164.43 → close $150.28 -268.85; ADBE×13 09:30 $242.17 → close $252.23 +130.78; RH×23 09:30 $135.71 → close $134.07 -37.72 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $354.43 | ▼ 09:30 equity $9,463.24 vs yday $9,572.35 (-109.11) | 09:30 open · cash $354.43 (unchanged overnight, no fees) · equity $9,463.24 vs prior close $9,572.35 (-109.11) · 3 name(s) re-marked at the open (per-name table). ORCL×19 yday $150.28 → 09:30 $141.42 -168.34; ADBE×13 yday $252.23 → 09:30 $261.51 +120.64; RH×23 yday $134.07 → 09:30 $131.40 -61.41 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 19 | $141.42 | $2.08 | $-441.32 | $3,039.33 | ▼ -441.32 after sell → book $9,461.16; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 13 | $261.51 | $2.07 | $+247.32 | $6,436.89 | ▲ +247.32 after sell → book $9,459.09; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 23 | $131.40 | $2.09 | $-103.28 | $9,457.00 | ▼ -103.28 after sell → book $9,457.00; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,457.00 | ▲ close $9,457.00 vs 09:30 $9,463.24 (session +0.00) | 16:00 close · cash $9,457.00 · no lots left · equity $9,457.00. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,457.00 | ▲ 09:30 equity $9,457.00 vs yday $9,457.00 (+0.00) | 09:30 open · cash $9,457.00 · no holdings · equity $9,457.00 vs prior close $9,457.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,457.00 | ▲ close $9,457.00 vs 09:30 $9,457.00 (session +0.00) | 16:00 close · cash $9,457.00 · no lots left · equity $9,457.00. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,457.00 | ▲ 09:30 equity $9,457.00 vs yday $9,457.00 (+0.00) | 09:30 open · cash $9,457.00 · no holdings · equity $9,457.00 vs prior close $9,457.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,457.00 | ▲ close $9,457.00 vs 09:30 $9,457.00 (session +0.00) | 16:00 close · cash $9,457.00 · no lots left · equity $9,457.00. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,457.00 | ▲ 09:30 equity $9,457.00 vs yday $9,457.00 (+0.00) | 09:30 open · cash $9,457.00 · no holdings · equity $9,457.00 vs prior close $9,457.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,457.00 | ▲ close $9,457.00 vs 09:30 $9,457.00 (session +0.00) | 16:00 close · cash $9,457.00 · no lots left · equity $9,457.00. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,457.00 | ▲ 09:30 equity $9,457.00 vs yday $9,457.00 (+0.00) | 09:30 open · cash $9,457.00 · no holdings · equity $9,457.00 vs prior close $9,457.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,457.00 | ▲ close $9,457.00 vs 09:30 $9,457.00 (session +0.00) | 16:00 close · cash $9,457.00 · no lots left · equity $9,457.00. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-09-10 | `WLTH` | hard_red | hard-red S=-13.28 sit; no new buys |
