# Factor mine action — `union_catal_present_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ catal_present, no 🚨

Cash book **-11.49%** ($8,851) · signal-only (no cash/fees) was -22.70%. Starts YES **0/30**. Fills 14 · skips 18 · realized $-156.79.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the catalyst camera printed something (any color, not blank).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

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
- **Gate** `catal_present=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,843.21.

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
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-26 09:30 ET | **BUY** | `CRM` | 25 | $199.94 | $2.06 | — | $4,999.44 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list overnight,overnight_mega; ret5=+2.1; leftover $5000.00 | — |
| 2026-08-26 09:30 ET | **BUY** | `NVDA` | 23 | $212.64 | $2.06 | — | $106.66 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list overnight,overnight_mega; 🔵; ret5=-3.0; leftover $5000.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.66 | ▲ close $10,069.34 vs 09:30 $10,000.00 (session +73.46) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.66 | ▲ 09:30 equity $10,983.69 vs yday $10,069.34 (+914.35) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.66 | ▲ close $11,651.45 vs 09:30 $10,983.69 (session +667.76) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.66 | ▼ 09:30 equity $11,597.69 vs yday $11,651.45 (-53.76) | — | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.66 | ▼ close $11,510.31 vs 09:30 $11,597.69 (session -87.38) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.66 | ▼ 09:30 equity $11,500.42 vs yday $11,510.31 (-9.89) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CRM` | 25 | $254.39 | $2.13 | $+1357.06 | $6,464.28 | ▲ +1,357.06 after sell → book $11,498.29; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `NVDA` | 23 | $218.87 | $2.11 | $+139.12 | $11,496.18 | ▲ +139.12 after sell → book $11,496.18; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,496.18 | ▲ close $11,496.18 vs 09:30 $11,500.42 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,496.18 | ▲ 09:30 equity $11,496.18 vs yday $11,496.18 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,496.18 | ▲ close $11,496.18 vs 09:30 $11,496.18 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,496.18 | ▲ 09:30 equity $11,496.18 vs yday $11,496.18 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,496.18 | ▲ close $11,496.18 vs 09:30 $11,496.18 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,496.18 | ▲ 09:30 equity $11,496.18 vs yday $11,496.18 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `CF` | 41 | $138.60 | $2.11 | — | $5,811.47 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list ohlc_hot; 🔵; ret5=+10.8; leftover $5748.09 | — |
| 2026-09-03 09:30 ET | **BUY** | `CTVA` | 63 | $90.24 | $2.18 | — | $124.17 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list ohlc_hot; 🔵; ret5=+8.6; leftover $5748.09 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.17 | ▼ close $11,357.44 vs 09:30 $11,496.18 (session -134.45) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.17 | ▼ 09:30 equity $11,198.12 vs yday $11,357.44 (-159.32) | — | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.17 | ▼ close $11,126.70 vs 09:30 $11,198.12 (session -71.42) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.17 | ▲ 09:30 equity $11,180.38 vs yday $11,126.70 (+53.68) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.17 | ▼ close $11,043.40 vs 09:30 $11,180.38 (session -136.98) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.17 | ▲ 09:30 equity $11,220.45 vs yday $11,043.40 (+177.05) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `CF` | 41 | $137.88 | $2.17 | $-33.80 | $5,775.08 | ▼ -33.80 after sell → book $11,218.28; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CTVA` | 63 | $86.40 | $2.23 | $-246.33 | $11,216.05 | ▼ -246.33 after sell → book $11,216.05; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,216.05 | ▲ close $11,216.05 vs 09:30 $11,220.45 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,216.05 | ▲ 09:30 equity $11,216.05 vs yday $11,216.05 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,216.05 | ▲ close $11,216.05 vs 09:30 $11,216.05 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,216.05 | ▲ 09:30 equity $11,216.05 vs yday $11,216.05 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 970 | $11.55 | $12.51 | — | $0.03 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $11216.05 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.03 | ▼ close $11,184.13 vs 09:30 $11,216.05 (session -19.40) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.03 | ▲ 09:30 equity $11,213.23 vs yday $11,184.13 (+29.10) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.03 | ▲ close $11,513.93 vs 09:30 $11,213.23 (session +300.70) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.03 | ▼ 09:30 equity $11,349.03 vs yday $11,513.93 (-164.90) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.03 | ▼ close $10,582.73 vs 09:30 $11,349.03 (session -766.30) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.03 | ▼ 09:30 equity $10,427.53 vs yday $10,582.73 (-155.20) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `FUBO` | 970 | $10.75 | $12.76 | $-801.27 | $10,414.78 | ▼ -801.27 after sell → book $10,414.78; vs 09:30 mark -12.75 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 887 | $5.87 | $11.44 | — | $5,196.64 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $5207.39 | — |
| 2026-09-16 09:30 ET | **BUY** | `KGS` | 89 | $58.00 | $2.26 | — | $32.39 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list ohlc_hot; ret5=+9.1; leftover $5207.39 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.39 | ▼ close $10,144.86 vs 09:30 $10,427.53 (session -256.22) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.39 | ▲ 09:30 equity $10,224.84 vs yday $10,144.86 (+79.98) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.39 | ▲ close $10,239.73 vs 09:30 $10,224.84 (session +14.89) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.39 | ▲ 09:30 equity $10,292.98 vs yday $10,239.73 (+53.25) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.39 | ▼ close $10,262.04 vs 09:30 $10,292.98 (session -30.94) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.39 | ▲ 09:30 equity $10,270.11 vs yday $10,262.04 (+8.07) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 887 | $5.62 | $11.63 | $-244.82 | $5,005.70 | ▼ -244.82 after sell → book $10,258.48; vs 09:30 mark -11.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,005.70 | ▼ close $10,011.06 vs 09:30 $10,270.11 (session -247.42) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,005.70 | ▲ 09:30 equity $10,011.06 vs yday $10,011.06 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,005.70 | ▲ close $10,011.06 vs 09:30 $10,011.06 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,005.70 | ▼ 09:30 equity $9,845.52 vs yday $10,011.06 (-165.54) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `KGS` | 89 | $54.38 | $2.31 | $-326.75 | $9,843.21 | ▼ -326.75 after sell → book $9,843.21; vs 09:30 mark -2.31 | dropped from list after 5 sess (min 3) | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,843.21 | ▲ close $9,843.21 vs 09:30 $9,845.52 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,843.21 | ▲ 09:30 equity $9,843.21 vs yday $9,843.21 (-0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,843.21 | ▲ close $9,843.21 vs 09:30 $9,843.21 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,850.74 | ▲ 09:30 equity $8,850.74 vs yday $8,850.74 (+0.00) | 09:30 open · cash $8,850.74 · no holdings · equity $8,850.74 vs prior close $8,850.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,850.74 | ▲ close $8,850.74 vs 09:30 $8,850.74 (session +0.00) | 16:00 close · cash $8,850.74 · no lots left · equity $8,850.74. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-27 | `CRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `NVDA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `CRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `NVDA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `FMC` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CF` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `CF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CTVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CTVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `FUBO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `NMAX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `FUBO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `KGS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `KGS` | no_price | no 09:30 open — carry |
