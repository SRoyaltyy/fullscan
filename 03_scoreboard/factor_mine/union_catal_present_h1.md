# Factor mine action — `union_catal_present_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ catal_present, no 🚨

Cash book **-5.50%** ($9,450) · signal-only (no cash/fees) was -5.09%. Starts YES **0/30**. Fills 16 · skips 5 · realized $-402.71.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `catal_present=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,597.29.

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
| 2026-08-27 09:30 ET | **SELL** | `CRM` | 25 | $230.05 | $2.12 | $+748.56 | $5,855.79 | ▲ +748.56 after sell → book $10,981.57; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NVDA` | 23 | $222.86 | $2.11 | $+230.89 | $10,979.46 | ▲ +230.89 after sell → book $10,979.46; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,979.46 | ▲ close $10,979.46 vs 09:30 $10,983.69 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,979.46 | ▲ 09:30 equity $10,979.46 vs yday $10,979.46 (-0.00) | — | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,979.46 | ▲ close $10,979.46 vs 09:30 $10,979.46 (session +0.00) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,979.46 | ▲ 09:30 equity $10,979.46 vs yday $10,979.46 (-0.00) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,979.46 | ▲ close $10,979.46 vs 09:30 $10,979.46 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,979.46 | ▲ 09:30 equity $10,979.46 vs yday $10,979.46 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,979.46 | ▲ close $10,979.46 vs 09:30 $10,979.46 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,979.46 | ▲ 09:30 equity $10,979.46 vs yday $10,979.46 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,979.46 | ▲ close $10,979.46 vs 09:30 $10,979.46 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,979.46 | ▲ 09:30 equity $10,979.46 vs yday $10,979.46 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `CF` | 39 | $138.60 | $2.11 | — | $5,571.95 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list ohlc_hot; 🔵; ret5=+10.8; leftover $5489.73 | — |
| 2026-09-03 09:30 ET | **BUY** | `CTVA` | 60 | $90.24 | $2.17 | — | $155.38 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list ohlc_hot; 🔵; ret5=+8.6; leftover $5489.73 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $155.38 | ▼ close $10,847.17 vs 09:30 $10,979.46 (session -128.01) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $155.38 | ▼ 09:30 equity $10,695.55 vs yday $10,847.17 (-151.62) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `CF` | 39 | $135.43 | $2.16 | $-127.90 | $5,434.99 | ▼ -127.90 after sell → book $10,693.39; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTVA` | 60 | $87.64 | $2.22 | $-160.39 | $10,691.17 | ▼ -160.39 after sell → book $10,691.17; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,691.17 | ▲ close $10,691.17 vs 09:30 $10,695.55 (session +0.00) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,691.17 | ▲ 09:30 equity $10,691.17 vs yday $10,691.17 (-0.00) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,691.17 | ▲ close $10,691.17 vs 09:30 $10,691.17 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,691.17 | ▲ 09:30 equity $10,691.17 vs yday $10,691.17 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,691.17 | ▲ close $10,691.17 vs 09:30 $10,691.17 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,691.17 | ▲ 09:30 equity $10,691.17 vs yday $10,691.17 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,691.17 | ▲ close $10,691.17 vs 09:30 $10,691.17 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,691.17 | ▲ 09:30 equity $10,691.17 vs yday $10,691.17 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 924 | $11.55 | $11.92 | — | $7.05 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $10691.17 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.05 | ▼ close $10,660.77 vs 09:30 $10,691.17 (session -18.48) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.05 | ▲ 09:30 equity $10,688.49 vs yday $10,660.77 (+27.72) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `FUBO` | 924 | $11.56 | $12.16 | $-14.84 | $10,676.33 | ▼ -14.84 after sell → book $10,676.33; vs 09:30 mark -12.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,676.33 | ▲ close $10,676.33 vs 09:30 $10,688.49 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,676.33 | ▲ 09:30 equity $10,676.33 vs yday $10,676.33 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,676.33 | ▲ close $10,676.33 vs 09:30 $10,676.33 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,676.33 | ▲ 09:30 equity $10,676.33 vs yday $10,676.33 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 909 | $5.87 | $11.73 | — | $5,328.77 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $5338.16 | — |
| 2026-09-16 09:30 ET | **BUY** | `KGS` | 91 | $58.00 | $2.26 | — | $48.51 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list ohlc_hot; ret5=+9.1; leftover $5338.16 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.51 | ▼ close $10,399.68 vs 09:30 $10,676.33 (session -262.66) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.51 | ▲ 09:30 equity $10,481.54 vs yday $10,399.68 (+81.86) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 909 | $5.58 | $11.92 | $-287.25 | $5,108.81 | ▼ -287.25 after sell → book $10,469.62; vs 09:30 mark -11.92 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,108.81 | ▼ close $10,412.29 vs 09:30 $10,481.54 (session -57.33) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,108.81 | ▲ 09:30 equity $10,421.39 vs yday $10,412.29 (+9.10) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `KGS` | 91 | $58.38 | $2.32 | $+30.00 | $10,419.07 | ▲ +30.00 after sell → book $10,419.07; vs 09:30 mark -2.32 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,419.07 | ▲ close $10,419.07 vs 09:30 $10,421.39 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,419.07 | ▲ 09:30 equity $10,419.07 vs yday $10,419.07 (+0.00) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `KGS` | 176 | $59.02 | $2.52 | — | $29.03 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list ohlc_hot; ret5=+9.1; leftover $10419.07 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.03 | ▼ close $9,927.27 vs 09:30 $10,419.07 (session -489.28) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.03 | ▲ 09:30 equity $9,927.27 vs yday $9,927.27 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.03 | ▲ close $9,927.27 vs 09:30 $9,927.27 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.03 | ▼ 09:30 equity $9,599.91 vs yday $9,927.27 (-327.36) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `KGS` | 176 | $54.38 | $2.62 | $-821.78 | $9,597.29 | ▼ -821.78 after sell → book $9,597.29; vs 09:30 mark -2.62 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,597.29 | ▲ close $9,597.29 vs 09:30 $9,599.91 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,597.29 | ▲ 09:30 equity $9,597.29 vs yday $9,597.29 (-0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,597.29 | ▲ close $9,597.29 vs 09:30 $9,597.29 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,449.85 | ▲ 09:30 equity $9,449.85 vs yday $9,449.85 (+0.00) | 09:30 open · cash $9,449.85 · no holdings · equity $9,449.85 vs prior close $9,449.85 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,449.85 | ▲ close $9,449.85 vs 09:30 $9,449.85 (session +0.00) | 16:00 close · cash $9,449.85 · no lots left · equity $9,449.85. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-09-02 | `FMC` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CF` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `NMAX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-22 | `KGS` | no_price | no 09:30 open — carry |
