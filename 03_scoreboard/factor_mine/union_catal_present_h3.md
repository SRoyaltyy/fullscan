# Factor mine action — `union_catal_present_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ catal_present, no 🚨

Cash book **-0.74%** ($9,926) · signal-only (no cash/fees) was -12.69%. Starts YES **0/27**. Fills 12 · skips 16 · realized $-73.76.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,926.24.

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
| 2026-08-20 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-21 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-24 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-25 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-26 | `CRM` | 25 | — | $199.94 | +0.00 | $205.62 | +142.00 | +142.00 | +0.00 | +142.00 |
| 2026-08-26 | `NVDA` | 23 | — | $212.64 | +0.00 | $209.66 | -68.54 | -68.54 | +0.00 | -68.54 |
| 2026-08-27 | `CRM` | 25 | $205.62 | $230.05 | +610.75 | $252.05 | +550.00 | +1160.75 | +752.75 | +1302.75 |
| 2026-08-27 | `NVDA` | 23 | $209.66 | $222.86 | +303.60 | $227.98 | +117.76 | +421.36 | +235.06 | +352.82 |
| 2026-08-28 | `CRM` | 25 | $252.05 | $250.47 | -39.50 | $256.00 | +138.25 | +98.75 | +1263.25 | +1401.50 |
| 2026-08-28 | `NVDA` | 23 | $227.98 | $227.36 | -14.26 | $217.55 | -225.63 | -239.89 | +338.56 | +112.93 |
| 2026-08-31 | `CRM` | 25 | $256.00 | $254.39 | -40.25 | — | +0.00 | -40.25 | +1361.25 | — |
| 2026-08-31 | `NVDA` | 23 | $217.55 | $218.87 | +30.36 | — | +0.00 | +30.36 | +143.29 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `CF` | 41 | — | $138.60 | +0.00 | $137.81 | -32.39 | -32.39 | +0.00 | -32.39 |
| 2026-09-03 | `CTVA` | 63 | — | $90.24 | +0.00 | $88.62 | -102.06 | -102.06 | +0.00 | -102.06 |
| 2026-09-04 | `CF` | 41 | $137.81 | $135.43 | -97.58 | $133.35 | -85.28 | -182.86 | -129.97 | -215.25 |
| 2026-09-04 | `CTVA` | 63 | $88.62 | $87.64 | -61.74 | $87.86 | +13.86 | -47.88 | -163.80 | -149.94 |
| 2026-09-08 | `CF` | 41 | $133.35 | $134.06 | +29.11 | $134.33 | +11.07 | +40.18 | -186.14 | -175.07 |
| 2026-09-08 | `CTVA` | 63 | $87.86 | $88.25 | +24.57 | $85.90 | -148.05 | -123.48 | -125.37 | -273.42 |
| 2026-09-09 | `CF` | 41 | $134.33 | $137.88 | +145.55 | — | +0.00 | +145.55 | -29.52 | — |
| 2026-09-09 | `CTVA` | 63 | $85.90 | $86.40 | +31.50 | — | +0.00 | +31.50 | -241.92 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `FUBO` | 970 | — | $11.55 | +0.00 | $11.53 | -19.40 | -19.40 | +0.00 | -19.40 |
| 2026-09-14 | `FUBO` | 970 | $11.53 | $11.56 | +29.10 | $11.87 | +300.70 | +329.80 | +9.70 | +310.40 |
| 2026-09-15 | `FUBO` | 970 | $11.87 | $11.70 | -164.90 | $10.91 | -766.30 | -931.20 | +145.50 | -620.80 |
| 2026-09-16 | `FUBO` | 970 | $10.91 | $10.75 | -155.20 | — | +0.00 | -155.20 | -776.00 | — |
| 2026-09-16 | `RIG` | 1770 | — | $5.87 | +0.00 | $5.54 | -584.10 | -584.10 | +0.00 | -584.10 |
| 2026-09-17 | `RIG` | 1770 | $5.54 | $5.58 | +70.80 | $5.66 | +141.60 | +212.40 | -513.30 | -371.70 |
| 2026-09-18 | `RIG` | 1770 | $5.66 | $5.71 | +88.50 | $5.64 | -123.90 | -35.40 | -283.20 | -407.10 |
| 2026-09-21 | `RIG` | 1770 | $5.64 | $5.62 | -35.40 | — | +0.00 | -35.40 | -442.50 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-21 | +3.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-24 | -5.17 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-25 | +1.80 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-26 | +2.02 | $10,000.00 | — | $10,000.00 | +0.00 | +73.46 | CRM, NVDA | — | $106.66 | $10,069.34 | CRM×25, NVDA×23 |
| 2026-08-27 | — | $106.66 | CRM×25, NVDA×23 | $10,983.69 | +914.35 | +667.76 | — | — | $106.66 | $11,651.45 | CRM×25, NVDA×23 |
| 2026-08-28 | +0.75 | $106.66 | CRM×25, NVDA×23 | $11,597.69 | -53.76 | -87.38 | — | — | $106.66 | $11,510.31 | CRM×25, NVDA×23 |
| 2026-08-31 | -5.85 | $106.66 | CRM×25, NVDA×23 | $11,500.42 | -9.89 | +0.00 | — | CRM, NVDA | $11,496.18 | $11,496.18 | — |
| 2026-09-01 | -6.30 | $11,496.18 | — | $11,496.18 | +0.00 | +0.00 | — | — | $11,496.18 | $11,496.18 | — |
| 2026-09-02 | -3.83 | $11,496.18 | — | $11,496.18 | +0.00 | +0.00 | — | — | $11,496.18 | $11,496.18 | — |
| 2026-09-03 | -0.90 | $11,496.18 | — | $11,496.18 | +0.00 | -134.45 | CF, CTVA | — | $124.17 | $11,357.44 | CF×41, CTVA×63 |
| 2026-09-04 | +2.25 | $124.17 | CF×41, CTVA×63 | $11,198.12 | -159.32 | -71.42 | — | — | $124.17 | $11,126.70 | CF×41, CTVA×63 |
| 2026-09-08 | -11.47 | $124.17 | CF×41, CTVA×63 | $11,180.38 | +53.68 | -136.98 | — | — | $124.17 | $11,043.40 | CF×41, CTVA×63 |
| 2026-09-09 | -13.95 | $124.17 | CF×41, CTVA×63 | $11,220.45 | +177.05 | +0.00 | — | CF, CTVA | $11,216.05 | $11,216.05 | — |
| 2026-09-10 | -13.28 | $11,216.05 | — | $11,216.05 | -0.00 | +0.00 | — | — | $11,216.05 | $11,216.05 | — |
| 2026-09-11 | +0.50 | $11,216.05 | — | $11,216.05 | -0.00 | -19.40 | FUBO | — | $0.03 | $11,184.13 | FUBO×970 |
| 2026-09-14 | -11.00 | $0.03 | FUBO×970 | $11,213.23 | +29.10 | +300.70 | — | — | $0.03 | $11,513.93 | FUBO×970 |
| 2026-09-15 | -3.84 | $0.03 | FUBO×970 | $11,349.03 | -164.90 | -766.30 | — | — | $0.03 | $10,582.73 | FUBO×970 |
| 2026-09-16 | +5.30 | $0.03 | FUBO×970 | $10,427.53 | -155.20 | -584.10 | RIG | FUBO | $2.04 | $9,807.84 | RIG×1770 |
| 2026-09-17 | +7.38 | $2.04 | RIG×1770 | $9,878.64 | +70.80 | +141.60 | — | — | $2.04 | $10,020.24 | RIG×1770 |
| 2026-09-18 | +4.86 | $2.04 | RIG×1770 | $10,108.74 | +88.50 | -123.90 | — | — | $2.04 | $9,984.84 | RIG×1770 |
| 2026-09-21 | +12.87 | $2.04 | RIG×1770 | $9,949.44 | -35.40 | +0.00 | — | RIG | $9,926.24 | $9,926.24 | — |

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
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 09:30 ET | **BUY** | `CRM` | 25 | $199.94 | $2.06 | — | $4,999.44 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list overnight,overnight_mega; ret5=+2.1; leftover $5000.00 | join🟡 sector🔴 gen🟢 news🔴 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 catal🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NVDA` | 23 | $212.64 | $2.06 | — | $106.66 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list overnight,overnight_mega; 🔵; ret5=-3.0; leftover $5000.00 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 catal🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.66 | ▲ close $10,069.34 vs 09:30 $10,000.00 (session +73.46) | 16:00 close · cash $106.66 · equity $10,069.34 vs 09:30 $10,000.00 (+69.34; session marks +73.46) · 2 name(s) marked open→close (per-name table). CRM×25 09:30 $199.94 → close $205.62 +142.00; NVDA×23 09:30 $212.64 → close $209.66 -68.54 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.66 | ▲ 09:30 equity $10,983.69 vs yday $10,069.34 (+914.35) | 09:30 open · cash $106.66 (unchanged overnight, no fees) · equity $10,983.69 vs prior close $10,069.34 (+914.35) · 2 name(s) re-marked at the open (per-name table). CRM×25 yday $205.62 → 09:30 $230.05 +610.75; NVDA×23 yday $209.66 → 09:30 $222.86 +303.60 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.66 | ▲ close $11,651.45 vs 09:30 $10,983.69 (session +667.76) | 16:00 close · cash $106.66 · equity $11,651.45 vs 09:30 $10,983.69 (+667.76; session marks +667.76) · 2 name(s) marked open→close (per-name table). CRM×25 09:30 $230.05 → close $252.05 +550.00; NVDA×23 09:30 $222.86 → close $227.98 +117.76 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.66 | ▼ 09:30 equity $11,597.69 vs yday $11,651.45 (-53.76) | 09:30 open · cash $106.66 (unchanged overnight, no fees) · equity $11,597.69 vs prior close $11,651.45 (-53.76) · 2 name(s) re-marked at the open (per-name table). CRM×25 yday $252.05 → 09:30 $250.47 -39.50; NVDA×23 yday $227.98 → 09:30 $227.36 -14.26 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.66 | ▼ close $11,510.31 vs 09:30 $11,597.69 (session -87.38) | 16:00 close · cash $106.66 · equity $11,510.31 vs 09:30 $11,597.69 (-87.38; session marks -87.38) · 2 name(s) marked open→close (per-name table). CRM×25 09:30 $250.47 → close $256.00 +138.25; NVDA×23 09:30 $227.36 → close $217.55 -225.63 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.66 | ▼ 09:30 equity $11,500.42 vs yday $11,510.31 (-9.89) | 09:30 open · cash $106.66 (unchanged overnight, no fees) · equity $11,500.42 vs prior close $11,510.31 (-9.89) · 2 name(s) re-marked at the open (per-name table). CRM×25 yday $256.00 → 09:30 $254.39 -40.25; NVDA×23 yday $217.55 → 09:30 $218.87 +30.36 | — |
| 2026-08-31 09:30 ET | **SELL** | `CRM` | 25 | $254.39 | $2.13 | $+1357.06 | $6,464.28 | ▲ +1,357.06 after sell → book $11,498.29; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `NVDA` | 23 | $218.87 | $2.11 | $+139.12 | $11,496.18 | ▲ +139.12 after sell → book $11,496.18; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,496.18 | ▲ close $11,496.18 vs 09:30 $11,500.42 (session +0.00) | 16:00 close · cash $11,496.18 · no lots left · equity $11,496.18. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,496.18 | ▲ 09:30 equity $11,496.18 vs yday $11,496.18 (+0.00) | 09:30 open · cash $11,496.18 · no holdings · equity $11,496.18 vs prior close $11,496.18 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,496.18 | ▲ close $11,496.18 vs 09:30 $11,496.18 (session +0.00) | 16:00 close · cash $11,496.18 · no lots left · equity $11,496.18. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,496.18 | ▲ 09:30 equity $11,496.18 vs yday $11,496.18 (+0.00) | 09:30 open · cash $11,496.18 · no holdings · equity $11,496.18 vs prior close $11,496.18 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,496.18 | ▲ close $11,496.18 vs 09:30 $11,496.18 (session +0.00) | 16:00 close · cash $11,496.18 · no lots left · equity $11,496.18. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,496.18 | ▲ 09:30 equity $11,496.18 vs yday $11,496.18 (+0.00) | 09:30 open · cash $11,496.18 · no holdings · equity $11,496.18 vs prior close $11,496.18 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `CF` | 41 | $138.60 | $2.11 | — | $5,811.47 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list ohlc_hot; 🔵; ret5=+10.8; leftover $5748.09 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 catal🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTVA` | 63 | $90.24 | $2.18 | — | $124.17 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list ohlc_hot; 🔵; ret5=+8.6; leftover $5748.09 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.17 | ▼ close $11,357.44 vs 09:30 $11,496.18 (session -134.45) | 16:00 close · cash $124.17 · equity $11,357.44 vs 09:30 $11,496.18 (-138.74; session marks -134.45) · 2 name(s) marked open→close (per-name table). CF×41 09:30 $138.60 → close $137.81 -32.39; CTVA×63 09:30 $90.24 → close $88.62 -102.06 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.17 | ▼ 09:30 equity $11,198.12 vs yday $11,357.44 (-159.32) | 09:30 open · cash $124.17 (unchanged overnight, no fees) · equity $11,198.12 vs prior close $11,357.44 (-159.32) · 2 name(s) re-marked at the open (per-name table). CF×41 yday $137.81 → 09:30 $135.43 -97.58; CTVA×63 yday $88.62 → 09:30 $87.64 -61.74 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.17 | ▼ close $11,126.70 vs 09:30 $11,198.12 (session -71.42) | 16:00 close · cash $124.17 · equity $11,126.70 vs 09:30 $11,198.12 (-71.42; session marks -71.42) · 2 name(s) marked open→close (per-name table). CF×41 09:30 $135.43 → close $133.35 -85.28; CTVA×63 09:30 $87.64 → close $87.86 +13.86 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.17 | ▲ 09:30 equity $11,180.38 vs yday $11,126.70 (+53.68) | 09:30 open · cash $124.17 (unchanged overnight, no fees) · equity $11,180.38 vs prior close $11,126.70 (+53.68) · 2 name(s) re-marked at the open (per-name table). CF×41 yday $133.35 → 09:30 $134.06 +29.11; CTVA×63 yday $87.86 → 09:30 $88.25 +24.57 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.17 | ▼ close $11,043.40 vs 09:30 $11,180.38 (session -136.98) | 16:00 close · cash $124.17 · equity $11,043.40 vs 09:30 $11,180.38 (-136.98; session marks -136.98) · 2 name(s) marked open→close (per-name table). CF×41 09:30 $134.06 → close $134.33 +11.07; CTVA×63 09:30 $88.25 → close $85.90 -148.05 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.17 | ▲ 09:30 equity $11,220.45 vs yday $11,043.40 (+177.05) | 09:30 open · cash $124.17 (unchanged overnight, no fees) · equity $11,220.45 vs prior close $11,043.40 (+177.05) · 2 name(s) re-marked at the open (per-name table). CF×41 yday $134.33 → 09:30 $137.88 +145.55; CTVA×63 yday $85.90 → 09:30 $86.40 +31.50 | — |
| 2026-09-09 09:30 ET | **SELL** | `CF` | 41 | $137.88 | $2.17 | $-33.80 | $5,775.08 | ▼ -33.80 after sell → book $11,218.28; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CTVA` | 63 | $86.40 | $2.23 | $-246.33 | $11,216.05 | ▼ -246.33 after sell → book $11,216.05; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,216.05 | ▲ close $11,216.05 vs 09:30 $11,220.45 (session +0.00) | 16:00 close · cash $11,216.05 · no lots left · equity $11,216.05. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,216.05 | ▲ 09:30 equity $11,216.05 vs yday $11,216.05 (-0.00) | 09:30 open · cash $11,216.05 · no holdings · equity $11,216.05 vs prior close $11,216.05 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,216.05 | ▲ close $11,216.05 vs 09:30 $11,216.05 (session +0.00) | 16:00 close · cash $11,216.05 · no lots left · equity $11,216.05. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,216.05 | ▲ 09:30 equity $11,216.05 vs yday $11,216.05 (-0.00) | 09:30 open · cash $11,216.05 · no holdings · equity $11,216.05 vs prior close $11,216.05 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 970 | $11.55 | $12.51 | — | $0.03 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $11216.05 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.03 | ▼ close $11,184.13 vs 09:30 $11,216.05 (session -19.40) | 16:00 close · cash $0.03 · equity $11,184.13 vs 09:30 $11,216.05 (-31.92; session marks -19.40) · 1 name(s) marked open→close (per-name table). FUBO×970 09:30 $11.55 → close $11.53 -19.40 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.03 | ▲ 09:30 equity $11,213.23 vs yday $11,184.13 (+29.10) | 09:30 open · cash $0.03 (unchanged overnight, no fees) · equity $11,213.23 vs prior close $11,184.13 (+29.10) · 1 name(s) re-marked at the open (per-name table). FUBO×970 yday $11.53 → 09:30 $11.56 +29.10 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.03 | ▲ close $11,513.93 vs 09:30 $11,213.23 (session +300.70) | 16:00 close · cash $0.03 · equity $11,513.93 vs 09:30 $11,213.23 (+300.70; session marks +300.70) · 1 name(s) marked open→close (per-name table). FUBO×970 09:30 $11.56 → close $11.87 +300.70 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.03 | ▼ 09:30 equity $11,349.03 vs yday $11,513.93 (-164.90) | 09:30 open · cash $0.03 (unchanged overnight, no fees) · equity $11,349.03 vs prior close $11,513.93 (-164.90) · 1 name(s) re-marked at the open (per-name table). FUBO×970 yday $11.87 → 09:30 $11.70 -164.90 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.03 | ▼ close $10,582.73 vs 09:30 $11,349.03 (session -766.30) | 16:00 close · cash $0.03 · equity $10,582.73 vs 09:30 $11,349.03 (-766.30; session marks -766.30) · 1 name(s) marked open→close (per-name table). FUBO×970 09:30 $11.70 → close $10.91 -766.30 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.03 | ▼ 09:30 equity $10,427.53 vs yday $10,582.73 (-155.20) | 09:30 open · cash $0.03 (unchanged overnight, no fees) · equity $10,427.53 vs prior close $10,582.73 (-155.20) · 1 name(s) re-marked at the open (per-name table). FUBO×970 yday $10.91 → 09:30 $10.75 -155.20 | — |
| 2026-09-16 09:30 ET | **SELL** | `FUBO` | 970 | $10.75 | $12.76 | $-801.27 | $10,414.78 | ▼ -801.27 after sell → book $10,414.78; vs 09:30 mark -12.75 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 1770 | $5.87 | $22.83 | — | $2.04 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $10414.78 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 catal🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.04 | ▼ close $9,807.84 vs 09:30 $10,427.53 (session -584.10) | 16:00 close · cash $2.04 · equity $9,807.84 vs 09:30 $10,427.53 (-619.69; session marks -584.10) · 1 name(s) marked open→close (per-name table). RIG×1770 09:30 $5.87 → close $5.54 -584.10 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.04 | ▲ 09:30 equity $9,878.64 vs yday $9,807.84 (+70.80) | 09:30 open · cash $2.04 (unchanged overnight, no fees) · equity $9,878.64 vs prior close $9,807.84 (+70.80) · 1 name(s) re-marked at the open (per-name table). RIG×1770 yday $5.54 → 09:30 $5.58 +70.80 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.04 | ▲ close $10,020.24 vs 09:30 $9,878.64 (session +141.60) | 16:00 close · cash $2.04 · equity $10,020.24 vs 09:30 $9,878.64 (+141.60; session marks +141.60) · 1 name(s) marked open→close (per-name table). RIG×1770 09:30 $5.58 → close $5.66 +141.60 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.04 | ▲ 09:30 equity $10,108.74 vs yday $10,020.24 (+88.50) | 09:30 open · cash $2.04 (unchanged overnight, no fees) · equity $10,108.74 vs prior close $10,020.24 (+88.50) · 1 name(s) re-marked at the open (per-name table). RIG×1770 yday $5.66 → 09:30 $5.71 +88.50 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.04 | ▼ close $9,984.84 vs 09:30 $10,108.74 (session -123.90) | 16:00 close · cash $2.04 · equity $9,984.84 vs 09:30 $10,108.74 (-123.90; session marks -123.90) · 1 name(s) marked open→close (per-name table). RIG×1770 09:30 $5.71 → close $5.64 -123.90 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.04 | ▼ 09:30 equity $9,949.44 vs yday $9,984.84 (-35.40) | 09:30 open · cash $2.04 (unchanged overnight, no fees) · equity $9,949.44 vs prior close $9,984.84 (-35.40) · 1 name(s) re-marked at the open (per-name table). RIG×1770 yday $5.64 → 09:30 $5.62 -35.40 | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 1770 | $5.62 | $23.21 | $-488.54 | $9,926.24 | ▼ -488.54 after sell → book $9,926.24; vs 09:30 mark -23.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,926.24 | ▲ close $9,926.24 vs 09:30 $9,949.44 (session +0.00) | 16:00 close · cash $9,926.24 · no lots left · equity $9,926.24. | — |

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
