# Factor mine action — `union_flow_in_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ flow_in, no 🚨

Cash book **-11.72%** ($8,828) · signal-only (no cash/fees) was +15.53%. Starts YES **3/30**. Fills 78 · skips 105 · realized $-281.48.

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
- **Gate** `flow_in=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,431.09.

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
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 197 | $50.62 | $2.58 | — | $24.65 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list flatten; ⚪; ret5=+6.2; leftover $10000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.65 | ▲ close $10,784.79 vs 09:30 $10,000.00 (session +787.37) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.65 | ▲ 09:30 equity $10,916.78 vs yday $10,784.79 (+131.99) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `KULR` | 2 | $2.50 | $0.06 | — | $19.59 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ⚪; ret5=+7.6; leftover $6.16 | — |
| 2026-08-14 09:30 ET | **BUY** | `NPWR` | 3 | $1.56 | $0.06 | — | $14.86 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+8.0; leftover $6.16 | — |
| 2026-08-14 09:30 ET | **BUY** | `RLX` | 3 | $1.85 | $0.06 | — | $9.24 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ⚪; ret5=+0.5; leftover $6.16 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.24 | ▼ close $10,473.10 vs 09:30 $10,916.78 (session -443.50) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.24 | ▼ 09:30 equity $10,402.01 vs yday $10,473.10 (-71.09) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.24 | ▼ close $10,223.97 vs 09:30 $10,402.01 (session -178.04) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.24 | ▼ 09:30 equity $10,223.67 vs yday $10,223.97 (-0.30) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 197 | $51.77 | $2.70 | $+220.64 | $10,205.24 | ▲ +220.64 after sell → book $10,220.98; vs 09:30 mark -2.69 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,205.24 | ▼ close $10,220.82 vs 09:30 $10,223.67 (session -0.16) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,205.24 | ▲ 09:30 equity $10,220.96 vs yday $10,220.82 (+0.14) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `KULR` | 2 | $2.55 | $0.08 | $-0.03 | $10,210.26 | ▼ -0.03 after sell → book $10,220.88; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NPWR` | 3 | $1.70 | $0.08 | $+0.28 | $10,215.28 | ▲ +0.28 after sell → book $10,220.80; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `RLX` | 3 | $1.84 | $0.08 | $-0.18 | $10,220.72 | ▼ -0.18 after sell → book $10,220.72; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,220.72 | ▲ close $10,220.72 vs 09:30 $10,220.96 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,220.72 | ▲ 09:30 equity $10,220.72 vs yday $10,220.72 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `FUTU` | 28 | $117.65 | $2.07 | — | $6,924.44 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+4.1; leftover $3406.91 | — |
| 2026-08-20 09:30 ET | **BUY** | `WMT` | 32 | $106.38 | $2.09 | — | $3,518.20 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-1.7; leftover $3406.91 | — |
| 2026-08-20 09:30 ET | **BUY** | `BJ` | 38 | $88.91 | $2.10 | — | $137.51 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list overnight; 🔵; ret5=-1.0; leftover $3406.91 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $137.51 | ▼ close $10,086.23 vs 09:30 $10,220.72 (session -128.22) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $137.51 | ▲ 09:30 equity $10,251.87 vs yday $10,086.23 (+165.64) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 55 | $2.43 | $1.50 | — | $2.36 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $137.51 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.36 | ▲ close $10,581.39 vs 09:30 $10,251.87 (session +331.02) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.36 | ▼ 09:30 equity $10,544.35 vs yday $10,581.39 (-37.04) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.36 | ▼ close $10,530.64 vs 09:30 $10,544.35 (session -13.71) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.36 | ▲ 09:30 equity $10,531.26 vs yday $10,530.64 (+0.62) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `FUTU` | 28 | $118.00 | $2.11 | $+5.62 | $3,304.25 | ▲ +5.62 after sell → book $10,529.15; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WMT` | 32 | $105.58 | $2.12 | $-29.81 | $6,680.69 | ▼ -29.81 after sell → book $10,527.03; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BJ` | 38 | $97.63 | $2.14 | $+327.11 | $10,388.48 | ▲ +327.11 after sell → book $10,524.88; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 119 | $28.86 | $2.35 | — | $6,951.80 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+13.7; leftover $3462.83 | — |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 79 | $43.76 | $2.23 | — | $3,492.53 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $3462.83 | — |
| 2026-08-25 09:30 ET | **BUY** | `ABUS` | 659 | $5.25 | $8.50 | — | $24.28 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+10.4; leftover $3462.83 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.28 | ▼ close $10,408.46 vs 09:30 $10,531.26 (session -103.35) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.28 | ▼ 09:30 equity $10,367.55 vs yday $10,408.46 (-40.91) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `HITI` | 55 | $2.57 | $1.60 | $+4.60 | $164.03 | ▲ +4.60 after sell → book $10,365.95; vs 09:30 mark -1.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `NCNO` | 2 | $19.33 | $0.39 | — | $124.98 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; ret5=+3.0; leftover $41.01 | — |
| 2026-08-26 09:30 ET | **BUY** | `PLAB` | 1 | $37.26 | $0.38 | — | $87.34 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-8.0; leftover $41.01 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $87.34 | ▲ close $10,586.78 vs 09:30 $10,367.55 (session +221.60) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.34 | ▼ 09:30 equity $10,560.62 vs yday $10,586.78 (-26.16) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `TRLV` | 3 | $11.38 | $0.35 | — | $52.85 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+13.3; leftover $43.67 | — |
| 2026-08-27 09:30 ET | **BUY** | `BOX` | 1 | $33.79 | $0.34 | — | $18.72 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+0.8; leftover $43.67 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.72 | ▲ close $10,575.30 vs 09:30 $10,560.62 (session +15.37) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.72 | ▼ 09:30 equity $10,513.51 vs yday $10,575.30 (-61.79) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `ZYME` | 119 | $28.91 | $2.39 | $+1.21 | $3,456.62 | ▲ +1.21 after sell → book $10,511.12; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RHI` | 79 | $44.51 | $2.27 | $+54.75 | $6,970.64 | ▲ +54.75 after sell → book $10,508.85; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ABUS` | 659 | $5.15 | $8.64 | $-83.04 | $10,355.85 | ▼ -83.04 after sell → book $10,500.21; vs 09:30 mark -8.64 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `JKS` | 258 | $13.37 | $3.33 | — | $6,903.06 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_mover; ret5=-14.9; leftover $3451.95 | — |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 11 | $306.34 | $2.02 | — | $3,531.30 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_mover; ret5=-23.0; leftover $3451.95 | — |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 6 | $542.00 | $2.01 | — | $277.29 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; ret5=+4.8; leftover $3451.95 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $277.29 | ▼ close $10,257.50 vs 09:30 $10,513.51 (session -235.35) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $277.29 | ▲ 09:30 equity $10,318.80 vs yday $10,257.50 (+61.30) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 2 | $22.66 | $0.48 | $+5.79 | $322.13 | ▲ +5.79 after sell → book $10,318.32; vs 09:30 mark -0.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `PLAB` | 1 | $28.04 | $0.30 | $-9.90 | $349.87 | ▼ -9.90 after sell → book $10,318.02; vs 09:30 mark -0.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $349.87 | ▼ close $10,091.73 vs 09:30 $10,318.80 (session -226.29) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $349.87 | ▼ 09:30 equity $9,992.98 vs yday $10,091.73 (-98.75) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `TRLV` | 3 | $11.54 | $0.38 | $-0.25 | $384.11 | ▼ -0.25 after sell → book $9,992.60; vs 09:30 mark -0.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BOX` | 1 | $35.69 | $0.38 | $+1.18 | $419.42 | ▲ +1.18 after sell → book $9,992.22; vs 09:30 mark -0.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $419.42 | ▲ close $10,065.78 vs 09:30 $9,992.98 (session +73.56) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $419.42 | ▲ 09:30 equity $10,073.49 vs yday $10,065.78 (+7.71) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `JKS` | 258 | $12.45 | $3.40 | $-244.08 | $3,628.13 | ▼ -244.08 after sell → book $10,070.10; vs 09:30 mark -3.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DY` | 11 | $287.99 | $2.06 | $-205.93 | $6,793.96 | ▼ -205.93 after sell → book $10,068.04; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ULTA` | 6 | $545.68 | $2.04 | $+18.03 | $10,065.99 | ▲ +18.03 after sell → book $10,065.99; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,065.99 | ▲ close $10,065.99 vs 09:30 $10,073.49 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,065.99 | ▲ 09:30 equity $10,065.99 vs yday $10,065.99 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 145 | $11.54 | $2.42 | — | $8,390.27 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $1677.67 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 4 | $351.74 | $2.00 | — | $6,981.31 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+3.3; leftover $1677.67 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 6 | $257.00 | $2.01 | — | $5,437.30 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-5.5; leftover $1677.67 | — |
| 2026-09-03 09:30 ET | **BUY** | `MOMO` | 305 | $5.50 | $3.93 | — | $3,755.86 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-4.8; leftover $1677.67 | — |
| 2026-09-03 09:30 ET | **BUY** | `PVH` | 22 | $74.96 | $2.06 | — | $2,104.69 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-7.9; leftover $1677.67 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 21 | $76.86 | $2.05 | — | $488.58 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-6.6; leftover $1677.67 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $488.58 | ▼ close $9,713.29 vs 09:30 $10,065.99 (session -338.23) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $488.58 | ▼ 09:30 equity $9,712.87 vs yday $9,713.29 (-0.42) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 1 | $52.03 | $0.52 | — | $436.02 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $69.80 | — |
| 2026-09-04 09:30 ET | **BUY** | `WNC` | 4 | $14.17 | $0.58 | — | $378.76 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_gainer; 🔵; ret5=+7.9; leftover $69.80 | — |
| 2026-09-04 09:30 ET | **BUY** | `ADCT` | 53 | $1.30 | $0.85 | — | $309.02 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ⚪; ret5=+17.9; leftover $69.80 | — |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 7 | $8.94 | $0.65 | — | $245.79 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+7.7; leftover $69.80 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 1 | $68.52 | $0.69 | — | $176.58 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+3.4; leftover $69.80 | — |
| 2026-09-04 09:30 ET | **BUY** | `XP` | 3 | $19.67 | $0.60 | — | $116.97 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+13.1; leftover $69.80 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.97 | ▲ close $9,945.84 vs 09:30 $9,712.87 (session +236.85) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.97 | ▼ 09:30 equity $9,870.80 vs yday $9,945.84 (-75.04) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.97 | ▲ close $9,921.46 vs 09:30 $9,870.80 (session +50.66) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.97 | ▼ 09:30 equity $9,845.04 vs yday $9,921.46 (-76.42) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `VIR` | 145 | $11.04 | $2.46 | $-77.39 | $1,715.31 | ▼ -77.39 after sell → book $9,842.58; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 4 | $366.23 | $2.02 | $+53.93 | $3,178.21 | ▲ +53.93 after sell → book $9,840.56; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 6 | $252.92 | $2.03 | $-28.52 | $4,693.70 | ▼ -28.52 after sell → book $9,838.53; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MOMO` | 305 | $5.26 | $4.00 | $-81.13 | $6,294.00 | ▼ -81.13 after sell → book $9,834.53; vs 09:30 mark -4.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `PVH` | 22 | $70.83 | $2.08 | $-94.99 | $7,850.18 | ▼ -94.99 after sell → book $9,832.45; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSXY` | 21 | $77.16 | $2.08 | $+2.17 | $9,468.46 | ▲ +2.17 after sell → book $9,830.37; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,468.46 | ▼ close $9,823.84 vs 09:30 $9,845.04 (session -6.53) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,468.46 | ▼ 09:30 equity $9,820.80 vs yday $9,823.84 (-3.04) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ATRC` | 1 | $52.31 | $0.55 | $-0.79 | $9,520.23 | ▼ -0.79 after sell → book $9,820.26; vs 09:30 mark -0.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `WNC` | 4 | $12.79 | $0.54 | $-6.64 | $9,570.84 | ▼ -6.64 after sell → book $9,819.71; vs 09:30 mark -0.55 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ADCT` | 53 | $1.21 | $0.82 | $-6.44 | $9,634.15 | ▼ -6.44 after sell → book $9,818.89; vs 09:30 mark -0.82 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `HAFN` | 7 | $9.08 | $0.68 | $-0.34 | $9,697.04 | ▼ -0.34 after sell → book $9,818.22; vs 09:30 mark -0.67 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOCU` | 1 | $64.60 | $0.67 | $-5.28 | $9,760.97 | ▼ -5.28 after sell → book $9,817.55; vs 09:30 mark -0.67 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `XP` | 3 | $18.86 | $0.59 | $-3.62 | $9,816.95 | ▼ -3.62 after sell → book $9,816.95; vs 09:30 mark -0.60 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,816.95 | ▲ close $9,816.95 vs 09:30 $9,820.80 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,816.95 | ▲ 09:30 equity $9,816.95 vs yday $9,816.95 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `SSL` | 683 | $14.35 | $8.81 | — | $7.09 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+15.5; leftover $9816.95 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.09 | ▲ close $9,972.06 vs 09:30 $9,816.95 (session +163.92) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.09 | ▲ 09:30 equity $10,040.36 vs yday $9,972.06 (+68.30) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.09 | ▼ close $9,910.59 vs 09:30 $10,040.36 (session -129.77) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.09 | ▼ 09:30 equity $9,903.76 vs yday $9,910.59 (-6.83) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.09 | ▲ close $10,327.22 vs 09:30 $9,903.76 (session +423.46) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.09 | ▼ 09:30 equity $9,992.55 vs yday $10,327.22 (-334.67) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `SSL` | 683 | $14.62 | $9.00 | $+166.60 | $9,983.55 | ▲ +166.60 after sell → book $9,983.55; vs 09:30 mark -9.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `ATRC` | 59 | $55.66 | $2.17 | — | $6,697.44 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+4.6; leftover $3327.85 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 81 | $40.93 | $2.23 | — | $3,379.88 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; ret5=-3.1; leftover $3327.85 | — |
| 2026-09-16 09:30 ET | **BUY** | `LEN` | 41 | $80.63 | $2.11 | — | $71.93 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list overnight; ret5=-0.4; leftover $3327.85 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.93 | ▼ close $9,930.78 vs 09:30 $9,992.55 (session -46.25) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.93 | ▲ 09:30 equity $10,116.56 vs yday $9,930.78 (+185.78) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.93 | ▼ close $10,096.87 vs 09:30 $10,116.56 (session -19.69) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.93 | ▼ 09:30 equity $10,021.68 vs yday $10,096.87 (-75.19) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.93 | ▼ close $9,928.54 vs 09:30 $10,021.68 (session -93.14) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.93 | ▲ 09:30 equity $9,984.68 vs yday $9,928.54 (+56.14) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `ATRC` | 59 | $58.23 | $2.20 | $+147.26 | $3,505.30 | ▲ +147.26 after sell → book $9,982.48; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 81 | $41.00 | $2.27 | $+1.16 | $6,824.03 | ▲ +1.16 after sell → book $9,980.21; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `LEN` | 41 | $76.98 | $2.15 | $-153.91 | $9,978.06 | ▼ -153.91 after sell → book $9,978.06; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 15 | $157.87 | $2.04 | — | $7,607.97 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list flatten; ret5=+6.5; leftover $2494.51 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 6 | $386.20 | $2.01 | — | $5,288.77 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list flatten; ret5=-5.8; leftover $2494.51 | — |
| 2026-09-21 09:30 ET | **BUY** | `UMC` | 100 | $24.93 | $2.29 | — | $2,793.48 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+8.7; leftover $2494.51 | — |
| 2026-09-21 09:30 ET | **BUY** | `NEO` | 125 | $19.92 | $2.37 | — | $301.11 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+15.0; leftover $2494.51 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $301.11 | ▲ close $9,970.94 vs 09:30 $9,984.68 (session +1.58) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $301.11 | ▼ 09:30 equity $9,953.94 vs yday $9,970.94 (-17.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $301.11 | ▲ close $10,004.94 vs 09:30 $9,953.94 (session +51.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $301.11 | ▼ 09:30 equity $9,883.46 vs yday $10,004.94 (-121.48) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 6 | $47.57 | $2.01 | — | $13.68 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-11.2; leftover $301.11 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.68 | ▼ close $9,790.28 vs 09:30 $9,883.46 (session -91.17) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.68 | ▼ 09:30 equity $9,721.20 vs yday $9,790.28 (-69.08) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 15 | $163.95 | $2.06 | $+87.10 | $2,470.87 | ▲ +87.10 after sell → book $9,719.14; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `HUM` | 6 | $374.54 | $2.04 | $-74.00 | $4,716.07 | ▼ -74.00 after sell → book $9,717.10; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `UMC` | 100 | $24.11 | $2.33 | $-86.62 | $7,124.75 | ▼ -86.62 after sell → book $9,714.78; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `NEO` | 125 | $18.47 | $2.40 | $-186.02 | $9,431.09 | ▼ -186.02 after sell → book $9,712.37; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,431.09 | ▲ close $9,742.13 vs 09:30 $9,721.20 (session +29.76) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,410.82 | ▲ 09:30 equity $8,829.94 vs yday $8,825.54 (+4.40) | 09:30 open · cash $8,410.82 (unchanged overnight, no fees) · equity $8,829.94 vs prior close $8,825.54 (+4.40) · 1 name(s) re-marked at the open (per-name table). CBRL×8 yday $51.84 → 09:30 $52.39 +4.40 | — |
| 2026-09-25 09:30 ET | **SELL** | `CBRL` | 8 | $52.39 | $2.03 | $+51.07 | $8,827.91 | ▲ +51.07 after sell → book $8,827.91; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,827.91 | ▲ close $8,827.91 vs 09:30 $8,829.94 (session +0.00) | 16:00 close · cash $8,827.91 · no lots left · equity $8,827.91. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SPHR` | cash | leftover split 6.16 < 1 share @ 176.68 |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `KULR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `RLX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `XP` | cash | leftover split 9.24 < 1 share @ 15.93 |
| 2026-08-18 | `KULR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `RLX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CIG` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WB` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `FUTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WMT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FUTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WMT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BJ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HITI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `HITI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RHI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SJM` | cash | leftover split 41.01 < 1 share @ 134.80 |
| 2026-08-26 | `URBN` | cash | leftover split 41.01 < 1 share @ 78.90 |
| 2026-08-27 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RHI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NCNO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `PLAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `NCNO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `PLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `TRLV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `BOX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TRLV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `BOX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `JKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ULTA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `JKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ULTA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MOMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PVH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSXY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDO` | cash | leftover split 69.80 < 1 share @ 162.10 |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MOMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PVH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSXY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `WNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ADCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `DOCU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `XP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `KFY` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `WNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ADCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `DOCU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `XP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `SSL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `SSL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TCOM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `LEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `LEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `HUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `UMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `NEO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `CBRL` | no_price | no 09:30 open |
| 2026-09-23 | `A` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `HUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `UMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `NEO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `CBRL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CBRL` | 6 | 2026-09-23 @ $47.57 | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-11.2; leftover $301.11 |
