# Factor mine action — `union_earn_react_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ earn_react, no 🚨

Cash book **-10.00%** ($9,000) · signal-only (no cash/fees) was -3.08%. Starts YES **4/30**. Fills 175 · skips 54 · realized $-117.67.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the name is in an earnings-reaction window (just reported, we are trading the reaction — not today's print).
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
- **Gate** `earn_react=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,882.31.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 6172 | $0.81 | $68.51 | — | $4,932.17 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten; ⚪; ret5=+13.2; leftover $5000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 223 | $22.01 | $2.88 | — | $21.06 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.06 | ▲ close $10,769.53 vs 09:30 $10,000.00 (session +840.92) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.06 | ▲ 09:30 equity $10,963.61 vs yday $10,769.53 (+194.08) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 6172 | $0.93 | $76.99 | $+595.14 | $5,684.04 | ▲ +595.14 after sell → book $10,886.63; vs 09:30 mark -76.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 223 | $23.33 | $2.96 | $+288.53 | $10,883.67 | ▲ +288.53 after sell → book $10,883.67; vs 09:30 mark -2.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 137 | $9.89 | $2.40 | — | $9,525.66 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; leftover $1360.46 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRJ` | 246 | $5.51 | $3.17 | — | $8,167.02 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+13.1; leftover $1360.46 | — |
| 2026-08-14 09:30 ET | **BUY** | `AMAT` | 2 | $499.40 | $2.00 | — | $7,166.23 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+1.3; leftover $1360.46 | — |
| 2026-08-14 09:30 ET | **BUY** | `AMPG` | 311 | $4.37 | $4.01 | — | $5,803.77 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+10.3; leftover $1360.46 | — |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 51 | $26.25 | $2.14 | — | $4,463.13 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $1360.46 | — |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1776 | $0.77 | $18.93 | — | $3,083.78 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $1360.46 | — |
| 2026-08-14 09:30 ET | **BUY** | `DEFT` | 2894 | $0.47 | $22.28 | — | $1,701.32 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+11.1; leftover $1360.46 | — |
| 2026-08-14 09:30 ET | **BUY** | `DGXX` | 347 | $3.92 | $4.48 | — | $336.60 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+10.1; leftover $1360.46 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $336.60 | ▼ close $10,583.79 vs 09:30 $10,963.61 (session -240.47) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $336.60 | ▼ 09:30 equity $10,578.62 vs yday $10,583.79 (-5.17) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 137 | $10.97 | $2.44 | $+142.44 | $1,837.06 | ▲ +142.44 after sell → book $10,576.18; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRJ` | 246 | $6.22 | $3.23 | $+168.26 | $3,363.95 | ▲ +168.26 after sell → book $10,572.95; vs 09:30 mark -3.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AMAT` | 2 | $517.45 | $2.02 | $+32.08 | $4,396.83 | ▲ +32.08 after sell → book $10,570.94; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AMPG` | 311 | $4.09 | $4.07 | $-94.54 | $5,664.74 | ▼ -94.54 after sell → book $10,566.86; vs 09:30 mark -4.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 51 | $23.00 | $2.16 | $-169.80 | $6,835.58 | ▼ -169.80 after sell → book $10,564.70; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1776 | $0.55 | $15.44 | $-414.43 | $7,800.50 | ▼ -414.43 after sell → book $10,549.27; vs 09:30 mark -15.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DEFT` | 2894 | $0.47 | $22.92 | $-30.73 | $9,152.23 | ▼ -30.73 after sell → book $10,526.35; vs 09:30 mark -22.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DGXX` | 347 | $3.96 | $4.54 | $+4.86 | $10,521.80 | ▲ +4.86 after sell → book $10,521.80; vs 09:30 mark -4.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,521.80 | ▲ close $10,521.80 vs 09:30 $10,578.62 (session +0.00) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,521.80 | ▲ 09:30 equity $10,521.80 vs yday $10,521.80 (+0.00) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,521.80 | ▲ close $10,521.80 vs 09:30 $10,521.80 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,521.80 | ▲ 09:30 equity $10,521.80 vs yday $10,521.80 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,521.80 | ▲ close $10,521.80 vs 09:30 $10,521.80 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,521.80 | ▲ 09:30 equity $10,521.80 vs yday $10,521.80 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 28 | $46.85 | $2.07 | — | $9,207.93 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+5.0; leftover $1315.23 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 145 | $9.01 | $2.42 | — | $7,899.05 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $1315.23 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 338 | $3.89 | $4.36 | — | $6,579.87 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.5; leftover $1315.23 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 38 | $34.05 | $2.10 | — | $5,283.87 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+9.3; leftover $1315.23 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 58 | $22.44 | $2.16 | — | $3,980.18 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.1; leftover $1315.23 | — |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 10 | $123.47 | $2.02 | — | $2,743.46 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+2.9; leftover $1315.23 | — |
| 2026-08-20 09:30 ET | **BUY** | `BILL` | 26 | $49.00 | $2.07 | — | $1,467.40 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-2.0; leftover $1315.23 | — |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 132 | $9.94 | $2.39 | — | $152.93 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+12.6; leftover $1315.23 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $152.93 | ▼ close $10,379.92 vs 09:30 $10,521.80 (session -122.28) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $152.93 | ▼ 09:30 equity $10,377.93 vs yday $10,379.92 (-1.99) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AAP` | 28 | $42.41 | $2.09 | $-128.49 | $1,338.32 | ▼ -128.49 after sell → book $10,375.84; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AEG` | 145 | $9.04 | $2.46 | $-0.53 | $2,646.66 | ▼ -0.53 after sell → book $10,373.38; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALVO` | 338 | $4.32 | $4.43 | $+136.55 | $4,102.39 | ▲ +136.55 after sell → book $10,368.95; vs 09:30 mark -4.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 38 | $34.31 | $2.12 | $+5.65 | $5,404.04 | ▲ +5.65 after sell → book $10,366.82; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 58 | $22.20 | $2.18 | $-18.27 | $6,689.46 | ▼ -18.27 after sell → book $10,364.64; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BABA` | 10 | $125.35 | $2.04 | $+14.74 | $7,940.92 | ▲ +14.74 after sell → book $10,362.60; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BILL` | 26 | $47.50 | $2.09 | $-43.16 | $9,173.83 | ▼ -43.16 after sell → book $10,360.51; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BULL` | 132 | $8.99 | $2.42 | $-130.20 | $10,358.09 | ▼ -130.20 after sell → book $10,358.09; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 115 | $17.93 | $2.33 | — | $8,293.23 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $2071.62 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 22 | $93.98 | $2.06 | — | $6,223.62 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.4; leftover $2071.62 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 48 | $43.08 | $2.13 | — | $4,153.64 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.9; leftover $2071.62 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 900 | $2.30 | $11.61 | — | $2,072.03 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.0; leftover $2071.62 | — |
| 2026-08-21 09:30 ET | **BUY** | `ROST` | 8 | $243.85 | $2.01 | — | $119.22 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-6.5; leftover $2071.62 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $119.22 | ▲ close $10,393.91 vs 09:30 $10,377.93 (session +55.97) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $119.22 | ▲ 09:30 equity $10,463.18 vs yday $10,393.91 (+69.27) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 115 | $18.05 | $2.37 | $+9.09 | $2,193.17 | ▲ +9.09 after sell → book $10,460.81; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 22 | $97.02 | $2.08 | $+62.74 | $4,325.53 | ▲ +62.74 after sell → book $10,458.73; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 48 | $44.22 | $2.16 | $+50.42 | $6,445.93 | ▲ +50.42 after sell → book $10,456.57; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 900 | $2.34 | $11.78 | $+12.61 | $8,540.15 | ▲ +12.61 after sell → book $10,444.79; vs 09:30 mark -11.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ROST` | 8 | $238.08 | $2.04 | $-50.21 | $10,442.75 | ▼ -50.21 after sell → book $10,442.75; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,442.75 | ▲ close $10,442.75 vs 09:30 $10,463.18 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,442.75 | ▲ 09:30 equity $10,442.75 vs yday $10,442.75 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 7 | $175.01 | $2.01 | — | $9,215.67 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-7.0; leftover $1305.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 14 | $88.94 | $2.03 | — | $7,968.48 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-4.9; leftover $1305.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 85 | $15.28 | $2.25 | — | $6,667.44 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-0.7; leftover $1305.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 9 | $142.36 | $2.02 | — | $5,384.18 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-8.6; leftover $1305.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 255 | $5.10 | $3.29 | — | $4,080.39 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-8.9; leftover $1305.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 27 | $47.89 | $2.07 | — | $2,785.29 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ⚪; ret5=+14.0; leftover $1305.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 93 | $13.92 | $2.27 | — | $1,488.46 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+5.9; leftover $1305.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 287 | $4.54 | $3.70 | — | $180.34 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-14.6; leftover $1305.34 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $180.34 | ▼ close $10,039.80 vs 09:30 $10,442.75 (session -383.32) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $180.34 | ▼ 09:30 equity $10,005.94 vs yday $10,039.80 (-33.86) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `BMO` | 7 | $173.22 | $2.03 | $-16.57 | $1,390.85 | ▼ -16.57 after sell → book $10,003.91; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BNS` | 14 | $92.65 | $2.05 | $+47.86 | $2,685.90 | ▲ +47.86 after sell → book $10,001.86; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BZ` | 85 | $16.77 | $2.27 | $+122.13 | $4,109.08 | ▲ +122.13 after sell → book $9,999.59; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DKS` | 9 | $121.87 | $2.04 | $-188.46 | $5,203.87 | ▼ -188.46 after sell → book $9,997.55; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EH` | 255 | $4.77 | $3.34 | $-90.78 | $6,416.88 | ▼ -90.78 after sell → book $9,994.21; vs 09:30 mark -3.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GFI` | 27 | $48.24 | $2.09 | $+5.29 | $7,717.27 | ▲ +5.29 after sell → book $9,992.12; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 93 | $14.03 | $2.29 | $+5.67 | $9,019.76 | ▲ +5.67 after sell → book $9,989.82; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SHMD` | 287 | $3.38 | $3.76 | $-341.82 | $9,986.06 | ▼ -341.82 after sell → book $9,986.06; vs 09:30 mark -3.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 239 | $5.21 | $3.08 | — | $8,737.79 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $1248.26 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 9 | $131.37 | $2.02 | — | $7,553.44 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.3; leftover $1248.26 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 68 | $18.26 | $2.19 | — | $6,309.57 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-11.4; leftover $1248.26 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 36 | $34.30 | $2.10 | — | $5,072.67 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+1.7; leftover $1248.26 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 3 | $326.91 | $2.00 | — | $4,089.94 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-15.2; leftover $1248.26 | — |
| 2026-08-26 09:30 ET | **BUY** | `FSCO` | 245 | $5.08 | $3.16 | — | $2,842.18 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-1.6; leftover $1248.26 | — |
| 2026-08-26 09:30 ET | **BUY** | `HEI` | 3 | $370.00 | $2.00 | — | $1,730.18 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.6; leftover $1248.26 | — |
| 2026-08-26 09:30 ET | **BUY** | `INTU` | 3 | $323.47 | $2.00 | — | $757.77 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+2.0; leftover $1248.26 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $757.77 | ▲ close $10,142.92 vs 09:30 $10,005.94 (session +175.41) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $757.77 | ▲ 09:30 equity $10,152.93 vs yday $10,142.92 (+10.01) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 239 | $5.49 | $3.13 | $+60.70 | $2,066.75 | ▲ +60.70 after sell → book $10,149.80; vs 09:30 mark -3.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ANF` | 9 | $144.70 | $2.04 | $+115.92 | $3,367.01 | ▲ +115.92 after sell → book $10,147.76; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BBWI` | 68 | $18.69 | $2.22 | $+24.83 | $4,635.72 | ▲ +24.83 after sell → book $10,145.55; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BOX` | 36 | $33.79 | $2.12 | $-22.58 | $5,850.04 | ▼ -22.58 after sell → book $10,143.43; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DY` | 3 | $314.90 | $2.02 | $-40.05 | $6,792.72 | ▼ -40.05 after sell → book $10,141.41; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FSCO` | 245 | $5.10 | $3.21 | $-1.47 | $8,039.01 | ▼ -1.47 after sell → book $10,138.20; vs 09:30 mark -3.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HEI` | 3 | $346.19 | $2.02 | $-75.45 | $9,075.56 | ▼ -75.45 after sell → book $10,136.18; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INTU` | 3 | $353.54 | $2.02 | $+86.19 | $10,134.16 | ▲ +86.19 after sell → book $10,134.16; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 15 | $80.60 | $2.04 | — | $8,923.13 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.0; leftover $1266.77 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 78 | $16.18 | $2.22 | — | $7,658.86 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-6.7; leftover $1266.77 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 10 | $118.77 | $2.02 | — | $6,469.14 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.3; leftover $1266.77 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 71 | $17.78 | $2.20 | — | $5,204.56 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-1.2; leftover $1266.77 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 94 | $13.41 | $2.27 | — | $3,941.75 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.1; leftover $1266.77 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 13 | $97.16 | $2.03 | — | $2,676.64 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.5; leftover $1266.77 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 6 | $206.82 | $2.01 | — | $1,433.71 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-0.2; leftover $1266.77 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 10 | $120.17 | $2.02 | — | $229.99 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.9; leftover $1266.77 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $229.99 | ▲ close $10,203.67 vs 09:30 $10,152.93 (session +86.32) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $229.99 | ▲ 09:30 equity $10,238.70 vs yday $10,203.67 (+35.03) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BBY` | 15 | $83.85 | $2.06 | $+44.66 | $1,485.69 | ▲ +44.66 after sell → book $10,236.65; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BILI` | 78 | $16.94 | $2.25 | $+54.81 | $2,804.76 | ▲ +54.81 after sell → book $10,234.40; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 10 | $115.66 | $2.04 | $-35.16 | $3,959.32 | ▼ -35.16 after sell → book $10,232.36; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CMBT` | 71 | $18.58 | $2.23 | $+52.37 | $5,276.27 | ▲ +52.37 after sell → book $10,230.13; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CSIQ` | 94 | $13.65 | $2.30 | $+17.99 | $6,557.07 | ▲ +17.99 after sell → book $10,227.83; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `HQY` | 13 | $93.62 | $2.05 | $-50.10 | $7,772.09 | ▼ -50.10 after sell → book $10,225.79; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RY` | 6 | $205.50 | $2.03 | $-11.96 | $9,003.06 | ▼ -11.96 after sell → book $10,223.76; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TD` | 10 | $122.07 | $2.04 | $+14.94 | $10,221.72 | ▲ +14.94 after sell → book $10,221.72; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $9,175.08 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+7.8; leftover $1277.71 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 85 | $15.01 | $2.25 | — | $7,896.98 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+3.7; leftover $1277.71 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 12 | $103.89 | $2.03 | — | $6,648.27 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.5; leftover $1277.71 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 329 | $3.88 | $4.24 | — | $5,367.51 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-8.6; leftover $1277.71 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 28 | $44.40 | $2.07 | — | $4,122.24 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.4; leftover $1277.71 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 51 | $24.69 | $2.14 | — | $2,860.90 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.8; leftover $1277.71 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 153 | $8.35 | $2.45 | — | $1,580.90 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.1; leftover $1277.71 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 33 | $37.65 | $2.09 | — | $336.53 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.9; leftover $1277.71 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $336.53 | ▼ close $9,827.20 vs 09:30 $10,238.70 (session -375.25) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $336.53 | ▲ 09:30 equity $9,838.08 vs yday $9,827.20 (+10.88) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $257.71 | $2.02 | $-17.82 | $1,365.35 | ▼ -17.82 after sell → book $9,836.06; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBAR` | 85 | $14.88 | $2.27 | $-15.56 | $2,627.88 | ▼ -15.56 after sell → book $9,833.79; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 12 | $98.00 | $2.05 | $-74.75 | $3,801.83 | ▼ -74.75 after sell → book $9,831.74; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 329 | $3.39 | $4.31 | $-169.76 | $4,912.83 | ▼ -169.76 after sell → book $9,827.43; vs 09:30 mark -4.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 28 | $44.85 | $2.09 | $+8.43 | $6,166.54 | ▲ +8.43 after sell → book $9,825.34; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 51 | $22.98 | $2.16 | $-91.52 | $7,336.36 | ▼ -91.52 after sell → book $9,823.18; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 153 | $8.53 | $2.48 | $+22.61 | $8,638.96 | ▲ +22.61 after sell → book $9,820.69; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `IREN` | 33 | $35.81 | $2.11 | $-64.75 | $9,818.58 | ▼ -64.75 after sell → book $9,818.58; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,818.58 | ▲ close $9,818.58 vs 09:30 $9,838.08 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,818.58 | ▲ 09:30 equity $9,818.58 vs yday $9,818.58 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,818.58 | ▲ close $9,818.58 vs 09:30 $9,818.58 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,818.58 | ▲ 09:30 equity $9,818.58 vs yday $9,818.58 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,818.58 | ▲ close $9,818.58 vs 09:30 $9,818.58 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,818.58 | ▲ 09:30 equity $9,818.58 vs yday $9,818.58 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 114 | $10.74 | $2.33 | — | $8,591.32 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+8.5; leftover $1227.32 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $7,534.10 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+3.3; leftover $1227.32 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 177 | $6.90 | $2.52 | — | $6,310.28 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.8; leftover $1227.32 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 3 | $354.49 | $2.00 | — | $5,244.81 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.3; leftover $1227.32 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 54 | $22.32 | $2.15 | — | $4,037.38 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+2.4; leftover $1227.32 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 4 | $257.00 | $2.00 | — | $3,007.38 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-5.5; leftover $1227.32 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 25 | $47.60 | $2.06 | — | $1,815.31 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-6.2; leftover $1227.32 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 81 | $15.09 | $2.23 | — | $590.79 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+6.1; leftover $1227.32 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $590.79 | ▲ close $10,221.19 vs 09:30 $9,818.58 (session +419.91) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $590.79 | ▲ 09:30 equity $10,258.91 vs yday $10,221.19 (+37.72) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 114 | $10.91 | $2.36 | $+14.12 | $1,832.17 | ▲ +14.12 after sell → book $10,256.55; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $2,909.25 | ▲ +19.86 after sell → book $10,254.53; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 177 | $9.28 | $2.56 | $+416.18 | $4,549.25 | ▲ +416.18 after sell → book $10,251.97; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 3 | $321.67 | $2.02 | $-102.48 | $5,512.24 | ▼ -102.48 after sell → book $10,249.95; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CPB` | 54 | $22.10 | $2.17 | $-16.20 | $6,703.47 | ▼ -16.20 after sell → book $10,247.78; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 4 | $238.88 | $2.02 | $-76.50 | $7,656.96 | ▼ -76.50 after sell → book $10,245.75; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 25 | $53.85 | $2.09 | $+152.10 | $9,001.13 | ▲ +152.10 after sell → book $10,243.67; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 81 | $15.34 | $2.26 | $+15.76 | $10,241.41 | ▲ +15.76 after sell → book $10,241.41; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 20 | $63.18 | $2.05 | — | $8,975.76 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-10.9; leftover $1280.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 146 | $8.74 | $2.43 | — | $7,697.29 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.8; leftover $1280.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 18 | $68.52 | $2.04 | — | $6,461.89 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+3.4; leftover $1280.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 354 | $3.62 | $4.57 | — | $5,177.61 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-3.1; leftover $1280.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 7 | $167.55 | $2.01 | — | $4,002.75 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.9; leftover $1280.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 28 | $44.90 | $2.07 | — | $2,743.48 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-7.5; leftover $1280.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 13 | $98.15 | $2.03 | — | $1,465.50 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.9; leftover $1280.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 81 | $15.70 | $2.23 | — | $191.57 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-0.4; leftover $1280.18 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $191.57 | ▼ close $10,138.96 vs 09:30 $10,258.91 (session -83.02) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $191.57 | ▼ 09:30 equity $10,079.07 vs yday $10,138.96 (-59.89) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `AMBA` | 20 | $63.83 | $2.07 | $+8.88 | $1,466.10 | ▲ +8.88 after sell → book $10,077.00; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASAN` | 146 | $8.73 | $2.46 | $-6.35 | $2,738.21 | ▼ -6.35 after sell → book $10,074.53; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 18 | $67.05 | $2.06 | $-30.57 | $3,943.05 | ▼ -30.57 after sell → book $10,072.47; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOMO` | 354 | $3.84 | $4.64 | $+70.45 | $5,297.77 | ▲ +70.45 after sell → book $10,067.83; vs 09:30 mark -4.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 7 | $160.52 | $2.03 | $-53.25 | $6,419.38 | ▼ -53.25 after sell → book $10,065.80; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 28 | $39.56 | $2.09 | $-153.69 | $7,524.97 | ▼ -153.69 after sell → book $10,063.71; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 13 | $100.58 | $2.05 | $+27.51 | $8,830.46 | ▲ +27.51 after sell → book $10,061.66; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MAMA` | 81 | $15.20 | $2.26 | $-44.99 | $10,059.40 | ▼ -44.99 after sell → book $10,059.40; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,059.40 | ▲ close $10,059.40 vs 09:30 $10,079.07 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,059.40 | ▲ 09:30 equity $10,059.40 vs yday $10,059.40 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,059.40 | ▲ close $10,059.40 vs 09:30 $10,059.40 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,059.40 | ▲ 09:30 equity $10,059.40 vs yday $10,059.40 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,059.40 | ▲ close $10,059.40 vs 09:30 $10,059.40 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,059.40 | ▲ 09:30 equity $10,059.40 vs yday $10,059.40 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $8,906.38 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1257.43 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 5 | $242.17 | $2.00 | — | $7,693.53 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-11.1; leftover $1257.43 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 39 | $32.01 | $2.11 | — | $6,443.03 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-4.4; leftover $1257.43 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 17 | $71.71 | $2.04 | — | $5,221.92 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-9.1; leftover $1257.43 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 22 | $56.02 | $2.06 | — | $3,987.42 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.2; leftover $1257.43 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 134 | $9.37 | $2.39 | — | $2,729.45 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+1.5; leftover $1257.43 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 95 | $13.10 | $2.27 | — | $1,482.68 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-6.9; leftover $1257.43 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 9 | $135.71 | $2.02 | — | $259.27 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-9.2; leftover $1257.43 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $259.27 | ▲ close $10,092.17 vs 09:30 $10,059.40 (session +49.67) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $259.27 | ▼ 09:30 equity $10,090.41 vs yday $10,092.17 (-1.76) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 7 | $141.42 | $2.03 | $-165.11 | $1,247.18 | ▼ -165.11 after sell → book $10,088.38; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 5 | $261.51 | $2.03 | $+92.67 | $2,552.70 | ▲ +92.67 after sell → book $10,086.35; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CPRT` | 39 | $30.63 | $2.13 | $-58.05 | $3,745.15 | ▼ -58.05 after sell → book $10,084.23; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DSGX` | 17 | $77.68 | $2.06 | $+97.39 | $5,063.64 | ▲ +97.39 after sell → book $10,082.16; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `KR` | 22 | $59.31 | $2.08 | $+68.25 | $6,366.39 | ▲ +68.25 after sell → book $10,080.09; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `LPTH` | 134 | $8.85 | $2.42 | $-74.50 | $7,549.86 | ▼ -74.50 after sell → book $10,077.66; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `REF` | 95 | $14.16 | $2.30 | $+96.12 | $8,892.76 | ▲ +96.12 after sell → book $10,075.36; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 9 | $131.40 | $2.04 | $-42.84 | $10,073.32 | ▼ -42.84 after sell → book $10,073.32; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,073.32 | ▲ close $10,073.32 vs 09:30 $10,090.41 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,073.32 | ▲ 09:30 equity $10,073.32 vs yday $10,073.32 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,073.32 | ▲ close $10,073.32 vs 09:30 $10,073.32 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,073.32 | ▲ 09:30 equity $10,073.32 vs yday $10,073.32 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 246 | $40.93 | $3.17 | — | $1.37 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.1; leftover $10073.32 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.37 | ▼ close $9,947.15 vs 09:30 $10,073.32 (session -123.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.37 | ▲ 09:30 equity $10,035.71 vs yday $9,947.15 (+88.56) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `TCOM` | 246 | $40.79 | $3.29 | $-40.91 | $10,032.42 | ▼ -40.91 after sell → book $10,032.42; vs 09:30 mark -3.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 447 | $11.21 | $5.77 | — | $5,015.78 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+1.0; leftover $5016.21 | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 61 | $81.00 | $2.17 | — | $72.61 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-3.0; leftover $5016.21 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.61 | ▲ close $10,094.92 vs 09:30 $10,035.71 (session +70.44) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.61 | ▼ 09:30 equity $10,048.94 vs yday $10,094.92 (-45.98) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ALMU` | 447 | $11.64 | $5.88 | $+180.56 | $5,269.81 | ▲ +180.56 after sell → book $10,043.06; vs 09:30 mark -5.88 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LEN` | 61 | $78.25 | $2.22 | $-172.14 | $10,040.83 | ▼ -172.14 after sell → book $10,040.83; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,040.83 | ▲ close $10,040.83 vs 09:30 $10,048.94 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,040.83 | ▲ 09:30 equity $10,040.83 vs yday $10,040.83 (+0.00) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,040.83 | ▲ close $10,040.83 vs 09:30 $10,040.83 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,040.83 | ▲ 09:30 equity $10,040.83 vs yday $10,040.83 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,040.83 | ▲ close $10,040.83 vs 09:30 $10,040.83 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,040.83 | ▲ 09:30 equity $10,040.83 vs yday $10,040.83 (+0.00) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 42 | $47.57 | $2.12 | — | $8,040.78 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-11.2; leftover $2008.17 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 10 | $196.78 | $2.02 | — | $6,070.96 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $2008.17 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 56 | $35.74 | $2.16 | — | $4,067.36 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.1; leftover $2008.17 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 42 | $47.15 | $2.12 | — | $2,084.94 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-1.9; leftover $2008.17 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 18 | $109.67 | $2.04 | — | $108.84 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-3.0; leftover $2008.17 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $108.84 | ▼ close $9,891.42 vs 09:30 $10,040.83 (session -138.96) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $108.84 | ▲ 09:30 equity $9,892.90 vs yday $9,891.42 (+1.48) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `CBRL` | 42 | $46.88 | $2.14 | $-33.24 | $2,075.66 | ▼ -33.24 after sell → book $9,890.75; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 10 | $192.26 | $2.05 | $-49.27 | $3,996.21 | ▼ -49.27 after sell → book $9,888.71; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `GIS` | 56 | $35.96 | $2.18 | $+7.98 | $6,007.79 | ▲ +7.98 after sell → book $9,886.52; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `KBH` | 42 | $47.14 | $2.14 | $-4.68 | $7,985.53 | ▼ -4.68 after sell → book $9,884.38; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PAYX` | 18 | $105.49 | $2.07 | $-79.32 | $9,882.31 | ▼ -79.32 after sell → book $9,882.31; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,882.31 | ▲ close $9,882.31 vs 09:30 $9,892.90 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,680.49 | ▲ 09:30 equity $8,680.49 vs yday $8,680.49 (+0.00) | 09:30 open · cash $8,680.49 · no holdings · equity $8,680.49 vs prior close $8,680.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 9 | $887.00 | $2.02 | — | $695.47 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+0.3; leftover $8680.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $695.47 | ▲ close $9,000.36 vs 09:30 $8,680.49 (session +321.88) | 16:00 close · cash $695.47 · equity $9,000.36 vs 09:30 $8,680.49 (+319.87; session marks +321.88) · 1 name(s) marked open→close (per-name table). COST×9 09:30 $887.00 → close $922.76 +321.88 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `LOW` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `LZB` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MRCY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `M` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAVN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SHOE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new buys |
