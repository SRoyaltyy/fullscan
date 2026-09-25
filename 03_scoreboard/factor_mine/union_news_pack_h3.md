# Factor mine action — `union_news_pack_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · morning packet news🟢 only (not the merged box)

Cash book **-14.48%** ($8,552) · signal-only (no cash/fees) was -16.54%. Starts YES **11/30**. Fills 46 · skips 85 · realized $-1342.35.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: the morning news packet box is green.
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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news_box=good` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $194.00.

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
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 27 | $120.00 | $2.07 | — | $6,757.93 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+0.6; leftover $3333.33 | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 9 | $359.83 | $2.02 | — | $3,517.44 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+5.9; leftover $3333.33 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 22 | $146.90 | $2.06 | — | $283.59 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+3.6; leftover $3333.33 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $283.59 | ▲ close $10,215.59 vs 09:30 $10,000.00 (session +221.73) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $283.59 | ▲ 09:30 equity $10,320.45 vs yday $10,215.59 (+104.86) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 2 | $46.18 | $0.93 | — | $190.30 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+6.7; leftover $94.53 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $190.30 | ▼ close $10,016.13 vs 09:30 $10,320.45 (session -303.39) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $190.30 | ▼ 09:30 equity $9,915.15 vs yday $10,016.13 (-100.98) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $190.30 | ▼ close $9,356.46 vs 09:30 $9,915.15 (session -558.69) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $190.30 | ▲ 09:30 equity $9,409.42 vs yday $9,356.46 (+52.96) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `NRG` | 27 | $116.20 | $2.11 | $-106.78 | $3,325.59 | ▼ -106.78 after sell → book $9,407.31; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `TLN` | 9 | $321.00 | $2.05 | $-353.54 | $6,212.54 | ▼ -353.54 after sell → book $9,405.26; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VST` | 22 | $140.74 | $2.09 | $-139.67 | $9,306.73 | ▼ -139.67 after sell → book $9,403.17; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,306.73 | ▼ close $9,403.11 vs 09:30 $9,409.42 (session -0.06) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,306.73 | ▲ 09:30 equity $9,404.77 vs yday $9,403.11 (+1.66) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 2 | $49.02 | $1.01 | $+3.74 | $9,403.76 | ▲ +3.74 after sell → book $9,403.76; vs 09:30 mark -1.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 210 | $44.76 | $2.71 | — | $1.45 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $9403.76 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.45 | ▼ close $9,323.35 vs 09:30 $9,404.77 (session -77.70) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.45 | ▲ 09:30 equity $9,350.65 vs yday $9,323.35 (+27.30) | — | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.45 | ▼ close $9,113.35 vs 09:30 $9,350.65 (session -237.30) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.45 | ▼ 09:30 equity $9,016.75 vs yday $9,113.35 (-96.60) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.45 | ▲ close $9,023.05 vs 09:30 $9,016.75 (session +6.30) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.45 | ▼ 09:30 equity $8,691.25 vs yday $9,023.05 (-331.80) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 210 | $41.38 | $2.81 | $-715.32 | $8,688.44 | ▼ -715.32 after sell → book $8,688.44; vs 09:30 mark -2.81 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 36 | $118.52 | $2.10 | — | $4,419.62 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $4344.22 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 56 | $77.13 | $2.16 | — | $98.18 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $4344.22 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.18 | ▲ close $9,015.18 vs 09:30 $8,691.25 (session +331.00) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.18 | ▼ 09:30 equity $8,854.02 vs yday $9,015.18 (-161.16) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.18 | ▼ close $8,774.14 vs 09:30 $8,854.02 (session -79.88) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.18 | ▼ 09:30 equity $8,739.42 vs yday $8,774.14 (-34.72) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.18 | ▲ close $8,752.10 vs 09:30 $8,739.42 (session +12.68) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.18 | ▲ 09:30 equity $8,788.94 vs yday $8,752.10 (+36.84) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 36 | $119.19 | $2.14 | $+19.88 | $4,386.88 | ▲ +19.88 after sell → book $8,786.80; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 56 | $78.57 | $2.20 | $+76.28 | $8,784.60 | ▲ +76.28 after sell → book $8,784.60; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $7,484.96 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1464.10 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,281.70 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1464.10 | — |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $4,973.68 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1464.10 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 6 | $240.22 | $2.01 | — | $3,530.35 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1464.10 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $2,222.54 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; ret5=+7.8; leftover $1464.10 | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 35 | $41.74 | $2.10 | — | $759.55 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; ret5=+2.4; leftover $1464.10 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $759.55 | ▼ close $8,607.29 vs 09:30 $8,788.94 (session -165.21) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $759.55 | ▲ 09:30 equity $8,609.07 vs yday $8,607.29 (+1.78) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $759.55 | ▲ close $8,627.61 vs 09:30 $8,609.07 (session +18.54) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $759.55 | ▼ 09:30 equity $8,553.79 vs yday $8,627.61 (-73.82) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $759.55 | ▼ close $8,409.07 vs 09:30 $8,553.79 (session -144.72) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $759.55 | ▼ 09:30 equity $8,352.14 vs yday $8,409.07 (-56.93) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 4 | $318.04 | $2.02 | $-29.50 | $2,029.68 | ▼ -29.50 after sell → book $8,350.11; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 1 | $1224.92 | $2.01 | $-85.12 | $3,252.59 | ▼ -85.12 after sell → book $8,348.10; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 6 | $219.46 | $2.03 | $-128.60 | $4,567.32 | ▼ -128.60 after sell → book $8,346.07; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 5 | $246.70 | $2.02 | $-76.33 | $5,798.80 | ▼ -76.33 after sell → book $8,344.05; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 35 | $42.10 | $2.12 | $+8.39 | $7,270.18 | ▲ +8.39 after sell → book $8,341.93; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,270.18 | ▼ close $8,332.66 vs 09:30 $8,352.14 (session -9.27) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,270.18 | ▲ 09:30 equity $8,333.65 vs yday $8,332.66 (+0.99) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 6 | $351.74 | $2.01 | — | $5,157.73 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $2423.39 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 4 | $486.31 | $2.00 | — | $3,210.49 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $2423.39 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 50 | $47.60 | $2.14 | — | $828.35 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $2423.39 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $828.35 | ▲ close $8,711.25 vs 09:30 $8,333.65 (session +383.75) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $828.35 | ▼ 09:30 equity $8,699.18 vs yday $8,711.25 (-12.07) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 3 | $321.67 | $2.02 | $-240.27 | $1,791.34 | ▼ -240.27 after sell → book $8,697.16; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 3) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 2 | $263.36 | $2.00 | — | $1,262.63 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $597.11 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 2 | $236.82 | $2.00 | — | $786.99 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; ret5=+8.1; leftover $597.11 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 4 | $137.35 | $2.00 | — | $235.59 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; ret5=+5.4; leftover $597.11 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $235.59 | ▼ close $8,674.95 vs 09:30 $8,699.18 (session -16.22) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $235.59 | ▲ 09:30 equity $8,710.21 vs yday $8,674.95 (+35.26) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $235.59 | ▲ close $8,982.73 vs 09:30 $8,710.21 (session +272.52) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $235.59 | ▲ 09:30 equity $9,046.67 vs yday $8,982.73 (+63.94) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 6 | $366.23 | $2.04 | $+82.90 | $2,430.93 | ▲ +82.90 after sell → book $9,044.63; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 4 | $538.47 | $2.03 | $+204.61 | $4,582.78 | ▲ +204.61 after sell → book $9,042.60; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 50 | $56.94 | $2.17 | $+462.69 | $7,427.61 | ▲ +462.69 after sell → book $9,040.43; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,427.61 | ▼ close $8,985.29 vs 09:30 $9,046.67 (session -55.14) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,427.61 | ▼ 09:30 equity $8,953.49 vs yday $8,985.29 (-31.80) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `CRM` | 2 | $245.35 | $2.02 | $-40.03 | $7,916.30 | ▼ -40.03 after sell → book $8,951.47; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BE` | 2 | $260.71 | $2.02 | $+43.76 | $8,435.70 | ▲ +43.76 after sell → book $8,949.46; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MSTR` | 4 | $128.44 | $2.02 | $-39.66 | $8,947.43 | ▼ -39.66 after sell → book $8,947.43; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,947.43 | ▲ close $8,947.43 vs 09:30 $8,953.49 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,947.43 | ▲ 09:30 equity $8,947.43 vs yday $8,947.43 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 36 | $242.17 | $2.10 | — | $227.22 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; ret5=-11.1; leftover $8947.43 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $227.22 | ▲ close $9,307.50 vs 09:30 $8,947.43 (session +362.16) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $227.22 | ▲ 09:30 equity $9,641.58 vs yday $9,307.50 (+334.08) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $227.22 | ▲ close $9,788.82 vs 09:30 $9,641.58 (session +147.24) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $227.22 | ▼ 09:30 equity $9,648.42 vs yday $9,788.82 (-140.40) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $227.22 | ▼ close $9,506.58 vs 09:30 $9,648.42 (session -141.84) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $227.22 | ▼ 09:30 equity $9,347.46 vs yday $9,506.58 (-159.12) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 36 | $253.34 | $2.18 | $+397.84 | $9,345.27 | ▲ +397.84 after sell → book $9,345.27; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 24 | $189.17 | $2.06 | — | $4,803.13 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $4672.64 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 116 | $39.99 | $2.34 | — | $161.95 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $4672.64 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $161.95 | ▼ close $9,024.67 vs 09:30 $9,347.46 (session -316.20) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.95 | ▲ 09:30 equity $9,088.47 vs yday $9,024.67 (+63.80) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $161.95 | ▼ close $8,979.51 vs 09:30 $9,088.47 (session -108.96) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.95 | ▲ 09:30 equity $9,031.03 vs yday $8,979.51 (+51.52) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $161.95 | ▼ close $8,715.75 vs 09:30 $9,031.03 (session -315.28) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.95 | ▼ 09:30 equity $8,662.15 vs yday $8,715.75 (-53.60) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `QCOM` | 24 | $180.61 | $2.11 | $-209.61 | $4,494.49 | ▼ -209.61 after sell → book $8,660.05; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SM` | 116 | $35.91 | $2.39 | $-478.01 | $8,657.66 | ▼ -478.01 after sell → book $8,657.66; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,657.66 | ▲ close $8,657.66 vs 09:30 $8,662.15 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,657.66 | ▲ 09:30 equity $8,657.66 vs yday $8,657.66 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,657.66 | ▲ close $8,657.66 vs 09:30 $8,657.66 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,657.66 | ▲ 09:30 equity $8,657.66 vs yday $8,657.66 (-0.00) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 43 | $196.78 | $2.12 | — | $194.00 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $8657.66 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $194.00 | ▼ close $8,448.71 vs 09:30 $8,657.66 (session -206.83) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $194.00 | ▲ 09:30 equity $8,461.18 vs yday $8,448.71 (+12.47) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $194.00 | ▲ close $8,694.24 vs 09:30 $8,461.18 (session +233.06) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,231.96 | ▲ 09:30 equity $8,231.96 vs yday $8,231.96 (+0.00) | 09:30 open · cash $8,231.96 · no holdings · equity $8,231.96 vs prior close $8,231.96 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 9 | $887.00 | $2.02 | — | $246.94 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $8231.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $246.94 | ▲ close $8,551.83 vs 09:30 $8,231.96 (session +321.88) | 16:00 close · cash $246.94 · equity $8,551.83 vs 09:30 $8,231.96 (+319.87; session marks +321.88) · 1 name(s) marked open→close (per-name table). COST×9 09:30 $887.00 → close $922.76 +321.88 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `EOG` | cash | leftover split 94.53 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 94.53 < 1 share @ 202.70 |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 0.73 < 1 share @ 119.43 |
| 2026-08-21 | `DE` | cash | leftover split 0.73 < 1 share @ 623.26 |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 49.09 < 1 share @ 267.02 |
| 2026-08-26 | `CM` | cash | leftover split 49.09 < 1 share @ 118.50 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 12.27 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 12.27 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 12.27 < 1 share @ 1746.53 |
| 2026-08-27 | `CM` | cash | leftover split 12.27 < 1 share @ 118.77 |
| 2026-08-27 | `LRCX` | cash | leftover split 12.27 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 12.27 < 1 share @ 222.86 |
| 2026-08-27 | `ADSK` | cash | leftover split 12.27 < 1 share @ 261.47 |
| 2026-08-27 | `AXTI` | cash | leftover split 12.27 < 1 share @ 70.30 |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DDOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DDOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HPE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HPE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ADBE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ADBE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `LITE` | cash | leftover split 161.95 < 1 share @ 934.88 |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `CTAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CTAS` | 43 | 2026-09-23 @ $196.78 | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $8657.66 |
