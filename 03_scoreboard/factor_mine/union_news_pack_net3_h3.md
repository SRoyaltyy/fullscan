# Factor mine action — `union_news_pack_net3_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 and camera net ≥ 3

Cash book **-21.76%** ($7,824) · signal-only (no cash/fees) was -17.00%. Starts YES **11/30**. Fills 40 · skips 76 · realized $-1552.71.

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
- Must-have: camera net (+G −R) is at least 3.
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
- **Gate** `news_box=good,cam_net_min=3` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $180.44.

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
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 27 | $120.00 | $2.07 | — | $6,757.93 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+0.6; leftover $3333.33 | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 9 | $359.83 | $2.02 | — | $3,517.44 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+5.9; leftover $3333.33 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 22 | $146.90 | $2.06 | — | $283.59 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+3.6; leftover $3333.33 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $283.59 | ▲ close $10,215.59 vs 09:30 $10,000.00 (session +221.73) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $283.59 | ▲ 09:30 equity $10,320.45 vs yday $10,215.59 (+104.86) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 2 | $46.18 | $0.93 | — | $190.30 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+6.7; leftover $94.53 | — |
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
| 2026-08-20 09:30 ET | **BUY** | `APA` | 210 | $44.76 | $2.71 | — | $1.45 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $9403.76 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.45 | ▼ close $9,323.35 vs 09:30 $9,404.77 (session -77.70) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.45 | ▲ 09:30 equity $9,350.65 vs yday $9,323.35 (+27.30) | — | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.45 | ▼ close $9,113.35 vs 09:30 $9,350.65 (session -237.30) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.45 | ▼ 09:30 equity $9,016.75 vs yday $9,113.35 (-96.60) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.45 | ▲ close $9,023.05 vs 09:30 $9,016.75 (session +6.30) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.45 | ▼ 09:30 equity $8,691.25 vs yday $9,023.05 (-331.80) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 210 | $41.38 | $2.81 | $-715.32 | $8,688.44 | ▼ -715.32 after sell → book $8,688.44; vs 09:30 mark -2.81 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 36 | $118.52 | $2.10 | — | $4,419.62 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $4344.22 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 56 | $77.13 | $2.16 | — | $98.18 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $4344.22 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.18 | ▲ close $9,015.18 vs 09:30 $8,691.25 (session +331.00) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.18 | ▼ 09:30 equity $8,854.02 vs yday $9,015.18 (-161.16) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.18 | ▼ close $8,774.14 vs 09:30 $8,854.02 (session -79.88) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.18 | ▼ 09:30 equity $8,739.42 vs yday $8,774.14 (-34.72) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.18 | ▲ close $8,752.10 vs 09:30 $8,739.42 (session +12.68) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.18 | ▲ 09:30 equity $8,788.94 vs yday $8,752.10 (+36.84) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 36 | $119.19 | $2.14 | $+19.88 | $4,386.88 | ▲ +19.88 after sell → book $8,786.80; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 56 | $78.57 | $2.20 | $+76.28 | $8,784.60 | ▲ +76.28 after sell → book $8,784.60; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 5 | $324.41 | $2.00 | — | $7,160.54 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1756.92 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 4 | $400.42 | $2.00 | — | $5,556.86 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1756.92 | — |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $4,248.84 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1756.92 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 7 | $240.22 | $2.01 | — | $2,565.29 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1756.92 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 6 | $261.16 | $2.01 | — | $996.32 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list earn_react; ret5=+7.8; leftover $1756.92 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $996.32 | ▼ close $8,589.01 vs 09:30 $8,788.94 (session -185.57) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $996.32 | ▼ 09:30 equity $8,568.45 vs yday $8,589.01 (-20.56) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $996.32 | ▲ close $8,619.25 vs 09:30 $8,568.45 (session +50.80) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $996.32 | ▼ 09:30 equity $8,507.38 vs yday $8,619.25 (-111.87) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $996.32 | ▼ close $8,312.97 vs 09:30 $8,507.38 (session -194.41) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $996.32 | ▼ 09:30 equity $8,256.86 vs yday $8,312.97 (-56.11) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 5 | $318.04 | $2.03 | $-35.88 | $2,584.49 | ▼ -35.88 after sell → book $8,254.83; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 4 | $357.25 | $2.02 | $-176.71 | $4,011.47 | ▼ -176.71 after sell → book $8,252.81; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 1 | $1224.92 | $2.01 | $-85.12 | $5,234.38 | ▼ -85.12 after sell → book $8,250.80; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 7 | $219.46 | $2.03 | $-149.36 | $6,768.56 | ▼ -149.36 after sell → book $8,248.76; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 6 | $246.70 | $2.03 | $-90.80 | $8,246.73 | ▼ -90.80 after sell → book $8,246.73; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,246.73 | ▲ close $8,246.73 vs 09:30 $8,256.86 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,246.73 | ▲ 09:30 equity $8,246.73 vs yday $8,246.73 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 7 | $351.74 | $2.01 | — | $5,782.54 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $2748.91 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 5 | $486.31 | $2.00 | — | $3,348.99 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $2748.91 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 57 | $47.60 | $2.16 | — | $633.63 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $2748.91 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $633.63 | ▲ close $8,818.78 vs 09:30 $8,246.73 (session +578.22) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $633.63 | ▼ 09:30 equity $8,789.88 vs yday $8,818.78 (-28.90) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 1 | $263.36 | $1.99 | — | $368.27 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $316.81 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 1 | $236.82 | $1.99 | — | $129.46 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list ohlc_hot; ret5=+8.1; leftover $316.81 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.46 | ▼ close $8,731.56 vs 09:30 $8,789.88 (session -54.33) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.46 | ▲ 09:30 equity $8,782.98 vs yday $8,731.56 (+51.42) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.46 | ▲ close $9,098.83 vs 09:30 $8,782.98 (session +315.85) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.46 | ▲ 09:30 equity $9,153.77 vs yday $9,098.83 (+54.94) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 7 | $366.23 | $2.04 | $+97.38 | $2,691.03 | ▲ +97.38 after sell → book $9,151.73; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 5 | $538.47 | $2.04 | $+256.76 | $5,381.34 | ▲ +256.76 after sell → book $9,149.69; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 57 | $56.94 | $2.20 | $+528.02 | $8,624.72 | ▲ +528.02 after sell → book $9,147.49; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,624.72 | ▼ close $9,138.16 vs 09:30 $9,153.77 (session -9.33) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,624.72 | ▼ 09:30 equity $9,130.78 vs yday $9,138.16 (-7.38) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `CRM` | 1 | $245.35 | $2.01 | $-22.02 | $8,868.06 | ▼ -22.02 after sell → book $9,128.77; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BE` | 1 | $260.71 | $2.01 | $+19.88 | $9,126.76 | ▲ +19.88 after sell → book $9,126.76; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,126.76 | ▲ close $9,126.76 vs 09:30 $9,130.78 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,126.76 | ▲ 09:30 equity $9,126.76 vs yday $9,126.76 (-0.00) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,126.76 | ▲ close $9,126.76 vs 09:30 $9,126.76 (session +0.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,126.76 | ▲ 09:30 equity $9,126.76 vs yday $9,126.76 (-0.00) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,126.76 | ▲ close $9,126.76 vs 09:30 $9,126.76 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,126.76 | ▲ 09:30 equity $9,126.76 vs yday $9,126.76 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,126.76 | ▲ close $9,126.76 vs 09:30 $9,126.76 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,126.76 | ▲ 09:30 equity $9,126.76 vs yday $9,126.76 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 24 | $189.17 | $2.06 | — | $4,584.61 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $4563.38 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 114 | $39.99 | $2.33 | — | $23.42 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $4563.38 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.42 | ▼ close $8,809.82 vs 09:30 $9,126.76 (session -312.54) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.42 | ▲ 09:30 equity $8,874.80 vs yday $8,809.82 (+64.98) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.42 | ▼ close $8,767.04 vs 09:30 $8,874.80 (session -107.76) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.42 | ▲ 09:30 equity $8,818.76 vs yday $8,767.04 (+51.72) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.42 | ▼ close $8,503.28 vs 09:30 $8,818.76 (session -315.48) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.42 | ▼ 09:30 equity $8,451.80 vs yday $8,503.28 (-51.48) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `QCOM` | 24 | $180.61 | $2.11 | $-209.61 | $4,355.96 | ▼ -209.61 after sell → book $8,449.70; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SM` | 114 | $35.91 | $2.38 | $-469.84 | $8,447.31 | ▼ -469.84 after sell → book $8,447.31; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,447.31 | ▲ close $8,447.31 vs 09:30 $8,451.80 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,447.31 | ▲ 09:30 equity $8,447.31 vs yday $8,447.31 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,447.31 | ▲ close $8,447.31 vs 09:30 $8,447.31 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,447.31 | ▲ 09:30 equity $8,447.31 vs yday $8,447.31 (+0.00) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 42 | $196.78 | $2.12 | — | $180.44 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $8447.31 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $180.44 | ▼ close $8,243.18 vs 09:30 $8,447.31 (session -202.02) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $180.44 | ▲ 09:30 equity $8,255.36 vs yday $8,243.18 (+12.18) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $180.44 | ▲ close $8,483.00 vs 09:30 $8,255.36 (session +227.64) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,539.73 | ▲ 09:30 equity $7,539.73 vs yday $7,539.73 (+0.00) | 09:30 open · cash $7,539.73 · no holdings · equity $7,539.73 vs prior close $7,539.73 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 8 | $887.00 | $2.01 | — | $441.72 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $7539.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $441.72 | ▲ close $7,823.84 vs 09:30 $7,539.73 (session +286.12) | 16:00 close · cash $441.72 · equity $7,823.84 vs 09:30 $7,539.73 (+284.11; session marks +286.12) · 1 name(s) marked open→close (per-name table). COST×8 09:30 $887.00 → close $922.76 +286.12 | — |

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
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
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
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `LITE` | cash | leftover split 23.42 < 1 share @ 934.88 |
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
| `CTAS` | 42 | 2026-09-23 @ $196.78 | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $8447.31 |
