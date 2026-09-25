# Factor mine action — `union_vol_ab_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-6.02%** ($9,398) · signal-only (no cash/fees) was +81.51%. Starts YES **27/30**. Fills 162 · skips 225 · realized $+477.57.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the volume camera (is this name unusually active?) is green.
- Must-have: the A/B camera (does our A/B score like this name?) is green.
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
- **Gate** `vol=good,ab=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,615.26.

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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,764.83 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,579.67 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,338.50 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 216 | $5.77 | $2.79 | — | $5,089.39 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,850.53 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,603.95 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 714 | $1.75 | $9.21 | — | $1,345.24 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $186.91 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $186.91 | ▲ close $10,208.28 vs 09:30 $10,000.00 (session +232.95) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.91 | ▲ 09:30 equity $10,475.50 vs yday $10,208.28 (+267.22) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $169.53 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $23.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $147.04 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $23.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 9 | $2.47 | $0.25 | — | $124.56 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $23.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 12 | $1.93 | $0.27 | — | $101.13 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $23.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 17 | $1.32 | $0.28 | — | $78.42 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $23.36 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.42 | ▲ close $10,474.93 vs 09:30 $10,475.50 (session +0.63) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.42 | ▲ 09:30 equity $10,582.83 vs yday $10,474.93 (+107.90) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.42 | ▼ close $10,549.90 vs 09:30 $10,582.83 (session -32.93) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.42 | ▼ 09:30 equity $10,384.32 vs yday $10,549.90 (-165.58) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 60 | $20.32 | $2.19 | $-18.16 | $1,295.43 | ▼ -18.16 after sell → book $10,382.13; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.86 | $2.05 | $+58.97 | $2,539.56 | ▲ +58.97 after sell → book $10,380.08; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 60 | $20.47 | $2.19 | $-15.16 | $3,765.57 | ▼ -15.16 after sell → book $10,377.89; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 216 | $5.53 | $2.83 | $-57.46 | $4,957.22 | ▼ -57.46 after sell → book $10,375.06; vs 09:30 mark -2.83 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 63 | $21.21 | $2.20 | $+95.16 | $6,291.25 | ▲ +95.16 after sell → book $10,372.86; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 42 | $32.32 | $2.14 | $+108.73 | $7,646.55 | ▲ +108.73 after sell → book $10,370.72; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 714 | $1.90 | $9.34 | $+88.55 | $8,993.81 | ▲ +88.55 after sell → book $10,361.38; vs 09:30 mark -9.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $10,243.86 | ▲ +91.71 after sell → book $10,359.35; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 107 | $13.59 | $2.31 | — | $8,787.42 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1463.41 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 39 | $36.96 | $2.11 | — | $7,343.87 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1463.41 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 321 | $4.55 | $4.14 | — | $5,879.18 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1463.41 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 897 | $1.63 | $11.57 | — | $4,405.50 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1463.41 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 279 | $5.24 | $3.60 | — | $2,939.94 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1463.41 | — |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2360 | $0.62 | $21.71 | — | $1,455.03 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $1463.41 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 227 | $6.37 | $2.93 | — | $6.11 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1463.41 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.11 | ▼ close $10,302.74 vs 09:30 $10,384.32 (session -8.24) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.11 | ▼ 09:30 equity $10,226.38 vs yday $10,302.74 (-76.36) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $22.52 | ▼ -0.96 after sell → book $10,226.19; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $52.89 | ▲ +7.88 after sell → book $10,225.86; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 9 | $2.41 | $0.26 | $-1.05 | $74.31 | ▼ -1.05 after sell → book $10,225.60; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 12 | $2.03 | $0.30 | $+0.63 | $98.37 | ▲ +0.63 after sell → book $10,225.30; vs 09:30 mark -0.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 17 | $1.60 | $0.34 | $+4.14 | $125.23 | ▲ +4.14 after sell → book $10,224.95; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 107 | $0.58 | $0.94 | — | $61.90 | — | combo gate; gate vol=good,ab=good; list yday_mover; 🔵; ret5=-27.5; leftover $62.61 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.90 | ▼ close $10,080.50 vs 09:30 $10,226.38 (session -143.50) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.90 | ▲ 09:30 equity $10,119.77 vs yday $10,080.50 (+39.27) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.90 | ▲ close $10,177.98 vs 09:30 $10,119.77 (session +58.21) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.90 | ▼ 09:30 equity $10,110.53 vs yday $10,177.98 (-67.45) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 107 | $13.05 | $2.34 | $-62.43 | $1,455.91 | ▼ -62.43 after sell → book $10,108.19; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 39 | $39.60 | $2.13 | $+98.72 | $2,998.18 | ▲ +98.72 after sell → book $10,106.06; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZIP` | 321 | $4.21 | $4.21 | $-117.49 | $4,345.39 | ▼ -117.49 after sell → book $10,101.86; vs 09:30 mark -4.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 897 | $1.69 | $11.73 | $+30.52 | $5,849.59 | ▲ +30.52 after sell → book $10,090.12; vs 09:30 mark -11.74 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 279 | $4.84 | $3.66 | $-118.86 | $7,196.29 | ▼ -118.86 after sell → book $10,086.47; vs 09:30 mark -3.65 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DEFT` | 2360 | $0.64 | $22.47 | $-8.78 | $8,672.42 | ▼ -8.78 after sell → book $10,064.00; vs 09:30 mark -22.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 227 | $5.88 | $2.98 | $-117.13 | $10,004.20 | ▼ -117.13 after sell → book $10,061.02; vs 09:30 mark -2.98 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 15 | $79.42 | $2.04 | — | $8,810.87 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1250.53 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $7,640.29 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1250.53 | — |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 68 | $18.15 | $2.19 | — | $6,403.90 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+14.1; leftover $1250.53 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $5,267.81 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1250.53 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 66 | $18.75 | $2.19 | — | $4,028.12 | — | combo gate; gate vol=good,ab=good; list yday_gainer; ret5=-5.0; leftover $1250.53 | — |
| 2026-08-28 09:30 ET | **BUY** | `CRDL` | 607 | $2.06 | $7.83 | — | $2,769.87 | — | combo gate; gate vol=good,ab=good; list yday_gainer; ret5=+9.3; leftover $1250.53 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 53 | $23.30 | $2.15 | — | $1,532.82 | — | combo gate; gate vol=good,ab=good; list ohlc_hot; 🔵; ret5=+14.5; leftover $1250.53 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 65 | $19.00 | $2.19 | — | $295.63 | — | combo gate; gate vol=good,ab=good; list ohlc_hot; ret5=+7.5; leftover $1250.53 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $295.63 | ▼ close $9,885.06 vs 09:30 $10,110.53 (session -153.35) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $295.63 | ▼ 09:30 equity $9,818.09 vs yday $9,885.06 (-66.97) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 107 | $0.51 | $0.89 | $-9.65 | $349.31 | ▼ -9.65 after sell → book $9,817.19; vs 09:30 mark -0.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $349.31 | ▲ close $9,819.38 vs 09:30 $9,818.09 (session +2.19) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $349.31 | ▼ 09:30 equity $9,658.47 vs yday $9,819.38 (-160.91) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $349.31 | ▲ close $9,805.42 vs 09:30 $9,658.47 (session +146.95) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $349.31 | ▼ 09:30 equity $9,784.79 vs yday $9,805.42 (-20.63) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 15 | $78.84 | $2.06 | $-12.79 | $1,529.85 | ▼ -12.79 after sell → book $9,782.73; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 8 | $139.65 | $2.03 | $-55.41 | $2,645.02 | ▼ -55.41 after sell → book $9,780.70; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 68 | $17.65 | $2.22 | $-38.41 | $3,843.01 | ▼ -38.41 after sell → book $9,778.49; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $133.00 | $2.03 | $-74.13 | $4,904.97 | ▼ -74.13 after sell → book $9,776.45; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBWI` | 66 | $18.41 | $2.21 | $-26.84 | $6,117.82 | ▼ -26.84 after sell → book $9,774.24; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRDL` | 607 | $2.16 | $7.94 | $+44.93 | $7,421.00 | ▲ +44.93 after sell → book $9,766.30; vs 09:30 mark -7.94 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NCNO` | 53 | $22.20 | $2.17 | $-62.62 | $8,595.43 | ▼ -62.62 after sell → book $9,764.13; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TH` | 65 | $17.98 | $2.21 | $-70.69 | $9,761.93 | ▼ -70.69 after sell → book $9,761.93; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,761.93 | ▲ close $9,761.93 vs 09:30 $9,784.79 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,761.93 | ▲ 09:30 equity $9,761.93 vs yday $9,761.93 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $8,567.86 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1220.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 78 | $15.45 | $2.22 | — | $7,360.54 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1220.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $6,190.96 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1220.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 72 | $16.77 | $2.21 | — | $4,981.32 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1220.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $55.42 | $2.06 | — | $3,760.02 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; ret5=-25.9; leftover $1220.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 559 | $2.18 | $7.21 | — | $2,534.19 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1220.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 51 | $23.88 | $2.14 | — | $1,314.17 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1220.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 117 | $10.42 | $2.34 | — | $92.68 | — | combo gate; gate vol=good,ab=good; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1220.24 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $92.68 | ▼ close $9,617.39 vs 09:30 $9,761.93 (session -122.32) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.68 | ▲ 09:30 equity $9,664.99 vs yday $9,617.39 (+47.60) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 3 | $3.46 | $0.11 | — | $82.19 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $11.59 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 4 | $2.52 | $0.11 | — | $72.00 | — | combo gate; gate vol=good,ab=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $11.59 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 1 | $6.71 | $0.07 | — | $65.22 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $11.59 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 6 | $1.90 | $0.13 | — | $53.69 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $11.59 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 2 | $4.78 | $0.10 | — | $44.02 | — | combo gate; gate vol=good,ab=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $11.59 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 1 | $11.31 | $0.12 | — | $32.60 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $11.59 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.60 | ▼ close $9,619.60 vs 09:30 $9,664.99 (session -44.75) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.60 | ▼ 09:30 equity $9,574.30 vs yday $9,619.60 (-45.30) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.60 | ▲ close $9,591.95 vs 09:30 $9,574.30 (session +17.65) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.60 | ▼ 09:30 equity $9,546.01 vs yday $9,591.95 (-45.94) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $1,162.49 | ▼ -64.17 after sell → book $9,543.97; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 78 | $15.16 | $2.25 | $-27.09 | $2,342.72 | ▼ -27.09 after sell → book $9,541.72; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 8 | $140.29 | $2.03 | $-49.25 | $3,463.05 | ▼ -49.25 after sell → book $9,539.69; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 72 | $15.46 | $2.23 | $-98.75 | $4,573.94 | ▼ -98.75 after sell → book $9,537.46; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `EIX` | 22 | $59.49 | $2.08 | $+85.41 | $5,880.65 | ▲ +85.41 after sell → book $9,535.39; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 559 | $2.22 | $7.31 | $+7.83 | $7,114.31 | ▲ +7.83 after sell → book $9,528.07; vs 09:30 mark -7.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 51 | $23.22 | $2.16 | $-37.97 | $8,296.37 | ▼ -37.97 after sell → book $9,525.91; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `NVAX` | 117 | $10.02 | $2.37 | $-51.51 | $9,466.34 | ▼ -51.51 after sell → book $9,523.54; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,466.34 | ▼ close $9,520.22 vs 09:30 $9,546.01 (session -3.32) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,466.34 | ▼ 09:30 equity $9,519.27 vs yday $9,520.22 (-0.95) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `CABA` | 3 | $2.85 | $0.11 | $-2.06 | $9,474.77 | ▼ -2.06 after sell → book $9,519.16; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 4 | $2.22 | $0.12 | $-1.43 | $9,483.53 | ▼ -1.43 after sell → book $9,519.04; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 1 | $6.11 | $0.08 | $-0.75 | $9,489.56 | ▼ -0.75 after sell → book $9,518.95; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 6 | $1.83 | $0.15 | $-0.70 | $9,500.39 | ▼ -0.70 after sell → book $9,518.81; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 2 | $3.92 | $0.10 | $-1.92 | $9,508.13 | ▼ -1.92 after sell → book $9,518.70; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 1 | $10.57 | $0.13 | $-0.98 | $9,518.57 | ▼ -0.98 after sell → book $9,518.57; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,518.57 | ▲ close $9,518.57 vs 09:30 $9,519.27 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,518.57 | ▲ 09:30 equity $9,518.57 vs yday $9,518.57 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $8,365.55 | — | combo gate; gate vol=good,ab=good; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1189.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 15 | $77.33 | $2.04 | — | $7,203.57 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; ret5=+2.5; leftover $1189.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 440 | $2.70 | $5.68 | — | $6,009.89 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1189.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 380 | $3.13 | $4.90 | — | $4,815.59 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+24.2; leftover $1189.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 108 | $10.95 | $2.31 | — | $3,630.67 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1189.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 242 | $4.91 | $3.12 | — | $2,439.33 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1189.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 14 | $84.27 | $2.03 | — | $1,257.52 | — | combo gate; gate vol=good,ab=good; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1189.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `ANGX` | 221 | $5.38 | $2.85 | — | $65.69 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=+19.8; leftover $1189.82 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $65.69 | ▲ close $9,536.39 vs 09:30 $9,518.57 (session +42.76) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $65.69 | ▲ 09:30 equity $9,542.32 vs yday $9,536.39 (+5.93) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $65.69 | ▲ close $9,736.43 vs 09:30 $9,542.32 (session +194.11) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $65.69 | ▲ 09:30 equity $9,812.66 vs yday $9,736.43 (+76.23) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $65.69 | ▲ close $9,972.39 vs 09:30 $9,812.66 (session +159.73) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $65.69 | ▼ 09:30 equity $9,875.47 vs yday $9,972.39 (-96.92) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 7 | $140.03 | $2.03 | $-174.84 | $1,043.87 | ▼ -174.84 after sell → book $9,873.44; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `VIST` | 15 | $76.75 | $2.06 | $-12.79 | $2,193.06 | ▼ -12.79 after sell → book $9,871.38; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `INDP` | 440 | $3.66 | $5.76 | $+410.96 | $3,797.70 | ▲ +410.96 after sell → book $9,865.62; vs 09:30 mark -5.76 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CMRC` | 380 | $3.48 | $4.98 | $+123.12 | $5,115.13 | ▲ +123.12 after sell → book $9,860.65; vs 09:30 mark -4.97 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `WLTH` | 108 | $10.82 | $2.34 | $-18.70 | $6,281.34 | ▼ -18.70 after sell → book $9,858.30; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BNC` | 242 | $4.77 | $3.17 | $-40.17 | $7,432.51 | ▼ -40.17 after sell → book $9,855.13; vs 09:30 mark -3.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ANGX` | 221 | $5.30 | $2.90 | $-23.43 | $8,600.91 | ▼ -23.43 after sell → book $9,852.23; vs 09:30 mark -2.90 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 15 | $77.12 | $2.04 | — | $7,442.08 | — | combo gate; gate vol=good,ab=good; list flatten,ohlc_hot; ret5=+7.2; leftover $1228.70 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 209 | $5.87 | $2.70 | — | $6,212.55 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1228.70 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 14 | $87.40 | $2.03 | — | $4,986.92 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1228.70 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 45 | $27.09 | $2.12 | — | $3,765.75 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1228.70 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 52 | $23.29 | $2.15 | — | $2,552.52 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=+16.1; leftover $1228.70 | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 37 | $33.14 | $2.10 | — | $1,324.24 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=-2.9; leftover $1228.70 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 43 | $28.16 | $2.12 | — | $111.24 | — | combo gate; gate vol=good,ab=good; list yday_gainer,ohlc_hot; ret5=+14.8; leftover $1228.70 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.24 | ▼ close $9,750.96 vs 09:30 $9,875.47 (session -86.02) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.24 | ▲ 09:30 equity $9,916.24 vs yday $9,750.96 (+165.28) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 14 | $86.76 | $2.05 | $+30.78 | $1,323.83 | ▲ +30.78 after sell → book $9,914.18; vs 09:30 mark -2.06 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 1 | $147.61 | $1.48 | — | $1,174.74 | — | combo gate; gate vol=good,ab=good; list flatten,ohlc_hot; ret5=+17.7; leftover $189.12 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 24 | $7.59 | $1.89 | — | $990.69 | — | combo gate; gate vol=good,ab=good; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $189.12 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 7 | $25.95 | $1.84 | — | $807.20 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $189.12 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 1 | $170.85 | $1.71 | — | $634.64 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $189.12 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 78 | $2.40 | $2.11 | — | $445.33 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $189.12 | — |
| 2026-09-17 09:30 ET | **BUY** | `CYPH` | 70 | $2.67 | $2.08 | — | $256.00 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=-0.4; leftover $189.12 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $256.00 | ▲ close $10,472.23 vs 09:30 $9,916.24 (session +569.16) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $256.00 | ▼ 09:30 equity $10,454.13 vs yday $10,472.23 (-18.10) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 2 | $14.79 | $0.30 | — | $226.12 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $42.67 | — |
| 2026-09-18 09:30 ET | **BUY** | `VITL` | 3 | $11.38 | $0.35 | — | $191.63 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+19.5; leftover $42.67 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $191.63 | ▼ close $10,414.04 vs 09:30 $10,454.13 (session -39.44) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $191.63 | ▲ 09:30 equity $10,504.68 vs yday $10,414.04 (+90.64) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 15 | $76.27 | $2.06 | $-16.84 | $1,333.62 | ▼ -16.84 after sell → book $10,502.62; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 209 | $5.62 | $2.74 | $-57.69 | $2,505.46 | ▼ -57.69 after sell → book $10,499.88; vs 09:30 mark -2.74 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `VAL` | 14 | $83.46 | $2.05 | $-59.24 | $3,671.85 | ▼ -59.24 after sell → book $10,497.83; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ADPT` | 45 | $28.69 | $2.15 | $+67.73 | $4,960.75 | ▲ +67.73 after sell → book $10,495.68; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 52 | $29.43 | $2.17 | $+314.97 | $6,488.95 | ▲ +314.97 after sell → book $10,493.52; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `FPS` | 37 | $40.03 | $2.12 | $+250.71 | $7,967.93 | ▲ +250.71 after sell → book $10,491.39; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `CAI` | 43 | $30.23 | $2.14 | $+84.75 | $9,265.68 | ▲ +84.75 after sell → book $10,489.25; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 7 | $157.87 | $2.01 | — | $8,158.58 | — | combo gate; gate vol=good,ab=good; list flatten; ret5=+6.5; leftover $1158.21 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 13 | $88.83 | $2.03 | — | $7,001.76 | — | combo gate; gate vol=good,ab=good; list flatten; ret5=+7.6; leftover $1158.21 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 124 | $9.31 | $2.36 | — | $5,844.96 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1158.21 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 85 | $13.47 | $2.25 | — | $4,697.34 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1158.21 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 88 | $13.05 | $2.25 | — | $3,546.69 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $1158.21 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 99 | $11.67 | $2.29 | — | $2,389.07 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+31.3; leftover $1158.21 | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 468 | $2.47 | $6.04 | — | $1,227.07 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+73.6; leftover $1158.21 | — |
| 2026-09-21 09:30 ET | **BUY** | `CAN` | 2770 | $0.42 | $19.89 | — | $49.32 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+10.7; leftover $1158.21 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.32 | ▲ close $10,526.61 vs 09:30 $10,504.68 (session +76.48) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.32 | ▼ 09:30 equity $10,482.19 vs yday $10,526.61 (-44.42) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `CYPH` | 70 | $3.51 | $2.22 | $+54.15 | $292.80 | ▲ +54.15 after sell → book $10,479.97; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 63 | $0.58 | $0.55 | — | $255.71 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $36.60 | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 4 | $9.11 | $0.38 | — | $218.89 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+44.4; leftover $36.60 | — |
| 2026-09-22 09:30 ET | **BUY** | `FSLY` | 1 | $28.02 | $0.28 | — | $190.59 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; leftover $36.60 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $190.59 | ▲ close $10,544.81 vs 09:30 $10,482.19 (session +66.05) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $190.59 | ▲ 09:30 equity $10,714.71 vs yday $10,544.81 (+169.90) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `RVTY` | 1 | $142.40 | $1.45 | $-8.14 | $331.54 | ▼ -8.14 after sell → book $10,713.27; vs 09:30 mark -1.44 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `PGEN` | 24 | $7.95 | $2.00 | $+4.75 | $520.34 | ▲ +4.75 after sell → book $10,711.27; vs 09:30 mark -2.00 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARQT` | 7 | $27.79 | $1.99 | $+9.06 | $712.89 | ▲ +9.06 after sell → book $10,709.28; vs 09:30 mark -1.99 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 1 | $174.50 | $1.77 | $+0.17 | $885.62 | ▲ +0.17 after sell → book $10,707.51; vs 09:30 mark -1.77 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SABR` | 78 | $2.24 | $2.00 | $-16.59 | $1,058.33 | ▼ -16.59 after sell → book $10,705.51; vs 09:30 mark -2.00 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RARE` | 2 | $15.40 | $0.33 | $+0.58 | $1,088.80 | ▲ +0.58 after sell → book $10,705.17; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `VITL` | 3 | $11.17 | $0.36 | $-1.34 | $1,121.95 | ▼ -1.34 after sell → book $10,704.81; vs 09:30 mark -0.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 10 | $20.65 | $2.02 | — | $913.43 | — | combo gate; gate vol=good,ab=good; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $224.39 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLLN` | 1 | $116.00 | $1.16 | — | $796.26 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $224.39 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 22 | $9.90 | $2.06 | — | $576.41 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $224.39 | — |
| 2026-09-23 09:30 ET | **BUY** | `INOD` | 3 | $70.84 | $2.00 | — | $361.89 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $224.39 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $361.89 | ▼ close $10,491.71 vs 09:30 $10,714.71 (session -205.86) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $361.89 | ▼ 09:30 equity $10,475.82 vs yday $10,491.71 (-15.89) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 7 | $163.95 | $2.03 | $+38.52 | $1,507.51 | ▲ +38.52 after sell → book $10,473.79; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 13 | $87.67 | $2.05 | $-19.09 | $2,645.23 | ▼ -19.09 after sell → book $10,471.74; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 124 | $8.67 | $2.39 | $-84.11 | $3,717.92 | ▼ -84.11 after sell → book $10,469.34; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 85 | $12.26 | $2.27 | $-107.79 | $4,757.75 | ▼ -107.79 after sell → book $10,467.08; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `USDE` | 88 | $12.76 | $2.28 | $-30.05 | $5,878.35 | ▼ -30.05 after sell → book $10,464.80; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SECZ` | 99 | $14.67 | $2.32 | $+292.40 | $7,328.37 | ▲ +292.40 after sell → book $10,462.48; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 468 | $2.68 | $6.12 | $+86.12 | $8,576.48 | ▲ +86.12 after sell → book $10,456.36; vs 09:30 mark -6.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `CAN` | 2770 | $0.38 | $19.36 | $-138.97 | $9,615.26 | ▼ -138.97 after sell → book $10,437.00; vs 09:30 mark -19.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,615.26 | ▲ close $10,456.97 vs 09:30 $10,475.82 (session +19.97) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,596.32 | ▲ 09:30 equity $9,490.58 vs yday $9,489.89 (+0.69) | 09:30 open · cash $8,596.32 (unchanged overnight, no fees) · equity $9,490.58 vs prior close $9,489.89 (+0.69) · 9 name(s) re-marked at the open (per-name table). BFLY×10 yday $9.41 → 09:30 $9.41 +0.00; DEFT×205 yday $0.53 → 09:30 $0.53 +0.00; FSLY×4 yday $26.68 → 09:30 $26.68 +0.00; GRAL×1 yday $125.21 → 09:30 $123.50 -1.71; INOD×1 yday $71.87 → 09:30 $71.87 +0.00; NEOG×8 yday $13.66 → 09:30 $13.66 +0.00; OMER×5 yday $20.13 → 09:30 $20.61 +2.40; RXRX×30 yday $3.89 → 09:30 $3.89 +0.00; TTAN×1 yday $59.98 → 09:30 $59.98 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 46 | $26.27 | $2.13 | — | $7,385.77 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1228.05 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 68 | $17.91 | $2.19 | — | $6,165.70 | — | combo gate; gate vol=good,ab=good; list probable; 🔵; ret5=+3.7; leftover $1228.05 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 14 | $83.69 | $2.03 | — | $4,991.94 | — | combo gate; gate vol=good,ab=good; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $1228.05 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GLND` | 202 | $6.06 | $2.61 | — | $3,765.21 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+342.1; leftover $1228.05 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 318 | $3.86 | $4.10 | — | $2,533.63 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1228.05 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DNA` | 120 | $10.20 | $2.35 | — | $1,307.28 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1228.05 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 6 | $184.00 | $2.01 | — | $201.27 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $1228.05 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $201.27 | ▼ close $9,397.61 vs 09:30 $9,490.58 (session -75.55) | 16:00 close · cash $201.27 · equity $9,397.61 vs 09:30 $9,490.58 (-92.97; session marks -75.55) · 16 name(s) marked open→close (per-name table). BFLY×10 09:30 $9.41 → close $9.41 -0.00; DEFT×205 09:30 $0.53 → close $0.53 +0.00; FSLY×4 09:30 $26.68 → close $26.68 +0.00; GRAL×1 09:30 $123.50 → close $126.89 +3.39; INOD×1 09:30 $71.87 → close $71.87 +0.00; NEOG×8 09:30 $13.66 → close $13.66 -0.00; OMER×5 09:30 $20.61 → close $20.08 -2.65; RXRX×30 09:30 $3.89 → close $3.89 +0.00; TTAN×1 09:30 $59.98 → close $59.98 -0.00; WRBY×46 09:30 $26.27 → close $26.71 +20.24; PL×68 09:30 $17.91 → close $17.43 -32.64; TEM×14 09:30 $83.69 → close $85.01 +18.41; GLND×202 09:30 $6.06 → close $5.54 -105.04; ZSQR×318 09:30 $3.86 → close $3.78 -25.44; DNA×120 09:30 $10.20 → close $10.66 +55.20; TWST×6 09:30 $184.00 → close $182.83 -7.02 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 23.36 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 23.36 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 23.36 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `QSI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZIP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DKS` | cash | leftover split 62.61 < 1 share @ 121.87 |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZIP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DKS` | cash | leftover split 61.90 < 1 share @ 128.73 |
| 2026-08-28 | `SLQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NCNO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NCNO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NVAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 11.59 < 1 share @ 513.78 |
| 2026-09-04 | `MLYS` | cash | leftover split 11.59 < 1 share @ 28.00 |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `EIX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `NVAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BTBT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SID` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `VIST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CMRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `WLTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `VIST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CMRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `WLTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RPD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `VAL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ADPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SDGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 189.12 < 1 share @ 233.85 |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `VAL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `FPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `GNRC` | cash | leftover split 42.67 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 42.67 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 42.67 < 1 share @ 85.00 |
| 2026-09-18 | `TEM` | cash | leftover split 42.67 < 1 share @ 81.40 |
| 2026-09-21 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `ARQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `VITL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `RVTY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ARQT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SABR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `VITL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SECZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `ARM` | cash | leftover split 36.60 < 1 share @ 319.41 |
| 2026-09-22 | `BRVE` | no_price | no 09:30 open |
| 2026-09-22 | `AMRX` | no_price | no 09:30 open |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SECZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `CRML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `FSLY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `VICR` | cash | leftover split 224.39 < 1 share @ 266.50 |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `CRML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BLLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BFLY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INOD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EGHT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SRFM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DEFT` | 63 | 2026-09-22 @ $0.58 | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $36.60 |
| `CRML` | 4 | 2026-09-22 @ $9.11 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+44.4; leftover $36.60 |
| `FSLY` | 1 | 2026-09-22 @ $28.02 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; leftover $36.60 |
| `OMER` | 10 | 2026-09-23 @ $20.65 | combo gate; gate vol=good,ab=good; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $224.39 |
| `BLLN` | 1 | 2026-09-23 @ $116.00 | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $224.39 |
| `BFLY` | 22 | 2026-09-23 @ $9.90 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $224.39 |
| `INOD` | 3 | 2026-09-23 @ $70.84 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $224.39 |
