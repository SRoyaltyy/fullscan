# Factor mine action — `union_blue_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-16.99%** ($8,301) · signal-only (no cash/fees) was -23.64%. Starts YES **0/30**. Fills 248 · skips 79 · realized $-1931.95.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the name is painted 🔵 (a turn higher on a still-red row).
- Must-have: prior 5-session return is at most 10% (not already exploded).
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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `blue=True,ret_5_max=10.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,068.04.

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
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,544.55 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $4,332.68 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 138 | $9.01 | $2.40 | — | $3,086.90 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1334 | $0.94 | $16.50 | — | $1,820.44 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $560.20 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $560.20 | ▲ close $10,051.46 vs 09:30 $10,000.00 (session +91.20) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $560.20 | ▲ 09:30 equity $10,054.84 vs yday $10,051.46 (+3.38) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,661.82 | ▲ +20.13 after sell → book $10,052.82; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $2,854.74 | ▲ +15.71 after sell → book $10,050.79; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $4,126.70 | ▲ +69.94 after sell → book $10,048.75; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $5,135.50 | ▲ +14.07 after sell → book $10,046.73; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 21 | $55.37 | $2.07 | $-51.17 | $6,296.20 | ▼ -51.17 after sell → book $10,044.66; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 138 | $9.22 | $2.44 | $+24.14 | $7,566.12 | ▲ +24.14 after sell → book $10,042.22; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1334 | $0.91 | $16.33 | $-72.85 | $8,759.73 | ▼ -72.85 after sell → book $10,025.89; vs 09:30 mark -16.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 833 | $1.52 | $10.89 | $-4.98 | $10,014.99 | ▼ -4.98 after sell → book $10,014.99; vs 09:30 mark -10.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,766.06 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+6.7; leftover $1251.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,621.89 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+5.8; leftover $1251.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,403.68 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+8.3; leftover $1251.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 309 | $4.05 | $3.99 | — | $5,148.25 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1251.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 147 | $8.46 | $2.43 | — | $3,902.19 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1251.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 137 | $9.12 | $2.40 | — | $2,650.35 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1251.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 77 | $16.20 | $2.22 | — | $1,400.73 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1251.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 772 | $1.62 | $9.96 | — | $140.13 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $1251.87 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $140.13 | ▼ close $9,863.96 vs 09:30 $10,054.84 (session -123.94) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $140.13 | ▼ 09:30 equity $9,755.43 vs yday $9,863.96 (-108.53) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,434.04 | ▲ +44.98 after sell → book $9,753.34; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,616.33 | ▲ +38.11 after sell → book $9,751.31; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,867.88 | ▲ +33.34 after sell → book $9,749.28; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 309 | $3.72 | $4.05 | $-110.00 | $5,013.31 | ▼ -110.00 after sell → book $9,745.23; vs 09:30 mark -4.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 147 | $8.55 | $2.47 | $+8.33 | $6,267.70 | ▲ +8.33 after sell → book $9,742.77; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 137 | $9.03 | $2.43 | $-17.16 | $7,502.37 | ▼ -17.16 after sell → book $9,740.33; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 77 | $15.78 | $2.24 | $-36.80 | $8,715.19 | ▼ -36.80 after sell → book $9,738.09; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INV` | 772 | $1.32 | $10.10 | $-247.80 | $9,727.99 | ▼ -247.80 after sell → book $9,727.99; vs 09:30 mark -10.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,727.99 | ▲ close $9,727.99 vs 09:30 $9,755.43 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,727.99 | ▲ 09:30 equity $9,727.99 vs yday $9,727.99 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,727.99 | ▲ close $9,727.99 vs 09:30 $9,727.99 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,727.99 | ▲ 09:30 equity $9,727.99 vs yday $9,727.99 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 59 | $20.55 | $2.17 | — | $8,513.38 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1216.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,328.22 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1216.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 210 | $5.77 | $2.71 | — | $6,113.81 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1216.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $4,914.20 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1216.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $3,697.26 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1216.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 694 | $1.75 | $8.95 | — | $2,473.81 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1216.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,315.48 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1216.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 247 | $4.92 | $3.19 | — | $97.05 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1216.00 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.05 | ▲ close $9,867.97 vs 09:30 $9,727.99 (session +165.32) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.05 | ▲ 09:30 equity $10,198.81 vs yday $9,867.97 (+330.84) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 59 | $21.90 | $2.19 | $+75.30 | $1,386.96 | ▲ +75.30 after sell → book $10,196.62; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,629.27 | ▲ +57.15 after sell → book $10,194.57; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 210 | $5.67 | $2.75 | $-26.46 | $3,817.22 | ▼ -26.46 after sell → book $10,191.82; vs 09:30 mark -2.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 61 | $21.17 | $2.19 | $+89.57 | $5,106.40 | ▲ +89.57 after sell → book $10,189.63; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $6,423.23 | ▲ +99.89 after sell → book $10,187.49; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 694 | $1.79 | $9.08 | $+9.73 | $7,656.41 | ▲ +9.73 after sell → book $10,178.41; vs 09:30 mark -9.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $8,891.98 | ▲ +77.23 after sell → book $10,176.38; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 247 | $5.20 | $3.24 | $+62.74 | $10,173.14 | ▲ +62.74 after sell → book $10,173.14; vs 09:30 mark -3.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $8,916.97 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1271.64 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $7,647.97 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1271.64 | — |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 38 | $33.36 | $2.10 | — | $6,378.18 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $1271.64 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 766 | $1.66 | $9.88 | — | $5,096.74 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1271.64 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $3,848.22 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1271.64 | — |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 9 | $127.43 | $2.02 | — | $2,699.34 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable; 🔵; ⚪; ret5=+7.9; leftover $1271.64 | — |
| 2026-08-21 09:30 ET | **BUY** | `WOLF` | 47 | $26.86 | $2.13 | — | $1,434.79 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_mover; 🔵; ret5=-16.4; leftover $1271.64 | — |
| 2026-08-21 09:30 ET | **BUY** | `AMRC` | 56 | $22.51 | $2.16 | — | $172.07 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_mover; 🔵; ret5=-20.2; leftover $1271.64 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $172.07 | ▼ close $10,094.03 vs 09:30 $10,198.81 (session -54.75) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $172.07 | ▼ 09:30 equity $10,008.91 vs yday $10,094.03 (-85.12) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.75 | $2.07 | $-24.50 | $1,403.75 | ▼ -24.50 after sell → book $10,006.84; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $121.00 | $2.04 | $+59.95 | $2,732.70 | ▲ +59.95 after sell → book $10,004.79; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 38 | $32.82 | $2.12 | $-24.75 | $3,977.74 | ▼ -24.75 after sell → book $10,002.67; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 766 | $1.55 | $10.02 | $-104.16 | $5,155.02 | ▼ -104.16 after sell → book $9,992.65; vs 09:30 mark -10.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $6,459.08 | ▲ +55.55 after sell → book $9,990.63; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 9 | $129.99 | $2.04 | $+18.99 | $7,626.96 | ▲ +18.99 after sell → book $9,988.60; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WOLF` | 47 | $25.00 | $2.15 | $-91.70 | $8,799.81 | ▼ -91.70 after sell → book $9,986.45; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AMRC` | 56 | $21.19 | $2.18 | $-78.26 | $9,984.27 | ▼ -78.26 after sell → book $9,984.27; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,984.27 | ▲ close $9,984.27 vs 09:30 $10,008.91 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,984.27 | ▲ 09:30 equity $9,984.27 vs yday $9,984.27 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 113 | $10.98 | $2.33 | — | $8,741.20 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+1.2; leftover $1248.03 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 20 | $61.19 | $2.05 | — | $7,515.35 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+7.4; leftover $1248.03 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 149 | $8.35 | $2.44 | — | $6,268.76 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1248.03 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 172 | $7.25 | $2.51 | — | $5,019.26 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1248.03 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 91 | $13.59 | $2.26 | — | $3,780.30 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1248.03 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 131 | $9.49 | $2.38 | — | $2,534.73 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1248.03 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 33 | $36.96 | $2.09 | — | $1,312.96 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1248.03 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 274 | $4.55 | $3.53 | — | $62.73 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1248.03 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $62.73 | ▲ close $10,210.24 vs 09:30 $9,984.27 (session +245.56) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.73 | ▼ 09:30 equity $10,169.46 vs yday $10,210.24 (-40.78) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `INSP` | 20 | $60.07 | $2.07 | $-26.52 | $1,262.06 | ▼ -26.52 after sell → book $10,167.39; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 172 | $8.29 | $2.55 | $+173.83 | $2,685.39 | ▲ +173.83 after sell → book $10,164.84; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 91 | $13.63 | $2.29 | $-0.91 | $3,923.43 | ▼ -0.91 after sell → book $10,162.55; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 131 | $9.89 | $2.42 | $+47.60 | $5,216.61 | ▲ +47.60 after sell → book $10,160.14; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 33 | $38.24 | $2.11 | $+38.04 | $6,476.42 | ▲ +38.04 after sell → book $10,158.03; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 274 | $4.31 | $3.59 | $-72.88 | $7,653.77 | ▼ -72.88 after sell → book $10,154.44; vs 09:30 mark -3.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `RZLT` | 254 | $5.01 | $3.28 | — | $6,377.95 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,yday_gainer; 🔵; ret5=+7.5; leftover $1275.63 | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 40 | $31.21 | $2.11 | — | $5,127.44 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1275.63 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 114 | $11.12 | $2.33 | — | $3,857.43 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1275.63 | — |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 129 | $9.83 | $2.38 | — | $2,586.98 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1275.63 | — |
| 2026-08-26 09:30 ET | **BUY** | `AVEX` | 72 | $17.51 | $2.21 | — | $1,324.06 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $1275.63 | — |
| 2026-08-26 09:30 ET | **BUY** | `AXTI` | 19 | $65.34 | $2.05 | — | $80.55 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1275.63 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $80.55 | ▲ close $10,157.07 vs 09:30 $10,169.46 (session +16.98) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.55 | ▲ 09:30 equity $10,290.79 vs yday $10,157.07 (+133.72) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 113 | $10.63 | $2.36 | $-44.24 | $1,279.38 | ▼ -44.24 after sell → book $10,288.43; vs 09:30 mark -2.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 149 | $8.49 | $2.47 | $+15.95 | $2,541.92 | ▲ +15.95 after sell → book $10,285.96; vs 09:30 mark -2.47 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 254 | $5.07 | $3.33 | $+8.63 | $3,826.37 | ▲ +8.63 after sell → book $10,282.63; vs 09:30 mark -3.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 40 | $30.79 | $2.13 | $-21.04 | $5,055.84 | ▼ -21.04 after sell → book $10,280.50; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ABX` | 129 | $9.68 | $2.41 | $-24.14 | $6,302.15 | ▼ -24.14 after sell → book $10,278.09; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVEX` | 72 | $18.43 | $2.23 | $+61.81 | $7,626.88 | ▲ +61.81 after sell → book $10,275.86; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BE` | 5 | $227.10 | $2.00 | — | $6,489.38 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.6; leftover $1271.15 | — |
| 2026-08-27 09:30 ET | **BUY** | `NVTS` | 96 | $13.18 | $2.28 | — | $5,221.82 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=-1.2; leftover $1271.15 | — |
| 2026-08-27 09:30 ET | **BUY** | `AAOI` | 10 | $117.03 | $2.02 | — | $4,049.50 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=-6.9; leftover $1271.15 | — |
| 2026-08-27 09:30 ET | **BUY** | `GRRR` | 79 | $15.94 | $2.23 | — | $2,788.01 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_mover; 🔵; ret5=+5.9; leftover $1271.15 | — |
| 2026-08-27 09:30 ET | **BUY** | `NCNO` | 57 | $22.03 | $2.16 | — | $1,530.14 | — | combo gate; gate blue=True,ret_5_max=10.0; list ohlc_hot,earn_react; 🔵; ret5=+4.0; leftover $1271.15 | — |
| 2026-08-27 09:30 ET | **BUY** | `NABL` | 328 | $3.87 | $4.23 | — | $256.55 | — | combo gate; gate blue=True,ret_5_max=10.0; list ohlc_hot; 🔵; ret5=+9.8; leftover $1271.15 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $256.55 | ▼ close $10,115.94 vs 09:30 $10,290.79 (session -145.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $256.55 | ▲ 09:30 equity $10,118.45 vs yday $10,115.94 (+2.51) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 114 | $11.27 | $2.36 | $+12.41 | $1,538.97 | ▲ +12.41 after sell → book $10,116.08; vs 09:30 mark -2.37 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 19 | $65.29 | $2.07 | $-5.06 | $2,777.41 | ▼ -5.06 after sell → book $10,114.02; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BE` | 5 | $215.71 | $2.02 | $-61.01 | $3,853.91 | ▼ -61.01 after sell → book $10,111.99; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVTS` | 96 | $12.44 | $2.30 | $-75.62 | $5,045.85 | ▼ -75.62 after sell → book $10,109.69; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AAOI` | 10 | $110.46 | $2.04 | $-69.76 | $6,148.41 | ▼ -69.76 after sell → book $10,107.65; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NCNO` | 57 | $23.30 | $2.18 | $+68.05 | $7,474.33 | ▲ +68.05 after sell → book $10,105.47; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NABL` | 328 | $4.25 | $4.30 | $+116.11 | $8,864.03 | ▲ +116.11 after sell → book $10,101.17; vs 09:30 mark -4.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 38 | $32.90 | $2.10 | — | $7,611.73 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1266.29 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 15 | $79.42 | $2.04 | — | $6,418.39 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1266.29 | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $122.81 | $2.02 | — | $5,188.27 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1266.29 | — |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 262 | $4.82 | $3.38 | — | $3,922.05 | — | combo gate; gate blue=True,ret_5_max=10.0; list ohlc_hot; 🔵; ret5=+8.8; leftover $1266.29 | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $2,946.82 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1266.29 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 13 | $91.49 | $2.03 | — | $1,755.42 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1266.29 | — |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 20 | $62.82 | $2.05 | — | $496.97 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1266.29 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $496.97 | ▼ close $9,808.07 vs 09:30 $10,118.45 (session -277.48) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $496.97 | ▲ 09:30 equity $9,815.29 vs yday $9,808.07 (+7.22) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 79 | $14.44 | $2.25 | $-122.98 | $1,635.48 | ▼ -122.98 after sell → book $9,813.04; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 38 | $31.15 | $2.12 | $-70.73 | $2,817.06 | ▼ -70.73 after sell → book $9,810.92; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 15 | $80.44 | $2.06 | $+11.21 | $4,021.60 | ▲ +11.21 after sell → book $9,808.86; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 10 | $118.83 | $2.04 | $-43.86 | $5,207.86 | ▼ -43.86 after sell → book $9,806.82; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 262 | $4.81 | $3.43 | $-9.43 | $6,464.65 | ▼ -9.43 after sell → book $9,803.39; vs 09:30 mark -3.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 3 | $322.49 | $2.02 | $-9.78 | $7,430.10 | ▼ -9.78 after sell → book $9,801.37; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 13 | $89.39 | $2.05 | $-31.38 | $8,590.12 | ▼ -31.38 after sell → book $9,799.32; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 20 | $60.46 | $2.07 | $-51.32 | $9,797.25 | ▼ -51.32 after sell → book $9,797.25; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,797.25 | ▲ close $9,797.25 vs 09:30 $9,815.29 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,797.25 | ▲ 09:30 equity $9,797.25 vs yday $9,797.25 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,797.25 | ▲ close $9,797.25 vs 09:30 $9,797.25 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,797.25 | ▲ 09:30 equity $9,797.25 vs yday $9,797.25 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,797.25 | ▲ close $9,797.25 vs 09:30 $9,797.25 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,797.25 | ▲ 09:30 equity $9,797.25 vs yday $9,797.25 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $8,578.95 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1224.66 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 28 | $42.93 | $2.07 | — | $7,374.84 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1224.66 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 337 | $3.63 | $4.35 | — | $6,147.18 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1224.66 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 152 | $8.03 | $2.45 | — | $4,924.18 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1224.66 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $3,730.11 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1224.66 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 79 | $15.45 | $2.23 | — | $2,507.33 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1224.66 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $1,337.76 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1224.66 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 73 | $16.77 | $2.21 | — | $111.34 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1224.66 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.34 | ▼ close $9,559.28 vs 09:30 $9,797.25 (session -218.58) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.34 | ▲ 09:30 equity $9,562.13 vs yday $9,559.28 (+2.85) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 152 | $7.91 | $2.48 | $-23.17 | $1,311.18 | ▼ -23.17 after sell → book $9,559.65; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $2,479.41 | ▼ -25.83 after sell → book $9,557.61; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 79 | $15.00 | $2.25 | $-40.03 | $3,662.16 | ▼ -40.03 after sell → book $9,555.36; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $153.62 | $2.03 | $+57.35 | $4,889.09 | ▲ +57.35 after sell → book $9,553.33; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 73 | $15.61 | $2.23 | $-89.12 | $6,026.39 | ▼ -89.12 after sell → book $9,551.10; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 478 | $2.52 | $6.17 | — | $4,815.66 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1205.28 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 179 | $6.71 | $2.53 | — | $3,612.04 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1205.28 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 252 | $4.78 | $3.25 | — | $2,404.23 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1205.28 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 106 | $11.31 | $2.31 | — | $1,203.06 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1205.28 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $147.62 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1205.28 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $147.62 | ▼ close $9,369.98 vs 09:30 $9,562.13 (session -164.86) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $147.62 | ▼ 09:30 equity $9,335.73 vs yday $9,369.98 (-34.25) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 23 | $54.31 | $2.08 | $+28.75 | $1,394.67 | ▲ +28.75 after sell → book $9,333.65; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HRMY` | 28 | $42.20 | $2.09 | $-24.61 | $2,574.18 | ▼ -24.61 after sell → book $9,331.56; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 337 | $3.43 | $4.41 | $-76.16 | $3,725.68 | ▼ -76.16 after sell → book $9,327.15; vs 09:30 mark -4.41 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 478 | $2.38 | $6.26 | $-79.34 | $4,857.06 | ▼ -79.34 after sell → book $9,320.89; vs 09:30 mark -6.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 179 | $6.57 | $2.57 | $-30.15 | $6,030.52 | ▼ -30.15 after sell → book $9,318.32; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 252 | $4.30 | $3.30 | $-127.51 | $7,110.82 | ▼ -127.51 after sell → book $9,315.02; vs 09:30 mark -3.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 106 | $11.22 | $2.34 | $-14.18 | $8,297.81 | ▼ -14.18 after sell → book $9,312.69; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $9,310.66 | ▼ -42.58 after sell → book $9,310.66; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,310.66 | ▲ close $9,310.66 vs 09:30 $9,335.73 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,310.66 | ▲ 09:30 equity $9,310.66 vs yday $9,310.66 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,310.66 | ▲ close $9,310.66 vs 09:30 $9,310.66 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,310.66 | ▲ 09:30 equity $9,310.66 vs yday $9,310.66 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,310.66 | ▲ close $9,310.66 vs 09:30 $9,310.66 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,310.66 | ▲ 09:30 equity $9,310.66 vs yday $9,310.66 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 71 | $16.28 | $2.20 | — | $8,152.58 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=-1.1; leftover $1163.83 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 426 | $2.73 | $5.50 | — | $6,984.11 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=-3.0; leftover $1163.83 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 7 | $157.78 | $2.01 | — | $5,877.63 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+4.7; leftover $1163.83 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 570 | $2.04 | $7.35 | — | $4,707.48 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1163.83 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 245 | $4.75 | $3.16 | — | $3,540.57 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1163.83 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 548 | $2.12 | $7.07 | — | $2,371.74 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1163.83 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 100 | $11.55 | $2.29 | — | $1,214.45 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1163.83 | — |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 7 | $157.55 | $2.01 | — | $109.59 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $1163.83 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $109.59 | ▼ close $9,259.12 vs 09:30 $9,310.66 (session -19.95) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.59 | ▼ 09:30 equity $9,197.35 vs yday $9,259.12 (-61.77) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AUPH` | 71 | $16.03 | $2.22 | $-22.18 | $1,245.50 | ▼ -22.18 after sell → book $9,195.13; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `OVID` | 426 | $2.75 | $5.58 | $-0.42 | $2,413.55 | ▼ -0.42 after sell → book $9,189.55; vs 09:30 mark -5.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `NVT` | 7 | $150.00 | $2.03 | $-58.50 | $3,461.52 | ▼ -58.50 after sell → book $9,187.52; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 570 | $2.01 | $7.46 | $-31.91 | $4,599.76 | ▼ -31.91 after sell → book $9,180.06; vs 09:30 mark -7.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 245 | $4.82 | $3.21 | $+10.78 | $5,777.45 | ▲ +10.78 after sell → book $9,176.85; vs 09:30 mark -3.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 548 | $2.05 | $7.17 | $-52.60 | $6,893.68 | ▼ -52.60 after sell → book $9,169.68; vs 09:30 mark -7.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `FUBO` | 100 | $11.56 | $2.32 | $-3.61 | $8,047.36 | ▼ -3.61 after sell → book $9,167.36; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RDDT` | 7 | $160.00 | $2.03 | $+13.11 | $9,165.33 | ▲ +13.11 after sell → book $9,165.33; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,165.33 | ▲ close $9,165.33 vs 09:30 $9,197.35 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,165.33 | ▲ 09:30 equity $9,165.33 vs yday $9,165.33 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,165.33 | ▲ close $9,165.33 vs 09:30 $9,165.33 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,165.33 | ▲ 09:30 equity $9,165.33 vs yday $9,165.33 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 31 | $36.46 | $2.08 | — | $8,032.99 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+2.9; leftover $1145.67 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 16 | $68.79 | $2.04 | — | $6,930.31 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1145.67 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 195 | $5.87 | $2.58 | — | $5,783.09 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1145.67 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 13 | $87.40 | $2.03 | — | $4,644.86 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1145.67 | — |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 30 | $38.01 | $2.08 | — | $3,502.48 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $1145.67 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 42 | $27.09 | $2.12 | — | $2,362.58 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1145.67 | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 34 | $33.14 | $2.09 | — | $1,233.73 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=-2.9; leftover $1145.67 | — |
| 2026-09-16 09:30 ET | **BUY** | `MRCY` | 13 | $87.52 | $2.03 | — | $93.94 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=+4.3; leftover $1145.67 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $93.94 | ▼ close $9,075.08 vs 09:30 $9,165.33 (session -73.21) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.94 | ▲ 09:30 equity $9,296.32 vs yday $9,075.08 (+221.24) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `BLFS` | 31 | $36.67 | $2.10 | $+2.32 | $1,228.61 | ▲ +2.32 after sell → book $9,294.22; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 16 | $72.70 | $2.06 | $+58.46 | $2,389.75 | ▲ +58.46 after sell → book $9,292.16; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 195 | $5.58 | $2.62 | $-61.74 | $3,475.23 | ▼ -61.74 after sell → book $9,289.54; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 13 | $83.20 | $2.05 | $-58.68 | $4,554.78 | ▼ -58.68 after sell → book $9,287.49; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `KRMN` | 30 | $37.89 | $2.10 | $-7.78 | $5,689.38 | ▼ -7.78 after sell → book $9,285.39; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 42 | $28.23 | $2.14 | $+43.63 | $6,872.91 | ▲ +43.63 after sell → book $9,283.26; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `FPS` | 34 | $36.76 | $2.11 | $+118.88 | $8,120.63 | ▲ +118.88 after sell → book $9,281.14; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `MRCY` | 13 | $89.27 | $2.05 | $+18.67 | $9,279.10 | ▲ +18.67 after sell → book $9,279.10; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 152 | $7.59 | $2.45 | — | $8,122.97 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1159.89 | — |
| 2026-09-17 09:30 ET | **BUY** | `AXTI` | 17 | $67.91 | $2.04 | — | $6,966.46 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.7; leftover $1159.89 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 44 | $25.95 | $2.12 | — | $5,822.54 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1159.89 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 6 | $170.85 | $2.01 | — | $4,795.43 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1159.89 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 483 | $2.40 | $6.23 | — | $3,630.00 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1159.89 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 64 | $18.04 | $2.18 | — | $2,473.58 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $1159.89 | — |
| 2026-09-17 09:30 ET | **BUY** | `AIB` | 794 | $1.46 | $10.24 | — | $1,304.09 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=+4.4; leftover $1159.89 | — |
| 2026-09-17 09:30 ET | **BUY** | `CYPH` | 433 | $2.67 | $5.59 | — | $140.23 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=-0.4; leftover $1159.89 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $140.23 | ▲ close $9,367.23 vs 09:30 $9,296.32 (session +120.99) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $140.23 | ▲ 09:30 equity $9,461.54 vs yday $9,367.23 (+94.31) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 152 | $7.98 | $2.48 | $+54.35 | $1,350.71 | ▲ +54.35 after sell → book $9,459.06; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AXTI` | 17 | $69.72 | $2.06 | $+26.67 | $2,533.89 | ▲ +26.67 after sell → book $9,457.00; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 44 | $26.14 | $2.14 | $+4.10 | $3,681.91 | ▲ +4.10 after sell → book $9,454.85; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 6 | $182.33 | $2.03 | $+64.84 | $4,773.86 | ▲ +64.84 after sell → book $9,452.83; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 483 | $2.29 | $6.32 | $-65.68 | $5,873.61 | ▼ -65.68 after sell → book $9,446.50; vs 09:30 mark -6.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 64 | $17.80 | $2.20 | $-19.42 | $7,010.61 | ▼ -19.42 after sell → book $9,444.30; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AIB` | 794 | $1.41 | $10.38 | $-60.33 | $8,119.76 | ▼ -60.33 after sell → book $9,433.92; vs 09:30 mark -10.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CYPH` | 433 | $3.04 | $5.67 | $+144.63 | $9,428.25 | ▲ +144.63 after sell → book $9,428.25; vs 09:30 mark -5.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 1214 | $0.97 | $15.42 | — | $8,235.25 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1178.53 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 298 | $3.95 | $3.84 | — | $7,054.31 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1178.53 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 83 | $14.07 | $2.24 | — | $5,884.26 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1178.53 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 202 | $5.83 | $2.61 | — | $4,703.99 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1178.53 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 329 | $3.58 | $4.24 | — | $3,521.93 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1178.53 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 79 | $14.79 | $2.23 | — | $2,351.29 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1178.53 | — |
| 2026-09-18 09:30 ET | **BUY** | `DCX` | 3329 | $0.35 | $21.77 | — | $1,151.05 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer,yday_mover; 🔵; ret5=-19.7; leftover $1178.53 | — |
| 2026-09-18 09:30 ET | **BUY** | `SHLS` | 150 | $7.64 | $2.44 | — | $2.61 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=+7.6; leftover $1178.53 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.61 | ▼ close $8,516.39 vs 09:30 $9,461.54 (session -857.07) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.61 | ▲ 09:30 equity $8,744.86 vs yday $8,516.39 (+228.47) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `TLSA` | 1214 | $0.94 | $15.27 | $-67.10 | $1,128.51 | ▼ -67.10 after sell → book $8,729.60; vs 09:30 mark -15.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 298 | $3.87 | $3.90 | $-31.59 | $2,277.86 | ▼ -31.59 after sell → book $8,725.69; vs 09:30 mark -3.91 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 83 | $13.90 | $2.26 | $-18.61 | $3,429.30 | ▼ -18.61 after sell → book $8,723.43; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BNC` | 202 | $6.42 | $2.65 | $+112.91 | $4,722.48 | ▲ +112.91 after sell → book $8,720.78; vs 09:30 mark -2.65 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DDD` | 329 | $3.71 | $4.31 | $+34.22 | $5,938.76 | ▲ +34.22 after sell → book $8,716.47; vs 09:30 mark -4.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 79 | $14.58 | $2.25 | $-21.07 | $7,088.33 | ▼ -21.07 after sell → book $8,714.22; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DCX` | 3329 | $0.14 | $15.24 | $-746.09 | $7,542.48 | ▼ -746.09 after sell → book $8,698.98; vs 09:30 mark -15.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SHLS` | 150 | $7.71 | $2.47 | $+5.59 | $8,696.50 | ▲ +5.59 after sell → book $8,696.50; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 108 | $9.99 | $2.31 | — | $7,615.27 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1087.06 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 595 | $1.82 | $7.68 | — | $6,521.72 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1087.06 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 41 | $25.95 | $2.11 | — | $5,455.66 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1087.06 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 505 | $2.15 | $6.51 | — | $4,363.39 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $1087.06 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTGO` | 138 | $7.85 | $2.40 | — | $3,277.69 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=-1.2; leftover $1087.06 | — |
| 2026-09-21 09:30 ET | **BUY** | `ASST` | 34 | $31.64 | $2.09 | — | $2,199.84 | — | combo gate; gate blue=True,ret_5_max=10.0; list ohlc_hot; 🔵; ret5=+8.9; leftover $1087.06 | — |
| 2026-09-21 09:30 ET | **BUY** | `TRMD` | 29 | $37.47 | $2.08 | — | $1,111.13 | — | combo gate; gate blue=True,ret_5_max=10.0; list ohlc_hot; 🔵; ret5=+9.0; leftover $1087.06 | — |
| 2026-09-21 09:30 ET | **BUY** | `TK` | 75 | $14.44 | $2.21 | — | $25.91 | — | combo gate; gate blue=True,ret_5_max=10.0; list ohlc_hot; 🔵; ret5=+6.8; leftover $1087.06 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.91 | ▼ close $8,580.35 vs 09:30 $8,744.86 (session -88.74) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.91 | ▼ 09:30 equity $8,516.00 vs yday $8,580.35 (-64.35) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 108 | $9.91 | $2.34 | $-13.30 | $1,093.85 | ▼ -13.30 after sell → book $8,513.66; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `BTBT` | 595 | $1.79 | $7.78 | $-33.31 | $2,154.09 | ▼ -33.31 after sell → book $8,505.87; vs 09:30 mark -7.79 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `BTGO` | 138 | $7.81 | $2.44 | $-10.36 | $3,229.44 | ▼ -10.36 after sell → book $8,503.44; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `ASST` | 34 | $29.30 | $2.11 | $-83.76 | $4,223.52 | ▼ -83.76 after sell → book $8,501.32; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,223.52 | ▲ close $8,501.32 vs 09:30 $8,516.00 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,223.52 | ▼ 09:30 equity $8,412.32 vs yday $8,501.32 (-89.00) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 41 | $26.58 | $2.13 | $+21.58 | $5,311.17 | ▲ +21.58 after sell → book $8,410.19; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMTX` | 505 | $2.09 | $6.61 | $-43.42 | $6,360.01 | ▼ -43.42 after sell → book $8,403.58; vs 09:30 mark -6.61 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TRMD` | 29 | $34.83 | $2.10 | $-80.73 | $7,367.99 | ▼ -80.73 after sell → book $8,401.49; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TK` | 75 | $13.78 | $2.24 | $-53.95 | $8,399.25 | ▼ -53.95 after sell → book $8,399.25; vs 09:30 mark -2.24 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 11 | $89.50 | $2.02 | — | $7,412.72 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1049.91 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 37 | $27.79 | $2.10 | — | $6,382.39 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1049.91 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 107 | $9.81 | $2.31 | — | $5,330.41 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1049.91 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 50 | $20.65 | $2.14 | — | $4,295.77 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1049.91 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 267 | $3.93 | $3.44 | — | $3,243.02 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1049.91 | — |
| 2026-09-23 09:30 ET | **BUY** | `MAZE` | 37 | $28.30 | $2.10 | — | $2,193.82 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $1049.91 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 66 | $15.72 | $2.19 | — | $1,154.11 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1049.91 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 41 | $25.40 | $2.11 | — | $110.60 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $1049.91 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.60 | ▼ close $8,135.92 vs 09:30 $8,412.32 (session -244.91) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $110.60 | ▼ 09:30 equity $8,086.66 vs yday $8,135.92 (-49.26) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 11 | $87.67 | $2.04 | $-24.14 | $1,072.98 | ▼ -24.14 after sell → book $8,084.62; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 37 | $26.22 | $2.12 | $-62.31 | $2,041.00 | ▼ -62.31 after sell → book $8,082.50; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 107 | $9.67 | $2.34 | $-19.63 | $3,073.35 | ▼ -19.63 after sell → book $8,080.16; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 50 | $20.52 | $2.16 | $-10.80 | $4,097.19 | ▼ -10.80 after sell → book $8,078.00; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INDP` | 267 | $3.77 | $3.50 | $-49.66 | $5,100.28 | ▼ -49.66 after sell → book $8,074.50; vs 09:30 mark -3.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `MAZE` | 37 | $28.15 | $2.12 | $-9.77 | $6,139.71 | ▼ -9.77 after sell → book $8,072.38; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 66 | $14.38 | $2.21 | $-92.84 | $7,086.58 | ▼ -92.84 after sell → book $8,070.17; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `TNGX` | 41 | $23.99 | $2.13 | $-62.06 | $8,068.04 | ▼ -62.06 after sell → book $8,068.04; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,068.04 | ▲ close $8,068.04 vs 09:30 $8,086.66 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,439.05 | ▲ 09:30 equity $8,439.05 vs yday $8,439.05 (+0.00) | 09:30 open · cash $8,439.05 · no holdings · equity $8,439.05 vs prior close $8,439.05 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 51 | $20.61 | $2.14 | — | $7,385.80 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+9.1; leftover $1054.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 137 | $7.65 | $2.40 | — | $6,335.35 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1054.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 40 | $26.27 | $2.11 | — | $5,282.44 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1054.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $4,275.29 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1054.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BRVE` | 44 | $23.58 | $2.12 | — | $3,235.65 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $1054.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 479 | $2.20 | $6.18 | — | $2,175.67 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1054.88 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 175 | $6.00 | $2.52 | — | $1,123.15 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $1054.88 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 58 | $17.91 | $2.16 | — | $82.21 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable; 🔵; ret5=+3.7; leftover $1054.88 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.21 | ▼ close $8,300.97 vs 09:30 $8,439.05 (session -116.42) | 16:00 close · cash $82.21 · equity $8,300.97 vs 09:30 $8,439.05 (-138.08; session marks -116.42) · 8 name(s) marked open→close (per-name table). OMER×51 09:30 $20.61 → close $20.08 -27.03; MRVI×137 09:30 $7.65 → close $7.60 -6.85; WRBY×40 09:30 $26.27 → close $26.71 +17.60; TXG×12 09:30 $83.76 → close $85.71 +23.40; BRVE×44 09:30 $23.58 → close $20.62 -130.24; HLP×479 09:30 $2.20 → close $2.21 +4.79; SATL×175 09:30 $6.00 → close $6.17 +29.75; PL×58 09:30 $17.91 → close $17.43 -27.84 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BJ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ZJYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RCKT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OHI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BMRN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AIAI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `PHM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OBE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `WDS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `KOS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LAC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FJET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BRZE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DHT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FRO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `KGS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VLO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IVVD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBLX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMTX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TRMD` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TK` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `LU` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `KVYO` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new buys |
