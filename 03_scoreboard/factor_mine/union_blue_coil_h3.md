# Factor mine action — `union_blue_coil_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-23.40%** ($7,660) · signal-only (no cash/fees) was -38.70%. Starts YES **1/30**. Fills 194 · skips 286 · realized $-1292.46.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `blue=True,ret_5_max=10.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,539.17.

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
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 1 | $46.18 | $0.46 | — | $513.55 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+6.7; leftover $70.02 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 17 | $4.05 | $0.74 | — | $443.96 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=-12.3; leftover $70.02 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 8 | $8.46 | $0.70 | — | $375.58 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+0.4; leftover $70.02 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 7 | $9.12 | $0.66 | — | $311.08 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $70.02 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 4 | $16.20 | $0.66 | — | $245.62 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $70.02 | — |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 43 | $1.62 | $0.83 | — | $175.14 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $70.02 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $175.14 | ▲ close $10,053.14 vs 09:30 $10,054.84 (session +2.34) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $175.14 | ▼ 09:30 equity $9,868.62 vs yday $10,053.14 (-184.52) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $175.14 | ▼ close $9,553.63 vs 09:30 $9,868.62 (session -314.99) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $175.14 | ▲ 09:30 equity $9,594.08 vs yday $9,553.63 (+40.45) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `TLN` | 3 | $321.00 | $2.02 | $-120.51 | $1,136.12 | ▼ -120.51 after sell → book $9,592.06; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VST` | 8 | $140.74 | $2.03 | $-53.33 | $2,260.00 | ▼ -53.33 after sell → book $9,590.02; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NRG` | 10 | $116.20 | $2.04 | $-42.06 | $3,419.96 | ▼ -42.06 after sell → book $9,587.98; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DAVE` | 3 | $334.00 | $2.02 | $+5.25 | $4,419.94 | ▲ +5.25 after sell → book $9,585.96; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SLG` | 21 | $57.50 | $2.07 | $-6.44 | $5,625.37 | ▼ -6.44 after sell → book $9,583.89; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 138 | $8.91 | $2.44 | $-18.64 | $6,852.51 | ▼ -18.64 after sell → book $9,581.45; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 1334 | $0.88 | $15.97 | $-108.51 | $8,010.46 | ▼ -108.51 after sell → book $9,565.48; vs 09:30 mark -15.97 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 833 | $1.42 | $10.89 | $-88.28 | $9,182.43 | ▼ -88.28 after sell → book $9,554.59; vs 09:30 mark -10.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,182.43 | ▲ close $9,560.86 vs 09:30 $9,594.08 (session +6.27) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,182.43 | ▼ 09:30 equity $9,558.69 vs yday $9,560.86 (-2.17) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 1 | $49.02 | $0.51 | $+1.86 | $9,230.93 | ▲ +1.86 after sell → book $9,558.17; vs 09:30 mark -0.52 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 17 | $3.92 | $0.74 | $-3.69 | $9,296.84 | ▼ -3.69 after sell → book $9,557.44; vs 09:30 mark -0.73 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 8 | $8.35 | $0.71 | $-2.29 | $9,362.92 | ▼ -2.29 after sell → book $9,556.72; vs 09:30 mark -0.72 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ABX` | 7 | $9.13 | $0.68 | $-1.27 | $9,426.15 | ▼ -1.27 after sell → book $9,556.04; vs 09:30 mark -0.68 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ALM` | 4 | $15.81 | $0.66 | $-2.88 | $9,488.73 | ▼ -2.88 after sell → book $9,555.38; vs 09:30 mark -0.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `INV` | 43 | $1.55 | $0.82 | $-4.65 | $9,554.56 | ▼ -4.65 after sell → book $9,554.56; vs 09:30 mark -0.82 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,360.50 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1194.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,175.34 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1194.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 206 | $5.77 | $2.66 | — | $5,984.06 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1194.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $4,804.09 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1194.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $3,616.78 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1194.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 682 | $1.75 | $8.80 | — | $2,414.49 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1194.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,256.15 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1194.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 242 | $4.92 | $3.12 | — | $62.39 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1194.32 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $62.39 | ▲ close $9,693.06 vs 09:30 $9,558.69 (session +163.56) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.39 | ▲ 09:30 equity $10,018.75 vs yday $9,693.06 (+325.69) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 4 | $1.66 | $0.08 | — | $55.67 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $7.80 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.67 | ▲ close $10,062.92 vs 09:30 $10,018.75 (session +44.25) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.67 | ▲ 09:30 equity $10,136.52 vs yday $10,062.92 (+73.60) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.67 | ▼ close $10,132.58 vs 09:30 $10,136.52 (session -3.94) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.67 | ▼ 09:30 equity $10,009.41 vs yday $10,132.58 (-123.17) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 58 | $20.32 | $2.18 | $-17.69 | $1,232.05 | ▼ -17.69 after sell → book $10,007.23; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.86 | $2.05 | $+58.97 | $2,476.18 | ▲ +58.97 after sell → book $10,005.18; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 206 | $5.53 | $2.70 | $-54.80 | $3,612.66 | ▼ -54.80 after sell → book $10,002.48; vs 09:30 mark -2.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 60 | $21.21 | $2.19 | $+90.44 | $4,883.07 | ▲ +90.44 after sell → book $10,000.29; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 40 | $32.32 | $2.13 | $+103.36 | $6,173.74 | ▲ +103.36 after sell → book $9,998.16; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 682 | $1.90 | $8.92 | $+84.58 | $7,460.62 | ▲ +84.58 after sell → book $9,989.24; vs 09:30 mark -8.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $8,710.66 | ▲ +91.71 after sell → book $9,987.20; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 242 | $5.25 | $3.17 | $+73.57 | $9,977.99 | ▲ +73.57 after sell → book $9,984.03; vs 09:30 mark -3.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 113 | $10.98 | $2.33 | — | $8,734.92 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+1.2; leftover $1247.25 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 20 | $61.19 | $2.05 | — | $7,509.07 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+7.4; leftover $1247.25 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 149 | $8.35 | $2.44 | — | $6,262.48 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1247.25 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 172 | $7.25 | $2.51 | — | $5,012.98 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1247.25 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 91 | $13.59 | $2.26 | — | $3,774.02 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1247.25 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 131 | $9.49 | $2.38 | — | $2,528.45 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1247.25 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 33 | $36.96 | $2.09 | — | $1,306.68 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1247.25 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 274 | $4.55 | $3.53 | — | $56.45 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1247.25 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.45 | ▲ close $10,210.28 vs 09:30 $10,009.41 (session +245.84) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.45 | ▼ 09:30 equity $10,169.30 vs yday $10,210.28 (-40.98) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `BTBT` | 4 | $1.53 | $0.09 | $-0.69 | $62.47 | ▼ -0.69 after sell → book $10,169.20; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `RZLT` | 2 | $5.01 | $0.11 | — | $52.35 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,yday_gainer; 🔵; ret5=+7.5; leftover $10.41 | — |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 1 | $9.83 | $0.10 | — | $42.42 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $10.41 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.42 | ▲ close $10,351.90 vs 09:30 $10,169.30 (session +182.90) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.42 | ▼ 09:30 equity $10,298.43 vs yday $10,351.90 (-53.47) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `NABL` | 1 | $3.87 | $0.04 | — | $38.51 | — | combo gate; gate blue=True,ret_5_max=10.0; list ohlc_hot; 🔵; ret5=+9.8; leftover $5.30 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.51 | ▲ close $10,412.95 vs 09:30 $10,298.43 (session +114.56) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.51 | ▼ 09:30 equity $10,338.42 vs yday $10,412.95 (-74.53) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 113 | $10.97 | $2.36 | $-5.82 | $1,275.76 | ▼ -5.82 after sell → book $10,336.06; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 20 | $60.52 | $2.07 | $-17.52 | $2,484.09 | ▼ -17.52 after sell → book $10,333.99; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 149 | $8.28 | $2.47 | $-15.34 | $3,715.34 | ▼ -15.34 after sell → book $10,331.52; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 172 | $9.73 | $2.55 | $+421.51 | $5,386.35 | ▲ +421.51 after sell → book $10,328.97; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 91 | $13.05 | $2.29 | $-53.69 | $6,571.61 | ▼ -53.69 after sell → book $10,326.68; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CCOI` | 131 | $9.70 | $2.41 | $+22.71 | $7,839.90 | ▲ +22.71 after sell → book $10,324.27; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 33 | $39.60 | $2.11 | $+82.92 | $9,144.59 | ▲ +82.92 after sell → book $10,322.16; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZIP` | 274 | $4.21 | $3.59 | $-100.28 | $10,294.54 | ▼ -100.28 after sell → book $10,318.57; vs 09:30 mark -3.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $32.90 | $2.11 | — | $9,009.33 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1286.82 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 82 | $15.66 | $2.24 | — | $7,722.97 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1286.82 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $79.42 | $2.04 | — | $6,450.21 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1286.82 | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $122.81 | $2.02 | — | $5,220.09 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1286.82 | — |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 266 | $4.82 | $3.43 | — | $3,934.54 | — | combo gate; gate blue=True,ret_5_max=10.0; list ohlc_hot; 🔵; ret5=+8.8; leftover $1286.82 | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $2,959.31 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1286.82 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.49 | $2.03 | — | $1,676.42 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1286.82 | — |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 20 | $62.82 | $2.05 | — | $417.97 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1286.82 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $417.97 | ▼ close $10,015.76 vs 09:30 $10,338.42 (session -284.89) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $417.97 | ▲ 09:30 equity $10,022.99 vs yday $10,015.76 (+7.23) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `RZLT` | 2 | $4.65 | $0.12 | $-0.95 | $427.15 | ▼ -0.95 after sell → book $10,022.87; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ABX` | 1 | $9.74 | $0.12 | $-0.31 | $436.77 | ▼ -0.31 after sell → book $10,022.75; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $436.77 | ▲ close $10,133.33 vs 09:30 $10,022.99 (session +110.58) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $436.77 | ▼ 09:30 equity $9,993.60 vs yday $10,133.33 (-139.73) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `NABL` | 1 | $3.93 | $0.06 | $-0.04 | $440.64 | ▼ -0.04 after sell → book $9,993.54; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $440.64 | ▼ close $9,951.46 vs 09:30 $9,993.60 (session -42.08) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $440.64 | ▼ 09:30 equity $9,921.26 vs yday $9,951.46 (-30.20) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 39 | $32.42 | $2.13 | $-22.95 | $1,702.89 | ▼ -22.95 after sell → book $9,919.13; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 82 | $13.92 | $2.26 | $-147.18 | $2,842.07 | ▼ -147.18 after sell → book $9,916.87; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 16 | $78.84 | $2.06 | $-13.38 | $4,101.46 | ▼ -13.38 after sell → book $9,914.82; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TTMI` | 10 | $114.22 | $2.04 | $-89.96 | $5,241.62 | ▼ -89.96 after sell → book $9,912.78; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TLS` | 266 | $4.73 | $3.49 | $-30.86 | $6,496.31 | ▼ -30.86 after sell → book $9,909.29; vs 09:30 mark -3.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 3 | $318.04 | $2.02 | $-23.13 | $7,448.41 | ▼ -23.13 after sell → book $9,907.27; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `AVT` | 14 | $89.39 | $2.05 | $-33.48 | $8,697.82 | ▼ -33.48 after sell → book $9,905.22; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CGNX` | 20 | $60.37 | $2.07 | $-53.12 | $9,903.15 | ▼ -53.12 after sell → book $9,903.15; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,903.15 | ▲ close $9,903.15 vs 09:30 $9,921.26 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,903.15 | ▲ 09:30 equity $9,903.15 vs yday $9,903.15 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $8,684.85 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1237.89 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 28 | $42.93 | $2.07 | — | $7,480.74 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1237.89 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 341 | $3.63 | $4.40 | — | $6,238.51 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1237.89 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 154 | $8.03 | $2.45 | — | $4,999.44 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1237.89 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $3,805.37 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1237.89 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 80 | $15.45 | $2.23 | — | $2,567.14 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1237.89 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $1,397.56 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1237.89 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 73 | $16.77 | $2.21 | — | $171.15 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1237.89 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $171.15 | ▼ close $9,663.92 vs 09:30 $9,903.15 (session -219.78) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $171.15 | ▲ 09:30 equity $9,666.60 vs yday $9,663.92 (+2.68) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 13 | $2.52 | $0.37 | — | $138.02 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $34.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 5 | $6.71 | $0.35 | — | $104.12 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $34.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 7 | $4.78 | $0.36 | — | $70.30 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $34.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 3 | $11.31 | $0.35 | — | $36.02 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $34.23 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.02 | ▲ close $9,691.34 vs 09:30 $9,666.60 (session +26.16) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.02 | ▲ 09:30 equity $9,720.38 vs yday $9,691.34 (+29.04) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.02 | ▼ close $9,556.79 vs 09:30 $9,720.38 (session -163.59) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.02 | ▼ 09:30 equity $9,509.27 vs yday $9,556.79 (-47.52) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 23 | $53.16 | $2.08 | $+2.30 | $1,256.63 | ▲ +2.30 after sell → book $9,507.20; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 28 | $42.01 | $2.09 | $-29.93 | $2,430.81 | ▼ -29.93 after sell → book $9,505.10; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 341 | $3.28 | $4.47 | $-128.21 | $3,544.83 | ▼ -128.21 after sell → book $9,500.64; vs 09:30 mark -4.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 154 | $8.01 | $2.49 | $-8.02 | $4,775.88 | ▼ -8.02 after sell → book $9,498.15; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $5,905.77 | ▼ -64.17 after sell → book $9,496.11; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 80 | $15.16 | $2.25 | $-27.68 | $7,116.32 | ▼ -27.68 after sell → book $9,493.86; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 8 | $140.29 | $2.03 | $-49.25 | $8,236.64 | ▼ -49.25 after sell → book $9,491.82; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 73 | $15.46 | $2.23 | $-100.07 | $9,362.99 | ▼ -100.07 after sell → book $9,489.59; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,362.99 | ▼ close $9,483.84 vs 09:30 $9,509.27 (session -5.76) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,362.99 | ▼ 09:30 equity $9,481.57 vs yday $9,483.84 (-2.27) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 13 | $2.22 | $0.35 | $-4.61 | $9,391.50 | ▼ -4.61 after sell → book $9,481.22; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 5 | $6.11 | $0.34 | $-3.69 | $9,421.71 | ▼ -3.69 after sell → book $9,480.88; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 7 | $3.92 | $0.32 | $-6.68 | $9,448.85 | ▼ -6.68 after sell → book $9,480.56; vs 09:30 mark -0.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 3 | $10.57 | $0.35 | $-2.91 | $9,480.22 | ▼ -2.91 after sell → book $9,480.22; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,480.22 | ▲ close $9,480.22 vs 09:30 $9,481.57 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,480.22 | ▲ 09:30 equity $9,480.22 vs yday $9,480.22 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 72 | $16.28 | $2.21 | — | $8,305.85 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=-1.1; leftover $1185.03 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 434 | $2.73 | $5.60 | — | $7,115.43 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=-3.0; leftover $1185.03 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 7 | $157.78 | $2.01 | — | $6,008.96 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+4.7; leftover $1185.03 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 580 | $2.04 | $7.48 | — | $4,818.28 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1185.03 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 249 | $4.75 | $3.21 | — | $3,632.32 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1185.03 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 558 | $2.12 | $7.20 | — | $2,442.16 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1185.03 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 102 | $11.55 | $2.30 | — | $1,261.76 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1185.03 | — |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 7 | $157.55 | $2.01 | — | $156.90 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $1185.03 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $156.90 | ▼ close $9,427.29 vs 09:30 $9,480.22 (session -20.91) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $156.90 | ▼ 09:30 equity $9,365.73 vs yday $9,427.29 (-61.56) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $156.90 | ▲ close $9,423.94 vs 09:30 $9,365.73 (session +58.21) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $156.90 | ▼ 09:30 equity $9,381.96 vs yday $9,423.94 (-41.98) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $156.90 | ▼ close $9,192.17 vs 09:30 $9,381.96 (session -189.79) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $156.90 | ▼ 09:30 equity $9,056.96 vs yday $9,192.17 (-135.21) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AUPH` | 72 | $16.16 | $2.23 | $-13.07 | $1,318.19 | ▼ -13.07 after sell → book $9,054.73; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `OVID` | 434 | $2.72 | $5.68 | $-15.62 | $2,492.99 | ▼ -15.62 after sell → book $9,049.05; vs 09:30 mark -5.68 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 7 | $147.79 | $2.03 | $-73.97 | $3,525.49 | ▼ -73.97 after sell → book $9,047.02; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 580 | $1.89 | $7.59 | $-102.07 | $4,614.10 | ▼ -102.07 after sell → book $9,039.43; vs 09:30 mark -7.59 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 249 | $4.73 | $3.26 | $-11.46 | $5,788.61 | ▼ -11.46 after sell → book $9,036.17; vs 09:30 mark -3.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAK` | 558 | $1.84 | $7.30 | $-170.74 | $6,808.03 | ▼ -170.74 after sell → book $9,028.87; vs 09:30 mark -7.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `FUBO` | 102 | $10.75 | $2.32 | $-86.22 | $7,902.21 | ▼ -86.22 after sell → book $9,026.55; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RDDT` | 7 | $160.62 | $2.03 | $+17.45 | $9,024.52 | ▲ +17.45 after sell → book $9,024.52; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 30 | $36.46 | $2.08 | — | $7,928.64 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+2.9; leftover $1128.06 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 16 | $68.79 | $2.04 | — | $6,825.96 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1128.06 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 192 | $5.87 | $2.57 | — | $5,696.35 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1128.06 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 12 | $87.40 | $2.03 | — | $4,645.53 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1128.06 | — |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 29 | $38.01 | $2.08 | — | $3,541.16 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $1128.06 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 41 | $27.09 | $2.11 | — | $2,428.36 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1128.06 | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 34 | $33.14 | $2.09 | — | $1,299.50 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=-2.9; leftover $1128.06 | — |
| 2026-09-16 09:30 ET | **BUY** | `MRCY` | 12 | $87.52 | $2.03 | — | $247.24 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=+4.3; leftover $1128.06 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $247.24 | ▼ close $8,941.27 vs 09:30 $9,056.96 (session -66.23) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $247.24 | ▲ 09:30 equity $9,157.62 vs yday $8,941.27 (+216.35) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 4 | $7.59 | $0.32 | — | $216.56 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $30.90 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 1 | $25.95 | $0.26 | — | $190.35 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $30.90 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 12 | $2.40 | $0.32 | — | $161.23 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $30.90 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 1 | $18.04 | $0.18 | — | $143.01 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $30.90 | — |
| 2026-09-17 09:30 ET | **BUY** | `AIB` | 21 | $1.46 | $0.37 | — | $111.98 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=+4.4; leftover $30.90 | — |
| 2026-09-17 09:30 ET | **BUY** | `CYPH` | 11 | $2.67 | $0.33 | — | $82.23 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=-0.4; leftover $30.90 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.23 | ▲ close $9,255.70 vs 09:30 $9,157.62 (session +99.87) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.23 | ▲ 09:30 equity $9,368.18 vs yday $9,255.70 (+112.48) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 10 | $0.97 | $0.13 | — | $72.40 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $10.28 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 2 | $3.95 | $0.09 | — | $64.41 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $10.28 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 1 | $5.83 | $0.06 | — | $58.52 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $10.28 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 2 | $3.58 | $0.08 | — | $51.28 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $10.28 | — |
| 2026-09-18 09:30 ET | **BUY** | `DCX` | 29 | $0.35 | $0.19 | — | $40.83 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer,yday_mover; 🔵; ret5=-19.7; leftover $10.28 | — |
| 2026-09-18 09:30 ET | **BUY** | `SHLS` | 1 | $7.64 | $0.08 | — | $33.11 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=+7.6; leftover $10.28 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.11 | ▼ close $9,255.05 vs 09:30 $9,368.18 (session -112.52) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.11 | ▲ 09:30 equity $9,328.14 vs yday $9,255.05 (+73.09) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 30 | $36.70 | $2.10 | $+3.02 | $1,132.01 | ▲ +3.02 after sell → book $9,326.04; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 16 | $79.08 | $2.06 | $+160.54 | $2,395.23 | ▲ +160.54 after sell → book $9,323.98; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 192 | $5.62 | $2.61 | $-53.17 | $3,471.66 | ▼ -53.17 after sell → book $9,321.37; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `VAL` | 12 | $83.46 | $2.05 | $-51.35 | $4,471.14 | ▼ -51.35 after sell → book $9,319.33; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `KRMN` | 29 | $36.30 | $2.10 | $-53.76 | $5,521.74 | ▼ -53.76 after sell → book $9,317.23; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ADPT` | 41 | $28.69 | $2.13 | $+61.35 | $6,695.90 | ▲ +61.35 after sell → book $9,315.10; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `FPS` | 34 | $40.03 | $2.11 | $+230.06 | $8,054.80 | ▲ +230.06 after sell → book $9,312.98; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `MRCY` | 12 | $86.52 | $2.05 | $-16.07 | $9,091.00 | ▼ -16.07 after sell → book $9,310.94; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 113 | $9.99 | $2.33 | — | $7,959.80 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1136.37 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 622 | $1.82 | $8.02 | — | $6,816.63 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1136.37 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 43 | $25.95 | $2.12 | — | $5,698.66 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1136.37 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 528 | $2.15 | $6.81 | — | $4,556.65 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $1136.37 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTGO` | 144 | $7.85 | $2.42 | — | $3,423.82 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=-1.2; leftover $1136.37 | — |
| 2026-09-21 09:30 ET | **BUY** | `ASST` | 35 | $31.64 | $2.10 | — | $2,314.33 | — | combo gate; gate blue=True,ret_5_max=10.0; list ohlc_hot; 🔵; ret5=+8.9; leftover $1136.37 | — |
| 2026-09-21 09:30 ET | **BUY** | `TRMD` | 30 | $37.47 | $2.08 | — | $1,188.15 | — | combo gate; gate blue=True,ret_5_max=10.0; list ohlc_hot; 🔵; ret5=+9.0; leftover $1136.37 | — |
| 2026-09-21 09:30 ET | **BUY** | `TK` | 78 | $14.44 | $2.22 | — | $59.60 | — | combo gate; gate blue=True,ret_5_max=10.0; list ohlc_hot; 🔵; ret5=+6.8; leftover $1136.37 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.60 | ▼ close $9,183.95 vs 09:30 $9,328.14 (session -98.89) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.60 | ▼ 09:30 equity $9,117.47 vs yday $9,183.95 (-66.48) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `CIFR` | 1 | $18.51 | $0.21 | $+0.08 | $77.91 | ▲ +0.08 after sell → book $9,117.26; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `CYPH` | 11 | $3.51 | $0.44 | $+8.42 | $116.08 | ▲ +8.42 after sell → book $9,116.82; vs 09:30 mark -0.44 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.08 | ▲ close $9,262.71 vs 09:30 $9,117.47 (session +145.89) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.08 | ▼ 09:30 equity $9,103.17 vs yday $9,262.71 (-159.54) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `PGEN` | 4 | $7.95 | $0.35 | $+0.77 | $147.53 | ▲ +0.77 after sell → book $9,102.82; vs 09:30 mark -0.35 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SABR` | 12 | $2.24 | $0.32 | $-2.57 | $174.08 | ▼ -2.57 after sell → book $9,102.49; vs 09:30 mark -0.33 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `AIB` | 21 | $1.37 | $0.37 | $-2.63 | $202.48 | ▼ -2.63 after sell → book $9,102.12; vs 09:30 mark -0.37 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TLSA` | 10 | $0.89 | $0.14 | $-1.07 | $211.24 | ▼ -1.07 after sell → book $9,101.98; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `EYPT` | 2 | $4.10 | $0.11 | $+0.11 | $219.34 | ▲ +0.11 after sell → book $9,101.88; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BNC` | 1 | $6.29 | $0.09 | $+0.31 | $225.54 | ▲ +0.31 after sell → book $9,101.79; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `DDD` | 2 | $3.59 | $0.10 | $-0.16 | $232.62 | ▼ -0.16 after sell → book $9,101.69; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `DCX` | 29 | $0.09 | $0.13 | $-8.01 | $235.07 | ▼ -8.01 after sell → book $9,101.56; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SHLS` | 1 | $8.15 | $0.10 | $+0.33 | $243.12 | ▲ +0.33 after sell → book $9,101.46; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 3 | $9.81 | $0.30 | — | $213.38 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+4.0; leftover $34.73 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 1 | $20.65 | $0.21 | — | $192.52 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $34.73 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 8 | $3.93 | $0.34 | — | $160.74 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $34.73 | — |
| 2026-09-23 09:30 ET | **BUY** | `MAZE` | 1 | $28.30 | $0.29 | — | $132.16 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $34.73 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 2 | $15.72 | $0.32 | — | $100.40 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $34.73 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 1 | $25.40 | $0.26 | — | $74.74 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $34.73 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $74.74 | ▼ close $8,852.63 vs 09:30 $9,103.17 (session -247.11) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.74 | ▼ 09:30 equity $8,728.51 vs yday $8,852.63 (-124.12) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 1 | $26.22 | $0.29 | $-0.28 | $100.68 | ▼ -0.28 after sell → book $8,728.22; vs 09:30 mark -0.29 | dropped from list after 5 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SBET` | 113 | $9.80 | $2.36 | $-26.16 | $1,205.72 | ▼ -26.16 after sell → book $8,725.86; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTBT` | 622 | $1.73 | $8.14 | $-78.36 | $2,270.53 | ▼ -78.36 after sell → book $8,717.73; vs 09:30 mark -8.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GLXY` | 43 | $25.00 | $2.14 | $-45.32 | $3,343.18 | ▼ -45.32 after sell → book $8,715.59; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMTX` | 528 | $1.88 | $6.91 | $-156.28 | $4,328.91 | ▼ -156.28 after sell → book $8,708.68; vs 09:30 mark -6.91 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTGO` | 144 | $7.92 | $2.46 | $+5.20 | $5,466.93 | ▲ +5.20 after sell → book $8,706.22; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ASST` | 35 | $28.52 | $2.12 | $-113.41 | $6,463.02 | ▼ -113.41 after sell → book $8,704.11; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `TRMD` | 30 | $34.25 | $2.10 | $-100.78 | $7,488.42 | ▼ -100.78 after sell → book $8,702.01; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `TK` | 78 | $13.50 | $2.25 | $-77.79 | $8,539.17 | ▼ -77.79 after sell → book $8,699.76; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,539.17 | ▼ close $8,699.10 vs 09:30 $8,728.51 (session -0.66) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,357.13 | ▲ 09:30 equity $7,784.71 vs yday $7,784.71 (-0.00) | 09:30 open · cash $7,357.13 (unchanged overnight, no fees) · equity $7,784.71 vs prior close $7,784.71 (-0.00) · 5 name(s) re-marked at the open (per-name table). ARHS×43 yday $9.47 → 09:30 $9.47 +0.00; CMPX×5 yday $1.13 → 09:30 $1.13 +0.00; GT×1 yday $5.07 → 09:30 $5.07 +0.00; INDP×1 yday $4.00 → 09:30 $4.00 +0.00; NMRA×8 yday $0.70 → 09:30 $0.70 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 44 | $20.61 | $2.12 | — | $6,448.17 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+9.1; leftover $919.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 120 | $7.65 | $2.35 | — | $5,527.82 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+5.2; leftover $919.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 35 | $26.27 | $2.10 | — | $4,606.27 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $919.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 10 | $83.76 | $2.02 | — | $3,766.65 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $919.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BRVE` | 39 | $23.58 | $2.11 | — | $2,844.93 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $919.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 418 | $2.20 | $5.39 | — | $1,919.93 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $919.64 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 153 | $6.00 | $2.45 | — | $999.48 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $919.64 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 51 | $17.91 | $2.14 | — | $83.93 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable; 🔵; ret5=+3.7; leftover $919.64 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.93 | ▼ close $7,659.88 vs 09:30 $7,784.71 (session -104.15) | 16:00 close · cash $83.93 · equity $7,659.88 vs 09:30 $7,784.71 (-124.83; session marks -104.15) · 13 name(s) marked open→close (per-name table). ARHS×43 09:30 $9.47 → close $9.47 +0.00; CMPX×5 09:30 $1.14 → close $1.14 -0.00; GT×1 09:30 $5.07 → close $5.07 +0.00; INDP×1 09:30 $4.00 → close $4.00 +0.00; NMRA×8 09:30 $0.70 → close $0.70 +0.00; OMER×44 09:30 $20.61 → close $20.08 -23.32; MRVI×120 09:30 $7.65 → close $7.60 -6.00; WRBY×35 09:30 $26.27 → close $26.71 +15.40; TXG×10 09:30 $83.76 → close $85.71 +19.50; BRVE×39 09:30 $23.58 → close $20.62 -115.44; HLP×418 09:30 $2.20 → close $2.21 +4.18; SATL×153 09:30 $6.00 → close $6.17 +26.01; PL×51 09:30 $17.91 → close $17.43 -24.48 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `TLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DAVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SLG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `EOG` | cash | leftover split 70.02 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 70.02 < 1 share @ 202.70 |
| 2026-08-18 | `TLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DAVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SLG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ABX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `INV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ALM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `INV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CRSP` | cash | leftover split 7.80 < 1 share @ 59.72 |
| 2026-08-21 | `FUTU` | cash | leftover split 7.80 < 1 share @ 115.18 |
| 2026-08-21 | `GMAB` | cash | leftover split 7.80 < 1 share @ 33.36 |
| 2026-08-21 | `DE` | cash | leftover split 7.80 < 1 share @ 623.26 |
| 2026-08-21 | `CF` | cash | leftover split 7.80 < 1 share @ 127.43 |
| 2026-08-21 | `WOLF` | cash | leftover split 7.80 < 1 share @ 26.86 |
| 2026-08-21 | `AMRC` | cash | leftover split 7.80 < 1 share @ 22.51 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BJ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `INSP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CCOI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZIP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AVBP` | cash | leftover split 10.41 < 1 share @ 31.21 |
| 2026-08-26 | `FLNC` | cash | leftover split 10.41 < 1 share @ 11.12 |
| 2026-08-26 | `AVEX` | cash | leftover split 10.41 < 1 share @ 17.51 |
| 2026-08-26 | `AXTI` | cash | leftover split 10.41 < 1 share @ 65.34 |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CCOI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZIP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ABX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `FLNC` | cash | leftover split 5.30 < 1 share @ 11.52 |
| 2026-08-27 | `BE` | cash | leftover split 5.30 < 1 share @ 227.10 |
| 2026-08-27 | `AXTI` | cash | leftover split 5.30 < 1 share @ 70.30 |
| 2026-08-27 | `NVTS` | cash | leftover split 5.30 < 1 share @ 13.18 |
| 2026-08-27 | `AAOI` | cash | leftover split 5.30 < 1 share @ 117.03 |
| 2026-08-27 | `GRRR` | cash | leftover split 5.30 < 1 share @ 15.94 |
| 2026-08-27 | `NCNO` | cash | leftover split 5.30 < 1 share @ 22.03 |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `NABL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NABL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `AVT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CGNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ZJYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RCKT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `AVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CGNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 34.23 < 1 share @ 263.36 |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AIAI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-14 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `OVID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `NVT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `FUBO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RDDT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DHT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FRO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `KGS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VLO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OVID` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `FUBO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RDDT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IVVD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBLX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `VAL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `KRMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ADPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `FPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `MRCY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AXTI` | cash | leftover split 30.90 < 1 share @ 67.91 |
| 2026-09-17 | `SMTC` | cash | leftover split 30.90 < 1 share @ 170.85 |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `VAL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `KRMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `FPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `MRCY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `CIFR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `AIB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BHVN` | cash | leftover split 10.28 < 1 share @ 14.07 |
| 2026-09-18 | `RARE` | cash | leftover split 10.28 < 1 share @ 14.79 |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `ARQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `CIFR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `AIB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `DDD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `DCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SHLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ARQT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SABR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AIB` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `DDD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `DCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `SHLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GLXY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GLXY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `TRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `TK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DXCM` | cash | leftover split 34.73 < 1 share @ 89.50 |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `MAZE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `TNGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `LU` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `KVYO` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ADMA` | 3 | 2026-09-23 @ $9.81 | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+4.0; leftover $34.73 |
| `OMER` | 1 | 2026-09-23 @ $20.65 | combo gate; gate blue=True,ret_5_max=10.0; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $34.73 |
| `INDP` | 8 | 2026-09-23 @ $3.93 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $34.73 |
| `MAZE` | 1 | 2026-09-23 @ $28.30 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $34.73 |
| `SGRY` | 2 | 2026-09-23 @ $15.72 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $34.73 |
| `TNGX` | 1 | 2026-09-23 @ $25.40 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $34.73 |
