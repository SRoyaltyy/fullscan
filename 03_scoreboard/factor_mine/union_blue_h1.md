# Factor mine action — `union_blue_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ blue, no 🚨

Cash book **-16.01%** ($8,399) · signal-only (no cash/fees) was -5.21%. Starts YES **0/30**. Fills 248 · skips 91 · realized $-561.25.

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
- **Gate** `blue=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,438.71.

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
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,544.55 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $4,332.68 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 138 | $9.01 | $2.40 | — | $3,086.90 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1334 | $0.94 | $16.50 | — | $1,820.44 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $560.20 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | — |
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
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,766.06 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+6.7; leftover $1251.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,621.89 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+5.8; leftover $1251.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,403.68 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+8.3; leftover $1251.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 309 | $4.05 | $3.99 | — | $5,148.25 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1251.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 147 | $8.46 | $2.43 | — | $3,902.19 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1251.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 137 | $9.12 | $2.40 | — | $2,650.35 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1251.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 77 | $16.20 | $2.22 | — | $1,400.73 | — | union ∩ blue, no 🚨; gate blue=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1251.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 85 | $14.66 | $2.25 | — | $152.39 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1251.87 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $152.39 | ▼ close $9,984.67 vs 09:30 $10,054.84 (session -10.94) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $152.39 | ▼ 09:30 equity $9,865.94 vs yday $9,984.67 (-118.73) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,446.30 | ▲ +44.98 after sell → book $9,863.85; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,628.58 | ▲ +38.11 after sell → book $9,861.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,880.13 | ▲ +33.34 after sell → book $9,859.78; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 309 | $3.72 | $4.05 | $-110.00 | $5,025.57 | ▼ -110.00 after sell → book $9,855.74; vs 09:30 mark -4.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 147 | $8.55 | $2.47 | $+8.33 | $6,279.95 | ▲ +8.33 after sell → book $9,853.27; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 137 | $9.03 | $2.43 | $-17.16 | $7,514.63 | ▼ -17.16 after sell → book $9,850.84; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 77 | $15.78 | $2.24 | $-36.80 | $8,727.44 | ▼ -36.80 after sell → book $9,848.59; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 85 | $13.19 | $2.27 | $-129.46 | $9,846.32 | ▼ -129.46 after sell → book $9,846.32; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,846.32 | ▲ close $9,846.32 vs 09:30 $9,865.94 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,846.32 | ▲ 09:30 equity $9,846.32 vs yday $9,846.32 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,846.32 | ▲ close $9,846.32 vs 09:30 $9,846.32 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,846.32 | ▲ 09:30 equity $9,846.32 vs yday $9,846.32 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 59 | $20.55 | $2.17 | — | $8,631.71 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1230.79 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,446.55 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1230.79 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 59 | $20.65 | $2.17 | — | $6,226.03 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1230.79 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 213 | $5.77 | $2.75 | — | $4,994.27 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1230.79 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 62 | $19.63 | $2.18 | — | $3,775.04 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1230.79 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $2,558.09 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1230.79 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 703 | $1.75 | $9.07 | — | $1,318.78 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1230.79 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $160.44 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1230.79 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.44 | ▲ close $10,051.62 vs 09:30 $9,846.32 (session +229.78) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.44 | ▲ 09:30 equity $10,315.34 vs yday $10,051.62 (+263.72) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 59 | $21.90 | $2.19 | $+75.30 | $1,450.35 | ▲ +75.30 after sell → book $10,313.15; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,692.67 | ▲ +57.15 after sell → book $10,311.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 59 | $21.75 | $2.19 | $+60.55 | $3,973.73 | ▲ +60.55 after sell → book $10,308.92; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 213 | $5.67 | $2.79 | $-26.84 | $5,178.65 | ▼ -26.84 after sell → book $10,306.13; vs 09:30 mark -2.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 62 | $21.17 | $2.20 | $+91.11 | $6,488.99 | ▲ +91.11 after sell → book $10,303.93; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $7,805.82 | ▲ +99.89 after sell → book $10,301.79; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 703 | $1.79 | $9.20 | $+9.86 | $9,055.00 | ▲ +9.86 after sell → book $10,292.60; vs 09:30 mark -9.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,290.57 | ▲ +77.23 after sell → book $10,290.57; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,094.25 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1286.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 74 | $17.20 | $2.21 | — | $7,819.23 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1286.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,735.73 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1286.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 115 | $11.13 | $2.33 | — | $5,453.44 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1286.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 520 | $2.47 | $6.71 | — | $4,162.34 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1286.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 666 | $1.93 | $8.59 | — | $2,868.36 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1286.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,612.19 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1286.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 974 | $1.32 | $12.56 | — | $313.95 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1286.32 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $313.95 | ▲ close $10,509.84 vs 09:30 $10,315.34 (session +257.76) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $313.95 | ▲ 09:30 equity $10,879.58 vs yday $10,509.84 (+369.74) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,517.01 | ▲ +6.74 after sell → book $10,877.54; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 74 | $16.57 | $2.23 | $-51.07 | $2,740.95 | ▼ -51.07 after sell → book $10,875.30; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,824.08 | ▼ -0.38 after sell → book $10,873.28; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 115 | $13.33 | $2.37 | $+248.30 | $5,354.66 | ▲ +248.30 after sell → book $10,870.91; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 520 | $2.40 | $6.80 | $-49.91 | $6,595.86 | ▼ -49.91 after sell → book $10,864.11; vs 09:30 mark -6.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 666 | $1.88 | $8.71 | $-50.60 | $7,839.22 | ▼ -50.60 after sell → book $10,855.39; vs 09:30 mark -8.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.75 | $2.07 | $-24.50 | $9,070.90 | ▼ -24.50 after sell → book $10,853.32; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 974 | $1.83 | $12.74 | $+471.43 | $10,840.58 | ▲ +471.43 after sell → book $10,840.58; vs 09:30 mark -12.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,840.58 | ▲ close $10,840.58 vs 09:30 $10,879.58 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,840.58 | ▲ 09:30 equity $10,840.58 vs yday $10,840.58 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 123 | $10.98 | $2.36 | — | $9,487.68 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+1.2; leftover $1355.07 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.19 | $2.06 | — | $8,139.45 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+7.4; leftover $1355.07 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 162 | $8.35 | $2.48 | — | $6,784.27 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1355.07 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 186 | $7.25 | $2.55 | — | $5,433.22 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1355.07 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 99 | $13.59 | $2.29 | — | $4,085.52 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1355.07 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 142 | $9.49 | $2.42 | — | $2,735.53 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1355.07 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 36 | $36.96 | $2.10 | — | $1,402.87 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1355.07 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 297 | $4.55 | $3.83 | — | $47.69 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1355.07 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.69 | ▲ close $11,086.61 vs 09:30 $10,840.58 (session +266.10) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.69 | ▼ 09:30 equity $11,042.00 vs yday $11,086.61 (-44.61) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `INSP` | 22 | $60.07 | $2.08 | $-28.77 | $1,367.15 | ▼ -28.77 after sell → book $11,039.92; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 186 | $8.29 | $2.59 | $+188.30 | $2,906.50 | ▲ +188.30 after sell → book $11,037.33; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 99 | $13.63 | $2.31 | $-0.64 | $4,253.56 | ▼ -0.64 after sell → book $11,035.02; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 142 | $9.89 | $2.45 | $+51.93 | $5,655.49 | ▲ +51.93 after sell → book $11,032.57; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 36 | $38.24 | $2.12 | $+41.86 | $7,030.01 | ▲ +41.86 after sell → book $11,030.45; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 297 | $4.31 | $3.89 | $-79.00 | $8,306.19 | ▼ -79.00 after sell → book $11,026.56; vs 09:30 mark -3.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `RZLT` | 276 | $5.01 | $3.56 | — | $6,919.87 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,yday_gainer; 🔵; ret5=+7.5; leftover $1384.36 | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 44 | $31.21 | $2.12 | — | $5,544.50 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1384.36 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 124 | $11.12 | $2.36 | — | $4,163.26 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1384.36 | — |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 140 | $9.83 | $2.41 | — | $2,784.65 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1384.36 | — |
| 2026-08-26 09:30 ET | **BUY** | `AVEX` | 79 | $17.51 | $2.23 | — | $1,399.14 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $1384.36 | — |
| 2026-08-26 09:30 ET | **BUY** | `BE` | 6 | $213.94 | $2.01 | — | $113.49 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1384.36 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.49 | ▲ close $11,059.82 vs 09:30 $11,042.00 (session +47.95) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.49 | ▲ 09:30 equity $11,152.69 vs yday $11,059.82 (+92.87) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 123 | $10.63 | $2.39 | $-47.80 | $1,418.59 | ▼ -47.80 after sell → book $11,150.30; vs 09:30 mark -2.39 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 162 | $8.49 | $2.51 | $+17.69 | $2,791.45 | ▲ +17.69 after sell → book $11,147.78; vs 09:30 mark -2.52 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 276 | $5.07 | $3.62 | $+9.38 | $4,187.16 | ▲ +9.38 after sell → book $11,144.17; vs 09:30 mark -3.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 44 | $30.79 | $2.14 | $-22.74 | $5,539.77 | ▼ -22.74 after sell → book $11,142.02; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ABX` | 140 | $9.68 | $2.44 | $-25.85 | $6,892.53 | ▼ -25.85 after sell → book $11,139.58; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVEX` | 79 | $18.43 | $2.25 | $+68.20 | $8,346.25 | ▲ +68.20 after sell → book $11,137.33; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 19 | $70.30 | $2.05 | — | $7,008.50 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,yday_mover; 🔵; ret5=-11.2; leftover $1391.04 | — |
| 2026-08-27 09:30 ET | **BUY** | `NVTS` | 105 | $13.18 | $2.31 | — | $5,622.30 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer; 🔵; ret5=-1.2; leftover $1391.04 | — |
| 2026-08-27 09:30 ET | **BUY** | `AAOI` | 11 | $117.03 | $2.02 | — | $4,332.94 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer; 🔵; ret5=-6.9; leftover $1391.04 | — |
| 2026-08-27 09:30 ET | **BUY** | `GRRR` | 87 | $15.94 | $2.25 | — | $2,943.91 | — | union ∩ blue, no 🚨; gate blue=True; list yday_mover; 🔵; ret5=+5.9; leftover $1391.04 | — |
| 2026-08-27 09:30 ET | **BUY** | `VYX` | 155 | $8.95 | $2.46 | — | $1,554.21 | — | union ∩ blue, no 🚨; gate blue=True; list ohlc_hot; 🔵; ret5=+16.2; leftover $1391.04 | — |
| 2026-08-27 09:30 ET | **BUY** | `NCNO` | 63 | $22.03 | $2.18 | — | $164.14 | — | union ∩ blue, no 🚨; gate blue=True; list ohlc_hot,earn_react; 🔵; ret5=+4.0; leftover $1391.04 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $164.14 | ▼ close $10,974.03 vs 09:30 $11,152.69 (session -150.04) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $164.14 | ▼ 09:30 equity $10,863.09 vs yday $10,974.03 (-110.94) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 124 | $11.27 | $2.39 | $+13.84 | $1,559.22 | ▲ +13.84 after sell → book $10,860.69; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BE` | 6 | $215.71 | $2.03 | $+6.55 | $2,851.43 | ▲ +6.55 after sell → book $10,858.67; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 19 | $65.29 | $2.07 | $-99.30 | $4,089.87 | ▼ -99.30 after sell → book $10,856.60; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVTS` | 105 | $12.44 | $2.33 | $-82.34 | $5,393.74 | ▼ -82.34 after sell → book $10,854.27; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AAOI` | 11 | $110.46 | $2.04 | $-76.34 | $6,606.75 | ▼ -76.34 after sell → book $10,852.22; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NCNO` | 63 | $23.30 | $2.20 | $+75.63 | $8,072.45 | ▲ +75.63 after sell → book $10,850.02; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 40 | $32.90 | $2.11 | — | $6,754.34 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1345.41 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $79.42 | $2.04 | — | $5,481.58 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1345.41 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $4,164.94 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1345.41 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $2,887.08 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1345.41 | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $122.81 | $2.02 | — | $1,656.96 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1345.41 | — |
| 2026-08-28 09:30 ET | **BUY** | `NVRI` | 59 | $22.66 | $2.17 | — | $317.85 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer; 🔵; ret5=+10.6; leftover $1345.41 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $317.85 | ▼ close $10,503.59 vs 09:30 $10,863.09 (session -334.06) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $317.85 | ▼ 09:30 equity $10,465.82 vs yday $10,503.59 (-37.77) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 87 | $14.44 | $2.28 | $-135.03 | $1,571.86 | ▼ -135.03 after sell → book $10,463.55; vs 09:30 mark -2.27 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 155 | $8.66 | $2.49 | $-49.90 | $2,911.67 | ▼ -49.90 after sell → book $10,461.06; vs 09:30 mark -2.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 40 | $31.15 | $2.13 | $-74.24 | $4,155.54 | ▼ -74.24 after sell → book $10,458.93; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $80.44 | $2.06 | $+12.22 | $5,440.52 | ▲ +12.22 after sell → book $10,456.87; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $6,770.75 | ▲ +13.59 after sell → book $10,454.83; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $7,959.41 | ▼ -89.19 after sell → book $10,452.79; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 10 | $118.83 | $2.04 | $-43.86 | $9,145.67 | ▼ -43.86 after sell → book $10,450.75; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NVRI` | 59 | $22.12 | $2.19 | $-36.21 | $10,448.57 | ▼ -36.21 after sell → book $10,448.57; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,448.57 | ▲ close $10,448.57 vs 09:30 $10,465.82 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,448.57 | ▲ 09:30 equity $10,448.57 vs yday $10,448.57 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,448.57 | ▲ close $10,448.57 vs 09:30 $10,448.57 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,448.57 | ▲ 09:30 equity $10,448.57 vs yday $10,448.57 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,448.57 | ▲ close $10,448.57 vs 09:30 $10,448.57 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,448.57 | ▲ 09:30 equity $10,448.57 vs yday $10,448.57 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $52.88 | $2.06 | — | $9,177.38 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1306.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $42.93 | $2.08 | — | $7,887.40 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1306.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 359 | $3.63 | $4.63 | — | $6,579.60 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1306.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 162 | $8.03 | $2.48 | — | $5,276.27 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1306.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $4,082.20 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1306.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 84 | $15.45 | $2.24 | — | $2,782.16 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1306.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $1,612.58 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1306.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 77 | $16.77 | $2.22 | — | $319.07 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1306.07 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $319.07 | ▼ close $10,196.54 vs 09:30 $10,448.57 (session -232.28) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $319.07 | ▲ 09:30 equity $10,197.55 vs yday $10,196.54 (+1.01) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 30 | $41.50 | $2.10 | $-47.08 | $1,561.97 | ▼ -47.08 after sell → book $10,195.45; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 162 | $7.91 | $2.51 | $-24.43 | $2,840.88 | ▼ -24.43 after sell → book $10,192.94; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $4,009.11 | ▼ -25.83 after sell → book $10,190.90; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 84 | $15.00 | $2.27 | $-42.31 | $5,266.85 | ▼ -42.31 after sell → book $10,188.64; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $153.62 | $2.03 | $+57.35 | $6,493.77 | ▲ +57.35 after sell → book $10,186.60; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 77 | $15.61 | $2.24 | $-93.78 | $7,693.50 | ▼ -93.78 after sell → book $10,184.36; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 508 | $2.52 | $6.55 | — | $6,406.78 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1282.25 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 191 | $6.71 | $2.56 | — | $5,122.61 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1282.25 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 674 | $1.90 | $8.69 | — | $3,833.32 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1282.25 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 268 | $4.78 | $3.46 | — | $2,548.82 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1282.25 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 806 | $1.59 | $10.40 | — | $1,256.88 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1282.25 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 110 | $11.31 | $2.32 | — | $10.46 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1282.25 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.46 | ▼ close $10,098.16 vs 09:30 $10,197.55 (session -52.21) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.46 | ▼ 09:30 equity $10,057.56 vs yday $10,098.16 (-40.60) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 24 | $54.31 | $2.08 | $+30.18 | $1,311.82 | ▲ +30.18 after sell → book $10,055.48; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 359 | $3.43 | $4.70 | $-81.13 | $2,538.49 | ▼ -81.13 after sell → book $10,050.78; vs 09:30 mark -4.70 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 508 | $2.38 | $6.65 | $-84.32 | $3,740.88 | ▼ -84.32 after sell → book $10,044.13; vs 09:30 mark -6.65 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 191 | $6.57 | $2.60 | $-31.91 | $4,993.15 | ▼ -31.91 after sell → book $10,041.53; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 674 | $2.00 | $8.82 | $+49.89 | $6,332.33 | ▲ +49.89 after sell → book $10,032.71; vs 09:30 mark -8.82 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 268 | $4.30 | $3.51 | $-135.61 | $7,481.22 | ▼ -135.61 after sell → book $10,029.20; vs 09:30 mark -3.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 806 | $1.63 | $10.54 | $+11.30 | $8,784.46 | ▲ +11.30 after sell → book $10,018.66; vs 09:30 mark -10.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 110 | $11.22 | $2.35 | $-14.57 | $10,016.31 | ▼ -14.57 after sell → book $10,016.31; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,016.31 | ▲ close $10,016.31 vs 09:30 $10,057.56 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,016.31 | ▲ 09:30 equity $10,016.31 vs yday $10,016.31 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,016.31 | ▲ close $10,016.31 vs 09:30 $10,016.31 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,016.31 | ▲ 09:30 equity $10,016.31 vs yday $10,016.31 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,016.31 | ▲ close $10,016.31 vs 09:30 $10,016.31 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,016.31 | ▲ 09:30 equity $10,016.31 vs yday $10,016.31 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 76 | $16.28 | $2.22 | — | $8,776.81 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=-1.1; leftover $1252.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 458 | $2.73 | $5.91 | — | $7,520.56 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=-3.0; leftover $1252.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 7 | $157.78 | $2.01 | — | $6,414.09 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+4.7; leftover $1252.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 22 | $56.09 | $2.06 | — | $5,178.06 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+19.6; leftover $1252.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 613 | $2.04 | $7.91 | — | $3,919.63 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1252.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 263 | $4.75 | $3.39 | — | $2,666.98 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1252.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 590 | $2.12 | $7.61 | — | $1,408.57 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1252.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 108 | $11.55 | $2.31 | — | $158.86 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1252.04 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.86 | ▼ close $9,979.13 vs 09:30 $10,016.31 (session -3.76) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.86 | ▼ 09:30 equity $9,795.76 vs yday $9,979.13 (-183.37) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AUPH` | 76 | $16.03 | $2.24 | $-23.46 | $1,374.90 | ▼ -23.46 after sell → book $9,793.52; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `OVID` | 458 | $2.75 | $5.99 | $-0.45 | $2,630.69 | ▼ -0.45 after sell → book $9,787.52; vs 09:30 mark -6.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `NVT` | 7 | $150.00 | $2.03 | $-58.50 | $3,678.66 | ▼ -58.50 after sell → book $9,785.49; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COHU` | 22 | $52.23 | $2.08 | $-89.05 | $4,825.65 | ▼ -89.05 after sell → book $9,783.42; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 613 | $2.01 | $8.02 | $-34.32 | $6,049.76 | ▼ -34.32 after sell → book $9,775.40; vs 09:30 mark -8.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 263 | $4.82 | $3.45 | $+11.57 | $7,313.97 | ▲ +11.57 after sell → book $9,771.95; vs 09:30 mark -3.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 590 | $2.05 | $7.72 | $-56.63 | $8,515.75 | ▼ -56.63 after sell → book $9,764.23; vs 09:30 mark -7.72 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `FUBO` | 108 | $11.56 | $2.34 | $-3.58 | $9,761.89 | ▼ -3.58 after sell → book $9,761.89; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,761.89 | ▲ close $9,761.89 vs 09:30 $9,795.76 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,761.89 | ▲ 09:30 equity $9,761.89 vs yday $9,761.89 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,761.89 | ▲ close $9,761.89 vs 09:30 $9,761.89 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,761.89 | ▲ 09:30 equity $9,761.89 vs yday $9,761.89 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 33 | $36.46 | $2.09 | — | $8,556.62 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+2.9; leftover $1220.24 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 17 | $68.79 | $2.04 | — | $7,385.15 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1220.24 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 207 | $5.87 | $2.67 | — | $6,167.39 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1220.24 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 13 | $87.40 | $2.03 | — | $5,029.16 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1220.24 | — |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 32 | $38.01 | $2.09 | — | $3,810.76 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $1220.24 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 45 | $27.09 | $2.12 | — | $2,589.58 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1220.24 | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 677 | $1.80 | $8.73 | — | $1,362.25 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $1220.24 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 13 | $89.38 | $2.03 | — | $198.28 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1220.24 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $198.28 | ▲ close $9,740.23 vs 09:30 $9,761.89 (session +2.14) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $198.28 | ▲ 09:30 equity $9,913.36 vs yday $9,740.23 (+173.13) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `BLFS` | 33 | $36.67 | $2.11 | $+2.73 | $1,406.28 | ▲ +2.73 after sell → book $9,911.25; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 17 | $72.70 | $2.06 | $+62.37 | $2,640.12 | ▲ +62.37 after sell → book $9,909.19; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 207 | $5.58 | $2.71 | $-65.41 | $3,792.46 | ▼ -65.41 after sell → book $9,906.47; vs 09:30 mark -2.72 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 13 | $83.20 | $2.05 | $-58.68 | $4,872.01 | ▼ -58.68 after sell → book $9,904.42; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `KRMN` | 32 | $37.89 | $2.11 | $-8.03 | $6,082.39 | ▼ -8.03 after sell → book $9,902.32; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 45 | $28.23 | $2.15 | $+47.03 | $7,350.59 | ▲ +47.03 after sell → book $9,900.17; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `HLP` | 677 | $2.10 | $8.86 | $+185.51 | $8,763.44 | ▲ +185.51 after sell → book $9,891.32; vs 09:30 mark -8.85 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 13 | $86.76 | $2.05 | $-38.14 | $9,889.27 | ▼ -38.14 after sell → book $9,889.27; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 8 | $151.43 | $2.01 | — | $8,675.81 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $1236.16 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 120 | $10.25 | $2.35 | — | $7,443.46 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $1236.16 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 162 | $7.59 | $2.48 | — | $6,211.41 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1236.16 | — |
| 2026-09-17 09:30 ET | **BUY** | `AXTI` | 18 | $67.91 | $2.04 | — | $4,986.98 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.7; leftover $1236.16 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 47 | $25.95 | $2.13 | — | $3,765.20 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1236.16 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 7 | $170.85 | $2.01 | — | $2,567.24 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1236.16 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 515 | $2.40 | $6.64 | — | $1,324.60 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1236.16 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 68 | $18.04 | $2.19 | — | $96.02 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $1236.16 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $96.02 | ▲ close $9,875.01 vs 09:30 $9,913.36 (session +7.61) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.02 | ▲ 09:30 equity $10,017.10 vs yday $9,875.01 (+142.09) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `TWST` | 8 | $158.04 | $2.03 | $+48.83 | $1,358.31 | ▲ +48.83 after sell → book $10,015.07; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 120 | $10.12 | $2.38 | $-20.33 | $2,570.33 | ▼ -20.33 after sell → book $10,012.69; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 162 | $7.98 | $2.51 | $+58.19 | $3,860.58 | ▲ +58.19 after sell → book $10,010.18; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AXTI` | 18 | $69.72 | $2.06 | $+28.47 | $5,113.47 | ▲ +28.47 after sell → book $10,008.11; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 47 | $26.14 | $2.15 | $+4.65 | $6,339.90 | ▲ +4.65 after sell → book $10,005.96; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 7 | $182.33 | $2.03 | $+76.32 | $7,614.18 | ▲ +76.32 after sell → book $10,003.93; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 515 | $2.29 | $6.74 | $-70.03 | $8,786.79 | ▼ -70.03 after sell → book $9,997.19; vs 09:30 mark -6.74 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 68 | $17.80 | $2.22 | $-20.39 | $9,994.98 | ▼ -20.39 after sell → book $9,994.98; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 5 | $209.52 | $2.00 | — | $8,945.37 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1249.37 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 5 | $219.62 | $2.00 | — | $7,845.27 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1249.37 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 14 | $85.00 | $2.03 | — | $6,653.23 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1249.37 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 36 | $34.44 | $2.10 | — | $5,411.30 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1249.37 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 1288 | $0.97 | $16.36 | — | $4,145.58 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1249.37 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 316 | $3.95 | $4.08 | — | $2,893.30 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1249.37 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 88 | $14.07 | $2.25 | — | $1,652.89 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1249.37 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 214 | $5.83 | $2.76 | — | $402.51 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1249.37 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $402.51 | ▼ close $9,778.49 vs 09:30 $10,017.10 (session -182.90) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $402.51 | ▲ 09:30 equity $9,981.03 vs yday $9,778.49 (+202.54) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 5 | $210.00 | $2.02 | $-1.63 | $1,450.48 | ▼ -1.63 after sell → book $9,979.00; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 5 | $230.25 | $2.02 | $+49.12 | $2,599.71 | ▲ +49.12 after sell → book $9,976.98; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 14 | $82.83 | $2.05 | $-34.46 | $3,757.28 | ▼ -34.46 after sell → book $9,974.93; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FIVN` | 36 | $33.00 | $2.12 | $-56.06 | $4,943.16 | ▼ -56.06 after sell → book $9,972.81; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TLSA` | 1288 | $0.94 | $16.20 | $-71.19 | $6,137.68 | ▼ -71.19 after sell → book $9,956.61; vs 09:30 mark -16.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 316 | $3.87 | $4.14 | $-33.50 | $7,356.46 | ▼ -33.50 after sell → book $9,952.47; vs 09:30 mark -4.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 88 | $13.90 | $2.28 | $-19.49 | $8,577.39 | ▼ -19.49 after sell → book $9,950.20; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BNC` | 214 | $6.42 | $2.81 | $+119.62 | $9,947.39 | ▲ +119.62 after sell → book $9,947.39; vs 09:30 mark -2.81 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 124 | $9.99 | $2.36 | — | $8,706.27 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1243.42 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 681 | $1.82 | $8.78 | — | $7,454.66 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1243.42 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 47 | $25.95 | $2.13 | — | $6,232.88 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1243.42 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 95 | $13.05 | $2.27 | — | $4,990.85 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $1243.42 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 216 | $5.75 | $2.79 | — | $3,744.98 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,yday_mover; 🔵; ret5=+30.3; leftover $1243.42 | — |
| 2026-09-21 09:30 ET | **BUY** | `FWDI` | 151 | $8.22 | $2.44 | — | $2,501.32 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1243.42 | — |
| 2026-09-21 09:30 ET | **BUY** | `DFDV` | 191 | $6.51 | $2.56 | — | $1,255.35 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,yday_mover; 🔵; ret5=+19.3; leftover $1243.42 | — |
| 2026-09-21 09:30 ET | **BUY** | `CAN` | 2952 | $0.42 | $21.20 | — | $0.22 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,yday_mover; 🔵; ret5=+10.7; leftover $1243.42 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.22 | ▼ close $9,901.35 vs 09:30 $9,981.03 (session -1.49) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.22 | ▼ 09:30 equity $9,891.31 vs yday $9,901.35 (-10.04) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 124 | $9.91 | $2.39 | $-14.67 | $1,226.66 | ▼ -14.67 after sell → book $9,888.92; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `BTBT` | 681 | $1.79 | $8.91 | $-38.12 | $2,440.15 | ▼ -38.12 after sell → book $9,880.01; vs 09:30 mark -8.91 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 95 | $12.99 | $2.30 | $-10.28 | $3,671.90 | ▼ -10.28 after sell → book $9,877.71; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `GEMI` | 216 | $6.05 | $2.83 | $+59.18 | $4,976.95 | ▲ +59.18 after sell → book $9,874.88; vs 09:30 mark -2.83 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `CAN` | 2952 | $0.41 | $21.31 | $-80.88 | $6,151.20 | ▼ -80.88 after sell → book $9,853.57; vs 09:30 mark -21.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,151.20 | ▲ close $9,853.57 vs 09:30 $9,891.31 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,151.20 | ▼ 09:30 equity $9,801.85 vs yday $9,853.57 (-51.72) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 47 | $26.58 | $2.15 | $+25.33 | $7,398.30 | ▲ +25.33 after sell → book $9,799.69; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FWDI` | 151 | $8.20 | $2.48 | $-7.94 | $8,634.03 | ▼ -7.94 after sell → book $9,797.22; vs 09:30 mark -2.47 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DFDV` | 191 | $6.09 | $2.60 | $-85.39 | $9,794.61 | ▼ -85.39 after sell → book $9,794.61; vs 09:30 mark -2.61 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 13 | $89.50 | $2.03 | — | $8,629.08 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1224.33 | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 7 | $166.54 | $2.01 | — | $7,461.29 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1224.33 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 10 | $116.85 | $2.02 | — | $6,290.77 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1224.33 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 44 | $27.79 | $2.12 | — | $5,065.89 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1224.33 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 154 | $7.95 | $2.45 | — | $3,839.14 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1224.33 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 124 | $9.81 | $2.36 | — | $2,620.34 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1224.33 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 60 | $20.25 | $2.17 | — | $1,403.17 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+15.0; leftover $1224.33 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 59 | $20.65 | $2.17 | — | $182.65 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1224.33 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $182.65 | ▼ close $9,508.92 vs 09:30 $9,801.85 (session -268.36) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $182.65 | ▼ 09:30 equity $9,456.23 vs yday $9,508.92 (-52.69) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 13 | $87.67 | $2.05 | $-27.80 | $1,320.37 | ▼ -27.80 after sell → book $9,454.18; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 7 | $163.95 | $2.03 | $-22.17 | $2,465.99 | ▼ -22.17 after sell → book $9,452.15; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HALO` | 10 | $112.22 | $2.04 | $-50.36 | $3,586.15 | ▼ -50.36 after sell → book $9,450.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 44 | $26.22 | $2.14 | $-73.34 | $4,737.69 | ▼ -73.34 after sell → book $9,447.97; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 154 | $7.38 | $2.49 | $-92.72 | $5,871.72 | ▼ -92.72 after sell → book $9,445.48; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 124 | $9.67 | $2.39 | $-22.11 | $7,068.41 | ▼ -22.11 after sell → book $9,443.09; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FTRE` | 60 | $19.40 | $2.19 | $-55.36 | $8,230.22 | ▼ -55.36 after sell → book $9,440.90; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 59 | $20.52 | $2.19 | $-12.02 | $9,438.71 | ▼ -12.02 after sell → book $9,438.71; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,438.71 | ▲ close $9,438.71 vs 09:30 $9,456.23 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,524.91 | ▲ 09:30 equity $8,524.91 vs yday $8,524.91 (+0.00) | 09:30 open · cash $8,524.91 · no holdings · equity $8,524.91 vs prior close $8,524.91 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 9 | $115.36 | $2.02 | — | $7,484.65 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1065.61 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 51 | $20.61 | $2.14 | — | $6,431.40 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+9.1; leftover $1065.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 139 | $7.65 | $2.41 | — | $5,365.64 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1065.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 40 | $26.27 | $2.11 | — | $4,312.73 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1065.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $3,305.59 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1065.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BRVE` | 45 | $23.58 | $2.12 | — | $2,242.36 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $1065.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 484 | $2.20 | $6.24 | — | $1,171.32 | — | union ∩ blue, no 🚨; gate blue=True; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1065.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 177 | $6.00 | $2.52 | — | $106.80 | — | union ∩ blue, no 🚨; gate blue=True; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $1065.61 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.80 | ▼ close $8,398.93 vs 09:30 $8,524.91 (session -104.39) | 16:00 close · cash $106.80 · equity $8,398.93 vs 09:30 $8,524.91 (-125.98; session marks -104.39) · 8 name(s) marked open→close (per-name table). HALO×9 09:30 $115.36 → close $113.90 -13.14; OMER×51 09:30 $20.61 → close $20.08 -27.03; MRVI×139 09:30 $7.65 → close $7.60 -6.95; WRBY×40 09:30 $26.27 → close $26.71 +17.60; TXG×12 09:30 $83.76 → close $85.71 +23.40; BRVE×45 09:30 $23.58 → close $20.62 -133.20; HLP×484 09:30 $2.20 → close $2.21 +4.84; SATL×177 09:30 $6.00 → close $6.17 +30.09 | — |

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
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SRPT` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AIAI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `QMCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `PHM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OBE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LAC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `MXL` | hard_red | hard-red S=-13.28 sit; no new buys |
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
| 2026-09-15 | `RPD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FWDI` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DFDV` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `LU` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AIB` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZSQR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `KVYO` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new buys |
