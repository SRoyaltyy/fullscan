# Factor mine action — `union_news_present_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ news_present, no 🚨

Cash book **-20.38%** ($7,962) · signal-only (no cash/fees) was -4.75%. Starts YES **1/30**. Fills 181 · skips 288 · realized $-1012.80.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the news camera printed something (any color, not blank).
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
- **Gate** `news_present=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,914.91.

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
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,544.55 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $4,332.68 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 138 | $9.01 | $2.40 | — | $3,086.90 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1334 | $0.94 | $16.50 | — | $1,820.44 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $560.20 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $560.20 | ▲ close $10,051.46 vs 09:30 $10,000.00 (session +91.20) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $560.20 | ▲ 09:30 equity $10,054.84 vs yday $10,051.46 (+3.38) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 1 | $46.18 | $0.46 | — | $513.55 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+6.7; leftover $70.02 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 17 | $4.05 | $0.74 | — | $443.96 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $70.02 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 8 | $8.46 | $0.70 | — | $375.58 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $70.02 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 21 | $3.24 | $0.74 | — | $306.80 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ⚪; ret5=+0.3; leftover $70.02 | — |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 13 | $5.07 | $0.70 | — | $240.19 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=-4.7; leftover $70.02 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $240.19 | ▲ close $10,058.88 vs 09:30 $10,054.84 (session +7.38) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $240.19 | ▼ 09:30 equity $9,876.26 vs yday $10,058.88 (-182.62) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $240.19 | ▼ close $9,561.28 vs 09:30 $9,876.26 (session -314.98) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $240.19 | ▲ 09:30 equity $9,598.39 vs yday $9,561.28 (+37.11) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `TLN` | 3 | $321.00 | $2.02 | $-120.51 | $1,201.17 | ▼ -120.51 after sell → book $9,596.37; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VST` | 8 | $140.74 | $2.03 | $-53.33 | $2,325.06 | ▼ -53.33 after sell → book $9,594.34; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NRG` | 10 | $116.20 | $2.04 | $-42.06 | $3,485.02 | ▼ -42.06 after sell → book $9,592.30; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DAVE` | 3 | $334.00 | $2.02 | $+5.25 | $4,485.00 | ▲ +5.25 after sell → book $9,590.28; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SLG` | 21 | $57.50 | $2.07 | $-6.44 | $5,690.42 | ▼ -6.44 after sell → book $9,588.20; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 138 | $8.91 | $2.44 | $-18.64 | $6,917.57 | ▼ -18.64 after sell → book $9,585.77; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 1334 | $0.88 | $15.97 | $-108.51 | $8,075.51 | ▼ -108.51 after sell → book $9,569.79; vs 09:30 mark -15.98 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 833 | $1.42 | $10.89 | $-88.28 | $9,247.48 | ▼ -88.28 after sell → book $9,558.90; vs 09:30 mark -10.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,247.48 | ▼ close $9,556.52 vs 09:30 $9,598.39 (session -2.38) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,247.48 | ▼ 09:30 equity $9,554.99 vs yday $9,556.52 (-1.53) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 1 | $49.02 | $0.51 | $+1.86 | $9,295.99 | ▲ +1.86 after sell → book $9,554.48; vs 09:30 mark -0.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 17 | $3.92 | $0.74 | $-3.69 | $9,361.89 | ▼ -3.69 after sell → book $9,553.74; vs 09:30 mark -0.74 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 8 | $8.35 | $0.71 | $-2.29 | $9,427.98 | ▼ -2.29 after sell → book $9,553.03; vs 09:30 mark -0.71 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 21 | $3.20 | $0.76 | $-2.34 | $9,494.42 | ▼ -2.34 after sell → book $9,552.27; vs 09:30 mark -0.76 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NB` | 13 | $4.45 | $0.64 | $-9.40 | $9,551.64 | ▼ -9.40 after sell → book $9,551.64; vs 09:30 mark -0.63 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,357.57 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1193.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,172.41 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1193.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $5,993.20 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1193.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 206 | $5.77 | $2.66 | — | $4,801.92 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1193.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $3,621.95 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1193.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $2,434.64 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1193.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 682 | $1.75 | $8.80 | — | $1,232.35 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1193.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $74.01 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1193.95 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $74.01 | ▲ close $9,753.61 vs 09:30 $9,554.99 (session +226.08) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.01 | ▲ 09:30 equity $10,011.72 vs yday $9,753.61 (+258.11) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 3 | $2.47 | $0.08 | — | $66.52 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $9.25 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 4 | $1.93 | $0.09 | — | $58.71 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $9.25 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 7 | $1.32 | $0.11 | — | $49.36 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $9.25 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.36 | ▼ close $10,009.57 vs 09:30 $10,011.72 (session -1.87) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.36 | ▲ 09:30 equity $10,109.80 vs yday $10,009.57 (+100.23) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.36 | ▼ close $10,078.21 vs 09:30 $10,109.80 (session -31.59) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.36 | ▼ 09:30 equity $9,918.97 vs yday $10,078.21 (-159.24) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 58 | $20.32 | $2.18 | $-17.69 | $1,225.73 | ▼ -17.69 after sell → book $9,916.78; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.86 | $2.05 | $+58.97 | $2,469.86 | ▲ +58.97 after sell → book $9,914.73; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 57 | $20.47 | $2.18 | $-14.60 | $3,634.47 | ▼ -14.60 after sell → book $9,912.55; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 206 | $5.53 | $2.70 | $-54.80 | $4,770.95 | ▼ -54.80 after sell → book $9,909.85; vs 09:30 mark -2.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 60 | $21.21 | $2.19 | $+90.44 | $6,041.36 | ▲ +90.44 after sell → book $9,907.66; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 40 | $32.32 | $2.13 | $+103.36 | $7,332.03 | ▲ +103.36 after sell → book $9,905.53; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 682 | $1.90 | $8.92 | $+84.58 | $8,618.91 | ▲ +84.58 after sell → book $9,896.61; vs 09:30 mark -8.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $9,868.96 | ▲ +91.71 after sell → book $9,894.58; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 51 | $23.77 | $2.14 | — | $8,654.54 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ⚪; ret5=+13.0; leftover $1233.62 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 112 | $10.98 | $2.33 | — | $7,422.46 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+1.2; leftover $1233.62 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 20 | $61.19 | $2.05 | — | $6,196.61 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+7.4; leftover $1233.62 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 147 | $8.35 | $2.43 | — | $4,966.73 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1233.62 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 249 | $4.94 | $3.21 | — | $3,733.45 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+7.1; leftover $1233.62 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $426.97 | $2.00 | — | $2,877.52 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+6.0; leftover $1233.62 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 170 | $7.25 | $2.50 | — | $1,642.52 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1233.62 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3445 | $0.36 | $22.67 | — | $386.54 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer; ret5=-15.6; leftover $1233.62 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $386.54 | ▲ close $10,083.23 vs 09:30 $9,918.97 (session +227.98) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $386.54 | ▼ 09:30 equity $10,081.88 vs yday $10,083.23 (-1.35) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 3 | $2.41 | $0.10 | $-0.36 | $393.67 | ▼ -0.36 after sell → book $10,081.78; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 4 | $2.03 | $0.11 | $+0.20 | $401.67 | ▲ +0.20 after sell → book $10,081.67; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 7 | $1.60 | $0.15 | $+1.69 | $412.72 | ▲ +1.69 after sell → book $10,081.52; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 6 | $31.21 | $1.89 | — | $223.57 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $206.36 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 18 | $11.12 | $2.04 | — | $21.37 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $206.36 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.37 | ▲ close $10,345.63 vs 09:30 $10,081.88 (session +268.05) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.37 | ▼ 09:30 equity $10,342.45 vs yday $10,345.63 (-3.18) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1 | $2.60 | $0.03 | — | $18.74 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,ohlc_hot; ret5=+13.0; leftover $4.27 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.74 | ▼ close $10,334.79 vs 09:30 $10,342.45 (session -7.63) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.74 | ▼ 09:30 equity $10,276.70 vs yday $10,334.79 (-58.09) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 112 | $10.97 | $2.35 | $-5.80 | $1,245.02 | ▼ -5.80 after sell → book $10,274.35; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 20 | $60.52 | $2.07 | $-17.52 | $2,453.35 | ▼ -17.52 after sell → book $10,272.28; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 147 | $8.28 | $2.47 | $-15.19 | $3,668.05 | ▼ -15.19 after sell → book $10,269.81; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 249 | $4.95 | $3.26 | $-3.99 | $4,897.33 | ▼ -3.99 after sell → book $10,266.55; vs 09:30 mark -3.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 2 | $423.76 | $2.02 | $-10.43 | $5,742.84 | ▼ -10.43 after sell → book $10,264.53; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 170 | $9.73 | $2.54 | $+416.56 | $7,394.40 | ▲ +416.56 after sell → book $10,261.99; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SAFX` | 3445 | $0.36 | $23.49 | $-22.04 | $8,628.33 | ▼ -22.04 after sell → book $10,238.50; vs 09:30 mark -23.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 34 | $41.74 | $2.09 | — | $7,207.08 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+2.4; leftover $1438.06 | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 98 | $14.63 | $2.28 | — | $5,771.05 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+5.8; leftover $1438.06 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 43 | $32.90 | $2.12 | — | $4,354.24 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1438.06 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 91 | $15.66 | $2.26 | — | $2,926.91 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1438.06 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 18 | $79.42 | $2.04 | — | $1,495.31 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1438.06 | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $232.10 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1438.06 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $232.10 | ▼ close $9,975.78 vs 09:30 $10,276.70 (session -249.91) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $232.10 | ▲ 09:30 equity $10,006.34 vs yday $9,975.78 (+30.56) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 51 | $23.68 | $2.16 | $-8.90 | $1,437.62 | ▼ -8.90 after sell → book $10,004.18; vs 09:30 mark -2.16 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVBP` | 6 | $29.94 | $1.83 | $-11.34 | $1,615.43 | ▼ -11.34 after sell → book $10,002.35; vs 09:30 mark -1.83 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FLNC` | 18 | $10.82 | $2.02 | $-9.47 | $1,808.16 | ▼ -9.47 after sell → book $10,000.32; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,808.16 | ▲ close $10,069.71 vs 09:30 $10,006.34 (session +69.39) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,808.16 | ▲ 09:30 equity $10,124.42 vs yday $10,069.71 (+54.71) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 1 | $2.67 | $0.05 | $-0.01 | $1,810.78 | ▼ -0.01 after sell → book $10,124.37; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,810.78 | ▼ close $10,115.57 vs 09:30 $10,124.42 (session -8.80) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,810.78 | ▼ 09:30 equity $10,039.23 vs yday $10,115.57 (-76.34) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 34 | $42.10 | $2.11 | $+8.03 | $3,240.07 | ▲ +8.03 after sell → book $10,037.12; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 98 | $15.70 | $2.31 | $+100.26 | $4,776.36 | ▲ +100.26 after sell → book $10,034.81; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 43 | $32.42 | $2.14 | $-24.90 | $6,168.28 | ▼ -24.90 after sell → book $10,032.67; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 91 | $13.92 | $2.29 | $-162.89 | $7,432.71 | ▼ -162.89 after sell → book $10,030.38; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 18 | $78.84 | $2.07 | $-14.55 | $8,849.77 | ▼ -14.55 after sell → book $10,028.32; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SIMO` | 5 | $235.71 | $2.02 | $-86.68 | $10,026.29 | ▼ -86.68 after sell → book $10,026.29; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,026.29 | ▲ close $10,026.29 vs 09:30 $10,039.23 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,026.29 | ▲ 09:30 equity $10,026.29 vs yday $10,026.29 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $8,807.99 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1253.29 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $7,560.94 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1253.29 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 345 | $3.63 | $4.45 | — | $6,304.14 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1253.29 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 156 | $8.03 | $2.46 | — | $5,049.01 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1253.29 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $3,854.94 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1253.29 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 81 | $15.45 | $2.23 | — | $2,601.26 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1253.29 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $1,431.68 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1253.29 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 74 | $16.77 | $2.21 | — | $188.49 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1253.29 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $188.49 | ▼ close $9,783.51 vs 09:30 $10,026.29 (session -223.26) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $188.49 | ▲ 09:30 equity $9,785.71 vs yday $9,783.51 (+2.20) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 12 | $2.52 | $0.34 | — | $157.91 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $31.41 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 4 | $6.71 | $0.28 | — | $130.79 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $31.41 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 16 | $1.90 | $0.35 | — | $100.04 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $31.41 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 6 | $4.78 | $0.30 | — | $71.05 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $31.41 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 19 | $1.59 | $0.36 | — | $40.48 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $31.41 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 2 | $11.31 | $0.23 | — | $17.63 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $31.41 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.63 | ▲ close $9,815.46 vs 09:30 $9,785.71 (session +31.62) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.63 | ▲ 09:30 equity $9,843.78 vs yday $9,815.46 (+28.32) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.63 | ▼ close $9,677.27 vs 09:30 $9,843.78 (session -166.51) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.63 | ▼ 09:30 equity $9,629.61 vs yday $9,677.27 (-47.66) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 23 | $53.16 | $2.08 | $+2.30 | $1,238.23 | ▲ +2.30 after sell → book $9,627.53; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 29 | $42.01 | $2.10 | $-30.85 | $2,454.43 | ▼ -30.85 after sell → book $9,625.44; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 345 | $3.28 | $4.52 | $-129.72 | $3,581.51 | ▼ -129.72 after sell → book $9,620.92; vs 09:30 mark -4.52 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 156 | $8.01 | $2.49 | $-8.07 | $4,828.57 | ▼ -8.07 after sell → book $9,618.42; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $5,958.47 | ▼ -64.17 after sell → book $9,616.39; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 81 | $15.16 | $2.26 | $-27.98 | $7,184.17 | ▼ -27.98 after sell → book $9,614.13; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 8 | $140.29 | $2.03 | $-49.25 | $8,304.50 | ▼ -49.25 after sell → book $9,612.10; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 74 | $15.46 | $2.23 | $-101.39 | $9,446.30 | ▼ -101.39 after sell → book $9,609.86; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,446.30 | ▼ close $9,602.67 vs 09:30 $9,629.61 (session -7.19) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,446.30 | ▼ 09:30 equity $9,600.41 vs yday $9,602.67 (-2.26) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 12 | $2.22 | $0.32 | $-4.26 | $9,472.62 | ▼ -4.26 after sell → book $9,600.08; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 4 | $6.11 | $0.28 | $-2.96 | $9,496.78 | ▼ -2.96 after sell → book $9,599.81; vs 09:30 mark -0.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 16 | $1.83 | $0.36 | $-1.83 | $9,525.70 | ▼ -1.83 after sell → book $9,599.45; vs 09:30 mark -0.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 6 | $3.92 | $0.27 | $-5.73 | $9,548.96 | ▼ -5.73 after sell → book $9,599.17; vs 09:30 mark -0.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 19 | $1.53 | $0.37 | $-1.87 | $9,577.66 | ▼ -1.87 after sell → book $9,598.80; vs 09:30 mark -0.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 2 | $10.57 | $0.24 | $-1.95 | $9,598.57 | ▼ -1.95 after sell → book $9,598.57; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,598.57 | ▲ close $9,598.57 vs 09:30 $9,600.41 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,598.57 | ▲ 09:30 equity $9,598.57 vs yday $9,598.57 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 73 | $16.28 | $2.21 | — | $8,407.92 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=-1.1; leftover $1199.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 439 | $2.73 | $5.66 | — | $7,203.79 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=-3.0; leftover $1199.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 5 | $206.84 | $2.00 | — | $6,167.58 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+8.3; leftover $1199.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $5,014.56 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1199.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 7 | $157.78 | $2.01 | — | $3,908.09 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+4.7; leftover $1199.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 21 | $56.09 | $2.05 | — | $2,728.15 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+19.6; leftover $1199.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 588 | $2.04 | $7.59 | — | $1,521.04 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1199.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 252 | $4.75 | $3.25 | — | $320.79 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1199.82 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $320.79 | ▼ close $9,540.82 vs 09:30 $9,598.57 (session -30.96) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $320.79 | ▼ 09:30 equity $9,266.21 vs yday $9,540.82 (-274.61) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $320.79 | ▼ close $9,222.96 vs 09:30 $9,266.21 (session -43.25) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $320.79 | ▲ 09:30 equity $9,248.90 vs yday $9,222.96 (+25.94) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $320.79 | ▼ close $9,002.84 vs 09:30 $9,248.90 (session -246.06) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $320.79 | ▲ 09:30 equity $9,063.86 vs yday $9,002.84 (+61.02) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AUPH` | 73 | $16.16 | $2.23 | $-13.20 | $1,498.24 | ▼ -13.20 after sell → book $9,061.63; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `OVID` | 439 | $2.72 | $5.75 | $-15.80 | $2,686.57 | ▼ -15.80 after sell → book $9,055.88; vs 09:30 mark -5.75 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 5 | $194.84 | $2.02 | $-64.03 | $3,658.75 | ▼ -64.03 after sell → book $9,053.86; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 7 | $140.03 | $2.03 | $-174.84 | $4,636.93 | ▼ -174.84 after sell → book $9,051.83; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 7 | $147.79 | $2.03 | $-73.97 | $5,669.43 | ▼ -73.97 after sell → book $9,049.80; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COHU` | 21 | $51.29 | $2.07 | $-104.93 | $6,744.44 | ▼ -104.93 after sell → book $9,047.72; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 588 | $1.89 | $7.69 | $-103.48 | $7,848.07 | ▼ -103.48 after sell → book $9,040.03; vs 09:30 mark -7.69 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 252 | $4.73 | $3.30 | $-11.59 | $9,036.73 | ▼ -11.59 after sell → book $9,036.73; vs 09:30 mark -3.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $7,951.16 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+4.0; leftover $1129.59 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 14 | $77.12 | $2.03 | — | $6,869.45 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,ohlc_hot; ret5=+7.2; leftover $1129.59 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 78 | $14.31 | $2.22 | — | $5,751.05 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+4.8; leftover $1129.59 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 30 | $36.46 | $2.08 | — | $4,655.17 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+2.9; leftover $1129.59 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 60 | $18.61 | $2.17 | — | $3,536.40 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1129.59 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 62 | $18.21 | $2.18 | — | $2,405.20 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer; ret5=-19.1; leftover $1129.59 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 16 | $68.79 | $2.04 | — | $1,302.52 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1129.59 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 192 | $5.87 | $2.57 | — | $172.92 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1129.59 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $172.92 | ▲ close $9,197.32 vs 09:30 $9,063.86 (session +177.88) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $172.92 | ▲ 09:30 equity $9,350.26 vs yday $9,197.32 (+152.94) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 2 | $10.25 | $0.21 | — | $152.21 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $21.61 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 2 | $7.59 | $0.16 | — | $136.87 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $21.61 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 127 | $0.17 | $0.60 | — | $114.68 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $21.61 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 1 | $15.87 | $0.16 | — | $98.65 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $21.61 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.65 | ▲ close $9,401.79 vs 09:30 $9,350.26 (session +52.66) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.65 | ▲ 09:30 equity $9,426.86 vs yday $9,401.79 (+25.07) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 12 | $0.97 | $0.15 | — | $86.86 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $12.33 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 3 | $3.95 | $0.13 | — | $74.88 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $12.33 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $74.88 | ▼ close $9,356.15 vs 09:30 $9,426.86 (session -70.43) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.88 | ▲ 09:30 equity $9,417.77 vs yday $9,356.15 (+61.62) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 4 | $266.76 | $2.02 | $-20.54 | $1,139.90 | ▼ -20.54 after sell → book $9,415.75; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 14 | $76.27 | $2.05 | $-15.98 | $2,205.63 | ▼ -15.98 after sell → book $9,413.70; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAH` | 78 | $13.65 | $2.25 | $-55.95 | $3,268.08 | ▼ -55.95 after sell → book $9,411.45; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 30 | $36.70 | $2.10 | $+3.02 | $4,366.98 | ▲ +3.02 after sell → book $9,409.35; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BBNX` | 60 | $22.11 | $2.19 | $+205.64 | $5,691.39 | ▲ +205.64 after sell → book $9,407.16; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARQQ` | 62 | $20.55 | $2.20 | $+140.71 | $6,963.29 | ▲ +140.71 after sell → book $9,404.96; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 16 | $79.08 | $2.06 | $+160.54 | $8,226.52 | ▲ +160.54 after sell → book $9,402.91; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 192 | $5.62 | $2.61 | $-53.17 | $9,302.95 | ▼ -53.17 after sell → book $9,400.30; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 7 | $157.87 | $2.01 | — | $8,195.85 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+6.5; leftover $1162.87 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 3 | $386.20 | $2.00 | — | $7,035.25 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=-5.8; leftover $1162.87 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 13 | $88.83 | $2.03 | — | $5,878.43 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+7.6; leftover $1162.87 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 124 | $9.31 | $2.36 | — | $4,721.63 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1162.87 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 86 | $13.47 | $2.25 | — | $3,560.53 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1162.87 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1047 | $1.11 | $13.51 | — | $2,384.85 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1162.87 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 116 | $9.99 | $2.34 | — | $1,223.67 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1162.87 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 637 | $1.82 | $8.22 | — | $52.93 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1162.87 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.93 | ▼ close $9,257.36 vs 09:30 $9,417.77 (session -108.22) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.93 | ▼ 09:30 equity $9,233.30 vs yday $9,257.36 (-24.06) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 2 | $10.18 | $0.23 | $-0.58 | $73.06 | ▼ -0.58 after sell → book $9,233.07; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `DVLT` | 127 | $0.16 | $0.62 | $-2.48 | $92.77 | ▼ -2.48 after sell → book $9,232.45; vs 09:30 mark -0.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 19 | $0.58 | $0.17 | — | $81.58 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $11.60 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.58 | ▲ close $9,435.50 vs 09:30 $9,233.30 (session +203.22) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.58 | ▼ 09:30 equity $9,425.56 vs yday $9,435.50 (-9.94) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BRUN` | 1 | $17.10 | $0.19 | $+0.87 | $98.49 | ▲ +0.87 after sell → book $9,425.37; vs 09:30 mark -0.19 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TLSA` | 12 | $0.89 | $0.16 | $-1.28 | $109.00 | ▼ -1.28 after sell → book $9,425.20; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `EYPT` | 3 | $4.10 | $0.15 | $+0.17 | $121.15 | ▲ +0.17 after sell → book $9,425.05; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 2 | $9.81 | $0.20 | — | $101.33 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+4.0; leftover $24.23 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 1 | $20.25 | $0.21 | — | $80.87 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+15.0; leftover $24.23 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 1 | $20.65 | $0.21 | — | $60.01 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $24.23 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.01 | ▼ close $9,093.14 vs 09:30 $9,425.56 (session -331.29) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.01 | ▼ 09:30 equity $9,019.38 vs yday $9,093.14 (-73.76) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 2 | $7.38 | $0.17 | $-0.75 | $74.60 | ▼ -0.75 after sell → book $9,019.21; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 7 | $163.95 | $2.03 | $+38.52 | $1,220.22 | ▲ +38.52 after sell → book $9,017.18; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `HUM` | 3 | $374.54 | $2.02 | $-39.00 | $2,341.82 | ▼ -39.00 after sell → book $9,015.16; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 13 | $87.67 | $2.05 | $-19.09 | $3,479.55 | ▼ -19.09 after sell → book $9,013.11; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 124 | $8.67 | $2.39 | $-84.11 | $4,552.23 | ▼ -84.11 after sell → book $9,010.72; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 86 | $12.26 | $2.27 | $-109.01 | $5,604.32 | ▼ -109.01 after sell → book $9,008.44; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ORBS` | 1047 | $1.05 | $13.69 | $-90.02 | $6,689.98 | ▼ -90.02 after sell → book $8,994.75; vs 09:30 mark -13.69 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SBET` | 116 | $9.80 | $2.37 | $-26.75 | $7,824.41 | ▼ -26.75 after sell → book $8,992.39; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTBT` | 637 | $1.73 | $8.33 | $-80.25 | $8,914.91 | ▼ -80.25 after sell → book $8,984.05; vs 09:30 mark -8.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,914.91 | ▲ close $8,984.20 vs 09:30 $9,019.38 (session +0.15) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,431.89 | ▲ 09:30 equity $7,937.43 vs yday $7,936.47 (+0.96) | 09:30 open · cash $7,431.89 (unchanged overnight, no fees) · equity $7,937.43 vs prior close $7,936.47 (+0.96) · 11 name(s) re-marked at the open (per-name table). ADMA×4 yday $9.52 → 09:30 $9.52 +0.00; APPS×5 yday $10.88 → 09:30 $10.88 +0.00; ARQT×1 yday $26.27 → 09:30 $26.27 +0.00; DEFT×119 yday $0.53 → 09:30 $0.53 +0.00; DLO×4 yday $13.88 → 09:30 $13.88 +0.00; FTRE×2 yday $20.02 → 09:30 $20.02 +0.00; MKC×1 yday $47.82 → 09:30 $47.82 +0.00; OMER×2 yday $20.13 → 09:30 $20.61 +0.96; PACS×1 yday $41.46 → 09:30 $41.46 +0.00; PGEN×5 yday $7.70 → 09:30 $7.70 +0.00; TDC×2 yday $29.46 → 09:30 $29.46 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 1 | $803.87 | $1.99 | — | $6,626.03 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+0.8; leftover $1061.70 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 9 | $115.36 | $2.02 | — | $5,585.77 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1061.70 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 27 | $38.51 | $2.07 | — | $4,543.93 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+4.7; leftover $1061.70 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 138 | $7.65 | $2.40 | — | $3,485.82 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1061.70 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 40 | $26.27 | $2.11 | — | $2,432.91 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1061.70 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $1,425.77 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1061.70 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 117 | $9.05 | $2.34 | — | $364.58 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer; ret5=-27.1; leftover $1061.70 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $364.58 | ▲ close $7,962.27 vs 09:30 $7,937.43 (session +39.80) | 16:00 close · cash $364.58 · equity $7,962.27 vs 09:30 $7,937.43 (+24.84; session marks +39.80) · 18 name(s) marked open→close (per-name table). ADMA×4 09:30 $9.52 → close $9.52 +0.00; APPS×5 09:30 $10.88 → close $10.88 +0.00; ARQT×1 09:30 $26.27 → close $26.27 +0.00; DEFT×119 09:30 $0.53 → close $0.53 +0.00; DLO×4 09:30 $13.88 → close $13.88 +0.00; FTRE×2 09:30 $20.02 → close $20.02 +0.00; MKC×1 09:30 $47.82 → close $47.82 -0.00; OMER×2 09:30 $20.61 → close $20.08 -1.06; PACS×1 09:30 $41.46 → close $41.46 -0.00; PGEN×5 09:30 $7.70 → close $7.70 -0.00; TDC×2 09:30 $29.46 → close $29.46 -0.00; REGN×1 09:30 $803.87 → close $788.04 -15.83; HALO×9 09:30 $115.36 → close $113.90 -13.14; BLFS×27 09:30 $38.51 → close $38.49 -0.54; MRVI×138 09:30 $7.65 → close $7.60 -6.90; WRBY×40 09:30 $26.27 → close $26.71 +17.60; TXG×12 09:30 $83.76 → close $85.71 +23.40; AEHL×117 09:30 $9.05 → close $9.36 +36.27 | — |

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
| 2026-08-17 | `ELF` | cash | leftover split 70.02 < 1 share @ 90.54 |
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
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 9.25 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 9.25 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 9.25 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 9.25 < 1 share @ 11.13 |
| 2026-08-21 | `CRSP` | cash | leftover split 9.25 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 4.27 < 1 share @ 41.44 |
| 2026-08-27 | `CRK` | cash | leftover split 4.27 < 1 share @ 14.42 |
| 2026-08-27 | `KURA` | cash | leftover split 4.27 < 1 share @ 12.98 |
| 2026-08-27 | `ABX` | cash | leftover split 4.27 < 1 share @ 9.68 |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `OVID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SANM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `COHU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CAN` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OVID` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `COHU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `IQV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BBNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ARQQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 21.61 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 21.61 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 21.61 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 21.61 < 1 share @ 34.93 |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ARQQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 12.33 < 1 share @ 108.55 |
| 2026-09-18 | `GNRC` | cash | leftover split 12.33 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 12.33 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 12.33 < 1 share @ 85.00 |
| 2026-09-18 | `FIVN` | cash | leftover split 12.33 < 1 share @ 34.44 |
| 2026-09-18 | `BHVN` | cash | leftover split 12.33 < 1 share @ 14.07 |
| 2026-09-21 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BRUN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `HUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 11.60 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-23 | `HUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `HALO` | cash | leftover split 24.23 < 1 share @ 116.85 |
| 2026-09-23 | `ARQT` | cash | leftover split 24.23 < 1 share @ 27.79 |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DEFT` | 19 | 2026-09-22 @ $0.58 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $11.60 |
| `ADMA` | 2 | 2026-09-23 @ $9.81 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+4.0; leftover $24.23 |
| `FTRE` | 1 | 2026-09-23 @ $20.25 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+15.0; leftover $24.23 |
| `OMER` | 1 | 2026-09-23 @ $20.65 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $24.23 |
