# Factor mine action — `union_ab_g_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ ab_g, no 🚨

Cash book **-24.33%** ($7,567) · signal-only (no cash/fees) was -10.05%. Starts YES **2/30**. Fills 158 · skips 241 · realized $-1471.05.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the A/B camera (does our A/B score like this name?) is green.
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
- **Gate** `ab=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6,916.41.

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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,764.83 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,579.67 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,338.50 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 216 | $5.77 | $2.79 | — | $5,089.39 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,850.53 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,603.95 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 714 | $1.75 | $9.21 | — | $1,345.24 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $186.91 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $186.91 | ▲ close $10,208.28 vs 09:30 $10,000.00 (session +232.95) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.91 | ▲ 09:30 equity $10,475.50 vs yday $10,208.28 (+267.22) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $169.53 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $23.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $147.04 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $23.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 9 | $2.47 | $0.25 | — | $124.56 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $23.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 12 | $1.93 | $0.27 | — | $101.13 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $23.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 17 | $1.32 | $0.28 | — | $78.42 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $23.36 | — |
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
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 53 | $23.77 | $2.15 | — | $8,981.90 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ⚪; ret5=+13.0; leftover $1280.48 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 116 | $10.98 | $2.34 | — | $7,705.88 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+1.2; leftover $1280.48 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 20 | $61.19 | $2.05 | — | $6,480.03 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+7.4; leftover $1280.48 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 153 | $8.35 | $2.45 | — | $5,200.03 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1280.48 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 259 | $4.94 | $3.34 | — | $3,917.23 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+7.1; leftover $1280.48 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $426.97 | $2.00 | — | $3,061.29 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+6.0; leftover $1280.48 | — |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 115 | $11.12 | $2.33 | — | $1,780.16 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; ret5=-0.7; leftover $1280.48 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 94 | $13.59 | $2.27 | — | $500.43 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1280.48 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $500.43 | ▲ close $10,411.59 vs 09:30 $10,384.32 (session +71.17) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $500.43 | ▼ 09:30 equity $10,408.60 vs yday $10,411.59 (-2.99) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $516.84 | ▼ -0.96 after sell → book $10,408.41; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $547.20 | ▲ +7.88 after sell → book $10,408.07; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 9 | $2.41 | $0.26 | $-1.05 | $568.63 | ▼ -1.05 after sell → book $10,407.81; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 12 | $2.03 | $0.30 | $+0.63 | $592.69 | ▲ +0.63 after sell → book $10,407.51; vs 09:30 mark -0.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 17 | $1.60 | $0.34 | $+4.14 | $619.55 | ▲ +4.14 after sell → book $10,407.17; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 9 | $31.21 | $2.02 | — | $336.64 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $309.77 | — |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 31 | $9.83 | $2.08 | — | $29.83 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $309.77 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.83 | ▼ close $10,308.16 vs 09:30 $10,408.60 (session -94.91) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.83 | ▼ 09:30 equity $10,289.04 vs yday $10,308.16 (-19.12) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 2 | $2.60 | $0.06 | — | $24.57 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; ret5=+13.0; leftover $7.46 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.57 | ▼ close $10,183.83 vs 09:30 $10,289.04 (session -105.15) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.57 | ▲ 09:30 equity $10,190.41 vs yday $10,183.83 (+6.58) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 116 | $10.97 | $2.37 | $-5.87 | $1,294.72 | ▼ -5.87 after sell → book $10,188.04; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 20 | $60.52 | $2.07 | $-17.52 | $2,503.05 | ▼ -17.52 after sell → book $10,185.97; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 153 | $8.28 | $2.48 | $-15.64 | $3,767.41 | ▼ -15.64 after sell → book $10,183.49; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 259 | $4.95 | $3.39 | $-4.15 | $5,046.06 | ▼ -4.15 after sell → book $10,180.09; vs 09:30 mark -3.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 2 | $423.76 | $2.02 | $-10.43 | $5,891.57 | ▼ -10.43 after sell → book $10,178.08; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `VITL` | 115 | $10.47 | $2.36 | $-79.45 | $7,093.25 | ▼ -79.45 after sell → book $10,175.71; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 94 | $13.05 | $2.30 | $-55.33 | $8,317.66 | ▼ -55.33 after sell → book $10,173.42; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 33 | $41.74 | $2.09 | — | $6,938.15 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+2.4; leftover $1386.28 | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 94 | $14.63 | $2.27 | — | $5,560.66 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+5.8; leftover $1386.28 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 88 | $15.66 | $2.25 | — | $4,180.32 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1386.28 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 17 | $79.42 | $2.04 | — | $2,828.14 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1386.28 | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $1,564.94 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1386.28 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $248.29 | — | union ∩ ab_g, no 🚨; gate ab=good; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1386.28 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $248.29 | ▼ close $10,000.54 vs 09:30 $10,190.41 (session -160.20) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $248.29 | ▲ 09:30 equity $10,038.37 vs yday $10,000.54 (+37.83) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 53 | $23.68 | $2.17 | $-9.09 | $1,501.16 | ▼ -9.09 after sell → book $10,036.20; vs 09:30 mark -2.17 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVBP` | 9 | $29.94 | $2.04 | $-15.48 | $1,768.58 | ▼ -15.48 after sell → book $10,034.16; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ABX` | 31 | $9.74 | $2.10 | $-6.98 | $2,068.42 | ▼ -6.98 after sell → book $10,032.06; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,068.42 | ▼ close $10,010.91 vs 09:30 $10,038.37 (session -21.15) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,068.42 | ▲ 09:30 equity $10,066.35 vs yday $10,010.91 (+55.44) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 2 | $2.67 | $0.08 | $+0.00 | $2,073.68 | ▼ +0.00 after sell → book $10,066.27; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,073.68 | ▼ close $10,019.48 vs 09:30 $10,066.35 (session -46.79) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,073.68 | ▼ 09:30 equity $9,939.42 vs yday $10,019.48 (-80.06) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 33 | $42.10 | $2.11 | $+7.68 | $3,460.87 | ▲ +7.68 after sell → book $9,937.31; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 94 | $15.70 | $2.30 | $+96.01 | $4,934.37 | ▲ +96.01 after sell → book $9,935.01; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 88 | $13.92 | $2.28 | $-157.65 | $6,157.05 | ▼ -157.65 after sell → book $9,932.73; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 17 | $78.84 | $2.06 | $-13.96 | $7,495.27 | ▼ -13.96 after sell → book $9,930.67; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SIMO` | 5 | $235.71 | $2.02 | $-86.68 | $8,671.80 | ▼ -86.68 after sell → book $9,928.65; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 9 | $139.65 | $2.04 | $-61.83 | $9,926.61 | ▼ -61.83 after sell → book $9,926.61; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,926.61 | ▲ close $9,926.61 vs 09:30 $9,939.42 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,926.61 | ▲ 09:30 equity $9,926.61 vs yday $9,926.61 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $8,708.31 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1240.83 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 28 | $42.93 | $2.07 | — | $7,504.19 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1240.83 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 341 | $3.63 | $4.40 | — | $6,261.97 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1240.83 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 154 | $8.03 | $2.45 | — | $5,022.89 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1240.83 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $3,828.83 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1240.83 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 80 | $15.45 | $2.23 | — | $2,590.60 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1240.83 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $1,421.02 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1240.83 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 73 | $16.77 | $2.21 | — | $194.60 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1240.83 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $194.60 | ▼ close $9,687.37 vs 09:30 $9,926.61 (session -219.78) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $194.60 | ▲ 09:30 equity $9,690.05 vs yday $9,687.37 (+2.68) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 12 | $2.52 | $0.34 | — | $164.03 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $32.43 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 4 | $6.71 | $0.28 | — | $136.91 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $32.43 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 17 | $1.90 | $0.37 | — | $104.23 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $32.43 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 6 | $4.78 | $0.30 | — | $75.25 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $32.43 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 20 | $1.59 | $0.38 | — | $43.07 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $32.43 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 2 | $11.31 | $0.23 | — | $20.22 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $32.43 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.22 | ▲ close $9,718.11 vs 09:30 $9,690.05 (session +29.96) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.22 | ▲ 09:30 equity $9,746.71 vs yday $9,718.11 (+28.60) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.22 | ▼ close $9,581.28 vs 09:30 $9,746.71 (session -165.43) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.22 | ▼ 09:30 equity $9,533.95 vs yday $9,581.28 (-47.33) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 23 | $53.16 | $2.08 | $+2.30 | $1,240.82 | ▲ +2.30 after sell → book $9,531.87; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 28 | $42.01 | $2.09 | $-29.93 | $2,415.00 | ▼ -29.93 after sell → book $9,529.77; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 341 | $3.28 | $4.47 | $-128.21 | $3,529.02 | ▼ -128.21 after sell → book $9,525.31; vs 09:30 mark -4.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 154 | $8.01 | $2.49 | $-8.02 | $4,760.07 | ▼ -8.02 after sell → book $9,522.82; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $5,889.96 | ▼ -64.17 after sell → book $9,520.78; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 80 | $15.16 | $2.25 | $-27.68 | $7,100.51 | ▼ -27.68 after sell → book $9,518.53; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 8 | $140.29 | $2.03 | $-49.25 | $8,220.84 | ▼ -49.25 after sell → book $9,516.50; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 73 | $15.46 | $2.23 | $-100.07 | $9,347.18 | ▼ -100.07 after sell → book $9,514.26; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,347.18 | ▼ close $9,506.94 vs 09:30 $9,533.95 (session -7.32) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,347.18 | ▼ 09:30 equity $9,504.65 vs yday $9,506.94 (-2.29) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 12 | $2.22 | $0.32 | $-4.26 | $9,373.50 | ▼ -4.26 after sell → book $9,504.32; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 4 | $6.11 | $0.28 | $-2.96 | $9,397.67 | ▼ -2.96 after sell → book $9,504.05; vs 09:30 mark -0.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 17 | $1.83 | $0.38 | $-1.95 | $9,428.39 | ▼ -1.95 after sell → book $9,503.67; vs 09:30 mark -0.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 6 | $3.92 | $0.27 | $-5.73 | $9,451.65 | ▼ -5.73 after sell → book $9,503.39; vs 09:30 mark -0.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 20 | $1.53 | $0.39 | $-1.96 | $9,481.87 | ▼ -1.96 after sell → book $9,503.01; vs 09:30 mark -0.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 2 | $10.57 | $0.24 | $-1.95 | $9,502.77 | ▼ -1.95 after sell → book $9,502.77; vs 09:30 mark -0.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,502.77 | ▲ close $9,502.77 vs 09:30 $9,504.65 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,502.77 | ▲ 09:30 equity $9,502.77 vs yday $9,502.77 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 72 | $16.28 | $2.21 | — | $8,328.40 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=-1.1; leftover $1187.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 435 | $2.73 | $5.61 | — | $7,135.24 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=-3.0; leftover $1187.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 5 | $206.84 | $2.00 | — | $6,099.04 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+8.3; leftover $1187.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $4,946.02 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1187.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 7 | $157.78 | $2.01 | — | $3,839.54 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+4.7; leftover $1187.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 21 | $56.09 | $2.05 | — | $2,659.60 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+19.6; leftover $1187.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 582 | $2.04 | $7.51 | — | $1,464.81 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1187.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 250 | $4.75 | $3.23 | — | $274.09 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1187.85 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $274.09 | ▼ close $9,445.56 vs 09:30 $9,502.77 (session -30.58) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $274.09 | ▼ 09:30 equity $9,170.76 vs yday $9,445.56 (-274.80) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $274.09 | ▼ close $9,126.99 vs 09:30 $9,170.76 (session -43.77) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $274.09 | ▲ 09:30 equity $9,153.23 vs yday $9,126.99 (+26.24) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $274.09 | ▼ close $8,908.39 vs 09:30 $9,153.23 (session -244.84) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $274.09 | ▲ 09:30 equity $8,969.32 vs yday $8,908.39 (+60.93) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AUPH` | 72 | $16.16 | $2.23 | $-13.07 | $1,435.38 | ▼ -13.07 after sell → book $8,967.09; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `OVID` | 435 | $2.72 | $5.69 | $-15.66 | $2,612.89 | ▼ -15.66 after sell → book $8,961.40; vs 09:30 mark -5.69 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 5 | $194.84 | $2.02 | $-64.03 | $3,585.06 | ▼ -64.03 after sell → book $8,959.37; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 7 | $140.03 | $2.03 | $-174.84 | $4,563.24 | ▼ -174.84 after sell → book $8,957.34; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 7 | $147.79 | $2.03 | $-73.97 | $5,595.74 | ▼ -73.97 after sell → book $8,955.31; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COHU` | 21 | $51.29 | $2.07 | $-104.93 | $6,670.76 | ▼ -104.93 after sell → book $8,953.24; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 582 | $1.89 | $7.61 | $-102.42 | $7,763.12 | ▼ -102.42 after sell → book $8,945.62; vs 09:30 mark -7.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 250 | $4.73 | $3.28 | $-11.50 | $8,942.35 | ▼ -11.50 after sell → book $8,942.35; vs 09:30 mark -3.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $7,856.78 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+4.0; leftover $1117.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 14 | $77.12 | $2.03 | — | $6,775.07 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; ret5=+7.2; leftover $1117.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 78 | $14.31 | $2.22 | — | $5,656.67 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+4.8; leftover $1117.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 30 | $36.46 | $2.08 | — | $4,560.79 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+2.9; leftover $1117.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 16 | $68.79 | $2.04 | — | $3,458.11 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1117.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 190 | $5.87 | $2.56 | — | $2,340.25 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1117.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 12 | $87.40 | $2.03 | — | $1,289.42 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1117.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 29 | $38.01 | $2.08 | — | $185.06 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $1117.79 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $185.06 | ▼ close $8,750.46 vs 09:30 $8,969.32 (session -174.85) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $185.06 | ▲ 09:30 equity $8,886.27 vs yday $8,750.46 (+135.81) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 2 | $10.25 | $0.21 | — | $164.35 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $23.13 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 3 | $7.59 | $0.24 | — | $141.34 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $23.13 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $141.34 | ▲ close $8,938.84 vs 09:30 $8,886.27 (session +53.02) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $141.34 | ▲ 09:30 equity $8,985.65 vs yday $8,938.84 (+46.81) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 3 | $5.83 | $0.18 | — | $123.67 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $17.67 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 4 | $3.58 | $0.16 | — | $109.19 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $17.67 | — |
| 2026-09-18 09:30 ET | **BUY** | `RANI` | 20 | $0.85 | $0.23 | — | $91.96 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; ret5=+3.6; leftover $17.67 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $91.96 | ▼ close $8,841.05 vs 09:30 $8,985.65 (session -144.03) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $91.96 | ▲ 09:30 equity $8,875.53 vs yday $8,841.05 (+34.48) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 4 | $266.76 | $2.02 | $-20.54 | $1,156.98 | ▼ -20.54 after sell → book $8,873.50; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 14 | $76.27 | $2.05 | $-15.98 | $2,222.71 | ▼ -15.98 after sell → book $8,871.45; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAH` | 78 | $13.65 | $2.25 | $-55.95 | $3,285.16 | ▼ -55.95 after sell → book $8,869.20; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 30 | $36.70 | $2.10 | $+3.02 | $4,384.06 | ▲ +3.02 after sell → book $8,867.10; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 16 | $79.08 | $2.06 | $+160.54 | $5,647.28 | ▲ +160.54 after sell → book $8,865.05; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 190 | $5.62 | $2.60 | $-52.66 | $6,712.48 | ▼ -52.66 after sell → book $8,862.44; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `VAL` | 12 | $83.46 | $2.05 | $-51.35 | $7,711.95 | ▼ -51.35 after sell → book $8,860.40; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `KRMN` | 29 | $36.30 | $2.10 | $-53.76 | $8,762.56 | ▼ -53.76 after sell → book $8,858.30; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 6 | $157.87 | $2.01 | — | $7,813.33 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+6.5; leftover $1095.32 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 2 | $386.20 | $2.00 | — | $7,038.93 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=-5.8; leftover $1095.32 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 12 | $88.83 | $2.03 | — | $5,970.95 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+7.6; leftover $1095.32 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 117 | $9.31 | $2.34 | — | $4,879.34 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1095.32 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 81 | $13.47 | $2.23 | — | $3,785.63 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1095.32 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 986 | $1.11 | $12.72 | — | $2,678.45 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1095.32 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 42 | $25.95 | $2.12 | — | $1,586.43 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1095.32 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,586.43 | ▼ close $8,742.12 vs 09:30 $8,875.53 (session -90.73) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,586.43 | ▼ 09:30 equity $8,741.29 vs yday $8,742.12 (-0.83) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 2 | $10.18 | $0.23 | $-0.58 | $1,606.56 | ▼ -0.58 after sell → book $8,741.06; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 2 | $93.97 | $1.89 | — | $1,416.74 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=-0.6; leftover $200.82 | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 346 | $0.58 | $3.04 | — | $1,213.01 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $200.82 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,213.01 | ▲ close $8,837.19 vs 09:30 $8,741.29 (session +101.06) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,213.01 | ▲ 09:30 equity $8,888.22 vs yday $8,837.19 (+51.03) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BNC` | 3 | $6.29 | $0.22 | $+0.98 | $1,231.67 | ▲ +0.98 after sell → book $8,888.00; vs 09:30 mark -0.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `DDD` | 4 | $3.59 | $0.18 | $-0.29 | $1,245.85 | ▼ -0.29 after sell → book $8,887.83; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RANI` | 20 | $0.81 | $0.24 | $-1.27 | $1,261.81 | ▼ -1.27 after sell → book $8,887.59; vs 09:30 mark -0.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 2 | $116.85 | $2.00 | — | $1,026.11 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+3.3; leftover $252.36 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 9 | $27.79 | $2.02 | — | $773.98 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+7.0; leftover $252.36 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 25 | $9.81 | $2.06 | — | $526.67 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+4.0; leftover $252.36 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 12 | $20.65 | $2.03 | — | $276.84 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $252.36 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLLN` | 2 | $116.00 | $2.00 | — | $42.85 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $252.36 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.85 | ▼ close $8,570.77 vs 09:30 $8,888.22 (session -306.72) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.85 | ▼ 09:30 equity $8,483.96 vs yday $8,570.77 (-86.81) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 3 | $7.38 | $0.25 | $-1.12 | $64.74 | ▼ -1.12 after sell → book $8,483.71; vs 09:30 mark -0.25 | dropped from list after 5 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 6 | $163.95 | $2.03 | $+32.44 | $1,046.41 | ▲ +32.44 after sell → book $8,481.68; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `HUM` | 2 | $374.54 | $2.02 | $-27.33 | $1,793.47 | ▼ -27.33 after sell → book $8,479.66; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 12 | $87.67 | $2.05 | $-17.93 | $2,843.53 | ▼ -17.93 after sell → book $8,477.62; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 117 | $8.67 | $2.37 | $-79.59 | $3,855.55 | ▼ -79.59 after sell → book $8,475.25; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 81 | $12.26 | $2.26 | $-102.90 | $4,846.35 | ▼ -102.90 after sell → book $8,472.99; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ORBS` | 986 | $1.05 | $12.89 | $-84.77 | $5,868.76 | ▼ -84.77 after sell → book $8,460.10; vs 09:30 mark -12.89 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GLXY` | 42 | $25.00 | $2.14 | $-44.36 | $6,916.41 | ▼ -44.36 after sell → book $8,457.96; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,916.41 | ▲ close $8,484.35 vs 09:30 $8,483.96 (session +26.39) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,126.65 | ▲ 09:30 equity $7,548.08 vs yday $7,547.60 (+0.48) | 09:30 open · cash $7,126.65 (unchanged overnight, no fees) · equity $7,548.08 vs prior close $7,547.60 (+0.48) · 11 name(s) re-marked at the open (per-name table). ADMA×3 yday $9.52 → 09:30 $9.52 +0.00; APPS×4 yday $10.88 → 09:30 $10.88 +0.00; ARHS×6 yday $9.47 → 09:30 $9.47 +0.00; ARQT×1 yday $26.27 → 09:30 $26.27 +0.00; DEFT×94 yday $0.53 → 09:30 $0.53 +0.00; DLO×3 yday $13.88 → 09:30 $13.88 +0.00; MKC×1 yday $47.82 → 09:30 $47.82 +0.00; OMER×1 yday $20.13 → 09:30 $20.61 +0.48; PACS×1 yday $41.46 → 09:30 $41.46 +0.00; PGEN×4 yday $7.70 → 09:30 $7.70 +0.00; TLYS×8 yday $4.24 → 09:30 $4.24 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 1 | $803.87 | $1.99 | — | $6,320.79 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+0.8; leftover $1018.09 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 8 | $115.36 | $2.01 | — | $5,395.89 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1018.09 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 26 | $38.51 | $2.07 | — | $4,392.56 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+4.7; leftover $1018.09 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 133 | $7.65 | $2.39 | — | $3,372.73 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1018.09 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 38 | $26.27 | $2.10 | — | $2,372.36 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1018.09 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $1,365.22 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1018.09 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 169 | $6.00 | $2.50 | — | $348.72 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $1018.09 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $348.72 | ▲ close $7,566.63 vs 09:30 $7,548.08 (session +33.64) | 16:00 close · cash $348.72 · equity $7,566.63 vs 09:30 $7,548.08 (+18.55; session marks +33.64) · 18 name(s) marked open→close (per-name table). ADMA×3 09:30 $9.52 → close $9.52 +0.00; APPS×4 09:30 $10.88 → close $10.88 +0.00; ARHS×6 09:30 $9.47 → close $9.47 +0.00; ARQT×1 09:30 $26.27 → close $26.27 +0.00; DEFT×94 09:30 $0.53 → close $0.53 +0.00; DLO×3 09:30 $13.88 → close $13.88 +0.00; MKC×1 09:30 $47.82 → close $47.82 -0.00; OMER×1 09:30 $20.61 → close $20.08 -0.53; PACS×1 09:30 $41.46 → close $41.46 -0.00; PGEN×4 09:30 $7.70 → close $7.70 -0.00; TLYS×8 09:30 $4.24 → close $4.24 -0.00; REGN×1 09:30 $803.87 → close $788.04 -15.83; HALO×8 09:30 $115.36 → close $113.90 -11.68; BLFS×26 09:30 $38.51 → close $38.49 -0.52; MRVI×133 09:30 $7.65 → close $7.60 -6.65; WRBY×38 09:30 $26.27 → close $26.71 +16.72; TXG×12 09:30 $83.76 → close $85.71 +23.40; SATL×169 09:30 $6.00 → close $6.17 +28.73 | — |

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
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `QSI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `VITL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `VITL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 7.46 < 1 share @ 41.44 |
| 2026-08-27 | `CRK` | cash | leftover split 7.46 < 1 share @ 14.42 |
| 2026-08-27 | `ITG` | cash | leftover split 7.46 < 1 share @ 12.36 |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
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
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
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
| 2026-09-09 | `CNTB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HAS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `OVID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SANM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `COHU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VICR` | hard_red | hard-red S=-11.00 sit; no new buys |
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
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `IQV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `VAL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `KRMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 23.13 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 23.13 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 23.13 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 23.13 < 1 share @ 34.93 |
| 2026-09-17 | `AXTI` | cash | leftover split 23.13 < 1 share @ 67.91 |
| 2026-09-17 | `ARQT` | cash | leftover split 23.13 < 1 share @ 25.95 |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `VAL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `KRMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 17.67 < 1 share @ 108.55 |
| 2026-09-18 | `GNRC` | cash | leftover split 17.67 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 17.67 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 17.67 < 1 share @ 85.00 |
| 2026-09-18 | `FIVN` | cash | leftover split 17.67 < 1 share @ 34.44 |
| 2026-09-21 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `DDD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RANI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SNDK` | cash | leftover split 1095.32 < 1 share @ 1826.00 |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `DDD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RANI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `HUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GLXY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-23 | `HUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GLXY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `USFD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `USFD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BLLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| `USFD` | 2 | 2026-09-22 @ $93.97 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=-0.6; leftover $200.82 |
| `DEFT` | 346 | 2026-09-22 @ $0.58 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $200.82 |
| `HALO` | 2 | 2026-09-23 @ $116.85 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+3.3; leftover $252.36 |
| `ARQT` | 9 | 2026-09-23 @ $27.79 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+7.0; leftover $252.36 |
| `ADMA` | 25 | 2026-09-23 @ $9.81 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+4.0; leftover $252.36 |
| `OMER` | 12 | 2026-09-23 @ $20.65 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $252.36 |
| `BLLN` | 2 | 2026-09-23 @ $116.00 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $252.36 |
