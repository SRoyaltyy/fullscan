# Factor mine action — `union_ab_g_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ ab_g, no 🚨

Cash book **-15.04%** ($8,496) · signal-only (no cash/fees) was -11.54%. Starts YES **0/30**. Fills 280 · skips 92 · realized $-226.14.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ab=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,773.83.

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
| 2026-08-21 09:30 ET | **SELL** | `AG` | 60 | $21.90 | $2.19 | $+76.64 | $1,498.71 | ▲ +76.64 after sell → book $10,473.30; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,741.03 | ▲ +57.15 after sell → book $10,471.26; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 60 | $21.75 | $2.19 | $+61.64 | $4,043.84 | ▲ +61.64 after sell → book $10,469.07; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 216 | $5.67 | $2.83 | $-27.22 | $5,265.72 | ▼ -27.22 after sell → book $10,466.23; vs 09:30 mark -2.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 63 | $21.17 | $2.20 | $+92.64 | $6,597.23 | ▲ +92.64 after sell → book $10,464.03; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $7,946.24 | ▲ +102.43 after sell → book $10,461.90; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 714 | $1.79 | $9.34 | $+10.01 | $9,214.96 | ▲ +10.01 after sell → book $10,452.56; vs 09:30 mark -9.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,450.52 | ▲ +77.23 after sell → book $10,450.52; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,254.20 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1306.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 75 | $17.20 | $2.21 | — | $7,961.99 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1306.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,662.18 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1306.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 117 | $11.13 | $2.34 | — | $5,357.63 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1306.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 528 | $2.47 | $6.81 | — | $4,046.66 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1306.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 676 | $1.93 | $8.72 | — | $2,733.26 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1306.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,477.08 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1306.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 989 | $1.32 | $12.76 | — | $158.85 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1306.32 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.85 | ▲ close $10,673.53 vs 09:30 $10,475.50 (session +261.93) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.85 | ▲ 09:30 equity $11,050.19 vs yday $10,673.53 (+376.66) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,361.91 | ▲ +6.74 after sell → book $11,048.15; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 75 | $16.57 | $2.24 | $-51.70 | $2,602.42 | ▼ -51.70 after sell → book $11,045.91; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,902.57 | ▲ +0.34 after sell → book $11,043.88; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 117 | $13.33 | $2.37 | $+252.69 | $5,459.81 | ▲ +252.69 after sell → book $11,041.51; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 528 | $2.40 | $6.91 | $-50.68 | $6,720.10 | ▼ -50.68 after sell → book $11,034.60; vs 09:30 mark -6.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 676 | $1.88 | $8.84 | $-51.36 | $7,982.14 | ▼ -51.36 after sell → book $11,025.76; vs 09:30 mark -8.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.75 | $2.07 | $-24.50 | $9,213.81 | ▼ -24.50 after sell → book $11,023.68; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 989 | $1.83 | $12.94 | $+478.70 | $11,010.75 | ▲ +478.70 after sell → book $11,010.75; vs 09:30 mark -12.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,010.75 | ▲ close $11,010.75 vs 09:30 $11,050.19 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,010.75 | ▲ 09:30 equity $11,010.75 vs yday $11,010.75 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 57 | $23.77 | $2.16 | — | $9,653.69 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ⚪; ret5=+13.0; leftover $1376.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 125 | $10.98 | $2.37 | — | $8,278.83 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+1.2; leftover $1376.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.19 | $2.06 | — | $6,930.59 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+7.4; leftover $1376.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 164 | $8.35 | $2.48 | — | $5,558.71 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1376.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 278 | $4.94 | $3.59 | — | $4,181.81 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+7.1; leftover $1376.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $2,898.90 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+6.0; leftover $1376.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 123 | $11.12 | $2.36 | — | $1,528.78 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; ret5=-0.7; leftover $1376.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 101 | $13.59 | $2.29 | — | $153.89 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1376.34 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $153.89 | ▲ close $11,062.84 vs 09:30 $11,010.75 (session +71.40) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $153.89 | ▼ 09:30 equity $11,059.06 vs yday $11,062.84 (-3.78) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `VITL` | 123 | $11.03 | $2.39 | $-15.82 | $1,508.19 | ▼ -15.82 after sell → book $11,056.67; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 101 | $13.63 | $2.32 | $-0.57 | $2,882.50 | ▼ -0.57 after sell → book $11,054.35; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 46 | $31.21 | $2.13 | — | $1,444.72 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1441.25 | — |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 146 | $9.83 | $2.43 | — | $7.11 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1441.25 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.11 | ▼ close $11,008.96 vs 09:30 $11,059.06 (session -40.84) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.11 | ▼ 09:30 equity $10,975.33 vs yday $11,008.96 (-33.63) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 125 | $10.63 | $2.40 | $-48.51 | $1,333.46 | ▼ -48.51 after sell → book $10,972.93; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 22 | $62.10 | $2.08 | $+15.89 | $2,697.58 | ▲ +15.89 after sell → book $10,970.85; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 164 | $8.49 | $2.52 | $+17.96 | $4,087.42 | ▲ +17.96 after sell → book $10,968.33; vs 09:30 mark -2.52 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 278 | $5.07 | $3.64 | $+28.91 | $5,493.24 | ▲ +28.91 after sell → book $10,964.69; vs 09:30 mark -3.64 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-11.10 | $6,765.05 | ▼ -11.10 after sell → book $10,962.67; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 32 | $41.44 | $2.09 | — | $5,436.88 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+3.1; leftover $1353.01 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 93 | $14.42 | $2.27 | — | $4,093.56 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+7.1; leftover $1353.01 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 520 | $2.60 | $6.71 | — | $2,734.85 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; ret5=+13.0; leftover $1353.01 | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 104 | $12.98 | $2.30 | — | $1,382.63 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $1353.01 | — |
| 2026-08-27 09:30 ET | **BUY** | `ITG` | 109 | $12.36 | $2.32 | — | $33.07 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=-3.0; leftover $1353.01 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.07 | ▲ close $11,087.06 vs 09:30 $10,975.33 (session +140.07) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.07 | ▼ 09:30 equity $11,086.26 vs yday $11,087.06 (-0.80) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AVBP` | 46 | $30.53 | $2.15 | $-35.56 | $1,435.30 | ▼ -35.56 after sell → book $11,084.11; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ABX` | 146 | $9.88 | $2.46 | $+2.41 | $2,875.32 | ▲ +2.41 after sell → book $11,081.65; vs 09:30 mark -2.46 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 104 | $13.05 | $2.33 | $+2.65 | $4,230.19 | ▲ +2.65 after sell → book $11,079.32; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ITG` | 109 | $12.79 | $2.35 | $+42.21 | $5,621.95 | ▲ +42.21 after sell → book $11,076.97; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 89 | $15.66 | $2.26 | — | $4,225.95 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1405.49 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 17 | $79.42 | $2.04 | — | $2,873.77 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1405.49 | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $1,610.57 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1405.49 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $293.92 | — | union ∩ ab_g, no 🚨; gate ab=good; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1405.49 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $293.92 | ▼ close $10,846.66 vs 09:30 $11,086.26 (session -221.99) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $293.92 | ▲ 09:30 equity $10,901.66 vs yday $10,846.66 (+55.00) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 57 | $23.68 | $2.18 | $-9.47 | $1,641.50 | ▼ -9.47 after sell → book $10,899.48; vs 09:30 mark -2.18 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 32 | $42.00 | $2.11 | $+13.73 | $2,983.39 | ▲ +13.73 after sell → book $10,897.37; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 93 | $14.54 | $2.30 | $+6.60 | $4,333.32 | ▲ +6.60 after sell → book $10,895.08; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 520 | $2.58 | $6.81 | $-23.91 | $5,668.11 | ▼ -23.91 after sell → book $10,888.27; vs 09:30 mark -6.81 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 89 | $14.44 | $2.28 | $-113.12 | $6,950.99 | ▼ -113.12 after sell → book $10,885.99; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 17 | $80.44 | $2.06 | $+13.24 | $8,316.41 | ▲ +13.24 after sell → book $10,883.93; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $9,549.63 | ▼ -29.98 after sell → book $10,881.90; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $10,879.86 | ▲ +13.59 after sell → book $10,879.86; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,879.86 | ▲ close $10,879.86 vs 09:30 $10,901.66 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,879.86 | ▲ 09:30 equity $10,879.86 vs yday $10,879.86 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,879.86 | ▲ close $10,879.86 vs 09:30 $10,879.86 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,879.86 | ▲ 09:30 equity $10,879.86 vs yday $10,879.86 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,879.86 | ▲ close $10,879.86 vs 09:30 $10,879.86 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,879.86 | ▲ 09:30 equity $10,879.86 vs yday $10,879.86 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,555.80 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1359.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $42.93 | $2.08 | — | $8,222.89 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1359.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 374 | $3.63 | $4.82 | — | $6,860.44 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1359.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 169 | $8.03 | $2.50 | — | $5,500.87 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1359.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,174.35 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1359.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 88 | $15.45 | $2.25 | — | $2,812.50 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1359.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,496.98 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1359.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 81 | $16.77 | $2.23 | — | $136.38 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1359.98 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $136.38 | ▼ close $10,617.77 vs 09:30 $10,879.86 (session -242.10) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $136.38 | ▲ 09:30 equity $10,621.75 vs yday $10,617.77 (+3.98) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 31 | $41.50 | $2.10 | $-48.52 | $1,420.77 | ▼ -48.52 after sell → book $10,619.64; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 169 | $7.91 | $2.54 | $-25.31 | $2,755.03 | ▼ -25.31 after sell → book $10,617.11; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $4,053.29 | ▼ -28.26 after sell → book $10,615.07; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 88 | $15.00 | $2.28 | $-44.13 | $5,371.01 | ▼ -44.13 after sell → book $10,612.79; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 9 | $153.62 | $2.04 | $+65.02 | $6,751.55 | ▲ +65.02 after sell → book $10,610.75; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 81 | $15.61 | $2.26 | $-98.45 | $8,013.70 | ▼ -98.45 after sell → book $10,608.49; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 530 | $2.52 | $6.84 | — | $6,671.26 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1335.62 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 199 | $6.71 | $2.59 | — | $5,333.39 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1335.62 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 702 | $1.90 | $9.06 | — | $3,990.53 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1335.62 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 279 | $4.78 | $3.60 | — | $2,653.31 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1335.62 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 840 | $1.59 | $10.84 | — | $1,306.88 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1335.62 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 115 | $11.31 | $2.33 | — | $3.89 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1335.62 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.89 | ▼ close $10,518.92 vs 09:30 $10,621.75 (session -54.33) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.89 | ▼ 09:30 equity $10,476.49 vs yday $10,518.92 (-42.43) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 25 | $54.31 | $2.09 | $+31.60 | $1,359.56 | ▲ +31.60 after sell → book $10,474.41; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 374 | $3.43 | $4.90 | $-84.52 | $2,637.48 | ▼ -84.52 after sell → book $10,469.51; vs 09:30 mark -4.90 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 530 | $2.38 | $6.94 | $-87.97 | $3,891.94 | ▼ -87.97 after sell → book $10,462.57; vs 09:30 mark -6.94 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 199 | $6.57 | $2.63 | $-33.08 | $5,196.74 | ▼ -33.08 after sell → book $10,459.94; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 702 | $2.00 | $9.18 | $+51.96 | $6,591.56 | ▲ +51.96 after sell → book $10,450.76; vs 09:30 mark -9.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 279 | $4.30 | $3.66 | $-141.17 | $7,787.60 | ▼ -141.17 after sell → book $10,447.10; vs 09:30 mark -3.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 840 | $1.63 | $10.99 | $+11.78 | $9,145.82 | ▲ +11.78 after sell → book $10,436.12; vs 09:30 mark -10.98 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 115 | $11.22 | $2.36 | $-15.05 | $10,433.75 | ▼ -15.05 after sell → book $10,433.75; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,433.75 | ▲ close $10,433.75 vs 09:30 $10,476.49 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,433.75 | ▲ 09:30 equity $10,433.75 vs yday $10,433.75 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,433.75 | ▲ close $10,433.75 vs 09:30 $10,433.75 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,433.75 | ▲ 09:30 equity $10,433.75 vs yday $10,433.75 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,433.75 | ▲ close $10,433.75 vs 09:30 $10,433.75 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,433.75 | ▲ 09:30 equity $10,433.75 vs yday $10,433.75 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 80 | $16.28 | $2.23 | — | $9,129.12 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=-1.1; leftover $1304.22 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 477 | $2.73 | $6.15 | — | $7,820.76 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=-3.0; leftover $1304.22 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $6,577.71 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+8.3; leftover $1304.22 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $5,424.69 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1304.22 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 8 | $157.78 | $2.01 | — | $4,160.44 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+4.7; leftover $1304.22 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 23 | $56.09 | $2.06 | — | $2,868.31 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+19.6; leftover $1304.22 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 639 | $2.04 | $8.24 | — | $1,556.51 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1304.22 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 274 | $4.75 | $3.53 | — | $251.47 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1304.22 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $251.47 | ▼ close $10,387.51 vs 09:30 $10,433.75 (session -17.99) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $251.47 | ▼ 09:30 equity $10,083.31 vs yday $10,387.51 (-304.20) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AUPH` | 80 | $16.03 | $2.25 | $-24.48 | $1,531.62 | ▼ -24.48 after sell → book $10,081.05; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `OVID` | 477 | $2.75 | $6.24 | $-0.47 | $2,839.51 | ▼ -0.47 after sell → book $10,074.81; vs 09:30 mark -6.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SANM` | 6 | $206.50 | $2.03 | $-6.08 | $4,076.48 | ▼ -6.08 after sell → book $10,072.78; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 7 | $141.42 | $2.03 | $-165.11 | $5,064.39 | ▼ -165.11 after sell → book $10,070.75; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COHU` | 23 | $52.23 | $2.08 | $-92.92 | $6,263.60 | ▼ -92.92 after sell → book $10,068.67; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 639 | $2.01 | $8.36 | $-35.77 | $7,539.63 | ▼ -35.77 after sell → book $10,060.31; vs 09:30 mark -8.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 274 | $4.82 | $3.59 | $+12.05 | $8,856.72 | ▲ +12.05 after sell → book $10,056.72; vs 09:30 mark -3.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,856.72 | ▼ close $10,029.84 vs 09:30 $10,083.31 (session -26.88) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,856.72 | ▲ 09:30 equity $10,065.68 vs yday $10,029.84 (+35.84) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `NVT` | 8 | $151.12 | $2.03 | $-57.33 | $10,063.65 | ▼ -57.33 after sell → book $10,063.65; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,063.65 | ▲ close $10,063.65 vs 09:30 $10,065.68 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,063.65 | ▲ 09:30 equity $10,063.65 vs yday $10,063.65 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $8,978.09 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+4.0; leftover $1257.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 16 | $77.12 | $2.04 | — | $7,742.13 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; ret5=+7.2; leftover $1257.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 87 | $14.31 | $2.25 | — | $6,494.91 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+4.8; leftover $1257.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 34 | $36.46 | $2.09 | — | $5,253.17 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+2.9; leftover $1257.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 18 | $68.79 | $2.04 | — | $4,012.91 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1257.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 214 | $5.87 | $2.76 | — | $2,753.97 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1257.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 14 | $87.40 | $2.03 | — | $1,528.34 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1257.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 33 | $38.01 | $2.09 | — | $271.92 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $1257.96 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $271.92 | ▼ close $9,847.36 vs 09:30 $10,063.65 (session -198.98) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $271.92 | ▲ 09:30 equity $9,998.94 vs yday $9,847.36 (+151.58) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `IQV` | 4 | $273.15 | $2.02 | $+5.02 | $1,362.50 | ▲ +5.02 after sell → book $9,996.92; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 16 | $76.44 | $2.06 | $-14.98 | $2,583.48 | ▼ -14.98 after sell → book $9,994.86; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AVAH` | 87 | $14.33 | $2.28 | $-2.79 | $3,827.91 | ▼ -2.79 after sell → book $9,992.58; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BLFS` | 34 | $36.67 | $2.11 | $+2.94 | $5,072.58 | ▲ +2.94 after sell → book $9,990.47; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 18 | $72.70 | $2.06 | $+66.27 | $6,379.12 | ▲ +66.27 after sell → book $9,988.41; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 214 | $5.58 | $2.81 | $-67.63 | $7,570.43 | ▼ -67.63 after sell → book $9,985.60; vs 09:30 mark -2.81 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 14 | $83.20 | $2.05 | $-62.88 | $8,733.18 | ▼ -62.88 after sell → book $9,983.55; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `KRMN` | 33 | $37.89 | $2.11 | $-8.16 | $9,981.44 | ▼ -8.16 after sell → book $9,981.44; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 5 | $233.85 | $2.00 | — | $8,810.18 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; ret5=+11.7; leftover $1247.68 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 8 | $151.43 | $2.01 | — | $7,596.73 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $1247.68 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 8 | $147.61 | $2.01 | — | $6,413.84 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; ret5=+17.7; leftover $1247.68 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 121 | $10.25 | $2.35 | — | $5,171.23 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $1247.68 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 164 | $7.59 | $2.48 | — | $3,923.99 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1247.68 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 35 | $34.93 | $2.10 | — | $2,699.35 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+1.6; leftover $1247.68 | — |
| 2026-09-17 09:30 ET | **BUY** | `AXTI` | 18 | $67.91 | $2.04 | — | $1,474.92 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.7; leftover $1247.68 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 48 | $25.95 | $2.13 | — | $227.19 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1247.68 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $227.19 | ▲ close $10,073.34 vs 09:30 $9,998.94 (session +109.04) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $227.19 | ▲ 09:30 equity $10,160.28 vs yday $10,073.34 (+86.94) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 5 | $249.13 | $2.02 | $+72.37 | $1,470.81 | ▲ +72.37 after sell → book $10,158.25; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TWST` | 8 | $158.04 | $2.03 | $+48.83 | $2,733.10 | ▲ +48.83 after sell → book $10,156.22; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 8 | $146.50 | $2.03 | $-12.93 | $3,903.07 | ▼ -12.93 after sell → book $10,154.19; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 121 | $10.12 | $2.38 | $-20.47 | $5,125.20 | ▼ -20.47 after sell → book $10,151.80; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 164 | $7.98 | $2.52 | $+58.96 | $6,431.40 | ▲ +58.96 after sell → book $10,149.28; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMN` | 35 | $34.52 | $2.12 | $-18.56 | $7,637.49 | ▼ -18.56 after sell → book $10,147.17; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AXTI` | 18 | $69.72 | $2.06 | $+28.47 | $8,890.38 | ▲ +28.47 after sell → book $10,145.10; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 48 | $26.14 | $2.15 | $+4.83 | $10,142.95 | ▲ +4.83 after sell → book $10,142.95; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 11 | $108.55 | $2.02 | — | $8,946.88 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ⚪; ret5=+21.3; leftover $1267.87 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 6 | $209.52 | $2.01 | — | $7,687.75 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1267.87 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 5 | $219.62 | $2.00 | — | $6,587.64 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1267.87 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 14 | $85.00 | $2.03 | — | $5,395.61 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1267.87 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 36 | $34.44 | $2.10 | — | $4,153.67 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1267.87 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 217 | $5.83 | $2.80 | — | $2,885.76 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1267.87 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 354 | $3.58 | $4.57 | — | $1,613.88 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1267.87 | — |
| 2026-09-18 09:30 ET | **BUY** | `RANI` | 1491 | $0.85 | $17.15 | — | $329.38 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; ret5=+3.6; leftover $1267.87 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $329.38 | ▼ close $10,087.57 vs 09:30 $10,160.28 (session -20.70) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $329.38 | ▲ 09:30 equity $10,265.14 vs yday $10,087.57 (+177.57) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 11 | $107.57 | $2.04 | $-14.85 | $1,510.61 | ▼ -14.85 after sell → book $10,263.10; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 6 | $210.00 | $2.03 | $-1.16 | $2,768.58 | ▼ -1.16 after sell → book $10,261.07; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 5 | $230.25 | $2.02 | $+49.12 | $3,917.81 | ▲ +49.12 after sell → book $10,259.04; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 14 | $82.83 | $2.05 | $-34.46 | $5,075.37 | ▼ -34.46 after sell → book $10,256.99; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FIVN` | 36 | $33.00 | $2.12 | $-56.06 | $6,261.26 | ▼ -56.06 after sell → book $10,254.87; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BNC` | 217 | $6.42 | $2.85 | $+121.30 | $7,650.46 | ▲ +121.30 after sell → book $10,252.03; vs 09:30 mark -2.84 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DDD` | 354 | $3.71 | $4.64 | $+36.82 | $8,959.17 | ▲ +36.82 after sell → book $10,247.39; vs 09:30 mark -4.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RANI` | 1491 | $0.86 | $17.61 | $-13.89 | $10,229.78 | ▼ -13.89 after sell → book $10,229.78; vs 09:30 mark -17.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 8 | $157.87 | $2.01 | — | $8,964.80 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+6.5; leftover $1278.72 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 3 | $386.20 | $2.00 | — | $7,804.21 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=-5.8; leftover $1278.72 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 14 | $88.83 | $2.03 | — | $6,558.55 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+7.6; leftover $1278.72 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 137 | $9.31 | $2.40 | — | $5,280.68 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1278.72 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 94 | $13.47 | $2.27 | — | $4,011.76 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1278.72 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1152 | $1.11 | $14.86 | — | $2,718.18 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1278.72 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 49 | $25.95 | $2.14 | — | $1,444.49 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1278.72 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,444.49 | ▼ close $10,098.50 vs 09:30 $10,265.14 (session -103.56) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,444.49 | ▲ 09:30 equity $10,098.50 vs yday $10,098.50 (+0.00) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `ORBS` | 1152 | $1.05 | $15.06 | $-99.04 | $2,639.03 | ▼ -99.04 after sell → book $10,083.44; vs 09:30 mark -15.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 3 | $93.97 | $2.00 | — | $2,355.12 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=-0.6; leftover $329.88 | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 568 | $0.58 | $5.00 | — | $2,020.68 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $329.88 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,020.68 | ▼ close $10,047.92 vs 09:30 $10,098.50 (session -28.52) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,020.68 | ▲ 09:30 equity $10,135.39 vs yday $10,047.92 (+87.47) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `HUM` | 3 | $370.00 | $2.02 | $-52.62 | $3,128.66 | ▼ -52.62 after sell → book $10,133.37; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 137 | $9.50 | $2.43 | $+21.19 | $4,427.73 | ▲ +21.19 after sell → book $10,130.94; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 94 | $12.84 | $2.30 | $-64.26 | $5,632.39 | ▼ -64.26 after sell → book $10,128.64; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 49 | $26.58 | $2.16 | $+26.58 | $6,932.66 | ▲ +26.58 after sell → book $10,126.49; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `USFD` | 3 | $93.97 | $2.02 | $-4.02 | $7,212.55 | ▼ -4.02 after sell → book $10,124.47; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DEFT` | 568 | $0.57 | $5.07 | $-12.91 | $7,534.07 | ▼ -12.91 after sell → book $10,119.39; vs 09:30 mark -5.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 10 | $116.85 | $2.02 | — | $6,363.55 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1255.68 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 45 | $27.79 | $2.12 | — | $5,110.88 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1255.68 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 157 | $7.95 | $2.46 | — | $3,860.27 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1255.68 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 127 | $9.81 | $2.37 | — | $2,612.03 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1255.68 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 60 | $20.65 | $2.17 | — | $1,370.86 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1255.68 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLLN` | 10 | $116.00 | $2.02 | — | $208.84 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $1255.68 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $208.84 | ▼ close $9,859.70 vs 09:30 $10,135.39 (session -246.53) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $208.84 | ▼ 09:30 equity $9,791.24 vs yday $9,859.70 (-68.46) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 8 | $163.95 | $2.03 | $+44.59 | $1,518.40 | ▲ +44.59 after sell → book $9,789.20; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 14 | $87.67 | $2.05 | $-20.25 | $2,743.80 | ▼ -20.25 after sell → book $9,787.15; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HALO` | 10 | $112.22 | $2.04 | $-50.36 | $3,863.96 | ▼ -50.36 after sell → book $9,785.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 45 | $26.22 | $2.15 | $-74.92 | $5,041.71 | ▼ -74.92 after sell → book $9,782.96; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 157 | $7.38 | $2.50 | $-94.45 | $6,197.88 | ▼ -94.45 after sell → book $9,780.47; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 127 | $9.67 | $2.40 | $-22.55 | $7,423.56 | ▼ -22.55 after sell → book $9,778.06; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 60 | $20.52 | $2.19 | $-12.16 | $8,652.57 | ▼ -12.16 after sell → book $9,775.87; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLLN` | 10 | $112.33 | $2.04 | $-40.76 | $9,773.83 | ▼ -40.76 after sell → book $9,773.83; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,773.83 | ▲ close $9,773.83 vs 09:30 $9,791.24 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,689.54 | ▲ 09:30 equity $8,689.54 vs yday $8,689.54 (+0.00) | 09:30 open · cash $8,689.54 · no holdings · equity $8,689.54 vs prior close $8,689.54 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 1 | $803.87 | $1.99 | — | $7,883.68 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+0.8; leftover $1086.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 9 | $115.36 | $2.02 | — | $6,843.42 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1086.19 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 52 | $20.61 | $2.15 | — | $5,769.55 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+9.1; leftover $1086.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 28 | $38.51 | $2.07 | — | $4,689.20 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+4.7; leftover $1086.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 141 | $7.65 | $2.41 | — | $3,608.14 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1086.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 41 | $26.27 | $2.11 | — | $2,528.95 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1086.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $1,521.81 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1086.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 181 | $6.00 | $2.53 | — | $433.27 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $1086.19 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $433.27 | ▲ close $8,680.30 vs 09:30 $8,689.54 (session +8.07) | 16:00 close · cash $433.27 · equity $8,680.30 vs 09:30 $8,689.54 (-9.24; session marks +8.07) · 8 name(s) marked open→close (per-name table). REGN×1 09:30 $803.87 → close $788.04 -15.83; HALO×9 09:30 $115.36 → close $113.90 -13.14; OMER×52 09:30 $20.61 → close $20.08 -27.56; BLFS×28 09:30 $38.51 → close $38.49 -0.56; MRVI×141 09:30 $7.65 → close $7.60 -7.05; WRBY×41 09:30 $26.27 → close $26.71 +18.04; TXG×12 09:30 $83.76 → close $85.71 +23.40; SATL×181 09:30 $6.00 → close $6.17 +30.77 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $433.27 | ▼ 09:30 equity $8,596.51 vs yday $8,680.30 (-83.79) | 09:30 open · cash $433.27 (unchanged overnight, no fees) · equity $8,596.51 vs prior close $8,680.30 (-83.79) · 8 name(s) re-marked at the open (per-name table). BLFS×28 yday $38.49 → 09:30 $38.13 -10.08; HALO×9 yday $113.90 → 09:30 $113.34 -5.04; MRVI×141 yday $7.60 → 09:30 $7.49 -15.51; OMER×52 yday $20.08 → 09:30 $19.83 -13.00; REGN×1 yday $788.04 → 09:30 $771.84 -16.20; SATL×181 yday $6.17 → 09:30 $6.17 +0.00; TXG×12 yday $85.71 → 09:30 $86.14 +5.16; WRBY×41 yday $26.71 → 09:30 $26.00 -29.11 | — |
| 2026-09-28 09:30 ET | **SELL** | `BLFS` | 28 | $38.13 | $2.09 | $-14.81 | $1,498.82 | ▼ -14.81 after sell → book $8,594.41; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `HALO` | 9 | $113.34 | $2.04 | $-22.23 | $2,516.84 | ▼ -22.23 after sell → book $8,592.38; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `MRVI` | 141 | $7.49 | $2.45 | $-27.42 | $3,570.48 | ▼ -27.42 after sell → book $8,589.93; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `OMER` | 52 | $19.83 | $2.17 | $-44.87 | $4,599.48 | ▼ -44.87 after sell → book $8,587.76; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `REGN` | 1 | $771.84 | $2.01 | $-36.04 | $5,369.30 | ▼ -36.04 after sell → book $8,585.75; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `SATL` | 181 | $6.17 | $2.57 | $+25.66 | $6,483.50 | ▲ +25.66 after sell → book $8,583.18; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `TXG` | 12 | $86.14 | $2.05 | $+24.49 | $7,515.13 | ▲ +24.49 after sell → book $8,581.13; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `WRBY` | 41 | $26.00 | $2.13 | $-15.32 | $8,579.00 | ▼ -15.32 after sell → book $8,579.00; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,579.00 | ▲ close $8,579.00 vs 09:30 $8,596.51 (session +0.00) | 16:00 close · cash $8,579.00 · no lots left · equity $8,579.00. | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,579.00 | ▲ 09:30 equity $8,579.00 vs yday $8,579.00 (+0.00) | 09:30 open · cash $8,579.00 · no holdings · equity $8,579.00 vs prior close $8,579.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-29 09:30 ET | **BUY** | `SN` | 5 | $184.05 | $2.00 | — | $7,656.74 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+8.7; leftover $1072.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `DT` | 18 | $57.39 | $2.04 | — | $6,621.68 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ⚪; ret5=+2.6; leftover $1072.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `TOST` | 35 | $30.40 | $2.10 | — | $5,555.59 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ⚪; ret5=+1.9; leftover $1072.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SONO` | 60 | $17.76 | $2.17 | — | $4,487.82 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ⚪; ret5=+9.8; leftover $1072.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-29 09:30 ET | **BUY** | `PDFS` | 21 | $50.25 | $2.05 | — | $3,430.51 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ⚪; ret5=+6.5; leftover $1072.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SHOO` | 23 | $45.06 | $2.06 | — | $2,392.07 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+4.8; leftover $1072.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `ADMA` | 106 | $10.04 | $2.31 | — | $1,325.53 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,ohlc_hot; ⚪; ret5=+6.5; leftover $1072.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `XNCR` | 41 | $25.84 | $2.11 | — | $263.77 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; ret5=+2.3; leftover $1072.38 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $263.77 | ▼ close $8,512.76 vs 09:30 $8,579.00 (session -49.39) | 16:00 close · cash $263.77 · equity $8,512.76 vs 09:30 $8,579.00 (-66.24; session marks -49.39) · 8 name(s) marked open→close (per-name table). SN×5 09:30 $184.05 → close $182.44 -8.05; DT×18 09:30 $57.39 → close $57.53 +2.52; TOST×35 09:30 $30.40 → close $30.46 +2.10; SONO×60 09:30 $17.76 → close $17.85 +5.40; PDFS×21 09:30 $50.25 → close $49.77 -10.08; SHOO×23 09:30 $45.06 → close $45.14 +1.84; ADMA×106 09:30 $10.04 → close $9.96 -8.48; XNCR×41 09:30 $25.84 → close $25.00 -34.64 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $263.77 | ▲ 09:30 equity $8,512.76 vs yday $8,512.76 (+0.00) | 09:30 open · cash $263.77 (unchanged overnight, no fees) · equity $8,512.76 vs prior close $8,512.76 (+0.00) · 8 name(s) re-marked at the open (per-name table). ADMA×106 yday $9.96 → 09:30 $9.96 +0.00; DT×18 yday $57.53 → 09:30 $57.53 +0.00; PDFS×21 yday $49.77 → 09:30 $49.77 +0.00; SHOO×23 yday $45.14 → 09:30 $45.14 +0.00; SN×5 yday $182.44 → 09:30 $182.44 +0.00; SONO×60 yday $17.85 → 09:30 $17.85 +0.00; TOST×35 yday $30.46 → 09:30 $30.46 +0.00; XNCR×41 yday $25.00 → 09:30 $25.00 +0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $263.77 | ▲ close $8,512.76 vs 09:30 $8,512.76 (session +0.00) | 16:00 close · cash $263.77 · equity $8,512.76 vs 09:30 $8,512.76 (+0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). ADMA×106 09:30 $9.96 → close $9.96 +0.00; DT×18 09:30 $57.53 → close $57.53 +0.00; PDFS×21 09:30 $49.77 → close $49.77 +0.00; SHOO×23 09:30 $45.14 → close $45.14 +0.00; SN×5 09:30 $182.44 → close $182.44 +0.00; SONO×60 09:30 $17.85 → close $17.85 +0.00; TOST×35 09:30 $30.46 → close $30.46 +0.00; XNCR×41 09:30 $25.00 → close $25.00 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $263.77 | ▲ 09:30 equity $8,517.82 vs yday $8,512.76 (+5.06) | 09:30 open · cash $263.77 (unchanged overnight, no fees) · equity $8,517.82 vs prior close $8,512.76 (+5.06) · 8 name(s) re-marked at the open (per-name table). ADMA×106 yday $9.96 → 09:30 $10.07 +11.66; DT×18 yday $57.53 → 09:30 $59.04 +27.18; PDFS×21 yday $49.77 → 09:30 $51.41 +34.44; SHOO×23 yday $45.14 → 09:30 $44.46 -15.64; SN×5 yday $182.44 → 09:30 $182.44 +0.00; SONO×60 yday $17.85 → 09:30 $18.09 +14.40; TOST×35 yday $30.46 → 09:30 $29.05 -49.35; XNCR×41 yday $25.00 → 09:30 $24.57 -17.63 | — |
| 2026-10-01 09:30 ET | **SELL** | `ADMA` | 106 | $10.07 | $2.34 | $-1.46 | $1,328.85 | ▼ -1.46 after sell → book $8,515.48; vs 09:30 mark -2.34 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `DT` | 18 | $59.04 | $2.06 | $+25.59 | $2,389.51 | ▲ +25.59 after sell → book $8,513.42; vs 09:30 mark -2.06 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `PDFS` | 21 | $51.41 | $2.07 | $+20.23 | $3,467.05 | ▲ +20.23 after sell → book $8,511.35; vs 09:30 mark -2.07 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `SHOO` | 23 | $44.46 | $2.08 | $-17.94 | $4,487.55 | ▼ -17.94 after sell → book $8,509.27; vs 09:30 mark -2.08 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `SONO` | 60 | $18.09 | $2.19 | $+15.44 | $5,570.76 | ▲ +15.44 after sell → book $8,507.08; vs 09:30 mark -2.19 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `TOST` | 35 | $29.05 | $2.12 | $-51.46 | $6,585.39 | ▼ -51.46 after sell → book $8,504.96; vs 09:30 mark -2.12 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `XNCR` | 41 | $24.57 | $2.13 | $-56.52 | $7,590.63 | ▼ -56.52 after sell → book $8,502.83; vs 09:30 mark -2.13 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **BUY** | `KSPI` | 10 | $92.93 | $2.02 | — | $6,659.31 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+0.2; leftover $948.83 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `IOT` | 24 | $38.99 | $2.06 | — | $5,721.49 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=-2.4; leftover $948.83 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `AVPT` | 66 | $14.27 | $2.19 | — | $4,777.48 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; 🔵; ret5=+7.3; leftover $948.83 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `RELY` | 44 | $21.28 | $2.12 | — | $3,839.04 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+5.7; leftover $948.83 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `CDNL` | 31 | $30.30 | $2.08 | — | $2,897.66 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; ret5=+6.7; leftover $948.83 | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `INO` | 765 | $1.24 | $9.87 | — | $1,939.19 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $948.83 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `ZETA` | 29 | $32.03 | $2.08 | — | $1,008.24 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+6.0; leftover $948.83 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `PMVP` | 564 | $1.68 | $7.28 | — | $53.44 | — | union ∩ ab_g, no 🚨; gate ab=good; list yday_gainer,yday_mover; ret5=+21.9; leftover $948.83 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.44 | ▲ close $8,495.39 vs 09:30 $8,517.82 (session +22.26) | 16:00 close · cash $53.44 · equity $8,495.39 vs 09:30 $8,517.82 (-22.43; session marks +22.26) · 9 name(s) marked open→close (per-name table). SN×5 09:30 $182.44 → close $182.44 +0.00; KSPI×10 09:30 $92.93 → close $92.02 -9.10; IOT×24 09:30 $38.99 → close $40.04 +25.20; AVPT×66 09:30 $14.27 → close $14.08 -12.54; RELY×44 09:30 $21.28 → close $21.48 +8.80; CDNL×31 09:30 $30.30 → close $29.74 -17.36; INO×765 09:30 $1.24 → close $1.21 -22.95; ZETA×29 09:30 $32.03 → close $32.40 +10.73; PMVP×564 09:30 $1.68 → close $1.75 +39.48 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.44 | ▲ 09:30 equity $8,572.85 vs yday $8,495.39 (+77.46) | 09:30 open · cash $53.44 (unchanged overnight, no fees) · equity $8,572.85 vs prior close $8,495.39 (+77.46) · 9 name(s) re-marked at the open (per-name table). AVPT×66 yday $14.08 → 09:30 $14.22 +9.24; CDNL×31 yday $29.74 → 09:30 $30.29 +17.05; INO×765 yday $1.21 → 09:30 $1.23 +15.30; IOT×24 yday $40.04 → 09:30 $40.41 +8.88; KSPI×10 yday $92.02 → 09:30 $92.05 +0.30; PMVP×564 yday $1.75 → 09:30 $1.75 +0.00; RELY×44 yday $21.48 → 09:30 $21.80 +14.08; SN×5 yday $182.44 → 09:30 $183.57 +5.65; ZETA×29 yday $32.40 → 09:30 $32.64 +6.96 | — |
| 2026-10-02 09:30 ET | **SELL** | `AVPT` | 66 | $14.22 | $2.21 | $-7.70 | $989.75 | ▼ -7.70 after sell → book $8,570.64; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `CDNL` | 31 | $30.29 | $2.10 | $-4.50 | $1,926.64 | ▼ -4.50 after sell → book $8,568.54; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `INO` | 765 | $1.23 | $10.01 | $-27.52 | $2,857.58 | ▼ -27.52 after sell → book $8,558.53; vs 09:30 mark -10.01 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `IOT` | 24 | $40.41 | $2.08 | $+29.94 | $3,825.34 | ▲ +29.94 after sell → book $8,556.45; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `KSPI` | 10 | $92.05 | $2.04 | $-12.86 | $4,743.80 | ▼ -12.86 after sell → book $8,554.41; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `PMVP` | 564 | $1.75 | $7.38 | $+24.83 | $5,723.42 | ▲ +24.83 after sell → book $8,547.03; vs 09:30 mark -7.38 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `RELY` | 44 | $21.80 | $2.14 | $+18.62 | $6,680.48 | ▲ +18.62 after sell → book $8,544.89; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `SN` | 5 | $183.57 | $2.02 | $-6.43 | $7,596.30 | ▼ -6.43 after sell → book $8,542.86; vs 09:30 mark -2.03 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-02 09:30 ET | **SELL** | `ZETA` | 29 | $32.64 | $2.10 | $+13.52 | $8,540.77 | ▲ +13.52 after sell → book $8,540.77; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **BUY** | `VEEV` | 3 | $283.10 | $2.00 | — | $7,689.47 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+2.6; leftover $1067.60 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `CORT` | 9 | $114.38 | $2.02 | — | $6,658.03 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=-3.8; leftover $1067.60 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `ILMN` | 4 | $265.91 | $2.00 | — | $5,592.39 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=-3.0; leftover $1067.60 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `CDNA` | 16 | $66.33 | $2.04 | — | $4,529.07 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; 🔵; ⚪; ret5=+7.9; leftover $1067.60 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `WRBY` | 38 | $27.63 | $2.10 | — | $3,477.03 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+4.6; leftover $1067.60 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `BLFS` | 28 | $37.02 | $2.07 | — | $2,438.39 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=-4.7; leftover $1067.60 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `ETON` | 20 | $52.42 | $2.05 | — | $1,387.94 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=-12.6; leftover $1067.60 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `COHR` | 3 | $316.56 | $2.00 | — | $436.25 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ret5=+9.8; leftover $1067.60 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $436.25 | ▲ close $8,658.73 vs 09:30 $8,572.85 (session +134.25) | 16:00 close · cash $436.25 · equity $8,658.73 vs 09:30 $8,572.85 (+85.88; session marks +134.25) · 8 name(s) marked open→close (per-name table). VEEV×3 09:30 $283.10 → close $273.33 -29.31; CORT×9 09:30 $114.38 → close $116.23 +16.65; ILMN×4 09:30 $265.91 → close $273.04 +28.52; CDNA×16 09:30 $66.33 → close $67.15 +13.12; WRBY×38 09:30 $27.63 → close $27.03 -22.80; BLFS×28 09:30 $37.02 → close $37.30 +7.84; ETON×20 09:30 $52.42 → close $55.36 +58.80; COHR×3 09:30 $316.56 → close $337.04 +61.43 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $436.25 | ▲ 09:30 equity $8,670.98 vs yday $8,658.73 (+12.25) | 09:30 open · cash $436.25 (unchanged overnight, no fees) · equity $8,670.98 vs prior close $8,658.73 (+12.25) · 8 name(s) re-marked at the open (per-name table). BLFS×28 yday $37.30 → 09:30 $37.16 -3.92; CDNA×16 yday $67.15 → 09:30 $66.90 -4.00; COHR×3 yday $337.04 → 09:30 $340.93 +11.65; CORT×9 yday $116.23 → 09:30 $115.59 -5.76; ETON×20 yday $55.36 → 09:30 $55.85 +9.80; ILMN×4 yday $273.04 → 09:30 $273.00 -0.16; VEEV×3 yday $273.33 → 09:30 $275.00 +5.01; WRBY×38 yday $27.03 → 09:30 $27.02 -0.38 | — |
| 2026-10-05 09:30 ET | **SELL** | `BLFS` | 28 | $37.16 | $2.09 | $-0.25 | $1,474.64 | ▼ -0.25 after sell → book $8,668.88; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `CDNA` | 16 | $66.90 | $2.06 | $+5.02 | $2,542.98 | ▲ +5.02 after sell → book $8,666.82; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `COHR` | 3 | $340.93 | $2.02 | $+69.06 | $3,563.73 | ▲ +69.06 after sell → book $8,664.80; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `CORT` | 9 | $115.59 | $2.04 | $+6.84 | $4,602.01 | ▲ +6.84 after sell → book $8,662.77; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `ETON` | 20 | $55.85 | $2.07 | $+64.48 | $5,716.94 | ▲ +64.48 after sell → book $8,660.70; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `ILMN` | 4 | $273.00 | $2.02 | $+24.34 | $6,806.92 | ▲ +24.34 after sell → book $8,658.68; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `VEEV` | 3 | $275.00 | $2.02 | $-28.32 | $7,629.90 | ▼ -28.32 after sell → book $8,656.66; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `WRBY` | 38 | $27.02 | $2.12 | $-27.41 | $8,654.53 | ▼ -27.41 after sell → book $8,654.53; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **BUY** | `COP` | 8 | $127.00 | $2.01 | — | $7,636.52 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=-0.4; leftover $1081.82 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `EXE` | 12 | $84.70 | $2.03 | — | $6,618.09 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=-1.5; leftover $1081.82 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `RRC` | 28 | $38.10 | $2.07 | — | $5,549.22 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=-1.0; leftover $1081.82 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `OBE` | 105 | $10.26 | $2.31 | — | $4,469.61 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=-2.7; leftover $1081.82 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `PENG` | 17 | $61.10 | $2.04 | — | $3,428.87 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+9.1; leftover $1081.82 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `INIO` | 53 | $20.09 | $2.15 | — | $2,361.95 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; ret5=+4.6; leftover $1081.82 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `FEAM` | 278 | $3.88 | $3.59 | — | $1,279.73 | — | union ∩ ab_g, no 🚨; gate ab=good; list yday_gainer,yday_mover; ret5=+38.6; leftover $1081.82 | join🟡 sector🟡 gen🟡 news🟡 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `MXL` | 10 | $106.00 | $2.02 | — | $217.71 | — | union ∩ ab_g, no 🚨; gate ab=good; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.9; leftover $1081.82 | join🟡 sector🟢 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $217.71 | ▲ close $8,650.00 vs 09:30 $8,670.98 (session +13.68) | 16:00 close · cash $217.71 · equity $8,650.00 vs 09:30 $8,670.98 (-20.98; session marks +13.68) · 8 name(s) marked open→close (per-name table). COP×8 09:30 $127.00 → close $128.40 +11.20; EXE×12 09:30 $84.70 → close $86.43 +20.76; RRC×28 09:30 $38.10 → close $38.67 +15.96; OBE×105 09:30 $10.26 → close $10.50 +25.20; PENG×17 09:30 $61.10 → close $60.71 -6.63; INIO×53 09:30 $20.09 → close $18.98 -58.83; FEAM×278 09:30 $3.88 → close $3.92 +11.12; MXL×10 09:30 $106.00 → close $105.49 -5.10 | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $217.71 | ▲ 09:30 equity $8,714.64 vs yday $8,650.00 (+64.64) | 09:30 open · cash $217.71 (unchanged overnight, no fees) · equity $8,714.64 vs prior close $8,650.00 (+64.64) · 8 name(s) re-marked at the open (per-name table). COP×8 yday $128.40 → 09:30 $128.19 -1.68; EXE×12 yday $86.43 → 09:30 $86.38 -0.60; FEAM×278 yday $3.92 → 09:30 $3.96 +11.12; INIO×53 yday $18.98 → 09:30 $19.33 +18.55; MXL×10 yday $105.49 → 09:30 $108.22 +27.30; OBE×105 yday $10.50 → 09:30 $10.44 -6.30; PENG×17 yday $60.71 → 09:30 $61.60 +15.13; RRC×28 yday $38.67 → 09:30 $38.71 +1.12 | — |
| 2026-10-06 09:30 ET | **SELL** | `COP` | 8 | $128.19 | $2.03 | $+5.47 | $1,241.20 | ▲ +5.47 after sell → book $8,712.61; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `EXE` | 12 | $86.38 | $2.05 | $+16.09 | $2,275.71 | ▲ +16.09 after sell → book $8,710.56; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `FEAM` | 278 | $3.96 | $3.64 | $+15.01 | $3,372.95 | ▲ +15.01 after sell → book $8,706.92; vs 09:30 mark -3.64 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `INIO` | 53 | $19.33 | $2.17 | $-44.60 | $4,395.27 | ▼ -44.60 after sell → book $8,704.75; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `MXL` | 10 | $108.22 | $2.04 | $+18.14 | $5,475.43 | ▲ +18.14 after sell → book $8,702.71; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `OBE` | 105 | $10.44 | $2.33 | $+14.26 | $6,569.30 | ▲ +14.26 after sell → book $8,700.38; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `PENG` | 17 | $61.60 | $2.06 | $+4.40 | $7,614.44 | ▲ +4.40 after sell → book $8,698.32; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **SELL** | `RRC` | 28 | $38.71 | $2.09 | $+12.91 | $8,696.22 | ▲ +12.91 after sell → book $8,696.22; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **BUY** | `SN` | 5 | $183.47 | $2.00 | — | $7,776.87 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+0.4; leftover $1087.03 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `TOST` | 36 | $30.07 | $2.10 | — | $6,692.25 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ⚪; ret5=-1.1; leftover $1087.03 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `PDFS` | 19 | $56.89 | $2.05 | — | $5,609.29 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+12.7; leftover $1087.03 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `AVPT` | 73 | $14.77 | $2.21 | — | $4,528.87 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ⚪; ret5=+7.2; leftover $1087.03 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `ARCT` | 73 | $14.70 | $2.21 | — | $3,453.56 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; ret5=+1.4; leftover $1087.03 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `ALVO` | 179 | $6.07 | $2.53 | — | $2,364.51 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; ret5=-0.7; leftover $1087.03 | join🔴 sector🟡 gen🟡 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `SIBN` | 54 | $20.00 | $2.15 | — | $1,282.35 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable; ret5=+5.6; leftover $1087.03 | join🟡 sector🟡 gen🟡 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `PTC` | 5 | $193.00 | $2.00 | — | $315.35 | — | union ∩ ab_g, no 🚨; gate ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+37.6; leftover $1087.03 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $315.35 | ▼ close $8,496.14 vs 09:30 $8,714.64 (session -182.83) | 16:00 close · cash $315.35 · equity $8,496.14 vs 09:30 $8,714.64 (-218.50; session marks -182.83) · 8 name(s) marked open→close (per-name table). SN×5 09:30 $183.47 → close $184.72 +6.25; TOST×36 09:30 $30.07 → close $30.25 +6.48; PDFS×19 09:30 $56.89 → close $54.44 -46.55; AVPT×73 09:30 $14.77 → close $14.59 -13.14; ARCT×73 09:30 $14.70 → close $13.00 -124.10; ALVO×179 09:30 $6.07 → close $6.14 +12.53; SIBN×54 09:30 $20.00 → close $19.55 -24.30; PTC×5 09:30 $193.00 → close $193.00 +0.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `QSI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
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
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VICR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-21 | `SNDK` | cash | leftover split 1278.72 < 1 share @ 1826.00 |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `HUM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
