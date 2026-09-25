# Factor mine action — `union_earn_react_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ earn_react, no 🚨

Cash book **-17.45%** ($8,255) · signal-only (no cash/fees) was -14.12%. Starts YES **25/30**. Fills 119 · skips 198 · realized $+2100.34.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `earn_react=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $137.94.

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
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 3 | $0.77 | $0.03 | — | $18.73 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $2.63 | — |
| 2026-08-14 09:30 ET | **BUY** | `DEFT` | 5 | $0.47 | $0.04 | — | $16.35 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+11.1; leftover $2.63 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.35 | ▲ close $11,883.74 vs 09:30 $10,963.61 (session +920.20) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.35 | ▼ 09:30 equity $11,733.35 vs yday $11,883.74 (-150.39) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.35 | ▲ close $12,249.26 vs 09:30 $11,733.35 (session +515.92) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.35 | ▼ 09:30 equity $12,145.01 vs yday $12,249.26 (-104.25) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 6172 | $1.14 | $80.70 | $+1887.55 | $6,971.73 | ▲ +1,887.55 after sell → book $12,064.31; vs 09:30 mark -80.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 223 | $22.82 | $2.95 | $+174.80 | $12,057.63 | ▲ +174.80 after sell → book $12,061.35; vs 09:30 mark -2.96 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,057.63 | ▲ close $12,061.49 vs 09:30 $12,145.01 (session +0.13) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,057.63 | ▲ 09:30 equity $12,061.52 vs yday $12,061.49 (+0.03) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `BZAI` | 3 | $0.57 | $0.05 | $-0.67 | $12,059.30 | ▼ -0.67 after sell → book $12,061.47; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DEFT` | 5 | $0.43 | $0.06 | $-0.27 | $12,061.41 | ▼ -0.27 after sell → book $12,061.41; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,061.41 | ▲ close $12,061.41 vs 09:30 $12,061.52 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,061.41 | ▲ 09:30 equity $12,061.41 vs yday $12,061.41 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 32 | $46.85 | $2.09 | — | $10,560.13 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+5.0; leftover $1507.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 167 | $9.01 | $2.49 | — | $9,052.97 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $1507.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 387 | $3.89 | $4.99 | — | $7,542.54 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.5; leftover $1507.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 44 | $34.05 | $2.12 | — | $6,042.22 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+9.3; leftover $1507.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 67 | $22.44 | $2.19 | — | $4,536.55 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.1; leftover $1507.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 12 | $123.47 | $2.03 | — | $3,052.89 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+2.9; leftover $1507.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `BILL` | 30 | $49.00 | $2.08 | — | $1,580.81 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-2.0; leftover $1507.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 151 | $9.94 | $2.44 | — | $77.42 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+12.6; leftover $1507.68 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $77.42 | ▼ close $11,904.81 vs 09:30 $12,061.41 (session -136.17) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $77.42 | ▼ 09:30 equity $11,899.79 vs yday $11,904.81 (-5.02) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 6 | $2.30 | $0.16 | — | $63.47 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.0; leftover $15.48 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.47 | ▼ close $11,865.95 vs 09:30 $11,899.79 (session -33.69) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.47 | ▲ 09:30 equity $11,966.47 vs yday $11,865.95 (+100.52) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.47 | ▲ close $12,187.45 vs 09:30 $11,966.47 (session +220.98) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.47 | ▼ 09:30 equity $12,166.76 vs yday $12,187.45 (-20.69) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 32 | $43.63 | $2.11 | $-107.23 | $1,457.52 | ▼ -107.23 after sell → book $12,164.65; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 167 | $9.23 | $2.53 | $+31.72 | $2,996.40 | ▲ +31.72 after sell → book $12,162.12; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 387 | $5.24 | $5.07 | $+512.38 | $5,019.21 | ▲ +512.38 after sell → book $12,157.05; vs 09:30 mark -5.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 44 | $34.72 | $2.14 | $+25.21 | $6,544.74 | ▲ +25.21 after sell → book $12,154.90; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 67 | $21.85 | $2.21 | $-43.93 | $8,006.48 | ▼ -43.93 after sell → book $12,152.69; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BABA` | 12 | $117.94 | $2.05 | $-70.43 | $9,419.71 | ▼ -70.43 after sell → book $12,150.64; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BILL` | 30 | $47.98 | $2.10 | $-34.63 | $10,857.16 | ▼ -34.63 after sell → book $12,148.54; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BULL` | 151 | $8.46 | $2.48 | $-228.40 | $12,132.14 | ▼ -228.40 after sell → book $12,146.06; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 8 | $175.01 | $2.01 | — | $10,730.05 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-7.0; leftover $1516.52 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 17 | $88.94 | $2.04 | — | $9,216.03 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-4.9; leftover $1516.52 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 99 | $15.28 | $2.29 | — | $7,701.02 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-0.7; leftover $1516.52 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 10 | $142.36 | $2.02 | — | $6,275.40 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-8.6; leftover $1516.52 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 297 | $5.10 | $3.83 | — | $4,756.87 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-8.9; leftover $1516.52 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 31 | $47.89 | $2.08 | — | $3,270.19 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ⚪; ret5=+14.0; leftover $1516.52 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 108 | $13.92 | $2.31 | — | $1,764.52 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+5.9; leftover $1516.52 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 333 | $4.54 | $4.30 | — | $246.74 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-14.6; leftover $1516.52 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $246.74 | ▼ close $11,691.69 vs 09:30 $12,166.76 (session -433.48) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $246.74 | ▼ 09:30 equity $11,653.49 vs yday $11,691.69 (-38.20) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 6 | $2.35 | $0.18 | $-0.03 | $260.66 | ▼ -0.03 after sell → book $11,653.31; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 6 | $5.21 | $0.33 | — | $229.07 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $32.58 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 1 | $18.26 | $0.19 | — | $210.62 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-11.4; leftover $32.58 | — |
| 2026-08-26 09:30 ET | **BUY** | `FSCO` | 6 | $5.08 | $0.32 | — | $179.82 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-1.6; leftover $32.58 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $179.82 | ▲ close $12,046.76 vs 09:30 $11,653.49 (session +394.29) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $179.82 | ▲ 09:30 equity $12,068.42 vs yday $12,046.76 (+21.66) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 1 | $16.18 | $0.16 | — | $163.48 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-6.7; leftover $22.48 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 1 | $17.78 | $0.18 | — | $145.52 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-1.2; leftover $22.48 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 1 | $13.41 | $0.14 | — | $131.97 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.1; leftover $22.48 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.97 | ▼ close $12,012.77 vs 09:30 $12,068.42 (session -55.16) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.97 | ▲ 09:30 equity $12,032.04 vs yday $12,012.77 (+19.27) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 8 | $172.76 | $2.04 | $-22.05 | $1,512.01 | ▼ -22.05 after sell → book $12,030.00; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 17 | $93.30 | $2.06 | $+70.02 | $3,096.05 | ▲ +70.02 after sell → book $12,027.94; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 99 | $18.15 | $2.32 | $+279.53 | $4,890.58 | ▲ +279.53 after sell → book $12,025.62; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 10 | $132.80 | $2.04 | $-99.66 | $6,216.54 | ▼ -99.66 after sell → book $12,023.58; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 297 | $4.58 | $3.89 | $-162.16 | $7,572.91 | ▼ -162.16 after sell → book $12,019.69; vs 09:30 mark -3.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 31 | $48.42 | $2.10 | $+12.24 | $9,071.82 | ▲ +12.24 after sell → book $12,017.58; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 108 | $15.66 | $2.35 | $+183.26 | $10,760.76 | ▲ +183.26 after sell → book $12,015.24; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 333 | $3.38 | $4.36 | $-396.60 | $11,881.94 | ▼ -396.60 after sell → book $12,010.88; vs 09:30 mark -4.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $10,574.13 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+7.8; leftover $1485.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 98 | $15.01 | $2.28 | — | $9,100.87 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+3.7; leftover $1485.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 14 | $103.89 | $2.03 | — | $7,644.38 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.5; leftover $1485.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 382 | $3.88 | $4.93 | — | $6,157.29 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-8.6; leftover $1485.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 33 | $44.40 | $2.09 | — | $4,690.00 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.4; leftover $1485.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 60 | $24.69 | $2.17 | — | $3,206.43 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.8; leftover $1485.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 177 | $8.35 | $2.52 | — | $1,725.96 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.1; leftover $1485.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 39 | $37.65 | $2.11 | — | $255.70 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.9; leftover $1485.24 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $255.70 | ▼ close $11,552.07 vs 09:30 $12,032.04 (session -438.68) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $255.70 | ▲ 09:30 equity $11,563.62 vs yday $11,552.07 (+11.55) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 6 | $5.00 | $0.34 | $-1.93 | $285.36 | ▼ -1.93 after sell → book $11,563.28; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 1 | $19.25 | $0.22 | $+0.59 | $304.39 | ▲ +0.59 after sell → book $11,563.06; vs 09:30 mark -0.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FSCO` | 6 | $5.20 | $0.35 | $+0.05 | $335.24 | ▲ +0.05 after sell → book $11,562.71; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $335.24 | ▲ close $11,650.73 vs 09:30 $11,563.62 (session +88.01) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $335.24 | ▼ 09:30 equity $11,519.74 vs yday $11,650.73 (-130.99) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BILI` | 1 | $15.97 | $0.18 | $-0.56 | $351.03 | ▼ -0.56 after sell → book $11,519.56; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CMBT` | 1 | $18.28 | $0.21 | $+0.11 | $369.11 | ▲ +0.11 after sell → book $11,519.36; vs 09:30 mark -0.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CSIQ` | 1 | $12.18 | $0.14 | $-1.51 | $381.14 | ▼ -1.51 after sell → book $11,519.21; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $381.14 | ▼ close $11,401.04 vs 09:30 $11,519.74 (session -118.17) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $381.14 | ▼ 09:30 equity $11,332.34 vs yday $11,401.04 (-68.70) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 5 | $246.70 | $2.02 | $-76.33 | $1,612.62 | ▼ -76.33 after sell → book $11,330.31; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 98 | $15.01 | $2.31 | $-4.60 | $3,081.28 | ▼ -4.60 after sell → book $11,328.00; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 14 | $92.00 | $2.05 | $-170.54 | $4,367.23 | ▼ -170.54 after sell → book $11,325.95; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 382 | $3.32 | $5.00 | $-223.85 | $5,630.47 | ▼ -223.85 after sell → book $11,320.95; vs 09:30 mark -5.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 33 | $44.17 | $2.11 | $-11.79 | $7,085.97 | ▼ -11.79 after sell → book $11,318.83; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 60 | $21.97 | $2.19 | $-167.56 | $8,401.98 | ▼ -167.56 after sell → book $11,316.64; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 177 | $8.58 | $2.56 | $+35.63 | $9,918.08 | ▲ +35.63 after sell → book $11,314.08; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 39 | $35.80 | $2.13 | $-76.39 | $11,311.95 | ▼ -76.39 after sell → book $11,311.95; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,311.95 | ▲ close $11,311.95 vs 09:30 $11,332.34 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,311.95 | ▲ 09:30 equity $11,311.95 vs yday $11,311.95 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 131 | $10.74 | $2.38 | — | $9,901.98 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+8.5; leftover $1413.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 4 | $351.74 | $2.00 | — | $8,493.01 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+3.3; leftover $1413.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 204 | $6.90 | $2.63 | — | $7,082.78 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.8; leftover $1413.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 3 | $354.49 | $2.00 | — | $6,017.31 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.3; leftover $1413.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 63 | $22.32 | $2.18 | — | $4,608.97 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+2.4; leftover $1413.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 5 | $257.00 | $2.00 | — | $3,321.97 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-5.5; leftover $1413.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 29 | $47.60 | $2.08 | — | $1,939.49 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-6.2; leftover $1413.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 93 | $15.09 | $2.27 | — | $533.85 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+6.1; leftover $1413.99 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $533.85 | ▲ close $11,792.60 vs 09:30 $11,311.95 (session +498.20) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $533.85 | ▲ 09:30 equity $11,834.96 vs yday $11,792.60 (+42.36) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 1 | $63.18 | $0.63 | — | $470.04 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-10.9; leftover $66.73 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 7 | $8.74 | $0.63 | — | $408.23 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.8; leftover $66.73 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 18 | $3.62 | $0.70 | — | $342.45 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-3.1; leftover $66.73 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 1 | $44.90 | $0.45 | — | $297.10 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-7.5; leftover $66.73 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 4 | $15.70 | $0.64 | — | $233.66 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-0.4; leftover $66.73 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $233.66 | ▲ close $11,886.43 vs 09:30 $11,834.96 (session +54.53) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $233.66 | ▲ 09:30 equity $11,906.71 vs yday $11,886.43 (+20.28) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $233.66 | ▼ close $11,889.84 vs 09:30 $11,906.71 (session -16.87) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $233.66 | ▼ 09:30 equity $11,878.71 vs yday $11,889.84 (-11.13) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 131 | $10.51 | $2.42 | $-35.58 | $1,608.05 | ▼ -35.58 after sell → book $11,876.29; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 4 | $366.23 | $2.02 | $+53.93 | $3,070.95 | ▲ +53.93 after sell → book $11,874.27; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 204 | $9.39 | $2.68 | $+502.65 | $4,983.83 | ▲ +502.65 after sell → book $11,871.59; vs 09:30 mark -2.68 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 3 | $341.90 | $2.02 | $-41.79 | $6,007.51 | ▼ -41.79 after sell → book $11,869.57; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CPB` | 63 | $21.67 | $2.20 | $-45.33 | $7,370.52 | ▼ -45.33 after sell → book $11,867.37; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 5 | $252.92 | $2.03 | $-24.43 | $8,633.09 | ▼ -24.43 after sell → book $11,865.34; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 29 | $56.94 | $2.10 | $+266.68 | $10,282.25 | ▲ +266.68 after sell → book $11,863.24; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MEI` | 93 | $13.84 | $2.29 | $-120.81 | $11,567.08 | ▼ -120.81 after sell → book $11,860.95; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,567.08 | ▲ close $11,861.26 vs 09:30 $11,878.71 (session +0.31) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,567.08 | ▼ 09:30 equity $11,859.28 vs yday $11,861.26 (-1.98) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `AMBA` | 1 | $67.44 | $0.70 | $+2.93 | $11,633.82 | ▲ +2.93 after sell → book $11,858.59; vs 09:30 mark -0.69 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ASAN` | 7 | $8.26 | $0.62 | $-4.61 | $11,691.02 | ▼ -4.61 after sell → book $11,857.97; vs 09:30 mark -0.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 18 | $3.76 | $0.75 | $+1.15 | $11,757.95 | ▲ +1.15 after sell → book $11,857.22; vs 09:30 mark -0.75 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `IOT` | 1 | $38.23 | $0.41 | $-7.53 | $11,795.77 | ▼ -7.53 after sell → book $11,856.81; vs 09:30 mark -0.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MAMA` | 4 | $15.26 | $0.64 | $-3.04 | $11,856.17 | ▼ -3.04 after sell → book $11,856.17; vs 09:30 mark -0.64 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,856.17 | ▲ close $11,856.17 vs 09:30 $11,859.28 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,856.17 | ▲ 09:30 equity $11,856.17 vs yday $11,856.17 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 9 | $164.43 | $2.02 | — | $10,374.28 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1482.02 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 6 | $242.17 | $2.01 | — | $8,919.25 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-11.1; leftover $1482.02 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 46 | $32.01 | $2.13 | — | $7,444.67 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-4.4; leftover $1482.02 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 20 | $71.71 | $2.05 | — | $6,008.42 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-9.1; leftover $1482.02 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 26 | $56.02 | $2.07 | — | $4,549.83 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.2; leftover $1482.02 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 158 | $9.37 | $2.46 | — | $3,066.90 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+1.5; leftover $1482.02 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 113 | $13.10 | $2.33 | — | $1,584.27 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-6.9; leftover $1482.02 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 10 | $135.71 | $2.02 | — | $225.15 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-9.2; leftover $1482.02 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $225.15 | ▲ close $11,889.98 vs 09:30 $11,856.17 (session +50.90) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $225.15 | ▼ 09:30 equity $11,884.01 vs yday $11,889.98 (-5.97) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $225.15 | ▲ close $12,186.21 vs 09:30 $11,884.01 (session +302.20) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $225.15 | ▼ 09:30 equity $12,145.17 vs yday $12,186.21 (-41.04) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $225.15 | ▲ close $12,189.62 vs 09:30 $12,145.17 (session +44.45) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $225.15 | ▼ 09:30 equity $12,104.71 vs yday $12,189.62 (-84.91) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 9 | $140.03 | $2.04 | $-223.65 | $1,483.39 | ▼ -223.65 after sell → book $12,102.68; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 6 | $253.34 | $2.03 | $+62.98 | $3,001.40 | ▲ +62.98 after sell → book $12,100.65; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CPRT` | 46 | $30.57 | $2.15 | $-70.52 | $4,405.47 | ▼ -70.52 after sell → book $12,098.50; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DSGX` | 20 | $78.12 | $2.07 | $+124.08 | $5,965.80 | ▲ +124.08 after sell → book $12,096.43; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `KR` | 26 | $61.93 | $2.09 | $+149.50 | $7,573.88 | ▲ +149.50 after sell → book $12,094.33; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `LPTH` | 158 | $9.40 | $2.50 | $-0.23 | $9,056.58 | ▼ -0.23 after sell → book $12,091.83; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `REF` | 113 | $15.75 | $2.36 | $+294.76 | $10,833.97 | ▲ +294.76 after sell → book $12,089.47; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RH` | 10 | $125.55 | $2.04 | $-105.66 | $12,087.43 | ▼ -105.66 after sell → book $12,087.43; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 295 | $40.93 | $3.81 | — | $9.27 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.1; leftover $12087.43 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.27 | ▼ close $11,936.12 vs 09:30 $12,104.71 (session -147.50) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.27 | ▲ 09:30 equity $12,042.32 vs yday $11,936.12 (+106.20) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.27 | ▼ close $11,915.47 vs 09:30 $12,042.32 (session -126.85) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.27 | ▲ 09:30 equity $11,989.22 vs yday $11,915.47 (+73.75) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.27 | ▲ close $12,009.87 vs 09:30 $11,989.22 (session +20.65) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.27 | ▲ 09:30 equity $12,104.27 vs yday $12,009.87 (+94.40) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 295 | $41.00 | $3.95 | $+12.89 | $12,100.32 | ▲ +12.89 after sell → book $12,100.32; vs 09:30 mark -3.95 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,100.32 | ▲ close $12,100.32 vs 09:30 $12,104.27 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,100.32 | ▲ 09:30 equity $12,100.32 vs yday $12,100.32 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,100.32 | ▲ close $12,100.32 vs 09:30 $12,100.32 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,100.32 | ▲ 09:30 equity $12,100.32 vs yday $12,100.32 (+0.00) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 50 | $47.57 | $2.14 | — | $9,719.68 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-11.2; leftover $2420.06 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 12 | $196.78 | $2.03 | — | $7,356.30 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $2420.06 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 67 | $35.74 | $2.19 | — | $4,959.53 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.1; leftover $2420.06 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 51 | $47.15 | $2.14 | — | $2,552.73 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-1.9; leftover $2420.06 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 22 | $109.67 | $2.06 | — | $137.94 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-3.0; leftover $2420.06 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $137.94 | ▼ close $11,920.95 vs 09:30 $12,100.32 (session -168.82) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $137.94 | ▲ 09:30 equity $11,923.34 vs yday $11,920.95 (+2.39) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $137.94 | ▲ close $12,100.84 vs 09:30 $11,923.34 (session +177.50) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.26 | ▲ 09:30 equity $8,255.35 vs yday $8,255.35 (+0.00) | 09:30 open · cash $99.26 (unchanged overnight, no fees) · equity $8,255.35 vs prior close $8,255.35 (+0.00) · 4 name(s) re-marked at the open (per-name table). ABVX×21 yday $94.87 → 09:30 $94.87 +0.00; ANAB×39 yday $51.70 → 09:30 $51.70 +0.00; MLKN×105 yday $19.91 → 09:30 $19.91 +0.00; THO×29 yday $70.93 → 09:30 $70.93 +0.00 | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.26 | ▲ close $8,255.35 vs 09:30 $8,255.35 (session +0.00) | 16:00 close · cash $99.26 · equity $8,255.35 vs 09:30 $8,255.35 (+0.00; session marks +0.00) · 4 name(s) marked open→close (per-name table). ABVX×21 09:30 $94.87 → close $94.87 +0.00; ANAB×39 09:30 $51.70 → close $51.70 +0.00; MLKN×105 09:30 $19.91 → close $19.91 -0.00; THO×29 09:30 $70.93 → close $70.93 +0.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `NMAX` | cash | leftover split 2.63 < 1 share @ 9.89 |
| 2026-08-14 | `AIRJ` | cash | leftover split 2.63 < 1 share @ 5.51 |
| 2026-08-14 | `AMAT` | cash | leftover split 2.63 < 1 share @ 499.40 |
| 2026-08-14 | `AMPG` | cash | leftover split 2.63 < 1 share @ 4.37 |
| 2026-08-14 | `BRUN` | cash | leftover split 2.63 < 1 share @ 26.25 |
| 2026-08-14 | `DGXX` | cash | leftover split 2.63 < 1 share @ 3.92 |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BZAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BZAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-08-21 | `AAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AEG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ATAT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ATHM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BILL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BULL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BEKE` | cash | leftover split 15.48 < 1 share @ 17.93 |
| 2026-08-21 | `BJ` | cash | leftover split 15.48 < 1 share @ 93.98 |
| 2026-08-21 | `BKE` | cash | leftover split 15.48 < 1 share @ 43.08 |
| 2026-08-21 | `ROST` | cash | leftover split 15.48 < 1 share @ 243.85 |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AEG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ATAT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ATHM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BILL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BULL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `PSEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `PSEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `BMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BNS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SHMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ANF` | cash | leftover split 32.58 < 1 share @ 131.37 |
| 2026-08-26 | `BOX` | cash | leftover split 32.58 < 1 share @ 34.30 |
| 2026-08-26 | `DY` | cash | leftover split 32.58 < 1 share @ 326.91 |
| 2026-08-26 | `HEI` | cash | leftover split 32.58 < 1 share @ 370.00 |
| 2026-08-26 | `INTU` | cash | leftover split 32.58 < 1 share @ 323.47 |
| 2026-08-27 | `BMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BNS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SHMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `TIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `FSCO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BBY` | cash | leftover split 22.48 < 1 share @ 80.60 |
| 2026-08-27 | `CM` | cash | leftover split 22.48 < 1 share @ 118.77 |
| 2026-08-27 | `HQY` | cash | leftover split 22.48 < 1 share @ 97.16 |
| 2026-08-27 | `RY` | cash | leftover split 22.48 < 1 share @ 206.82 |
| 2026-08-27 | `TD` | cash | leftover split 22.48 < 1 share @ 120.17 |
| 2026-08-28 | `TIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FSCO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BILI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `CMBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `CSIQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BILI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CMBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CSIQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBAR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ESTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FINV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBAR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ESTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FINV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CHPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CPB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HPE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MEI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DOCU` | cash | leftover split 66.73 < 1 share @ 68.52 |
| 2026-09-04 | `GWRE` | cash | leftover split 66.73 < 1 share @ 167.55 |
| 2026-09-04 | `LULU` | cash | leftover split 66.73 < 1 share @ 98.15 |
| 2026-09-08 | `AI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CHPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CPB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HPE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MEI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `AMBA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ASAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `DOMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `IOT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `MAMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AMBA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ASAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `DOMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `IOT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MAMA` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ADBE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CPRT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `DSGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `KR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `LPTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `REF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ADBE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CPRT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `DSGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `KR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `LPTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `REF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `TCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ALMU` | cash | leftover split 4.64 < 1 share @ 11.21 |
| 2026-09-17 | `LEN` | cash | leftover split 4.64 < 1 share @ 81.00 |
| 2026-09-18 | `TCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-24 | `CBRL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CTAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `GIS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `KBH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `PAYX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CBRL` | 50 | 2026-09-23 @ $47.57 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-11.2; leftover $2420.06 |
| `CTAS` | 12 | 2026-09-23 @ $196.78 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $2420.06 |
| `GIS` | 67 | 2026-09-23 @ $35.74 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.1; leftover $2420.06 |
| `KBH` | 51 | 2026-09-23 @ $47.15 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-1.9; leftover $2420.06 |
| `PAYX` | 22 | 2026-09-23 @ $109.67 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-3.0; leftover $2420.06 |
