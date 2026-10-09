# Factor mine action — `short_extended_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · ret_5>15

Cash book **-12.68%** ($8,733) · signal-only (no cash/fees) was -55.91%. Starts YES **1/30**. Fills 301 · skips 265 · realized $-1568.09.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: prior 5-session return is at least 15%.

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a SHORT sleeve: it borrows the name and profits if the price falls. Equity treats the short as a liability (must keep enough to cover).

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ret_5_min=15.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $17,651.78.

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
| 2026-08-13 09:30 ET | **SHORT** | `TNDM` | 214 | $23.33 | $2.97 | — | $14,989.65 | — | ret_5>15; gate ret_5_min=15.0; list flatten; ⚪; ret5=+19.7; leftover $5000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,989.65 | ▲ close $10,039.83 vs 09:30 $10,000.00 (session +42.80) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,989.65 | ▲ 09:30 equity $10,084.77 vs yday $10,039.83 (+44.94) | — | — |
| 2026-08-14 09:30 ET | **SHORT** | `ARX` | 32 | $19.57 | $2.12 | — | $15,613.76 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $630.30 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OMER` | 36 | $17.35 | $2.14 | — | $16,236.23 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $630.30 | — |
| 2026-08-14 09:30 ET | **SHORT** | `AIRO` | 56 | $11.12 | $2.20 | — | $16,856.75 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $630.30 | — |
| 2026-08-14 09:30 ET | **SHORT** | `MXCT` | 453 | $1.39 | $5.95 | — | $17,480.48 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+25.2; leftover $630.30 | — |
| 2026-08-14 09:30 ET | **SHORT** | `QMLS` | 86 | $7.29 | $2.29 | — | $18,105.13 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $630.30 | — |
| 2026-08-14 09:30 ET | **SHORT** | `AVAH` | 52 | $11.91 | $2.18 | — | $18,722.27 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+21.3; leftover $630.30 | — |
| 2026-08-14 09:30 ET | **SHORT** | `TBBB` | 12 | $48.82 | $2.06 | — | $19,306.04 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $630.30 | — |
| 2026-08-14 09:30 ET | **SHORT** | `AMPY` | 127 | $4.94 | $2.42 | — | $19,931.00 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.4; leftover $630.30 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,931.00 | ▲ close $10,238.94 vs 09:30 $10,084.77 (session +175.53) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,931.00 | ▲ 09:30 equity $10,294.30 vs yday $10,238.94 (+55.36) | — | — |
| 2026-08-17 09:30 ET | **SHORT** | `CAPR` | 93 | $6.87 | $2.31 | — | $20,567.60 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+62.6; leftover $643.39 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HTFL` | 15 | $41.23 | $2.07 | — | $21,183.98 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+46.0; leftover $643.39 | — |
| 2026-08-17 09:30 ET | **SHORT** | `UMAC` | 19 | $32.55 | $2.08 | — | $21,800.35 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $643.39 | — |
| 2026-08-17 09:30 ET | **SHORT** | `NPWR` | 335 | $1.92 | $4.40 | — | $22,439.14 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $643.39 | — |
| 2026-08-17 09:30 ET | **SHORT** | `LPTH` | 43 | $14.94 | $2.16 | — | $23,079.40 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $643.39 | — |
| 2026-08-17 09:30 ET | **SHORT** | `NMAX` | 58 | $10.97 | $2.20 | — | $23,713.46 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $643.39 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ALOY` | 43 | $14.66 | $2.16 | — | $24,341.69 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $643.39 | — |
| 2026-08-17 09:30 ET | **SHORT** | `INO` | 601 | $1.07 | $7.88 | — | $24,976.88 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+62.7; leftover $643.39 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,976.88 | ▲ close $10,373.14 vs 09:30 $10,294.30 (session +104.10) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,976.88 | ▲ 09:30 equity $10,561.20 vs yday $10,373.14 (+188.06) | — | — |
| 2026-08-18 09:30 ET | **COVER** | `TNDM` | 214 | $22.16 | $2.76 | $+244.65 | $20,231.88 | ▲ +244.65 after sell → book $10,558.44; vs 09:30 mark -2.76 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,231.88 | ▼ close $10,434.83 vs 09:30 $10,561.20 (session -123.61) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,231.88 | ▼ 09:30 equity $10,346.15 vs yday $10,434.83 (-88.68) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `ARX` | 32 | $19.58 | $2.09 | $-4.53 | $19,603.23 | ▼ -4.53 after sell → book $10,344.06; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OMER` | 36 | $17.13 | $2.10 | $+3.69 | $18,984.45 | ▲ +3.69 after sell → book $10,341.96; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `AIRO` | 56 | $9.10 | $2.16 | $+108.77 | $18,472.69 | ▲ +108.77 after sell → book $10,339.80; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `MXCT` | 453 | $1.29 | $5.84 | $+33.51 | $17,882.48 | ▲ +33.51 after sell → book $10,333.96; vs 09:30 mark -5.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `QMLS` | 86 | $6.74 | $2.25 | $+42.76 | $17,300.59 | ▲ +42.76 after sell → book $10,331.71; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `AVAH` | 52 | $12.92 | $2.15 | $-56.85 | $16,626.61 | ▼ -56.85 after sell → book $10,329.57; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `TBBB` | 12 | $48.62 | $2.03 | $-1.69 | $16,041.14 | ▼ -1.69 after sell → book $10,327.54; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `AMPY` | 127 | $4.88 | $2.37 | $+2.83 | $15,419.01 | ▲ +2.83 after sell → book $10,325.17; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,419.01 | ▲ close $10,375.44 vs 09:30 $10,346.15 (session +50.27) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,419.01 | ▲ 09:30 equity $10,436.28 vs yday $10,375.44 (+60.84) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `CAPR` | 93 | $7.66 | $2.27 | $-78.05 | $14,704.36 | ▼ -78.05 after sell → book $10,434.01; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HTFL` | 15 | $45.90 | $2.04 | $-74.16 | $14,013.83 | ▼ -74.16 after sell → book $10,431.98; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `UMAC` | 19 | $28.32 | $2.05 | $+76.24 | $13,473.70 | ▲ +76.24 after sell → book $10,429.93; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `NPWR` | 335 | $1.64 | $4.32 | $+85.07 | $12,919.98 | ▲ +85.07 after sell → book $10,425.61; vs 09:30 mark -4.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `LPTH` | 43 | $13.09 | $2.12 | $+75.27 | $12,354.99 | ▲ +75.27 after sell → book $10,423.49; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `NMAX` | 58 | $10.89 | $2.16 | $+0.27 | $11,721.20 | ▲ +0.27 after sell → book $10,421.32; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ALOY` | 43 | $12.06 | $2.12 | $+107.52 | $11,200.50 | ▲ +107.52 after sell → book $10,419.20; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `INO` | 601 | $1.30 | $7.75 | $-153.86 | $10,411.45 | ▼ -153.86 after sell → book $10,411.45; vs 09:30 mark -7.75 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `MRNA` | 4 | $150.14 | $2.04 | — | $11,009.97 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $650.72 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AZI` | 474 | $1.37 | $6.22 | — | $11,653.13 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $650.72 | — |
| 2026-08-20 09:30 ET | **SHORT** | `CYPH` | 565 | $1.15 | $7.41 | — | $12,295.47 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $650.72 | — |
| 2026-08-20 09:30 ET | **SHORT** | `BNTX` | 5 | $109.06 | $2.04 | — | $12,838.73 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $650.72 | — |
| 2026-08-20 09:30 ET | **SHORT** | `BTGO` | 98 | $6.61 | $2.33 | — | $13,483.69 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $650.72 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ASST` | 40 | $16.00 | $2.15 | — | $14,121.55 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $650.72 | — |
| 2026-08-20 09:30 ET | **SHORT** | `PPC` | 21 | $30.65 | $2.09 | — | $14,763.11 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+16.5; leftover $650.72 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 55 | $11.81 | $2.19 | — | $15,410.74 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $650.72 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,410.74 | ▼ close $10,383.71 vs 09:30 $10,436.28 (session -1.28) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,410.74 | ▼ 09:30 equity $10,208.28 vs yday $10,383.71 (-175.43) | — | — |
| 2026-08-21 09:30 ET | **SHORT** | `AU` | 6 | $119.43 | $2.05 | — | $16,125.27 | — | ret_5>15; gate ret_5_min=15.0; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $729.16 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AEM` | 3 | $216.30 | $2.04 | — | $16,772.13 | — | ret_5>15; gate ret_5_min=15.0; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $729.16 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARCT` | 65 | $11.13 | $2.23 | — | $17,493.36 | — | ret_5>15; gate ret_5_min=15.0; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $729.16 | — |
| 2026-08-21 09:30 ET | **SHORT** | `INDP` | 524 | $1.39 | $6.88 | — | $18,214.84 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $729.16 | — |
| 2026-08-21 09:30 ET | **SHORT** | `CAN` | 2480 | $0.29 | $15.17 | — | $18,928.79 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $729.16 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRVI` | 88 | $8.28 | $2.30 | — | $19,655.13 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $729.16 | — |
| 2026-08-21 09:30 ET | **SHORT** | `DFDV` | 180 | $4.04 | $2.59 | — | $20,379.74 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $729.16 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,379.74 | ▼ close $9,752.17 vs 09:30 $10,208.28 (session -422.86) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,379.74 | ▼ 09:30 equity $9,461.17 vs yday $9,752.17 (-291.00) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,379.74 | ▲ close $9,691.19 vs 09:30 $9,461.17 (session +230.02) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,379.74 | ▲ 09:30 equity $9,803.27 vs yday $9,691.19 (+112.08) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `MRNA` | 4 | $143.50 | $2.00 | $+22.52 | $19,803.74 | ▲ +22.52 after sell → book $9,801.27; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AZI` | 474 | $1.31 | $6.11 | $+16.10 | $19,176.68 | ▲ +16.10 after sell → book $9,795.15; vs 09:30 mark -6.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `BNTX` | 5 | $113.88 | $2.00 | $-28.14 | $18,605.28 | ▼ -28.14 after sell → book $9,793.15; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `BTGO` | 98 | $6.75 | $2.28 | $-18.82 | $17,941.49 | ▼ -18.82 after sell → book $9,790.86; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ASST` | 40 | $19.04 | $2.11 | $-125.86 | $17,177.78 | ▼ -125.86 after sell → book $9,788.75; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `PPC` | 21 | $31.47 | $2.05 | $-21.36 | $16,514.86 | ▼ -21.36 after sell → book $9,786.70; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 55 | $11.00 | $2.15 | $+40.48 | $15,907.71 | ▲ +40.48 after sell → book $9,784.55; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMEA` | 428 | $1.63 | $5.62 | — | $16,599.73 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $698.90 | — |
| 2026-08-25 09:30 ET | **SHORT** | `NPWR` | 349 | $2.00 | $4.59 | — | $17,293.14 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+15.0; leftover $698.90 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ALVO` | 133 | $5.24 | $2.44 | — | $17,987.62 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $698.90 | — |
| 2026-08-25 09:30 ET | **SHORT** | `SUJA` | 79 | $8.79 | $2.27 | — | $18,679.76 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $698.90 | — |
| 2026-08-25 09:30 ET | **SHORT** | `FWDI` | 122 | $5.71 | $2.41 | — | $19,373.97 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $698.90 | — |
| 2026-08-25 09:30 ET | **SHORT** | `DEFT` | 1127 | $0.62 | $10.58 | — | $20,062.13 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $698.90 | — |
| 2026-08-25 09:30 ET | **SHORT** | `GORO` | 196 | $3.55 | $2.64 | — | $20,755.29 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+27.9; leftover $698.90 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,755.29 | ▼ close $9,181.04 vs 09:30 $9,803.27 (session -572.96) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,755.29 | ▲ 09:30 equity $9,388.65 vs yday $9,181.04 (+207.61) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `CYPH` | 565 | $1.60 | $7.29 | $-268.95 | $19,844.00 | ▼ -268.95 after sell → book $9,381.36; vs 09:30 mark -7.29 | dropped from list after 4 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AU` | 6 | $119.80 | $2.01 | $-6.28 | $19,123.19 | ▼ -6.28 after sell → book $9,379.36; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AEM` | 3 | $219.50 | $2.00 | $-13.64 | $18,462.69 | ▼ -13.64 after sell → book $9,377.36; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARCT` | 65 | $15.35 | $2.19 | $-278.71 | $17,462.76 | ▼ -278.71 after sell → book $9,375.17; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `CAN` | 2480 | $0.40 | $17.29 | $-287.90 | $16,460.91 | ▼ -287.90 after sell → book $9,357.89; vs 09:30 mark -17.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRVI` | 88 | $8.85 | $2.25 | $-54.71 | $15,679.86 | ▼ -54.71 after sell → book $9,355.63; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `DFDV` | 180 | $4.35 | $2.53 | $-60.92 | $14,894.33 | ▼ -60.92 after sell → book $9,353.10; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `CAPR` | 94 | $8.29 | $2.32 | — | $15,671.27 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $779.43 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BRR` | 354 | $2.20 | $4.66 | — | $16,445.41 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+17.8; leftover $779.43 | — |
| 2026-08-26 09:30 ET | **SHORT** | `USDE` | 134 | $5.81 | $2.45 | — | $17,221.51 | — | ret_5>15; gate ret_5_min=15.0; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $779.43 | — |
| 2026-08-26 09:30 ET | **SHORT** | `FIGR` | 19 | $40.50 | $2.09 | — | $17,988.92 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+15.8; leftover $779.43 | — |
| 2026-08-26 09:30 ET | **SHORT** | `MNRO` | 55 | $14.00 | $2.20 | — | $18,756.72 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.8; leftover $779.43 | — |
| 2026-08-26 09:30 ET | **SHORT** | `FUTU` | 6 | $124.67 | $2.05 | — | $19,502.69 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+15.7; leftover $779.43 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,502.69 | ▲ close $9,446.50 vs 09:30 $9,388.65 (session +109.15) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,502.69 | ▼ 09:30 equity $9,299.94 vs yday $9,446.50 (-146.56) | — | — |
| 2026-08-27 09:30 ET | **SHORT** | `BZ` | 83 | $18.50 | $2.31 | — | $21,035.89 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+17.2; leftover $1549.99 | — |
| 2026-08-27 09:30 ET | **SHORT** | `AQST` | 287 | $5.39 | $3.80 | — | $22,579.01 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.4; leftover $1549.99 | — |
| 2026-08-27 09:30 ET | **SHORT** | `VYX` | 173 | $8.95 | $2.59 | — | $24,124.77 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+16.2; leftover $1549.99 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,124.77 | ▼ close $8,990.93 vs 09:30 $9,299.94 (session -300.31) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,124.77 | ▲ 09:30 equity $9,150.69 vs yday $8,990.93 (+159.76) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `INDP` | 524 | $1.16 | $6.76 | $+106.88 | $23,510.17 | ▲ +106.88 after sell → book $9,143.93; vs 09:30 mark -6.76 | dropped from list after 5 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMEA` | 428 | $1.69 | $5.52 | $-36.82 | $22,781.33 | ▼ -36.82 after sell → book $9,138.41; vs 09:30 mark -5.52 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `NPWR` | 349 | $1.89 | $4.50 | $+29.30 | $22,117.22 | ▲ +29.30 after sell → book $9,133.91; vs 09:30 mark -4.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ALVO` | 133 | $4.84 | $2.39 | $+48.37 | $21,471.11 | ▲ +48.37 after sell → book $9,131.52; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `SUJA` | 79 | $9.08 | $2.23 | $-27.41 | $20,751.56 | ▼ -27.41 after sell → book $9,129.30; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `FWDI` | 122 | $6.73 | $2.36 | $-129.20 | $19,928.14 | ▼ -129.20 after sell → book $9,126.94; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `DEFT` | 1127 | $0.64 | $10.54 | $-38.03 | $19,201.96 | ▼ -38.03 after sell → book $9,116.40; vs 09:30 mark -10.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `GORO` | 196 | $3.80 | $2.58 | $-54.22 | $18,454.58 | ▼ -54.22 after sell → book $9,113.82; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SLI` | 283 | $2.68 | $3.73 | — | $19,209.30 | — | ret_5>15; gate ret_5_min=15.0; list flatten,ohlc_hot; ret5=+16.3; leftover $759.49 | — |
| 2026-08-28 09:30 ET | **SHORT** | `ANF` | 5 | $146.07 | $2.05 | — | $19,937.60 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $759.49 | — |
| 2026-08-28 09:30 ET | **SHORT** | `BHVN` | 47 | $15.88 | $2.17 | — | $20,681.79 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+19.4; leftover $759.49 | — |
| 2026-08-28 09:30 ET | **SHORT** | `LVWR` | 546 | $1.39 | $7.16 | — | $21,433.56 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+20.4; leftover $759.49 | — |
| 2026-08-28 09:30 ET | **SHORT** | `OPTU` | 759 | $1.00 | $9.95 | — | $22,182.62 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; ret5=+16.8; leftover $759.49 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SBET` | 87 | $8.65 | $2.30 | — | $22,932.87 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.0; leftover $759.49 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,932.87 | ▲ close $9,346.60 vs 09:30 $9,150.69 (session +260.13) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,932.87 | ▲ 09:30 equity $9,397.38 vs yday $9,346.60 (+50.78) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `CAPR` | 94 | $9.50 | $2.27 | $-118.33 | $22,037.60 | ▼ -118.33 after sell → book $9,395.10; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `BRR` | 354 | $2.23 | $4.57 | $-19.84 | $21,243.61 | ▼ -19.84 after sell → book $9,390.54; vs 09:30 mark -4.56 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `USDE` | 134 | $6.76 | $2.39 | $-132.14 | $20,335.38 | ▼ -132.14 after sell → book $9,388.14; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `FIGR` | 19 | $35.77 | $2.05 | $+85.73 | $19,653.70 | ▲ +85.73 after sell → book $9,386.10; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `MNRO` | 55 | $12.77 | $2.15 | $+63.30 | $18,949.20 | ▲ +63.30 after sell → book $9,383.94; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `FUTU` | 6 | $123.67 | $2.01 | $+1.94 | $18,205.17 | ▲ +1.94 after sell → book $9,381.93; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,205.17 | ▲ close $9,503.84 vs 09:30 $9,397.38 (session +121.90) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,205.17 | ▲ 09:30 equity $9,548.62 vs yday $9,503.84 (+44.78) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `BZ` | 83 | $17.29 | $2.24 | $+95.88 | $16,767.86 | ▲ +95.88 after sell → book $9,546.39; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `AQST` | 287 | $5.15 | $3.70 | $+61.37 | $15,286.11 | ▲ +61.37 after sell → book $9,542.68; vs 09:30 mark -3.71 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `VYX` | 173 | $8.30 | $2.51 | $+107.35 | $13,847.70 | ▲ +107.35 after sell → book $9,540.17; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,847.70 | ▲ close $9,654.14 vs 09:30 $9,548.62 (session +113.97) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,847.70 | ▼ 09:30 equity $9,650.35 vs yday $9,654.14 (-3.79) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SLI` | 283 | $2.49 | $3.65 | $+46.39 | $13,139.38 | ▲ +46.39 after sell → book $9,646.70; vs 09:30 mark -3.65 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `ANF` | 5 | $139.65 | $2.00 | $+28.05 | $12,439.12 | ▲ +28.05 after sell → book $9,644.70; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `BHVN` | 47 | $15.97 | $2.13 | $-8.53 | $11,686.40 | ▼ -8.53 after sell → book $9,642.57; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `LVWR` | 546 | $1.17 | $7.04 | $+105.91 | $11,040.54 | ▲ +105.91 after sell → book $9,635.52; vs 09:30 mark -7.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `OPTU` | 759 | $0.93 | $9.36 | $+31.55 | $10,323.03 | ▲ +31.55 after sell → book $9,626.16; vs 09:30 mark -9.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SBET` | 87 | $8.01 | $2.25 | $+51.13 | $9,623.91 | ▲ +51.13 after sell → book $9,623.91; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,623.91 | ▲ close $9,623.91 vs 09:30 $9,650.35 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,623.91 | ▲ 09:30 equity $9,623.91 vs yday $9,623.91 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **SHORT** | `GPRO` | 337 | $1.78 | $4.43 | — | $10,219.34 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+183.1; leftover $601.49 | — |
| 2026-09-03 09:30 ET | **SHORT** | `FRVO` | 32 | $18.28 | $2.12 | — | $10,802.18 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+16.5; leftover $601.49 | — |
| 2026-09-03 09:30 ET | **SHORT** | `MMED` | 25 | $23.88 | $2.10 | — | $11,397.08 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $601.49 | — |
| 2026-09-03 09:30 ET | **SHORT** | `CNXC` | 18 | $32.88 | $2.08 | — | $11,986.84 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+16.2; leftover $601.49 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SION` | 82 | $7.31 | $2.28 | — | $12,583.98 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+18.5; leftover $601.49 | — |
| 2026-09-03 09:30 ET | **SHORT** | `CNH` | 43 | $13.71 | $2.16 | — | $13,171.36 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+17.5; leftover $601.49 | — |
| 2026-09-03 09:30 ET | **SHORT** | `TARS` | 7 | $82.76 | $2.05 | — | $13,748.63 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.1; leftover $601.49 | — |
| 2026-09-03 09:30 ET | **SHORT** | `DFDV` | 107 | $5.59 | $2.36 | — | $14,344.94 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.0; leftover $601.49 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,344.94 | ▲ close $9,758.48 vs 09:30 $9,623.91 (session +154.13) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,344.94 | ▲ 09:30 equity $9,769.44 vs yday $9,758.48 (+10.96) | — | — |
| 2026-09-04 09:30 ET | **SHORT** | `BAK` | 419 | $1.94 | $5.51 | — | $15,152.29 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+18.3; leftover $814.12 | — |
| 2026-09-04 09:30 ET | **SHORT** | `SLBT` | 258 | $3.15 | $3.40 | — | $15,961.59 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+19.3; leftover $814.12 | — |
| 2026-09-04 09:30 ET | **SHORT** | `IRD` | 179 | $4.53 | $2.59 | — | $16,769.87 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $814.12 | — |
| 2026-09-04 09:30 ET | **SHORT** | `FMC` | 62 | $12.95 | $2.22 | — | $17,570.55 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+21.8; leftover $814.12 | — |
| 2026-09-04 09:30 ET | **SHORT** | `BRR` | 324 | $2.51 | $4.27 | — | $18,379.53 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $814.12 | — |
| 2026-09-04 09:30 ET | **SHORT** | `LENZ` | 141 | $5.75 | $2.47 | — | $19,187.81 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $814.12 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,187.81 | ▼ close $9,523.93 vs 09:30 $9,769.44 (session -225.06) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,187.81 | ▲ 09:30 equity $9,583.97 vs yday $9,523.93 (+60.04) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,187.81 | ▲ close $9,878.37 vs 09:30 $9,583.97 (session +294.40) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,187.81 | ▼ 09:30 equity $9,685.20 vs yday $9,878.37 (-193.17) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `GPRO` | 337 | $1.45 | $4.35 | $+102.43 | $18,694.81 | ▲ +102.43 after sell → book $9,680.86; vs 09:30 mark -4.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `FRVO` | 32 | $18.60 | $2.09 | $-14.45 | $18,097.53 | ▼ -14.45 after sell → book $9,678.77; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `MMED` | 25 | $23.22 | $2.06 | $+12.33 | $17,514.96 | ▲ +12.33 after sell → book $9,676.71; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `CNXC` | 18 | $28.13 | $2.04 | $+81.38 | $17,006.58 | ▲ +81.38 after sell → book $9,674.66; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `SION` | 82 | $7.27 | $2.24 | $-1.23 | $16,408.20 | ▼ -1.23 after sell → book $9,672.43; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `CNH` | 43 | $13.64 | $2.12 | $-1.26 | $15,819.56 | ▼ -1.26 after sell → book $9,670.31; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `TARS` | 7 | $86.31 | $2.01 | $-28.91 | $15,213.38 | ▼ -28.91 after sell → book $9,668.30; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `DFDV` | 107 | $6.02 | $2.31 | $-50.14 | $14,566.93 | ▼ -50.14 after sell → book $9,665.99; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,566.93 | ▼ close $9,661.52 vs 09:30 $9,685.20 (session -4.46) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,566.93 | ▲ 09:30 equity $9,663.06 vs yday $9,661.52 (+1.54) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `BAK` | 419 | $1.97 | $5.41 | $-23.48 | $13,736.10 | ▼ -23.48 after sell → book $9,657.66; vs 09:30 mark -5.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `SLBT` | 258 | $2.58 | $3.33 | $+140.33 | $13,067.13 | ▲ +140.33 after sell → book $9,654.33; vs 09:30 mark -3.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `FMC` | 62 | $12.07 | $2.18 | $+50.17 | $12,316.61 | ▲ +50.17 after sell → book $9,652.15; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `BRR` | 324 | $2.87 | $4.18 | $-125.09 | $11,382.55 | ▼ -125.09 after sell → book $9,647.97; vs 09:30 mark -4.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `LENZ` | 141 | $4.85 | $2.41 | $+122.02 | $10,696.29 | ▲ +122.02 after sell → book $9,645.56; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,696.29 | ▼ close $9,609.76 vs 09:30 $9,663.06 (session -35.80) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,696.29 | ▼ 09:30 equity $9,593.65 vs yday $9,609.76 (-16.11) | — | — |
| 2026-09-11 09:30 ET | **COVER** | `IRD` | 179 | $6.16 | $2.53 | $-296.89 | $9,591.12 | ▼ -296.89 after sell → book $9,591.12; vs 09:30 mark -2.53 | dropped from list after 4 sess (min 3) | — |
| 2026-09-11 09:30 ET | **SHORT** | `COHU` | 10 | $56.09 | $2.06 | — | $10,149.97 | — | ret_5>15; gate ret_5_min=15.0; list flatten; 🔵; ret5=+19.6; leftover $599.45 | — |
| 2026-09-11 09:30 ET | **SHORT** | `INDP` | 222 | $2.70 | $2.93 | — | $10,746.44 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $599.45 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CMRC` | 191 | $3.13 | $2.62 | — | $11,341.65 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+24.2; leftover $599.45 | — |
| 2026-09-11 09:30 ET | **SHORT** | `WLTH` | 54 | $10.95 | $2.19 | — | $11,930.76 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $599.45 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BNC` | 122 | $4.91 | $2.40 | — | $12,527.38 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $599.45 | — |
| 2026-09-11 09:30 ET | **SHORT** | `SWKS` | 7 | $84.27 | $2.05 | — | $13,115.22 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $599.45 | — |
| 2026-09-11 09:30 ET | **SHORT** | `ANGX` | 111 | $5.38 | $2.37 | — | $13,710.03 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+19.8; leftover $599.45 | — |
| 2026-09-11 09:30 ET | **SHORT** | `APPS` | 50 | $11.88 | $2.18 | — | $14,301.86 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+20.7; leftover $599.45 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,301.86 | ▼ close $9,486.64 vs 09:30 $9,593.65 (session -85.69) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,301.86 | ▲ 09:30 equity $9,510.04 vs yday $9,486.64 (+23.40) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,301.86 | ▼ close $9,442.20 vs 09:30 $9,510.04 (session -67.84) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,301.86 | ▼ 09:30 equity $9,401.55 vs yday $9,442.20 (-40.65) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,301.86 | ▼ close $9,340.33 vs 09:30 $9,401.55 (session -61.22) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,301.86 | ▲ 09:30 equity $9,366.58 vs yday $9,340.33 (+26.25) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `COHU` | 10 | $51.29 | $2.02 | $+43.92 | $13,786.94 | ▲ +43.92 after sell → book $9,364.56; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CMRC` | 191 | $3.48 | $2.56 | $-72.03 | $13,119.69 | ▼ -72.03 after sell → book $9,361.99; vs 09:30 mark -2.57 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `WLTH` | 54 | $10.82 | $2.15 | $+2.68 | $12,533.26 | ▲ +2.68 after sell → book $9,359.84; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BNC` | 122 | $4.77 | $2.36 | $+12.32 | $11,948.96 | ▲ +12.32 after sell → book $9,357.48; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `ANGX` | 111 | $5.30 | $2.32 | $+4.19 | $11,358.34 | ▲ +4.19 after sell → book $9,355.16; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `APPS` | 50 | $11.30 | $2.14 | $+24.68 | $10,791.20 | ▲ +24.68 after sell → book $9,353.02; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SHORT** | `HLP` | 519 | $1.80 | $6.82 | — | $11,718.58 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $935.30 | — |
| 2026-09-16 09:30 ET | **SHORT** | `FTRE` | 47 | $19.75 | $2.18 | — | $12,644.66 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+15.7; leftover $935.30 | — |
| 2026-09-16 09:30 ET | **SHORT** | `SDGR` | 40 | $23.29 | $2.16 | — | $13,574.10 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+16.1; leftover $935.30 | — |
| 2026-09-16 09:30 ET | **SHORT** | `REF` | 59 | $15.75 | $2.21 | — | $14,501.14 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,ohlc_hot; ret5=+17.3; leftover $935.30 | — |
| 2026-09-16 09:30 ET | **SHORT** | `CRWD` | 3 | $236.92 | $2.04 | — | $15,209.86 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+15.5; leftover $935.30 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,209.86 | ▼ close $9,275.84 vs 09:30 $9,366.58 (session -61.78) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,209.86 | ▼ 09:30 equity $9,218.60 vs yday $9,275.84 (-57.24) | — | — |
| 2026-09-17 09:30 ET | **COVER** | `SWKS` | 7 | $86.76 | $2.01 | $-21.49 | $14,600.53 | ▼ -21.49 after sell → book $9,216.59; vs 09:30 mark -2.01 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **SHORT** | `RVTY` | 6 | $147.61 | $2.05 | — | $15,484.14 | — | ret_5>15; gate ret_5_min=15.0; list flatten,ohlc_hot; ret5=+17.7; leftover $921.66 | — |
| 2026-09-17 09:30 ET | **SHORT** | `IOVA` | 89 | $10.25 | $2.31 | — | $16,394.08 | — | ret_5>15; gate ret_5_min=15.0; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $921.66 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BBNX` | 41 | $22.46 | $2.16 | — | $17,312.78 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+27.3; leftover $921.66 | — |
| 2026-09-17 09:30 ET | **SHORT** | `EMAT` | 238 | $3.86 | $3.14 | — | $18,228.32 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+18.7; leftover $921.66 | — |
| 2026-09-17 09:30 ET | **SHORT** | `IQ` | 861 | $1.07 | $11.29 | — | $19,138.30 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,ohlc_hot; 🔵; ret5=+15.8; leftover $921.66 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,138.30 | ▼ close $8,859.66 vs 09:30 $9,218.60 (session -335.98) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,138.30 | ▲ 09:30 equity $8,948.37 vs yday $8,859.66 (+88.71) | — | — |
| 2026-09-18 09:30 ET | **SHORT** | `RBRK` | 6 | $108.55 | $2.05 | — | $19,787.56 | — | ret_5>15; gate ret_5_min=15.0; list flatten; ⚪; ret5=+21.3; leftover $745.70 | — |
| 2026-09-18 09:30 ET | **SHORT** | `DELL` | 1 | $593.15 | $2.03 | — | $20,378.68 | — | ret_5>15; gate ret_5_min=15.0; list flatten,ohlc_hot; ret5=+16.1; leftover $745.70 | — |
| 2026-09-18 09:30 ET | **SHORT** | `VICR` | 3 | $219.62 | $2.04 | — | $21,035.50 | — | ret_5>15; gate ret_5_min=15.0; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $745.70 | — |
| 2026-09-18 09:30 ET | **SHORT** | `ECO` | 8 | $85.00 | $2.05 | — | $21,713.45 | — | ret_5>15; gate ret_5_min=15.0; list flatten; 🔵; ⚪; ret5=+18.3; leftover $745.70 | — |
| 2026-09-18 09:30 ET | **SHORT** | `CYPH` | 245 | $3.04 | $3.23 | — | $22,453.79 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $745.70 | — |
| 2026-09-18 09:30 ET | **SHORT** | `USDE` | 78 | $9.54 | $2.27 | — | $23,195.65 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+15.8; leftover $745.70 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23,195.65 | ▲ close $8,955.83 vs 09:30 $8,948.37 (session +21.12) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23,195.65 | ▼ 09:30 equity $8,496.50 vs yday $8,955.83 (-459.33) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `INDP` | 222 | $3.55 | $2.86 | $-194.49 | $22,404.68 | ▼ -194.49 after sell → book $8,493.63; vs 09:30 mark -2.87 | dropped from list after 6 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `HLP` | 519 | $2.08 | $6.70 | $-158.83 | $21,318.47 | ▼ -158.83 after sell → book $8,486.94; vs 09:30 mark -6.69 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `FTRE` | 47 | $20.29 | $2.13 | $-29.69 | $20,362.71 | ▼ -29.69 after sell → book $8,484.81; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `SDGR` | 40 | $29.43 | $2.11 | $-249.87 | $19,183.40 | ▼ -249.87 after sell → book $8,482.70; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `REF` | 59 | $14.79 | $2.17 | $+52.26 | $18,308.62 | ▲ +52.26 after sell → book $8,480.53; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `CRWD` | 3 | $231.62 | $2.00 | $+11.86 | $17,611.76 | ▲ +11.86 after sell → book $8,478.53; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SHORT** | `TJGC` | 50 | $16.91 | $2.18 | — | $18,455.08 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+50.5; leftover $847.85 | — |
| 2026-09-21 09:30 ET | **SHORT** | `GEMI` | 147 | $5.75 | $2.49 | — | $19,298.57 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+30.3; leftover $847.85 | — |
| 2026-09-21 09:30 ET | **SHORT** | `FWDI` | 103 | $8.22 | $2.35 | — | $20,142.88 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $847.85 | — |
| 2026-09-21 09:30 ET | **SHORT** | `SECZ` | 72 | $11.67 | $2.25 | — | $20,980.87 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+31.3; leftover $847.85 | — |
| 2026-09-21 09:30 ET | **SHORT** | `FEAM` | 343 | $2.47 | $4.51 | — | $21,823.57 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+73.6; leftover $847.85 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,823.57 | ▲ close $8,640.24 vs 09:30 $8,496.50 (session +175.49) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,823.57 | ▼ 09:30 equity $8,618.59 vs yday $8,640.24 (-21.65) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `IOVA` | 89 | $10.18 | $2.26 | $+1.67 | $20,915.29 | ▲ +1.67 after sell → book $8,616.34; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SHORT** | `GLND` | 244 | $2.94 | $3.22 | — | $21,629.43 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+136.1; leftover $718.03 | — |
| 2026-09-22 09:30 ET | **SHORT** | `CRML` | 78 | $9.11 | $2.27 | — | $22,337.75 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+44.4; leftover $718.03 | — |
| 2026-09-22 09:30 ET | **SHORT** | `NUAI` | 99 | $7.23 | $2.33 | — | $23,051.18 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+36.6; leftover $718.03 | — |
| 2026-09-22 09:30 ET | **SHORT** | `VGZ` | 270 | $2.65 | $3.56 | — | $23,763.13 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+26.3; leftover $718.03 | — |
| 2026-09-22 09:30 ET | **SHORT** | `ARM` | 2 | $319.41 | $2.03 | — | $24,399.91 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+35.1; leftover $718.03 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,399.91 | ▲ close $8,640.71 vs 09:30 $8,618.59 (session +37.78) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,399.91 | ▼ 09:30 equity $8,353.68 vs yday $8,640.71 (-287.03) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `RVTY` | 6 | $142.40 | $2.01 | $+27.20 | $23,543.50 | ▲ +27.20 after sell → book $8,351.67; vs 09:30 mark -2.01 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `BBNX` | 41 | $23.00 | $2.11 | $-26.41 | $22,598.39 | ▼ -26.41 after sell → book $8,349.56; vs 09:30 mark -2.11 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `EMAT` | 238 | $3.65 | $3.07 | $+43.76 | $21,726.62 | ▲ +43.76 after sell → book $8,346.49; vs 09:30 mark -3.07 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `IQ` | 861 | $1.03 | $11.11 | $+12.05 | $20,828.68 | ▲ +12.05 after sell → book $8,335.38; vs 09:30 mark -11.11 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `RBRK` | 6 | $112.46 | $2.01 | $-27.51 | $20,151.92 | ▼ -27.51 after sell → book $8,333.38; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `DELL` | 1 | $559.00 | $1.99 | $+30.13 | $19,590.92 | ▲ +30.13 after sell → book $8,331.38; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `ECO` | 8 | $77.55 | $2.01 | $+55.53 | $18,968.51 | ▲ +55.53 after sell → book $8,329.37; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `CYPH` | 245 | $3.82 | $3.16 | $-198.72 | $18,029.45 | ▼ -198.72 after sell → book $8,326.21; vs 09:30 mark -3.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `USDE` | 78 | $13.22 | $2.22 | $-291.53 | $16,996.06 | ▼ -291.53 after sell → book $8,323.98; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SHORT** | `VKTX` | 16 | $41.76 | $2.08 | — | $17,662.15 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $693.67 | — |
| 2026-09-23 09:30 ET | **SHORT** | `BFLY` | 70 | $9.90 | $2.24 | — | $18,352.91 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $693.67 | — |
| 2026-09-23 09:30 ET | **SHORT** | `EVTL` | 945 | $0.73 | $9.96 | — | $19,036.58 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+27.5; leftover $693.67 | — |
| 2026-09-23 09:30 ET | **SHORT** | `INOD` | 9 | $70.84 | $2.05 | — | $19,672.09 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $693.67 | — |
| 2026-09-23 09:30 ET | **SHORT** | `SVIA` | 154 | $4.49 | $2.51 | — | $20,361.04 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+26.4; leftover $693.67 | — |
| 2026-09-23 09:30 ET | **SHORT** | `THM` | 242 | $2.86 | $3.19 | — | $21,049.97 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+25.5; leftover $693.67 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,049.97 | ▼ close $8,125.74 vs 09:30 $8,353.68 (session -176.22) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,049.97 | ▲ 09:30 equity $8,222.20 vs yday $8,125.74 (+96.46) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `VICR` | 3 | $274.61 | $2.00 | $-169.01 | $20,224.14 | ▼ -169.01 after sell → book $8,220.20; vs 09:30 mark -2.00 | dropped from list after 4 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `GEMI` | 147 | $5.62 | $2.43 | $+14.93 | $19,395.57 | ▲ +14.93 after sell → book $8,217.77; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `FWDI` | 103 | $7.94 | $2.30 | $+24.19 | $18,575.45 | ▲ +24.19 after sell → book $8,215.47; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `FEAM` | 343 | $2.68 | $4.42 | $-80.97 | $17,651.78 | ▼ -80.97 after sell → book $8,211.04; vs 09:30 mark -4.43 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,651.78 | ▼ close $7,254.37 vs 09:30 $8,222.20 (session -956.68) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,128.22 | ▼ 09:30 equity $8,144.76 vs yday $8,376.50 (-231.74) | 09:30 open · cash $19,128.22 (unchanged overnight, no fees) · equity $8,144.76 vs prior close $8,376.50 (-231.74) · 14 name(s) re-marked at the open (per-name table). ARM×2 yday $306.34 → 09:30 $306.34 -0.00; ARQQ×28 yday $23.12 → 09:30 $23.12 -0.00; ARQT×27 yday $26.27 → 09:30 $26.27 -0.00; BFLY×77 yday $9.41 → 09:30 $9.41 -0.00; CRML×72 yday $8.17 → 09:30 $8.17 -0.00; DNA×83 yday $10.25 → 09:30 $10.20 +4.15; GLND×225 yday $5.35 → 09:30 $6.06 -159.75; GRAL×6 yday $125.21 → 09:30 $123.50 +10.26; MAZE×27 yday $26.21 → 09:30 $26.21 -0.00; NUAI×91 yday $6.94 → 09:30 $6.94 -0.00; OMER×37 yday $20.13 → 09:30 $20.61 -17.76; TJGC×44 yday $28.20 → 09:30 $29.76 -68.64; VGZ×250 yday $2.71 → 09:30 $2.71 -0.00; VKTX×18 yday $36.75 → 09:30 $36.75 -0.00 | — |
| 2026-09-25 09:30 ET | **SHORT** | `ZSQR` | 263 | $3.86 | $3.47 | — | $20,139.93 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1018.09 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `TWST` | 5 | $184.00 | $2.05 | — | $21,057.88 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $1018.09 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `SECZ` | 62 | $16.21 | $2.22 | — | $22,060.67 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1018.09 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `QMCO` | 34 | $29.80 | $2.14 | — | $23,071.73 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+18.2; leftover $1018.09 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23,071.73 | ▲ close $8,346.31 vs 09:30 $8,144.76 (session +211.44) | 16:00 close · cash $23,071.73 · equity $8,346.31 vs 09:30 $8,144.76 (+201.55; session marks +211.44) · 18 name(s) marked open→close (per-name table). ARM×2 09:30 $306.34 → close $306.34 +0.00; ARQQ×28 09:30 $23.12 → close $23.12 -0.00; ARQT×27 09:30 $26.27 → close $26.27 -0.00; BFLY×77 09:30 $9.41 → close $9.41 +0.00; CRML×72 09:30 $8.17 → close $8.17 -0.00; DNA×83 09:30 $10.20 → close $10.66 -38.18; GLND×225 09:30 $6.06 → close $5.54 +117.00; GRAL×6 09:30 $123.50 → close $126.89 -20.34; MAZE×27 09:30 $26.21 → close $26.21 +0.00; NUAI×91 09:30 $6.94 → close $6.94 -0.00; OMER×37 09:30 $20.61 → close $20.08 +19.61; TJGC×44 09:30 $29.76 → close $26.24 +154.88; VGZ×250 09:30 $2.71 → close $2.71 -0.00; VKTX×18 09:30 $36.75 → close $36.75 -0.00; ZSQR×263 09:30 $3.86 → close $3.78 +21.04; TWST×5 09:30 $184.00 → close $182.83 +5.85; SECZ×62 09:30 $16.21 → close $15.96 +15.50; QMCO×34 09:30 $29.80 → close $31.68 -63.92 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23,071.73 | ▲ 09:30 equity $8,531.02 vs yday $8,346.31 (+184.71) | 09:30 open · cash $23,071.73 (unchanged overnight, no fees) · equity $8,531.02 vs prior close $8,346.31 (+184.71) · 18 name(s) re-marked at the open (per-name table). ARM×2 yday $306.34 → 09:30 $305.83 +1.02; ARQQ×28 yday $23.12 → 09:30 $24.30 -33.04; ARQT×27 yday $26.27 → 09:30 $26.70 -11.61; BFLY×77 yday $9.41 → 09:30 $9.28 +10.01; CRML×72 yday $8.17 → 09:30 $7.62 +39.89; DNA×83 yday $10.66 → 09:30 $10.38 +23.66; GLND×225 yday $5.54 → 09:30 $5.14 +90.45; GRAL×6 yday $126.89 → 09:30 $128.90 -12.06; MAZE×27 yday $26.21 → 09:30 $25.97 +6.48; NUAI×91 yday $6.94 → 09:30 $6.98 -3.64; OMER×37 yday $20.08 → 09:30 $19.83 +9.25; QMCO×34 yday $31.68 → 09:30 $31.65 +1.02; SECZ×62 yday $15.96 → 09:30 $16.00 -2.48; TJGC×44 yday $26.24 → 09:30 $26.50 -11.44; TWST×5 yday $182.83 → 09:30 $181.87 +4.80; VGZ×250 yday $2.71 → 09:30 $2.62 +22.50; VKTX×18 yday $36.75 → 09:30 $35.00 +31.50; ZSQR×263 yday $3.78 → 09:30 $3.71 +18.41 | — |
| 2026-09-28 09:30 ET | **COVER** | `ARM` | 2 | $305.83 | $2.00 | $+23.13 | $22,458.07 | ▲ +23.13 after sell → book $8,529.03; vs 09:30 mark -1.99 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **COVER** | `ARQQ` | 28 | $24.30 | $2.07 | $-26.31 | $21,775.60 | ▼ -26.31 after sell → book $8,526.95; vs 09:30 mark -2.08 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **COVER** | `ARQT` | 27 | $26.70 | $2.07 | $+25.25 | $21,052.63 | ▲ +25.25 after sell → book $8,524.88; vs 09:30 mark -2.07 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **COVER** | `BFLY` | 77 | $9.28 | $2.22 | $+43.25 | $20,335.85 | ▲ +43.25 after sell → book $8,522.66; vs 09:30 mark -2.22 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **COVER** | `CRML` | 72 | $7.62 | $2.21 | $+103.12 | $19,785.29 | ▲ +103.12 after sell → book $8,520.46; vs 09:30 mark -2.20 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **COVER** | `DNA` | 83 | $10.38 | $2.24 | $-107.86 | $18,921.93 | ▼ -107.86 after sell → book $8,518.22; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-28 09:30 ET | **COVER** | `GLND` | 225 | $5.14 | $2.90 | $-500.42 | $17,762.97 | ▼ -500.42 after sell → book $8,515.31; vs 09:30 mark -2.91 | dropped from list after 4 sess (min 3) | — |
| 2026-09-28 09:30 ET | **COVER** | `GRAL` | 6 | $128.90 | $2.01 | $-136.95 | $16,987.57 | ▼ -136.95 after sell → book $8,513.31; vs 09:30 mark -2.00 | dropped from list after 4 sess (min 3) | — |
| 2026-09-28 09:30 ET | **COVER** | `MAZE` | 27 | $25.97 | $2.07 | $+58.73 | $16,284.30 | ▲ +58.73 after sell → book $8,511.23; vs 09:30 mark -2.08 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **COVER** | `NUAI` | 91 | $6.98 | $2.26 | $+18.18 | $15,646.86 | ▲ +18.18 after sell → book $8,508.97; vs 09:30 mark -2.26 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **COVER** | `OMER` | 37 | $19.83 | $2.10 | $+26.10 | $14,911.05 | ▲ +26.10 after sell → book $8,506.87; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-28 09:30 ET | **COVER** | `TJGC` | 44 | $26.50 | $2.12 | $-426.24 | $13,742.93 | ▼ -426.24 after sell → book $8,504.75; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 3) | — |
| 2026-09-28 09:30 ET | **COVER** | `VGZ` | 250 | $2.62 | $3.23 | $+0.98 | $13,084.70 | ▲ +0.98 after sell → book $8,501.52; vs 09:30 mark -3.23 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **COVER** | `VKTX` | 18 | $35.00 | $2.04 | $+117.55 | $12,452.66 | ▲ +117.55 after sell → book $8,499.48; vs 09:30 mark -2.04 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,452.66 | ▼ close $8,261.98 vs 09:30 $8,531.02 (session -237.50) | 16:00 close · cash $12,452.66 · equity $8,261.98 vs 09:30 $8,531.02 (-269.04; session marks -237.50) · 4 name(s) marked open→close (per-name table). QMCO×34 09:30 $31.65 → close $31.40 +8.50; SECZ×62 09:30 $16.00 → close $16.63 -39.06; TWST×5 09:30 $181.87 → close $179.60 +11.35; ZSQR×263 09:30 $3.71 → close $4.54 -218.29 | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,452.66 | ▼ 09:30 equity $8,250.09 vs yday $8,261.98 (-11.89) | 09:30 open · cash $12,452.66 (unchanged overnight, no fees) · equity $8,250.09 vs prior close $8,261.98 (-11.89) · 4 name(s) re-marked at the open (per-name table). QMCO×34 yday $31.40 → 09:30 $32.32 -31.28; SECZ×62 yday $16.63 → 09:30 $16.59 +2.48; TWST×5 yday $179.60 → 09:30 $179.90 -1.50; ZSQR×263 yday $4.54 → 09:30 $4.47 +18.41 | — |
| 2026-09-29 09:30 ET | **SHORT** | `KOD` | 6 | $87.50 | $2.04 | — | $12,975.62 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+177.4; leftover $589.29 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **SHORT** | `QNC` | 286 | $2.06 | $3.76 | — | $13,561.01 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+24.3; leftover $589.29 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **SHORT** | `NAUT` | 497 | $1.19 | $6.52 | — | $14,143.44 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+16.2; leftover $589.29 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **SHORT** | `MX` | 147 | $3.99 | $2.48 | — | $14,727.49 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+18.4; leftover $589.29 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **SHORT** | `TJGC` | 21 | $27.68 | $2.09 | — | $15,306.68 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+62.3; leftover $589.29 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-29 09:30 ET | **SHORT** | `HLP` | 244 | $2.41 | $3.21 | — | $15,891.51 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,ohlc_hot; ret5=+17.7; leftover $589.29 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-29 09:30 ET | **SHORT** | `QTRX` | 167 | $3.52 | $2.54 | — | $16,476.80 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,ohlc_hot; ret5=+15.8; leftover $589.29 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,476.80 | ▼ close $8,116.54 vs 09:30 $8,250.09 (session -110.90) | 16:00 close · cash $16,476.80 · equity $8,116.54 vs 09:30 $8,250.09 (-133.55; session marks -110.90) · 11 name(s) marked open→close (per-name table). QMCO×34 09:30 $32.32 → close $32.86 -18.36; SECZ×62 09:30 $16.59 → close $15.84 +46.50; TWST×5 09:30 $179.90 → close $186.13 -31.15; ZSQR×263 09:30 $4.47 → close $4.15 +84.16; KOD×6 09:30 $87.50 → close $91.12 -21.72; QNC×286 09:30 $2.06 → close $1.75 +88.66; NAUT×497 09:30 $1.19 → close $1.36 -89.46; MX×147 09:30 $3.99 → close $3.89 +14.70; TJGC×21 09:30 $27.68 → close $34.80 -149.42; HLP×244 09:30 $2.41 → close $2.45 -9.76; QTRX×167 09:30 $3.52 → close $3.67 -25.05 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,476.80 | ▲ 09:30 equity $8,116.54 vs yday $8,116.54 (-0.00) | 09:30 open · cash $16,476.80 (unchanged overnight, no fees) · equity $8,116.54 vs prior close $8,116.54 (-0.00) · 11 name(s) re-marked at the open (per-name table). HLP×244 yday $2.45 → 09:30 $2.45 -0.00; KOD×6 yday $91.12 → 09:30 $91.12 -0.00; MX×147 yday $3.89 → 09:30 $3.89 -0.00; NAUT×497 yday $1.36 → 09:30 $1.36 -0.00; QMCO×34 yday $32.86 → 09:30 $32.86 -0.00; QNC×286 yday $1.75 → 09:30 $1.75 -0.00; QTRX×167 yday $3.67 → 09:30 $3.67 -0.00; SECZ×62 yday $15.84 → 09:30 $15.84 -0.00; TJGC×21 yday $34.80 → 09:30 $34.80 -0.00; TWST×5 yday $186.13 → 09:30 $186.13 -0.00; ZSQR×263 yday $4.15 → 09:30 $4.15 -0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,476.80 | ▲ close $8,116.54 vs 09:30 $8,116.54 (session +0.00) | 16:00 close · cash $16,476.80 · equity $8,116.54 vs 09:30 $8,116.54 (-0.00; session marks +0.00) · 11 name(s) marked open→close (per-name table). HLP×244 09:30 $2.45 → close $2.45 -0.00; KOD×6 09:30 $91.12 → close $91.12 -0.00; MX×147 09:30 $3.89 → close $3.89 -0.00; NAUT×497 09:30 $1.36 → close $1.36 -0.00; QMCO×34 09:30 $32.86 → close $32.86 -0.00; QNC×286 09:30 $1.75 → close $1.75 -0.00; QTRX×167 09:30 $3.67 → close $3.67 -0.00; SECZ×62 09:30 $15.84 → close $15.84 -0.00; TJGC×21 09:30 $34.80 → close $34.80 -0.00; TWST×5 09:30 $186.13 → close $186.13 -0.00; ZSQR×263 09:30 $4.15 → close $4.15 -0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,476.80 | ▲ 09:30 equity $8,294.40 vs yday $8,116.54 (+177.86) | 09:30 open · cash $16,476.80 (unchanged overnight, no fees) · equity $8,294.40 vs prior close $8,116.54 (+177.86) · 11 name(s) re-marked at the open (per-name table). HLP×244 yday $2.45 → 09:30 $2.52 -17.08; KOD×6 yday $91.12 → 09:30 $95.41 -25.74; MX×147 yday $3.89 → 09:30 $3.78 +16.17; NAUT×497 yday $1.36 → 09:30 $1.43 -32.30; QMCO×34 yday $32.86 → 09:30 $33.19 -11.22; QNC×286 yday $1.75 → 09:30 $1.57 +51.48; QTRX×167 yday $3.67 → 09:30 $3.72 -8.35; SECZ×62 yday $15.84 → 09:30 $14.55 +79.98; TJGC×21 yday $34.80 → 09:30 $26.31 +178.19; TWST×5 yday $186.13 → 09:30 $193.10 -34.85; ZSQR×263 yday $4.15 → 09:30 $4.22 -18.41 | — |
| 2026-10-01 09:30 ET | **COVER** | `HLP` | 244 | $2.52 | $3.15 | $-33.20 | $15,858.77 | ▼ -33.20 after sell → book $8,291.25; vs 09:30 mark -3.15 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **COVER** | `KOD` | 6 | $95.41 | $2.01 | $-51.51 | $15,284.30 | ▼ -51.51 after sell → book $8,289.24; vs 09:30 mark -2.01 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **COVER** | `MX` | 147 | $3.78 | $2.43 | $+25.96 | $14,726.21 | ▲ +25.96 after sell → book $8,286.81; vs 09:30 mark -2.43 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **COVER** | `NAUT` | 497 | $1.43 | $6.41 | $-134.70 | $14,009.09 | ▼ -134.70 after sell → book $8,280.40; vs 09:30 mark -6.41 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **COVER** | `QMCO` | 34 | $33.19 | $2.09 | $-119.49 | $12,878.54 | ▼ -119.49 after sell → book $8,278.31; vs 09:30 mark -2.09 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **COVER** | `QNC` | 286 | $1.57 | $3.69 | $+132.69 | $12,425.83 | ▲ +132.69 after sell → book $8,274.62; vs 09:30 mark -3.69 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **COVER** | `QTRX` | 167 | $3.72 | $2.49 | $-38.44 | $11,802.10 | ▼ -38.44 after sell → book $8,272.13; vs 09:30 mark -2.49 | exit unpriced hold on first bar after 2 sess | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **COVER** | `SECZ` | 62 | $14.55 | $2.18 | $+98.52 | $10,897.82 | ▲ +98.52 after sell → book $8,269.95; vs 09:30 mark -2.18 | exit unpriced hold on first bar after 4 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **COVER** | `TJGC` | 21 | $26.31 | $2.05 | $+24.63 | $10,343.26 | ▲ +24.63 after sell → book $8,267.90; vs 09:30 mark -2.05 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-01 09:30 ET | **COVER** | `TWST` | 5 | $193.10 | $2.00 | $-49.56 | $9,375.76 | ▼ -49.56 after sell → book $8,265.90; vs 09:30 mark -2.00 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **COVER** | `ZSQR` | 263 | $4.22 | $3.39 | $-101.55 | $8,262.50 | ▼ -101.55 after sell → book $8,262.50; vs 09:30 mark -3.40 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SHORT** | `PACB` | 220 | $2.34 | $2.90 | — | $8,774.40 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+68.1; leftover $516.41 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SHORT** | `AEHL` | 53 | $9.74 | $2.18 | — | $9,288.44 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+24.8; leftover $516.41 | join🔴 sector🟢 gen🔴 news🔴 digest🟢 judge🔴 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-10-01 09:30 ET | **SHORT** | `PMVP` | 307 | $1.68 | $4.04 | — | $9,800.17 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+21.9; leftover $516.41 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SHORT** | `MNKD` | 134 | $3.84 | $2.44 | — | $10,312.29 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+16.8; leftover $516.41 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SHORT** | `QSI` | 430 | $1.20 | $5.64 | — | $10,822.64 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; ret5=+41.0; leftover $516.41 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SHORT** | `FEAM` | 148 | $3.48 | $2.48 | — | $11,335.20 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; ret5=+27.7; leftover $516.41 | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🔴 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **SHORT** | `SHMD` | 107 | $4.80 | $2.35 | — | $11,846.45 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; ret5=+32.1; leftover $516.41 | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SHORT** | `GLND` | 102 | $5.06 | $2.34 | — | $12,360.23 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; ret5=+76.3; leftover $516.41 | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟡 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,360.23 | ▼ close $8,185.65 vs 09:30 $8,294.40 (session -52.49) | 16:00 close · cash $12,360.23 · equity $8,185.65 vs 09:30 $8,294.40 (-108.75; session marks -52.49) · 8 name(s) marked open→close (per-name table). PACB×220 09:30 $2.34 → close $2.51 -37.40; AEHL×53 09:30 $9.74 → close $10.16 -22.26; PMVP×307 09:30 $1.68 → close $1.75 -21.49; MNKD×134 09:30 $3.84 → close $3.95 -14.74; QSI×430 09:30 $1.20 → close $1.43 -98.90; FEAM×148 09:30 $3.48 → close $3.23 +37.00; SHMD×107 09:30 $4.80 → close $4.45 +37.98; GLND×102 09:30 $5.06 → close $4.40 +67.32 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,360.23 | ▲ 09:30 equity $8,253.09 vs yday $8,185.65 (+67.44) | 09:30 open · cash $12,360.23 (unchanged overnight, no fees) · equity $8,253.09 vs prior close $8,185.65 (+67.44) · 8 name(s) re-marked at the open (per-name table). AEHL×53 yday $10.16 → 09:30 $9.37 +41.87; FEAM×148 yday $3.23 → 09:30 $3.27 -5.92; GLND×102 yday $4.40 → 09:30 $4.25 +15.30; MNKD×134 yday $3.95 → 09:30 $4.01 -8.04; PACB×220 yday $2.51 → 09:30 $2.50 +2.20; PMVP×307 yday $1.75 → 09:30 $1.75 -0.00; QSI×430 yday $1.43 → 09:30 $1.38 +21.50; SHMD×107 yday $4.45 → 09:30 $4.44 +0.53 | — |
| 2026-10-02 09:30 ET | **SHORT** | `SDEV` | 116 | $5.06 | $2.38 | — | $12,944.81 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+183.7; leftover $589.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **SHORT** | `SES` | 685 | $0.86 | $8.09 | — | $13,525.82 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+54.3; leftover $589.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **SHORT** | `ACN` | 2 | $211.02 | $2.03 | — | $13,945.83 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+19.7; leftover $589.51 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **SHORT** | `SNPS` | 1 | $497.86 | $2.03 | — | $14,441.66 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+15.4; leftover $589.51 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **SHORT** | `WOLF` | 18 | $32.35 | $2.08 | — | $15,021.88 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; ret5=+18.5; leftover $589.51 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **SHORT** | `NAUT` | 354 | $1.67 | $4.65 | — | $15,606.64 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ⚪; ret5=+58.4; leftover $589.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **SHORT** | `TJGC` | 16 | $34.92 | $2.07 | — | $16,163.29 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,ohlc_hot; ret5=+15.5; leftover $589.51 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,163.29 | ▼ close $7,682.81 vs 09:30 $8,253.09 (session -546.95) | 16:00 close · cash $16,163.29 · equity $7,682.81 vs 09:30 $8,253.09 (-570.28; session marks -546.95) · 15 name(s) marked open→close (per-name table). AEHL×53 09:30 $9.37 → close $10.00 -33.39; FEAM×148 09:30 $3.27 → close $3.88 -90.28; GLND×102 09:30 $4.25 → close $3.73 +53.04; MNKD×134 09:30 $4.01 → close $3.80 +28.14; PACB×220 09:30 $2.50 → close $2.54 -8.80; PMVP×307 09:30 $1.75 → close $1.79 -12.28; QSI×430 09:30 $1.38 → close $1.55 -73.10; SHMD×107 09:30 $4.44 → close $4.57 -13.91; SDEV×116 09:30 $5.06 → close $7.48 -280.72; SES×685 09:30 $0.86 → close $0.88 -16.92; ACN×2 09:30 $211.02 → close $198.90 +24.24; SNPS×1 09:30 $497.86 → close $489.90 +7.96; WOLF×18 09:30 $32.35 → close $35.28 -52.74; NAUT×354 09:30 $1.67 → close $1.96 -104.43; TJGC×16 09:30 $34.92 → close $33.28 +26.24 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,163.29 | ▼ 09:30 equity $7,369.99 vs yday $7,682.81 (-312.82) | 09:30 open · cash $16,163.29 (unchanged overnight, no fees) · equity $7,369.99 vs prior close $7,682.81 (-312.82) · 15 name(s) re-marked at the open (per-name table). ACN×2 yday $198.90 → 09:30 $196.30 +5.20; AEHL×53 yday $10.00 → 09:30 $9.76 +12.72; FEAM×148 yday $3.88 → 09:30 $3.88 -0.00; GLND×102 yday $3.73 → 09:30 $3.92 -19.89; MNKD×134 yday $3.80 → 09:30 $3.82 -2.68; NAUT×354 yday $1.96 → 09:30 $2.18 -77.88; PACB×220 yday $2.54 → 09:30 $2.54 -0.00; PMVP×307 yday $1.79 → 09:30 $1.80 -3.07; QSI×430 yday $1.55 → 09:30 $1.52 +10.75; SDEV×116 yday $7.48 → 09:30 $9.71 -258.68; SES×685 yday $0.88 → 09:30 $0.90 -10.48; SHMD×107 yday $4.57 → 09:30 $4.51 +6.42; SNPS×1 yday $489.90 → 09:30 $496.25 -6.35; TJGC×16 yday $33.28 → 09:30 $32.73 +8.80; WOLF×18 yday $35.28 → 09:30 $34.04 +22.32 | — |
| 2026-10-05 09:30 ET | **SHORT** | `NNBR` | 200 | $4.59 | $2.66 | — | $17,078.63 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+22.9; leftover $921.25 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **SHORT** | `NEOV` | 300 | $3.07 | $3.96 | — | $17,995.68 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+39.3; leftover $921.25 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **SHORT** | `SYNA` | 7 | $120.98 | $2.05 | — | $18,840.48 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+17.9; leftover $921.25 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **SHORT** | `VSH` | 24 | $37.64 | $2.11 | — | $19,741.74 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,ohlc_hot; ret5=+17.3; leftover $921.25 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,741.74 | ▲ close $8,321.69 vs 09:30 $7,369.99 (session +962.48) | 16:00 close · cash $19,741.74 · equity $8,321.69 vs 09:30 $7,369.99 (+951.70; session marks +962.48) · 19 name(s) marked open→close (per-name table). ACN×2 09:30 $196.30 → close $195.05 +2.50; AEHL×53 09:30 $9.76 → close $10.86 -58.30; FEAM×148 09:30 $3.88 → close $3.92 -5.92; GLND×102 09:30 $3.92 → close $3.71 +21.93; MNKD×134 09:30 $3.82 → close $3.82 -0.00; NAUT×354 09:30 $2.18 → close $1.87 +109.74; PACB×220 09:30 $2.54 → close $2.87 -72.60; PMVP×307 09:30 $1.80 → close $1.78 +6.14; QSI×430 09:30 $1.52 → close $1.28 +105.35; SDEV×116 09:30 $9.71 → close $3.94 +669.32; SES×685 09:30 $0.90 → close $0.84 +43.98; SHMD×107 09:30 $4.51 → close $4.50 +1.07; SNPS×1 09:30 $496.25 → close $488.47 +7.78; TJGC×16 09:30 $32.73 → close $32.88 -2.40; WOLF×18 09:30 $34.04 → close $33.53 +9.18; NNBR×200 09:30 $4.59 → close $4.24 +70.00; NEOV×300 09:30 $3.07 → close $2.96 +33.00; SYNA×7 09:30 $120.98 → close $119.73 +8.75; VSH×24 09:30 $37.64 → close $37.10 +12.96 | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,741.74 | ▲ 09:30 equity $8,347.45 vs yday $8,321.69 (+25.76) | 09:30 open · cash $19,741.74 (unchanged overnight, no fees) · equity $8,347.45 vs prior close $8,321.69 (+25.76) · 19 name(s) re-marked at the open (per-name table). ACN×2 yday $195.05 → 09:30 $195.43 -0.76; AEHL×53 yday $10.86 → 09:30 $10.64 +11.66; FEAM×148 yday $3.92 → 09:30 $3.96 -5.92; GLND×102 yday $3.71 → 09:30 $3.48 +23.46; MNKD×134 yday $3.82 → 09:30 $3.86 -5.36; NAUT×354 yday $1.87 → 09:30 $1.87 -0.00; NEOV×300 yday $2.96 → 09:30 $2.97 -3.00; NNBR×200 yday $4.24 → 09:30 $4.24 -0.00; PACB×220 yday $2.87 → 09:30 $2.99 -26.40; PMVP×307 yday $1.78 → 09:30 $1.78 -0.00; QSI×430 yday $1.28 → 09:30 $1.29 -4.30; SDEV×116 yday $3.94 → 09:30 $3.48 +53.36; SES×685 yday $0.84 → 09:30 $0.84 -1.10; SHMD×107 yday $4.50 → 09:30 $4.46 +4.28; SNPS×1 yday $488.47 → 09:30 $491.10 -2.63; SYNA×7 yday $119.73 → 09:30 $120.65 -6.44; TJGC×16 yday $32.88 → 09:30 $33.00 -1.92; VSH×24 yday $37.10 → 09:30 $37.13 -0.72; WOLF×18 yday $33.53 → 09:30 $34.00 -8.46 | — |
| 2026-10-06 09:30 ET | **COVER** | `AEHL` | 53 | $10.64 | $2.15 | $-52.03 | $19,175.67 | ▼ -52.03 after sell → book $8,345.30; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **COVER** | `FEAM` | 148 | $3.96 | $2.43 | $-75.96 | $18,587.16 | ▼ -75.96 after sell → book $8,342.87; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **COVER** | `GLND` | 102 | $3.48 | $2.30 | $+156.53 | $18,229.90 | ▲ +156.53 after sell → book $8,340.57; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **COVER** | `MNKD` | 134 | $3.86 | $2.39 | $-7.51 | $17,710.27 | ▼ -7.51 after sell → book $8,338.18; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **COVER** | `PACB` | 220 | $2.99 | $2.84 | $-148.74 | $17,049.63 | ▼ -148.74 after sell → book $8,335.34; vs 09:30 mark -2.84 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **COVER** | `PMVP` | 307 | $1.78 | $3.96 | $-38.70 | $16,499.21 | ▼ -38.70 after sell → book $8,331.38; vs 09:30 mark -3.96 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **COVER** | `QSI` | 430 | $1.29 | $5.55 | $-49.89 | $15,938.96 | ▼ -49.89 after sell → book $8,325.83; vs 09:30 mark -5.55 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **COVER** | `SHMD` | 107 | $4.46 | $2.31 | $+31.72 | $15,459.43 | ▲ +31.72 after sell → book $8,323.52; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **SHORT** | `QTEX` | 305 | $1.71 | $4.01 | — | $15,975.45 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+123.8; leftover $520.22 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **SHORT** | `PTC` | 2 | $193.00 | $2.03 | — | $16,359.42 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+37.6; leftover $520.22 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **SHORT** | `XP` | 17 | $29.20 | $2.07 | — | $16,853.75 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+39.1; leftover $520.22 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **SHORT** | `PCVX` | 7 | $73.35 | $2.05 | — | $17,365.15 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+26.7; leftover $520.22 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟢 |
| 2026-10-06 09:30 ET | **SHORT** | `RXO` | 18 | $28.73 | $2.08 | — | $17,880.21 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+42.2; leftover $520.22 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **SHORT** | `PAGS` | 47 | $10.96 | $2.17 | — | $18,393.17 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+23.0; leftover $520.22 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **SHORT** | `DNA` | 34 | $15.08 | $2.13 | — | $18,903.76 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+43.2; leftover $520.22 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **SHORT** | `STNE` | 44 | $11.67 | $2.16 | — | $19,415.09 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+24.5; leftover $520.22 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,415.09 | ▲ close $8,759.33 vs 09:30 $8,347.45 (session +454.48) | 16:00 close · cash $19,415.09 · equity $8,759.33 vs 09:30 $8,347.45 (+411.88; session marks +454.48) · 19 name(s) marked open→close (per-name table). ACN×2 09:30 $195.43 → close $193.40 +4.06; NAUT×354 09:30 $1.87 → close $1.77 +35.40; NEOV×300 09:30 $2.97 → close $2.85 +36.00; NNBR×200 09:30 $4.24 → close $4.04 +40.00; SDEV×116 09:30 $3.48 → close $3.25 +26.68; SES×685 09:30 $0.84 → close $0.79 +32.40; SNPS×1 09:30 $491.10 → close $505.17 -14.07; SYNA×7 09:30 $120.65 → close $119.39 +8.82; TJGC×16 09:30 $33.00 → close $27.82 +82.88; VSH×24 09:30 $37.13 → close $37.84 -17.04; WOLF×18 09:30 $34.00 → close $31.83 +39.06; QTEX×305 09:30 $1.71 → close $1.59 +35.07; PTC×2 09:30 $193.00 → close $193.00 -0.00; XP×17 09:30 $29.20 → close $29.73 -9.01; PCVX×7 09:30 $73.35 → close $66.70 +46.55; RXO×18 09:30 $28.73 → close $28.54 +3.42; PAGS×47 09:30 $10.96 → close $10.72 +11.28; DNA×34 09:30 $15.08 → close $12.41 +90.78; STNE×44 09:30 $11.67 → close $11.62 +2.20 | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,415.09 | ▲ 09:30 equity $9,053.66 vs yday $8,759.33 (+294.33) | 09:30 open · cash $19,415.09 (unchanged overnight, no fees) · equity $9,053.66 vs prior close $8,759.33 (+294.33) · 19 name(s) re-marked at the open (per-name table). ACN×2 yday $193.40 → 09:30 $195.00 -3.20; DNA×34 yday $12.41 → 09:30 $11.80 +20.57; NAUT×354 yday $1.77 → 09:30 $1.67 +35.40; NEOV×300 yday $2.85 → 09:30 $2.73 +36.00; NNBR×200 yday $4.04 → 09:30 $3.95 +18.00; PAGS×47 yday $10.72 → 09:30 $10.64 +3.76; PCVX×7 yday $66.70 → 09:30 $65.86 +5.88; PTC×2 yday $193.00 → 09:30 $193.20 -0.40; QTEX×305 yday $1.59 → 09:30 $1.38 +65.58; RXO×18 yday $28.54 → 09:30 $28.34 +3.51; SDEV×116 yday $3.25 → 09:30 $3.27 -2.32; SES×685 yday $0.79 → 09:30 $0.76 +23.98; SNPS×1 yday $505.17 → 09:30 $505.35 -0.18; STNE×44 yday $11.62 → 09:30 $11.56 +2.64; SYNA×7 yday $119.39 → 09:30 $118.50 +6.23; TJGC×16 yday $27.82 → 09:30 $26.61 +19.36; VSH×24 yday $37.84 → 09:30 $36.54 +31.20; WOLF×18 yday $31.83 → 09:30 $30.53 +23.40; XP×17 yday $29.73 → 09:30 $29.44 +4.93 | — |
| 2026-10-07 09:30 ET | **COVER** | `ACN` | 2 | $195.00 | $2.00 | $+28.02 | $19,023.09 | ▲ +28.02 after sell → book $9,051.67; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-10-07 09:30 ET | **COVER** | `NAUT` | 354 | $1.67 | $4.57 | $-10.99 | $18,427.35 | ▼ -10.99 after sell → book $9,047.10; vs 09:30 mark -4.57 | dropped from list after 3 sess (min 3) | — |
| 2026-10-07 09:30 ET | **COVER** | `SDEV` | 116 | $3.27 | $2.34 | $+202.92 | $18,045.69 | ▲ +202.92 after sell → book $9,044.76; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-10-07 09:30 ET | **COVER** | `SES` | 685 | $0.76 | $7.23 | $+56.54 | $17,521.22 | ▲ +56.54 after sell → book $9,037.53; vs 09:30 mark -7.23 | dropped from list after 3 sess (min 3) | — |
| 2026-10-07 09:30 ET | **COVER** | `SNPS` | 1 | $505.35 | $1.99 | $-11.51 | $17,013.88 | ▼ -11.51 after sell → book $9,035.54; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-10-07 09:30 ET | **COVER** | `TJGC` | 16 | $26.61 | $2.04 | $+128.85 | $16,586.08 | ▲ +128.85 after sell → book $9,033.50; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-07 09:30 ET | **COVER** | `WOLF` | 18 | $30.53 | $2.04 | $+28.64 | $16,034.49 | ▲ +28.64 after sell → book $9,031.46; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,034.49 | ▼ close $8,907.29 vs 09:30 $9,053.66 (session -124.16) | 16:00 close · cash $16,034.49 · equity $8,907.29 vs 09:30 $9,053.66 (-146.37; session marks -124.16) · 12 name(s) marked open→close (per-name table). DNA×34 09:30 $11.80 → close $11.47 +11.39; NEOV×300 09:30 $2.73 → close $2.79 -18.00; NNBR×200 09:30 $3.95 → close $4.02 -14.00; PAGS×47 09:30 $10.64 → close $10.51 +6.11; PCVX×7 09:30 $65.86 → close $66.96 -7.70; PTC×2 09:30 $193.20 → close $193.60 -0.80; QTEX×305 09:30 $1.38 → close $1.66 -86.92; RXO×18 09:30 $28.34 → close $28.36 -0.27; STNE×44 09:30 $11.56 → close $11.51 +2.20; SYNA×7 09:30 $118.50 → close $119.46 -6.72; VSH×24 09:30 $36.54 → close $36.87 -7.92; XP×17 09:30 $29.44 → close $29.53 -1.53 | — |
| 2026-10-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,034.49 | ▲ 09:30 equity $8,907.29 vs yday $8,907.29 (+0.00) | 09:30 open · cash $16,034.49 (unchanged overnight, no fees) · equity $8,907.29 vs prior close $8,907.29 (+0.00) · 12 name(s) re-marked at the open (per-name table). DNA×34 yday $11.47 → 09:30 $11.47 -0.00; NEOV×300 yday $2.79 → 09:30 $2.79 -0.00; NNBR×200 yday $4.02 → 09:30 $4.02 -0.00; PAGS×47 yday $10.51 → 09:30 $10.51 -0.00; PCVX×7 yday $66.96 → 09:30 $66.96 -0.00; PTC×2 yday $193.60 → 09:30 $193.60 -0.00; QTEX×305 yday $1.66 → 09:30 $1.66 -0.00; RXO×18 yday $28.36 → 09:30 $28.36 -0.00; STNE×44 yday $11.51 → 09:30 $11.51 -0.00; SYNA×7 yday $119.46 → 09:30 $119.46 -0.00; VSH×24 yday $36.87 → 09:30 $36.87 -0.00; XP×17 yday $29.53 → 09:30 $29.53 -0.00 | — |
| 2026-10-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,034.49 | ▲ close $8,907.29 vs 09:30 $8,907.29 (session +0.00) | 16:00 close · cash $16,034.49 · equity $8,907.29 vs 09:30 $8,907.29 (+0.00; session marks +0.00) · 12 name(s) marked open→close (per-name table). DNA×34 09:30 $11.47 → close $11.47 -0.00; NEOV×300 09:30 $2.79 → close $2.79 -0.00; NNBR×200 09:30 $4.02 → close $4.02 -0.00; PAGS×47 09:30 $10.51 → close $10.51 -0.00; PCVX×7 09:30 $66.96 → close $66.96 -0.00; PTC×2 09:30 $193.60 → close $193.60 -0.00; QTEX×305 09:30 $1.66 → close $1.66 -0.00; RXO×18 09:30 $28.36 → close $28.36 -0.00; STNE×44 09:30 $11.51 → close $11.51 -0.00; SYNA×7 09:30 $119.46 → close $119.46 -0.00; VSH×24 09:30 $36.87 → close $36.87 -0.00; XP×17 09:30 $29.53 → close $29.53 -0.00 | — |
| 2026-10-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,034.49 | ▲ 09:30 equity $8,929.71 vs yday $8,907.29 (+22.42) | 09:30 open · cash $16,034.49 (unchanged overnight, no fees) · equity $8,929.71 vs prior close $8,907.29 (+22.42) · 12 name(s) re-marked at the open (per-name table). DNA×34 yday $11.47 → 09:30 $11.76 -9.86; NEOV×300 yday $2.79 → 09:30 $2.65 +42.00; NNBR×200 yday $4.02 → 09:30 $4.08 -12.00; PAGS×47 yday $10.51 → 09:30 $10.81 -14.10; PCVX×7 yday $66.96 → 09:30 $64.78 +15.26; PTC×2 yday $193.60 → 09:30 $193.87 -0.54; QTEX×305 yday $1.66 → 09:30 $1.63 +9.15; RXO×18 yday $28.36 → 09:30 $28.97 -10.98; STNE×44 yday $11.51 → 09:30 $11.63 -5.28; SYNA×7 yday $119.46 → 09:30 $119.87 -2.87; VSH×24 yday $36.87 → 09:30 $35.28 +38.16; XP×17 yday $29.53 → 09:30 $31.09 -26.52 | — |
| 2026-10-09 09:30 ET | **COVER** | `DNA` | 34 | $11.76 | $2.09 | $+108.66 | $15,632.56 | ▲ +108.66 after sell → book $8,927.62; vs 09:30 mark -2.09 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **COVER** | `NEOV` | 300 | $2.65 | $3.87 | $+118.17 | $14,833.69 | ▲ +118.17 after sell → book $8,923.75; vs 09:30 mark -3.87 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **COVER** | `NNBR` | 200 | $4.08 | $2.59 | $+96.75 | $14,015.10 | ▲ +96.75 after sell → book $8,921.16; vs 09:30 mark -2.59 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **COVER** | `PAGS` | 47 | $10.81 | $2.13 | $+2.75 | $13,504.90 | ▲ +2.75 after sell → book $8,919.03; vs 09:30 mark -2.13 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **COVER** | `PCVX` | 7 | $64.78 | $2.01 | $+55.93 | $13,049.43 | ▲ +55.93 after sell → book $8,917.02; vs 09:30 mark -2.01 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **COVER** | `PTC` | 2 | $193.87 | $2.00 | $-5.76 | $12,659.69 | ▼ -5.76 after sell → book $8,915.02; vs 09:30 mark -2.00 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **COVER** | `QTEX` | 305 | $1.63 | $3.93 | $+14.93 | $12,158.61 | ▲ +14.93 after sell → book $8,911.09; vs 09:30 mark -3.93 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **COVER** | `RXO` | 18 | $28.97 | $2.04 | $-8.44 | $11,635.10 | ▼ -8.44 after sell → book $8,909.04; vs 09:30 mark -2.05 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **COVER** | `STNE` | 44 | $11.63 | $2.12 | $-2.52 | $11,121.26 | ▼ -2.52 after sell → book $8,906.92; vs 09:30 mark -2.12 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **COVER** | `SYNA` | 7 | $119.87 | $2.01 | $+3.70 | $10,280.16 | ▲ +3.70 after sell → book $8,904.91; vs 09:30 mark -2.01 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **COVER** | `VSH` | 24 | $35.28 | $2.06 | $+52.47 | $9,431.38 | ▲ +52.47 after sell → book $8,902.85; vs 09:30 mark -2.06 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **COVER** | `XP` | 17 | $31.09 | $2.04 | $-36.25 | $8,900.81 | ▼ -36.25 after sell → book $8,900.81; vs 09:30 mark -2.04 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **SHORT** | `PCRX` | 15 | $36.33 | $2.07 | — | $9,443.69 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+45.5; leftover $556.30 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **SHORT** | `BYRN` | 118 | $4.70 | $2.39 | — | $9,995.90 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+35.9; leftover $556.30 | join🔴 sector🟡 gen🔴 news🟡 digest🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **SHORT** | `ADCT` | 352 | $1.58 | $4.62 | — | $10,547.43 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+47.2; leftover $556.30 | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **SHORT** | `RC` | 427 | $1.30 | $5.60 | — | $11,096.93 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; ret5=+21.1; leftover $556.30 | join🔴 sector🔴 gen🔴 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **SHORT** | `NAUT` | 291 | $1.91 | $3.83 | — | $11,649.20 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+15.9; leftover $556.30 | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **SHORT** | `SUJA` | 52 | $10.54 | $2.18 | — | $12,195.10 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.4; leftover $556.30 | join🟢 sector🟢 gen🔴 news🟡 digest🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-09 09:30 ET | **SHORT** | `FNKO` | 85 | $6.50 | $2.28 | — | $12,745.32 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.1; leftover $556.30 | join🟢 sector🔴 gen🔴 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-10-09 09:30 ET | **SHORT** | `JBS` | 42 | $12.94 | $2.15 | — | $13,286.65 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.4; leftover $556.30 | join🟢 sector🟢 gen🔴 news🟡 digest🔴 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,286.65 | ▼ close $8,732.55 vs 09:30 $8,929.71 (session -143.13) | 16:00 close · cash $13,286.65 · equity $8,732.55 vs 09:30 $8,929.71 (-197.16; session marks -143.13) · 8 name(s) marked open→close (per-name table). PCRX×15 09:30 $36.33 → close $36.31 +0.30; BYRN×118 09:30 $4.70 → close $4.52 +21.24; ADCT×352 09:30 $1.58 → close $1.73 -52.80; RC×427 09:30 $1.30 → close $1.30 -0.00; NAUT×291 09:30 $1.91 → close $2.25 -98.65; SUJA×52 09:30 $10.54 → close $10.86 -16.64; FNKO×85 09:30 $6.50 → close $6.44 +5.10; JBS×42 09:30 $12.94 → close $12.98 -1.68 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MXCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `QMLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TBBB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AMPY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OMER` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MXCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `QMLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TBBB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AMPY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HTFL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `UMAC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `LPTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NMAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ALOY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HIVE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SNDK` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HTFL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `UMAC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `LPTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ALOY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AZI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BNTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BTGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `PPC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AZI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BNTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `PPC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `USDE` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SLS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `COIN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FWDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FWDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `FIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `MNRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `BRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MNRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FUTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `AQST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `VYX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LVWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OPTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CNXC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NABL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RZLV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `OPTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PURR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SKYX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RXST` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RZLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CNXC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SION` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CNH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `GPRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CNXC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CNH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `TARS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SLBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `IRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `FMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `LENZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `USDE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `SLBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `IRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `FMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LENZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ORBS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `PAYP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HYLN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LIFE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HELP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `COHU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `WLTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `APPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `VERI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `XHLD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BAND` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HUT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SMR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `COHU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CMRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `WLTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `APPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RPD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `DBI` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SION` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INSP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `FTRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SDGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `REF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CRWD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `HLP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `FTRE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `REF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CRWD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `EMAT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `IQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `EMAT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `IQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `RBRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `RVTY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BBNX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `EMAT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `IQ` | no_price | no 09:30 open — carry |
| 2026-09-22 | `RBRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `ECO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `TJGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GEMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FWDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-23 | `TJGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GEMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `FWDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SECZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GLND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `CRML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `NUAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `VGZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `ARM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CRML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `NUAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `VGZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `ARM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `VKTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BFLY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `EVTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INOD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SVIA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `THM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SWRD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `TJGC` | 50 | 2026-09-21 @ $16.91 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+50.5; leftover $847.85 |
| `SECZ` | 72 | 2026-09-21 @ $11.67 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+31.3; leftover $847.85 |
| `GLND` | 244 | 2026-09-22 @ $2.94 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+136.1; leftover $718.03 |
| `CRML` | 78 | 2026-09-22 @ $9.11 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+44.4; leftover $718.03 |
| `NUAI` | 99 | 2026-09-22 @ $7.23 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+36.6; leftover $718.03 |
| `VGZ` | 270 | 2026-09-22 @ $2.65 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+26.3; leftover $718.03 |
| `ARM` | 2 | 2026-09-22 @ $319.41 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+35.1; leftover $718.03 |
| `VKTX` | 16 | 2026-09-23 @ $41.76 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $693.67 |
| `BFLY` | 70 | 2026-09-23 @ $9.90 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $693.67 |
| `EVTL` | 945 | 2026-09-23 @ $0.73 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+27.5; leftover $693.67 |
| `INOD` | 9 | 2026-09-23 @ $70.84 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $693.67 |
| `SVIA` | 154 | 2026-09-23 @ $4.49 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+26.4; leftover $693.67 |
| `THM` | 242 | 2026-09-23 @ $2.86 | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+25.5; leftover $693.67 |
