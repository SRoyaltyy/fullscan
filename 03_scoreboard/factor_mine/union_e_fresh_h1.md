# Factor mine action — `union_e_fresh_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ e_fresh, no 🚨

Cash book **-4.84%** ($9,516) · signal-only (no cash/fees) was +4.74%. Starts YES **24/30**. Fills 177 · skips 56 · realized $+1231.21.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: earnings (E) printed within the last 1 session(s).
- Must-have: the earnings flag is on.
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
- **Gate** `days_since_E_max=1,flag_E_min=0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,231.21.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 6172 | $0.81 | $68.51 | — | $4,932.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; leftover $5000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 223 | $22.01 | $2.88 | — | $21.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.06 | ▲ close $10,769.53 vs 09:30 $10,000.00 (session +840.92) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.06 | ▲ 09:30 equity $10,963.61 vs yday $10,769.53 (+194.08) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 6172 | $0.93 | $76.99 | $+595.14 | $5,684.04 | ▲ +595.14 after sell → book $10,886.63; vs 09:30 mark -76.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 223 | $23.33 | $2.96 | $+288.53 | $10,883.67 | ▲ +288.53 after sell → book $10,883.67; vs 09:30 mark -2.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 906 | $1.50 | $11.69 | — | $9,512.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1360.46 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 69 | $19.57 | $2.20 | — | $8,160.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1360.46 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 122 | $11.12 | $2.36 | — | $6,801.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1360.46 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 100 | $13.55 | $2.29 | — | $5,444.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1360.46 | — |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 125 | $10.83 | $2.37 | — | $4,088.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-30.1; leftover $1360.46 | — |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 1152 | $1.18 | $14.86 | — | $2,713.84 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1360.46 | — |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 70 | $19.17 | $2.20 | — | $1,369.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1360.46 | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 137 | $9.89 | $2.40 | — | $11.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; leftover $1360.46 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.72 | ▲ close $10,869.01 vs 09:30 $10,963.61 (session +25.69) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.72 | ▲ 09:30 equity $10,935.77 vs yday $10,869.01 (+66.76) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 906 | $1.52 | $11.85 | $-5.42 | $1,376.99 | ▼ -5.42 after sell → book $10,923.92; vs 09:30 mark -11.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 69 | $19.57 | $2.22 | $-4.42 | $2,725.10 | ▼ -4.42 after sell → book $10,921.70; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 122 | $9.57 | $2.39 | $-193.84 | $3,890.26 | ▼ -193.84 after sell → book $10,919.32; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 100 | $13.16 | $2.32 | $-43.61 | $5,203.94 | ▼ -43.61 after sell → book $10,917.00; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 125 | $11.19 | $2.40 | $+40.24 | $6,600.29 | ▲ +40.24 after sell → book $10,914.60; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `EU` | 1152 | $1.21 | $15.06 | $+4.64 | $7,979.15 | ▲ +4.64 after sell → book $10,899.54; vs 09:30 mark -15.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 70 | $20.25 | $2.22 | $+71.18 | $9,394.43 | ▲ +71.18 after sell → book $10,897.32; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 137 | $10.97 | $2.44 | $+142.44 | $10,894.88 | ▲ +142.44 after sell → book $10,894.88; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,894.88 | ▲ close $10,894.88 vs 09:30 $10,935.77 (session +0.00) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,894.88 | ▲ 09:30 equity $10,894.88 vs yday $10,894.88 (+0.00) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,894.88 | ▲ close $10,894.88 vs 09:30 $10,894.88 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,894.88 | ▲ 09:30 equity $10,894.88 vs yday $10,894.88 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,894.88 | ▲ close $10,894.88 vs 09:30 $10,894.88 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,894.88 | ▲ 09:30 equity $10,894.88 vs yday $10,894.88 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 13 | $97.43 | $2.03 | — | $9,626.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; leftover $1361.86 | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 307 | $4.43 | $3.96 | — | $8,262.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; leftover $1361.86 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 4539 | $0.30 | $27.23 | — | $6,873.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; leftover $1361.86 | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 29 | $46.85 | $2.08 | — | $5,512.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; leftover $1361.86 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 151 | $9.01 | $2.44 | — | $4,149.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $1361.86 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 350 | $3.89 | $4.51 | — | $2,783.66 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; leftover $1361.86 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 39 | $34.05 | $2.11 | — | $1,453.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; leftover $1361.86 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 60 | $22.44 | $2.17 | — | $105.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; leftover $1361.86 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $105.03 | ▲ close $10,940.84 vs 09:30 $10,894.88 (session +92.49) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $105.03 | ▲ 09:30 equity $10,983.65 vs yday $10,940.84 (+42.81) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 13 | $96.75 | $2.05 | $-12.92 | $1,360.74 | ▼ -12.92 after sell → book $10,981.61; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TOYO` | 307 | $4.68 | $4.02 | $+68.77 | $2,793.47 | ▲ +68.77 after sell → book $10,977.58; vs 09:30 mark -4.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DVLT` | 4539 | $0.31 | $28.45 | $-10.30 | $4,172.11 | ▼ -10.30 after sell → book $10,949.13; vs 09:30 mark -28.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AEG` | 151 | $9.04 | $2.48 | $-0.39 | $5,534.67 | ▼ -0.39 after sell → book $10,946.65; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALVO` | 350 | $4.32 | $4.59 | $+141.40 | $7,042.09 | ▲ +141.40 after sell → book $10,942.07; vs 09:30 mark -4.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 39 | $34.31 | $2.13 | $+5.91 | $8,378.05 | ▲ +5.91 after sell → book $10,939.94; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 60 | $22.20 | $2.19 | $-18.76 | $9,707.86 | ▼ -18.76 after sell → book $10,937.75; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 12 | $115.18 | $2.03 | — | $8,323.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1386.84 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $7,075.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1386.84 | — |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 13 | $103.69 | $2.03 | — | $5,725.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; ret5=-10.3; leftover $1386.84 | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 77 | $17.93 | $2.22 | — | $4,341.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $1386.84 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 14 | $93.98 | $2.03 | — | $3,024.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; leftover $1386.84 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 32 | $43.08 | $2.09 | — | $1,643.54 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; leftover $1386.84 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 602 | $2.30 | $7.77 | — | $251.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; leftover $1386.84 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $251.18 | ▲ close $11,133.93 vs 09:30 $10,983.65 (session +216.34) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $251.18 | ▲ 09:30 equity $11,183.76 vs yday $11,133.93 (+49.83) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 29 | $43.05 | $2.10 | $-114.37 | $1,497.53 | ▼ -114.37 after sell → book $11,181.66; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 12 | $121.00 | $2.05 | $+65.77 | $2,947.48 | ▲ +65.77 after sell → book $11,179.62; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $4,251.55 | ▲ +55.55 after sell → book $11,177.60; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WMT` | 13 | $104.14 | $2.05 | $+1.77 | $5,603.32 | ▲ +1.77 after sell → book $11,175.55; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 77 | $18.05 | $2.24 | $+4.77 | $6,991.31 | ▲ +4.77 after sell → book $11,173.31; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 14 | $97.02 | $2.05 | $+38.48 | $8,347.53 | ▲ +38.48 after sell → book $11,171.25; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 32 | $44.22 | $2.11 | $+32.29 | $9,760.47 | ▲ +32.29 after sell → book $11,169.15; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 602 | $2.34 | $7.88 | $+8.44 | $11,161.27 | ▲ +8.44 after sell → book $11,161.27; vs 09:30 mark -7.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,161.27 | ▲ close $11,161.27 vs 09:30 $11,183.76 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,161.27 | ▲ 09:30 equity $11,161.27 vs yday $11,161.27 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 7 | $175.01 | $2.01 | — | $9,934.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; leftover $1395.16 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 15 | $88.94 | $2.04 | — | $8,598.05 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; leftover $1395.16 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 91 | $15.28 | $2.26 | — | $7,205.31 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; leftover $1395.16 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 9 | $142.36 | $2.02 | — | $5,922.05 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; leftover $1395.16 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 273 | $5.10 | $3.52 | — | $4,526.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; leftover $1395.16 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 29 | $47.89 | $2.08 | — | $3,135.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; leftover $1395.16 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 100 | $13.92 | $2.29 | — | $1,741.05 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; leftover $1395.16 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 306 | $4.54 | $3.95 | — | $346.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; leftover $1395.16 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $346.34 | ▼ close $10,744.58 vs 09:30 $11,161.27 (session -396.53) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $346.34 | ▼ 09:30 equity $10,709.98 vs yday $10,744.58 (-34.60) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `BMO` | 7 | $173.22 | $2.03 | $-16.57 | $1,556.85 | ▼ -16.57 after sell → book $10,707.95; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BNS` | 15 | $92.65 | $2.06 | $+51.56 | $2,944.54 | ▲ +51.56 after sell → book $10,705.89; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EH` | 273 | $4.77 | $3.58 | $-97.19 | $4,243.17 | ▼ -97.19 after sell → book $10,702.31; vs 09:30 mark -3.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GFI` | 29 | $48.24 | $2.10 | $+5.97 | $5,640.03 | ▲ +5.97 after sell → book $10,700.21; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 100 | $14.03 | $2.32 | $+6.39 | $7,040.72 | ▲ +6.39 after sell → book $10,697.90; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SHMD` | 306 | $3.38 | $4.01 | $-364.45 | $8,070.99 | ▼ -364.45 after sell → book $10,693.89; vs 09:30 mark -4.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 2307 | $0.58 | $20.37 | — | $6,705.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; leftover $1345.16 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 258 | $5.21 | $3.33 | — | $5,358.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $1345.16 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 10 | $131.37 | $2.02 | — | $4,042.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; leftover $1345.16 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 73 | $18.26 | $2.21 | — | $2,707.22 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; leftover $1345.16 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 39 | $34.30 | $2.11 | — | $1,367.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; leftover $1345.16 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 4 | $326.91 | $2.00 | — | $57.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-15.2; leftover $1345.16 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.77 | ▲ close $11,019.73 vs 09:30 $10,709.98 (session +357.88) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.77 | ▼ 09:30 equity $10,927.75 vs yday $11,019.73 (-91.98) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 91 | $18.50 | $2.29 | $+288.47 | $1,738.98 | ▲ +288.47 after sell → book $10,925.46; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 9 | $128.73 | $2.04 | $-126.72 | $2,895.51 | ▼ -126.72 after sell → book $10,923.42; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 2307 | $0.53 | $19.54 | $-162.18 | $4,098.68 | ▼ -162.18 after sell → book $10,903.88; vs 09:30 mark -19.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 258 | $5.49 | $3.38 | $+65.53 | $5,511.72 | ▲ +65.53 after sell → book $10,900.50; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ANF` | 10 | $144.70 | $2.04 | $+129.24 | $6,956.68 | ▲ +129.24 after sell → book $10,898.46; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BBWI` | 73 | $18.69 | $2.23 | $+26.95 | $8,318.81 | ▲ +26.95 after sell → book $10,896.22; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BOX` | 39 | $33.79 | $2.13 | $-24.12 | $9,634.50 | ▼ -24.12 after sell → book $10,894.10; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DY` | 4 | $314.90 | $2.02 | $-52.06 | $10,892.07 | ▼ -52.06 after sell → book $10,892.07; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 16 | $80.60 | $2.04 | — | $9,600.44 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.0; leftover $1361.51 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 84 | $16.18 | $2.24 | — | $8,239.07 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.7; leftover $1361.51 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 11 | $118.77 | $2.02 | — | $6,930.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.3; leftover $1361.51 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 76 | $17.78 | $2.22 | — | $5,577.08 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.2; leftover $1361.51 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 101 | $13.41 | $2.29 | — | $4,220.38 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; leftover $1361.51 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 14 | $97.16 | $2.03 | — | $2,858.11 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; leftover $1361.51 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 6 | $206.82 | $2.01 | — | $1,615.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.2; leftover $1361.51 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 11 | $120.17 | $2.02 | — | $291.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; leftover $1361.51 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $291.29 | ▲ close $10,967.70 vs 09:30 $10,927.75 (session +92.50) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $291.29 | ▲ 09:30 equity $11,005.29 vs yday $10,967.70 (+37.59) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BBY` | 16 | $83.85 | $2.06 | $+47.90 | $1,630.83 | ▲ +47.90 after sell → book $11,003.23; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BILI` | 84 | $16.94 | $2.27 | $+59.33 | $3,051.52 | ▲ +59.33 after sell → book $11,000.96; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 11 | $115.66 | $2.04 | $-38.28 | $4,321.74 | ▼ -38.28 after sell → book $10,998.92; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CMBT` | 76 | $18.58 | $2.24 | $+56.34 | $5,731.58 | ▲ +56.34 after sell → book $10,996.68; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CSIQ` | 101 | $13.65 | $2.32 | $+19.63 | $7,107.91 | ▲ +19.63 after sell → book $10,994.36; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `HQY` | 14 | $93.62 | $2.05 | $-53.64 | $8,416.53 | ▼ -53.64 after sell → book $10,992.30; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RY` | 6 | $205.50 | $2.03 | $-11.96 | $9,647.51 | ▼ -11.96 after sell → book $10,990.28; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TD` | 11 | $122.07 | $2.04 | $+16.83 | $10,988.23 | ▲ +16.83 after sell → book $10,988.23; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $9,680.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; leftover $1373.53 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 91 | $15.01 | $2.26 | — | $8,312.25 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; leftover $1373.53 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 13 | $103.89 | $2.03 | — | $6,959.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; leftover $1373.53 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 354 | $3.88 | $4.57 | — | $5,581.57 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; leftover $1373.53 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 30 | $44.40 | $2.08 | — | $4,247.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; leftover $1373.53 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 55 | $24.69 | $2.15 | — | $2,887.38 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; leftover $1373.53 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 164 | $8.35 | $2.48 | — | $1,515.50 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; leftover $1373.53 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 36 | $37.65 | $2.10 | — | $158.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; leftover $1373.53 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.18 | ▼ close $10,563.06 vs 09:30 $11,005.29 (session -405.49) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.18 | ▲ 09:30 equity $10,572.35 vs yday $10,563.06 (+9.29) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 5 | $257.71 | $2.03 | $-21.28 | $1,444.71 | ▼ -21.28 after sell → book $10,570.33; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBAR` | 91 | $14.88 | $2.29 | $-16.38 | $2,796.50 | ▼ -16.38 after sell → book $10,568.04; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 13 | $98.00 | $2.05 | $-80.65 | $4,068.45 | ▼ -80.65 after sell → book $10,565.99; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 354 | $3.39 | $4.64 | $-182.66 | $5,263.87 | ▼ -182.66 after sell → book $10,561.35; vs 09:30 mark -4.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 30 | $44.85 | $2.10 | $+9.32 | $6,607.27 | ▲ +9.32 after sell → book $10,559.25; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 55 | $22.98 | $2.18 | $-98.38 | $7,869.00 | ▼ -98.38 after sell → book $10,557.08; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 164 | $8.53 | $2.52 | $+24.52 | $9,265.40 | ▲ +24.52 after sell → book $10,554.56; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `IREN` | 36 | $35.81 | $2.12 | $-70.28 | $10,552.44 | ▼ -70.28 after sell → book $10,552.44; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,552.44 | ▲ close $10,552.44 vs 09:30 $10,572.35 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,552.44 | ▲ 09:30 equity $10,552.44 vs yday $10,552.44 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,552.44 | ▲ close $10,552.44 vs 09:30 $10,552.44 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,552.44 | ▲ 09:30 equity $10,552.44 vs yday $10,552.44 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,552.44 | ▲ close $10,552.44 vs 09:30 $10,552.44 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,552.44 | ▲ 09:30 equity $10,552.44 vs yday $10,552.44 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 122 | $10.74 | $2.36 | — | $9,239.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; leftover $1319.05 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,181.97 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; leftover $1319.05 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 191 | $6.90 | $2.56 | — | $6,861.51 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; leftover $1319.05 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 3 | $354.49 | $2.00 | — | $5,796.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; leftover $1319.05 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 59 | $22.32 | $2.17 | — | $4,477.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; leftover $1319.05 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 5 | $257.00 | $2.00 | — | $3,189.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; leftover $1319.05 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 27 | $47.60 | $2.07 | — | $1,902.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; leftover $1319.05 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 87 | $15.09 | $2.25 | — | $587.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; leftover $1319.05 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $587.64 | ▲ close $10,983.77 vs 09:30 $10,552.44 (session +448.74) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $587.64 | ▲ 09:30 equity $11,022.08 vs yday $10,983.77 (+38.31) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 122 | $10.91 | $2.39 | $+15.39 | $1,916.27 | ▲ +15.39 after sell → book $11,019.69; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $2,993.35 | ▲ +19.86 after sell → book $11,017.67; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 191 | $9.28 | $2.61 | $+449.41 | $4,763.22 | ▲ +449.41 after sell → book $11,015.06; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 3 | $321.67 | $2.02 | $-102.48 | $5,726.21 | ▼ -102.48 after sell → book $11,013.04; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CPB` | 59 | $22.10 | $2.19 | $-17.33 | $7,027.93 | ▼ -17.33 after sell → book $11,010.86; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 5 | $238.88 | $2.02 | $-94.63 | $8,220.30 | ▼ -94.63 after sell → book $11,008.83; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 27 | $53.85 | $2.09 | $+164.59 | $9,672.16 | ▲ +164.59 after sell → book $11,006.74; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 87 | $15.34 | $2.28 | $+17.22 | $11,004.46 | ▲ +17.22 after sell → book $11,004.46; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 21 | $63.18 | $2.05 | — | $9,675.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; leftover $1375.56 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 157 | $8.74 | $2.46 | — | $8,300.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; leftover $1375.56 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 20 | $68.52 | $2.05 | — | $6,928.54 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; leftover $1375.56 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 380 | $3.62 | $4.90 | — | $5,549.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; leftover $1375.56 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 8 | $167.55 | $2.01 | — | $4,207.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; leftover $1375.56 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 30 | $44.90 | $2.08 | — | $2,858.44 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; leftover $1375.56 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 14 | $98.15 | $2.03 | — | $1,482.31 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; leftover $1375.56 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 87 | $15.70 | $2.25 | — | $114.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; leftover $1375.56 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $114.16 | ▼ close $10,893.44 vs 09:30 $11,022.08 (session -91.18) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $114.16 | ▼ 09:30 equity $10,826.88 vs yday $10,893.44 (-66.56) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `AMBA` | 21 | $63.83 | $2.07 | $+9.52 | $1,452.52 | ▲ +9.52 after sell → book $10,824.81; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASAN` | 157 | $8.73 | $2.50 | $-6.53 | $2,820.63 | ▼ -6.53 after sell → book $10,822.31; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 20 | $67.05 | $2.07 | $-33.52 | $4,159.56 | ▼ -33.52 after sell → book $10,820.24; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOMO` | 380 | $3.84 | $4.98 | $+75.62 | $5,613.78 | ▲ +75.62 after sell → book $10,815.26; vs 09:30 mark -4.98 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 8 | $160.52 | $2.03 | $-60.29 | $6,895.91 | ▼ -60.29 after sell → book $10,813.23; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 30 | $39.56 | $2.10 | $-164.38 | $8,080.61 | ▼ -164.38 after sell → book $10,811.13; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 14 | $100.58 | $2.05 | $+29.93 | $9,486.67 | ▲ +29.93 after sell → book $10,809.07; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MAMA` | 87 | $15.20 | $2.28 | $-48.03 | $10,806.80 | ▼ -48.03 after sell → book $10,806.80; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.80 | ▲ close $10,806.80 vs 09:30 $10,826.88 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.80 | ▲ 09:30 equity $10,806.80 vs yday $10,806.80 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.80 | ▲ close $10,806.80 vs 09:30 $10,806.80 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.80 | ▲ 09:30 equity $10,806.80 vs yday $10,806.80 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.80 | ▲ close $10,806.80 vs 09:30 $10,806.80 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.80 | ▲ 09:30 equity $10,806.80 vs yday $10,806.80 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 8 | $164.43 | $2.01 | — | $9,489.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1350.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 228 | $5.91 | $2.94 | — | $8,138.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; leftover $1350.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 5 | $242.17 | $2.00 | — | $6,926.07 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; leftover $1350.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 42 | $32.01 | $2.12 | — | $5,579.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; leftover $1350.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 18 | $71.71 | $2.04 | — | $4,286.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; leftover $1350.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 24 | $56.02 | $2.06 | — | $2,940.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; leftover $1350.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 144 | $9.37 | $2.42 | — | $1,588.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; leftover $1350.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 103 | $13.10 | $2.30 | — | $236.86 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; leftover $1350.85 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $236.86 | ▲ close $10,841.16 vs 09:30 $10,806.80 (session +52.27) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $236.86 | ▲ 09:30 equity $10,852.87 vs yday $10,841.16 (+11.71) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 8 | $141.42 | $2.03 | $-188.13 | $1,366.19 | ▼ -188.13 after sell → book $10,850.84; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DBI` | 228 | $5.86 | $2.99 | $-17.33 | $2,699.28 | ▼ -17.33 after sell → book $10,847.85; vs 09:30 mark -2.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 5 | $261.51 | $2.03 | $+92.67 | $4,004.81 | ▲ +92.67 after sell → book $10,845.83; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CPRT` | 42 | $30.63 | $2.14 | $-62.21 | $5,289.13 | ▼ -62.21 after sell → book $10,843.69; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DSGX` | 18 | $77.68 | $2.07 | $+103.35 | $6,685.30 | ▲ +103.35 after sell → book $10,841.62; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `KR` | 24 | $59.31 | $2.08 | $+74.81 | $8,106.66 | ▲ +74.81 after sell → book $10,839.54; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `LPTH` | 144 | $8.85 | $2.46 | $-79.76 | $9,378.60 | ▼ -79.76 after sell → book $10,837.08; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `REF` | 103 | $14.16 | $2.33 | $+104.55 | $10,834.76 | ▲ +104.55 after sell → book $10,834.76; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,834.76 | ▲ close $10,834.76 vs 09:30 $10,852.87 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,834.76 | ▲ 09:30 equity $10,834.76 vs yday $10,834.76 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,834.76 | ▲ close $10,834.76 vs 09:30 $10,834.76 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,834.76 | ▲ 09:30 equity $10,834.76 vs yday $10,834.76 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 163 | $33.14 | $2.48 | — | $5,430.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; leftover $5417.38 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 132 | $40.93 | $2.39 | — | $25.31 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; leftover $5417.38 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.31 | ▲ close $11,040.99 vs 09:30 $10,834.76 (session +211.10) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.31 | ▲ 09:30 equity $11,401.47 vs yday $11,040.99 (+360.48) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `FPS` | 163 | $36.76 | $2.55 | $+585.03 | $6,014.64 | ▲ +585.03 after sell → book $11,398.92; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TCOM` | 132 | $40.79 | $2.45 | $-23.32 | $11,396.47 | ▼ -23.32 after sell → book $11,396.47; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 508 | $11.21 | $6.55 | — | $5,695.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.0; leftover $5698.23 | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 70 | $81.00 | $2.20 | — | $23.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.0; leftover $5698.23 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.03 | ▲ close $11,466.89 vs 09:30 $11,401.47 (session +79.18) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.03 | ▼ 09:30 equity $11,413.65 vs yday $11,466.89 (-53.24) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ALMU` | 508 | $11.64 | $6.68 | $+205.20 | $5,929.47 | ▲ +205.20 after sell → book $11,406.97; vs 09:30 mark -6.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LEN` | 70 | $78.25 | $2.26 | $-196.96 | $11,404.71 | ▼ -196.96 after sell → book $11,404.71; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,404.71 | ▲ close $11,404.71 vs 09:30 $11,413.65 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,404.71 | ▲ 09:30 equity $11,404.71 vs yday $11,404.71 (+0.00) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,404.71 | ▲ close $11,404.71 vs 09:30 $11,404.71 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,404.71 | ▲ 09:30 equity $11,404.71 vs yday $11,404.71 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,404.71 | ▲ close $11,404.71 vs 09:30 $11,404.71 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,404.71 | ▲ 09:30 equity $11,404.71 vs yday $11,404.71 (+0.00) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 47 | $47.57 | $2.13 | — | $9,166.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; leftover $2280.94 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 11 | $196.78 | $2.02 | — | $7,000.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $2280.94 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 63 | $35.74 | $2.18 | — | $4,746.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; leftover $2280.94 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 48 | $47.15 | $2.13 | — | $2,481.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; leftover $2280.94 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 20 | $109.67 | $2.05 | — | $285.61 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; leftover $2280.94 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $285.61 | ▼ close $11,240.38 vs 09:30 $11,404.71 (session -153.82) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $285.61 | ▲ 09:30 equity $11,241.87 vs yday $11,240.38 (+1.49) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `CBRL` | 47 | $46.88 | $2.16 | $-36.72 | $2,486.81 | ▼ -36.72 after sell → book $11,239.71; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 11 | $192.26 | $2.05 | $-53.79 | $4,599.62 | ▼ -53.79 after sell → book $11,237.66; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `GIS` | 63 | $35.96 | $2.21 | $+9.47 | $6,862.89 | ▲ +9.47 after sell → book $11,235.45; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `KBH` | 48 | $47.14 | $2.16 | $-4.78 | $9,123.45 | ▼ -4.78 after sell → book $11,233.29; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PAYX` | 20 | $105.49 | $2.08 | $-87.69 | $11,231.21 | ▼ -87.69 after sell → book $11,231.21; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,231.21 | ▲ close $11,231.21 vs 09:30 $11,241.87 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,160.20 | ▲ 09:30 equity $9,160.20 vs yday $9,160.20 (+0.00) | 09:30 open · cash $9,160.20 · no holdings · equity $9,160.20 vs prior close $9,160.20 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 10 | $887.00 | $2.02 | — | $288.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+0.3; leftover $9160.20 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $288.18 | ▲ close $9,515.83 vs 09:30 $9,160.20 (session +357.65) | 16:00 close · cash $288.18 · equity $9,515.83 vs 09:30 $9,160.20 (+355.63; session marks +357.65) · 1 name(s) marked open→close (per-name table). COST×10 09:30 $887.00 → close $922.76 +357.65 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new buys |
