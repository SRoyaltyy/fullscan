# Factor mine action — `yday_gainer_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `yday_gainer` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-15.01%** ($8,499) · signal-only (no cash/fees) was +49.33%. Starts YES **2/30**. Fills 186 · skips 312 · realized $-869.27.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at yesterday's top liquid winners and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: yesterday's top liquid winners.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).

### When it buys

- At 09:30, take names on yesterday's top liquid winners that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
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

- **Universe** `yday_gainer` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,007.56.

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
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $8,746.36 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `WWW` | 60 | $20.60 | $2.17 | — | $7,508.19 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+4.4; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $6,254.51 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $5,019.42 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `OMER` | 72 | $17.35 | $2.21 | — | $3,768.02 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $2,520.25 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $1,266.11 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `MXCT` | 899 | $1.39 | $11.60 | — | $4.90 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ret5=+25.2; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.90 | ▼ close $9,804.72 vs 09:30 $10,000.00 (session -161.22) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.90 | ▲ 09:30 equity $9,850.47 vs yday $9,804.72 (+45.75) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.90 | ▼ close $9,771.77 vs 09:30 $9,850.47 (session -78.70) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.90 | ▼ 09:30 equity $9,666.38 vs yday $9,771.77 (-105.39) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.90 | ▼ close $9,551.67 vs 09:30 $9,666.38 (session -114.71) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.90 | ▲ 09:30 equity $9,589.58 vs yday $9,551.67 (+37.91) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 290 | $4.79 | $3.80 | $+131.66 | $1,390.20 | ▲ +131.66 after sell → book $9,585.78; vs 09:30 mark -3.80 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `WWW` | 60 | $20.08 | $2.19 | $-35.56 | $2,592.81 | ▼ -35.56 after sell → book $9,583.59; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 299 | $3.87 | $3.92 | $-100.46 | $3,746.02 | ▼ -100.46 after sell → book $9,579.67; vs 09:30 mark -3.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 63 | $19.58 | $2.20 | $-3.75 | $4,977.36 | ▼ -3.75 after sell → book $9,577.47; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `OMER` | 72 | $17.13 | $2.23 | $-20.27 | $6,208.49 | ▼ -20.27 after sell → book $9,575.24; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 112 | $9.10 | $2.35 | $-230.92 | $7,225.34 | ▼ -230.92 after sell → book $9,572.89; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NCMI` | 464 | $2.56 | $6.07 | $-72.38 | $8,407.11 | ▼ -72.38 after sell → book $9,566.82; vs 09:30 mark -6.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MXCT` | 899 | $1.29 | $11.76 | $-113.25 | $9,555.06 | ▼ -113.25 after sell → book $9,555.06; vs 09:30 mark -11.76 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,555.06 | ▲ close $9,555.06 vs 09:30 $9,589.58 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,555.06 | ▲ 09:30 equity $9,555.06 vs yday $9,555.06 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $8,375.85 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1194.38 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 160 | $7.44 | $2.47 | — | $7,182.98 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1194.38 | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 160 | $7.45 | $2.47 | — | $5,988.51 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1194.38 | — |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 10 | $113.23 | $2.02 | — | $4,854.19 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1194.38 | — |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 110 | $10.77 | $2.32 | — | $3,667.17 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1194.38 | — |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 126 | $9.46 | $2.37 | — | $2,472.84 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1194.38 | — |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 142 | $8.38 | $2.42 | — | $1,280.47 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1194.38 | — |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 40 | $29.20 | $2.11 | — | $110.36 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1194.38 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.36 | ▲ close $9,681.31 vs 09:30 $9,555.06 (session +144.58) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $110.36 | ▲ 09:30 equity $10,017.21 vs yday $9,681.31 (+335.90) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $99.11 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $13.79 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 10 | $1.32 | $0.16 | — | $85.75 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $13.79 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 8 | $1.66 | $0.16 | — | $72.31 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $13.79 | — |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 8 | $1.71 | $0.16 | — | $58.47 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $13.79 | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 15 | $0.86 | $0.17 | — | $45.34 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $13.79 | — |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 4 | $3.11 | $0.14 | — | $32.76 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+7.1; leftover $13.79 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.76 | ▼ close $9,908.45 vs 09:30 $10,017.21 (session -107.85) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.76 | ▲ 09:30 equity $9,986.89 vs yday $9,908.45 (+78.44) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.76 | ▼ close $9,907.40 vs 09:30 $9,986.89 (session -79.49) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.76 | ▼ 09:30 equity $9,808.54 vs yday $9,907.40 (-98.86) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 57 | $20.47 | $2.18 | $-14.60 | $1,197.37 | ▼ -14.60 after sell → book $9,806.36; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRVI` | 160 | $8.53 | $2.51 | $+169.42 | $2,559.66 | ▲ +169.42 after sell → book $9,803.85; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DNA` | 160 | $6.94 | $2.51 | $-86.58 | $3,667.56 | ▼ -86.58 after sell → book $9,801.35; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MSTR` | 10 | $119.11 | $2.04 | $+54.74 | $4,856.62 | ▲ +54.74 after sell → book $9,799.31; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EXK` | 110 | $10.44 | $2.35 | $-40.97 | $6,002.67 | ▼ -40.97 after sell → book $9,796.96; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SCZM` | 126 | $9.45 | $2.40 | $-6.03 | $7,190.97 | ▼ -6.03 after sell → book $9,794.56; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NG` | 142 | $9.31 | $2.45 | $+127.19 | $8,510.54 | ▲ +127.19 after sell → book $9,792.11; vs 09:30 mark -2.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BLSH` | 40 | $30.00 | $2.13 | $+27.76 | $9,708.41 | ▲ +27.76 after sell → book $9,789.98; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 167 | $7.25 | $2.49 | — | $8,495.17 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1213.55 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3389 | $0.36 | $22.30 | — | $7,259.61 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-15.6; leftover $1213.55 | — |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 109 | $11.12 | $2.32 | — | $6,045.21 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.7; leftover $1213.55 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 89 | $13.59 | $2.26 | — | $4,833.44 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1213.55 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 127 | $9.49 | $2.37 | — | $3,625.84 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1213.55 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 32 | $36.96 | $2.09 | — | $2,441.03 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1213.55 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 266 | $4.55 | $3.43 | — | $1,227.30 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1213.55 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 744 | $1.63 | $9.60 | — | $4.99 | — | baseline list, no extra gate; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1213.55 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.99 | ▲ close $10,027.52 vs 09:30 $9,808.54 (session +284.39) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.99 | ▼ 09:30 equity $10,016.61 vs yday $10,027.52 (-10.91) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $20.16 | ▲ +3.93 after sell → book $10,016.44; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 10 | $1.60 | $0.21 | $+2.43 | $35.95 | ▲ +2.43 after sell → book $10,016.23; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BTBT` | 8 | $1.53 | $0.17 | $-1.36 | $48.02 | ▼ -1.36 after sell → book $10,016.06; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ENHA` | 8 | $1.63 | $0.17 | $-0.98 | $60.89 | ▼ -0.98 after sell → book $10,015.89; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ORBS` | 15 | $0.80 | $0.18 | $-1.38 | $72.64 | ▼ -1.38 after sell → book $10,015.70; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 4 | $3.77 | $0.18 | $+2.32 | $87.54 | ▲ +2.32 after sell → book $10,015.52; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `RZLT` | 2 | $5.01 | $0.11 | — | $77.41 | — | baseline list, no extra gate; list flatten,yday_gainer; 🔵; ret5=+7.5; leftover $10.94 | — |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 1 | $9.83 | $0.10 | — | $67.48 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $10.94 | — |
| 2026-08-26 09:30 ET | **BUY** | `SENS` | 1 | $9.48 | $0.10 | — | $57.91 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $10.94 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.91 | ▲ close $10,259.75 vs 09:30 $10,016.61 (session +244.54) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.91 | ▼ 09:30 equity $10,241.16 vs yday $10,259.75 (-18.59) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `FLNC` | 1 | $11.52 | $0.12 | — | $46.27 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-8.2; leftover $11.58 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.27 | ▼ close $10,209.66 vs 09:30 $10,241.16 (session -31.39) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.27 | ▼ 09:30 equity $10,127.60 vs yday $10,209.66 (-82.06) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 167 | $9.73 | $2.53 | $+409.14 | $1,668.65 | ▲ +409.14 after sell → book $10,125.07; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `VITL` | 109 | $10.47 | $2.35 | $-75.51 | $2,807.53 | ▼ -75.51 after sell → book $10,122.73; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 89 | $13.05 | $2.28 | $-52.60 | $3,966.70 | ▼ -52.60 after sell → book $10,120.44; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CCOI` | 127 | $9.70 | $2.40 | $+21.90 | $5,196.20 | ▲ +21.90 after sell → book $10,118.04; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 32 | $39.60 | $2.11 | $+80.29 | $6,461.29 | ▲ +80.29 after sell → book $10,115.94; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZIP` | 266 | $4.21 | $3.49 | $-97.36 | $7,577.66 | ▼ -97.36 after sell → book $10,112.45; vs 09:30 mark -3.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 744 | $1.69 | $9.73 | $+25.31 | $8,825.29 | ▲ +25.31 after sell → book $10,102.72; vs 09:30 mark -9.73 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 38 | $32.90 | $2.10 | — | $7,572.99 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1260.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 80 | $15.66 | $2.23 | — | $6,317.96 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1260.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 15 | $79.42 | $2.04 | — | $5,124.62 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1260.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 379 | $3.32 | $4.89 | — | $3,861.46 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.4; leftover $1260.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 4 | $252.24 | $2.00 | — | $2,850.49 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1260.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 146 | $8.61 | $2.43 | — | $1,591.01 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.7; leftover $1260.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `XPOF` | 234 | $5.38 | $3.02 | — | $329.07 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.5; leftover $1260.76 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $329.07 | ▼ close $9,869.59 vs 09:30 $10,127.60 (session -214.42) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $329.07 | ▼ 09:30 equity $9,842.03 vs yday $9,869.59 (-27.56) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SAFX` | 3389 | $0.36 | $23.01 | $-31.75 | $1,532.88 | ▼ -31.75 after sell → book $9,819.03; vs 09:30 mark -23.00 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `RZLT` | 2 | $4.65 | $0.12 | $-0.95 | $1,542.06 | ▼ -0.95 after sell → book $9,818.91; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ABX` | 1 | $9.74 | $0.12 | $-0.31 | $1,551.68 | ▼ -0.31 after sell → book $9,818.79; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `SENS` | 1 | $9.29 | $0.12 | $-0.40 | $1,560.85 | ▼ -0.40 after sell → book $9,818.67; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,560.85 | ▲ close $9,895.17 vs 09:30 $9,842.03 (session +76.50) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,560.85 | ▼ 09:30 equity $9,758.11 vs yday $9,895.17 (-137.06) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `FLNC` | 1 | $10.58 | $0.13 | $-1.18 | $1,571.31 | ▼ -1.18 after sell → book $9,757.99; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,571.31 | ▼ close $9,615.54 vs 09:30 $9,758.11 (session -142.45) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,571.31 | ▼ 09:30 equity $9,585.38 vs yday $9,615.54 (-30.16) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 38 | $32.42 | $2.12 | $-22.47 | $2,801.14 | ▼ -22.47 after sell → book $9,583.25; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 80 | $13.92 | $2.25 | $-143.68 | $3,912.49 | ▼ -143.68 after sell → book $9,581.00; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 15 | $78.84 | $2.06 | $-12.79 | $5,093.03 | ▼ -12.79 after sell → book $9,578.94; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `PYXS` | 379 | $3.45 | $4.96 | $+39.42 | $6,395.62 | ▲ +39.42 after sell → book $9,573.98; vs 09:30 mark -4.96 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SIMO` | 4 | $235.71 | $2.02 | $-70.14 | $7,336.44 | ▼ -70.14 after sell → book $9,571.96; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `OPTX` | 146 | $7.25 | $2.46 | $-203.45 | $8,392.48 | ▼ -203.45 after sell → book $9,569.50; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `XPOF` | 234 | $5.03 | $3.07 | $-87.99 | $9,566.43 | ▼ -87.99 after sell → book $9,566.43; vs 09:30 mark -3.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,566.43 | ▲ close $9,566.43 vs 09:30 $9,585.38 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,566.43 | ▲ 09:30 equity $9,566.43 vs yday $9,566.43 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 77 | $15.45 | $2.22 | — | $8,374.56 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1195.80 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $7,204.98 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1195.80 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 71 | $16.77 | $2.20 | — | $6,012.11 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1195.80 | — |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 80 | $14.85 | $2.23 | — | $4,821.88 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1195.80 | — |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 21 | $55.42 | $2.05 | — | $3,656.01 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-25.9; leftover $1195.80 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 548 | $2.18 | $7.07 | — | $2,454.30 | — | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1195.80 | — |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 85 | $13.96 | $2.25 | — | $1,265.45 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-6.4; leftover $1195.80 | — |
| 2026-09-03 09:30 ET | **BUY** | `SAFX` | 3171 | $0.38 | $21.47 | — | $48.52 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-2.3; leftover $1195.80 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.52 | ▼ close $9,486.53 vs 09:30 $9,566.43 (session -38.40) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.52 | ▲ 09:30 equity $9,496.75 vs yday $9,486.53 (+10.22) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 2 | $2.52 | $0.06 | — | $43.42 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $6.06 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 1 | $4.78 | $0.05 | — | $38.59 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $6.06 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 3 | $1.59 | $0.06 | — | $33.76 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $6.06 | — |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 1 | $3.52 | $0.04 | — | $30.21 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $6.06 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.21 | ▲ close $9,641.27 vs 09:30 $9,496.75 (session +144.73) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.21 | ▼ 09:30 equity $9,635.89 vs yday $9,641.27 (-5.38) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.21 | ▲ close $9,657.45 vs 09:30 $9,635.89 (session +21.56) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.21 | ▼ 09:30 equity $9,602.57 vs yday $9,657.45 (-54.88) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 77 | $15.16 | $2.24 | $-26.79 | $1,195.28 | ▼ -26.79 after sell → book $9,600.32; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 8 | $140.29 | $2.03 | $-49.25 | $2,315.61 | ▼ -49.25 after sell → book $9,598.29; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 71 | $15.46 | $2.22 | $-97.44 | $3,411.04 | ▼ -97.44 after sell → book $9,596.06; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SLN` | 80 | $13.60 | $2.25 | $-104.48 | $4,496.79 | ▼ -104.48 after sell → book $9,593.81; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `EIX` | 21 | $59.49 | $2.07 | $+81.34 | $5,744.01 | ▲ +81.34 after sell → book $9,591.74; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 548 | $2.22 | $7.17 | $+7.68 | $6,953.40 | ▲ +7.68 after sell → book $9,584.57; vs 09:30 mark -7.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CLYM` | 85 | $15.82 | $2.27 | $+153.59 | $8,295.83 | ▲ +153.59 after sell → book $9,582.30; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SAFX` | 3171 | $0.40 | $22.73 | $+28.73 | $9,541.49 | ▲ +28.73 after sell → book $9,559.56; vs 09:30 mark -22.74 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,541.49 | ▼ close $9,558.82 vs 09:30 $9,602.57 (session -0.75) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,541.49 | ▼ 09:30 equity $9,558.41 vs yday $9,558.82 (-0.41) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 2 | $2.22 | $0.07 | $-0.73 | $9,545.86 | ▼ -0.73 after sell → book $9,558.34; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 1 | $3.92 | $0.06 | $-0.97 | $9,549.72 | ▼ -0.97 after sell → book $9,558.28; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 3 | $1.53 | $0.07 | $-0.31 | $9,554.24 | ▼ -0.31 after sell → book $9,558.20; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `EOSE` | 1 | $3.96 | $0.06 | $+0.34 | $9,558.14 | ▲ +0.34 after sell → book $9,558.14; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,558.14 | ▲ close $9,558.14 vs 09:30 $9,558.41 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,558.14 | ▲ 09:30 equity $9,558.14 vs yday $9,558.14 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 585 | $2.04 | $7.55 | — | $8,357.19 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1194.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 251 | $4.75 | $3.24 | — | $7,161.71 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1194.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 563 | $2.12 | $7.26 | — | $5,960.88 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1194.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 50 | $23.63 | $2.14 | — | $4,777.24 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-6.3; leftover $1194.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 103 | $11.55 | $2.30 | — | $3,585.30 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1194.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 7 | $157.55 | $2.01 | — | $2,480.43 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $1194.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 15 | $77.33 | $2.04 | — | $1,318.45 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+2.5; leftover $1194.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 22 | $52.55 | $2.06 | — | $160.29 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1194.77 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.29 | ▼ close $9,505.67 vs 09:30 $9,558.14 (session -23.88) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.29 | ▲ 09:30 equity $9,579.09 vs yday $9,505.67 (+73.42) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.29 | ▼ close $9,549.03 vs 09:30 $9,579.09 (session -30.06) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.29 | ▼ 09:30 equity $9,482.88 vs yday $9,549.03 (-66.15) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.29 | ▼ close $9,419.40 vs 09:30 $9,482.88 (session -63.48) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.29 | ▼ 09:30 equity $9,220.13 vs yday $9,419.40 (-199.27) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 585 | $1.89 | $7.65 | $-102.95 | $1,258.29 | ▼ -102.95 after sell → book $9,212.48; vs 09:30 mark -7.65 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 251 | $4.73 | $3.29 | $-11.55 | $2,442.23 | ▼ -11.55 after sell → book $9,209.19; vs 09:30 mark -3.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAK` | 563 | $1.84 | $7.37 | $-172.27 | $3,470.78 | ▼ -172.27 after sell → book $9,201.82; vs 09:30 mark -7.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `TYRA` | 50 | $25.58 | $2.16 | $+93.20 | $4,747.62 | ▲ +93.20 after sell → book $9,199.66; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `FUBO` | 103 | $10.75 | $2.33 | $-87.03 | $5,852.55 | ▼ -87.03 after sell → book $9,197.34; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RDDT` | 7 | $160.62 | $2.03 | $+17.45 | $6,974.86 | ▲ +17.45 after sell → book $9,195.31; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `VIST` | 15 | $76.75 | $2.06 | $-12.79 | $8,124.05 | ▼ -12.79 after sell → book $9,193.25; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAND` | 22 | $48.60 | $2.08 | $-91.03 | $9,191.18 | ▼ -91.03 after sell → book $9,191.18; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 61 | $18.61 | $2.17 | — | $8,053.79 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1148.90 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 63 | $18.21 | $2.18 | — | $6,904.38 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-19.1; leftover $1148.90 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 16 | $68.79 | $2.04 | — | $5,801.71 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1148.90 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 195 | $5.87 | $2.58 | — | $4,654.48 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1148.90 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 422 | $2.72 | $5.44 | — | $3,501.20 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.4; leftover $1148.90 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 13 | $87.40 | $2.03 | — | $2,362.97 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1148.90 | — |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 30 | $38.01 | $2.08 | — | $1,220.59 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $1148.90 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 42 | $27.09 | $2.12 | — | $80.69 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1148.90 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $80.69 | ▲ close $9,402.24 vs 09:30 $9,220.13 (session +231.70) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.69 | ▲ 09:30 equity $9,580.86 vs yday $9,402.24 (+178.62) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 59 | $0.17 | $0.28 | — | $70.38 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $10.09 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 4 | $2.40 | $0.11 | — | $60.68 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $10.09 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.68 | ▲ close $9,671.74 vs 09:30 $9,580.86 (session +91.26) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.68 | ▲ 09:30 equity $9,737.50 vs yday $9,671.74 (+65.76) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 7 | $0.97 | $0.09 | — | $53.80 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $7.58 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 3 | $2.08 | $0.07 | — | $47.49 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $7.58 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 1 | $3.95 | $0.04 | — | $43.49 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $7.58 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 1 | $5.83 | $0.06 | — | $37.60 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $7.58 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 2 | $3.58 | $0.08 | — | $30.36 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $7.58 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.36 | ▼ close $9,685.42 vs 09:30 $9,737.50 (session -51.73) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.36 | ▲ 09:30 equity $9,783.94 vs yday $9,685.42 (+98.52) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `BBNX` | 61 | $22.11 | $2.19 | $+209.13 | $1,376.88 | ▲ +209.13 after sell → book $9,781.75; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARQQ` | 63 | $20.55 | $2.20 | $+143.04 | $2,669.33 | ▲ +143.04 after sell → book $9,779.55; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 16 | $79.08 | $2.06 | $+160.54 | $3,932.55 | ▲ +160.54 after sell → book $9,777.49; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 195 | $5.62 | $2.62 | $-53.94 | $5,025.84 | ▼ -53.94 after sell → book $9,774.87; vs 09:30 mark -2.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QTRX` | 422 | $3.13 | $5.52 | $+162.05 | $6,341.17 | ▲ +162.05 after sell → book $9,769.35; vs 09:30 mark -5.52 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `VAL` | 13 | $83.46 | $2.05 | $-55.30 | $7,424.10 | ▼ -55.30 after sell → book $9,767.30; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `KRMN` | 30 | $36.30 | $2.10 | $-55.48 | $8,511.00 | ▼ -55.48 after sell → book $9,765.20; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ADPT` | 42 | $28.69 | $2.14 | $+62.95 | $9,713.85 | ▲ +62.95 after sell → book $9,763.06; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 303 | $4.00 | $3.91 | — | $8,497.94 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $1214.23 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 130 | $9.31 | $2.38 | — | $7,285.26 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1214.23 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 90 | $13.47 | $2.26 | — | $6,070.25 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1214.23 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1093 | $1.11 | $14.10 | — | $4,842.92 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1214.23 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 121 | $9.99 | $2.35 | — | $3,631.77 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1214.23 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 665 | $1.82 | $8.58 | — | $2,409.57 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1214.23 | — |
| 2026-09-21 09:30 ET | **BUY** | `SGML` | 119 | $10.13 | $2.35 | — | $1,201.16 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+4.9; leftover $1214.23 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,201.16 | ▼ close $9,438.40 vs 09:30 $9,783.94 (session -288.73) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,201.16 | ▲ 09:30 equity $9,446.36 vs yday $9,438.40 (+7.96) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `DVLT` | 59 | $0.16 | $0.29 | $-1.16 | $1,210.31 | ▼ -1.16 after sell → book $9,446.07; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 260 | $0.58 | $2.29 | — | $1,057.22 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $151.29 | — |
| 2026-09-22 09:30 ET | **BUY** | `ALOY` | 16 | $9.40 | $1.55 | — | $905.27 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+9.5; leftover $151.29 | — |
| 2026-09-22 09:30 ET | **BUY** | `GLND` | 51 | $2.94 | $1.65 | — | $753.68 | — | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+136.1; leftover $151.29 | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 16 | $9.11 | $1.51 | — | $606.41 | — | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+44.4; leftover $151.29 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 149 | $1.01 | $1.95 | — | $453.97 | — | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+14.3; leftover $151.29 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $453.97 | ▲ close $9,660.10 vs 09:30 $9,446.36 (session +222.99) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $453.97 | ▲ 09:30 equity $9,678.41 vs yday $9,660.10 (+18.31) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `SABR` | 4 | $2.24 | $0.12 | $-0.87 | $462.81 | ▼ -0.87 after sell → book $9,678.29; vs 09:30 mark -0.12 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TLSA` | 7 | $0.89 | $0.10 | $-0.75 | $468.93 | ▼ -0.75 after sell → book $9,678.19; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SWRD` | 3 | $2.16 | $0.09 | $+0.07 | $475.32 | ▲ +0.07 after sell → book $9,678.09; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `EYPT` | 1 | $4.10 | $0.06 | $+0.04 | $479.35 | ▲ +0.04 after sell → book $9,678.03; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BNC` | 1 | $6.29 | $0.09 | $+0.31 | $485.56 | ▲ +0.31 after sell → book $9,677.94; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `DDD` | 2 | $3.59 | $0.10 | $-0.16 | $492.64 | ▼ -0.16 after sell → book $9,677.85; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 2 | $20.65 | $0.42 | — | $450.92 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $61.58 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 15 | $3.93 | $0.63 | — | $391.34 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $61.58 | — |
| 2026-09-23 09:30 ET | **BUY** | `MAZE` | 2 | $28.30 | $0.57 | — | $334.17 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $61.58 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 3 | $15.72 | $0.48 | — | $286.53 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $61.58 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 2 | $25.40 | $0.51 | — | $235.21 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $61.58 | — |
| 2026-09-23 09:30 ET | **BUY** | `CLPT` | 3 | $15.55 | $0.48 | — | $188.09 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $61.58 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 80 | $0.77 | $0.85 | — | $125.79 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $61.58 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $125.79 | ▼ close $9,239.32 vs 09:30 $9,678.41 (session -434.57) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $125.79 | ▼ 09:30 equity $9,092.61 vs yday $9,239.32 (-146.71) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `CYPH` | 303 | $3.40 | $3.97 | $-189.68 | $1,152.02 | ▼ -189.68 after sell → book $9,088.64; vs 09:30 mark -3.97 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 130 | $8.67 | $2.41 | $-87.99 | $2,276.71 | ▼ -87.99 after sell → book $9,086.23; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 90 | $12.26 | $2.28 | $-113.89 | $3,377.83 | ▼ -113.89 after sell → book $9,083.94; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ORBS` | 1093 | $1.05 | $14.29 | $-93.97 | $4,511.18 | ▼ -93.97 after sell → book $9,069.65; vs 09:30 mark -14.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SBET` | 121 | $9.80 | $2.38 | $-27.73 | $5,694.60 | ▼ -27.73 after sell → book $9,067.27; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTBT` | 665 | $1.73 | $8.70 | $-83.78 | $6,833.03 | ▼ -83.78 after sell → book $9,058.57; vs 09:30 mark -8.70 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGML` | 119 | $9.89 | $2.38 | $-33.88 | $8,007.56 | ▼ -33.88 after sell → book $9,056.19; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,007.56 | ▲ close $9,166.47 vs 09:30 $9,092.61 (session +110.28) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,459.58 | ▲ 09:30 equity $8,698.31 vs yday $8,664.51 (+33.80) | 09:30 open · cash $7,459.58 (unchanged overnight, no fees) · equity $8,698.31 vs prior close $8,664.51 (+33.80) · 11 name(s) re-marked at the open (per-name table). ALOY×15 yday $8.52 → 09:30 $8.52 +0.00; APPS×12 yday $10.88 → 09:30 $10.88 +0.00; CRML×16 yday $8.17 → 09:30 $8.17 +0.00; DEFT×251 yday $0.53 → 09:30 $0.53 +0.00; FJET×73 yday $1.80 → 09:30 $1.80 +0.00; GLND×50 yday $5.35 → 09:30 $6.06 +35.50; GRAL×1 yday $125.21 → 09:30 $123.50 -1.71; INDP×2 yday $4.00 → 09:30 $4.00 +0.00; IVVD×146 yday $0.91 → 09:30 $0.91 +0.00; NMRA×12 yday $0.70 → 09:30 $0.70 +0.00; TLYS×2 yday $4.24 → 09:30 $4.24 +0.00 | — |
| 2026-09-25 09:30 ET | **SELL** | `GRAL` | 1 | $123.50 | $1.26 | $+14.42 | $7,581.82 | ▲ +14.42 after sell → book $8,697.05; vs 09:30 mark -1.26 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 41 | $26.27 | $2.11 | — | $6,502.64 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1083.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $5,495.49 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1083.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 119 | $9.05 | $2.35 | — | $4,416.20 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-27.1; leftover $1083.12 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BRVE` | 45 | $23.58 | $2.12 | — | $3,352.97 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $1083.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 280 | $3.86 | $3.61 | — | $2,268.56 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1083.12 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 36 | $29.76 | $2.10 | — | $1,195.10 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ret5=+156.1; leftover $1083.12 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DNA` | 106 | $10.20 | $2.31 | — | $111.59 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1083.12 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.59 | ▼ close $8,499.19 vs 09:30 $8,698.31 (session -181.23) | 16:00 close · cash $111.59 · equity $8,499.19 vs 09:30 $8,698.31 (-199.12; session marks -181.23) · 17 name(s) marked open→close (per-name table). ALOY×15 09:30 $8.52 → close $8.52 +0.00; APPS×12 09:30 $10.88 → close $10.88 +0.00; CRML×16 09:30 $8.17 → close $8.17 +0.00; DEFT×251 09:30 $0.53 → close $0.53 +0.00; FJET×73 09:30 $1.80 → close $1.80 -0.00; GLND×50 09:30 $6.06 → close $5.54 -26.00; INDP×2 09:30 $4.00 → close $4.00 +0.00; IVVD×146 09:30 $0.91 → close $0.91 -0.00; NMRA×12 09:30 $0.70 → close $0.70 +0.00; TLYS×2 09:30 $4.24 → close $4.24 -0.00; WRBY×41 09:30 $26.27 → close $26.71 +18.04; TXG×12 09:30 $83.76 → close $85.71 +23.40; AEHL×119 09:30 $9.05 → close $9.36 +36.89; BRVE×45 09:30 $23.58 → close $20.62 -133.20; ZSQR×280 09:30 $3.86 → close $3.78 -22.40; TJGC×36 09:30 $29.76 → close $26.24 -126.72; DNA×106 09:30 $10.20 → close $10.66 +48.76 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `WWW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MXCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CDNL` | cash | leftover split 0.61 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 0.61 < 1 share @ 9.12 |
| 2026-08-17 | `FCEL` | cash | leftover split 0.61 < 1 share @ 22.37 |
| 2026-08-17 | `VERA` | cash | leftover split 0.61 < 1 share @ 31.30 |
| 2026-08-17 | `CELC` | cash | leftover split 0.61 < 1 share @ 92.99 |
| 2026-08-17 | `CAPR` | cash | leftover split 0.61 < 1 share @ 6.87 |
| 2026-08-17 | `HTFL` | cash | leftover split 0.61 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 0.61 < 1 share @ 32.55 |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `WWW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OMER` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MXCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CBRS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COHR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDTH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRVI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MSTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BLSH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DE` | cash | leftover split 13.79 < 1 share @ 623.26 |
| 2026-08-21 | `QDEL` | cash | leftover split 13.79 < 1 share @ 14.96 |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BLSH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ENHA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ENHA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `VITL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CCOI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZIP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AVBP` | cash | leftover split 10.94 < 1 share @ 31.21 |
| 2026-08-26 | `FLNC` | cash | leftover split 10.94 < 1 share @ 11.12 |
| 2026-08-26 | `AVEX` | cash | leftover split 10.94 < 1 share @ 17.51 |
| 2026-08-26 | `ITG` | cash | leftover split 10.94 < 1 share @ 12.04 |
| 2026-08-26 | `BE` | cash | leftover split 10.94 < 1 share @ 213.94 |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `VITL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CCOI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZIP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `AVBP` | cash | leftover split 11.58 < 1 share @ 30.79 |
| 2026-08-27 | `AVEX` | cash | leftover split 11.58 < 1 share @ 18.43 |
| 2026-08-27 | `ITG` | cash | leftover split 11.58 < 1 share @ 12.36 |
| 2026-08-27 | `BE` | cash | leftover split 11.58 < 1 share @ 227.10 |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PYXS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OPTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `XPOF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `PYXS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `OPTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `XPOF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ELMT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `IRD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HQ` | cash | leftover split 6.06 < 1 share @ 15.90 |
| 2026-09-04 | `DELL` | cash | leftover split 6.06 < 1 share @ 513.78 |
| 2026-09-04 | `MLYS` | cash | leftover split 6.06 < 1 share @ 28.00 |
| 2026-09-04 | `CCOI` | cash | leftover split 6.06 < 1 share @ 10.02 |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `EIX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CLYM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `EOSE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `XLAB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `EOSE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CABA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CNTB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HAS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BHC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SARO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `TYRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `FUBO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RDDT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `VIST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CAN` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `USDE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `FUBO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RDDT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `VIST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TRX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IVVD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `BBNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ARQQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `VAL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `KRMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ADPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BRUN` | cash | leftover split 10.09 < 1 share @ 15.87 |
| 2026-09-17 | `AXTI` | cash | leftover split 10.09 < 1 share @ 67.91 |
| 2026-09-17 | `ARQT` | cash | leftover split 10.09 < 1 share @ 25.95 |
| 2026-09-17 | `SMTC` | cash | leftover split 10.09 < 1 share @ 170.85 |
| 2026-09-17 | `CIFR` | cash | leftover split 10.09 < 1 share @ 18.04 |
| 2026-09-17 | `EROC` | cash | leftover split 10.09 < 1 share @ 12.64 |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ARQQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `VAL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `KRMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `GNRC` | cash | leftover split 7.58 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 7.58 < 1 share @ 219.62 |
| 2026-09-18 | `BHVN` | cash | leftover split 7.58 < 1 share @ 14.07 |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SWRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `DDD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SNDK` | cash | leftover split 1214.23 < 1 share @ 1826.00 |
| 2026-09-22 | `SABR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `SWRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `DDD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SGML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `MRAM` | no_price | no 09:30 open |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SGML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `ALOY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `GLND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `CRML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `IVVD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `BLLN` | cash | leftover split 61.58 < 1 share @ 116.00 |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `ALOY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `GLND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `CRML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `IVVD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `MAZE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `TNGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CLPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `NMRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EGHT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SRFM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TDTH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `LU` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DEFT` | 260 | 2026-09-22 @ $0.58 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $151.29 |
| `ALOY` | 16 | 2026-09-22 @ $9.40 | baseline list, no extra gate; list probable,yday_gainer; ret5=+9.5; leftover $151.29 |
| `GLND` | 51 | 2026-09-22 @ $2.94 | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+136.1; leftover $151.29 |
| `CRML` | 16 | 2026-09-22 @ $9.11 | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+44.4; leftover $151.29 |
| `IVVD` | 149 | 2026-09-22 @ $1.01 | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+14.3; leftover $151.29 |
| `OMER` | 2 | 2026-09-23 @ $20.65 | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $61.58 |
| `INDP` | 15 | 2026-09-23 @ $3.93 | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $61.58 |
| `MAZE` | 2 | 2026-09-23 @ $28.30 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $61.58 |
| `SGRY` | 3 | 2026-09-23 @ $15.72 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $61.58 |
| `TNGX` | 2 | 2026-09-23 @ $25.40 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $61.58 |
| `CLPT` | 3 | 2026-09-23 @ $15.55 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $61.58 |
| `NMRA` | 80 | 2026-09-23 @ $0.77 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $61.58 |
