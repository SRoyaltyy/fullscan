# Factor mine action — `yday_gainer_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `yday_gainer` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-7.00%** ($9,300) · signal-only (no cash/fees) was +30.33%. Starts YES **6/30**. Fills 155 · skips 452 · realized $+2.09.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at yesterday's top liquid winners and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `yday_gainer` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $58.88.

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
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.90 | ▲ close $9,679.61 vs 09:30 $9,589.58 (session +90.03) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.90 | ▼ 09:30 equity $9,572.33 vs yday $9,679.61 (-107.28) | — | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.90 | ▼ close $9,427.80 vs 09:30 $9,572.33 (session -144.53) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.90 | ▲ 09:30 equity $9,487.85 vs yday $9,427.80 (+60.05) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 290 | $4.43 | $3.80 | $+27.26 | $1,285.80 | ▲ +27.26 after sell → book $9,484.05; vs 09:30 mark -3.80 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `WWW` | 60 | $20.32 | $2.19 | $-21.16 | $2,502.81 | ▼ -21.16 after sell → book $9,481.86; vs 09:30 mark -2.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 299 | $3.42 | $3.92 | $-235.01 | $3,521.47 | ▼ -235.01 after sell → book $9,477.94; vs 09:30 mark -3.92 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $4,752.18 | ▼ -4.38 after sell → book $9,475.74; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `OMER` | 72 | $18.64 | $2.23 | $+88.45 | $6,092.03 | ▲ +88.45 after sell → book $9,473.51; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `AIRO` | 112 | $8.39 | $2.35 | $-310.44 | $7,029.36 | ▼ -310.44 after sell → book $9,471.16; vs 09:30 mark -2.35 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `NCMI` | 464 | $2.55 | $6.07 | $-77.02 | $8,206.49 | ▼ -77.02 after sell → book $9,465.09; vs 09:30 mark -6.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `MXCT` | 899 | $1.40 | $11.76 | $-14.36 | $9,453.33 | ▼ -14.36 after sell → book $9,453.33; vs 09:30 mark -11.76 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 106 | $11.13 | $2.31 | — | $8,271.24 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1181.67 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 895 | $1.32 | $11.55 | — | $7,078.30 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1181.67 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 711 | $1.66 | $9.17 | — | $5,888.86 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1181.67 | — |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 691 | $1.71 | $8.91 | — | $4,698.34 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $1181.67 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $4,073.09 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1181.67 | — |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 78 | $14.96 | $2.22 | — | $2,903.98 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-1.6; leftover $1181.67 | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1367 | $0.86 | $15.91 | — | $1,706.98 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1181.67 | — |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 379 | $3.11 | $4.89 | — | $523.41 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+7.1; leftover $1181.67 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $523.41 | ▲ close $9,705.52 vs 09:30 $9,487.85 (session +309.14) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $523.41 | ▲ 09:30 equity $10,110.82 vs yday $9,705.52 (+405.30) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $523.41 | ▼ close $10,018.91 vs 09:30 $10,110.82 (session -91.91) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $523.41 | ▼ 09:30 equity $9,885.53 vs yday $10,018.91 (-133.38) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 9 | $7.25 | $0.68 | — | $457.48 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $65.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 182 | $0.36 | $1.20 | — | $391.12 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-15.6; leftover $65.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 5 | $11.12 | $0.57 | — | $334.95 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.7; leftover $65.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 4 | $13.59 | $0.56 | — | $280.04 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $65.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 6 | $9.49 | $0.59 | — | $222.51 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $65.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 1 | $36.96 | $0.37 | — | $185.18 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $65.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 14 | $4.55 | $0.68 | — | $120.80 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $65.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 40 | $1.63 | $0.77 | — | $54.82 | — | baseline list, no extra gate; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $65.43 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.82 | ▲ close $10,250.75 vs 09:30 $9,885.53 (session +370.64) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.82 | ▼ 09:30 equity $10,130.86 vs yday $10,250.75 (-119.89) | — | — |
| 2026-08-26 09:30 ET | **BUY** | `RZLT` | 1 | $5.01 | $0.05 | — | $49.76 | — | baseline list, no extra gate; list flatten,yday_gainer; 🔵; ret5=+7.5; leftover $6.85 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.76 | ▲ close $10,145.98 vs 09:30 $10,130.86 (session +15.17) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.76 | ▲ 09:30 equity $10,320.14 vs yday $10,145.98 (+174.16) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.76 | ▲ close $10,546.26 vs 09:30 $10,320.14 (session +226.12) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.76 | ▼ 09:30 equity $10,387.22 vs yday $10,546.26 (-159.04) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 106 | $15.43 | $2.34 | $+451.15 | $1,683.00 | ▲ +451.15 after sell → book $10,384.88; vs 09:30 mark -2.34 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 895 | $1.82 | $11.71 | $+424.25 | $3,300.20 | ▲ +424.25 after sell → book $10,373.17; vs 09:30 mark -11.71 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `BTBT` | 711 | $1.58 | $9.30 | $-75.35 | $4,414.28 | ▼ -75.35 after sell → book $10,363.87; vs 09:30 mark -9.30 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ENHA` | 691 | $1.56 | $9.04 | $-121.60 | $5,483.20 | ▼ -121.60 after sell → book $10,354.83; vs 09:30 mark -9.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `DE` | 1 | $626.50 | $2.01 | $-0.77 | $6,107.68 | ▼ -0.77 after sell → book $10,352.82; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `QDEL` | 78 | $15.09 | $2.25 | $+5.67 | $7,282.46 | ▲ +5.67 after sell → book $10,350.58; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ORBS` | 1367 | $0.83 | $15.74 | $-72.66 | $8,406.80 | ▼ -72.66 after sell → book $10,334.84; vs 09:30 mark -15.74 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `GORO` | 379 | $3.80 | $4.96 | $+251.66 | $9,842.03 | ▲ +251.66 after sell → book $10,329.87; vs 09:30 mark -4.97 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 42 | $32.90 | $2.12 | — | $8,458.12 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1406.00 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 89 | $15.66 | $2.26 | — | $7,062.12 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1406.00 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 17 | $79.42 | $2.04 | — | $5,709.94 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1406.00 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 423 | $3.32 | $5.46 | — | $4,300.12 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.4; leftover $1406.00 | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $3,036.92 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1406.00 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 163 | $8.61 | $2.48 | — | $1,631.01 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.7; leftover $1406.00 | — |
| 2026-08-28 09:30 ET | **BUY** | `XPOF` | 261 | $5.38 | $3.37 | — | $223.46 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.5; leftover $1406.00 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $223.46 | ▼ close $10,092.49 vs 09:30 $10,387.22 (session -217.66) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $223.46 | ▼ 09:30 equity $10,049.73 vs yday $10,092.49 (-42.76) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $223.46 | ▲ close $10,131.37 vs 09:30 $10,049.73 (session +81.65) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $223.46 | ▼ 09:30 equity $9,980.02 vs yday $10,131.37 (-151.35) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `CAPR` | 9 | $10.77 | $1.02 | $+29.98 | $319.37 | ▲ +29.98 after sell → book $9,979.00; vs 09:30 mark -1.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `SAFX` | 182 | $0.36 | $1.25 | $-1.17 | $384.55 | ▼ -1.17 after sell → book $9,977.75; vs 09:30 mark -1.25 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `VITL` | 5 | $10.67 | $0.57 | $-3.39 | $437.34 | ▼ -3.39 after sell → book $9,977.19; vs 09:30 mark -0.56 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `KURA` | 4 | $12.54 | $0.53 | $-5.29 | $486.96 | ▼ -5.29 after sell → book $9,976.65; vs 09:30 mark -0.54 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CCOI` | 6 | $9.42 | $0.60 | $-1.61 | $542.88 | ▼ -1.61 after sell → book $9,976.05; vs 09:30 mark -0.60 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `LIFE` | 1 | $35.06 | $0.37 | $-2.65 | $577.57 | ▼ -2.65 after sell → book $9,975.68; vs 09:30 mark -0.37 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ZIP` | 14 | $4.14 | $0.64 | $-7.06 | $634.88 | ▼ -7.06 after sell → book $9,975.03; vs 09:30 mark -0.65 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 40 | $1.68 | $0.81 | $+0.42 | $701.27 | ▲ +0.42 after sell → book $9,974.22; vs 09:30 mark -0.81 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $701.27 | ▼ close $9,813.63 vs 09:30 $9,980.02 (session -160.59) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $701.27 | ▼ 09:30 equity $9,779.05 vs yday $9,813.63 (-34.58) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `RZLT` | 1 | $4.50 | $0.07 | $-0.63 | $705.70 | ▼ -0.63 after sell → book $9,778.98; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $705.70 | ▲ close $10,042.15 vs 09:30 $9,779.05 (session +263.17) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $705.70 | ▲ 09:30 equity $10,049.04 vs yday $10,042.15 (+6.89) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 5 | $15.45 | $0.79 | — | $627.67 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $88.21 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 5 | $16.77 | $0.85 | — | $542.96 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $88.21 | — |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 5 | $14.85 | $0.76 | — | $467.96 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $88.21 | — |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 1 | $55.42 | $0.56 | — | $411.98 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-25.9; leftover $88.21 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 40 | $2.18 | $0.99 | — | $323.79 | — | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $88.21 | — |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 6 | $13.96 | $0.86 | — | $239.17 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-6.4; leftover $88.21 | — |
| 2026-09-03 09:30 ET | **BUY** | `SAFX` | 233 | $0.38 | $1.58 | — | $149.75 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-2.3; leftover $88.21 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $149.75 | ▼ close $9,943.44 vs 09:30 $10,049.04 (session -99.22) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.75 | ▼ 09:30 equity $9,915.09 vs yday $9,943.44 (-28.35) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `SEDG` | 42 | $33.86 | $2.14 | $+36.07 | $1,569.73 | ▲ +36.07 after sell → book $9,912.95; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `GRRR` | 89 | $13.56 | $2.28 | $-191.44 | $2,774.29 | ▼ -191.44 after sell → book $9,910.67; vs 09:30 mark -2.28 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `URBN` | 17 | $79.55 | $2.06 | $-1.89 | $4,124.58 | ▼ -1.89 after sell → book $9,908.61; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `PYXS` | 423 | $3.53 | $5.54 | $+77.83 | $5,612.23 | ▲ +77.83 after sell → book $9,903.07; vs 09:30 mark -5.54 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SIMO` | 5 | $239.23 | $2.02 | $-69.08 | $6,806.36 | ▼ -69.08 after sell → book $9,901.04; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 163 | $7.79 | $2.52 | $-138.66 | $8,073.61 | ▼ -138.66 after sell → book $9,898.53; vs 09:30 mark -2.51 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `XPOF` | 261 | $4.91 | $3.42 | $-129.46 | $9,351.70 | ▼ -129.46 after sell → book $9,895.10; vs 09:30 mark -3.43 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 463 | $2.52 | $5.97 | — | $8,178.97 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1168.96 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 244 | $4.78 | $3.15 | — | $7,009.50 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1168.96 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 735 | $1.59 | $9.48 | — | $5,831.37 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1168.96 | — |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 73 | $15.90 | $2.21 | — | $4,668.46 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.1; leftover $1168.96 | — |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 332 | $3.52 | $4.28 | — | $3,495.54 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $1168.96 | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $2,465.98 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1168.96 | — |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 41 | $28.00 | $2.11 | — | $1,315.87 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+8.7; leftover $1168.96 | — |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 116 | $10.02 | $2.34 | — | $151.21 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.2; leftover $1168.96 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.21 | ▲ close $9,904.65 vs 09:30 $9,915.09 (session +41.09) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.21 | ▼ 09:30 equity $9,855.99 vs yday $9,904.65 (-48.66) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.21 | ▲ close $10,127.23 vs 09:30 $9,855.99 (session +271.24) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.21 | ▼ 09:30 equity $10,039.36 vs yday $10,127.23 (-87.87) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.21 | ▼ close $9,821.85 vs 09:30 $10,039.36 (session -217.51) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.21 | ▼ 09:30 equity $9,601.25 vs yday $9,821.85 (-220.60) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.21 | ▼ close $9,457.24 vs 09:30 $9,601.25 (session -144.01) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.21 | ▲ 09:30 equity $9,553.18 vs yday $9,457.24 (+95.94) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `CRK` | 5 | $15.03 | $0.79 | $-3.67 | $225.57 | ▼ -3.67 after sell → book $9,552.39; vs 09:30 mark -0.79 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `ARCT` | 5 | $14.06 | $0.74 | $-15.14 | $295.14 | ▼ -15.14 after sell → book $9,551.65; vs 09:30 mark -0.74 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `SLN` | 5 | $13.32 | $0.70 | $-9.11 | $361.03 | ▼ -9.11 after sell → book $9,550.95; vs 09:30 mark -0.70 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `EIX` | 1 | $57.30 | $0.60 | $+0.73 | $417.74 | ▲ +0.73 after sell → book $9,550.35; vs 09:30 mark -0.60 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CRDL` | 40 | $2.03 | $0.95 | $-7.94 | $497.99 | ▼ -7.94 after sell → book $9,549.40; vs 09:30 mark -0.95 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CLYM` | 6 | $15.21 | $0.95 | $+5.69 | $588.30 | ▲ +5.69 after sell → book $9,548.45; vs 09:30 mark -0.95 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `SAFX` | 233 | $0.41 | $1.71 | $+5.56 | $683.28 | ▲ +5.56 after sell → book $9,546.74; vs 09:30 mark -1.71 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 41 | $2.04 | $0.96 | — | $598.68 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $85.41 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 17 | $4.75 | $0.86 | — | $517.07 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $85.41 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 40 | $2.12 | $0.97 | — | $431.30 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $85.41 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 3 | $23.63 | $0.72 | — | $359.69 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-6.3; leftover $85.41 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 7 | $11.55 | $0.83 | — | $278.01 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $85.41 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 1 | $77.33 | $0.78 | — | $199.91 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+2.5; leftover $85.41 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 1 | $52.55 | $0.53 | — | $146.83 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $85.41 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $146.83 | ▲ close $9,607.69 vs 09:30 $9,553.18 (session +66.59) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $146.83 | ▼ 09:30 equity $9,460.87 vs yday $9,607.69 (-146.82) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 463 | $2.15 | $6.06 | $-183.34 | $1,136.22 | ▼ -183.34 after sell → book $9,454.81; vs 09:30 mark -6.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 244 | $4.13 | $3.20 | $-164.95 | $2,140.74 | ▼ -164.95 after sell → book $9,451.61; vs 09:30 mark -3.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 735 | $1.59 | $9.61 | $-19.10 | $3,299.78 | ▼ -19.10 after sell → book $9,442.00; vs 09:30 mark -9.61 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `HQ` | 73 | $14.67 | $2.23 | $-94.23 | $4,368.46 | ▼ -94.23 after sell → book $9,439.77; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `EOSE` | 332 | $3.77 | $4.35 | $+74.37 | $5,615.75 | ▲ +74.37 after sell → book $9,435.42; vs 09:30 mark -4.35 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `MLYS` | 41 | $28.14 | $2.13 | $+1.49 | $6,767.36 | ▲ +1.49 after sell → book $9,433.29; vs 09:30 mark -2.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `CCOI` | 116 | $9.12 | $2.37 | $-109.11 | $7,822.91 | ▼ -109.11 after sell → book $9,430.92; vs 09:30 mark -2.37 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,822.91 | ▼ close $9,422.20 vs 09:30 $9,460.87 (session -8.72) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,822.91 | ▲ 09:30 equity $9,434.24 vs yday $9,422.20 (+12.04) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `DELL` | 2 | $541.55 | $2.02 | $+51.53 | $8,903.99 | ▲ +51.53 after sell → book $9,432.22; vs 09:30 mark -2.02 | dropped from list after 6 sess (min 5) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,903.99 | ▼ close $9,426.62 vs 09:30 $9,434.24 (session -5.60) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,903.99 | ▼ 09:30 equity $9,412.83 vs yday $9,426.62 (-13.79) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 59 | $18.61 | $2.17 | — | $7,803.83 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1113.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 61 | $18.21 | $2.17 | — | $6,690.85 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-19.1; leftover $1113.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 16 | $68.79 | $2.04 | — | $5,588.17 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1113.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 189 | $5.87 | $2.56 | — | $4,476.19 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1113.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 409 | $2.72 | $5.28 | — | $3,358.43 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.4; leftover $1113.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 12 | $87.40 | $2.03 | — | $2,307.60 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1113.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 29 | $38.01 | $2.08 | — | $1,203.24 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $1113.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 41 | $27.09 | $2.11 | — | $90.43 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1113.00 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.43 | ▲ close $9,608.06 vs 09:30 $9,412.83 (session +215.66) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.43 | ▲ 09:30 equity $9,785.63 vs yday $9,608.06 (+177.57) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 66 | $0.17 | $0.31 | — | $78.90 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $11.30 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 4 | $2.40 | $0.11 | — | $69.20 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $11.30 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $69.20 | ▲ close $9,869.38 vs 09:30 $9,785.63 (session +84.16) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.20 | ▲ 09:30 equity $9,933.96 vs yday $9,869.38 (+64.58) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AMTX` | 41 | $1.90 | $0.92 | $-7.62 | $146.17 | ▼ -7.62 after sell → book $9,933.04; vs 09:30 mark -0.92 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `CLOV` | 17 | $4.50 | $0.84 | $-5.94 | $221.84 | ▼ -5.94 after sell → book $9,932.20; vs 09:30 mark -0.84 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 40 | $1.77 | $0.85 | $-15.82 | $291.79 | ▼ -15.82 after sell → book $9,931.36; vs 09:30 mark -0.84 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `TYRA` | 3 | $24.58 | $0.77 | $+1.37 | $364.76 | ▲ +1.37 after sell → book $9,930.59; vs 09:30 mark -0.77 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `FUBO` | 7 | $9.90 | $0.73 | $-13.11 | $433.33 | ▼ -13.11 after sell → book $9,929.86; vs 09:30 mark -0.73 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `VIST` | 1 | $72.99 | $0.75 | $-5.87 | $505.57 | ▼ -5.87 after sell → book $9,929.10; vs 09:30 mark -0.76 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAND` | 1 | $51.19 | $0.53 | $-2.43 | $556.22 | ▼ -2.43 after sell → book $9,928.57; vs 09:30 mark -0.53 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 71 | $0.97 | $0.90 | — | $486.45 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $69.53 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 33 | $2.08 | $0.79 | — | $417.02 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $69.53 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 17 | $3.95 | $0.72 | — | $349.15 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $69.53 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 4 | $14.07 | $0.57 | — | $292.29 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $69.53 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 11 | $5.83 | $0.67 | — | $227.49 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $69.53 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 19 | $3.58 | $0.74 | — | $158.73 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $69.53 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.73 | ▼ close $9,869.26 vs 09:30 $9,933.96 (session -54.91) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.73 | ▲ 09:30 equity $9,974.65 vs yday $9,869.26 (+105.39) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 4 | $4.00 | $0.17 | — | $142.56 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $19.84 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 2 | $9.31 | $0.19 | — | $123.75 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $19.84 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 1 | $13.47 | $0.14 | — | $110.13 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $19.84 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 17 | $1.11 | $0.24 | — | $91.02 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $19.84 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 1 | $9.99 | $0.10 | — | $80.93 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $19.84 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 10 | $1.82 | $0.21 | — | $62.47 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $19.84 | — |
| 2026-09-21 09:30 ET | **BUY** | `SGML` | 1 | $10.13 | $0.10 | — | $52.23 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+4.9; leftover $19.84 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.23 | ▼ close $9,969.02 vs 09:30 $9,974.65 (session -4.46) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.23 | ▼ 09:30 equity $9,948.52 vs yday $9,969.02 (-20.50) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 11 | $0.58 | $0.10 | — | $45.75 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $6.53 | — |
| 2026-09-22 09:30 ET | **BUY** | `GLND` | 2 | $2.94 | $0.06 | — | $39.81 | — | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+136.1; leftover $6.53 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 6 | $1.01 | $0.08 | — | $33.67 | — | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+14.3; leftover $6.53 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.67 | ▲ close $9,963.35 vs 09:30 $9,948.52 (session +15.07) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.67 | ▲ 09:30 equity $10,025.46 vs yday $9,963.35 (+62.11) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BBNX` | 59 | $23.00 | $2.19 | $+254.66 | $1,388.48 | ▲ +254.66 after sell → book $10,023.28; vs 09:30 mark -2.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARQQ` | 61 | $23.30 | $2.19 | $+306.12 | $2,807.59 | ▲ +306.12 after sell → book $10,021.08; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `TEM` | 16 | $76.47 | $2.06 | $+118.78 | $4,029.05 | ▲ +118.78 after sell → book $10,019.02; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RIG` | 189 | $5.53 | $2.60 | $-69.42 | $5,071.62 | ▼ -69.42 after sell → book $10,016.43; vs 09:30 mark -2.59 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `QTRX` | 409 | $3.17 | $5.35 | $+173.42 | $6,362.80 | ▲ +173.42 after sell → book $10,011.07; vs 09:30 mark -5.36 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `VAL` | 12 | $83.39 | $2.05 | $-52.19 | $7,361.43 | ▼ -52.19 after sell → book $10,009.03; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `KRMN` | 29 | $33.34 | $2.10 | $-139.60 | $8,326.19 | ▼ -139.60 after sell → book $10,006.93; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `ADPT` | 41 | $27.74 | $2.13 | $+22.40 | $9,461.40 | ▲ +22.40 after sell → book $10,004.80; vs 09:30 mark -2.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 57 | $20.65 | $2.16 | — | $8,282.19 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1182.68 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 300 | $3.93 | $3.87 | — | $7,099.32 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1182.68 | — |
| 2026-09-23 09:30 ET | **BUY** | `MAZE` | 41 | $28.30 | $2.11 | — | $5,936.91 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $1182.68 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 75 | $15.72 | $2.21 | — | $4,755.69 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1182.68 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 46 | $25.40 | $2.13 | — | $3,585.16 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $1182.68 | — |
| 2026-09-23 09:30 ET | **BUY** | `CLPT` | 76 | $15.55 | $2.22 | — | $2,401.15 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $1182.68 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 1539 | $0.77 | $16.44 | — | $1,202.76 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $1182.68 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLLN` | 10 | $116.00 | $2.02 | — | $40.74 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $1182.68 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.74 | ▼ close $9,671.40 vs 09:30 $10,025.46 (session -300.23) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.74 | ▼ 09:30 equity $9,615.64 vs yday $9,671.40 (-55.76) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `DVLT` | 66 | $0.15 | $0.32 | $-1.95 | $50.32 | ▼ -1.95 after sell → book $9,615.32; vs 09:30 mark -0.32 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `SABR` | 4 | $2.17 | $0.12 | $-1.15 | $58.88 | ▼ -1.15 after sell → book $9,615.20; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $58.88 | ▲ close $9,659.72 vs 09:30 $9,615.64 (session +44.52) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.72 | ▲ 09:30 equity $9,330.72 vs yday $9,302.08 (+28.64) | 09:30 open · cash $104.72 (unchanged overnight, no fees) · equity $9,330.72 vs prior close $9,302.08 (+28.64) · 22 name(s) re-marked at the open (per-name table). AIB×14 yday $1.42 → 09:30 $1.42 +0.00; BHVN×1 yday $13.19 → 09:30 $13.19 +0.00; BNC×3 yday $6.26 → 09:30 $6.26 +0.00; BTBT×4 yday $1.79 → 09:30 $1.79 +0.00; CNXC×39 yday $29.39 → 09:30 $29.39 +0.00; CYPH×2 yday $4.08 → 09:30 $4.00 -0.15; DDD×6 yday $3.43 → 09:30 $3.43 +0.00; DEFT×10 yday $0.53 → 09:30 $0.53 +0.00; EYPT×5 yday $3.65 → 09:30 $3.65 +0.00; FJET×3 yday $1.80 → 09:30 $1.80 +0.00; GLND×2 yday $5.35 → 09:30 $6.06 +1.42; INDP×303 yday $4.00 → 09:30 $4.00 +0.00; IVVD×6 yday $0.91 → 09:30 $0.91 +0.00; MAZE×42 yday $26.21 → 09:30 $26.21 +0.00; NMRA×1552 yday $0.70 → 09:30 $0.70 +0.00; OMER×57 yday $20.13 → 09:30 $20.61 +27.36; ORBS×8 yday $1.03 → 09:30 $1.03 +0.00; RANI×27 yday $0.75 → 09:30 $0.75 +0.00; RARE×1 yday $14.77 → 09:30 $14.77 +0.00; TLYS×274 yday $4.24 → 09:30 $4.24 +0.00; TNGX×46 yday $24.63 → 09:30 $24.63 +0.00; VKTX×28 yday $36.75 → 09:30 $36.75 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 1 | $9.05 | $0.09 | — | $95.58 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-27.1; leftover $14.96 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 3 | $3.86 | $0.12 | — | $83.87 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $14.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DNA` | 1 | $10.20 | $0.10 | — | $73.57 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $14.96 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.57 | ▼ close $9,299.90 vs 09:30 $9,330.72 (session -30.49) | 16:00 close · cash $73.57 · equity $9,299.90 vs 09:30 $9,330.72 (-30.82; session marks -30.49) · 25 name(s) marked open→close (per-name table). AIB×14 09:30 $1.42 → close $1.42 -0.00; BHVN×1 09:30 $13.19 → close $13.19 -0.00; BNC×3 09:30 $6.26 → close $6.26 +0.00; BTBT×4 09:30 $1.79 → close $1.79 -0.00; CNXC×39 09:30 $29.39 → close $29.39 -0.00; CYPH×2 09:30 $4.00 → close $4.12 +0.23; DDD×6 09:30 $3.43 → close $3.43 +0.00; DEFT×10 09:30 $0.53 → close $0.53 +0.00; EYPT×5 09:30 $3.65 → close $3.65 +0.00; FJET×3 09:30 $1.80 → close $1.80 -0.00; GLND×2 09:30 $6.06 → close $5.54 -1.04; INDP×303 09:30 $4.00 → close $4.00 +0.00; IVVD×6 09:30 $0.91 → close $0.91 -0.00; MAZE×42 09:30 $26.21 → close $26.21 -0.00; NMRA×1552 09:30 $0.70 → close $0.70 +0.00; OMER×57 09:30 $20.61 → close $20.08 -30.21; ORBS×8 09:30 $1.03 → close $1.03 -0.00; RANI×27 09:30 $0.75 → close $0.75 +0.00; RARE×1 09:30 $14.77 → close $14.77 +0.00; TLYS×274 09:30 $4.24 → close $4.24 -0.00; TNGX×46 09:30 $24.63 → close $24.63 -0.00; VKTX×28 09:30 $36.75 → close $36.75 +0.00; AEHL×1 09:30 $9.05 → close $9.36 +0.31; ZSQR×3 09:30 $3.86 → close $3.78 -0.24; DNA×1 09:30 $10.20 → close $10.66 +0.46 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `WWW` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `OMER` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `MXCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `CDNL` | cash | leftover split 0.61 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 0.61 < 1 share @ 9.12 |
| 2026-08-17 | `FCEL` | cash | leftover split 0.61 < 1 share @ 22.37 |
| 2026-08-17 | `VERA` | cash | leftover split 0.61 < 1 share @ 31.30 |
| 2026-08-17 | `CELC` | cash | leftover split 0.61 < 1 share @ 92.99 |
| 2026-08-17 | `CAPR` | cash | leftover split 0.61 < 1 share @ 6.87 |
| 2026-08-17 | `HTFL` | cash | leftover split 0.61 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 0.61 < 1 share @ 32.55 |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `WWW` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `OMER` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `MXCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CBRS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COHR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDTH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `ANGX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `WWW` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HYLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ARX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `OMER` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `AIRO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `NCMI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `MXCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `WWW` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HYLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ARX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `OMER` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `AIRO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `NCMI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `MXCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `CDE` | cash | leftover split 0.61 < 1 share @ 20.65 |
| 2026-08-20 | `MRVI` | cash | leftover split 0.61 < 1 share @ 7.44 |
| 2026-08-20 | `DNA` | cash | leftover split 0.61 < 1 share @ 7.45 |
| 2026-08-20 | `MSTR` | cash | leftover split 0.61 < 1 share @ 113.23 |
| 2026-08-20 | `EXK` | cash | leftover split 0.61 < 1 share @ 10.77 |
| 2026-08-20 | `SCZM` | cash | leftover split 0.61 < 1 share @ 9.46 |
| 2026-08-20 | `NG` | cash | leftover split 0.61 < 1 share @ 8.38 |
| 2026-08-20 | `BLSH` | cash | leftover split 0.61 < 1 share @ 29.20 |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ENHA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `DE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `QDEL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `GORO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ENHA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `DE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `QDEL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `GORO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ENHA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `DE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `QDEL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ORBS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `GORO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `VITL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `CCOI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ZIP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `AVBP` | cash | leftover split 6.85 < 1 share @ 31.21 |
| 2026-08-26 | `FLNC` | cash | leftover split 6.85 < 1 share @ 11.12 |
| 2026-08-26 | `ABX` | cash | leftover split 6.85 < 1 share @ 9.83 |
| 2026-08-26 | `AVEX` | cash | leftover split 6.85 < 1 share @ 17.51 |
| 2026-08-26 | `ITG` | cash | leftover split 6.85 < 1 share @ 12.04 |
| 2026-08-26 | `SENS` | cash | leftover split 6.85 < 1 share @ 9.48 |
| 2026-08-26 | `BE` | cash | leftover split 6.85 < 1 share @ 213.94 |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ENHA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `DE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `QDEL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ORBS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `GORO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `VITL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CCOI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ZIP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-27 | `AVBP` | cash | leftover split 7.11 < 1 share @ 30.79 |
| 2026-08-27 | `FLNC` | cash | leftover split 7.11 < 1 share @ 11.52 |
| 2026-08-27 | `ABX` | cash | leftover split 7.11 < 1 share @ 9.68 |
| 2026-08-27 | `AVEX` | cash | leftover split 7.11 < 1 share @ 18.43 |
| 2026-08-27 | `ITG` | cash | leftover split 7.11 < 1 share @ 12.36 |
| 2026-08-27 | `SENS` | cash | leftover split 7.11 < 1 share @ 9.33 |
| 2026-08-27 | `BE` | cash | leftover split 7.11 < 1 share @ 227.10 |
| 2026-08-28 | `CAPR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `VITL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `KURA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CCOI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `LIFE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ZIP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `SAFX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `VITL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `KURA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CCOI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `LIFE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ZIP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `PYXS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `OPTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `XPOF` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `PYXS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `OPTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `XPOF` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ELMT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `SEDG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `GRRR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `URBN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `PYXS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SIMO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `OPTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `XPOF` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `IRD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `SEDG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `GRRR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `URBN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `PYXS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SIMO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `OPTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `XPOF` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `MRNA` | cash | leftover split 88.21 < 1 share @ 145.94 |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CLYM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `SAFX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `SLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `EIX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CLYM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `SAFX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `HQ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `EOSE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `MLYS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `CCOI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `XLAB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `SLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `EIX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CLYM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `SAFX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `HQ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `EOSE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `DELL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `MLYS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `CCOI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CABA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CNTB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HAS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BHC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SARO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `CRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `SLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `EIX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CLYM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `SAFX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ALEC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `OABI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `OPK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `HQ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `EOSE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `DELL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `MLYS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `CCOI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `ALEC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OABI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OPK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `HQ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `EOSE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `DELL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `MLYS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `CCOI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `RDDT` | cash | leftover split 85.41 < 1 share @ 157.55 |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `TYRA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `FUBO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `VIST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CAN` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `USDE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `BAK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `FUBO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `VIST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `BAND` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TRX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IVVD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-16 | `AMTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `CLOV` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `BAK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `TYRA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `FUBO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `VIST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `BAND` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-17 | `AMTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `CLOV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `BAK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `TYRA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `FUBO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `VIST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `BAND` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `BBNX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ARQQ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `QTRX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `VAL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `KRMN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ADPT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `BRUN` | cash | leftover split 11.30 < 1 share @ 15.87 |
| 2026-09-17 | `AXTI` | cash | leftover split 11.30 < 1 share @ 67.91 |
| 2026-09-17 | `ARQT` | cash | leftover split 11.30 < 1 share @ 25.95 |
| 2026-09-17 | `SMTC` | cash | leftover split 11.30 < 1 share @ 170.85 |
| 2026-09-17 | `CIFR` | cash | leftover split 11.30 < 1 share @ 18.04 |
| 2026-09-17 | `EROC` | cash | leftover split 11.30 < 1 share @ 12.64 |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `ARQQ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `QTRX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `VAL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `KRMN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `GNRC` | cash | leftover split 69.53 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 69.53 < 1 share @ 219.62 |
| 2026-09-21 | `BBNX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `ARQQ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `TEM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RIG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `QTRX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `VAL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `KRMN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `ADPT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `SWRD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `BNC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `DDD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `SNDK` | cash | leftover split 19.84 < 1 share @ 1826.00 |
| 2026-09-22 | `BBNX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `ARQQ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `TEM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `RIG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `QTRX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `VAL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `KRMN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `ADPT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `DVLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `SABR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `SWRD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `BNC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `DDD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `SGML` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `ALOY` | cash | leftover split 6.53 < 1 share @ 9.40 |
| 2026-09-22 | `MRAM` | no_price | no 09:30 open |
| 2026-09-22 | `CRML` | cash | leftover split 6.53 < 1 share @ 9.11 |
| 2026-09-23 | `DVLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `SABR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `TLSA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `SWRD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `EYPT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `BHVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `BNC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `DDD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `ORBS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `SGML` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-23 | `GLND` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-23 | `IVVD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `TLSA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `SWRD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `EYPT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `BHVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `BNC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `DDD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `BKKT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `BTDR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `ORBS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `SBET` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `SGML` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-24 | `GLND` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-24 | `IVVD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `INDP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `MAZE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `TNGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `CLPT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `NMRA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `BLLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| `TLSA` | 71 | 2026-09-18 @ $0.97 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $69.53 |
| `SWRD` | 33 | 2026-09-18 @ $2.08 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $69.53 |
| `EYPT` | 17 | 2026-09-18 @ $3.95 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $69.53 |
| `BHVN` | 4 | 2026-09-18 @ $14.07 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $69.53 |
| `BNC` | 11 | 2026-09-18 @ $5.83 | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $69.53 |
| `DDD` | 19 | 2026-09-18 @ $3.58 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $69.53 |
| `CYPH` | 4 | 2026-09-21 @ $4.00 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $19.84 |
| `BKKT` | 2 | 2026-09-21 @ $9.31 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $19.84 |
| `BTDR` | 1 | 2026-09-21 @ $13.47 | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $19.84 |
| `ORBS` | 17 | 2026-09-21 @ $1.11 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $19.84 |
| `SBET` | 1 | 2026-09-21 @ $9.99 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $19.84 |
| `BTBT` | 10 | 2026-09-21 @ $1.82 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $19.84 |
| `SGML` | 1 | 2026-09-21 @ $10.13 | baseline list, no extra gate; list probable,yday_gainer; ret5=+4.9; leftover $19.84 |
| `DEFT` | 11 | 2026-09-22 @ $0.58 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $6.53 |
| `GLND` | 2 | 2026-09-22 @ $2.94 | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+136.1; leftover $6.53 |
| `IVVD` | 6 | 2026-09-22 @ $1.01 | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+14.3; leftover $6.53 |
| `OMER` | 57 | 2026-09-23 @ $20.65 | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1182.68 |
| `INDP` | 300 | 2026-09-23 @ $3.93 | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1182.68 |
| `MAZE` | 41 | 2026-09-23 @ $28.30 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $1182.68 |
| `SGRY` | 75 | 2026-09-23 @ $15.72 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1182.68 |
| `TNGX` | 46 | 2026-09-23 @ $25.40 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $1182.68 |
| `CLPT` | 76 | 2026-09-23 @ $15.55 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $1182.68 |
| `NMRA` | 1539 | 2026-09-23 @ $0.77 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $1182.68 |
| `BLLN` | 10 | 2026-09-23 @ $116.00 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $1182.68 |
