# Factor mine action — `yday_gainer_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `yday_gainer` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-13.96%** ($8,604) · signal-only (no cash/fees) was -1.36%. Starts YES **6/30**. Fills 258 · skips 103 · realized $+133.85.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at yesterday's top liquid winners and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `yday_gainer` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,133.85.

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
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $1,335.10 | ▲ +76.56 after sell → book $9,846.67; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WWW` | 60 | $20.98 | $2.19 | $+18.44 | $2,591.71 | ▲ +18.44 after sell → book $9,844.48; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $3,813.69 | ▼ -31.69 after sell → book $9,840.56; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $5,044.40 | ▼ -4.38 after sell → book $9,838.36; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `OMER` | 72 | $17.17 | $2.23 | $-17.39 | $6,278.41 | ▼ -17.39 after sell → book $9,836.13; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $7,347.90 | ▼ -178.28 after sell → book $9,833.78; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 464 | $2.80 | $6.07 | $+38.98 | $8,641.03 | ▲ +38.98 after sell → book $9,827.71; vs 09:30 mark -6.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MXCT` | 899 | $1.32 | $11.76 | $-86.28 | $9,815.95 | ▼ -86.28 after sell → book $9,815.95; vs 09:30 mark -11.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 30 | $39.85 | $2.08 | — | $8,618.37 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1226.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 134 | $9.12 | $2.39 | — | $7,393.90 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1226.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `FCEL` | 54 | $22.37 | $2.15 | — | $6,183.77 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=+9.5; leftover $1226.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 39 | $31.30 | $2.11 | — | $4,960.96 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-3.8; leftover $1226.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 13 | $92.99 | $2.03 | — | $3,750.06 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.8; leftover $1226.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 178 | $6.87 | $2.52 | — | $2,524.68 | — | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+62.6; leftover $1226.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $1,326.93 | — | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+46.0; leftover $1226.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $120.48 | — | baseline list, no extra gate; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1226.99 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $120.48 | ▲ close $9,820.10 vs 09:30 $9,850.47 (session +21.61) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $120.48 | ▼ 09:30 equity $9,739.68 vs yday $9,820.10 (-80.42) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 30 | $41.57 | $2.10 | $+47.42 | $1,365.48 | ▲ +47.42 after sell → book $9,737.58; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 134 | $9.03 | $2.42 | $-16.88 | $2,573.07 | ▼ -16.88 after sell → book $9,735.15; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FCEL` | 54 | $21.18 | $2.17 | $-68.58 | $3,714.62 | ▼ -68.58 after sell → book $9,732.98; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 39 | $31.31 | $2.13 | $-3.84 | $4,933.59 | ▼ -3.84 after sell → book $9,730.86; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 13 | $92.38 | $2.05 | $-12.01 | $6,132.48 | ▼ -12.01 after sell → book $9,728.81; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CAPR` | 178 | $7.50 | $2.56 | $+107.05 | $7,464.91 | ▲ +107.05 after sell → book $9,726.24; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $8,666.31 | ▲ +3.66 after sell → book $9,724.14; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $9,722.02 | ▼ -150.74 after sell → book $9,722.02; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,722.02 | ▲ close $9,722.02 vs 09:30 $9,739.68 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,722.02 | ▲ 09:30 equity $9,722.02 vs yday $9,722.02 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,722.02 | ▲ close $9,722.02 vs 09:30 $9,722.02 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,722.02 | ▲ 09:30 equity $9,722.02 vs yday $9,722.02 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 58 | $20.65 | $2.16 | — | $8,522.16 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1215.25 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 163 | $7.44 | $2.48 | — | $7,306.96 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1215.25 | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 163 | $7.45 | $2.48 | — | $6,090.13 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1215.25 | — |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 10 | $113.23 | $2.02 | — | $4,955.81 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1215.25 | — |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 112 | $10.77 | $2.33 | — | $3,747.25 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1215.25 | — |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 128 | $9.46 | $2.37 | — | $2,533.99 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1215.25 | — |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 145 | $8.38 | $2.42 | — | $1,316.47 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1215.25 | — |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 41 | $29.20 | $2.11 | — | $117.15 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1215.25 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $117.15 | ▲ close $9,850.84 vs 09:30 $9,722.02 (session +147.20) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $117.15 | ▲ 09:30 equity $10,191.87 vs yday $9,850.84 (+341.03) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 58 | $21.75 | $2.18 | $+59.45 | $1,376.47 | ▲ +59.45 after sell → book $10,189.69; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRVI` | 163 | $8.28 | $2.52 | $+131.92 | $2,723.59 | ▲ +131.92 after sell → book $10,187.17; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 163 | $7.09 | $2.52 | $-63.68 | $3,876.75 | ▼ -63.68 after sell → book $10,184.66; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MSTR` | 10 | $119.69 | $2.04 | $+60.54 | $5,071.61 | ▲ +60.54 after sell → book $10,182.62; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `EXK` | 112 | $11.34 | $2.35 | $+59.16 | $6,339.33 | ▲ +59.16 after sell → book $10,180.26; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 128 | $10.26 | $2.41 | $+97.62 | $7,650.21 | ▲ +97.62 after sell → book $10,177.86; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NG` | 145 | $9.02 | $2.46 | $+87.92 | $8,955.65 | ▲ +87.92 after sell → book $10,175.40; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BLSH` | 41 | $29.75 | $2.13 | $+18.30 | $10,173.26 | ▲ +18.30 after sell → book $10,173.26; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 114 | $11.13 | $2.33 | — | $8,902.11 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1271.66 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 963 | $1.32 | $12.42 | — | $7,618.53 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1271.66 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 766 | $1.66 | $9.88 | — | $6,337.09 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1271.66 | — |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 743 | $1.71 | $9.58 | — | $5,056.97 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $1271.66 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $3,808.46 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1271.66 | — |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 85 | $14.96 | $2.25 | — | $2,534.61 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-1.6; leftover $1271.66 | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1471 | $0.86 | $17.12 | — | $1,246.55 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1271.66 | — |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 399 | $3.11 | $5.15 | — | $0.51 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+7.1; leftover $1271.66 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.51 | ▲ close $10,466.34 vs 09:30 $10,191.87 (session +353.81) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.51 | ▲ 09:30 equity $10,907.51 vs yday $10,466.34 (+441.17) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 114 | $13.33 | $2.36 | $+246.10 | $1,517.77 | ▲ +246.10 after sell → book $10,905.15; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 963 | $1.83 | $12.60 | $+466.11 | $3,267.46 | ▲ +466.11 after sell → book $10,892.55; vs 09:30 mark -12.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 766 | $1.55 | $10.02 | $-104.16 | $4,444.74 | ▼ -104.16 after sell → book $10,882.53; vs 09:30 mark -10.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ENHA` | 743 | $1.74 | $9.72 | $+2.99 | $5,727.84 | ▲ +2.99 after sell → book $10,872.81; vs 09:30 mark -9.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $7,031.91 | ▲ +55.55 after sell → book $10,870.80; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 85 | $14.74 | $2.27 | $-23.21 | $8,282.54 | ▼ -23.21 after sell → book $10,868.53; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1471 | $0.89 | $17.76 | $+3.36 | $9,573.97 | ▲ +3.36 after sell → book $10,850.77; vs 09:30 mark -17.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GORO` | 399 | $3.20 | $5.22 | $+25.54 | $10,845.54 | ▲ +25.54 after sell → book $10,845.54; vs 09:30 mark -5.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,845.54 | ▲ close $10,845.54 vs 09:30 $10,907.51 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,845.54 | ▲ 09:30 equity $10,845.54 vs yday $10,845.54 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 186 | $7.25 | $2.55 | — | $9,494.49 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1355.69 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3786 | $0.36 | $24.91 | — | $8,114.20 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-15.6; leftover $1355.69 | — |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 121 | $11.12 | $2.35 | — | $6,766.32 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.7; leftover $1355.69 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 99 | $13.59 | $2.29 | — | $5,418.63 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1355.69 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 142 | $9.49 | $2.42 | — | $4,068.63 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1355.69 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 36 | $36.96 | $2.10 | — | $2,735.97 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1355.69 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 297 | $4.55 | $3.83 | — | $1,380.79 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1355.69 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 831 | $1.63 | $10.72 | — | $15.54 | — | baseline list, no extra gate; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1355.69 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.54 | ▲ close $11,108.14 vs 09:30 $10,845.54 (session +313.77) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.54 | ▼ 09:30 equity $11,097.43 vs yday $11,108.14 (-10.71) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 186 | $8.29 | $2.59 | $+188.30 | $1,554.89 | ▲ +188.30 after sell → book $11,094.84; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 3786 | $0.35 | $25.36 | $-69.20 | $2,865.98 | ▼ -69.20 after sell → book $11,069.48; vs 09:30 mark -25.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `VITL` | 121 | $11.03 | $2.38 | $-15.63 | $4,198.23 | ▼ -15.63 after sell → book $11,067.10; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 99 | $13.63 | $2.31 | $-0.64 | $5,545.29 | ▼ -0.64 after sell → book $11,064.78; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 142 | $9.89 | $2.45 | $+51.93 | $6,947.22 | ▲ +51.93 after sell → book $11,062.33; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 36 | $38.24 | $2.12 | $+41.86 | $8,321.74 | ▲ +41.86 after sell → book $11,060.21; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 297 | $4.31 | $3.89 | $-79.00 | $9,597.92 | ▼ -79.00 after sell → book $11,056.32; vs 09:30 mark -3.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 831 | $1.75 | $10.87 | $+82.29 | $11,045.45 | ▲ +82.29 after sell → book $11,045.45; vs 09:30 mark -10.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `RZLT` | 275 | $5.01 | $3.55 | — | $9,664.15 | — | baseline list, no extra gate; list flatten,yday_gainer; 🔵; ret5=+7.5; leftover $1380.68 | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 44 | $31.21 | $2.12 | — | $8,288.79 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1380.68 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 124 | $11.12 | $2.36 | — | $6,907.55 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1380.68 | — |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 140 | $9.83 | $2.41 | — | $5,528.94 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1380.68 | — |
| 2026-08-26 09:30 ET | **BUY** | `AVEX` | 78 | $17.51 | $2.22 | — | $4,160.94 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $1380.68 | — |
| 2026-08-26 09:30 ET | **BUY** | `ITG` | 114 | $12.04 | $2.33 | — | $2,786.04 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-5.1; leftover $1380.68 | — |
| 2026-08-26 09:30 ET | **BUY** | `SENS` | 145 | $9.48 | $2.42 | — | $1,409.02 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $1380.68 | — |
| 2026-08-26 09:30 ET | **BUY** | `BE` | 6 | $213.94 | $2.01 | — | $123.37 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1380.68 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.37 | ▲ close $11,136.03 vs 09:30 $11,097.43 (session +110.01) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.37 | ▲ 09:30 equity $11,218.09 vs yday $11,136.03 (+82.06) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 275 | $5.07 | $3.60 | $+9.35 | $1,514.02 | ▲ +9.35 after sell → book $11,214.49; vs 09:30 mark -3.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 116 | $12.98 | $2.34 | — | $6.00 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $1514.02 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.00 | ▲ close $11,292.84 vs 09:30 $11,218.09 (session +80.69) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.00 | ▼ 09:30 equity $11,220.14 vs yday $11,292.84 (-72.70) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AVBP` | 44 | $30.53 | $2.14 | $-34.18 | $1,347.18 | ▼ -34.18 after sell → book $11,218.00; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 124 | $11.27 | $2.39 | $+13.84 | $2,742.26 | ▲ +13.84 after sell → book $11,215.60; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ABX` | 140 | $9.88 | $2.44 | $+2.15 | $4,123.02 | ▲ +2.15 after sell → book $11,213.16; vs 09:30 mark -2.44 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AVEX` | 78 | $18.75 | $2.25 | $+92.25 | $5,583.27 | ▲ +92.25 after sell → book $11,210.91; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ITG` | 114 | $12.79 | $2.36 | $+80.81 | $7,038.97 | ▲ +80.81 after sell → book $11,208.55; vs 09:30 mark -2.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SENS` | 145 | $9.39 | $2.46 | $-17.93 | $8,398.06 | ▼ -17.93 after sell → book $11,206.09; vs 09:30 mark -2.46 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BE` | 6 | $215.71 | $2.03 | $+6.55 | $9,690.26 | ▲ +6.55 after sell → book $11,204.06; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 116 | $13.05 | $2.37 | $+3.41 | $11,201.69 | ▲ +3.41 after sell → book $11,201.69; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 42 | $32.90 | $2.12 | — | $9,817.77 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1400.21 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 89 | $15.66 | $2.26 | — | $8,421.78 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1400.21 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 17 | $79.42 | $2.04 | — | $7,069.60 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1400.21 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 421 | $3.32 | $5.43 | — | $5,666.44 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.4; leftover $1400.21 | — |
| 2026-08-28 09:30 ET | **BUY** | `SAFX` | 3836 | $0.36 | $25.51 | — | $4,240.79 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+7.6; leftover $1400.21 | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $2,977.59 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1400.21 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 162 | $8.61 | $2.48 | — | $1,580.29 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.7; leftover $1400.21 | — |
| 2026-08-28 09:30 ET | **BUY** | `XPOF` | 260 | $5.38 | $3.35 | — | $178.14 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.5; leftover $1400.21 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $178.14 | ▼ close $10,916.42 vs 09:30 $11,220.14 (session -240.08) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $178.14 | ▼ 09:30 equity $10,886.60 vs yday $10,916.42 (-29.82) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 42 | $31.15 | $2.14 | $-77.75 | $1,484.30 | ▼ -77.75 after sell → book $10,884.47; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 89 | $14.44 | $2.28 | $-113.12 | $2,767.18 | ▼ -113.12 after sell → book $10,882.18; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 17 | $80.44 | $2.06 | $+13.24 | $4,132.60 | ▲ +13.24 after sell → book $10,880.12; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PYXS` | 421 | $3.20 | $5.51 | $-61.46 | $5,474.29 | ▼ -61.46 after sell → book $10,874.61; vs 09:30 mark -5.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SAFX` | 3836 | $0.36 | $26.04 | $-63.06 | $6,836.88 | ▼ -63.06 after sell → book $10,848.57; vs 09:30 mark -26.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $8,070.10 | ▼ -29.98 after sell → book $10,846.54; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 162 | $8.52 | $2.51 | $-19.57 | $9,447.83 | ▼ -19.57 after sell → book $10,844.03; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `XPOF` | 260 | $5.37 | $3.41 | $-9.36 | $10,840.62 | ▼ -9.36 after sell → book $10,840.62; vs 09:30 mark -3.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,840.62 | ▲ close $10,840.62 vs 09:30 $10,886.60 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,840.62 | ▲ 09:30 equity $10,840.62 vs yday $10,840.62 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,840.62 | ▲ close $10,840.62 vs 09:30 $10,840.62 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,840.62 | ▲ 09:30 equity $10,840.62 vs yday $10,840.62 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,840.62 | ▲ close $10,840.62 vs 09:30 $10,840.62 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,840.62 | ▲ 09:30 equity $10,840.62 vs yday $10,840.62 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 87 | $15.45 | $2.25 | — | $9,494.22 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1355.08 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $8,178.70 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1355.08 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 80 | $16.77 | $2.23 | — | $6,834.87 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1355.08 | — |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 91 | $14.85 | $2.26 | — | $5,481.25 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1355.08 | — |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 24 | $55.42 | $2.06 | — | $4,149.11 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-25.9; leftover $1355.08 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 621 | $2.18 | $8.01 | — | $2,787.32 | — | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1355.08 | — |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 97 | $13.96 | $2.28 | — | $1,430.92 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-6.4; leftover $1355.08 | — |
| 2026-09-03 09:30 ET | **BUY** | `SAFX` | 3594 | $0.38 | $24.33 | — | $51.65 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-2.3; leftover $1355.08 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.65 | ▼ close $10,752.74 vs 09:30 $10,840.62 (session -42.43) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.65 | ▲ 09:30 equity $10,763.74 vs yday $10,752.74 (+11.00) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 87 | $15.00 | $2.28 | $-43.68 | $1,354.38 | ▼ -43.68 after sell → book $10,761.47; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 9 | $153.62 | $2.04 | $+65.02 | $2,734.92 | ▲ +65.02 after sell → book $10,759.43; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 80 | $15.61 | $2.25 | $-97.28 | $3,981.46 | ▼ -97.28 after sell → book $10,757.18; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 91 | $14.63 | $2.29 | $-24.57 | $5,310.51 | ▼ -24.57 after sell → book $10,754.89; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 24 | $55.79 | $2.08 | $+4.74 | $6,647.38 | ▲ +4.74 after sell → book $10,752.80; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 621 | $2.16 | $8.12 | $-28.56 | $7,980.62 | ▼ -28.56 after sell → book $10,744.68; vs 09:30 mark -8.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CLYM` | 97 | $14.49 | $2.31 | $+46.82 | $9,383.84 | ▲ +46.82 after sell → book $10,742.37; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SAFX` | 3594 | $0.38 | $24.97 | $-45.71 | $10,717.40 | ▼ -45.71 after sell → book $10,717.40; vs 09:30 mark -24.97 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 531 | $2.52 | $6.85 | — | $9,372.43 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1339.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 280 | $4.78 | $3.61 | — | $8,030.41 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1339.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 842 | $1.59 | $10.86 | — | $6,680.77 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1339.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 84 | $15.90 | $2.24 | — | $5,342.93 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.1; leftover $1339.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 380 | $3.52 | $4.90 | — | $4,000.43 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $1339.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $2,970.87 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1339.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 47 | $28.00 | $2.13 | — | $1,652.74 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+8.7; leftover $1339.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 133 | $10.02 | $2.39 | — | $317.69 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.2; leftover $1339.67 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $317.69 | ▲ close $10,709.47 vs 09:30 $10,763.74 (session +27.06) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $317.69 | ▼ 09:30 equity $10,654.78 vs yday $10,709.47 (-54.69) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 531 | $2.38 | $6.95 | $-88.14 | $1,574.52 | ▼ -88.14 after sell → book $10,647.83; vs 09:30 mark -6.95 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 280 | $4.30 | $3.67 | $-141.68 | $2,774.86 | ▼ -141.68 after sell → book $10,644.17; vs 09:30 mark -3.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 842 | $1.63 | $11.01 | $+11.81 | $4,136.30 | ▲ +11.81 after sell → book $10,633.15; vs 09:30 mark -11.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HQ` | 84 | $15.40 | $2.27 | $-46.51 | $5,427.64 | ▼ -46.51 after sell → book $10,630.89; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `EOSE` | 380 | $3.99 | $4.98 | $+168.72 | $6,938.86 | ▲ +168.72 after sell → book $10,625.91; vs 09:30 mark -4.98 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $7,979.14 | ▲ +10.73 after sell → book $10,623.89; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MLYS` | 47 | $28.03 | $2.15 | $-2.87 | $9,294.40 | ▼ -2.87 after sell → book $10,621.74; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CCOI` | 133 | $9.98 | $2.42 | $-10.13 | $10,619.32 | ▼ -10.13 after sell → book $10,619.32; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,619.32 | ▲ close $10,619.32 vs 09:30 $10,654.78 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,619.32 | ▲ 09:30 equity $10,619.32 vs yday $10,619.32 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,619.32 | ▲ close $10,619.32 vs 09:30 $10,619.32 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,619.32 | ▲ 09:30 equity $10,619.32 vs yday $10,619.32 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,619.32 | ▲ close $10,619.32 vs 09:30 $10,619.32 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,619.32 | ▲ 09:30 equity $10,619.32 vs yday $10,619.32 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 650 | $2.04 | $8.38 | — | $9,284.94 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1327.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 279 | $4.75 | $3.60 | — | $7,956.09 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1327.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 626 | $2.12 | $8.08 | — | $6,620.89 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1327.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 56 | $23.63 | $2.16 | — | $5,295.45 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-6.3; leftover $1327.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 114 | $11.55 | $2.33 | — | $3,976.42 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1327.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 8 | $157.55 | $2.01 | — | $2,714.01 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $1327.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 17 | $77.33 | $2.04 | — | $1,397.36 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+2.5; leftover $1327.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 25 | $52.55 | $2.06 | — | $81.54 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1327.42 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.54 | ▼ close $10,563.50 vs 09:30 $10,619.32 (session -25.15) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.54 | ▲ 09:30 equity $10,646.36 vs yday $10,563.50 (+82.86) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 650 | $2.01 | $8.50 | $-36.39 | $1,379.54 | ▼ -36.39 after sell → book $10,637.86; vs 09:30 mark -8.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 279 | $4.82 | $3.66 | $+12.27 | $2,720.66 | ▲ +12.27 after sell → book $10,634.20; vs 09:30 mark -3.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 626 | $2.05 | $8.19 | $-60.09 | $3,995.77 | ▼ -60.09 after sell → book $10,626.01; vs 09:30 mark -8.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 56 | $23.20 | $2.18 | $-28.42 | $5,292.79 | ▼ -28.42 after sell → book $10,623.83; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `FUBO` | 114 | $11.56 | $2.36 | $-3.55 | $6,608.27 | ▼ -3.55 after sell → book $10,621.47; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RDDT` | 8 | $160.00 | $2.03 | $+15.55 | $7,886.24 | ▲ +15.55 after sell → book $10,619.44; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIST` | 17 | $77.10 | $2.06 | $-8.01 | $9,194.88 | ▼ -8.01 after sell → book $10,617.38; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 25 | $56.90 | $2.09 | $+104.60 | $10,615.29 | ▲ +104.60 after sell → book $10,615.29; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,615.29 | ▲ close $10,615.29 vs 09:30 $10,646.36 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,615.29 | ▲ 09:30 equity $10,615.29 vs yday $10,615.29 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,615.29 | ▲ close $10,615.29 vs 09:30 $10,615.29 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,615.29 | ▲ 09:30 equity $10,615.29 vs yday $10,615.29 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 71 | $18.61 | $2.20 | — | $9,291.78 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1326.91 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 72 | $18.21 | $2.21 | — | $7,978.45 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-19.1; leftover $1326.91 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 19 | $68.79 | $2.05 | — | $6,669.39 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1326.91 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 226 | $5.87 | $2.92 | — | $5,339.86 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1326.91 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 487 | $2.72 | $6.28 | — | $4,008.94 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.4; leftover $1326.91 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 15 | $87.40 | $2.04 | — | $2,695.90 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1326.91 | — |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 34 | $38.01 | $2.09 | — | $1,401.47 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $1326.91 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 48 | $27.09 | $2.13 | — | $99.02 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1326.91 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.02 | ▲ close $10,863.08 vs 09:30 $10,615.29 (session +269.70) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.02 | ▲ 09:30 equity $11,069.62 vs yday $10,863.08 (+206.54) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `BBNX` | 71 | $22.46 | $2.23 | $+268.92 | $1,691.45 | ▲ +268.92 after sell → book $11,067.39; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ARQQ` | 72 | $19.59 | $2.23 | $+94.92 | $3,099.70 | ▲ +94.92 after sell → book $11,065.16; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 19 | $72.70 | $2.07 | $+70.17 | $4,478.93 | ▲ +70.17 after sell → book $11,063.09; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 226 | $5.58 | $2.96 | $-71.42 | $5,737.05 | ▼ -71.42 after sell → book $11,060.13; vs 09:30 mark -2.96 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QTRX` | 487 | $2.94 | $6.37 | $+94.48 | $7,162.45 | ▲ +94.48 after sell → book $11,053.75; vs 09:30 mark -6.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 15 | $83.20 | $2.06 | $-67.09 | $8,408.40 | ▼ -67.09 after sell → book $11,051.70; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `KRMN` | 34 | $37.89 | $2.11 | $-8.28 | $9,694.55 | ▼ -8.28 after sell → book $11,049.59; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 48 | $28.23 | $2.15 | $+50.43 | $11,047.43 | ▲ +50.43 after sell → book $11,047.43; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 8123 | $0.17 | $38.18 | — | $9,628.34 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $1380.93 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 87 | $15.87 | $2.25 | — | $8,245.40 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $1380.93 | — |
| 2026-09-17 09:30 ET | **BUY** | `AXTI` | 20 | $67.91 | $2.05 | — | $6,885.15 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.7; leftover $1380.93 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 53 | $25.95 | $2.15 | — | $5,507.65 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1380.93 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 8 | $170.85 | $2.01 | — | $4,138.84 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1380.93 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 575 | $2.40 | $7.42 | — | $2,751.42 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1380.93 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 76 | $18.04 | $2.22 | — | $1,378.54 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $1380.93 | — |
| 2026-09-17 09:30 ET | **BUY** | `EROC` | 108 | $12.64 | $2.31 | — | $11.11 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-3.6; leftover $1380.93 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.11 | ▼ close $10,959.49 vs 09:30 $11,069.62 (session -29.35) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.11 | ▲ 09:30 equity $11,221.31 vs yday $10,959.49 (+261.82) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `DVLT` | 8123 | $0.17 | $39.54 | $-77.72 | $1,352.48 | ▼ -77.72 after sell → book $11,181.77; vs 09:30 mark -39.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRUN` | 87 | $17.44 | $2.28 | $+132.06 | $2,867.48 | ▲ +132.06 after sell → book $11,179.49; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AXTI` | 20 | $69.72 | $2.07 | $+32.08 | $4,259.81 | ▲ +32.08 after sell → book $11,177.42; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 53 | $26.14 | $2.17 | $+5.75 | $5,643.06 | ▲ +5.75 after sell → book $11,175.25; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 8 | $182.33 | $2.04 | $+87.79 | $7,099.67 | ▲ +87.79 after sell → book $11,173.22; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 575 | $2.29 | $7.52 | $-78.19 | $8,408.89 | ▼ -78.19 after sell → book $11,165.69; vs 09:30 mark -7.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 76 | $17.80 | $2.24 | $-22.32 | $9,759.45 | ▼ -22.32 after sell → book $11,163.45; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `EROC` | 108 | $13.00 | $2.34 | $+34.22 | $11,161.11 | ▲ +34.22 after sell → book $11,161.11; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 6 | $209.52 | $2.01 | — | $9,901.98 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1395.14 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 6 | $219.62 | $2.01 | — | $8,582.25 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1395.14 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 1438 | $0.97 | $18.26 | — | $7,169.13 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1395.14 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 670 | $2.08 | $8.64 | — | $5,766.89 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $1395.14 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 353 | $3.95 | $4.55 | — | $4,367.98 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1395.14 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 99 | $14.07 | $2.29 | — | $2,972.77 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1395.14 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 239 | $5.83 | $3.08 | — | $1,576.31 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1395.14 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 389 | $3.58 | $5.02 | — | $178.68 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1395.14 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $178.68 | ▼ close $11,057.44 vs 09:30 $11,221.31 (session -57.81) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $178.68 | ▲ 09:30 equity $11,330.98 vs yday $11,057.44 (+273.54) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 6 | $210.00 | $2.03 | $-1.16 | $1,436.65 | ▼ -1.16 after sell → book $11,328.95; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 6 | $230.25 | $2.03 | $+59.74 | $2,816.12 | ▲ +59.74 after sell → book $11,326.92; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TLSA` | 1438 | $0.94 | $18.08 | $-79.48 | $4,149.76 | ▼ -79.48 after sell → book $11,308.84; vs 09:30 mark -18.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SWRD` | 670 | $2.15 | $8.77 | $+29.49 | $5,581.49 | ▲ +29.49 after sell → book $11,300.08; vs 09:30 mark -8.76 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 353 | $3.87 | $4.62 | $-37.42 | $6,942.98 | ▼ -37.42 after sell → book $11,295.45; vs 09:30 mark -4.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 99 | $13.90 | $2.31 | $-21.43 | $8,316.76 | ▼ -21.43 after sell → book $11,293.14; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BNC` | 239 | $6.42 | $3.13 | $+133.60 | $9,846.81 | ▲ +133.60 after sell → book $11,290.00; vs 09:30 mark -3.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DDD` | 389 | $3.71 | $5.09 | $+40.46 | $11,284.91 | ▲ +40.46 after sell → book $11,284.91; vs 09:30 mark -5.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 352 | $4.00 | $4.54 | — | $9,872.37 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $1410.61 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 151 | $9.31 | $2.44 | — | $8,464.12 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1410.61 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 104 | $13.47 | $2.30 | — | $7,060.41 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1410.61 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1270 | $1.11 | $16.38 | — | $5,634.33 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1410.61 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 141 | $9.99 | $2.41 | — | $4,223.33 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1410.61 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 772 | $1.82 | $9.96 | — | $2,804.47 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1410.61 | — |
| 2026-09-21 09:30 ET | **BUY** | `SGML` | 139 | $10.13 | $2.41 | — | $1,393.30 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+4.9; leftover $1410.61 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,393.30 | ▼ close $10,910.05 vs 09:30 $11,330.98 (session -334.42) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,393.30 | ▲ 09:30 equity $10,919.60 vs yday $10,910.05 (+9.55) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `CYPH` | 352 | $3.51 | $4.61 | $-181.63 | $2,624.21 | ▼ -181.63 after sell → book $10,914.99; vs 09:30 mark -4.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `ORBS` | 1270 | $1.05 | $16.60 | $-109.19 | $3,941.10 | ▼ -109.19 after sell → book $10,898.38; vs 09:30 mark -16.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 141 | $9.91 | $2.45 | $-16.14 | $5,335.97 | ▼ -16.14 after sell → book $10,895.94; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `BTBT` | 772 | $1.79 | $10.10 | $-43.22 | $6,711.61 | ▼ -43.22 after sell → book $10,885.84; vs 09:30 mark -10.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 1446 | $0.58 | $12.72 | — | $5,860.20 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $838.95 | — |
| 2026-09-22 09:30 ET | **BUY** | `ALOY` | 89 | $9.40 | $2.26 | — | $5,021.35 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+9.5; leftover $838.95 | — |
| 2026-09-22 09:30 ET | **BUY** | `GLND` | 285 | $2.94 | $3.68 | — | $4,179.77 | — | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+136.1; leftover $838.95 | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 92 | $9.11 | $2.27 | — | $3,339.38 | — | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+44.4; leftover $838.95 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 830 | $1.01 | $10.71 | — | $2,490.38 | — | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+14.3; leftover $838.95 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,490.38 | ▼ close $10,535.81 vs 09:30 $10,919.60 (session -318.40) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,490.38 | ▲ 09:30 equity $10,639.81 vs yday $10,535.81 (+104.00) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 151 | $9.50 | $2.48 | $+23.77 | $3,922.40 | ▲ +23.77 after sell → book $10,637.33; vs 09:30 mark -2.48 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 104 | $12.84 | $2.33 | $-70.67 | $5,255.43 | ▼ -70.67 after sell → book $10,635.00; vs 09:30 mark -2.33 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SGML` | 139 | $10.26 | $2.44 | $+12.53 | $6,679.13 | ▲ +12.53 after sell → book $10,632.56; vs 09:30 mark -2.44 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DEFT` | 1446 | $0.57 | $12.90 | $-32.86 | $7,497.67 | ▼ -32.86 after sell → book $10,619.65; vs 09:30 mark -12.91 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ALOY` | 89 | $8.90 | $2.28 | $-49.04 | $8,287.49 | ▼ -49.04 after sell → book $10,617.37; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLND` | 285 | $2.70 | $3.73 | $-75.81 | $9,053.26 | ▼ -75.81 after sell → book $10,613.64; vs 09:30 mark -3.73 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 92 | $8.39 | $2.29 | $-70.80 | $9,822.85 | ▼ -70.80 after sell → book $10,611.35; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 830 | $0.95 | $10.52 | $-71.03 | $10,600.82 | ▼ -71.03 after sell → book $10,600.82; vs 09:30 mark -10.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 64 | $20.65 | $2.18 | — | $9,277.04 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1325.10 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 337 | $3.93 | $4.35 | — | $7,948.28 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1325.10 | — |
| 2026-09-23 09:30 ET | **BUY** | `MAZE` | 46 | $28.30 | $2.13 | — | $6,644.36 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $1325.10 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 84 | $15.72 | $2.24 | — | $5,321.63 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1325.10 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 52 | $25.40 | $2.15 | — | $3,998.69 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $1325.10 | — |
| 2026-09-23 09:30 ET | **BUY** | `CLPT` | 85 | $15.55 | $2.25 | — | $2,674.69 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $1325.10 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 1725 | $0.77 | $18.42 | — | $1,331.47 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $1325.10 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLLN` | 11 | $116.00 | $2.02 | — | $53.45 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $1325.10 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.45 | ▼ close $10,229.92 vs 09:30 $10,639.81 (session -335.17) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.45 | ▼ 09:30 equity $10,169.70 vs yday $10,229.92 (-60.22) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 64 | $20.52 | $2.20 | $-12.71 | $1,364.52 | ▼ -12.71 after sell → book $10,167.49; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INDP` | 337 | $3.77 | $4.41 | $-62.68 | $2,630.60 | ▼ -62.68 after sell → book $10,163.08; vs 09:30 mark -4.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `MAZE` | 46 | $28.15 | $2.15 | $-11.18 | $3,923.35 | ▼ -11.18 after sell → book $10,160.93; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 84 | $14.38 | $2.27 | $-117.07 | $5,129.01 | ▼ -117.07 after sell → book $10,158.67; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `TNGX` | 52 | $23.99 | $2.17 | $-77.63 | $6,374.32 | ▼ -77.63 after sell → book $10,156.50; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CLPT` | 85 | $14.82 | $2.27 | $-66.56 | $7,631.75 | ▼ -66.56 after sell → book $10,154.23; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `NMRA` | 1725 | $0.75 | $18.34 | $-74.71 | $8,900.26 | ▼ -74.71 after sell → book $10,135.89; vs 09:30 mark -18.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLLN` | 11 | $112.33 | $2.04 | $-44.44 | $10,133.85 | ▼ -44.44 after sell → book $10,133.85; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,133.85 | ▲ close $10,133.85 vs 09:30 $10,169.70 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,879.06 | ▲ 09:30 equity $8,879.06 vs yday $8,879.06 (+0.00) | 09:30 open · cash $8,879.06 · no holdings · equity $8,879.06 vs prior close $8,879.06 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 42 | $26.27 | $2.12 | — | $7,773.60 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1109.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 13 | $83.76 | $2.03 | — | $6,682.69 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1109.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 122 | $9.05 | $2.36 | — | $5,576.24 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-27.1; leftover $1109.88 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BRVE` | 47 | $23.58 | $2.13 | — | $4,465.85 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $1109.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GLND` | 183 | $6.06 | $2.54 | — | $3,354.33 | — | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+342.1; leftover $1109.88 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 287 | $3.86 | $3.70 | — | $2,242.81 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1109.88 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 37 | $29.76 | $2.10 | — | $1,139.59 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ret5=+156.1; leftover $1109.88 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DNA` | 108 | $10.20 | $2.31 | — | $35.67 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1109.88 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.67 | ▼ close $8,603.62 vs 09:30 $8,879.06 (session -256.15) | 16:00 close · cash $35.67 · equity $8,603.62 vs 09:30 $8,879.06 (-275.44; session marks -256.15) · 8 name(s) marked open→close (per-name table). WRBY×42 09:30 $26.27 → close $26.71 +18.48; TXG×13 09:30 $83.76 → close $85.71 +25.35; AEHL×122 09:30 $9.05 → close $9.36 +37.82; BRVE×47 09:30 $23.58 → close $20.62 -139.12; GLND×183 09:30 $6.06 → close $5.54 -95.16; ZSQR×287 09:30 $3.86 → close $3.78 -22.96; TJGC×37 09:30 $29.76 → close $26.24 -130.24; DNA×108 09:30 $10.20 → close $10.66 +49.68 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
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
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `XLAB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
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
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CAN` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `USDE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TRX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IVVD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-21 | `SNDK` | cash | leftover split 1410.61 < 1 share @ 1826.00 |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SGML` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `MRAM` | no_price | no 09:30 open |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EGHT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SRFM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TDTH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `LU` | hard_red | hard-red S=-7.66 sit; no new buys |
