# Factor mine action — `probable_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-13.29%** ($8,671) · signal-only (no cash/fees) was +7.17%. Starts YES **3/30**. Fills 188 · skips 315 · realized $-769.20.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at yesterday's 'likely to keep moving' list and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: yesterday's 'likely to keep moving' list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).

### When it buys

- At 09:30, take names on yesterday's 'likely to keep moving' list that pass the must-haves.
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

- **Universe** `probable` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,025.97.

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
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $5,245.52 | — | baseline list, no extra gate; list probable; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `FOSL` | 221 | $5.64 | $2.85 | — | $3,996.23 | — | baseline list, no extra gate; list probable; 🔵; ret5=-4.1; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $2,756.51 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRS` | 370 | $3.37 | $4.77 | — | $1,504.84 | — | baseline list, no extra gate; list probable; ret5=-29.1; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 28 | $44.06 | $2.07 | — | $269.08 | — | baseline list, no extra gate; list probable; 🔵; ret5=+3.9; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $269.08 | ▲ close $9,985.46 vs 09:30 $10,000.00 (session +9.14) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $269.08 | ▲ 09:30 equity $10,059.20 vs yday $9,985.46 (+73.74) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 3 | $9.12 | $0.28 | — | $241.44 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $33.64 | — |
| 2026-08-17 09:30 ET | **BUY** | `FCEL` | 1 | $22.37 | $0.23 | — | $218.84 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=+9.5; leftover $33.64 | — |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 1 | $31.30 | $0.32 | — | $187.23 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-3.8; leftover $33.64 | — |
| 2026-08-17 09:30 ET | **BUY** | `BW` | 3 | $10.35 | $0.32 | — | $155.86 | — | baseline list, no extra gate; list probable; ⚪; ret5=+9.8; leftover $33.64 | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 1 | $18.24 | $0.19 | — | $137.43 | — | baseline list, no extra gate; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $33.64 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 2 | $16.20 | $0.33 | — | $104.70 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $33.64 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $104.70 | ▼ close $9,954.02 vs 09:30 $10,059.20 (session -103.52) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.70 | ▼ 09:30 equity $9,758.08 vs yday $9,954.02 (-195.94) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $104.70 | ▼ close $9,500.71 vs 09:30 $9,758.08 (session -257.37) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.70 | ▲ 09:30 equity $9,522.41 vs yday $9,500.71 (+21.70) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 290 | $4.79 | $3.80 | $+131.66 | $1,490.00 | ▲ +131.66 after sell → book $9,518.61; vs 09:30 mark -3.80 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `WWW` | 60 | $20.08 | $2.19 | $-35.56 | $2,692.61 | ▼ -35.56 after sell → book $9,516.42; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 299 | $3.87 | $3.92 | $-100.46 | $3,845.83 | ▼ -100.46 after sell → book $9,512.51; vs 09:30 mark -3.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `WDC` | 2 | $494.28 | $2.02 | $-22.45 | $4,832.37 | ▼ -22.45 after sell → book $9,510.49; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `FOSL` | 221 | $5.54 | $2.90 | $-27.85 | $6,053.81 | ▼ -27.85 after sell → book $9,507.59; vs 09:30 mark -2.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ADUR` | 75 | $15.65 | $2.24 | $-68.20 | $7,225.32 | ▼ -68.20 after sell → book $9,505.35; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRS` | 370 | $2.71 | $4.84 | $-253.82 | $8,223.18 | ▼ -253.82 after sell → book $9,500.51; vs 09:30 mark -4.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ALGM` | 28 | $40.00 | $2.09 | $-117.85 | $9,341.09 | ▼ -117.85 after sell → book $9,498.42; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,341.09 | ▼ close $9,495.16 vs 09:30 $9,522.41 (session -3.26) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,341.09 | ▼ 09:30 equity $9,493.85 vs yday $9,495.16 (-1.31) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `ABX` | 3 | $9.13 | $0.30 | $-0.56 | $9,368.17 | ▼ -0.56 after sell → book $9,493.55; vs 09:30 mark -0.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `FCEL` | 1 | $20.21 | $0.23 | $-2.61 | $9,388.16 | ▼ -2.61 after sell → book $9,493.32; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `VERA` | 1 | $32.30 | $0.35 | $+0.33 | $9,420.11 | ▲ +0.33 after sell → book $9,492.98; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `BW` | 3 | $9.05 | $0.30 | $-4.52 | $9,446.96 | ▼ -4.52 after sell → book $9,492.68; vs 09:30 mark -0.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `OCC` | 1 | $14.10 | $0.16 | $-4.49 | $9,460.89 | ▼ -4.49 after sell → book $9,492.51; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ALM` | 2 | $15.81 | $0.34 | $-1.45 | $9,492.17 | ▼ -1.45 after sell → book $9,492.17; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 159 | $7.44 | $2.47 | — | $8,306.74 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1186.52 | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 159 | $7.45 | $2.47 | — | $7,119.73 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1186.52 | — |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 10 | $113.23 | $2.02 | — | $5,985.41 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1186.52 | — |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 110 | $10.77 | $2.32 | — | $4,798.39 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1186.52 | — |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 125 | $9.46 | $2.37 | — | $3,613.52 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1186.52 | — |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 141 | $8.38 | $2.41 | — | $2,429.53 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1186.52 | — |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 40 | $29.20 | $2.11 | — | $1,259.42 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1186.52 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRCL` | 14 | $82.99 | $2.03 | — | $95.53 | — | baseline list, no extra gate; list probable; 🔵; ⚪; ret5=+7.4; leftover $1186.52 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.53 | ▲ close $9,600.78 vs 09:30 $9,493.85 (session +126.80) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.53 | ▲ 09:30 equity $9,959.70 vs yday $9,600.78 (+358.92) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 7 | $1.66 | $0.14 | — | $83.77 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $11.94 | — |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 6 | $1.71 | $0.12 | — | $73.39 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $11.94 | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 13 | $0.86 | $0.15 | — | $62.01 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $11.94 | — |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 3 | $3.11 | $0.10 | — | $52.57 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+7.1; leftover $11.94 | — |
| 2026-08-21 09:30 ET | **BUY** | `QTRX` | 3 | $3.11 | $0.10 | — | $43.14 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $11.94 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.14 | ▼ close $9,892.23 vs 09:30 $9,959.70 (session -66.85) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.14 | ▲ 09:30 equity $9,977.32 vs yday $9,892.23 (+85.09) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.14 | ▼ close $9,888.83 vs 09:30 $9,977.32 (session -88.49) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.14 | ▼ 09:30 equity $9,772.80 vs yday $9,888.83 (-116.03) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `MRVI` | 159 | $8.53 | $2.50 | $+168.34 | $1,396.91 | ▲ +168.34 after sell → book $9,770.30; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DNA` | 159 | $6.94 | $2.50 | $-86.06 | $2,497.86 | ▼ -86.06 after sell → book $9,767.79; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MSTR` | 10 | $119.11 | $2.04 | $+54.74 | $3,686.92 | ▲ +54.74 after sell → book $9,765.75; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EXK` | 110 | $10.44 | $2.35 | $-40.97 | $4,832.98 | ▼ -40.97 after sell → book $9,763.41; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SCZM` | 125 | $9.45 | $2.40 | $-6.01 | $6,011.83 | ▼ -6.01 after sell → book $9,761.01; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NG` | 141 | $9.31 | $2.45 | $+126.27 | $7,322.09 | ▲ +126.27 after sell → book $9,758.56; vs 09:30 mark -2.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BLSH` | 40 | $30.00 | $2.13 | $+27.76 | $8,519.96 | ▲ +27.76 after sell → book $9,756.43; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRCL` | 14 | $84.73 | $2.05 | $+20.28 | $9,704.13 | ▲ +20.28 after sell → book $9,754.38; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 167 | $7.25 | $2.49 | — | $8,490.89 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1213.02 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3388 | $0.36 | $22.29 | — | $7,255.69 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-15.6; leftover $1213.02 | — |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 109 | $11.12 | $2.32 | — | $6,041.30 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.7; leftover $1213.02 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 89 | $13.59 | $2.26 | — | $4,829.53 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1213.02 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 127 | $9.49 | $2.37 | — | $3,621.93 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1213.02 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 32 | $36.96 | $2.09 | — | $2,437.12 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1213.02 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 266 | $4.55 | $3.43 | — | $1,223.39 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1213.02 | — |
| 2026-08-25 09:30 ET | **BUY** | `ADIG` | 55 | $21.79 | $2.15 | — | $22.79 | — | baseline list, no extra gate; list probable; 🔵; ret5=+3.1; leftover $1213.02 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.79 | ▲ close $9,948.92 vs 09:30 $9,772.80 (session +233.94) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.79 | ▼ 09:30 equity $9,893.23 vs yday $9,948.92 (-55.69) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `BTBT` | 7 | $1.53 | $0.15 | $-1.20 | $33.35 | ▼ -1.20 after sell → book $9,893.08; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ENHA` | 6 | $1.63 | $0.14 | $-0.74 | $42.99 | ▼ -0.74 after sell → book $9,892.94; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ORBS` | 13 | $0.80 | $0.16 | $-1.20 | $53.18 | ▼ -1.20 after sell → book $9,892.78; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 3 | $3.77 | $0.14 | $+1.74 | $64.34 | ▲ +1.74 after sell → book $9,892.64; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `QTRX` | 3 | $2.83 | $0.11 | $-1.06 | $72.72 | ▼ -1.06 after sell → book $9,892.52; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.72 | ▲ close $10,160.19 vs 09:30 $9,893.23 (session +267.66) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.72 | ▼ 09:30 equity $10,140.78 vs yday $10,160.19 (-19.41) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 1 | $9.68 | $0.10 | — | $62.94 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $10.39 | — |
| 2026-08-27 09:30 ET | **BUY** | `SENS` | 1 | $9.33 | $0.10 | — | $53.51 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.5; leftover $10.39 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.51 | ▲ close $10,148.63 vs 09:30 $10,140.78 (session +8.05) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.51 | ▼ 09:30 equity $10,071.45 vs yday $10,148.63 (-77.18) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 167 | $9.73 | $2.53 | $+409.14 | $1,675.89 | ▲ +409.14 after sell → book $10,068.92; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `VITL` | 109 | $10.47 | $2.35 | $-75.51 | $2,814.78 | ▼ -75.51 after sell → book $10,066.58; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 89 | $13.05 | $2.28 | $-52.60 | $3,973.95 | ▼ -52.60 after sell → book $10,064.30; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CCOI` | 127 | $9.70 | $2.40 | $+21.90 | $5,203.44 | ▲ +21.90 after sell → book $10,061.89; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 32 | $39.60 | $2.11 | $+80.29 | $6,468.54 | ▲ +80.29 after sell → book $10,059.79; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZIP` | 266 | $4.21 | $3.49 | $-97.36 | $7,584.91 | ▼ -97.36 after sell → book $10,056.30; vs 09:30 mark -3.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ADIG` | 55 | $22.10 | $2.17 | $+12.72 | $8,798.24 | ▲ +12.72 after sell → book $10,054.13; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 38 | $32.90 | $2.10 | — | $7,545.93 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1256.89 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 80 | $15.66 | $2.23 | — | $6,290.90 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1256.89 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 15 | $79.42 | $2.04 | — | $5,097.57 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1256.89 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 378 | $3.32 | $4.88 | — | $3,837.73 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.4; leftover $1256.89 | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 4 | $252.24 | $2.00 | — | $2,826.77 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1256.89 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 145 | $8.61 | $2.42 | — | $1,575.89 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.7; leftover $1256.89 | — |
| 2026-08-28 09:30 ET | **BUY** | `XPOF` | 233 | $5.38 | $3.01 | — | $319.35 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.5; leftover $1256.89 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $319.35 | ▼ close $9,822.21 vs 09:30 $10,071.45 (session -213.24) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $319.35 | ▼ 09:30 equity $9,794.75 vs yday $9,822.21 (-27.46) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SAFX` | 3388 | $0.36 | $23.00 | $-31.74 | $1,522.80 | ▼ -31.74 after sell → book $9,771.74; vs 09:30 mark -23.01 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,522.80 | ▲ close $9,848.26 vs 09:30 $9,794.75 (session +76.52) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,522.80 | ▼ 09:30 equity $9,711.51 vs yday $9,848.26 (-136.75) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `ABX` | 1 | $9.43 | $0.12 | $-0.47 | $1,532.12 | ▼ -0.47 after sell → book $9,711.40; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SENS` | 1 | $9.17 | $0.11 | $-0.37 | $1,541.17 | ▼ -0.37 after sell → book $9,711.28; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,541.17 | ▼ close $9,569.65 vs 09:30 $9,711.51 (session -141.63) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,541.17 | ▼ 09:30 equity $9,539.51 vs yday $9,569.65 (-30.14) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 38 | $32.42 | $2.12 | $-22.47 | $2,771.01 | ▼ -22.47 after sell → book $9,537.39; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 80 | $13.92 | $2.25 | $-143.68 | $3,882.35 | ▼ -143.68 after sell → book $9,535.13; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 15 | $78.84 | $2.06 | $-12.79 | $5,062.90 | ▼ -12.79 after sell → book $9,533.08; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `PYXS` | 378 | $3.45 | $4.95 | $+39.31 | $6,362.05 | ▲ +39.31 after sell → book $9,528.13; vs 09:30 mark -4.95 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SIMO` | 4 | $235.71 | $2.02 | $-70.14 | $7,302.87 | ▼ -70.14 after sell → book $9,526.11; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `OPTX` | 145 | $7.25 | $2.46 | $-202.08 | $8,351.66 | ▼ -202.08 after sell → book $9,523.65; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `XPOF` | 233 | $5.03 | $3.05 | $-87.61 | $9,520.60 | ▼ -87.61 after sell → book $9,520.60; vs 09:30 mark -3.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,520.60 | ▲ close $9,520.60 vs 09:30 $9,539.51 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,520.60 | ▲ 09:30 equity $9,520.60 vs yday $9,520.60 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 77 | $15.45 | $2.22 | — | $8,328.72 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1190.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $7,159.15 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1190.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 70 | $16.77 | $2.20 | — | $5,983.05 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1190.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 80 | $14.85 | $2.23 | — | $4,792.82 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1190.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 21 | $55.42 | $2.05 | — | $3,626.95 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-25.9; leftover $1190.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 545 | $2.18 | $7.03 | — | $2,431.82 | — | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1190.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 85 | $13.96 | $2.25 | — | $1,242.97 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-6.4; leftover $1190.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `SAFX` | 3156 | $0.38 | $21.37 | — | $31.79 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-2.3; leftover $1190.07 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $31.79 | ▼ close $9,442.08 vs 09:30 $9,520.60 (session -37.16) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.79 | ▲ 09:30 equity $9,452.26 vs yday $9,442.08 (+10.18) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 1 | $2.52 | $0.03 | — | $29.25 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $3.97 | — |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 1 | $3.52 | $0.04 | — | $25.69 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $3.97 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.69 | ▲ close $9,596.73 vs 09:30 $9,452.26 (session +144.53) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.69 | ▼ 09:30 equity $9,591.73 vs yday $9,596.73 (-5.00) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.69 | ▲ close $9,613.01 vs 09:30 $9,591.73 (session +21.28) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.69 | ▼ 09:30 equity $9,558.51 vs yday $9,613.01 (-54.50) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 77 | $15.16 | $2.24 | $-26.79 | $1,190.76 | ▼ -26.79 after sell → book $9,556.26; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 8 | $140.29 | $2.03 | $-49.25 | $2,311.09 | ▼ -49.25 after sell → book $9,554.23; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 70 | $15.46 | $2.22 | $-96.12 | $3,391.07 | ▼ -96.12 after sell → book $9,552.01; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SLN` | 80 | $13.60 | $2.25 | $-104.48 | $4,476.81 | ▼ -104.48 after sell → book $9,549.75; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `EIX` | 21 | $59.49 | $2.07 | $+81.34 | $5,724.03 | ▲ +81.34 after sell → book $9,547.68; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 545 | $2.22 | $7.13 | $+7.64 | $6,926.80 | ▲ +7.64 after sell → book $9,540.55; vs 09:30 mark -7.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CLYM` | 85 | $15.82 | $2.27 | $+153.59 | $8,269.23 | ▲ +153.59 after sell → book $9,538.28; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SAFX` | 3156 | $0.40 | $22.63 | $+28.60 | $9,509.00 | ▲ +28.60 after sell → book $9,515.65; vs 09:30 mark -22.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,509.00 | ▼ close $9,515.42 vs 09:30 $9,558.51 (session -0.23) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,509.00 | ▼ 09:30 equity $9,515.19 vs yday $9,515.42 (-0.23) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 1 | $2.22 | $0.05 | $-0.37 | $9,511.18 | ▼ -0.37 after sell → book $9,515.14; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `EOSE` | 1 | $3.96 | $0.06 | $+0.34 | $9,515.08 | ▲ +0.34 after sell → book $9,515.08; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,515.08 | ▲ close $9,515.08 vs 09:30 $9,515.19 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,515.08 | ▲ 09:30 equity $9,515.08 vs yday $9,515.08 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 583 | $2.04 | $7.52 | — | $8,318.24 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1189.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 250 | $4.75 | $3.23 | — | $7,127.52 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1189.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 561 | $2.12 | $7.24 | — | $5,930.96 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1189.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 50 | $23.63 | $2.14 | — | $4,747.32 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-6.3; leftover $1189.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 102 | $11.55 | $2.30 | — | $3,566.92 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1189.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 7 | $157.55 | $2.01 | — | $2,462.06 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $1189.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 15 | $77.33 | $2.04 | — | $1,300.08 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+2.5; leftover $1189.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 22 | $52.55 | $2.06 | — | $141.92 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1189.39 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $141.92 | ▼ close $9,462.77 vs 09:30 $9,515.08 (session -23.79) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $141.92 | ▲ 09:30 equity $9,536.22 vs yday $9,462.77 (+73.45) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $141.92 | ▼ close $9,505.77 vs 09:30 $9,536.22 (session -30.45) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $141.92 | ▼ 09:30 equity $9,439.85 vs yday $9,505.77 (-65.92) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $141.92 | ▼ close $9,377.44 vs 09:30 $9,439.85 (session -62.41) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $141.92 | ▼ 09:30 equity $9,178.82 vs yday $9,377.44 (-198.62) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 583 | $1.89 | $7.63 | $-102.60 | $1,236.16 | ▼ -102.60 after sell → book $9,171.19; vs 09:30 mark -7.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 250 | $4.73 | $3.28 | $-11.50 | $2,415.39 | ▼ -11.50 after sell → book $9,167.92; vs 09:30 mark -3.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAK` | 561 | $1.84 | $7.34 | $-171.66 | $3,440.29 | ▼ -171.66 after sell → book $9,160.58; vs 09:30 mark -7.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `TYRA` | 50 | $25.58 | $2.16 | $+93.20 | $4,717.13 | ▲ +93.20 after sell → book $9,158.42; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `FUBO` | 102 | $10.75 | $2.32 | $-86.22 | $5,811.30 | ▼ -86.22 after sell → book $9,156.09; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RDDT` | 7 | $160.62 | $2.03 | $+17.45 | $6,933.61 | ▲ +17.45 after sell → book $9,154.06; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `VIST` | 15 | $76.75 | $2.06 | $-12.79 | $8,082.81 | ▼ -12.79 after sell → book $9,152.01; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAND` | 22 | $48.60 | $2.08 | $-91.03 | $9,149.93 | ▼ -91.03 after sell → book $9,149.93; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 61 | $18.61 | $2.17 | — | $8,012.55 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1143.74 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 62 | $18.21 | $2.18 | — | $6,881.35 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-19.1; leftover $1143.74 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 16 | $68.79 | $2.04 | — | $5,778.67 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1143.74 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 194 | $5.87 | $2.57 | — | $4,637.32 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1143.74 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 420 | $2.72 | $5.42 | — | $3,489.50 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.4; leftover $1143.74 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 13 | $87.40 | $2.03 | — | $2,351.28 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1143.74 | — |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 30 | $38.01 | $2.08 | — | $1,208.90 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $1143.74 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 42 | $27.09 | $2.12 | — | $69.00 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1143.74 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $69.00 | ▲ close $9,360.18 vs 09:30 $9,178.82 (session +230.85) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.00 | ▲ 09:30 equity $9,538.12 vs yday $9,360.18 (+177.94) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 50 | $0.17 | $0.23 | — | $60.26 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $8.62 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 3 | $2.40 | $0.08 | — | $52.98 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $8.62 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.98 | ▲ close $9,628.86 vs 09:30 $9,538.12 (session +91.06) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.98 | ▲ 09:30 equity $9,694.27 vs yday $9,628.86 (+65.41) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 6 | $0.97 | $0.08 | — | $47.09 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $6.62 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 3 | $2.08 | $0.07 | — | $40.78 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $6.62 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 1 | $3.95 | $0.04 | — | $36.78 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $6.62 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 1 | $5.83 | $0.06 | — | $30.89 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $6.62 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 1 | $3.58 | $0.04 | — | $27.27 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $6.62 | — |
| 2026-09-18 09:30 ET | **BUY** | `RANI` | 7 | $0.85 | $0.08 | — | $21.24 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+3.6; leftover $6.62 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.24 | ▼ close $9,642.28 vs 09:30 $9,694.27 (session -51.63) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.24 | ▲ 09:30 equity $9,740.09 vs yday $9,642.28 (+97.81) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `BBNX` | 61 | $22.11 | $2.19 | $+209.13 | $1,367.76 | ▲ +209.13 after sell → book $9,737.89; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARQQ` | 62 | $20.55 | $2.20 | $+140.71 | $2,639.66 | ▲ +140.71 after sell → book $9,735.70; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 16 | $79.08 | $2.06 | $+160.54 | $3,902.88 | ▲ +160.54 after sell → book $9,733.64; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 194 | $5.62 | $2.61 | $-53.69 | $4,990.55 | ▼ -53.69 after sell → book $9,731.02; vs 09:30 mark -2.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QTRX` | 420 | $3.13 | $5.50 | $+161.28 | $6,299.65 | ▲ +161.28 after sell → book $9,725.53; vs 09:30 mark -5.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `VAL` | 13 | $83.46 | $2.05 | $-55.30 | $7,382.58 | ▼ -55.30 after sell → book $9,723.48; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `KRMN` | 30 | $36.30 | $2.10 | $-55.48 | $8,469.48 | ▼ -55.48 after sell → book $9,721.38; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ADPT` | 42 | $28.69 | $2.14 | $+62.95 | $9,672.33 | ▲ +62.95 after sell → book $9,719.24; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 129 | $9.31 | $2.38 | — | $8,468.96 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1209.04 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 89 | $13.47 | $2.26 | — | $7,267.43 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1209.04 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1089 | $1.11 | $14.05 | — | $6,044.59 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1209.04 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 121 | $9.99 | $2.35 | — | $4,833.45 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1209.04 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 662 | $1.82 | $8.54 | — | $3,616.76 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1209.04 | — |
| 2026-09-21 09:30 ET | **BUY** | `SGML` | 119 | $10.13 | $2.35 | — | $2,408.35 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+4.9; leftover $1209.04 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 46 | $25.95 | $2.13 | — | $1,212.52 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1209.04 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,212.52 | ▼ close $9,584.21 vs 09:30 $9,740.09 (session -100.97) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,212.52 | ▼ 09:30 equity $9,558.92 vs yday $9,584.21 (-25.29) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `DVLT` | 50 | $0.16 | $0.25 | $-0.98 | $1,220.27 | ▼ -0.98 after sell → book $9,558.67; vs 09:30 mark -0.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 262 | $0.58 | $2.31 | — | $1,066.00 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $152.53 | — |
| 2026-09-22 09:30 ET | **BUY** | `ALOY` | 16 | $9.40 | $1.55 | — | $914.05 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+9.5; leftover $152.53 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $914.05 | ▲ close $9,748.26 vs 09:30 $9,558.92 (session +193.45) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $914.05 | ▲ 09:30 equity $9,757.85 vs yday $9,748.26 (+9.59) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `SABR` | 3 | $2.24 | $0.10 | $-0.66 | $920.67 | ▼ -0.66 after sell → book $9,757.75; vs 09:30 mark -0.10 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TLSA` | 6 | $0.89 | $0.09 | $-0.65 | $925.92 | ▼ -0.65 after sell → book $9,757.66; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SWRD` | 3 | $2.16 | $0.09 | $+0.07 | $932.31 | ▲ +0.07 after sell → book $9,757.57; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `EYPT` | 1 | $4.10 | $0.06 | $+0.04 | $936.34 | ▲ +0.04 after sell → book $9,757.50; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BNC` | 1 | $6.29 | $0.09 | $+0.31 | $942.55 | ▲ +0.31 after sell → book $9,757.42; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `DDD` | 1 | $3.59 | $0.06 | $-0.09 | $946.08 | ▼ -0.09 after sell → book $9,757.36; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RANI` | 7 | $0.81 | $0.10 | $-0.46 | $951.65 | ▼ -0.46 after sell → book $9,757.26; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 5 | $20.65 | $1.05 | — | $847.35 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $118.96 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 30 | $3.93 | $1.27 | — | $728.19 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $118.96 | — |
| 2026-09-23 09:30 ET | **BUY** | `MAZE` | 4 | $28.30 | $1.14 | — | $613.84 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $118.96 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 7 | $15.72 | $1.12 | — | $502.68 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $118.96 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 4 | $25.40 | $1.03 | — | $400.05 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $118.96 | — |
| 2026-09-23 09:30 ET | **BUY** | `CLPT` | 7 | $15.55 | $1.11 | — | $290.09 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $118.96 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 154 | $0.77 | $1.64 | — | $170.18 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $118.96 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLLN` | 1 | $116.00 | $1.16 | — | $53.01 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $118.96 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.01 | ▼ close $9,330.47 vs 09:30 $9,757.85 (session -417.26) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.01 | ▼ 09:30 equity $9,186.76 vs yday $9,330.47 (-143.71) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 129 | $8.67 | $2.41 | $-87.35 | $1,169.03 | ▼ -87.35 after sell → book $9,184.35; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 89 | $12.26 | $2.28 | $-112.67 | $2,257.89 | ▼ -112.67 after sell → book $9,182.07; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ORBS` | 1089 | $1.05 | $14.24 | $-93.63 | $3,387.10 | ▼ -93.63 after sell → book $9,167.83; vs 09:30 mark -14.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SBET` | 121 | $9.80 | $2.38 | $-27.73 | $4,570.52 | ▼ -27.73 after sell → book $9,165.45; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTBT` | 662 | $1.73 | $8.66 | $-83.40 | $5,703.81 | ▼ -83.40 after sell → book $9,156.79; vs 09:30 mark -8.66 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGML` | 119 | $9.89 | $2.38 | $-33.88 | $6,878.34 | ▼ -33.88 after sell → book $9,154.41; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GLXY` | 46 | $25.00 | $2.15 | $-48.21 | $8,025.97 | ▼ -48.21 after sell → book $9,152.26; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,025.97 | ▲ close $9,160.53 vs 09:30 $9,186.76 (session +8.26) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,642.07 | ▲ 09:30 equity $8,719.95 vs yday $8,719.95 (+0.00) | 09:30 open · cash $7,642.07 (unchanged overnight, no fees) · equity $8,719.95 vs prior close $8,719.95 (+0.00) · 12 name(s) re-marked at the open (per-name table). ALOY×15 yday $8.52 → 09:30 $8.52 +0.00; APPS×11 yday $10.88 → 09:30 $10.88 +0.00; ARHS×16 yday $9.47 → 09:30 $9.47 +0.00; BTQ×51 yday $2.79 → 09:30 $2.79 +0.00; DEFT×247 yday $0.53 → 09:30 $0.53 +0.00; FJET×72 yday $1.80 → 09:30 $1.80 +0.00; GT×1 yday $5.07 → 09:30 $5.07 +0.00; HELP×10 yday $12.59 → 09:30 $12.59 +0.00; INDP×1 yday $4.00 → 09:30 $4.00 +0.00; NMRA×9 yday $0.70 → 09:30 $0.70 +0.00; NN×9 yday $14.45 → 09:30 $14.45 +0.00; TLYS×1 yday $4.24 → 09:30 $4.24 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 36 | $26.27 | $2.10 | — | $6,694.25 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $955.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 11 | $83.76 | $2.02 | — | $5,770.87 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $955.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 105 | $9.05 | $2.31 | — | $4,818.31 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-27.1; leftover $955.26 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BRVE` | 40 | $23.58 | $2.11 | — | $3,873.00 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $955.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 434 | $2.20 | $5.60 | — | $2,912.61 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $955.26 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 159 | $6.00 | $2.47 | — | $1,956.14 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $955.26 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 53 | $17.91 | $2.15 | — | $1,004.76 | — | baseline list, no extra gate; list probable; 🔵; ret5=+3.7; leftover $955.26 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 11 | $83.69 | $2.02 | — | $82.09 | — | baseline list, no extra gate; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $955.26 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.09 | ▼ close $8,671.01 vs 09:30 $8,719.95 (session -28.16) | 16:00 close · cash $82.09 · equity $8,671.01 vs 09:30 $8,719.95 (-48.94; session marks -28.16) · 20 name(s) marked open→close (per-name table). ALOY×15 09:30 $8.52 → close $8.52 +0.00; APPS×11 09:30 $10.88 → close $10.88 +0.00; ARHS×16 09:30 $9.47 → close $9.47 +0.00; BTQ×51 09:30 $2.79 → close $2.79 -0.00; DEFT×247 09:30 $0.53 → close $0.53 +0.00; FJET×72 09:30 $1.80 → close $1.80 -0.00; GT×1 09:30 $5.07 → close $5.07 +0.00; HELP×10 09:30 $12.59 → close $12.59 +0.00; INDP×1 09:30 $4.00 → close $4.00 +0.00; NMRA×9 09:30 $0.70 → close $0.70 +0.00; NN×9 09:30 $14.45 → close $14.45 -0.00; TLYS×1 09:30 $4.24 → close $4.24 -0.00; WRBY×36 09:30 $26.27 → close $26.71 +15.84; TXG×11 09:30 $83.76 → close $85.71 +21.45; AEHL×105 09:30 $9.05 → close $9.36 +32.55; BRVE×40 09:30 $23.58 → close $20.62 -118.40; HLP×434 09:30 $2.20 → close $2.21 +4.34; SATL×159 09:30 $6.00 → close $6.17 +27.03; PL×53 09:30 $17.91 → close $17.43 -25.44; TEM×11 09:30 $83.69 → close $85.01 +14.47 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `WWW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `WDC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `FOSL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ALGM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CDNL` | cash | leftover split 33.64 < 1 share @ 39.85 |
| 2026-08-17 | `CELC` | cash | leftover split 33.64 < 1 share @ 92.99 |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `WWW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `WDC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `FOSL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ALGM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ABX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `FCEL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `VERA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OCC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CBRS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COHR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDTH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `FCEL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `VERA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OCC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ALM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `MRVI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MSTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BLSH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CRCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DE` | cash | leftover split 11.94 < 1 share @ 623.26 |
| 2026-08-21 | `QDEL` | cash | leftover split 11.94 < 1 share @ 14.96 |
| 2026-08-21 | `CF` | cash | leftover split 11.94 < 1 share @ 127.43 |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BLSH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CRCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ENHA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ENHA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `VITL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CCOI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZIP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ADIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AVBP` | cash | leftover split 9.09 < 1 share @ 31.21 |
| 2026-08-26 | `FLNC` | cash | leftover split 9.09 < 1 share @ 11.12 |
| 2026-08-26 | `ABX` | cash | leftover split 9.09 < 1 share @ 9.83 |
| 2026-08-26 | `AVEX` | cash | leftover split 9.09 < 1 share @ 17.51 |
| 2026-08-26 | `ITG` | cash | leftover split 9.09 < 1 share @ 12.04 |
| 2026-08-26 | `SENS` | cash | leftover split 9.09 < 1 share @ 9.48 |
| 2026-08-26 | `BE` | cash | leftover split 9.09 < 1 share @ 213.94 |
| 2026-08-26 | `AXTI` | cash | leftover split 9.09 < 1 share @ 65.34 |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `VITL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CCOI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZIP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ADIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AVBP` | cash | leftover split 10.39 < 1 share @ 30.79 |
| 2026-08-27 | `FLNC` | cash | leftover split 10.39 < 1 share @ 11.52 |
| 2026-08-27 | `AVEX` | cash | leftover split 10.39 < 1 share @ 18.43 |
| 2026-08-27 | `ITG` | cash | leftover split 10.39 < 1 share @ 12.36 |
| 2026-08-27 | `BE` | cash | leftover split 10.39 < 1 share @ 227.10 |
| 2026-08-28 | `ABX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SENS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `OABI` | cash | leftover split 3.97 < 1 share @ 4.78 |
| 2026-09-04 | `HQ` | cash | leftover split 3.97 < 1 share @ 15.90 |
| 2026-09-04 | `DELL` | cash | leftover split 3.97 < 1 share @ 513.78 |
| 2026-09-04 | `MLYS` | cash | leftover split 3.97 < 1 share @ 28.00 |
| 2026-09-04 | `CCOI` | cash | leftover split 3.97 < 1 share @ 10.02 |
| 2026-09-04 | `UAMY` | cash | leftover split 3.97 < 1 share @ 5.25 |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `EIX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CLYM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-09-10 | `LAC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XHG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
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
| 2026-09-17 | `BRUN` | cash | leftover split 8.62 < 1 share @ 15.87 |
| 2026-09-17 | `AXTI` | cash | leftover split 8.62 < 1 share @ 67.91 |
| 2026-09-17 | `ARQT` | cash | leftover split 8.62 < 1 share @ 25.95 |
| 2026-09-17 | `SMTC` | cash | leftover split 8.62 < 1 share @ 170.85 |
| 2026-09-17 | `CIFR` | cash | leftover split 8.62 < 1 share @ 18.04 |
| 2026-09-17 | `EROC` | cash | leftover split 8.62 < 1 share @ 12.64 |
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
| 2026-09-18 | `BHVN` | cash | leftover split 6.62 < 1 share @ 14.07 |
| 2026-09-18 | `RARE` | cash | leftover split 6.62 < 1 share @ 14.79 |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SWRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `DDD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RANI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SNDK` | cash | leftover split 1209.04 < 1 share @ 1826.00 |
| 2026-09-22 | `SABR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `SWRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `DDD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RANI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SGML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GLXY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `MRAM` | no_price | no 09:30 open |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SGML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GLXY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `ALOY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `ALOY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `MAZE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `TNGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CLPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `NMRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BLLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| `DEFT` | 262 | 2026-09-22 @ $0.58 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $152.53 |
| `ALOY` | 16 | 2026-09-22 @ $9.40 | baseline list, no extra gate; list probable,yday_gainer; ret5=+9.5; leftover $152.53 |
| `OMER` | 5 | 2026-09-23 @ $20.65 | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $118.96 |
| `INDP` | 30 | 2026-09-23 @ $3.93 | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $118.96 |
| `MAZE` | 4 | 2026-09-23 @ $28.30 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $118.96 |
| `SGRY` | 7 | 2026-09-23 @ $15.72 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $118.96 |
| `TNGX` | 4 | 2026-09-23 @ $25.40 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $118.96 |
| `CLPT` | 7 | 2026-09-23 @ $15.55 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $118.96 |
| `NMRA` | 154 | 2026-09-23 @ $0.77 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $118.96 |
| `BLLN` | 1 | 2026-09-23 @ $116.00 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $118.96 |
