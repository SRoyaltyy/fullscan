# Factor mine action — `probable_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-7.25%** ($9,275) · signal-only (no cash/fees) was -15.54%. Starts YES **4/30**. Fills 181 · skips 495 · realized $-1482.45.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at yesterday's 'likely to keep moving' list and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `probable` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $210.04.

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
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $104.70 | ▼ close $9,390.53 vs 09:30 $9,522.41 (session -131.89) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.70 | ▼ 09:30 equity $9,303.97 vs yday $9,390.53 (-86.56) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 1 | $7.44 | $0.08 | — | $97.19 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $13.09 | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 1 | $7.45 | $0.08 | — | $89.66 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $13.09 | — |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 1 | $10.77 | $0.11 | — | $78.78 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $13.09 | — |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 1 | $9.46 | $0.10 | — | $69.22 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $13.09 | — |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 1 | $8.38 | $0.09 | — | $60.75 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $13.09 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.75 | ▼ close $9,148.46 vs 09:30 $9,303.97 (session -155.06) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.75 | ▲ 09:30 equity $9,246.31 vs yday $9,148.46 (+97.85) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 290 | $4.43 | $3.80 | $+27.26 | $1,341.65 | ▲ +27.26 after sell → book $9,242.51; vs 09:30 mark -3.80 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `WWW` | 60 | $20.32 | $2.19 | $-21.16 | $2,558.66 | ▼ -21.16 after sell → book $9,240.32; vs 09:30 mark -2.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 299 | $3.42 | $3.92 | $-235.01 | $3,577.33 | ▼ -235.01 after sell → book $9,236.41; vs 09:30 mark -3.91 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `WDC` | 2 | $477.27 | $2.02 | $-56.47 | $4,529.85 | ▼ -56.47 after sell → book $9,234.39; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `FOSL` | 221 | $5.65 | $2.90 | $-3.54 | $5,775.60 | ▼ -3.54 after sell → book $9,231.49; vs 09:30 mark -2.90 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ADUR` | 75 | $16.00 | $2.24 | $-41.95 | $6,973.37 | ▼ -41.95 after sell → book $9,229.26; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `AIRS` | 370 | $2.71 | $4.84 | $-253.82 | $7,971.22 | ▼ -253.82 after sell → book $9,224.41; vs 09:30 mark -4.85 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALGM` | 28 | $37.62 | $2.09 | $-184.35 | $9,022.63 | ▼ -184.35 after sell → book $9,222.32; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 679 | $1.66 | $8.76 | — | $7,886.73 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1127.83 | — |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 659 | $1.71 | $8.50 | — | $6,751.34 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $1127.83 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $6,126.08 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1127.83 | — |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 75 | $14.96 | $2.21 | — | $5,001.87 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-1.6; leftover $1127.83 | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1305 | $0.86 | $15.19 | — | $3,859.16 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1127.83 | — |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 362 | $3.11 | $4.67 | — | $2,728.67 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+7.1; leftover $1127.83 | — |
| 2026-08-21 09:30 ET | **BUY** | `QTRX` | 362 | $3.11 | $4.67 | — | $1,598.18 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $1127.83 | — |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 8 | $127.43 | $2.01 | — | $576.73 | — | baseline list, no extra gate; list probable; 🔵; ⚪; ret5=+7.9; leftover $1127.83 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $576.73 | ▼ close $9,125.97 vs 09:30 $9,246.31 (session -48.34) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $576.73 | ▲ 09:30 equity $9,177.65 vs yday $9,125.97 (+51.68) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `ABX` | 3 | $9.76 | $0.32 | $+1.32 | $605.68 | ▲ +1.32 after sell → book $9,177.33; vs 09:30 mark -0.32 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `FCEL` | 1 | $18.69 | $0.21 | $-4.12 | $624.16 | ▼ -4.12 after sell → book $9,177.12; vs 09:30 mark -0.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `VERA` | 1 | $32.25 | $0.35 | $+0.29 | $656.07 | ▲ +0.29 after sell → book $9,176.77; vs 09:30 mark -0.35 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `BW` | 3 | $8.15 | $0.27 | $-7.19 | $680.24 | ▼ -7.19 after sell → book $9,176.50; vs 09:30 mark -0.27 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `OCC` | 1 | $13.60 | $0.16 | $-4.98 | $693.69 | ▼ -4.98 after sell → book $9,176.34; vs 09:30 mark -0.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `ALM` | 2 | $18.70 | $0.40 | $+4.27 | $730.69 | ▲ +4.27 after sell → book $9,175.94; vs 09:30 mark -0.40 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $730.69 | ▼ close $9,038.60 vs 09:30 $9,177.65 (session -137.35) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $730.69 | ▼ 09:30 equity $9,013.60 vs yday $9,038.60 (-25.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 12 | $7.25 | $0.91 | — | $642.78 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $91.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 255 | $0.36 | $1.68 | — | $549.81 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-15.6; leftover $91.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 8 | $11.12 | $0.91 | — | $459.94 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.7; leftover $91.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 6 | $13.59 | $0.83 | — | $377.56 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $91.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 9 | $9.49 | $0.88 | — | $291.27 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $91.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 2 | $36.96 | $0.75 | — | $216.61 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $91.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 20 | $4.55 | $0.97 | — | $124.64 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $91.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `ADIG` | 4 | $21.79 | $0.88 | — | $36.59 | — | baseline list, no extra gate; list probable; 🔵; ret5=+3.1; leftover $91.34 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.59 | ▲ close $9,165.45 vs 09:30 $9,013.60 (session +159.67) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.59 | ▼ 09:30 equity $9,094.41 vs yday $9,165.45 (-71.04) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.59 | ▼ close $8,984.53 vs 09:30 $9,094.41 (session -109.87) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.59 | ▲ 09:30 equity $9,043.19 vs yday $8,984.53 (+58.66) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `MRVI` | 1 | $8.76 | $0.11 | $+1.13 | $45.24 | ▲ +1.13 after sell → book $9,043.08; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `DNA` | 1 | $7.34 | $0.10 | $-0.28 | $52.49 | ▼ -0.28 after sell → book $9,042.98; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `EXK` | 1 | $10.80 | $0.13 | $-0.21 | $63.16 | ▼ -0.21 after sell → book $9,042.85; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `SCZM` | 1 | $9.61 | $0.12 | $-0.07 | $72.65 | ▼ -0.07 after sell → book $9,042.73; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NG` | 1 | $9.27 | $0.12 | $+0.69 | $81.80 | ▲ +0.69 after sell → book $9,042.62; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `FLNC` | 1 | $11.52 | $0.12 | — | $70.16 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-8.2; leftover $11.69 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 1 | $9.68 | $0.10 | — | $60.38 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $11.69 | — |
| 2026-08-27 09:30 ET | **BUY** | `SENS` | 1 | $9.33 | $0.10 | — | $50.96 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.5; leftover $11.69 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.96 | ▲ close $9,095.50 vs 09:30 $9,043.19 (session +53.19) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.96 | ▼ 09:30 equity $9,084.93 vs yday $9,095.50 (-10.57) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BTBT` | 679 | $1.58 | $8.88 | $-71.96 | $1,114.90 | ▼ -71.96 after sell → book $9,076.05; vs 09:30 mark -8.88 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ENHA` | 659 | $1.56 | $8.62 | $-115.97 | $2,134.32 | ▼ -115.97 after sell → book $9,067.43; vs 09:30 mark -8.62 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `DE` | 1 | $626.50 | $2.01 | $-0.77 | $2,758.80 | ▼ -0.77 after sell → book $9,065.42; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `QDEL` | 75 | $15.09 | $2.24 | $+5.30 | $3,888.32 | ▲ +5.30 after sell → book $9,063.18; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ORBS` | 1305 | $0.83 | $15.03 | $-69.37 | $4,961.66 | ▼ -69.37 after sell → book $9,048.15; vs 09:30 mark -15.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `GORO` | 362 | $3.80 | $4.74 | $+240.37 | $6,332.52 | ▲ +240.37 after sell → book $9,043.41; vs 09:30 mark -4.74 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `QTRX` | 362 | $2.64 | $4.74 | $-179.55 | $7,283.46 | ▼ -179.55 after sell → book $9,038.67; vs 09:30 mark -4.74 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CF` | 8 | $126.71 | $2.03 | $-9.81 | $8,295.10 | ▼ -9.81 after sell → book $9,036.64; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 36 | $32.90 | $2.10 | — | $7,108.61 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1185.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 75 | $15.66 | $2.21 | — | $5,931.89 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1185.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 14 | $79.42 | $2.03 | — | $4,817.98 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1185.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 356 | $3.32 | $4.59 | — | $3,631.47 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.4; leftover $1185.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 4 | $252.24 | $2.00 | — | $2,620.51 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1185.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 137 | $8.61 | $2.40 | — | $1,438.53 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.7; leftover $1185.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `XPOF` | 220 | $5.38 | $2.84 | — | $252.10 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.5; leftover $1185.01 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $252.10 | ▼ close $8,832.12 vs 09:30 $9,084.93 (session -186.34) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $252.10 | ▼ 09:30 equity $8,795.90 vs yday $8,832.12 (-36.22) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $252.10 | ▲ close $8,862.67 vs 09:30 $8,795.90 (session +66.77) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $252.10 | ▼ 09:30 equity $8,737.96 vs yday $8,862.67 (-124.71) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `CAPR` | 12 | $10.77 | $1.35 | $+39.99 | $379.99 | ▲ +39.99 after sell → book $8,736.62; vs 09:30 mark -1.34 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `SAFX` | 255 | $0.36 | $1.75 | $-1.64 | $471.32 | ▼ -1.64 after sell → book $8,734.87; vs 09:30 mark -1.75 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `VITL` | 8 | $10.67 | $0.90 | $-5.41 | $555.78 | ▼ -5.41 after sell → book $8,733.97; vs 09:30 mark -0.90 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `KURA` | 6 | $12.54 | $0.79 | $-7.92 | $630.23 | ▼ -7.92 after sell → book $8,733.18; vs 09:30 mark -0.79 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CCOI` | 9 | $9.42 | $0.89 | $-2.41 | $714.11 | ▼ -2.41 after sell → book $8,732.28; vs 09:30 mark -0.90 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `LIFE` | 2 | $35.06 | $0.73 | $-5.27 | $783.51 | ▼ -5.27 after sell → book $8,731.56; vs 09:30 mark -0.72 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ZIP` | 20 | $4.14 | $0.91 | $-10.08 | $865.40 | ▼ -10.08 after sell → book $8,730.65; vs 09:30 mark -0.91 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ADIG` | 4 | $21.86 | $0.91 | $-1.51 | $951.93 | ▼ -1.51 after sell → book $8,729.74; vs 09:30 mark -0.91 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $951.93 | ▼ close $8,595.39 vs 09:30 $8,737.96 (session -134.35) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $951.93 | ▼ 09:30 equity $8,566.82 vs yday $8,595.39 (-28.57) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $951.93 | ▲ close $8,788.83 vs 09:30 $8,566.82 (session +222.02) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $951.93 | ▲ 09:30 equity $8,794.40 vs yday $8,788.83 (+5.57) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `FLNC` | 1 | $10.01 | $0.12 | $-1.75 | $961.82 | ▼ -1.75 after sell → book $8,794.28; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `ABX` | 1 | $9.68 | $0.12 | $-0.22 | $971.38 | ▼ -0.22 after sell → book $8,794.16; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SENS` | 1 | $9.97 | $0.12 | $+0.42 | $981.23 | ▲ +0.42 after sell → book $8,794.04; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 7 | $15.45 | $1.10 | — | $871.97 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $122.65 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 7 | $16.77 | $1.19 | — | $753.39 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $122.65 | — |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 8 | $14.85 | $1.21 | — | $633.38 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $122.65 | — |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 2 | $55.42 | $1.11 | — | $521.42 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-25.9; leftover $122.65 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 56 | $2.18 | $1.39 | — | $397.95 | — | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $122.65 | — |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 8 | $13.96 | $1.14 | — | $285.13 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-6.4; leftover $122.65 | — |
| 2026-09-03 09:30 ET | **BUY** | `SAFX` | 325 | $0.38 | $2.20 | — | $160.41 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-2.3; leftover $122.65 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.41 | ▼ close $8,699.33 vs 09:30 $8,794.40 (session -85.35) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.41 | ▼ 09:30 equity $8,673.72 vs yday $8,699.33 (-25.61) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `SEDG` | 36 | $33.86 | $2.12 | $+30.34 | $1,377.25 | ▲ +30.34 after sell → book $8,671.60; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `GRRR` | 75 | $13.56 | $2.24 | $-161.95 | $2,392.01 | ▼ -161.95 after sell → book $8,669.36; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `URBN` | 14 | $79.55 | $2.05 | $-2.26 | $3,503.66 | ▼ -2.26 after sell → book $8,667.31; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `PYXS` | 356 | $3.53 | $4.66 | $+65.51 | $4,755.68 | ▲ +65.51 after sell → book $8,662.65; vs 09:30 mark -4.66 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SIMO` | 4 | $239.23 | $2.02 | $-56.06 | $5,710.58 | ▼ -56.06 after sell → book $8,660.63; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 137 | $7.79 | $2.43 | $-117.17 | $6,775.37 | ▼ -117.17 after sell → book $8,658.19; vs 09:30 mark -2.44 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `XPOF` | 220 | $4.91 | $2.88 | $-109.12 | $7,852.69 | ▼ -109.12 after sell → book $8,655.31; vs 09:30 mark -2.88 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 389 | $2.52 | $5.02 | — | $6,867.39 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $981.59 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 205 | $4.78 | $2.64 | — | $5,884.85 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $981.59 | — |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 61 | $15.90 | $2.17 | — | $4,912.77 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.1; leftover $981.59 | — |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 278 | $3.52 | $3.59 | — | $3,930.63 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $981.59 | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 1 | $513.78 | $1.99 | — | $3,414.85 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $981.59 | — |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 35 | $28.00 | $2.10 | — | $2,432.76 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+8.7; leftover $981.59 | — |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 97 | $10.02 | $2.28 | — | $1,458.54 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.2; leftover $981.59 | — |
| 2026-09-04 09:30 ET | **BUY** | `UAMY` | 186 | $5.25 | $2.55 | — | $479.49 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-0.4; leftover $981.59 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $479.49 | ▼ close $8,628.33 vs 09:30 $8,673.72 (session -4.64) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $479.49 | ▼ 09:30 equity $8,609.70 vs yday $8,628.33 (-18.63) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $479.49 | ▲ close $8,887.94 vs 09:30 $8,609.70 (session +278.24) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $479.49 | ▼ 09:30 equity $8,816.97 vs yday $8,887.94 (-70.97) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $479.49 | ▼ close $8,631.91 vs 09:30 $8,816.97 (session -185.07) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $479.49 | ▼ 09:30 equity $8,416.23 vs yday $8,631.91 (-215.68) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $479.49 | ▼ close $8,293.73 vs 09:30 $8,416.23 (session -122.50) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $479.49 | ▲ 09:30 equity $8,381.98 vs yday $8,293.73 (+88.25) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `CRK` | 7 | $15.03 | $1.09 | $-5.14 | $583.61 | ▼ -5.14 after sell → book $8,380.88; vs 09:30 mark -1.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `ARCT` | 7 | $14.06 | $1.03 | $-21.19 | $681.00 | ▼ -21.19 after sell → book $8,379.86; vs 09:30 mark -1.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `SLN` | 8 | $13.32 | $1.11 | $-14.56 | $786.45 | ▼ -14.56 after sell → book $8,378.75; vs 09:30 mark -1.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `EIX` | 2 | $57.30 | $1.17 | $+1.47 | $899.88 | ▲ +1.47 after sell → book $8,377.58; vs 09:30 mark -1.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CRDL` | 56 | $2.03 | $1.32 | $-11.11 | $1,012.24 | ▼ -11.11 after sell → book $8,376.25; vs 09:30 mark -1.33 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CLYM` | 8 | $15.21 | $1.26 | $+7.60 | $1,132.65 | ▲ +7.60 after sell → book $8,374.99; vs 09:30 mark -1.26 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `SAFX` | 325 | $0.41 | $2.39 | $+7.76 | $1,265.14 | ▲ +7.76 after sell → book $8,372.60; vs 09:30 mark -2.39 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 77 | $2.04 | $1.80 | — | $1,106.26 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $158.14 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 33 | $4.75 | $1.67 | — | $947.84 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $158.14 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 74 | $2.12 | $1.79 | — | $789.17 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $158.14 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 6 | $23.63 | $1.44 | — | $645.96 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-6.3; leftover $158.14 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 13 | $11.55 | $1.54 | — | $494.27 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $158.14 | — |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 1 | $157.55 | $1.58 | — | $335.14 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $158.14 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 2 | $77.33 | $1.55 | — | $178.93 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+2.5; leftover $158.14 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 3 | $52.55 | $1.59 | — | $19.69 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $158.14 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.69 | ▼ close $8,308.88 vs 09:30 $8,381.98 (session -50.77) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.69 | ▼ 09:30 equity $8,173.42 vs yday $8,308.88 (-135.46) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 389 | $2.15 | $5.09 | $-154.04 | $850.95 | ▼ -154.04 after sell → book $8,168.33; vs 09:30 mark -5.09 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 205 | $4.13 | $2.69 | $-138.58 | $1,694.91 | ▼ -138.58 after sell → book $8,165.64; vs 09:30 mark -2.69 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `HQ` | 61 | $14.67 | $2.19 | $-79.40 | $2,587.59 | ▼ -79.40 after sell → book $8,163.45; vs 09:30 mark -2.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `EOSE` | 278 | $3.77 | $3.64 | $+62.27 | $3,632.00 | ▲ +62.27 after sell → book $8,159.80; vs 09:30 mark -3.65 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `MLYS` | 35 | $28.14 | $2.12 | $+0.69 | $4,614.79 | ▲ +0.69 after sell → book $8,157.69; vs 09:30 mark -2.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `CCOI` | 97 | $9.12 | $2.31 | $-91.89 | $5,497.12 | ▼ -91.89 after sell → book $8,155.38; vs 09:30 mark -2.31 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `UAMY` | 186 | $4.73 | $2.59 | $-101.86 | $6,374.31 | ▼ -101.86 after sell → book $8,152.79; vs 09:30 mark -2.59 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,374.31 | ▼ close $8,142.62 vs 09:30 $8,173.42 (session -10.17) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,374.31 | ▼ 09:30 equity $8,141.20 vs yday $8,142.62 (-1.42) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `DELL` | 1 | $541.55 | $2.01 | $+23.76 | $6,913.85 | ▲ +23.76 after sell → book $8,139.19; vs 09:30 mark -2.01 | dropped from list after 6 sess (min 5) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,913.85 | ▼ close $8,131.10 vs 09:30 $8,141.20 (session -8.09) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,913.85 | ▼ 09:30 equity $8,104.78 vs yday $8,131.10 (-26.32) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 46 | $18.61 | $2.13 | — | $6,055.66 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $864.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 47 | $18.21 | $2.13 | — | $5,197.66 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-19.1; leftover $864.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 12 | $68.79 | $2.03 | — | $4,370.15 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $864.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 147 | $5.87 | $2.43 | — | $3,504.83 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $864.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 317 | $2.72 | $4.09 | — | $2,638.50 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.4; leftover $864.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 9 | $87.40 | $2.02 | — | $1,849.89 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $864.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 22 | $38.01 | $2.06 | — | $1,011.61 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $864.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 31 | $27.09 | $2.08 | — | $169.74 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $864.23 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $169.74 | ▲ close $8,237.68 vs 09:30 $8,104.78 (session +151.86) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $169.74 | ▲ 09:30 equity $8,374.99 vs yday $8,237.68 (+137.31) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 124 | $0.17 | $0.58 | — | $148.08 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $21.22 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 1 | $15.87 | $0.16 | — | $132.04 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $21.22 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 8 | $2.40 | $0.22 | — | $112.63 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $21.22 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 1 | $18.04 | $0.18 | — | $94.41 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $21.22 | — |
| 2026-09-17 09:30 ET | **BUY** | `EROC` | 1 | $12.64 | $0.13 | — | $81.64 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-3.6; leftover $21.22 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.64 | ▲ close $8,425.46 vs 09:30 $8,374.99 (session +51.75) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.64 | ▲ 09:30 equity $8,477.62 vs yday $8,425.46 (+52.16) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AMTX` | 77 | $1.90 | $1.72 | $-14.30 | $226.22 | ▼ -14.30 after sell → book $8,475.91; vs 09:30 mark -1.71 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `CLOV` | 33 | $4.50 | $1.60 | $-11.52 | $373.12 | ▼ -11.52 after sell → book $8,474.30; vs 09:30 mark -1.61 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 74 | $1.77 | $1.55 | $-29.24 | $502.55 | ▼ -29.24 after sell → book $8,472.75; vs 09:30 mark -1.55 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `TYRA` | 6 | $24.58 | $1.51 | $+2.75 | $648.51 | ▲ +2.75 after sell → book $8,471.24; vs 09:30 mark -1.51 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `FUBO` | 13 | $9.90 | $1.35 | $-24.34 | $775.87 | ▼ -24.34 after sell → book $8,469.89; vs 09:30 mark -1.35 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `RDDT` | 1 | $152.69 | $1.55 | $-7.99 | $927.01 | ▼ -7.99 after sell → book $8,468.34; vs 09:30 mark -1.55 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `VIST` | 2 | $72.99 | $1.49 | $-11.72 | $1,071.50 | ▼ -11.72 after sell → book $8,466.86; vs 09:30 mark -1.48 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAND` | 3 | $51.19 | $1.56 | $-7.25 | $1,223.49 | ▼ -7.25 after sell → book $8,465.29; vs 09:30 mark -1.57 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 157 | $0.97 | $1.99 | — | $1,069.21 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $152.94 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 73 | $2.08 | $1.74 | — | $915.63 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $152.94 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 38 | $3.95 | $1.61 | — | $763.91 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $152.94 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 10 | $14.07 | $1.44 | — | $621.78 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $152.94 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 26 | $5.83 | $1.59 | — | $468.60 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $152.94 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 42 | $3.58 | $1.63 | — | $316.61 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $152.94 | — |
| 2026-09-18 09:30 ET | **BUY** | `RANI` | 179 | $0.85 | $2.06 | — | $162.41 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+3.6; leftover $152.94 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 10 | $14.79 | $1.51 | — | $13.00 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $152.94 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.00 | ▼ close $8,404.68 vs 09:30 $8,477.62 (session -47.03) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.00 | ▲ 09:30 equity $8,503.64 vs yday $8,404.68 (+98.96) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1 | $1.11 | $0.01 | — | $11.87 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1.62 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.87 | ▼ close $8,482.42 vs 09:30 $8,503.64 (session -21.20) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.87 | ▼ 09:30 equity $8,462.60 vs yday $8,482.42 (-19.82) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 2 | $0.58 | $0.02 | — | $10.70 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $1.48 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.70 | ▲ close $8,483.33 vs 09:30 $8,462.60 (session +20.74) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.70 | ▲ 09:30 equity $8,547.07 vs yday $8,483.33 (+63.74) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BBNX` | 46 | $23.00 | $2.15 | $+197.66 | $1,066.55 | ▲ +197.66 after sell → book $8,544.92; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARQQ` | 47 | $23.30 | $2.15 | $+234.95 | $2,159.50 | ▲ +234.95 after sell → book $8,542.77; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `TEM` | 12 | $76.47 | $2.05 | $+88.09 | $3,075.09 | ▲ +88.09 after sell → book $8,540.72; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RIG` | 147 | $5.53 | $2.47 | $-54.88 | $3,885.53 | ▼ -54.88 after sell → book $8,538.25; vs 09:30 mark -2.47 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `QTRX` | 317 | $3.17 | $4.15 | $+134.41 | $4,886.27 | ▲ +134.41 after sell → book $8,534.10; vs 09:30 mark -4.15 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `VAL` | 9 | $83.39 | $2.04 | $-40.14 | $5,634.75 | ▼ -40.14 after sell → book $8,532.07; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `KRMN` | 22 | $33.34 | $2.08 | $-106.87 | $6,366.15 | ▼ -106.87 after sell → book $8,529.99; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `ADPT` | 31 | $27.74 | $2.10 | $+15.96 | $7,223.99 | ▲ +15.96 after sell → book $8,527.89; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 43 | $20.65 | $2.12 | — | $6,333.92 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $903.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 229 | $3.93 | $2.95 | — | $5,430.99 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $903.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `MAZE` | 31 | $28.30 | $2.08 | — | $4,551.61 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $903.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 57 | $15.72 | $2.16 | — | $3,653.41 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $903.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 35 | $25.40 | $2.10 | — | $2,762.31 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $903.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `CLPT` | 58 | $15.55 | $2.16 | — | $1,858.25 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $903.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 1175 | $0.77 | $12.55 | — | $943.30 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $903.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLLN` | 7 | $116.00 | $2.01 | — | $129.29 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $903.00 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.29 | ▼ close $8,266.60 vs 09:30 $8,547.07 (session -233.16) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.29 | ▼ 09:30 equity $8,224.82 vs yday $8,266.60 (-41.78) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `DVLT` | 124 | $0.15 | $0.59 | $-3.65 | $147.30 | ▼ -3.65 after sell → book $8,224.23; vs 09:30 mark -0.59 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `BRUN` | 1 | $16.07 | $0.18 | $-0.15 | $163.19 | ▼ -0.15 after sell → book $8,224.05; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `SABR` | 8 | $2.17 | $0.22 | $-2.27 | $180.33 | ▼ -2.27 after sell → book $8,223.83; vs 09:30 mark -0.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `CIFR` | 1 | $17.86 | $0.20 | $-0.55 | $197.99 | ▼ -0.55 after sell → book $8,223.63; vs 09:30 mark -0.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `EROC` | 1 | $12.19 | $0.14 | $-0.72 | $210.04 | ▼ -0.72 after sell → book $8,223.49; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $210.04 | ▲ close $8,253.11 vs 09:30 $8,224.82 (session +29.61) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.63 | ▲ 09:30 equity $9,275.06 vs yday $9,275.06 (-0.00) | 09:30 open · cash $26.63 (unchanged overnight, no fees) · equity $9,275.06 vs prior close $9,275.06 (-0.00) · 20 name(s) re-marked at the open (per-name table). AIB×9 yday $1.42 → 09:30 $1.42 +0.00; BHVN×1 yday $13.19 → 09:30 $13.19 +0.00; BNC×4 yday $6.26 → 09:30 $6.26 +0.00; BTBT×1 yday $1.79 → 09:30 $1.79 +0.00; CNXC×39 yday $29.39 → 09:30 $29.39 +0.00; DDD×6 yday $3.43 → 09:30 $3.43 +0.00; DEFT×4 yday $0.53 → 09:30 $0.53 +0.00; EYPT×6 yday $3.65 → 09:30 $3.65 +0.00; FJET×1 yday $1.80 → 09:30 $1.80 +0.00; GT×217 yday $5.07 → 09:30 $5.07 +0.00; HYMC×54 yday $20.89 → 09:30 $20.89 +0.00; INDP×301 yday $4.00 → 09:30 $4.00 +0.00; NMRA×1543 yday $0.70 → 09:30 $0.70 +0.00; ORBS×2 yday $1.03 → 09:30 $1.03 +0.00; RANI×28 yday $0.75 → 09:30 $0.75 +0.00; RARE×1 yday $14.77 → 09:30 $14.77 +0.00; SHLS×3 yday $7.45 → 09:30 $7.45 +0.00; TLYS×272 yday $4.24 → 09:30 $4.24 +0.00; TNGX×46 yday $24.63 → 09:30 $24.63 +0.00; TTAN×19 yday $59.98 → 09:30 $59.98 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 1 | $2.20 | $0.03 | — | $24.40 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $3.33 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.40 | ▲ close $9,275.04 vs 09:30 $9,275.06 (session +0.01) | 16:00 close · cash $24.40 · equity $9,275.04 vs 09:30 $9,275.06 (-0.02; session marks +0.01) · 21 name(s) marked open→close (per-name table). AIB×9 09:30 $1.42 → close $1.42 -0.00; BHVN×1 09:30 $13.19 → close $13.19 -0.00; BNC×4 09:30 $6.26 → close $6.26 +0.00; BTBT×1 09:30 $1.79 → close $1.79 -0.00; CNXC×39 09:30 $29.39 → close $29.39 -0.00; DDD×6 09:30 $3.43 → close $3.43 +0.00; DEFT×4 09:30 $0.53 → close $0.53 +0.00; EYPT×6 09:30 $3.65 → close $3.65 +0.00; FJET×1 09:30 $1.80 → close $1.80 -0.00; GT×217 09:30 $5.07 → close $5.07 +0.00; HYMC×54 09:30 $20.89 → close $20.89 -0.00; INDP×301 09:30 $4.00 → close $4.00 +0.00; NMRA×1543 09:30 $0.70 → close $0.70 +0.00; ORBS×2 09:30 $1.03 → close $1.03 -0.00; RANI×28 09:30 $0.75 → close $0.75 +0.00; RARE×1 09:30 $14.77 → close $14.77 +0.00; SHLS×3 09:30 $7.45 → close $7.45 -0.00; TLYS×272 09:30 $4.24 → close $4.24 -0.00; TNGX×46 09:30 $24.63 → close $24.63 -0.00; TTAN×19 09:30 $59.98 → close $59.98 -0.00; HLP×1 09:30 $2.20 → close $2.21 +0.01 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `WWW` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `WDC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `FOSL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `AIRS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ALGM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `CDNL` | cash | leftover split 33.64 < 1 share @ 39.85 |
| 2026-08-17 | `CELC` | cash | leftover split 33.64 < 1 share @ 92.99 |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `WWW` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `WDC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `FOSL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `AIRS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ALGM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ABX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `FCEL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `VERA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `BW` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `OCC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-08-19 | `WDC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `FOSL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ADUR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `AIRS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ALGM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ABX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `FCEL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `VERA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `BW` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `OCC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `ALM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `WWW` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HYLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `WDC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `FOSL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ADUR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `AIRS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ALGM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ABX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `FCEL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `VERA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `BW` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `OCC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `ALM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `MSTR` | cash | leftover split 13.09 < 1 share @ 113.23 |
| 2026-08-20 | `BLSH` | cash | leftover split 13.09 < 1 share @ 29.20 |
| 2026-08-20 | `CRCL` | cash | leftover split 13.09 < 1 share @ 82.99 |
| 2026-08-21 | `ABX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `FCEL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `VERA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `BW` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `OCC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `ALM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `MRVI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ENHA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `DE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `QDEL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `GORO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `QTRX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CF` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `MRVI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `DNA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `EXK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `SCZM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ENHA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `DE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `QDEL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `GORO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `QTRX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CF` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-26 | `MRVI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `DNA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `EXK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `SCZM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ENHA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `DE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `QDEL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ORBS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `GORO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `QTRX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CF` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `VITL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `CCOI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ZIP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ADIG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `AVBP` | cash | leftover split 4.57 < 1 share @ 31.21 |
| 2026-08-26 | `FLNC` | cash | leftover split 4.57 < 1 share @ 11.12 |
| 2026-08-26 | `ABX` | cash | leftover split 4.57 < 1 share @ 9.83 |
| 2026-08-26 | `AVEX` | cash | leftover split 4.57 < 1 share @ 17.51 |
| 2026-08-26 | `ITG` | cash | leftover split 4.57 < 1 share @ 12.04 |
| 2026-08-26 | `SENS` | cash | leftover split 4.57 < 1 share @ 9.48 |
| 2026-08-26 | `BE` | cash | leftover split 4.57 < 1 share @ 213.94 |
| 2026-08-26 | `AXTI` | cash | leftover split 4.57 < 1 share @ 65.34 |
| 2026-08-27 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ENHA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `DE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `QDEL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ORBS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `GORO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `QTRX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CF` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `VITL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CCOI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ZIP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ADIG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `AVBP` | cash | leftover split 11.69 < 1 share @ 30.79 |
| 2026-08-27 | `AVEX` | cash | leftover split 11.69 < 1 share @ 18.43 |
| 2026-08-27 | `ITG` | cash | leftover split 11.69 < 1 share @ 12.36 |
| 2026-08-27 | `BE` | cash | leftover split 11.69 < 1 share @ 227.10 |
| 2026-08-28 | `CAPR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `VITL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `KURA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CCOI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `LIFE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ZIP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ADIG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `ABX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `SENS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `SAFX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `VITL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `KURA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CCOI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `LIFE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ZIP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ADIG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `FLNC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `ABX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SENS` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-09-01 | `FLNC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `ABX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SENS` | min_hold | dropped but min-hold 3/5 sess — no sell |
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
| 2026-09-02 | `FLNC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `ABX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `SENS` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-09-03 | `MRNA` | cash | leftover split 122.65 < 1 share @ 145.94 |
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
| 2026-09-08 | `HQ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `EOSE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `MLYS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `CCOI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `UAMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-09-09 | `HQ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `EOSE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `DELL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `MLYS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `CCOI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `UAMY` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-09-10 | `HQ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `EOSE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `DELL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `MLYS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `CCOI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `UAMY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LAC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XHG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `ALEC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OABI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `HQ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `EOSE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `DELL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `MLYS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `CCOI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `UAMY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `TYRA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `FUBO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `RDDT` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-09-15 | `RDDT` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-09-16 | `RDDT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `VIST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `BAND` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-17 | `AMTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `CLOV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `BAK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `TYRA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `FUBO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `RDDT` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-09-17 | `AXTI` | cash | leftover split 21.22 < 1 share @ 67.91 |
| 2026-09-17 | `ARQT` | cash | leftover split 21.22 < 1 share @ 25.95 |
| 2026-09-17 | `SMTC` | cash | leftover split 21.22 < 1 share @ 170.85 |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `ARQQ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `QTRX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `VAL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `KRMN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `CIFR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `EROC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `BBNX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `ARQQ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `TEM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RIG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `QTRX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `VAL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `KRMN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `ADPT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `CIFR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `EROC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `SWRD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `BNC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `DDD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `RANI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `BKKT` | cash | leftover split 1.62 < 1 share @ 9.31 |
| 2026-09-21 | `BTDR` | cash | leftover split 1.62 < 1 share @ 13.47 |
| 2026-09-21 | `SBET` | cash | leftover split 1.62 < 1 share @ 9.99 |
| 2026-09-21 | `BTBT` | cash | leftover split 1.62 < 1 share @ 1.82 |
| 2026-09-21 | `SGML` | cash | leftover split 1.62 < 1 share @ 10.13 |
| 2026-09-21 | `SNDK` | cash | leftover split 1.62 < 1 share @ 1826.00 |
| 2026-09-21 | `GLXY` | cash | leftover split 1.62 < 1 share @ 25.95 |
| 2026-09-22 | `BBNX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `ARQQ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `TEM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `RIG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `QTRX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `VAL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `KRMN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `ADPT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `DVLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `BRUN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `SABR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `CIFR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `EROC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `SWRD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `BNC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `DDD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `RANI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `ALOY` | cash | leftover split 1.48 < 1 share @ 9.40 |
| 2026-09-22 | `MRAM` | no_price | no 09:30 open |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-23 | `DVLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `BRUN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `SABR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `CIFR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `EROC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `TLSA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `SWRD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `EYPT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `BHVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `BNC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `DDD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `RANI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `RARE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `ORBS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `TLSA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `SWRD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `EYPT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `BHVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `BNC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `DDD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `RANI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `RARE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `ORBS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| `TLSA` | 157 | 2026-09-18 @ $0.97 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $152.94 |
| `SWRD` | 73 | 2026-09-18 @ $2.08 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $152.94 |
| `EYPT` | 38 | 2026-09-18 @ $3.95 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $152.94 |
| `BHVN` | 10 | 2026-09-18 @ $14.07 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $152.94 |
| `BNC` | 26 | 2026-09-18 @ $5.83 | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $152.94 |
| `DDD` | 42 | 2026-09-18 @ $3.58 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $152.94 |
| `RANI` | 179 | 2026-09-18 @ $0.85 | baseline list, no extra gate; list probable,yday_gainer; ret5=+3.6; leftover $152.94 |
| `RARE` | 10 | 2026-09-18 @ $14.79 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $152.94 |
| `ORBS` | 1 | 2026-09-21 @ $1.11 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1.62 |
| `DEFT` | 2 | 2026-09-22 @ $0.58 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $1.48 |
| `OMER` | 43 | 2026-09-23 @ $20.65 | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $903.00 |
| `INDP` | 229 | 2026-09-23 @ $3.93 | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $903.00 |
| `MAZE` | 31 | 2026-09-23 @ $28.30 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $903.00 |
| `SGRY` | 57 | 2026-09-23 @ $15.72 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $903.00 |
| `TNGX` | 35 | 2026-09-23 @ $25.40 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $903.00 |
| `CLPT` | 58 | 2026-09-23 @ $15.55 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $903.00 |
| `NMRA` | 1175 | 2026-09-23 @ $0.77 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $903.00 |
| `BLLN` | 7 | 2026-09-23 @ $116.00 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $903.00 |
