# Factor mine action — `short_last_red_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · last bar red

Cash book **-0.05%** ($9,995) · signal-only (no cash/fees) was -38.90%. Starts YES **0/30**. Fills 241 · skips 326 · realized $-1274.58.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the last finished bar was red (closed down).

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
- **Gate** `last_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $14,935.15.

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
| 2026-08-13 09:30 ET | **SHORT** | `TGTX` | 25 | $49.70 | $2.12 | — | $11,240.38 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **SHORT** | `SLS` | 106 | $11.70 | $2.37 | — | $12,478.21 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **SHORT** | `HIMS` | 42 | $29.74 | $2.17 | — | $13,725.12 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **SHORT** | `VOR` | 56 | $22.01 | $2.21 | — | $14,955.47 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,955.47 | ▼ close $9,934.23 vs 09:30 $10,000.00 (session -56.90) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,955.47 | ▼ 09:30 equity $9,928.54 vs yday $9,934.23 (-5.69) | — | — |
| 2026-08-14 09:30 ET | **SHORT** | `TLN` | 1 | $359.83 | $2.02 | — | $15,313.28 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+5.9; leftover $620.53 | — |
| 2026-08-14 09:30 ET | **SHORT** | `NRG` | 5 | $120.00 | $2.04 | — | $15,911.24 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+0.6; leftover $620.53 | — |
| 2026-08-14 09:30 ET | **SHORT** | `MARA` | 68 | $9.01 | $2.23 | — | $16,521.68 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $620.53 | — |
| 2026-08-14 09:30 ET | **SHORT** | `FOSL` | 110 | $5.64 | $2.37 | — | $17,139.72 | — | last bar red; gate last_red=True; list probable; 🔵; ret5=-4.1; leftover $620.53 | — |
| 2026-08-14 09:30 ET | **SHORT** | `ARX` | 31 | $19.57 | $2.12 | — | $17,744.27 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $620.53 | — |
| 2026-08-14 09:30 ET | **SHORT** | `CRMD` | 77 | $8.05 | $2.26 | — | $18,361.86 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $620.53 | — |
| 2026-08-14 09:30 ET | **SHORT** | `BIRK` | 15 | $39.75 | $2.07 | — | $18,956.04 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+10.2; leftover $620.53 | — |
| 2026-08-14 09:30 ET | **SHORT** | `HLIT` | 47 | $13.18 | $2.17 | — | $19,573.33 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $620.53 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,573.33 | ▼ close $9,863.88 vs 09:30 $9,928.54 (session -47.38) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,573.33 | ▲ 09:30 equity $9,867.77 vs yday $9,863.88 (+3.89) | — | — |
| 2026-08-17 09:30 ET | **SHORT** | `TMC` | 152 | $4.05 | $2.50 | — | $20,186.43 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $616.74 | — |
| 2026-08-17 09:30 ET | **SHORT** | `TGB` | 72 | $8.46 | $2.24 | — | $20,793.31 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $616.74 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ELF` | 6 | $90.54 | $2.04 | — | $21,334.50 | — | last bar red; gate last_red=True; list flatten; ret5=-7.2; leftover $616.74 | — |
| 2026-08-17 09:30 ET | **SHORT** | `DNN` | 190 | $3.24 | $2.62 | — | $21,947.48 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $616.74 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HNST` | 128 | $4.81 | $2.42 | — | $22,560.74 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=-11.4; leftover $616.74 | — |
| 2026-08-17 09:30 ET | **SHORT** | `CAPR` | 89 | $6.87 | $2.30 | — | $23,169.87 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+62.6; leftover $616.74 | — |
| 2026-08-17 09:30 ET | **SHORT** | `BYND` | 48 | $12.83 | $2.17 | — | $23,783.54 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ⚪; ret5=-34.1; leftover $616.74 | — |
| 2026-08-17 09:30 ET | **SHORT** | `NU` | 40 | $15.40 | $2.15 | — | $24,397.40 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $616.74 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,397.40 | ▲ close $9,855.62 vs 09:30 $9,867.77 (session +6.29) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,397.40 | ▲ 09:30 equity $10,051.15 vs yday $9,855.62 (+195.53) | — | — |
| 2026-08-18 09:30 ET | **COVER** | `TGTX` | 25 | $49.28 | $2.06 | $+6.32 | $23,163.33 | ▲ +6.32 after sell → book $10,049.08; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **COVER** | `SLS` | 106 | $12.66 | $2.31 | $-106.44 | $21,819.06 | ▼ -106.44 after sell → book $10,046.77; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **COVER** | `HIMS` | 42 | $27.85 | $2.12 | $+75.09 | $20,647.25 | ▲ +75.09 after sell → book $10,044.66; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **COVER** | `VOR` | 56 | $22.82 | $2.16 | $-49.73 | $19,367.17 | ▼ -49.73 after sell → book $10,042.50; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,367.17 | ▲ close $10,090.26 vs 09:30 $10,051.15 (session +47.76) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,367.17 | ▼ 09:30 equity $9,991.40 vs yday $10,090.26 (-98.86) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `TLN` | 1 | $321.00 | $1.99 | $+34.81 | $19,044.18 | ▲ +34.81 after sell → book $9,989.41; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `NRG` | 5 | $116.20 | $2.00 | $+14.95 | $18,461.17 | ▲ +14.95 after sell → book $9,987.40; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `MARA` | 68 | $8.91 | $2.19 | $+2.37 | $17,853.10 | ▲ +2.37 after sell → book $9,985.21; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `FOSL` | 110 | $5.54 | $2.32 | $+6.31 | $17,241.38 | ▲ +6.31 after sell → book $9,982.89; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `ARX` | 31 | $19.58 | $2.08 | $-4.51 | $16,632.31 | ▼ -4.51 after sell → book $9,980.80; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `CRMD` | 77 | $8.30 | $2.22 | $-23.73 | $15,990.99 | ▼ -23.73 after sell → book $9,978.58; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `BIRK` | 15 | $37.50 | $2.04 | $+29.64 | $15,426.46 | ▲ +29.64 after sell → book $9,976.55; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `HLIT` | 47 | $12.90 | $2.13 | $+8.86 | $14,818.03 | ▲ +8.86 after sell → book $9,974.42; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,818.03 | ▼ close $9,782.03 vs 09:30 $9,991.40 (session -192.39) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,818.03 | ▲ 09:30 equity $9,862.31 vs yday $9,782.03 (+80.28) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `TMC` | 152 | $3.92 | $2.45 | $+14.82 | $14,219.74 | ▲ +14.82 after sell → book $9,859.86; vs 09:30 mark -2.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `TGB` | 72 | $8.35 | $2.21 | $+3.47 | $13,616.33 | ▲ +3.47 after sell → book $9,857.65; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ELF` | 6 | $98.15 | $2.01 | $-49.71 | $13,025.43 | ▼ -49.71 after sell → book $9,855.65; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `DNN` | 190 | $3.20 | $2.56 | $+2.42 | $12,414.87 | ▲ +2.42 after sell → book $9,853.09; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HNST` | 128 | $4.98 | $2.37 | $-26.56 | $11,775.05 | ▼ -26.56 after sell → book $9,850.71; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `CAPR` | 89 | $7.66 | $2.26 | $-74.87 | $11,091.06 | ▼ -74.87 after sell → book $9,848.46; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `BYND` | 48 | $13.60 | $2.13 | $-41.26 | $10,436.12 | ▼ -41.26 after sell → book $9,846.32; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `NU` | 40 | $14.74 | $2.11 | $+21.94 | $9,844.21 | ▲ +21.94 after sell → book $9,844.21; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `BHP` | 6 | $91.01 | $2.04 | — | $10,388.23 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $615.26 | — |
| 2026-08-20 09:30 ET | **SHORT** | `MRVI` | 82 | $7.44 | $2.28 | — | $10,996.03 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $615.26 | — |
| 2026-08-20 09:30 ET | **SHORT** | `CRCL` | 7 | $82.99 | $2.05 | — | $11,574.92 | — | last bar red; gate last_red=True; list probable; 🔵; ⚪; ret5=+7.4; leftover $615.26 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 28 | $21.40 | $2.11 | — | $12,172.01 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-25.2; leftover $615.26 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 138 | $4.43 | $2.45 | — | $12,780.89 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-23.1; leftover $615.26 | — |
| 2026-08-20 09:30 ET | **SHORT** | `DVLT` | 2050 | $0.30 | $12.67 | — | $13,383.22 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-3.2; leftover $615.26 | — |
| 2026-08-20 09:30 ET | **SHORT** | `SAFX` | 1738 | $0.35 | $11.68 | — | $13,986.79 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-29.4; leftover $615.26 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $14,593.78 | — | last bar red; gate last_red=True; list earn_react; 🔵; ret5=+5.0; leftover $615.26 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,593.78 | ▼ close $9,747.84 vs 09:30 $9,862.31 (session -59.02) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,593.78 | ▼ 09:30 equity $9,680.55 vs yday $9,747.84 (-67.29) | — | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUTL` | 244 | $2.47 | $3.21 | — | $15,193.24 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $605.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `CRDL` | 313 | $1.93 | $4.12 | — | $15,793.22 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $605.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `CRSP` | 10 | $59.72 | $2.06 | — | $16,388.36 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $605.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `FUTU` | 5 | $115.18 | $2.04 | — | $16,962.22 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $605.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `GMAB` | 18 | $33.36 | $2.08 | — | $17,560.62 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $605.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ENHA` | 353 | $1.71 | $4.64 | — | $18,159.61 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $605.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `CAN` | 2057 | $0.29 | $12.59 | — | $18,751.78 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $605.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `PRQR` | 265 | $2.28 | $3.49 | — | $19,352.49 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+22.7; leftover $605.03 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,352.49 | ▼ close $9,483.25 vs 09:30 $9,680.55 (session -163.08) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,352.49 | ▼ 09:30 equity $9,460.35 vs yday $9,483.25 (-22.90) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,352.49 | ▲ close $9,576.91 vs 09:30 $9,460.35 (session +116.56) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,352.49 | ▼ 09:30 equity $9,553.80 vs yday $9,576.91 (-23.11) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `BHP` | 6 | $95.86 | $2.01 | $-33.15 | $18,775.33 | ▼ -33.15 after sell → book $9,551.79; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `MRVI` | 82 | $8.53 | $2.24 | $-93.89 | $18,073.63 | ▼ -93.89 after sell → book $9,549.56; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `CRCL` | 7 | $84.73 | $2.01 | $-16.24 | $17,478.51 | ▼ -16.24 after sell → book $9,547.54; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 28 | $20.90 | $2.07 | $+9.82 | $16,891.23 | ▲ +9.82 after sell → book $9,545.47; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 138 | $4.42 | $2.40 | $-3.48 | $16,278.87 | ▼ -3.48 after sell → book $9,543.07; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `DVLT` | 2050 | $0.31 | $12.51 | $-45.67 | $15,630.87 | ▼ -45.67 after sell → book $9,530.56; vs 09:30 mark -12.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `SAFX` | 1738 | $0.36 | $11.44 | $-30.07 | $14,997.23 | ▼ -30.07 after sell → book $9,519.13; vs 09:30 mark -11.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 13 | $43.63 | $2.03 | $+37.77 | $14,428.01 | ▲ +37.77 after sell → book $9,517.10; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `MOS` | 25 | $23.77 | $2.10 | — | $15,020.16 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=+13.0; leftover $594.82 | — |
| 2026-08-25 09:30 ET | **SHORT** | `OCUL` | 54 | $10.98 | $2.19 | — | $15,610.89 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+1.2; leftover $594.82 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INSP` | 9 | $61.19 | $2.05 | — | $16,159.55 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+7.4; leftover $594.82 | — |
| 2026-08-25 09:30 ET | **SHORT** | `RZLT` | 120 | $4.94 | $2.40 | — | $16,749.95 | — | last bar red; gate last_red=True; list flatten; ret5=+7.1; leftover $594.82 | — |
| 2026-08-25 09:30 ET | **SHORT** | `HCA` | 1 | $426.97 | $2.02 | — | $17,174.89 | — | last bar red; gate last_red=True; list flatten; ret5=+6.0; leftover $594.82 | — |
| 2026-08-25 09:30 ET | **SHORT** | `CAPR` | 82 | $7.25 | $2.28 | — | $17,767.12 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $594.82 | — |
| 2026-08-25 09:30 ET | **SHORT** | `PUSA` | 156 | $3.80 | $2.51 | — | $18,357.41 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $594.82 | — |
| 2026-08-25 09:30 ET | **SHORT** | `CYPH` | 381 | $1.56 | $5.00 | — | $18,946.76 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $594.82 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,946.76 | ▼ close $9,137.75 vs 09:30 $9,553.80 (session -358.79) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,946.76 | ▲ 09:30 equity $9,206.79 vs yday $9,137.75 (+69.04) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `AUTL` | 244 | $2.41 | $3.15 | $+8.28 | $18,355.58 | ▲ +8.28 after sell → book $9,203.65; vs 09:30 mark -3.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `CRDL` | 313 | $2.03 | $4.04 | $-39.45 | $17,716.15 | ▼ -39.45 after sell → book $9,199.61; vs 09:30 mark -4.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `CRSP` | 10 | $60.18 | $2.02 | $-8.68 | $17,112.33 | ▼ -8.68 after sell → book $9,197.59; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `FUTU` | 5 | $124.67 | $2.00 | $-51.50 | $16,486.97 | ▼ -51.50 after sell → book $9,195.58; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `GMAB` | 18 | $33.78 | $2.04 | $-11.68 | $15,876.89 | ▼ -11.68 after sell → book $9,193.54; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ENHA` | 353 | $1.63 | $4.55 | $+19.05 | $15,296.95 | ▲ +19.05 after sell → book $9,188.99; vs 09:30 mark -4.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `CAN` | 2057 | $0.40 | $14.34 | $-238.79 | $14,465.98 | ▼ -238.79 after sell → book $9,174.65; vs 09:30 mark -14.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `PRQR` | 265 | $2.38 | $3.42 | $-33.41 | $13,831.86 | ▼ -33.41 after sell → book $9,171.23; vs 09:30 mark -3.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `FLNC` | 68 | $11.12 | $2.24 | — | $14,585.79 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $764.27 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AVEX` | 43 | $17.51 | $2.16 | — | $15,336.56 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $764.27 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AXTI` | 11 | $65.34 | $2.06 | — | $16,053.23 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $764.27 | — |
| 2026-08-26 09:30 ET | **SHORT** | `INDP` | 701 | $1.09 | $9.19 | — | $16,808.13 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+17.0; leftover $764.27 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NVTS` | 60 | $12.60 | $2.21 | — | $17,561.92 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-5.5; leftover $764.27 | — |
| 2026-08-26 09:30 ET | **SHORT** | `IRDM` | 16 | $46.96 | $2.08 | — | $18,311.20 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-3.9; leftover $764.27 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,311.20 | ▼ close $8,974.73 vs 09:30 $9,206.79 (session -176.56) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,311.20 | ▼ 09:30 equity $8,831.34 vs yday $8,974.73 (-143.39) | — | — |
| 2026-08-27 09:30 ET | **SHORT** | `KURA` | 68 | $12.98 | $2.24 | — | $19,191.60 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $883.13 | — |
| 2026-08-27 09:30 ET | **SHORT** | `AVBP` | 28 | $30.79 | $2.12 | — | $20,051.61 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=+3.7; leftover $883.13 | — |
| 2026-08-27 09:30 ET | **SHORT** | `ABX` | 91 | $9.68 | $2.31 | — | $20,930.17 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $883.13 | — |
| 2026-08-27 09:30 ET | **SHORT** | `SENS` | 94 | $9.33 | $2.32 | — | $21,804.87 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=+2.5; leftover $883.13 | — |
| 2026-08-27 09:30 ET | **SHORT** | `ACRS` | 143 | $6.15 | $2.48 | — | $22,681.85 | — | last bar red; gate last_red=True; list yday_gainer; ret5=-3.9; leftover $883.13 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,681.85 | ▼ close $8,733.71 vs 09:30 $8,831.34 (session -86.17) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,681.85 | ▲ 09:30 equity $8,840.48 vs yday $8,733.71 (+106.77) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `OCUL` | 54 | $10.97 | $2.15 | $-3.80 | $22,087.31 | ▼ -3.80 after sell → book $8,838.32; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INSP` | 9 | $60.52 | $2.02 | $+1.96 | $21,540.62 | ▲ +1.96 after sell → book $8,836.31; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `RZLT` | 120 | $4.95 | $2.35 | $-5.95 | $20,944.27 | ▼ -5.95 after sell → book $8,833.96; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `HCA` | 1 | $423.76 | $1.99 | $-0.81 | $20,518.51 | ▼ -0.81 after sell → book $8,831.96; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `CAPR` | 82 | $9.73 | $2.24 | $-207.87 | $19,718.42 | ▼ -207.87 after sell → book $8,829.73; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `PUSA` | 156 | $3.77 | $2.46 | $-0.29 | $19,127.84 | ▼ -0.29 after sell → book $8,827.27; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `CYPH` | 381 | $1.82 | $4.91 | $-108.98 | $18,429.50 | ▼ -108.98 after sell → book $8,822.35; vs 09:30 mark -4.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SEDG` | 19 | $32.90 | $2.08 | — | $19,052.52 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $630.17 | — |
| 2026-08-28 09:30 ET | **SHORT** | `GRRR` | 40 | $15.66 | $2.15 | — | $19,676.77 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $630.17 | — |
| 2026-08-28 09:30 ET | **SHORT** | `URBN` | 7 | $79.42 | $2.05 | — | $20,230.67 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $630.17 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SAFX` | 1726 | $0.36 | $11.79 | — | $20,848.87 | — | last bar red; gate last_red=True; list probable,yday_gainer; ret5=+7.6; leftover $630.17 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 2 | $252.24 | $2.03 | — | $21,351.32 | — | last bar red; gate last_red=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $630.17 | — |
| 2026-08-28 09:30 ET | **SHORT** | `XPOF` | 117 | $5.38 | $2.39 | — | $21,978.39 | — | last bar red; gate last_red=True; list probable,yday_gainer; ret5=+6.5; leftover $630.17 | — |
| 2026-08-28 09:30 ET | **SHORT** | `BHVN` | 39 | $15.88 | $2.14 | — | $22,595.56 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+19.4; leftover $630.17 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,595.56 | ▲ close $9,206.60 vs 09:30 $8,840.48 (session +408.88) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,595.56 | ▲ 09:30 equity $9,218.20 vs yday $9,206.60 (+11.60) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `MOS` | 25 | $23.68 | $2.06 | $-1.92 | $22,001.50 | ▼ -1.92 after sell → book $9,216.14; vs 09:30 mark -2.06 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `FLNC` | 68 | $10.82 | $2.19 | $+15.97 | $21,263.54 | ▲ +15.97 after sell → book $9,213.94; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AVEX` | 43 | $17.63 | $2.12 | $-9.44 | $20,503.34 | ▼ -9.44 after sell → book $9,211.82; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AXTI` | 11 | $60.05 | $2.02 | $+54.10 | $19,840.76 | ▲ +54.10 after sell → book $9,209.80; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `INDP` | 701 | $1.14 | $9.04 | $-53.28 | $19,032.58 | ▼ -53.28 after sell → book $9,200.76; vs 09:30 mark -9.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NVTS` | 60 | $11.45 | $2.17 | $+64.62 | $18,343.41 | ▲ +64.62 after sell → book $9,198.59; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `IRDM` | 16 | $46.64 | $2.04 | $+1.00 | $17,595.13 | ▲ +1.00 after sell → book $9,196.55; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,595.13 | ▼ close $9,167.62 vs 09:30 $9,218.20 (session -28.93) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,595.13 | ▲ 09:30 equity $9,218.25 vs yday $9,167.62 (+50.63) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `KURA` | 68 | $12.54 | $2.19 | $+25.49 | $16,740.22 | ▲ +25.49 after sell → book $9,216.06; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `AVBP` | 28 | $30.10 | $2.07 | $+15.13 | $15,895.34 | ▲ +15.13 after sell → book $9,213.98; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `ABX` | 91 | $9.43 | $2.26 | $+18.17 | $15,034.95 | ▲ +18.17 after sell → book $9,211.72; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `SENS` | 94 | $9.17 | $2.27 | $+10.45 | $14,170.70 | ▲ +10.45 after sell → book $9,209.45; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `ACRS` | 143 | $6.09 | $2.42 | $+3.68 | $13,297.41 | ▲ +3.68 after sell → book $9,207.03; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,297.41 | ▲ close $9,228.52 vs 09:30 $9,218.25 (session +21.49) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,297.41 | ▼ 09:30 equity $9,216.85 vs yday $9,228.52 (-11.67) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SEDG` | 19 | $32.42 | $2.05 | $+4.99 | $12,679.38 | ▲ +4.99 after sell → book $9,214.80; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `GRRR` | 40 | $13.92 | $2.11 | $+65.34 | $12,120.47 | ▲ +65.34 after sell → book $9,212.69; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `URBN` | 7 | $78.84 | $2.01 | $+0.00 | $11,566.58 | ▼ +0.00 after sell → book $9,210.68; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SAFX` | 1726 | $0.39 | $11.91 | $-66.85 | $10,881.53 | ▼ -66.85 after sell → book $9,198.77; vs 09:30 mark -11.91 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 2 | $235.71 | $2.00 | $+29.03 | $10,408.12 | ▲ +29.03 after sell → book $9,196.78; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `XPOF` | 117 | $5.03 | $2.34 | $+36.22 | $9,817.27 | ▲ +36.22 after sell → book $9,194.44; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `BHVN` | 39 | $15.97 | $2.11 | $-7.76 | $9,192.33 | ▼ -7.76 after sell → book $9,192.33; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,192.33 | ▲ close $9,192.33 vs 09:30 $9,216.85 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,192.33 | ▲ 09:30 equity $9,192.33 vs yday $9,192.33 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **SHORT** | `CRK` | 37 | $15.45 | $2.14 | — | $9,761.84 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $574.52 | — |
| 2026-09-03 09:30 ET | **SHORT** | `MRNA` | 3 | $145.94 | $2.03 | — | $10,197.65 | — | last bar red; gate last_red=True; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $574.52 | — |
| 2026-09-03 09:30 ET | **SHORT** | `EIX` | 10 | $55.42 | $2.06 | — | $10,749.79 | — | last bar red; gate last_red=True; list probable,yday_gainer; ret5=-25.9; leftover $574.52 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SAFX` | 1523 | $0.38 | $10.59 | — | $11,313.37 | — | last bar red; gate last_red=True; list probable,yday_gainer; ret5=-2.3; leftover $574.52 | — |
| 2026-09-03 09:30 ET | **SHORT** | `FRVO` | 31 | $18.28 | $2.12 | — | $11,877.93 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+16.5; leftover $574.52 | — |
| 2026-09-03 09:30 ET | **SHORT** | `DEFT` | 883 | $0.65 | $8.56 | — | $12,443.32 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+10.2; leftover $574.52 | — |
| 2026-09-03 09:30 ET | **SHORT** | `GMRS` | 44 | $12.83 | $2.16 | — | $13,005.69 | — | last bar red; gate last_red=True; list yday_gainer; ret5=-0.2; leftover $574.52 | — |
| 2026-09-03 09:30 ET | **SHORT** | `KLRA` | 36 | $15.95 | $2.13 | — | $13,577.75 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-14.0; leftover $574.52 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,577.75 | ▼ close $9,149.58 vs 09:30 $9,192.33 (session -10.98) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,577.75 | ▼ 09:30 equity $9,137.30 vs yday $9,149.58 (-12.28) | — | — |
| 2026-09-04 09:30 ET | **SHORT** | `CABA` | 165 | $3.46 | $2.54 | — | $14,146.11 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $571.08 | — |
| 2026-09-04 09:30 ET | **SHORT** | `ALEC` | 226 | $2.52 | $2.98 | — | $14,712.66 | — | last bar red; gate last_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $571.08 | — |
| 2026-09-04 09:30 ET | **SHORT** | `BHC` | 85 | $6.71 | $2.28 | — | $15,280.72 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $571.08 | — |
| 2026-09-04 09:30 ET | **SHORT** | `BMEA` | 300 | $1.90 | $3.95 | — | $15,846.78 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $571.08 | — |
| 2026-09-04 09:30 ET | **SHORT** | `OABI` | 119 | $4.78 | $2.39 | — | $16,413.20 | — | last bar red; gate last_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $571.08 | — |
| 2026-09-04 09:30 ET | **SHORT** | `OPK` | 359 | $1.59 | $4.72 | — | $16,979.30 | — | last bar red; gate last_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $571.08 | — |
| 2026-09-04 09:30 ET | **SHORT** | `VIR` | 50 | $11.31 | $2.18 | — | $17,542.62 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $571.08 | — |
| 2026-09-04 09:30 ET | **SHORT** | `ATRC` | 10 | $52.03 | $2.05 | — | $18,060.87 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $571.08 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,060.87 | ▼ close $9,067.20 vs 09:30 $9,137.30 (session -47.02) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,060.87 | ▲ 09:30 equity $9,104.26 vs yday $9,067.20 (+37.06) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,060.87 | ▲ close $9,133.31 vs 09:30 $9,104.26 (session +29.06) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,060.87 | ▲ 09:30 equity $9,177.15 vs yday $9,133.31 (+43.84) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `CRK` | 37 | $15.16 | $2.10 | $+6.49 | $17,497.85 | ▲ +6.49 after sell → book $9,175.05; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `MRNA` | 3 | $140.29 | $2.00 | $+12.92 | $17,074.96 | ▲ +12.92 after sell → book $9,173.05; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `EIX` | 10 | $59.49 | $2.02 | $-44.78 | $16,478.04 | ▼ -44.78 after sell → book $9,171.03; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `SAFX` | 1523 | $0.40 | $10.66 | $-56.28 | $15,858.18 | ▼ -56.28 after sell → book $9,160.37; vs 09:30 mark -10.66 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `FRVO` | 31 | $18.60 | $2.08 | $-14.12 | $15,279.50 | ▼ -14.12 after sell → book $9,158.29; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `DEFT` | 883 | $0.63 | $8.18 | $+4.45 | $14,718.56 | ▲ +4.45 after sell → book $9,150.11; vs 09:30 mark -8.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `GMRS` | 44 | $13.80 | $2.12 | $-46.96 | $14,109.24 | ▼ -46.96 after sell → book $9,147.99; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `KLRA` | 36 | $16.27 | $2.10 | $-15.75 | $13,521.42 | ▼ -15.75 after sell → book $9,145.89; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,521.42 | ▲ close $9,349.71 vs 09:30 $9,177.15 (session +203.82) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,521.42 | ▲ 09:30 equity $9,413.52 vs yday $9,349.71 (+63.81) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `CABA` | 165 | $2.85 | $2.48 | $+95.63 | $13,048.69 | ▲ +95.63 after sell → book $9,411.03; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `ALEC` | 226 | $2.22 | $2.92 | $+61.91 | $12,544.05 | ▲ +61.91 after sell → book $9,408.11; vs 09:30 mark -2.92 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `BHC` | 85 | $6.11 | $2.25 | $+46.47 | $12,022.46 | ▲ +46.47 after sell → book $9,405.87; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `BMEA` | 300 | $1.83 | $3.87 | $+13.18 | $11,469.59 | ▲ +13.18 after sell → book $9,402.00; vs 09:30 mark -3.87 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `OABI` | 119 | $3.92 | $2.35 | $+97.36 | $11,000.52 | ▲ +97.36 after sell → book $9,399.65; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `OPK` | 359 | $1.53 | $4.63 | $+12.19 | $10,446.62 | ▲ +12.19 after sell → book $9,395.02; vs 09:30 mark -4.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `VIR` | 50 | $10.57 | $2.14 | $+32.68 | $9,915.98 | ▲ +32.68 after sell → book $9,392.88; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `ATRC` | 10 | $52.31 | $2.02 | $-6.87 | $9,390.86 | ▼ -6.87 after sell → book $9,390.86; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,390.86 | ▲ close $9,390.86 vs 09:30 $9,413.52 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,390.86 | ▲ 09:30 equity $9,390.86 vs yday $9,390.86 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **SHORT** | `AUPH` | 36 | $16.28 | $2.13 | — | $9,974.81 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=-1.1; leftover $586.93 | — |
| 2026-09-11 09:30 ET | **SHORT** | `OVID` | 214 | $2.73 | $2.82 | — | $10,556.21 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=-3.0; leftover $586.93 | — |
| 2026-09-11 09:30 ET | **SHORT** | `ORCL` | 3 | $164.43 | $2.03 | — | $11,047.46 | — | last bar red; gate last_red=True; list flatten,earn_react; ⚪; ret5=+4.9; leftover $586.93 | — |
| 2026-09-11 09:30 ET | **SHORT** | `NVT` | 3 | $157.78 | $2.03 | — | $11,518.77 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+4.7; leftover $586.93 | — |
| 2026-09-11 09:30 ET | **SHORT** | `NAVN` | 28 | $20.61 | $2.11 | — | $12,093.74 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-24.7; leftover $586.93 | — |
| 2026-09-11 09:30 ET | **SHORT** | `SLBT` | 280 | $2.09 | $3.68 | — | $12,675.26 | — | last bar red; gate last_red=True; list yday_mover; ret5=-36.6; leftover $586.93 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BHVN` | 45 | $13.03 | $2.16 | — | $13,259.45 | — | last bar red; gate last_red=True; list yday_mover; ret5=-18.5; leftover $586.93 | — |
| 2026-09-11 09:30 ET | **SHORT** | `AEO` | 39 | $14.71 | $2.14 | — | $13,830.99 | — | last bar red; gate last_red=True; list yday_mover; ret5=-12.8; leftover $586.93 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,830.99 | ▲ close $9,414.66 vs 09:30 $9,390.86 (session +42.92) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,830.99 | ▲ 09:30 equity $9,491.13 vs yday $9,414.66 (+76.47) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,830.99 | ▼ close $9,431.05 vs 09:30 $9,491.13 (session -60.08) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,830.99 | ▲ 09:30 equity $9,442.36 vs yday $9,431.05 (+11.31) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,830.99 | ▲ close $9,513.47 vs 09:30 $9,442.36 (session +71.11) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,830.99 | ▼ 09:30 equity $9,505.61 vs yday $9,513.47 (-7.86) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `AUPH` | 36 | $16.16 | $2.10 | $+0.09 | $13,247.14 | ▲ +0.09 after sell → book $9,503.52; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `OVID` | 214 | $2.72 | $2.76 | $-3.44 | $12,662.29 | ▼ -3.44 after sell → book $9,500.75; vs 09:30 mark -2.77 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `ORCL` | 3 | $140.03 | $2.00 | $+69.17 | $12,240.21 | ▲ +69.17 after sell → book $9,498.76; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `NVT` | 3 | $147.79 | $2.00 | $+25.94 | $11,794.84 | ▲ +25.94 after sell → book $9,496.76; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `NAVN` | 28 | $22.50 | $2.07 | $-57.10 | $11,162.76 | ▼ -57.10 after sell → book $9,494.68; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `SLBT` | 280 | $1.91 | $3.61 | $+43.10 | $10,624.35 | ▲ +43.10 after sell → book $9,491.07; vs 09:30 mark -3.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BHVN` | 45 | $12.34 | $2.12 | $+26.76 | $10,066.93 | ▲ +26.76 after sell → book $9,488.95; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `AEO` | 39 | $14.82 | $2.11 | $-8.54 | $9,486.84 | ▼ -8.54 after sell → book $9,486.84; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SHORT** | `AVAH` | 41 | $14.31 | $2.15 | — | $10,071.40 | — | last bar red; gate last_red=True; list flatten; ret5=+4.8; leftover $592.93 | — |
| 2026-09-16 09:30 ET | **SHORT** | `WAY` | 22 | $26.27 | $2.09 | — | $10,647.25 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+10.0; leftover $592.93 | — |
| 2026-09-16 09:30 ET | **SHORT** | `SWRD` | 296 | $2.00 | $3.89 | — | $11,235.35 | — | last bar red; gate last_red=True; list yday_mover; ret5=+0.0; leftover $592.93 | — |
| 2026-09-16 09:30 ET | **SHORT** | `ALHC` | 57 | $10.30 | $2.20 | — | $11,820.26 | — | last bar red; gate last_red=True; list yday_mover; ret5=-23.0; leftover $592.93 | — |
| 2026-09-16 09:30 ET | **SHORT** | `PLAY` | 86 | $6.86 | $2.29 | — | $12,407.93 | — | last bar red; gate last_red=True; list yday_mover; ret5=-22.4; leftover $592.93 | — |
| 2026-09-16 09:30 ET | **SHORT** | `DVLT` | 3705 | $0.16 | $17.68 | — | $12,983.04 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-23.8; leftover $592.93 | — |
| 2026-09-16 09:30 ET | **SHORT** | `USDE` | 97 | $6.06 | $2.32 | — | $13,568.54 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-19.6; leftover $592.93 | — |
| 2026-09-16 09:30 ET | **SHORT** | `NMRA` | 702 | $0.84 | $8.17 | — | $14,152.86 | — | last bar red; gate last_red=True; list yday_mover; ret5=-33.5; leftover $592.93 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,152.86 | ▲ close $9,550.08 vs 09:30 $9,505.61 (session +104.04) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,152.86 | ▲ 09:30 equity $9,566.79 vs yday $9,550.08 (+16.71) | — | — |
| 2026-09-17 09:30 ET | **SHORT** | `CYPH` | 255 | $2.67 | $3.36 | — | $14,831.62 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-0.4; leftover $683.34 | — |
| 2026-09-17 09:30 ET | **SHORT** | `MRLN` | 301 | $2.27 | $3.96 | — | $15,510.93 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-29.6; leftover $683.34 | — |
| 2026-09-17 09:30 ET | **SHORT** | `PALI` | 390 | $1.75 | $5.12 | — | $16,188.30 | — | last bar red; gate last_red=True; list yday_mover; ret5=-17.6; leftover $683.34 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BAK` | 386 | $1.77 | $5.07 | — | $16,866.45 | — | last bar red; gate last_red=True; list yday_mover; ret5=-10.2; leftover $683.34 | — |
| 2026-09-17 09:30 ET | **SHORT** | `JBHT` | 2 | $238.60 | $2.03 | — | $17,341.62 | — | last bar red; gate last_red=True; list yday_mover; ret5=-11.6; leftover $683.34 | — |
| 2026-09-17 09:30 ET | **SHORT** | `INDP` | 207 | $3.30 | $2.73 | — | $18,021.99 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=+54.3; leftover $683.34 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BTGO` | 104 | $6.56 | $2.35 | — | $18,701.88 | — | last bar red; gate last_red=True; list yday_mover; ret5=-14.6; leftover $683.34 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,701.88 | ▼ close $9,242.90 vs 09:30 $9,566.79 (session -299.26) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,701.88 | ▼ 09:30 equity $9,054.46 vs yday $9,242.90 (-188.44) | — | — |
| 2026-09-18 09:30 ET | **SHORT** | `GNRC` | 10 | $209.52 | $2.10 | — | $20,794.98 | — | last bar red; gate last_red=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $2263.62 | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 65 | $34.44 | $2.28 | — | $23,031.30 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+14.0; leftover $2263.62 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23,031.30 | ▲ close $9,202.75 vs 09:30 $9,054.46 (session +152.67) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23,031.30 | ▼ 09:30 equity $8,631.07 vs yday $9,202.75 (-571.68) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `AVAH` | 41 | $13.65 | $2.11 | $+22.80 | $22,469.54 | ▲ +22.80 after sell → book $8,628.95; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `WAY` | 22 | $25.94 | $2.06 | $+3.11 | $21,896.80 | ▲ +3.11 after sell → book $8,626.90; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `SWRD` | 296 | $2.15 | $3.82 | $-52.11 | $21,256.58 | ▼ -52.11 after sell → book $8,623.08; vs 09:30 mark -3.82 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `ALHC` | 57 | $8.33 | $2.16 | $+107.93 | $20,779.61 | ▲ +107.93 after sell → book $8,620.92; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `PLAY` | 86 | $6.68 | $2.25 | $+10.94 | $20,202.89 | ▲ +10.94 after sell → book $8,618.67; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `DVLT` | 3705 | $0.16 | $17.04 | $-34.73 | $19,593.04 | ▼ -34.73 after sell → book $8,601.63; vs 09:30 mark -17.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `USDE` | 97 | $13.05 | $2.28 | $-682.63 | $18,324.91 | ▼ -682.63 after sell → book $8,599.35; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `NMRA` | 702 | $0.74 | $7.28 | $+59.66 | $17,800.26 | ▲ +59.66 after sell → book $8,592.07; vs 09:30 mark -7.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SHORT** | `PGEN` | 68 | $7.84 | $2.23 | — | $18,331.15 | — | last bar red; gate last_red=True; list flatten; ret5=+13.6; leftover $537.00 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 65 | $8.26 | $2.22 | — | $18,865.83 | — | last bar red; gate last_red=True; list yday_mover; ret5=+7.7; leftover $537.00 | — |
| 2026-09-21 09:30 ET | **SHORT** | `XENE` | 13 | $40.00 | $2.06 | — | $19,383.76 | — | last bar red; gate last_red=True; list yday_mover; ret5=-32.2; leftover $537.00 | — |
| 2026-09-21 09:30 ET | **SHORT** | `SION` | 89 | $6.00 | $2.30 | — | $19,915.47 | — | last bar red; gate last_red=True; list yday_mover; ret5=-24.1; leftover $537.00 | — |
| 2026-09-21 09:30 ET | **SHORT** | `KDK` | 157 | $3.42 | $2.51 | — | $20,449.90 | — | last bar red; gate last_red=True; list yday_mover; ret5=-12.3; leftover $537.00 | — |
| 2026-09-21 09:30 ET | **SHORT** | `ABVX` | 5 | $105.72 | $2.04 | — | $20,976.46 | — | last bar red; gate last_red=True; list overnight; ret5=-11.5; leftover $537.00 | — |
| 2026-09-21 09:30 ET | **SHORT** | `MLKN` | 25 | $20.85 | $2.10 | — | $21,495.61 | — | last bar red; gate last_red=True; list overnight; ret5=-2.5; leftover $537.00 | — |
| 2026-09-21 09:30 ET | **SHORT** | `THO` | 7 | $68.39 | $2.04 | — | $21,972.29 | — | last bar red; gate last_red=True; list overnight; ret5=-7.0; leftover $537.00 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,972.29 | ▲ close $8,791.40 vs 09:30 $8,631.07 (session +216.84) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,972.29 | ▼ 09:30 equity $8,764.36 vs yday $8,791.40 (-27.04) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `CYPH` | 255 | $3.51 | $3.29 | $-219.57 | $21,073.95 | ▼ -219.57 after sell → book $8,761.07; vs 09:30 mark -3.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **COVER** | `MRLN` | 301 | $1.87 | $3.88 | $+112.56 | $20,507.20 | ▲ +112.56 after sell → book $8,757.19; vs 09:30 mark -3.88 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **COVER** | `PALI` | 390 | $1.69 | $5.03 | $+13.24 | $19,843.07 | ▲ +13.24 after sell → book $8,752.16; vs 09:30 mark -5.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **COVER** | `INDP` | 207 | $3.10 | $2.67 | $+36.00 | $19,198.70 | ▲ +36.00 after sell → book $8,749.49; vs 09:30 mark -2.67 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **COVER** | `BTGO` | 104 | $7.81 | $2.30 | $-134.65 | $18,384.16 | ▼ -134.65 after sell → book $8,747.19; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SHORT** | `DEFT` | 942 | $0.58 | $8.47 | — | $18,922.05 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $546.70 | — |
| 2026-09-22 09:30 ET | **SHORT** | `GLND` | 185 | $2.94 | $2.60 | — | $19,463.35 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+136.1; leftover $546.70 | — |
| 2026-09-22 09:30 ET | **SHORT** | `USDE` | 42 | $12.99 | $2.15 | — | $20,006.77 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+69.4; leftover $546.70 | — |
| 2026-09-22 09:30 ET | **SHORT** | `MX` | 171 | $3.18 | $2.56 | — | $20,548.00 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+11.1; leftover $546.70 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,548.00 | ▲ close $8,828.87 vs 09:30 $8,764.36 (session +97.46) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,548.00 | ▼ 09:30 equity $8,639.51 vs yday $8,828.87 (-189.36) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `BAK` | 386 | $1.68 | $4.98 | $+24.69 | $19,894.54 | ▲ +24.69 after sell → book $8,634.53; vs 09:30 mark -4.98 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `JBHT` | 2 | $236.50 | $2.00 | $+0.17 | $19,419.54 | ▲ +0.17 after sell → book $8,632.54; vs 09:30 mark -1.99 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `GNRC` | 10 | $204.39 | $2.02 | $+47.18 | $17,373.62 | ▲ +47.18 after sell → book $8,630.52; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `FIVN` | 65 | $38.91 | $2.19 | $-294.69 | $14,842.61 | ▼ -294.69 after sell → book $8,628.33; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 6 | $116.85 | $2.05 | — | $15,541.67 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+3.3; leftover $719.03 | — |
| 2026-09-23 09:30 ET | **SHORT** | `FTRE` | 35 | $20.25 | $2.13 | — | $16,248.28 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+15.0; leftover $719.03 | — |
| 2026-09-23 09:30 ET | **SHORT** | `MAZE` | 25 | $28.30 | $2.10 | — | $16,953.68 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $719.03 | — |
| 2026-09-23 09:30 ET | **SHORT** | `BLLN` | 6 | $116.00 | $2.05 | — | $17,647.63 | — | last bar red; gate last_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $719.03 | — |
| 2026-09-23 09:30 ET | **SHORT** | `VICR` | 2 | $266.50 | $2.03 | — | $18,178.60 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.2; leftover $719.03 | — |
| 2026-09-23 09:30 ET | **SHORT** | `DNA` | 78 | $9.13 | $2.27 | — | $18,888.47 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+13.8; leftover $719.03 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,888.47 | ▲ close $8,823.11 vs 09:30 $8,639.51 (session +207.41) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,888.47 | ▲ 09:30 equity $8,846.55 vs yday $8,823.11 (+23.44) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `PGEN` | 68 | $7.38 | $2.19 | $+26.86 | $18,384.44 | ▲ +26.86 after sell → book $8,844.35; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `AEHL` | 65 | $8.21 | $2.19 | $-1.16 | $17,848.60 | ▼ -1.16 after sell → book $8,842.17; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `XENE` | 13 | $36.50 | $2.03 | $+41.41 | $17,372.07 | ▲ +41.41 after sell → book $8,840.14; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `SION` | 89 | $5.50 | $2.26 | $+39.95 | $16,880.32 | ▲ +39.95 after sell → book $8,837.88; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `KDK` | 157 | $2.96 | $2.46 | $+67.25 | $16,413.14 | ▲ +67.25 after sell → book $8,835.42; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `ABVX` | 5 | $92.97 | $2.00 | $+59.71 | $15,946.28 | ▲ +59.71 after sell → book $8,833.42; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `MLKN` | 25 | $19.96 | $2.06 | $+18.09 | $15,445.22 | ▲ +18.09 after sell → book $8,831.35; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `THO` | 7 | $72.58 | $2.01 | $-33.39 | $14,935.15 | ▼ -33.39 after sell → book $8,829.34; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,935.15 | ▼ close $8,154.94 vs 09:30 $8,846.55 (session -674.40) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,596.59 | ▼ 09:30 equity $9,859.37 vs yday $10,124.83 (-265.46) | 09:30 open · cash $20,596.59 (unchanged overnight, no fees) · equity $9,859.37 vs prior close $10,124.83 (-265.46) · 14 name(s) re-marked at the open (per-name table). CBRL×15 yday $51.84 → 09:30 $52.39 -8.25; CMPX×608 yday $1.13 → 09:30 $1.13 -0.00; DEFT×1262 yday $0.53 → 09:30 $0.53 -0.00; DLO×51 yday $13.88 → 09:30 $13.88 -0.00; EMAT×211 yday $3.21 → 09:30 $3.21 -0.00; EVER×38 yday $18.06 → 09:30 $18.06 -0.00; GIS×20 yday $34.83 → 09:30 $34.83 -0.00; GLND×253 yday $5.35 → 09:30 $6.06 -179.63; KBH×15 yday $47.65 → 09:30 $47.65 -0.00; MX×234 yday $3.18 → 09:30 $3.18 -0.00; PACS×17 yday $41.46 → 09:30 $41.46 -0.00; PAYX×6 yday $101.59 → 09:30 $101.59 -0.00; USDE×57 yday $14.22 → 09:30 $15.58 -77.58; XNDU×123 yday $5.10 → 09:30 $5.10 -0.00 | — |
| 2026-09-25 09:30 ET | **COVER** | `GLND` | 253 | $6.06 | $3.26 | $-795.96 | $19,060.15 | ▼ -795.96 after sell → book $9,856.11; vs 09:30 mark -3.26 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **COVER** | `USDE` | 57 | $15.58 | $2.16 | $-152.05 | $18,169.86 | ▼ -152.05 after sell → book $9,853.95; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `OMER` | 29 | $20.61 | $2.11 | — | $18,765.44 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+9.1; leftover $615.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `NEOV` | 257 | $2.39 | $3.38 | — | $19,376.28 | — | last bar red; gate last_red=True; list yday_mover; ret5=-31.4; leftover $615.87 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `SFIX` | 279 | $2.20 | $3.67 | — | $19,986.41 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-24.1; leftover $615.87 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `LRMR` | 185 | $3.32 | $2.60 | — | $20,598.01 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-12.6; leftover $615.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `ACAD` | 27 | $22.21 | $2.11 | — | $21,195.57 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-19.2; leftover $615.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `SGMT` | 67 | $9.11 | $2.23 | — | $21,803.71 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $615.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `SMWB` | 82 | $7.50 | $2.28 | — | $22,416.44 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-9.7; leftover $615.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,416.44 | ▲ close $9,994.95 vs 09:30 $9,859.37 (session +159.39) | 16:00 close · cash $22,416.44 · equity $9,994.95 vs 09:30 $9,859.37 (+135.58; session marks +159.39) · 19 name(s) marked open→close (per-name table). CBRL×15 09:30 $52.39 → close $51.81 +8.70; CMPX×608 09:30 $1.14 → close $1.14 +0.00; DEFT×1262 09:30 $0.53 → close $0.53 -0.00; DLO×51 09:30 $13.88 → close $13.88 -0.00; EMAT×211 09:30 $3.21 → close $3.21 -0.00; EVER×38 09:30 $18.06 → close $18.06 +0.00; GIS×20 09:30 $34.83 → close $34.83 -0.00; KBH×15 09:30 $47.65 → close $47.65 -0.00; MX×234 09:30 $3.18 → close $3.18 -0.00; PACS×17 09:30 $41.46 → close $41.46 +0.00; PAYX×6 09:30 $101.59 → close $101.59 +0.00; XNDU×123 09:30 $5.10 → close $5.10 +0.00; OMER×29 09:30 $20.61 → close $20.08 +15.37; NEOV×257 09:30 $2.39 → close $2.19 +51.40; SFIX×279 09:30 $2.20 → close $2.15 +12.56; LRMR×185 09:30 $3.32 → close $3.08 +45.32; ACAD×27 09:30 $22.21 → close $20.68 +41.31; SGMT×67 09:30 $9.11 → close $9.24 -8.71; SMWB×82 09:30 $7.50 → close $7.58 -6.56 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `FOSL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BIRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `FOSL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BIRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ELF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BYND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TBPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENHA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENVX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INV` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ELF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BYND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `INMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRVI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CRCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WYFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TOYO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CRCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WYFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TOYO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRSP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FUTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `GMAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ENHA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `PRQR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `XHG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AEM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `FUTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `GMAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ENHA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `PRQR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `MOS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RZLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `HCA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `PUSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AVEX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `NVTS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `IRDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AVEX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AXTI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `NVTS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `IRDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `ABX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SENS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `ACRS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `AVBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ACRS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `XPOF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `XPOF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ELMT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FOX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BEP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VLRS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `GMRS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `KLRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `EIX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `GMRS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `KLRA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `XLAB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CNTB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HAS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LAC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TTAN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FJET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `OVID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `NVT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `NAVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SLBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AEO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ON` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SYNA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CDW` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ADBT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OVID` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `NAVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SLBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `AEO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TRX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PANW` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `HQ` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `XHLD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ARQQ` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SWRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `PLAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `NMRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SWRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ALHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `PLAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `NMRA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `MRLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PALI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `JBHT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BTGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `MRLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `PALI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `JBHT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BTGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `GNRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BAK` | no_price | no 09:30 open — carry |
| 2026-09-22 | `JBHT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GNRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `FIVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `AEHL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `XENE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SION` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `KDK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ABVX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MLKN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `THO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-23 | `XENE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `KDK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ABVX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MLKN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `THO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `GLND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `MX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `GLND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `MX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `MAZE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BLLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EGHT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SRFM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `LU` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CGEM` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DEFT` | 942 | 2026-09-22 @ $0.58 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $546.70 |
| `GLND` | 185 | 2026-09-22 @ $2.94 | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+136.1; leftover $546.70 |
| `USDE` | 42 | 2026-09-22 @ $12.99 | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+69.4; leftover $546.70 |
| `MX` | 171 | 2026-09-22 @ $3.18 | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+11.1; leftover $546.70 |
| `HALO` | 6 | 2026-09-23 @ $116.85 | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+3.3; leftover $719.03 |
| `FTRE` | 35 | 2026-09-23 @ $20.25 | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+15.0; leftover $719.03 |
| `MAZE` | 25 | 2026-09-23 @ $28.30 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $719.03 |
| `BLLN` | 6 | 2026-09-23 @ $116.00 | last bar red; gate last_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $719.03 |
| `VICR` | 2 | 2026-09-23 @ $266.50 | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.2; leftover $719.03 |
| `DNA` | 78 | 2026-09-23 @ $9.13 | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+13.8; leftover $719.03 |
