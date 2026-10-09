# Factor mine action — `short_clk_ext_veto_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · Clock-B #5 extreme ext + fade/fail-break as a short

Cash book **-4.86%** ($9,514) · signal-only (no cash/fees) was -97.41%. Starts YES **0/30**. Fills 242 · skips 219 · realized $-964.15.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: Clock-B #5 long veto: extreme prior extension plus diminishing progress or a failed breakout.

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
- **Gate** `clk_ext_veto=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $15,682.22.

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
| 2026-08-13 09:30 ET | **SHORT** | `IREN` | 54 | $45.98 | $2.25 | — | $12,480.67 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list flatten; ⚪; ret5=+12.3; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **SHORT** | `TPG` | 49 | $50.62 | $2.23 | — | $14,958.97 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list flatten; ⚪; ret5=+6.2; leftover $2500.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,958.97 | ▼ close $9,865.55 vs 09:30 $10,000.00 (session -129.96) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,958.97 | ▲ 09:30 equity $9,868.90 vs yday $9,865.55 (+3.35) | — | — |
| 2026-08-14 09:30 ET | **SHORT** | `ARX` | 31 | $19.57 | $2.12 | — | $15,563.52 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $616.81 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LIFE` | 17 | $35.04 | $2.08 | — | $16,157.13 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $616.81 | — |
| 2026-08-14 09:30 ET | **SHORT** | `VOYG` | 13 | $44.49 | $2.06 | — | $16,733.43 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+15.6; leftover $616.81 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 32 | $19.17 | $2.12 | — | $17,344.75 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $616.81 | — |
| 2026-08-14 09:30 ET | **SHORT** | `BETA` | 24 | $25.21 | $2.10 | — | $17,947.69 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $616.81 | — |
| 2026-08-14 09:30 ET | **SHORT** | `FORM` | 4 | $129.48 | $2.04 | — | $18,463.57 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+14.3; leftover $616.81 | — |
| 2026-08-14 09:30 ET | **SHORT** | `ENTG` | 3 | $162.45 | $2.03 | — | $18,948.89 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+14.8; leftover $616.81 | — |
| 2026-08-14 09:30 ET | **SHORT** | `SATL` | 103 | $5.98 | $2.34 | — | $19,562.49 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+16.9; leftover $616.81 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,562.49 | ▲ close $10,026.68 vs 09:30 $9,868.90 (session +174.67) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,562.49 | ▼ 09:30 equity $9,947.11 vs yday $10,026.68 (-79.57) | — | — |
| 2026-08-17 09:30 ET | **SHORT** | `CAPR` | 103 | $6.87 | $2.35 | — | $20,267.75 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; ret5=+62.6; leftover $710.51 | — |
| 2026-08-17 09:30 ET | **SHORT** | `UMAC` | 21 | $32.55 | $2.09 | — | $20,949.21 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $710.51 | — |
| 2026-08-17 09:30 ET | **SHORT** | `LPTH` | 47 | $14.94 | $2.17 | — | $21,649.22 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $710.51 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ALOY` | 48 | $14.66 | $2.17 | — | $22,350.73 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $710.51 | — |
| 2026-08-17 09:30 ET | **SHORT** | `AEHR` | 5 | $132.79 | $2.04 | — | $23,012.63 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer; ⚪; ret5=+30.1; leftover $710.51 | — |
| 2026-08-17 09:30 ET | **SHORT** | `CLYM` | 43 | $16.25 | $2.16 | — | $23,709.23 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,ohlc_hot; ret5=+16.6; leftover $710.51 | — |
| 2026-08-17 09:30 ET | **SHORT** | `SGMT` | 67 | $10.45 | $2.23 | — | $24,407.14 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+16.4; leftover $710.51 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,407.14 | ▼ close $9,825.62 vs 09:30 $9,947.11 (session -106.27) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,407.14 | ▲ 09:30 equity $10,260.43 vs yday $9,825.62 (+434.81) | — | — |
| 2026-08-18 09:30 ET | **COVER** | `IREN` | 54 | $43.56 | $2.15 | $+126.28 | $22,052.75 | ▲ +126.28 after sell → book $10,258.28; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **COVER** | `TPG` | 49 | $51.77 | $2.14 | $-60.56 | $19,513.89 | ▼ -60.56 after sell → book $10,256.15; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,513.89 | ▼ close $10,220.05 vs 09:30 $10,260.43 (session -36.10) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,513.89 | ▼ 09:30 equity $10,155.79 vs yday $10,220.05 (-64.26) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `ARX` | 31 | $19.58 | $2.08 | $-4.51 | $18,904.82 | ▼ -4.51 after sell → book $10,153.70; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `LIFE` | 17 | $34.37 | $2.04 | $+7.27 | $18,318.49 | ▲ +7.27 after sell → book $10,151.66; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `VOYG` | 13 | $41.93 | $2.03 | $+29.19 | $17,771.37 | ▲ +29.19 after sell → book $10,149.63; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `LUNR` | 32 | $18.98 | $2.09 | $+1.87 | $17,161.93 | ▲ +1.87 after sell → book $10,147.55; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `FORM` | 4 | $126.03 | $2.00 | $+9.76 | $16,655.80 | ▲ +9.76 after sell → book $10,145.54; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `ENTG` | 3 | $152.52 | $2.00 | $+25.76 | $16,196.25 | ▲ +25.76 after sell → book $10,143.55; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `SATL` | 103 | $5.82 | $2.30 | $+11.84 | $15,594.49 | ▲ +11.84 after sell → book $10,141.25; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,594.49 | ▲ close $10,327.61 vs 09:30 $10,155.79 (session +186.36) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,594.49 | ▲ 09:30 equity $10,418.75 vs yday $10,327.61 (+91.14) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `BETA` | 24 | $26.16 | $2.06 | $-26.96 | $14,964.58 | ▼ -26.96 after sell → book $10,416.68; vs 09:30 mark -2.07 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `CAPR` | 103 | $7.66 | $2.30 | $-86.01 | $14,173.31 | ▼ -86.01 after sell → book $10,414.39; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `UMAC` | 21 | $28.32 | $2.05 | $+84.69 | $13,576.53 | ▲ +84.69 after sell → book $10,412.33; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `LPTH` | 47 | $13.09 | $2.13 | $+82.65 | $12,959.17 | ▲ +82.65 after sell → book $10,410.20; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ALOY` | 48 | $12.06 | $2.13 | $+120.49 | $12,378.16 | ▲ +120.49 after sell → book $10,408.07; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `AEHR` | 5 | $106.01 | $2.00 | $+129.85 | $11,846.10 | ▲ +129.85 after sell → book $10,406.06; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `CLYM` | 43 | $17.16 | $2.12 | $-43.41 | $11,106.10 | ▼ -43.41 after sell → book $10,403.94; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `SGMT` | 67 | $10.48 | $2.19 | $-6.43 | $10,401.75 | ▼ -6.43 after sell → book $10,401.75; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `WPM` | 4 | $144.54 | $2.04 | — | $10,977.87 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $650.11 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $11,589.19 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $650.11 | — |
| 2026-08-20 09:30 ET | **SHORT** | `SCZM` | 68 | $9.46 | $2.23 | — | $12,230.24 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $650.11 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AZI` | 474 | $1.37 | $6.22 | — | $12,873.39 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $650.11 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEM` | 10 | $61.83 | $2.06 | — | $13,489.64 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $650.11 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 55 | $11.81 | $2.19 | — | $14,137.27 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $650.11 | — |
| 2026-08-20 09:30 ET | **SHORT** | `SENS` | 72 | $8.91 | $2.25 | — | $14,776.54 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $650.11 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ALEC` | 270 | $2.40 | $3.56 | — | $15,420.99 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+13.0; leftover $650.11 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,420.99 | ▼ close $10,289.54 vs 09:30 $10,418.75 (session -89.64) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,420.99 | ▼ 09:30 equity $10,190.34 vs yday $10,289.54 (-99.20) | — | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARCT` | 57 | $11.13 | $2.20 | — | $16,053.20 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $636.90 | — |
| 2026-08-21 09:30 ET | **SHORT** | `CRDL` | 329 | $1.93 | $4.33 | — | $16,683.84 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $636.90 | — |
| 2026-08-21 09:30 ET | **SHORT** | `CAN` | 2166 | $0.29 | $13.25 | — | $17,307.40 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $636.90 | — |
| 2026-08-21 09:30 ET | **SHORT** | `PRQR` | 279 | $2.28 | $3.67 | — | $17,939.84 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer; 🔵; ⚪; ret5=+22.7; leftover $636.90 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MSTR` | 5 | $119.69 | $2.04 | — | $18,536.25 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,ohlc_hot; ret5=+15.7; leftover $636.90 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ABTC` | 73 | $8.66 | $2.25 | — | $19,166.18 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $636.90 | — |
| 2026-08-21 09:30 ET | **SHORT** | `CAI` | 25 | $24.73 | $2.10 | — | $19,782.33 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer; 🔵; ⚪; ret5=+10.8; leftover $636.90 | — |
| 2026-08-21 09:30 ET | **SHORT** | `XHG` | 141 | $4.49 | $2.46 | — | $20,412.96 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+12.7; leftover $636.90 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,412.96 | ▼ close $9,848.56 vs 09:30 $10,190.34 (session -309.48) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,412.96 | ▼ 09:30 equity $9,826.27 vs yday $9,848.56 (-22.29) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,412.96 | ▲ close $9,938.21 vs 09:30 $9,826.27 (session +111.94) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,412.96 | ▲ 09:30 equity $9,981.24 vs yday $9,938.21 (+43.03) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `WPM` | 4 | $156.51 | $2.00 | $-51.92 | $19,784.91 | ▼ -51.92 after sell → book $9,979.23; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $212.00 | $2.00 | $-26.68 | $19,146.92 | ▼ -26.68 after sell → book $9,977.24; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `SCZM` | 68 | $9.45 | $2.19 | $-3.75 | $18,502.12 | ▼ -3.75 after sell → book $9,975.04; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AZI` | 474 | $1.31 | $6.11 | $+16.10 | $17,875.07 | ▲ +16.10 after sell → book $9,968.93; vs 09:30 mark -6.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEM` | 10 | $66.58 | $2.02 | $-51.58 | $17,207.25 | ▼ -51.58 after sell → book $9,966.91; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 55 | $11.00 | $2.15 | $+40.48 | $16,600.09 | ▲ +40.48 after sell → book $9,964.75; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `SENS` | 72 | $9.36 | $2.21 | $-36.85 | $15,923.97 | ▼ -36.85 after sell → book $9,962.55; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ALEC` | 270 | $2.32 | $3.48 | $+14.56 | $15,294.08 | ▲ +14.56 after sell → book $9,959.06; vs 09:30 mark -3.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `LIFE` | 19 | $36.96 | $2.09 | — | $15,994.24 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $711.36 | — |
| 2026-08-25 09:30 ET | **SHORT** | `CYPH` | 456 | $1.56 | $5.99 | — | $16,699.61 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $711.36 | — |
| 2026-08-25 09:30 ET | **SHORT** | `RUM` | 75 | $9.42 | $2.26 | — | $17,403.85 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $711.36 | — |
| 2026-08-25 09:30 ET | **SHORT** | `DFDV` | 175 | $4.06 | $2.57 | — | $18,111.78 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer; 🔵; ret5=+29.4; leftover $711.36 | — |
| 2026-08-25 09:30 ET | **SHORT** | `NIQ` | 37 | $19.00 | $2.14 | — | $18,812.64 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+11.2; leftover $711.36 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 52 | $13.62 | $2.19 | — | $19,518.95 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $711.36 | — |
| 2026-08-25 09:30 ET | **SHORT** | `WIX` | 8 | $83.15 | $2.05 | — | $20,182.10 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+14.5; leftover $711.36 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,182.10 | ▼ close $9,344.81 vs 09:30 $9,981.24 (session -594.97) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,182.10 | ▲ 09:30 equity $9,559.01 vs yday $9,344.81 (+214.20) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `ARCT` | 57 | $15.35 | $2.16 | $-244.90 | $19,304.99 | ▼ -244.90 after sell → book $9,556.85; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `CAN` | 2166 | $0.40 | $15.10 | $-251.45 | $18,429.99 | ▼ -251.45 after sell → book $9,541.75; vs 09:30 mark -15.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `PRQR` | 279 | $2.38 | $3.60 | $-35.17 | $17,762.37 | ▼ -35.17 after sell → book $9,538.15; vs 09:30 mark -3.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MSTR` | 5 | $123.26 | $2.00 | $-21.90 | $17,144.07 | ▼ -21.90 after sell → book $9,536.15; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ABTC` | 73 | $8.84 | $2.21 | $-17.60 | $16,496.54 | ▼ -17.60 after sell → book $9,533.94; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `CAI` | 25 | $27.70 | $2.06 | $-78.42 | $15,801.97 | ▼ -78.42 after sell → book $9,531.87; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `SENS` | 83 | $9.48 | $2.28 | — | $16,586.53 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $794.32 | — |
| 2026-08-26 09:30 ET | **SHORT** | `KURA` | 58 | $13.63 | $2.21 | — | $17,374.86 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+10.2; leftover $794.32 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CAPR` | 95 | $8.29 | $2.32 | — | $18,160.09 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $794.32 | — |
| 2026-08-26 09:30 ET | **SHORT** | `FIGR` | 19 | $40.50 | $2.09 | — | $18,927.50 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+15.8; leftover $794.32 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 65 | $12.22 | $2.23 | — | $19,719.57 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+12.4; leftover $794.32 | — |
| 2026-08-26 09:30 ET | **SHORT** | `HTFL` | 15 | $50.02 | $2.08 | — | $20,467.80 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+10.3; leftover $794.32 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,467.80 | ▼ close $9,498.22 vs 09:30 $9,559.01 (session -20.44) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,467.80 | ▼ 09:30 equity $9,392.31 vs yday $9,498.22 (-105.91) | — | — |
| 2026-08-27 09:30 ET | **COVER** | `CRDL` | 329 | $2.09 | $4.24 | $-61.21 | $19,775.95 | ▼ -61.21 after sell → book $9,388.07; vs 09:30 mark -4.24 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SHORT** | `SUJA` | 166 | $9.41 | $2.57 | — | $21,335.43 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_mover; ret5=+27.7; leftover $1564.68 | — |
| 2026-08-27 09:30 ET | **SHORT** | `OABI` | 325 | $4.81 | $4.30 | — | $22,894.38 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+14.8; leftover $1564.68 | — |
| 2026-08-27 09:30 ET | **SHORT** | `EL` | 14 | $104.49 | $2.09 | — | $24,355.15 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+7.2; leftover $1564.68 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,355.15 | ▼ close $9,237.18 vs 09:30 $9,392.31 (session -141.92) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,355.15 | ▲ 09:30 equity $9,331.59 vs yday $9,237.18 (+94.41) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `XHG` | 141 | $3.69 | $2.41 | $+107.92 | $23,832.45 | ▲ +107.92 after sell → book $9,329.18; vs 09:30 mark -2.41 | dropped from list after 5 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `LIFE` | 19 | $39.60 | $2.05 | $-54.29 | $23,078.00 | ▼ -54.29 after sell → book $9,327.13; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `CYPH` | 456 | $1.82 | $5.88 | $-130.43 | $22,242.20 | ▼ -130.43 after sell → book $9,321.25; vs 09:30 mark -5.88 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `RUM` | 75 | $9.30 | $2.21 | $+4.53 | $21,542.48 | ▲ +4.53 after sell → book $9,319.04; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `DFDV` | 175 | $5.26 | $2.52 | $-215.09 | $20,619.47 | ▼ -215.09 after sell → book $9,316.52; vs 09:30 mark -2.52 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `NIQ` | 37 | $19.37 | $2.10 | $-17.93 | $19,900.68 | ▼ -17.93 after sell → book $9,314.42; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 52 | $13.90 | $2.15 | $-18.63 | $19,175.73 | ▼ -18.63 after sell → book $9,312.27; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `WIX` | 8 | $85.32 | $2.01 | $-21.43 | $18,491.16 | ▼ -21.43 after sell → book $9,310.26; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `CRDL` | 564 | $2.06 | $7.41 | — | $19,645.58 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer; ret5=+9.3; leftover $1163.78 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SBET` | 134 | $8.65 | $2.46 | — | $20,802.23 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+17.0; leftover $1163.78 | — |
| 2026-08-28 09:30 ET | **SHORT** | `CRCL` | 12 | $92.61 | $2.08 | — | $21,911.47 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+12.6; leftover $1163.78 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FSM` | 90 | $12.84 | $2.32 | — | $23,064.76 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+7.6; leftover $1163.78 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23,064.76 | ▲ close $9,707.97 vs 09:30 $9,331.59 (session +411.97) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23,064.76 | ▲ 09:30 equity $9,736.66 vs yday $9,707.97 (+28.69) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `SENS` | 83 | $9.29 | $2.24 | $+11.25 | $22,291.45 | ▲ +11.25 after sell → book $9,734.42; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `KURA` | 58 | $12.71 | $2.16 | $+48.99 | $21,552.10 | ▲ +48.99 after sell → book $9,732.25; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CAPR` | 95 | $9.50 | $2.27 | $-119.55 | $20,647.33 | ▼ -119.55 after sell → book $9,729.98; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `FIGR` | 19 | $35.77 | $2.05 | $+85.73 | $19,965.65 | ▲ +85.73 after sell → book $9,727.93; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 65 | $11.10 | $2.19 | $+68.39 | $19,241.97 | ▲ +68.39 after sell → book $9,725.75; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `HTFL` | 15 | $47.68 | $2.04 | $+30.99 | $18,524.73 | ▲ +30.99 after sell → book $9,723.71; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,524.73 | ▼ close $9,493.29 vs 09:30 $9,736.66 (session -230.42) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,524.73 | ▲ 09:30 equity $9,675.00 vs yday $9,493.29 (+181.71) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `SUJA` | 166 | $9.98 | $2.49 | $-99.68 | $16,865.56 | ▼ -99.68 after sell → book $9,672.51; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `OABI` | 325 | $4.35 | $4.19 | $+141.01 | $15,447.62 | ▲ +141.01 after sell → book $9,668.32; vs 09:30 mark -4.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `EL` | 14 | $101.32 | $2.03 | $+40.25 | $14,027.11 | ▲ +40.25 after sell → book $9,666.29; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,027.11 | ▼ close $9,591.15 vs 09:30 $9,675.00 (session -75.14) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,027.11 | ▲ 09:30 equity $9,595.27 vs yday $9,591.15 (+4.12) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `CRDL` | 564 | $2.16 | $7.28 | $-71.09 | $12,801.59 | ▼ -71.09 after sell → book $9,587.99; vs 09:30 mark -7.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SBET` | 134 | $8.01 | $2.39 | $+80.91 | $11,725.86 | ▲ +80.91 after sell → book $9,585.60; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `CRCL` | 12 | $87.75 | $2.03 | $+54.16 | $10,670.77 | ▲ +54.16 after sell → book $9,583.57; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FSM` | 90 | $12.08 | $2.26 | $+63.82 | $9,581.31 | ▲ +63.82 after sell → book $9,581.31; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,581.31 | ▲ close $9,581.31 vs 09:30 $9,595.27 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,581.31 | ▲ 09:30 equity $9,581.31 vs yday $9,581.31 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **SHORT** | `CRK` | 38 | $15.45 | $2.14 | — | $10,166.27 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $598.83 | — |
| 2026-09-03 09:30 ET | **SHORT** | `ARCT` | 35 | $16.77 | $2.13 | — | $10,751.09 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $598.83 | — |
| 2026-09-03 09:30 ET | **SHORT** | `CRDL` | 274 | $2.18 | $3.61 | — | $11,344.81 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $598.83 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SID` | 440 | $1.36 | $5.78 | — | $11,937.43 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $598.83 | — |
| 2026-09-03 09:30 ET | **SHORT** | `BMEA` | 310 | $1.93 | $4.08 | — | $12,531.65 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $598.83 | — |
| 2026-09-03 09:30 ET | **SHORT** | `VIR` | 51 | $11.54 | $2.18 | — | $13,118.02 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $598.83 | — |
| 2026-09-03 09:30 ET | **SHORT** | `AGCO` | 4 | $127.91 | $2.04 | — | $13,627.62 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $598.83 | — |
| 2026-09-03 09:30 ET | **SHORT** | `ASST` | 23 | $25.62 | $2.10 | — | $14,214.90 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+13.1; leftover $598.83 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,214.90 | ▲ close $9,659.77 vs 09:30 $9,581.31 (session +102.49) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,214.90 | ▲ 09:30 equity $9,719.68 vs yday $9,659.77 (+59.91) | — | — |
| 2026-09-04 09:30 ET | **SHORT** | `OABI` | 169 | $4.78 | $2.56 | — | $15,020.16 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $809.97 | — |
| 2026-09-04 09:30 ET | **SHORT** | `CRM` | 3 | $263.36 | $2.04 | — | $15,808.20 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $809.97 | — |
| 2026-09-04 09:30 ET | **SHORT** | `IRD` | 178 | $4.53 | $2.59 | — | $16,611.96 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $809.97 | — |
| 2026-09-04 09:30 ET | **SHORT** | `LENZ` | 140 | $5.75 | $2.47 | — | $17,414.49 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $809.97 | — |
| 2026-09-04 09:30 ET | **SHORT** | `DFDV` | 139 | $5.79 | $2.46 | — | $18,216.84 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $809.97 | — |
| 2026-09-04 09:30 ET | **SHORT** | `HOOD` | 6 | $120.47 | $2.05 | — | $18,937.64 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+13.6; leftover $809.97 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,937.64 | ▼ close $9,573.00 vs 09:30 $9,719.68 (session -132.52) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,937.64 | ▲ 09:30 equity $9,623.08 vs yday $9,573.00 (+50.08) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,937.64 | ▲ close $9,838.94 vs 09:30 $9,623.08 (session +215.86) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,937.64 | ▼ 09:30 equity $9,618.97 vs yday $9,838.94 (-219.97) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `CRK` | 38 | $15.16 | $2.10 | $+6.78 | $18,359.46 | ▲ +6.78 after sell → book $9,616.87; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `ARCT` | 35 | $15.46 | $2.10 | $+41.62 | $17,816.26 | ▲ +41.62 after sell → book $9,614.77; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `CRDL` | 274 | $2.22 | $3.53 | $-18.10 | $17,204.45 | ▼ -18.10 after sell → book $9,611.24; vs 09:30 mark -3.53 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `SID` | 440 | $1.28 | $5.68 | $+23.75 | $16,635.57 | ▲ +23.75 after sell → book $9,605.56; vs 09:30 mark -5.68 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `BMEA` | 310 | $1.94 | $4.00 | $-11.18 | $16,030.17 | ▼ -11.18 after sell → book $9,601.56; vs 09:30 mark -4.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `VIR` | 51 | $11.04 | $2.14 | $+21.18 | $15,464.99 | ▲ +21.18 after sell → book $9,599.42; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `AGCO` | 4 | $127.69 | $2.00 | $-3.16 | $14,952.23 | ▼ -3.16 after sell → book $9,597.42; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `ASST` | 23 | $28.00 | $2.06 | $-58.78 | $14,306.17 | ▼ -58.78 after sell → book $9,595.36; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,306.17 | ▲ close $9,739.97 vs 09:30 $9,618.97 (session +144.62) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,306.17 | ▲ 09:30 equity $9,781.72 vs yday $9,739.97 (+41.75) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `OABI` | 169 | $3.92 | $2.50 | $+139.95 | $13,640.85 | ▲ +139.95 after sell → book $9,779.22; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `CRM` | 3 | $245.35 | $2.00 | $+49.99 | $12,902.80 | ▲ +49.99 after sell → book $9,777.22; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `IRD` | 178 | $5.87 | $2.52 | $-243.63 | $11,855.42 | ▼ -243.63 after sell → book $9,774.70; vs 09:30 mark -2.52 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `LENZ` | 140 | $4.85 | $2.41 | $+121.12 | $11,174.01 | ▲ +121.12 after sell → book $9,772.29; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `DFDV` | 139 | $5.22 | $2.41 | $+74.36 | $10,446.02 | ▲ +74.36 after sell → book $9,769.88; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `HOOD` | 6 | $112.69 | $2.01 | $+42.65 | $9,767.87 | ▲ +42.65 after sell → book $9,767.87; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,767.87 | ▲ close $9,767.87 vs 09:30 $9,781.72 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,767.87 | ▲ 09:30 equity $9,767.87 vs yday $9,767.87 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **SHORT** | `TJGC` | 65 | $10.65 | $2.22 | — | $10,457.90 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+6.3; leftover $697.71 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CVI` | 14 | $48.36 | $2.07 | — | $11,132.87 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+13.2; leftover $697.71 | — |
| 2026-09-11 09:30 ET | **SHORT** | `HAFN` | 74 | $9.32 | $2.25 | — | $11,820.30 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+5.4; leftover $697.71 | — |
| 2026-09-11 09:30 ET | **SHORT** | `FRO` | 14 | $48.05 | $2.07 | — | $12,490.93 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+6.6; leftover $697.71 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CLMT` | 12 | $55.98 | $2.06 | — | $13,160.62 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+6.4; leftover $697.71 | — |
| 2026-09-11 09:30 ET | **SHORT** | `UGP` | 92 | $7.55 | $2.31 | — | $13,852.91 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+6.0; leftover $697.71 | — |
| 2026-09-11 09:30 ET | **SHORT** | `PBR-A` | 36 | $19.09 | $2.14 | — | $14,538.01 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+2.2; leftover $697.71 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,538.01 | ▼ close $9,677.40 vs 09:30 $9,767.87 (session -75.34) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,538.01 | ▼ 09:30 equity $9,656.19 vs yday $9,677.40 (-21.21) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,538.01 | ▲ close $9,660.44 vs 09:30 $9,656.19 (session +4.25) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,538.01 | ▲ 09:30 equity $9,662.77 vs yday $9,660.44 (+2.33) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,538.01 | ▼ close $9,589.90 vs 09:30 $9,662.77 (session -72.87) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,538.01 | ▲ 09:30 equity $9,606.42 vs yday $9,589.90 (+16.52) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `TJGC` | 65 | $10.95 | $2.19 | $-23.91 | $13,824.08 | ▼ -23.91 after sell → book $9,604.24; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CVI` | 14 | $51.05 | $2.03 | $-41.76 | $13,107.35 | ▼ -41.76 after sell → book $9,602.21; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `HAFN` | 74 | $9.59 | $2.21 | $-24.45 | $12,395.48 | ▼ -24.45 after sell → book $9,600.00; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `FRO` | 14 | $52.52 | $2.03 | $-66.68 | $11,658.16 | ▼ -66.68 after sell → book $9,597.96; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CLMT` | 12 | $56.53 | $2.03 | $-10.69 | $10,977.78 | ▼ -10.69 after sell → book $9,595.94; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `UGP` | 92 | $7.46 | $2.27 | $+3.70 | $10,289.19 | ▲ +3.70 after sell → book $9,593.67; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `PBR-A` | 36 | $19.32 | $2.10 | $-12.51 | $9,591.57 | ▼ -12.51 after sell → book $9,591.57; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SHORT** | `INDP` | 655 | $3.66 | $8.64 | — | $11,980.23 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; ret5=+96.8; leftover $2397.89 | — |
| 2026-09-16 09:30 ET | **SHORT** | `ATRC` | 43 | $55.66 | $2.21 | — | $14,371.40 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+4.6; leftover $2397.89 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,371.40 | ▲ close $9,811.83 vs 09:30 $9,606.42 (session +231.11) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,371.40 | ▼ 09:30 equity $9,717.62 vs yday $9,811.83 (-94.21) | — | — |
| 2026-09-17 09:30 ET | **SHORT** | `CVI` | 46 | $51.88 | $2.22 | — | $16,755.65 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+11.1; leftover $2429.40 | — |
| 2026-09-17 09:30 ET | **SHORT** | `HAFN` | 249 | $9.75 | $3.34 | — | $19,180.06 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+6.0; leftover $2429.40 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,180.06 | ▼ close $9,153.03 vs 09:30 $9,717.62 (session -559.02) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,180.06 | ▲ 09:30 equity $9,201.13 vs yday $9,153.03 (+48.10) | — | — |
| 2026-09-18 09:30 ET | **SHORT** | `CYPH` | 757 | $3.04 | $9.97 | — | $21,467.59 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $2300.28 | — |
| 2026-09-18 09:30 ET | **SHORT** | `CHPT` | 230 | $10.00 | $3.09 | — | $23,764.50 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ⚪; ret5=+11.1; leftover $2300.28 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23,764.50 | ▼ close $8,852.53 vs 09:30 $9,201.13 (session -335.54) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23,764.50 | ▼ 09:30 equity $8,659.27 vs yday $8,852.53 (-193.26) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `INDP` | 655 | $3.55 | $8.45 | $+54.96 | $21,430.80 | ▲ +54.96 after sell → book $8,650.82; vs 09:30 mark -8.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `ATRC` | 43 | $58.23 | $2.12 | $-114.84 | $18,924.79 | ▼ -114.84 after sell → book $8,648.70; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,924.79 | ▲ close $9,324.00 vs 09:30 $8,659.27 (session +675.30) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,924.79 | ▼ 09:30 equity $9,240.73 vs yday $9,324.00 (-83.27) | — | — |
| 2026-09-22 09:30 ET | **SHORT** | `GLND` | 261 | $2.94 | $3.44 | — | $19,688.69 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; ret5=+136.1; leftover $770.06 | — |
| 2026-09-22 09:30 ET | **SHORT** | `USDE` | 59 | $12.99 | $2.21 | — | $20,452.89 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; ret5=+69.4; leftover $770.06 | — |
| 2026-09-22 09:30 ET | **SHORT** | `HIVE` | 223 | $3.45 | $2.94 | — | $21,219.30 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+16.4; leftover $770.06 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,219.30 | ▼ close $9,125.31 vs 09:30 $9,240.73 (session -106.83) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,219.30 | ▼ 09:30 equity $9,085.48 vs yday $9,125.31 (-39.83) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `CVI` | 46 | $52.66 | $2.13 | $-40.23 | $18,794.81 | ▼ -40.23 after sell → book $9,083.36; vs 09:30 mark -2.12 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `HAFN` | 249 | $9.29 | $3.21 | $+109.23 | $16,479.63 | ▲ +109.23 after sell → book $9,080.14; vs 09:30 mark -3.22 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `CYPH` | 757 | $3.82 | $9.77 | $-613.98 | $13,578.13 | ▼ -613.98 after sell → book $9,070.38; vs 09:30 mark -9.76 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `CHPT` | 230 | $9.76 | $2.97 | $+49.15 | $11,330.36 | ▲ +49.15 after sell → book $9,067.41; vs 09:30 mark -2.97 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SHORT** | `VICR` | 2 | $266.50 | $2.03 | — | $11,861.33 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.2; leftover $647.67 | — |
| 2026-09-23 09:30 ET | **SHORT** | `SVIA` | 144 | $4.49 | $2.47 | — | $12,505.42 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; ret5=+26.4; leftover $647.67 | — |
| 2026-09-23 09:30 ET | **SHORT** | `VERI` | 498 | $1.30 | $6.53 | — | $13,146.28 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+15.3; leftover $647.67 | — |
| 2026-09-23 09:30 ET | **SHORT** | `FWDI` | 78 | $8.20 | $2.26 | — | $13,783.62 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+17.6; leftover $647.67 | — |
| 2026-09-23 09:30 ET | **SHORT** | `GME` | 27 | $23.94 | $2.11 | — | $14,427.89 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+12.1; leftover $647.67 | — |
| 2026-09-23 09:30 ET | **SHORT** | `NTSK` | 34 | $18.57 | $2.13 | — | $15,057.31 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+8.2; leftover $647.67 | — |
| 2026-09-23 09:30 ET | **SHORT** | `FORM` | 5 | $125.39 | $2.04 | — | $15,682.22 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+15.9; leftover $647.67 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,682.22 | ▲ close $9,053.05 vs 09:30 $9,085.48 (session +5.22) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,682.22 | ▲ 09:30 equity $9,086.29 vs yday $9,053.05 (+33.24) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,682.22 | ▼ close $8,322.87 vs 09:30 $9,086.29 (session -763.42) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,858.14 | ▼ 09:30 equity $8,750.42 vs yday $9,199.24 (-448.82) | 09:30 open · cash $20,858.14 (unchanged overnight, no fees) · equity $8,750.42 vs prior close $9,199.24 (-448.82) · 8 name(s) re-marked at the open (per-name table). BAND×22 yday $61.83 → 09:30 $61.83 -0.00; FWDI×160 yday $8.35 → 09:30 $8.35 -0.00; GLND×445 yday $5.35 → 09:30 $6.06 -315.95; GME×54 yday $25.02 → 09:30 $24.96 +3.24; NTSK×71 yday $18.57 → 09:30 $18.57 -0.00; SVIA×291 yday $3.96 → 09:30 $3.96 -0.00; USDE×100 yday $14.22 → 09:30 $15.58 -136.11; VERI×1006 yday $1.33 → 09:30 $1.33 -0.00 | — |
| 2026-09-25 09:30 ET | **COVER** | `GLND` | 445 | $6.06 | $5.74 | $-1400.00 | $18,155.70 | ▼ -1,400.00 after sell → book $8,744.68; vs 09:30 mark -5.74 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **COVER** | `USDE` | 100 | $15.58 | $2.29 | $-263.75 | $16,595.30 | ▼ -263.75 after sell → book $8,742.39; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `TXG` | 6 | $83.76 | $2.04 | — | $17,095.82 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $546.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `HLP` | 248 | $2.20 | $3.27 | — | $17,638.15 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $546.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `TEM` | 6 | $83.69 | $2.04 | — | $18,138.28 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $546.40 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `TWST` | 2 | $184.00 | $2.03 | — | $18,504.25 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $546.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `CYPH` | 136 | $4.00 | $2.45 | — | $19,046.49 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; leftover $546.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `BLLN` | 4 | $125.19 | $2.04 | — | $19,545.21 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+12.9; leftover $546.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `ILMN` | 2 | $272.16 | $2.03 | — | $20,087.50 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $546.40 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `LTRX` | 82 | $6.66 | $2.27 | — | $20,631.35 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+13.4; leftover $546.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,631.35 | ▲ close $8,757.29 vs 09:30 $8,750.42 (session +33.06) | 16:00 close · cash $20,631.35 · equity $8,757.29 vs 09:30 $8,750.42 (+6.87; session marks +33.06) · 14 name(s) marked open→close (per-name table). BAND×22 09:30 $61.83 → close $61.83 -0.00; FWDI×160 09:30 $8.35 → close $8.35 -0.00; GME×54 09:30 $24.96 → close $23.39 +84.78; NTSK×71 09:30 $18.57 → close $18.57 +0.00; SVIA×291 09:30 $3.96 → close $3.96 -0.00; VERI×1006 09:30 $1.33 → close $1.33 -0.00; TXG×6 09:30 $83.76 → close $85.71 -11.70; HLP×248 09:30 $2.20 → close $2.21 -2.48; TEM×6 09:30 $83.69 → close $85.01 -7.89; TWST×2 09:30 $184.00 → close $182.83 +2.34; CYPH×136 09:30 $4.00 → close $4.12 -15.64; BLLN×4 09:30 $125.19 → close $123.08 +8.44; ILMN×2 09:30 $272.16 → close $270.00 +4.32; LTRX×82 09:30 $6.66 → close $7.01 -29.11 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,631.35 | ▲ 09:30 equity $8,985.67 vs yday $8,757.29 (+228.38) | 09:30 open · cash $20,631.35 (unchanged overnight, no fees) · equity $8,985.67 vs prior close $8,757.29 (+228.38) · 14 name(s) re-marked at the open (per-name table). BAND×22 yday $61.83 → 09:30 $61.64 +4.18; BLLN×4 yday $123.08 → 09:30 $120.64 +9.76; CYPH×136 yday $4.12 → 09:30 $4.03 +11.90; FWDI×160 yday $8.35 → 09:30 $8.31 +6.40; GME×54 yday $23.39 → 09:30 $23.26 +7.02; HLP×248 yday $2.21 → 09:30 $2.22 -2.48; ILMN×2 yday $270.00 → 09:30 $265.95 +8.10; LTRX×82 yday $7.01 → 09:30 $6.91 +8.61; NTSK×71 yday $18.57 → 09:30 $17.15 +100.48; SVIA×291 yday $3.96 → 09:30 $3.87 +26.19; TEM×6 yday $85.01 → 09:30 $83.57 +8.64; TWST×2 yday $182.83 → 09:30 $181.87 +1.92; TXG×6 yday $85.71 → 09:30 $86.14 -2.58; VERI×1006 yday $1.33 → 09:30 $1.29 +40.24 | — |
| 2026-09-28 09:30 ET | **COVER** | `BAND` | 22 | $61.64 | $2.06 | $-84.03 | $19,273.21 | ▼ -84.03 after sell → book $8,983.61; vs 09:30 mark -2.06 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **COVER** | `FWDI` | 160 | $8.31 | $2.47 | $-27.41 | $17,941.14 | ▼ -27.41 after sell → book $8,981.14; vs 09:30 mark -2.47 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **COVER** | `GME` | 54 | $23.26 | $2.15 | $+32.36 | $16,682.95 | ▲ +32.36 after sell → book $8,978.99; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-28 09:30 ET | **COVER** | `NTSK` | 71 | $17.15 | $2.20 | $+71.88 | $15,462.76 | ▲ +71.88 after sell → book $8,976.79; vs 09:30 mark -2.20 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **COVER** | `SVIA` | 291 | $3.87 | $3.75 | $+172.82 | $14,332.83 | ▲ +172.82 after sell → book $8,973.03; vs 09:30 mark -3.76 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **COVER** | `VERI` | 1006 | $1.29 | $12.98 | $-16.11 | $13,022.12 | ▼ -16.11 after sell → book $8,960.06; vs 09:30 mark -12.97 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,022.12 | ▼ close $8,955.84 vs 09:30 $8,985.67 (session -4.22) | 16:00 close · cash $13,022.12 · equity $8,955.84 vs 09:30 $8,985.67 (-29.83; session marks -4.22) · 8 name(s) marked open→close (per-name table). BLLN×4 09:30 $120.64 → close $121.72 -4.32; CYPH×136 09:30 $4.03 → close $3.31 +98.26; HLP×248 09:30 $2.22 → close $2.39 -42.16; ILMN×2 09:30 $265.95 → close $271.90 -11.90; LTRX×82 09:30 $6.91 → close $7.22 -25.42; TEM×6 09:30 $83.57 → close $85.08 -9.06; TWST×2 09:30 $181.87 → close $179.60 +4.54; TXG×6 09:30 $86.14 → close $88.50 -14.16 | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,022.12 | ▼ 09:30 equity $8,933.75 vs yday $8,955.84 (-22.09) | 09:30 open · cash $13,022.12 (unchanged overnight, no fees) · equity $8,933.75 vs prior close $8,955.84 (-22.09) · 8 name(s) re-marked at the open (per-name table). BLLN×4 yday $121.72 → 09:30 $121.72 -0.00; CYPH×136 yday $3.31 → 09:30 $3.34 -4.08; HLP×248 yday $2.39 → 09:30 $2.41 -4.96; ILMN×2 yday $271.90 → 09:30 $271.00 +1.80; LTRX×82 yday $7.22 → 09:30 $7.34 -9.84; TEM×6 yday $85.08 → 09:30 $85.48 -2.43; TWST×2 yday $179.60 → 09:30 $179.90 -0.60; TXG×6 yday $88.50 → 09:30 $88.83 -1.98 | — |
| 2026-09-29 09:30 ET | **SHORT** | `ABCL` | 45 | $14.12 | $2.16 | — | $13,655.36 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list probable,yday_gainer,ohlc_hot; ret5=+10.0; leftover $638.12 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **SHORT** | `MX` | 159 | $3.99 | $2.52 | — | $14,287.25 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; ret5=+18.4; leftover $638.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **SHORT** | `AMC` | 192 | $3.32 | $2.63 | — | $14,922.06 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+13.8; leftover $638.12 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **SHORT** | `AGEN` | 66 | $9.58 | $2.23 | — | $15,552.12 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,ohlc_hot; ret5=+13.2; leftover $638.12 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-29 09:30 ET | **SHORT** | `TJGC` | 23 | $27.68 | $2.10 | — | $16,186.66 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer; 🔵; ret5=+62.3; leftover $638.12 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-29 09:30 ET | **SHORT** | `XERS` | 65 | $9.77 | $2.22 | — | $16,819.49 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $638.12 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **SHORT** | `QTRX` | 181 | $3.52 | $2.59 | — | $17,454.02 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,ohlc_hot; ret5=+15.8; leftover $638.12 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,454.02 | ▼ close $8,747.63 vs 09:30 $8,933.75 (session -169.68) | 16:00 close · cash $17,454.02 · equity $8,747.63 vs 09:30 $8,933.75 (-186.12; session marks -169.68) · 15 name(s) marked open→close (per-name table). BLLN×4 09:30 $121.72 → close $118.18 +14.16; CYPH×136 09:30 $3.34 → close $3.23 +14.96; HLP×248 09:30 $2.41 → close $2.45 -9.92; ILMN×2 09:30 $271.00 → close $272.00 -2.00; LTRX×82 09:30 $7.34 → close $7.22 +9.84; TEM×6 09:30 $85.48 → close $82.58 +17.43; TWST×2 09:30 $179.90 → close $186.13 -12.46; TXG×6 09:30 $88.83 → close $90.04 -7.26; ABCL×45 09:30 $14.12 → close $14.82 -31.50; MX×159 09:30 $3.99 → close $3.89 +15.90; AMC×192 09:30 $3.32 → close $3.08 +46.08; AGEN×66 09:30 $9.58 → close $9.89 -20.46; TJGC×23 09:30 $27.68 → close $34.80 -163.65; XERS×65 09:30 $9.77 → close $9.98 -13.65; QTRX×181 09:30 $3.52 → close $3.67 -27.15 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,454.02 | ▲ 09:30 equity $8,747.64 vs yday $8,747.63 (+0.01) | 09:30 open · cash $17,454.02 (unchanged overnight, no fees) · equity $8,747.64 vs prior close $8,747.63 (+0.01) · 15 name(s) re-marked at the open (per-name table). ABCL×45 yday $14.82 → 09:30 $14.82 -0.00; AGEN×66 yday $9.89 → 09:30 $9.89 -0.00; AMC×192 yday $3.08 → 09:30 $3.08 -0.00; BLLN×4 yday $118.18 → 09:30 $118.18 -0.00; CYPH×136 yday $3.23 → 09:30 $3.23 -0.00; HLP×248 yday $2.45 → 09:30 $2.45 -0.00; ILMN×2 yday $272.00 → 09:30 $272.00 -0.00; LTRX×82 yday $7.22 → 09:30 $7.22 -0.00; MX×159 yday $3.89 → 09:30 $3.89 -0.00; QTRX×181 yday $3.67 → 09:30 $3.67 -0.00; TEM×6 yday $82.58 → 09:30 $82.58 -0.00; TJGC×23 yday $34.80 → 09:30 $34.80 -0.00; TWST×2 yday $186.13 → 09:30 $186.13 -0.00; TXG×6 yday $90.04 → 09:30 $90.04 -0.00; XERS×65 yday $9.98 → 09:30 $9.98 -0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,454.02 | ▲ close $8,747.64 vs 09:30 $8,747.64 (session +0.00) | 16:00 close · cash $17,454.02 · equity $8,747.64 vs 09:30 $8,747.64 (-0.00; session marks +0.00) · 15 name(s) marked open→close (per-name table). ABCL×45 09:30 $14.82 → close $14.82 -0.00; AGEN×66 09:30 $9.89 → close $9.89 -0.00; AMC×192 09:30 $3.08 → close $3.08 -0.00; BLLN×4 09:30 $118.18 → close $118.18 -0.00; CYPH×136 09:30 $3.23 → close $3.23 -0.00; HLP×248 09:30 $2.45 → close $2.45 -0.00; ILMN×2 09:30 $272.00 → close $272.00 -0.00; LTRX×82 09:30 $7.22 → close $7.22 -0.00; MX×159 09:30 $3.89 → close $3.89 -0.00; QTRX×181 09:30 $3.67 → close $3.67 -0.00; TEM×6 09:30 $82.58 → close $82.58 -0.00; TJGC×23 09:30 $34.80 → close $34.80 -0.00; TWST×2 09:30 $186.13 → close $186.13 -0.00; TXG×6 09:30 $90.04 → close $90.04 -0.00; XERS×65 09:30 $9.98 → close $9.98 -0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,454.02 | ▲ 09:30 equity $9,038.83 vs yday $8,747.64 (+291.19) | 09:30 open · cash $17,454.02 (unchanged overnight, no fees) · equity $9,038.83 vs prior close $8,747.64 (+291.19) · 15 name(s) re-marked at the open (per-name table). ABCL×45 yday $14.82 → 09:30 $14.59 +10.57; AGEN×66 yday $9.89 → 09:30 $9.95 -3.96; AMC×192 yday $3.08 → 09:30 $2.98 +19.20; BLLN×4 yday $118.18 → 09:30 $105.67 +50.04; CYPH×136 yday $3.23 → 09:30 $3.13 +13.60; HLP×248 yday $2.45 → 09:30 $2.52 -17.36; ILMN×2 yday $272.00 → 09:30 $273.22 -2.44; LTRX×82 yday $7.22 → 09:30 $7.25 -2.46; MX×159 yday $3.89 → 09:30 $3.78 +17.49; QTRX×181 yday $3.67 → 09:30 $3.72 -9.05; TEM×6 yday $82.58 → 09:30 $82.58 -0.00; TJGC×23 yday $34.80 → 09:30 $26.31 +195.16; TWST×2 yday $186.13 → 09:30 $193.10 -13.94; TXG×6 yday $90.04 → 09:30 $89.30 +4.44; XERS×65 yday $9.98 → 09:30 $9.52 +29.90 | — |
| 2026-10-01 09:30 ET | **COVER** | `ABCL` | 45 | $14.59 | $2.12 | $-25.21 | $16,795.57 | ▼ -25.21 after sell → book $9,036.70; vs 09:30 mark -2.12 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **COVER** | `AGEN` | 66 | $9.95 | $2.19 | $-28.83 | $16,136.68 | ▼ -28.83 after sell → book $9,034.51; vs 09:30 mark -2.19 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **COVER** | `AMC` | 192 | $2.98 | $2.57 | $+60.09 | $15,561.96 | ▲ +60.09 after sell → book $9,031.95; vs 09:30 mark -2.56 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **COVER** | `BLLN` | 4 | $105.67 | $2.00 | $+74.04 | $15,137.27 | ▲ +74.04 after sell → book $9,029.94; vs 09:30 mark -2.01 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **COVER** | `CYPH` | 136 | $3.13 | $2.40 | $+114.16 | $14,709.20 | ▲ +114.16 after sell → book $9,027.55; vs 09:30 mark -2.39 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **COVER** | `HLP` | 248 | $2.52 | $3.20 | $-85.82 | $14,081.04 | ▼ -85.82 after sell → book $9,024.35; vs 09:30 mark -3.20 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **COVER** | `ILMN` | 2 | $273.22 | $2.00 | $-6.15 | $13,532.60 | ▼ -6.15 after sell → book $9,022.35; vs 09:30 mark -2.00 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **COVER** | `LTRX` | 82 | $7.25 | $2.24 | $-52.89 | $12,935.86 | ▼ -52.89 after sell → book $9,020.11; vs 09:30 mark -2.24 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **COVER** | `MX` | 159 | $3.78 | $2.47 | $+28.40 | $12,332.38 | ▲ +28.40 after sell → book $9,017.65; vs 09:30 mark -2.46 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **COVER** | `QTRX` | 181 | $3.72 | $2.53 | $-41.32 | $11,656.52 | ▼ -41.32 after sell → book $9,015.11; vs 09:30 mark -2.54 | exit unpriced hold on first bar after 2 sess | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **COVER** | `TJGC` | 23 | $26.31 | $2.06 | $+27.35 | $11,049.34 | ▲ +27.35 after sell → book $9,013.06; vs 09:30 mark -2.05 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-01 09:30 ET | **COVER** | `TWST` | 2 | $193.10 | $2.00 | $-22.22 | $10,661.14 | ▼ -22.22 after sell → book $9,011.06; vs 09:30 mark -2.00 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **COVER** | `TXG` | 6 | $89.30 | $2.01 | $-37.29 | $10,123.33 | ▼ -37.29 after sell → book $9,009.05; vs 09:30 mark -2.01 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **COVER** | `XERS` | 65 | $9.52 | $2.19 | $+11.84 | $9,502.35 | ▲ +11.84 after sell → book $9,006.87; vs 09:30 mark -2.18 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SHORT** | `FEAM` | 215 | $3.48 | $2.84 | — | $10,247.71 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer; ret5=+27.7; leftover $750.57 | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🔴 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **SHORT** | `FORM` | 5 | $149.45 | $2.05 | — | $10,992.91 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,ohlc_hot; ret5=+13.8; leftover $750.57 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SHORT** | `GLND` | 148 | $5.06 | $2.49 | — | $11,739.30 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer; ret5=+76.3; leftover $750.57 | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟡 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SHORT** | `VECO` | 14 | $51.93 | $2.07 | — | $12,464.25 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+13.0; leftover $750.57 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **SHORT** | `CTKB` | 125 | $5.96 | $2.42 | — | $13,206.83 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+4.9; leftover $750.57 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **SHORT** | `SG` | 85 | $8.80 | $2.29 | — | $13,952.54 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+15.0; leftover $750.57 | join🔴 sector🔴 gen🔴 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,952.54 | ▲ close $9,160.50 vs 09:30 $9,038.83 (session +167.79) | 16:00 close · cash $13,952.54 · equity $9,160.50 vs 09:30 $9,038.83 (+121.67; session marks +167.79) · 7 name(s) marked open→close (per-name table). TEM×6 09:30 $82.58 → close $82.58 -0.00; FEAM×215 09:30 $3.48 → close $3.23 +53.75; FORM×5 09:30 $149.45 → close $148.44 +5.05; GLND×148 09:30 $5.06 → close $4.40 +97.68; VECO×14 09:30 $51.93 → close $53.19 -17.64; CTKB×125 09:30 $5.96 → close $5.81 +18.75; SG×85 09:30 $8.80 → close $8.68 +10.20 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,952.54 | ▼ 09:30 equity $9,153.98 vs yday $9,160.50 (-6.52) | 09:30 open · cash $13,952.54 (unchanged overnight, no fees) · equity $9,153.98 vs prior close $9,160.50 (-6.52) · 7 name(s) re-marked at the open (per-name table). CTKB×125 yday $5.81 → 09:30 $5.86 -6.25; FEAM×215 yday $3.23 → 09:30 $3.27 -8.60; FORM×5 yday $148.44 → 09:30 $150.50 -10.30; GLND×148 yday $4.40 → 09:30 $4.25 +22.20; SG×85 yday $8.68 → 09:30 $8.71 -2.55; TEM×6 yday $82.58 → 09:30 $77.36 +31.32; VECO×14 yday $53.19 → 09:30 $55.50 -32.34 | — |
| 2026-10-02 09:30 ET | **COVER** | `TEM` | 6 | $77.36 | $2.01 | $+33.96 | $13,486.37 | ▲ +33.96 after sell → book $9,151.97; vs 09:30 mark -2.01 | exit unpriced hold on first bar after 5 sess | — |
| 2026-10-02 09:30 ET | **SHORT** | `CDNA` | 9 | $66.33 | $2.05 | — | $14,081.29 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+7.9; leftover $653.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **SHORT** | `ACN` | 3 | $211.02 | $2.04 | — | $14,712.31 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; ret5=+19.7; leftover $653.71 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **SHORT** | `EFXT` | 25 | $25.28 | $2.10 | — | $15,342.21 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer; ret5=+9.4; leftover $653.71 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **SHORT** | `TJGC` | 18 | $34.92 | $2.08 | — | $15,968.69 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,ohlc_hot; ret5=+15.5; leftover $653.71 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-02 09:30 ET | **SHORT** | `VICR` | 2 | $311.30 | $2.03 | — | $16,589.26 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+11.8; leftover $653.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **SHORT** | `COHU` | 8 | $73.18 | $2.05 | — | $17,172.66 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+10.4; leftover $653.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **SHORT** | `P` | 4 | $134.75 | $2.04 | — | $17,709.62 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+10.1; leftover $653.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,709.62 | ▼ close $9,086.02 vs 09:30 $9,153.98 (session -51.55) | 16:00 close · cash $17,709.62 · equity $9,086.02 vs 09:30 $9,153.98 (-67.96; session marks -51.55) · 13 name(s) marked open→close (per-name table). CTKB×125 09:30 $5.86 → close $5.87 -1.25; FEAM×215 09:30 $3.27 → close $3.88 -131.15; FORM×5 09:30 $150.50 → close $149.15 +6.75; GLND×148 09:30 $4.25 → close $3.73 +76.96; SG×85 09:30 $8.71 → close $9.10 -33.15; VECO×14 09:30 $55.50 → close $57.38 -26.32; CDNA×9 09:30 $66.33 → close $67.15 -7.38; ACN×3 09:30 $211.02 → close $198.90 +36.36; EFXT×25 09:30 $25.28 → close $24.59 +17.25; TJGC×18 09:30 $34.92 → close $33.28 +29.52; VICR×2 09:30 $311.30 → close $308.90 +4.80; COHU×8 09:30 $73.18 → close $73.48 -2.38; P×4 09:30 $134.75 → close $140.14 -21.56 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,709.62 | ▼ 09:30 equity $9,059.49 vs yday $9,086.02 (-26.53) | 09:30 open · cash $17,709.62 (unchanged overnight, no fees) · equity $9,059.49 vs prior close $9,086.02 (-26.53) · 13 name(s) re-marked at the open (per-name table). ACN×3 yday $198.90 → 09:30 $196.30 +7.80; CDNA×9 yday $67.15 → 09:30 $66.90 +2.25; COHU×8 yday $73.48 → 09:30 $73.05 +3.44; CTKB×125 yday $5.87 → 09:30 $5.88 -1.25; EFXT×25 yday $24.59 → 09:30 $24.79 -5.00; FEAM×215 yday $3.88 → 09:30 $3.88 -0.00; FORM×5 yday $149.15 → 09:30 $150.64 -7.45; GLND×148 yday $3.73 → 09:30 $3.92 -28.86; P×4 yday $140.14 → 09:30 $141.30 -4.64; SG×85 yday $9.10 → 09:30 $9.24 -11.90; TJGC×18 yday $33.28 → 09:30 $32.73 +9.90; VECO×14 yday $57.38 → 09:30 $56.94 +6.16; VICR×2 yday $308.90 → 09:30 $307.39 +3.02 | — |
| 2026-10-05 09:30 ET | **SHORT** | `MXL` | 8 | $106.00 | $2.06 | — | $18,555.56 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.9; leftover $905.95 | join🟡 sector🟢 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **SHORT** | `SYNA` | 7 | $120.98 | $2.05 | — | $19,400.37 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; ret5=+17.9; leftover $905.95 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **SHORT** | `VIAV` | 19 | $47.39 | $2.09 | — | $20,298.69 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+15.8; leftover $905.95 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **SHORT** | `GRAL` | 6 | $142.75 | $2.05 | — | $21,153.14 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+12.9; leftover $905.95 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **SHORT** | `DNA` | 72 | $12.50 | $2.25 | — | $22,050.88 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+15.9; leftover $905.95 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,050.88 | ▼ close $8,812.73 vs 09:30 $9,059.49 (session -236.25) | 16:00 close · cash $22,050.88 · equity $8,812.73 vs 09:30 $9,059.49 (-246.76; session marks -236.25) · 18 name(s) marked open→close (per-name table). ACN×3 09:30 $196.30 → close $195.05 +3.75; CDNA×9 09:30 $66.90 → close $69.85 -26.55; COHU×8 09:30 $73.05 → close $73.73 -5.44; CTKB×125 09:30 $5.88 → close $6.09 -26.25; EFXT×25 09:30 $24.79 → close $25.02 -5.75; FEAM×215 09:30 $3.88 → close $3.92 -8.60; FORM×5 09:30 $150.64 → close $147.99 +13.25; GLND×148 09:30 $3.92 → close $3.71 +31.82; P×4 09:30 $141.30 → close $143.88 -10.32; SG×85 09:30 $9.24 → close $9.09 +12.75; TJGC×18 09:30 $32.73 → close $32.88 -2.70; VECO×14 09:30 $56.94 → close $56.31 +8.82; VICR×2 09:30 $307.39 → close $305.61 +3.56; MXL×8 09:30 $106.00 → close $105.49 +4.08; SYNA×7 09:30 $120.98 → close $119.73 +8.75; VIAV×19 09:30 $47.39 → close $46.91 +9.12; GRAL×6 09:30 $142.75 → close $154.08 -67.98; DNA×72 09:30 $12.50 → close $14.98 -178.56 | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,050.88 | ▼ 09:30 equity $8,723.44 vs yday $8,812.73 (-89.29) | 09:30 open · cash $22,050.88 (unchanged overnight, no fees) · equity $8,723.44 vs prior close $8,812.73 (-89.29) · 18 name(s) re-marked at the open (per-name table). ACN×3 yday $195.05 → 09:30 $195.43 -1.14; CDNA×9 yday $69.85 → 09:30 $70.89 -9.36; COHU×8 yday $73.73 → 09:30 $74.67 -7.54; CTKB×125 yday $6.09 → 09:30 $6.13 -5.00; DNA×72 yday $14.98 → 09:30 $15.08 -7.20; EFXT×25 yday $25.02 → 09:30 $24.55 +11.75; FEAM×215 yday $3.92 → 09:30 $3.96 -8.60; FORM×5 yday $147.99 → 09:30 $148.32 -1.65; GLND×148 yday $3.71 → 09:30 $3.48 +34.04; GRAL×6 yday $154.08 → 09:30 $159.01 -29.58; MXL×8 yday $105.49 → 09:30 $108.22 -21.84; P×4 yday $143.88 → 09:30 $146.00 -8.48; SG×85 yday $9.09 → 09:30 $9.08 +0.85; SYNA×7 yday $119.73 → 09:30 $120.65 -6.44; TJGC×18 yday $32.88 → 09:30 $33.00 -2.16; VECO×14 yday $56.31 → 09:30 $57.24 -13.06; VIAV×19 yday $46.91 → 09:30 $47.41 -9.50; VICR×2 yday $305.61 → 09:30 $307.80 -4.38 | — |
| 2026-10-06 09:30 ET | **COVER** | `CTKB` | 125 | $6.13 | $2.37 | $-26.03 | $21,282.26 | ▼ -26.03 after sell → book $8,721.07; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **COVER** | `FEAM` | 215 | $3.96 | $2.77 | $-108.81 | $20,428.09 | ▼ -108.81 after sell → book $8,718.30; vs 09:30 mark -2.77 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **COVER** | `FORM` | 5 | $148.32 | $2.00 | $+1.60 | $19,684.49 | ▲ +1.60 after sell → book $8,716.29; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **COVER** | `GLND` | 148 | $3.48 | $2.43 | $+228.92 | $19,167.01 | ▲ +228.92 after sell → book $8,713.86; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **COVER** | `SG` | 85 | $9.08 | $2.25 | $-28.33 | $18,392.97 | ▼ -28.33 after sell → book $8,711.61; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **COVER** | `VECO` | 14 | $57.24 | $2.03 | $-78.48 | $17,589.53 | ▼ -78.48 after sell → book $8,709.58; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **SHORT** | `PTC` | 5 | $193.00 | $2.05 | — | $18,552.48 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+37.6; leftover $1088.70 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **SHORT** | `PCVX` | 14 | $73.35 | $2.08 | — | $19,577.30 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; ret5=+26.7; leftover $1088.70 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟢 |
| 2026-10-06 09:30 ET | **SHORT** | `RXO` | 37 | $28.73 | $2.15 | — | $20,638.16 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; ret5=+42.2; leftover $1088.70 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **SHORT** | `KOD` | 11 | $93.00 | $2.07 | — | $21,659.09 | — | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+4.3; leftover $1088.70 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,659.09 | ▲ close $9,310.08 vs 09:30 $8,723.44 (session +608.85) | 16:00 close · cash $21,659.09 · equity $9,310.08 vs 09:30 $8,723.44 (+586.64; session marks +608.85) · 16 name(s) marked open→close (per-name table). ACN×3 09:30 $195.43 → close $193.40 +6.09; CDNA×9 09:30 $70.89 → close $63.79 +63.90; COHU×8 09:30 $74.67 → close $73.10 +12.58; DNA×72 09:30 $15.08 → close $12.41 +192.24; EFXT×25 09:30 $24.55 → close $24.37 +4.50; GRAL×6 09:30 $159.01 → close $137.28 +130.38; MXL×8 09:30 $108.22 → close $109.30 -8.64; P×4 09:30 $146.00 → close $147.20 -4.80; SYNA×7 09:30 $120.65 → close $119.39 +8.82; TJGC×18 09:30 $33.00 → close $27.82 +93.24; VIAV×19 09:30 $47.41 → close $48.14 -13.87; VICR×2 09:30 $307.80 → close $299.29 +17.02; PTC×5 09:30 $193.00 → close $193.00 -0.00; PCVX×14 09:30 $73.35 → close $66.70 +93.10; RXO×37 09:30 $28.73 → close $28.54 +7.03; KOD×11 09:30 $93.00 → close $92.34 +7.26 | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,659.09 | ▲ 09:30 equity $9,556.28 vs yday $9,310.08 (+246.20) | 09:30 open · cash $21,659.09 (unchanged overnight, no fees) · equity $9,556.28 vs prior close $9,310.08 (+246.20) · 16 name(s) re-marked at the open (per-name table). ACN×3 yday $193.40 → 09:30 $195.00 -4.80; CDNA×9 yday $63.79 → 09:30 $61.38 +21.67; COHU×8 yday $73.10 → 09:30 $70.95 +17.20; DNA×72 yday $12.41 → 09:30 $11.80 +43.56; EFXT×25 yday $24.37 → 09:30 $24.32 +1.25; GRAL×6 yday $137.28 → 09:30 $134.00 +19.68; KOD×11 yday $92.34 → 09:30 $91.74 +6.60; MXL×8 yday $109.30 → 09:30 $105.38 +31.40; P×4 yday $147.20 → 09:30 $144.58 +10.48; PCVX×14 yday $66.70 → 09:30 $65.86 +11.76; PTC×5 yday $193.00 → 09:30 $193.20 -1.00; RXO×37 yday $28.54 → 09:30 $28.34 +7.22; SYNA×7 yday $119.39 → 09:30 $118.50 +6.23; TJGC×18 yday $27.82 → 09:30 $26.61 +21.78; VIAV×19 yday $48.14 → 09:30 $46.53 +30.59; VICR×2 yday $299.29 → 09:30 $288.00 +22.58 | — |
| 2026-10-07 09:30 ET | **COVER** | `ACN` | 3 | $195.00 | $2.00 | $+44.02 | $21,072.09 | ▲ +44.02 after sell → book $9,554.28; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-10-07 09:30 ET | **COVER** | `CDNA` | 9 | $61.38 | $2.02 | $+40.46 | $20,517.64 | ▲ +40.46 after sell → book $9,552.26; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-10-07 09:30 ET | **COVER** | `COHU` | 8 | $70.95 | $2.01 | $+13.79 | $19,948.02 | ▲ +13.79 after sell → book $9,550.25; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-10-07 09:30 ET | **COVER** | `EFXT` | 25 | $24.32 | $2.06 | $+19.83 | $19,337.96 | ▲ +19.83 after sell → book $9,548.18; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-10-07 09:30 ET | **COVER** | `TJGC` | 18 | $26.61 | $2.04 | $+145.45 | $18,856.93 | ▲ +145.45 after sell → book $9,546.14; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-07 09:30 ET | **COVER** | `VICR` | 2 | $288.00 | $2.00 | $+42.57 | $18,278.94 | ▲ +42.57 after sell → book $9,544.14; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,278.94 | ▼ close $9,514.19 vs 09:30 $9,556.28 (session -29.96) | 16:00 close · cash $18,278.94 · equity $9,514.19 vs 09:30 $9,556.28 (-42.09; session marks -29.96) · 10 name(s) marked open→close (per-name table). DNA×72 09:30 $11.80 → close $11.47 +24.12; GRAL×6 09:30 $134.00 → close $132.90 +6.60; KOD×11 09:30 $91.74 → close $91.08 +7.26; MXL×8 09:30 $105.38 → close $107.36 -15.88; P×4 09:30 $144.58 → close $152.66 -32.32; PCVX×14 09:30 $65.86 → close $66.96 -15.40; PTC×5 09:30 $193.20 → close $193.60 -2.00; RXO×37 09:30 $28.34 → close $28.36 -0.56; SYNA×7 09:30 $118.50 → close $119.46 -6.72; VIAV×19 09:30 $46.53 → close $46.27 +4.94 | — |
| 2026-10-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,278.94 | ▲ 09:30 equity $9,514.19 vs yday $9,514.19 (-0.00) | 09:30 open · cash $18,278.94 (unchanged overnight, no fees) · equity $9,514.19 vs prior close $9,514.19 (-0.00) · 10 name(s) re-marked at the open (per-name table). DNA×72 yday $11.47 → 09:30 $11.47 -0.00; GRAL×6 yday $132.90 → 09:30 $132.90 -0.00; KOD×11 yday $91.08 → 09:30 $91.08 -0.00; MXL×8 yday $107.36 → 09:30 $107.36 -0.00; P×4 yday $152.66 → 09:30 $152.66 -0.00; PCVX×14 yday $66.96 → 09:30 $66.96 -0.00; PTC×5 yday $193.60 → 09:30 $193.60 -0.00; RXO×37 yday $28.36 → 09:30 $28.36 -0.00; SYNA×7 yday $119.46 → 09:30 $119.46 -0.00; VIAV×19 yday $46.27 → 09:30 $46.27 -0.00 | — |
| 2026-10-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,278.94 | ▲ close $9,514.19 vs 09:30 $9,514.19 (session +0.00) | 16:00 close · cash $18,278.94 · equity $9,514.19 vs 09:30 $9,514.19 (-0.00; session marks +0.00) · 10 name(s) marked open→close (per-name table). DNA×72 09:30 $11.47 → close $11.47 -0.00; GRAL×6 09:30 $132.90 → close $132.90 -0.00; KOD×11 09:30 $91.08 → close $91.08 -0.00; MXL×8 09:30 $107.36 → close $107.36 -0.00; P×4 09:30 $152.66 → close $152.66 -0.00; PCVX×14 09:30 $66.96 → close $66.96 -0.00; PTC×5 09:30 $193.60 → close $193.60 -0.00; RXO×37 09:30 $28.36 → close $28.36 -0.00; SYNA×7 09:30 $119.46 → close $119.46 -0.00; VIAV×19 09:30 $46.27 → close $46.27 -0.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VOYG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `FORM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ENTG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SATL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VOYG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LUNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `FORM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ENTG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SATL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `UMAC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `LPTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ALOY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `AEHR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `SGMT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `BYND` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AAOI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ELMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STDN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `REAX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `UMAC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `LPTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ALOY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `AEHR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CLYM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `SGMT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OABI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AZI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SENS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AZI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `PRQR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MSTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `NIQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CVI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NOG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `PRQR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NIQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `WIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NIQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `WIX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `HTFL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `SUJA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SUJA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `EL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FSM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MSTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ARCT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FSM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PURR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ARCT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ACB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AGCO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SID` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `AGCO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `IRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `LENZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `HOOD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `PURR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `CRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `IRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LENZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `HOOD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ATRC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XHLD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GPRO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CVI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TJGC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLMT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CRDL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `CVI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `FRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CLMT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `UGP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `PBR-A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `TJGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `FRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CLMT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `UGP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `PBR-A` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBLX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SION` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `BLSH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-18 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `CVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `CHPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `CVI` | no_price | no 09:30 open — carry |
| 2026-09-22 | `HAFN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `CHPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `FWDI` | no_price | no 09:30 open |
| 2026-09-22 | `NTSK` | no_price | no 09:30 open |
| 2026-09-22 | `FORM` | no_price | no 09:30 open |
| 2026-09-23 | `GLND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `GLND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SVIA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `FWDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `GME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `FORM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INDP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CTKB` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CAI` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `HLP` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GLND` | 261 | 2026-09-22 @ $2.94 | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; ret5=+136.1; leftover $770.06 |
| `USDE` | 59 | 2026-09-22 @ $12.99 | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; ret5=+69.4; leftover $770.06 |
| `HIVE` | 223 | 2026-09-22 @ $3.45 | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+16.4; leftover $770.06 |
| `VICR` | 2 | 2026-09-23 @ $266.50 | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.2; leftover $647.67 |
| `SVIA` | 144 | 2026-09-23 @ $4.49 | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list yday_gainer,yday_mover; ret5=+26.4; leftover $647.67 |
| `VERI` | 498 | 2026-09-23 @ $1.30 | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+15.3; leftover $647.67 |
| `FWDI` | 78 | 2026-09-23 @ $8.20 | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+17.6; leftover $647.67 |
| `GME` | 27 | 2026-09-23 @ $23.94 | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; ret5=+12.1; leftover $647.67 |
| `NTSK` | 34 | 2026-09-23 @ $18.57 | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+8.2; leftover $647.67 |
| `FORM` | 5 | 2026-09-23 @ $125.39 | Clock-B #5 extreme ext + fade/fail-break as a short; gate clk_ext_veto=True; list ohlc_hot; 🔵; ret5=+15.9; leftover $647.67 |
