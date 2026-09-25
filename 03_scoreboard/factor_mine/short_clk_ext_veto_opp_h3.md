# Factor mine action — `short_clk_ext_veto_opp_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `opp_rvol` · size `leftover` · sell `list` · S-boost `none` · Clock-B #5 ∩ Theme Radar T−1 oppset

Cash book **+3.16%** ($10,316) · signal-only (no cash/fees) was -20.56%. Starts YES **26/30**. Fills 92 · skips 114 · realized $+404.70.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: Theme Radar T−1 relative volume (Clock-B opportunity-set; not same-day RelVol).
- Must-have: Clock-B #5 long veto: extreme prior extension plus diminishing progress or a failed breakout.
- Must-have: Theme Radar Clock-B opportunity-set: T−1 gap or RelVol (or week move) flagged — not today's Gap/RelVol.

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- Sort the keepers by Theme Radar T−1 relative volume (Clock-B opportunity-set; not same-day RelVol) and keep the top 8.
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
- **Gate** `clk_ext_veto=True,oppset=True` · **rank** `opp_rvol` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,404.70.

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
| 2026-08-14 09:30 ET | **SHORT** | `ARX` | 31 | $19.57 | $2.12 | — | $10,604.55 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `TPG` | 11 | $55.29 | $2.06 | — | $11,210.68 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+13.8; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LIFE` | 17 | $35.04 | $2.08 | — | $11,804.28 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 32 | $19.17 | $2.12 | — | $12,415.60 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `BETA` | 24 | $25.21 | $2.10 | — | $13,018.54 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `VELO` | 40 | $15.38 | $2.15 | — | $13,631.60 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `SATL` | 104 | $5.98 | $2.35 | — | $14,251.17 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+16.9; leftover $625.00 | — |
| 2026-08-14 09:30 ET | **SHORT** | `VOYG` | 14 | $44.49 | $2.07 | — | $14,871.96 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+15.6; leftover $625.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,871.96 | ▲ close $10,047.03 vs 09:30 $10,000.00 (session +64.07) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,871.96 | ▼ 09:30 equity $10,032.85 vs yday $10,047.03 (-14.18) | — | — |
| 2026-08-17 09:30 ET | **SHORT** | `CAPR` | 91 | $6.87 | $2.31 | — | $15,494.82 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+62.6; leftover $627.05 | — |
| 2026-08-17 09:30 ET | **SHORT** | `UMAC` | 19 | $32.55 | $2.08 | — | $16,111.19 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $627.05 | — |
| 2026-08-17 09:30 ET | **SHORT** | `SGMT` | 60 | $10.45 | $2.21 | — | $16,735.98 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+16.4; leftover $627.05 | — |
| 2026-08-17 09:30 ET | **SHORT** | `CLYM` | 38 | $16.25 | $2.14 | — | $17,351.34 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list yday_gainer,ohlc_hot; ret5=+16.6; leftover $627.05 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ALOY` | 42 | $14.66 | $2.15 | — | $17,964.91 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $627.05 | — |
| 2026-08-17 09:30 ET | **SHORT** | `LPTH` | 41 | $14.94 | $2.15 | — | $18,575.30 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $627.05 | — |
| 2026-08-17 09:30 ET | **SHORT** | `MP` | 10 | $58.01 | $2.06 | — | $19,153.34 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $627.05 | — |
| 2026-08-17 09:30 ET | **SHORT** | `SMJF` | 62 | $10.10 | $2.21 | — | $19,777.33 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list mover_buy; ret5=+22.8; leftover $627.05 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,777.33 | ▼ close $9,932.88 vs 09:30 $10,032.85 (session -82.66) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,777.33 | ▲ 09:30 equity $10,225.37 vs yday $9,932.88 (+292.49) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,777.33 | ▼ close $10,074.70 vs 09:30 $10,225.37 (session -150.67) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,777.33 | ▼ 09:30 equity $10,042.11 vs yday $10,074.70 (-32.59) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `ARX` | 31 | $19.58 | $2.08 | $-4.51 | $19,168.27 | ▼ -4.51 after sell → book $10,040.03; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `TPG` | 11 | $52.26 | $2.02 | $+29.25 | $18,591.38 | ▲ +29.25 after sell → book $10,038.00; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `LIFE` | 17 | $34.37 | $2.04 | $+7.27 | $18,005.05 | ▲ +7.27 after sell → book $10,035.96; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `BETA` | 24 | $26.80 | $2.06 | $-42.32 | $17,359.79 | ▼ -42.32 after sell → book $10,033.90; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `VELO` | 40 | $14.51 | $2.11 | $+30.54 | $16,777.28 | ▲ +30.54 after sell → book $10,031.79; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `SATL` | 104 | $5.82 | $2.30 | $+11.99 | $16,169.70 | ▲ +11.99 after sell → book $10,029.49; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `VOYG` | 14 | $41.93 | $2.03 | $+31.74 | $15,580.65 | ▲ +31.74 after sell → book $10,027.46; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,580.65 | ▲ close $10,130.71 vs 09:30 $10,042.11 (session +103.25) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,580.65 | ▲ 09:30 equity $10,218.92 vs yday $10,130.71 (+88.21) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 32 | $18.13 | $2.09 | $+29.07 | $14,998.40 | ▲ +29.07 after sell → book $10,216.83; vs 09:30 mark -2.09 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `CAPR` | 91 | $7.66 | $2.26 | $-76.46 | $14,299.08 | ▼ -76.46 after sell → book $10,214.57; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `UMAC` | 19 | $28.32 | $2.05 | $+76.24 | $13,758.95 | ▲ +76.24 after sell → book $10,212.52; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `SGMT` | 60 | $10.48 | $2.17 | $-6.18 | $13,127.98 | ▼ -6.18 after sell → book $10,210.35; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `CLYM` | 38 | $17.16 | $2.10 | $-38.82 | $12,473.80 | ▼ -38.82 after sell → book $10,208.25; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ALOY` | 42 | $12.06 | $2.12 | $+104.93 | $11,965.16 | ▲ +104.93 after sell → book $10,206.13; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `LPTH` | 41 | $13.09 | $2.11 | $+71.59 | $11,426.36 | ▲ +71.59 after sell → book $10,204.02; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `MP` | 10 | $55.77 | $2.02 | $+18.32 | $10,866.64 | ▲ +18.32 after sell → book $10,202.00; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `SMJF` | 62 | $10.72 | $2.18 | $-42.83 | $10,199.82 | ▼ -42.83 after sell → book $10,199.82; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEM` | 10 | $61.83 | $2.06 | — | $10,816.07 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $637.49 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WPM` | 4 | $144.54 | $2.04 | — | $11,392.19 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $637.49 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $12,003.50 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $637.49 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AUGO` | 7 | $83.58 | $2.05 | — | $12,586.51 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+9.6; leftover $637.49 | — |
| 2026-08-20 09:30 ET | **SHORT** | `SCZM` | 67 | $9.46 | $2.23 | — | $13,218.11 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $637.49 | — |
| 2026-08-20 09:30 ET | **SHORT** | `BBNX` | 31 | $20.00 | $2.12 | — | $13,835.99 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $637.49 | — |
| 2026-08-20 09:30 ET | **SHORT** | `SENS` | 71 | $8.91 | $2.24 | — | $14,466.35 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $637.49 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 53 | $11.81 | $2.19 | — | $15,090.36 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $637.49 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,090.36 | ▼ close $10,082.68 vs 09:30 $10,218.92 (session -100.19) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,090.36 | ▼ 09:30 equity $9,981.79 vs yday $10,082.68 (-100.89) | — | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARCT` | 89 | $11.13 | $2.31 | — | $16,078.62 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $998.18 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SMJF` | 87 | $11.35 | $2.30 | — | $17,063.77 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+13.4; leftover $998.18 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MSTR` | 8 | $119.69 | $2.06 | — | $18,019.23 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list yday_gainer,ohlc_hot; ret5=+15.7; leftover $998.18 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ABTC` | 115 | $8.66 | $2.39 | — | $19,012.74 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $998.18 | — |
| 2026-08-21 09:30 ET | **SHORT** | `CAI` | 40 | $24.73 | $2.16 | — | $19,999.78 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ⚪; ret5=+10.8; leftover $998.18 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,999.78 | ▼ close $9,748.15 vs 09:30 $9,981.79 (session -222.43) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,999.78 | ▲ 09:30 equity $9,793.77 vs yday $9,748.15 (+45.62) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,999.78 | ▼ close $9,676.08 vs 09:30 $9,793.77 (session -117.68) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,999.78 | ▲ 09:30 equity $9,753.00 vs yday $9,676.08 (+76.92) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `TEM` | 10 | $66.58 | $2.02 | $-51.58 | $19,331.96 | ▼ -51.58 after sell → book $9,750.98; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WPM` | 4 | $156.51 | $2.00 | $-51.92 | $18,703.92 | ▼ -51.92 after sell → book $9,748.98; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $212.00 | $2.00 | $-26.68 | $18,065.92 | ▼ -26.68 after sell → book $9,746.98; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AUGO` | 7 | $85.78 | $2.01 | $-19.46 | $17,463.45 | ▼ -19.46 after sell → book $9,744.97; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `SCZM` | 67 | $9.45 | $2.19 | $-3.75 | $16,828.11 | ▼ -3.75 after sell → book $9,742.78; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `BBNX` | 31 | $19.92 | $2.08 | $-1.72 | $16,208.51 | ▼ -1.72 after sell → book $9,740.70; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `SENS` | 71 | $9.36 | $2.20 | $-36.40 | $15,541.74 | ▼ -36.40 after sell → book $9,738.49; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 53 | $11.00 | $2.15 | $+38.86 | $14,956.59 | ▲ +38.86 after sell → book $9,736.34; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `LIFE` | 26 | $36.96 | $2.11 | — | $15,915.44 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $973.63 | — |
| 2026-08-25 09:30 ET | **SHORT** | `RUM` | 103 | $9.42 | $2.35 | — | $16,883.35 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $973.63 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 71 | $13.62 | $2.25 | — | $17,848.47 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $973.63 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ANRO` | 26 | $36.52 | $2.11 | — | $18,795.88 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+7.9; leftover $973.63 | — |
| 2026-08-25 09:30 ET | **SHORT** | `HQ` | 50 | $19.40 | $2.19 | — | $19,763.69 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+15.7; leftover $973.63 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,763.69 | ▼ close $9,287.64 vs 09:30 $9,753.00 (session -437.68) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,763.69 | ▲ 09:30 equity $9,435.41 vs yday $9,287.64 (+147.77) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `ARCT` | 89 | $15.35 | $2.26 | $-380.15 | $18,395.28 | ▼ -380.15 after sell → book $9,433.15; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SMJF` | 87 | $11.15 | $2.25 | $+12.85 | $17,422.98 | ▲ +12.85 after sell → book $9,430.90; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MSTR` | 8 | $123.26 | $2.01 | $-32.63 | $16,434.89 | ▼ -32.63 after sell → book $9,428.89; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ABTC` | 115 | $8.84 | $2.33 | $-25.43 | $15,415.95 | ▼ -25.43 after sell → book $9,426.55; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `CAI` | 40 | $27.70 | $2.11 | $-123.07 | $14,305.84 | ▼ -123.07 after sell → book $9,424.44; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `CAPR` | 142 | $8.29 | $2.48 | — | $15,480.54 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $1178.06 | — |
| 2026-08-26 09:30 ET | **SHORT** | `KURA` | 86 | $13.63 | $2.30 | — | $16,650.42 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+10.2; leftover $1178.06 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 96 | $12.22 | $2.34 | — | $17,821.20 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+12.4; leftover $1178.06 | — |
| 2026-08-26 09:30 ET | **SHORT** | `FIGR` | 29 | $40.50 | $2.13 | — | $18,993.57 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+15.8; leftover $1178.06 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,993.57 | ▲ close $9,509.15 vs 09:30 $9,435.41 (session +93.96) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,993.57 | ▲ 09:30 equity $9,527.10 vs yday $9,509.15 (+17.95) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,993.57 | ▼ close $9,393.34 vs 09:30 $9,527.10 (session -133.76) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,993.57 | ▲ 09:30 equity $9,521.72 vs yday $9,393.34 (+128.38) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `LIFE` | 26 | $39.60 | $2.07 | $-72.82 | $17,961.90 | ▼ -72.82 after sell → book $9,519.65; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `RUM` | 103 | $9.30 | $2.30 | $+7.71 | $17,001.70 | ▲ +7.71 after sell → book $9,517.35; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 71 | $13.90 | $2.20 | $-23.98 | $16,012.60 | ▼ -23.98 after sell → book $9,515.15; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ANRO` | 26 | $35.00 | $2.07 | $+35.34 | $15,100.53 | ▲ +35.34 after sell → book $9,513.08; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `HQ` | 50 | $16.30 | $2.14 | $+150.67 | $14,283.39 | ▲ +150.67 after sell → book $9,510.94; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SBET` | 549 | $8.65 | $7.34 | — | $19,024.90 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+17.0; leftover $4755.47 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,024.90 | ▲ close $9,937.51 vs 09:30 $9,521.72 (session +433.91) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,024.90 | ▲ 09:30 equity $9,956.15 vs yday $9,937.51 (+18.64) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `CAPR` | 142 | $9.50 | $2.42 | $-176.72 | $17,673.49 | ▼ -176.72 after sell → book $9,953.74; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `KURA` | 86 | $12.71 | $2.25 | $+74.57 | $16,578.18 | ▲ +74.57 after sell → book $9,951.49; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 96 | $11.10 | $2.28 | $+102.91 | $15,510.30 | ▲ +102.91 after sell → book $9,949.21; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `FIGR` | 29 | $35.77 | $2.08 | $+132.96 | $14,470.89 | ▲ +132.96 after sell → book $9,947.13; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,470.89 | ▼ close $9,733.02 vs 09:30 $9,956.15 (session -214.11) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,470.89 | ▲ 09:30 equity $9,919.68 vs yday $9,733.02 (+186.66) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,470.89 | ▲ close $9,996.54 vs 09:30 $9,919.68 (session +76.86) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,470.89 | ▲ 09:30 equity $10,073.40 vs yday $9,996.54 (+76.86) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SBET` | 549 | $8.01 | $7.08 | $+336.94 | $10,066.32 | ▲ +336.94 after sell → book $10,066.32; vs 09:30 mark -7.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,066.32 | ▲ close $10,066.32 vs 09:30 $10,073.40 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,066.32 | ▲ 09:30 equity $10,066.32 vs yday $10,066.32 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **SHORT** | `AGCO` | 13 | $127.91 | $2.10 | — | $11,727.05 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1677.72 | — |
| 2026-09-03 09:30 ET | **SHORT** | `ARCT` | 100 | $16.77 | $2.37 | — | $13,401.69 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1677.72 | — |
| 2026-09-03 09:30 ET | **SHORT** | `ASST` | 65 | $25.62 | $2.25 | — | $15,065.06 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+13.1; leftover $1677.72 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,065.06 | ▲ close $10,130.10 vs 09:30 $10,066.32 (session +70.49) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,065.06 | ▲ 09:30 equity $10,239.50 vs yday $10,130.10 (+109.40) | — | — |
| 2026-09-04 09:30 ET | **SHORT** | `HOOD` | 42 | $120.47 | $2.31 | — | $20,122.70 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+13.6; leftover $5119.75 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,122.70 | ▼ close $9,913.78 vs 09:30 $10,239.50 (session -323.41) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,122.70 | ▼ 09:30 equity $9,873.34 vs yday $9,913.78 (-40.44) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,122.70 | ▲ close $10,210.99 vs 09:30 $9,873.34 (session +337.65) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,122.70 | ▼ 09:30 equity $10,024.39 vs yday $10,210.99 (-186.60) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `AGCO` | 13 | $127.69 | $2.03 | $-1.27 | $18,460.70 | ▼ -1.27 after sell → book $10,022.36; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `ARCT` | 100 | $15.46 | $2.29 | $+126.34 | $16,912.41 | ▲ +126.34 after sell → book $10,020.07; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `ASST` | 65 | $28.00 | $2.19 | $-158.81 | $15,090.23 | ▼ -158.81 after sell → book $10,017.89; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,090.23 | ▲ close $10,248.47 vs 09:30 $10,024.39 (session +230.58) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,090.23 | ▲ 09:30 equity $10,357.25 vs yday $10,248.47 (+108.78) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `HOOD` | 42 | $112.69 | $2.12 | $+322.55 | $10,355.13 | ▲ +322.55 after sell → book $10,355.13; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,355.13 | ▲ close $10,355.13 vs 09:30 $10,357.25 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,355.13 | ▲ 09:30 equity $10,355.13 vs yday $10,355.13 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **SHORT** | `HAFN` | 277 | $9.32 | $3.71 | — | $12,933.06 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+5.4; leftover $2588.78 | — |
| 2026-09-11 09:30 ET | **SHORT** | `UGP` | 342 | $7.55 | $4.56 | — | $15,510.60 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+6.0; leftover $2588.78 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,510.60 | ▲ close $10,364.44 vs 09:30 $10,355.13 (session +17.58) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,510.60 | ▲ 09:30 equity $10,417.21 vs yday $10,364.44 (+52.77) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,510.60 | ▼ close $10,381.71 vs 09:30 $10,417.21 (session -35.50) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,510.60 | ▲ 09:30 equity $10,385.13 vs yday $10,381.71 (+3.42) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,510.60 | ▼ close $10,340.16 vs 09:30 $10,385.13 (session -44.97) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,510.60 | ▼ 09:30 equity $10,302.85 vs yday $10,340.16 (-37.31) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `HAFN` | 277 | $9.59 | $3.57 | $-82.07 | $12,850.60 | ▼ -82.07 after sell → book $10,299.28; vs 09:30 mark -3.57 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `UGP` | 342 | $7.46 | $4.41 | $+21.81 | $10,294.87 | ▲ +21.81 after sell → book $10,294.87; vs 09:30 mark -4.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,294.87 | ▲ close $10,294.87 vs 09:30 $10,302.85 (session +0.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,294.87 | ▲ 09:30 equity $10,294.87 vs yday $10,294.87 (-0.00) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,294.87 | ▲ close $10,294.87 vs 09:30 $10,294.87 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,294.87 | ▲ 09:30 equity $10,294.87 vs yday $10,294.87 (-0.00) | — | — |
| 2026-09-18 09:30 ET | **SHORT** | `CHPT` | 514 | $10.00 | $6.90 | — | $15,427.97 | — | Clock-B #5 ∩ Theme Radar T−1 oppset; gate clk_ext_veto=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+11.1; leftover $5147.43 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,427.97 | ▼ close $10,123.49 vs 09:30 $10,294.87 (session -164.48) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,427.97 | ▲ 09:30 equity $10,123.49 vs yday $10,123.49 (-0.00) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,427.97 | ▲ close $10,591.23 vs 09:30 $10,123.49 (session +467.74) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,427.97 | ▲ 09:30 equity $10,591.23 vs yday $10,591.23 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,427.97 | ▲ close $10,591.23 vs 09:30 $10,591.23 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,427.97 | ▼ 09:30 equity $10,411.33 vs yday $10,591.23 (-179.90) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `CHPT` | 514 | $9.76 | $6.63 | $+109.83 | $10,404.70 | ▲ +109.83 after sell → book $10,404.70; vs 09:30 mark -6.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,404.70 | ▲ close $10,404.70 vs 09:30 $10,411.33 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,404.70 | ▲ 09:30 equity $10,404.70 vs yday $10,404.70 (-0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,404.70 | ▲ close $10,404.70 vs 09:30 $10,404.70 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,316.39 | ▲ 09:30 equity $10,316.39 vs yday $10,316.39 (+0.00) | 09:30 open · cash $10,316.39 · no holdings · equity $10,316.39 vs prior close $10,316.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,316.39 | ▲ close $10,316.39 vs 09:30 $10,316.39 (session +0.00) | 16:00 close · cash $10,316.39 · no lots left · equity $10,316.39. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LUNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VELO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SATL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VOYG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LUNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VELO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SATL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VOYG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `UMAC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `SGMT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ALOY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `LPTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `MP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `SMJF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OCUL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AAOI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ELMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STDN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `UMAC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `SGMT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CLYM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ALOY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `LPTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `MP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `SMJF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BBNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SENS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `SMJF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MSTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CVI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `SMJF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ANRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `HQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ANRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `FIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ARCT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MSTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `PURR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AGCO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `AGCO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HOOD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `PURR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `HOOD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ATRC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `UGP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `UGP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RBLX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `BLSH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SION` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-21 | `CHPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `CHPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
