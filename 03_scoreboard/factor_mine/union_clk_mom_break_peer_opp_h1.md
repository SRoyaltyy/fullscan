# Factor mine action — `union_clk_mom_break_peer_opp_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `opp_rvol` · size `leftover` · sell `list` · S-boost `none` · Clock-B #1 ∩ Theme Radar T−1 oppset

Cash book **-0.35%** ($9,965) · signal-only (no cash/fees) was +20.76%. Starts YES **26/30**. Fills 132 · skips 48 · realized $+1577.14.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: Theme Radar T−1 relative volume (Clock-B opportunity-set; not same-day RelVol).
- Must-have: Clock-B #1: moderate prior momentum, a completed 10-session breakout (or candle capture), and peer or sector camera green.
- Must-have: Theme Radar Clock-B opportunity-set: T−1 gap or RelVol (or week move) flagged — not today's Gap/RelVol.
- Must-not: Clock-B #5 long veto: extreme prior extension plus diminishing progress or a failed breakout.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by Theme Radar T−1 relative volume (Clock-B opportunity-set; not same-day RelVol) and keep the top 8.
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
- **Gate** `clk_mom_break_peer=True,oppset=True` · **rank** `opp_rvol` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,577.13.

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
| 2026-08-14 09:30 ET | **BUY** | `YSS` | 331 | $10.06 | $4.27 | — | $6,665.87 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ⚪; ret5=+5.7; leftover $3333.33 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 202 | $16.50 | $2.61 | — | $3,330.26 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $3333.33 | — |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 6 | $503.50 | $2.01 | — | $307.26 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable; 🔵; ⚪; ret5=+7.9; leftover $3333.33 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $307.26 | ▲ close $10,244.23 vs 09:30 $10,000.00 (session +253.11) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $307.26 | ▼ 09:30 equity $10,067.06 vs yday $10,244.23 (-177.17) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `YSS` | 331 | $10.36 | $4.35 | $+90.68 | $3,732.06 | ▲ +90.68 after sell → book $10,062.70; vs 09:30 mark -4.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 202 | $15.73 | $2.67 | $-160.81 | $6,906.86 | ▼ -160.81 after sell → book $10,060.04; vs 09:30 mark -2.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 6 | $525.53 | $2.04 | $+128.13 | $10,058.00 | ▲ +128.13 after sell → book $10,058.00; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 620 | $16.20 | $8.00 | — | $6.00 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $10058.00 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.00 | ▲ close $10,149.20 vs 09:30 $10,067.06 (session +99.20) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.00 | ▼ 09:30 equity $9,789.60 vs yday $10,149.20 (-359.60) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 620 | $15.78 | $8.18 | $-276.58 | $9,781.42 | ▼ -276.58 after sell → book $9,781.42; vs 09:30 mark -8.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,781.42 | ▲ close $9,781.42 vs 09:30 $9,789.60 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,781.42 | ▲ 09:30 equity $9,781.42 vs yday $9,781.42 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,781.42 | ▲ close $9,781.42 vs 09:30 $9,781.42 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,781.42 | ▲ 09:30 equity $9,781.42 vs yday $9,781.42 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 10 | $113.23 | $2.02 | — | $8,647.10 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1222.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 164 | $7.45 | $2.48 | — | $7,422.82 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1222.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 62 | $19.63 | $2.18 | — | $6,203.58 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1222.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $5,018.42 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1222.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 35 | $34.05 | $2.10 | — | $3,824.58 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+9.3; leftover $1222.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $2,607.63 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1222.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 59 | $20.55 | $2.17 | — | $1,393.02 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1222.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 41 | $29.20 | $2.11 | — | $193.70 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1222.68 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $193.70 | ▲ close $9,850.86 vs 09:30 $9,781.42 (session +86.64) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $193.70 | ▲ 09:30 equity $10,141.93 vs yday $9,850.86 (+291.07) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `MSTR` | 10 | $119.69 | $2.04 | $+60.54 | $1,388.56 | ▲ +60.54 after sell → book $10,139.89; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 164 | $7.09 | $2.52 | $-64.04 | $2,548.80 | ▼ -64.04 after sell → book $10,137.37; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 62 | $21.17 | $2.20 | $+91.11 | $3,859.15 | ▲ +91.11 after sell → book $10,135.18; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $5,101.46 | ▲ +57.15 after sell → book $10,133.13; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 35 | $34.31 | $2.12 | $+4.89 | $6,300.19 | ▲ +4.89 after sell → book $10,131.01; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $7,617.03 | ▲ +99.89 after sell → book $10,128.88; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 59 | $21.90 | $2.19 | $+75.30 | $8,906.94 | ▲ +75.30 after sell → book $10,126.69; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BLSH` | 41 | $29.75 | $2.13 | $+18.30 | $10,124.56 | ▲ +18.30 after sell → book $10,124.56; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 75 | $33.36 | $2.21 | — | $7,620.34 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $2531.14 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 4 | $623.26 | $2.00 | — | $5,125.30 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $2531.14 | — |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 19 | $127.43 | $2.05 | — | $2,702.09 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable; 🔵; ⚪; ret5=+7.9; leftover $2531.14 | — |
| 2026-08-21 09:30 ET | **BUY** | `DXYZ` | 72 | $34.89 | $2.21 | — | $187.80 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+8.6; leftover $2531.14 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $187.80 | ▲ close $10,227.79 vs 09:30 $10,141.93 (session +111.70) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $187.80 | ▼ 09:30 equity $10,114.47 vs yday $10,227.79 (-113.32) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 75 | $32.82 | $2.25 | $-44.96 | $2,647.05 | ▼ -44.96 after sell → book $10,112.22; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 4 | $653.04 | $2.03 | $+115.09 | $5,257.18 | ▲ +115.09 after sell → book $10,110.19; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 19 | $129.99 | $2.08 | $+44.52 | $7,724.91 | ▲ +44.52 after sell → book $10,108.11; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DXYZ` | 72 | $33.10 | $2.24 | $-133.32 | $10,105.88 | ▼ -133.32 after sell → book $10,105.88; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,105.88 | ▲ close $10,105.88 vs 09:30 $10,114.47 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,105.88 | ▲ 09:30 equity $10,105.88 vs yday $10,105.88 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `VALE` | 672 | $15.01 | $8.67 | — | $10.49 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list mover_buy; ⚪; ret5=+9.4; leftover $10105.88 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.49 | ▲ close $10,312.25 vs 09:30 $10,105.88 (session +215.04) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.49 | ▲ 09:30 equity $10,339.13 vs yday $10,312.25 (+26.88) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `VALE` | 672 | $15.37 | $8.86 | $+224.39 | $10,330.26 | ▲ +224.39 after sell → book $10,330.26; vs 09:30 mark -8.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BZ` | 615 | $16.77 | $7.93 | — | $8.78 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $10330.26 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.78 | ▲ close $11,595.38 vs 09:30 $10,339.13 (session +1,273.05) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.78 | ▼ 09:30 equity $11,386.28 vs yday $11,595.38 (-209.10) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 615 | $18.50 | $8.13 | $+1047.89 | $11,378.15 | ▲ +1,047.89 after sell → book $11,378.15; vs 09:30 mark -8.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,378.15 | ▲ close $11,378.15 vs 09:30 $11,386.28 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,378.15 | ▲ 09:30 equity $11,378.15 vs yday $11,378.15 (+0.00) | — | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 43 | $32.90 | $2.12 | — | $9,961.33 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1422.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 49 | $28.91 | $2.14 | — | $8,542.61 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; ret5=+9.2; leftover $1422.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 74 | $19.00 | $2.21 | — | $7,134.40 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+7.5; leftover $1422.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 170 | $8.35 | $2.50 | — | $5,712.40 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; ret5=+5.1; leftover $1422.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `S` | 66 | $21.49 | $2.19 | — | $4,291.87 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; ret5=+8.5; leftover $1422.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $2,984.06 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; ret5=+7.8; leftover $1422.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `PD` | 108 | $13.09 | $2.31 | — | $1,568.03 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; ret5=+4.2; leftover $1422.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `RBRK` | 14 | $98.95 | $2.03 | — | $180.70 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; ret5=+9.7; leftover $1422.27 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $180.70 | ▼ close $11,250.44 vs 09:30 $11,378.15 (session -110.21) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $180.70 | ▼ 09:30 equity $11,156.95 vs yday $11,250.44 (-93.49) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 43 | $31.15 | $2.14 | $-79.51 | $1,518.01 | ▼ -79.51 after sell → book $11,154.81; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 49 | $28.06 | $2.16 | $-45.94 | $2,890.79 | ▼ -45.94 after sell → book $11,152.65; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 74 | $18.12 | $2.23 | $-69.20 | $4,229.80 | ▼ -69.20 after sell → book $11,150.41; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 170 | $8.53 | $2.54 | $+25.56 | $5,677.36 | ▲ +25.56 after sell → book $11,147.87; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `S` | 66 | $21.45 | $2.21 | $-7.04 | $7,090.85 | ▼ -7.04 after sell → book $11,145.66; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 5 | $257.71 | $2.03 | $-21.28 | $8,377.38 | ▼ -21.28 after sell → book $11,143.64; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PD` | 108 | $13.58 | $2.34 | $+48.26 | $9,841.68 | ▲ +48.26 after sell → book $11,141.30; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RBRK` | 14 | $92.83 | $2.05 | $-89.76 | $11,139.24 | ▼ -89.76 after sell → book $11,139.24; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,139.24 | ▲ close $11,139.24 vs 09:30 $11,156.95 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,139.24 | ▲ 09:30 equity $11,139.24 vs yday $11,139.24 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,139.24 | ▲ close $11,139.24 vs 09:30 $11,139.24 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,139.24 | ▲ 09:30 equity $11,139.24 vs yday $11,139.24 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,139.24 | ▲ close $11,139.24 vs 09:30 $11,139.24 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,139.24 | ▲ 09:30 equity $11,139.24 vs yday $11,139.24 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 5 | $486.31 | $2.00 | — | $8,705.69 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list mover_buy; 🔵; ret5=+6.1; leftover $2784.81 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 346 | $8.03 | $4.46 | — | $5,922.84 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $2784.81 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 184 | $15.09 | $2.54 | — | $3,143.74 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+6.1; leftover $2784.81 | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 52 | $52.88 | $2.15 | — | $391.84 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten; 🔵; ⚪; ret5=+9.2; leftover $2784.81 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $391.84 | ▲ close $11,281.67 vs 09:30 $11,139.24 (session +153.58) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $391.84 | ▼ 09:30 equity $11,225.72 vs yday $11,281.67 (-55.95) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 346 | $7.91 | $4.54 | $-50.53 | $3,124.15 | ▼ -50.53 after sell → book $11,221.17; vs 09:30 mark -4.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 184 | $15.34 | $2.60 | $+40.86 | $5,944.12 | ▲ +40.86 after sell → book $11,218.58; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 52 | $52.03 | $2.18 | $-48.52 | $8,647.50 | ▼ -48.52 after sell → book $11,216.40; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 12 | $98.15 | $2.03 | — | $7,467.67 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; ret5=+5.9; leftover $1235.36 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 16 | $75.65 | $2.04 | — | $6,255.24 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1235.36 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 8 | $137.35 | $2.01 | — | $5,154.42 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+5.4; leftover $1235.36 | — |
| 2026-09-04 09:30 ET | **BUY** | `KYIV` | 86 | $14.24 | $2.25 | — | $3,927.53 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ⚪; ret5=+5.2; leftover $1235.36 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 7 | $167.55 | $2.01 | — | $2,752.67 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list earn_react; ret5=+0.9; leftover $1235.36 | — |
| 2026-09-04 09:30 ET | **BUY** | `BLSH` | 35 | $34.69 | $2.10 | — | $1,536.43 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+7.9; leftover $1235.36 | — |
| 2026-09-04 09:30 ET | **BUY** | `ZETA` | 37 | $32.65 | $2.10 | — | $326.28 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+8.1; leftover $1235.36 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $326.28 | ▲ close $11,319.37 vs 09:30 $11,225.72 (session +117.50) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $326.28 | ▼ 09:30 equity $11,247.53 vs yday $11,319.37 (-71.84) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 5 | $521.15 | $2.04 | $+170.16 | $2,929.99 | ▲ +170.16 after sell → book $11,245.49; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 12 | $100.58 | $2.05 | $+25.09 | $4,134.91 | ▲ +25.09 after sell → book $11,243.45; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 16 | $78.84 | $2.06 | $+46.94 | $5,394.29 | ▲ +46.94 after sell → book $11,241.39; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MSTR` | 8 | $137.62 | $2.03 | $-1.89 | $6,493.21 | ▼ -1.89 after sell → book $11,239.35; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `KYIV` | 86 | $14.14 | $2.27 | $-13.12 | $7,706.98 | ▼ -13.12 after sell → book $11,237.08; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 7 | $160.52 | $2.03 | $-53.25 | $8,828.59 | ▼ -53.25 after sell → book $11,235.05; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BLSH` | 35 | $35.90 | $2.12 | $+38.14 | $10,082.98 | ▲ +38.14 after sell → book $11,232.94; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ZETA` | 37 | $31.08 | $2.12 | $-62.31 | $11,230.81 | ▼ -62.31 after sell → book $11,230.81; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,230.81 | ▲ close $11,230.81 vs 09:30 $11,247.53 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,230.81 | ▲ 09:30 equity $11,230.81 vs yday $11,230.81 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,230.81 | ▲ close $11,230.81 vs 09:30 $11,230.81 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,230.81 | ▲ 09:30 equity $11,230.81 vs yday $11,230.81 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,230.81 | ▲ close $11,230.81 vs 09:30 $11,230.81 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,230.81 | ▲ 09:30 equity $11,230.81 vs yday $11,230.81 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 18 | $77.33 | $2.04 | — | $9,836.83 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=+2.5; leftover $1403.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `PBR` | 66 | $21.21 | $2.19 | — | $8,434.78 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+2.5; leftover $1403.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `GME` | 66 | $21.04 | $2.19 | — | $7,043.95 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+7.5; leftover $1403.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `INSP` | 20 | $69.88 | $2.05 | — | $5,644.30 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+8.0; leftover $1403.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `PUMP` | 121 | $11.57 | $2.35 | — | $4,241.98 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+5.9; leftover $1403.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 138 | $10.11 | $2.40 | — | $2,844.40 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $1403.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 26 | $52.55 | $2.07 | — | $1,476.03 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1403.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 121 | $11.55 | $2.35 | — | $76.13 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1403.85 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.13 | ▲ close $11,359.85 vs 09:30 $11,230.81 (session +146.68) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.13 | ▼ 09:30 equity $11,303.64 vs yday $11,359.85 (-56.21) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `VIST` | 18 | $77.10 | $2.07 | $-8.25 | $1,461.86 | ▼ -8.25 after sell → book $11,301.57; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PBR` | 66 | $21.23 | $2.21 | $-3.08 | $2,860.83 | ▼ -3.08 after sell → book $11,299.36; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INSP` | 20 | $72.14 | $2.07 | $+41.08 | $4,301.56 | ▲ +41.08 after sell → book $11,297.29; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PUMP` | 121 | $11.17 | $2.38 | $-53.14 | $5,650.75 | ▼ -53.14 after sell → book $11,294.91; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAGS` | 138 | $10.00 | $2.44 | $-20.02 | $7,028.31 | ▼ -20.02 after sell → book $11,292.47; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 26 | $56.90 | $2.09 | $+108.94 | $8,505.62 | ▲ +108.94 after sell → book $11,290.38; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `FUBO` | 121 | $11.56 | $2.38 | $-3.53 | $9,901.99 | ▼ -3.53 after sell → book $11,287.99; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,901.99 | ▲ close $11,328.91 vs 09:30 $11,303.64 (session +40.92) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,901.99 | ▼ 09:30 equity $11,321.65 vs yday $11,328.91 (-7.26) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `GME` | 66 | $21.51 | $2.21 | $+26.62 | $11,319.44 | ▲ +26.62 after sell → book $11,319.44; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,319.44 | ▲ close $11,319.44 vs 09:30 $11,321.65 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,319.44 | ▲ 09:30 equity $11,319.44 vs yday $11,319.44 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `TALO` | 79 | $17.87 | $2.23 | — | $9,905.49 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+6.8; leftover $1414.93 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 16 | $87.40 | $2.04 | — | $8,505.05 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1414.93 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 241 | $5.87 | $3.11 | — | $7,087.27 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1414.93 | — |
| 2026-09-16 09:30 ET | **BUY** | `ILMN` | 6 | $224.49 | $2.01 | — | $5,738.32 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+5.3; leftover $1414.93 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 52 | $27.09 | $2.15 | — | $4,327.50 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1414.93 | — |
| 2026-09-16 09:30 ET | **BUY** | `MRCY` | 16 | $87.52 | $2.04 | — | $2,925.14 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+4.3; leftover $1414.93 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 20 | $68.79 | $2.05 | — | $1,547.29 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1414.93 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 35 | $39.99 | $2.10 | — | $145.54 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+9.3; leftover $1414.93 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $145.54 | ▼ close $11,120.60 vs 09:30 $11,319.44 (session -181.13) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $145.54 | ▲ 09:30 equity $11,247.86 vs yday $11,120.60 (+127.26) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `TALO` | 79 | $17.19 | $2.25 | $-58.20 | $1,501.30 | ▼ -58.20 after sell → book $11,245.61; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 16 | $83.20 | $2.06 | $-71.30 | $2,830.44 | ▼ -71.30 after sell → book $11,243.55; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 241 | $5.58 | $3.16 | $-76.16 | $4,172.06 | ▼ -76.16 after sell → book $11,240.39; vs 09:30 mark -3.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ILMN` | 6 | $233.85 | $2.03 | $+52.12 | $5,573.13 | ▲ +52.12 after sell → book $11,238.36; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 52 | $28.23 | $2.17 | $+54.97 | $7,038.93 | ▲ +54.97 after sell → book $11,236.20; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `MRCY` | 16 | $89.27 | $2.06 | $+23.90 | $8,465.19 | ▲ +23.90 after sell → book $11,234.14; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 20 | $72.70 | $2.07 | $+74.08 | $9,917.12 | ▲ +74.08 after sell → book $11,232.07; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 35 | $37.57 | $2.12 | $-88.91 | $11,229.95 | ▼ -88.91 after sell → book $11,229.95; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 72 | $25.95 | $2.21 | — | $9,359.34 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1871.66 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMRX` | 100 | $18.56 | $2.29 | — | $7,501.05 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+4.8; leftover $1871.66 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 246 | $7.59 | $3.17 | — | $5,630.74 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1871.66 | — |
| 2026-09-17 09:30 ET | **BUY** | `FTAI` | 9 | $196.50 | $2.02 | — | $3,860.22 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+2.5; leftover $1871.66 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 10 | $170.85 | $2.02 | — | $2,149.70 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1871.66 | — |
| 2026-09-17 09:30 ET | **BUY** | `FOSL` | 342 | $5.46 | $4.41 | — | $277.97 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+9.5; leftover $1871.66 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $277.97 | ▲ close $11,410.10 vs 09:30 $11,247.86 (session +196.27) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $277.97 | ▲ 09:30 equity $11,443.84 vs yday $11,410.10 (+33.74) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 72 | $26.14 | $2.23 | $+9.24 | $2,157.82 | ▲ +9.24 after sell → book $11,441.61; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMRX` | 100 | $18.12 | $2.32 | $-48.61 | $3,967.50 | ▼ -48.61 after sell → book $11,439.29; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 246 | $7.98 | $3.23 | $+89.54 | $5,927.35 | ▲ +89.54 after sell → book $11,436.06; vs 09:30 mark -3.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FTAI` | 9 | $195.55 | $2.04 | $-12.61 | $7,685.26 | ▼ -12.61 after sell → book $11,434.02; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 10 | $182.33 | $2.04 | $+110.74 | $9,506.51 | ▲ +110.74 after sell → book $11,431.97; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FOSL` | 342 | $5.63 | $4.48 | $+49.24 | $11,427.49 | ▲ +49.24 after sell → book $11,427.49; vs 09:30 mark -4.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 110 | $14.79 | $2.32 | — | $9,798.27 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1632.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 116 | $14.07 | $2.34 | — | $8,163.81 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1632.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 78 | $20.91 | $2.22 | — | $6,530.61 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1632.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `SYM` | 36 | $44.70 | $2.10 | — | $4,919.31 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+8.5; leftover $1632.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `AMD` | 2 | $547.37 | $2.00 | — | $3,822.57 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+8.2; leftover $1632.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `SHLS` | 213 | $7.64 | $2.75 | — | $2,192.50 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+7.6; leftover $1632.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 280 | $5.83 | $3.61 | — | $556.49 | — | Clock-B #1 ∩ Theme Radar T−1 oppset; gate clk_mom_break_peer=True,oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1632.50 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $556.49 | ▼ close $11,306.21 vs 09:30 $11,443.84 (session -103.94) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $556.49 | ▲ 09:30 equity $11,594.70 vs yday $11,306.21 (+288.49) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 110 | $14.58 | $2.35 | $-27.77 | $2,157.94 | ▼ -27.77 after sell → book $11,592.35; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 116 | $13.90 | $2.37 | $-24.43 | $3,767.97 | ▼ -24.43 after sell → book $11,589.98; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 78 | $21.65 | $2.25 | $+53.25 | $5,454.42 | ▲ +53.25 after sell → book $11,587.73; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SYM` | 36 | $42.42 | $2.12 | $-86.30 | $6,979.42 | ▼ -86.30 after sell → book $11,585.61; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `AMD` | 2 | $583.88 | $2.02 | $+69.01 | $8,145.16 | ▲ +69.01 after sell → book $11,583.59; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SHLS` | 213 | $7.71 | $2.80 | $+9.37 | $9,784.60 | ▲ +9.37 after sell → book $11,580.80; vs 09:30 mark -2.79 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BNC` | 280 | $6.42 | $3.67 | $+156.52 | $11,577.13 | ▲ +156.52 after sell → book $11,577.13; vs 09:30 mark -3.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,577.13 | ▲ close $11,577.13 vs 09:30 $11,594.70 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,577.13 | ▲ 09:30 equity $11,577.13 vs yday $11,577.13 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,577.13 | ▲ close $11,577.13 vs 09:30 $11,577.13 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,577.13 | ▲ 09:30 equity $11,577.13 vs yday $11,577.13 (-0.00) | — | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,577.13 | ▲ close $11,577.13 vs 09:30 $11,577.13 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,577.13 | ▲ 09:30 equity $11,577.13 vs yday $11,577.13 (-0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,577.13 | ▲ close $11,577.13 vs 09:30 $11,577.13 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,964.85 | ▲ 09:30 equity $9,964.85 vs yday $9,964.85 (+0.00) | 09:30 open · cash $9,964.85 · no holdings · equity $9,964.85 vs prior close $9,964.85 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,964.85 | ▲ close $9,964.85 vs 09:30 $9,964.85 (session +0.00) | 16:00 close · cash $9,964.85 · no lots left · equity $9,964.85. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MRCY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DK` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FNKO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `AGCO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `HAL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TSLA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AGRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VIR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `KEP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `STX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `NMAX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `KGS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IOVA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PD` | hard_red | hard-red S=-3.84 sit; no new buys |
