# Factor mine action — `flatten_vol_g_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · flatten wish-list ∩ vol🟢

Cash book **-29.32%** ($7,068) · signal-only (no cash/fees) was -19.12%. Starts YES **6/30**. Fills 58 · skips 71 · realized $-2440.55.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the flatten wish-list (names the flatten board wanted that morning) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the flatten wish-list (names the flatten board wanted that morning).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the volume camera (is this name unusually active?) is green.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the flatten wish-list (names the flatten board wanted that morning) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
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

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,517.69.

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
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 3333 | $1.50 | $43.00 | — | $4,957.50 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $5000.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 334 | $14.80 | $4.31 | — | $10.00 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-9.9; leftover $5000.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.00 | ▼ close $9,828.63 vs 09:30 $10,000.00 (session -124.07) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.00 | ▼ 09:30 equity $9,641.94 vs yday $9,828.63 (-186.69) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $1.81 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $10.00 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.81 | ▲ close $9,864.51 vs 09:30 $9,641.94 (session +222.66) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.81 | ▼ 09:30 equity $9,554.21 vs yday $9,864.51 (-310.30) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.81 | ▼ close $9,201.20 vs 09:30 $9,554.21 (session -353.01) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.81 | ▼ 09:30 equity $9,094.55 vs yday $9,201.20 (-106.65) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 3333 | $1.42 | $43.59 | $-353.22 | $4,691.08 | ▼ -353.22 after sell → book $9,050.96; vs 09:30 mark -43.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BETR` | 334 | $13.03 | $4.40 | $-599.89 | $9,038.70 | ▼ -599.89 after sell → book $9,046.56; vs 09:30 mark -4.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,038.70 | ▲ close $9,046.64 vs 09:30 $9,094.55 (session +0.08) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,038.70 | ▼ 09:30 equity $9,046.54 vs yday $9,046.64 (-0.10) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 2 | $3.92 | $0.10 | $-0.45 | $9,046.44 | ▼ -0.45 after sell → book $9,046.44; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 55 | $20.55 | $2.15 | — | $7,914.03 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1130.80 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $6,819.89 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1130.80 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 54 | $20.65 | $2.15 | — | $5,702.64 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1130.80 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 195 | $5.77 | $2.58 | — | $4,574.91 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1130.80 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 57 | $19.63 | $2.16 | — | $3,453.84 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1130.80 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 38 | $29.63 | $2.10 | — | $2,325.80 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1130.80 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 646 | $1.75 | $8.33 | — | $1,186.96 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1130.80 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $173.17 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1130.80 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $173.17 | ▲ close $9,233.36 vs 09:30 $9,046.54 (session +210.44) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.17 | ▲ 09:30 equity $9,474.85 vs yday $9,233.36 (+241.49) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $155.80 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $21.65 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $144.55 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $21.65 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 8 | $2.47 | $0.22 | — | $124.57 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $21.65 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 11 | $1.93 | $0.25 | — | $103.09 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $21.65 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 16 | $1.32 | $0.26 | — | $81.72 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $21.65 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.72 | ▼ close $9,471.78 vs 09:30 $9,474.85 (session -2.06) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.72 | ▲ 09:30 equity $9,569.45 vs yday $9,471.78 (+97.67) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.72 | ▼ close $9,538.39 vs 09:30 $9,569.45 (session -31.06) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.72 | ▼ 09:30 equity $9,389.01 vs yday $9,538.39 (-149.38) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 55 | $20.32 | $2.17 | $-16.98 | $1,197.14 | ▼ -16.98 after sell → book $9,386.83; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 12 | $95.86 | $2.05 | $+54.13 | $2,345.41 | ▲ +54.13 after sell → book $9,384.78; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 54 | $20.47 | $2.17 | $-14.04 | $3,448.62 | ▼ -14.04 after sell → book $9,382.61; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 195 | $5.53 | $2.62 | $-51.99 | $4,524.36 | ▼ -51.99 after sell → book $9,380.00; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 57 | $21.21 | $2.18 | $+85.72 | $5,731.14 | ▲ +85.72 after sell → book $9,377.81; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 38 | $32.32 | $2.12 | $+97.99 | $6,957.18 | ▲ +97.99 after sell → book $9,375.69; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 646 | $1.90 | $8.45 | $+80.12 | $8,176.13 | ▲ +80.12 after sell → book $9,367.24; vs 09:30 mark -8.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 7 | $156.51 | $2.03 | $+79.75 | $9,269.67 | ▲ +79.75 after sell → book $9,365.21; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,269.67 | ▲ close $9,369.62 vs 09:30 $9,389.01 (session +4.41) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,269.67 | ▼ 09:30 equity $9,368.83 vs yday $9,369.62 (-0.79) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $9,286.08 | ▼ -0.96 after sell → book $9,368.64; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $9,301.25 | ▲ +3.93 after sell → book $9,368.46; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 8 | $2.41 | $0.24 | $-0.94 | $9,320.30 | ▼ -0.94 after sell → book $9,368.23; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 11 | $2.03 | $0.28 | $+0.58 | $9,342.35 | ▲ +0.58 after sell → book $9,367.95; vs 09:30 mark -0.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 16 | $1.60 | $0.32 | $+3.90 | $9,367.63 | ▲ +3.90 after sell → book $9,367.63; vs 09:30 mark -0.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,367.63 | ▲ close $9,367.63 vs 09:30 $9,368.83 (session +0.00) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,367.63 | ▲ 09:30 equity $9,367.63 vs yday $9,367.63 (-0.00) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,367.63 | ▲ close $9,367.63 vs 09:30 $9,367.63 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,367.63 | ▲ 09:30 equity $9,367.63 vs yday $9,367.63 (-0.00) | — | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,367.63 | ▲ close $9,367.63 vs 09:30 $9,367.63 (session +0.00) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,367.63 | ▲ 09:30 equity $9,367.63 vs yday $9,367.63 (-0.00) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,367.63 | ▲ close $9,367.63 vs 09:30 $9,367.63 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,367.63 | ▲ 09:30 equity $9,367.63 vs yday $9,367.63 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,367.63 | ▲ close $9,367.63 vs 09:30 $9,367.63 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,367.63 | ▲ 09:30 equity $9,367.63 vs yday $9,367.63 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,367.63 | ▲ close $9,367.63 vs 09:30 $9,367.63 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,367.63 | ▲ 09:30 equity $9,367.63 vs yday $9,367.63 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 70 | $132.45 | $2.20 | — | $93.93 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; leftover $9367.63 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $93.93 | ▼ close $9,238.03 vs 09:30 $9,367.63 (session -127.40) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.93 | ▼ 09:30 equity $9,196.03 vs yday $9,238.03 (-42.00) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 3 | $3.46 | $0.11 | — | $83.43 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $13.42 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 5 | $2.52 | $0.14 | — | $70.69 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $13.42 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 1 | $6.71 | $0.07 | — | $63.91 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $13.42 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 7 | $1.90 | $0.15 | — | $50.46 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $13.42 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 2 | $4.78 | $0.10 | — | $40.80 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $13.42 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 8 | $1.59 | $0.15 | — | $27.93 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $13.42 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 1 | $11.31 | $0.12 | — | $16.50 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $13.42 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.50 | ▲ close $9,208.54 vs 09:30 $9,196.03 (session +13.36) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.50 | ▼ 09:30 equity $9,087.12 vs yday $9,208.54 (-121.42) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.50 | ▼ close $8,986.58 vs 09:30 $9,087.12 (session -100.54) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.50 | ▼ 09:30 equity $8,894.65 vs yday $8,986.58 (-91.93) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 70 | $125.77 | $2.28 | $-472.08 | $8,818.12 | ▼ -472.08 after sell → book $8,892.37; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,818.12 | ▼ close $8,888.43 vs 09:30 $8,894.65 (session -3.93) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,818.12 | ▼ 09:30 equity $8,887.34 vs yday $8,888.43 (-1.09) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `CABA` | 3 | $2.85 | $0.11 | $-2.06 | $8,826.55 | ▼ -2.06 after sell → book $8,887.23; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 5 | $2.22 | $0.15 | $-1.79 | $8,837.51 | ▼ -1.79 after sell → book $8,887.08; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 1 | $6.11 | $0.08 | $-0.75 | $8,843.53 | ▼ -0.75 after sell → book $8,887.00; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 7 | $1.83 | $0.17 | $-0.81 | $8,856.17 | ▼ -0.81 after sell → book $8,886.83; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 2 | $3.92 | $0.10 | $-1.92 | $8,863.91 | ▼ -1.92 after sell → book $8,886.72; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 8 | $1.53 | $0.17 | $-0.80 | $8,875.99 | ▼ -0.80 after sell → book $8,886.56; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 1 | $10.57 | $0.13 | $-0.98 | $8,886.43 | ▼ -0.98 after sell → book $8,886.43; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,886.43 | ▲ close $8,886.43 vs 09:30 $8,887.34 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,886.43 | ▲ 09:30 equity $8,886.43 vs yday $8,886.43 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 54 | $164.43 | $2.15 | — | $5.06 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,earn_react; wish-list (live io HOLD — not a ticket); ⚪; ret5=+4.9; leftover $8886.43 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.06 | ▼ close $8,120.18 vs 09:30 $8,886.43 (session -764.10) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.06 | ▼ 09:30 equity $7,641.74 vs yday $8,120.18 (-478.44) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.06 | ▲ close $7,823.72 vs 09:30 $7,641.74 (session +181.98) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.06 | ▼ 09:30 equity $7,751.90 vs yday $7,823.72 (-71.82) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.06 | ▼ close $7,583.96 vs 09:30 $7,751.90 (session -167.94) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.06 | ▼ 09:30 equity $7,566.68 vs yday $7,583.96 (-17.28) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 54 | $140.03 | $2.22 | $-1321.97 | $7,564.45 | ▼ -1,321.97 after sell → book $7,564.45; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 98 | $77.12 | $2.28 | — | $4.41 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+7.2; leftover $7564.45 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.41 | ▼ close $7,430.85 vs 09:30 $7,566.68 (session -131.32) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.41 | ▲ 09:30 equity $7,495.53 vs yday $7,430.85 (+64.68) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.41 | ▲ close $7,626.85 vs 09:30 $7,495.53 (session +131.32) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.41 | ▼ 09:30 equity $7,615.09 vs yday $7,626.85 (-11.76) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.41 | ▼ close $7,452.41 vs 09:30 $7,615.09 (session -162.68) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.41 | ▲ 09:30 equity $7,478.87 vs yday $7,452.41 (+26.46) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 98 | $76.27 | $2.36 | $-87.94 | $7,476.51 | ▼ -87.94 after sell → book $7,476.51; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 23 | $157.87 | $2.06 | — | $3,843.44 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; leftover $3738.25 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 42 | $88.83 | $2.12 | — | $110.46 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; leftover $3738.25 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.46 | ▲ close $7,580.22 vs 09:30 $7,478.87 (session +107.89) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $110.46 | ▲ 09:30 equity $7,580.22 vs yday $7,580.22 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.46 | ▲ close $7,580.22 vs 09:30 $7,580.22 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $110.46 | ▲ 09:30 equity $7,699.88 vs yday $7,580.22 (+119.66) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 2 | $20.65 | $0.42 | — | $68.75 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $55.23 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $68.75 | ▼ close $7,597.25 vs 09:30 $7,699.88 (session -102.22) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $68.75 | ▼ 09:30 equity $7,562.99 vs yday $7,597.25 (-34.26) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 23 | $163.95 | $2.10 | $+135.68 | $3,837.50 | ▲ +135.68 after sell → book $7,560.89; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 42 | $87.67 | $2.16 | $-52.78 | $7,517.69 | ▼ -52.78 after sell → book $7,558.73; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,517.69 | ▼ close $7,557.95 vs 09:30 $7,562.99 (session -0.78) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,099.38 | ▲ 09:30 equity $7,161.21 vs yday $7,159.77 (+1.44) | 09:30 open · cash $7,099.38 (unchanged overnight, no fees) · equity $7,161.21 vs prior close $7,159.77 (+1.44) · 1 name(s) re-marked at the open (per-name table). OMER×3 yday $20.13 → 09:30 $20.61 +1.44 | — |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 61 | $115.36 | $2.17 | — | $60.25 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.1; leftover $7099.38 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.25 | ▼ close $7,068.39 vs 09:30 $7,161.21 (session -90.65) | 16:00 close · cash $60.25 · equity $7,068.39 vs 09:30 $7,161.21 (-92.82; session marks -90.65) · 2 name(s) marked open→close (per-name table). OMER×3 09:30 $20.61 → close $20.08 -1.59; HALO×61 09:30 $115.36 → close $113.90 -89.06 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 21.65 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 21.65 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 21.65 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 1.47 < 1 share @ 233.85 |
| 2026-09-17 | `RVTY` | cash | leftover split 1.47 < 1 share @ 147.61 |
| 2026-09-17 | `PGEN` | cash | leftover split 1.47 < 1 share @ 7.59 |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `GNRC` | cash | leftover split 1.47 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 1.47 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 1.47 < 1 share @ 85.00 |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `HALO` | cash | leftover split 55.23 < 1 share @ 116.85 |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `OMER` | 2 | 2026-09-23 @ $20.65 | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $55.23 |
