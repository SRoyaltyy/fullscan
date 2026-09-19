# Factor mine action — `union_flow_in_white_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+3.72%** ($10,372) · signal-only (no cash/fees) was +6.27%. Starts YES **1/26**. Fills 41 · skips 0 · realized $+662.78.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: money came in (prior rel vol ≥ 1.5) but price barely moved (|1-day| ≤ 1.2%).
- Must-have: no morning camera is red (the 'white' / all-clear row).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
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

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `flow_in=True,zero_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $17.04.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TPG` | 197 | — | $50.62 | +0.00 | $54.62 | +787.37 | +787.37 | +0.00 | +787.37 |
| 2026-08-14 | `TPG` | 197 | $54.62 | $55.29 | +131.99 | — | +0.00 | +131.99 | +919.36 | — |
| 2026-08-14 | `SPHR` | 8 | — | $176.68 | +0.00 | $168.00 | -69.44 | -69.44 | +0.00 | -69.44 |
| 2026-08-14 | `KULR` | 623 | — | $2.50 | +0.00 | $2.64 | +87.22 | +87.22 | +0.00 | +87.22 |
| 2026-08-14 | `RLX` | 842 | — | $1.85 | +0.00 | $1.94 | +75.78 | +75.78 | +0.00 | +75.78 |
| 2026-08-14 | `BSBR` | 269 | — | $5.79 | +0.00 | $5.77 | -5.38 | -5.38 | +0.00 | -5.38 |
| 2026-08-14 | `ENB` | 30 | — | $51.15 | +0.00 | $50.91 | -7.20 | -7.20 | +0.00 | -7.20 |
| 2026-08-14 | `RUM` | 207 | — | $7.51 | +0.00 | $7.46 | -10.35 | -10.35 | +0.00 | -10.35 |
| 2026-08-14 | `JBTM` | 13 | — | $117.42 | +0.00 | $120.18 | +35.88 | +35.88 | +0.00 | +35.88 |
| 2026-08-17 | `SPHR` | 8 | $168.00 | $168.10 | +0.80 | — | +0.00 | +0.80 | -68.64 | — |
| 2026-08-17 | `KULR` | 623 | $2.64 | $2.63 | -6.23 | — | +0.00 | -6.23 | +80.99 | — |
| 2026-08-17 | `RLX` | 842 | $1.94 | $1.92 | -16.84 | — | +0.00 | -16.84 | +58.94 | — |
| 2026-08-17 | `BSBR` | 269 | $5.77 | $5.78 | +2.69 | — | +0.00 | +2.69 | -2.69 | — |
| 2026-08-17 | `ENB` | 30 | $50.91 | $50.80 | -3.30 | — | +0.00 | -3.30 | -10.50 | — |
| 2026-08-17 | `RUM` | 207 | $7.46 | $7.43 | -6.21 | — | +0.00 | -6.21 | -16.56 | — |
| 2026-08-17 | `JBTM` | 13 | $120.18 | $119.18 | -13.00 | — | +0.00 | -13.00 | +22.88 | — |
| 2026-08-17 | `VIV` | 472 | — | $11.55 | +0.00 | $11.40 | -70.80 | -70.80 | +0.00 | -70.80 |
| 2026-08-17 | `WBS` | 69 | — | $79.00 | +0.00 | $78.69 | -21.39 | -21.39 | +0.00 | -21.39 |
| 2026-08-18 | `VIV` | 472 | $11.40 | $11.45 | +23.60 | — | +0.00 | +23.60 | -47.20 | — |
| 2026-08-18 | `WBS` | 69 | $78.69 | $78.52 | -11.73 | — | +0.00 | -11.73 | -33.12 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `WBS` | 139 | — | $77.57 | +0.00 | $77.57 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-21 | `WBS` | 139 | $77.57 | $77.57 | +0.00 | — | +0.00 | +0.00 | +0.00 | — |
| 2026-08-21 | `HITI` | 4426 | — | $2.43 | +0.00 | $2.45 | +88.52 | +88.52 | +0.00 | +88.52 |
| 2026-08-24 | `HITI` | 4426 | $2.45 | $2.45 | +0.00 | — | +0.00 | +0.00 | +88.52 | — |
| 2026-08-25 | `RHI` | 82 | — | $43.76 | +0.00 | $44.90 | +93.48 | +93.48 | +0.00 | +93.48 |
| 2026-08-25 | `ABUS` | 684 | — | $5.25 | +0.00 | $5.20 | -34.20 | -34.20 | +0.00 | -34.20 |
| 2026-08-25 | `DBRG` | 224 | — | $15.98 | +0.00 | $15.97 | -2.24 | -2.24 | +0.00 | -2.24 |
| 2026-08-26 | `RHI` | 82 | $44.90 | $44.33 | -46.74 | — | +0.00 | -46.74 | +46.74 | — |
| 2026-08-26 | `ABUS` | 684 | $5.20 | $5.19 | -6.84 | — | +0.00 | -6.84 | -41.04 | — |
| 2026-08-26 | `DBRG` | 224 | $15.97 | $15.97 | +0.00 | — | +0.00 | +0.00 | -2.24 | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-31 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `VIR` | 931 | — | $11.54 | +0.00 | $11.45 | -83.79 | -83.79 | +0.00 | -83.79 |
| 2026-09-04 | `VIR` | 931 | $11.45 | $11.31 | -130.34 | — | +0.00 | -130.34 | -214.13 | — |
| 2026-09-04 | `ATRC` | 50 | — | $52.03 | +0.00 | $51.52 | -25.50 | -25.50 | +0.00 | -25.50 |
| 2026-09-04 | `ADCT` | 2023 | — | $1.30 | +0.00 | $1.36 | +121.38 | +121.38 | +0.00 | +121.38 |
| 2026-09-04 | `XP` | 133 | — | $19.67 | +0.00 | $19.86 | +25.27 | +25.27 | +0.00 | +25.27 |
| 2026-09-04 | `MMED` | 110 | — | $23.84 | +0.00 | $23.29 | -60.50 | -60.50 | +0.00 | -60.50 |
| 2026-09-08 | `ATRC` | 50 | $51.52 | $54.31 | +139.50 | — | +0.00 | +139.50 | +114.00 | — |
| 2026-09-08 | `ADCT` | 2023 | $1.36 | $1.33 | -60.69 | — | +0.00 | -60.69 | +60.69 | — |
| 2026-09-08 | `XP` | 133 | $19.86 | $20.46 | +79.80 | — | +0.00 | +79.80 | +105.07 | — |
| 2026-09-08 | `MMED` | 110 | $23.29 | $23.16 | -14.30 | — | +0.00 | -14.30 | -74.80 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-18 | `S` | 460 | — | $23.13 | +0.00 | $22.51 | -285.20 | -285.20 | +0.00 | -285.20 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +787.37 | TPG | — | $24.65 | $10,784.79 | TPG×197 |
| 2026-08-14 | +5.50 | $24.65 | TPG×197 | $10,916.78 | +131.99 | +106.51 | SPHR, KULR, RLX, BSBR, ENB, RUM, JBTM | TPG | $181.24 | $10,989.43 | SPHR×8, KULR×623, RLX×842, BSBR×269, ENB×30, RUM×207, JBTM×13 |
| 2026-08-17 | +2.25 | $181.24 | SPHR×8, KULR×623, RLX×842, BSBR×269, ENB×30, RUM×207, JBTM×13 | $10,947.34 | -42.09 | -92.19 | VIV, WBS | SPHR, KULR, RLX, BSBR, ENB, RUM, JBTM | $4.85 | $10,815.26 | VIV×472, WBS×69 |
| 2026-08-18 | -6.20 | $4.85 | VIV×472, WBS×69 | $10,827.13 | +11.87 | +0.00 | — | VIV, WBS | $10,818.67 | $10,818.67 | — |
| 2026-08-19 | -7.20 | $10,818.67 | — | $10,818.67 | -0.00 | +0.00 | — | — | $10,818.67 | $10,818.67 | — |
| 2026-08-20 | +1.12 | $10,818.67 | — | $10,818.67 | -0.00 | +0.00 | WBS | — | $34.03 | $10,816.26 | WBS×139 |
| 2026-08-21 | +3.25 | $34.03 | WBS×139 | $10,816.26 | +0.00 | +88.52 | HITI | WBS | $1.47 | $10,845.17 | HITI×4426 |
| 2026-08-24 | -5.17 | $1.47 | HITI×4426 | $10,845.17 | -0.00 | +0.00 | — | HITI | $10,787.25 | $10,787.25 | — |
| 2026-08-25 | +1.80 | $10,787.25 | — | $10,787.25 | +0.00 | +57.04 | RHI, ABUS, DBRG | — | $14.46 | $10,830.34 | RHI×82, ABUS×684, DBRG×224 |
| 2026-08-26 | +2.02 | $14.46 | RHI×82, ABUS×684, DBRG×224 | $10,776.76 | -53.58 | +0.00 | — | RHI, ABUS, DBRG | $10,762.56 | $10,762.56 | — |
| 2026-08-27 | — | $10,762.56 | — | $10,762.56 | +0.00 | +0.00 | — | — | $10,762.56 | $10,762.56 | — |
| 2026-08-28 | +0.75 | $10,762.56 | — | $10,762.56 | +0.00 | +0.00 | — | — | $10,762.56 | $10,762.56 | — |
| 2026-08-31 | -5.85 | $10,762.56 | — | $10,762.56 | +0.00 | +0.00 | — | — | $10,762.56 | $10,762.56 | — |
| 2026-09-01 | -6.30 | $10,762.56 | — | $10,762.56 | +0.00 | +0.00 | — | — | $10,762.56 | $10,762.56 | — |
| 2026-09-02 | -3.83 | $10,762.56 | — | $10,762.56 | +0.00 | +0.00 | — | — | $10,762.56 | $10,762.56 | — |
| 2026-09-03 | -0.90 | $10,762.56 | — | $10,762.56 | +0.00 | -83.79 | VIR | — | $6.81 | $10,666.76 | VIR×931 |
| 2026-09-04 | +2.25 | $6.81 | VIR×931 | $10,536.42 | -130.34 | +60.65 | ATRC, ADCT, XP, MMED | VIR | $21.32 | $10,551.88 | ATRC×50, ADCT×2023, XP×133, MMED×110 |
| 2026-09-08 | -11.47 | $21.32 | ATRC×50, ADCT×2023, XP×133, MMED×110 | $10,696.19 | +144.31 | +0.00 | — | ATRC, ADCT, XP, MMED | $10,662.77 | $10,662.77 | — |
| 2026-09-09 | -13.95 | $10,662.77 | — | $10,662.77 | +0.00 | +0.00 | — | — | $10,662.77 | $10,662.77 | — |
| 2026-09-10 | -13.28 | $10,662.77 | — | $10,662.77 | +0.00 | +0.00 | — | — | $10,662.77 | $10,662.77 | — |
| 2026-09-11 | +0.50 | $10,662.77 | — | $10,662.77 | +0.00 | +0.00 | — | — | $10,662.77 | $10,662.77 | — |
| 2026-09-14 | -11.00 | $10,662.77 | — | $10,662.77 | +0.00 | +0.00 | — | — | $10,662.77 | $10,662.77 | — |
| 2026-09-15 | -3.84 | $10,662.77 | — | $10,662.77 | +0.00 | +0.00 | — | — | $10,662.77 | $10,662.77 | — |
| 2026-09-16 | +5.30 | $10,662.77 | — | $10,662.77 | +0.00 | +0.00 | — | — | $10,662.77 | $10,662.77 | — |
| 2026-09-17 | +7.38 | $10,662.77 | — | $10,662.77 | +0.00 | +0.00 | — | — | $10,662.77 | $10,662.77 | — |
| 2026-09-18 | +4.86 | $10,662.77 | — | $10,662.77 | +0.00 | -285.20 | S | — | $17.04 | $10,371.64 | S×460 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 197 | $50.62 | $2.58 | — | $24.65 | — | combo gate; gate flow_in=True,zero_red=True; list flatten; ⚪; ret5=+6.2; leftover $10000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.65 | ▲ close $10,784.79 vs 09:30 $10,000.00 (session +787.37) | 16:00 close · cash $24.65 · equity $10,784.79 vs 09:30 $10,000.00 (+784.79; session marks +787.37) · 1 name(s) marked open→close (per-name table). TPG×197 09:30 $50.62 → close $54.62 +787.37 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.65 | ▲ 09:30 equity $10,916.78 vs yday $10,784.79 (+131.99) | 09:30 open · cash $24.65 (unchanged overnight, no fees) · equity $10,916.78 vs prior close $10,784.79 (+131.99) · 1 name(s) re-marked at the open (per-name table). TPG×197 yday $54.62 → 09:30 $55.29 +131.99 | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 197 | $55.29 | $2.70 | $+914.08 | $10,914.08 | ▲ +914.08 after sell → book $10,914.08; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SPHR` | 8 | $176.68 | $2.01 | — | $9,498.62 | — | combo gate; gate flow_in=True,zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+14.4; leftover $1559.15 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `KULR` | 623 | $2.50 | $8.04 | — | $7,933.09 | — | combo gate; gate flow_in=True,zero_red=True; list earn_react; 🔵; ⚪; ret5=+7.6; leftover $1559.15 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `RLX` | 842 | $1.85 | $10.86 | — | $6,364.53 | — | combo gate; gate flow_in=True,zero_red=True; list earn_react; 🔵; ⚪; ret5=+0.5; leftover $1559.15 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BSBR` | 269 | $5.79 | $3.47 | — | $4,803.55 | — | combo gate; gate flow_in=True,zero_red=True; list oppset; 🔵; ⚪; ret5=-0.3; leftover $1559.15 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ENB` | 30 | $51.15 | $2.08 | — | $3,266.97 | — | combo gate; gate flow_in=True,zero_red=True; list oppset; 🔵; ⚪; ret5=-0.8; leftover $1559.15 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `RUM` | 207 | $7.51 | $2.67 | — | $1,709.72 | — | combo gate; gate flow_in=True,zero_red=True; list oppset; 🔵; ⚪; ret5=+21.6; leftover $1559.15 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `JBTM` | 13 | $117.42 | $2.03 | — | $181.24 | — | combo gate; gate flow_in=True,zero_red=True; list oppset; 🔵; ⚪; ret5=-3.7; leftover $1559.15 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $181.24 | ▲ close $10,989.43 vs 09:30 $10,916.78 (session +106.51) | 16:00 close · cash $181.24 · equity $10,989.43 vs 09:30 $10,916.78 (+72.65; session marks +106.51) · 7 name(s) marked open→close (per-name table). SPHR×8 09:30 $176.68 → close $168.00 -69.44; KULR×623 09:30 $2.50 → close $2.64 +87.22; RLX×842 09:30 $1.85 → close $1.94 +75.78; BSBR×269 09:30 $5.79 → close $5.77 -5.38; ENB×30 09:30 $51.15 → close $50.91 -7.20; RUM×207 09:30 $7.51 → close $7.46 -10.35; JBTM×13 09:30 $117.42 → close $120.18 +35.88 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $181.24 | ▼ 09:30 equity $10,947.34 vs yday $10,989.43 (-42.09) | 09:30 open · cash $181.24 (unchanged overnight, no fees) · equity $10,947.34 vs prior close $10,989.43 (-42.09) · 7 name(s) re-marked at the open (per-name table). SPHR×8 yday $168.00 → 09:30 $168.10 +0.80; KULR×623 yday $2.64 → 09:30 $2.63 -6.23; RLX×842 yday $1.94 → 09:30 $1.92 -16.84; BSBR×269 yday $5.77 → 09:30 $5.78 +2.69; ENB×30 yday $50.91 → 09:30 $50.80 -3.30; RUM×207 yday $7.46 → 09:30 $7.43 -6.21; JBTM×13 yday $120.18 → 09:30 $119.18 -13.00 | — |
| 2026-08-17 09:30 ET | **SELL** | `SPHR` | 8 | $168.10 | $2.03 | $-72.69 | $1,524.00 | ▼ -72.69 after sell → book $10,945.30; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `KULR` | 623 | $2.63 | $8.15 | $+64.80 | $3,154.34 | ▲ +64.80 after sell → book $10,937.15; vs 09:30 mark -8.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `RLX` | 842 | $1.92 | $11.01 | $+37.06 | $4,759.96 | ▲ +37.06 after sell → book $10,926.13; vs 09:30 mark -11.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BSBR` | 269 | $5.78 | $3.53 | $-9.69 | $6,311.26 | ▼ -9.69 after sell → book $10,922.61; vs 09:30 mark -3.52 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `ENB` | 30 | $50.80 | $2.10 | $-14.68 | $7,833.15 | ▼ -14.68 after sell → book $10,920.50; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `RUM` | 207 | $7.43 | $2.72 | $-21.95 | $9,368.45 | ▼ -21.95 after sell → book $10,917.79; vs 09:30 mark -2.71 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `JBTM` | 13 | $119.18 | $2.05 | $+18.80 | $10,915.74 | ▲ +18.80 after sell → book $10,915.74; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `VIV` | 472 | $11.55 | $6.09 | — | $5,458.05 | — | combo gate; gate flow_in=True,zero_red=True; list oppset; ⚪; ret5=-5.0; leftover $5457.87 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `WBS` | 69 | $79.00 | $2.20 | — | $4.85 | — | combo gate; gate flow_in=True,zero_red=True; list oppset; ⚪; ret5=+0.5; leftover $5457.87 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.85 | ▼ close $10,815.26 vs 09:30 $10,947.34 (session -92.19) | 16:00 close · cash $4.85 · equity $10,815.26 vs 09:30 $10,947.34 (-132.08; session marks -92.19) · 2 name(s) marked open→close (per-name table). VIV×472 09:30 $11.55 → close $11.40 -70.80; WBS×69 09:30 $79.00 → close $78.69 -21.39 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.85 | ▲ 09:30 equity $10,827.13 vs yday $10,815.26 (+11.87) | 09:30 open · cash $4.85 (unchanged overnight, no fees) · equity $10,827.13 vs prior close $10,815.26 (+11.87) · 2 name(s) re-marked at the open (per-name table). VIV×472 yday $11.40 → 09:30 $11.45 +23.60; WBS×69 yday $78.69 → 09:30 $78.52 -11.73 | — |
| 2026-08-18 09:30 ET | **SELL** | `VIV` | 472 | $11.45 | $6.21 | $-59.50 | $5,403.04 | ▼ -59.50 after sell → book $10,820.92; vs 09:30 mark -6.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `WBS` | 69 | $78.52 | $2.25 | $-37.57 | $10,818.67 | ▼ -37.57 after sell → book $10,818.67; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,818.67 | ▲ close $10,818.67 vs 09:30 $10,827.13 (session +0.00) | 16:00 close · cash $10,818.67 · no lots left · equity $10,818.67. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,818.67 | ▲ 09:30 equity $10,818.67 vs yday $10,818.67 (-0.00) | 09:30 open · cash $10,818.67 · no holdings · equity $10,818.67 vs prior close $10,818.67 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,818.67 | ▲ close $10,818.67 vs 09:30 $10,818.67 (session +0.00) | 16:00 close · cash $10,818.67 · no lots left · equity $10,818.67. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,818.67 | ▲ 09:30 equity $10,818.67 vs yday $10,818.67 (-0.00) | 09:30 open · cash $10,818.67 · no holdings · equity $10,818.67 vs prior close $10,818.67 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `WBS` | 139 | $77.57 | $2.41 | — | $34.03 | — | combo gate; gate flow_in=True,zero_red=True; list oppset; 🔵; ⚪; ret5=-1.9; leftover $10818.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.03 | ▲ close $10,816.26 vs 09:30 $10,818.67 (session +0.00) | 16:00 close · cash $34.03 · equity $10,816.26 vs 09:30 $10,818.67 (-2.41; session marks +0.00) · 1 name(s) marked open→close (per-name table). WBS×139 09:30 $77.57 → close $77.57 +0.00 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.03 | ▲ 09:30 equity $10,816.26 vs yday $10,816.26 (+0.00) | 09:30 open · cash $34.03 (unchanged overnight, no fees) · equity $10,816.26 vs prior close $10,816.26 (+0.00) · 1 name(s) re-marked at the open (per-name table). WBS×139 yday $77.57 → 09:30 $77.57 +0.00 | — |
| 2026-08-21 09:30 ET | **SELL** | `WBS` | 139 | $77.57 | $2.52 | $-4.92 | $10,813.74 | ▼ -4.92 after sell → book $10,813.74; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 4426 | $2.43 | $57.10 | — | $1.47 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $10813.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.47 | ▲ close $10,845.17 vs 09:30 $10,816.26 (session +88.52) | 16:00 close · cash $1.47 · equity $10,845.17 vs 09:30 $10,816.26 (+28.91; session marks +88.52) · 1 name(s) marked open→close (per-name table). HITI×4426 09:30 $2.43 → close $2.45 +88.52 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.47 | ▲ 09:30 equity $10,845.17 vs yday $10,845.17 (-0.00) | 09:30 open · cash $1.47 (unchanged overnight, no fees) · equity $10,845.17 vs prior close $10,845.17 (-0.00) · 1 name(s) re-marked at the open (per-name table). HITI×4426 yday $2.45 → 09:30 $2.45 +0.00 | — |
| 2026-08-24 09:30 ET | **SELL** | `HITI` | 4426 | $2.45 | $57.92 | $-26.49 | $10,787.25 | ▼ -26.49 after sell → book $10,787.25; vs 09:30 mark -57.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,787.25 | ▲ close $10,787.25 vs 09:30 $10,845.17 (session +0.00) | 16:00 close · cash $10,787.25 · no lots left · equity $10,787.25. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,787.25 | ▲ 09:30 equity $10,787.25 vs yday $10,787.25 (+0.00) | 09:30 open · cash $10,787.25 · no holdings · equity $10,787.25 vs prior close $10,787.25 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 82 | $43.76 | $2.24 | — | $7,196.70 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $3595.75 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ABUS` | 684 | $5.25 | $8.82 | — | $3,596.87 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy,oppset; 🔵; ⚪; ret5=+10.4; leftover $3595.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DBRG` | 224 | $15.98 | $2.89 | — | $14.46 | — | combo gate; gate flow_in=True,zero_red=True; list oppset; 🔵; ⚪; ret5=+0.4; leftover $3595.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.46 | ▲ close $10,830.34 vs 09:30 $10,787.25 (session +57.04) | 16:00 close · cash $14.46 · equity $10,830.34 vs 09:30 $10,787.25 (+43.09; session marks +57.04) · 3 name(s) marked open→close (per-name table). RHI×82 09:30 $43.76 → close $44.90 +93.48; ABUS×684 09:30 $5.25 → close $5.20 -34.20; DBRG×224 09:30 $15.98 → close $15.97 -2.24 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.46 | ▼ 09:30 equity $10,776.76 vs yday $10,830.34 (-53.58) | 09:30 open · cash $14.46 (unchanged overnight, no fees) · equity $10,776.76 vs prior close $10,830.34 (-53.58) · 3 name(s) re-marked at the open (per-name table). RHI×82 yday $44.90 → 09:30 $44.33 -46.74; ABUS×684 yday $5.20 → 09:30 $5.19 -6.84; DBRG×224 yday $15.97 → 09:30 $15.97 +0.00 | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 82 | $44.33 | $2.28 | $+42.23 | $3,647.24 | ▲ +42.23 after sell → book $10,774.48; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ABUS` | 684 | $5.19 | $8.97 | $-58.83 | $7,188.24 | ▼ -58.83 after sell → book $10,765.52; vs 09:30 mark -8.96 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `DBRG` | 224 | $15.97 | $2.96 | $-8.08 | $10,762.56 | ▼ -8.08 after sell → book $10,762.56; vs 09:30 mark -2.96 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,762.56 | ▲ close $10,762.56 vs 09:30 $10,776.76 (session +0.00) | 16:00 close · cash $10,762.56 · no lots left · equity $10,762.56. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,762.56 | ▲ 09:30 equity $10,762.56 vs yday $10,762.56 (+0.00) | 09:30 open · cash $10,762.56 · no holdings · equity $10,762.56 vs prior close $10,762.56 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,762.56 | ▲ close $10,762.56 vs 09:30 $10,762.56 (session +0.00) | 16:00 close · cash $10,762.56 · no lots left · equity $10,762.56. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,762.56 | ▲ 09:30 equity $10,762.56 vs yday $10,762.56 (+0.00) | 09:30 open · cash $10,762.56 · no holdings · equity $10,762.56 vs prior close $10,762.56 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,762.56 | ▲ close $10,762.56 vs 09:30 $10,762.56 (session +0.00) | 16:00 close · cash $10,762.56 · no lots left · equity $10,762.56. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,762.56 | ▲ 09:30 equity $10,762.56 vs yday $10,762.56 (+0.00) | 09:30 open · cash $10,762.56 · no holdings · equity $10,762.56 vs prior close $10,762.56 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,762.56 | ▲ close $10,762.56 vs 09:30 $10,762.56 (session +0.00) | 16:00 close · cash $10,762.56 · no lots left · equity $10,762.56. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,762.56 | ▲ 09:30 equity $10,762.56 vs yday $10,762.56 (+0.00) | 09:30 open · cash $10,762.56 · no holdings · equity $10,762.56 vs prior close $10,762.56 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,762.56 | ▲ close $10,762.56 vs 09:30 $10,762.56 (session +0.00) | 16:00 close · cash $10,762.56 · no lots left · equity $10,762.56. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,762.56 | ▲ 09:30 equity $10,762.56 vs yday $10,762.56 (+0.00) | 09:30 open · cash $10,762.56 · no holdings · equity $10,762.56 vs prior close $10,762.56 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,762.56 | ▲ close $10,762.56 vs 09:30 $10,762.56 (session +0.00) | 16:00 close · cash $10,762.56 · no lots left · equity $10,762.56. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,762.56 | ▲ 09:30 equity $10,762.56 vs yday $10,762.56 (+0.00) | 09:30 open · cash $10,762.56 · no holdings · equity $10,762.56 vs prior close $10,762.56 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 931 | $11.54 | $12.01 | — | $6.81 | — | combo gate; gate flow_in=True,zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $10762.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.81 | ▼ close $10,666.76 vs 09:30 $10,762.56 (session -83.79) | 16:00 close · cash $6.81 · equity $10,666.76 vs 09:30 $10,762.56 (-95.80; session marks -83.79) · 1 name(s) marked open→close (per-name table). VIR×931 09:30 $11.54 → close $11.45 -83.79 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.81 | ▼ 09:30 equity $10,536.42 vs yday $10,666.76 (-130.34) | 09:30 open · cash $6.81 (unchanged overnight, no fees) · equity $10,536.42 vs prior close $10,666.76 (-130.34) · 1 name(s) re-marked at the open (per-name table). VIR×931 yday $11.45 → 09:30 $11.31 -130.34 | — |
| 2026-09-04 09:30 ET | **SELL** | `VIR` | 931 | $11.31 | $12.25 | $-238.39 | $10,524.17 | ▼ -238.39 after sell → book $10,524.17; vs 09:30 mark -12.25 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 50 | $52.03 | $2.14 | — | $7,920.53 | — | combo gate; gate flow_in=True,zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $2631.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ADCT` | 2023 | $1.30 | $26.10 | — | $5,264.54 | — | combo gate; gate flow_in=True,zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+17.9; leftover $2631.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `XP` | 133 | $19.67 | $2.39 | — | $2,646.04 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+13.1; leftover $2631.04 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MMED` | 110 | $23.84 | $2.32 | — | $21.32 | — | combo gate; gate flow_in=True,zero_red=True; list oppset; 🔵; ⚪; ret5=+22.2; leftover $2631.04 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.32 | ▲ close $10,551.88 vs 09:30 $10,536.42 (session +60.65) | 16:00 close · cash $21.32 · equity $10,551.88 vs 09:30 $10,536.42 (+15.46; session marks +60.65) · 4 name(s) marked open→close (per-name table). ATRC×50 09:30 $52.03 → close $51.52 -25.50; ADCT×2023 09:30 $1.30 → close $1.36 +121.38; XP×133 09:30 $19.67 → close $19.86 +25.27; MMED×110 09:30 $23.84 → close $23.29 -60.50 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.32 | ▲ 09:30 equity $10,696.19 vs yday $10,551.88 (+144.31) | 09:30 open · cash $21.32 (unchanged overnight, no fees) · equity $10,696.19 vs prior close $10,551.88 (+144.31) · 4 name(s) re-marked at the open (per-name table). ATRC×50 yday $51.52 → 09:30 $54.31 +139.50; ADCT×2023 yday $1.36 → 09:30 $1.33 -60.69; XP×133 yday $19.86 → 09:30 $20.46 +79.80; MMED×110 yday $23.29 → 09:30 $23.16 -14.30 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 50 | $54.31 | $2.17 | $+109.69 | $2,734.65 | ▲ +109.69 after sell → book $10,694.02; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ADCT` | 2023 | $1.33 | $26.45 | $+8.14 | $5,398.78 | ▲ +8.14 after sell → book $10,667.56; vs 09:30 mark -26.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `XP` | 133 | $20.46 | $2.43 | $+100.25 | $8,117.53 | ▲ +100.25 after sell → book $10,665.13; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MMED` | 110 | $23.16 | $2.36 | $-79.48 | $10,662.77 | ▼ -79.48 after sell → book $10,662.77; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,662.77 | ▲ close $10,662.77 vs 09:30 $10,696.19 (session +0.00) | 16:00 close · cash $10,662.77 · no lots left · equity $10,662.77. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,662.77 | ▲ 09:30 equity $10,662.77 vs yday $10,662.77 (+0.00) | 09:30 open · cash $10,662.77 · no holdings · equity $10,662.77 vs prior close $10,662.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,662.77 | ▲ close $10,662.77 vs 09:30 $10,662.77 (session +0.00) | 16:00 close · cash $10,662.77 · no lots left · equity $10,662.77. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,662.77 | ▲ 09:30 equity $10,662.77 vs yday $10,662.77 (+0.00) | 09:30 open · cash $10,662.77 · no holdings · equity $10,662.77 vs prior close $10,662.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,662.77 | ▲ close $10,662.77 vs 09:30 $10,662.77 (session +0.00) | 16:00 close · cash $10,662.77 · no lots left · equity $10,662.77. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,662.77 | ▲ 09:30 equity $10,662.77 vs yday $10,662.77 (+0.00) | 09:30 open · cash $10,662.77 · no holdings · equity $10,662.77 vs prior close $10,662.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,662.77 | ▲ close $10,662.77 vs 09:30 $10,662.77 (session +0.00) | 16:00 close · cash $10,662.77 · no lots left · equity $10,662.77. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,662.77 | ▲ 09:30 equity $10,662.77 vs yday $10,662.77 (+0.00) | 09:30 open · cash $10,662.77 · no holdings · equity $10,662.77 vs prior close $10,662.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,662.77 | ▲ close $10,662.77 vs 09:30 $10,662.77 (session +0.00) | 16:00 close · cash $10,662.77 · no lots left · equity $10,662.77. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,662.77 | ▲ 09:30 equity $10,662.77 vs yday $10,662.77 (+0.00) | 09:30 open · cash $10,662.77 · no holdings · equity $10,662.77 vs prior close $10,662.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,662.77 | ▲ close $10,662.77 vs 09:30 $10,662.77 (session +0.00) | 16:00 close · cash $10,662.77 · no lots left · equity $10,662.77. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,662.77 | ▲ 09:30 equity $10,662.77 vs yday $10,662.77 (+0.00) | 09:30 open · cash $10,662.77 · no holdings · equity $10,662.77 vs prior close $10,662.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,662.77 | ▲ close $10,662.77 vs 09:30 $10,662.77 (session +0.00) | 16:00 close · cash $10,662.77 · no lots left · equity $10,662.77. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,662.77 | ▲ 09:30 equity $10,662.77 vs yday $10,662.77 (+0.00) | 09:30 open · cash $10,662.77 · no holdings · equity $10,662.77 vs prior close $10,662.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,662.77 | ▲ close $10,662.77 vs 09:30 $10,662.77 (session +0.00) | 16:00 close · cash $10,662.77 · no lots left · equity $10,662.77. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,662.77 | ▲ 09:30 equity $10,662.77 vs yday $10,662.77 (+0.00) | 09:30 open · cash $10,662.77 · no holdings · equity $10,662.77 vs prior close $10,662.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 09:30 ET | **BUY** | `S` | 460 | $23.13 | $5.93 | — | $17.04 | — | combo gate; gate flow_in=True,zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $10662.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.04 | ▼ close $10,371.64 vs 09:30 $10,662.77 (session -285.20) | 16:00 close · cash $17.04 · equity $10,371.64 vs 09:30 $10,662.77 (-291.13; session marks -285.20) · 1 name(s) marked open→close (per-name table). S×460 09:30 $23.13 → close $22.51 -285.20 | — |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `S` | 460 | 2026-09-18 @ $23.13 | combo gate; gate flow_in=True,zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $10662.77 |
