# Factor mine action — `union_flow_in_white_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+5.23%** ($10,523) · signal-only (no cash/fees) was +8.08%. Starts YES **2/27**. Fills 25 · skips 0 · realized $+806.69.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $3.06.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TPG` | 197 | — | $50.62 | +0.00 | $54.62 | +787.37 | +787.37 | +0.00 | +787.37 |
| 2026-08-14 | `TPG` | 197 | $54.62 | $55.29 | +131.99 | — | +0.00 | +131.99 | +919.36 | — |
| 2026-08-14 | `SPHR` | 20 | — | $176.68 | +0.00 | $168.00 | -173.60 | -173.60 | +0.00 | -173.60 |
| 2026-08-14 | `KULR` | 1455 | — | $2.50 | +0.00 | $2.64 | +203.70 | +203.70 | +0.00 | +203.70 |
| 2026-08-14 | `RLX` | 1966 | — | $1.85 | +0.00 | $1.94 | +176.94 | +176.94 | +0.00 | +176.94 |
| 2026-08-17 | `SPHR` | 20 | $168.00 | $168.10 | +2.00 | — | +0.00 | +2.00 | -171.60 | — |
| 2026-08-17 | `KULR` | 1455 | $2.64 | $2.63 | -14.55 | — | +0.00 | -14.55 | +189.15 | — |
| 2026-08-17 | `RLX` | 1966 | $1.94 | $1.92 | -39.32 | — | +0.00 | -39.32 | +137.62 | — |
| 2026-08-17 | `XP` | 688 | — | $15.93 | +0.00 | $15.70 | -158.24 | -158.24 | +0.00 | -158.24 |
| 2026-08-18 | `XP` | 688 | $15.70 | $15.70 | +0.00 | — | +0.00 | +0.00 | -158.24 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-21 | `HITI` | 4420 | — | $2.43 | +0.00 | $2.45 | +88.40 | +88.40 | +0.00 | +88.40 |
| 2026-08-24 | `HITI` | 4420 | $2.45 | $2.45 | +0.00 | — | +0.00 | +0.00 | +88.40 | — |
| 2026-08-25 | `RHI` | 123 | — | $43.76 | +0.00 | $44.90 | +140.22 | +140.22 | +0.00 | +140.22 |
| 2026-08-25 | `ABUS` | 1023 | — | $5.25 | +0.00 | $5.20 | -51.15 | -51.15 | +0.00 | -51.15 |
| 2026-08-26 | `RHI` | 123 | $44.90 | $44.33 | -70.11 | — | +0.00 | -70.11 | +70.11 | — |
| 2026-08-26 | `ABUS` | 1023 | $5.20 | $5.19 | -10.23 | — | +0.00 | -10.23 | -61.38 | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-31 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `VIR` | 930 | — | $11.54 | +0.00 | $11.45 | -83.70 | -83.70 | +0.00 | -83.70 |
| 2026-09-04 | `VIR` | 930 | $11.45 | $11.31 | -130.20 | — | +0.00 | -130.20 | -213.90 | — |
| 2026-09-04 | `ATRC` | 67 | — | $52.03 | +0.00 | $51.52 | -34.17 | -34.17 | +0.00 | -34.17 |
| 2026-09-04 | `ADCT` | 2695 | — | $1.30 | +0.00 | $1.36 | +161.70 | +161.70 | +0.00 | +161.70 |
| 2026-09-04 | `XP` | 177 | — | $19.67 | +0.00 | $19.86 | +33.63 | +33.63 | +0.00 | +33.63 |
| 2026-09-08 | `ATRC` | 67 | $51.52 | $54.31 | +186.93 | — | +0.00 | +186.93 | +152.76 | — |
| 2026-09-08 | `ADCT` | 2695 | $1.36 | $1.33 | -80.85 | — | +0.00 | -80.85 | +80.85 | — |
| 2026-09-08 | `XP` | 177 | $19.86 | $20.46 | +106.20 | — | +0.00 | +106.20 | +139.83 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-21 | `NEO` | 542 | — | $19.92 | +0.00 | $19.41 | -276.42 | -276.42 | +0.00 | -276.42 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +787.37 | TPG | — | $24.65 | $10,784.79 | TPG×197 |
| 2026-08-14 | +5.50 | $24.65 | TPG×197 | $10,916.78 | +131.99 | +207.04 | SPHR, KULR, RLX | TPG | $59.70 | $11,074.94 | SPHR×20, KULR×1455, RLX×1966 |
| 2026-08-17 | +2.25 | $59.70 | SPHR×20, KULR×1455, RLX×1966 | $11,023.07 | -51.87 | -158.24 | XP | SPHR, KULR, RLX | $7.51 | $10,809.11 | XP×688 |
| 2026-08-18 | -6.20 | $7.51 | XP×688 | $10,809.11 | -0.00 | +0.00 | — | XP | $10,800.03 | $10,800.03 | — |
| 2026-08-19 | -7.20 | $10,800.03 | — | $10,800.03 | -0.00 | +0.00 | — | — | $10,800.03 | $10,800.03 | — |
| 2026-08-20 | +1.12 | $10,800.03 | — | $10,800.03 | -0.00 | +0.00 | — | — | $10,800.03 | $10,800.03 | — |
| 2026-08-21 | +3.25 | $10,800.03 | — | $10,800.03 | -0.00 | +88.40 | HITI | — | $2.41 | $10,831.41 | HITI×4420 |
| 2026-08-24 | -5.17 | $2.41 | HITI×4420 | $10,831.41 | +0.00 | +0.00 | — | HITI | $10,773.57 | $10,773.57 | — |
| 2026-08-25 | +1.80 | $10,773.57 | — | $10,773.57 | +0.00 | +89.07 | RHI, ABUS | — | $4.79 | $10,847.09 | RHI×123, ABUS×1023 |
| 2026-08-26 | +2.02 | $4.79 | RHI×123, ABUS×1023 | $10,766.75 | -80.34 | +0.00 | — | RHI, ABUS | $10,750.92 | $10,750.92 | — |
| 2026-08-27 | — | $10,750.92 | — | $10,750.92 | -0.00 | +0.00 | — | — | $10,750.92 | $10,750.92 | — |
| 2026-08-28 | +0.75 | $10,750.92 | — | $10,750.92 | -0.00 | +0.00 | — | — | $10,750.92 | $10,750.92 | — |
| 2026-08-31 | -5.85 | $10,750.92 | — | $10,750.92 | -0.00 | +0.00 | — | — | $10,750.92 | $10,750.92 | — |
| 2026-09-01 | -6.30 | $10,750.92 | — | $10,750.92 | -0.00 | +0.00 | — | — | $10,750.92 | $10,750.92 | — |
| 2026-09-02 | -3.83 | $10,750.92 | — | $10,750.92 | -0.00 | +0.00 | — | — | $10,750.92 | $10,750.92 | — |
| 2026-09-03 | -0.90 | $10,750.92 | — | $10,750.92 | -0.00 | -83.70 | VIR | — | $6.72 | $10,655.22 | VIR×930 |
| 2026-09-04 | +2.25 | $6.72 | VIR×930 | $10,525.02 | -130.20 | +161.16 | ATRC, ADCT, XP | VIR | $2.21 | $10,634.47 | ATRC×67, ADCT×2695, XP×177 |
| 2026-09-08 | -11.47 | $2.21 | ATRC×67, ADCT×2695, XP×177 | $10,846.75 | +212.28 | +0.00 | — | ATRC, ADCT, XP | $10,806.69 | $10,806.69 | — |
| 2026-09-09 | -13.95 | $10,806.69 | — | $10,806.69 | +0.00 | +0.00 | — | — | $10,806.69 | $10,806.69 | — |
| 2026-09-10 | -13.28 | $10,806.69 | — | $10,806.69 | +0.00 | +0.00 | — | — | $10,806.69 | $10,806.69 | — |
| 2026-09-11 | +0.50 | $10,806.69 | — | $10,806.69 | +0.00 | +0.00 | — | — | $10,806.69 | $10,806.69 | — |
| 2026-09-14 | -11.00 | $10,806.69 | — | $10,806.69 | +0.00 | +0.00 | — | — | $10,806.69 | $10,806.69 | — |
| 2026-09-15 | -3.84 | $10,806.69 | — | $10,806.69 | +0.00 | +0.00 | — | — | $10,806.69 | $10,806.69 | — |
| 2026-09-16 | +5.30 | $10,806.69 | — | $10,806.69 | +0.00 | +0.00 | — | — | $10,806.69 | $10,806.69 | — |
| 2026-09-17 | +7.38 | $10,806.69 | — | $10,806.69 | +0.00 | +0.00 | — | — | $10,806.69 | $10,806.69 | — |
| 2026-09-18 | +4.86 | $10,806.69 | — | $10,806.69 | +0.00 | +0.00 | — | — | $10,806.69 | $10,806.69 | — |
| 2026-09-21 | +12.87 | $10,806.69 | — | $10,806.69 | +0.00 | -276.42 | NEO | — | $3.06 | $10,523.28 | NEO×542 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 197 | $50.62 | $2.58 | — | $24.65 | — | combo gate; gate flow_in=True,zero_red=True; list flatten; ⚪; ret5=+6.2; leftover $10000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.65 | ▲ close $10,784.79 vs 09:30 $10,000.00 (session +787.37) | 16:00 close · cash $24.65 · equity $10,784.79 vs 09:30 $10,000.00 (+784.79; session marks +787.37) · 1 name(s) marked open→close (per-name table). TPG×197 09:30 $50.62 → close $54.62 +787.37 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.65 | ▲ 09:30 equity $10,916.78 vs yday $10,784.79 (+131.99) | 09:30 open · cash $24.65 (unchanged overnight, no fees) · equity $10,916.78 vs prior close $10,784.79 (+131.99) · 1 name(s) re-marked at the open (per-name table). TPG×197 yday $54.62 → 09:30 $55.29 +131.99 | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 197 | $55.29 | $2.70 | $+914.08 | $10,914.08 | ▲ +914.08 after sell → book $10,914.08; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SPHR` | 20 | $176.68 | $2.05 | — | $7,378.43 | — | combo gate; gate flow_in=True,zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+14.4; leftover $3638.03 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `KULR` | 1455 | $2.50 | $18.77 | — | $3,722.16 | — | combo gate; gate flow_in=True,zero_red=True; list earn_react; 🔵; ⚪; ret5=+7.6; leftover $3638.03 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `RLX` | 1966 | $1.85 | $25.36 | — | $59.70 | — | combo gate; gate flow_in=True,zero_red=True; list earn_react; 🔵; ⚪; ret5=+0.5; leftover $3638.03 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.70 | ▲ close $11,074.94 vs 09:30 $10,916.78 (session +207.04) | 16:00 close · cash $59.70 · equity $11,074.94 vs 09:30 $10,916.78 (+158.16; session marks +207.04) · 3 name(s) marked open→close (per-name table). SPHR×20 09:30 $176.68 → close $168.00 -173.60; KULR×1455 09:30 $2.50 → close $2.64 +203.70; RLX×1966 09:30 $1.85 → close $1.94 +176.94 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.70 | ▼ 09:30 equity $11,023.07 vs yday $11,074.94 (-51.87) | 09:30 open · cash $59.70 (unchanged overnight, no fees) · equity $11,023.07 vs prior close $11,074.94 (-51.87) · 3 name(s) re-marked at the open (per-name table). SPHR×20 yday $168.00 → 09:30 $168.10 +2.00; KULR×1455 yday $2.64 → 09:30 $2.63 -14.55; RLX×1966 yday $1.94 → 09:30 $1.92 -39.32 | — |
| 2026-08-17 09:30 ET | **SELL** | `SPHR` | 20 | $168.10 | $2.09 | $-175.74 | $3,419.61 | ▼ -175.74 after sell → book $11,020.98; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `KULR` | 1455 | $2.63 | $19.04 | $+151.34 | $7,227.22 | ▲ +151.34 after sell → book $11,001.94; vs 09:30 mark -19.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `RLX` | 1966 | $1.92 | $25.72 | $+86.54 | $10,976.22 | ▲ +86.54 after sell → book $10,976.22; vs 09:30 mark -25.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XP` | 688 | $15.93 | $8.88 | — | $7.51 | — | combo gate; gate flow_in=True,zero_red=True; list overnight; ⚪; ret5=-2.6; leftover $10976.22 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.51 | ▼ close $10,809.11 vs 09:30 $11,023.07 (session -158.24) | 16:00 close · cash $7.51 · equity $10,809.11 vs 09:30 $11,023.07 (-213.96; session marks -158.24) · 1 name(s) marked open→close (per-name table). XP×688 09:30 $15.93 → close $15.70 -158.24 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.51 | ▲ 09:30 equity $10,809.11 vs yday $10,809.11 (-0.00) | 09:30 open · cash $7.51 (unchanged overnight, no fees) · equity $10,809.11 vs prior close $10,809.11 (-0.00) · 1 name(s) re-marked at the open (per-name table). XP×688 yday $15.70 → 09:30 $15.70 +0.00 | — |
| 2026-08-18 09:30 ET | **SELL** | `XP` | 688 | $15.70 | $9.08 | $-176.19 | $10,800.03 | ▼ -176.19 after sell → book $10,800.03; vs 09:30 mark -9.08 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,800.03 | ▲ close $10,800.03 vs 09:30 $10,809.11 (session +0.00) | 16:00 close · cash $10,800.03 · no lots left · equity $10,800.03. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,800.03 | ▲ 09:30 equity $10,800.03 vs yday $10,800.03 (-0.00) | 09:30 open · cash $10,800.03 · no holdings · equity $10,800.03 vs prior close $10,800.03 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,800.03 | ▲ close $10,800.03 vs 09:30 $10,800.03 (session +0.00) | 16:00 close · cash $10,800.03 · no lots left · equity $10,800.03. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,800.03 | ▲ 09:30 equity $10,800.03 vs yday $10,800.03 (-0.00) | 09:30 open · cash $10,800.03 · no holdings · equity $10,800.03 vs prior close $10,800.03 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,800.03 | ▲ close $10,800.03 vs 09:30 $10,800.03 (session +0.00) | 16:00 close · cash $10,800.03 · no lots left · equity $10,800.03. | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,800.03 | ▲ 09:30 equity $10,800.03 vs yday $10,800.03 (-0.00) | 09:30 open · cash $10,800.03 · no holdings · equity $10,800.03 vs prior close $10,800.03 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 4420 | $2.43 | $57.02 | — | $2.41 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $10800.03 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.41 | ▲ close $10,831.41 vs 09:30 $10,800.03 (session +88.40) | 16:00 close · cash $2.41 · equity $10,831.41 vs 09:30 $10,800.03 (+31.38; session marks +88.40) · 1 name(s) marked open→close (per-name table). HITI×4420 09:30 $2.43 → close $2.45 +88.40 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.41 | ▲ 09:30 equity $10,831.41 vs yday $10,831.41 (+0.00) | 09:30 open · cash $2.41 (unchanged overnight, no fees) · equity $10,831.41 vs prior close $10,831.41 (+0.00) · 1 name(s) re-marked at the open (per-name table). HITI×4420 yday $2.45 → 09:30 $2.45 +0.00 | — |
| 2026-08-24 09:30 ET | **SELL** | `HITI` | 4420 | $2.45 | $57.84 | $-26.46 | $10,773.57 | ▼ -26.46 after sell → book $10,773.57; vs 09:30 mark -57.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,773.57 | ▲ close $10,773.57 vs 09:30 $10,831.41 (session +0.00) | 16:00 close · cash $10,773.57 · no lots left · equity $10,773.57. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,773.57 | ▲ 09:30 equity $10,773.57 vs yday $10,773.57 (+0.00) | 09:30 open · cash $10,773.57 · no holdings · equity $10,773.57 vs prior close $10,773.57 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 123 | $43.76 | $2.36 | — | $5,388.73 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $5386.79 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ABUS` | 1023 | $5.25 | $13.20 | — | $4.79 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+10.4; leftover $5386.79 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.79 | ▲ close $10,847.09 vs 09:30 $10,773.57 (session +89.07) | 16:00 close · cash $4.79 · equity $10,847.09 vs 09:30 $10,773.57 (+73.52; session marks +89.07) · 2 name(s) marked open→close (per-name table). RHI×123 09:30 $43.76 → close $44.90 +140.22; ABUS×1023 09:30 $5.25 → close $5.20 -51.15 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.79 | ▼ 09:30 equity $10,766.75 vs yday $10,847.09 (-80.34) | 09:30 open · cash $4.79 (unchanged overnight, no fees) · equity $10,766.75 vs prior close $10,847.09 (-80.34) · 2 name(s) re-marked at the open (per-name table). RHI×123 yday $44.90 → 09:30 $44.33 -70.11; ABUS×1023 yday $5.20 → 09:30 $5.19 -10.23 | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 123 | $44.33 | $2.42 | $+65.33 | $5,454.95 | ▲ +65.33 after sell → book $10,764.32; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ABUS` | 1023 | $5.19 | $13.41 | $-87.99 | $10,750.92 | ▼ -87.99 after sell → book $10,750.92; vs 09:30 mark -13.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,750.92 | ▲ close $10,750.92 vs 09:30 $10,766.75 (session +0.00) | 16:00 close · cash $10,750.92 · no lots left · equity $10,750.92. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,750.92 | ▲ 09:30 equity $10,750.92 vs yday $10,750.92 (-0.00) | 09:30 open · cash $10,750.92 · no holdings · equity $10,750.92 vs prior close $10,750.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,750.92 | ▲ close $10,750.92 vs 09:30 $10,750.92 (session +0.00) | 16:00 close · cash $10,750.92 · no lots left · equity $10,750.92. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,750.92 | ▲ 09:30 equity $10,750.92 vs yday $10,750.92 (-0.00) | 09:30 open · cash $10,750.92 · no holdings · equity $10,750.92 vs prior close $10,750.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,750.92 | ▲ close $10,750.92 vs 09:30 $10,750.92 (session +0.00) | 16:00 close · cash $10,750.92 · no lots left · equity $10,750.92. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,750.92 | ▲ 09:30 equity $10,750.92 vs yday $10,750.92 (-0.00) | 09:30 open · cash $10,750.92 · no holdings · equity $10,750.92 vs prior close $10,750.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,750.92 | ▲ close $10,750.92 vs 09:30 $10,750.92 (session +0.00) | 16:00 close · cash $10,750.92 · no lots left · equity $10,750.92. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,750.92 | ▲ 09:30 equity $10,750.92 vs yday $10,750.92 (-0.00) | 09:30 open · cash $10,750.92 · no holdings · equity $10,750.92 vs prior close $10,750.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,750.92 | ▲ close $10,750.92 vs 09:30 $10,750.92 (session +0.00) | 16:00 close · cash $10,750.92 · no lots left · equity $10,750.92. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,750.92 | ▲ 09:30 equity $10,750.92 vs yday $10,750.92 (-0.00) | 09:30 open · cash $10,750.92 · no holdings · equity $10,750.92 vs prior close $10,750.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,750.92 | ▲ close $10,750.92 vs 09:30 $10,750.92 (session +0.00) | 16:00 close · cash $10,750.92 · no lots left · equity $10,750.92. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,750.92 | ▲ 09:30 equity $10,750.92 vs yday $10,750.92 (-0.00) | 09:30 open · cash $10,750.92 · no holdings · equity $10,750.92 vs prior close $10,750.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 930 | $11.54 | $12.00 | — | $6.72 | — | combo gate; gate flow_in=True,zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $10750.92 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.72 | ▼ close $10,655.22 vs 09:30 $10,750.92 (session -83.70) | 16:00 close · cash $6.72 · equity $10,655.22 vs 09:30 $10,750.92 (-95.70; session marks -83.70) · 1 name(s) marked open→close (per-name table). VIR×930 09:30 $11.54 → close $11.45 -83.70 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.72 | ▼ 09:30 equity $10,525.02 vs yday $10,655.22 (-130.20) | 09:30 open · cash $6.72 (unchanged overnight, no fees) · equity $10,525.02 vs prior close $10,655.22 (-130.20) · 1 name(s) re-marked at the open (per-name table). VIR×930 yday $11.45 → 09:30 $11.31 -130.20 | — |
| 2026-09-04 09:30 ET | **SELL** | `VIR` | 930 | $11.31 | $12.24 | $-238.13 | $10,512.78 | ▼ -238.13 after sell → book $10,512.78; vs 09:30 mark -12.24 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 67 | $52.03 | $2.19 | — | $7,024.58 | — | combo gate; gate flow_in=True,zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $3504.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ADCT` | 2695 | $1.30 | $34.77 | — | $3,486.32 | — | combo gate; gate flow_in=True,zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+17.9; leftover $3504.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `XP` | 177 | $19.67 | $2.52 | — | $2.21 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+13.1; leftover $3504.26 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.21 | ▲ close $10,634.47 vs 09:30 $10,525.02 (session +161.16) | 16:00 close · cash $2.21 · equity $10,634.47 vs 09:30 $10,525.02 (+109.45; session marks +161.16) · 3 name(s) marked open→close (per-name table). ATRC×67 09:30 $52.03 → close $51.52 -34.17; ADCT×2695 09:30 $1.30 → close $1.36 +161.70; XP×177 09:30 $19.67 → close $19.86 +33.63 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.21 | ▲ 09:30 equity $10,846.75 vs yday $10,634.47 (+212.28) | 09:30 open · cash $2.21 (unchanged overnight, no fees) · equity $10,846.75 vs prior close $10,634.47 (+212.28) · 3 name(s) re-marked at the open (per-name table). ATRC×67 yday $51.52 → 09:30 $54.31 +186.93; ADCT×2695 yday $1.36 → 09:30 $1.33 -80.85; XP×177 yday $19.86 → 09:30 $20.46 +106.20 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 67 | $54.31 | $2.23 | $+148.34 | $3,638.74 | ▲ +148.34 after sell → book $10,844.51; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ADCT` | 2695 | $1.33 | $35.24 | $+10.84 | $7,187.85 | ▲ +10.84 after sell → book $10,809.27; vs 09:30 mark -35.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `XP` | 177 | $20.46 | $2.58 | $+134.73 | $10,806.69 | ▲ +134.73 after sell → book $10,806.69; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,846.75 (session +0.00) | 16:00 close · cash $10,806.69 · no lots left · equity $10,806.69. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | 09:30 open · cash $10,806.69 · no holdings · equity $10,806.69 vs prior close $10,806.69 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,806.69 (session +0.00) | 16:00 close · cash $10,806.69 · no lots left · equity $10,806.69. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | 09:30 open · cash $10,806.69 · no holdings · equity $10,806.69 vs prior close $10,806.69 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,806.69 (session +0.00) | 16:00 close · cash $10,806.69 · no lots left · equity $10,806.69. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | 09:30 open · cash $10,806.69 · no holdings · equity $10,806.69 vs prior close $10,806.69 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,806.69 (session +0.00) | 16:00 close · cash $10,806.69 · no lots left · equity $10,806.69. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | 09:30 open · cash $10,806.69 · no holdings · equity $10,806.69 vs prior close $10,806.69 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,806.69 (session +0.00) | 16:00 close · cash $10,806.69 · no lots left · equity $10,806.69. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | 09:30 open · cash $10,806.69 · no holdings · equity $10,806.69 vs prior close $10,806.69 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,806.69 (session +0.00) | 16:00 close · cash $10,806.69 · no lots left · equity $10,806.69. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | 09:30 open · cash $10,806.69 · no holdings · equity $10,806.69 vs prior close $10,806.69 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,806.69 (session +0.00) | 16:00 close · cash $10,806.69 · no lots left · equity $10,806.69. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | 09:30 open · cash $10,806.69 · no holdings · equity $10,806.69 vs prior close $10,806.69 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,806.69 (session +0.00) | 16:00 close · cash $10,806.69 · no lots left · equity $10,806.69. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | 09:30 open · cash $10,806.69 · no holdings · equity $10,806.69 vs prior close $10,806.69 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,806.69 (session +0.00) | 16:00 close · cash $10,806.69 · no lots left · equity $10,806.69. | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | 09:30 open · cash $10,806.69 · no holdings · equity $10,806.69 vs prior close $10,806.69 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-21 09:30 ET | **BUY** | `NEO` | 542 | $19.92 | $6.99 | — | $3.06 | — | combo gate; gate flow_in=True,zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $10806.69 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.06 | ▼ close $10,523.28 vs 09:30 $10,806.69 (session -276.42) | 16:00 close · cash $3.06 · equity $10,523.28 vs 09:30 $10,806.69 (-283.41; session marks -276.42) · 1 name(s) marked open→close (per-name table). NEO×542 09:30 $19.92 → close $19.41 -276.42 | — |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `NEO` | 542 | 2026-09-21 @ $19.92 | combo gate; gate flow_in=True,zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $10806.69 |
