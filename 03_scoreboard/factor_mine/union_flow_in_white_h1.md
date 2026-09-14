# Factor mine action — `union_flow_in_white_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+9.83%** ($10,983) · signal-only (no cash/fees) was +12.54%. Starts YES **17/22**. Fills 22 · skips 0 · realized $+983.07.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,983.08.

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
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-21 | `HITI` | 4492 | — | $2.43 | +0.00 | $2.45 | +89.84 | +89.84 | +0.00 | +89.84 |
| 2026-08-24 | `HITI` | 4492 | $2.45 | $2.45 | +0.00 | — | +0.00 | +0.00 | +89.84 | — |
| 2026-08-25 | `RHI` | 125 | — | $43.76 | +0.00 | $44.90 | +142.50 | +142.50 | +0.00 | +142.50 |
| 2026-08-25 | `ABUS` | 1040 | — | $5.25 | +0.00 | $5.20 | -52.00 | -52.00 | +0.00 | -52.00 |
| 2026-08-26 | `RHI` | 125 | $44.90 | $44.33 | -71.25 | — | +0.00 | -71.25 | +71.25 | — |
| 2026-08-26 | `ABUS` | 1040 | $5.20 | $5.19 | -10.40 | — | +0.00 | -10.40 | -62.40 | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-31 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `VIR` | 945 | — | $11.54 | +0.00 | $11.45 | -85.05 | -85.05 | +0.00 | -85.05 |
| 2026-09-04 | `VIR` | 945 | $11.45 | $11.31 | -132.30 | — | +0.00 | -132.30 | -217.35 | — |
| 2026-09-04 | `ATRC` | 68 | — | $52.03 | +0.00 | $51.52 | -34.68 | -34.68 | +0.00 | -34.68 |
| 2026-09-04 | `ADCT` | 2739 | — | $1.30 | +0.00 | $1.36 | +164.34 | +164.34 | +0.00 | +164.34 |
| 2026-09-04 | `XP` | 180 | — | $19.67 | +0.00 | $19.86 | +34.20 | +34.20 | +0.00 | +34.20 |
| 2026-09-08 | `ATRC` | 68 | $51.52 | $54.31 | +189.72 | — | +0.00 | +189.72 | +155.04 | — |
| 2026-09-08 | `ADCT` | 2739 | $1.36 | $1.33 | -82.17 | — | +0.00 | -82.17 | +82.17 | — |
| 2026-09-08 | `XP` | 180 | $19.86 | $20.46 | +108.00 | — | +0.00 | +108.00 | +142.20 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +787.37 | TPG | — | $24.65 | $10,784.79 | TPG×197 |
| 2026-08-14 | +5.50 | $24.65 | TPG×197 | $10,916.78 | +131.99 | +207.04 | SPHR, KULR, RLX | TPG | $59.70 | $11,074.94 | SPHR×20, KULR×1455, RLX×1966 |
| 2026-08-17 | +2.25 | $59.70 | SPHR×20, KULR×1455, RLX×1966 | $11,023.07 | -51.87 | +0.00 | — | SPHR, KULR, RLX | $10,976.22 | $10,976.22 | — |
| 2026-08-18 | -6.20 | $10,976.22 | — | $10,976.22 | +0.00 | +0.00 | — | — | $10,976.22 | $10,976.22 | — |
| 2026-08-19 | -7.20 | $10,976.22 | — | $10,976.22 | +0.00 | +0.00 | — | — | $10,976.22 | $10,976.22 | — |
| 2026-08-20 | +1.12 | $10,976.22 | — | $10,976.22 | +0.00 | +0.00 | — | — | $10,976.22 | $10,976.22 | — |
| 2026-08-21 | +3.25 | $10,976.22 | — | $10,976.22 | +0.00 | +89.84 | HITI | — | $2.71 | $11,008.11 | HITI×4492 |
| 2026-08-24 | -5.17 | $2.71 | HITI×4492 | $11,008.11 | +0.00 | +0.00 | — | HITI | $10,949.33 | $10,949.33 | — |
| 2026-08-25 | +1.80 | $10,949.33 | — | $10,949.33 | +0.00 | +90.50 | RHI, ABUS | — | $3.55 | $11,024.05 | RHI×125, ABUS×1040 |
| 2026-08-26 | +2.02 | $3.55 | RHI×125, ABUS×1040 | $10,942.40 | -81.65 | +0.00 | — | RHI, ABUS | $10,926.34 | $10,926.34 | — |
| 2026-08-27 | — | $10,926.34 | — | $10,926.34 | +0.00 | +0.00 | — | — | $10,926.34 | $10,926.34 | — |
| 2026-08-28 | +0.75 | $10,926.34 | — | $10,926.34 | +0.00 | +0.00 | — | — | $10,926.34 | $10,926.34 | — |
| 2026-08-31 | -5.85 | $10,926.34 | — | $10,926.34 | +0.00 | +0.00 | — | — | $10,926.34 | $10,926.34 | — |
| 2026-09-01 | -6.30 | $10,926.34 | — | $10,926.34 | +0.00 | +0.00 | — | — | $10,926.34 | $10,926.34 | — |
| 2026-09-02 | -3.83 | $10,926.34 | — | $10,926.34 | +0.00 | +0.00 | — | — | $10,926.34 | $10,926.34 | — |
| 2026-09-03 | -0.90 | $10,926.34 | — | $10,926.34 | +0.00 | -85.05 | VIR | — | $8.85 | $10,829.10 | VIR×945 |
| 2026-09-04 | +2.25 | $8.85 | VIR×945 | $10,696.80 | -132.30 | +163.86 | ATRC, ADCT, XP | VIR | $4.97 | $10,808.17 | ATRC×68, ADCT×2739, XP×180 |
| 2026-09-08 | -11.47 | $4.97 | ATRC×68, ADCT×2739, XP×180 | $11,023.72 | +215.55 | +0.00 | — | ATRC, ADCT, XP | $10,983.08 | $10,983.08 | — |
| 2026-09-09 | -13.95 | $10,983.08 | — | $10,983.08 | -0.00 | +0.00 | — | — | $10,983.08 | $10,983.08 | — |
| 2026-09-10 | -13.28 | $10,983.08 | — | $10,983.08 | -0.00 | +0.00 | — | — | $10,983.08 | $10,983.08 | — |
| 2026-09-11 | +0.50 | $10,983.08 | — | $10,983.08 | -0.00 | +0.00 | — | — | $10,983.08 | $10,983.08 | — |
| 2026-09-14 | -11.00 | $10,983.08 | — | $10,983.08 | -0.00 | +0.00 | — | — | $10,983.08 | $10,983.08 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 197 | $50.62 | $2.58 | — | $24.65 | — | combo gate; gate flow_in=True,zero_red=True; list flatten; ⚪; ret5=+6.2; leftover $10000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.65 | ▲ close $10,784.79 vs 09:30 $10,000.00 (session +787.37) | 16:00 close · cash $24.65 · equity $10,784.79 vs 09:30 $10,000.00 (+784.79; session marks +787.37) · 1 name(s) marked open→close (per-name table). TPG×197 09:30 $50.62 → close $54.62 +787.37 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.65 | ▲ 09:30 equity $10,916.78 vs yday $10,784.79 (+131.99) | 09:30 open · cash $24.65 (unchanged overnight, no fees) · equity $10,916.78 vs prior close $10,784.79 (+131.99) · 1 name(s) re-marked at the open (per-name table). TPG×197 yday $54.62 → 09:30 $55.29 +131.99 | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 197 | $55.29 | $2.70 | $+914.08 | $10,914.08 | ▲ +914.08 after sell → book $10,914.08; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SPHR` | 20 | $176.68 | $2.05 | — | $7,378.43 | — | combo gate; gate flow_in=True,zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+14.4; leftover $3638.03 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `KULR` | 1455 | $2.50 | $18.77 | — | $3,722.16 | — | combo gate; gate flow_in=True,zero_red=True; list earn_react; 🔵; ⚪; ret5=+7.6; leftover $3638.03 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `RLX` | 1966 | $1.85 | $25.36 | — | $59.70 | — | combo gate; gate flow_in=True,zero_red=True; list earn_react; 🔵; ⚪; ret5=+0.5; leftover $3638.03 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.70 | ▲ close $11,074.94 vs 09:30 $10,916.78 (session +207.04) | 16:00 close · cash $59.70 · equity $11,074.94 vs 09:30 $10,916.78 (+158.16; session marks +207.04) · 3 name(s) marked open→close (per-name table). SPHR×20 09:30 $176.68 → close $168.00 -173.60; KULR×1455 09:30 $2.50 → close $2.64 +203.70; RLX×1966 09:30 $1.85 → close $1.94 +176.94 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.70 | ▼ 09:30 equity $11,023.07 vs yday $11,074.94 (-51.87) | 09:30 open · cash $59.70 (unchanged overnight, no fees) · equity $11,023.07 vs prior close $11,074.94 (-51.87) · 3 name(s) re-marked at the open (per-name table). SPHR×20 yday $168.00 → 09:30 $168.10 +2.00; KULR×1455 yday $2.64 → 09:30 $2.63 -14.55; RLX×1966 yday $1.94 → 09:30 $1.92 -39.32 | — |
| 2026-08-17 09:30 ET | **SELL** | `SPHR` | 20 | $168.10 | $2.09 | $-175.74 | $3,419.61 | ▼ -175.74 after sell → book $11,020.98; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `KULR` | 1455 | $2.63 | $19.04 | $+151.34 | $7,227.22 | ▲ +151.34 after sell → book $11,001.94; vs 09:30 mark -19.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `RLX` | 1966 | $1.92 | $25.72 | $+86.54 | $10,976.22 | ▲ +86.54 after sell → book $10,976.22; vs 09:30 mark -25.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,976.22 | ▲ close $10,976.22 vs 09:30 $11,023.07 (session +0.00) | 16:00 close · cash $10,976.22 · no lots left · equity $10,976.22. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,976.22 | ▲ 09:30 equity $10,976.22 vs yday $10,976.22 (+0.00) | 09:30 open · cash $10,976.22 · no holdings · equity $10,976.22 vs prior close $10,976.22 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,976.22 | ▲ close $10,976.22 vs 09:30 $10,976.22 (session +0.00) | 16:00 close · cash $10,976.22 · no lots left · equity $10,976.22. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,976.22 | ▲ 09:30 equity $10,976.22 vs yday $10,976.22 (+0.00) | 09:30 open · cash $10,976.22 · no holdings · equity $10,976.22 vs prior close $10,976.22 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,976.22 | ▲ close $10,976.22 vs 09:30 $10,976.22 (session +0.00) | 16:00 close · cash $10,976.22 · no lots left · equity $10,976.22. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,976.22 | ▲ 09:30 equity $10,976.22 vs yday $10,976.22 (+0.00) | 09:30 open · cash $10,976.22 · no holdings · equity $10,976.22 vs prior close $10,976.22 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,976.22 | ▲ close $10,976.22 vs 09:30 $10,976.22 (session +0.00) | 16:00 close · cash $10,976.22 · no lots left · equity $10,976.22. | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,976.22 | ▲ 09:30 equity $10,976.22 vs yday $10,976.22 (+0.00) | 09:30 open · cash $10,976.22 · no holdings · equity $10,976.22 vs prior close $10,976.22 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 4492 | $2.43 | $57.95 | — | $2.71 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $10976.22 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.71 | ▲ close $11,008.11 vs 09:30 $10,976.22 (session +89.84) | 16:00 close · cash $2.71 · equity $11,008.11 vs 09:30 $10,976.22 (+31.89; session marks +89.84) · 1 name(s) marked open→close (per-name table). HITI×4492 09:30 $2.43 → close $2.45 +89.84 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.71 | ▲ 09:30 equity $11,008.11 vs yday $11,008.11 (+0.00) | 09:30 open · cash $2.71 (unchanged overnight, no fees) · equity $11,008.11 vs prior close $11,008.11 (+0.00) · 1 name(s) re-marked at the open (per-name table). HITI×4492 yday $2.45 → 09:30 $2.45 +0.00 | — |
| 2026-08-24 09:30 ET | **SELL** | `HITI` | 4492 | $2.45 | $58.78 | $-26.89 | $10,949.33 | ▼ -26.89 after sell → book $10,949.33; vs 09:30 mark -58.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,949.33 | ▲ close $10,949.33 vs 09:30 $11,008.11 (session +0.00) | 16:00 close · cash $10,949.33 · no lots left · equity $10,949.33. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,949.33 | ▲ 09:30 equity $10,949.33 vs yday $10,949.33 (+0.00) | 09:30 open · cash $10,949.33 · no holdings · equity $10,949.33 vs prior close $10,949.33 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 125 | $43.76 | $2.37 | — | $5,476.97 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $5474.67 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ABUS` | 1040 | $5.25 | $13.42 | — | $3.55 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+10.4; leftover $5474.67 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.55 | ▲ close $11,024.05 vs 09:30 $10,949.33 (session +90.50) | 16:00 close · cash $3.55 · equity $11,024.05 vs 09:30 $10,949.33 (+74.72; session marks +90.50) · 2 name(s) marked open→close (per-name table). RHI×125 09:30 $43.76 → close $44.90 +142.50; ABUS×1040 09:30 $5.25 → close $5.20 -52.00 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.55 | ▼ 09:30 equity $10,942.40 vs yday $11,024.05 (-81.65) | 09:30 open · cash $3.55 (unchanged overnight, no fees) · equity $10,942.40 vs prior close $11,024.05 (-81.65) · 2 name(s) re-marked at the open (per-name table). RHI×125 yday $44.90 → 09:30 $44.33 -71.25; ABUS×1040 yday $5.20 → 09:30 $5.19 -10.40 | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 125 | $44.33 | $2.43 | $+66.45 | $5,542.37 | ▲ +66.45 after sell → book $10,939.97; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ABUS` | 1040 | $5.19 | $13.63 | $-89.45 | $10,926.34 | ▼ -89.45 after sell → book $10,926.34; vs 09:30 mark -13.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,926.34 | ▲ close $10,926.34 vs 09:30 $10,942.40 (session +0.00) | 16:00 close · cash $10,926.34 · no lots left · equity $10,926.34. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,926.34 | ▲ 09:30 equity $10,926.34 vs yday $10,926.34 (+0.00) | 09:30 open · cash $10,926.34 · no holdings · equity $10,926.34 vs prior close $10,926.34 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,926.34 | ▲ close $10,926.34 vs 09:30 $10,926.34 (session +0.00) | 16:00 close · cash $10,926.34 · no lots left · equity $10,926.34. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,926.34 | ▲ 09:30 equity $10,926.34 vs yday $10,926.34 (+0.00) | 09:30 open · cash $10,926.34 · no holdings · equity $10,926.34 vs prior close $10,926.34 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,926.34 | ▲ close $10,926.34 vs 09:30 $10,926.34 (session +0.00) | 16:00 close · cash $10,926.34 · no lots left · equity $10,926.34. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,926.34 | ▲ 09:30 equity $10,926.34 vs yday $10,926.34 (+0.00) | 09:30 open · cash $10,926.34 · no holdings · equity $10,926.34 vs prior close $10,926.34 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,926.34 | ▲ close $10,926.34 vs 09:30 $10,926.34 (session +0.00) | 16:00 close · cash $10,926.34 · no lots left · equity $10,926.34. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,926.34 | ▲ 09:30 equity $10,926.34 vs yday $10,926.34 (+0.00) | 09:30 open · cash $10,926.34 · no holdings · equity $10,926.34 vs prior close $10,926.34 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,926.34 | ▲ close $10,926.34 vs 09:30 $10,926.34 (session +0.00) | 16:00 close · cash $10,926.34 · no lots left · equity $10,926.34. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,926.34 | ▲ 09:30 equity $10,926.34 vs yday $10,926.34 (+0.00) | 09:30 open · cash $10,926.34 · no holdings · equity $10,926.34 vs prior close $10,926.34 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,926.34 | ▲ close $10,926.34 vs 09:30 $10,926.34 (session +0.00) | 16:00 close · cash $10,926.34 · no lots left · equity $10,926.34. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,926.34 | ▲ 09:30 equity $10,926.34 vs yday $10,926.34 (+0.00) | 09:30 open · cash $10,926.34 · no holdings · equity $10,926.34 vs prior close $10,926.34 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 945 | $11.54 | $12.19 | — | $8.85 | — | combo gate; gate flow_in=True,zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $10926.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.85 | ▼ close $10,829.10 vs 09:30 $10,926.34 (session -85.05) | 16:00 close · cash $8.85 · equity $10,829.10 vs 09:30 $10,926.34 (-97.24; session marks -85.05) · 1 name(s) marked open→close (per-name table). VIR×945 09:30 $11.54 → close $11.45 -85.05 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.85 | ▼ 09:30 equity $10,696.80 vs yday $10,829.10 (-132.30) | 09:30 open · cash $8.85 (unchanged overnight, no fees) · equity $10,696.80 vs prior close $10,829.10 (-132.30) · 1 name(s) re-marked at the open (per-name table). VIR×945 yday $11.45 → 09:30 $11.31 -132.30 | — |
| 2026-09-04 09:30 ET | **SELL** | `VIR` | 945 | $11.31 | $12.43 | $-241.97 | $10,684.37 | ▼ -241.97 after sell → book $10,684.37; vs 09:30 mark -12.43 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 68 | $52.03 | $2.19 | — | $7,144.13 | — | combo gate; gate flow_in=True,zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $3561.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ADCT` | 2739 | $1.30 | $35.33 | — | $3,548.10 | — | combo gate; gate flow_in=True,zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+17.9; leftover $3561.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `XP` | 180 | $19.67 | $2.53 | — | $4.97 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+13.1; leftover $3561.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.97 | ▲ close $10,808.17 vs 09:30 $10,696.80 (session +163.86) | 16:00 close · cash $4.97 · equity $10,808.17 vs 09:30 $10,696.80 (+111.37; session marks +163.86) · 3 name(s) marked open→close (per-name table). ATRC×68 09:30 $52.03 → close $51.52 -34.68; ADCT×2739 09:30 $1.30 → close $1.36 +164.34; XP×180 09:30 $19.67 → close $19.86 +34.20 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.97 | ▲ 09:30 equity $11,023.72 vs yday $10,808.17 (+215.55) | 09:30 open · cash $4.97 (unchanged overnight, no fees) · equity $11,023.72 vs prior close $10,808.17 (+215.55) · 3 name(s) re-marked at the open (per-name table). ATRC×68 yday $51.52 → 09:30 $54.31 +189.72; ADCT×2739 yday $1.36 → 09:30 $1.33 -82.17; XP×180 yday $19.86 → 09:30 $20.46 +108.00 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 68 | $54.31 | $2.23 | $+150.61 | $3,695.81 | ▲ +150.61 after sell → book $11,021.48; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ADCT` | 2739 | $1.33 | $35.82 | $+11.02 | $7,302.87 | ▲ +11.02 after sell → book $10,985.67; vs 09:30 mark -35.81 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `XP` | 180 | $20.46 | $2.59 | $+137.08 | $10,983.08 | ▲ +137.08 after sell → book $10,983.08; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,983.08 | ▲ close $10,983.08 vs 09:30 $11,023.72 (session +0.00) | 16:00 close · cash $10,983.08 · no lots left · equity $10,983.08. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,983.08 | ▲ 09:30 equity $10,983.08 vs yday $10,983.08 (-0.00) | 09:30 open · cash $10,983.08 · no holdings · equity $10,983.08 vs prior close $10,983.08 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,983.08 | ▲ close $10,983.08 vs 09:30 $10,983.08 (session +0.00) | 16:00 close · cash $10,983.08 · no lots left · equity $10,983.08. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,983.08 | ▲ 09:30 equity $10,983.08 vs yday $10,983.08 (-0.00) | 09:30 open · cash $10,983.08 · no holdings · equity $10,983.08 vs prior close $10,983.08 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,983.08 | ▲ close $10,983.08 vs 09:30 $10,983.08 (session +0.00) | 16:00 close · cash $10,983.08 · no lots left · equity $10,983.08. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,983.08 | ▲ 09:30 equity $10,983.08 vs yday $10,983.08 (-0.00) | 09:30 open · cash $10,983.08 · no holdings · equity $10,983.08 vs prior close $10,983.08 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,983.08 | ▲ close $10,983.08 vs 09:30 $10,983.08 (session +0.00) | 16:00 close · cash $10,983.08 · no lots left · equity $10,983.08. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,983.08 | ▲ 09:30 equity $10,983.08 vs yday $10,983.08 (-0.00) | 09:30 open · cash $10,983.08 · no holdings · equity $10,983.08 vs prior close $10,983.08 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,983.08 | ▲ close $10,983.08 vs 09:30 $10,983.08 (session +0.00) | 16:00 close · cash $10,983.08 · no lots left · equity $10,983.08. | — |
