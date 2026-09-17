# Factor mine action — `union_flow_in_white_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+0.64%** ($10,063) · signal-only (no cash/fees) was +4.31%. Starts YES **17/25**. Fills 20 · skips 0 · realized $+63.51.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,063.49.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `SPHR` | 18 | — | $176.68 | +0.00 | $168.00 | -156.24 | -156.24 | +0.00 | -156.24 |
| 2026-08-14 | `KULR` | 1333 | — | $2.50 | +0.00 | $2.64 | +186.62 | +186.62 | +0.00 | +186.62 |
| 2026-08-14 | `RLX` | 1801 | — | $1.85 | +0.00 | $1.94 | +162.09 | +162.09 | +0.00 | +162.09 |
| 2026-08-17 | `SPHR` | 18 | $168.00 | $168.10 | +1.80 | — | +0.00 | +1.80 | -154.44 | — |
| 2026-08-17 | `KULR` | 1333 | $2.64 | $2.63 | -13.33 | — | +0.00 | -13.33 | +173.29 | — |
| 2026-08-17 | `RLX` | 1801 | $1.94 | $1.92 | -36.02 | — | +0.00 | -36.02 | +126.07 | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-21 | `HITI` | 4117 | — | $2.43 | +0.00 | $2.45 | +82.34 | +82.34 | +0.00 | +82.34 |
| 2026-08-24 | `HITI` | 4117 | $2.45 | $2.45 | +0.00 | — | +0.00 | +0.00 | +82.34 | — |
| 2026-08-25 | `RHI` | 114 | — | $43.76 | +0.00 | $44.90 | +129.96 | +129.96 | +0.00 | +129.96 |
| 2026-08-25 | `ABUS` | 955 | — | $5.25 | +0.00 | $5.20 | -47.75 | -47.75 | +0.00 | -47.75 |
| 2026-08-26 | `RHI` | 114 | $44.90 | $44.33 | -64.98 | — | +0.00 | -64.98 | +64.98 | — |
| 2026-08-26 | `ABUS` | 955 | $5.20 | $5.19 | -9.55 | — | +0.00 | -9.55 | -57.30 | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-31 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `VIR` | 866 | — | $11.54 | +0.00 | $11.45 | -77.94 | -77.94 | +0.00 | -77.94 |
| 2026-09-04 | `VIR` | 866 | $11.45 | $11.31 | -121.24 | — | +0.00 | -121.24 | -199.18 | — |
| 2026-09-04 | `ATRC` | 62 | — | $52.03 | +0.00 | $51.52 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-09-04 | `ADCT` | 2510 | — | $1.30 | +0.00 | $1.36 | +150.60 | +150.60 | +0.00 | +150.60 |
| 2026-09-04 | `XP` | 165 | — | $19.67 | +0.00 | $19.86 | +31.35 | +31.35 | +0.00 | +31.35 |
| 2026-09-08 | `ATRC` | 62 | $51.52 | $54.31 | +172.98 | — | +0.00 | +172.98 | +141.36 | — |
| 2026-09-08 | `ADCT` | 2510 | $1.36 | $1.33 | -75.30 | — | +0.00 | -75.30 | +75.30 | — |
| 2026-09-08 | `XP` | 165 | $19.86 | $20.46 | +99.00 | — | +0.00 | +99.00 | +130.35 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +192.47 | SPHR, KULR, RLX | — | $112.94 | $10,150.00 | SPHR×18, KULR×1333, RLX×1801 |
| 2026-08-17 | +2.25 | $112.94 | SPHR×18, KULR×1333, RLX×1801 | $10,102.45 | -47.55 | +0.00 | — | SPHR, KULR, RLX | $10,059.36 | $10,059.36 | — |
| 2026-08-18 | -6.20 | $10,059.36 | — | $10,059.36 | +0.00 | +0.00 | — | — | $10,059.36 | $10,059.36 | — |
| 2026-08-19 | -7.20 | $10,059.36 | — | $10,059.36 | +0.00 | +0.00 | — | — | $10,059.36 | $10,059.36 | — |
| 2026-08-20 | +1.12 | $10,059.36 | — | $10,059.36 | +0.00 | +0.00 | — | — | $10,059.36 | $10,059.36 | — |
| 2026-08-21 | +3.25 | $10,059.36 | — | $10,059.36 | +0.00 | +82.34 | HITI | — | $1.95 | $10,088.60 | HITI×4117 |
| 2026-08-24 | -5.17 | $1.95 | HITI×4117 | $10,088.60 | -0.00 | +0.00 | — | HITI | $10,034.72 | $10,034.72 | — |
| 2026-08-25 | +1.80 | $10,034.72 | — | $10,034.72 | +0.00 | +82.21 | RHI, ABUS | — | $17.68 | $10,102.28 | RHI×114, ABUS×955 |
| 2026-08-26 | +2.02 | $17.68 | RHI×114, ABUS×955 | $10,027.75 | -74.53 | +0.00 | — | RHI, ABUS | $10,012.84 | $10,012.84 | — |
| 2026-08-27 | — | $10,012.84 | — | $10,012.84 | +0.00 | +0.00 | — | — | $10,012.84 | $10,012.84 | — |
| 2026-08-28 | +0.75 | $10,012.84 | — | $10,012.84 | +0.00 | +0.00 | — | — | $10,012.84 | $10,012.84 | — |
| 2026-08-31 | -5.85 | $10,012.84 | — | $10,012.84 | +0.00 | +0.00 | — | — | $10,012.84 | $10,012.84 | — |
| 2026-09-01 | -6.30 | $10,012.84 | — | $10,012.84 | +0.00 | +0.00 | — | — | $10,012.84 | $10,012.84 | — |
| 2026-09-02 | -3.83 | $10,012.84 | — | $10,012.84 | +0.00 | +0.00 | — | — | $10,012.84 | $10,012.84 | — |
| 2026-09-03 | -0.90 | $10,012.84 | — | $10,012.84 | +0.00 | -77.94 | VIR | — | $8.03 | $9,923.73 | VIR×866 |
| 2026-09-04 | +2.25 | $8.03 | VIR×866 | $9,802.49 | -121.24 | +150.33 | ATRC, ADCT, XP | VIR | $19.65 | $9,904.39 | ATRC×62, ADCT×2510, XP×165 |
| 2026-09-08 | -11.47 | $19.65 | ATRC×62, ADCT×2510, XP×165 | $10,101.07 | +196.68 | +0.00 | — | ATRC, ADCT, XP | $10,063.49 | $10,063.49 | — |
| 2026-09-09 | -13.95 | $10,063.49 | — | $10,063.49 | +0.00 | +0.00 | — | — | $10,063.49 | $10,063.49 | — |
| 2026-09-10 | -13.28 | $10,063.49 | — | $10,063.49 | +0.00 | +0.00 | — | — | $10,063.49 | $10,063.49 | — |
| 2026-09-11 | +0.50 | $10,063.49 | — | $10,063.49 | +0.00 | +0.00 | — | — | $10,063.49 | $10,063.49 | — |
| 2026-09-14 | -11.00 | $10,063.49 | — | $10,063.49 | +0.00 | +0.00 | — | — | $10,063.49 | $10,063.49 | — |
| 2026-09-15 | -3.84 | $10,063.49 | — | $10,063.49 | +0.00 | +0.00 | — | — | $10,063.49 | $10,063.49 | — |
| 2026-09-16 | +5.30 | $10,063.49 | — | $10,063.49 | +0.00 | +0.00 | — | — | $10,063.49 | $10,063.49 | — |
| 2026-09-17 | +7.38 | $10,063.49 | — | $10,063.49 | +0.00 | +0.00 | — | — | $10,063.49 | $10,063.49 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `SPHR` | 18 | $176.68 | $2.04 | — | $6,817.72 | — | combo gate; gate flow_in=True,zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+14.4; leftover $3333.33 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `KULR` | 1333 | $2.50 | $17.20 | — | $3,468.02 | — | combo gate; gate flow_in=True,zero_red=True; list earn_react; 🔵; ⚪; ret5=+7.6; leftover $3333.33 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `RLX` | 1801 | $1.85 | $23.23 | — | $112.94 | — | combo gate; gate flow_in=True,zero_red=True; list earn_react; 🔵; ⚪; ret5=+0.5; leftover $3333.33 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $112.94 | ▲ close $10,150.00 vs 09:30 $10,000.00 (session +192.47) | 16:00 close · cash $112.94 · equity $10,150.00 vs 09:30 $10,000.00 (+150.00; session marks +192.47) · 3 name(s) marked open→close (per-name table). SPHR×18 09:30 $176.68 → close $168.00 -156.24; KULR×1333 09:30 $2.50 → close $2.64 +186.62; RLX×1801 09:30 $1.85 → close $1.94 +162.09 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $112.94 | ▼ 09:30 equity $10,102.45 vs yday $10,150.00 (-47.55) | 09:30 open · cash $112.94 (unchanged overnight, no fees) · equity $10,102.45 vs prior close $10,150.00 (-47.55) · 3 name(s) re-marked at the open (per-name table). SPHR×18 yday $168.00 → 09:30 $168.10 +1.80; KULR×1333 yday $2.64 → 09:30 $2.63 -13.33; RLX×1801 yday $1.94 → 09:30 $1.92 -36.02 | — |
| 2026-08-17 09:30 ET | **SELL** | `SPHR` | 18 | $168.10 | $2.08 | $-158.56 | $3,136.66 | ▼ -158.56 after sell → book $10,100.37; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `KULR` | 1333 | $2.63 | $17.45 | $+138.65 | $6,625.00 | ▲ +138.65 after sell → book $10,082.92; vs 09:30 mark -17.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `RLX` | 1801 | $1.92 | $23.56 | $+79.28 | $10,059.36 | ▲ +79.28 after sell → book $10,059.36; vs 09:30 mark -23.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,059.36 | ▲ close $10,059.36 vs 09:30 $10,102.45 (session +0.00) | 16:00 close · cash $10,059.36 · no lots left · equity $10,059.36. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,059.36 | ▲ 09:30 equity $10,059.36 vs yday $10,059.36 (+0.00) | 09:30 open · cash $10,059.36 · no holdings · equity $10,059.36 vs prior close $10,059.36 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,059.36 | ▲ close $10,059.36 vs 09:30 $10,059.36 (session +0.00) | 16:00 close · cash $10,059.36 · no lots left · equity $10,059.36. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,059.36 | ▲ 09:30 equity $10,059.36 vs yday $10,059.36 (+0.00) | 09:30 open · cash $10,059.36 · no holdings · equity $10,059.36 vs prior close $10,059.36 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,059.36 | ▲ close $10,059.36 vs 09:30 $10,059.36 (session +0.00) | 16:00 close · cash $10,059.36 · no lots left · equity $10,059.36. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,059.36 | ▲ 09:30 equity $10,059.36 vs yday $10,059.36 (+0.00) | 09:30 open · cash $10,059.36 · no holdings · equity $10,059.36 vs prior close $10,059.36 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,059.36 | ▲ close $10,059.36 vs 09:30 $10,059.36 (session +0.00) | 16:00 close · cash $10,059.36 · no lots left · equity $10,059.36. | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,059.36 | ▲ 09:30 equity $10,059.36 vs yday $10,059.36 (+0.00) | 09:30 open · cash $10,059.36 · no holdings · equity $10,059.36 vs prior close $10,059.36 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 4117 | $2.43 | $53.11 | — | $1.95 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $10059.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.95 | ▲ close $10,088.60 vs 09:30 $10,059.36 (session +82.34) | 16:00 close · cash $1.95 · equity $10,088.60 vs 09:30 $10,059.36 (+29.24; session marks +82.34) · 1 name(s) marked open→close (per-name table). HITI×4117 09:30 $2.43 → close $2.45 +82.34 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.95 | ▲ 09:30 equity $10,088.60 vs yday $10,088.60 (-0.00) | 09:30 open · cash $1.95 (unchanged overnight, no fees) · equity $10,088.60 vs prior close $10,088.60 (-0.00) · 1 name(s) re-marked at the open (per-name table). HITI×4117 yday $2.45 → 09:30 $2.45 +0.00 | — |
| 2026-08-24 09:30 ET | **SELL** | `HITI` | 4117 | $2.45 | $53.87 | $-24.64 | $10,034.72 | ▼ -24.64 after sell → book $10,034.72; vs 09:30 mark -53.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,034.72 | ▲ close $10,034.72 vs 09:30 $10,088.60 (session +0.00) | 16:00 close · cash $10,034.72 · no lots left · equity $10,034.72. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,034.72 | ▲ 09:30 equity $10,034.72 vs yday $10,034.72 (+0.00) | 09:30 open · cash $10,034.72 · no holdings · equity $10,034.72 vs prior close $10,034.72 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 114 | $43.76 | $2.33 | — | $5,043.75 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $5017.36 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ABUS` | 955 | $5.25 | $12.32 | — | $17.68 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+10.4; leftover $5017.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.68 | ▲ close $10,102.28 vs 09:30 $10,034.72 (session +82.21) | 16:00 close · cash $17.68 · equity $10,102.28 vs 09:30 $10,034.72 (+67.56; session marks +82.21) · 2 name(s) marked open→close (per-name table). RHI×114 09:30 $43.76 → close $44.90 +129.96; ABUS×955 09:30 $5.25 → close $5.20 -47.75 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.68 | ▼ 09:30 equity $10,027.75 vs yday $10,102.28 (-74.53) | 09:30 open · cash $17.68 (unchanged overnight, no fees) · equity $10,027.75 vs prior close $10,102.28 (-74.53) · 2 name(s) re-marked at the open (per-name table). RHI×114 yday $44.90 → 09:30 $44.33 -64.98; ABUS×955 yday $5.20 → 09:30 $5.19 -9.55 | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 114 | $44.33 | $2.39 | $+60.26 | $5,068.91 | ▲ +60.26 after sell → book $10,025.36; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ABUS` | 955 | $5.19 | $12.52 | $-82.14 | $10,012.84 | ▼ -82.14 after sell → book $10,012.84; vs 09:30 mark -12.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,012.84 | ▲ close $10,012.84 vs 09:30 $10,027.75 (session +0.00) | 16:00 close · cash $10,012.84 · no lots left · equity $10,012.84. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,012.84 | ▲ 09:30 equity $10,012.84 vs yday $10,012.84 (+0.00) | 09:30 open · cash $10,012.84 · no holdings · equity $10,012.84 vs prior close $10,012.84 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,012.84 | ▲ close $10,012.84 vs 09:30 $10,012.84 (session +0.00) | 16:00 close · cash $10,012.84 · no lots left · equity $10,012.84. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,012.84 | ▲ 09:30 equity $10,012.84 vs yday $10,012.84 (+0.00) | 09:30 open · cash $10,012.84 · no holdings · equity $10,012.84 vs prior close $10,012.84 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,012.84 | ▲ close $10,012.84 vs 09:30 $10,012.84 (session +0.00) | 16:00 close · cash $10,012.84 · no lots left · equity $10,012.84. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,012.84 | ▲ 09:30 equity $10,012.84 vs yday $10,012.84 (+0.00) | 09:30 open · cash $10,012.84 · no holdings · equity $10,012.84 vs prior close $10,012.84 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,012.84 | ▲ close $10,012.84 vs 09:30 $10,012.84 (session +0.00) | 16:00 close · cash $10,012.84 · no lots left · equity $10,012.84. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,012.84 | ▲ 09:30 equity $10,012.84 vs yday $10,012.84 (+0.00) | 09:30 open · cash $10,012.84 · no holdings · equity $10,012.84 vs prior close $10,012.84 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,012.84 | ▲ close $10,012.84 vs 09:30 $10,012.84 (session +0.00) | 16:00 close · cash $10,012.84 · no lots left · equity $10,012.84. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,012.84 | ▲ 09:30 equity $10,012.84 vs yday $10,012.84 (+0.00) | 09:30 open · cash $10,012.84 · no holdings · equity $10,012.84 vs prior close $10,012.84 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,012.84 | ▲ close $10,012.84 vs 09:30 $10,012.84 (session +0.00) | 16:00 close · cash $10,012.84 · no lots left · equity $10,012.84. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,012.84 | ▲ 09:30 equity $10,012.84 vs yday $10,012.84 (+0.00) | 09:30 open · cash $10,012.84 · no holdings · equity $10,012.84 vs prior close $10,012.84 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 866 | $11.54 | $11.17 | — | $8.03 | — | combo gate; gate flow_in=True,zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $10012.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.03 | ▼ close $9,923.73 vs 09:30 $10,012.84 (session -77.94) | 16:00 close · cash $8.03 · equity $9,923.73 vs 09:30 $10,012.84 (-89.11; session marks -77.94) · 1 name(s) marked open→close (per-name table). VIR×866 09:30 $11.54 → close $11.45 -77.94 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.03 | ▼ 09:30 equity $9,802.49 vs yday $9,923.73 (-121.24) | 09:30 open · cash $8.03 (unchanged overnight, no fees) · equity $9,802.49 vs prior close $9,923.73 (-121.24) · 1 name(s) re-marked at the open (per-name table). VIR×866 yday $11.45 → 09:30 $11.31 -121.24 | — |
| 2026-09-04 09:30 ET | **SELL** | `VIR` | 866 | $11.31 | $11.39 | $-221.74 | $9,791.10 | ▼ -221.74 after sell → book $9,791.10; vs 09:30 mark -11.39 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 62 | $52.03 | $2.18 | — | $6,563.06 | — | combo gate; gate flow_in=True,zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $3263.70 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ADCT` | 2510 | $1.30 | $32.38 | — | $3,267.68 | — | combo gate; gate flow_in=True,zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+17.9; leftover $3263.70 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `XP` | 165 | $19.67 | $2.48 | — | $19.65 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+13.1; leftover $3263.70 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.65 | ▲ close $9,904.39 vs 09:30 $9,802.49 (session +150.33) | 16:00 close · cash $19.65 · equity $9,904.39 vs 09:30 $9,802.49 (+101.90; session marks +150.33) · 3 name(s) marked open→close (per-name table). ATRC×62 09:30 $52.03 → close $51.52 -31.62; ADCT×2510 09:30 $1.30 → close $1.36 +150.60; XP×165 09:30 $19.67 → close $19.86 +31.35 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.65 | ▲ 09:30 equity $10,101.07 vs yday $9,904.39 (+196.68) | 09:30 open · cash $19.65 (unchanged overnight, no fees) · equity $10,101.07 vs prior close $9,904.39 (+196.68) · 3 name(s) re-marked at the open (per-name table). ATRC×62 yday $51.52 → 09:30 $54.31 +172.98; ADCT×2510 yday $1.36 → 09:30 $1.33 -75.30; XP×165 yday $19.86 → 09:30 $20.46 +99.00 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 62 | $54.31 | $2.21 | $+136.97 | $3,384.65 | ▲ +136.97 after sell → book $10,098.85; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ADCT` | 2510 | $1.33 | $32.82 | $+10.10 | $6,690.13 | ▲ +10.10 after sell → book $10,066.03; vs 09:30 mark -32.82 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `XP` | 165 | $20.46 | $2.54 | $+125.33 | $10,063.49 | ▲ +125.33 after sell → book $10,063.49; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,063.49 | ▲ close $10,063.49 vs 09:30 $10,101.07 (session +0.00) | 16:00 close · cash $10,063.49 · no lots left · equity $10,063.49. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,063.49 | ▲ 09:30 equity $10,063.49 vs yday $10,063.49 (+0.00) | 09:30 open · cash $10,063.49 · no holdings · equity $10,063.49 vs prior close $10,063.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,063.49 | ▲ close $10,063.49 vs 09:30 $10,063.49 (session +0.00) | 16:00 close · cash $10,063.49 · no lots left · equity $10,063.49. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,063.49 | ▲ 09:30 equity $10,063.49 vs yday $10,063.49 (+0.00) | 09:30 open · cash $10,063.49 · no holdings · equity $10,063.49 vs prior close $10,063.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,063.49 | ▲ close $10,063.49 vs 09:30 $10,063.49 (session +0.00) | 16:00 close · cash $10,063.49 · no lots left · equity $10,063.49. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,063.49 | ▲ 09:30 equity $10,063.49 vs yday $10,063.49 (+0.00) | 09:30 open · cash $10,063.49 · no holdings · equity $10,063.49 vs prior close $10,063.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,063.49 | ▲ close $10,063.49 vs 09:30 $10,063.49 (session +0.00) | 16:00 close · cash $10,063.49 · no lots left · equity $10,063.49. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,063.49 | ▲ 09:30 equity $10,063.49 vs yday $10,063.49 (+0.00) | 09:30 open · cash $10,063.49 · no holdings · equity $10,063.49 vs prior close $10,063.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,063.49 | ▲ close $10,063.49 vs 09:30 $10,063.49 (session +0.00) | 16:00 close · cash $10,063.49 · no lots left · equity $10,063.49. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,063.49 | ▲ 09:30 equity $10,063.49 vs yday $10,063.49 (+0.00) | 09:30 open · cash $10,063.49 · no holdings · equity $10,063.49 vs prior close $10,063.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,063.49 | ▲ close $10,063.49 vs 09:30 $10,063.49 (session +0.00) | 16:00 close · cash $10,063.49 · no lots left · equity $10,063.49. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,063.49 | ▲ 09:30 equity $10,063.49 vs yday $10,063.49 (+0.00) | 09:30 open · cash $10,063.49 · no holdings · equity $10,063.49 vs prior close $10,063.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,063.49 | ▲ close $10,063.49 vs 09:30 $10,063.49 (session +0.00) | 16:00 close · cash $10,063.49 · no lots left · equity $10,063.49. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,063.49 | ▲ 09:30 equity $10,063.49 vs yday $10,063.49 (+0.00) | 09:30 open · cash $10,063.49 · no holdings · equity $10,063.49 vs prior close $10,063.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,063.49 | ▲ close $10,063.49 vs 09:30 $10,063.49 (session +0.00) | 16:00 close · cash $10,063.49 · no lots left · equity $10,063.49. | — |
