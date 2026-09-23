# Factor mine action — `overnight_mega_h2`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **2** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `overnight_mega` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · same mega calendar; hold 2 also keeps the session after the print

Cash book **-7.35%** ($9,265) · signal-only (no cash/fees) was -6.81%. Starts YES **1/29**. Fills 10 · skips 26 · realized $-735.01.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the same calendar list, kept only when prior-export mcap is at least $50B and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 2 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the same calendar list, kept only when prior-export mcap is at least $50B.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the same calendar list, kept only when prior-export mcap is at least $50B that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 2 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 2 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `overnight_mega` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **2**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,264.99.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `ROST` | 43 | — | $229.55 | +0.00 | $228.99 | -24.08 | -24.08 | +0.00 | -24.08 |
| 2026-08-21 | `ROST` | 43 | $228.99 | $243.85 | +638.98 | $239.04 | -206.83 | +432.15 | +614.90 | +408.07 |
| 2026-08-21 | `PDD` | 1 | — | $90.03 | +0.00 | $88.38 | -1.65 | -1.65 | +0.00 | -1.65 |
| 2026-08-24 | `ROST` | 43 | $239.04 | $238.08 | -41.28 | — | +0.00 | -41.28 | +366.79 | — |
| 2026-08-24 | `PDD` | 1 | $88.38 | $90.95 | +2.57 | $87.07 | -3.88 | -1.31 | +0.92 | -2.96 |
| 2026-08-25 | `PDD` | 1 | $87.07 | $86.65 | -0.42 | — | +0.00 | -0.42 | -3.38 | — |
| 2026-08-25 | `INTU` | 28 | — | $364.35 | +0.00 | $357.46 | -192.92 | -192.92 | +0.00 | -192.92 |
| 2026-08-26 | `INTU` | 28 | $357.46 | $323.47 | -951.72 | $345.88 | +627.48 | -324.24 | -1144.64 | -517.16 |
| 2026-08-27 | `INTU` | 28 | $345.88 | $353.54 | +214.48 | — | +0.00 | +214.48 | -302.68 | — |
| 2026-08-27 | `ADSK` | 19 | — | $261.47 | +0.00 | $270.58 | +173.09 | +173.09 | +0.00 | +173.09 |
| 2026-08-27 | `MRVL` | 19 | — | $253.44 | +0.00 | $241.45 | -227.81 | -227.81 | +0.00 | -227.81 |
| 2026-08-28 | `ADSK` | 19 | $270.58 | $261.16 | -178.98 | $260.66 | -9.50 | -188.48 | -5.89 | -15.39 |
| 2026-08-28 | `MRVL` | 19 | $241.45 | $225.26 | -307.61 | $216.62 | -164.16 | -471.77 | -535.42 | -699.58 |
| 2026-08-31 | `ADSK` | 19 | $260.66 | $257.71 | -56.05 | — | +0.00 | -56.05 | -71.44 | — |
| 2026-08-31 | `MRVL` | 19 | $216.62 | $216.30 | -6.08 | — | +0.00 | -6.08 | -705.66 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-08 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-21 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-22 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-23 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | -24.08 | ROST | — | $127.23 | $9,973.80 | ROST×43 |
| 2026-08-21 | +3.25 | $127.23 | ROST×43 | $10,612.78 | +638.98 | -208.48 | PDD | — | $36.30 | $10,403.40 | ROST×43, PDD×1 |
| 2026-08-24 | -5.17 | $36.30 | ROST×43, PDD×1 | $10,364.69 | -38.71 | -3.88 | — | ROST | $10,271.53 | $10,358.60 | PDD×1 |
| 2026-08-25 | +1.80 | $10,271.53 | PDD×1 | $10,358.18 | -0.42 | -192.92 | INTU | PDD | $153.41 | $10,162.29 | INTU×28 |
| 2026-08-26 | +2.02 | $153.41 | INTU×28 | $9,210.57 | -951.72 | +627.48 | — | — | $153.41 | $9,838.05 | INTU×28 |
| 2026-08-27 | — | $153.41 | INTU×28 | $10,052.53 | +214.48 | -54.72 | ADSK, MRVL | INTU | $262.99 | $9,991.56 | ADSK×19, MRVL×19 |
| 2026-08-28 | +0.75 | $262.99 | ADSK×19, MRVL×19 | $9,504.97 | -486.59 | -173.66 | — | — | $262.99 | $9,331.31 | ADSK×19, MRVL×19 |
| 2026-08-31 | -5.85 | $262.99 | ADSK×19, MRVL×19 | $9,269.18 | -62.13 | +0.00 | — | ADSK, MRVL | $9,264.99 | $9,264.99 | — |
| 2026-09-01 | -6.30 | $9,264.99 | — | $9,264.99 | -0.00 | +0.00 | — | — | $9,264.99 | $9,264.99 | — |
| 2026-09-02 | -3.83 | $9,264.99 | — | $9,264.99 | -0.00 | +0.00 | — | — | $9,264.99 | $9,264.99 | — |
| 2026-09-03 | -0.90 | $9,264.99 | — | $9,264.99 | -0.00 | +0.00 | — | — | $9,264.99 | $9,264.99 | — |
| 2026-09-04 | +2.25 | $9,264.99 | — | $9,264.99 | -0.00 | +0.00 | — | — | $9,264.99 | $9,264.99 | — |
| 2026-09-08 | -11.47 | $9,264.99 | — | $9,264.99 | -0.00 | +0.00 | — | — | $9,264.99 | $9,264.99 | — |
| 2026-09-09 | -13.95 | $9,264.99 | — | $9,264.99 | -0.00 | +0.00 | — | — | $9,264.99 | $9,264.99 | — |
| 2026-09-10 | -13.28 | $9,264.99 | — | $9,264.99 | -0.00 | +0.00 | — | — | $9,264.99 | $9,264.99 | — |
| 2026-09-11 | +0.50 | $9,264.99 | — | $9,264.99 | -0.00 | +0.00 | — | — | $9,264.99 | $9,264.99 | — |
| 2026-09-14 | -11.00 | $9,264.99 | — | $9,264.99 | -0.00 | +0.00 | — | — | $9,264.99 | $9,264.99 | — |
| 2026-09-15 | -3.84 | $9,264.99 | — | $9,264.99 | -0.00 | +0.00 | — | — | $9,264.99 | $9,264.99 | — |
| 2026-09-16 | +5.30 | $9,264.99 | — | $9,264.99 | -0.00 | +0.00 | — | — | $9,264.99 | $9,264.99 | — |
| 2026-09-17 | +7.38 | $9,264.99 | — | $9,264.99 | -0.00 | +0.00 | — | — | $9,264.99 | $9,264.99 | — |
| 2026-09-18 | +4.86 | $9,264.99 | — | $9,264.99 | -0.00 | +0.00 | — | — | $9,264.99 | $9,264.99 | — |
| 2026-09-21 | +12.87 | $9,264.99 | — | $9,264.99 | -0.00 | +0.00 | — | — | $9,264.99 | $9,264.99 | — |
| 2026-09-22 | -0.50 | $9,264.99 | — | $9,264.99 | -0.00 | +0.00 | — | — | $9,264.99 | $9,264.99 | — |
| 2026-09-23 | +2.29 | $9,264.99 | — | $9,264.99 | -0.00 | +0.00 | — | — | $9,264.99 | $9,264.99 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `ROST` | 43 | $229.55 | $2.12 | — | $127.23 | — | same mega calendar; hold 2 also keeps the session after the print; list overnight,overnight_mega; 🔵; ret5=-5.5; leftover $10000.00 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.23 | ▼ close $9,973.80 vs 09:30 $10,000.00 (session -24.08) | 16:00 close · cash $127.23 · equity $9,973.80 vs 09:30 $10,000.00 (-26.20; session marks -24.08) · 1 name(s) marked open→close (per-name table). ROST×43 09:30 $229.55 → close $228.99 -24.08 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.23 | ▲ 09:30 equity $10,612.78 vs yday $9,973.80 (+638.98) | 09:30 open · cash $127.23 (unchanged overnight, no fees) · equity $10,612.78 vs prior close $9,973.80 (+638.98) · 1 name(s) re-marked at the open (per-name table). ROST×43 yday $228.99 → 09:30 $243.85 +638.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `PDD` | 1 | $90.03 | $0.90 | — | $36.30 | — | same mega calendar; hold 2 also keeps the session after the print; list overnight,overnight_mega; 🔵; ret5=+6.4; leftover $127.23 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.30 | ▼ close $10,403.40 vs 09:30 $10,612.78 (session -208.48) | 16:00 close · cash $36.30 · equity $10,403.40 vs 09:30 $10,612.78 (-209.38; session marks -208.48) · 2 name(s) marked open→close (per-name table). ROST×43 09:30 $243.85 → close $239.04 -206.83; PDD×1 09:30 $90.03 → close $88.38 -1.65 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.30 | ▼ 09:30 equity $10,364.69 vs yday $10,403.40 (-38.71) | 09:30 open · cash $36.30 (unchanged overnight, no fees) · equity $10,364.69 vs prior close $10,403.40 (-38.71) · 2 name(s) re-marked at the open (per-name table). ROST×43 yday $239.04 → 09:30 $238.08 -41.28; PDD×1 yday $88.38 → 09:30 $90.95 +2.57 | — |
| 2026-08-24 09:30 ET | **SELL** | `ROST` | 43 | $238.08 | $2.21 | $+362.46 | $10,271.53 | ▲ +362.46 after sell → book $10,362.48; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,271.53 | ▼ close $10,358.60 vs 09:30 $10,364.69 (session -3.88) | 16:00 close · cash $10,271.53 · equity $10,358.60 vs 09:30 $10,364.69 (-6.09; session marks -3.88) · 1 name(s) marked open→close (per-name table). PDD×1 09:30 $90.95 → close $87.07 -3.88 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,271.53 | ▼ 09:30 equity $10,358.18 vs yday $10,358.60 (-0.42) | 09:30 open · cash $10,271.53 (unchanged overnight, no fees) · equity $10,358.18 vs prior close $10,358.60 (-0.42) · 1 name(s) re-marked at the open (per-name table). PDD×1 yday $87.07 → 09:30 $86.65 -0.42 | — |
| 2026-08-25 09:30 ET | **SELL** | `PDD` | 1 | $86.65 | $0.89 | $-5.17 | $10,357.29 | ▼ -5.17 after sell → book $10,357.29; vs 09:30 mark -0.89 | dropped from list after 2 sess (min 2) | — |
| 2026-08-25 09:30 ET | **BUY** | `INTU` | 28 | $364.35 | $2.07 | — | $153.41 | — | same mega calendar; hold 2 also keeps the session after the print; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $10357.29 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $153.41 | ▼ close $10,162.29 vs 09:30 $10,358.18 (session -192.92) | 16:00 close · cash $153.41 · equity $10,162.29 vs 09:30 $10,358.18 (-195.89; session marks -192.92) · 1 name(s) marked open→close (per-name table). INTU×28 09:30 $364.35 → close $357.46 -192.92 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $153.41 | ▼ 09:30 equity $9,210.57 vs yday $10,162.29 (-951.72) | 09:30 open · cash $153.41 (unchanged overnight, no fees) · equity $9,210.57 vs prior close $10,162.29 (-951.72) · 1 name(s) re-marked at the open (per-name table). INTU×28 yday $357.46 → 09:30 $323.47 -951.72 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $153.41 | ▲ close $9,838.05 vs 09:30 $9,210.57 (session +627.48) | 16:00 close · cash $153.41 · equity $9,838.05 vs 09:30 $9,210.57 (+627.48; session marks +627.48) · 1 name(s) marked open→close (per-name table). INTU×28 09:30 $323.47 → close $345.88 +627.48 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $153.41 | ▲ 09:30 equity $10,052.53 vs yday $9,838.05 (+214.48) | 09:30 open · cash $153.41 (unchanged overnight, no fees) · equity $10,052.53 vs prior close $9,838.05 (+214.48) · 1 name(s) re-marked at the open (per-name table). INTU×28 yday $345.88 → 09:30 $353.54 +214.48 | — |
| 2026-08-27 09:30 ET | **SELL** | `INTU` | 28 | $353.54 | $2.16 | $-306.92 | $10,050.37 | ▼ -306.92 after sell → book $10,050.37; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 2) | join🔴 sector🟢 gen🟢 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 19 | $261.47 | $2.05 | — | $5,080.39 | — | same mega calendar; hold 2 also keeps the session after the print; list overnight,overnight_mega; 🔵; ret5=+1.4; leftover $5025.19 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 19 | $253.44 | $2.05 | — | $262.99 | — | same mega calendar; hold 2 also keeps the session after the print; list overnight,overnight_mega,mover_buy; 🔵; ret5=+3.3; leftover $5025.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $262.99 | ▼ close $9,991.56 vs 09:30 $10,052.53 (session -54.72) | 16:00 close · cash $262.99 · equity $9,991.56 vs 09:30 $10,052.53 (-60.97; session marks -54.72) · 2 name(s) marked open→close (per-name table). ADSK×19 09:30 $261.47 → close $270.58 +173.09; MRVL×19 09:30 $253.44 → close $241.45 -227.81 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $262.99 | ▼ 09:30 equity $9,504.97 vs yday $9,991.56 (-486.59) | 09:30 open · cash $262.99 (unchanged overnight, no fees) · equity $9,504.97 vs prior close $9,991.56 (-486.59) · 2 name(s) re-marked at the open (per-name table). ADSK×19 yday $270.58 → 09:30 $261.16 -178.98; MRVL×19 yday $241.45 → 09:30 $225.26 -307.61 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $262.99 | ▼ close $9,331.31 vs 09:30 $9,504.97 (session -173.66) | 16:00 close · cash $262.99 · equity $9,331.31 vs 09:30 $9,504.97 (-173.66; session marks -173.66) · 2 name(s) marked open→close (per-name table). ADSK×19 09:30 $261.16 → close $260.66 -9.50; MRVL×19 09:30 $225.26 → close $216.62 -164.16 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $262.99 | ▼ 09:30 equity $9,269.18 vs yday $9,331.31 (-62.13) | 09:30 open · cash $262.99 (unchanged overnight, no fees) · equity $9,269.18 vs prior close $9,331.31 (-62.13) · 2 name(s) re-marked at the open (per-name table). ADSK×19 yday $260.66 → 09:30 $257.71 -56.05; MRVL×19 yday $216.62 → 09:30 $216.30 -6.08 | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 19 | $257.71 | $2.10 | $-75.58 | $5,157.38 | ▼ -75.58 after sell → book $9,267.08; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 2) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRVL` | 19 | $216.30 | $2.09 | $-709.80 | $9,264.99 | ▼ -709.80 after sell → book $9,264.99; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 2) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,264.99 | ▲ close $9,264.99 vs 09:30 $9,269.18 (session +0.00) | 16:00 close · cash $9,264.99 · no lots left · equity $9,264.99. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,264.99 | ▲ 09:30 equity $9,264.99 vs yday $9,264.99 (-0.00) | 09:30 open · cash $9,264.99 · no holdings · equity $9,264.99 vs prior close $9,264.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,264.99 | ▲ close $9,264.99 vs 09:30 $9,264.99 (session +0.00) | 16:00 close · cash $9,264.99 · no lots left · equity $9,264.99. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,264.99 | ▲ 09:30 equity $9,264.99 vs yday $9,264.99 (-0.00) | 09:30 open · cash $9,264.99 · no holdings · equity $9,264.99 vs prior close $9,264.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,264.99 | ▲ close $9,264.99 vs 09:30 $9,264.99 (session +0.00) | 16:00 close · cash $9,264.99 · no lots left · equity $9,264.99. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,264.99 | ▲ 09:30 equity $9,264.99 vs yday $9,264.99 (-0.00) | 09:30 open · cash $9,264.99 · no holdings · equity $9,264.99 vs prior close $9,264.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,264.99 | ▲ close $9,264.99 vs 09:30 $9,264.99 (session +0.00) | 16:00 close · cash $9,264.99 · no lots left · equity $9,264.99. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,264.99 | ▲ 09:30 equity $9,264.99 vs yday $9,264.99 (-0.00) | 09:30 open · cash $9,264.99 · no holdings · equity $9,264.99 vs prior close $9,264.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,264.99 | ▲ close $9,264.99 vs 09:30 $9,264.99 (session +0.00) | 16:00 close · cash $9,264.99 · no lots left · equity $9,264.99. | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,264.99 | ▲ 09:30 equity $9,264.99 vs yday $9,264.99 (-0.00) | 09:30 open · cash $9,264.99 · no holdings · equity $9,264.99 vs prior close $9,264.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,264.99 | ▲ close $9,264.99 vs 09:30 $9,264.99 (session +0.00) | 16:00 close · cash $9,264.99 · no lots left · equity $9,264.99. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,264.99 | ▲ 09:30 equity $9,264.99 vs yday $9,264.99 (-0.00) | 09:30 open · cash $9,264.99 · no holdings · equity $9,264.99 vs prior close $9,264.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,264.99 | ▲ close $9,264.99 vs 09:30 $9,264.99 (session +0.00) | 16:00 close · cash $9,264.99 · no lots left · equity $9,264.99. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,264.99 | ▲ 09:30 equity $9,264.99 vs yday $9,264.99 (-0.00) | 09:30 open · cash $9,264.99 · no holdings · equity $9,264.99 vs prior close $9,264.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,264.99 | ▲ close $9,264.99 vs 09:30 $9,264.99 (session +0.00) | 16:00 close · cash $9,264.99 · no lots left · equity $9,264.99. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,264.99 | ▲ 09:30 equity $9,264.99 vs yday $9,264.99 (-0.00) | 09:30 open · cash $9,264.99 · no holdings · equity $9,264.99 vs prior close $9,264.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,264.99 | ▲ close $9,264.99 vs 09:30 $9,264.99 (session +0.00) | 16:00 close · cash $9,264.99 · no lots left · equity $9,264.99. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,264.99 | ▲ 09:30 equity $9,264.99 vs yday $9,264.99 (-0.00) | 09:30 open · cash $9,264.99 · no holdings · equity $9,264.99 vs prior close $9,264.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,264.99 | ▲ close $9,264.99 vs 09:30 $9,264.99 (session +0.00) | 16:00 close · cash $9,264.99 · no lots left · equity $9,264.99. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,264.99 | ▲ 09:30 equity $9,264.99 vs yday $9,264.99 (-0.00) | 09:30 open · cash $9,264.99 · no holdings · equity $9,264.99 vs prior close $9,264.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,264.99 | ▲ close $9,264.99 vs 09:30 $9,264.99 (session +0.00) | 16:00 close · cash $9,264.99 · no lots left · equity $9,264.99. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,264.99 | ▲ 09:30 equity $9,264.99 vs yday $9,264.99 (-0.00) | 09:30 open · cash $9,264.99 · no holdings · equity $9,264.99 vs prior close $9,264.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,264.99 | ▲ close $9,264.99 vs 09:30 $9,264.99 (session +0.00) | 16:00 close · cash $9,264.99 · no lots left · equity $9,264.99. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,264.99 | ▲ 09:30 equity $9,264.99 vs yday $9,264.99 (-0.00) | 09:30 open · cash $9,264.99 · no holdings · equity $9,264.99 vs prior close $9,264.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,264.99 | ▲ close $9,264.99 vs 09:30 $9,264.99 (session +0.00) | 16:00 close · cash $9,264.99 · no lots left · equity $9,264.99. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,264.99 | ▲ 09:30 equity $9,264.99 vs yday $9,264.99 (-0.00) | 09:30 open · cash $9,264.99 · no holdings · equity $9,264.99 vs prior close $9,264.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,264.99 | ▲ close $9,264.99 vs 09:30 $9,264.99 (session +0.00) | 16:00 close · cash $9,264.99 · no lots left · equity $9,264.99. | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,264.99 | ▲ 09:30 equity $9,264.99 vs yday $9,264.99 (-0.00) | 09:30 open · cash $9,264.99 · no holdings · equity $9,264.99 vs prior close $9,264.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,264.99 | ▲ close $9,264.99 vs 09:30 $9,264.99 (session +0.00) | 16:00 close · cash $9,264.99 · no lots left · equity $9,264.99. | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,264.99 | ▲ 09:30 equity $9,264.99 vs yday $9,264.99 (-0.00) | 09:30 open · cash $9,264.99 · no holdings · equity $9,264.99 vs prior close $9,264.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,264.99 | ▲ close $9,264.99 vs 09:30 $9,264.99 (session +0.00) | 16:00 close · cash $9,264.99 · no lots left · equity $9,264.99. | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,264.99 | ▲ 09:30 equity $9,264.99 vs yday $9,264.99 (-0.00) | 09:30 open · cash $9,264.99 · no holdings · equity $9,264.99 vs prior close $9,264.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,264.99 | ▲ close $9,264.99 vs 09:30 $9,264.99 (session +0.00) | 16:00 close · cash $9,264.99 · no lots left · equity $9,264.99. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `LOW` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TGT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TJX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BABA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NTES` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WMT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `ROST` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-24 | `PDD` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-24 | `BNS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `INTU` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `CM` | cash | leftover split 21.92 < 1 share @ 118.50 |
| 2026-08-26 | `CRM` | cash | leftover split 21.92 < 1 share @ 199.94 |
| 2026-08-26 | `CRWD` | cash | leftover split 21.92 < 1 share @ 182.75 |
| 2026-08-26 | `NVDA` | cash | leftover split 21.92 < 1 share @ 212.64 |
| 2026-08-26 | `RY` | cash | leftover split 21.92 < 1 share @ 206.95 |
| 2026-08-26 | `SNPS` | cash | leftover split 21.92 < 1 share @ 405.10 |
| 2026-08-26 | `TD` | cash | leftover split 21.92 < 1 share @ 119.11 |
| 2026-08-28 | `ADSK` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-28 | `MRVL` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-31 | `MDT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DELL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PANW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SNOW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
