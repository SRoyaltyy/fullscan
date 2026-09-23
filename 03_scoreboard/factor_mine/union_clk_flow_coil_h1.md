# Factor mine action — `union_clk_flow_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · Clock-B #8 flow-in + green + not extended

Cash book **-10.38%** ($8,962) · signal-only (no cash/fees) was -18.05%. Starts YES **0/29**. Fills 28 · skips 7 · realized $-1038.24.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: Clock-B #8: prior flow-in, last bar green, not already extended.
- Must-not: Clock-B #5 long veto: extreme prior extension plus diminishing progress or a failed breakout.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 8.
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
- **Gate** `clk_flow_coil=True` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,961.75.

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
| 2026-08-20 | `FUTU` | 84 | — | $117.65 | +0.00 | $112.73 | -413.28 | -413.28 | +0.00 | -413.28 |
| 2026-08-21 | `FUTU` | 84 | $112.73 | $115.18 | +205.80 | — | +0.00 | +205.80 | -207.48 | — |
| 2026-08-24 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-25 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-26 | `SJM` | 24 | — | $134.80 | +0.00 | $130.90 | -93.60 | -93.60 | +0.00 | -93.60 |
| 2026-08-26 | `URBN` | 41 | — | $78.90 | +0.00 | $82.95 | +166.05 | +166.05 | +0.00 | +166.05 |
| 2026-08-26 | `NCNO` | 168 | — | $19.33 | +0.00 | $21.51 | +366.24 | +366.24 | +0.00 | +366.24 |
| 2026-08-27 | `SJM` | 24 | $130.90 | $130.29 | -14.64 | — | +0.00 | -14.64 | -108.24 | — |
| 2026-08-27 | `URBN` | 41 | $82.95 | $82.70 | -10.25 | — | +0.00 | -10.25 | +155.80 | — |
| 2026-08-27 | `NCNO` | 168 | $21.51 | $22.03 | +87.36 | — | +0.00 | +87.36 | +453.60 | — |
| 2026-08-28 | `ULTA` | 18 | — | $542.00 | +0.00 | $517.50 | -441.00 | -441.00 | +0.00 | -441.00 |
| 2026-08-31 | `ULTA` | 18 | $517.50 | $521.10 | +64.80 | — | +0.00 | +64.80 | -376.20 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `MOMO` | 899 | — | $5.50 | +0.00 | $5.10 | -359.60 | -359.60 | +0.00 | -359.60 |
| 2026-09-03 | `VSXY` | 64 | — | $76.86 | +0.00 | $73.64 | -206.08 | -206.08 | +0.00 | -206.08 |
| 2026-09-04 | `MOMO` | 899 | $5.10 | $5.13 | +26.97 | — | +0.00 | +26.97 | -332.63 | — |
| 2026-09-04 | `VSXY` | 64 | $73.64 | $73.63 | -0.64 | — | +0.00 | -0.64 | -206.72 | — |
| 2026-09-04 | `HAFN` | 1041 | — | $8.94 | +0.00 | $9.22 | +291.48 | +291.48 | +0.00 | +291.48 |
| 2026-09-08 | `HAFN` | 1041 | $9.22 | $8.81 | -426.81 | — | +0.00 | -426.81 | -135.33 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ARLO` | 692 | — | $13.22 | +0.00 | $13.19 | -20.76 | -20.76 | +0.00 | -20.76 |
| 2026-09-14 | `ARLO` | 692 | $13.19 | $13.07 | -83.04 | — | +0.00 | -83.04 | -103.80 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `ARLO` | 332 | — | $13.62 | +0.00 | $13.40 | -73.04 | -73.04 | +0.00 | -73.04 |
| 2026-09-16 | `LEN` | 56 | — | $80.63 | +0.00 | $78.36 | -127.12 | -127.12 | +0.00 | -127.12 |
| 2026-09-17 | `ARLO` | 332 | $13.40 | $13.62 | +73.04 | $13.35 | -89.64 | -16.60 | +0.00 | -89.64 |
| 2026-09-17 | `LEN` | 56 | $78.36 | $81.00 | +147.84 | — | +0.00 | +147.84 | +20.72 | — |
| 2026-09-18 | `ARLO` | 332 | $13.35 | $13.42 | +23.24 | — | +0.00 | +23.24 | -66.40 | — |
| 2026-09-21 | `A` | 18 | — | $157.87 | +0.00 | $161.94 | +73.26 | +73.26 | +0.00 | +73.26 |
| 2026-09-21 | `ARLO` | 223 | — | $13.38 | +0.00 | $13.33 | -11.15 | -11.15 | +0.00 | -11.15 |
| 2026-09-21 | `HUM` | 7 | — | $386.20 | +0.00 | $378.58 | -53.34 | -53.34 | +0.00 | -53.34 |
| 2026-09-22 | `A` | 18 | $161.94 | $160.93 | -18.18 | — | +0.00 | -18.18 | +55.08 | — |
| 2026-09-22 | `ARLO` | 223 | $13.33 | $13.33 | +0.00 | — | +0.00 | +0.00 | -11.15 | — |
| 2026-09-22 | `HUM` | 7 | $378.58 | $378.58 | +0.00 | — | +0.00 | +0.00 | -53.34 | — |
| 2026-09-23 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | -413.28 | FUTU | — | $115.16 | $9,584.48 | FUTU×84 |
| 2026-08-21 | +3.25 | $115.16 | FUTU×84 | $9,790.28 | +205.80 | +0.00 | — | FUTU | $9,787.94 | $9,787.94 | — |
| 2026-08-24 | -5.17 | $9,787.94 | — | $9,787.94 | +0.00 | +0.00 | — | — | $9,787.94 | $9,787.94 | — |
| 2026-08-25 | +1.80 | $9,787.94 | — | $9,787.94 | +0.00 | +0.00 | — | — | $9,787.94 | $9,787.94 | — |
| 2026-08-26 | +2.02 | $9,787.94 | — | $9,787.94 | +0.00 | +438.69 | SJM, URBN, NCNO | — | $63.74 | $10,219.97 | SJM×24, URBN×41, NCNO×168 |
| 2026-08-27 | — | $63.74 | SJM×24, URBN×41, NCNO×168 | $10,282.44 | +62.47 | +0.00 | — | SJM, URBN, NCNO | $10,275.64 | $10,275.64 | — |
| 2026-08-28 | +0.75 | $10,275.64 | — | $10,275.64 | -0.00 | -441.00 | ULTA | — | $517.59 | $9,832.59 | ULTA×18 |
| 2026-08-31 | -5.85 | $517.59 | ULTA×18 | $9,897.39 | +64.80 | +0.00 | — | ULTA | $9,895.26 | $9,895.26 | — |
| 2026-09-01 | -6.30 | $9,895.26 | — | $9,895.26 | +0.00 | +0.00 | — | — | $9,895.26 | $9,895.26 | — |
| 2026-09-02 | -3.83 | $9,895.26 | — | $9,895.26 | +0.00 | +0.00 | — | — | $9,895.26 | $9,895.26 | — |
| 2026-09-03 | -0.90 | $9,895.26 | — | $9,895.26 | +0.00 | -565.68 | MOMO, VSXY | — | $17.95 | $9,315.81 | MOMO×899, VSXY×64 |
| 2026-09-04 | +2.25 | $17.95 | MOMO×899, VSXY×64 | $9,342.13 | +26.32 | +291.48 | HAFN | MOMO, VSXY | $8.15 | $9,606.17 | HAFN×1041 |
| 2026-09-08 | -11.47 | $8.15 | HAFN×1041 | $9,179.36 | -426.81 | +0.00 | — | HAFN | $9,165.69 | $9,165.69 | — |
| 2026-09-09 | -13.95 | $9,165.69 | — | $9,165.69 | -0.00 | +0.00 | — | — | $9,165.69 | $9,165.69 | — |
| 2026-09-10 | -13.28 | $9,165.69 | — | $9,165.69 | -0.00 | +0.00 | — | — | $9,165.69 | $9,165.69 | — |
| 2026-09-11 | +0.50 | $9,165.69 | — | $9,165.69 | -0.00 | -20.76 | ARLO | — | $8.52 | $9,136.00 | ARLO×692 |
| 2026-09-14 | -11.00 | $8.52 | ARLO×692 | $9,052.96 | -83.04 | +0.00 | — | ARLO | $9,043.85 | $9,043.85 | — |
| 2026-09-15 | -3.84 | $9,043.85 | — | $9,043.85 | -0.00 | +0.00 | — | — | $9,043.85 | $9,043.85 | — |
| 2026-09-16 | +5.30 | $9,043.85 | — | $9,043.85 | -0.00 | -200.16 | ARLO, LEN | — | $0.29 | $8,837.25 | ARLO×332, LEN×56 |
| 2026-09-17 | +7.38 | $0.29 | ARLO×332, LEN×56 | $9,058.13 | +220.88 | -89.64 | — | LEN | $4,534.08 | $8,966.28 | ARLO×332 |
| 2026-09-18 | +4.86 | $4,534.08 | ARLO×332 | $8,989.52 | +23.24 | +0.00 | — | ARLO | $8,985.15 | $8,985.15 | — |
| 2026-09-21 | +12.87 | $8,985.15 | — | $8,985.15 | -0.00 | +8.77 | A, ARLO, HUM | — | $449.42 | $8,986.99 | A×18, ARLO×223, HUM×7 |
| 2026-09-22 | -0.50 | $449.42 | A×18, ARLO×223, HUM×7 | $8,968.81 | -18.18 | +0.00 | — | A, ARLO, HUM | $8,961.75 | $8,961.75 | — |
| 2026-09-23 | +2.29 | $8,961.75 | — | $8,961.75 | -0.00 | +0.00 | — | — | $8,961.75 | $8,961.75 | — |

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
| 2026-08-20 09:30 ET | **BUY** | `FUTU` | 84 | $117.65 | $2.24 | — | $115.16 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list earn_react; 🔵; ret5=+4.1; leftover $10000.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $115.16 | ▼ close $9,584.48 vs 09:30 $10,000.00 (session -413.28) | 16:00 close · cash $115.16 · equity $9,584.48 vs 09:30 $10,000.00 (-415.52; session marks -413.28) · 1 name(s) marked open→close (per-name table). FUTU×84 09:30 $117.65 → close $112.73 -413.28 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $115.16 | ▲ 09:30 equity $9,790.28 vs yday $9,584.48 (+205.80) | 09:30 open · cash $115.16 (unchanged overnight, no fees) · equity $9,790.28 vs prior close $9,584.48 (+205.80) · 1 name(s) re-marked at the open (per-name table). FUTU×84 yday $112.73 → 09:30 $115.18 +205.80 | — |
| 2026-08-21 09:30 ET | **SELL** | `FUTU` | 84 | $115.18 | $2.33 | $-212.06 | $9,787.94 | ▼ -212.06 after sell → book $9,787.94; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,787.94 | ▲ close $9,787.94 vs 09:30 $9,790.28 (session +0.00) | 16:00 close · cash $9,787.94 · no lots left · equity $9,787.94. | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,787.94 | ▲ 09:30 equity $9,787.94 vs yday $9,787.94 (+0.00) | 09:30 open · cash $9,787.94 · no holdings · equity $9,787.94 vs prior close $9,787.94 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,787.94 | ▲ close $9,787.94 vs 09:30 $9,787.94 (session +0.00) | 16:00 close · cash $9,787.94 · no lots left · equity $9,787.94. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,787.94 | ▲ 09:30 equity $9,787.94 vs yday $9,787.94 (+0.00) | 09:30 open · cash $9,787.94 · no holdings · equity $9,787.94 vs prior close $9,787.94 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,787.94 | ▲ close $9,787.94 vs 09:30 $9,787.94 (session +0.00) | 16:00 close · cash $9,787.94 · no lots left · equity $9,787.94. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,787.94 | ▲ 09:30 equity $9,787.94 vs yday $9,787.94 (+0.00) | 09:30 open · cash $9,787.94 · no holdings · equity $9,787.94 vs prior close $9,787.94 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 09:30 ET | **BUY** | `SJM` | 24 | $134.80 | $2.06 | — | $6,550.68 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list earn_react; 🔵; ret5=+5.9; leftover $3262.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `URBN` | 41 | $78.90 | $2.11 | — | $3,313.67 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list overnight; 🔵; ret5=+1.6; leftover $3262.65 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NCNO` | 168 | $19.33 | $2.49 | — | $63.74 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list earn_react; ret5=+3.0; leftover $3262.65 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.74 | ▲ close $10,219.97 vs 09:30 $9,787.94 (session +438.69) | 16:00 close · cash $63.74 · equity $10,219.97 vs 09:30 $9,787.94 (+432.03; session marks +438.69) · 3 name(s) marked open→close (per-name table). SJM×24 09:30 $134.80 → close $130.90 -93.60; URBN×41 09:30 $78.90 → close $82.95 +166.05; NCNO×168 09:30 $19.33 → close $21.51 +366.24 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.74 | ▲ 09:30 equity $10,282.44 vs yday $10,219.97 (+62.47) | 09:30 open · cash $63.74 (unchanged overnight, no fees) · equity $10,282.44 vs prior close $10,219.97 (+62.47) · 3 name(s) re-marked at the open (per-name table). SJM×24 yday $130.90 → 09:30 $130.29 -14.64; URBN×41 yday $82.95 → 09:30 $82.70 -10.25; NCNO×168 yday $21.51 → 09:30 $22.03 +87.36 | — |
| 2026-08-27 09:30 ET | **SELL** | `SJM` | 24 | $130.29 | $2.10 | $-112.40 | $3,188.60 | ▼ -112.40 after sell → book $10,280.34; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `URBN` | 41 | $82.70 | $2.15 | $+151.54 | $6,577.15 | ▲ +151.54 after sell → book $10,278.19; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NCNO` | 168 | $22.03 | $2.55 | $+448.55 | $10,275.64 | ▲ +448.55 after sell → book $10,275.64; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,275.64 | ▲ close $10,275.64 vs 09:30 $10,282.44 (session +0.00) | 16:00 close · cash $10,275.64 · no lots left · equity $10,275.64. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,275.64 | ▲ 09:30 equity $10,275.64 vs yday $10,275.64 (-0.00) | 09:30 open · cash $10,275.64 · no holdings · equity $10,275.64 vs prior close $10,275.64 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 18 | $542.00 | $2.04 | — | $517.59 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list earn_react; ret5=+4.8; leftover $10275.64 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $517.59 | ▼ close $9,832.59 vs 09:30 $10,275.64 (session -441.00) | 16:00 close · cash $517.59 · equity $9,832.59 vs 09:30 $10,275.64 (-443.05; session marks -441.00) · 1 name(s) marked open→close (per-name table). ULTA×18 09:30 $542.00 → close $517.50 -441.00 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $517.59 | ▲ 09:30 equity $9,897.39 vs yday $9,832.59 (+64.80) | 09:30 open · cash $517.59 (unchanged overnight, no fees) · equity $9,897.39 vs prior close $9,832.59 (+64.80) · 1 name(s) re-marked at the open (per-name table). ULTA×18 yday $517.50 → 09:30 $521.10 +64.80 | — |
| 2026-08-31 09:30 ET | **SELL** | `ULTA` | 18 | $521.10 | $2.13 | $-380.37 | $9,895.26 | ▼ -380.37 after sell → book $9,895.26; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,895.26 | ▲ close $9,895.26 vs 09:30 $9,897.39 (session +0.00) | 16:00 close · cash $9,895.26 · no lots left · equity $9,895.26. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,895.26 | ▲ 09:30 equity $9,895.26 vs yday $9,895.26 (+0.00) | 09:30 open · cash $9,895.26 · no holdings · equity $9,895.26 vs prior close $9,895.26 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,895.26 | ▲ close $9,895.26 vs 09:30 $9,895.26 (session +0.00) | 16:00 close · cash $9,895.26 · no lots left · equity $9,895.26. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,895.26 | ▲ 09:30 equity $9,895.26 vs yday $9,895.26 (+0.00) | 09:30 open · cash $9,895.26 · no holdings · equity $9,895.26 vs prior close $9,895.26 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,895.26 | ▲ close $9,895.26 vs 09:30 $9,895.26 (session +0.00) | 16:00 close · cash $9,895.26 · no lots left · equity $9,895.26. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,895.26 | ▲ 09:30 equity $9,895.26 vs yday $9,895.26 (+0.00) | 09:30 open · cash $9,895.26 · no holdings · equity $9,895.26 vs prior close $9,895.26 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MOMO` | 899 | $5.50 | $11.60 | — | $4,939.17 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list earn_react; 🔵; ret5=-4.8; leftover $4947.63 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 64 | $76.86 | $2.18 | — | $17.95 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list earn_react; 🔵; ret5=-6.6; leftover $4947.63 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.95 | ▼ close $9,315.81 vs 09:30 $9,895.26 (session -565.68) | 16:00 close · cash $17.95 · equity $9,315.81 vs 09:30 $9,895.26 (-579.45; session marks -565.68) · 2 name(s) marked open→close (per-name table). MOMO×899 09:30 $5.50 → close $5.10 -359.60; VSXY×64 09:30 $76.86 → close $73.64 -206.08 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.95 | ▲ 09:30 equity $9,342.13 vs yday $9,315.81 (+26.32) | 09:30 open · cash $17.95 (unchanged overnight, no fees) · equity $9,342.13 vs prior close $9,315.81 (+26.32) · 2 name(s) re-marked at the open (per-name table). MOMO×899 yday $5.10 → 09:30 $5.13 +26.97; VSXY×64 yday $73.64 → 09:30 $73.63 -0.64 | — |
| 2026-09-04 09:30 ET | **SELL** | `MOMO` | 899 | $5.13 | $11.78 | $-356.01 | $4,618.03 | ▼ -356.01 after sell → book $9,330.35; vs 09:30 mark -11.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSXY` | 64 | $73.63 | $2.23 | $-211.13 | $9,328.12 | ▼ -211.13 after sell → book $9,328.12; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 1041 | $8.94 | $13.43 | — | $8.15 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list ohlc_hot; ret5=+7.7; leftover $9328.12 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.15 | ▲ close $9,606.17 vs 09:30 $9,342.13 (session +291.48) | 16:00 close · cash $8.15 · equity $9,606.17 vs 09:30 $9,342.13 (+264.04; session marks +291.48) · 1 name(s) marked open→close (per-name table). HAFN×1041 09:30 $8.94 → close $9.22 +291.48 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.15 | ▼ 09:30 equity $9,179.36 vs yday $9,606.17 (-426.81) | 09:30 open · cash $8.15 (unchanged overnight, no fees) · equity $9,179.36 vs prior close $9,606.17 (-426.81) · 1 name(s) re-marked at the open (per-name table). HAFN×1041 yday $9.22 → 09:30 $8.81 -426.81 | — |
| 2026-09-08 09:30 ET | **SELL** | `HAFN` | 1041 | $8.81 | $13.68 | $-162.43 | $9,165.69 | ▼ -162.43 after sell → book $9,165.69; vs 09:30 mark -13.67 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,165.69 | ▲ close $9,165.69 vs 09:30 $9,179.36 (session +0.00) | 16:00 close · cash $9,165.69 · no lots left · equity $9,165.69. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,165.69 | ▲ 09:30 equity $9,165.69 vs yday $9,165.69 (-0.00) | 09:30 open · cash $9,165.69 · no holdings · equity $9,165.69 vs prior close $9,165.69 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,165.69 | ▲ close $9,165.69 vs 09:30 $9,165.69 (session +0.00) | 16:00 close · cash $9,165.69 · no lots left · equity $9,165.69. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,165.69 | ▲ 09:30 equity $9,165.69 vs yday $9,165.69 (-0.00) | 09:30 open · cash $9,165.69 · no holdings · equity $9,165.69 vs prior close $9,165.69 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,165.69 | ▲ close $9,165.69 vs 09:30 $9,165.69 (session +0.00) | 16:00 close · cash $9,165.69 · no lots left · equity $9,165.69. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,165.69 | ▲ 09:30 equity $9,165.69 vs yday $9,165.69 (-0.00) | 09:30 open · cash $9,165.69 · no holdings · equity $9,165.69 vs prior close $9,165.69 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ARLO` | 692 | $13.22 | $8.93 | — | $8.52 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list ohlc_hot; ret5=+7.3; leftover $9165.69 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.52 | ▼ close $9,136.00 vs 09:30 $9,165.69 (session -20.76) | 16:00 close · cash $8.52 · equity $9,136.00 vs 09:30 $9,165.69 (-29.69; session marks -20.76) · 1 name(s) marked open→close (per-name table). ARLO×692 09:30 $13.22 → close $13.19 -20.76 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.52 | ▼ 09:30 equity $9,052.96 vs yday $9,136.00 (-83.04) | 09:30 open · cash $8.52 (unchanged overnight, no fees) · equity $9,052.96 vs prior close $9,136.00 (-83.04) · 1 name(s) re-marked at the open (per-name table). ARLO×692 yday $13.19 → 09:30 $13.07 -83.04 | — |
| 2026-09-14 09:30 ET | **SELL** | `ARLO` | 692 | $13.07 | $9.11 | $-121.84 | $9,043.85 | ▼ -121.84 after sell → book $9,043.85; vs 09:30 mark -9.11 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,043.85 | ▲ close $9,043.85 vs 09:30 $9,052.96 (session +0.00) | 16:00 close · cash $9,043.85 · no lots left · equity $9,043.85. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,043.85 | ▲ 09:30 equity $9,043.85 vs yday $9,043.85 (-0.00) | 09:30 open · cash $9,043.85 · no holdings · equity $9,043.85 vs prior close $9,043.85 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,043.85 | ▲ close $9,043.85 vs 09:30 $9,043.85 (session +0.00) | 16:00 close · cash $9,043.85 · no lots left · equity $9,043.85. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,043.85 | ▲ 09:30 equity $9,043.85 vs yday $9,043.85 (-0.00) | 09:30 open · cash $9,043.85 · no holdings · equity $9,043.85 vs prior close $9,043.85 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `ARLO` | 332 | $13.62 | $4.28 | — | $4,517.72 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list ohlc_hot; ret5=+7.3; leftover $4521.92 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `LEN` | 56 | $80.63 | $2.16 | — | $0.29 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list overnight; ret5=-0.4; leftover $4521.92 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🔴 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.29 | ▼ close $8,837.25 vs 09:30 $9,043.85 (session -200.16) | 16:00 close · cash $0.29 · equity $8,837.25 vs 09:30 $9,043.85 (-206.60; session marks -200.16) · 2 name(s) marked open→close (per-name table). ARLO×332 09:30 $13.62 → close $13.40 -73.04; LEN×56 09:30 $80.63 → close $78.36 -127.12 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.29 | ▲ 09:30 equity $9,058.13 vs yday $8,837.25 (+220.88) | 09:30 open · cash $0.29 (unchanged overnight, no fees) · equity $9,058.13 vs prior close $8,837.25 (+220.88) · 2 name(s) re-marked at the open (per-name table). ARLO×332 yday $13.40 → 09:30 $13.62 +73.04; LEN×56 yday $78.36 → 09:30 $81.00 +147.84 | — |
| 2026-09-17 09:30 ET | **SELL** | `LEN` | 56 | $81.00 | $2.20 | $+16.36 | $4,534.08 | ▲ +16.36 after sell → book $9,055.92; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,534.08 | ▼ close $8,966.28 vs 09:30 $9,058.13 (session -89.64) | 16:00 close · cash $4,534.08 · equity $8,966.28 vs 09:30 $9,058.13 (-91.85; session marks -89.64) · 1 name(s) marked open→close (per-name table). ARLO×332 09:30 $13.62 → close $13.35 -89.64 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,534.08 | ▲ 09:30 equity $8,989.52 vs yday $8,966.28 (+23.24) | 09:30 open · cash $4,534.08 (unchanged overnight, no fees) · equity $8,989.52 vs prior close $8,966.28 (+23.24) · 1 name(s) re-marked at the open (per-name table). ARLO×332 yday $13.35 → 09:30 $13.42 +23.24 | — |
| 2026-09-18 09:30 ET | **SELL** | `ARLO` | 332 | $13.42 | $4.37 | $-75.06 | $8,985.15 | ▼ -75.06 after sell → book $8,985.15; vs 09:30 mark -4.37 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,985.15 | ▲ close $8,985.15 vs 09:30 $8,989.52 (session +0.00) | 16:00 close · cash $8,985.15 · no lots left · equity $8,985.15. | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,985.15 | ▲ 09:30 equity $8,985.15 vs yday $8,985.15 (-0.00) | 09:30 open · cash $8,985.15 · no holdings · equity $8,985.15 vs prior close $8,985.15 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 18 | $157.87 | $2.04 | — | $6,141.44 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list flatten; ret5=+6.5; leftover $2995.05 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `ARLO` | 223 | $13.38 | $2.88 | — | $3,154.83 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list ohlc_hot; ret5=+7.3; leftover $2995.05 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 7 | $386.20 | $2.01 | — | $449.42 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list flatten; ret5=-5.8; leftover $2995.05 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $449.42 | ▲ close $8,986.99 vs 09:30 $8,985.15 (session +8.77) | 16:00 close · cash $449.42 · equity $8,986.99 vs 09:30 $8,985.15 (+1.84; session marks +8.77) · 3 name(s) marked open→close (per-name table). A×18 09:30 $157.87 → close $161.94 +73.26; ARLO×223 09:30 $13.38 → close $13.33 -11.15; HUM×7 09:30 $386.20 → close $378.58 -53.34 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $449.42 | ▼ 09:30 equity $8,968.81 vs yday $8,986.99 (-18.18) | 09:30 open · cash $449.42 (unchanged overnight, no fees) · equity $8,968.81 vs prior close $8,986.99 (-18.18) · 3 name(s) re-marked at the open (per-name table). A×18 yday $161.94 → 09:30 $160.93 -18.18; ARLO×223 yday $13.33 → 09:30 $13.33 +0.00; HUM×7 yday $378.58 → 09:30 $378.58 +0.00 | — |
| 2026-09-22 09:30 ET | **SELL** | `A` | 18 | $160.93 | $2.08 | $+50.96 | $3,344.08 | ▲ +50.96 after sell → book $8,966.73; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `ARLO` | 223 | $13.33 | $2.94 | $-16.96 | $6,313.73 | ▼ -16.96 after sell → book $8,963.79; vs 09:30 mark -2.94 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `HUM` | 7 | $378.58 | $2.04 | $-57.39 | $8,961.75 | ▼ -57.39 after sell → book $8,961.75; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,961.75 | ▲ close $8,961.75 vs 09:30 $8,968.81 (session +0.00) | 16:00 close · cash $8,961.75 · no lots left · equity $8,961.75. | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,961.75 | ▲ 09:30 equity $8,961.75 vs yday $8,961.75 (-0.00) | 09:30 open · cash $8,961.75 · no holdings · equity $8,961.75 vs prior close $8,961.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,961.75 | ▲ close $8,961.75 vs 09:30 $8,961.75 (session +0.00) | 16:00 close · cash $8,961.75 · no lots left · equity $8,961.75. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-09-08 | `KFY` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `ARLO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-15 | `ARLO` | hard_red | hard-red S=-3.84 sit; no new buys |
