# Factor mine action — `union_clk_insider_cash_stab_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · Clock-B #7 insider/Form-4 + inst Tx + stabilize (if data)

Cash book **-12.31%** ($8,769) · signal-only (no cash/fees) was -16.14%. Starts YES **0/26**. Fills 4 · skips 8 · realized $-1230.76.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: Clock-B #7: insider / Form-4 buying is present, institutional transactions are not deteriorating, and the tape is stabilizing.
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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `clk_insider_cash_stab=True` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,769.24.

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
| 2026-08-20 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-21 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-24 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-25 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-26 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | `ADSK` | 38 | — | $261.16 | +0.00 | $260.66 | -19.00 | -19.00 | +0.00 | -19.00 |
| 2026-08-31 | `ADSK` | 38 | $260.66 | $257.71 | -112.10 | $258.53 | +31.16 | -80.94 | -131.10 | -99.94 |
| 2026-09-01 | `ADSK` | 38 | $258.53 | $253.48 | -191.90 | $247.69 | -220.02 | -411.92 | -291.84 | -511.86 |
| 2026-09-02 | `ADSK` | 38 | $247.69 | $246.70 | -37.62 | — | +0.00 | -37.62 | -549.48 | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | `SLGN` | 229 | — | $41.16 | +0.00 | $41.18 | +4.58 | +4.58 | +0.00 | +4.58 |
| 2026-09-08 | `SLGN` | 229 | $41.18 | $40.60 | -132.82 | $39.31 | -295.41 | -428.23 | -128.24 | -423.65 |
| 2026-09-09 | `SLGN` | 229 | $39.31 | $39.14 | -38.93 | $38.35 | -180.91 | -219.84 | -462.58 | -643.49 |
| 2026-09-10 | `SLGN` | 229 | $38.35 | $38.23 | -27.48 | — | +0.00 | -27.48 | -670.97 | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-21 | +3.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-24 | -5.17 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-25 | +1.80 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-26 | +2.02 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-27 | — | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-28 | +0.75 | $10,000.00 | — | $10,000.00 | +0.00 | -19.00 | ADSK | — | $73.82 | $9,978.90 | ADSK×38 |
| 2026-08-31 | -5.85 | $73.82 | ADSK×38 | $9,866.80 | -112.10 | +31.16 | — | — | $73.82 | $9,897.96 | ADSK×38 |
| 2026-09-01 | -6.30 | $73.82 | ADSK×38 | $9,706.06 | -191.90 | -220.02 | — | — | $73.82 | $9,486.04 | ADSK×38 |
| 2026-09-02 | -3.83 | $73.82 | ADSK×38 | $9,448.42 | -37.62 | +0.00 | — | ADSK | $9,446.23 | $9,446.23 | — |
| 2026-09-03 | -0.90 | $9,446.23 | — | $9,446.23 | -0.00 | +0.00 | — | — | $9,446.23 | $9,446.23 | — |
| 2026-09-04 | +2.25 | $9,446.23 | — | $9,446.23 | -0.00 | +4.58 | SLGN | — | $17.63 | $9,447.85 | SLGN×229 |
| 2026-09-08 | -11.47 | $17.63 | SLGN×229 | $9,315.03 | -132.82 | -295.41 | — | — | $17.63 | $9,019.62 | SLGN×229 |
| 2026-09-09 | -13.95 | $17.63 | SLGN×229 | $8,980.69 | -38.93 | -180.91 | — | — | $17.63 | $8,799.78 | SLGN×229 |
| 2026-09-10 | -13.28 | $17.63 | SLGN×229 | $8,772.30 | -27.48 | +0.00 | — | SLGN | $8,769.24 | $8,769.24 | — |
| 2026-09-11 | +0.50 | $8,769.24 | — | $8,769.24 | +0.00 | +0.00 | — | — | $8,769.24 | $8,769.24 | — |
| 2026-09-14 | -11.00 | $8,769.24 | — | $8,769.24 | +0.00 | +0.00 | — | — | $8,769.24 | $8,769.24 | — |
| 2026-09-15 | -3.84 | $8,769.24 | — | $8,769.24 | +0.00 | +0.00 | — | — | $8,769.24 | $8,769.24 | — |
| 2026-09-16 | +5.30 | $8,769.24 | — | $8,769.24 | +0.00 | +0.00 | — | — | $8,769.24 | $8,769.24 | — |
| 2026-09-17 | +7.38 | $8,769.24 | — | $8,769.24 | +0.00 | +0.00 | — | — | $8,769.24 | $8,769.24 | — |
| 2026-09-18 | +4.86 | $8,769.24 | — | $8,769.24 | +0.00 | +0.00 | — | — | $8,769.24 | $8,769.24 | — |

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
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 38 | $261.16 | $2.10 | — | $73.82 | — | Clock-B #7 insider/Form-4 + inst Tx + stabilize (if data); gate clk_insider_cash_stab=True; rank cond; list earn_react; ret5=+7.8; leftover $10000.00 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.82 | ▼ close $9,978.90 vs 09:30 $10,000.00 (session -19.00) | 16:00 close · cash $73.82 · equity $9,978.90 vs 09:30 $10,000.00 (-21.10; session marks -19.00) · 1 name(s) marked open→close (per-name table). ADSK×38 09:30 $261.16 → close $260.66 -19.00 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.82 | ▼ 09:30 equity $9,866.80 vs yday $9,978.90 (-112.10) | 09:30 open · cash $73.82 (unchanged overnight, no fees) · equity $9,866.80 vs prior close $9,978.90 (-112.10) · 1 name(s) re-marked at the open (per-name table). ADSK×38 yday $260.66 → 09:30 $257.71 -112.10 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.82 | ▲ close $9,897.96 vs 09:30 $9,866.80 (session +31.16) | 16:00 close · cash $73.82 · equity $9,897.96 vs 09:30 $9,866.80 (+31.16; session marks +31.16) · 1 name(s) marked open→close (per-name table). ADSK×38 09:30 $257.71 → close $258.53 +31.16 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.82 | ▼ 09:30 equity $9,706.06 vs yday $9,897.96 (-191.90) | 09:30 open · cash $73.82 (unchanged overnight, no fees) · equity $9,706.06 vs prior close $9,897.96 (-191.90) · 1 name(s) re-marked at the open (per-name table). ADSK×38 yday $258.53 → 09:30 $253.48 -191.90 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.82 | ▼ close $9,486.04 vs 09:30 $9,706.06 (session -220.02) | 16:00 close · cash $73.82 · equity $9,486.04 vs 09:30 $9,706.06 (-220.02; session marks -220.02) · 1 name(s) marked open→close (per-name table). ADSK×38 09:30 $253.48 → close $247.69 -220.02 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.82 | ▼ 09:30 equity $9,448.42 vs yday $9,486.04 (-37.62) | 09:30 open · cash $73.82 (unchanged overnight, no fees) · equity $9,448.42 vs prior close $9,486.04 (-37.62) · 1 name(s) re-marked at the open (per-name table). ADSK×38 yday $247.69 → 09:30 $246.70 -37.62 | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 38 | $246.70 | $2.19 | $-553.77 | $9,446.23 | ▼ -553.77 after sell → book $9,446.23; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,446.23 | ▲ close $9,446.23 vs 09:30 $9,448.42 (session +0.00) | 16:00 close · cash $9,446.23 · no lots left · equity $9,446.23. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,446.23 | ▲ 09:30 equity $9,446.23 vs yday $9,446.23 (-0.00) | 09:30 open · cash $9,446.23 · no holdings · equity $9,446.23 vs prior close $9,446.23 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,446.23 | ▲ close $9,446.23 vs 09:30 $9,446.23 (session +0.00) | 16:00 close · cash $9,446.23 · no lots left · equity $9,446.23. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,446.23 | ▲ 09:30 equity $9,446.23 vs yday $9,446.23 (-0.00) | 09:30 open · cash $9,446.23 · no holdings · equity $9,446.23 vs prior close $9,446.23 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 09:30 ET | **BUY** | `SLGN` | 229 | $41.16 | $2.95 | — | $17.63 | — | Clock-B #7 insider/Form-4 + inst Tx + stabilize (if data); gate clk_insider_cash_stab=True; rank cond; list oppset; 🔵; ret5=-0.8; leftover $9446.23 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.63 | ▲ close $9,447.85 vs 09:30 $9,446.23 (session +4.58) | 16:00 close · cash $17.63 · equity $9,447.85 vs 09:30 $9,446.23 (+1.62; session marks +4.58) · 1 name(s) marked open→close (per-name table). SLGN×229 09:30 $41.16 → close $41.18 +4.58 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.63 | ▼ 09:30 equity $9,315.03 vs yday $9,447.85 (-132.82) | 09:30 open · cash $17.63 (unchanged overnight, no fees) · equity $9,315.03 vs prior close $9,447.85 (-132.82) · 1 name(s) re-marked at the open (per-name table). SLGN×229 yday $41.18 → 09:30 $40.60 -132.82 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.63 | ▼ close $9,019.62 vs 09:30 $9,315.03 (session -295.41) | 16:00 close · cash $17.63 · equity $9,019.62 vs 09:30 $9,315.03 (-295.41; session marks -295.41) · 1 name(s) marked open→close (per-name table). SLGN×229 09:30 $40.60 → close $39.31 -295.41 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.63 | ▼ 09:30 equity $8,980.69 vs yday $9,019.62 (-38.93) | 09:30 open · cash $17.63 (unchanged overnight, no fees) · equity $8,980.69 vs prior close $9,019.62 (-38.93) · 1 name(s) re-marked at the open (per-name table). SLGN×229 yday $39.31 → 09:30 $39.14 -38.93 | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.63 | ▼ close $8,799.78 vs 09:30 $8,980.69 (session -180.91) | 16:00 close · cash $17.63 · equity $8,799.78 vs 09:30 $8,980.69 (-180.91; session marks -180.91) · 1 name(s) marked open→close (per-name table). SLGN×229 09:30 $39.14 → close $38.35 -180.91 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.63 | ▼ 09:30 equity $8,772.30 vs yday $8,799.78 (-27.48) | 09:30 open · cash $17.63 (unchanged overnight, no fees) · equity $8,772.30 vs prior close $8,799.78 (-27.48) · 1 name(s) re-marked at the open (per-name table). SLGN×229 yday $38.35 → 09:30 $38.23 -27.48 | — |
| 2026-09-10 09:30 ET | **SELL** | `SLGN` | 229 | $38.23 | $3.06 | $-676.99 | $8,769.24 | ▼ -676.99 after sell → book $8,769.24; vs 09:30 mark -3.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,769.24 | ▲ close $8,769.24 vs 09:30 $8,772.30 (session +0.00) | 16:00 close · cash $8,769.24 · no lots left · equity $8,769.24. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,769.24 | ▲ 09:30 equity $8,769.24 vs yday $8,769.24 (+0.00) | 09:30 open · cash $8,769.24 · no holdings · equity $8,769.24 vs prior close $8,769.24 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,769.24 | ▲ close $8,769.24 vs 09:30 $8,769.24 (session +0.00) | 16:00 close · cash $8,769.24 · no lots left · equity $8,769.24. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,769.24 | ▲ 09:30 equity $8,769.24 vs yday $8,769.24 (+0.00) | 09:30 open · cash $8,769.24 · no holdings · equity $8,769.24 vs prior close $8,769.24 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,769.24 | ▲ close $8,769.24 vs 09:30 $8,769.24 (session +0.00) | 16:00 close · cash $8,769.24 · no lots left · equity $8,769.24. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,769.24 | ▲ 09:30 equity $8,769.24 vs yday $8,769.24 (+0.00) | 09:30 open · cash $8,769.24 · no holdings · equity $8,769.24 vs prior close $8,769.24 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,769.24 | ▲ close $8,769.24 vs 09:30 $8,769.24 (session +0.00) | 16:00 close · cash $8,769.24 · no lots left · equity $8,769.24. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,769.24 | ▲ 09:30 equity $8,769.24 vs yday $8,769.24 (+0.00) | 09:30 open · cash $8,769.24 · no holdings · equity $8,769.24 vs prior close $8,769.24 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,769.24 | ▲ close $8,769.24 vs 09:30 $8,769.24 (session +0.00) | 16:00 close · cash $8,769.24 · no lots left · equity $8,769.24. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,769.24 | ▲ 09:30 equity $8,769.24 vs yday $8,769.24 (+0.00) | 09:30 open · cash $8,769.24 · no holdings · equity $8,769.24 vs prior close $8,769.24 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,769.24 | ▲ close $8,769.24 vs 09:30 $8,769.24 (session +0.00) | 16:00 close · cash $8,769.24 · no lots left · equity $8,769.24. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,769.24 | ▲ 09:30 equity $8,769.24 vs yday $8,769.24 (+0.00) | 09:30 open · cash $8,769.24 · no holdings · equity $8,769.24 vs prior close $8,769.24 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,769.24 | ▲ close $8,769.24 vs 09:30 $8,769.24 (session +0.00) | 16:00 close · cash $8,769.24 · no lots left · equity $8,769.24. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SLGN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-09 | `SLGN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-14 | `CLX` | hard_red | hard-red S=-11.00 sit; no new buys |
