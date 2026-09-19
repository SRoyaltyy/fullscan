# Factor mine action — `union_clk_nr7_mom_opp_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `opp_rvol` · size `leftover` · sell `list` · S-boost `none` · Clock-B #10 ∩ Theme Radar T−1 oppset

Cash book **-0.74%** ($9,926) · signal-only (no cash/fees) was +0.88%. Starts YES **1/26**. Fills 19 · skips 4 · realized $-79.48.

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
- Must-have: Clock-B #10: prior NR7 compression plus moderate momentum.
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
- **Gate** `clk_nr7_mom=True,oppset=True` · **rank** `opp_rvol` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $4.01.

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
| 2026-08-20 | `CTRE` | 251 | — | $39.79 | +0.00 | $39.76 | -7.53 | -7.53 | +0.00 | -7.53 |
| 2026-08-21 | `CTRE` | 251 | $39.76 | $40.00 | +60.24 | — | +0.00 | +60.24 | +52.71 | — |
| 2026-08-24 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-25 | `AMX` | 140 | — | $23.80 | +0.00 | $23.75 | -7.00 | -7.00 | +0.00 | -7.00 |
| 2026-08-25 | `SAN` | 228 | — | $14.65 | +0.00 | $14.63 | -4.56 | -4.56 | +0.00 | -4.56 |
| 2026-08-25 | `GRRR` | 240 | — | $13.92 | +0.00 | $14.04 | +28.80 | +28.80 | +0.00 | +28.80 |
| 2026-08-26 | `AMX` | 140 | $23.75 | $23.75 | +0.00 | — | +0.00 | +0.00 | -7.00 | — |
| 2026-08-26 | `SAN` | 228 | $14.63 | $14.82 | +43.32 | — | +0.00 | +43.32 | +38.76 | — |
| 2026-08-26 | `GRRR` | 240 | $14.04 | $14.03 | -2.40 | — | +0.00 | -2.40 | +26.40 | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | `MRNA` | 36 | — | $137.19 | +0.00 | $137.99 | +28.80 | +28.80 | +0.00 | +28.80 |
| 2026-08-28 | `MNRO` | 407 | — | $12.38 | +0.00 | $12.96 | +236.06 | +236.06 | +0.00 | +236.06 |
| 2026-08-31 | `MRNA` | 36 | $137.99 | $134.10 | -140.04 | — | +0.00 | -140.04 | -111.24 | — |
| 2026-08-31 | `MNRO` | 407 | $12.96 | $12.77 | -77.33 | — | +0.00 | -77.33 | +158.73 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-08 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `DRVN` | 403 | — | $12.55 | +0.00 | $12.15 | -161.20 | -161.20 | +0.00 | -161.20 |
| 2026-09-11 | `CIM` | 449 | — | $11.24 | +0.00 | $11.13 | -49.39 | -49.39 | +0.00 | -49.39 |
| 2026-09-14 | `DRVN` | 403 | $12.15 | $12.33 | +72.54 | — | +0.00 | +72.54 | -88.66 | — |
| 2026-09-14 | `CIM` | 449 | $11.13 | $11.12 | -4.49 | — | +0.00 | -4.49 | -53.88 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | `KNX` | 148 | — | $66.85 | +0.00 | $66.67 | -26.64 | -26.64 | +0.00 | -26.64 |
| 2026-09-18 | `KNX` | 148 | $66.67 | $66.65 | -2.96 | — | +0.00 | -2.96 | -29.60 | — |
| 2026-09-18 | `DRVN` | 808 | — | $12.26 | +0.00 | $12.28 | +16.16 | +16.16 | +0.00 | +16.16 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | -7.53 | CTRE | — | $9.47 | $9,989.23 | CTRE×251 |
| 2026-08-21 | +3.25 | $9.47 | CTRE×251 | $10,049.47 | +60.24 | +0.00 | — | CTRE | $10,046.11 | $10,046.11 | — |
| 2026-08-24 | -5.17 | $10,046.11 | — | $10,046.11 | +0.00 | +0.00 | — | — | $10,046.11 | $10,046.11 | — |
| 2026-08-25 | +1.80 | $10,046.11 | — | $10,046.11 | +0.00 | +17.24 | AMX, SAN, GRRR | — | $24.67 | $10,054.91 | AMX×140, SAN×228, GRRR×240 |
| 2026-08-26 | +2.02 | $24.67 | AMX×140, SAN×228, GRRR×240 | $10,095.83 | +40.92 | +0.00 | — | AMX, SAN, GRRR | $10,087.20 | $10,087.20 | — |
| 2026-08-27 | — | $10,087.20 | — | $10,087.20 | -0.00 | +0.00 | — | — | $10,087.20 | $10,087.20 | — |
| 2026-08-28 | +0.75 | $10,087.20 | — | $10,087.20 | -0.00 | +264.86 | MRNA, MNRO | — | $102.35 | $10,344.71 | MRNA×36, MNRO×407 |
| 2026-08-31 | -5.85 | $102.35 | MRNA×36, MNRO×407 | $10,127.34 | -217.37 | +0.00 | — | MRNA, MNRO | $10,119.83 | $10,119.83 | — |
| 2026-09-01 | -6.30 | $10,119.83 | — | $10,119.83 | +0.00 | +0.00 | — | — | $10,119.83 | $10,119.83 | — |
| 2026-09-02 | -3.83 | $10,119.83 | — | $10,119.83 | +0.00 | +0.00 | — | — | $10,119.83 | $10,119.83 | — |
| 2026-09-03 | -0.90 | $10,119.83 | — | $10,119.83 | +0.00 | +0.00 | — | — | $10,119.83 | $10,119.83 | — |
| 2026-09-04 | +2.25 | $10,119.83 | — | $10,119.83 | +0.00 | +0.00 | — | — | $10,119.83 | $10,119.83 | — |
| 2026-09-08 | -11.47 | $10,119.83 | — | $10,119.83 | +0.00 | +0.00 | — | — | $10,119.83 | $10,119.83 | — |
| 2026-09-09 | -13.95 | $10,119.83 | — | $10,119.83 | +0.00 | +0.00 | — | — | $10,119.83 | $10,119.83 | — |
| 2026-09-10 | -13.28 | $10,119.83 | — | $10,119.83 | +0.00 | +0.00 | — | — | $10,119.83 | $10,119.83 | — |
| 2026-09-11 | +0.50 | $10,119.83 | — | $10,119.83 | +0.00 | -210.59 | DRVN, CIM | — | $4.43 | $9,898.25 | DRVN×403, CIM×449 |
| 2026-09-14 | -11.00 | $4.43 | DRVN×403, CIM×449 | $9,966.30 | +68.05 | +0.00 | — | DRVN, CIM | $9,955.09 | $9,955.09 | — |
| 2026-09-15 | -3.84 | $9,955.09 | — | $9,955.09 | -0.00 | +0.00 | — | — | $9,955.09 | $9,955.09 | — |
| 2026-09-16 | +5.30 | $9,955.09 | — | $9,955.09 | -0.00 | +0.00 | — | — | $9,955.09 | $9,955.09 | — |
| 2026-09-17 | +7.38 | $9,955.09 | — | $9,955.09 | -0.00 | -26.64 | KNX | — | $58.86 | $9,926.02 | KNX×148 |
| 2026-09-18 | +4.86 | $58.86 | KNX×148 | $9,923.06 | -2.96 | +16.16 | DRVN | KNX | $4.01 | $9,926.25 | DRVN×808 |

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
| 2026-08-20 09:30 ET | **BUY** | `CTRE` | 251 | $39.79 | $3.24 | — | $9.47 | — | Clock-B #10 ∩ Theme Radar T−1 oppset; gate clk_nr7_mom=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+1.7; leftover $10000.00 | join🟢 sector🔴 gen🟢 news🟡 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.47 | ▼ close $9,989.23 vs 09:30 $10,000.00 (session -7.53) | 16:00 close · cash $9.47 · equity $9,989.23 vs 09:30 $10,000.00 (-10.77; session marks -7.53) · 1 name(s) marked open→close (per-name table). CTRE×251 09:30 $39.79 → close $39.76 -7.53 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.47 | ▲ 09:30 equity $10,049.47 vs yday $9,989.23 (+60.24) | 09:30 open · cash $9.47 (unchanged overnight, no fees) · equity $10,049.47 vs prior close $9,989.23 (+60.24) · 1 name(s) re-marked at the open (per-name table). CTRE×251 yday $39.76 → 09:30 $40.00 +60.24 | — |
| 2026-08-21 09:30 ET | **SELL** | `CTRE` | 251 | $40.00 | $3.36 | $+46.11 | $10,046.11 | ▲ +46.11 after sell → book $10,046.11; vs 09:30 mark -3.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,046.11 | ▲ close $10,046.11 vs 09:30 $10,049.47 (session +0.00) | 16:00 close · cash $10,046.11 · no lots left · equity $10,046.11. | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,046.11 | ▲ 09:30 equity $10,046.11 vs yday $10,046.11 (+0.00) | 09:30 open · cash $10,046.11 · no holdings · equity $10,046.11 vs prior close $10,046.11 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,046.11 | ▲ close $10,046.11 vs 09:30 $10,046.11 (session +0.00) | 16:00 close · cash $10,046.11 · no lots left · equity $10,046.11. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,046.11 | ▲ 09:30 equity $10,046.11 vs yday $10,046.11 (+0.00) | 09:30 open · cash $10,046.11 · no holdings · equity $10,046.11 vs prior close $10,046.11 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `AMX` | 140 | $23.80 | $2.41 | — | $6,711.70 | — | Clock-B #10 ∩ Theme Radar T−1 oppset; gate clk_nr7_mom=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.5; leftover $3348.70 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAN` | 228 | $14.65 | $2.94 | — | $3,368.56 | — | Clock-B #10 ∩ Theme Radar T−1 oppset; gate clk_nr7_mom=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+0.9; leftover $3348.70 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 240 | $13.92 | $3.10 | — | $24.67 | — | Clock-B #10 ∩ Theme Radar T−1 oppset; gate clk_nr7_mom=True,oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=+5.9; leftover $3348.70 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.67 | ▲ close $10,054.91 vs 09:30 $10,046.11 (session +17.24) | 16:00 close · cash $24.67 · equity $10,054.91 vs 09:30 $10,046.11 (+8.80; session marks +17.24) · 3 name(s) marked open→close (per-name table). AMX×140 09:30 $23.80 → close $23.75 -7.00; SAN×228 09:30 $14.65 → close $14.63 -4.56; GRRR×240 09:30 $13.92 → close $14.04 +28.80 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.67 | ▲ 09:30 equity $10,095.83 vs yday $10,054.91 (+40.92) | 09:30 open · cash $24.67 (unchanged overnight, no fees) · equity $10,095.83 vs prior close $10,054.91 (+40.92) · 3 name(s) re-marked at the open (per-name table). AMX×140 yday $23.75 → 09:30 $23.75 +0.00; SAN×228 yday $14.63 → 09:30 $14.82 +43.32; GRRR×240 yday $14.04 → 09:30 $14.03 -2.40 | — |
| 2026-08-26 09:30 ET | **SELL** | `AMX` | 140 | $23.75 | $2.46 | $-11.87 | $3,347.21 | ▼ -11.87 after sell → book $10,093.37; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `SAN` | 228 | $14.82 | $3.01 | $+32.81 | $6,723.16 | ▲ +32.81 after sell → book $10,090.36; vs 09:30 mark -3.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 240 | $14.03 | $3.16 | $+20.14 | $10,087.20 | ▲ +20.14 after sell → book $10,087.20; vs 09:30 mark -3.16 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,087.20 | ▲ close $10,087.20 vs 09:30 $10,095.83 (session +0.00) | 16:00 close · cash $10,087.20 · no lots left · equity $10,087.20. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,087.20 | ▲ 09:30 equity $10,087.20 vs yday $10,087.20 (-0.00) | 09:30 open · cash $10,087.20 · no holdings · equity $10,087.20 vs prior close $10,087.20 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,087.20 | ▲ close $10,087.20 vs 09:30 $10,087.20 (session +0.00) | 16:00 close · cash $10,087.20 · no lots left · equity $10,087.20. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,087.20 | ▲ 09:30 equity $10,087.20 vs yday $10,087.20 (-0.00) | 09:30 open · cash $10,087.20 · no holdings · equity $10,087.20 vs prior close $10,087.20 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 36 | $137.19 | $2.10 | — | $5,146.26 | — | Clock-B #10 ∩ Theme Radar T−1 oppset; gate clk_nr7_mom=True,oppset=True; rank opp_rvol; list ohlc_hot; ret5=+7.1; leftover $5043.60 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MNRO` | 407 | $12.38 | $5.25 | — | $102.35 | — | Clock-B #10 ∩ Theme Radar T−1 oppset; gate clk_nr7_mom=True,oppset=True; rank opp_rvol; list yday_mover; ret5=+2.4; leftover $5043.60 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $102.35 | ▲ close $10,344.71 vs 09:30 $10,087.20 (session +264.86) | 16:00 close · cash $102.35 · equity $10,344.71 vs 09:30 $10,087.20 (+257.51; session marks +264.86) · 2 name(s) marked open→close (per-name table). MRNA×36 09:30 $137.19 → close $137.99 +28.80; MNRO×407 09:30 $12.38 → close $12.96 +236.06 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $102.35 | ▼ 09:30 equity $10,127.34 vs yday $10,344.71 (-217.37) | 09:30 open · cash $102.35 (unchanged overnight, no fees) · equity $10,127.34 vs prior close $10,344.71 (-217.37) · 2 name(s) re-marked at the open (per-name table). MRNA×36 yday $137.99 → 09:30 $134.10 -140.04; MNRO×407 yday $12.96 → 09:30 $12.77 -77.33 | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 36 | $134.10 | $2.15 | $-115.48 | $4,927.80 | ▼ -115.48 after sell → book $10,125.19; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `MNRO` | 407 | $12.77 | $5.36 | $+148.12 | $10,119.83 | ▲ +148.12 after sell → book $10,119.83; vs 09:30 mark -5.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,119.83 | ▲ close $10,119.83 vs 09:30 $10,127.34 (session +0.00) | 16:00 close · cash $10,119.83 · no lots left · equity $10,119.83. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,119.83 | ▲ 09:30 equity $10,119.83 vs yday $10,119.83 (+0.00) | 09:30 open · cash $10,119.83 · no holdings · equity $10,119.83 vs prior close $10,119.83 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,119.83 | ▲ close $10,119.83 vs 09:30 $10,119.83 (session +0.00) | 16:00 close · cash $10,119.83 · no lots left · equity $10,119.83. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,119.83 | ▲ 09:30 equity $10,119.83 vs yday $10,119.83 (+0.00) | 09:30 open · cash $10,119.83 · no holdings · equity $10,119.83 vs prior close $10,119.83 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,119.83 | ▲ close $10,119.83 vs 09:30 $10,119.83 (session +0.00) | 16:00 close · cash $10,119.83 · no lots left · equity $10,119.83. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,119.83 | ▲ 09:30 equity $10,119.83 vs yday $10,119.83 (+0.00) | 09:30 open · cash $10,119.83 · no holdings · equity $10,119.83 vs prior close $10,119.83 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,119.83 | ▲ close $10,119.83 vs 09:30 $10,119.83 (session +0.00) | 16:00 close · cash $10,119.83 · no lots left · equity $10,119.83. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,119.83 | ▲ 09:30 equity $10,119.83 vs yday $10,119.83 (+0.00) | 09:30 open · cash $10,119.83 · no holdings · equity $10,119.83 vs prior close $10,119.83 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,119.83 | ▲ close $10,119.83 vs 09:30 $10,119.83 (session +0.00) | 16:00 close · cash $10,119.83 · no lots left · equity $10,119.83. | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,119.83 | ▲ 09:30 equity $10,119.83 vs yday $10,119.83 (+0.00) | 09:30 open · cash $10,119.83 · no holdings · equity $10,119.83 vs prior close $10,119.83 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,119.83 | ▲ close $10,119.83 vs 09:30 $10,119.83 (session +0.00) | 16:00 close · cash $10,119.83 · no lots left · equity $10,119.83. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,119.83 | ▲ 09:30 equity $10,119.83 vs yday $10,119.83 (+0.00) | 09:30 open · cash $10,119.83 · no holdings · equity $10,119.83 vs prior close $10,119.83 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,119.83 | ▲ close $10,119.83 vs 09:30 $10,119.83 (session +0.00) | 16:00 close · cash $10,119.83 · no lots left · equity $10,119.83. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,119.83 | ▲ 09:30 equity $10,119.83 vs yday $10,119.83 (+0.00) | 09:30 open · cash $10,119.83 · no holdings · equity $10,119.83 vs prior close $10,119.83 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,119.83 | ▲ close $10,119.83 vs 09:30 $10,119.83 (session +0.00) | 16:00 close · cash $10,119.83 · no lots left · equity $10,119.83. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,119.83 | ▲ 09:30 equity $10,119.83 vs yday $10,119.83 (+0.00) | 09:30 open · cash $10,119.83 · no holdings · equity $10,119.83 vs prior close $10,119.83 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `DRVN` | 403 | $12.55 | $5.20 | — | $5,056.98 | — | Clock-B #10 ∩ Theme Radar T−1 oppset; gate clk_nr7_mom=True,oppset=True; rank opp_rvol; list oppset; ret5=+6.4; leftover $5059.92 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CIM` | 449 | $11.24 | $5.79 | — | $4.43 | — | Clock-B #10 ∩ Theme Radar T−1 oppset; gate clk_nr7_mom=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+1.1; leftover $5059.92 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.43 | ▼ close $9,898.25 vs 09:30 $10,119.83 (session -210.59) | 16:00 close · cash $4.43 · equity $9,898.25 vs 09:30 $10,119.83 (-221.58; session marks -210.59) · 2 name(s) marked open→close (per-name table). DRVN×403 09:30 $12.55 → close $12.15 -161.20; CIM×449 09:30 $11.24 → close $11.13 -49.39 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.43 | ▲ 09:30 equity $9,966.30 vs yday $9,898.25 (+68.05) | 09:30 open · cash $4.43 (unchanged overnight, no fees) · equity $9,966.30 vs prior close $9,898.25 (+68.05) · 2 name(s) re-marked at the open (per-name table). DRVN×403 yday $12.15 → 09:30 $12.33 +72.54; CIM×449 yday $11.13 → 09:30 $11.12 -4.49 | — |
| 2026-09-14 09:30 ET | **SELL** | `DRVN` | 403 | $12.33 | $5.31 | $-99.16 | $4,968.12 | ▼ -99.16 after sell → book $9,961.00; vs 09:30 mark -5.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CIM` | 449 | $11.12 | $5.91 | $-65.58 | $9,955.09 | ▼ -65.58 after sell → book $9,955.09; vs 09:30 mark -5.91 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,955.09 | ▲ close $9,955.09 vs 09:30 $9,966.30 (session +0.00) | 16:00 close · cash $9,955.09 · no lots left · equity $9,955.09. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,955.09 | ▲ 09:30 equity $9,955.09 vs yday $9,955.09 (-0.00) | 09:30 open · cash $9,955.09 · no holdings · equity $9,955.09 vs prior close $9,955.09 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,955.09 | ▲ close $9,955.09 vs 09:30 $9,955.09 (session +0.00) | 16:00 close · cash $9,955.09 · no lots left · equity $9,955.09. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,955.09 | ▲ 09:30 equity $9,955.09 vs yday $9,955.09 (-0.00) | 09:30 open · cash $9,955.09 · no holdings · equity $9,955.09 vs prior close $9,955.09 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,955.09 | ▲ close $9,955.09 vs 09:30 $9,955.09 (session +0.00) | 16:00 close · cash $9,955.09 · no lots left · equity $9,955.09. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,955.09 | ▲ 09:30 equity $9,955.09 vs yday $9,955.09 (-0.00) | 09:30 open · cash $9,955.09 · no holdings · equity $9,955.09 vs prior close $9,955.09 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 09:30 ET | **BUY** | `KNX` | 148 | $66.85 | $2.43 | — | $58.86 | — | Clock-B #10 ∩ Theme Radar T−1 oppset; gate clk_nr7_mom=True,oppset=True; rank opp_rvol; list oppset; 🔵; ret5=+3.3; leftover $9955.09 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $58.86 | ▼ close $9,926.02 vs 09:30 $9,955.09 (session -26.64) | 16:00 close · cash $58.86 · equity $9,926.02 vs 09:30 $9,955.09 (-29.07; session marks -26.64) · 1 name(s) marked open→close (per-name table). KNX×148 09:30 $66.85 → close $66.67 -26.64 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $58.86 | ▼ 09:30 equity $9,923.06 vs yday $9,926.02 (-2.96) | 09:30 open · cash $58.86 (unchanged overnight, no fees) · equity $9,923.06 vs prior close $9,926.02 (-2.96) · 1 name(s) re-marked at the open (per-name table). KNX×148 yday $66.67 → 09:30 $66.65 -2.96 | — |
| 2026-09-18 09:30 ET | **SELL** | `KNX` | 148 | $66.65 | $2.54 | $-34.57 | $9,920.52 | ▼ -34.57 after sell → book $9,920.52; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `DRVN` | 808 | $12.26 | $10.42 | — | $4.01 | — | Clock-B #10 ∩ Theme Radar T−1 oppset; gate clk_nr7_mom=True,oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+6.4; leftover $9920.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.01 | ▲ close $9,926.25 vs 09:30 $9,923.06 (session +16.16) | 16:00 close · cash $4.01 · equity $9,926.25 vs 09:30 $9,923.06 (+3.19; session marks +16.16) · 1 name(s) marked open→close (per-name table). DRVN×808 09:30 $12.26 → close $12.28 +16.16 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-09-08 | `BSBR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-10 | `OPFI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-15 | `GRNT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TECK` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DRVN` | 808 | 2026-09-18 @ $12.26 | Clock-B #10 ∩ Theme Radar T−1 oppset; gate clk_nr7_mom=True,oppset=True; rank opp_rvol; list oppset; 🔵; ⚪; ret5=+6.4; leftover $9920.52 |
