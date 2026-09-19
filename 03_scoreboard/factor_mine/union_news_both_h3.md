# Factor mine action — `union_news_both_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 AND headline🟢 (thin; kept as a KILL)

Cash book **+4.04%** ($10,403) · signal-only (no cash/fees) was +6.07%. Starts YES **21/26**. Fills 4 · skips 8 · realized $+403.48.

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
- Must-have: the morning news packet AND the prior-export headline are both green.
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
- **Gate** `news_and_headline=True` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,403.48.

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
| 2026-08-26 | `FNV` | 37 | — | $267.02 | +0.00 | $267.37 | +12.95 | +12.95 | +0.00 | +12.95 |
| 2026-08-27 | `FNV` | 37 | $267.37 | $267.23 | -5.18 | $271.31 | +150.96 | +145.78 | +7.77 | +158.73 |
| 2026-08-28 | `FNV` | 37 | $271.31 | $272.84 | +56.61 | $266.16 | -247.16 | -190.55 | +215.34 | -31.82 |
| 2026-08-31 | `FNV` | 37 | $266.16 | $265.78 | -14.06 | — | +0.00 | -14.06 | -45.88 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-08 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ADBE` | 41 | — | $242.17 | +0.00 | $252.23 | +412.46 | +412.46 | +0.00 | +412.46 |
| 2026-09-14 | `ADBE` | 41 | $252.23 | $261.51 | +380.48 | $265.60 | +167.69 | +548.17 | +792.94 | +960.63 |
| 2026-09-15 | `ADBE` | 41 | $265.60 | $261.70 | -159.90 | $257.76 | -161.54 | -321.44 | +800.73 | +639.19 |
| 2026-09-16 | `ADBE` | 41 | $257.76 | $253.34 | -181.22 | — | +0.00 | -181.22 | +457.97 | — |
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
| 2026-08-26 | +2.02 | $10,000.00 | — | $10,000.00 | +0.00 | +12.95 | FNV | — | $118.16 | $10,010.85 | FNV×37 |
| 2026-08-27 | — | $118.16 | FNV×37 | $10,005.67 | -5.18 | +150.96 | — | — | $118.16 | $10,156.63 | FNV×37 |
| 2026-08-28 | +0.75 | $118.16 | FNV×37 | $10,213.24 | +56.61 | -247.16 | — | — | $118.16 | $9,966.08 | FNV×37 |
| 2026-08-31 | -5.85 | $118.16 | FNV×37 | $9,952.02 | -14.06 | +0.00 | — | FNV | $9,949.83 | $9,949.83 | — |
| 2026-09-01 | -6.30 | $9,949.83 | — | $9,949.83 | -0.00 | +0.00 | — | — | $9,949.83 | $9,949.83 | — |
| 2026-09-02 | -3.83 | $9,949.83 | — | $9,949.83 | -0.00 | +0.00 | — | — | $9,949.83 | $9,949.83 | — |
| 2026-09-03 | -0.90 | $9,949.83 | — | $9,949.83 | -0.00 | +0.00 | — | — | $9,949.83 | $9,949.83 | — |
| 2026-09-04 | +2.25 | $9,949.83 | — | $9,949.83 | -0.00 | +0.00 | — | — | $9,949.83 | $9,949.83 | — |
| 2026-09-08 | -11.47 | $9,949.83 | — | $9,949.83 | -0.00 | +0.00 | — | — | $9,949.83 | $9,949.83 | — |
| 2026-09-09 | -13.95 | $9,949.83 | — | $9,949.83 | -0.00 | +0.00 | — | — | $9,949.83 | $9,949.83 | — |
| 2026-09-10 | -13.28 | $9,949.83 | — | $9,949.83 | -0.00 | +0.00 | — | — | $9,949.83 | $9,949.83 | — |
| 2026-09-11 | +0.50 | $9,949.83 | — | $9,949.83 | -0.00 | +412.46 | ADBE | — | $18.75 | $10,360.18 | ADBE×41 |
| 2026-09-14 | -11.00 | $18.75 | ADBE×41 | $10,740.66 | +380.48 | +167.69 | — | — | $18.75 | $10,908.35 | ADBE×41 |
| 2026-09-15 | -3.84 | $18.75 | ADBE×41 | $10,748.45 | -159.90 | -161.54 | — | — | $18.75 | $10,586.91 | ADBE×41 |
| 2026-09-16 | +5.30 | $18.75 | ADBE×41 | $10,405.69 | -181.22 | +0.00 | — | ADBE | $10,403.48 | $10,403.48 | — |
| 2026-09-17 | +7.38 | $10,403.48 | — | $10,403.48 | +0.00 | +0.00 | — | — | $10,403.48 | $10,403.48 | — |
| 2026-09-18 | +4.86 | $10,403.48 | — | $10,403.48 | +0.00 | +0.00 | — | — | $10,403.48 | $10,403.48 | — |

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
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 37 | $267.02 | $2.10 | — | $118.16 | — | packet🟢 AND headline🟢 (thin; kept as a KILL); gate news_and_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $10000.00 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $118.16 | ▲ close $10,010.85 vs 09:30 $10,000.00 (session +12.95) | 16:00 close · cash $118.16 · equity $10,010.85 vs 09:30 $10,000.00 (+10.85; session marks +12.95) · 1 name(s) marked open→close (per-name table). FNV×37 09:30 $267.02 → close $267.37 +12.95 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.16 | ▼ 09:30 equity $10,005.67 vs yday $10,010.85 (-5.18) | 09:30 open · cash $118.16 (unchanged overnight, no fees) · equity $10,005.67 vs prior close $10,010.85 (-5.18) · 1 name(s) re-marked at the open (per-name table). FNV×37 yday $267.37 → 09:30 $267.23 -5.18 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $118.16 | ▲ close $10,156.63 vs 09:30 $10,005.67 (session +150.96) | 16:00 close · cash $118.16 · equity $10,156.63 vs 09:30 $10,005.67 (+150.96; session marks +150.96) · 1 name(s) marked open→close (per-name table). FNV×37 09:30 $267.23 → close $271.31 +150.96 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.16 | ▲ 09:30 equity $10,213.24 vs yday $10,156.63 (+56.61) | 09:30 open · cash $118.16 (unchanged overnight, no fees) · equity $10,213.24 vs prior close $10,156.63 (+56.61) · 1 name(s) re-marked at the open (per-name table). FNV×37 yday $271.31 → 09:30 $272.84 +56.61 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $118.16 | ▼ close $9,966.08 vs 09:30 $10,213.24 (session -247.16) | 16:00 close · cash $118.16 · equity $9,966.08 vs 09:30 $10,213.24 (-247.16; session marks -247.16) · 1 name(s) marked open→close (per-name table). FNV×37 09:30 $272.84 → close $266.16 -247.16 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.16 | ▼ 09:30 equity $9,952.02 vs yday $9,966.08 (-14.06) | 09:30 open · cash $118.16 (unchanged overnight, no fees) · equity $9,952.02 vs prior close $9,966.08 (-14.06) · 1 name(s) re-marked at the open (per-name table). FNV×37 yday $266.16 → 09:30 $265.78 -14.06 | — |
| 2026-08-31 09:30 ET | **SELL** | `FNV` | 37 | $265.78 | $2.19 | $-50.17 | $9,949.83 | ▼ -50.17 after sell → book $9,949.83; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,949.83 | ▲ close $9,949.83 vs 09:30 $9,952.02 (session +0.00) | 16:00 close · cash $9,949.83 · no lots left · equity $9,949.83. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,949.83 | ▲ 09:30 equity $9,949.83 vs yday $9,949.83 (-0.00) | 09:30 open · cash $9,949.83 · no holdings · equity $9,949.83 vs prior close $9,949.83 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,949.83 | ▲ close $9,949.83 vs 09:30 $9,949.83 (session +0.00) | 16:00 close · cash $9,949.83 · no lots left · equity $9,949.83. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,949.83 | ▲ 09:30 equity $9,949.83 vs yday $9,949.83 (-0.00) | 09:30 open · cash $9,949.83 · no holdings · equity $9,949.83 vs prior close $9,949.83 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,949.83 | ▲ close $9,949.83 vs 09:30 $9,949.83 (session +0.00) | 16:00 close · cash $9,949.83 · no lots left · equity $9,949.83. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,949.83 | ▲ 09:30 equity $9,949.83 vs yday $9,949.83 (-0.00) | 09:30 open · cash $9,949.83 · no holdings · equity $9,949.83 vs prior close $9,949.83 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,949.83 | ▲ close $9,949.83 vs 09:30 $9,949.83 (session +0.00) | 16:00 close · cash $9,949.83 · no lots left · equity $9,949.83. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,949.83 | ▲ 09:30 equity $9,949.83 vs yday $9,949.83 (-0.00) | 09:30 open · cash $9,949.83 · no holdings · equity $9,949.83 vs prior close $9,949.83 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,949.83 | ▲ close $9,949.83 vs 09:30 $9,949.83 (session +0.00) | 16:00 close · cash $9,949.83 · no lots left · equity $9,949.83. | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,949.83 | ▲ 09:30 equity $9,949.83 vs yday $9,949.83 (-0.00) | 09:30 open · cash $9,949.83 · no holdings · equity $9,949.83 vs prior close $9,949.83 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,949.83 | ▲ close $9,949.83 vs 09:30 $9,949.83 (session +0.00) | 16:00 close · cash $9,949.83 · no lots left · equity $9,949.83. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,949.83 | ▲ 09:30 equity $9,949.83 vs yday $9,949.83 (-0.00) | 09:30 open · cash $9,949.83 · no holdings · equity $9,949.83 vs prior close $9,949.83 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,949.83 | ▲ close $9,949.83 vs 09:30 $9,949.83 (session +0.00) | 16:00 close · cash $9,949.83 · no lots left · equity $9,949.83. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,949.83 | ▲ 09:30 equity $9,949.83 vs yday $9,949.83 (-0.00) | 09:30 open · cash $9,949.83 · no holdings · equity $9,949.83 vs prior close $9,949.83 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,949.83 | ▲ close $9,949.83 vs 09:30 $9,949.83 (session +0.00) | 16:00 close · cash $9,949.83 · no lots left · equity $9,949.83. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,949.83 | ▲ 09:30 equity $9,949.83 vs yday $9,949.83 (-0.00) | 09:30 open · cash $9,949.83 · no holdings · equity $9,949.83 vs prior close $9,949.83 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 41 | $242.17 | $2.11 | — | $18.75 | — | packet🟢 AND headline🟢 (thin; kept as a KILL); gate news_and_headline=True; rank cond; list earn_react; ret5=-11.1; leftover $9949.83 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.75 | ▲ close $10,360.18 vs 09:30 $9,949.83 (session +412.46) | 16:00 close · cash $18.75 · equity $10,360.18 vs 09:30 $9,949.83 (+410.35; session marks +412.46) · 1 name(s) marked open→close (per-name table). ADBE×41 09:30 $242.17 → close $252.23 +412.46 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.75 | ▲ 09:30 equity $10,740.66 vs yday $10,360.18 (+380.48) | 09:30 open · cash $18.75 (unchanged overnight, no fees) · equity $10,740.66 vs prior close $10,360.18 (+380.48) · 1 name(s) re-marked at the open (per-name table). ADBE×41 yday $252.23 → 09:30 $261.51 +380.48 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.75 | ▲ close $10,908.35 vs 09:30 $10,740.66 (session +167.69) | 16:00 close · cash $18.75 · equity $10,908.35 vs 09:30 $10,740.66 (+167.69; session marks +167.69) · 1 name(s) marked open→close (per-name table). ADBE×41 09:30 $261.51 → close $265.60 +167.69 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.75 | ▼ 09:30 equity $10,748.45 vs yday $10,908.35 (-159.90) | 09:30 open · cash $18.75 (unchanged overnight, no fees) · equity $10,748.45 vs prior close $10,908.35 (-159.90) · 1 name(s) re-marked at the open (per-name table). ADBE×41 yday $265.60 → 09:30 $261.70 -159.90 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.75 | ▼ close $10,586.91 vs 09:30 $10,748.45 (session -161.54) | 16:00 close · cash $18.75 · equity $10,586.91 vs 09:30 $10,748.45 (-161.54; session marks -161.54) · 1 name(s) marked open→close (per-name table). ADBE×41 09:30 $261.70 → close $257.76 -161.54 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.75 | ▼ 09:30 equity $10,405.69 vs yday $10,586.91 (-181.22) | 09:30 open · cash $18.75 (unchanged overnight, no fees) · equity $10,405.69 vs prior close $10,586.91 (-181.22) · 1 name(s) re-marked at the open (per-name table). ADBE×41 yday $257.76 → 09:30 $253.34 -181.22 | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 41 | $253.34 | $2.21 | $+453.65 | $10,403.48 | ▲ +453.65 after sell → book $10,403.48; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,403.48 | ▲ close $10,403.48 vs 09:30 $10,405.69 (session +0.00) | 16:00 close · cash $10,403.48 · no lots left · equity $10,403.48. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,403.48 | ▲ 09:30 equity $10,403.48 vs yday $10,403.48 (+0.00) | 09:30 open · cash $10,403.48 · no holdings · equity $10,403.48 vs prior close $10,403.48 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,403.48 | ▲ close $10,403.48 vs 09:30 $10,403.48 (session +0.00) | 16:00 close · cash $10,403.48 · no lots left · equity $10,403.48. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,403.48 | ▲ 09:30 equity $10,403.48 vs yday $10,403.48 (+0.00) | 09:30 open · cash $10,403.48 · no holdings · equity $10,403.48 vs prior close $10,403.48 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,403.48 | ▲ close $10,403.48 vs 09:30 $10,403.48 (session +0.00) | 16:00 close · cash $10,403.48 · no lots left · equity $10,403.48. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-27 | `FNV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ASML` | cash | leftover split 118.16 < 1 share @ 1746.53 |
| 2026-08-28 | `FNV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-14 | `ADBE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `ADBE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `QCOM` | hard_red | hard-red S=-3.84 sit; no new buys |
