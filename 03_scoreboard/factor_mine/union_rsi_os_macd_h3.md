# Factor mine action — `union_rsi_os_macd_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+3.06%** ($10,306) · signal-only (no cash/fees) was +18.13%. Starts YES **24/27**. Fills 9 · skips 7 · realized $+328.64.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: prior RSI is oversold (≤30) — Finviz prior export, else computed on prior bars.
- Must-have: prior MACD histogram is above zero (momentum still up).
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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `rsi_os=True,macd_up=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $4.49.

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
| 2026-08-28 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-31 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `SION` | 683 | — | $7.31 | +0.00 | $6.75 | -382.48 | -382.48 | +0.00 | -382.48 |
| 2026-09-03 | `EVTL` | 7695 | — | $0.64 | +0.00 | $0.60 | -307.80 | -307.80 | +0.00 | -307.80 |
| 2026-09-04 | `SION` | 683 | $6.75 | $6.68 | -47.81 | $7.18 | +341.50 | +293.69 | -430.29 | -88.79 |
| 2026-09-04 | `EVTL` | 7695 | $0.60 | $0.60 | +0.00 | $0.60 | -15.39 | -15.39 | -307.80 | -323.19 |
| 2026-09-08 | `SION` | 683 | $7.18 | $7.13 | -34.15 | $7.30 | +116.11 | +81.96 | -122.94 | -6.83 |
| 2026-09-08 | `EVTL` | 7695 | $0.60 | $0.60 | +0.00 | $0.59 | -61.56 | -61.56 | -323.19 | -384.75 |
| 2026-09-09 | `SION` | 683 | $7.30 | $7.27 | -20.49 | — | +0.00 | -20.49 | -27.32 | — |
| 2026-09-09 | `EVTL` | 7695 | $0.59 | $0.59 | +30.78 | — | +0.00 | +30.78 | -353.97 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `ZSQR` | 2021 | — | $2.34 | +0.00 | $2.29 | -101.05 | -101.05 | +0.00 | -101.05 |
| 2026-09-16 | `EYPT` | 1280 | — | $3.66 | +0.00 | $3.45 | -268.80 | -268.80 | +0.00 | -268.80 |
| 2026-09-17 | `ZSQR` | 2021 | $2.29 | $2.35 | +121.26 | $2.54 | +383.99 | +505.25 | +20.21 | +404.20 |
| 2026-09-17 | `EYPT` | 1280 | $3.45 | $3.57 | +153.60 | $3.99 | +537.60 | +691.20 | -115.20 | +422.40 |
| 2026-09-18 | `ZSQR` | 2021 | $2.54 | $2.50 | -80.84 | $2.68 | +363.78 | +282.94 | +323.36 | +687.14 |
| 2026-09-18 | `EYPT` | 1280 | $3.99 | $3.95 | -51.20 | $3.85 | -128.00 | -179.20 | +371.20 | +243.20 |
| 2026-09-21 | `ZSQR` | 2021 | $2.68 | $2.68 | +0.00 | — | +0.00 | +0.00 | +687.14 | — |
| 2026-09-21 | `EYPT` | 1280 | $3.85 | $3.87 | +25.60 | — | +0.00 | +25.60 | +268.80 | — |
| 2026-09-21 | `SION` | 1717 | — | $6.00 | +0.00 | $6.00 | +0.00 | +0.00 | +0.00 | +0.00 |

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
| 2026-08-28 | +0.75 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-31 | -5.85 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-09-01 | -6.30 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-09-02 | -3.83 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-09-03 | -0.90 | $10,000.00 | — | $10,000.00 | +0.00 | -690.28 | SION, EVTL | — | $1.33 | $9,228.58 | SION×683, EVTL×7695 |
| 2026-09-04 | +2.25 | $1.33 | SION×683, EVTL×7695 | $9,180.77 | -47.81 | +326.11 | — | — | $1.33 | $9,506.88 | SION×683, EVTL×7695 |
| 2026-09-08 | -11.47 | $1.33 | SION×683, EVTL×7695 | $9,472.73 | -34.15 | +54.55 | — | — | $1.33 | $9,527.28 | SION×683, EVTL×7695 |
| 2026-09-09 | -13.95 | $1.33 | SION×683, EVTL×7695 | $9,537.57 | +10.29 | +0.00 | — | SION, EVTL | $9,458.50 | $9,458.50 | — |
| 2026-09-10 | -13.28 | $9,458.50 | — | $9,458.50 | -0.00 | +0.00 | — | — | $9,458.50 | $9,458.50 | — |
| 2026-09-11 | +0.50 | $9,458.50 | — | $9,458.50 | -0.00 | +0.00 | — | — | $9,458.50 | $9,458.50 | — |
| 2026-09-14 | -11.00 | $9,458.50 | — | $9,458.50 | -0.00 | +0.00 | — | — | $9,458.50 | $9,458.50 | — |
| 2026-09-15 | -3.84 | $9,458.50 | — | $9,458.50 | -0.00 | +0.00 | — | — | $9,458.50 | $9,458.50 | — |
| 2026-09-16 | +5.30 | $9,458.50 | — | $9,458.50 | -0.00 | -369.85 | ZSQR, EYPT | — | $1.97 | $9,046.06 | ZSQR×2021, EYPT×1280 |
| 2026-09-17 | +7.38 | $1.97 | ZSQR×2021, EYPT×1280 | $9,320.92 | +274.86 | +921.59 | — | — | $1.97 | $10,242.51 | ZSQR×2021, EYPT×1280 |
| 2026-09-18 | +4.86 | $1.97 | ZSQR×2021, EYPT×1280 | $10,110.47 | -132.04 | +235.78 | — | — | $1.97 | $10,346.25 | ZSQR×2021, EYPT×1280 |
| 2026-09-21 | +12.87 | $1.97 | ZSQR×2021, EYPT×1280 | $10,371.85 | +25.60 | +0.00 | SION | ZSQR, EYPT | $4.49 | $10,306.49 | SION×1717 |

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
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 683 | $7.31 | $8.81 | — | $4,998.46 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_gainer; 🔵; ret5=+18.5; leftover $5000.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EVTL` | 7695 | $0.64 | $72.33 | — | $1.33 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; ret5=-22.0; leftover $5000.00 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.33 | ▼ close $9,228.58 vs 09:30 $10,000.00 (session -690.28) | 16:00 close · cash $1.33 · equity $9,228.58 vs 09:30 $10,000.00 (-771.42; session marks -690.28) · 2 name(s) marked open→close (per-name table). SION×683 09:30 $7.31 → close $6.75 -382.48; EVTL×7695 09:30 $0.64 → close $0.60 -307.80 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.33 | ▼ 09:30 equity $9,180.77 vs yday $9,228.58 (-47.81) | 09:30 open · cash $1.33 (unchanged overnight, no fees) · equity $9,180.77 vs prior close $9,228.58 (-47.81) · 2 name(s) re-marked at the open (per-name table). SION×683 yday $6.75 → 09:30 $6.68 -47.81; EVTL×7695 yday $0.60 → 09:30 $0.60 +0.00 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.33 | ▲ close $9,506.88 vs 09:30 $9,180.77 (session +326.11) | 16:00 close · cash $1.33 · equity $9,506.88 vs 09:30 $9,180.77 (+326.11; session marks +326.11) · 2 name(s) marked open→close (per-name table). SION×683 09:30 $6.68 → close $7.18 +341.50; EVTL×7695 09:30 $0.60 → close $0.60 -15.39 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.33 | ▼ 09:30 equity $9,472.73 vs yday $9,506.88 (-34.15) | 09:30 open · cash $1.33 (unchanged overnight, no fees) · equity $9,472.73 vs prior close $9,506.88 (-34.15) · 2 name(s) re-marked at the open (per-name table). SION×683 yday $7.18 → 09:30 $7.13 -34.15; EVTL×7695 yday $0.60 → 09:30 $0.60 +0.00 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.33 | ▲ close $9,527.28 vs 09:30 $9,472.73 (session +54.55) | 16:00 close · cash $1.33 · equity $9,527.28 vs 09:30 $9,472.73 (+54.55; session marks +54.55) · 2 name(s) marked open→close (per-name table). SION×683 09:30 $7.13 → close $7.30 +116.11; EVTL×7695 09:30 $0.60 → close $0.59 -61.56 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.33 | ▲ 09:30 equity $9,537.57 vs yday $9,527.28 (+10.29) | 09:30 open · cash $1.33 (unchanged overnight, no fees) · equity $9,537.57 vs prior close $9,527.28 (+10.29) · 2 name(s) re-marked at the open (per-name table). SION×683 yday $7.30 → 09:30 $7.27 -20.49; EVTL×7695 yday $0.59 → 09:30 $0.59 +30.78 | — |
| 2026-09-09 09:30 ET | **SELL** | `SION` | 683 | $7.27 | $8.96 | $-45.09 | $4,957.77 | ▼ -45.09 after sell → book $9,528.60; vs 09:30 mark -8.97 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `EVTL` | 7695 | $0.59 | $70.11 | $-496.41 | $9,458.50 | ▼ -496.41 after sell → book $9,458.50; vs 09:30 mark -70.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,458.50 | ▲ close $9,458.50 vs 09:30 $9,537.57 (session +0.00) | 16:00 close · cash $9,458.50 · no lots left · equity $9,458.50. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,458.50 | ▲ 09:30 equity $9,458.50 vs yday $9,458.50 (-0.00) | 09:30 open · cash $9,458.50 · no holdings · equity $9,458.50 vs prior close $9,458.50 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,458.50 | ▲ close $9,458.50 vs 09:30 $9,458.50 (session +0.00) | 16:00 close · cash $9,458.50 · no lots left · equity $9,458.50. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,458.50 | ▲ 09:30 equity $9,458.50 vs yday $9,458.50 (-0.00) | 09:30 open · cash $9,458.50 · no holdings · equity $9,458.50 vs prior close $9,458.50 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,458.50 | ▲ close $9,458.50 vs 09:30 $9,458.50 (session +0.00) | 16:00 close · cash $9,458.50 · no lots left · equity $9,458.50. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,458.50 | ▲ 09:30 equity $9,458.50 vs yday $9,458.50 (-0.00) | 09:30 open · cash $9,458.50 · no holdings · equity $9,458.50 vs prior close $9,458.50 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,458.50 | ▲ close $9,458.50 vs 09:30 $9,458.50 (session +0.00) | 16:00 close · cash $9,458.50 · no lots left · equity $9,458.50. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,458.50 | ▲ 09:30 equity $9,458.50 vs yday $9,458.50 (-0.00) | 09:30 open · cash $9,458.50 · no holdings · equity $9,458.50 vs prior close $9,458.50 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,458.50 | ▲ close $9,458.50 vs 09:30 $9,458.50 (session +0.00) | 16:00 close · cash $9,458.50 · no lots left · equity $9,458.50. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,458.50 | ▲ 09:30 equity $9,458.50 vs yday $9,458.50 (-0.00) | 09:30 open · cash $9,458.50 · no holdings · equity $9,458.50 vs prior close $9,458.50 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `ZSQR` | 2021 | $2.34 | $26.07 | — | $4,703.28 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; 🔵; ret5=-25.2; leftover $4729.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `EYPT` | 1280 | $3.66 | $16.51 | — | $1.97 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; ret5=-19.7; leftover $4729.25 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.97 | ▼ close $9,046.06 vs 09:30 $9,458.50 (session -369.85) | 16:00 close · cash $1.97 · equity $9,046.06 vs 09:30 $9,458.50 (-412.44; session marks -369.85) · 2 name(s) marked open→close (per-name table). ZSQR×2021 09:30 $2.34 → close $2.29 -101.05; EYPT×1280 09:30 $3.66 → close $3.45 -268.80 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.97 | ▲ 09:30 equity $9,320.92 vs yday $9,046.06 (+274.86) | 09:30 open · cash $1.97 (unchanged overnight, no fees) · equity $9,320.92 vs prior close $9,046.06 (+274.86) · 2 name(s) re-marked at the open (per-name table). ZSQR×2021 yday $2.29 → 09:30 $2.35 +121.26; EYPT×1280 yday $3.45 → 09:30 $3.57 +153.60 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.97 | ▲ close $10,242.51 vs 09:30 $9,320.92 (session +921.59) | 16:00 close · cash $1.97 · equity $10,242.51 vs 09:30 $9,320.92 (+921.59; session marks +921.59) · 2 name(s) marked open→close (per-name table). ZSQR×2021 09:30 $2.35 → close $2.54 +383.99; EYPT×1280 09:30 $3.57 → close $3.99 +537.60 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.97 | ▼ 09:30 equity $10,110.47 vs yday $10,242.51 (-132.04) | 09:30 open · cash $1.97 (unchanged overnight, no fees) · equity $10,110.47 vs prior close $10,242.51 (-132.04) · 2 name(s) re-marked at the open (per-name table). ZSQR×2021 yday $2.54 → 09:30 $2.50 -80.84; EYPT×1280 yday $3.99 → 09:30 $3.95 -51.20 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.97 | ▲ close $10,346.25 vs 09:30 $10,110.47 (session +235.78) | 16:00 close · cash $1.97 · equity $10,346.25 vs 09:30 $10,110.47 (+235.78; session marks +235.78) · 2 name(s) marked open→close (per-name table). ZSQR×2021 09:30 $2.50 → close $2.68 +363.78; EYPT×1280 09:30 $3.95 → close $3.85 -128.00 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.97 | ▲ 09:30 equity $10,371.85 vs yday $10,346.25 (+25.60) | 09:30 open · cash $1.97 (unchanged overnight, no fees) · equity $10,371.85 vs prior close $10,346.25 (+25.60) · 2 name(s) re-marked at the open (per-name table). ZSQR×2021 yday $2.68 → 09:30 $2.68 +0.00; EYPT×1280 yday $3.85 → 09:30 $3.87 +25.60 | — |
| 2026-09-21 09:30 ET | **SELL** | `ZSQR` | 2021 | $2.68 | $26.45 | $+634.62 | $5,391.80 | ▲ +634.62 after sell → book $10,345.40; vs 09:30 mark -26.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 1280 | $3.87 | $16.76 | $+235.52 | $10,328.64 | ▲ +235.52 after sell → book $10,328.64; vs 09:30 mark -16.76 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 1717 | $6.00 | $22.15 | — | $4.49 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; 🔵; ret5=-24.1; leftover $10328.64 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.49 | ▲ close $10,306.49 vs 09:30 $10,371.85 (session +0.00) | 16:00 close · cash $4.49 · equity $10,306.49 vs 09:30 $10,371.85 (-65.36; session marks +0.00) · 1 name(s) marked open→close (per-name table). SION×1717 09:30 $6.00 → close $6.00 +0.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-09-04 | `EVTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `EVTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-17 | `ZSQR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ZSQR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `SION` | 1717 | 2026-09-21 @ $6.00 | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; 🔵; ret5=-24.1; leftover $10328.64 |
