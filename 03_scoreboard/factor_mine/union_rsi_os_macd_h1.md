# Factor mine action — `union_rsi_os_macd_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-8.27%** ($9,174) · signal-only (no cash/fees) was -3.93%. Starts YES **1/29**. Fills 10 · skips 0 · realized $-826.45.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `rsi_os=True,macd_up=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,173.55.

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
| 2026-09-04 | `EVTL` | 7695 | $0.60 | $0.60 | +0.00 | — | +0.00 | +0.00 | -307.80 | — |
| 2026-09-08 | `SION` | 683 | $7.18 | $7.13 | -34.15 | — | +0.00 | -34.15 | -122.94 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `ZSQR` | 2010 | — | $2.34 | +0.00 | $2.29 | -100.50 | -100.50 | +0.00 | -100.50 |
| 2026-09-16 | `EYPT` | 1273 | — | $3.66 | +0.00 | $3.45 | -267.33 | -267.33 | +0.00 | -267.33 |
| 2026-09-17 | `ZSQR` | 2010 | $2.29 | $2.35 | +120.60 | — | +0.00 | +120.60 | +20.10 | — |
| 2026-09-17 | `EYPT` | 1273 | $3.45 | $3.57 | +152.76 | — | +0.00 | +152.76 | -114.57 | — |
| 2026-09-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-21 | `SION` | 1534 | — | $6.00 | +0.00 | $6.00 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-22 | `SION` | 1534 | $6.00 | $5.99 | -15.34 | — | +0.00 | -15.34 | -15.34 | — |
| 2026-09-23 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

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
| 2026-09-04 | +2.25 | $1.33 | SION×683, EVTL×7695 | $9,180.77 | -47.81 | +341.50 | — | EVTL | $4,547.76 | $9,451.70 | SION×683 |
| 2026-09-08 | -11.47 | $4,547.76 | SION×683 | $9,417.55 | -34.15 | +0.00 | — | SION | $9,408.58 | $9,408.58 | — |
| 2026-09-09 | -13.95 | $9,408.58 | — | $9,408.58 | +0.00 | +0.00 | — | — | $9,408.58 | $9,408.58 | — |
| 2026-09-10 | -13.28 | $9,408.58 | — | $9,408.58 | +0.00 | +0.00 | — | — | $9,408.58 | $9,408.58 | — |
| 2026-09-11 | +0.50 | $9,408.58 | — | $9,408.58 | +0.00 | +0.00 | — | — | $9,408.58 | $9,408.58 | — |
| 2026-09-14 | -11.00 | $9,408.58 | — | $9,408.58 | +0.00 | +0.00 | — | — | $9,408.58 | $9,408.58 | — |
| 2026-09-15 | -3.84 | $9,408.58 | — | $9,408.58 | +0.00 | +0.00 | — | — | $9,408.58 | $9,408.58 | — |
| 2026-09-16 | +5.30 | $9,408.58 | — | $9,408.58 | +0.00 | -367.83 | ZSQR, EYPT | — | $3.65 | $8,998.40 | ZSQR×2010, EYPT×1273 |
| 2026-09-17 | +7.38 | $3.65 | ZSQR×2010, EYPT×1273 | $9,271.76 | +273.36 | +0.00 | — | ZSQR, EYPT | $9,228.79 | $9,228.79 | — |
| 2026-09-18 | +4.86 | $9,228.79 | — | $9,228.79 | +0.00 | +0.00 | — | — | $9,228.79 | $9,228.79 | — |
| 2026-09-21 | +12.87 | $9,228.79 | — | $9,228.79 | +0.00 | +0.00 | SION | — | $5.00 | $9,209.00 | SION×1534 |
| 2026-09-22 | -0.50 | $5.00 | SION×1534 | $9,193.66 | -15.34 | +0.00 | — | SION | $9,173.55 | $9,173.55 | — |
| 2026-09-23 | +2.29 | $9,173.55 | — | $9,173.55 | -0.00 | +0.00 | — | — | $9,173.55 | $9,173.55 | — |

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
| 2026-09-04 09:30 ET | **SELL** | `EVTL` | 7695 | $0.60 | $70.57 | $-450.70 | $4,547.76 | ▼ -450.70 after sell → book $9,110.20; vs 09:30 mark -70.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,547.76 | ▲ close $9,451.70 vs 09:30 $9,180.77 (session +341.50) | 16:00 close · cash $4,547.76 · equity $9,451.70 vs 09:30 $9,180.77 (+270.93; session marks +341.50) · 1 name(s) marked open→close (per-name table). SION×683 09:30 $6.68 → close $7.18 +341.50 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,547.76 | ▼ 09:30 equity $9,417.55 vs yday $9,451.70 (-34.15) | 09:30 open · cash $4,547.76 (unchanged overnight, no fees) · equity $9,417.55 vs prior close $9,451.70 (-34.15) · 1 name(s) re-marked at the open (per-name table). SION×683 yday $7.18 → 09:30 $7.13 -34.15 | — |
| 2026-09-08 09:30 ET | **SELL** | `SION` | 683 | $7.13 | $8.96 | $-140.71 | $9,408.58 | ▼ -140.71 after sell → book $9,408.58; vs 09:30 mark -8.97 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,408.58 | ▲ close $9,408.58 vs 09:30 $9,417.55 (session +0.00) | 16:00 close · cash $9,408.58 · no lots left · equity $9,408.58. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,408.58 | ▲ 09:30 equity $9,408.58 vs yday $9,408.58 (+0.00) | 09:30 open · cash $9,408.58 · no holdings · equity $9,408.58 vs prior close $9,408.58 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,408.58 | ▲ close $9,408.58 vs 09:30 $9,408.58 (session +0.00) | 16:00 close · cash $9,408.58 · no lots left · equity $9,408.58. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,408.58 | ▲ 09:30 equity $9,408.58 vs yday $9,408.58 (+0.00) | 09:30 open · cash $9,408.58 · no holdings · equity $9,408.58 vs prior close $9,408.58 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,408.58 | ▲ close $9,408.58 vs 09:30 $9,408.58 (session +0.00) | 16:00 close · cash $9,408.58 · no lots left · equity $9,408.58. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,408.58 | ▲ 09:30 equity $9,408.58 vs yday $9,408.58 (+0.00) | 09:30 open · cash $9,408.58 · no holdings · equity $9,408.58 vs prior close $9,408.58 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,408.58 | ▲ close $9,408.58 vs 09:30 $9,408.58 (session +0.00) | 16:00 close · cash $9,408.58 · no lots left · equity $9,408.58. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,408.58 | ▲ 09:30 equity $9,408.58 vs yday $9,408.58 (+0.00) | 09:30 open · cash $9,408.58 · no holdings · equity $9,408.58 vs prior close $9,408.58 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,408.58 | ▲ close $9,408.58 vs 09:30 $9,408.58 (session +0.00) | 16:00 close · cash $9,408.58 · no lots left · equity $9,408.58. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,408.58 | ▲ 09:30 equity $9,408.58 vs yday $9,408.58 (+0.00) | 09:30 open · cash $9,408.58 · no holdings · equity $9,408.58 vs prior close $9,408.58 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,408.58 | ▲ close $9,408.58 vs 09:30 $9,408.58 (session +0.00) | 16:00 close · cash $9,408.58 · no lots left · equity $9,408.58. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,408.58 | ▲ 09:30 equity $9,408.58 vs yday $9,408.58 (+0.00) | 09:30 open · cash $9,408.58 · no holdings · equity $9,408.58 vs prior close $9,408.58 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `ZSQR` | 2010 | $2.34 | $25.93 | — | $4,679.26 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; 🔵; ret5=-25.2; leftover $4704.29 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `EYPT` | 1273 | $3.66 | $16.42 | — | $3.65 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; ret5=-19.7; leftover $4704.29 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.65 | ▼ close $8,998.40 vs 09:30 $9,408.58 (session -367.83) | 16:00 close · cash $3.65 · equity $8,998.40 vs 09:30 $9,408.58 (-410.18; session marks -367.83) · 2 name(s) marked open→close (per-name table). ZSQR×2010 09:30 $2.34 → close $2.29 -100.50; EYPT×1273 09:30 $3.66 → close $3.45 -267.33 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.65 | ▲ 09:30 equity $9,271.76 vs yday $8,998.40 (+273.36) | 09:30 open · cash $3.65 (unchanged overnight, no fees) · equity $9,271.76 vs prior close $8,998.40 (+273.36) · 2 name(s) re-marked at the open (per-name table). ZSQR×2010 yday $2.29 → 09:30 $2.35 +120.60; EYPT×1273 yday $3.45 → 09:30 $3.57 +152.76 | — |
| 2026-09-17 09:30 ET | **SELL** | `ZSQR` | 2010 | $2.35 | $26.30 | $-32.13 | $4,700.85 | ▼ -32.13 after sell → book $9,245.46; vs 09:30 mark -26.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `EYPT` | 1273 | $3.57 | $16.67 | $-147.66 | $9,228.79 | ▼ -147.66 after sell → book $9,228.79; vs 09:30 mark -16.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,228.79 | ▲ close $9,228.79 vs 09:30 $9,271.76 (session +0.00) | 16:00 close · cash $9,228.79 · no lots left · equity $9,228.79. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,228.79 | ▲ 09:30 equity $9,228.79 vs yday $9,228.79 (+0.00) | 09:30 open · cash $9,228.79 · no holdings · equity $9,228.79 vs prior close $9,228.79 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,228.79 | ▲ close $9,228.79 vs 09:30 $9,228.79 (session +0.00) | 16:00 close · cash $9,228.79 · no lots left · equity $9,228.79. | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,228.79 | ▲ 09:30 equity $9,228.79 vs yday $9,228.79 (+0.00) | 09:30 open · cash $9,228.79 · no holdings · equity $9,228.79 vs prior close $9,228.79 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 1534 | $6.00 | $19.79 | — | $5.00 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; ret5=-24.1; leftover $9228.79 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.00 | ▲ close $9,209.00 vs 09:30 $9,228.79 (session +0.00) | 16:00 close · cash $5.00 · equity $9,209.00 vs 09:30 $9,228.79 (-19.79; session marks +0.00) · 1 name(s) marked open→close (per-name table). SION×1534 09:30 $6.00 → close $6.00 +0.00 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.00 | ▼ 09:30 equity $9,193.66 vs yday $9,209.00 (-15.34) | 09:30 open · cash $5.00 (unchanged overnight, no fees) · equity $9,193.66 vs prior close $9,209.00 (-15.34) · 1 name(s) re-marked at the open (per-name table). SION×1534 yday $6.00 → 09:30 $5.99 -15.34 | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 1534 | $5.99 | $20.12 | $-55.25 | $9,173.55 | ▼ -55.25 after sell → book $9,173.55; vs 09:30 mark -20.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,173.55 | ▲ close $9,173.55 vs 09:30 $9,193.66 (session +0.00) | 16:00 close · cash $9,173.55 · no lots left · equity $9,173.55. | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,173.55 | ▲ 09:30 equity $9,173.55 vs yday $9,173.55 (-0.00) | 09:30 open · cash $9,173.55 · no holdings · equity $9,173.55 vs prior close $9,173.55 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,173.55 | ▲ close $9,173.55 vs 09:30 $9,173.55 (session +0.00) | 16:00 close · cash $9,173.55 · no lots left · equity $9,173.55. | — |
