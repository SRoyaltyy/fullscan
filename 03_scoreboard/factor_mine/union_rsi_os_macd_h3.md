# Factor mine action — `union_rsi_os_macd_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+1.01%** ($10,101) · signal-only (no cash/fees) was +9.58%. Starts YES **26/26**. Fills 10 · skips 14 · realized $-548.71.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $24.72.

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
| 2026-09-11 | `ENB` | 195 | — | $48.37 | +0.00 | $47.76 | -118.95 | -118.95 | +0.00 | -118.95 |
| 2026-09-14 | `ENB` | 195 | $47.76 | $47.85 | +17.55 | $48.16 | +60.45 | +78.00 | -101.40 | -40.95 |
| 2026-09-15 | `ENB` | 195 | $48.16 | $48.58 | +81.90 | $48.36 | -42.90 | +39.00 | +40.95 | -1.95 |
| 2026-09-16 | `ENB` | 195 | $48.36 | $48.36 | +0.00 | — | +0.00 | +0.00 | -1.95 | — |
| 2026-09-16 | `ZSQR` | 1346 | — | $2.34 | +0.00 | $2.29 | -67.30 | -67.30 | +0.00 | -67.30 |
| 2026-09-16 | `EYPT` | 860 | — | $3.66 | +0.00 | $3.45 | -180.60 | -180.60 | +0.00 | -180.60 |
| 2026-09-16 | `BHF` | 63 | — | $49.01 | +0.00 | $48.79 | -13.86 | -13.86 | +0.00 | -13.86 |
| 2026-09-17 | `ZSQR` | 1346 | $2.29 | $2.35 | +80.76 | $2.54 | +255.74 | +336.50 | +13.46 | +269.20 |
| 2026-09-17 | `EYPT` | 860 | $3.45 | $3.57 | +103.20 | $3.99 | +361.20 | +464.40 | -77.40 | +283.80 |
| 2026-09-17 | `BHF` | 63 | $48.79 | $48.79 | +0.00 | $49.40 | +38.43 | +38.43 | -13.86 | +24.57 |
| 2026-09-18 | `ZSQR` | 1346 | $2.54 | $2.50 | -53.84 | $2.68 | +242.28 | +188.44 | +215.36 | +457.64 |
| 2026-09-18 | `EYPT` | 860 | $3.99 | $3.95 | -34.40 | $3.85 | -86.00 | -120.40 | +249.40 | +163.40 |
| 2026-09-18 | `BHF` | 63 | $49.40 | $49.06 | -21.42 | $49.95 | +56.07 | +34.65 | +3.15 | +59.22 |
| 2026-09-18 | `MNR` | 1 | — | $10.95 | +0.00 | $11.13 | +0.18 | +0.18 | +0.00 | +0.18 |

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
| 2026-09-11 | +0.50 | $9,458.50 | — | $9,458.50 | -0.00 | -118.95 | ENB | — | $23.77 | $9,336.97 | ENB×195 |
| 2026-09-14 | -11.00 | $23.77 | ENB×195 | $9,354.52 | +17.55 | +60.45 | — | — | $23.77 | $9,414.97 | ENB×195 |
| 2026-09-15 | -3.84 | $23.77 | ENB×195 | $9,496.87 | +81.90 | -42.90 | — | — | $23.77 | $9,453.97 | ENB×195 |
| 2026-09-16 | +5.30 | $23.77 | ENB×195 | $9,453.97 | +0.00 | -261.76 | ZSQR, EYPT, BHF | ENB | $35.78 | $9,158.89 | ZSQR×1346, EYPT×860, BHF×63 |
| 2026-09-17 | +7.38 | $35.78 | ZSQR×1346, EYPT×860, BHF×63 | $9,342.85 | +183.96 | +655.37 | — | — | $35.78 | $9,998.22 | ZSQR×1346, EYPT×860, BHF×63 |
| 2026-09-18 | +4.86 | $35.78 | ZSQR×1346, EYPT×860, BHF×63 | $9,888.56 | -109.66 | +212.53 | MNR | — | $24.72 | $10,100.98 | ZSQR×1346, EYPT×860, BHF×63, MNR×1 |

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
| 2026-09-11 09:30 ET | **BUY** | `ENB` | 195 | $48.37 | $2.58 | — | $23.77 | — | combo gate; gate rsi_os=True,macd_up=True; list oppset; ret5=-0.2; leftover $9458.50 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.77 | ▼ close $9,336.97 vs 09:30 $9,458.50 (session -118.95) | 16:00 close · cash $23.77 · equity $9,336.97 vs 09:30 $9,458.50 (-121.53; session marks -118.95) · 1 name(s) marked open→close (per-name table). ENB×195 09:30 $48.37 → close $47.76 -118.95 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.77 | ▲ 09:30 equity $9,354.52 vs yday $9,336.97 (+17.55) | 09:30 open · cash $23.77 (unchanged overnight, no fees) · equity $9,354.52 vs prior close $9,336.97 (+17.55) · 1 name(s) re-marked at the open (per-name table). ENB×195 yday $47.76 → 09:30 $47.85 +17.55 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.77 | ▲ close $9,414.97 vs 09:30 $9,354.52 (session +60.45) | 16:00 close · cash $23.77 · equity $9,414.97 vs 09:30 $9,354.52 (+60.45; session marks +60.45) · 1 name(s) marked open→close (per-name table). ENB×195 09:30 $47.85 → close $48.16 +60.45 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.77 | ▲ 09:30 equity $9,496.87 vs yday $9,414.97 (+81.90) | 09:30 open · cash $23.77 (unchanged overnight, no fees) · equity $9,496.87 vs prior close $9,414.97 (+81.90) · 1 name(s) re-marked at the open (per-name table). ENB×195 yday $48.16 → 09:30 $48.58 +81.90 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.77 | ▼ close $9,453.97 vs 09:30 $9,496.87 (session -42.90) | 16:00 close · cash $23.77 · equity $9,453.97 vs 09:30 $9,496.87 (-42.90; session marks -42.90) · 1 name(s) marked open→close (per-name table). ENB×195 09:30 $48.58 → close $48.36 -42.90 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.77 | ▲ 09:30 equity $9,453.97 vs yday $9,453.97 (+0.00) | 09:30 open · cash $23.77 (unchanged overnight, no fees) · equity $9,453.97 vs prior close $9,453.97 (+0.00) · 1 name(s) re-marked at the open (per-name table). ENB×195 yday $48.36 → 09:30 $48.36 +0.00 | — |
| 2026-09-16 09:30 ET | **SELL** | `ENB` | 195 | $48.36 | $2.68 | $-7.21 | $9,451.29 | ▼ -7.21 after sell → book $9,451.29; vs 09:30 mark -2.68 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `ZSQR` | 1346 | $2.34 | $17.36 | — | $6,284.28 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; 🔵; ret5=-25.2; leftover $3150.43 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `EYPT` | 860 | $3.66 | $11.09 | — | $3,125.59 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; ret5=-19.7; leftover $3150.43 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `BHF` | 63 | $49.01 | $2.18 | — | $35.78 | — | combo gate; gate rsi_os=True,macd_up=True; list oppset; 🔵; ret5=-1.5; leftover $3150.43 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.78 | ▼ close $9,158.89 vs 09:30 $9,453.97 (session -261.76) | 16:00 close · cash $35.78 · equity $9,158.89 vs 09:30 $9,453.97 (-295.08; session marks -261.76) · 3 name(s) marked open→close (per-name table). ZSQR×1346 09:30 $2.34 → close $2.29 -67.30; EYPT×860 09:30 $3.66 → close $3.45 -180.60; BHF×63 09:30 $49.01 → close $48.79 -13.86 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.78 | ▲ 09:30 equity $9,342.85 vs yday $9,158.89 (+183.96) | 09:30 open · cash $35.78 (unchanged overnight, no fees) · equity $9,342.85 vs prior close $9,158.89 (+183.96) · 3 name(s) re-marked at the open (per-name table). ZSQR×1346 yday $2.29 → 09:30 $2.35 +80.76; EYPT×860 yday $3.45 → 09:30 $3.57 +103.20; BHF×63 yday $48.79 → 09:30 $48.79 +0.00 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.78 | ▲ close $9,998.22 vs 09:30 $9,342.85 (session +655.37) | 16:00 close · cash $35.78 · equity $9,998.22 vs 09:30 $9,342.85 (+655.37; session marks +655.37) · 3 name(s) marked open→close (per-name table). ZSQR×1346 09:30 $2.35 → close $2.54 +255.74; EYPT×860 09:30 $3.57 → close $3.99 +361.20; BHF×63 09:30 $48.79 → close $49.40 +38.43 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.78 | ▼ 09:30 equity $9,888.56 vs yday $9,998.22 (-109.66) | 09:30 open · cash $35.78 (unchanged overnight, no fees) · equity $9,888.56 vs prior close $9,998.22 (-109.66) · 3 name(s) re-marked at the open (per-name table). ZSQR×1346 yday $2.54 → 09:30 $2.50 -53.84; EYPT×860 yday $3.99 → 09:30 $3.95 -34.40; BHF×63 yday $49.40 → 09:30 $49.06 -21.42 | — |
| 2026-09-18 09:30 ET | **BUY** | `MNR` | 1 | $10.95 | $0.11 | — | $24.72 | — | combo gate; gate rsi_os=True,macd_up=True; list oppset; ret5=+1.7; leftover $17.89 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.72 | ▲ close $10,100.98 vs 09:30 $9,888.56 (session +212.53) | 16:00 close · cash $24.72 · equity $10,100.98 vs 09:30 $9,888.56 (+212.42; session marks +212.53) · 4 name(s) marked open→close (per-name table). ZSQR×1346 09:30 $2.50 → close $2.68 +242.28; EYPT×860 09:30 $3.95 → close $3.85 -86.00; BHF×63 09:30 $49.06 → close $49.95 +56.07; MNR×1 09:30 $10.95 → close $11.13 +0.18 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-09-04 | `EVTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AMX` | cash | leftover split 1.33 < 1 share @ 23.03 |
| 2026-09-08 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `EVTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-10 | `SFD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ENB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `ENB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-17 | `ZSQR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BHF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ZSQR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BHF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `MTZ` | cash | leftover split 17.89 < 1 share @ 210.53 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ZSQR` | 1346 | 2026-09-16 @ $2.34 | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; 🔵; ret5=-25.2; leftover $3150.43 |
| `EYPT` | 860 | 2026-09-16 @ $3.66 | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; ret5=-19.7; leftover $3150.43 |
| `BHF` | 63 | 2026-09-16 @ $49.01 | combo gate; gate rsi_os=True,macd_up=True; list oppset; 🔵; ret5=-1.5; leftover $3150.43 |
| `MNR` | 1 | 2026-09-18 @ $10.95 | combo gate; gate rsi_os=True,macd_up=True; list oppset; ret5=+1.7; leftover $17.89 |
