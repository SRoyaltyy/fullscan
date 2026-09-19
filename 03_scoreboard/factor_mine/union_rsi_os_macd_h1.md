# Factor mine action — `union_rsi_os_macd_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-20.68%** ($7,932) · signal-only (no cash/fees) was -13.91%. Starts YES **0/26**. Fills 13 · skips 0 · realized $-1820.72.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $4.89.

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
| 2026-09-16 | `ALHC` | 304 | — | $10.30 | +0.00 | $8.71 | -483.36 | -483.36 | +0.00 | -483.36 |
| 2026-09-16 | `ZSQR` | 1340 | — | $2.34 | +0.00 | $2.29 | -67.00 | -67.00 | +0.00 | -67.00 |
| 2026-09-16 | `EYPT` | 849 | — | $3.66 | +0.00 | $3.45 | -178.29 | -178.29 | +0.00 | -178.29 |
| 2026-09-17 | `ALHC` | 304 | $8.71 | $8.58 | -39.52 | $8.70 | +36.48 | -3.04 | -522.88 | -486.40 |
| 2026-09-17 | `ZSQR` | 1340 | $2.29 | $2.35 | +80.40 | — | +0.00 | +80.40 | +13.40 | — |
| 2026-09-17 | `EYPT` | 849 | $3.45 | $3.57 | +101.88 | — | +0.00 | +101.88 | -76.41 | — |
| 2026-09-17 | `MRLN` | 2695 | — | $2.27 | +0.00 | $2.06 | -565.95 | -565.95 | +0.00 | -565.95 |
| 2026-09-18 | `ALHC` | 304 | $8.70 | $8.68 | -6.08 | — | +0.00 | -6.08 | -492.48 | — |
| 2026-09-18 | `MRLN` | 2695 | $2.06 | $2.07 | +26.95 | — | +0.00 | +26.95 | -539.00 | — |
| 2026-09-18 | `FLNC` | 1083 | — | $7.54 | +0.00 | $7.32 | -232.84 | -232.84 | +0.00 | -232.84 |

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
| 2026-09-16 | +5.30 | $9,408.58 | — | $9,408.58 | +0.00 | -728.65 | ALHC, ZSQR, EYPT | — | $2.28 | $8,647.77 | ALHC×304, ZSQR×1340, EYPT×849 |
| 2026-09-17 | +7.38 | $2.28 | ALHC×304, ZSQR×1340, EYPT×849 | $8,790.53 | +142.76 | -529.47 | MRLN | ZSQR, EYPT | $1.15 | $8,197.65 | ALHC×304, MRLN×2695 |
| 2026-09-18 | +4.86 | $1.15 | ALHC×304, MRLN×2695 | $8,218.52 | +20.87 | -232.84 | FLNC | ALHC, MRLN | $4.89 | $7,932.45 | FLNC×1083 |

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
| 2026-09-16 09:30 ET | **BUY** | `ALHC` | 304 | $10.30 | $3.92 | — | $6,273.46 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; ret5=+0.4; leftover $3136.19 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ZSQR` | 1340 | $2.34 | $17.29 | — | $3,120.58 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; 🔵; ret5=+4.7; leftover $3136.19 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `EYPT` | 849 | $3.66 | $10.95 | — | $2.28 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; ret5=-7.4; leftover $3136.19 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.28 | ▼ close $8,647.77 vs 09:30 $9,408.58 (session -728.65) | 16:00 close · cash $2.28 · equity $8,647.77 vs 09:30 $9,408.58 (-760.81; session marks -728.65) · 3 name(s) marked open→close (per-name table). ALHC×304 09:30 $10.30 → close $8.71 -483.36; ZSQR×1340 09:30 $2.34 → close $2.29 -67.00; EYPT×849 09:30 $3.66 → close $3.45 -178.29 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.28 | ▲ 09:30 equity $8,790.53 vs yday $8,647.77 (+142.76) | 09:30 open · cash $2.28 (unchanged overnight, no fees) · equity $8,790.53 vs prior close $8,647.77 (+142.76) · 3 name(s) re-marked at the open (per-name table). ALHC×304 yday $8.71 → 09:30 $8.58 -39.52; ZSQR×1340 yday $2.29 → 09:30 $2.35 +80.40; EYPT×849 yday $3.45 → 09:30 $3.57 +101.88 | — |
| 2026-09-17 09:30 ET | **SELL** | `ZSQR` | 1340 | $2.35 | $17.53 | $-21.42 | $3,133.75 | ▼ -21.42 after sell → book $8,773.00; vs 09:30 mark -17.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `EYPT` | 849 | $3.57 | $11.12 | $-98.48 | $6,153.56 | ▼ -98.48 after sell → book $8,761.88; vs 09:30 mark -11.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `MRLN` | 2695 | $2.27 | $34.77 | — | $1.15 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; 🔵; ret5=-11.3; leftover $6153.56 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.15 | ▼ close $8,197.65 vs 09:30 $8,790.53 (session -529.47) | 16:00 close · cash $1.15 · equity $8,197.65 vs 09:30 $8,790.53 (-592.88; session marks -529.47) · 2 name(s) marked open→close (per-name table). ALHC×304 09:30 $8.58 → close $8.70 +36.48; MRLN×2695 09:30 $2.27 → close $2.06 -565.95 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.15 | ▲ 09:30 equity $8,218.52 vs yday $8,197.65 (+20.87) | 09:30 open · cash $1.15 (unchanged overnight, no fees) · equity $8,218.52 vs prior close $8,197.65 (+20.87) · 2 name(s) re-marked at the open (per-name table). ALHC×304 yday $8.70 → 09:30 $8.68 -6.08; MRLN×2695 yday $2.06 → 09:30 $2.07 +26.95 | — |
| 2026-09-18 09:30 ET | **SELL** | `ALHC` | 304 | $8.68 | $3.99 | $-500.39 | $2,635.87 | ▼ -500.39 after sell → book $8,214.52; vs 09:30 mark -4.00 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `MRLN` | 2695 | $2.07 | $35.26 | $-609.02 | $8,179.27 | ▼ -609.02 after sell → book $8,179.27; vs 09:30 mark -35.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 1083 | $7.54 | $13.97 | — | $4.89 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; 🔵; ret5=-2.6; leftover $8179.27 | join🟡 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.89 | ▼ close $7,932.45 vs 09:30 $8,218.52 (session -232.84) | 16:00 close · cash $4.89 · equity $7,932.45 vs 09:30 $8,218.52 (-286.07; session marks -232.84) · 1 name(s) marked open→close (per-name table). FLNC×1083 09:30 $7.54 → close $7.32 -232.84 | — |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FLNC` | 1083 | 2026-09-18 @ $7.54 | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; 🔵; ret5=-2.6; leftover $8179.27 |
