# Factor mine action — `union_rsi_os_macd_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+17.90%** ($11,790) · signal-only (no cash/fees) was -3.93%. Starts YES **1/30**. Fills 43 · skips 0 · realized $-826.45.

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
- **Gate** `macd_up=True,rsi_os=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,173.55.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 683 | $7.31 | $8.81 | — | $4,998.46 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_gainer; 🔵; ret5=+18.5; leftover $5000.00 | — |
| 2026-09-03 09:30 ET | **BUY** | `EVTL` | 7695 | $0.64 | $72.33 | — | $1.33 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; ret5=-22.0; leftover $5000.00 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.33 | ▼ close $9,228.58 vs 09:30 $10,000.00 (session -690.28) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.33 | ▼ 09:30 equity $9,180.77 vs yday $9,228.58 (-47.81) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `EVTL` | 7695 | $0.60 | $70.57 | $-450.70 | $4,547.76 | ▼ -450.70 after sell → book $9,110.20; vs 09:30 mark -70.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,547.76 | ▲ close $9,451.70 vs 09:30 $9,180.77 (session +341.50) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,547.76 | ▼ 09:30 equity $9,417.55 vs yday $9,451.70 (-34.15) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `SION` | 683 | $7.13 | $8.96 | $-140.71 | $9,408.58 | ▼ -140.71 after sell → book $9,408.58; vs 09:30 mark -8.97 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,408.58 | ▲ close $9,408.58 vs 09:30 $9,417.55 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,408.58 | ▲ 09:30 equity $9,408.58 vs yday $9,408.58 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,408.58 | ▲ close $9,408.58 vs 09:30 $9,408.58 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,408.58 | ▲ 09:30 equity $9,408.58 vs yday $9,408.58 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,408.58 | ▲ close $9,408.58 vs 09:30 $9,408.58 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,408.58 | ▲ 09:30 equity $9,408.58 vs yday $9,408.58 (+0.00) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,408.58 | ▲ close $9,408.58 vs 09:30 $9,408.58 (session +0.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,408.58 | ▲ 09:30 equity $9,408.58 vs yday $9,408.58 (+0.00) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,408.58 | ▲ close $9,408.58 vs 09:30 $9,408.58 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,408.58 | ▲ 09:30 equity $9,408.58 vs yday $9,408.58 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,408.58 | ▲ close $9,408.58 vs 09:30 $9,408.58 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,408.58 | ▲ 09:30 equity $9,408.58 vs yday $9,408.58 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `ZSQR` | 2010 | $2.34 | $25.93 | — | $4,679.26 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; 🔵; ret5=-25.2; leftover $4704.29 | — |
| 2026-09-16 09:30 ET | **BUY** | `EYPT` | 1273 | $3.66 | $16.42 | — | $3.65 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; ret5=-19.7; leftover $4704.29 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.65 | ▼ close $8,998.40 vs 09:30 $9,408.58 (session -367.83) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.65 | ▲ 09:30 equity $9,271.76 vs yday $8,998.40 (+273.36) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `ZSQR` | 2010 | $2.35 | $26.30 | $-32.13 | $4,700.85 | ▼ -32.13 after sell → book $9,245.46; vs 09:30 mark -26.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `EYPT` | 1273 | $3.57 | $16.67 | $-147.66 | $9,228.79 | ▼ -147.66 after sell → book $9,228.79; vs 09:30 mark -16.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,228.79 | ▲ close $9,228.79 vs 09:30 $9,271.76 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,228.79 | ▲ 09:30 equity $9,228.79 vs yday $9,228.79 (+0.00) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,228.79 | ▲ close $9,228.79 vs 09:30 $9,228.79 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,228.79 | ▲ 09:30 equity $9,228.79 vs yday $9,228.79 (+0.00) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 1534 | $6.00 | $19.79 | — | $5.00 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; ret5=-24.1; leftover $9228.79 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.00 | ▲ close $9,209.00 vs 09:30 $9,228.79 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.00 | ▼ 09:30 equity $9,193.66 vs yday $9,209.00 (-15.34) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 1534 | $5.99 | $20.12 | $-55.25 | $9,173.55 | ▼ -55.25 after sell → book $9,173.55; vs 09:30 mark -20.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,173.55 | ▲ close $9,173.55 vs 09:30 $9,193.66 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,173.55 | ▲ 09:30 equity $9,173.55 vs yday $9,173.55 (-0.00) | — | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,173.55 | ▲ close $9,173.55 vs 09:30 $9,173.55 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,173.55 | ▲ 09:30 equity $9,173.55 vs yday $9,173.55 (-0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,173.55 | ▲ close $9,173.55 vs 09:30 $9,173.55 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,367.74 | ▲ 09:30 equity $10,367.74 vs yday $10,367.74 (+0.00) | 09:30 open · cash $10,367.74 · no holdings · equity $10,367.74 vs prior close $10,367.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,367.74 | ▲ close $10,367.74 vs 09:30 $10,367.74 (session +0.00) | 16:00 close · cash $10,367.74 · no lots left · equity $10,367.74. | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,367.74 | ▲ 09:30 equity $10,367.74 vs yday $10,367.74 (+0.00) | 09:30 open · cash $10,367.74 · no holdings · equity $10,367.74 vs prior close $10,367.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,367.74 | ▲ close $10,367.74 vs 09:30 $10,367.74 (session +0.00) | 16:00 close · cash $10,367.74 · no lots left · equity $10,367.74. | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,367.74 | ▲ 09:30 equity $10,367.74 vs yday $10,367.74 (+0.00) | 09:30 open · cash $10,367.74 · no holdings · equity $10,367.74 vs prior close $10,367.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,367.74 | ▲ close $10,367.74 vs 09:30 $10,367.74 (session +0.00) | 16:00 close · cash $10,367.74 · no lots left · equity $10,367.74. | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,367.74 | ▲ 09:30 equity $10,367.74 vs yday $10,367.74 (+0.00) | 09:30 open · cash $10,367.74 · no holdings · equity $10,367.74 vs prior close $10,367.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,367.74 | ▲ close $10,367.74 vs 09:30 $10,367.74 (session +0.00) | 16:00 close · cash $10,367.74 · no lots left · equity $10,367.74. | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,367.74 | ▲ 09:30 equity $10,367.74 vs yday $10,367.74 (+0.00) | 09:30 open · cash $10,367.74 · no holdings · equity $10,367.74 vs prior close $10,367.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,367.74 | ▲ close $10,367.74 vs 09:30 $10,367.74 (session +0.00) | 16:00 close · cash $10,367.74 · no lots left · equity $10,367.74. | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,367.74 | ▲ 09:30 equity $10,367.74 vs yday $10,367.74 (+0.00) | 09:30 open · cash $10,367.74 · no holdings · equity $10,367.74 vs prior close $10,367.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,367.74 | ▲ close $10,367.74 vs 09:30 $10,367.74 (session +0.00) | 16:00 close · cash $10,367.74 · no lots left · equity $10,367.74. | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,367.74 | ▲ 09:30 equity $10,367.74 vs yday $10,367.74 (+0.00) | 09:30 open · cash $10,367.74 · no holdings · equity $10,367.74 vs prior close $10,367.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,367.74 | ▲ close $10,367.74 vs 09:30 $10,367.74 (session +0.00) | 16:00 close · cash $10,367.74 · no lots left · equity $10,367.74. | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,367.74 | ▲ 09:30 equity $10,367.74 vs yday $10,367.74 (+0.00) | 09:30 open · cash $10,367.74 · no holdings · equity $10,367.74 vs prior close $10,367.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,367.74 | ▲ close $10,367.74 vs 09:30 $10,367.74 (session +0.00) | 16:00 close · cash $10,367.74 · no lots left · equity $10,367.74. | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,367.74 | ▲ 09:30 equity $10,367.74 vs yday $10,367.74 (+0.00) | 09:30 open · cash $10,367.74 · no holdings · equity $10,367.74 vs prior close $10,367.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,367.74 | ▲ close $10,367.74 vs 09:30 $10,367.74 (session +0.00) | 16:00 close · cash $10,367.74 · no lots left · equity $10,367.74. | — |
| 2026-10-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,367.74 | ▲ 09:30 equity $10,367.74 vs yday $10,367.74 (+0.00) | 09:30 open · cash $10,367.74 · no holdings · equity $10,367.74 vs prior close $10,367.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,367.74 | ▲ close $10,367.74 vs 09:30 $10,367.74 (session +0.00) | 16:00 close · cash $10,367.74 · no lots left · equity $10,367.74. | — |
| 2026-10-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,367.74 | ▲ 09:30 equity $10,367.74 vs yday $10,367.74 (+0.00) | 09:30 open · cash $10,367.74 · no holdings · equity $10,367.74 vs prior close $10,367.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,367.74 | ▲ close $10,367.74 vs 09:30 $10,367.74 (session +0.00) | 16:00 close · cash $10,367.74 · no lots left · equity $10,367.74. | — |
| 2026-10-12 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,367.74 | ▲ 09:30 equity $10,367.74 vs yday $10,367.74 (+0.00) | 09:30 open · cash $10,367.74 · no holdings · equity $10,367.74 vs prior close $10,367.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-12 09:30 ET | **BUY** | `ALHC` | 1381 | $7.49 | $17.81 | — | $6.24 | — | combo gate; gate macd_up=True,rsi_os=True; list yday_mover; 🔵; ret5=-5.0; leftover $10367.74 | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-12 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.24 | ▲ close $11,178.53 vs 09:30 $10,367.74 (session +828.60) | 16:00 close · cash $6.24 · equity $11,178.53 vs 09:30 $10,367.74 (+810.79; session marks +828.60) · 1 name(s) marked open→close (per-name table). ALHC×1381 09:30 $7.49 → close $8.09 +828.60 | — |
| 2026-10-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.24 | ▲ 09:30 equity $11,482.35 vs yday $11,178.53 (+303.82) | 09:30 open · cash $6.24 (unchanged overnight, no fees) · equity $11,482.35 vs prior close $11,178.53 (+303.82) · 1 name(s) re-marked at the open (per-name table). ALHC×1381 yday $8.09 → 09:30 $8.31 +303.82 | — |
| 2026-10-13 09:30 ET | **SELL** | `ALHC` | 1381 | $8.31 | $18.14 | $+1096.47 | $11,464.21 | ▲ +1,096.47 after sell → book $11,464.21; vs 09:30 mark -18.14 | dropped from list after 1 sess (min 1) | — |
| 2026-10-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,464.21 | ▲ close $11,464.21 vs 09:30 $11,482.35 (session +0.00) | 16:00 close · cash $11,464.21 · no lots left · equity $11,464.21. | — |
| 2026-10-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,464.21 | ▲ 09:30 equity $11,464.21 vs yday $11,464.21 (+0.00) | 09:30 open · cash $11,464.21 · no holdings · equity $11,464.21 vs prior close $11,464.21 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-14 09:30 ET | **BUY** | `LW` | 55 | $52.02 | $2.15 | — | $8,600.95 | — | combo gate; gate macd_up=True,rsi_os=True; list ohlc_hot; 🔵; ret5=+8.5; leftover $2866.05 | join🔴 sector🟡 gen🔴 news🟡 digest🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-14 09:30 ET | **BUY** | `BAC` | 57 | $49.83 | $2.16 | — | $5,758.48 | — | combo gate; gate macd_up=True,rsi_os=True; list earn_react; 🔵; ret5=-0.2; leftover $2866.05 | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-10-14 09:30 ET | **BUY** | `MS` | 15 | $180.45 | $2.04 | — | $3,049.70 | — | combo gate; gate macd_up=True,rsi_os=True; list earn_react; 🔵; ret5=-2.3; leftover $2866.05 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,049.70 | ▲ close $11,617.68 vs 09:30 $11,464.21 (session +159.82) | 16:00 close · cash $3,049.70 · equity $11,617.68 vs 09:30 $11,464.21 (+153.47; session marks +159.82) · 3 name(s) marked open→close (per-name table). LW×55 09:30 $52.02 → close $53.02 +55.00; BAC×57 09:30 $49.83 → close $51.39 +88.92; MS×15 09:30 $180.45 → close $181.51 +15.90 | — |
| 2026-10-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,049.70 | ▲ 09:30 equity $11,678.72 vs yday $11,617.68 (+61.04) | 09:30 open · cash $3,049.70 (unchanged overnight, no fees) · equity $11,678.72 vs prior close $11,617.68 (+61.04) · 3 name(s) re-marked at the open (per-name table). BAC×57 yday $51.39 → 09:30 $51.46 +3.99; LW×55 yday $53.02 → 09:30 $53.79 +42.35; MS×15 yday $181.51 → 09:30 $182.49 +14.70 | — |
| 2026-10-15 09:30 ET | **SELL** | `BAC` | 57 | $51.46 | $2.19 | $+88.55 | $5,980.73 | ▲ +88.55 after sell → book $11,676.53; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-10-15 09:30 ET | **SELL** | `MS` | 15 | $182.49 | $2.07 | $+26.50 | $8,716.01 | ▲ +26.50 after sell → book $11,674.46; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-10-15 09:30 ET | **BUY** | `FIS` | 59 | $36.40 | $2.17 | — | $6,566.24 | — | combo gate; gate macd_up=True,rsi_os=True; list flatten; 🔵; ret5=+4.9; leftover $2179.00 | join🔴 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-10-15 09:30 ET | **BUY** | `PNC` | 10 | $208.49 | $2.02 | — | $4,479.32 | — | combo gate; gate macd_up=True,rsi_os=True; list earn_react; ret5=+0.0; leftover $2179.00 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-15 09:30 ET | **BUY** | `JBHT` | 9 | $232.93 | $2.02 | — | $2,380.93 | — | combo gate; gate macd_up=True,rsi_os=True; list overnight; 🔵; ret5=+4.3; leftover $2179.00 | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,380.93 | ▲ close $11,761.34 vs 09:30 $11,678.72 (session +93.09) | 16:00 close · cash $2,380.93 · equity $11,761.34 vs 09:30 $11,678.72 (+82.62; session marks +93.09) · 4 name(s) marked open→close (per-name table). LW×55 09:30 $53.79 → close $54.21 +23.10; FIS×59 09:30 $36.40 → close $36.13 -15.93; PNC×10 09:30 $208.49 → close $214.94 +64.50; JBHT×9 09:30 $232.93 → close $235.31 +21.42 | — |
| 2026-10-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,380.93 | ▼ 09:30 equity $11,743.91 vs yday $11,761.34 (-17.43) | 09:30 open · cash $2,380.93 (unchanged overnight, no fees) · equity $11,743.91 vs prior close $11,761.34 (-17.43) · 4 name(s) re-marked at the open (per-name table). FIS×59 yday $36.13 → 09:30 $36.42 +17.11; JBHT×9 yday $235.31 → 09:30 $228.00 -65.79; LW×55 yday $54.21 → 09:30 $54.48 +14.85; PNC×10 yday $214.94 → 09:30 $216.58 +16.40 | — |
| 2026-10-16 09:30 ET | **SELL** | `FIS` | 59 | $36.42 | $2.19 | $-3.18 | $4,527.52 | ▼ -3.18 after sell → book $11,741.72; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-10-16 09:30 ET | **SELL** | `PNC` | 10 | $216.58 | $2.05 | $+76.83 | $6,691.27 | ▲ +76.83 after sell → book $11,739.67; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-10-16 09:30 ET | **BUY** | `DGX` | 4 | $239.67 | $2.00 | — | $5,730.59 | — | combo gate; gate macd_up=True,rsi_os=True; list yday_mover; 🔵; ret5=+3.6; leftover $1115.21 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-16 09:30 ET | **BUY** | `PLNT` | 22 | $49.48 | $2.06 | — | $4,639.97 | — | combo gate; gate macd_up=True,rsi_os=True; list ohlc_hot; 🔵; ret5=+12.2; leftover $1115.21 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-16 09:30 ET | **BUY** | `WM` | 4 | $224.78 | $2.00 | — | $3,738.85 | — | combo gate; gate macd_up=True,rsi_os=True; list ohlc_hot; ret5=+6.7; leftover $1115.21 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-16 09:30 ET | **BUY** | `EFX` | 6 | $161.77 | $2.01 | — | $2,766.22 | — | combo gate; gate macd_up=True,rsi_os=True; list ohlc_hot; ret5=+10.8; leftover $1115.21 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-16 09:30 ET | **BUY** | `INTU` | 3 | $332.34 | $2.00 | — | $1,767.20 | — | combo gate; gate macd_up=True,rsi_os=True; list ohlc_hot; 🔵; ret5=+8.7; leftover $1115.21 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 judge🔴 ab🟡 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,767.20 | ▼ close $11,684.30 vs 09:30 $11,743.91 (session -45.30) | 16:00 close · cash $1,767.20 · equity $11,684.30 vs 09:30 $11,743.91 (-59.61; session marks -45.30) · 7 name(s) marked open→close (per-name table). JBHT×9 09:30 $228.00 → close $228.78 +7.02; LW×55 09:30 $54.48 → close $53.59 -48.95; DGX×4 09:30 $239.67 → close $235.00 -18.68; PLNT×22 09:30 $49.48 → close $50.92 +31.68; WM×4 09:30 $224.78 → close $226.86 +8.32; EFX×6 09:30 $161.77 → close $161.25 -3.12; INTU×3 09:30 $332.34 → close $325.15 -21.57 | — |
| 2026-10-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,767.20 | ▼ 09:30 equity $11,602.64 vs yday $11,684.30 (-81.66) | 09:30 open · cash $1,767.20 (unchanged overnight, no fees) · equity $11,602.64 vs prior close $11,684.30 (-81.66) · 7 name(s) re-marked at the open (per-name table). DGX×4 yday $235.00 → 09:30 $234.40 -2.40; EFX×6 yday $161.25 → 09:30 $159.08 -13.02; INTU×3 yday $325.15 → 09:30 $322.63 -7.56; JBHT×9 yday $228.78 → 09:30 $227.30 -13.32; LW×55 yday $53.59 → 09:30 $52.87 -39.60; PLNT×22 yday $50.92 → 09:30 $51.08 +3.52; WM×4 yday $226.86 → 09:30 $224.54 -9.28 | — |
| 2026-10-19 09:30 ET | **SELL** | `EFX` | 6 | $159.08 | $2.03 | $-20.18 | $2,719.65 | ▼ -20.18 after sell → book $11,600.61; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-10-19 09:30 ET | **SELL** | `INTU` | 3 | $322.63 | $2.02 | $-33.15 | $3,685.52 | ▼ -33.15 after sell → book $11,598.59; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-10-19 09:30 ET | **SELL** | `JBHT` | 9 | $227.30 | $2.04 | $-54.73 | $5,729.18 | ▼ -54.73 after sell → book $11,596.55; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-10-19 09:30 ET | **SELL** | `LW` | 55 | $52.87 | $2.19 | $+42.41 | $8,634.84 | ▲ +42.41 after sell → book $11,594.36; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 1) | — |
| 2026-10-19 09:30 ET | **BUY** | `ALL` | 35 | $245.91 | $2.10 | — | $25.90 | — | combo gate; gate macd_up=True,rsi_os=True; list ohlc_hot; ret5=+8.0; leftover $8634.84 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.90 | ▲ close $11,674.51 vs 09:30 $11,602.64 (session +82.24) | 16:00 close · cash $25.90 · equity $11,674.51 vs 09:30 $11,602.64 (+71.87; session marks +82.24) · 4 name(s) marked open→close (per-name table). DGX×4 09:30 $234.40 → close $234.91 +2.04; PLNT×22 09:30 $51.08 → close $52.89 +39.82; WM×4 09:30 $224.54 → close $226.41 +7.48; ALL×35 09:30 $245.91 → close $246.85 +32.90 | — |
| 2026-10-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.90 | ▲ 09:30 equity $11,805.72 vs yday $11,674.51 (+131.21) | 09:30 open · cash $25.90 (unchanged overnight, no fees) · equity $11,805.72 vs prior close $11,674.51 (+131.21) · 4 name(s) re-marked at the open (per-name table). ALL×35 yday $246.85 → 09:30 $250.00 +110.25; DGX×4 yday $234.91 → 09:30 $236.00 +4.36; PLNT×22 yday $52.89 → 09:30 $53.21 +7.04; WM×4 yday $226.41 → 09:30 $228.80 +9.56 | — |
| 2026-10-20 09:30 ET | **SELL** | `ALL` | 35 | $250.00 | $2.17 | $+138.88 | $8,773.73 | ▲ +138.88 after sell → book $11,803.55; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-10-20 09:30 ET | **SELL** | `DGX` | 4 | $236.00 | $2.02 | $-18.70 | $9,715.70 | ▼ -18.70 after sell → book $11,801.52; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-20 09:30 ET | **SELL** | `WM` | 4 | $228.80 | $2.02 | $+12.06 | $10,628.88 | ▲ +12.06 after sell → book $11,799.50; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-10-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,628.88 | ▼ close $11,794.00 vs 09:30 $11,805.72 (session -5.50) | 16:00 close · cash $10,628.88 · equity $11,794.00 vs 09:30 $11,805.72 (-11.72; session marks -5.50) · 1 name(s) marked open→close (per-name table). PLNT×22 09:30 $53.21 → close $52.96 -5.50 | — |
| 2026-10-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,628.88 | ▼ 09:30 equity $11,781.24 vs yday $11,794.00 (-12.76) | 09:30 open · cash $10,628.88 (unchanged overnight, no fees) · equity $11,781.24 vs prior close $11,794.00 (-12.76) · 1 name(s) re-marked at the open (per-name table). PLNT×22 yday $52.96 → 09:30 $52.38 -12.76 | — |
| 2026-10-21 09:30 ET | **BUY** | `DGX` | 9 | $235.34 | $2.02 | — | $8,508.80 | — | combo gate; gate macd_up=True,rsi_os=True; list yday_mover; ret5=+2.9; leftover $2125.78 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-21 09:30 ET | **BUY** | `QTWO` | 32 | $65.96 | $2.09 | — | $6,396.00 | — | combo gate; gate macd_up=True,rsi_os=True; list ohlc_hot; 🔵; ret5=+11.3; leftover $2125.78 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-21 09:30 ET | **BUY** | `BROS` | 49 | $43.05 | $2.14 | — | $4,284.41 | — | combo gate; gate macd_up=True,rsi_os=True; list ohlc_hot; ret5=+15.4; leftover $2125.78 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-21 09:30 ET | **BUY** | `BRO` | 30 | $70.29 | $2.08 | — | $2,173.63 | — | combo gate; gate macd_up=True,rsi_os=True; list ohlc_hot; 🔵; ret5=+10.7; leftover $2125.78 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,173.63 | ▲ close $11,772.94 vs 09:30 $11,781.24 (session +0.02) | 16:00 close · cash $2,173.63 · equity $11,772.94 vs 09:30 $11,781.24 (-8.30; session marks +0.02) · 5 name(s) marked open→close (per-name table). PLNT×22 09:30 $52.38 → close $51.69 -15.18; DGX×9 09:30 $235.34 → close $238.93 +32.31; QTWO×32 09:30 $65.96 → close $66.41 +14.40; BROS×49 09:30 $43.05 → close $42.26 -38.71; BRO×30 09:30 $70.29 → close $70.53 +7.20 | — |
| 2026-10-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,173.63 | ▼ 09:30 equity $11,647.07 vs yday $11,772.94 (-125.87) | 09:30 open · cash $2,173.63 (unchanged overnight, no fees) · equity $11,647.07 vs prior close $11,772.94 (-125.87) · 5 name(s) re-marked at the open (per-name table). BRO×30 yday $70.53 → 09:30 $71.47 +28.20; BROS×49 yday $42.26 → 09:30 $40.78 -72.52; DGX×9 yday $238.93 → 09:30 $226.50 -111.87; PLNT×22 yday $51.69 → 09:30 $52.21 +11.44; QTWO×32 yday $66.41 → 09:30 $67.00 +18.88 | — |
| 2026-10-22 09:30 ET | **SELL** | `BRO` | 30 | $71.47 | $2.11 | $+31.21 | $4,315.62 | ▲ +31.21 after sell → book $11,644.96; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-10-22 09:30 ET | **SELL** | `BROS` | 49 | $40.78 | $2.16 | $-115.53 | $6,311.68 | ▼ -115.53 after sell → book $11,642.80; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-10-22 09:30 ET | **SELL** | `PLNT` | 22 | $52.21 | $2.08 | $+55.93 | $7,458.22 | ▲ +55.93 after sell → book $11,640.72; vs 09:30 mark -2.08 | dropped from list after 4 sess (min 1) | — |
| 2026-10-22 09:30 ET | **SELL** | `QTWO` | 32 | $67.00 | $2.11 | $+29.08 | $9,600.11 | ▲ +29.08 after sell → book $11,638.61; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-10-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,600.11 | ▲ close $11,789.63 vs 09:30 $11,647.07 (session +151.02) | 16:00 close · cash $9,600.11 · equity $11,789.63 vs 09:30 $11,647.07 (+142.56; session marks +151.02) · 1 name(s) marked open→close (per-name table). DGX×9 09:30 $226.50 → close $243.28 +151.02 | — |
