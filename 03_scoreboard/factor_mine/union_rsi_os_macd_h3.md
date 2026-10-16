# Factor mine action — `union_rsi_os_macd_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+21.10%** ($12,110) · signal-only (no cash/fees) was +8.48%. Starts YES **1/30**. Fills 21 · skips 9 · realized $-574.52.

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
- **Gate** `macd_up=True,rsi_os=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,425.48.

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
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.33 | ▲ close $9,506.88 vs 09:30 $9,180.77 (session +326.11) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.33 | ▼ 09:30 equity $9,472.73 vs yday $9,506.88 (-34.15) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.33 | ▲ close $9,527.28 vs 09:30 $9,472.73 (session +54.55) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.33 | ▲ 09:30 equity $9,537.57 vs yday $9,527.28 (+10.29) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `SION` | 683 | $7.27 | $8.96 | $-45.09 | $4,957.77 | ▼ -45.09 after sell → book $9,528.60; vs 09:30 mark -8.97 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `EVTL` | 7695 | $0.59 | $70.11 | $-496.41 | $9,458.50 | ▼ -496.41 after sell → book $9,458.50; vs 09:30 mark -70.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,458.50 | ▲ close $9,458.50 vs 09:30 $9,537.57 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,458.50 | ▲ 09:30 equity $9,458.50 vs yday $9,458.50 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,458.50 | ▲ close $9,458.50 vs 09:30 $9,458.50 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,458.50 | ▲ 09:30 equity $9,458.50 vs yday $9,458.50 (-0.00) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,458.50 | ▲ close $9,458.50 vs 09:30 $9,458.50 (session +0.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,458.50 | ▲ 09:30 equity $9,458.50 vs yday $9,458.50 (-0.00) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,458.50 | ▲ close $9,458.50 vs 09:30 $9,458.50 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,458.50 | ▲ 09:30 equity $9,458.50 vs yday $9,458.50 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,458.50 | ▲ close $9,458.50 vs 09:30 $9,458.50 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,458.50 | ▲ 09:30 equity $9,458.50 vs yday $9,458.50 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `ZSQR` | 2021 | $2.34 | $26.07 | — | $4,703.28 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; 🔵; ret5=-25.2; leftover $4729.25 | — |
| 2026-09-16 09:30 ET | **BUY** | `EYPT` | 1280 | $3.66 | $16.51 | — | $1.97 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; ret5=-19.7; leftover $4729.25 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.97 | ▼ close $9,046.06 vs 09:30 $9,458.50 (session -369.85) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.97 | ▲ 09:30 equity $9,320.92 vs yday $9,046.06 (+274.86) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.97 | ▲ close $10,242.51 vs 09:30 $9,320.92 (session +921.59) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.97 | ▼ 09:30 equity $10,110.47 vs yday $10,242.51 (-132.04) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.97 | ▲ close $10,346.25 vs 09:30 $10,110.47 (session +235.78) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.97 | ▲ 09:30 equity $10,371.85 vs yday $10,346.25 (+25.60) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `ZSQR` | 2021 | $2.68 | $26.45 | $+634.62 | $5,391.80 | ▲ +634.62 after sell → book $10,345.40; vs 09:30 mark -26.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 1280 | $3.87 | $16.76 | $+235.52 | $10,328.64 | ▲ +235.52 after sell → book $10,328.64; vs 09:30 mark -16.76 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 1717 | $6.00 | $22.15 | — | $4.49 | — | combo gate; gate rsi_os=True,macd_up=True; list yday_mover; ret5=-24.1; leftover $10328.64 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.49 | ▲ close $10,306.49 vs 09:30 $10,371.85 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.49 | ▼ 09:30 equity $10,289.32 vs yday $10,306.49 (-17.17) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.49 | ▲ close $10,340.83 vs 09:30 $10,289.32 (session +51.51) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.49 | ▲ 09:30 equity $10,358.00 vs yday $10,340.83 (+17.17) | — | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.49 | ▼ close $9,465.16 vs 09:30 $10,358.00 (session -892.84) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.49 | ▼ 09:30 equity $9,447.99 vs yday $9,465.16 (-17.17) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `SION` | 1717 | $5.50 | $22.51 | $-903.16 | $9,425.48 | ▼ -903.16 after sell → book $9,425.48; vs 09:30 mark -22.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,425.48 | ▲ close $9,425.48 vs 09:30 $9,447.99 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,998.02 | ▲ 09:30 equity $10,998.02 vs yday $10,998.02 (+0.00) | 09:30 open · cash $10,998.02 · no holdings · equity $10,998.02 vs prior close $10,998.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,998.02 | ▲ close $10,998.02 vs 09:30 $10,998.02 (session +0.00) | 16:00 close · cash $10,998.02 · no lots left · equity $10,998.02. | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,998.02 | ▲ 09:30 equity $10,998.02 vs yday $10,998.02 (+0.00) | 09:30 open · cash $10,998.02 · no holdings · equity $10,998.02 vs prior close $10,998.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,998.02 | ▲ close $10,998.02 vs 09:30 $10,998.02 (session +0.00) | 16:00 close · cash $10,998.02 · no lots left · equity $10,998.02. | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,998.02 | ▲ 09:30 equity $10,998.02 vs yday $10,998.02 (+0.00) | 09:30 open · cash $10,998.02 · no holdings · equity $10,998.02 vs prior close $10,998.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,998.02 | ▲ close $10,998.02 vs 09:30 $10,998.02 (session +0.00) | 16:00 close · cash $10,998.02 · no lots left · equity $10,998.02. | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,998.02 | ▲ 09:30 equity $10,998.02 vs yday $10,998.02 (+0.00) | 09:30 open · cash $10,998.02 · no holdings · equity $10,998.02 vs prior close $10,998.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,998.02 | ▲ close $10,998.02 vs 09:30 $10,998.02 (session +0.00) | 16:00 close · cash $10,998.02 · no lots left · equity $10,998.02. | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,998.02 | ▲ 09:30 equity $10,998.02 vs yday $10,998.02 (+0.00) | 09:30 open · cash $10,998.02 · no holdings · equity $10,998.02 vs prior close $10,998.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,998.02 | ▲ close $10,998.02 vs 09:30 $10,998.02 (session +0.00) | 16:00 close · cash $10,998.02 · no lots left · equity $10,998.02. | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,998.02 | ▲ 09:30 equity $10,998.02 vs yday $10,998.02 (+0.00) | 09:30 open · cash $10,998.02 · no holdings · equity $10,998.02 vs prior close $10,998.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,998.02 | ▲ close $10,998.02 vs 09:30 $10,998.02 (session +0.00) | 16:00 close · cash $10,998.02 · no lots left · equity $10,998.02. | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,998.02 | ▲ 09:30 equity $10,998.02 vs yday $10,998.02 (+0.00) | 09:30 open · cash $10,998.02 · no holdings · equity $10,998.02 vs prior close $10,998.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,998.02 | ▲ close $10,998.02 vs 09:30 $10,998.02 (session +0.00) | 16:00 close · cash $10,998.02 · no lots left · equity $10,998.02. | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,998.02 | ▲ 09:30 equity $10,998.02 vs yday $10,998.02 (+0.00) | 09:30 open · cash $10,998.02 · no holdings · equity $10,998.02 vs prior close $10,998.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,998.02 | ▲ close $10,998.02 vs 09:30 $10,998.02 (session +0.00) | 16:00 close · cash $10,998.02 · no lots left · equity $10,998.02. | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,998.02 | ▲ 09:30 equity $10,998.02 vs yday $10,998.02 (+0.00) | 09:30 open · cash $10,998.02 · no holdings · equity $10,998.02 vs prior close $10,998.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,998.02 | ▲ close $10,998.02 vs 09:30 $10,998.02 (session +0.00) | 16:00 close · cash $10,998.02 · no lots left · equity $10,998.02. | — |
| 2026-10-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,998.02 | ▲ 09:30 equity $10,998.02 vs yday $10,998.02 (+0.00) | 09:30 open · cash $10,998.02 · no holdings · equity $10,998.02 vs prior close $10,998.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,998.02 | ▲ close $10,998.02 vs 09:30 $10,998.02 (session +0.00) | 16:00 close · cash $10,998.02 · no lots left · equity $10,998.02. | — |
| 2026-10-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,998.02 | ▲ 09:30 equity $10,998.02 vs yday $10,998.02 (+0.00) | 09:30 open · cash $10,998.02 · no holdings · equity $10,998.02 vs prior close $10,998.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,998.02 | ▲ close $10,998.02 vs 09:30 $10,998.02 (session +0.00) | 16:00 close · cash $10,998.02 · no lots left · equity $10,998.02. | — |
| 2026-10-12 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,998.02 | ▲ 09:30 equity $10,998.02 vs yday $10,998.02 (+0.00) | 09:30 open · cash $10,998.02 · no holdings · equity $10,998.02 vs prior close $10,998.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-12 09:30 ET | **BUY** | `ALHC` | 1465 | $7.49 | $18.90 | — | $6.27 | — | combo gate; gate macd_up=True,rsi_os=True; list yday_mover; 🔵; ret5=-5.0; leftover $10998.02 | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-12 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.27 | ▲ close $11,858.12 vs 09:30 $10,998.02 (session +879.00) | 16:00 close · cash $6.27 · equity $11,858.12 vs 09:30 $10,998.02 (+860.10; session marks +879.00) · 1 name(s) marked open→close (per-name table). ALHC×1465 09:30 $7.49 → close $8.09 +879.00 | — |
| 2026-10-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.27 | ▲ 09:30 equity $12,180.42 vs yday $11,858.12 (+322.30) | 09:30 open · cash $6.27 (unchanged overnight, no fees) · equity $12,180.42 vs prior close $11,858.12 (+322.30) · 1 name(s) re-marked at the open (per-name table). ALHC×1465 yday $8.09 → 09:30 $8.31 +322.30 | — |
| 2026-10-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.27 | ▼ close $12,165.77 vs 09:30 $12,180.42 (session -14.65) | 16:00 close · cash $6.27 · equity $12,165.77 vs 09:30 $12,180.42 (-14.65; session marks -14.65) · 1 name(s) marked open→close (per-name table). ALHC×1465 09:30 $8.31 → close $8.30 -14.65 | — |
| 2026-10-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.27 | ▲ 09:30 equity $12,290.30 vs yday $12,165.77 (+124.52) | 09:30 open · cash $6.27 (unchanged overnight, no fees) · equity $12,290.30 vs prior close $12,165.77 (+124.52) · 1 name(s) re-marked at the open (per-name table). ALHC×1465 yday $8.30 → 09:30 $8.38 +124.52 | — |
| 2026-10-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.27 | ▼ close $12,063.22 vs 09:30 $12,290.30 (session -227.07) | 16:00 close · cash $6.27 · equity $12,063.22 vs 09:30 $12,290.30 (-227.08; session marks -227.07) · 1 name(s) marked open→close (per-name table). ALHC×1465 09:30 $8.38 → close $8.23 -227.07 | — |
| 2026-10-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.27 | ▲ 09:30 equity $12,121.82 vs yday $12,063.22 (+58.60) | 09:30 open · cash $6.27 (unchanged overnight, no fees) · equity $12,121.82 vs prior close $12,063.22 (+58.60) · 1 name(s) re-marked at the open (per-name table). ALHC×1465 yday $8.23 → 09:30 $8.27 +58.60 | — |
| 2026-10-15 09:30 ET | **SELL** | `ALHC` | 1465 | $8.27 | $19.24 | $+1104.56 | $12,102.58 | ▲ +1,104.56 after sell → book $12,102.58; vs 09:30 mark -19.24 | dropped from list after 3 sess (min 3) | — |
| 2026-10-15 09:30 ET | **BUY** | `FIS` | 66 | $36.40 | $2.19 | — | $9,697.99 | — | combo gate; gate macd_up=True,rsi_os=True; list flatten; 🔵; ret5=+4.9; leftover $2420.52 | join🔴 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-10-15 09:30 ET | **BUY** | `LW` | 44 | $53.79 | $2.12 | — | $7,329.11 | — | combo gate; gate macd_up=True,rsi_os=True; list ohlc_hot; ret5=+10.3; leftover $2420.52 | join🔴 sector🟡 gen🔴 news🟡 digest🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-15 09:30 ET | **BUY** | `PNC` | 11 | $208.49 | $2.02 | — | $5,033.70 | — | combo gate; gate macd_up=True,rsi_os=True; list earn_react; ret5=+0.0; leftover $2420.52 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-15 09:30 ET | **BUY** | `JBHT` | 10 | $232.93 | $2.02 | — | $2,702.38 | — | combo gate; gate macd_up=True,rsi_os=True; list overnight; 🔵; ret5=+4.3; leftover $2420.52 | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,702.38 | ▲ close $12,189.64 vs 09:30 $12,121.82 (session +95.41) | 16:00 close · cash $2,702.38 · equity $12,189.64 vs 09:30 $12,121.82 (+67.82; session marks +95.41) · 4 name(s) marked open→close (per-name table). FIS×66 09:30 $36.40 → close $36.13 -17.82; LW×44 09:30 $53.79 → close $54.21 +18.48; PNC×11 09:30 $208.49 → close $214.94 +70.95; JBHT×10 09:30 $232.93 → close $235.31 +23.80 | — |
| 2026-10-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,702.38 | ▼ 09:30 equity $12,165.60 vs yday $12,189.64 (-24.04) | 09:30 open · cash $2,702.38 (unchanged overnight, no fees) · equity $12,165.60 vs prior close $12,189.64 (-24.04) · 4 name(s) re-marked at the open (per-name table). FIS×66 yday $36.13 → 09:30 $36.42 +19.14; JBHT×10 yday $235.31 → 09:30 $228.00 -73.10; LW×44 yday $54.21 → 09:30 $54.48 +11.88; PNC×11 yday $214.94 → 09:30 $216.58 +18.04 | — |
| 2026-10-16 09:30 ET | **BUY** | `DGX` | 1 | $239.67 | $1.99 | — | $2,460.72 | — | combo gate; gate macd_up=True,rsi_os=True; list yday_mover; 🔵; ret5=+3.6; leftover $450.40 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-16 09:30 ET | **BUY** | `PLNT` | 9 | $49.48 | $2.02 | — | $2,013.38 | — | combo gate; gate macd_up=True,rsi_os=True; list ohlc_hot; 🔵; ret5=+12.2; leftover $450.40 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-16 09:30 ET | **BUY** | `WM` | 2 | $224.78 | $2.00 | — | $1,561.82 | — | combo gate; gate macd_up=True,rsi_os=True; list ohlc_hot; ret5=+6.7; leftover $450.40 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-16 09:30 ET | **BUY** | `EFX` | 2 | $161.77 | $2.00 | — | $1,236.29 | — | combo gate; gate macd_up=True,rsi_os=True; list ohlc_hot; ret5=+10.8; leftover $450.40 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-16 09:30 ET | **BUY** | `INTU` | 1 | $332.34 | $1.99 | — | $901.96 | — | combo gate; gate macd_up=True,rsi_os=True; list ohlc_hot; 🔵; ret5=+8.7; leftover $450.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 judge🔴 ab🟡 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $901.96 | ▼ close $12,110.31 vs 09:30 $12,165.60 (session -45.29) | 16:00 close · cash $901.96 · equity $12,110.31 vs 09:30 $12,165.60 (-55.29; session marks -45.29) · 9 name(s) marked open→close (per-name table). FIS×66 09:30 $36.42 → close $36.01 -27.06; JBHT×10 09:30 $228.00 → close $228.78 +7.80; LW×44 09:30 $54.48 → close $53.59 -39.16; PNC×11 09:30 $216.58 → close $217.39 +8.91; DGX×1 09:30 $239.67 → close $235.00 -4.67; PLNT×9 09:30 $49.48 → close $50.92 +12.96; WM×2 09:30 $224.78 → close $226.86 +4.16; EFX×2 09:30 $161.77 → close $161.25 -1.04; INTU×1 09:30 $332.34 → close $325.15 -7.19 | — |

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
| 2026-09-22 | `SION` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
