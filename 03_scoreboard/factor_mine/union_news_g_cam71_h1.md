# Factor mine action — `union_news_g_cam71_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · merged news🟢 and cameras +7 −≤1

Cash book **-10.46%** ($8,954) · signal-only (no cash/fees) was -4.11%. Starts YES **0/22**. Fills 50 · skips 2 · realized $-1046.31.

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
- Must-have: the news camera (does the morning packet like the headline?) is green.
- Must-have: at least 7 green cameras (the +G half of +G −R).
- Must-have: at most 1 red cameras (the −R half of +G −R; 🚨 is not counted here).
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
- **Gate** `news=good,n_pos_min=7,cam_bad_max=1` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,953.70.

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
| 2026-08-20 | `BHP` | 54 | — | $91.01 | +0.00 | $93.63 | +141.48 | +141.48 | +0.00 | +141.48 |
| 2026-08-20 | `APA` | 111 | — | $44.76 | +0.00 | $44.39 | -41.07 | -41.07 | +0.00 | -41.07 |
| 2026-08-21 | `BHP` | 54 | $93.63 | $95.72 | +112.86 | — | +0.00 | +112.86 | +254.34 | — |
| 2026-08-21 | `APA` | 111 | $44.39 | $44.52 | +14.43 | — | +0.00 | +14.43 | -26.64 | — |
| 2026-08-21 | `AU` | 17 | — | $119.43 | +0.00 | $121.22 | +30.43 | +30.43 | +0.00 | +30.43 |
| 2026-08-21 | `AUTL` | 827 | — | $2.47 | +0.00 | $2.41 | -49.62 | -49.62 | +0.00 | -49.62 |
| 2026-08-21 | `CRSP` | 34 | — | $59.72 | +0.00 | $59.50 | -7.48 | -7.48 | +0.00 | -7.48 |
| 2026-08-21 | `FUTU` | 17 | — | $115.18 | +0.00 | $123.64 | +143.82 | +143.82 | +0.00 | +143.82 |
| 2026-08-21 | `GRAL` | 25 | — | $78.88 | +0.00 | $79.54 | +16.50 | +16.50 | +0.00 | +16.50 |
| 2026-08-24 | `AU` | 17 | $121.22 | $120.51 | -12.07 | — | +0.00 | -12.07 | +18.36 | — |
| 2026-08-24 | `AUTL` | 827 | $2.41 | $2.40 | -8.27 | — | +0.00 | -8.27 | -57.89 | — |
| 2026-08-24 | `CRSP` | 34 | $59.50 | $58.75 | -25.50 | — | +0.00 | -25.50 | -32.98 | — |
| 2026-08-24 | `FUTU` | 17 | $123.64 | $121.00 | -44.88 | — | +0.00 | -44.88 | +98.94 | — |
| 2026-08-24 | `GRAL` | 25 | $79.54 | $81.87 | +58.25 | — | +0.00 | +58.25 | +74.75 | — |
| 2026-08-25 | `AU` | 43 | — | $118.52 | +0.00 | $123.39 | +209.41 | +209.41 | +0.00 | +209.41 |
| 2026-08-25 | `FCX` | 66 | — | $77.13 | +0.00 | $79.91 | +183.48 | +183.48 | +0.00 | +183.48 |
| 2026-08-26 | `AU` | 43 | $123.39 | $119.80 | -154.37 | — | +0.00 | -154.37 | +55.04 | — |
| 2026-08-26 | `FCX` | 66 | $79.91 | $79.34 | -37.62 | — | +0.00 | -37.62 | +145.86 | — |
| 2026-08-27 | `ACMR` | 25 | — | $81.65 | +0.00 | $80.49 | -29.00 | -29.00 | +0.00 | -29.00 |
| 2026-08-27 | `MU` | 2 | — | $967.01 | +0.00 | $935.39 | -63.24 | -63.24 | +0.00 | -63.24 |
| 2026-08-27 | `ASML` | 1 | — | $1746.53 | +0.00 | $1735.01 | -11.52 | -11.52 | +0.00 | -11.52 |
| 2026-08-27 | `LRCX` | 6 | — | $318.88 | +0.00 | $318.58 | -1.80 | -1.80 | +0.00 | -1.80 |
| 2026-08-27 | `NVDA` | 9 | — | $222.86 | +0.00 | $227.98 | +46.08 | +46.08 | +0.00 | +46.08 |
| 2026-08-28 | `ACMR` | 25 | $80.49 | $79.27 | -30.50 | — | +0.00 | -30.50 | -59.50 | — |
| 2026-08-28 | `MU` | 2 | $935.39 | $919.29 | -32.20 | — | +0.00 | -32.20 | -95.44 | — |
| 2026-08-28 | `ASML` | 1 | $1735.01 | $1734.75 | -0.26 | — | +0.00 | -0.26 | -11.78 | — |
| 2026-08-28 | `LRCX` | 6 | $318.58 | $318.03 | -3.30 | — | +0.00 | -3.30 | -5.10 | — |
| 2026-08-28 | `NVDA` | 9 | $227.98 | $227.36 | -5.58 | — | +0.00 | -5.58 | +40.50 | — |
| 2026-08-28 | `KEYS` | 6 | — | $324.41 | +0.00 | $319.97 | -26.64 | -26.64 | +0.00 | -26.64 |
| 2026-08-28 | `SMTC` | 14 | — | $141.76 | +0.00 | $131.17 | -148.26 | -148.26 | +0.00 | -148.26 |
| 2026-08-28 | `CIEN` | 5 | — | $400.42 | +0.00 | $378.44 | -109.90 | -109.90 | +0.00 | -109.90 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 8 | — | $240.22 | +0.00 | $236.98 | -25.92 | -25.92 | +0.00 | -25.92 |
| 2026-08-31 | `KEYS` | 6 | $319.97 | $322.49 | +15.12 | — | +0.00 | +15.12 | -11.52 | — |
| 2026-08-31 | `SMTC` | 14 | $131.17 | $132.30 | +15.82 | — | +0.00 | +15.82 | -132.44 | — |
| 2026-08-31 | `CIEN` | 5 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -109.90 | — |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | — | +0.00 | +5.64 | -44.13 | — |
| 2026-08-31 | `DDOG` | 8 | $236.98 | $233.97 | -24.12 | — | +0.00 | -24.12 | -50.04 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 14 | — | $351.74 | +0.00 | $357.16 | +75.88 | +75.88 | +0.00 | +75.88 |
| 2026-09-03 | `DELL` | 10 | — | $486.31 | +0.00 | $516.39 | +300.80 | +300.80 | +0.00 | +300.80 |
| 2026-09-04 | `AVGO` | 14 | $357.16 | $359.70 | +35.56 | — | +0.00 | +35.56 | +111.44 | — |
| 2026-09-04 | `DELL` | 10 | $516.39 | $513.78 | -26.10 | — | +0.00 | -26.10 | +274.70 | — |
| 2026-09-04 | `CRM` | 13 | — | $263.36 | +0.00 | $259.23 | -53.69 | -53.69 | +0.00 | -53.69 |
| 2026-09-04 | `FRNM` | 210 | — | $16.40 | +0.00 | $16.31 | -18.90 | -18.90 | +0.00 | -18.90 |
| 2026-09-04 | `MRX` | 45 | — | $75.65 | +0.00 | $78.27 | +117.90 | +117.90 | +0.00 | +117.90 |
| 2026-09-08 | `CRM` | 13 | $259.23 | $253.72 | -71.63 | — | +0.00 | -71.63 | -125.32 | — |
| 2026-09-08 | `FRNM` | 210 | $16.31 | $16.74 | +90.30 | — | +0.00 | +90.30 | +71.40 | — |
| 2026-09-08 | `MRX` | 45 | $78.27 | $78.84 | +25.65 | — | +0.00 | +25.65 | +143.55 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 63 | — | $164.43 | +0.00 | $150.28 | -891.45 | -891.45 | +0.00 | -891.45 |
| 2026-09-14 | `ORCL` | 63 | $150.28 | $141.42 | -558.18 | — | +0.00 | -558.18 | -1449.63 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +100.41 | BHP, APA | — | $112.62 | $10,095.93 | BHP×54, APA×111 |
| 2026-08-21 | +3.25 | $112.62 | BHP×54, APA×111 | $10,223.23 | +127.30 | +133.65 | AU, AUTL, CRSP, FUTU, GRAL | BHP, APA | $166.19 | $10,333.38 | AU×17, AUTL×827, CRSP×34, FUTU×17, GRAL×25 |
| 2026-08-24 | -5.17 | $166.19 | AU×17, AUTL×827, CRSP×34, FUTU×17, GRAL×25 | $10,300.91 | -32.47 | +0.00 | — | AU, AUTL, CRSP, FUTU, GRAL | $10,281.75 | $10,281.75 | — |
| 2026-08-25 | +1.80 | $10,281.75 | — | $10,281.75 | -0.00 | +392.89 | AU, FCX | — | $90.50 | $10,670.33 | AU×43, FCX×66 |
| 2026-08-26 | +2.02 | $90.50 | AU×43, FCX×66 | $10,478.34 | -191.99 | +0.00 | — | AU, FCX | $10,473.93 | $10,473.93 | — |
| 2026-08-27 | — | $10,473.93 | — | $10,473.93 | -0.00 | -59.48 | ACMR, MU, ASML, LRCX, NVDA | — | $823.03 | $10,404.37 | ACMR×25, MU×2, ASML×1, LRCX×6, NVDA×9 |
| 2026-08-28 | +0.75 | $823.03 | ACMR×25, MU×2, ASML×1, LRCX×6, NVDA×9 | $10,332.53 | -71.84 | -360.49 | KEYS, SMTC, CIEN, MPWR, DDOG | ACMR, MU, ASML, LRCX, NVDA | $1,151.28 | $9,951.78 | KEYS×6, SMTC×14, CIEN×5, MPWR×1, DDOG×8 |
| 2026-08-31 | -5.85 | $1,151.28 | KEYS×6, SMTC×14, CIEN×5, MPWR×1, DDOG×8 | $9,964.24 | +12.46 | +0.00 | — | KEYS, SMTC, CIEN, MPWR, DDOG | $9,954.07 | $9,954.07 | — |
| 2026-09-01 | -6.30 | $9,954.07 | — | $9,954.07 | +0.00 | +0.00 | — | — | $9,954.07 | $9,954.07 | — |
| 2026-09-02 | -3.83 | $9,954.07 | — | $9,954.07 | +0.00 | +0.00 | — | — | $9,954.07 | $9,954.07 | — |
| 2026-09-03 | -0.90 | $9,954.07 | — | $9,954.07 | +0.00 | +376.68 | AVGO, DELL | — | $162.56 | $10,326.70 | AVGO×14, DELL×10 |
| 2026-09-04 | +2.25 | $162.56 | AVGO×14, DELL×10 | $10,336.16 | +9.46 | +45.31 | CRM, FRNM, MRX | AVGO, DELL | $53.21 | $10,370.45 | CRM×13, FRNM×210, MRX×45 |
| 2026-09-08 | -11.47 | $53.21 | CRM×13, FRNM×210, MRX×45 | $10,414.77 | +44.32 | +0.00 | — | CRM, FRNM, MRX | $10,407.77 | $10,407.77 | — |
| 2026-09-09 | -13.95 | $10,407.77 | — | $10,407.77 | +0.00 | +0.00 | — | — | $10,407.77 | $10,407.77 | — |
| 2026-09-10 | -13.28 | $10,407.77 | — | $10,407.77 | +0.00 | +0.00 | — | — | $10,407.77 | $10,407.77 | — |
| 2026-09-11 | +0.50 | $10,407.77 | — | $10,407.77 | +0.00 | -891.45 | ORCL | — | $46.50 | $9,514.14 | ORCL×63 |
| 2026-09-14 | -11.00 | $46.50 | ORCL×63 | $8,955.96 | -558.18 | +0.00 | — | ORCL | $8,953.70 | $8,953.70 | — |

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
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 54 | $91.01 | $2.15 | — | $5,083.31 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $5000.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 111 | $44.76 | $2.32 | — | $112.62 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $5000.00 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $112.62 | ▲ close $10,095.93 vs 09:30 $10,000.00 (session +100.41) | 16:00 close · cash $112.62 · equity $10,095.93 vs 09:30 $10,000.00 (+95.93; session marks +100.41) · 2 name(s) marked open→close (per-name table). BHP×54 09:30 $91.01 → close $93.63 +141.48; APA×111 09:30 $44.76 → close $44.39 -41.07 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $112.62 | ▲ 09:30 equity $10,223.23 vs yday $10,095.93 (+127.30) | 09:30 open · cash $112.62 (unchanged overnight, no fees) · equity $10,223.23 vs prior close $10,095.93 (+127.30) · 2 name(s) re-marked at the open (per-name table). BHP×54 yday $93.63 → 09:30 $95.72 +112.86; APA×111 yday $44.39 → 09:30 $44.52 +14.43 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 54 | $95.72 | $2.20 | $+249.98 | $5,279.30 | ▲ +249.98 after sell → book $10,221.02; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 111 | $44.52 | $2.38 | $-31.34 | $10,218.64 | ▼ -31.34 after sell → book $10,218.64; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 17 | $119.43 | $2.04 | — | $8,186.29 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $2043.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 827 | $2.47 | $10.67 | — | $6,132.93 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $2043.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 34 | $59.72 | $2.09 | — | $4,100.36 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $2043.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 17 | $115.18 | $2.04 | — | $2,140.26 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $2043.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 25 | $78.88 | $2.06 | — | $166.19 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $2043.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $166.19 | ▲ close $10,333.38 vs 09:30 $10,223.23 (session +133.65) | 16:00 close · cash $166.19 · equity $10,333.38 vs 09:30 $10,223.23 (+110.15; session marks +133.65) · 5 name(s) marked open→close (per-name table). AU×17 09:30 $119.43 → close $121.22 +30.43; AUTL×827 09:30 $2.47 → close $2.41 -49.62; CRSP×34 09:30 $59.72 → close $59.50 -7.48; FUTU×17 09:30 $115.18 → close $123.64 +143.82; GRAL×25 09:30 $78.88 → close $79.54 +16.50 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $166.19 | ▼ 09:30 equity $10,300.91 vs yday $10,333.38 (-32.47) | 09:30 open · cash $166.19 (unchanged overnight, no fees) · equity $10,300.91 vs prior close $10,333.38 (-32.47) · 5 name(s) re-marked at the open (per-name table). AU×17 yday $121.22 → 09:30 $120.51 -12.07; AUTL×827 yday $2.41 → 09:30 $2.40 -8.27; CRSP×34 yday $59.50 → 09:30 $58.75 -25.50; FUTU×17 yday $123.64 → 09:30 $121.00 -44.88; GRAL×25 yday $79.54 → 09:30 $81.87 +58.25 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 17 | $120.51 | $2.07 | $+14.25 | $2,212.80 | ▲ +14.25 after sell → book $10,298.85; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 827 | $2.40 | $10.82 | $-79.38 | $4,186.77 | ▼ -79.38 after sell → book $10,288.02; vs 09:30 mark -10.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 34 | $58.75 | $2.12 | $-37.19 | $6,182.16 | ▼ -37.19 after sell → book $10,285.91; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 17 | $121.00 | $2.07 | $+94.83 | $8,237.09 | ▲ +94.83 after sell → book $10,283.84; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 25 | $81.87 | $2.09 | $+70.59 | $10,281.75 | ▲ +70.59 after sell → book $10,281.75; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,281.75 | ▲ close $10,281.75 vs 09:30 $10,300.91 (session +0.00) | 16:00 close · cash $10,281.75 · no lots left · equity $10,281.75. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,281.75 | ▲ 09:30 equity $10,281.75 vs yday $10,281.75 (-0.00) | 09:30 open · cash $10,281.75 · no holdings · equity $10,281.75 vs prior close $10,281.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 43 | $118.52 | $2.12 | — | $5,183.27 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $5140.87 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 66 | $77.13 | $2.19 | — | $90.50 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $5140.87 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.50 | ▲ close $10,670.33 vs 09:30 $10,281.75 (session +392.89) | 16:00 close · cash $90.50 · equity $10,670.33 vs 09:30 $10,281.75 (+388.58; session marks +392.89) · 2 name(s) marked open→close (per-name table). AU×43 09:30 $118.52 → close $123.39 +209.41; FCX×66 09:30 $77.13 → close $79.91 +183.48 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.50 | ▼ 09:30 equity $10,478.34 vs yday $10,670.33 (-191.99) | 09:30 open · cash $90.50 (unchanged overnight, no fees) · equity $10,478.34 vs prior close $10,670.33 (-191.99) · 2 name(s) re-marked at the open (per-name table). AU×43 yday $123.39 → 09:30 $119.80 -154.37; FCX×66 yday $79.91 → 09:30 $79.34 -37.62 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 43 | $119.80 | $2.17 | $+50.75 | $5,239.73 | ▲ +50.75 after sell → book $10,476.17; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 66 | $79.34 | $2.24 | $+141.43 | $10,473.93 | ▲ +141.43 after sell → book $10,473.93; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,473.93 | ▲ close $10,473.93 vs 09:30 $10,478.34 (session +0.00) | 16:00 close · cash $10,473.93 · no lots left · equity $10,473.93. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,473.93 | ▲ 09:30 equity $10,473.93 vs yday $10,473.93 (-0.00) | 09:30 open · cash $10,473.93 · no holdings · equity $10,473.93 vs prior close $10,473.93 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 25 | $81.65 | $2.06 | — | $8,430.61 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $2094.79 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 2 | $967.01 | $2.00 | — | $6,494.60 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $2094.79 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ASML` | 1 | $1746.53 | $1.99 | — | $4,746.08 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=-0.3; leftover $2094.79 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 6 | $318.88 | $2.01 | — | $2,830.79 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $2094.79 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 9 | $222.86 | $2.02 | — | $823.03 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $2094.79 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $823.03 | ▼ close $10,404.37 vs 09:30 $10,473.93 (session -59.48) | 16:00 close · cash $823.03 · equity $10,404.37 vs 09:30 $10,473.93 (-69.56; session marks -59.48) · 5 name(s) marked open→close (per-name table). ACMR×25 09:30 $81.65 → close $80.49 -29.00; MU×2 09:30 $967.01 → close $935.39 -63.24; ASML×1 09:30 $1746.53 → close $1735.01 -11.52; LRCX×6 09:30 $318.88 → close $318.58 -1.80; NVDA×9 09:30 $222.86 → close $227.98 +46.08 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $823.03 | ▼ 09:30 equity $10,332.53 vs yday $10,404.37 (-71.84) | 09:30 open · cash $823.03 (unchanged overnight, no fees) · equity $10,332.53 vs prior close $10,404.37 (-71.84) · 5 name(s) re-marked at the open (per-name table). ACMR×25 yday $80.49 → 09:30 $79.27 -30.50; MU×2 yday $935.39 → 09:30 $919.29 -32.20; ASML×1 yday $1735.01 → 09:30 $1734.75 -0.26; LRCX×6 yday $318.58 → 09:30 $318.03 -3.30; NVDA×9 yday $227.98 → 09:30 $227.36 -5.58 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 25 | $79.27 | $2.09 | $-63.66 | $2,802.69 | ▼ -63.66 after sell → book $10,330.44; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 2 | $919.29 | $2.02 | $-99.46 | $4,639.25 | ▼ -99.46 after sell → book $10,328.42; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ASML` | 1 | $1734.75 | $2.02 | $-15.79 | $6,371.98 | ▼ -15.79 after sell → book $10,326.40; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 6 | $318.03 | $2.03 | $-9.14 | $8,278.13 | ▼ -9.14 after sell → book $10,324.37; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 9 | $227.36 | $2.04 | $+36.44 | $10,322.33 | ▲ +36.44 after sell → book $10,322.33; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 6 | $324.41 | $2.01 | — | $8,373.86 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $2064.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 14 | $141.76 | $2.03 | — | $6,387.19 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $2064.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 5 | $400.42 | $2.00 | — | $4,383.08 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $2064.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $3,075.06 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $2064.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 8 | $240.22 | $2.01 | — | $1,151.28 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $2064.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,151.28 | ▼ close $9,951.78 vs 09:30 $10,332.53 (session -360.49) | 16:00 close · cash $1,151.28 · equity $9,951.78 vs 09:30 $10,332.53 (-380.75; session marks -360.49) · 5 name(s) marked open→close (per-name table). KEYS×6 09:30 $324.41 → close $319.97 -26.64; SMTC×14 09:30 $141.76 → close $131.17 -148.26; CIEN×5 09:30 $400.42 → close $378.44 -109.90; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×8 09:30 $240.22 → close $236.98 -25.92 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,151.28 | ▲ 09:30 equity $9,964.24 vs yday $9,951.78 (+12.46) | 09:30 open · cash $1,151.28 (unchanged overnight, no fees) · equity $9,964.24 vs prior close $9,951.78 (+12.46) · 5 name(s) re-marked at the open (per-name table). KEYS×6 yday $319.97 → 09:30 $322.49 +15.12; SMTC×14 yday $131.17 → 09:30 $132.30 +15.82; CIEN×5 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×8 yday $236.98 → 09:30 $233.97 -24.12 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 6 | $322.49 | $2.03 | $-15.56 | $3,084.19 | ▼ -15.56 after sell → book $9,962.21; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 14 | $132.30 | $2.06 | $-136.53 | $4,934.33 | ▼ -136.53 after sell → book $9,960.15; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 5 | $378.44 | $2.03 | $-113.94 | $6,824.50 | ▼ -113.94 after sell → book $9,958.12; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $8,084.39 | ▼ -48.14 after sell → book $9,956.11; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 8 | $233.97 | $2.04 | $-54.09 | $9,954.07 | ▼ -54.09 after sell → book $9,954.07; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,954.07 | ▲ close $9,954.07 vs 09:30 $9,964.24 (session +0.00) | 16:00 close · cash $9,954.07 · no lots left · equity $9,954.07. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,954.07 | ▲ 09:30 equity $9,954.07 vs yday $9,954.07 (+0.00) | 09:30 open · cash $9,954.07 · no holdings · equity $9,954.07 vs prior close $9,954.07 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,954.07 | ▲ close $9,954.07 vs 09:30 $9,954.07 (session +0.00) | 16:00 close · cash $9,954.07 · no lots left · equity $9,954.07. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,954.07 | ▲ 09:30 equity $9,954.07 vs yday $9,954.07 (+0.00) | 09:30 open · cash $9,954.07 · no holdings · equity $9,954.07 vs prior close $9,954.07 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,954.07 | ▲ close $9,954.07 vs 09:30 $9,954.07 (session +0.00) | 16:00 close · cash $9,954.07 · no lots left · equity $9,954.07. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,954.07 | ▲ 09:30 equity $9,954.07 vs yday $9,954.07 (+0.00) | 09:30 open · cash $9,954.07 · no holdings · equity $9,954.07 vs prior close $9,954.07 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 14 | $351.74 | $2.03 | — | $5,027.68 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $4977.04 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 10 | $486.31 | $2.02 | — | $162.56 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $4977.04 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $162.56 | ▲ close $10,326.70 vs 09:30 $9,954.07 (session +376.68) | 16:00 close · cash $162.56 · equity $10,326.70 vs 09:30 $9,954.07 (+372.63; session marks +376.68) · 2 name(s) marked open→close (per-name table). AVGO×14 09:30 $351.74 → close $357.16 +75.88; DELL×10 09:30 $486.31 → close $516.39 +300.80 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $162.56 | ▲ 09:30 equity $10,336.16 vs yday $10,326.70 (+9.46) | 09:30 open · cash $162.56 (unchanged overnight, no fees) · equity $10,336.16 vs prior close $10,326.70 (+9.46) · 2 name(s) re-marked at the open (per-name table). AVGO×14 yday $357.16 → 09:30 $359.70 +35.56; DELL×10 yday $516.39 → 09:30 $513.78 -26.10 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 14 | $359.70 | $2.08 | $+107.33 | $5,196.28 | ▲ +107.33 after sell → book $10,334.08; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 10 | $513.78 | $2.07 | $+270.61 | $10,332.01 | ▲ +270.61 after sell → book $10,332.01; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 13 | $263.36 | $2.03 | — | $6,906.30 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $3444.00 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 210 | $16.40 | $2.71 | — | $3,459.59 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $3444.00 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 45 | $75.65 | $2.12 | — | $53.21 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $3444.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.21 | ▲ close $10,370.45 vs 09:30 $10,336.16 (session +45.31) | 16:00 close · cash $53.21 · equity $10,370.45 vs 09:30 $10,336.16 (+34.29; session marks +45.31) · 3 name(s) marked open→close (per-name table). CRM×13 09:30 $263.36 → close $259.23 -53.69; FRNM×210 09:30 $16.40 → close $16.31 -18.90; MRX×45 09:30 $75.65 → close $78.27 +117.90 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.21 | ▲ 09:30 equity $10,414.77 vs yday $10,370.45 (+44.32) | 09:30 open · cash $53.21 (unchanged overnight, no fees) · equity $10,414.77 vs prior close $10,370.45 (+44.32) · 3 name(s) re-marked at the open (per-name table). CRM×13 yday $259.23 → 09:30 $253.72 -71.63; FRNM×210 yday $16.31 → 09:30 $16.74 +90.30; MRX×45 yday $78.27 → 09:30 $78.84 +25.65 | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 13 | $253.72 | $2.07 | $-129.41 | $3,349.51 | ▼ -129.41 after sell → book $10,412.71; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 210 | $16.74 | $2.77 | $+65.92 | $6,862.13 | ▲ +65.92 after sell → book $10,409.93; vs 09:30 mark -2.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 45 | $78.84 | $2.16 | $+139.26 | $10,407.77 | ▲ +139.26 after sell → book $10,407.77; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,407.77 | ▲ close $10,407.77 vs 09:30 $10,414.77 (session +0.00) | 16:00 close · cash $10,407.77 · no lots left · equity $10,407.77. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,407.77 | ▲ 09:30 equity $10,407.77 vs yday $10,407.77 (+0.00) | 09:30 open · cash $10,407.77 · no holdings · equity $10,407.77 vs prior close $10,407.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,407.77 | ▲ close $10,407.77 vs 09:30 $10,407.77 (session +0.00) | 16:00 close · cash $10,407.77 · no lots left · equity $10,407.77. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,407.77 | ▲ 09:30 equity $10,407.77 vs yday $10,407.77 (+0.00) | 09:30 open · cash $10,407.77 · no holdings · equity $10,407.77 vs prior close $10,407.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,407.77 | ▲ close $10,407.77 vs 09:30 $10,407.77 (session +0.00) | 16:00 close · cash $10,407.77 · no lots left · equity $10,407.77. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,407.77 | ▲ 09:30 equity $10,407.77 vs yday $10,407.77 (+0.00) | 09:30 open · cash $10,407.77 · no holdings · equity $10,407.77 vs prior close $10,407.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 63 | $164.43 | $2.18 | — | $46.50 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $10407.77 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.50 | ▼ close $9,514.14 vs 09:30 $10,407.77 (session -891.45) | 16:00 close · cash $46.50 · equity $9,514.14 vs 09:30 $10,407.77 (-893.63; session marks -891.45) · 1 name(s) marked open→close (per-name table). ORCL×63 09:30 $164.43 → close $150.28 -891.45 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.50 | ▼ 09:30 equity $8,955.96 vs yday $9,514.14 (-558.18) | 09:30 open · cash $46.50 (unchanged overnight, no fees) · equity $8,955.96 vs prior close $9,514.14 (-558.18) · 1 name(s) re-marked at the open (per-name table). ORCL×63 yday $150.28 → 09:30 $141.42 -558.18 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 63 | $141.42 | $2.26 | $-1454.07 | $8,953.70 | ▼ -1,454.07 after sell → book $8,953.70; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,953.70 | ▲ close $8,953.70 vs 09:30 $8,955.96 (session +0.00) | 16:00 close · cash $8,953.70 · no lots left · equity $8,953.70. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
