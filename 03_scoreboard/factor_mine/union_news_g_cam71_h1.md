# Factor mine action — `union_news_g_cam71_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · merged news🟢 and cameras +7 −≤1

Cash book **+1.13%** ($10,113) · signal-only (no cash/fees) was +1.03%. Starts YES **10/26**. Fills 73 · skips 3 · realized $+177.91.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6.51.

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
| 2026-08-21 | `AU` | 14 | — | $119.43 | +0.00 | $121.22 | +25.06 | +25.06 | +0.00 | +25.06 |
| 2026-08-21 | `AUTL` | 689 | — | $2.47 | +0.00 | $2.41 | -41.34 | -41.34 | +0.00 | -41.34 |
| 2026-08-21 | `CRSP` | 28 | — | $59.72 | +0.00 | $59.50 | -6.16 | -6.16 | +0.00 | -6.16 |
| 2026-08-21 | `FUTU` | 14 | — | $115.18 | +0.00 | $123.64 | +118.44 | +118.44 | +0.00 | +118.44 |
| 2026-08-21 | `GRAL` | 21 | — | $78.88 | +0.00 | $79.54 | +13.86 | +13.86 | +0.00 | +13.86 |
| 2026-08-21 | `VIRT` | 28 | — | $60.66 | +0.00 | $67.93 | +203.56 | +203.56 | +0.00 | +203.56 |
| 2026-08-24 | `AU` | 14 | $121.22 | $120.51 | -9.94 | — | +0.00 | -9.94 | +15.12 | — |
| 2026-08-24 | `AUTL` | 689 | $2.41 | $2.40 | -6.89 | — | +0.00 | -6.89 | -48.23 | — |
| 2026-08-24 | `CRSP` | 28 | $59.50 | $58.75 | -21.00 | — | +0.00 | -21.00 | -27.16 | — |
| 2026-08-24 | `FUTU` | 14 | $123.64 | $121.00 | -36.96 | — | +0.00 | -36.96 | +81.48 | — |
| 2026-08-24 | `GRAL` | 21 | $79.54 | $81.87 | +48.93 | — | +0.00 | +48.93 | +62.79 | — |
| 2026-08-24 | `VIRT` | 28 | $67.93 | $66.80 | -31.64 | — | +0.00 | -31.64 | +171.92 | — |
| 2026-08-25 | `AU` | 44 | — | $118.52 | +0.00 | $123.39 | +214.28 | +214.28 | +0.00 | +214.28 |
| 2026-08-25 | `FCX` | 67 | — | $77.13 | +0.00 | $79.91 | +186.26 | +186.26 | +0.00 | +186.26 |
| 2026-08-26 | `AU` | 44 | $123.39 | $119.80 | -157.96 | — | +0.00 | -157.96 | +56.32 | — |
| 2026-08-26 | `FCX` | 67 | $79.91 | $79.34 | -38.19 | — | +0.00 | -38.19 | +148.07 | — |
| 2026-08-26 | `ASST` | 512 | — | $20.72 | +0.00 | $21.50 | +399.36 | +399.36 | +0.00 | +399.36 |
| 2026-08-27 | `ASST` | 512 | $21.50 | $22.45 | +486.40 | — | +0.00 | +486.40 | +885.76 | — |
| 2026-08-27 | `ACMR` | 20 | — | $81.65 | +0.00 | $80.49 | -23.20 | -23.20 | +0.00 | -23.20 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `CM` | 13 | — | $118.77 | +0.00 | $114.84 | -51.09 | -51.09 | +0.00 | -51.09 |
| 2026-08-27 | `GEN` | 55 | — | $29.83 | +0.00 | $30.50 | +36.85 | +36.85 | +0.00 | +36.85 |
| 2026-08-27 | `LRCX` | 5 | — | $318.88 | +0.00 | $318.58 | -1.50 | -1.50 | +0.00 | -1.50 |
| 2026-08-27 | `NVDA` | 7 | — | $222.86 | +0.00 | $227.98 | +35.84 | +35.84 | +0.00 | +35.84 |
| 2026-08-28 | `ACMR` | 20 | $80.49 | $79.27 | -24.40 | — | +0.00 | -24.40 | -47.60 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `CM` | 13 | $114.84 | $115.66 | +10.66 | — | +0.00 | +10.66 | -40.43 | — |
| 2026-08-28 | `GEN` | 55 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +36.85 | — |
| 2026-08-28 | `LRCX` | 5 | $318.58 | $318.03 | -2.75 | — | +0.00 | -2.75 | -4.25 | — |
| 2026-08-28 | `NVDA` | 7 | $227.98 | $227.36 | -4.34 | — | +0.00 | -4.34 | +31.50 | — |
| 2026-08-28 | `KEYS` | 5 | — | $324.41 | +0.00 | $319.97 | -22.20 | -22.20 | +0.00 | -22.20 |
| 2026-08-28 | `SMTC` | 13 | — | $141.76 | +0.00 | $131.17 | -137.67 | -137.67 | +0.00 | -137.67 |
| 2026-08-28 | `CIEN` | 4 | — | $400.42 | +0.00 | $378.44 | -87.92 | -87.92 | +0.00 | -87.92 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 7 | — | $240.22 | +0.00 | $236.98 | -22.68 | -22.68 | +0.00 | -22.68 |
| 2026-08-28 | `PLAB` | 63 | — | $30.01 | +0.00 | $27.73 | -143.64 | -143.64 | +0.00 | -143.64 |
| 2026-08-31 | `KEYS` | 5 | $319.97 | $322.49 | +12.60 | — | +0.00 | +12.60 | -9.60 | — |
| 2026-08-31 | `SMTC` | 13 | $131.17 | $132.30 | +14.69 | — | +0.00 | +14.69 | -122.98 | — |
| 2026-08-31 | `CIEN` | 4 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -87.92 | — |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | — | +0.00 | +5.64 | -44.13 | — |
| 2026-08-31 | `DDOG` | 7 | $236.98 | $233.97 | -21.10 | — | +0.00 | -21.10 | -43.78 | — |
| 2026-08-31 | `PLAB` | 63 | $27.73 | $28.04 | +19.53 | — | +0.00 | +19.53 | -124.11 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 15 | — | $351.74 | +0.00 | $357.16 | +81.30 | +81.30 | +0.00 | +81.30 |
| 2026-09-03 | `DELL` | 11 | — | $486.31 | +0.00 | $516.39 | +330.88 | +330.88 | +0.00 | +330.88 |
| 2026-09-04 | `AVGO` | 15 | $357.16 | $359.70 | +38.10 | — | +0.00 | +38.10 | +119.40 | — |
| 2026-09-04 | `DELL` | 11 | $516.39 | $513.78 | -28.71 | — | +0.00 | -28.71 | +302.17 | — |
| 2026-09-04 | `CRM` | 8 | — | $263.36 | +0.00 | $259.23 | -33.04 | -33.04 | +0.00 | -33.04 |
| 2026-09-04 | `FRNM` | 138 | — | $16.40 | +0.00 | $16.31 | -12.42 | -12.42 | +0.00 | -12.42 |
| 2026-09-04 | `MMED` | 95 | — | $23.84 | +0.00 | $23.29 | -52.25 | -52.25 | +0.00 | -52.25 |
| 2026-09-04 | `HPE` | 42 | — | $53.85 | +0.00 | $52.00 | -77.70 | -77.70 | +0.00 | -77.70 |
| 2026-09-04 | `MRX` | 30 | — | $75.65 | +0.00 | $78.27 | +78.60 | +78.60 | +0.00 | +78.60 |
| 2026-09-08 | `CRM` | 8 | $259.23 | $253.72 | -44.08 | — | +0.00 | -44.08 | -77.12 | — |
| 2026-09-08 | `FRNM` | 138 | $16.31 | $16.74 | +59.34 | — | +0.00 | +59.34 | +46.92 | — |
| 2026-09-08 | `MMED` | 95 | $23.29 | $23.16 | -12.35 | — | +0.00 | -12.35 | -64.60 | — |
| 2026-09-08 | `HPE` | 42 | $52.00 | $52.29 | +12.18 | — | +0.00 | +12.18 | -65.52 | — |
| 2026-09-08 | `MRX` | 30 | $78.27 | $78.84 | +17.10 | — | +0.00 | +17.10 | +95.70 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 68 | — | $164.43 | +0.00 | $150.28 | -962.20 | -962.20 | +0.00 | -962.20 |
| 2026-09-14 | `ORCL` | 68 | $150.28 | $141.42 | -602.48 | — | +0.00 | -602.48 | -1564.68 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 123 | — | $26.27 | +0.00 | $26.59 | +39.36 | +39.36 | +0.00 | +39.36 |
| 2026-09-16 | `QCOM` | 17 | — | $189.17 | +0.00 | $184.84 | -73.61 | -73.61 | +0.00 | -73.61 |
| 2026-09-16 | `SM` | 80 | — | $39.99 | +0.00 | $38.16 | -146.40 | -146.40 | +0.00 | -146.40 |
| 2026-09-17 | `WAY` | 123 | $26.59 | $26.51 | -9.84 | — | +0.00 | -9.84 | +29.52 | — |
| 2026-09-17 | `QCOM` | 17 | $184.84 | $190.35 | +93.67 | — | +0.00 | +93.67 | +20.06 | — |
| 2026-09-17 | `SM` | 80 | $38.16 | $37.57 | -47.20 | — | +0.00 | -47.20 | -193.60 | — |
| 2026-09-17 | `SMTC` | 55 | — | $170.85 | +0.00 | $178.19 | +403.70 | +403.70 | +0.00 | +403.70 |
| 2026-09-18 | `SMTC` | 55 | $178.19 | $182.33 | +227.70 | — | +0.00 | +227.70 | +631.40 | — |
| 2026-09-18 | `TH` | 162 | — | $20.91 | +0.00 | $21.19 | +45.36 | +45.36 | +0.00 | +45.36 |
| 2026-09-18 | `GME` | 148 | — | $22.90 | +0.00 | $22.64 | -38.48 | -38.48 | +0.00 | -38.48 |
| 2026-09-18 | `RARE` | 229 | — | $14.79 | +0.00 | $14.51 | -64.12 | -64.12 | +0.00 | -64.12 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +100.41 | BHP, APA | — | $112.62 | $10,095.93 | BHP×54, APA×111 |
| 2026-08-21 | +3.25 | $112.62 | BHP×54, APA×111 | $10,223.23 | +127.30 | +313.42 | AU, AUTL, CRSP, FUTU, GRAL, VIRT | BHP, APA | $186.00 | $10,512.91 | AU×14, AUTL×689, CRSP×28, FUTU×14, GRAL×21, VIRT×28 |
| 2026-08-24 | -5.17 | $186.00 | AU×14, AUTL×689, CRSP×28, FUTU×14, GRAL×21, VIRT×28 | $10,455.41 | -57.50 | +0.00 | — | AU, AUTL, CRSP, FUTU, GRAL, VIRT | $10,436.01 | $10,436.01 | — |
| 2026-08-25 | +1.80 | $10,436.01 | — | $10,436.01 | -0.00 | +400.54 | AU, FCX | — | $49.10 | $10,832.23 | AU×44, FCX×67 |
| 2026-08-26 | +2.02 | $49.10 | AU×44, FCX×67 | $10,636.08 | -196.15 | +399.36 | ASST | AU, FCX | $16.42 | $11,024.42 | ASST×512 |
| 2026-08-27 | — | $16.42 | ASST×512 | $11,510.82 | +486.40 | -34.72 | ACMR, MU, CM, GEN, LRCX, NVDA | ASST | $2,552.71 | $11,457.08 | ACMR×20, MU×1, CM×13, GEN×55, LRCX×5, NVDA×7 |
| 2026-08-28 | +0.75 | $2,552.71 | ACMR×20, MU×1, CM×13, GEN×55, LRCX×5, NVDA×7 | $11,420.15 | -36.93 | -463.88 | KEYS, SMTC, CIEN, MPWR, DDOG, PLAB | ACMR, MU, CM, GEN, LRCX, NVDA | $1,450.74 | $10,931.67 | KEYS×5, SMTC×13, CIEN×4, MPWR×1, DDOG×7, PLAB×63 |
| 2026-08-31 | -5.85 | $1,450.74 | KEYS×5, SMTC×13, CIEN×4, MPWR×1, DDOG×7, PLAB×63 | $10,963.03 | +31.36 | +0.00 | — | KEYS, SMTC, CIEN, MPWR, DDOG, PLAB | $10,950.67 | $10,950.67 | — |
| 2026-09-01 | -6.30 | $10,950.67 | — | $10,950.67 | +0.00 | +0.00 | — | — | $10,950.67 | $10,950.67 | — |
| 2026-09-02 | -3.83 | $10,950.67 | — | $10,950.67 | +0.00 | +0.00 | — | — | $10,950.67 | $10,950.67 | — |
| 2026-09-03 | -0.90 | $10,950.67 | — | $10,950.67 | +0.00 | +412.18 | AVGO, DELL | — | $321.10 | $11,358.79 | AVGO×15, DELL×11 |
| 2026-09-04 | +2.25 | $321.10 | AVGO×15, DELL×11 | $11,368.18 | +9.39 | -96.81 | CRM, FRNM, MMED, HPE, MRX | AVGO, DELL | $187.05 | $11,256.32 | CRM×8, FRNM×138, MMED×95, HPE×42, MRX×30 |
| 2026-09-08 | -11.47 | $187.05 | CRM×8, FRNM×138, MMED×95, HPE×42, MRX×30 | $11,288.51 | +32.19 | +0.00 | — | CRM, FRNM, MMED, HPE, MRX | $11,277.46 | $11,277.46 | — |
| 2026-09-09 | -13.95 | $11,277.46 | — | $11,277.46 | +0.00 | +0.00 | — | — | $11,277.46 | $11,277.46 | — |
| 2026-09-10 | -13.28 | $11,277.46 | — | $11,277.46 | +0.00 | +0.00 | — | — | $11,277.46 | $11,277.46 | — |
| 2026-09-11 | +0.50 | $11,277.46 | — | $11,277.46 | +0.00 | -962.20 | ORCL | — | $94.03 | $10,313.07 | ORCL×68 |
| 2026-09-14 | -11.00 | $94.03 | ORCL×68 | $9,710.59 | -602.48 | +0.00 | — | ORCL | $9,708.30 | $9,708.30 | — |
| 2026-09-15 | -3.84 | $9,708.30 | — | $9,708.30 | +0.00 | +0.00 | — | — | $9,708.30 | $9,708.30 | — |
| 2026-09-16 | +5.30 | $9,708.30 | — | $9,708.30 | +0.00 | -180.65 | WAY, QCOM, SM | — | $55.37 | $9,521.02 | WAY×123, QCOM×17, SM×80 |
| 2026-09-17 | +7.38 | $55.37 | WAY×123, QCOM×17, SM×80 | $9,557.65 | +36.63 | +403.70 | SMTC | WAY, QCOM, SM | $152.00 | $9,952.45 | SMTC×55 |
| 2026-09-18 | +4.86 | $152.00 | SMTC×55 | $10,180.15 | +227.70 | -57.24 | TH, GME, RARE | SMTC | $6.51 | $10,112.80 | TH×162, GME×148, RARE×229 |

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
| 2026-08-21 09:30 ET | **BUY** | `AU` | 14 | $119.43 | $2.03 | — | $8,544.59 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1703.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 689 | $2.47 | $8.89 | — | $6,833.87 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1703.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 28 | $59.72 | $2.07 | — | $5,159.64 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1703.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 14 | $115.18 | $2.03 | — | $3,545.08 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1703.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 21 | $78.88 | $2.05 | — | $1,886.55 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1703.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `VIRT` | 28 | $60.66 | $2.07 | — | $186.00 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list oppset; 🔵; ⚪; ret5=+7.0; leftover $1703.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $186.00 | ▲ close $10,512.91 vs 09:30 $10,223.23 (session +313.42) | 16:00 close · cash $186.00 · equity $10,512.91 vs 09:30 $10,223.23 (+289.68; session marks +313.42) · 6 name(s) marked open→close (per-name table). AU×14 09:30 $119.43 → close $121.22 +25.06; AUTL×689 09:30 $2.47 → close $2.41 -41.34; CRSP×28 09:30 $59.72 → close $59.50 -6.16; FUTU×14 09:30 $115.18 → close $123.64 +118.44; GRAL×21 09:30 $78.88 → close $79.54 +13.86; VIRT×28 09:30 $60.66 → close $67.93 +203.56 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.00 | ▼ 09:30 equity $10,455.41 vs yday $10,512.91 (-57.50) | 09:30 open · cash $186.00 (unchanged overnight, no fees) · equity $10,455.41 vs prior close $10,512.91 (-57.50) · 6 name(s) re-marked at the open (per-name table). AU×14 yday $121.22 → 09:30 $120.51 -9.94; AUTL×689 yday $2.41 → 09:30 $2.40 -6.89; CRSP×28 yday $59.50 → 09:30 $58.75 -21.00; FUTU×14 yday $123.64 → 09:30 $121.00 -36.96; GRAL×21 yday $79.54 → 09:30 $81.87 +48.93; VIRT×28 yday $67.93 → 09:30 $66.80 -31.64 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 14 | $120.51 | $2.06 | $+11.03 | $1,871.08 | ▲ +11.03 after sell → book $10,453.35; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 689 | $2.40 | $9.02 | $-66.13 | $3,515.67 | ▼ -66.13 after sell → book $10,444.34; vs 09:30 mark -9.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 28 | $58.75 | $2.10 | $-31.33 | $5,158.57 | ▼ -31.33 after sell → book $10,442.24; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 14 | $121.00 | $2.06 | $+77.39 | $6,850.51 | ▲ +77.39 after sell → book $10,440.18; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 21 | $81.87 | $2.08 | $+58.66 | $8,567.71 | ▲ +58.66 after sell → book $10,438.11; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `VIRT` | 28 | $66.80 | $2.10 | $+167.75 | $10,436.01 | ▲ +167.75 after sell → book $10,436.01; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,436.01 | ▲ close $10,436.01 vs 09:30 $10,455.41 (session +0.00) | 16:00 close · cash $10,436.01 · no lots left · equity $10,436.01. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,436.01 | ▲ 09:30 equity $10,436.01 vs yday $10,436.01 (-0.00) | 09:30 open · cash $10,436.01 · no holdings · equity $10,436.01 vs prior close $10,436.01 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 44 | $118.52 | $2.12 | — | $5,219.01 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $5218.00 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 67 | $77.13 | $2.19 | — | $49.10 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $5218.00 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.10 | ▲ close $10,832.23 vs 09:30 $10,436.01 (session +400.54) | 16:00 close · cash $49.10 · equity $10,832.23 vs 09:30 $10,436.01 (+396.22; session marks +400.54) · 2 name(s) marked open→close (per-name table). AU×44 09:30 $118.52 → close $123.39 +214.28; FCX×67 09:30 $77.13 → close $79.91 +186.26 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.10 | ▼ 09:30 equity $10,636.08 vs yday $10,832.23 (-196.15) | 09:30 open · cash $49.10 (unchanged overnight, no fees) · equity $10,636.08 vs prior close $10,832.23 (-196.15) · 2 name(s) re-marked at the open (per-name table). AU×44 yday $123.39 → 09:30 $119.80 -157.96; FCX×67 yday $79.91 → 09:30 $79.34 -38.19 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 44 | $119.80 | $2.17 | $+52.02 | $5,318.13 | ▲ +52.02 after sell → book $10,633.91; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 67 | $79.34 | $2.24 | $+143.63 | $10,631.67 | ▲ +143.63 after sell → book $10,631.67; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `ASST` | 512 | $20.72 | $6.60 | — | $16.42 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list oppset; 🔵; ret5=+67.1; leftover $10631.67 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.42 | ▲ close $11,024.42 vs 09:30 $10,636.08 (session +399.36) | 16:00 close · cash $16.42 · equity $11,024.42 vs 09:30 $10,636.08 (+388.34; session marks +399.36) · 1 name(s) marked open→close (per-name table). ASST×512 09:30 $20.72 → close $21.50 +399.36 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.42 | ▲ 09:30 equity $11,510.82 vs yday $11,024.42 (+486.40) | 09:30 open · cash $16.42 (unchanged overnight, no fees) · equity $11,510.82 vs prior close $11,024.42 (+486.40) · 1 name(s) re-marked at the open (per-name table). ASST×512 yday $21.50 → 09:30 $22.45 +486.40 | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 512 | $22.45 | $6.78 | $+872.37 | $11,504.04 | ▲ +872.37 after sell → book $11,504.04; vs 09:30 mark -6.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 20 | $81.65 | $2.05 | — | $9,868.99 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1643.43 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $8,899.99 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1643.43 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 13 | $118.77 | $2.03 | — | $7,353.95 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; ret5=+0.3; leftover $1643.43 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 55 | $29.83 | $2.15 | — | $5,711.14 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+7.6; leftover $1643.43 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 5 | $318.88 | $2.00 | — | $4,114.74 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1643.43 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 7 | $222.86 | $2.01 | — | $2,552.71 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $1643.43 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,552.71 | ▼ close $11,457.08 vs 09:30 $11,510.82 (session -34.72) | 16:00 close · cash $2,552.71 · equity $11,457.08 vs 09:30 $11,510.82 (-53.74; session marks -34.72) · 6 name(s) marked open→close (per-name table). ACMR×20 09:30 $81.65 → close $80.49 -23.20; MU×1 09:30 $967.01 → close $935.39 -31.62; CM×13 09:30 $118.77 → close $114.84 -51.09; GEN×55 09:30 $29.83 → close $30.50 +36.85; LRCX×5 09:30 $318.88 → close $318.58 -1.50; NVDA×7 09:30 $222.86 → close $227.98 +35.84 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,552.71 | ▼ 09:30 equity $11,420.15 vs yday $11,457.08 (-36.93) | 09:30 open · cash $2,552.71 (unchanged overnight, no fees) · equity $11,420.15 vs prior close $11,457.08 (-36.93) · 6 name(s) re-marked at the open (per-name table). ACMR×20 yday $80.49 → 09:30 $79.27 -24.40; MU×1 yday $935.39 → 09:30 $919.29 -16.10; CM×13 yday $114.84 → 09:30 $115.66 +10.66; GEN×55 yday $30.50 → 09:30 $30.50 +0.00; LRCX×5 yday $318.58 → 09:30 $318.03 -2.75; NVDA×7 yday $227.98 → 09:30 $227.36 -4.34 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 20 | $79.27 | $2.07 | $-51.72 | $4,136.03 | ▼ -51.72 after sell → book $11,418.07; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $5,053.31 | ▼ -51.73 after sell → book $11,416.06; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 13 | $115.66 | $2.05 | $-44.51 | $6,554.84 | ▼ -44.51 after sell → book $11,414.01; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 55 | $30.50 | $2.18 | $+32.52 | $8,230.16 | ▲ +32.52 after sell → book $11,411.83; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 5 | $318.03 | $2.03 | $-8.28 | $9,818.28 | ▼ -8.28 after sell → book $11,409.80; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 7 | $227.36 | $2.03 | $+27.46 | $11,407.77 | ▲ +27.46 after sell → book $11,407.77; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 5 | $324.41 | $2.00 | — | $9,783.71 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1901.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 13 | $141.76 | $2.03 | — | $7,938.81 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $1901.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 4 | $400.42 | $2.00 | — | $6,335.12 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1901.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $5,027.10 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1901.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 7 | $240.22 | $2.01 | — | $3,343.55 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1901.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PLAB` | 63 | $30.01 | $2.18 | — | $1,450.74 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list oppset; 🔵; ret5=-0.9; leftover $1901.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,450.74 | ▼ close $10,931.67 vs 09:30 $11,420.15 (session -463.88) | 16:00 close · cash $1,450.74 · equity $10,931.67 vs 09:30 $11,420.15 (-488.48; session marks -463.88) · 6 name(s) marked open→close (per-name table). KEYS×5 09:30 $324.41 → close $319.97 -22.20; SMTC×13 09:30 $141.76 → close $131.17 -137.67; CIEN×4 09:30 $400.42 → close $378.44 -87.92; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×7 09:30 $240.22 → close $236.98 -22.68; PLAB×63 09:30 $30.01 → close $27.73 -143.64 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,450.74 | ▲ 09:30 equity $10,963.03 vs yday $10,931.67 (+31.36) | 09:30 open · cash $1,450.74 (unchanged overnight, no fees) · equity $10,963.03 vs prior close $10,931.67 (+31.36) · 6 name(s) re-marked at the open (per-name table). KEYS×5 yday $319.97 → 09:30 $322.49 +12.60; SMTC×13 yday $131.17 → 09:30 $132.30 +14.69; CIEN×4 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×7 yday $236.98 → 09:30 $233.97 -21.10; PLAB×63 yday $27.73 → 09:30 $28.04 +19.53 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 5 | $322.49 | $2.03 | $-13.63 | $3,061.16 | ▼ -13.63 after sell → book $10,961.00; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 13 | $132.30 | $2.05 | $-127.06 | $4,779.01 | ▼ -127.06 after sell → book $10,958.95; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 4 | $378.44 | $2.02 | $-91.95 | $6,290.75 | ▼ -91.95 after sell → book $10,956.92; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $7,550.63 | ▼ -48.14 after sell → book $10,954.91; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 7 | $233.97 | $2.03 | $-47.83 | $9,186.35 | ▼ -47.83 after sell → book $10,952.87; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PLAB` | 63 | $28.04 | $2.20 | $-128.49 | $10,950.67 | ▼ -128.49 after sell → book $10,950.67; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,950.67 | ▲ close $10,950.67 vs 09:30 $10,963.03 (session +0.00) | 16:00 close · cash $10,950.67 · no lots left · equity $10,950.67. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,950.67 | ▲ 09:30 equity $10,950.67 vs yday $10,950.67 (+0.00) | 09:30 open · cash $10,950.67 · no holdings · equity $10,950.67 vs prior close $10,950.67 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,950.67 | ▲ close $10,950.67 vs 09:30 $10,950.67 (session +0.00) | 16:00 close · cash $10,950.67 · no lots left · equity $10,950.67. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,950.67 | ▲ 09:30 equity $10,950.67 vs yday $10,950.67 (+0.00) | 09:30 open · cash $10,950.67 · no holdings · equity $10,950.67 vs prior close $10,950.67 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,950.67 | ▲ close $10,950.67 vs 09:30 $10,950.67 (session +0.00) | 16:00 close · cash $10,950.67 · no lots left · equity $10,950.67. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,950.67 | ▲ 09:30 equity $10,950.67 vs yday $10,950.67 (+0.00) | 09:30 open · cash $10,950.67 · no holdings · equity $10,950.67 vs prior close $10,950.67 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 15 | $351.74 | $2.04 | — | $5,672.54 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $5475.34 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 11 | $486.31 | $2.02 | — | $321.10 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $5475.34 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $321.10 | ▲ close $11,358.79 vs 09:30 $10,950.67 (session +412.18) | 16:00 close · cash $321.10 · equity $11,358.79 vs 09:30 $10,950.67 (+408.12; session marks +412.18) · 2 name(s) marked open→close (per-name table). AVGO×15 09:30 $351.74 → close $357.16 +81.30; DELL×11 09:30 $486.31 → close $516.39 +330.88 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $321.10 | ▲ 09:30 equity $11,368.18 vs yday $11,358.79 (+9.39) | 09:30 open · cash $321.10 (unchanged overnight, no fees) · equity $11,368.18 vs prior close $11,358.79 (+9.39) · 2 name(s) re-marked at the open (per-name table). AVGO×15 yday $357.16 → 09:30 $359.70 +38.10; DELL×11 yday $516.39 → 09:30 $513.78 -28.71 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 15 | $359.70 | $2.09 | $+115.28 | $5,714.51 | ▲ +115.28 after sell → book $11,366.09; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 11 | $513.78 | $2.08 | $+298.07 | $11,364.02 | ▲ +298.07 after sell → book $11,364.02; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 8 | $263.36 | $2.01 | — | $9,255.12 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $2272.80 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 138 | $16.40 | $2.40 | — | $6,989.52 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $2272.80 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MMED` | 95 | $23.84 | $2.27 | — | $4,722.44 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list oppset; 🔵; ⚪; ret5=+22.2; leftover $2272.80 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HPE` | 42 | $53.85 | $2.12 | — | $2,458.63 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list oppset; 🔵; ret5=+0.1; leftover $2272.80 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟡 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 30 | $75.65 | $2.08 | — | $187.05 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $2272.80 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $187.05 | ▼ close $11,256.32 vs 09:30 $11,368.18 (session -96.81) | 16:00 close · cash $187.05 · equity $11,256.32 vs 09:30 $11,368.18 (-111.86; session marks -96.81) · 5 name(s) marked open→close (per-name table). CRM×8 09:30 $263.36 → close $259.23 -33.04; FRNM×138 09:30 $16.40 → close $16.31 -12.42; MMED×95 09:30 $23.84 → close $23.29 -52.25; HPE×42 09:30 $53.85 → close $52.00 -77.70; MRX×30 09:30 $75.65 → close $78.27 +78.60 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $187.05 | ▲ 09:30 equity $11,288.51 vs yday $11,256.32 (+32.19) | 09:30 open · cash $187.05 (unchanged overnight, no fees) · equity $11,288.51 vs prior close $11,256.32 (+32.19) · 5 name(s) re-marked at the open (per-name table). CRM×8 yday $259.23 → 09:30 $253.72 -44.08; FRNM×138 yday $16.31 → 09:30 $16.74 +59.34; MMED×95 yday $23.29 → 09:30 $23.16 -12.35; HPE×42 yday $52.00 → 09:30 $52.29 +12.18; MRX×30 yday $78.27 → 09:30 $78.84 +17.10 | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 8 | $253.72 | $2.04 | $-81.17 | $2,214.77 | ▼ -81.17 after sell → book $11,286.47; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 138 | $16.74 | $2.45 | $+42.07 | $4,522.44 | ▲ +42.07 after sell → book $11,284.02; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MMED` | 95 | $23.16 | $2.31 | $-69.18 | $6,720.33 | ▼ -69.18 after sell → book $11,281.71; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HPE` | 42 | $52.29 | $2.14 | $-69.78 | $8,914.37 | ▼ -69.78 after sell → book $11,279.57; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 30 | $78.84 | $2.11 | $+91.51 | $11,277.46 | ▲ +91.51 after sell → book $11,277.46; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,277.46 | ▲ close $11,277.46 vs 09:30 $11,288.51 (session +0.00) | 16:00 close · cash $11,277.46 · no lots left · equity $11,277.46. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,277.46 | ▲ 09:30 equity $11,277.46 vs yday $11,277.46 (+0.00) | 09:30 open · cash $11,277.46 · no holdings · equity $11,277.46 vs prior close $11,277.46 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,277.46 | ▲ close $11,277.46 vs 09:30 $11,277.46 (session +0.00) | 16:00 close · cash $11,277.46 · no lots left · equity $11,277.46. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,277.46 | ▲ 09:30 equity $11,277.46 vs yday $11,277.46 (+0.00) | 09:30 open · cash $11,277.46 · no holdings · equity $11,277.46 vs prior close $11,277.46 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,277.46 | ▲ close $11,277.46 vs 09:30 $11,277.46 (session +0.00) | 16:00 close · cash $11,277.46 · no lots left · equity $11,277.46. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,277.46 | ▲ 09:30 equity $11,277.46 vs yday $11,277.46 (+0.00) | 09:30 open · cash $11,277.46 · no holdings · equity $11,277.46 vs prior close $11,277.46 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 68 | $164.43 | $2.19 | — | $94.03 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $11277.46 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.03 | ▼ close $10,313.07 vs 09:30 $11,277.46 (session -962.20) | 16:00 close · cash $94.03 · equity $10,313.07 vs 09:30 $11,277.46 (-964.39; session marks -962.20) · 1 name(s) marked open→close (per-name table). ORCL×68 09:30 $164.43 → close $150.28 -962.20 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.03 | ▼ 09:30 equity $9,710.59 vs yday $10,313.07 (-602.48) | 09:30 open · cash $94.03 (unchanged overnight, no fees) · equity $9,710.59 vs prior close $10,313.07 (-602.48) · 1 name(s) re-marked at the open (per-name table). ORCL×68 yday $150.28 → 09:30 $141.42 -602.48 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 68 | $141.42 | $2.28 | $-1569.16 | $9,708.30 | ▼ -1,569.16 after sell → book $9,708.30; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,708.30 | ▲ close $9,708.30 vs 09:30 $9,710.59 (session +0.00) | 16:00 close · cash $9,708.30 · no lots left · equity $9,708.30. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,708.30 | ▲ 09:30 equity $9,708.30 vs yday $9,708.30 (+0.00) | 09:30 open · cash $9,708.30 · no holdings · equity $9,708.30 vs prior close $9,708.30 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,708.30 | ▲ close $9,708.30 vs 09:30 $9,708.30 (session +0.00) | 16:00 close · cash $9,708.30 · no lots left · equity $9,708.30. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,708.30 | ▲ 09:30 equity $9,708.30 vs yday $9,708.30 (+0.00) | 09:30 open · cash $9,708.30 · no holdings · equity $9,708.30 vs prior close $9,708.30 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 123 | $26.27 | $2.36 | — | $6,474.74 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $3236.10 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 17 | $189.17 | $2.04 | — | $3,256.80 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $3236.10 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 80 | $39.99 | $2.23 | — | $55.37 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $3236.10 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.37 | ▼ close $9,521.02 vs 09:30 $9,708.30 (session -180.65) | 16:00 close · cash $55.37 · equity $9,521.02 vs 09:30 $9,708.30 (-187.28; session marks -180.65) · 3 name(s) marked open→close (per-name table). WAY×123 09:30 $26.27 → close $26.59 +39.36; QCOM×17 09:30 $189.17 → close $184.84 -73.61; SM×80 09:30 $39.99 → close $38.16 -146.40 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.37 | ▲ 09:30 equity $9,557.65 vs yday $9,521.02 (+36.63) | 09:30 open · cash $55.37 (unchanged overnight, no fees) · equity $9,557.65 vs prior close $9,521.02 (+36.63) · 3 name(s) re-marked at the open (per-name table). WAY×123 yday $26.59 → 09:30 $26.51 -9.84; QCOM×17 yday $184.84 → 09:30 $190.35 +93.67; SM×80 yday $38.16 → 09:30 $37.57 -47.20 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 123 | $26.51 | $2.41 | $+24.76 | $3,313.70 | ▲ +24.76 after sell → book $9,555.25; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 17 | $190.35 | $2.08 | $+15.94 | $6,547.57 | ▲ +15.94 after sell → book $9,553.17; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 80 | $37.57 | $2.27 | $-198.10 | $9,550.90 | ▼ -198.10 after sell → book $9,550.90; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 55 | $170.85 | $2.15 | — | $152.00 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $9550.90 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $152.00 | ▲ close $9,952.45 vs 09:30 $9,557.65 (session +403.70) | 16:00 close · cash $152.00 · equity $9,952.45 vs 09:30 $9,557.65 (+394.80; session marks +403.70) · 1 name(s) marked open→close (per-name table). SMTC×55 09:30 $170.85 → close $178.19 +403.70 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $152.00 | ▲ 09:30 equity $10,180.15 vs yday $9,952.45 (+227.70) | 09:30 open · cash $152.00 (unchanged overnight, no fees) · equity $10,180.15 vs prior close $9,952.45 (+227.70) · 1 name(s) re-marked at the open (per-name table). SMTC×55 yday $178.19 → 09:30 $182.33 +227.70 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 55 | $182.33 | $2.25 | $+627.00 | $10,177.90 | ▲ +627.00 after sell → book $10,177.90; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 162 | $20.91 | $2.48 | — | $6,788.01 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $3392.63 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `GME` | 148 | $22.90 | $2.43 | — | $3,396.37 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $3392.63 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 229 | $14.79 | $2.95 | — | $6.51 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $3392.63 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.51 | ▼ close $10,112.80 vs 09:30 $10,180.15 (session -57.24) | 16:00 close · cash $6.51 · equity $10,112.80 vs 09:30 $10,180.15 (-67.35; session marks -57.24) · 3 name(s) marked open→close (per-name table). TH×162 09:30 $20.91 → close $21.19 +45.36; GME×148 09:30 $22.90 → close $22.64 -38.48; RARE×229 09:30 $14.79 → close $14.51 -64.12 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1643.43 < 1 share @ 1746.53 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `TH` | 162 | 2026-09-18 @ $20.91 | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $3392.63 |
| `GME` | 148 | 2026-09-18 @ $22.90 | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $3392.63 |
| `RARE` | 229 | 2026-09-18 @ $14.79 | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $3392.63 |
