# Factor mine action — `union_news_g_cam71_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · merged news🟢 and cameras +7 −≤1

Cash book **-19.33%** ($8,067) · signal-only (no cash/fees) was -2.95%. Starts YES **1/26**. Fills 41 · skips 61 · realized $-1501.17.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=good,n_pos_min=7,cam_bad_max=1` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $46.24.

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
| 2026-08-21 | `BHP` | 54 | $93.63 | $95.72 | +112.86 | $97.03 | +70.74 | +183.60 | +254.34 | +325.08 |
| 2026-08-21 | `APA` | 111 | $44.39 | $44.52 | +14.43 | $43.39 | -125.43 | -111.00 | -26.64 | -152.07 |
| 2026-08-21 | `AUTL` | 7 | — | $2.47 | +0.00 | $2.41 | -0.42 | -0.42 | +0.00 | -0.42 |
| 2026-08-24 | `BHP` | 54 | $97.03 | $97.31 | +15.12 | $97.13 | -9.72 | +5.40 | +340.20 | +330.48 |
| 2026-08-24 | `APA` | 111 | $43.39 | $42.93 | -51.06 | $42.96 | +3.33 | -47.73 | -203.13 | -199.80 |
| 2026-08-24 | `AUTL` | 7 | $2.41 | $2.40 | -0.07 | $2.34 | -0.42 | -0.49 | -0.49 | -0.91 |
| 2026-08-25 | `BHP` | 54 | $97.13 | $95.86 | -68.58 | — | +0.00 | -68.58 | +261.90 | — |
| 2026-08-25 | `APA` | 111 | $42.96 | $41.38 | -175.38 | — | +0.00 | -175.38 | -375.18 | — |
| 2026-08-25 | `AUTL` | 7 | $2.34 | $2.38 | +0.28 | $2.44 | +0.42 | +0.70 | -0.63 | -0.21 |
| 2026-08-25 | `AU` | 41 | — | $118.52 | +0.00 | $123.39 | +199.67 | +199.67 | +0.00 | +199.67 |
| 2026-08-25 | `FCX` | 63 | — | $77.13 | +0.00 | $79.91 | +175.14 | +175.14 | +0.00 | +175.14 |
| 2026-08-26 | `AUTL` | 7 | $2.44 | $2.41 | -0.21 | — | +0.00 | -0.21 | -0.42 | — |
| 2026-08-26 | `AU` | 41 | $123.39 | $119.80 | -147.19 | $118.11 | -69.29 | -216.48 | +52.48 | -16.81 |
| 2026-08-26 | `FCX` | 63 | $79.91 | $79.34 | -35.91 | $79.00 | -21.42 | -57.33 | +139.23 | +117.81 |
| 2026-08-26 | `ASST` | 7 | — | $20.72 | +0.00 | $21.50 | +5.46 | +5.46 | +0.00 | +5.46 |
| 2026-08-27 | `AU` | 41 | $118.11 | $117.41 | -28.70 | $118.40 | +40.59 | +11.89 | -45.51 | -4.92 |
| 2026-08-27 | `FCX` | 63 | $79.00 | $78.83 | -10.71 | $78.42 | -25.83 | -36.54 | +107.10 | +81.27 |
| 2026-08-27 | `ASST` | 7 | $21.50 | $22.45 | +6.65 | $23.12 | +4.69 | +11.34 | +12.11 | +16.80 |
| 2026-08-28 | `AU` | 41 | $118.40 | $119.19 | +32.39 | — | +0.00 | +32.39 | +27.47 | — |
| 2026-08-28 | `FCX` | 63 | $78.42 | $78.57 | +9.45 | — | +0.00 | +9.45 | +90.72 | — |
| 2026-08-28 | `ASST` | 7 | $23.12 | $22.50 | -4.34 | $21.74 | -5.32 | -9.66 | +12.46 | +7.14 |
| 2026-08-28 | `KEYS` | 5 | — | $324.41 | +0.00 | $319.97 | -22.20 | -22.20 | +0.00 | -22.20 |
| 2026-08-28 | `SMTC` | 11 | — | $141.76 | +0.00 | $131.17 | -116.49 | -116.49 | +0.00 | -116.49 |
| 2026-08-28 | `CIEN` | 4 | — | $400.42 | +0.00 | $378.44 | -87.92 | -87.92 | +0.00 | -87.92 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 6 | — | $240.22 | +0.00 | $236.98 | -19.44 | -19.44 | +0.00 | -19.44 |
| 2026-08-28 | `PLAB` | 54 | — | $30.01 | +0.00 | $27.73 | -123.12 | -123.12 | +0.00 | -123.12 |
| 2026-08-31 | `ASST` | 7 | $21.74 | $22.54 | +5.60 | — | +0.00 | +5.60 | +12.74 | — |
| 2026-08-31 | `KEYS` | 5 | $319.97 | $322.49 | +12.60 | $322.70 | +1.05 | +13.65 | -9.60 | -8.55 |
| 2026-08-31 | `SMTC` | 11 | $131.17 | $132.30 | +12.43 | $132.96 | +7.26 | +19.69 | -104.06 | -96.80 |
| 2026-08-31 | `CIEN` | 4 | $378.44 | $378.44 | +0.00 | $382.80 | +17.44 | +17.44 | -87.92 | -70.48 |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | $1267.77 | +5.87 | +11.51 | -44.13 | -38.26 |
| 2026-08-31 | `DDOG` | 6 | $236.98 | $233.97 | -18.09 | $237.04 | +18.45 | +0.36 | -37.53 | -19.08 |
| 2026-08-31 | `PLAB` | 54 | $27.73 | $28.04 | +16.74 | $28.14 | +5.40 | +22.14 | -106.38 | -100.98 |
| 2026-09-01 | `KEYS` | 5 | $322.70 | $321.47 | -6.15 | $319.27 | -11.00 | -17.15 | -14.70 | -25.70 |
| 2026-09-01 | `SMTC` | 11 | $132.96 | $127.63 | -58.63 | $132.27 | +51.04 | -7.59 | -155.43 | -104.39 |
| 2026-09-01 | `CIEN` | 4 | $382.80 | $376.89 | -23.64 | $360.33 | -66.24 | -89.88 | -94.12 | -160.36 |
| 2026-09-01 | `MPWR` | 1 | $1267.77 | $1245.11 | -22.66 | $1225.96 | -19.15 | -41.81 | -60.92 | -80.07 |
| 2026-09-01 | `DDOG` | 6 | $237.04 | $232.88 | -24.96 | $223.84 | -54.24 | -79.20 | -44.04 | -98.28 |
| 2026-09-01 | `PLAB` | 54 | $28.14 | $27.69 | -24.30 | $27.33 | -19.44 | -43.74 | -125.28 | -144.72 |
| 2026-09-02 | `KEYS` | 5 | $319.27 | $318.04 | -6.15 | — | +0.00 | -6.15 | -31.85 | — |
| 2026-09-02 | `SMTC` | 11 | $132.27 | $133.00 | +8.03 | — | +0.00 | +8.03 | -96.36 | — |
| 2026-09-02 | `CIEN` | 4 | $360.33 | $357.25 | -12.32 | — | +0.00 | -12.32 | -172.68 | — |
| 2026-09-02 | `MPWR` | 1 | $1225.96 | $1224.92 | -1.04 | — | +0.00 | -1.04 | -81.11 | — |
| 2026-09-02 | `DDOG` | 6 | $223.84 | $219.46 | -26.28 | — | +0.00 | -26.28 | -124.56 | — |
| 2026-09-02 | `PLAB` | 54 | $27.33 | $27.41 | +4.32 | — | +0.00 | +4.32 | -140.40 | — |
| 2026-09-03 | `AVGO` | 13 | — | $351.74 | +0.00 | $357.16 | +70.46 | +70.46 | +0.00 | +70.46 |
| 2026-09-03 | `DELL` | 9 | — | $486.31 | +0.00 | $516.39 | +270.72 | +270.72 | +0.00 | +270.72 |
| 2026-09-04 | `AVGO` | 13 | $357.16 | $359.70 | +33.02 | $357.90 | -23.40 | +9.62 | +103.48 | +80.08 |
| 2026-09-04 | `DELL` | 9 | $516.39 | $513.78 | -23.49 | $524.14 | +93.24 | +69.75 | +247.23 | +340.47 |
| 2026-09-04 | `FRNM` | 4 | — | $16.40 | +0.00 | $16.31 | -0.36 | -0.36 | +0.00 | -0.36 |
| 2026-09-04 | `MMED` | 3 | — | $23.84 | +0.00 | $23.29 | -1.65 | -1.65 | +0.00 | -1.65 |
| 2026-09-04 | `HPE` | 1 | — | $53.85 | +0.00 | $52.00 | -1.85 | -1.85 | +0.00 | -1.85 |
| 2026-09-08 | `AVGO` | 13 | $357.90 | $363.68 | +75.14 | $368.56 | +63.44 | +138.58 | +155.22 | +218.66 |
| 2026-09-08 | `DELL` | 9 | $524.14 | $521.15 | -26.91 | $533.88 | +114.57 | +87.66 | +313.56 | +428.13 |
| 2026-09-08 | `FRNM` | 4 | $16.31 | $16.74 | +1.72 | $15.99 | -3.00 | -1.28 | +1.36 | -1.64 |
| 2026-09-08 | `MMED` | 3 | $23.29 | $23.16 | -0.39 | $23.32 | +0.48 | +0.09 | -2.04 | -1.56 |
| 2026-09-08 | `HPE` | 1 | $52.00 | $52.29 | +0.29 | $56.03 | +3.74 | +4.03 | -1.56 | +2.18 |
| 2026-09-09 | `AVGO` | 13 | $368.56 | $366.23 | -30.29 | — | +0.00 | -30.29 | +188.37 | — |
| 2026-09-09 | `DELL` | 9 | $533.88 | $538.47 | +41.31 | — | +0.00 | +41.31 | +469.44 | — |
| 2026-09-09 | `FRNM` | 4 | $15.99 | $15.96 | -0.12 | $15.75 | -0.84 | -0.96 | -1.76 | -2.60 |
| 2026-09-09 | `MMED` | 3 | $23.32 | $23.22 | -0.30 | $22.76 | -1.38 | -1.68 | -1.86 | -3.24 |
| 2026-09-09 | `HPE` | 1 | $56.03 | $56.94 | +0.91 | $58.90 | +1.96 | +2.87 | +3.09 | +5.05 |
| 2026-09-10 | `FRNM` | 4 | $15.75 | $15.64 | -0.44 | — | +0.00 | -0.44 | -3.04 | — |
| 2026-09-10 | `MMED` | 3 | $22.76 | $22.54 | -0.66 | — | +0.00 | -0.66 | -3.90 | — |
| 2026-09-10 | `HPE` | 1 | $58.90 | $57.80 | -1.10 | — | +0.00 | -1.10 | +3.95 | — |
| 2026-09-11 | `ORCL` | 60 | — | $164.43 | +0.00 | $150.28 | -849.00 | -849.00 | +0.00 | -849.00 |
| 2026-09-14 | `ORCL` | 60 | $150.28 | $141.42 | -531.60 | $144.79 | +202.20 | -329.40 | -1380.60 | -1178.40 |
| 2026-09-15 | `ORCL` | 60 | $144.79 | $143.46 | -79.80 | $140.35 | -186.60 | -266.40 | -1258.20 | -1444.80 |
| 2026-09-16 | `ORCL` | 60 | $140.35 | $140.03 | -19.20 | — | +0.00 | -19.20 | -1464.00 | — |
| 2026-09-16 | `WAY` | 107 | — | $26.27 | +0.00 | $26.59 | +34.24 | +34.24 | +0.00 | +34.24 |
| 2026-09-16 | `QCOM` | 14 | — | $189.17 | +0.00 | $184.84 | -60.62 | -60.62 | +0.00 | -60.62 |
| 2026-09-16 | `SM` | 70 | — | $39.99 | +0.00 | $38.16 | -128.10 | -128.10 | +0.00 | -128.10 |
| 2026-09-17 | `WAY` | 107 | $26.59 | $26.51 | -8.56 | $26.51 | +0.00 | -8.56 | +25.68 | +25.68 |
| 2026-09-17 | `QCOM` | 14 | $184.84 | $190.35 | +77.14 | $188.71 | -22.96 | +54.18 | +16.52 | -6.44 |
| 2026-09-17 | `SM` | 70 | $38.16 | $37.57 | -41.30 | $36.97 | -42.00 | -83.30 | -169.40 | -211.40 |
| 2026-09-17 | `SMTC` | 1 | — | $170.85 | +0.00 | $178.19 | +7.34 | +7.34 | +0.00 | +7.34 |
| 2026-09-18 | `WAY` | 107 | $26.51 | $26.95 | +47.08 | $25.66 | -138.03 | -90.95 | +72.76 | -65.27 |
| 2026-09-18 | `QCOM` | 14 | $188.71 | $191.34 | +36.82 | $177.72 | -190.68 | -153.86 | +30.38 | -160.30 |
| 2026-09-18 | `SM` | 70 | $36.97 | $36.87 | -7.00 | $36.97 | +7.00 | +0.00 | -218.40 | -211.40 |
| 2026-09-18 | `SMTC` | 1 | $178.19 | $182.33 | +4.14 | $185.00 | +2.67 | +6.81 | +11.48 | +14.15 |
| 2026-09-18 | `RARE` | 1 | — | $14.79 | +0.00 | $14.51 | -0.28 | -0.28 | +0.00 | -0.28 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +100.41 | BHP, APA | — | $112.62 | $10,095.93 | BHP×54, APA×111 |
| 2026-08-21 | +3.25 | $112.62 | BHP×54, APA×111 | $10,223.23 | +127.30 | -55.11 | AUTL | — | $95.14 | $10,167.92 | BHP×54, APA×111, AUTL×7 |
| 2026-08-24 | -5.17 | $95.14 | BHP×54, APA×111, AUTL×7 | $10,131.91 | -36.01 | -6.81 | — | — | $95.14 | $10,125.10 | BHP×54, APA×111, AUTL×7 |
| 2026-08-25 | +1.80 | $95.14 | BHP×54, APA×111, AUTL×7 | $9,881.42 | -243.68 | +375.23 | AU, FCX | BHP, APA | $137.38 | $10,247.78 | AUTL×7, AU×41, FCX×63 |
| 2026-08-26 | +2.02 | $137.38 | AUTL×7, AU×41, FCX×63 | $10,064.47 | -183.31 | -85.25 | ASST | AUTL | $7.53 | $9,977.54 | AU×41, FCX×63, ASST×7 |
| 2026-08-27 | — | $7.53 | AU×41, FCX×63, ASST×7 | $9,944.78 | -32.76 | +19.45 | — | — | $7.53 | $9,964.23 | AU×41, FCX×63, ASST×7 |
| 2026-08-28 | +0.75 | $7.53 | AU×41, FCX×63, ASST×7 | $10,001.73 | +37.50 | -424.26 | KEYS, SMTC, CIEN, MPWR, DDOG, PLAB | AU, FCX | $676.67 | $9,560.89 | ASST×7, KEYS×5, SMTC×11, CIEN×4, MPWR×1, DDOG×6, PLAB×54 |
| 2026-08-31 | -5.85 | $676.67 | ASST×7, KEYS×5, SMTC×11, CIEN×4, MPWR×1, DDOG×6, PLAB×54 | $9,595.81 | +34.92 | +55.47 | — | ASST | $832.83 | $9,649.66 | KEYS×5, SMTC×11, CIEN×4, MPWR×1, DDOG×6, PLAB×54 |
| 2026-09-01 | -6.30 | $832.83 | KEYS×5, SMTC×11, CIEN×4, MPWR×1, DDOG×6, PLAB×54 | $9,489.32 | -160.34 | -119.03 | — | — | $832.83 | $9,370.29 | KEYS×5, SMTC×11, CIEN×4, MPWR×1, DDOG×6, PLAB×54 |
| 2026-09-02 | -3.83 | $832.83 | KEYS×5, SMTC×11, CIEN×4, MPWR×1, DDOG×6, PLAB×54 | $9,336.85 | -33.44 | +0.00 | — | KEYS, SMTC, CIEN, MPWR, DDOG, PLAB | $9,324.54 | $9,324.54 | — |
| 2026-09-03 | -0.90 | $9,324.54 | — | $9,324.54 | +0.00 | +341.18 | AVGO, DELL | — | $371.09 | $9,661.68 | AVGO×13, DELL×9 |
| 2026-09-04 | +2.25 | $371.09 | AVGO×13, DELL×9 | $9,671.21 | +9.53 | +65.98 | FRNM, MMED, HPE | — | $178.18 | $9,735.25 | AVGO×13, DELL×9, FRNM×4, MMED×3, HPE×1 |
| 2026-09-08 | -11.47 | $178.18 | AVGO×13, DELL×9, FRNM×4, MMED×3, HPE×1 | $9,785.10 | +49.85 | +179.23 | — | — | $178.18 | $9,964.33 | AVGO×13, DELL×9, FRNM×4, MMED×3, HPE×1 |
| 2026-09-09 | -13.95 | $178.18 | AVGO×13, DELL×9, FRNM×4, MMED×3, HPE×1 | $9,975.84 | +11.51 | -0.26 | — | AVGO, DELL | $9,781.26 | $9,971.44 | FRNM×4, MMED×3, HPE×1 |
| 2026-09-10 | -13.28 | $9,781.26 | FRNM×4, MMED×3, HPE×1 | $9,969.24 | -2.20 | +0.00 | — | FRNM, MMED, HPE | $9,967.28 | $9,967.28 | — |
| 2026-09-11 | +0.50 | $9,967.28 | — | $9,967.28 | -0.00 | -849.00 | ORCL | — | $99.31 | $9,116.11 | ORCL×60 |
| 2026-09-14 | -11.00 | $99.31 | ORCL×60 | $8,584.51 | -531.60 | +202.20 | — | — | $99.31 | $8,786.71 | ORCL×60 |
| 2026-09-15 | -3.84 | $99.31 | ORCL×60 | $8,706.91 | -79.80 | -186.60 | — | — | $99.31 | $8,520.31 | ORCL×60 |
| 2026-09-16 | +5.30 | $99.31 | ORCL×60 | $8,501.11 | -19.20 | -154.48 | WAY, QCOM, SM | ORCL | $233.75 | $8,337.84 | WAY×107, QCOM×14, SM×70 |
| 2026-09-17 | +7.38 | $233.75 | WAY×107, QCOM×14, SM×70 | $8,365.12 | +27.28 | -57.62 | SMTC | — | $61.18 | $8,305.78 | WAY×107, QCOM×14, SM×70, SMTC×1 |
| 2026-09-18 | +4.86 | $61.18 | WAY×107, QCOM×14, SM×70, SMTC×1 | $8,386.82 | +81.04 | -319.32 | RARE | — | $46.24 | $8,067.35 | WAY×107, QCOM×14, SM×70, SMTC×1, RARE×1 |

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
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 7 | $2.47 | $0.19 | — | $95.14 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $18.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.14 | ▼ close $10,167.92 vs 09:30 $10,223.23 (session -55.11) | 16:00 close · cash $95.14 · equity $10,167.92 vs 09:30 $10,223.23 (-55.31; session marks -55.11) · 3 name(s) marked open→close (per-name table). BHP×54 09:30 $95.72 → close $97.03 +70.74; APA×111 09:30 $44.52 → close $43.39 -125.43; AUTL×7 09:30 $2.47 → close $2.41 -0.42 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.14 | ▼ 09:30 equity $10,131.91 vs yday $10,167.92 (-36.01) | 09:30 open · cash $95.14 (unchanged overnight, no fees) · equity $10,131.91 vs prior close $10,167.92 (-36.01) · 3 name(s) re-marked at the open (per-name table). BHP×54 yday $97.03 → 09:30 $97.31 +15.12; APA×111 yday $43.39 → 09:30 $42.93 -51.06; AUTL×7 yday $2.41 → 09:30 $2.40 -0.07 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.14 | ▼ close $10,125.10 vs 09:30 $10,131.91 (session -6.81) | 16:00 close · cash $95.14 · equity $10,125.10 vs 09:30 $10,131.91 (-6.81; session marks -6.81) · 3 name(s) marked open→close (per-name table). BHP×54 09:30 $97.31 → close $97.13 -9.72; APA×111 09:30 $42.93 → close $42.96 +3.33; AUTL×7 09:30 $2.40 → close $2.34 -0.42 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.14 | ▼ 09:30 equity $9,881.42 vs yday $10,125.10 (-243.68) | 09:30 open · cash $95.14 (unchanged overnight, no fees) · equity $9,881.42 vs prior close $10,125.10 (-243.68) · 3 name(s) re-marked at the open (per-name table). BHP×54 yday $97.13 → 09:30 $95.86 -68.58; APA×111 yday $42.96 → 09:30 $41.38 -175.38; AUTL×7 yday $2.34 → 09:30 $2.38 +0.28 | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 54 | $95.86 | $2.20 | $+257.54 | $5,269.38 | ▲ +257.54 after sell → book $9,879.22; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 111 | $41.38 | $2.38 | $-379.88 | $9,860.18 | ▼ -379.88 after sell → book $9,876.84; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 41 | $118.52 | $2.11 | — | $4,998.75 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $4930.09 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 63 | $77.13 | $2.18 | — | $137.38 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $4930.09 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $137.38 | ▲ close $10,247.78 vs 09:30 $9,881.42 (session +375.23) | 16:00 close · cash $137.38 · equity $10,247.78 vs 09:30 $9,881.42 (+366.36; session marks +375.23) · 3 name(s) marked open→close (per-name table). AUTL×7 09:30 $2.38 → close $2.44 +0.42; AU×41 09:30 $118.52 → close $123.39 +199.67; FCX×63 09:30 $77.13 → close $79.91 +175.14 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $137.38 | ▼ 09:30 equity $10,064.47 vs yday $10,247.78 (-183.31) | 09:30 open · cash $137.38 (unchanged overnight, no fees) · equity $10,064.47 vs prior close $10,247.78 (-183.31) · 3 name(s) re-marked at the open (per-name table). AUTL×7 yday $2.44 → 09:30 $2.41 -0.21; AU×41 yday $123.39 → 09:30 $119.80 -147.19; FCX×63 yday $79.91 → 09:30 $79.34 -35.91 | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 7 | $2.41 | $0.21 | $-0.82 | $154.04 | ▼ -0.82 after sell → book $10,064.26; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `ASST` | 7 | $20.72 | $1.47 | — | $7.53 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list oppset; 🔵; ret5=+67.1; leftover $154.04 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.53 | ▼ close $9,977.54 vs 09:30 $10,064.47 (session -85.25) | 16:00 close · cash $7.53 · equity $9,977.54 vs 09:30 $10,064.47 (-86.93; session marks -85.25) · 3 name(s) marked open→close (per-name table). AU×41 09:30 $119.80 → close $118.11 -69.29; FCX×63 09:30 $79.34 → close $79.00 -21.42; ASST×7 09:30 $20.72 → close $21.50 +5.46 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.53 | ▼ 09:30 equity $9,944.78 vs yday $9,977.54 (-32.76) | 09:30 open · cash $7.53 (unchanged overnight, no fees) · equity $9,944.78 vs prior close $9,977.54 (-32.76) · 3 name(s) re-marked at the open (per-name table). AU×41 yday $118.11 → 09:30 $117.41 -28.70; FCX×63 yday $79.00 → 09:30 $78.83 -10.71; ASST×7 yday $21.50 → 09:30 $22.45 +6.65 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.53 | ▲ close $9,964.23 vs 09:30 $9,944.78 (session +19.45) | 16:00 close · cash $7.53 · equity $9,964.23 vs 09:30 $9,944.78 (+19.45; session marks +19.45) · 3 name(s) marked open→close (per-name table). AU×41 09:30 $117.41 → close $118.40 +40.59; FCX×63 09:30 $78.83 → close $78.42 -25.83; ASST×7 09:30 $22.45 → close $23.12 +4.69 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.53 | ▲ 09:30 equity $10,001.73 vs yday $9,964.23 (+37.50) | 09:30 open · cash $7.53 (unchanged overnight, no fees) · equity $10,001.73 vs prior close $9,964.23 (+37.50) · 3 name(s) re-marked at the open (per-name table). AU×41 yday $118.40 → 09:30 $119.19 +32.39; FCX×63 yday $78.42 → 09:30 $78.57 +9.45; ASST×7 yday $23.12 → 09:30 $22.50 -4.34 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 41 | $119.19 | $2.16 | $+23.19 | $4,892.15 | ▲ +23.19 after sell → book $9,999.56; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 63 | $78.57 | $2.23 | $+86.31 | $9,839.84 | ▲ +86.31 after sell → book $9,997.34; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 5 | $324.41 | $2.00 | — | $8,215.78 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1639.97 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 11 | $141.76 | $2.02 | — | $6,654.40 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $1639.97 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 4 | $400.42 | $2.00 | — | $5,050.72 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1639.97 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $3,742.69 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1639.97 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 6 | $240.22 | $2.01 | — | $2,299.36 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1639.97 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PLAB` | 54 | $30.01 | $2.15 | — | $676.67 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list oppset; 🔵; ret5=-0.9; leftover $1639.97 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $676.67 | ▼ close $9,560.89 vs 09:30 $10,001.73 (session -424.26) | 16:00 close · cash $676.67 · equity $9,560.89 vs 09:30 $10,001.73 (-440.84; session marks -424.26) · 7 name(s) marked open→close (per-name table). ASST×7 09:30 $22.50 → close $21.74 -5.32; KEYS×5 09:30 $324.41 → close $319.97 -22.20; SMTC×11 09:30 $141.76 → close $131.17 -116.49; CIEN×4 09:30 $400.42 → close $378.44 -87.92; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×6 09:30 $240.22 → close $236.98 -19.44; PLAB×54 09:30 $30.01 → close $27.73 -123.12 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $676.67 | ▲ 09:30 equity $9,595.81 vs yday $9,560.89 (+34.92) | 09:30 open · cash $676.67 (unchanged overnight, no fees) · equity $9,595.81 vs prior close $9,560.89 (+34.92) · 7 name(s) re-marked at the open (per-name table). ASST×7 yday $21.74 → 09:30 $22.54 +5.60; KEYS×5 yday $319.97 → 09:30 $322.49 +12.60; SMTC×11 yday $131.17 → 09:30 $132.30 +12.43; CIEN×4 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×6 yday $236.98 → 09:30 $233.97 -18.09; PLAB×54 yday $27.73 → 09:30 $28.04 +16.74 | — |
| 2026-08-31 09:30 ET | **SELL** | `ASST` | 7 | $22.54 | $1.62 | $+9.65 | $832.83 | ▲ +9.65 after sell → book $9,594.19; vs 09:30 mark -1.62 | dropped from list after 3 sess (min 3) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $832.83 | ▲ close $9,649.66 vs 09:30 $9,595.81 (session +55.47) | 16:00 close · cash $832.83 · equity $9,649.66 vs 09:30 $9,595.81 (+53.85; session marks +55.47) · 6 name(s) marked open→close (per-name table). KEYS×5 09:30 $322.49 → close $322.70 +1.05; SMTC×11 09:30 $132.30 → close $132.96 +7.26; CIEN×4 09:30 $378.44 → close $382.80 +17.44; MPWR×1 09:30 $1261.90 → close $1267.77 +5.87; DDOG×6 09:30 $233.97 → close $237.04 +18.45; PLAB×54 09:30 $28.04 → close $28.14 +5.40 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $832.83 | ▼ 09:30 equity $9,489.32 vs yday $9,649.66 (-160.34) | 09:30 open · cash $832.83 (unchanged overnight, no fees) · equity $9,489.32 vs prior close $9,649.66 (-160.34) · 6 name(s) re-marked at the open (per-name table). KEYS×5 yday $322.70 → 09:30 $321.47 -6.15; SMTC×11 yday $132.96 → 09:30 $127.63 -58.63; CIEN×4 yday $382.80 → 09:30 $376.89 -23.64; MPWR×1 yday $1267.77 → 09:30 $1245.11 -22.66; DDOG×6 yday $237.04 → 09:30 $232.88 -24.96; PLAB×54 yday $28.14 → 09:30 $27.69 -24.30 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $832.83 | ▼ close $9,370.29 vs 09:30 $9,489.32 (session -119.03) | 16:00 close · cash $832.83 · equity $9,370.29 vs 09:30 $9,489.32 (-119.03; session marks -119.03) · 6 name(s) marked open→close (per-name table). KEYS×5 09:30 $321.47 → close $319.27 -11.00; SMTC×11 09:30 $127.63 → close $132.27 +51.04; CIEN×4 09:30 $376.89 → close $360.33 -66.24; MPWR×1 09:30 $1245.11 → close $1225.96 -19.15; DDOG×6 09:30 $232.88 → close $223.84 -54.24; PLAB×54 09:30 $27.69 → close $27.33 -19.44 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $832.83 | ▼ 09:30 equity $9,336.85 vs yday $9,370.29 (-33.44) | 09:30 open · cash $832.83 (unchanged overnight, no fees) · equity $9,336.85 vs prior close $9,370.29 (-33.44) · 6 name(s) re-marked at the open (per-name table). KEYS×5 yday $319.27 → 09:30 $318.04 -6.15; SMTC×11 yday $132.27 → 09:30 $133.00 +8.03; CIEN×4 yday $360.33 → 09:30 $357.25 -12.32; MPWR×1 yday $1225.96 → 09:30 $1224.92 -1.04; DDOG×6 yday $223.84 → 09:30 $219.46 -26.28; PLAB×54 yday $27.33 → 09:30 $27.41 +4.32 | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 5 | $318.04 | $2.03 | $-35.88 | $2,421.01 | ▼ -35.88 after sell → book $9,334.83; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 11 | $133.00 | $2.04 | $-100.43 | $3,881.96 | ▼ -100.43 after sell → book $9,332.78; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 4 | $357.25 | $2.02 | $-176.71 | $5,308.94 | ▼ -176.71 after sell → book $9,330.76; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 1 | $1224.92 | $2.01 | $-85.12 | $6,531.84 | ▼ -85.12 after sell → book $9,328.74; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 6 | $219.46 | $2.03 | $-128.60 | $7,846.58 | ▼ -128.60 after sell → book $9,326.72; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `PLAB` | 54 | $27.41 | $2.17 | $-144.73 | $9,324.54 | ▼ -144.73 after sell → book $9,324.54; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,324.54 | ▲ close $9,324.54 vs 09:30 $9,336.85 (session +0.00) | 16:00 close · cash $9,324.54 · no lots left · equity $9,324.54. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,324.54 | ▲ 09:30 equity $9,324.54 vs yday $9,324.54 (+0.00) | 09:30 open · cash $9,324.54 · no holdings · equity $9,324.54 vs prior close $9,324.54 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 13 | $351.74 | $2.03 | — | $4,749.89 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $4662.27 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 9 | $486.31 | $2.02 | — | $371.09 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $4662.27 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $371.09 | ▲ close $9,661.68 vs 09:30 $9,324.54 (session +341.18) | 16:00 close · cash $371.09 · equity $9,661.68 vs 09:30 $9,324.54 (+337.14; session marks +341.18) · 2 name(s) marked open→close (per-name table). AVGO×13 09:30 $351.74 → close $357.16 +70.46; DELL×9 09:30 $486.31 → close $516.39 +270.72 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $371.09 | ▲ 09:30 equity $9,671.21 vs yday $9,661.68 (+9.53) | 09:30 open · cash $371.09 (unchanged overnight, no fees) · equity $9,671.21 vs prior close $9,661.68 (+9.53) · 2 name(s) re-marked at the open (per-name table). AVGO×13 yday $357.16 → 09:30 $359.70 +33.02; DELL×9 yday $516.39 → 09:30 $513.78 -23.49 | — |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 4 | $16.40 | $0.67 | — | $304.82 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $74.22 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MMED` | 3 | $23.84 | $0.72 | — | $232.57 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list oppset; 🔵; ⚪; ret5=+22.2; leftover $74.22 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HPE` | 1 | $53.85 | $0.54 | — | $178.18 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list oppset; 🔵; ret5=+0.1; leftover $74.22 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟡 vol🟢 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $178.18 | ▲ close $9,735.25 vs 09:30 $9,671.21 (session +65.98) | 16:00 close · cash $178.18 · equity $9,735.25 vs 09:30 $9,671.21 (+64.04; session marks +65.98) · 5 name(s) marked open→close (per-name table). AVGO×13 09:30 $359.70 → close $357.90 -23.40; DELL×9 09:30 $513.78 → close $524.14 +93.24; FRNM×4 09:30 $16.40 → close $16.31 -0.36; MMED×3 09:30 $23.84 → close $23.29 -1.65; HPE×1 09:30 $53.85 → close $52.00 -1.85 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $178.18 | ▲ 09:30 equity $9,785.10 vs yday $9,735.25 (+49.85) | 09:30 open · cash $178.18 (unchanged overnight, no fees) · equity $9,785.10 vs prior close $9,735.25 (+49.85) · 5 name(s) re-marked at the open (per-name table). AVGO×13 yday $357.90 → 09:30 $363.68 +75.14; DELL×9 yday $524.14 → 09:30 $521.15 -26.91; FRNM×4 yday $16.31 → 09:30 $16.74 +1.72; MMED×3 yday $23.29 → 09:30 $23.16 -0.39; HPE×1 yday $52.00 → 09:30 $52.29 +0.29 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $178.18 | ▲ close $9,964.33 vs 09:30 $9,785.10 (session +179.23) | 16:00 close · cash $178.18 · equity $9,964.33 vs 09:30 $9,785.10 (+179.23; session marks +179.23) · 5 name(s) marked open→close (per-name table). AVGO×13 09:30 $363.68 → close $368.56 +63.44; DELL×9 09:30 $521.15 → close $533.88 +114.57; FRNM×4 09:30 $16.74 → close $15.99 -3.00; MMED×3 09:30 $23.16 → close $23.32 +0.48; HPE×1 09:30 $52.29 → close $56.03 +3.74 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $178.18 | ▲ 09:30 equity $9,975.84 vs yday $9,964.33 (+11.51) | 09:30 open · cash $178.18 (unchanged overnight, no fees) · equity $9,975.84 vs prior close $9,964.33 (+11.51) · 5 name(s) re-marked at the open (per-name table). AVGO×13 yday $368.56 → 09:30 $366.23 -30.29; DELL×9 yday $533.88 → 09:30 $538.47 +41.31; FRNM×4 yday $15.99 → 09:30 $15.96 -0.12; MMED×3 yday $23.32 → 09:30 $23.22 -0.30; HPE×1 yday $56.03 → 09:30 $56.94 +0.91 | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 13 | $366.23 | $2.08 | $+184.26 | $4,937.10 | ▲ +184.26 after sell → book $9,973.77; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 9 | $538.47 | $2.07 | $+465.36 | $9,781.26 | ▲ +465.36 after sell → book $9,971.70; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,781.26 | ▼ close $9,971.44 vs 09:30 $9,975.84 (session -0.26) | 16:00 close · cash $9,781.26 · equity $9,971.44 vs 09:30 $9,975.84 (-4.40; session marks -0.26) · 3 name(s) marked open→close (per-name table). FRNM×4 09:30 $15.96 → close $15.75 -0.84; MMED×3 09:30 $23.22 → close $22.76 -1.38; HPE×1 09:30 $56.94 → close $58.90 +1.96 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,781.26 | ▼ 09:30 equity $9,969.24 vs yday $9,971.44 (-2.20) | 09:30 open · cash $9,781.26 (unchanged overnight, no fees) · equity $9,969.24 vs prior close $9,971.44 (-2.20) · 3 name(s) re-marked at the open (per-name table). FRNM×4 yday $15.75 → 09:30 $15.64 -0.44; MMED×3 yday $22.76 → 09:30 $22.54 -0.66; HPE×1 yday $58.90 → 09:30 $57.80 -1.10 | — |
| 2026-09-10 09:30 ET | **SELL** | `FRNM` | 4 | $15.64 | $0.66 | $-4.37 | $9,843.16 | ▼ -4.37 after sell → book $9,968.58; vs 09:30 mark -0.66 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MMED` | 3 | $22.54 | $0.71 | $-5.33 | $9,910.08 | ▼ -5.33 after sell → book $9,967.88; vs 09:30 mark -0.70 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `HPE` | 1 | $57.80 | $0.60 | $+2.81 | $9,967.28 | ▲ +2.81 after sell → book $9,967.28; vs 09:30 mark -0.60 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,967.28 | ▲ close $9,967.28 vs 09:30 $9,969.24 (session +0.00) | 16:00 close · cash $9,967.28 · no lots left · equity $9,967.28. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,967.28 | ▲ 09:30 equity $9,967.28 vs yday $9,967.28 (-0.00) | 09:30 open · cash $9,967.28 · no holdings · equity $9,967.28 vs prior close $9,967.28 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 60 | $164.43 | $2.17 | — | $99.31 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $9967.28 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.31 | ▼ close $9,116.11 vs 09:30 $9,967.28 (session -849.00) | 16:00 close · cash $99.31 · equity $9,116.11 vs 09:30 $9,967.28 (-851.17; session marks -849.00) · 1 name(s) marked open→close (per-name table). ORCL×60 09:30 $164.43 → close $150.28 -849.00 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.31 | ▼ 09:30 equity $8,584.51 vs yday $9,116.11 (-531.60) | 09:30 open · cash $99.31 (unchanged overnight, no fees) · equity $8,584.51 vs prior close $9,116.11 (-531.60) · 1 name(s) re-marked at the open (per-name table). ORCL×60 yday $150.28 → 09:30 $141.42 -531.60 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.31 | ▲ close $8,786.71 vs 09:30 $8,584.51 (session +202.20) | 16:00 close · cash $99.31 · equity $8,786.71 vs 09:30 $8,584.51 (+202.20; session marks +202.20) · 1 name(s) marked open→close (per-name table). ORCL×60 09:30 $141.42 → close $144.79 +202.20 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.31 | ▼ 09:30 equity $8,706.91 vs yday $8,786.71 (-79.80) | 09:30 open · cash $99.31 (unchanged overnight, no fees) · equity $8,706.91 vs prior close $8,786.71 (-79.80) · 1 name(s) re-marked at the open (per-name table). ORCL×60 yday $144.79 → 09:30 $143.46 -79.80 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.31 | ▼ close $8,520.31 vs 09:30 $8,706.91 (session -186.60) | 16:00 close · cash $99.31 · equity $8,520.31 vs 09:30 $8,706.91 (-186.60; session marks -186.60) · 1 name(s) marked open→close (per-name table). ORCL×60 09:30 $143.46 → close $140.35 -186.60 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.31 | ▼ 09:30 equity $8,501.11 vs yday $8,520.31 (-19.20) | 09:30 open · cash $99.31 (unchanged overnight, no fees) · equity $8,501.11 vs prior close $8,520.31 (-19.20) · 1 name(s) re-marked at the open (per-name table). ORCL×60 yday $140.35 → 09:30 $140.03 -19.20 | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 60 | $140.03 | $2.25 | $-1468.42 | $8,498.86 | ▼ -1,468.42 after sell → book $8,498.86; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 107 | $26.27 | $2.31 | — | $5,685.66 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2832.95 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 14 | $189.17 | $2.03 | — | $3,035.25 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $2832.95 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 70 | $39.99 | $2.20 | — | $233.75 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2832.95 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $233.75 | ▼ close $8,337.84 vs 09:30 $8,501.11 (session -154.48) | 16:00 close · cash $233.75 · equity $8,337.84 vs 09:30 $8,501.11 (-163.27; session marks -154.48) · 3 name(s) marked open→close (per-name table). WAY×107 09:30 $26.27 → close $26.59 +34.24; QCOM×14 09:30 $189.17 → close $184.84 -60.62; SM×70 09:30 $39.99 → close $38.16 -128.10 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $233.75 | ▲ 09:30 equity $8,365.12 vs yday $8,337.84 (+27.28) | 09:30 open · cash $233.75 (unchanged overnight, no fees) · equity $8,365.12 vs prior close $8,337.84 (+27.28) · 3 name(s) re-marked at the open (per-name table). WAY×107 yday $26.59 → 09:30 $26.51 -8.56; QCOM×14 yday $184.84 → 09:30 $190.35 +77.14; SM×70 yday $38.16 → 09:30 $37.57 -41.30 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 1 | $170.85 | $1.71 | — | $61.18 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $233.75 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.18 | ▼ close $8,305.78 vs 09:30 $8,365.12 (session -57.62) | 16:00 close · cash $61.18 · equity $8,305.78 vs 09:30 $8,365.12 (-59.34; session marks -57.62) · 4 name(s) marked open→close (per-name table). WAY×107 09:30 $26.51 → close $26.51 +0.00; QCOM×14 09:30 $190.35 → close $188.71 -22.96; SM×70 09:30 $37.57 → close $36.97 -42.00; SMTC×1 09:30 $170.85 → close $178.19 +7.34 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.18 | ▲ 09:30 equity $8,386.82 vs yday $8,305.78 (+81.04) | 09:30 open · cash $61.18 (unchanged overnight, no fees) · equity $8,386.82 vs prior close $8,305.78 (+81.04) · 4 name(s) re-marked at the open (per-name table). WAY×107 yday $26.51 → 09:30 $26.95 +47.08; QCOM×14 yday $188.71 → 09:30 $191.34 +36.82; SM×70 yday $36.97 → 09:30 $36.87 -7.00; SMTC×1 yday $178.19 → 09:30 $182.33 +4.14 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 1 | $14.79 | $0.15 | — | $46.24 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $20.39 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.24 | ▼ close $8,067.35 vs 09:30 $8,386.82 (session -319.32) | 16:00 close · cash $46.24 · equity $8,067.35 vs 09:30 $8,386.82 (-319.47; session marks -319.32) · 5 name(s) marked open→close (per-name table). WAY×107 09:30 $26.95 → close $25.66 -138.03; QCOM×14 09:30 $191.34 → close $177.72 -190.68; SM×70 09:30 $36.87 → close $36.97 +7.00; SMTC×1 09:30 $182.33 → close $185.00 +2.67; RARE×1 09:30 $14.79 → close $14.51 -0.28 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 18.77 < 1 share @ 119.43 |
| 2026-08-21 | `CRSP` | cash | leftover split 18.77 < 1 share @ 59.72 |
| 2026-08-21 | `FUTU` | cash | leftover split 18.77 < 1 share @ 115.18 |
| 2026-08-21 | `GRAL` | cash | leftover split 18.77 < 1 share @ 78.88 |
| 2026-08-21 | `VIRT` | cash | leftover split 18.77 < 1 share @ 60.66 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 1.08 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 1.08 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 1.08 < 1 share @ 1746.53 |
| 2026-08-27 | `CM` | cash | leftover split 1.08 < 1 share @ 118.77 |
| 2026-08-27 | `GEN` | cash | leftover split 1.08 < 1 share @ 29.83 |
| 2026-08-27 | `LRCX` | cash | leftover split 1.08 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 1.08 < 1 share @ 222.86 |
| 2026-08-28 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DDOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PLAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DDOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `PLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 74.22 < 1 share @ 263.36 |
| 2026-09-04 | `MRX` | cash | leftover split 74.22 < 1 share @ 75.65 |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `HPE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-09 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `HPE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `TH` | cash | leftover split 20.39 < 1 share @ 20.91 |
| 2026-09-18 | `GME` | cash | leftover split 20.39 < 1 share @ 22.90 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `WAY` | 107 | 2026-09-16 @ $26.27 | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2832.95 |
| `QCOM` | 14 | 2026-09-16 @ $189.17 | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $2832.95 |
| `SM` | 70 | 2026-09-16 @ $39.99 | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2832.95 |
| `SMTC` | 1 | 2026-09-17 @ $170.85 | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $233.75 |
| `RARE` | 1 | 2026-09-18 @ $14.79 | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $20.39 |
