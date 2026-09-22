# Factor mine action — `union_news_g_cam71_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · merged news🟢 and cameras +7 −≤1

Cash book **-19.09%** ($8,091) · signal-only (no cash/fees) was -16.16%. Starts YES **2/27**. Fills 38 · skips 56 · realized $-1909.09.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,090.93.

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
| 2026-08-21 | `AUTL` | 9 | — | $2.47 | +0.00 | $2.41 | -0.54 | -0.54 | +0.00 | -0.54 |
| 2026-08-24 | `BHP` | 54 | $97.03 | $97.31 | +15.12 | $97.13 | -9.72 | +5.40 | +340.20 | +330.48 |
| 2026-08-24 | `APA` | 111 | $43.39 | $42.93 | -51.06 | $42.96 | +3.33 | -47.73 | -203.13 | -199.80 |
| 2026-08-24 | `AUTL` | 9 | $2.41 | $2.40 | -0.09 | $2.34 | -0.54 | -0.63 | -0.63 | -1.17 |
| 2026-08-25 | `BHP` | 54 | $97.13 | $95.86 | -68.58 | — | +0.00 | -68.58 | +261.90 | — |
| 2026-08-25 | `APA` | 111 | $42.96 | $41.38 | -175.38 | — | +0.00 | -175.38 | -375.18 | — |
| 2026-08-25 | `AUTL` | 9 | $2.34 | $2.38 | +0.36 | $2.44 | +0.54 | +0.90 | -0.81 | -0.27 |
| 2026-08-25 | `AU` | 41 | — | $118.52 | +0.00 | $123.39 | +199.67 | +199.67 | +0.00 | +199.67 |
| 2026-08-25 | `FCX` | 63 | — | $77.13 | +0.00 | $79.91 | +175.14 | +175.14 | +0.00 | +175.14 |
| 2026-08-26 | `AUTL` | 9 | $2.44 | $2.41 | -0.27 | — | +0.00 | -0.27 | -0.54 | — |
| 2026-08-26 | `AU` | 41 | $123.39 | $119.80 | -147.19 | $118.11 | -69.29 | -216.48 | +52.48 | -16.81 |
| 2026-08-26 | `FCX` | 63 | $79.91 | $79.34 | -35.91 | $79.00 | -21.42 | -57.33 | +139.23 | +117.81 |
| 2026-08-26 | `CM` | 1 | — | $118.50 | +0.00 | $118.20 | -0.30 | -0.30 | +0.00 | -0.30 |
| 2026-08-27 | `AU` | 41 | $118.11 | $117.41 | -28.70 | $118.40 | +40.59 | +11.89 | -45.51 | -4.92 |
| 2026-08-27 | `FCX` | 63 | $79.00 | $78.83 | -10.71 | $78.42 | -25.83 | -36.54 | +107.10 | +81.27 |
| 2026-08-27 | `CM` | 1 | $118.20 | $118.77 | +0.57 | $114.84 | -3.93 | -3.36 | +0.27 | -3.66 |
| 2026-08-28 | `AU` | 41 | $118.40 | $119.19 | +32.39 | — | +0.00 | +32.39 | +27.47 | — |
| 2026-08-28 | `FCX` | 63 | $78.42 | $78.57 | +9.45 | — | +0.00 | +9.45 | +90.72 | — |
| 2026-08-28 | `CM` | 1 | $114.84 | $115.66 | +0.82 | $114.33 | -1.33 | -0.51 | -2.84 | -4.17 |
| 2026-08-28 | `KEYS` | 6 | — | $324.41 | +0.00 | $319.97 | -26.64 | -26.64 | +0.00 | -26.64 |
| 2026-08-28 | `SMTC` | 13 | — | $141.76 | +0.00 | $131.17 | -137.67 | -137.67 | +0.00 | -137.67 |
| 2026-08-28 | `CIEN` | 4 | — | $400.42 | +0.00 | $378.44 | -87.92 | -87.92 | +0.00 | -87.92 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 8 | — | $240.22 | +0.00 | $236.98 | -25.92 | -25.92 | +0.00 | -25.92 |
| 2026-08-31 | `CM` | 1 | $114.33 | $114.46 | +0.13 | — | +0.00 | +0.13 | -4.04 | — |
| 2026-08-31 | `KEYS` | 6 | $319.97 | $322.49 | +15.12 | $322.70 | +1.26 | +16.38 | -11.52 | -10.26 |
| 2026-08-31 | `SMTC` | 13 | $131.17 | $132.30 | +14.69 | $132.96 | +8.58 | +23.27 | -122.98 | -114.40 |
| 2026-08-31 | `CIEN` | 4 | $378.44 | $378.44 | +0.00 | $382.80 | +17.44 | +17.44 | -87.92 | -70.48 |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | $1267.77 | +5.87 | +11.51 | -44.13 | -38.26 |
| 2026-08-31 | `DDOG` | 8 | $236.98 | $233.97 | -24.12 | $237.04 | +24.60 | +0.48 | -50.04 | -25.44 |
| 2026-09-01 | `KEYS` | 6 | $322.70 | $321.47 | -7.38 | $319.27 | -13.20 | -20.58 | -17.64 | -30.84 |
| 2026-09-01 | `SMTC` | 13 | $132.96 | $127.63 | -69.29 | $132.27 | +60.32 | -8.97 | -183.69 | -123.37 |
| 2026-09-01 | `CIEN` | 4 | $382.80 | $376.89 | -23.64 | $360.33 | -66.24 | -89.88 | -94.12 | -160.36 |
| 2026-09-01 | `MPWR` | 1 | $1267.77 | $1245.11 | -22.66 | $1225.96 | -19.15 | -41.81 | -60.92 | -80.07 |
| 2026-09-01 | `DDOG` | 8 | $237.04 | $232.88 | -33.28 | $223.84 | -72.32 | -105.60 | -58.72 | -131.04 |
| 2026-09-02 | `KEYS` | 6 | $319.27 | $318.04 | -7.38 | — | +0.00 | -7.38 | -38.22 | — |
| 2026-09-02 | `SMTC` | 13 | $132.27 | $133.00 | +9.49 | — | +0.00 | +9.49 | -113.88 | — |
| 2026-09-02 | `CIEN` | 4 | $360.33 | $357.25 | -12.32 | — | +0.00 | -12.32 | -172.68 | — |
| 2026-09-02 | `MPWR` | 1 | $1225.96 | $1224.92 | -1.04 | — | +0.00 | -1.04 | -81.11 | — |
| 2026-09-02 | `DDOG` | 8 | $223.84 | $219.46 | -35.04 | — | +0.00 | -35.04 | -166.08 | — |
| 2026-09-03 | `AVGO` | 13 | — | $351.74 | +0.00 | $357.16 | +70.46 | +70.46 | +0.00 | +70.46 |
| 2026-09-03 | `DELL` | 9 | — | $486.31 | +0.00 | $516.39 | +270.72 | +270.72 | +0.00 | +270.72 |
| 2026-09-04 | `AVGO` | 13 | $357.16 | $359.70 | +33.02 | $357.90 | -23.40 | +9.62 | +103.48 | +80.08 |
| 2026-09-04 | `DELL` | 9 | $516.39 | $513.78 | -23.49 | $524.14 | +93.24 | +69.75 | +247.23 | +340.47 |
| 2026-09-04 | `FRNM` | 8 | — | $16.40 | +0.00 | $16.31 | -0.72 | -0.72 | +0.00 | -0.72 |
| 2026-09-04 | `MRX` | 1 | — | $75.65 | +0.00 | $78.27 | +2.62 | +2.62 | +0.00 | +2.62 |
| 2026-09-08 | `AVGO` | 13 | $357.90 | $363.68 | +75.14 | $368.56 | +63.44 | +138.58 | +155.22 | +218.66 |
| 2026-09-08 | `DELL` | 9 | $524.14 | $521.15 | -26.91 | $533.88 | +114.57 | +87.66 | +313.56 | +428.13 |
| 2026-09-08 | `FRNM` | 8 | $16.31 | $16.74 | +3.44 | $15.99 | -6.00 | -2.56 | +2.72 | -3.28 |
| 2026-09-08 | `MRX` | 1 | $78.27 | $78.84 | +0.57 | $76.71 | -2.13 | -1.56 | +3.19 | +1.06 |
| 2026-09-09 | `AVGO` | 13 | $368.56 | $366.23 | -30.29 | — | +0.00 | -30.29 | +188.37 | — |
| 2026-09-09 | `DELL` | 9 | $533.88 | $538.47 | +41.31 | — | +0.00 | +41.31 | +469.44 | — |
| 2026-09-09 | `FRNM` | 8 | $15.99 | $15.96 | -0.24 | $15.75 | -1.68 | -1.92 | -3.52 | -5.20 |
| 2026-09-09 | `MRX` | 1 | $76.71 | $76.60 | -0.11 | $75.72 | -0.88 | -0.99 | +0.95 | +0.07 |
| 2026-09-10 | `FRNM` | 8 | $15.75 | $15.64 | -0.88 | — | +0.00 | -0.88 | -6.08 | — |
| 2026-09-10 | `MRX` | 1 | $75.72 | $75.00 | -0.72 | — | +0.00 | -0.72 | -0.65 | — |
| 2026-09-11 | `ORCL` | 60 | — | $164.43 | +0.00 | $150.28 | -849.00 | -849.00 | +0.00 | -849.00 |
| 2026-09-14 | `ORCL` | 60 | $150.28 | $141.42 | -531.60 | $144.79 | +202.20 | -329.40 | -1380.60 | -1178.40 |
| 2026-09-15 | `ORCL` | 60 | $144.79 | $143.46 | -79.80 | $140.35 | -186.60 | -266.40 | -1258.20 | -1444.80 |
| 2026-09-16 | `ORCL` | 60 | $140.35 | $140.03 | -19.20 | — | +0.00 | -19.20 | -1464.00 | — |
| 2026-09-16 | `WAY` | 108 | — | $26.27 | +0.00 | $26.59 | +34.56 | +34.56 | +0.00 | +34.56 |
| 2026-09-16 | `QCOM` | 15 | — | $189.17 | +0.00 | $184.84 | -64.95 | -64.95 | +0.00 | -64.95 |
| 2026-09-16 | `SM` | 71 | — | $39.99 | +0.00 | $38.16 | -129.93 | -129.93 | +0.00 | -129.93 |
| 2026-09-17 | `WAY` | 108 | $26.59 | $26.51 | -8.64 | $26.51 | +0.00 | -8.64 | +25.92 | +25.92 |
| 2026-09-17 | `QCOM` | 15 | $184.84 | $190.35 | +82.65 | $188.71 | -24.60 | +58.05 | +17.70 | -6.90 |
| 2026-09-17 | `SM` | 71 | $38.16 | $37.57 | -41.89 | $36.97 | -42.60 | -84.49 | -171.82 | -214.42 |
| 2026-09-18 | `WAY` | 108 | $26.51 | $26.95 | +47.52 | $25.66 | -139.32 | -91.80 | +73.44 | -65.88 |
| 2026-09-18 | `QCOM` | 15 | $188.71 | $191.34 | +39.45 | $177.72 | -204.30 | -164.85 | +32.55 | -171.75 |
| 2026-09-18 | `SM` | 71 | $36.97 | $36.87 | -7.10 | $36.97 | +7.10 | +0.00 | -221.52 | -214.42 |
| 2026-09-21 | `WAY` | 108 | $25.66 | $25.94 | +30.24 | — | +0.00 | +30.24 | -35.64 | — |
| 2026-09-21 | `QCOM` | 15 | $177.72 | $180.61 | +43.35 | — | +0.00 | +43.35 | -128.40 | — |
| 2026-09-21 | `SM` | 71 | $36.97 | $35.91 | -75.26 | — | +0.00 | -75.26 | -289.68 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +100.41 | BHP, APA | — | $112.62 | $10,095.93 | BHP×54, APA×111 |
| 2026-08-21 | +3.25 | $112.62 | BHP×54, APA×111 | $10,223.23 | +127.30 | -55.23 | AUTL | — | $90.15 | $10,167.75 | BHP×54, APA×111, AUTL×9 |
| 2026-08-24 | -5.17 | $90.15 | BHP×54, APA×111, AUTL×9 | $10,131.72 | -36.03 | -6.93 | — | — | $90.15 | $10,124.79 | BHP×54, APA×111, AUTL×9 |
| 2026-08-25 | +1.80 | $90.15 | BHP×54, APA×111, AUTL×9 | $9,881.19 | -243.60 | +375.35 | AU, FCX | BHP, APA | $132.38 | $10,247.66 | AUTL×9, AU×41, FCX×63 |
| 2026-08-26 | +2.02 | $132.38 | AUTL×9, AU×41, FCX×63 | $10,064.29 | -183.37 | -91.01 | CM | AUTL | $34.12 | $9,971.83 | AU×41, FCX×63, CM×1 |
| 2026-08-27 | — | $34.12 | AU×41, FCX×63, CM×1 | $9,932.99 | -38.84 | +10.83 | — | — | $34.12 | $9,943.82 | AU×41, FCX×63, CM×1 |
| 2026-08-28 | +0.75 | $34.12 | AU×41, FCX×63, CM×1 | $9,986.48 | +42.66 | -329.25 | KEYS, SMTC, CIEN, MPWR, DDOG | AU, FCX | $1,237.57 | $9,642.79 | CM×1, KEYS×6, SMTC×13, CIEN×4, MPWR×1, DDOG×8 |
| 2026-08-31 | -5.85 | $1,237.57 | CM×1, KEYS×6, SMTC×13, CIEN×4, MPWR×1, DDOG×8 | $9,654.25 | +11.46 | +57.75 | — | CM | $1,350.87 | $9,710.84 | KEYS×6, SMTC×13, CIEN×4, MPWR×1, DDOG×8 |
| 2026-09-01 | -6.30 | $1,350.87 | KEYS×6, SMTC×13, CIEN×4, MPWR×1, DDOG×8 | $9,554.59 | -156.25 | -110.59 | — | — | $1,350.87 | $9,444.00 | KEYS×6, SMTC×13, CIEN×4, MPWR×1, DDOG×8 |
| 2026-09-02 | -3.83 | $1,350.87 | KEYS×6, SMTC×13, CIEN×4, MPWR×1, DDOG×8 | $9,397.71 | -46.29 | +0.00 | — | KEYS, SMTC, CIEN, MPWR, DDOG | $9,387.54 | $9,387.54 | — |
| 2026-09-03 | -0.90 | $9,387.54 | — | $9,387.54 | +0.00 | +341.18 | AVGO, DELL | — | $434.09 | $9,724.68 | AVGO×13, DELL×9 |
| 2026-09-04 | +2.25 | $434.09 | AVGO×13, DELL×9 | $9,734.21 | +9.53 | +71.74 | FRNM, MRX | — | $225.14 | $9,803.85 | AVGO×13, DELL×9, FRNM×8, MRX×1 |
| 2026-09-08 | -11.47 | $225.14 | AVGO×13, DELL×9, FRNM×8, MRX×1 | $9,856.09 | +52.24 | +169.88 | — | — | $225.14 | $10,025.97 | AVGO×13, DELL×9, FRNM×8, MRX×1 |
| 2026-09-09 | -13.95 | $225.14 | AVGO×13, DELL×9, FRNM×8, MRX×1 | $10,036.64 | +10.67 | -2.56 | — | AVGO, DELL | $9,828.22 | $10,029.94 | FRNM×8, MRX×1 |
| 2026-09-10 | -13.28 | $9,828.22 | FRNM×8, MRX×1 | $10,028.34 | -1.60 | +0.00 | — | FRNM, MRX | $10,026.27 | $10,026.27 | — |
| 2026-09-11 | +0.50 | $10,026.27 | — | $10,026.27 | +0.00 | -849.00 | ORCL | — | $158.30 | $9,175.10 | ORCL×60 |
| 2026-09-14 | -11.00 | $158.30 | ORCL×60 | $8,643.50 | -531.60 | +202.20 | — | — | $158.30 | $8,845.70 | ORCL×60 |
| 2026-09-15 | -3.84 | $158.30 | ORCL×60 | $8,765.90 | -79.80 | -186.60 | — | — | $158.30 | $8,579.30 | ORCL×60 |
| 2026-09-16 | +5.30 | $158.30 | ORCL×60 | $8,560.10 | -19.20 | -160.32 | WAY, QCOM, SM | ORCL | $37.30 | $8,390.98 | WAY×108, QCOM×15, SM×71 |
| 2026-09-17 | +7.38 | $37.30 | WAY×108, QCOM×15, SM×71 | $8,423.10 | +32.12 | -67.20 | — | — | $37.30 | $8,355.90 | WAY×108, QCOM×15, SM×71 |
| 2026-09-18 | +4.86 | $37.30 | WAY×108, QCOM×15, SM×71 | $8,435.77 | +79.87 | -336.52 | — | — | $37.30 | $8,099.25 | WAY×108, QCOM×15, SM×71 |
| 2026-09-21 | +12.87 | $37.30 | WAY×108, QCOM×15, SM×71 | $8,097.58 | -1.67 | +0.00 | — | WAY, QCOM, SM | $8,090.93 | $8,090.93 | — |

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
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 9 | $2.47 | $0.25 | — | $90.15 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $22.52 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.15 | ▼ close $10,167.75 vs 09:30 $10,223.23 (session -55.23) | 16:00 close · cash $90.15 · equity $10,167.75 vs 09:30 $10,223.23 (-55.48; session marks -55.23) · 3 name(s) marked open→close (per-name table). BHP×54 09:30 $95.72 → close $97.03 +70.74; APA×111 09:30 $44.52 → close $43.39 -125.43; AUTL×9 09:30 $2.47 → close $2.41 -0.54 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.15 | ▼ 09:30 equity $10,131.72 vs yday $10,167.75 (-36.03) | 09:30 open · cash $90.15 (unchanged overnight, no fees) · equity $10,131.72 vs prior close $10,167.75 (-36.03) · 3 name(s) re-marked at the open (per-name table). BHP×54 yday $97.03 → 09:30 $97.31 +15.12; APA×111 yday $43.39 → 09:30 $42.93 -51.06; AUTL×9 yday $2.41 → 09:30 $2.40 -0.09 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.15 | ▼ close $10,124.79 vs 09:30 $10,131.72 (session -6.93) | 16:00 close · cash $90.15 · equity $10,124.79 vs 09:30 $10,131.72 (-6.93; session marks -6.93) · 3 name(s) marked open→close (per-name table). BHP×54 09:30 $97.31 → close $97.13 -9.72; APA×111 09:30 $42.93 → close $42.96 +3.33; AUTL×9 09:30 $2.40 → close $2.34 -0.54 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.15 | ▼ 09:30 equity $9,881.19 vs yday $10,124.79 (-243.60) | 09:30 open · cash $90.15 (unchanged overnight, no fees) · equity $9,881.19 vs prior close $10,124.79 (-243.60) · 3 name(s) re-marked at the open (per-name table). BHP×54 yday $97.13 → 09:30 $95.86 -68.58; APA×111 yday $42.96 → 09:30 $41.38 -175.38; AUTL×9 yday $2.34 → 09:30 $2.38 +0.36 | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 54 | $95.86 | $2.20 | $+257.54 | $5,264.38 | ▲ +257.54 after sell → book $9,878.98; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 111 | $41.38 | $2.38 | $-379.88 | $9,855.18 | ▼ -379.88 after sell → book $9,876.60; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 41 | $118.52 | $2.11 | — | $4,993.75 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $4927.59 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 63 | $77.13 | $2.18 | — | $132.38 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $4927.59 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.38 | ▲ close $10,247.66 vs 09:30 $9,881.19 (session +375.35) | 16:00 close · cash $132.38 · equity $10,247.66 vs 09:30 $9,881.19 (+366.47; session marks +375.35) · 3 name(s) marked open→close (per-name table). AUTL×9 09:30 $2.38 → close $2.44 +0.54; AU×41 09:30 $118.52 → close $123.39 +199.67; FCX×63 09:30 $77.13 → close $79.91 +175.14 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.38 | ▼ 09:30 equity $10,064.29 vs yday $10,247.66 (-183.37) | 09:30 open · cash $132.38 (unchanged overnight, no fees) · equity $10,064.29 vs prior close $10,247.66 (-183.37) · 3 name(s) re-marked at the open (per-name table). AUTL×9 yday $2.44 → 09:30 $2.41 -0.27; AU×41 yday $123.39 → 09:30 $119.80 -147.19; FCX×63 yday $79.91 → 09:30 $79.34 -35.91 | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 9 | $2.41 | $0.26 | $-1.05 | $153.81 | ▼ -1.05 after sell → book $10,064.03; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 1 | $118.50 | $1.19 | — | $34.12 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $153.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.12 | ▼ close $9,971.83 vs 09:30 $10,064.29 (session -91.01) | 16:00 close · cash $34.12 · equity $9,971.83 vs 09:30 $10,064.29 (-92.46; session marks -91.01) · 3 name(s) marked open→close (per-name table). AU×41 09:30 $119.80 → close $118.11 -69.29; FCX×63 09:30 $79.34 → close $79.00 -21.42; CM×1 09:30 $118.50 → close $118.20 -0.30 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.12 | ▼ 09:30 equity $9,932.99 vs yday $9,971.83 (-38.84) | 09:30 open · cash $34.12 (unchanged overnight, no fees) · equity $9,932.99 vs prior close $9,971.83 (-38.84) · 3 name(s) re-marked at the open (per-name table). AU×41 yday $118.11 → 09:30 $117.41 -28.70; FCX×63 yday $79.00 → 09:30 $78.83 -10.71; CM×1 yday $118.20 → 09:30 $118.77 +0.57 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.12 | ▲ close $9,943.82 vs 09:30 $9,932.99 (session +10.83) | 16:00 close · cash $34.12 · equity $9,943.82 vs 09:30 $9,932.99 (+10.83; session marks +10.83) · 3 name(s) marked open→close (per-name table). AU×41 09:30 $117.41 → close $118.40 +40.59; FCX×63 09:30 $78.83 → close $78.42 -25.83; CM×1 09:30 $118.77 → close $114.84 -3.93 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.12 | ▲ 09:30 equity $9,986.48 vs yday $9,943.82 (+42.66) | 09:30 open · cash $34.12 (unchanged overnight, no fees) · equity $9,986.48 vs prior close $9,943.82 (+42.66) · 3 name(s) re-marked at the open (per-name table). AU×41 yday $118.40 → 09:30 $119.19 +32.39; FCX×63 yday $78.42 → 09:30 $78.57 +9.45; CM×1 yday $114.84 → 09:30 $115.66 +0.82 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 41 | $119.19 | $2.16 | $+23.19 | $4,918.75 | ▲ +23.19 after sell → book $9,984.32; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 63 | $78.57 | $2.23 | $+86.31 | $9,866.43 | ▲ +86.31 after sell → book $9,982.09; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 6 | $324.41 | $2.01 | — | $7,917.96 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1973.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 13 | $141.76 | $2.03 | — | $6,073.05 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1973.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 4 | $400.42 | $2.00 | — | $4,469.37 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1973.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $3,161.35 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1973.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 8 | $240.22 | $2.01 | — | $1,237.57 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1973.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,237.57 | ▼ close $9,642.79 vs 09:30 $9,986.48 (session -329.25) | 16:00 close · cash $1,237.57 · equity $9,642.79 vs 09:30 $9,986.48 (-343.69; session marks -329.25) · 6 name(s) marked open→close (per-name table). CM×1 09:30 $115.66 → close $114.33 -1.33; KEYS×6 09:30 $324.41 → close $319.97 -26.64; SMTC×13 09:30 $141.76 → close $131.17 -137.67; CIEN×4 09:30 $400.42 → close $378.44 -87.92; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×8 09:30 $240.22 → close $236.98 -25.92 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,237.57 | ▲ 09:30 equity $9,654.25 vs yday $9,642.79 (+11.46) | 09:30 open · cash $1,237.57 (unchanged overnight, no fees) · equity $9,654.25 vs prior close $9,642.79 (+11.46) · 6 name(s) re-marked at the open (per-name table). CM×1 yday $114.33 → 09:30 $114.46 +0.13; KEYS×6 yday $319.97 → 09:30 $322.49 +15.12; SMTC×13 yday $131.17 → 09:30 $132.30 +14.69; CIEN×4 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×8 yday $236.98 → 09:30 $233.97 -24.12 | — |
| 2026-08-31 09:30 ET | **SELL** | `CM` | 1 | $114.46 | $1.17 | $-6.40 | $1,350.87 | ▼ -6.40 after sell → book $9,653.09; vs 09:30 mark -1.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,350.87 | ▲ close $9,710.84 vs 09:30 $9,654.25 (session +57.75) | 16:00 close · cash $1,350.87 · equity $9,710.84 vs 09:30 $9,654.25 (+56.59; session marks +57.75) · 5 name(s) marked open→close (per-name table). KEYS×6 09:30 $322.49 → close $322.70 +1.26; SMTC×13 09:30 $132.30 → close $132.96 +8.58; CIEN×4 09:30 $378.44 → close $382.80 +17.44; MPWR×1 09:30 $1261.90 → close $1267.77 +5.87; DDOG×8 09:30 $233.97 → close $237.04 +24.60 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,350.87 | ▼ 09:30 equity $9,554.59 vs yday $9,710.84 (-156.25) | 09:30 open · cash $1,350.87 (unchanged overnight, no fees) · equity $9,554.59 vs prior close $9,710.84 (-156.25) · 5 name(s) re-marked at the open (per-name table). KEYS×6 yday $322.70 → 09:30 $321.47 -7.38; SMTC×13 yday $132.96 → 09:30 $127.63 -69.29; CIEN×4 yday $382.80 → 09:30 $376.89 -23.64; MPWR×1 yday $1267.77 → 09:30 $1245.11 -22.66; DDOG×8 yday $237.04 → 09:30 $232.88 -33.28 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,350.87 | ▼ close $9,444.00 vs 09:30 $9,554.59 (session -110.59) | 16:00 close · cash $1,350.87 · equity $9,444.00 vs 09:30 $9,554.59 (-110.59; session marks -110.59) · 5 name(s) marked open→close (per-name table). KEYS×6 09:30 $321.47 → close $319.27 -13.20; SMTC×13 09:30 $127.63 → close $132.27 +60.32; CIEN×4 09:30 $376.89 → close $360.33 -66.24; MPWR×1 09:30 $1245.11 → close $1225.96 -19.15; DDOG×8 09:30 $232.88 → close $223.84 -72.32 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,350.87 | ▼ 09:30 equity $9,397.71 vs yday $9,444.00 (-46.29) | 09:30 open · cash $1,350.87 (unchanged overnight, no fees) · equity $9,397.71 vs prior close $9,444.00 (-46.29) · 5 name(s) re-marked at the open (per-name table). KEYS×6 yday $319.27 → 09:30 $318.04 -7.38; SMTC×13 yday $132.27 → 09:30 $133.00 +9.49; CIEN×4 yday $360.33 → 09:30 $357.25 -12.32; MPWR×1 yday $1225.96 → 09:30 $1224.92 -1.04; DDOG×8 yday $223.84 → 09:30 $219.46 -35.04 | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 6 | $318.04 | $2.03 | $-42.26 | $3,257.07 | ▼ -42.26 after sell → book $9,395.67; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 13 | $133.00 | $2.05 | $-117.96 | $4,984.02 | ▼ -117.96 after sell → book $9,393.62; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 4 | $357.25 | $2.02 | $-176.71 | $6,411.00 | ▼ -176.71 after sell → book $9,391.60; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 1 | $1224.92 | $2.01 | $-85.12 | $7,633.90 | ▼ -85.12 after sell → book $9,389.58; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 8 | $219.46 | $2.04 | $-170.13 | $9,387.54 | ▼ -170.13 after sell → book $9,387.54; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,387.54 | ▲ close $9,387.54 vs 09:30 $9,397.71 (session +0.00) | 16:00 close · cash $9,387.54 · no lots left · equity $9,387.54. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,387.54 | ▲ 09:30 equity $9,387.54 vs yday $9,387.54 (+0.00) | 09:30 open · cash $9,387.54 · no holdings · equity $9,387.54 vs prior close $9,387.54 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 13 | $351.74 | $2.03 | — | $4,812.90 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $4693.77 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 9 | $486.31 | $2.02 | — | $434.09 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $4693.77 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $434.09 | ▲ close $9,724.68 vs 09:30 $9,387.54 (session +341.18) | 16:00 close · cash $434.09 · equity $9,724.68 vs 09:30 $9,387.54 (+337.14; session marks +341.18) · 2 name(s) marked open→close (per-name table). AVGO×13 09:30 $351.74 → close $357.16 +70.46; DELL×9 09:30 $486.31 → close $516.39 +270.72 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $434.09 | ▲ 09:30 equity $9,734.21 vs yday $9,724.68 (+9.53) | 09:30 open · cash $434.09 (unchanged overnight, no fees) · equity $9,734.21 vs prior close $9,724.68 (+9.53) · 2 name(s) re-marked at the open (per-name table). AVGO×13 yday $357.16 → 09:30 $359.70 +33.02; DELL×9 yday $516.39 → 09:30 $513.78 -23.49 | — |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 8 | $16.40 | $1.34 | — | $301.55 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $144.70 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 1 | $75.65 | $0.76 | — | $225.14 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $144.70 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $225.14 | ▲ close $9,803.85 vs 09:30 $9,734.21 (session +71.74) | 16:00 close · cash $225.14 · equity $9,803.85 vs 09:30 $9,734.21 (+69.64; session marks +71.74) · 4 name(s) marked open→close (per-name table). AVGO×13 09:30 $359.70 → close $357.90 -23.40; DELL×9 09:30 $513.78 → close $524.14 +93.24; FRNM×8 09:30 $16.40 → close $16.31 -0.72; MRX×1 09:30 $75.65 → close $78.27 +2.62 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $225.14 | ▲ 09:30 equity $9,856.09 vs yday $9,803.85 (+52.24) | 09:30 open · cash $225.14 (unchanged overnight, no fees) · equity $9,856.09 vs prior close $9,803.85 (+52.24) · 4 name(s) re-marked at the open (per-name table). AVGO×13 yday $357.90 → 09:30 $363.68 +75.14; DELL×9 yday $524.14 → 09:30 $521.15 -26.91; FRNM×8 yday $16.31 → 09:30 $16.74 +3.44; MRX×1 yday $78.27 → 09:30 $78.84 +0.57 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $225.14 | ▲ close $10,025.97 vs 09:30 $9,856.09 (session +169.88) | 16:00 close · cash $225.14 · equity $10,025.97 vs 09:30 $9,856.09 (+169.88; session marks +169.88) · 4 name(s) marked open→close (per-name table). AVGO×13 09:30 $363.68 → close $368.56 +63.44; DELL×9 09:30 $521.15 → close $533.88 +114.57; FRNM×8 09:30 $16.74 → close $15.99 -6.00; MRX×1 09:30 $78.84 → close $76.71 -2.13 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $225.14 | ▲ 09:30 equity $10,036.64 vs yday $10,025.97 (+10.67) | 09:30 open · cash $225.14 (unchanged overnight, no fees) · equity $10,036.64 vs prior close $10,025.97 (+10.67) · 4 name(s) re-marked at the open (per-name table). AVGO×13 yday $368.56 → 09:30 $366.23 -30.29; DELL×9 yday $533.88 → 09:30 $538.47 +41.31; FRNM×8 yday $15.99 → 09:30 $15.96 -0.24; MRX×1 yday $76.71 → 09:30 $76.60 -0.11 | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 13 | $366.23 | $2.08 | $+184.26 | $4,984.06 | ▲ +184.26 after sell → book $10,034.57; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 9 | $538.47 | $2.07 | $+465.36 | $9,828.22 | ▲ +465.36 after sell → book $10,032.50; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,828.22 | ▼ close $10,029.94 vs 09:30 $10,036.64 (session -2.56) | 16:00 close · cash $9,828.22 · equity $10,029.94 vs 09:30 $10,036.64 (-6.70; session marks -2.56) · 2 name(s) marked open→close (per-name table). FRNM×8 09:30 $15.96 → close $15.75 -1.68; MRX×1 09:30 $76.60 → close $75.72 -0.88 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,828.22 | ▼ 09:30 equity $10,028.34 vs yday $10,029.94 (-1.60) | 09:30 open · cash $9,828.22 (unchanged overnight, no fees) · equity $10,028.34 vs prior close $10,029.94 (-1.60) · 2 name(s) re-marked at the open (per-name table). FRNM×8 yday $15.75 → 09:30 $15.64 -0.88; MRX×1 yday $75.72 → 09:30 $75.00 -0.72 | — |
| 2026-09-10 09:30 ET | **SELL** | `FRNM` | 8 | $15.64 | $1.30 | $-8.71 | $9,952.05 | ▼ -8.71 after sell → book $10,027.05; vs 09:30 mark -1.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MRX` | 1 | $75.00 | $0.77 | $-2.18 | $10,026.27 | ▼ -2.18 after sell → book $10,026.27; vs 09:30 mark -0.78 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,026.27 | ▲ close $10,026.27 vs 09:30 $10,028.34 (session +0.00) | 16:00 close · cash $10,026.27 · no lots left · equity $10,026.27. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,026.27 | ▲ 09:30 equity $10,026.27 vs yday $10,026.27 (+0.00) | 09:30 open · cash $10,026.27 · no holdings · equity $10,026.27 vs prior close $10,026.27 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 60 | $164.43 | $2.17 | — | $158.30 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $10026.27 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.30 | ▼ close $9,175.10 vs 09:30 $10,026.27 (session -849.00) | 16:00 close · cash $158.30 · equity $9,175.10 vs 09:30 $10,026.27 (-851.17; session marks -849.00) · 1 name(s) marked open→close (per-name table). ORCL×60 09:30 $164.43 → close $150.28 -849.00 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.30 | ▼ 09:30 equity $8,643.50 vs yday $9,175.10 (-531.60) | 09:30 open · cash $158.30 (unchanged overnight, no fees) · equity $8,643.50 vs prior close $9,175.10 (-531.60) · 1 name(s) re-marked at the open (per-name table). ORCL×60 yday $150.28 → 09:30 $141.42 -531.60 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.30 | ▲ close $8,845.70 vs 09:30 $8,643.50 (session +202.20) | 16:00 close · cash $158.30 · equity $8,845.70 vs 09:30 $8,643.50 (+202.20; session marks +202.20) · 1 name(s) marked open→close (per-name table). ORCL×60 09:30 $141.42 → close $144.79 +202.20 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.30 | ▼ 09:30 equity $8,765.90 vs yday $8,845.70 (-79.80) | 09:30 open · cash $158.30 (unchanged overnight, no fees) · equity $8,765.90 vs prior close $8,845.70 (-79.80) · 1 name(s) re-marked at the open (per-name table). ORCL×60 yday $144.79 → 09:30 $143.46 -79.80 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.30 | ▼ close $8,579.30 vs 09:30 $8,765.90 (session -186.60) | 16:00 close · cash $158.30 · equity $8,579.30 vs 09:30 $8,765.90 (-186.60; session marks -186.60) · 1 name(s) marked open→close (per-name table). ORCL×60 09:30 $143.46 → close $140.35 -186.60 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.30 | ▼ 09:30 equity $8,560.10 vs yday $8,579.30 (-19.20) | 09:30 open · cash $158.30 (unchanged overnight, no fees) · equity $8,560.10 vs prior close $8,579.30 (-19.20) · 1 name(s) re-marked at the open (per-name table). ORCL×60 yday $140.35 → 09:30 $140.03 -19.20 | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 60 | $140.03 | $2.25 | $-1468.42 | $8,557.86 | ▼ -1,468.42 after sell → book $8,557.86; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 108 | $26.27 | $2.31 | — | $5,718.38 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2852.62 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 15 | $189.17 | $2.04 | — | $2,878.80 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2852.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 71 | $39.99 | $2.20 | — | $37.30 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2852.62 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.30 | ▼ close $8,390.98 vs 09:30 $8,560.10 (session -160.32) | 16:00 close · cash $37.30 · equity $8,390.98 vs 09:30 $8,560.10 (-169.12; session marks -160.32) · 3 name(s) marked open→close (per-name table). WAY×108 09:30 $26.27 → close $26.59 +34.56; QCOM×15 09:30 $189.17 → close $184.84 -64.95; SM×71 09:30 $39.99 → close $38.16 -129.93 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.30 | ▲ 09:30 equity $8,423.10 vs yday $8,390.98 (+32.12) | 09:30 open · cash $37.30 (unchanged overnight, no fees) · equity $8,423.10 vs prior close $8,390.98 (+32.12) · 3 name(s) re-marked at the open (per-name table). WAY×108 yday $26.59 → 09:30 $26.51 -8.64; QCOM×15 yday $184.84 → 09:30 $190.35 +82.65; SM×71 yday $38.16 → 09:30 $37.57 -41.89 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.30 | ▼ close $8,355.90 vs 09:30 $8,423.10 (session -67.20) | 16:00 close · cash $37.30 · equity $8,355.90 vs 09:30 $8,423.10 (-67.20; session marks -67.20) · 3 name(s) marked open→close (per-name table). WAY×108 09:30 $26.51 → close $26.51 +0.00; QCOM×15 09:30 $190.35 → close $188.71 -24.60; SM×71 09:30 $37.57 → close $36.97 -42.60 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.30 | ▲ 09:30 equity $8,435.77 vs yday $8,355.90 (+79.87) | 09:30 open · cash $37.30 (unchanged overnight, no fees) · equity $8,435.77 vs prior close $8,355.90 (+79.87) · 3 name(s) re-marked at the open (per-name table). WAY×108 yday $26.51 → 09:30 $26.95 +47.52; QCOM×15 yday $188.71 → 09:30 $191.34 +39.45; SM×71 yday $36.97 → 09:30 $36.87 -7.10 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.30 | ▼ close $8,099.25 vs 09:30 $8,435.77 (session -336.52) | 16:00 close · cash $37.30 · equity $8,099.25 vs 09:30 $8,435.77 (-336.52; session marks -336.52) · 3 name(s) marked open→close (per-name table). WAY×108 09:30 $26.95 → close $25.66 -139.32; QCOM×15 09:30 $191.34 → close $177.72 -204.30; SM×71 09:30 $36.87 → close $36.97 +7.10 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.30 | ▼ 09:30 equity $8,097.58 vs yday $8,099.25 (-1.67) | 09:30 open · cash $37.30 (unchanged overnight, no fees) · equity $8,097.58 vs prior close $8,099.25 (-1.67) · 3 name(s) re-marked at the open (per-name table). WAY×108 yday $25.66 → 09:30 $25.94 +30.24; QCOM×15 yday $177.72 → 09:30 $180.61 +43.35; SM×71 yday $36.97 → 09:30 $35.91 -75.26 | — |
| 2026-09-21 09:30 ET | **SELL** | `WAY` | 108 | $25.94 | $2.35 | $-40.31 | $2,836.47 | ▼ -40.31 after sell → book $8,095.23; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QCOM` | 15 | $180.61 | $2.07 | $-132.50 | $5,543.55 | ▼ -132.50 after sell → book $8,093.16; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SM` | 71 | $35.91 | $2.24 | $-294.12 | $8,090.93 | ▼ -294.12 after sell → book $8,090.93; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,090.93 | ▲ close $8,090.93 vs 09:30 $8,097.58 (session +0.00) | 16:00 close · cash $8,090.93 · no lots left · equity $8,090.93. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 22.52 < 1 share @ 119.43 |
| 2026-08-21 | `CRSP` | cash | leftover split 22.52 < 1 share @ 59.72 |
| 2026-08-21 | `FUTU` | cash | leftover split 22.52 < 1 share @ 115.18 |
| 2026-08-21 | `GRAL` | cash | leftover split 22.52 < 1 share @ 78.88 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 5.69 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 5.69 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 5.69 < 1 share @ 1746.53 |
| 2026-08-27 | `GEN` | cash | leftover split 5.69 < 1 share @ 29.83 |
| 2026-08-27 | `LRCX` | cash | leftover split 5.69 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 5.69 < 1 share @ 222.86 |
| 2026-08-28 | `CM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DDOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DDOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 144.70 < 1 share @ 263.36 |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `MRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-09 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 18.65 < 1 share @ 170.85 |
| 2026-09-17 | `CLS` | cash | leftover split 18.65 < 1 share @ 337.75 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TH` | cash | leftover split 9.33 < 1 share @ 20.91 |
| 2026-09-18 | `GME` | cash | leftover split 9.33 < 1 share @ 22.90 |
| 2026-09-18 | `RARE` | cash | leftover split 9.33 < 1 share @ 14.79 |
| 2026-09-18 | `CLS` | cash | leftover split 9.33 < 1 share @ 332.06 |
