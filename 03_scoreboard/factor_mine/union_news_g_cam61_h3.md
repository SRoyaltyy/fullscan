# Factor mine action — `union_news_g_cam61_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · merged news🟢 and cameras +6 −≤1

Cash book **-18.29%** ($8,171) · signal-only (no cash/fees) was -25.52%. Starts YES **1/29**. Fills 62 · skips 79 · realized $-2231.98.

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
- Must-have: at least 6 green cameras (the +G half of +G −R).
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
- **Gate** `news=good,n_pos_min=6,cam_bad_max=1` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10.52.

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
| 2026-08-20 | `BHP` | 27 | — | $91.01 | +0.00 | $93.63 | +70.74 | +70.74 | +0.00 | +70.74 |
| 2026-08-20 | `APA` | 55 | — | $44.76 | +0.00 | $44.39 | -20.35 | -20.35 | +0.00 | -20.35 |
| 2026-08-20 | `AUTL` | 1012 | — | $2.47 | +0.00 | $2.46 | -10.12 | -10.12 | +0.00 | -10.12 |
| 2026-08-20 | `CRSP` | 42 | — | $58.73 | +0.00 | $58.12 | -25.62 | -25.62 | +0.00 | -25.62 |
| 2026-08-21 | `BHP` | 27 | $93.63 | $95.72 | +56.43 | $97.03 | +35.37 | +91.80 | +127.17 | +162.54 |
| 2026-08-21 | `APA` | 55 | $44.39 | $44.52 | +7.15 | $43.39 | -62.15 | -55.00 | -13.20 | -75.35 |
| 2026-08-21 | `AUTL` | 1012 | $2.46 | $2.47 | +10.12 | $2.41 | -60.72 | -50.60 | +0.00 | -60.72 |
| 2026-08-21 | `CRSP` | 42 | $58.12 | $59.72 | +67.20 | $59.50 | -9.24 | +57.96 | +41.58 | +32.34 |
| 2026-08-21 | `ABTC` | 1 | — | $8.66 | +0.00 | $7.93 | -0.73 | -0.73 | +0.00 | -0.73 |
| 2026-08-21 | `HIVE` | 4 | — | $3.24 | +0.00 | $3.03 | -0.84 | -0.84 | +0.00 | -0.84 |
| 2026-08-21 | `MARA` | 1 | — | $11.70 | +0.00 | $11.26 | -0.44 | -0.44 | +0.00 | -0.44 |
| 2026-08-24 | `BHP` | 27 | $97.03 | $97.31 | +7.56 | $97.13 | -4.86 | +2.70 | +170.10 | +165.24 |
| 2026-08-24 | `APA` | 55 | $43.39 | $42.93 | -25.30 | $42.96 | +1.65 | -23.65 | -100.65 | -99.00 |
| 2026-08-24 | `AUTL` | 1012 | $2.41 | $2.40 | -10.12 | $2.34 | -60.72 | -70.84 | -70.84 | -131.56 |
| 2026-08-24 | `CRSP` | 42 | $59.50 | $58.75 | -31.50 | $57.08 | -70.35 | -101.85 | +0.84 | -69.51 |
| 2026-08-24 | `ABTC` | 1 | $7.93 | $8.00 | +0.07 | $8.64 | +0.64 | +0.71 | -0.66 | -0.02 |
| 2026-08-24 | `HIVE` | 4 | $3.03 | $2.99 | -0.16 | $2.86 | -0.52 | -0.68 | -1.00 | -1.52 |
| 2026-08-24 | `MARA` | 1 | $11.26 | $11.17 | -0.09 | $11.18 | +0.01 | -0.08 | -0.53 | -0.52 |
| 2026-08-25 | `BHP` | 27 | $97.13 | $95.86 | -34.29 | — | +0.00 | -34.29 | +130.95 | — |
| 2026-08-25 | `APA` | 55 | $42.96 | $41.38 | -86.90 | — | +0.00 | -86.90 | -185.90 | — |
| 2026-08-25 | `AUTL` | 1012 | $2.34 | $2.38 | +40.48 | — | +0.00 | +40.48 | -91.08 | — |
| 2026-08-25 | `CRSP` | 42 | $57.08 | $57.93 | +35.91 | — | +0.00 | +35.91 | -33.60 | — |
| 2026-08-25 | `ABTC` | 1 | $8.64 | $8.62 | -0.02 | $9.24 | +0.62 | +0.60 | -0.04 | +0.58 |
| 2026-08-25 | `HIVE` | 4 | $2.86 | $2.87 | +0.04 | $3.02 | +0.60 | +0.64 | -1.48 | -0.88 |
| 2026-08-25 | `MARA` | 1 | $11.18 | $11.07 | -0.11 | $11.83 | +0.76 | +0.65 | -0.63 | +0.13 |
| 2026-08-25 | `AU` | 41 | — | $118.52 | +0.00 | $123.39 | +199.67 | +199.67 | +0.00 | +199.67 |
| 2026-08-25 | `FCX` | 63 | — | $77.13 | +0.00 | $79.91 | +175.14 | +175.14 | +0.00 | +175.14 |
| 2026-08-26 | `ABTC` | 1 | $9.24 | $8.84 | -0.40 | — | +0.00 | -0.40 | +0.18 | — |
| 2026-08-26 | `HIVE` | 4 | $3.02 | $2.95 | -0.28 | — | +0.00 | -0.28 | -1.16 | — |
| 2026-08-26 | `MARA` | 1 | $11.83 | $11.56 | -0.27 | — | +0.00 | -0.27 | -0.14 | — |
| 2026-08-26 | `AU` | 41 | $123.39 | $119.80 | -147.19 | $118.11 | -69.29 | -216.48 | +52.48 | -16.81 |
| 2026-08-26 | `FCX` | 63 | $79.91 | $79.34 | -35.91 | $79.00 | -21.42 | -57.33 | +139.23 | +117.81 |
| 2026-08-27 | `AU` | 41 | $118.11 | $117.41 | -28.70 | $118.40 | +40.59 | +11.89 | -45.51 | -4.92 |
| 2026-08-27 | `FCX` | 63 | $79.00 | $78.83 | -10.71 | $78.42 | -25.83 | -36.54 | +107.10 | +81.27 |
| 2026-08-28 | `AU` | 41 | $118.40 | $119.19 | +32.39 | — | +0.00 | +32.39 | +27.47 | — |
| 2026-08-28 | `FCX` | 63 | $78.42 | $78.57 | +9.45 | — | +0.00 | +9.45 | +90.72 | — |
| 2026-08-28 | `KEYS` | 5 | — | $324.41 | +0.00 | $319.97 | -22.20 | -22.20 | +0.00 | -22.20 |
| 2026-08-28 | `SMTC` | 11 | — | $141.76 | +0.00 | $131.17 | -116.49 | -116.49 | +0.00 | -116.49 |
| 2026-08-28 | `CIEN` | 4 | — | $400.42 | +0.00 | $378.44 | -87.92 | -87.92 | +0.00 | -87.92 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 6 | — | $240.22 | +0.00 | $236.98 | -19.44 | -19.44 | +0.00 | -19.44 |
| 2026-08-28 | `SEDG` | 50 | — | $32.90 | +0.00 | $31.41 | -74.50 | -74.50 | +0.00 | -74.50 |
| 2026-08-31 | `KEYS` | 5 | $319.97 | $322.49 | +12.60 | $322.70 | +1.05 | +13.65 | -9.60 | -8.55 |
| 2026-08-31 | `SMTC` | 11 | $131.17 | $132.30 | +12.43 | $132.96 | +7.26 | +19.69 | -104.06 | -96.80 |
| 2026-08-31 | `CIEN` | 4 | $378.44 | $378.44 | +0.00 | $382.80 | +17.44 | +17.44 | -87.92 | -70.48 |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | $1267.77 | +5.87 | +11.51 | -44.13 | -38.26 |
| 2026-08-31 | `DDOG` | 6 | $236.98 | $233.97 | -18.09 | $237.04 | +18.45 | +0.36 | -37.53 | -19.08 |
| 2026-08-31 | `SEDG` | 50 | $31.41 | $31.15 | -13.00 | $32.20 | +52.50 | +39.50 | -87.50 | -35.00 |
| 2026-09-01 | `KEYS` | 5 | $322.70 | $321.47 | -6.15 | $319.27 | -11.00 | -17.15 | -14.70 | -25.70 |
| 2026-09-01 | `SMTC` | 11 | $132.96 | $127.63 | -58.63 | $132.27 | +51.04 | -7.59 | -155.43 | -104.39 |
| 2026-09-01 | `CIEN` | 4 | $382.80 | $376.89 | -23.64 | $360.33 | -66.24 | -89.88 | -94.12 | -160.36 |
| 2026-09-01 | `MPWR` | 1 | $1267.77 | $1245.11 | -22.66 | $1225.96 | -19.15 | -41.81 | -60.92 | -80.07 |
| 2026-09-01 | `DDOG` | 6 | $237.04 | $232.88 | -24.96 | $223.84 | -54.24 | -79.20 | -44.04 | -98.28 |
| 2026-09-01 | `SEDG` | 50 | $32.20 | $31.87 | -16.50 | $32.49 | +31.00 | +14.50 | -51.50 | -20.50 |
| 2026-09-02 | `KEYS` | 5 | $319.27 | $318.04 | -6.15 | — | +0.00 | -6.15 | -31.85 | — |
| 2026-09-02 | `SMTC` | 11 | $132.27 | $133.00 | +8.03 | — | +0.00 | +8.03 | -96.36 | — |
| 2026-09-02 | `CIEN` | 4 | $360.33 | $357.25 | -12.32 | — | +0.00 | -12.32 | -172.68 | — |
| 2026-09-02 | `MPWR` | 1 | $1225.96 | $1224.92 | -1.04 | — | +0.00 | -1.04 | -81.11 | — |
| 2026-09-02 | `DDOG` | 6 | $223.84 | $219.46 | -26.28 | — | +0.00 | -26.28 | -124.56 | — |
| 2026-09-02 | `SEDG` | 50 | $32.49 | $32.42 | -3.50 | — | +0.00 | -3.50 | -24.00 | — |
| 2026-09-03 | `AVGO` | 5 | — | $351.74 | +0.00 | $357.16 | +27.10 | +27.10 | +0.00 | +27.10 |
| 2026-09-03 | `DELL` | 3 | — | $486.31 | +0.00 | $516.39 | +90.24 | +90.24 | +0.00 | +90.24 |
| 2026-09-03 | `CXW` | 57 | — | $32.31 | +0.00 | $33.66 | +76.95 | +76.95 | +0.00 | +76.95 |
| 2026-09-03 | `FRNM` | 117 | — | $15.87 | +0.00 | $16.90 | +120.51 | +120.51 | +0.00 | +120.51 |
| 2026-09-03 | `MMED` | 78 | — | $23.88 | +0.00 | $23.84 | -3.12 | -3.12 | +0.00 | -3.12 |
| 2026-09-04 | `AVGO` | 5 | $357.16 | $359.70 | +12.70 | $357.90 | -9.00 | +3.70 | +39.80 | +30.80 |
| 2026-09-04 | `DELL` | 3 | $516.39 | $513.78 | -7.83 | $524.14 | +31.08 | +23.25 | +82.41 | +113.49 |
| 2026-09-04 | `CXW` | 57 | $33.66 | $33.46 | -11.40 | $34.71 | +71.25 | +59.85 | +65.55 | +136.80 |
| 2026-09-04 | `FRNM` | 117 | $16.90 | $16.40 | -58.50 | $16.31 | -10.53 | -69.03 | +62.01 | +51.48 |
| 2026-09-04 | `MMED` | 78 | $23.84 | $23.84 | +0.00 | $23.29 | -42.90 | -42.90 | -3.12 | -46.02 |
| 2026-09-04 | `CRM` | 1 | — | $263.36 | +0.00 | $259.23 | -4.13 | -4.13 | +0.00 | -4.13 |
| 2026-09-04 | `MRX` | 3 | — | $75.65 | +0.00 | $78.27 | +7.86 | +7.86 | +0.00 | +7.86 |
| 2026-09-08 | `AVGO` | 5 | $357.90 | $363.68 | +28.90 | $368.56 | +24.40 | +53.30 | +59.70 | +84.10 |
| 2026-09-08 | `DELL` | 3 | $524.14 | $521.15 | -8.97 | $533.88 | +38.19 | +29.22 | +104.52 | +142.71 |
| 2026-09-08 | `CXW` | 57 | $34.71 | $34.49 | -12.54 | $35.05 | +31.92 | +19.38 | +124.26 | +156.18 |
| 2026-09-08 | `FRNM` | 117 | $16.31 | $16.74 | +50.31 | $15.99 | -87.75 | -37.44 | +101.79 | +14.04 |
| 2026-09-08 | `MMED` | 78 | $23.29 | $23.16 | -10.14 | $23.32 | +12.48 | +2.34 | -56.16 | -43.68 |
| 2026-09-08 | `CRM` | 1 | $259.23 | $253.72 | -5.51 | $249.12 | -4.60 | -10.11 | -9.64 | -14.24 |
| 2026-09-08 | `MRX` | 3 | $78.27 | $78.84 | +1.71 | $76.71 | -6.39 | -4.68 | +9.57 | +3.18 |
| 2026-09-09 | `AVGO` | 5 | $368.56 | $366.23 | -11.65 | — | +0.00 | -11.65 | +72.45 | — |
| 2026-09-09 | `DELL` | 3 | $533.88 | $538.47 | +13.77 | — | +0.00 | +13.77 | +156.48 | — |
| 2026-09-09 | `CXW` | 57 | $35.05 | $35.09 | +2.28 | — | +0.00 | +2.28 | +158.46 | — |
| 2026-09-09 | `FRNM` | 117 | $15.99 | $15.96 | -3.51 | — | +0.00 | -3.51 | +10.53 | — |
| 2026-09-09 | `MMED` | 78 | $23.32 | $23.22 | -7.80 | — | +0.00 | -7.80 | -51.48 | — |
| 2026-09-09 | `CRM` | 1 | $249.12 | $249.78 | +0.66 | $244.16 | -5.62 | -4.96 | -13.58 | -19.20 |
| 2026-09-09 | `MRX` | 3 | $76.71 | $76.60 | -0.33 | $75.72 | -2.64 | -2.97 | +2.85 | +0.21 |
| 2026-09-10 | `CRM` | 1 | $244.16 | $245.35 | +1.19 | — | +0.00 | +1.19 | -18.01 | — |
| 2026-09-10 | `MRX` | 3 | $75.72 | $75.00 | -2.16 | — | +0.00 | -2.16 | -1.95 | — |
| 2026-09-11 | `ORCL` | 58 | — | $164.43 | +0.00 | $150.28 | -820.70 | -820.70 | +0.00 | -820.70 |
| 2026-09-14 | `ORCL` | 58 | $150.28 | $141.42 | -513.88 | $144.79 | +195.46 | -318.42 | -1334.58 | -1139.12 |
| 2026-09-15 | `ORCL` | 58 | $144.79 | $143.46 | -77.14 | $140.35 | -180.38 | -257.52 | -1216.26 | -1396.64 |
| 2026-09-16 | `ORCL` | 58 | $140.35 | $140.03 | -18.56 | — | +0.00 | -18.56 | -1415.20 | — |
| 2026-09-16 | `WAY` | 104 | — | $26.27 | +0.00 | $26.59 | +33.28 | +33.28 | +0.00 | +33.28 |
| 2026-09-16 | `QCOM` | 14 | — | $189.17 | +0.00 | $184.84 | -60.62 | -60.62 | +0.00 | -60.62 |
| 2026-09-16 | `SM` | 68 | — | $39.99 | +0.00 | $38.16 | -124.44 | -124.44 | +0.00 | -124.44 |
| 2026-09-17 | `WAY` | 104 | $26.59 | $26.51 | -8.32 | $26.51 | +0.00 | -8.32 | +24.96 | +24.96 |
| 2026-09-17 | `QCOM` | 14 | $184.84 | $190.35 | +77.14 | $188.71 | -22.96 | +54.18 | +16.52 | -6.44 |
| 2026-09-17 | `SM` | 68 | $38.16 | $37.57 | -40.12 | $36.97 | -40.80 | -80.92 | -164.56 | -205.36 |
| 2026-09-17 | `GME` | 1 | — | $22.12 | +0.00 | $22.77 | +0.65 | +0.65 | +0.00 | +0.65 |
| 2026-09-18 | `WAY` | 104 | $26.51 | $26.95 | +45.76 | $25.66 | -134.16 | -88.40 | +70.72 | -63.44 |
| 2026-09-18 | `QCOM` | 14 | $188.71 | $191.34 | +36.82 | $177.72 | -190.68 | -153.86 | +30.38 | -160.30 |
| 2026-09-18 | `SM` | 68 | $36.97 | $36.87 | -6.80 | $36.97 | +6.80 | +0.00 | -212.16 | -205.36 |
| 2026-09-18 | `GME` | 1 | $22.77 | $22.90 | +0.13 | $22.64 | -0.26 | -0.13 | +0.78 | +0.52 |
| 2026-09-18 | `RARE` | 1 | — | $14.79 | +0.00 | $14.51 | -0.28 | -0.28 | +0.00 | -0.28 |
| 2026-09-18 | `BHVN` | 1 | — | $14.07 | +0.00 | $13.62 | -0.45 | -0.45 | +0.00 | -0.45 |
| 2026-09-21 | `WAY` | 104 | $25.66 | $25.94 | +29.12 | — | +0.00 | +29.12 | -34.32 | — |
| 2026-09-21 | `QCOM` | 14 | $177.72 | $180.61 | +40.46 | — | +0.00 | +40.46 | -119.84 | — |
| 2026-09-21 | `SM` | 68 | $36.97 | $35.91 | -72.08 | — | +0.00 | -72.08 | -277.44 | — |
| 2026-09-21 | `GME` | 1 | $22.64 | $22.78 | +0.14 | $22.76 | -0.02 | +0.12 | +0.66 | +0.64 |
| 2026-09-21 | `RARE` | 1 | $14.51 | $14.58 | +0.07 | $14.65 | +0.07 | +0.14 | -0.21 | -0.14 |
| 2026-09-21 | `BHVN` | 1 | $13.62 | $13.90 | +0.28 | $14.22 | +0.32 | +0.60 | -0.17 | +0.15 |
| 2026-09-21 | `VICR` | 16 | — | $230.25 | +0.00 | $223.90 | -101.60 | -101.60 | +0.00 | -101.60 |
| 2026-09-21 | `SMTC` | 20 | — | $190.30 | +0.00 | $177.37 | -258.60 | -258.60 | +0.00 | -258.60 |
| 2026-09-22 | `GME` | 1 | $22.76 | $23.50 | +0.74 | — | +0.00 | +0.74 | +1.38 | — |
| 2026-09-22 | `RARE` | 1 | $14.65 | $14.78 | +0.13 | $15.62 | +0.84 | +0.97 | -0.01 | +0.83 |
| 2026-09-22 | `BHVN` | 1 | $14.22 | $14.22 | +0.00 | $14.22 | +0.00 | +0.00 | +0.15 | +0.15 |
| 2026-09-22 | `VICR` | 16 | $223.90 | $252.37 | +455.52 | $249.00 | -53.92 | +401.60 | +353.92 | +300.00 |
| 2026-09-22 | `SMTC` | 20 | $177.37 | $177.30 | -1.40 | $175.00 | -46.00 | -47.40 | -260.00 | -306.00 |
| 2026-09-23 | `RARE` | 1 | $15.62 | $15.40 | -0.21 | — | +0.00 | -0.21 | +0.61 | — |
| 2026-09-23 | `BHVN` | 1 | $14.22 | $14.84 | +0.62 | — | +0.00 | +0.62 | +0.77 | — |
| 2026-09-23 | `VICR` | 16 | $249.00 | $266.50 | +280.00 | $283.16 | +266.56 | +546.56 | +580.00 | +846.56 |
| 2026-09-23 | `SMTC` | 20 | $175.00 | $174.50 | -10.00 | $169.33 | -103.40 | -113.40 | -316.00 | -419.40 |
| 2026-09-23 | `PGEN` | 17 | — | $7.95 | +0.00 | $7.44 | -8.67 | -8.67 | +0.00 | -8.67 |
| 2026-09-23 | `SGRY` | 8 | — | $15.70 | +0.00 | $14.56 | -9.12 | -9.12 | +0.00 | -9.12 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +14.65 | BHP, APA, AUTL, CRSP | — | $95.23 | $9,995.25 | BHP×27, APA×55, AUTL×1012, CRSP×42 |
| 2026-08-21 | +3.25 | $95.23 | BHP×27, APA×55, AUTL×1012, CRSP×42 | $10,136.15 | +140.90 | -98.75 | ABTC, HIVE, MARA | — | $61.56 | $10,037.05 | BHP×27, APA×55, AUTL×1012, CRSP×42, ABTC×1, HIVE×4, MARA×1 |
| 2026-08-24 | -5.17 | $61.56 | BHP×27, APA×55, AUTL×1012, CRSP×42, ABTC×1, HIVE×4, MARA×1 | $9,977.51 | -59.54 | -134.15 | — | — | $61.56 | $9,843.36 | BHP×27, APA×55, AUTL×1012, CRSP×42, ABTC×1, HIVE×4, MARA×1 |
| 2026-08-25 | +1.80 | $61.56 | BHP×27, APA×55, AUTL×1012, CRSP×42, ABTC×1, HIVE×4, MARA×1 | $9,798.47 | -44.89 | +376.79 | AU, FCX | BHP, APA, AUTL, CRSP | $24.83 | $10,151.30 | ABTC×1, HIVE×4, MARA×1, AU×41, FCX×63 |
| 2026-08-26 | +2.02 | $24.83 | ABTC×1, HIVE×4, MARA×1, AU×41, FCX×63 | $9,967.25 | -184.05 | -90.71 | — | ABTC, HIVE, MARA | $56.63 | $9,876.14 | AU×41, FCX×63 |
| 2026-08-27 | — | $56.63 | AU×41, FCX×63 | $9,836.73 | -39.41 | +14.76 | — | — | $56.63 | $9,851.49 | AU×41, FCX×63 |
| 2026-08-28 | +0.75 | $56.63 | AU×41, FCX×63 | $9,893.33 | +41.84 | -370.32 | KEYS, SMTC, CIEN, MPWR, DDOG, SEDG | AU, FCX | $701.33 | $9,506.45 | KEYS×5, SMTC×11, CIEN×4, MPWR×1, DDOG×6, SEDG×50 |
| 2026-08-31 | -5.85 | $701.33 | KEYS×5, SMTC×11, CIEN×4, MPWR×1, DDOG×6, SEDG×50 | $9,506.03 | -0.42 | +102.57 | — | — | $701.33 | $9,608.60 | KEYS×5, SMTC×11, CIEN×4, MPWR×1, DDOG×6, SEDG×50 |
| 2026-09-01 | -6.30 | $701.33 | KEYS×5, SMTC×11, CIEN×4, MPWR×1, DDOG×6, SEDG×50 | $9,456.06 | -152.54 | -68.59 | — | — | $701.33 | $9,387.47 | KEYS×5, SMTC×11, CIEN×4, MPWR×1, DDOG×6, SEDG×50 |
| 2026-09-02 | -3.83 | $701.33 | KEYS×5, SMTC×11, CIEN×4, MPWR×1, DDOG×6, SEDG×50 | $9,346.21 | -41.26 | +0.00 | — | KEYS, SMTC, CIEN, MPWR, DDOG, SEDG | $9,333.91 | $9,333.91 | — |
| 2026-09-03 | -0.90 | $9,333.91 | — | $9,333.91 | -0.00 | +311.68 | AVGO, DELL, CXW, FRNM, MMED | — | $544.45 | $9,634.86 | AVGO×5, DELL×3, CXW×57, FRNM×117, MMED×78 |
| 2026-09-04 | +2.25 | $544.45 | AVGO×5, DELL×3, CXW×57, FRNM×117, MMED×78 | $9,569.83 | -65.03 | +43.63 | CRM, MRX | — | $50.14 | $9,609.46 | AVGO×5, DELL×3, CXW×57, FRNM×117, MMED×78, CRM×1, MRX×3 |
| 2026-09-08 | -11.47 | $50.14 | AVGO×5, DELL×3, CXW×57, FRNM×117, MMED×78, CRM×1, MRX×3 | $9,653.22 | +43.76 | +8.25 | — | — | $50.14 | $9,661.47 | AVGO×5, DELL×3, CXW×57, FRNM×117, MMED×78, CRM×1, MRX×3 |
| 2026-09-09 | -13.95 | $50.14 | AVGO×5, DELL×3, CXW×57, FRNM×117, MMED×78, CRM×1, MRX×3 | $9,654.89 | -6.58 | -8.26 | — | AVGO, DELL, CXW, FRNM, MMED | $9,164.45 | $9,635.77 | CRM×1, MRX×3 |
| 2026-09-10 | -13.28 | $9,164.45 | CRM×1, MRX×3 | $9,634.80 | -0.97 | +0.00 | — | CRM, MRX | $9,630.77 | $9,630.77 | — |
| 2026-09-11 | +0.50 | $9,630.77 | — | $9,630.77 | -0.00 | -820.70 | ORCL | — | $91.66 | $8,807.90 | ORCL×58 |
| 2026-09-14 | -11.00 | $91.66 | ORCL×58 | $8,294.02 | -513.88 | +195.46 | — | — | $91.66 | $8,489.48 | ORCL×58 |
| 2026-09-15 | -3.84 | $91.66 | ORCL×58 | $8,412.34 | -77.14 | -180.38 | — | — | $91.66 | $8,231.96 | ORCL×58 |
| 2026-09-16 | +5.30 | $91.66 | ORCL×58 | $8,213.40 | -18.56 | -151.78 | WAY, QCOM, SM | ORCL | $104.85 | $8,052.85 | WAY×104, QCOM×14, SM×68 |
| 2026-09-17 | +7.38 | $104.85 | WAY×104, QCOM×14, SM×68 | $8,081.55 | +28.70 | -63.11 | GME | — | $82.51 | $8,018.22 | WAY×104, QCOM×14, SM×68, GME×1 |
| 2026-09-18 | +4.86 | $82.51 | WAY×104, QCOM×14, SM×68, GME×1 | $8,094.13 | +75.91 | -319.03 | RARE, BHVN | — | $53.36 | $7,774.81 | WAY×104, QCOM×14, SM×68, GME×1, RARE×1, BHVN×1 |
| 2026-09-21 | +12.87 | $53.36 | WAY×104, QCOM×14, SM×68, GME×1, RARE×1, BHVN×1 | $7,772.80 | -2.01 | -359.83 | VICR, SMTC | WAY, QCOM, SM | $220.82 | $7,402.25 | GME×1, RARE×1, BHVN×1, VICR×16, SMTC×20 |
| 2026-09-22 | -0.50 | $220.82 | GME×1, RARE×1, BHVN×1, VICR×16, SMTC×20 | $7,857.24 | +454.99 | -99.08 | — | GME | $244.06 | $7,757.90 | RARE×1, BHVN×1, VICR×16, SMTC×20 |
| 2026-09-23 | +2.29 | $244.06 | RARE×1, BHVN×1, VICR×16, SMTC×20 | $8,028.30 | +270.40 | +145.37 | PGEN, SGRY | RARE, BHVN | $10.52 | $8,170.64 | VICR×16, SMTC×20, PGEN×17, SGRY×8 |

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
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 27 | $91.01 | $2.07 | — | $7,540.66 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2500.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 55 | $44.76 | $2.15 | — | $5,076.70 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $2500.00 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 1012 | $2.47 | $13.05 | — | $2,564.01 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $2500.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 42 | $58.73 | $2.12 | — | $95.23 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $2500.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.23 | ▲ close $9,995.25 vs 09:30 $10,000.00 (session +14.65) | 16:00 close · cash $95.23 · equity $9,995.25 vs 09:30 $10,000.00 (-4.75; session marks +14.65) · 4 name(s) marked open→close (per-name table). BHP×27 09:30 $91.01 → close $93.63 +70.74; APA×55 09:30 $44.76 → close $44.39 -20.35; AUTL×1012 09:30 $2.47 → close $2.46 -10.12; CRSP×42 09:30 $58.73 → close $58.12 -25.62 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.23 | ▲ 09:30 equity $10,136.15 vs yday $9,995.25 (+140.90) | 09:30 open · cash $95.23 (unchanged overnight, no fees) · equity $10,136.15 vs prior close $9,995.25 (+140.90) · 4 name(s) re-marked at the open (per-name table). BHP×27 yday $93.63 → 09:30 $95.72 +56.43; APA×55 yday $44.39 → 09:30 $44.52 +7.15; AUTL×1012 yday $2.46 → 09:30 $2.47 +10.12; CRSP×42 yday $58.12 → 09:30 $59.72 +67.20 | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 1 | $8.66 | $0.09 | — | $86.48 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $15.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 4 | $3.24 | $0.14 | — | $73.38 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $15.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 1 | $11.70 | $0.12 | — | $61.56 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $15.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.56 | ▼ close $10,037.05 vs 09:30 $10,136.15 (session -98.75) | 16:00 close · cash $61.56 · equity $10,037.05 vs 09:30 $10,136.15 (-99.10; session marks -98.75) · 7 name(s) marked open→close (per-name table). BHP×27 09:30 $95.72 → close $97.03 +35.37; APA×55 09:30 $44.52 → close $43.39 -62.15; AUTL×1012 09:30 $2.47 → close $2.41 -60.72; CRSP×42 09:30 $59.72 → close $59.50 -9.24; ABTC×1 09:30 $8.66 → close $7.93 -0.73; HIVE×4 09:30 $3.24 → close $3.03 -0.84; MARA×1 09:30 $11.70 → close $11.26 -0.44 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.56 | ▼ 09:30 equity $9,977.51 vs yday $10,037.05 (-59.54) | 09:30 open · cash $61.56 (unchanged overnight, no fees) · equity $9,977.51 vs prior close $10,037.05 (-59.54) · 7 name(s) re-marked at the open (per-name table). BHP×27 yday $97.03 → 09:30 $97.31 +7.56; APA×55 yday $43.39 → 09:30 $42.93 -25.30; AUTL×1012 yday $2.41 → 09:30 $2.40 -10.12; CRSP×42 yday $59.50 → 09:30 $58.75 -31.50; ABTC×1 yday $7.93 → 09:30 $8.00 +0.07; HIVE×4 yday $3.03 → 09:30 $2.99 -0.16; MARA×1 yday $11.26 → 09:30 $11.17 -0.09 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.56 | ▼ close $9,843.36 vs 09:30 $9,977.51 (session -134.15) | 16:00 close · cash $61.56 · equity $9,843.36 vs 09:30 $9,977.51 (-134.15; session marks -134.15) · 7 name(s) marked open→close (per-name table). BHP×27 09:30 $97.31 → close $97.13 -4.86; APA×55 09:30 $42.93 → close $42.96 +1.65; AUTL×1012 09:30 $2.40 → close $2.34 -60.72; CRSP×42 09:30 $58.75 → close $57.08 -70.35; ABTC×1 09:30 $8.00 → close $8.64 +0.64; HIVE×4 09:30 $2.99 → close $2.86 -0.52; MARA×1 09:30 $11.17 → close $11.18 +0.01 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.56 | ▼ 09:30 equity $9,798.47 vs yday $9,843.36 (-44.89) | 09:30 open · cash $61.56 (unchanged overnight, no fees) · equity $9,798.47 vs prior close $9,843.36 (-44.89) · 7 name(s) re-marked at the open (per-name table). BHP×27 yday $97.13 → 09:30 $95.86 -34.29; APA×55 yday $42.96 → 09:30 $41.38 -86.90; AUTL×1012 yday $2.34 → 09:30 $2.38 +40.48; CRSP×42 yday $57.08 → 09:30 $57.93 +35.91; ABTC×1 yday $8.64 → 09:30 $8.62 -0.02; HIVE×4 yday $2.86 → 09:30 $2.87 +0.04; MARA×1 yday $11.18 → 09:30 $11.07 -0.11 | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 27 | $95.86 | $2.10 | $+126.78 | $2,647.68 | ▲ +126.78 after sell → book $9,796.37; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 55 | $41.38 | $2.18 | $-190.24 | $4,921.40 | ▼ -190.24 after sell → book $9,794.19; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 1012 | $2.38 | $13.24 | $-117.38 | $7,316.71 | ▼ -117.38 after sell → book $9,780.94; vs 09:30 mark -13.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 42 | $57.93 | $2.15 | $-37.86 | $9,747.63 | ▼ -37.86 after sell → book $9,778.80; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 41 | $118.52 | $2.11 | — | $4,886.20 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $4873.81 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 63 | $77.13 | $2.18 | — | $24.83 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $4873.81 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.83 | ▲ close $10,151.30 vs 09:30 $9,798.47 (session +376.79) | 16:00 close · cash $24.83 · equity $10,151.30 vs 09:30 $9,798.47 (+352.83; session marks +376.79) · 5 name(s) marked open→close (per-name table). ABTC×1 09:30 $8.62 → close $9.24 +0.62; HIVE×4 09:30 $2.87 → close $3.02 +0.60; MARA×1 09:30 $11.07 → close $11.83 +0.76; AU×41 09:30 $118.52 → close $123.39 +199.67; FCX×63 09:30 $77.13 → close $79.91 +175.14 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.83 | ▼ 09:30 equity $9,967.25 vs yday $10,151.30 (-184.05) | 09:30 open · cash $24.83 (unchanged overnight, no fees) · equity $9,967.25 vs prior close $10,151.30 (-184.05) · 5 name(s) re-marked at the open (per-name table). ABTC×1 yday $9.24 → 09:30 $8.84 -0.40; HIVE×4 yday $3.02 → 09:30 $2.95 -0.28; MARA×1 yday $11.83 → 09:30 $11.56 -0.27; AU×41 yday $123.39 → 09:30 $119.80 -147.19; FCX×63 yday $79.91 → 09:30 $79.34 -35.91 | — |
| 2026-08-26 09:30 ET | **SELL** | `ABTC` | 1 | $8.84 | $0.11 | $-0.02 | $33.56 | ▼ -0.02 after sell → book $9,967.14; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `HIVE` | 4 | $2.95 | $0.15 | $-1.45 | $45.21 | ▼ -1.45 after sell → book $9,966.99; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MARA` | 1 | $11.56 | $0.14 | $-0.40 | $56.63 | ▼ -0.40 after sell → book $9,966.85; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.63 | ▼ close $9,876.14 vs 09:30 $9,967.25 (session -90.71) | 16:00 close · cash $56.63 · equity $9,876.14 vs 09:30 $9,967.25 (-91.11; session marks -90.71) · 2 name(s) marked open→close (per-name table). AU×41 09:30 $119.80 → close $118.11 -69.29; FCX×63 09:30 $79.34 → close $79.00 -21.42 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.63 | ▼ 09:30 equity $9,836.73 vs yday $9,876.14 (-39.41) | 09:30 open · cash $56.63 (unchanged overnight, no fees) · equity $9,836.73 vs prior close $9,876.14 (-39.41) · 2 name(s) re-marked at the open (per-name table). AU×41 yday $118.11 → 09:30 $117.41 -28.70; FCX×63 yday $79.00 → 09:30 $78.83 -10.71 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.63 | ▲ close $9,851.49 vs 09:30 $9,836.73 (session +14.76) | 16:00 close · cash $56.63 · equity $9,851.49 vs 09:30 $9,836.73 (+14.76; session marks +14.76) · 2 name(s) marked open→close (per-name table). AU×41 09:30 $117.41 → close $118.40 +40.59; FCX×63 09:30 $78.83 → close $78.42 -25.83 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.63 | ▲ 09:30 equity $9,893.33 vs yday $9,851.49 (+41.84) | 09:30 open · cash $56.63 (unchanged overnight, no fees) · equity $9,893.33 vs prior close $9,851.49 (+41.84) · 2 name(s) re-marked at the open (per-name table). AU×41 yday $118.40 → 09:30 $119.19 +32.39; FCX×63 yday $78.42 → 09:30 $78.57 +9.45 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 41 | $119.19 | $2.16 | $+23.19 | $4,941.26 | ▲ +23.19 after sell → book $9,891.17; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 63 | $78.57 | $2.23 | $+86.31 | $9,888.94 | ▲ +86.31 after sell → book $9,888.94; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 5 | $324.41 | $2.00 | — | $8,264.88 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1648.16 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 11 | $141.76 | $2.02 | — | $6,703.50 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1648.16 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 4 | $400.42 | $2.00 | — | $5,099.82 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1648.16 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $3,791.79 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1648.16 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 6 | $240.22 | $2.01 | — | $2,348.47 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1648.16 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 50 | $32.90 | $2.14 | — | $701.33 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1648.16 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $701.33 | ▼ close $9,506.45 vs 09:30 $9,893.33 (session -370.32) | 16:00 close · cash $701.33 · equity $9,506.45 vs 09:30 $9,893.33 (-386.88; session marks -370.32) · 6 name(s) marked open→close (per-name table). KEYS×5 09:30 $324.41 → close $319.97 -22.20; SMTC×11 09:30 $141.76 → close $131.17 -116.49; CIEN×4 09:30 $400.42 → close $378.44 -87.92; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×6 09:30 $240.22 → close $236.98 -19.44; SEDG×50 09:30 $32.90 → close $31.41 -74.50 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $701.33 | ▼ 09:30 equity $9,506.03 vs yday $9,506.45 (-0.42) | 09:30 open · cash $701.33 (unchanged overnight, no fees) · equity $9,506.03 vs prior close $9,506.45 (-0.42) · 6 name(s) re-marked at the open (per-name table). KEYS×5 yday $319.97 → 09:30 $322.49 +12.60; SMTC×11 yday $131.17 → 09:30 $132.30 +12.43; CIEN×4 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×6 yday $236.98 → 09:30 $233.97 -18.09; SEDG×50 yday $31.41 → 09:30 $31.15 -13.00 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $701.33 | ▲ close $9,608.60 vs 09:30 $9,506.03 (session +102.57) | 16:00 close · cash $701.33 · equity $9,608.60 vs 09:30 $9,506.03 (+102.57; session marks +102.57) · 6 name(s) marked open→close (per-name table). KEYS×5 09:30 $322.49 → close $322.70 +1.05; SMTC×11 09:30 $132.30 → close $132.96 +7.26; CIEN×4 09:30 $378.44 → close $382.80 +17.44; MPWR×1 09:30 $1261.90 → close $1267.77 +5.87; DDOG×6 09:30 $233.97 → close $237.04 +18.45; SEDG×50 09:30 $31.15 → close $32.20 +52.50 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $701.33 | ▼ 09:30 equity $9,456.06 vs yday $9,608.60 (-152.54) | 09:30 open · cash $701.33 (unchanged overnight, no fees) · equity $9,456.06 vs prior close $9,608.60 (-152.54) · 6 name(s) re-marked at the open (per-name table). KEYS×5 yday $322.70 → 09:30 $321.47 -6.15; SMTC×11 yday $132.96 → 09:30 $127.63 -58.63; CIEN×4 yday $382.80 → 09:30 $376.89 -23.64; MPWR×1 yday $1267.77 → 09:30 $1245.11 -22.66; DDOG×6 yday $237.04 → 09:30 $232.88 -24.96; SEDG×50 yday $32.20 → 09:30 $31.87 -16.50 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $701.33 | ▼ close $9,387.47 vs 09:30 $9,456.06 (session -68.59) | 16:00 close · cash $701.33 · equity $9,387.47 vs 09:30 $9,456.06 (-68.59; session marks -68.59) · 6 name(s) marked open→close (per-name table). KEYS×5 09:30 $321.47 → close $319.27 -11.00; SMTC×11 09:30 $127.63 → close $132.27 +51.04; CIEN×4 09:30 $376.89 → close $360.33 -66.24; MPWR×1 09:30 $1245.11 → close $1225.96 -19.15; DDOG×6 09:30 $232.88 → close $223.84 -54.24; SEDG×50 09:30 $31.87 → close $32.49 +31.00 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $701.33 | ▼ 09:30 equity $9,346.21 vs yday $9,387.47 (-41.26) | 09:30 open · cash $701.33 (unchanged overnight, no fees) · equity $9,346.21 vs prior close $9,387.47 (-41.26) · 6 name(s) re-marked at the open (per-name table). KEYS×5 yday $319.27 → 09:30 $318.04 -6.15; SMTC×11 yday $132.27 → 09:30 $133.00 +8.03; CIEN×4 yday $360.33 → 09:30 $357.25 -12.32; MPWR×1 yday $1225.96 → 09:30 $1224.92 -1.04; DDOG×6 yday $223.84 → 09:30 $219.46 -26.28; SEDG×50 yday $32.49 → 09:30 $32.42 -3.50 | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 5 | $318.04 | $2.03 | $-35.88 | $2,289.50 | ▼ -35.88 after sell → book $9,344.18; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 11 | $133.00 | $2.04 | $-100.43 | $3,750.45 | ▼ -100.43 after sell → book $9,342.13; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 4 | $357.25 | $2.02 | $-176.71 | $5,177.43 | ▼ -176.71 after sell → book $9,340.11; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 1 | $1224.92 | $2.01 | $-85.12 | $6,400.34 | ▼ -85.12 after sell → book $9,338.10; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 6 | $219.46 | $2.03 | $-128.60 | $7,715.07 | ▼ -128.60 after sell → book $9,336.07; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 50 | $32.42 | $2.16 | $-28.30 | $9,333.91 | ▼ -28.30 after sell → book $9,333.91; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,333.91 | ▲ close $9,333.91 vs 09:30 $9,346.21 (session +0.00) | 16:00 close · cash $9,333.91 · no lots left · equity $9,333.91. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,333.91 | ▲ 09:30 equity $9,333.91 vs yday $9,333.91 (-0.00) | 09:30 open · cash $9,333.91 · no holdings · equity $9,333.91 vs prior close $9,333.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 5 | $351.74 | $2.00 | — | $7,573.20 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1866.78 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $6,112.27 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1866.78 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 57 | $32.31 | $2.16 | — | $4,268.44 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1866.78 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 117 | $15.87 | $2.34 | — | $2,409.31 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1866.78 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 78 | $23.88 | $2.22 | — | $544.45 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1866.78 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $544.45 | ▲ close $9,634.86 vs 09:30 $9,333.91 (session +311.68) | 16:00 close · cash $544.45 · equity $9,634.86 vs 09:30 $9,333.91 (+300.95; session marks +311.68) · 5 name(s) marked open→close (per-name table). AVGO×5 09:30 $351.74 → close $357.16 +27.10; DELL×3 09:30 $486.31 → close $516.39 +90.24; CXW×57 09:30 $32.31 → close $33.66 +76.95; FRNM×117 09:30 $15.87 → close $16.90 +120.51; MMED×78 09:30 $23.88 → close $23.84 -3.12 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $544.45 | ▼ 09:30 equity $9,569.83 vs yday $9,634.86 (-65.03) | 09:30 open · cash $544.45 (unchanged overnight, no fees) · equity $9,569.83 vs prior close $9,634.86 (-65.03) · 5 name(s) re-marked at the open (per-name table). AVGO×5 yday $357.16 → 09:30 $359.70 +12.70; DELL×3 yday $516.39 → 09:30 $513.78 -7.83; CXW×57 yday $33.66 → 09:30 $33.46 -11.40; FRNM×117 yday $16.90 → 09:30 $16.40 -58.50; MMED×78 yday $23.84 → 09:30 $23.84 +0.00 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 1 | $263.36 | $1.99 | — | $279.09 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $272.22 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 3 | $75.65 | $2.00 | — | $50.14 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $272.22 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.14 | ▲ close $9,609.46 vs 09:30 $9,569.83 (session +43.63) | 16:00 close · cash $50.14 · equity $9,609.46 vs 09:30 $9,569.83 (+39.63; session marks +43.63) · 7 name(s) marked open→close (per-name table). AVGO×5 09:30 $359.70 → close $357.90 -9.00; DELL×3 09:30 $513.78 → close $524.14 +31.08; CXW×57 09:30 $33.46 → close $34.71 +71.25; FRNM×117 09:30 $16.40 → close $16.31 -10.53; MMED×78 09:30 $23.84 → close $23.29 -42.90; CRM×1 09:30 $263.36 → close $259.23 -4.13; MRX×3 09:30 $75.65 → close $78.27 +7.86 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.14 | ▲ 09:30 equity $9,653.22 vs yday $9,609.46 (+43.76) | 09:30 open · cash $50.14 (unchanged overnight, no fees) · equity $9,653.22 vs prior close $9,609.46 (+43.76) · 7 name(s) re-marked at the open (per-name table). AVGO×5 yday $357.90 → 09:30 $363.68 +28.90; DELL×3 yday $524.14 → 09:30 $521.15 -8.97; CXW×57 yday $34.71 → 09:30 $34.49 -12.54; FRNM×117 yday $16.31 → 09:30 $16.74 +50.31; MMED×78 yday $23.29 → 09:30 $23.16 -10.14; CRM×1 yday $259.23 → 09:30 $253.72 -5.51; MRX×3 yday $78.27 → 09:30 $78.84 +1.71 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.14 | ▲ close $9,661.47 vs 09:30 $9,653.22 (session +8.25) | 16:00 close · cash $50.14 · equity $9,661.47 vs 09:30 $9,653.22 (+8.25; session marks +8.25) · 7 name(s) marked open→close (per-name table). AVGO×5 09:30 $363.68 → close $368.56 +24.40; DELL×3 09:30 $521.15 → close $533.88 +38.19; CXW×57 09:30 $34.49 → close $35.05 +31.92; FRNM×117 09:30 $16.74 → close $15.99 -87.75; MMED×78 09:30 $23.16 → close $23.32 +12.48; CRM×1 09:30 $253.72 → close $249.12 -4.60; MRX×3 09:30 $78.84 → close $76.71 -6.39 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.14 | ▼ 09:30 equity $9,654.89 vs yday $9,661.47 (-6.58) | 09:30 open · cash $50.14 (unchanged overnight, no fees) · equity $9,654.89 vs prior close $9,661.47 (-6.58) · 7 name(s) re-marked at the open (per-name table). AVGO×5 yday $368.56 → 09:30 $366.23 -11.65; DELL×3 yday $533.88 → 09:30 $538.47 +13.77; CXW×57 yday $35.05 → 09:30 $35.09 +2.28; FRNM×117 yday $15.99 → 09:30 $15.96 -3.51; MMED×78 yday $23.32 → 09:30 $23.22 -7.80; CRM×1 yday $249.12 → 09:30 $249.78 +0.66; MRX×3 yday $76.71 → 09:30 $76.60 -0.33 | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 5 | $366.23 | $2.03 | $+68.42 | $1,879.26 | ▲ +68.42 after sell → book $9,652.86; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 3 | $538.47 | $2.02 | $+152.46 | $3,492.65 | ▲ +152.46 after sell → book $9,650.84; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CXW` | 57 | $35.09 | $2.19 | $+154.11 | $5,490.59 | ▲ +154.11 after sell → book $9,648.65; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 117 | $15.96 | $2.38 | $+5.81 | $7,355.54 | ▲ +5.81 after sell → book $9,646.28; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 78 | $23.22 | $2.25 | $-55.96 | $9,164.45 | ▼ -55.96 after sell → book $9,644.03; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,164.45 | ▼ close $9,635.77 vs 09:30 $9,654.89 (session -8.26) | 16:00 close · cash $9,164.45 · equity $9,635.77 vs 09:30 $9,654.89 (-19.12; session marks -8.26) · 2 name(s) marked open→close (per-name table). CRM×1 09:30 $249.78 → close $244.16 -5.62; MRX×3 09:30 $76.60 → close $75.72 -2.64 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,164.45 | ▼ 09:30 equity $9,634.80 vs yday $9,635.77 (-0.97) | 09:30 open · cash $9,164.45 (unchanged overnight, no fees) · equity $9,634.80 vs prior close $9,635.77 (-0.97) · 2 name(s) re-marked at the open (per-name table). CRM×1 yday $244.16 → 09:30 $245.35 +1.19; MRX×3 yday $75.72 → 09:30 $75.00 -2.16 | — |
| 2026-09-10 09:30 ET | **SELL** | `CRM` | 1 | $245.35 | $2.01 | $-22.02 | $9,407.78 | ▼ -22.02 after sell → book $9,632.78; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MRX` | 3 | $75.00 | $2.02 | $-5.97 | $9,630.77 | ▼ -5.97 after sell → book $9,630.77; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,630.77 | ▲ close $9,630.77 vs 09:30 $9,634.80 (session +0.00) | 16:00 close · cash $9,630.77 · no lots left · equity $9,630.77. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,630.77 | ▲ 09:30 equity $9,630.77 vs yday $9,630.77 (-0.00) | 09:30 open · cash $9,630.77 · no holdings · equity $9,630.77 vs prior close $9,630.77 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 58 | $164.43 | $2.16 | — | $91.66 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $9630.77 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $91.66 | ▼ close $8,807.90 vs 09:30 $9,630.77 (session -820.70) | 16:00 close · cash $91.66 · equity $8,807.90 vs 09:30 $9,630.77 (-822.87; session marks -820.70) · 1 name(s) marked open→close (per-name table). ORCL×58 09:30 $164.43 → close $150.28 -820.70 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $91.66 | ▼ 09:30 equity $8,294.02 vs yday $8,807.90 (-513.88) | 09:30 open · cash $91.66 (unchanged overnight, no fees) · equity $8,294.02 vs prior close $8,807.90 (-513.88) · 1 name(s) re-marked at the open (per-name table). ORCL×58 yday $150.28 → 09:30 $141.42 -513.88 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $91.66 | ▲ close $8,489.48 vs 09:30 $8,294.02 (session +195.46) | 16:00 close · cash $91.66 · equity $8,489.48 vs 09:30 $8,294.02 (+195.46; session marks +195.46) · 1 name(s) marked open→close (per-name table). ORCL×58 09:30 $141.42 → close $144.79 +195.46 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $91.66 | ▼ 09:30 equity $8,412.34 vs yday $8,489.48 (-77.14) | 09:30 open · cash $91.66 (unchanged overnight, no fees) · equity $8,412.34 vs prior close $8,489.48 (-77.14) · 1 name(s) re-marked at the open (per-name table). ORCL×58 yday $144.79 → 09:30 $143.46 -77.14 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $91.66 | ▼ close $8,231.96 vs 09:30 $8,412.34 (session -180.38) | 16:00 close · cash $91.66 · equity $8,231.96 vs 09:30 $8,412.34 (-180.38; session marks -180.38) · 1 name(s) marked open→close (per-name table). ORCL×58 09:30 $143.46 → close $140.35 -180.38 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $91.66 | ▼ 09:30 equity $8,213.40 vs yday $8,231.96 (-18.56) | 09:30 open · cash $91.66 (unchanged overnight, no fees) · equity $8,213.40 vs prior close $8,231.96 (-18.56) · 1 name(s) re-marked at the open (per-name table). ORCL×58 yday $140.35 → 09:30 $140.03 -18.56 | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 58 | $140.03 | $2.24 | $-1419.60 | $8,211.16 | ▼ -1,419.60 after sell → book $8,211.16; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 104 | $26.27 | $2.30 | — | $5,476.78 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2737.05 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 14 | $189.17 | $2.03 | — | $2,826.37 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2737.05 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 68 | $39.99 | $2.19 | — | $104.85 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2737.05 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $104.85 | ▼ close $8,052.85 vs 09:30 $8,213.40 (session -151.78) | 16:00 close · cash $104.85 · equity $8,052.85 vs 09:30 $8,213.40 (-160.55; session marks -151.78) · 3 name(s) marked open→close (per-name table). WAY×104 09:30 $26.27 → close $26.59 +33.28; QCOM×14 09:30 $189.17 → close $184.84 -60.62; SM×68 09:30 $39.99 → close $38.16 -124.44 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.85 | ▲ 09:30 equity $8,081.55 vs yday $8,052.85 (+28.70) | 09:30 open · cash $104.85 (unchanged overnight, no fees) · equity $8,081.55 vs prior close $8,052.85 (+28.70) · 3 name(s) re-marked at the open (per-name table). WAY×104 yday $26.59 → 09:30 $26.51 -8.32; QCOM×14 yday $184.84 → 09:30 $190.35 +77.14; SM×68 yday $38.16 → 09:30 $37.57 -40.12 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 1 | $22.12 | $0.22 | — | $82.51 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $34.95 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.51 | ▼ close $8,018.22 vs 09:30 $8,081.55 (session -63.11) | 16:00 close · cash $82.51 · equity $8,018.22 vs 09:30 $8,081.55 (-63.33; session marks -63.11) · 4 name(s) marked open→close (per-name table). WAY×104 09:30 $26.51 → close $26.51 +0.00; QCOM×14 09:30 $190.35 → close $188.71 -22.96; SM×68 09:30 $37.57 → close $36.97 -40.80; GME×1 09:30 $22.12 → close $22.77 +0.65 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.51 | ▲ 09:30 equity $8,094.13 vs yday $8,018.22 (+75.91) | 09:30 open · cash $82.51 (unchanged overnight, no fees) · equity $8,094.13 vs prior close $8,018.22 (+75.91) · 4 name(s) re-marked at the open (per-name table). WAY×104 yday $26.51 → 09:30 $26.95 +45.76; QCOM×14 yday $188.71 → 09:30 $191.34 +36.82; SM×68 yday $36.97 → 09:30 $36.87 -6.80; GME×1 yday $22.77 → 09:30 $22.90 +0.13 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 1 | $14.79 | $0.15 | — | $67.57 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $20.63 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 1 | $14.07 | $0.14 | — | $53.36 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $20.63 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.36 | ▼ close $7,774.81 vs 09:30 $8,094.13 (session -319.03) | 16:00 close · cash $53.36 · equity $7,774.81 vs 09:30 $8,094.13 (-319.32; session marks -319.03) · 6 name(s) marked open→close (per-name table). WAY×104 09:30 $26.95 → close $25.66 -134.16; QCOM×14 09:30 $191.34 → close $177.72 -190.68; SM×68 09:30 $36.87 → close $36.97 +6.80; GME×1 09:30 $22.90 → close $22.64 -0.26; RARE×1 09:30 $14.79 → close $14.51 -0.28; BHVN×1 09:30 $14.07 → close $13.62 -0.45 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.36 | ▼ 09:30 equity $7,772.80 vs yday $7,774.81 (-2.01) | 09:30 open · cash $53.36 (unchanged overnight, no fees) · equity $7,772.80 vs prior close $7,774.81 (-2.01) · 6 name(s) re-marked at the open (per-name table). WAY×104 yday $25.66 → 09:30 $25.94 +29.12; QCOM×14 yday $177.72 → 09:30 $180.61 +40.46; SM×68 yday $36.97 → 09:30 $35.91 -72.08; GME×1 yday $22.64 → 09:30 $22.78 +0.14; RARE×1 yday $14.51 → 09:30 $14.58 +0.07; BHVN×1 yday $13.62 → 09:30 $13.90 +0.28 | — |
| 2026-09-21 09:30 ET | **SELL** | `WAY` | 104 | $25.94 | $2.34 | $-38.96 | $2,748.78 | ▼ -38.96 after sell → book $7,770.46; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QCOM` | 14 | $180.61 | $2.06 | $-123.93 | $5,275.25 | ▼ -123.93 after sell → book $7,768.39; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SM` | 68 | $35.91 | $2.22 | $-281.86 | $7,714.91 | ▼ -281.86 after sell → book $7,766.17; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 16 | $230.25 | $2.04 | — | $4,028.87 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+12.5; leftover $3857.45 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 20 | $190.30 | $2.05 | — | $220.82 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+10.6; leftover $3857.45 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $220.82 | ▼ close $7,402.25 vs 09:30 $7,772.80 (session -359.83) | 16:00 close · cash $220.82 · equity $7,402.25 vs 09:30 $7,772.80 (-370.55; session marks -359.83) · 5 name(s) marked open→close (per-name table). GME×1 09:30 $22.78 → close $22.76 -0.02; RARE×1 09:30 $14.58 → close $14.65 +0.07; BHVN×1 09:30 $13.90 → close $14.22 +0.32; VICR×16 09:30 $230.25 → close $223.90 -101.60; SMTC×20 09:30 $190.30 → close $177.37 -258.60 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $220.82 | ▲ 09:30 equity $7,857.24 vs yday $7,402.25 (+454.99) | 09:30 open · cash $220.82 (unchanged overnight, no fees) · equity $7,857.24 vs prior close $7,402.25 (+454.99) · 5 name(s) re-marked at the open (per-name table). GME×1 yday $22.76 → 09:30 $23.50 +0.74; RARE×1 yday $14.65 → 09:30 $14.78 +0.13; BHVN×1 yday $14.22 → 09:30 $14.22 +0.00; VICR×16 yday $223.90 → 09:30 $252.37 +455.52; SMTC×20 yday $177.37 → 09:30 $177.30 -1.40 | — |
| 2026-09-22 09:30 ET | **SELL** | `GME` | 1 | $23.50 | $0.26 | $+0.90 | $244.06 | ▲ +0.90 after sell → book $7,856.98; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $244.06 | ▼ close $7,757.90 vs 09:30 $7,857.24 (session -99.08) | 16:00 close · cash $244.06 · equity $7,757.90 vs 09:30 $7,857.24 (-99.34; session marks -99.08) · 4 name(s) marked open→close (per-name table). RARE×1 09:30 $14.78 → close $15.62 +0.84; BHVN×1 09:30 $14.22 → close $14.22 +0.00; VICR×16 09:30 $252.37 → close $249.00 -53.92; SMTC×20 09:30 $177.30 → close $175.00 -46.00 | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $244.06 | ▲ 09:30 equity $8,028.30 vs yday $7,757.90 (+270.40) | 09:30 open · cash $244.06 (unchanged overnight, no fees) · equity $8,028.30 vs prior close $7,757.90 (+270.40) · 4 name(s) re-marked at the open (per-name table). RARE×1 yday $15.62 → 09:30 $15.40 -0.21; BHVN×1 yday $14.22 → 09:30 $14.84 +0.62; VICR×16 yday $249.00 → 09:30 $266.50 +280.00; SMTC×20 yday $175.00 → 09:30 $174.50 -10.00 | — |
| 2026-09-23 09:30 ET | **SELL** | `RARE` | 1 | $15.40 | $0.18 | $+0.28 | $259.29 | ▲ +0.28 after sell → book $8,028.13; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BHVN` | 1 | $14.84 | $0.17 | $+0.45 | $273.95 | ▲ +0.45 after sell → book $8,027.95; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 17 | $7.95 | $1.40 | — | $137.40 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $136.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 8 | $15.70 | $1.28 | — | $10.52 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.5; leftover $136.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.52 | ▲ close $8,170.64 vs 09:30 $8,028.30 (session +145.37) | 16:00 close · cash $10.52 · equity $8,170.64 vs 09:30 $8,028.30 (+142.34; session marks +145.37) · 4 name(s) marked open→close (per-name table). VICR×16 09:30 $266.50 → close $283.16 +266.56; SMTC×20 09:30 $174.50 → close $169.33 -103.40; PGEN×17 09:30 $7.95 → close $7.44 -8.67; SGRY×8 09:30 $15.70 → close $14.56 -9.12 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 15.87 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 15.87 < 1 share @ 115.18 |
| 2026-08-21 | `GRAL` | cash | leftover split 15.87 < 1 share @ 78.88 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CM` | cash | leftover split 56.63 < 1 share @ 118.50 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 8.09 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 8.09 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 8.09 < 1 share @ 1746.53 |
| 2026-08-27 | `CM` | cash | leftover split 8.09 < 1 share @ 118.77 |
| 2026-08-27 | `GEN` | cash | leftover split 8.09 < 1 share @ 29.83 |
| 2026-08-27 | `LRCX` | cash | leftover split 8.09 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 8.09 < 1 share @ 222.86 |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DDOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DDOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CXW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CXW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `MRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-09 | `CRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 34.95 < 1 share @ 170.85 |
| 2026-09-17 | `CLS` | cash | leftover split 34.95 < 1 share @ 337.75 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TH` | cash | leftover split 20.63 < 1 share @ 20.91 |
| 2026-09-18 | `CLS` | cash | leftover split 20.63 < 1 share @ 332.06 |
| 2026-09-21 | `GME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `VICR` | 16 | 2026-09-21 @ $230.25 | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+12.5; leftover $3857.45 |
| `SMTC` | 20 | 2026-09-21 @ $190.30 | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+10.6; leftover $3857.45 |
| `PGEN` | 17 | 2026-09-23 @ $7.95 | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $136.98 |
| `SGRY` | 8 | 2026-09-23 @ $15.70 | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.5; leftover $136.98 |
