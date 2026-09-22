# Factor mine action — `union_news_g_conv_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `conviction` · sell `list` · S-boost `none` · merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5

Cash book **-13.03%** ($8,697) · signal-only (no cash/fees) was +215.31%. Starts YES **2/27**. Fills 69 · skips 113 · realized $-1133.43.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: the news camera (does the morning packet like the headline?) is green.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 4.
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
- **Gate** `news=good` · **rank** `cond` · **top_n** 4.
- **Size** `conviction` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $150.95.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `HLIT` | 531 | — | $13.18 | +0.00 | $13.92 | +392.94 | +392.94 | +0.00 | +392.94 |
| 2026-08-14 | `ANGX` | 232 | — | $4.31 | +0.00 | $4.37 | +13.92 | +13.92 | +0.00 | +13.92 |
| 2026-08-14 | `ARX` | 51 | — | $19.57 | +0.00 | $19.58 | +0.51 | +0.51 | +0.00 | +0.51 |
| 2026-08-17 | `HLIT` | 531 | $13.92 | $13.84 | -42.48 | $13.43 | -217.71 | -260.19 | +350.46 | +132.75 |
| 2026-08-17 | `ANGX` | 232 | $4.37 | $4.60 | +53.36 | $4.71 | +25.52 | +78.88 | +67.28 | +92.80 |
| 2026-08-17 | `ARX` | 51 | $19.58 | $19.57 | -0.51 | $19.54 | -1.53 | -2.04 | +0.00 | -1.53 |
| 2026-08-17 | `DVN` | 8 | — | $46.18 | +0.00 | $47.57 | +11.12 | +11.12 | +0.00 | +11.12 |
| 2026-08-17 | `EOG` | 2 | — | $142.77 | +0.00 | $146.15 | +6.76 | +6.76 | +0.00 | +6.76 |
| 2026-08-17 | `OUST` | 2 | — | $49.00 | +0.00 | $48.13 | -1.74 | -1.74 | +0.00 | -1.74 |
| 2026-08-18 | `HLIT` | 531 | $13.43 | $12.93 | -265.50 | $12.73 | -106.20 | -371.70 | -132.75 | -238.95 |
| 2026-08-18 | `ANGX` | 232 | $4.71 | $4.79 | +18.56 | $4.85 | +13.92 | +32.48 | +111.36 | +125.28 |
| 2026-08-18 | `ARX` | 51 | $19.54 | $19.57 | +1.53 | $19.56 | -0.51 | +1.02 | +0.00 | -0.51 |
| 2026-08-18 | `DVN` | 8 | $47.57 | $48.00 | +3.44 | $47.83 | -1.36 | +2.08 | +14.56 | +13.20 |
| 2026-08-18 | `EOG` | 2 | $146.15 | $148.04 | +3.78 | $148.70 | +1.32 | +5.10 | +10.54 | +11.86 |
| 2026-08-18 | `OUST` | 2 | $48.13 | $45.09 | -6.08 | $42.99 | -4.20 | -10.28 | -7.82 | -12.02 |
| 2026-08-19 | `HLIT` | 531 | $12.73 | $12.90 | +90.27 | — | +0.00 | +90.27 | -148.68 | — |
| 2026-08-19 | `ANGX` | 232 | $4.85 | $4.79 | -13.92 | $4.60 | -44.08 | -58.00 | +111.36 | +67.28 |
| 2026-08-19 | `ARX` | 51 | $19.56 | $19.58 | +1.02 | — | +0.00 | +1.02 | +0.51 | — |
| 2026-08-19 | `DVN` | 8 | $47.83 | $48.22 | +3.12 | $48.19 | -0.24 | +2.88 | +16.32 | +16.08 |
| 2026-08-19 | `EOG` | 2 | $148.70 | $149.86 | +2.32 | $149.48 | -0.76 | +1.56 | +14.18 | +13.42 |
| 2026-08-19 | `OUST` | 2 | $42.99 | $43.00 | +0.02 | $40.06 | -5.88 | -5.86 | -12.00 | -17.88 |
| 2026-08-20 | `ANGX` | 232 | $4.60 | $4.57 | -6.96 | — | +0.00 | -6.96 | +60.32 | — |
| 2026-08-20 | `DVN` | 8 | $48.19 | $49.02 | +6.64 | — | +0.00 | +6.64 | +22.72 | — |
| 2026-08-20 | `EOG` | 2 | $149.48 | $151.45 | +3.94 | — | +0.00 | +3.94 | +17.36 | — |
| 2026-08-20 | `OUST` | 2 | $40.06 | $40.63 | +1.14 | — | +0.00 | +1.14 | -16.74 | — |
| 2026-08-20 | `BHP` | 76 | — | $91.01 | +0.00 | $93.63 | +199.12 | +199.12 | +0.00 | +199.12 |
| 2026-08-20 | `APA` | 22 | — | $44.76 | +0.00 | $44.39 | -8.14 | -8.14 | +0.00 | -8.14 |
| 2026-08-20 | `AUTL` | 400 | — | $2.47 | +0.00 | $2.46 | -4.00 | -4.00 | +0.00 | -4.00 |
| 2026-08-20 | `CRSP` | 16 | — | $58.73 | +0.00 | $58.12 | -9.76 | -9.76 | +0.00 | -9.76 |
| 2026-08-21 | `BHP` | 76 | $93.63 | $95.72 | +158.84 | $97.03 | +99.56 | +258.40 | +357.96 | +457.52 |
| 2026-08-21 | `APA` | 22 | $44.39 | $44.52 | +2.86 | $43.39 | -24.86 | -22.00 | -5.28 | -30.14 |
| 2026-08-21 | `AUTL` | 400 | $2.46 | $2.47 | +4.00 | $2.41 | -24.00 | -20.00 | +0.00 | -24.00 |
| 2026-08-21 | `CRSP` | 16 | $58.12 | $59.72 | +25.60 | $59.50 | -3.52 | +22.08 | +15.84 | +12.32 |
| 2026-08-24 | `BHP` | 76 | $97.03 | $97.31 | +21.28 | $97.13 | -13.68 | +7.60 | +478.80 | +465.12 |
| 2026-08-24 | `APA` | 22 | $43.39 | $42.93 | -10.12 | $42.96 | +0.66 | -9.46 | -40.26 | -39.60 |
| 2026-08-24 | `AUTL` | 400 | $2.41 | $2.40 | -4.00 | $2.34 | -24.00 | -28.00 | -28.00 | -52.00 |
| 2026-08-24 | `CRSP` | 16 | $59.50 | $58.75 | -12.00 | $57.08 | -26.80 | -38.80 | +0.32 | -26.48 |
| 2026-08-25 | `BHP` | 76 | $97.13 | $95.86 | -96.52 | — | +0.00 | -96.52 | +368.60 | — |
| 2026-08-25 | `APA` | 22 | $42.96 | $41.38 | -34.76 | — | +0.00 | -34.76 | -74.36 | — |
| 2026-08-25 | `AUTL` | 400 | $2.34 | $2.38 | +16.00 | — | +0.00 | +16.00 | -36.00 | — |
| 2026-08-25 | `CRSP` | 16 | $57.08 | $57.93 | +13.68 | — | +0.00 | +13.68 | -12.80 | — |
| 2026-08-25 | `AU` | 59 | — | $118.52 | +0.00 | $123.39 | +287.33 | +287.33 | +0.00 | +287.33 |
| 2026-08-25 | `FCX` | 13 | — | $77.13 | +0.00 | $79.91 | +36.14 | +36.14 | +0.00 | +36.14 |
| 2026-08-25 | `EZPW` | 28 | — | $35.05 | +0.00 | $35.23 | +5.04 | +5.04 | +0.00 | +5.04 |
| 2026-08-25 | `RUM` | 107 | — | $9.42 | +0.00 | $10.23 | +86.67 | +86.67 | +0.00 | +86.67 |
| 2026-08-26 | `AU` | 59 | $123.39 | $119.80 | -211.81 | $118.11 | -99.71 | -311.52 | +75.52 | -24.19 |
| 2026-08-26 | `FCX` | 13 | $79.91 | $79.34 | -7.41 | $79.00 | -4.42 | -11.83 | +28.73 | +24.31 |
| 2026-08-26 | `EZPW` | 28 | $35.23 | $35.70 | +13.16 | $33.90 | -50.40 | -37.24 | +18.20 | -32.20 |
| 2026-08-26 | `RUM` | 107 | $10.23 | $10.07 | -17.12 | $9.38 | -74.37 | -91.49 | +69.55 | -4.81 |
| 2026-08-26 | `TRLV` | 1 | — | $11.22 | +0.00 | $11.43 | +0.21 | +0.21 | +0.00 | +0.21 |
| 2026-08-26 | `CAPR` | 1 | — | $8.29 | +0.00 | $9.36 | +1.07 | +1.07 | +0.00 | +1.07 |
| 2026-08-27 | `AU` | 59 | $118.11 | $117.41 | -41.30 | $118.40 | +58.41 | +17.11 | -65.49 | -7.08 |
| 2026-08-27 | `FCX` | 13 | $79.00 | $78.83 | -2.21 | $78.42 | -5.33 | -7.54 | +22.10 | +16.77 |
| 2026-08-27 | `EZPW` | 28 | $33.90 | $33.50 | -11.20 | $34.41 | +25.48 | +14.28 | -43.40 | -17.92 |
| 2026-08-27 | `RUM` | 107 | $9.38 | $9.51 | +14.44 | $9.43 | -8.56 | +5.88 | +9.63 | +1.07 |
| 2026-08-27 | `TRLV` | 1 | $11.43 | $11.38 | -0.05 | $11.03 | -0.35 | -0.40 | +0.16 | -0.19 |
| 2026-08-27 | `CAPR` | 1 | $9.36 | $9.19 | -0.17 | $10.06 | +0.87 | +0.70 | +0.90 | +1.77 |
| 2026-08-28 | `AU` | 59 | $118.40 | $119.19 | +46.61 | — | +0.00 | +46.61 | +39.53 | — |
| 2026-08-28 | `FCX` | 13 | $78.42 | $78.57 | +1.95 | — | +0.00 | +1.95 | +18.72 | — |
| 2026-08-28 | `EZPW` | 28 | $34.41 | $34.50 | +2.52 | — | +0.00 | +2.52 | -15.40 | — |
| 2026-08-28 | `RUM` | 107 | $9.43 | $9.30 | -13.91 | — | +0.00 | -13.91 | -12.84 | — |
| 2026-08-28 | `TRLV` | 1 | $11.03 | $11.00 | -0.03 | $11.82 | +0.82 | +0.79 | -0.22 | +0.60 |
| 2026-08-28 | `CAPR` | 1 | $10.06 | $9.73 | -0.33 | $9.59 | -0.14 | -0.47 | +1.44 | +1.30 |
| 2026-08-28 | `KEYS` | 21 | — | $324.41 | +0.00 | $319.97 | -93.24 | -93.24 | +0.00 | -93.24 |
| 2026-08-28 | `SMTC` | 7 | — | $141.76 | +0.00 | $131.17 | -74.13 | -74.13 | +0.00 | -74.13 |
| 2026-08-28 | `CIEN` | 2 | — | $400.42 | +0.00 | $378.44 | -43.96 | -43.96 | +0.00 | -43.96 |
| 2026-08-31 | `TRLV` | 1 | $11.82 | $11.80 | -0.02 | — | +0.00 | -0.02 | +0.58 | — |
| 2026-08-31 | `CAPR` | 1 | $9.59 | $9.50 | -0.09 | — | +0.00 | -0.09 | +1.21 | — |
| 2026-08-31 | `KEYS` | 21 | $319.97 | $322.49 | +52.92 | $322.70 | +4.41 | +57.33 | -40.32 | -35.91 |
| 2026-08-31 | `SMTC` | 7 | $131.17 | $132.30 | +7.91 | $132.96 | +4.62 | +12.53 | -66.22 | -61.60 |
| 2026-08-31 | `CIEN` | 2 | $378.44 | $378.44 | +0.00 | $382.80 | +8.72 | +8.72 | -43.96 | -35.24 |
| 2026-09-01 | `KEYS` | 21 | $322.70 | $321.47 | -25.83 | $319.27 | -46.20 | -72.03 | -61.74 | -107.94 |
| 2026-09-01 | `SMTC` | 7 | $132.96 | $127.63 | -37.31 | $132.27 | +32.48 | -4.83 | -98.91 | -66.43 |
| 2026-09-01 | `CIEN` | 2 | $382.80 | $376.89 | -11.82 | $360.33 | -33.12 | -44.94 | -47.06 | -80.18 |
| 2026-09-02 | `KEYS` | 21 | $319.27 | $318.04 | -25.83 | — | +0.00 | -25.83 | -133.77 | — |
| 2026-09-02 | `SMTC` | 7 | $132.27 | $133.00 | +5.11 | — | +0.00 | +5.11 | -61.32 | — |
| 2026-09-02 | `CIEN` | 2 | $360.33 | $357.25 | -6.16 | — | +0.00 | -6.16 | -86.34 | — |
| 2026-09-03 | `AVGO` | 19 | — | $351.74 | +0.00 | $357.16 | +102.98 | +102.98 | +0.00 | +102.98 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `CXW` | 30 | — | $32.31 | +0.00 | $33.66 | +40.50 | +40.50 | +0.00 | +40.50 |
| 2026-09-03 | `FRNM` | 62 | — | $15.87 | +0.00 | $16.90 | +63.86 | +63.86 | +0.00 | +63.86 |
| 2026-09-04 | `AVGO` | 19 | $357.16 | $359.70 | +48.26 | $357.90 | -34.20 | +14.06 | +151.24 | +117.04 |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | $524.14 | +20.72 | +15.50 | +54.94 | +75.66 |
| 2026-09-04 | `CXW` | 30 | $33.66 | $33.46 | -6.00 | $34.71 | +37.50 | +31.50 | +34.50 | +72.00 |
| 2026-09-04 | `FRNM` | 62 | $16.90 | $16.40 | -31.00 | $16.31 | -5.58 | -36.58 | +32.86 | +27.28 |
| 2026-09-08 | `AVGO` | 19 | $357.90 | $363.68 | +109.82 | $368.56 | +92.72 | +202.54 | +226.86 | +319.58 |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | $533.88 | +25.46 | +19.48 | +69.68 | +95.14 |
| 2026-09-08 | `CXW` | 30 | $34.71 | $34.49 | -6.60 | $35.05 | +16.80 | +10.20 | +65.40 | +82.20 |
| 2026-09-08 | `FRNM` | 62 | $16.31 | $16.74 | +26.66 | $15.99 | -46.50 | -19.84 | +53.94 | +7.44 |
| 2026-09-09 | `AVGO` | 19 | $368.56 | $366.23 | -44.27 | — | +0.00 | -44.27 | +275.31 | — |
| 2026-09-09 | `DELL` | 2 | $533.88 | $538.47 | +9.18 | — | +0.00 | +9.18 | +104.32 | — |
| 2026-09-09 | `CXW` | 30 | $35.05 | $35.09 | +1.20 | — | +0.00 | +1.20 | +83.40 | — |
| 2026-09-09 | `FRNM` | 62 | $15.99 | $15.96 | -1.86 | — | +0.00 | -1.86 | +5.58 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 43 | — | $164.43 | +0.00 | $150.28 | -608.45 | -608.45 | +0.00 | -608.45 |
| 2026-09-11 | `ADBE` | 4 | — | $242.17 | +0.00 | $252.23 | +40.24 | +40.24 | +0.00 | +40.24 |
| 2026-09-11 | `BAK` | 485 | — | $2.12 | +0.00 | $2.08 | -19.40 | -19.40 | +0.00 | -19.40 |
| 2026-09-11 | `AMTX` | 504 | — | $2.04 | +0.00 | $2.01 | -15.12 | -15.12 | +0.00 | -15.12 |
| 2026-09-14 | `ORCL` | 43 | $150.28 | $141.42 | -380.98 | $144.79 | +144.91 | -236.07 | -989.43 | -844.52 |
| 2026-09-14 | `ADBE` | 4 | $252.23 | $261.51 | +37.12 | $265.60 | +16.36 | +53.48 | +77.36 | +93.72 |
| 2026-09-14 | `BAK` | 485 | $2.08 | $2.05 | -14.55 | $2.01 | -19.40 | -33.95 | -33.95 | -53.35 |
| 2026-09-14 | `AMTX` | 504 | $2.01 | $2.01 | +0.00 | $1.95 | -30.24 | -30.24 | -15.12 | -45.36 |
| 2026-09-15 | `ORCL` | 43 | $144.79 | $143.46 | -57.19 | $140.35 | -133.73 | -190.92 | -901.71 | -1035.44 |
| 2026-09-15 | `ADBE` | 4 | $265.60 | $261.70 | -15.60 | $257.76 | -15.76 | -31.36 | +78.12 | +62.36 |
| 2026-09-15 | `BAK` | 485 | $2.01 | $2.02 | +4.85 | $2.10 | +38.80 | +43.65 | -48.50 | -9.70 |
| 2026-09-15 | `AMTX` | 504 | $1.95 | $1.93 | -10.08 | $1.91 | -10.08 | -20.16 | -55.44 | -65.52 |
| 2026-09-16 | `ORCL` | 43 | $140.35 | $140.03 | -13.76 | — | +0.00 | -13.76 | -1049.20 | — |
| 2026-09-16 | `ADBE` | 4 | $257.76 | $253.34 | -17.68 | — | +0.00 | -17.68 | +44.68 | — |
| 2026-09-16 | `BAK` | 485 | $2.10 | $1.84 | -126.10 | — | +0.00 | -126.10 | -135.80 | — |
| 2026-09-16 | `AMTX` | 504 | $1.91 | $1.89 | -10.08 | — | +0.00 | -10.08 | -75.60 | — |
| 2026-09-16 | `WAY` | 241 | — | $26.27 | +0.00 | $26.59 | +77.12 | +77.12 | +0.00 | +77.12 |
| 2026-09-16 | `QCOM` | 4 | — | $189.17 | +0.00 | $184.84 | -17.32 | -17.32 | +0.00 | -17.32 |
| 2026-09-16 | `SM` | 22 | — | $39.99 | +0.00 | $38.16 | -40.26 | -40.26 | +0.00 | -40.26 |
| 2026-09-16 | `CLS` | 2 | — | $320.20 | +0.00 | $323.83 | +7.26 | +7.26 | +0.00 | +7.26 |
| 2026-09-17 | `WAY` | 241 | $26.59 | $26.51 | -19.28 | $26.51 | +0.00 | -19.28 | +57.84 | +57.84 |
| 2026-09-17 | `QCOM` | 4 | $184.84 | $190.35 | +22.04 | $188.71 | -6.56 | +15.48 | +4.72 | -1.84 |
| 2026-09-17 | `SM` | 22 | $38.16 | $37.57 | -12.98 | $36.97 | -13.20 | -26.18 | -53.24 | -66.44 |
| 2026-09-17 | `CLS` | 2 | $323.83 | $337.75 | +27.84 | $329.94 | -15.62 | +12.22 | +35.10 | +19.48 |
| 2026-09-17 | `SMTC` | 1 | — | $170.85 | +0.00 | $178.19 | +7.34 | +7.34 | +0.00 | +7.34 |
| 2026-09-17 | `GME` | 2 | — | $22.12 | +0.00 | $22.77 | +1.30 | +1.30 | +0.00 | +1.30 |
| 2026-09-18 | `WAY` | 241 | $26.51 | $26.95 | +106.04 | $25.66 | -310.89 | -204.85 | +163.88 | -147.01 |
| 2026-09-18 | `QCOM` | 4 | $188.71 | $191.34 | +10.52 | $177.72 | -54.48 | -43.96 | +8.68 | -45.80 |
| 2026-09-18 | `SM` | 22 | $36.97 | $36.87 | -2.20 | $36.97 | +2.20 | +0.00 | -68.64 | -66.44 |
| 2026-09-18 | `CLS` | 2 | $329.94 | $332.06 | +4.24 | $332.63 | +1.14 | +5.38 | +23.72 | +24.86 |
| 2026-09-18 | `SMTC` | 1 | $178.19 | $182.33 | +4.14 | $185.00 | +2.67 | +6.81 | +11.48 | +14.15 |
| 2026-09-18 | `GME` | 2 | $22.77 | $22.90 | +0.26 | $22.64 | -0.52 | -0.26 | +1.56 | +1.04 |
| 2026-09-18 | `TH` | 7 | — | $20.91 | +0.00 | $21.19 | +1.96 | +1.96 | +0.00 | +1.96 |
| 2026-09-18 | `RARE` | 4 | — | $14.79 | +0.00 | $14.51 | -1.12 | -1.12 | +0.00 | -1.12 |
| 2026-09-21 | `WAY` | 241 | $25.66 | $25.94 | +67.48 | — | +0.00 | +67.48 | -79.53 | — |
| 2026-09-21 | `QCOM` | 4 | $177.72 | $180.61 | +11.56 | — | +0.00 | +11.56 | -34.24 | — |
| 2026-09-21 | `SM` | 22 | $36.97 | $35.91 | -23.32 | — | +0.00 | -23.32 | -89.76 | — |
| 2026-09-21 | `CLS` | 2 | $332.63 | $341.45 | +17.64 | — | +0.00 | +17.64 | +42.50 | — |
| 2026-09-21 | `SMTC` | 1 | $185.00 | $190.30 | +5.30 | $177.37 | -12.93 | -7.63 | +19.45 | +6.52 |
| 2026-09-21 | `GME` | 2 | $22.64 | $22.78 | +0.28 | $22.76 | -0.04 | +0.24 | +1.32 | +1.28 |
| 2026-09-21 | `TH` | 7 | $21.19 | $21.65 | +3.22 | $21.28 | -2.59 | +0.63 | +5.18 | +2.59 |
| 2026-09-21 | `RARE` | 4 | $14.51 | $14.58 | +0.28 | $14.65 | +0.28 | +0.56 | -0.84 | -0.56 |
| 2026-09-21 | `VICR` | 25 | — | $230.25 | +0.00 | $223.90 | -158.75 | -158.75 | +0.00 | -158.75 |
| 2026-09-21 | `ALVO` | 213 | — | $5.92 | +0.00 | $5.88 | -8.52 | -8.52 | +0.00 | -8.52 |
| 2026-09-21 | `SION` | 211 | — | $6.00 | +0.00 | $6.00 | +0.00 | +0.00 | +0.00 | +0.00 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +407.37 | HLIT, ANGX, ARX | — | $991.44 | $10,395.38 | HLIT×531, ANGX×232, ARX×51 |
| 2026-08-17 | +2.25 | $991.44 | HLIT×531, ANGX×232, ARX×51 | $10,405.75 | +10.37 | -177.58 | DVN, EOG, OUST | — | $233.47 | $10,223.18 | HLIT×531, ANGX×232, ARX×51, DVN×8, EOG×2, OUST×2 |
| 2026-08-18 | -6.20 | $233.47 | HLIT×531, ANGX×232, ARX×51, DVN×8, EOG×2, OUST×2 | $9,978.91 | -244.27 | -97.03 | — | — | $233.47 | $9,881.88 | HLIT×531, ANGX×232, ARX×51, DVN×8, EOG×2, OUST×2 |
| 2026-08-19 | -7.20 | $233.47 | HLIT×531, ANGX×232, ARX×51, DVN×8, EOG×2, OUST×2 | $9,964.71 | +82.83 | -50.96 | — | HLIT, ARX | $8,072.79 | $9,904.59 | ANGX×232, DVN×8, EOG×2, OUST×2 |
| 2026-08-20 | +1.12 | $8,072.79 | ANGX×232, DVN×8, EOG×2, OUST×2 | $9,909.35 | +4.76 | +177.22 | BHP, APA, AUTL, CRSP | ANGX, DVN, EOG, OUST | $60.79 | $10,067.17 | BHP×76, APA×22, AUTL×400, CRSP×16 |
| 2026-08-21 | +3.25 | $60.79 | BHP×76, APA×22, AUTL×400, CRSP×16 | $10,258.47 | +191.30 | +47.18 | — | — | $60.79 | $10,305.65 | BHP×76, APA×22, AUTL×400, CRSP×16 |
| 2026-08-24 | -5.17 | $60.79 | BHP×76, APA×22, AUTL×400, CRSP×16 | $10,300.81 | -4.84 | -63.82 | — | — | $60.79 | $10,236.99 | BHP×76, APA×22, AUTL×400, CRSP×16 |
| 2026-08-25 | +1.80 | $60.79 | BHP×76, APA×22, AUTL×400, CRSP×16 | $10,135.39 | -101.60 | +415.18 | AU, FCX, EZPW, RUM | BHP, APA, AUTL, CRSP | $130.44 | $10,530.33 | AU×59, FCX×13, EZPW×28, RUM×107 |
| 2026-08-26 | +2.02 | $130.44 | AU×59, FCX×13, EZPW×28, RUM×107 | $10,307.15 | -223.18 | -227.62 | TRLV, CAPR | — | $110.73 | $10,079.33 | AU×59, FCX×13, EZPW×28, RUM×107, TRLV×1, CAPR×1 |
| 2026-08-27 | — | $110.73 | AU×59, FCX×13, EZPW×28, RUM×107, TRLV×1, CAPR×1 | $10,038.85 | -40.48 | +70.52 | — | — | $110.73 | $10,109.37 | AU×59, FCX×13, EZPW×28, RUM×107, TRLV×1, CAPR×1 |
| 2026-08-28 | +0.75 | $110.73 | AU×59, FCX×13, EZPW×28, RUM×107, TRLV×1, CAPR×1 | $10,146.18 | +36.81 | -210.65 | KEYS, SMTC, CIEN | AU, FCX, EZPW, RUM | $1,504.90 | $9,920.75 | TRLV×1, CAPR×1, KEYS×21, SMTC×7, CIEN×2 |
| 2026-08-31 | -5.85 | $1,504.90 | TRLV×1, CAPR×1, KEYS×21, SMTC×7, CIEN×2 | $9,981.47 | +60.72 | +17.75 | — | TRLV, CAPR | $1,525.95 | $9,998.97 | KEYS×21, SMTC×7, CIEN×2 |
| 2026-09-01 | -6.30 | $1,525.95 | KEYS×21, SMTC×7, CIEN×2 | $9,924.01 | -74.96 | -46.84 | — | — | $1,525.95 | $9,877.17 | KEYS×21, SMTC×7, CIEN×2 |
| 2026-09-02 | -3.83 | $1,525.95 | KEYS×21, SMTC×7, CIEN×2 | $9,850.29 | -26.88 | +0.00 | — | KEYS, SMTC, CIEN | $9,844.12 | $9,844.12 | — |
| 2026-09-03 | -0.90 | $9,844.12 | — | $9,844.12 | +0.00 | +267.50 | AVGO, DELL, CXW, FRNM | — | $226.90 | $10,103.32 | AVGO×19, DELL×2, CXW×30, FRNM×62 |
| 2026-09-04 | +2.25 | $226.90 | AVGO×19, DELL×2, CXW×30, FRNM×62 | $10,109.36 | +6.04 | +18.44 | — | — | $226.90 | $10,127.80 | AVGO×19, DELL×2, CXW×30, FRNM×62 |
| 2026-09-08 | -11.47 | $226.90 | AVGO×19, DELL×2, CXW×30, FRNM×62 | $10,251.70 | +123.90 | +88.48 | — | — | $226.90 | $10,340.18 | AVGO×19, DELL×2, CXW×30, FRNM×62 |
| 2026-09-09 | -13.95 | $226.90 | AVGO×19, DELL×2, CXW×30, FRNM×62 | $10,304.43 | -35.75 | +0.00 | — | AVGO, DELL, CXW, FRNM | $10,296.01 | $10,296.01 | — |
| 2026-09-10 | -13.28 | $10,296.01 | — | $10,296.01 | -0.00 | +0.00 | — | — | $10,296.01 | $10,296.01 | — |
| 2026-09-11 | +0.50 | $10,296.01 | — | $10,296.01 | -0.00 | -602.73 | ORCL, ADBE, BAK, AMTX | — | $183.60 | $9,676.40 | ORCL×43, ADBE×4, BAK×485, AMTX×504 |
| 2026-09-14 | -11.00 | $183.60 | ORCL×43, ADBE×4, BAK×485, AMTX×504 | $9,317.99 | -358.41 | +111.63 | — | — | $183.60 | $9,429.62 | ORCL×43, ADBE×4, BAK×485, AMTX×504 |
| 2026-09-15 | -3.84 | $183.60 | ORCL×43, ADBE×4, BAK×485, AMTX×504 | $9,351.60 | -78.02 | -120.77 | — | — | $183.60 | $9,230.83 | ORCL×43, ADBE×4, BAK×485, AMTX×504 |
| 2026-09-16 | +5.30 | $183.60 | ORCL×43, ADBE×4, BAK×485, AMTX×504 | $9,063.21 | -167.62 | +26.80 | WAY, QCOM, SM, CLS | ORCL, ADBE, BAK, AMTX | $428.97 | $9,063.70 | WAY×241, QCOM×4, SM×22, CLS×2 |
| 2026-09-17 | +7.38 | $428.97 | WAY×241, QCOM×4, SM×22, CLS×2 | $9,081.32 | +17.62 | -26.74 | SMTC, GME | — | $211.72 | $9,052.42 | WAY×241, QCOM×4, SM×22, CLS×2, SMTC×1, GME×2 |
| 2026-09-18 | +4.86 | $211.72 | WAY×241, QCOM×4, SM×22, CLS×2, SMTC×1, GME×2 | $9,175.42 | +123.00 | -359.04 | TH, RARE | — | $4.11 | $8,814.30 | WAY×241, QCOM×4, SM×22, CLS×2, SMTC×1, GME×2, TH×7, RARE×4 |
| 2026-09-21 | +12.87 | $4.11 | WAY×241, QCOM×4, SM×22, CLS×2, SMTC×1, GME×2, TH×7, RARE×4 | $8,896.74 | +82.44 | -182.55 | VICR, ALVO, SION | WAY, QCOM, SM, CLS | $150.95 | $8,697.34 | SMTC×1, GME×2, TH×7, RARE×4, VICR×25, ALVO×213, SION×211 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 531 | $13.18 | $6.85 | — | $2,994.57 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $7000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 232 | $4.31 | $2.99 | — | $1,991.66 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 51 | $19.57 | $2.14 | — | $991.44 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $991.44 | ▲ close $10,395.38 vs 09:30 $10,000.00 (session +407.37) | 16:00 close · cash $991.44 · equity $10,395.38 vs 09:30 $10,000.00 (+395.38; session marks +407.37) · 3 name(s) marked open→close (per-name table). HLIT×531 09:30 $13.18 → close $13.92 +392.94; ANGX×232 09:30 $4.31 → close $4.37 +13.92; ARX×51 09:30 $19.57 → close $19.58 +0.51 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $991.44 | ▲ 09:30 equity $10,405.75 vs yday $10,395.38 (+10.37) | 09:30 open · cash $991.44 (unchanged overnight, no fees) · equity $10,405.75 vs prior close $10,395.38 (+10.37) · 3 name(s) re-marked at the open (per-name table). HLIT×531 yday $13.92 → 09:30 $13.84 -42.48; ANGX×232 yday $4.37 → 09:30 $4.60 +53.36; ARX×51 yday $19.58 → 09:30 $19.57 -0.51 | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 8 | $46.18 | $2.01 | — | $619.99 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten; 🔵; ret5=+6.7; leftover $396.58 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 2 | $142.77 | $2.00 | — | $332.45 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten; 🔵; ret5=+5.8; leftover $297.43 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 2 | $49.00 | $0.99 | — | $233.47 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $99.14 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $233.47 | ▼ close $10,223.18 vs 09:30 $10,405.75 (session -177.58) | 16:00 close · cash $233.47 · equity $10,223.18 vs 09:30 $10,405.75 (-182.57; session marks -177.58) · 6 name(s) marked open→close (per-name table). HLIT×531 09:30 $13.84 → close $13.43 -217.71; ANGX×232 09:30 $4.60 → close $4.71 +25.52; ARX×51 09:30 $19.57 → close $19.54 -1.53; DVN×8 09:30 $46.18 → close $47.57 +11.12; EOG×2 09:30 $142.77 → close $146.15 +6.76; OUST×2 09:30 $49.00 → close $48.13 -1.74 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $233.47 | ▼ 09:30 equity $9,978.91 vs yday $10,223.18 (-244.27) | 09:30 open · cash $233.47 (unchanged overnight, no fees) · equity $9,978.91 vs prior close $10,223.18 (-244.27) · 6 name(s) re-marked at the open (per-name table). HLIT×531 yday $13.43 → 09:30 $12.93 -265.50; ANGX×232 yday $4.71 → 09:30 $4.79 +18.56; ARX×51 yday $19.54 → 09:30 $19.57 +1.53; DVN×8 yday $47.57 → 09:30 $48.00 +3.44; EOG×2 yday $146.15 → 09:30 $148.04 +3.78; OUST×2 yday $48.13 → 09:30 $45.09 -6.08 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $233.47 | ▼ close $9,881.88 vs 09:30 $9,978.91 (session -97.03) | 16:00 close · cash $233.47 · equity $9,881.88 vs 09:30 $9,978.91 (-97.03; session marks -97.03) · 6 name(s) marked open→close (per-name table). HLIT×531 09:30 $12.93 → close $12.73 -106.20; ANGX×232 09:30 $4.79 → close $4.85 +13.92; ARX×51 09:30 $19.57 → close $19.56 -0.51; DVN×8 09:30 $48.00 → close $47.83 -1.36; EOG×2 09:30 $148.04 → close $148.70 +1.32; OUST×2 09:30 $45.09 → close $42.99 -4.20 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $233.47 | ▲ 09:30 equity $9,964.71 vs yday $9,881.88 (+82.83) | 09:30 open · cash $233.47 (unchanged overnight, no fees) · equity $9,964.71 vs prior close $9,881.88 (+82.83) · 6 name(s) re-marked at the open (per-name table). HLIT×531 yday $12.73 → 09:30 $12.90 +90.27; ANGX×232 yday $4.85 → 09:30 $4.79 -13.92; ARX×51 yday $19.56 → 09:30 $19.58 +1.02; DVN×8 yday $47.83 → 09:30 $48.22 +3.12; EOG×2 yday $148.70 → 09:30 $149.86 +2.32; OUST×2 yday $42.99 → 09:30 $43.00 +0.02 | — |
| 2026-08-19 09:30 ET | **SELL** | `HLIT` | 531 | $12.90 | $6.99 | $-162.52 | $7,076.38 | ▼ -162.52 after sell → book $9,957.72; vs 09:30 mark -6.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 51 | $19.58 | $2.16 | $-3.80 | $8,072.79 | ▼ -3.80 after sell → book $9,955.55; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,072.79 | ▼ close $9,904.59 vs 09:30 $9,964.71 (session -50.96) | 16:00 close · cash $8,072.79 · equity $9,904.59 vs 09:30 $9,964.71 (-60.12; session marks -50.96) · 4 name(s) marked open→close (per-name table). ANGX×232 09:30 $4.79 → close $4.60 -44.08; DVN×8 09:30 $48.22 → close $48.19 -0.24; EOG×2 09:30 $149.86 → close $149.48 -0.76; OUST×2 09:30 $43.00 → close $40.06 -5.88 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,072.79 | ▲ 09:30 equity $9,909.35 vs yday $9,904.59 (+4.76) | 09:30 open · cash $8,072.79 (unchanged overnight, no fees) · equity $9,909.35 vs prior close $9,904.59 (+4.76) · 4 name(s) re-marked at the open (per-name table). ANGX×232 yday $4.60 → 09:30 $4.57 -6.96; DVN×8 yday $48.19 → 09:30 $49.02 +6.64; EOG×2 yday $149.48 → 09:30 $151.45 +3.94; OUST×2 yday $40.06 → 09:30 $40.63 +1.14 | — |
| 2026-08-20 09:30 ET | **SELL** | `ANGX` | 232 | $4.57 | $3.04 | $+54.29 | $9,129.99 | ▲ +54.29 after sell → book $9,906.31; vs 09:30 mark -3.04 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 8 | $49.02 | $2.03 | $+18.67 | $9,520.12 | ▲ +18.67 after sell → book $9,904.28; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `EOG` | 2 | $151.45 | $2.02 | $+13.35 | $9,821.00 | ▲ +13.35 after sell → book $9,902.26; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `OUST` | 2 | $40.63 | $0.84 | $-18.56 | $9,901.42 | ▼ -18.56 after sell → book $9,901.42; vs 09:30 mark -0.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 76 | $91.01 | $2.22 | — | $2,982.44 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $6931.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 22 | $44.76 | $2.06 | — | $1,995.67 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $990.14 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 400 | $2.47 | $5.16 | — | $1,002.51 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $990.14 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 16 | $58.73 | $2.04 | — | $60.79 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $990.14 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.79 | ▲ close $10,067.17 vs 09:30 $9,909.35 (session +177.22) | 16:00 close · cash $60.79 · equity $10,067.17 vs 09:30 $9,909.35 (+157.82; session marks +177.22) · 4 name(s) marked open→close (per-name table). BHP×76 09:30 $91.01 → close $93.63 +199.12; APA×22 09:30 $44.76 → close $44.39 -8.14; AUTL×400 09:30 $2.47 → close $2.46 -4.00; CRSP×16 09:30 $58.73 → close $58.12 -9.76 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.79 | ▲ 09:30 equity $10,258.47 vs yday $10,067.17 (+191.30) | 09:30 open · cash $60.79 (unchanged overnight, no fees) · equity $10,258.47 vs prior close $10,067.17 (+191.30) · 4 name(s) re-marked at the open (per-name table). BHP×76 yday $93.63 → 09:30 $95.72 +158.84; APA×22 yday $44.39 → 09:30 $44.52 +2.86; AUTL×400 yday $2.46 → 09:30 $2.47 +4.00; CRSP×16 yday $58.12 → 09:30 $59.72 +25.60 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.79 | ▲ close $10,305.65 vs 09:30 $10,258.47 (session +47.18) | 16:00 close · cash $60.79 · equity $10,305.65 vs 09:30 $10,258.47 (+47.18; session marks +47.18) · 4 name(s) marked open→close (per-name table). BHP×76 09:30 $95.72 → close $97.03 +99.56; APA×22 09:30 $44.52 → close $43.39 -24.86; AUTL×400 09:30 $2.47 → close $2.41 -24.00; CRSP×16 09:30 $59.72 → close $59.50 -3.52 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.79 | ▼ 09:30 equity $10,300.81 vs yday $10,305.65 (-4.84) | 09:30 open · cash $60.79 (unchanged overnight, no fees) · equity $10,300.81 vs prior close $10,305.65 (-4.84) · 4 name(s) re-marked at the open (per-name table). BHP×76 yday $97.03 → 09:30 $97.31 +21.28; APA×22 yday $43.39 → 09:30 $42.93 -10.12; AUTL×400 yday $2.41 → 09:30 $2.40 -4.00; CRSP×16 yday $59.50 → 09:30 $58.75 -12.00 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.79 | ▼ close $10,236.99 vs 09:30 $10,300.81 (session -63.82) | 16:00 close · cash $60.79 · equity $10,236.99 vs 09:30 $10,300.81 (-63.82; session marks -63.82) · 4 name(s) marked open→close (per-name table). BHP×76 09:30 $97.31 → close $97.13 -13.68; APA×22 09:30 $42.93 → close $42.96 +0.66; AUTL×400 09:30 $2.40 → close $2.34 -24.00; CRSP×16 09:30 $58.75 → close $57.08 -26.80 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.79 | ▼ 09:30 equity $10,135.39 vs yday $10,236.99 (-101.60) | 09:30 open · cash $60.79 (unchanged overnight, no fees) · equity $10,135.39 vs prior close $10,236.99 (-101.60) · 4 name(s) re-marked at the open (per-name table). BHP×76 yday $97.13 → 09:30 $95.86 -96.52; APA×22 yday $42.96 → 09:30 $41.38 -34.76; AUTL×400 yday $2.34 → 09:30 $2.38 +16.00; CRSP×16 yday $57.08 → 09:30 $57.93 +13.68 | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 76 | $95.86 | $2.29 | $+364.09 | $7,343.86 | ▲ +364.09 after sell → book $10,133.10; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 22 | $41.38 | $2.08 | $-78.49 | $8,252.15 | ▼ -78.49 after sell → book $10,131.03; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 400 | $2.38 | $5.24 | $-46.40 | $9,198.91 | ▼ -46.40 after sell → book $10,125.79; vs 09:30 mark -5.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 16 | $57.93 | $2.06 | $-16.90 | $10,123.73 | ▼ -16.90 after sell → book $10,123.73; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 59 | $118.52 | $2.17 | — | $3,128.88 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $7086.61 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 13 | $77.13 | $2.03 | — | $2,124.17 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1012.37 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 28 | $35.05 | $2.07 | — | $1,140.69 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1012.37 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 107 | $9.42 | $2.31 | — | $130.44 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1012.37 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $130.44 | ▲ close $10,530.33 vs 09:30 $10,135.39 (session +415.18) | 16:00 close · cash $130.44 · equity $10,530.33 vs 09:30 $10,135.39 (+394.94; session marks +415.18) · 4 name(s) marked open→close (per-name table). AU×59 09:30 $118.52 → close $123.39 +287.33; FCX×13 09:30 $77.13 → close $79.91 +36.14; EZPW×28 09:30 $35.05 → close $35.23 +5.04; RUM×107 09:30 $9.42 → close $10.23 +86.67 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $130.44 | ▼ 09:30 equity $10,307.15 vs yday $10,530.33 (-223.18) | 09:30 open · cash $130.44 (unchanged overnight, no fees) · equity $10,307.15 vs prior close $10,530.33 (-223.18) · 4 name(s) re-marked at the open (per-name table). AU×59 yday $123.39 → 09:30 $119.80 -211.81; FCX×13 yday $79.91 → 09:30 $79.34 -7.41; EZPW×28 yday $35.23 → 09:30 $35.70 +13.16; RUM×107 yday $10.23 → 09:30 $10.07 -17.12 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 1 | $11.22 | $0.12 | — | $119.11 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+16.8; leftover $13.04 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 1 | $8.29 | $0.09 | — | $110.73 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $13.04 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.73 | ▼ close $10,079.33 vs 09:30 $10,307.15 (session -227.62) | 16:00 close · cash $110.73 · equity $10,079.33 vs 09:30 $10,307.15 (-227.82; session marks -227.62) · 6 name(s) marked open→close (per-name table). AU×59 09:30 $119.80 → close $118.11 -99.71; FCX×13 09:30 $79.34 → close $79.00 -4.42; EZPW×28 09:30 $35.70 → close $33.90 -50.40; RUM×107 09:30 $10.07 → close $9.38 -74.37; TRLV×1 09:30 $11.22 → close $11.43 +0.21; CAPR×1 09:30 $8.29 → close $9.36 +1.07 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $110.73 | ▼ 09:30 equity $10,038.85 vs yday $10,079.33 (-40.48) | 09:30 open · cash $110.73 (unchanged overnight, no fees) · equity $10,038.85 vs prior close $10,079.33 (-40.48) · 6 name(s) re-marked at the open (per-name table). AU×59 yday $118.11 → 09:30 $117.41 -41.30; FCX×13 yday $79.00 → 09:30 $78.83 -2.21; EZPW×28 yday $33.90 → 09:30 $33.50 -11.20; RUM×107 yday $9.38 → 09:30 $9.51 +14.44; TRLV×1 yday $11.43 → 09:30 $11.38 -0.05; CAPR×1 yday $9.36 → 09:30 $9.19 -0.17 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.73 | ▲ close $10,109.37 vs 09:30 $10,038.85 (session +70.52) | 16:00 close · cash $110.73 · equity $10,109.37 vs 09:30 $10,038.85 (+70.52; session marks +70.52) · 6 name(s) marked open→close (per-name table). AU×59 09:30 $117.41 → close $118.40 +58.41; FCX×13 09:30 $78.83 → close $78.42 -5.33; EZPW×28 09:30 $33.50 → close $34.41 +25.48; RUM×107 09:30 $9.51 → close $9.43 -8.56; TRLV×1 09:30 $11.38 → close $11.03 -0.35; CAPR×1 09:30 $9.19 → close $10.06 +0.87 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $110.73 | ▲ 09:30 equity $10,146.18 vs yday $10,109.37 (+36.81) | 09:30 open · cash $110.73 (unchanged overnight, no fees) · equity $10,146.18 vs prior close $10,109.37 (+36.81) · 6 name(s) re-marked at the open (per-name table). AU×59 yday $118.40 → 09:30 $119.19 +46.61; FCX×13 yday $78.42 → 09:30 $78.57 +1.95; EZPW×28 yday $34.41 → 09:30 $34.50 +2.52; RUM×107 yday $9.43 → 09:30 $9.30 -13.91; TRLV×1 yday $11.03 → 09:30 $11.00 -0.03; CAPR×1 yday $10.06 → 09:30 $9.73 -0.33 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 59 | $119.19 | $2.23 | $+35.13 | $7,140.71 | ▲ +35.13 after sell → book $10,143.95; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 13 | $78.57 | $2.05 | $+14.64 | $8,160.07 | ▲ +14.64 after sell → book $10,141.90; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 28 | $34.50 | $2.09 | $-19.57 | $9,123.97 | ▼ -19.57 after sell → book $10,139.80; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 107 | $9.30 | $2.34 | $-17.49 | $10,116.73 | ▼ -17.49 after sell → book $10,137.46; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 21 | $324.41 | $2.05 | — | $3,302.07 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $7081.71 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 7 | $141.76 | $2.01 | — | $2,307.74 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1011.67 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 2 | $400.42 | $2.00 | — | $1,504.90 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1011.67 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,504.90 | ▼ close $9,920.75 vs 09:30 $10,146.18 (session -210.65) | 16:00 close · cash $1,504.90 · equity $9,920.75 vs 09:30 $10,146.18 (-225.43; session marks -210.65) · 5 name(s) marked open→close (per-name table). TRLV×1 09:30 $11.00 → close $11.82 +0.82; CAPR×1 09:30 $9.73 → close $9.59 -0.14; KEYS×21 09:30 $324.41 → close $319.97 -93.24; SMTC×7 09:30 $141.76 → close $131.17 -74.13; CIEN×2 09:30 $400.42 → close $378.44 -43.96 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,504.90 | ▲ 09:30 equity $9,981.47 vs yday $9,920.75 (+60.72) | 09:30 open · cash $1,504.90 (unchanged overnight, no fees) · equity $9,981.47 vs prior close $9,920.75 (+60.72) · 5 name(s) re-marked at the open (per-name table). TRLV×1 yday $11.82 → 09:30 $11.80 -0.02; CAPR×1 yday $9.59 → 09:30 $9.50 -0.09; KEYS×21 yday $319.97 → 09:30 $322.49 +52.92; SMTC×7 yday $131.17 → 09:30 $132.30 +7.91; CIEN×2 yday $378.44 → 09:30 $378.44 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 1 | $11.80 | $0.14 | $+0.32 | $1,516.56 | ▲ +0.32 after sell → book $9,981.33; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 1 | $9.50 | $0.12 | $+1.01 | $1,525.95 | ▲ +1.01 after sell → book $9,981.22; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,525.95 | ▲ close $9,998.97 vs 09:30 $9,981.47 (session +17.75) | 16:00 close · cash $1,525.95 · equity $9,998.97 vs 09:30 $9,981.47 (+17.50; session marks +17.75) · 3 name(s) marked open→close (per-name table). KEYS×21 09:30 $322.49 → close $322.70 +4.41; SMTC×7 09:30 $132.30 → close $132.96 +4.62; CIEN×2 09:30 $378.44 → close $382.80 +8.72 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,525.95 | ▼ 09:30 equity $9,924.01 vs yday $9,998.97 (-74.96) | 09:30 open · cash $1,525.95 (unchanged overnight, no fees) · equity $9,924.01 vs prior close $9,998.97 (-74.96) · 3 name(s) re-marked at the open (per-name table). KEYS×21 yday $322.70 → 09:30 $321.47 -25.83; SMTC×7 yday $132.96 → 09:30 $127.63 -37.31; CIEN×2 yday $382.80 → 09:30 $376.89 -11.82 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,525.95 | ▼ close $9,877.17 vs 09:30 $9,924.01 (session -46.84) | 16:00 close · cash $1,525.95 · equity $9,877.17 vs 09:30 $9,924.01 (-46.84; session marks -46.84) · 3 name(s) marked open→close (per-name table). KEYS×21 09:30 $321.47 → close $319.27 -46.20; SMTC×7 09:30 $127.63 → close $132.27 +32.48; CIEN×2 09:30 $376.89 → close $360.33 -33.12 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,525.95 | ▼ 09:30 equity $9,850.29 vs yday $9,877.17 (-26.88) | 09:30 open · cash $1,525.95 (unchanged overnight, no fees) · equity $9,850.29 vs prior close $9,877.17 (-26.88) · 3 name(s) re-marked at the open (per-name table). KEYS×21 yday $319.27 → 09:30 $318.04 -25.83; SMTC×7 yday $132.27 → 09:30 $133.00 +5.11; CIEN×2 yday $360.33 → 09:30 $357.25 -6.16 | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 21 | $318.04 | $2.12 | $-137.94 | $8,202.67 | ▼ -137.94 after sell → book $9,848.17; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 7 | $133.00 | $2.03 | $-65.36 | $9,131.64 | ▼ -65.36 after sell → book $9,846.14; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 2 | $357.25 | $2.02 | $-90.35 | $9,844.12 | ▼ -90.35 after sell → book $9,844.12; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,844.12 | ▲ close $9,844.12 vs 09:30 $9,850.29 (session +0.00) | 16:00 close · cash $9,844.12 · no lots left · equity $9,844.12. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,844.12 | ▲ 09:30 equity $9,844.12 vs yday $9,844.12 (+0.00) | 09:30 open · cash $9,844.12 · no holdings · equity $9,844.12 vs prior close $9,844.12 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 19 | $351.74 | $2.05 | — | $3,159.01 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $6890.89 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $2,184.40 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $984.41 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 30 | $32.31 | $2.08 | — | $1,213.02 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $984.41 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 62 | $15.87 | $2.18 | — | $226.90 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $984.41 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $226.90 | ▲ close $10,103.32 vs 09:30 $9,844.12 (session +267.50) | 16:00 close · cash $226.90 · equity $10,103.32 vs 09:30 $9,844.12 (+259.20; session marks +267.50) · 4 name(s) marked open→close (per-name table). AVGO×19 09:30 $351.74 → close $357.16 +102.98; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×30 09:30 $32.31 → close $33.66 +40.50; FRNM×62 09:30 $15.87 → close $16.90 +63.86 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $226.90 | ▲ 09:30 equity $10,109.36 vs yday $10,103.32 (+6.04) | 09:30 open · cash $226.90 (unchanged overnight, no fees) · equity $10,109.36 vs prior close $10,103.32 (+6.04) · 4 name(s) re-marked at the open (per-name table). AVGO×19 yday $357.16 → 09:30 $359.70 +48.26; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×30 yday $33.66 → 09:30 $33.46 -6.00; FRNM×62 yday $16.90 → 09:30 $16.40 -31.00 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $226.90 | ▲ close $10,127.80 vs 09:30 $10,109.36 (session +18.44) | 16:00 close · cash $226.90 · equity $10,127.80 vs 09:30 $10,109.36 (+18.44; session marks +18.44) · 4 name(s) marked open→close (per-name table). AVGO×19 09:30 $359.70 → close $357.90 -34.20; DELL×2 09:30 $513.78 → close $524.14 +20.72; CXW×30 09:30 $33.46 → close $34.71 +37.50; FRNM×62 09:30 $16.40 → close $16.31 -5.58 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $226.90 | ▲ 09:30 equity $10,251.70 vs yday $10,127.80 (+123.90) | 09:30 open · cash $226.90 (unchanged overnight, no fees) · equity $10,251.70 vs prior close $10,127.80 (+123.90) · 4 name(s) re-marked at the open (per-name table). AVGO×19 yday $357.90 → 09:30 $363.68 +109.82; DELL×2 yday $524.14 → 09:30 $521.15 -5.98; CXW×30 yday $34.71 → 09:30 $34.49 -6.60; FRNM×62 yday $16.31 → 09:30 $16.74 +26.66 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $226.90 | ▲ close $10,340.18 vs 09:30 $10,251.70 (session +88.48) | 16:00 close · cash $226.90 · equity $10,340.18 vs 09:30 $10,251.70 (+88.48; session marks +88.48) · 4 name(s) marked open→close (per-name table). AVGO×19 09:30 $363.68 → close $368.56 +92.72; DELL×2 09:30 $521.15 → close $533.88 +25.46; CXW×30 09:30 $34.49 → close $35.05 +16.80; FRNM×62 09:30 $16.74 → close $15.99 -46.50 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $226.90 | ▼ 09:30 equity $10,304.43 vs yday $10,340.18 (-35.75) | 09:30 open · cash $226.90 (unchanged overnight, no fees) · equity $10,304.43 vs prior close $10,340.18 (-35.75) · 4 name(s) re-marked at the open (per-name table). AVGO×19 yday $368.56 → 09:30 $366.23 -44.27; DELL×2 yday $533.88 → 09:30 $538.47 +9.18; CXW×30 yday $35.05 → 09:30 $35.09 +1.20; FRNM×62 yday $15.99 → 09:30 $15.96 -1.86 | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 19 | $366.23 | $2.11 | $+271.15 | $7,183.16 | ▲ +271.15 after sell → book $10,302.32; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 2 | $538.47 | $2.02 | $+100.31 | $8,258.08 | ▲ +100.31 after sell → book $10,300.30; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CXW` | 30 | $35.09 | $2.10 | $+79.22 | $9,308.68 | ▲ +79.22 after sell → book $10,298.20; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 62 | $15.96 | $2.20 | $+1.21 | $10,296.01 | ▲ +1.21 after sell → book $10,296.01; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,296.01 | ▲ close $10,296.01 vs 09:30 $10,304.43 (session +0.00) | 16:00 close · cash $10,296.01 · no lots left · equity $10,296.01. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,296.01 | ▲ 09:30 equity $10,296.01 vs yday $10,296.01 (-0.00) | 09:30 open · cash $10,296.01 · no holdings · equity $10,296.01 vs prior close $10,296.01 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,296.01 | ▲ close $10,296.01 vs 09:30 $10,296.01 (session +0.00) | 16:00 close · cash $10,296.01 · no lots left · equity $10,296.01. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,296.01 | ▲ 09:30 equity $10,296.01 vs yday $10,296.01 (-0.00) | 09:30 open · cash $10,296.01 · no holdings · equity $10,296.01 vs prior close $10,296.01 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 43 | $164.43 | $2.12 | — | $3,223.40 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $7207.21 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 4 | $242.17 | $2.00 | — | $2,252.72 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list earn_react; ret5=-11.1; leftover $1029.60 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 485 | $2.12 | $6.26 | — | $1,218.26 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1029.60 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 504 | $2.04 | $6.50 | — | $183.60 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1029.60 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $183.60 | ▼ close $9,676.40 vs 09:30 $10,296.01 (session -602.73) | 16:00 close · cash $183.60 · equity $9,676.40 vs 09:30 $10,296.01 (-619.61; session marks -602.73) · 4 name(s) marked open→close (per-name table). ORCL×43 09:30 $164.43 → close $150.28 -608.45; ADBE×4 09:30 $242.17 → close $252.23 +40.24; BAK×485 09:30 $2.12 → close $2.08 -19.40; AMTX×504 09:30 $2.04 → close $2.01 -15.12 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $183.60 | ▼ 09:30 equity $9,317.99 vs yday $9,676.40 (-358.41) | 09:30 open · cash $183.60 (unchanged overnight, no fees) · equity $9,317.99 vs prior close $9,676.40 (-358.41) · 4 name(s) re-marked at the open (per-name table). ORCL×43 yday $150.28 → 09:30 $141.42 -380.98; ADBE×4 yday $252.23 → 09:30 $261.51 +37.12; BAK×485 yday $2.08 → 09:30 $2.05 -14.55; AMTX×504 yday $2.01 → 09:30 $2.01 +0.00 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $183.60 | ▲ close $9,429.62 vs 09:30 $9,317.99 (session +111.63) | 16:00 close · cash $183.60 · equity $9,429.62 vs 09:30 $9,317.99 (+111.63; session marks +111.63) · 4 name(s) marked open→close (per-name table). ORCL×43 09:30 $141.42 → close $144.79 +144.91; ADBE×4 09:30 $261.51 → close $265.60 +16.36; BAK×485 09:30 $2.05 → close $2.01 -19.40; AMTX×504 09:30 $2.01 → close $1.95 -30.24 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $183.60 | ▼ 09:30 equity $9,351.60 vs yday $9,429.62 (-78.02) | 09:30 open · cash $183.60 (unchanged overnight, no fees) · equity $9,351.60 vs prior close $9,429.62 (-78.02) · 4 name(s) re-marked at the open (per-name table). ORCL×43 yday $144.79 → 09:30 $143.46 -57.19; ADBE×4 yday $265.60 → 09:30 $261.70 -15.60; BAK×485 yday $2.01 → 09:30 $2.02 +4.85; AMTX×504 yday $1.95 → 09:30 $1.93 -10.08 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $183.60 | ▼ close $9,230.83 vs 09:30 $9,351.60 (session -120.77) | 16:00 close · cash $183.60 · equity $9,230.83 vs 09:30 $9,351.60 (-120.77; session marks -120.77) · 4 name(s) marked open→close (per-name table). ORCL×43 09:30 $143.46 → close $140.35 -133.73; ADBE×4 09:30 $261.70 → close $257.76 -15.76; BAK×485 09:30 $2.02 → close $2.10 +38.80; AMTX×504 09:30 $1.93 → close $1.91 -10.08 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $183.60 | ▼ 09:30 equity $9,063.21 vs yday $9,230.83 (-167.62) | 09:30 open · cash $183.60 (unchanged overnight, no fees) · equity $9,063.21 vs prior close $9,230.83 (-167.62) · 4 name(s) re-marked at the open (per-name table). ORCL×43 yday $140.35 → 09:30 $140.03 -13.76; ADBE×4 yday $257.76 → 09:30 $253.34 -17.68; BAK×485 yday $2.10 → 09:30 $1.84 -126.10; AMTX×504 yday $1.91 → 09:30 $1.89 -10.08 | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 43 | $140.03 | $2.18 | $-1053.50 | $6,202.71 | ▼ -1,053.50 after sell → book $9,061.03; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 4 | $253.34 | $2.02 | $+40.66 | $7,214.05 | ▲ +40.66 after sell → book $9,059.01; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAK` | 485 | $1.84 | $6.35 | $-148.40 | $8,100.10 | ▼ -148.40 after sell → book $9,052.66; vs 09:30 mark -6.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 504 | $1.89 | $6.60 | $-88.70 | $9,046.07 | ▼ -88.70 after sell → book $9,046.07; vs 09:30 mark -6.59 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 241 | $26.27 | $3.11 | — | $2,711.89 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $6332.25 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 4 | $189.17 | $2.00 | — | $1,953.21 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $904.61 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 22 | $39.99 | $2.06 | — | $1,071.37 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $904.61 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `CLS` | 2 | $320.20 | $2.00 | — | $428.97 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; ret5=+10.2; leftover $904.61 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $428.97 | ▲ close $9,063.70 vs 09:30 $9,063.21 (session +26.80) | 16:00 close · cash $428.97 · equity $9,063.70 vs 09:30 $9,063.21 (+0.49; session marks +26.80) · 4 name(s) marked open→close (per-name table). WAY×241 09:30 $26.27 → close $26.59 +77.12; QCOM×4 09:30 $189.17 → close $184.84 -17.32; SM×22 09:30 $39.99 → close $38.16 -40.26; CLS×2 09:30 $320.20 → close $323.83 +7.26 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $428.97 | ▲ 09:30 equity $9,081.32 vs yday $9,063.70 (+17.62) | 09:30 open · cash $428.97 (unchanged overnight, no fees) · equity $9,081.32 vs prior close $9,063.70 (+17.62) · 4 name(s) re-marked at the open (per-name table). WAY×241 yday $26.59 → 09:30 $26.51 -19.28; QCOM×4 yday $184.84 → 09:30 $190.35 +22.04; SM×22 yday $38.16 → 09:30 $37.57 -12.98; CLS×2 yday $323.83 → 09:30 $337.75 +27.84 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 1 | $170.85 | $1.71 | — | $256.41 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $300.28 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 2 | $22.12 | $0.45 | — | $211.72 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $64.35 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $211.72 | ▼ close $9,052.42 vs 09:30 $9,081.32 (session -26.74) | 16:00 close · cash $211.72 · equity $9,052.42 vs 09:30 $9,081.32 (-28.90; session marks -26.74) · 6 name(s) marked open→close (per-name table). WAY×241 09:30 $26.51 → close $26.51 +0.00; QCOM×4 09:30 $190.35 → close $188.71 -6.56; SM×22 09:30 $37.57 → close $36.97 -13.20; CLS×2 09:30 $337.75 → close $329.94 -15.62; SMTC×1 09:30 $170.85 → close $178.19 +7.34; GME×2 09:30 $22.12 → close $22.77 +1.30 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $211.72 | ▲ 09:30 equity $9,175.42 vs yday $9,052.42 (+123.00) | 09:30 open · cash $211.72 (unchanged overnight, no fees) · equity $9,175.42 vs prior close $9,052.42 (+123.00) · 6 name(s) re-marked at the open (per-name table). WAY×241 yday $26.51 → 09:30 $26.95 +106.04; QCOM×4 yday $188.71 → 09:30 $191.34 +10.52; SM×22 yday $36.97 → 09:30 $36.87 -2.20; CLS×2 yday $329.94 → 09:30 $332.06 +4.24; SMTC×1 yday $178.19 → 09:30 $182.33 +4.14; GME×2 yday $22.77 → 09:30 $22.90 +0.26 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 7 | $20.91 | $1.48 | — | $63.87 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $148.21 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 4 | $14.79 | $0.60 | — | $4.11 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $63.52 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.11 | ▼ close $8,814.30 vs 09:30 $9,175.42 (session -359.04) | 16:00 close · cash $4.11 · equity $8,814.30 vs 09:30 $9,175.42 (-361.12; session marks -359.04) · 8 name(s) marked open→close (per-name table). WAY×241 09:30 $26.95 → close $25.66 -310.89; QCOM×4 09:30 $191.34 → close $177.72 -54.48; SM×22 09:30 $36.87 → close $36.97 +2.20; CLS×2 09:30 $332.06 → close $332.63 +1.14; SMTC×1 09:30 $182.33 → close $185.00 +2.67; GME×2 09:30 $22.90 → close $22.64 -0.52; TH×7 09:30 $20.91 → close $21.19 +1.96; RARE×4 09:30 $14.79 → close $14.51 -1.12 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.11 | ▲ 09:30 equity $8,896.74 vs yday $8,814.30 (+82.44) | 09:30 open · cash $4.11 (unchanged overnight, no fees) · equity $8,896.74 vs prior close $8,814.30 (+82.44) · 8 name(s) re-marked at the open (per-name table). WAY×241 yday $25.66 → 09:30 $25.94 +67.48; QCOM×4 yday $177.72 → 09:30 $180.61 +11.56; SM×22 yday $36.97 → 09:30 $35.91 -23.32; CLS×2 yday $332.63 → 09:30 $341.45 +17.64; SMTC×1 yday $185.00 → 09:30 $190.30 +5.30; GME×2 yday $22.64 → 09:30 $22.78 +0.28; TH×7 yday $21.19 → 09:30 $21.65 +3.22; RARE×4 yday $14.51 → 09:30 $14.58 +0.28 | — |
| 2026-09-21 09:30 ET | **SELL** | `WAY` | 241 | $25.94 | $3.20 | $-85.84 | $6,252.45 | ▼ -85.84 after sell → book $8,893.54; vs 09:30 mark -3.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QCOM` | 4 | $180.61 | $2.02 | $-38.26 | $6,972.87 | ▼ -38.26 after sell → book $8,891.52; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SM` | 22 | $35.91 | $2.08 | $-93.89 | $7,760.81 | ▼ -93.89 after sell → book $8,889.44; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `CLS` | 2 | $341.45 | $2.02 | $+38.49 | $8,441.69 | ▲ +38.49 after sell → book $8,887.42; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 25 | $230.25 | $2.06 | — | $2,683.38 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; ret5=+12.5; leftover $5909.19 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-21 09:30 ET | **BUY** | `ALVO` | 213 | $5.92 | $2.75 | — | $1,419.67 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; ret5=+11.0; leftover $1266.25 | join🔴 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 211 | $6.00 | $2.72 | — | $150.95 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_mover; 🔵; ret5=-24.1; leftover $1266.25 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $150.95 | ▼ close $8,697.34 vs 09:30 $8,896.74 (session -182.55) | 16:00 close · cash $150.95 · equity $8,697.34 vs 09:30 $8,896.74 (-199.40; session marks -182.55) · 7 name(s) marked open→close (per-name table). SMTC×1 09:30 $190.30 → close $177.37 -12.93; GME×2 09:30 $22.78 → close $22.76 -0.04; TH×7 09:30 $21.65 → close $21.28 -2.59; RARE×4 09:30 $14.58 → close $14.65 +0.28; VICR×25 09:30 $230.25 → close $223.90 -158.75; ALVO×213 09:30 $5.92 → close $5.88 -8.52; SION×211 09:30 $6.00 → close $6.00 +0.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1000.00 < 1 share @ 1646.93 |
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `FANG` | cash | leftover split 198.29 < 1 share @ 202.70 |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OUST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OUST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 42.55 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 18.24 < 1 share @ 115.18 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 91.31 < 1 share @ 267.02 |
| 2026-08-26 | `CM` | cash | leftover split 13.04 < 1 share @ 118.50 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `TRLV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 77.51 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 11.07 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 11.07 < 1 share @ 1746.53 |
| 2026-08-27 | `CM` | cash | leftover split 11.07 < 1 share @ 118.77 |
| 2026-08-28 | `TRLV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MPWR` | cash | leftover split 1011.67 < 1 share @ 1306.03 |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CXW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 158.83 < 1 share @ 263.36 |
| 2026-09-04 | `MRX` | cash | leftover split 34.04 < 1 share @ 75.65 |
| 2026-09-04 | `BE` | cash | leftover split 34.04 < 1 share @ 236.82 |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CXW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ADBE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ADBE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `JBHT` | cash | leftover split 64.35 < 1 share @ 238.60 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `GME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `SMTC` | 1 | 2026-09-17 @ $170.85 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $300.28 |
| `GME` | 2 | 2026-09-17 @ $22.12 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $64.35 |
| `TH` | 7 | 2026-09-18 @ $20.91 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $148.21 |
| `RARE` | 4 | 2026-09-18 @ $14.79 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $63.52 |
| `VICR` | 25 | 2026-09-21 @ $230.25 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; ret5=+12.5; leftover $5909.19 |
| `ALVO` | 213 | 2026-09-21 @ $5.92 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; ret5=+11.0; leftover $1266.25 |
| `SION` | 211 | 2026-09-21 @ $6.00 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_mover; 🔵; ret5=-24.1; leftover $1266.25 |
