# Factor mine action — `union_news_g_conv_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `conviction` · sell `list` · S-boost `none` · merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5

Cash book **-9.59%** ($9,041) · signal-only (no cash/fees) was +204.26%. Starts YES **2/26**. Fills 60 · skips 117 · realized $-651.67.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $43.31.

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
| 2026-08-17 | `GLOB` | 2 | — | $37.18 | +0.00 | $36.26 | -1.84 | -1.84 | +0.00 | -1.84 |
| 2026-08-18 | `HLIT` | 531 | $13.43 | $12.93 | -265.50 | $12.73 | -106.20 | -371.70 | -132.75 | -238.95 |
| 2026-08-18 | `ANGX` | 232 | $4.71 | $4.79 | +18.56 | $4.85 | +13.92 | +32.48 | +111.36 | +125.28 |
| 2026-08-18 | `ARX` | 51 | $19.54 | $19.57 | +1.53 | $19.56 | -0.51 | +1.02 | +0.00 | -0.51 |
| 2026-08-18 | `DVN` | 8 | $47.57 | $48.00 | +3.44 | $47.83 | -1.36 | +2.08 | +14.56 | +13.20 |
| 2026-08-18 | `EOG` | 2 | $146.15 | $148.04 | +3.78 | $148.70 | +1.32 | +5.10 | +10.54 | +11.86 |
| 2026-08-18 | `GLOB` | 2 | $36.26 | $36.98 | +1.44 | $37.31 | +0.66 | +2.10 | -0.40 | +0.26 |
| 2026-08-19 | `HLIT` | 531 | $12.73 | $12.90 | +90.27 | — | +0.00 | +90.27 | -148.68 | — |
| 2026-08-19 | `ANGX` | 232 | $4.85 | $4.79 | -13.92 | — | +0.00 | -13.92 | +111.36 | — |
| 2026-08-19 | `ARX` | 51 | $19.56 | $19.58 | +1.02 | — | +0.00 | +1.02 | +0.51 | — |
| 2026-08-19 | `DVN` | 8 | $47.83 | $48.22 | +3.12 | $48.19 | -0.24 | +2.88 | +16.32 | +16.08 |
| 2026-08-19 | `EOG` | 2 | $148.70 | $149.86 | +2.32 | $149.48 | -0.76 | +1.56 | +14.18 | +13.42 |
| 2026-08-19 | `GLOB` | 2 | $37.31 | $37.61 | +0.60 | $39.52 | +3.82 | +4.42 | +0.86 | +4.68 |
| 2026-08-20 | `DVN` | 8 | $48.19 | $49.02 | +6.64 | — | +0.00 | +6.64 | +22.72 | — |
| 2026-08-20 | `EOG` | 2 | $149.48 | $151.45 | +3.94 | — | +0.00 | +3.94 | +17.36 | — |
| 2026-08-20 | `GLOB` | 2 | $39.52 | $39.37 | -0.30 | — | +0.00 | -0.30 | +4.38 | — |
| 2026-08-20 | `BHP` | 76 | — | $91.01 | +0.00 | $93.63 | +199.12 | +199.12 | +0.00 | +199.12 |
| 2026-08-20 | `APA` | 22 | — | $44.76 | +0.00 | $44.39 | -8.14 | -8.14 | +0.00 | -8.14 |
| 2026-08-20 | `AUTL` | 403 | — | $2.47 | +0.00 | $2.46 | -4.03 | -4.03 | +0.00 | -4.03 |
| 2026-08-20 | `CRSP` | 16 | — | $58.73 | +0.00 | $58.12 | -9.76 | -9.76 | +0.00 | -9.76 |
| 2026-08-21 | `BHP` | 76 | $93.63 | $95.72 | +158.84 | $97.03 | +99.56 | +258.40 | +357.96 | +457.52 |
| 2026-08-21 | `APA` | 22 | $44.39 | $44.52 | +2.86 | $43.39 | -24.86 | -22.00 | -5.28 | -30.14 |
| 2026-08-21 | `AUTL` | 403 | $2.46 | $2.47 | +4.03 | $2.41 | -24.18 | -20.15 | +0.00 | -24.18 |
| 2026-08-21 | `CRSP` | 16 | $58.12 | $59.72 | +25.60 | $59.50 | -3.52 | +22.08 | +15.84 | +12.32 |
| 2026-08-24 | `BHP` | 76 | $97.03 | $97.31 | +21.28 | $97.13 | -13.68 | +7.60 | +478.80 | +465.12 |
| 2026-08-24 | `APA` | 22 | $43.39 | $42.93 | -10.12 | $42.96 | +0.66 | -9.46 | -40.26 | -39.60 |
| 2026-08-24 | `AUTL` | 403 | $2.41 | $2.40 | -4.03 | $2.34 | -24.18 | -28.21 | -28.21 | -52.39 |
| 2026-08-24 | `CRSP` | 16 | $59.50 | $58.75 | -12.00 | $57.08 | -26.80 | -38.80 | +0.32 | -26.48 |
| 2026-08-25 | `BHP` | 76 | $97.13 | $95.86 | -96.52 | — | +0.00 | -96.52 | +368.60 | — |
| 2026-08-25 | `APA` | 22 | $42.96 | $41.38 | -34.76 | — | +0.00 | -34.76 | -74.36 | — |
| 2026-08-25 | `AUTL` | 403 | $2.34 | $2.38 | +16.12 | — | +0.00 | +16.12 | -36.27 | — |
| 2026-08-25 | `CRSP` | 16 | $57.08 | $57.93 | +13.68 | — | +0.00 | +13.68 | -12.80 | — |
| 2026-08-25 | `AU` | 60 | — | $118.52 | +0.00 | $123.39 | +292.20 | +292.20 | +0.00 | +292.20 |
| 2026-08-25 | `FCX` | 13 | — | $77.13 | +0.00 | $79.91 | +36.14 | +36.14 | +0.00 | +36.14 |
| 2026-08-25 | `EZPW` | 29 | — | $35.05 | +0.00 | $35.23 | +5.22 | +5.22 | +0.00 | +5.22 |
| 2026-08-25 | `AMX` | 42 | — | $23.80 | +0.00 | $23.75 | -2.10 | -2.10 | +0.00 | -2.10 |
| 2026-08-26 | `AU` | 60 | $123.39 | $119.80 | -215.40 | $118.11 | -101.40 | -316.80 | +76.80 | -24.60 |
| 2026-08-26 | `FCX` | 13 | $79.91 | $79.34 | -7.41 | $79.00 | -4.42 | -11.83 | +28.73 | +24.31 |
| 2026-08-26 | `EZPW` | 29 | $35.23 | $35.70 | +13.63 | $33.90 | -52.20 | -38.57 | +18.85 | -33.35 |
| 2026-08-26 | `AMX` | 42 | $23.75 | $23.75 | +0.00 | $23.62 | -5.46 | -5.46 | -2.10 | -7.56 |
| 2026-08-27 | `AU` | 60 | $118.11 | $117.41 | -42.00 | $118.40 | +59.40 | +17.40 | -66.60 | -7.20 |
| 2026-08-27 | `FCX` | 13 | $79.00 | $78.83 | -2.21 | $78.42 | -5.33 | -7.54 | +22.10 | +16.77 |
| 2026-08-27 | `EZPW` | 29 | $33.90 | $33.50 | -11.60 | $34.41 | +26.39 | +14.79 | -44.95 | -18.56 |
| 2026-08-27 | `AMX` | 42 | $23.62 | $23.77 | +6.30 | $23.50 | -11.34 | -5.04 | -1.26 | -12.60 |
| 2026-08-28 | `AU` | 60 | $118.40 | $119.19 | +47.40 | — | +0.00 | +47.40 | +40.20 | — |
| 2026-08-28 | `FCX` | 13 | $78.42 | $78.57 | +1.95 | — | +0.00 | +1.95 | +18.72 | — |
| 2026-08-28 | `EZPW` | 29 | $34.41 | $34.50 | +2.61 | — | +0.00 | +2.61 | -15.95 | — |
| 2026-08-28 | `AMX` | 42 | $23.50 | $23.64 | +5.88 | — | +0.00 | +5.88 | -6.72 | — |
| 2026-08-28 | `KEYS` | 22 | — | $324.41 | +0.00 | $319.97 | -97.68 | -97.68 | +0.00 | -97.68 |
| 2026-08-28 | `SMTC` | 7 | — | $141.76 | +0.00 | $131.17 | -74.13 | -74.13 | +0.00 | -74.13 |
| 2026-08-28 | `CIEN` | 2 | — | $400.42 | +0.00 | $378.44 | -43.96 | -43.96 | +0.00 | -43.96 |
| 2026-08-31 | `KEYS` | 22 | $319.97 | $322.49 | +55.44 | $322.70 | +4.62 | +60.06 | -42.24 | -37.62 |
| 2026-08-31 | `SMTC` | 7 | $131.17 | $132.30 | +7.91 | $132.96 | +4.62 | +12.53 | -66.22 | -61.60 |
| 2026-08-31 | `CIEN` | 2 | $378.44 | $378.44 | +0.00 | $382.80 | +8.72 | +8.72 | -43.96 | -35.24 |
| 2026-09-01 | `KEYS` | 22 | $322.70 | $321.47 | -27.06 | $319.27 | -48.40 | -75.46 | -64.68 | -113.08 |
| 2026-09-01 | `SMTC` | 7 | $132.96 | $127.63 | -37.31 | $132.27 | +32.48 | -4.83 | -98.91 | -66.43 |
| 2026-09-01 | `CIEN` | 2 | $382.80 | $376.89 | -11.82 | $360.33 | -33.12 | -44.94 | -47.06 | -80.18 |
| 2026-09-02 | `KEYS` | 22 | $319.27 | $318.04 | -27.06 | — | +0.00 | -27.06 | -140.14 | — |
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
| 2026-09-04 | `MMED` | 1 | — | $23.84 | +0.00 | $23.29 | -0.55 | -0.55 | +0.00 | -0.55 |
| 2026-09-08 | `AVGO` | 19 | $357.90 | $363.68 | +109.82 | $368.56 | +92.72 | +202.54 | +226.86 | +319.58 |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | $533.88 | +25.46 | +19.48 | +69.68 | +95.14 |
| 2026-09-08 | `CXW` | 30 | $34.71 | $34.49 | -6.60 | $35.05 | +16.80 | +10.20 | +65.40 | +82.20 |
| 2026-09-08 | `FRNM` | 62 | $16.31 | $16.74 | +26.66 | $15.99 | -46.50 | -19.84 | +53.94 | +7.44 |
| 2026-09-08 | `MMED` | 1 | $23.29 | $23.16 | -0.13 | $23.32 | +0.16 | +0.03 | -0.68 | -0.52 |
| 2026-09-09 | `AVGO` | 19 | $368.56 | $366.23 | -44.27 | — | +0.00 | -44.27 | +275.31 | — |
| 2026-09-09 | `DELL` | 2 | $533.88 | $538.47 | +9.18 | — | +0.00 | +9.18 | +104.32 | — |
| 2026-09-09 | `CXW` | 30 | $35.05 | $35.09 | +1.20 | — | +0.00 | +1.20 | +83.40 | — |
| 2026-09-09 | `FRNM` | 62 | $15.99 | $15.96 | -1.86 | — | +0.00 | -1.86 | +5.58 | — |
| 2026-09-09 | `MMED` | 1 | $23.32 | $23.22 | -0.10 | $22.76 | -0.46 | -0.56 | -0.62 | -1.08 |
| 2026-09-10 | `MMED` | 1 | $22.76 | $22.54 | -0.22 | — | +0.00 | -0.22 | -1.30 | — |
| 2026-09-11 | `ORCL` | 44 | — | $164.43 | +0.00 | $150.28 | -622.60 | -622.60 | +0.00 | -622.60 |
| 2026-09-11 | `BTI` | 18 | — | $56.03 | +0.00 | $55.24 | -14.22 | -14.22 | +0.00 | -14.22 |
| 2026-09-11 | `ADBE` | 4 | — | $242.17 | +0.00 | $252.23 | +40.24 | +40.24 | +0.00 | +40.24 |
| 2026-09-11 | `CNQ` | 20 | — | $49.94 | +0.00 | $50.07 | +2.60 | +2.60 | +0.00 | +2.60 |
| 2026-09-14 | `ORCL` | 44 | $150.28 | $141.42 | -389.84 | $144.79 | +148.28 | -241.56 | -1012.44 | -864.16 |
| 2026-09-14 | `BTI` | 18 | $55.24 | $57.12 | +33.84 | $57.29 | +3.06 | +36.90 | +19.62 | +22.68 |
| 2026-09-14 | `ADBE` | 4 | $252.23 | $261.51 | +37.12 | $265.60 | +16.36 | +53.48 | +77.36 | +93.72 |
| 2026-09-14 | `CNQ` | 20 | $50.07 | $50.76 | +13.80 | $50.32 | -8.80 | +5.00 | +16.40 | +7.60 |
| 2026-09-15 | `ORCL` | 44 | $144.79 | $143.46 | -58.52 | $140.35 | -136.84 | -195.36 | -922.68 | -1059.52 |
| 2026-09-15 | `BTI` | 18 | $57.29 | $56.46 | -14.94 | $56.52 | +1.08 | -13.86 | +7.74 | +8.82 |
| 2026-09-15 | `ADBE` | 4 | $265.60 | $261.70 | -15.60 | $257.76 | -15.76 | -31.36 | +78.12 | +62.36 |
| 2026-09-15 | `CNQ` | 20 | $50.32 | $50.45 | +2.60 | $51.55 | +22.00 | +24.60 | +10.20 | +32.20 |
| 2026-09-16 | `ORCL` | 44 | $140.35 | $140.03 | -14.08 | — | +0.00 | -14.08 | -1073.60 | — |
| 2026-09-16 | `BTI` | 18 | $56.52 | $56.54 | +0.36 | — | +0.00 | +0.36 | +9.18 | — |
| 2026-09-16 | `ADBE` | 4 | $257.76 | $253.34 | -17.68 | — | +0.00 | -17.68 | +44.68 | — |
| 2026-09-16 | `CNQ` | 20 | $51.55 | $50.91 | -12.80 | — | +0.00 | -12.80 | +19.40 | — |
| 2026-09-16 | `WAY` | 249 | — | $26.27 | +0.00 | $26.59 | +79.68 | +79.68 | +0.00 | +79.68 |
| 2026-09-16 | `QCOM` | 4 | — | $189.17 | +0.00 | $184.84 | -17.32 | -17.32 | +0.00 | -17.32 |
| 2026-09-16 | `SM` | 23 | — | $39.99 | +0.00 | $38.16 | -42.09 | -42.09 | +0.00 | -42.09 |
| 2026-09-16 | `AMX` | 40 | — | $23.18 | +0.00 | $22.98 | -8.00 | -8.00 | +0.00 | -8.00 |
| 2026-09-17 | `WAY` | 249 | $26.59 | $26.51 | -19.92 | $26.51 | +0.00 | -19.92 | +59.76 | +59.76 |
| 2026-09-17 | `QCOM` | 4 | $184.84 | $190.35 | +22.04 | $188.71 | -6.56 | +15.48 | +4.72 | -1.84 |
| 2026-09-17 | `SM` | 23 | $38.16 | $37.57 | -13.57 | $36.97 | -13.80 | -27.37 | -55.66 | -69.46 |
| 2026-09-17 | `AMX` | 40 | $22.98 | $23.09 | +4.40 | $23.03 | -2.40 | +2.00 | -3.60 | -6.00 |
| 2026-09-17 | `AVTR` | 1 | — | $15.81 | +0.00 | $15.86 | +0.05 | +0.05 | +0.00 | +0.05 |
| 2026-09-18 | `WAY` | 249 | $26.51 | $26.95 | +109.56 | $25.66 | -321.21 | -211.65 | +169.32 | -151.89 |
| 2026-09-18 | `QCOM` | 4 | $188.71 | $191.34 | +10.52 | $177.72 | -54.48 | -43.96 | +8.68 | -45.80 |
| 2026-09-18 | `SM` | 23 | $36.97 | $36.87 | -2.30 | $36.97 | +2.30 | +0.00 | -71.76 | -69.46 |
| 2026-09-18 | `AMX` | 40 | $23.03 | $22.90 | -5.20 | $22.43 | -18.80 | -24.00 | -11.20 | -30.00 |
| 2026-09-18 | `AVTR` | 1 | $15.86 | $15.87 | +0.01 | $15.52 | -0.35 | -0.34 | +0.06 | -0.29 |
| 2026-09-18 | `TH` | 5 | — | $20.91 | +0.00 | $21.19 | +1.40 | +1.40 | +0.00 | +1.40 |
| 2026-09-18 | `RARE` | 1 | — | $14.79 | +0.00 | $14.51 | -0.28 | -0.28 | +0.00 | -0.28 |
| 2026-09-18 | `BHVN` | 1 | — | $14.07 | +0.00 | $13.62 | -0.45 | -0.45 | +0.00 | -0.45 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +407.37 | HLIT, ANGX, ARX | — | $991.44 | $10,395.38 | HLIT×531, ANGX×232, ARX×51 |
| 2026-08-17 | +2.25 | $991.44 | HLIT×531, ANGX×232, ARX×51 | $10,405.75 | +10.37 | -177.68 | DVN, EOG, GLOB | — | $257.34 | $10,223.31 | HLIT×531, ANGX×232, ARX×51, DVN×8, EOG×2, GLOB×2 |
| 2026-08-18 | -6.20 | $257.34 | HLIT×531, ANGX×232, ARX×51, DVN×8, EOG×2, GLOB×2 | $9,986.56 | -236.75 | -92.17 | — | — | $257.34 | $9,894.39 | HLIT×531, ANGX×232, ARX×51, DVN×8, EOG×2, GLOB×2 |
| 2026-08-19 | -7.20 | $257.34 | HLIT×531, ANGX×232, ARX×51, DVN×8, EOG×2, GLOB×2 | $9,977.80 | +83.41 | +2.82 | — | HLIT, ANGX, ARX | $9,204.91 | $9,968.43 | DVN×8, EOG×2, GLOB×2 |
| 2026-08-20 | +1.12 | $9,204.91 | DVN×8, EOG×2, GLOB×2 | $9,978.71 | +10.28 | +177.19 | BHP, APA, AUTL, CRSP | DVN, EOG, GLOB | $125.76 | $10,139.52 | BHP×76, APA×22, AUTL×403, CRSP×16 |
| 2026-08-21 | +3.25 | $125.76 | BHP×76, APA×22, AUTL×403, CRSP×16 | $10,330.85 | +191.33 | +47.00 | — | — | $125.76 | $10,377.85 | BHP×76, APA×22, AUTL×403, CRSP×16 |
| 2026-08-24 | -5.17 | $125.76 | BHP×76, APA×22, AUTL×403, CRSP×16 | $10,372.98 | -4.87 | -64.00 | — | — | $125.76 | $10,308.98 | BHP×76, APA×22, AUTL×403, CRSP×16 |
| 2026-08-25 | +1.80 | $125.76 | BHP×76, APA×22, AUTL×403, CRSP×16 | $10,207.50 | -101.48 | +331.46 | AU, FCX, EZPW, AMX | BHP, APA, AUTL, CRSP | $57.47 | $10,518.87 | AU×60, FCX×13, EZPW×29, AMX×42 |
| 2026-08-26 | +2.02 | $57.47 | AU×60, FCX×13, EZPW×29, AMX×42 | $10,309.69 | -209.18 | -163.48 | — | — | $57.47 | $10,146.21 | AU×60, FCX×13, EZPW×29, AMX×42 |
| 2026-08-27 | — | $57.47 | AU×60, FCX×13, EZPW×29, AMX×42 | $10,096.70 | -49.51 | +69.12 | — | — | $57.47 | $10,165.82 | AU×60, FCX×13, EZPW×29, AMX×42 |
| 2026-08-28 | +0.75 | $57.47 | AU×60, FCX×13, EZPW×29, AMX×42 | $10,223.66 | +57.84 | -215.77 | KEYS, SMTC, CIEN | AU, FCX, EZPW, AMX | $1,278.90 | $9,993.31 | KEYS×22, SMTC×7, CIEN×2 |
| 2026-08-31 | -5.85 | $1,278.90 | KEYS×22, SMTC×7, CIEN×2 | $10,056.66 | +63.35 | +17.96 | — | — | $1,278.90 | $10,074.62 | KEYS×22, SMTC×7, CIEN×2 |
| 2026-09-01 | -6.30 | $1,278.90 | KEYS×22, SMTC×7, CIEN×2 | $9,998.43 | -76.19 | -49.04 | — | — | $1,278.90 | $9,949.39 | KEYS×22, SMTC×7, CIEN×2 |
| 2026-09-02 | -3.83 | $1,278.90 | KEYS×22, SMTC×7, CIEN×2 | $9,921.28 | -28.11 | +0.00 | — | KEYS, SMTC, CIEN | $9,915.11 | $9,915.11 | — |
| 2026-09-03 | -0.90 | $9,915.11 | — | $9,915.11 | +0.00 | +267.50 | AVGO, DELL, CXW, FRNM | — | $297.89 | $10,174.31 | AVGO×19, DELL×2, CXW×30, FRNM×62 |
| 2026-09-04 | +2.25 | $297.89 | AVGO×19, DELL×2, CXW×30, FRNM×62 | $10,180.35 | +6.04 | +17.89 | MMED | — | $273.81 | $10,198.00 | AVGO×19, DELL×2, CXW×30, FRNM×62, MMED×1 |
| 2026-09-08 | -11.47 | $273.81 | AVGO×19, DELL×2, CXW×30, FRNM×62, MMED×1 | $10,321.77 | +123.77 | +88.64 | — | — | $273.81 | $10,410.41 | AVGO×19, DELL×2, CXW×30, FRNM×62, MMED×1 |
| 2026-09-09 | -13.95 | $273.81 | AVGO×19, DELL×2, CXW×30, FRNM×62, MMED×1 | $10,374.56 | -35.85 | -0.46 | — | AVGO, DELL, CXW, FRNM | $10,342.92 | $10,365.68 | MMED×1 |
| 2026-09-10 | -13.28 | $10,342.92 | MMED×1 | $10,365.46 | -0.22 | +0.00 | — | MMED | $10,365.21 | $10,365.21 | — |
| 2026-09-11 | +0.50 | $10,365.21 | — | $10,365.21 | -0.00 | -593.98 | ORCL, BTI, ADBE, CNQ | — | $146.05 | $9,763.01 | ORCL×44, BTI×18, ADBE×4, CNQ×20 |
| 2026-09-14 | -11.00 | $146.05 | ORCL×44, BTI×18, ADBE×4, CNQ×20 | $9,457.93 | -305.08 | +158.90 | — | — | $146.05 | $9,616.83 | ORCL×44, BTI×18, ADBE×4, CNQ×20 |
| 2026-09-15 | -3.84 | $146.05 | ORCL×44, BTI×18, ADBE×4, CNQ×20 | $9,530.37 | -86.46 | -129.52 | — | — | $146.05 | $9,400.85 | ORCL×44, BTI×18, ADBE×4, CNQ×20 |
| 2026-09-16 | +5.30 | $146.05 | ORCL×44, BTI×18, ADBE×4, CNQ×20 | $9,356.65 | -44.20 | +12.27 | WAY, QCOM, SM, AMX | ORCL, BTI, ADBE, CNQ | $194.05 | $9,351.20 | WAY×249, QCOM×4, SM×23, AMX×40 |
| 2026-09-17 | +7.38 | $194.05 | WAY×249, QCOM×4, SM×23, AMX×40 | $9,344.15 | -7.05 | -22.71 | AVTR | — | $178.08 | $9,321.28 | WAY×249, QCOM×4, SM×23, AMX×40, AVTR×1 |
| 2026-09-18 | +4.86 | $178.08 | WAY×249, QCOM×4, SM×23, AMX×40, AVTR×1 | $9,433.87 | +112.59 | -391.87 | TH, RARE, BHVN | — | $43.31 | $9,040.64 | WAY×249, QCOM×4, SM×23, AMX×40, AVTR×1, TH×5, RARE×1, BHVN×1 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 531 | $13.18 | $6.85 | — | $2,994.57 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $7000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 232 | $4.31 | $2.99 | — | $1,991.66 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 51 | $19.57 | $2.14 | — | $991.44 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+58.7; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $991.44 | ▲ close $10,395.38 vs 09:30 $10,000.00 (session +407.37) | 16:00 close · cash $991.44 · equity $10,395.38 vs 09:30 $10,000.00 (+395.38; session marks +407.37) · 3 name(s) marked open→close (per-name table). HLIT×531 09:30 $13.18 → close $13.92 +392.94; ANGX×232 09:30 $4.31 → close $4.37 +13.92; ARX×51 09:30 $19.57 → close $19.58 +0.51 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $991.44 | ▲ 09:30 equity $10,405.75 vs yday $10,395.38 (+10.37) | 09:30 open · cash $991.44 (unchanged overnight, no fees) · equity $10,405.75 vs prior close $10,395.38 (+10.37) · 3 name(s) re-marked at the open (per-name table). HLIT×531 yday $13.92 → 09:30 $13.84 -42.48; ANGX×232 yday $4.37 → 09:30 $4.60 +53.36; ARX×51 yday $19.58 → 09:30 $19.57 -0.51 | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 8 | $46.18 | $2.01 | — | $619.99 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten; 🔵; ret5=+6.7; leftover $396.58 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 2 | $142.77 | $2.00 | — | $332.45 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten; 🔵; ret5=+5.8; leftover $297.43 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `GLOB` | 2 | $37.18 | $0.75 | — | $257.34 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list oppset; ⚪; ret5=-0.1; leftover $99.14 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $257.34 | ▼ close $10,223.31 vs 09:30 $10,405.75 (session -177.68) | 16:00 close · cash $257.34 · equity $10,223.31 vs 09:30 $10,405.75 (-182.44; session marks -177.68) · 6 name(s) marked open→close (per-name table). HLIT×531 09:30 $13.84 → close $13.43 -217.71; ANGX×232 09:30 $4.60 → close $4.71 +25.52; ARX×51 09:30 $19.57 → close $19.54 -1.53; DVN×8 09:30 $46.18 → close $47.57 +11.12; EOG×2 09:30 $142.77 → close $146.15 +6.76; GLOB×2 09:30 $37.18 → close $36.26 -1.84 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $257.34 | ▼ 09:30 equity $9,986.56 vs yday $10,223.31 (-236.75) | 09:30 open · cash $257.34 (unchanged overnight, no fees) · equity $9,986.56 vs prior close $10,223.31 (-236.75) · 6 name(s) re-marked at the open (per-name table). HLIT×531 yday $13.43 → 09:30 $12.93 -265.50; ANGX×232 yday $4.71 → 09:30 $4.79 +18.56; ARX×51 yday $19.54 → 09:30 $19.57 +1.53; DVN×8 yday $47.57 → 09:30 $48.00 +3.44; EOG×2 yday $146.15 → 09:30 $148.04 +3.78; GLOB×2 yday $36.26 → 09:30 $36.98 +1.44 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $257.34 | ▼ close $9,894.39 vs 09:30 $9,986.56 (session -92.17) | 16:00 close · cash $257.34 · equity $9,894.39 vs 09:30 $9,986.56 (-92.17; session marks -92.17) · 6 name(s) marked open→close (per-name table). HLIT×531 09:30 $12.93 → close $12.73 -106.20; ANGX×232 09:30 $4.79 → close $4.85 +13.92; ARX×51 09:30 $19.57 → close $19.56 -0.51; DVN×8 09:30 $48.00 → close $47.83 -1.36; EOG×2 09:30 $148.04 → close $148.70 +1.32; GLOB×2 09:30 $36.98 → close $37.31 +0.66 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $257.34 | ▲ 09:30 equity $9,977.80 vs yday $9,894.39 (+83.41) | 09:30 open · cash $257.34 (unchanged overnight, no fees) · equity $9,977.80 vs prior close $9,894.39 (+83.41) · 6 name(s) re-marked at the open (per-name table). HLIT×531 yday $12.73 → 09:30 $12.90 +90.27; ANGX×232 yday $4.85 → 09:30 $4.79 -13.92; ARX×51 yday $19.56 → 09:30 $19.58 +1.02; DVN×8 yday $47.83 → 09:30 $48.22 +3.12; EOG×2 yday $148.70 → 09:30 $149.86 +2.32; GLOB×2 yday $37.31 → 09:30 $37.61 +0.60 | — |
| 2026-08-19 09:30 ET | **SELL** | `HLIT` | 531 | $12.90 | $6.99 | $-162.52 | $7,100.25 | ▼ -162.52 after sell → book $9,970.81; vs 09:30 mark -6.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 232 | $4.79 | $3.04 | $+105.33 | $8,208.49 | ▲ +105.33 after sell → book $9,967.77; vs 09:30 mark -3.04 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 51 | $19.58 | $2.16 | $-3.80 | $9,204.91 | ▼ -3.80 after sell → book $9,965.61; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,204.91 | ▲ close $9,968.43 vs 09:30 $9,977.80 (session +2.82) | 16:00 close · cash $9,204.91 · equity $9,968.43 vs 09:30 $9,977.80 (-9.37; session marks +2.82) · 3 name(s) marked open→close (per-name table). DVN×8 09:30 $48.22 → close $48.19 -0.24; EOG×2 09:30 $149.86 → close $149.48 -0.76; GLOB×2 09:30 $37.61 → close $39.52 +3.82 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,204.91 | ▲ 09:30 equity $9,978.71 vs yday $9,968.43 (+10.28) | 09:30 open · cash $9,204.91 (unchanged overnight, no fees) · equity $9,978.71 vs prior close $9,968.43 (+10.28) · 3 name(s) re-marked at the open (per-name table). DVN×8 yday $48.19 → 09:30 $49.02 +6.64; EOG×2 yday $149.48 → 09:30 $151.45 +3.94; GLOB×2 yday $39.52 → 09:30 $39.37 -0.30 | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 8 | $49.02 | $2.03 | $+18.67 | $9,595.03 | ▲ +18.67 after sell → book $9,976.67; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `EOG` | 2 | $151.45 | $2.02 | $+13.35 | $9,895.92 | ▲ +13.35 after sell → book $9,974.66; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `GLOB` | 2 | $39.37 | $0.81 | $+2.82 | $9,973.84 | ▲ +2.82 after sell → book $9,973.84; vs 09:30 mark -0.82 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 76 | $91.01 | $2.22 | — | $3,054.87 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $6981.69 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 22 | $44.76 | $2.06 | — | $2,068.09 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $997.38 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 403 | $2.47 | $5.20 | — | $1,067.48 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $997.38 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 16 | $58.73 | $2.04 | — | $125.76 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $997.38 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $125.76 | ▲ close $10,139.52 vs 09:30 $9,978.71 (session +177.19) | 16:00 close · cash $125.76 · equity $10,139.52 vs 09:30 $9,978.71 (+160.81; session marks +177.19) · 4 name(s) marked open→close (per-name table). BHP×76 09:30 $91.01 → close $93.63 +199.12; APA×22 09:30 $44.76 → close $44.39 -8.14; AUTL×403 09:30 $2.47 → close $2.46 -4.03; CRSP×16 09:30 $58.73 → close $58.12 -9.76 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $125.76 | ▲ 09:30 equity $10,330.85 vs yday $10,139.52 (+191.33) | 09:30 open · cash $125.76 (unchanged overnight, no fees) · equity $10,330.85 vs prior close $10,139.52 (+191.33) · 4 name(s) re-marked at the open (per-name table). BHP×76 yday $93.63 → 09:30 $95.72 +158.84; APA×22 yday $44.39 → 09:30 $44.52 +2.86; AUTL×403 yday $2.46 → 09:30 $2.47 +4.03; CRSP×16 yday $58.12 → 09:30 $59.72 +25.60 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $125.76 | ▲ close $10,377.85 vs 09:30 $10,330.85 (session +47.00) | 16:00 close · cash $125.76 · equity $10,377.85 vs 09:30 $10,330.85 (+47.00; session marks +47.00) · 4 name(s) marked open→close (per-name table). BHP×76 09:30 $95.72 → close $97.03 +99.56; APA×22 09:30 $44.52 → close $43.39 -24.86; AUTL×403 09:30 $2.47 → close $2.41 -24.18; CRSP×16 09:30 $59.72 → close $59.50 -3.52 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $125.76 | ▼ 09:30 equity $10,372.98 vs yday $10,377.85 (-4.87) | 09:30 open · cash $125.76 (unchanged overnight, no fees) · equity $10,372.98 vs prior close $10,377.85 (-4.87) · 4 name(s) re-marked at the open (per-name table). BHP×76 yday $97.03 → 09:30 $97.31 +21.28; APA×22 yday $43.39 → 09:30 $42.93 -10.12; AUTL×403 yday $2.41 → 09:30 $2.40 -4.03; CRSP×16 yday $59.50 → 09:30 $58.75 -12.00 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $125.76 | ▼ close $10,308.98 vs 09:30 $10,372.98 (session -64.00) | 16:00 close · cash $125.76 · equity $10,308.98 vs 09:30 $10,372.98 (-64.00; session marks -64.00) · 4 name(s) marked open→close (per-name table). BHP×76 09:30 $97.31 → close $97.13 -13.68; APA×22 09:30 $42.93 → close $42.96 +0.66; AUTL×403 09:30 $2.40 → close $2.34 -24.18; CRSP×16 09:30 $58.75 → close $57.08 -26.80 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $125.76 | ▼ 09:30 equity $10,207.50 vs yday $10,308.98 (-101.48) | 09:30 open · cash $125.76 (unchanged overnight, no fees) · equity $10,207.50 vs prior close $10,308.98 (-101.48) · 4 name(s) re-marked at the open (per-name table). BHP×76 yday $97.13 → 09:30 $95.86 -96.52; APA×22 yday $42.96 → 09:30 $41.38 -34.76; AUTL×403 yday $2.34 → 09:30 $2.38 +16.12; CRSP×16 yday $57.08 → 09:30 $57.93 +13.68 | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 76 | $95.86 | $2.29 | $+364.09 | $7,408.83 | ▲ +364.09 after sell → book $10,205.21; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 22 | $41.38 | $2.08 | $-78.49 | $8,317.12 | ▼ -78.49 after sell → book $10,203.14; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 403 | $2.38 | $5.28 | $-46.74 | $9,270.98 | ▼ -46.74 after sell → book $10,197.86; vs 09:30 mark -5.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 16 | $57.93 | $2.06 | $-16.90 | $10,195.81 | ▼ -16.90 after sell → book $10,195.81; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 60 | $118.52 | $2.17 | — | $3,082.44 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $7137.06 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 13 | $77.13 | $2.03 | — | $2,077.72 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1019.58 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 29 | $35.05 | $2.08 | — | $1,059.19 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,oppset; 🔵; ⚪; ret5=+19.7; leftover $1019.58 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMX` | 42 | $23.80 | $2.12 | — | $57.47 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list oppset; 🔵; ret5=+0.5; leftover $1019.58 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.47 | ▲ close $10,518.87 vs 09:30 $10,207.50 (session +331.46) | 16:00 close · cash $57.47 · equity $10,518.87 vs 09:30 $10,207.50 (+311.37; session marks +331.46) · 4 name(s) marked open→close (per-name table). AU×60 09:30 $118.52 → close $123.39 +292.20; FCX×13 09:30 $77.13 → close $79.91 +36.14; EZPW×29 09:30 $35.05 → close $35.23 +5.22; AMX×42 09:30 $23.80 → close $23.75 -2.10 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.47 | ▼ 09:30 equity $10,309.69 vs yday $10,518.87 (-209.18) | 09:30 open · cash $57.47 (unchanged overnight, no fees) · equity $10,309.69 vs prior close $10,518.87 (-209.18) · 4 name(s) re-marked at the open (per-name table). AU×60 yday $123.39 → 09:30 $119.80 -215.40; FCX×13 yday $79.91 → 09:30 $79.34 -7.41; EZPW×29 yday $35.23 → 09:30 $35.70 +13.63; AMX×42 yday $23.75 → 09:30 $23.75 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.47 | ▼ close $10,146.21 vs 09:30 $10,309.69 (session -163.48) | 16:00 close · cash $57.47 · equity $10,146.21 vs 09:30 $10,309.69 (-163.48; session marks -163.48) · 4 name(s) marked open→close (per-name table). AU×60 09:30 $119.80 → close $118.11 -101.40; FCX×13 09:30 $79.34 → close $79.00 -4.42; EZPW×29 09:30 $35.70 → close $33.90 -52.20; AMX×42 09:30 $23.75 → close $23.62 -5.46 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.47 | ▼ 09:30 equity $10,096.70 vs yday $10,146.21 (-49.51) | 09:30 open · cash $57.47 (unchanged overnight, no fees) · equity $10,096.70 vs prior close $10,146.21 (-49.51) · 4 name(s) re-marked at the open (per-name table). AU×60 yday $118.11 → 09:30 $117.41 -42.00; FCX×13 yday $79.00 → 09:30 $78.83 -2.21; EZPW×29 yday $33.90 → 09:30 $33.50 -11.60; AMX×42 yday $23.62 → 09:30 $23.77 +6.30 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.47 | ▲ close $10,165.82 vs 09:30 $10,096.70 (session +69.12) | 16:00 close · cash $57.47 · equity $10,165.82 vs 09:30 $10,096.70 (+69.12; session marks +69.12) · 4 name(s) marked open→close (per-name table). AU×60 09:30 $117.41 → close $118.40 +59.40; FCX×13 09:30 $78.83 → close $78.42 -5.33; EZPW×29 09:30 $33.50 → close $34.41 +26.39; AMX×42 09:30 $23.77 → close $23.50 -11.34 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.47 | ▲ 09:30 equity $10,223.66 vs yday $10,165.82 (+57.84) | 09:30 open · cash $57.47 (unchanged overnight, no fees) · equity $10,223.66 vs prior close $10,165.82 (+57.84) · 4 name(s) re-marked at the open (per-name table). AU×60 yday $118.40 → 09:30 $119.19 +47.40; FCX×13 yday $78.42 → 09:30 $78.57 +1.95; EZPW×29 yday $34.41 → 09:30 $34.50 +2.61; AMX×42 yday $23.50 → 09:30 $23.64 +5.88 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 60 | $119.19 | $2.24 | $+35.79 | $7,206.64 | ▲ +35.79 after sell → book $10,221.43; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 13 | $78.57 | $2.05 | $+14.64 | $8,226.00 | ▲ +14.64 after sell → book $10,219.38; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 29 | $34.50 | $2.10 | $-20.12 | $9,224.40 | ▼ -20.12 after sell → book $10,217.28; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AMX` | 42 | $23.64 | $2.14 | $-10.97 | $10,215.14 | ▼ -10.97 after sell → book $10,215.14; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 22 | $324.41 | $2.06 | — | $3,076.07 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $7150.60 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 7 | $141.76 | $2.01 | — | $2,081.74 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $1021.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 2 | $400.42 | $2.00 | — | $1,278.90 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1021.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,278.90 | ▼ close $9,993.31 vs 09:30 $10,223.66 (session -215.77) | 16:00 close · cash $1,278.90 · equity $9,993.31 vs 09:30 $10,223.66 (-230.35; session marks -215.77) · 3 name(s) marked open→close (per-name table). KEYS×22 09:30 $324.41 → close $319.97 -97.68; SMTC×7 09:30 $141.76 → close $131.17 -74.13; CIEN×2 09:30 $400.42 → close $378.44 -43.96 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,278.90 | ▲ 09:30 equity $10,056.66 vs yday $9,993.31 (+63.35) | 09:30 open · cash $1,278.90 (unchanged overnight, no fees) · equity $10,056.66 vs prior close $9,993.31 (+63.35) · 3 name(s) re-marked at the open (per-name table). KEYS×22 yday $319.97 → 09:30 $322.49 +55.44; SMTC×7 yday $131.17 → 09:30 $132.30 +7.91; CIEN×2 yday $378.44 → 09:30 $378.44 +0.00 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,278.90 | ▲ close $10,074.62 vs 09:30 $10,056.66 (session +17.96) | 16:00 close · cash $1,278.90 · equity $10,074.62 vs 09:30 $10,056.66 (+17.96; session marks +17.96) · 3 name(s) marked open→close (per-name table). KEYS×22 09:30 $322.49 → close $322.70 +4.62; SMTC×7 09:30 $132.30 → close $132.96 +4.62; CIEN×2 09:30 $378.44 → close $382.80 +8.72 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,278.90 | ▼ 09:30 equity $9,998.43 vs yday $10,074.62 (-76.19) | 09:30 open · cash $1,278.90 (unchanged overnight, no fees) · equity $9,998.43 vs prior close $10,074.62 (-76.19) · 3 name(s) re-marked at the open (per-name table). KEYS×22 yday $322.70 → 09:30 $321.47 -27.06; SMTC×7 yday $132.96 → 09:30 $127.63 -37.31; CIEN×2 yday $382.80 → 09:30 $376.89 -11.82 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,278.90 | ▼ close $9,949.39 vs 09:30 $9,998.43 (session -49.04) | 16:00 close · cash $1,278.90 · equity $9,949.39 vs 09:30 $9,998.43 (-49.04; session marks -49.04) · 3 name(s) marked open→close (per-name table). KEYS×22 09:30 $321.47 → close $319.27 -48.40; SMTC×7 09:30 $127.63 → close $132.27 +32.48; CIEN×2 09:30 $376.89 → close $360.33 -33.12 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,278.90 | ▼ 09:30 equity $9,921.28 vs yday $9,949.39 (-28.11) | 09:30 open · cash $1,278.90 (unchanged overnight, no fees) · equity $9,921.28 vs prior close $9,949.39 (-28.11) · 3 name(s) re-marked at the open (per-name table). KEYS×22 yday $319.27 → 09:30 $318.04 -27.06; SMTC×7 yday $132.27 → 09:30 $133.00 +5.11; CIEN×2 yday $360.33 → 09:30 $357.25 -6.16 | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 22 | $318.04 | $2.12 | $-144.32 | $8,273.66 | ▼ -144.32 after sell → book $9,919.16; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 7 | $133.00 | $2.03 | $-65.36 | $9,202.63 | ▼ -65.36 after sell → book $9,917.13; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 2 | $357.25 | $2.02 | $-90.35 | $9,915.11 | ▼ -90.35 after sell → book $9,915.11; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,915.11 | ▲ close $9,915.11 vs 09:30 $9,921.28 (session +0.00) | 16:00 close · cash $9,915.11 · no lots left · equity $9,915.11. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,915.11 | ▲ 09:30 equity $9,915.11 vs yday $9,915.11 (+0.00) | 09:30 open · cash $9,915.11 · no holdings · equity $9,915.11 vs prior close $9,915.11 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 19 | $351.74 | $2.05 | — | $3,230.00 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $6940.58 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $2,255.39 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $991.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 30 | $32.31 | $2.08 | — | $1,284.01 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $991.51 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 62 | $15.87 | $2.18 | — | $297.89 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $991.51 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $297.89 | ▲ close $10,174.31 vs 09:30 $9,915.11 (session +267.50) | 16:00 close · cash $297.89 · equity $10,174.31 vs 09:30 $9,915.11 (+259.20; session marks +267.50) · 4 name(s) marked open→close (per-name table). AVGO×19 09:30 $351.74 → close $357.16 +102.98; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×30 09:30 $32.31 → close $33.66 +40.50; FRNM×62 09:30 $15.87 → close $16.90 +63.86 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $297.89 | ▲ 09:30 equity $10,180.35 vs yday $10,174.31 (+6.04) | 09:30 open · cash $297.89 (unchanged overnight, no fees) · equity $10,180.35 vs prior close $10,174.31 (+6.04) · 4 name(s) re-marked at the open (per-name table). AVGO×19 yday $357.16 → 09:30 $359.70 +48.26; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×30 yday $33.66 → 09:30 $33.46 -6.00; FRNM×62 yday $16.90 → 09:30 $16.40 -31.00 | — |
| 2026-09-04 09:30 ET | **BUY** | `MMED` | 1 | $23.84 | $0.24 | — | $273.81 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list oppset; 🔵; ⚪; ret5=+22.2; leftover $44.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $273.81 | ▲ close $10,198.00 vs 09:30 $10,180.35 (session +17.89) | 16:00 close · cash $273.81 · equity $10,198.00 vs 09:30 $10,180.35 (+17.65; session marks +17.89) · 5 name(s) marked open→close (per-name table). AVGO×19 09:30 $359.70 → close $357.90 -34.20; DELL×2 09:30 $513.78 → close $524.14 +20.72; CXW×30 09:30 $33.46 → close $34.71 +37.50; FRNM×62 09:30 $16.40 → close $16.31 -5.58; MMED×1 09:30 $23.84 → close $23.29 -0.55 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $273.81 | ▲ 09:30 equity $10,321.77 vs yday $10,198.00 (+123.77) | 09:30 open · cash $273.81 (unchanged overnight, no fees) · equity $10,321.77 vs prior close $10,198.00 (+123.77) · 5 name(s) re-marked at the open (per-name table). AVGO×19 yday $357.90 → 09:30 $363.68 +109.82; DELL×2 yday $524.14 → 09:30 $521.15 -5.98; CXW×30 yday $34.71 → 09:30 $34.49 -6.60; FRNM×62 yday $16.31 → 09:30 $16.74 +26.66; MMED×1 yday $23.29 → 09:30 $23.16 -0.13 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $273.81 | ▲ close $10,410.41 vs 09:30 $10,321.77 (session +88.64) | 16:00 close · cash $273.81 · equity $10,410.41 vs 09:30 $10,321.77 (+88.64; session marks +88.64) · 5 name(s) marked open→close (per-name table). AVGO×19 09:30 $363.68 → close $368.56 +92.72; DELL×2 09:30 $521.15 → close $533.88 +25.46; CXW×30 09:30 $34.49 → close $35.05 +16.80; FRNM×62 09:30 $16.74 → close $15.99 -46.50; MMED×1 09:30 $23.16 → close $23.32 +0.16 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $273.81 | ▼ 09:30 equity $10,374.56 vs yday $10,410.41 (-35.85) | 09:30 open · cash $273.81 (unchanged overnight, no fees) · equity $10,374.56 vs prior close $10,410.41 (-35.85) · 5 name(s) re-marked at the open (per-name table). AVGO×19 yday $368.56 → 09:30 $366.23 -44.27; DELL×2 yday $533.88 → 09:30 $538.47 +9.18; CXW×30 yday $35.05 → 09:30 $35.09 +1.20; FRNM×62 yday $15.99 → 09:30 $15.96 -1.86; MMED×1 yday $23.32 → 09:30 $23.22 -0.10 | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 19 | $366.23 | $2.11 | $+271.15 | $7,230.07 | ▲ +271.15 after sell → book $10,372.45; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 2 | $538.47 | $2.02 | $+100.31 | $8,304.99 | ▲ +100.31 after sell → book $10,370.43; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CXW` | 30 | $35.09 | $2.10 | $+79.22 | $9,355.59 | ▲ +79.22 after sell → book $10,368.33; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 62 | $15.96 | $2.20 | $+1.21 | $10,342.92 | ▲ +1.21 after sell → book $10,366.14; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,342.92 | ▼ close $10,365.68 vs 09:30 $10,374.56 (session -0.46) | 16:00 close · cash $10,342.92 · equity $10,365.68 vs 09:30 $10,374.56 (-8.88; session marks -0.46) · 1 name(s) marked open→close (per-name table). MMED×1 09:30 $23.22 → close $22.76 -0.46 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,342.92 | ▼ 09:30 equity $10,365.46 vs yday $10,365.68 (-0.22) | 09:30 open · cash $10,342.92 (unchanged overnight, no fees) · equity $10,365.46 vs prior close $10,365.68 (-0.22) · 1 name(s) re-marked at the open (per-name table). MMED×1 yday $22.76 → 09:30 $22.54 -0.22 | — |
| 2026-09-10 09:30 ET | **SELL** | `MMED` | 1 | $22.54 | $0.25 | $-1.79 | $10,365.21 | ▼ -1.79 after sell → book $10,365.21; vs 09:30 mark -0.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,365.21 | ▲ close $10,365.21 vs 09:30 $10,365.46 (session +0.00) | 16:00 close · cash $10,365.21 · no lots left · equity $10,365.21. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,365.21 | ▲ 09:30 equity $10,365.21 vs yday $10,365.21 (-0.00) | 09:30 open · cash $10,365.21 · no holdings · equity $10,365.21 vs prior close $10,365.21 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 44 | $164.43 | $2.12 | — | $3,128.17 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $7255.65 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BTI` | 18 | $56.03 | $2.04 | — | $2,117.58 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list oppset; 🔵; ret5=-0.8; leftover $1036.52 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 4 | $242.17 | $2.00 | — | $1,146.90 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list earn_react; ret5=-11.1; leftover $1036.52 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CNQ` | 20 | $49.94 | $2.05 | — | $146.05 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list oppset; ret5=+1.7; leftover $1036.52 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $146.05 | ▼ close $9,763.01 vs 09:30 $10,365.21 (session -593.98) | 16:00 close · cash $146.05 · equity $9,763.01 vs 09:30 $10,365.21 (-602.20; session marks -593.98) · 4 name(s) marked open→close (per-name table). ORCL×44 09:30 $164.43 → close $150.28 -622.60; BTI×18 09:30 $56.03 → close $55.24 -14.22; ADBE×4 09:30 $242.17 → close $252.23 +40.24; CNQ×20 09:30 $49.94 → close $50.07 +2.60 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $146.05 | ▼ 09:30 equity $9,457.93 vs yday $9,763.01 (-305.08) | 09:30 open · cash $146.05 (unchanged overnight, no fees) · equity $9,457.93 vs prior close $9,763.01 (-305.08) · 4 name(s) re-marked at the open (per-name table). ORCL×44 yday $150.28 → 09:30 $141.42 -389.84; BTI×18 yday $55.24 → 09:30 $57.12 +33.84; ADBE×4 yday $252.23 → 09:30 $261.51 +37.12; CNQ×20 yday $50.07 → 09:30 $50.76 +13.80 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $146.05 | ▲ close $9,616.83 vs 09:30 $9,457.93 (session +158.90) | 16:00 close · cash $146.05 · equity $9,616.83 vs 09:30 $9,457.93 (+158.90; session marks +158.90) · 4 name(s) marked open→close (per-name table). ORCL×44 09:30 $141.42 → close $144.79 +148.28; BTI×18 09:30 $57.12 → close $57.29 +3.06; ADBE×4 09:30 $261.51 → close $265.60 +16.36; CNQ×20 09:30 $50.76 → close $50.32 -8.80 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $146.05 | ▼ 09:30 equity $9,530.37 vs yday $9,616.83 (-86.46) | 09:30 open · cash $146.05 (unchanged overnight, no fees) · equity $9,530.37 vs prior close $9,616.83 (-86.46) · 4 name(s) re-marked at the open (per-name table). ORCL×44 yday $144.79 → 09:30 $143.46 -58.52; BTI×18 yday $57.29 → 09:30 $56.46 -14.94; ADBE×4 yday $265.60 → 09:30 $261.70 -15.60; CNQ×20 yday $50.32 → 09:30 $50.45 +2.60 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $146.05 | ▼ close $9,400.85 vs 09:30 $9,530.37 (session -129.52) | 16:00 close · cash $146.05 · equity $9,400.85 vs 09:30 $9,530.37 (-129.52; session marks -129.52) · 4 name(s) marked open→close (per-name table). ORCL×44 09:30 $143.46 → close $140.35 -136.84; BTI×18 09:30 $56.46 → close $56.52 +1.08; ADBE×4 09:30 $261.70 → close $257.76 -15.76; CNQ×20 09:30 $50.45 → close $51.55 +22.00 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $146.05 | ▼ 09:30 equity $9,356.65 vs yday $9,400.85 (-44.20) | 09:30 open · cash $146.05 (unchanged overnight, no fees) · equity $9,356.65 vs prior close $9,400.85 (-44.20) · 4 name(s) re-marked at the open (per-name table). ORCL×44 yday $140.35 → 09:30 $140.03 -14.08; BTI×18 yday $56.52 → 09:30 $56.54 +0.36; ADBE×4 yday $257.76 → 09:30 $253.34 -17.68; CNQ×20 yday $51.55 → 09:30 $50.91 -12.80 | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 44 | $140.03 | $2.18 | $-1077.90 | $6,305.19 | ▼ -1,077.90 after sell → book $9,354.47; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BTI` | 18 | $56.54 | $2.06 | $+5.07 | $7,320.84 | ▲ +5.07 after sell → book $9,352.40; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 4 | $253.34 | $2.02 | $+40.66 | $8,332.18 | ▲ +40.66 after sell → book $9,350.38; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CNQ` | 20 | $50.91 | $2.07 | $+15.28 | $9,348.31 | ▲ +15.28 after sell → book $9,348.31; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 249 | $26.27 | $3.21 | — | $2,803.87 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $6543.82 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 4 | $189.17 | $2.00 | — | $2,045.19 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $934.83 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 23 | $39.99 | $2.06 | — | $1,123.36 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $934.83 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `AMX` | 40 | $23.18 | $2.11 | — | $194.05 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list oppset; 🔵; ret5=-0.2; leftover $934.83 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $194.05 | ▲ close $9,351.20 vs 09:30 $9,356.65 (session +12.27) | 16:00 close · cash $194.05 · equity $9,351.20 vs 09:30 $9,356.65 (-5.45; session marks +12.27) · 4 name(s) marked open→close (per-name table). WAY×249 09:30 $26.27 → close $26.59 +79.68; QCOM×4 09:30 $189.17 → close $184.84 -17.32; SM×23 09:30 $39.99 → close $38.16 -42.09; AMX×40 09:30 $23.18 → close $22.98 -8.00 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $194.05 | ▼ 09:30 equity $9,344.15 vs yday $9,351.20 (-7.05) | 09:30 open · cash $194.05 (unchanged overnight, no fees) · equity $9,344.15 vs prior close $9,351.20 (-7.05) · 4 name(s) re-marked at the open (per-name table). WAY×249 yday $26.59 → 09:30 $26.51 -19.92; QCOM×4 yday $184.84 → 09:30 $190.35 +22.04; SM×23 yday $38.16 → 09:30 $37.57 -13.57; AMX×40 yday $22.98 → 09:30 $23.09 +4.40 | — |
| 2026-09-17 09:30 ET | **BUY** | `AVTR` | 1 | $15.81 | $0.16 | — | $178.08 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $19.40 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $178.08 | ▼ close $9,321.28 vs 09:30 $9,344.15 (session -22.71) | 16:00 close · cash $178.08 · equity $9,321.28 vs 09:30 $9,344.15 (-22.87; session marks -22.71) · 5 name(s) marked open→close (per-name table). WAY×249 09:30 $26.51 → close $26.51 +0.00; QCOM×4 09:30 $190.35 → close $188.71 -6.56; SM×23 09:30 $37.57 → close $36.97 -13.80; AMX×40 09:30 $23.09 → close $23.03 -2.40; AVTR×1 09:30 $15.81 → close $15.86 +0.05 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $178.08 | ▲ 09:30 equity $9,433.87 vs yday $9,321.28 (+112.59) | 09:30 open · cash $178.08 (unchanged overnight, no fees) · equity $9,433.87 vs prior close $9,321.28 (+112.59) · 5 name(s) re-marked at the open (per-name table). WAY×249 yday $26.51 → 09:30 $26.95 +109.56; QCOM×4 yday $188.71 → 09:30 $191.34 +10.52; SM×23 yday $36.97 → 09:30 $36.87 -2.30; AMX×40 yday $23.03 → 09:30 $22.90 -5.20; AVTR×1 yday $15.86 → 09:30 $15.87 +0.01 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 5 | $20.91 | $1.06 | — | $72.47 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $124.65 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 1 | $14.79 | $0.15 | — | $57.53 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $17.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 1 | $14.07 | $0.14 | — | $43.31 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $17.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.31 | ▼ close $9,040.64 vs 09:30 $9,433.87 (session -391.87) | 16:00 close · cash $43.31 · equity $9,040.64 vs 09:30 $9,433.87 (-393.23; session marks -391.87) · 8 name(s) marked open→close (per-name table). WAY×249 09:30 $26.95 → close $25.66 -321.21; QCOM×4 09:30 $191.34 → close $177.72 -54.48; SM×23 09:30 $36.87 → close $36.97 +2.30; AMX×40 09:30 $22.90 → close $22.43 -18.80; AVTR×1 09:30 $15.87 → close $15.52 -0.35; TH×5 09:30 $20.91 → close $21.19 +1.40; RARE×1 09:30 $14.79 → close $14.51 -0.28; BHVN×1 09:30 $14.07 → close $13.62 -0.45 | — |

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
| 2026-08-18 | `GLOB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `GLOB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HDSN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 88.03 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 37.73 < 1 share @ 115.18 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 40.23 < 1 share @ 267.02 |
| 2026-08-26 | `ASST` | cash | leftover split 8.62 < 1 share @ 20.72 |
| 2026-08-26 | `ZYME` | cash | leftover split 8.62 < 1 share @ 27.56 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 40.23 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 5.75 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 5.75 < 1 share @ 1746.53 |
| 2026-08-27 | `CM` | cash | leftover split 5.75 < 1 share @ 118.77 |
| 2026-08-28 | `MPWR` | cash | leftover split 1021.51 < 1 share @ 1306.03 |
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
| 2026-09-04 | `CRM` | cash | leftover split 208.52 < 1 share @ 263.36 |
| 2026-09-04 | `HPE` | cash | leftover split 44.68 < 1 share @ 53.85 |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CXW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `RPRX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BTI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ADBE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CNQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `QCOM` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BTI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ADBE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CNQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CVE` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SRRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 135.83 < 1 share @ 170.85 |
| 2026-09-17 | `GME` | cash | leftover split 19.40 < 1 share @ 22.12 |
| 2026-09-17 | `JBHT` | cash | leftover split 19.40 < 1 share @ 238.60 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `GME` | cash | leftover split 17.81 < 1 share @ 22.90 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `WAY` | 249 | 2026-09-16 @ $26.27 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $6543.82 |
| `QCOM` | 4 | 2026-09-16 @ $189.17 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $934.83 |
| `SM` | 23 | 2026-09-16 @ $39.99 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $934.83 |
| `AMX` | 40 | 2026-09-16 @ $23.18 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list oppset; 🔵; ret5=-0.2; leftover $934.83 |
| `AVTR` | 1 | 2026-09-17 @ $15.81 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $19.40 |
| `TH` | 5 | 2026-09-18 @ $20.91 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $124.65 |
| `RARE` | 1 | 2026-09-18 @ $14.79 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $17.81 |
| `BHVN` | 1 | 2026-09-18 @ $14.07 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $17.81 |
