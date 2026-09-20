# Factor mine action — `union_news_g_cam61_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · merged news🟢 and cameras +6 −≤1

Cash book **-14.78%** ($8,522) · signal-only (no cash/fees) was -13.88%. Starts YES **1/26**. Fills 60 · skips 75 · realized $-1001.94.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $17.03.

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
| 2026-08-20 | `BHP` | 21 | — | $91.01 | +0.00 | $93.63 | +55.02 | +55.02 | +0.00 | +55.02 |
| 2026-08-20 | `APA` | 44 | — | $44.76 | +0.00 | $44.39 | -16.28 | -16.28 | +0.00 | -16.28 |
| 2026-08-20 | `AUTL` | 809 | — | $2.47 | +0.00 | $2.46 | -8.09 | -8.09 | +0.00 | -8.09 |
| 2026-08-20 | `CRSP` | 34 | — | $58.73 | +0.00 | $58.12 | -20.74 | -20.74 | +0.00 | -20.74 |
| 2026-08-20 | `MRK` | 13 | — | $150.78 | +0.00 | $148.99 | -23.27 | -23.27 | +0.00 | -23.27 |
| 2026-08-21 | `BHP` | 21 | $93.63 | $95.72 | +43.89 | $97.03 | +27.51 | +71.40 | +98.91 | +126.42 |
| 2026-08-21 | `APA` | 44 | $44.39 | $44.52 | +5.72 | $43.39 | -49.72 | -44.00 | -10.56 | -60.28 |
| 2026-08-21 | `AUTL` | 809 | $2.46 | $2.47 | +8.09 | $2.41 | -48.54 | -40.45 | +0.00 | -48.54 |
| 2026-08-21 | `CRSP` | 34 | $58.12 | $59.72 | +54.40 | $59.50 | -7.48 | +46.92 | +33.66 | +26.18 |
| 2026-08-21 | `MRK` | 13 | $148.99 | $149.12 | +1.69 | $152.55 | +44.59 | +46.28 | -21.58 | +23.01 |
| 2026-08-21 | `ABTC` | 2 | — | $8.66 | +0.00 | $7.93 | -1.46 | -1.46 | +0.00 | -1.46 |
| 2026-08-24 | `BHP` | 21 | $97.03 | $97.31 | +5.88 | $97.13 | -3.78 | +2.10 | +132.30 | +128.52 |
| 2026-08-24 | `APA` | 44 | $43.39 | $42.93 | -20.24 | $42.96 | +1.32 | -18.92 | -80.52 | -79.20 |
| 2026-08-24 | `AUTL` | 809 | $2.41 | $2.40 | -8.09 | $2.34 | -48.54 | -56.63 | -56.63 | -105.17 |
| 2026-08-24 | `CRSP` | 34 | $59.50 | $58.75 | -25.50 | $57.08 | -56.95 | -82.45 | +0.68 | -56.27 |
| 2026-08-24 | `MRK` | 13 | $152.55 | $150.72 | -23.79 | $150.66 | -0.78 | -24.57 | -0.78 | -1.56 |
| 2026-08-24 | `ABTC` | 2 | $7.93 | $8.00 | +0.14 | $8.64 | +1.28 | +1.42 | -1.32 | -0.04 |
| 2026-08-25 | `BHP` | 21 | $97.13 | $95.86 | -26.67 | — | +0.00 | -26.67 | +101.85 | — |
| 2026-08-25 | `APA` | 44 | $42.96 | $41.38 | -69.52 | — | +0.00 | -69.52 | -148.72 | — |
| 2026-08-25 | `AUTL` | 809 | $2.34 | $2.38 | +32.36 | — | +0.00 | +32.36 | -72.81 | — |
| 2026-08-25 | `CRSP` | 34 | $57.08 | $57.93 | +29.07 | — | +0.00 | +29.07 | -27.20 | — |
| 2026-08-25 | `MRK` | 13 | $150.66 | $151.00 | +4.42 | — | +0.00 | +4.42 | +2.86 | — |
| 2026-08-25 | `ABTC` | 2 | $8.64 | $8.62 | -0.04 | $9.24 | +1.24 | +1.20 | -0.08 | +1.16 |
| 2026-08-25 | `AU` | 41 | — | $118.52 | +0.00 | $123.39 | +199.67 | +199.67 | +0.00 | +199.67 |
| 2026-08-25 | `FCX` | 63 | — | $77.13 | +0.00 | $79.91 | +175.14 | +175.14 | +0.00 | +175.14 |
| 2026-08-26 | `ABTC` | 2 | $9.24 | $8.84 | -0.80 | — | +0.00 | -0.80 | +0.36 | — |
| 2026-08-26 | `AU` | 41 | $123.39 | $119.80 | -147.19 | $118.11 | -69.29 | -216.48 | +52.48 | -16.81 |
| 2026-08-26 | `FCX` | 63 | $79.91 | $79.34 | -35.91 | $79.00 | -21.42 | -57.33 | +139.23 | +117.81 |
| 2026-08-26 | `ASST` | 2 | — | $20.72 | +0.00 | $21.50 | +1.56 | +1.56 | +0.00 | +1.56 |
| 2026-08-26 | `AMX` | 2 | — | $23.75 | +0.00 | $23.62 | -0.26 | -0.26 | +0.00 | -0.26 |
| 2026-08-27 | `AU` | 41 | $118.11 | $117.41 | -28.70 | $118.40 | +40.59 | +11.89 | -45.51 | -4.92 |
| 2026-08-27 | `FCX` | 63 | $79.00 | $78.83 | -10.71 | $78.42 | -25.83 | -36.54 | +107.10 | +81.27 |
| 2026-08-27 | `ASST` | 2 | $21.50 | $22.45 | +1.90 | $23.12 | +1.34 | +3.24 | +3.46 | +4.80 |
| 2026-08-27 | `AMX` | 2 | $23.62 | $23.77 | +0.30 | $23.50 | -0.54 | -0.24 | +0.04 | -0.50 |
| 2026-08-28 | `AU` | 41 | $118.40 | $119.19 | +32.39 | — | +0.00 | +32.39 | +27.47 | — |
| 2026-08-28 | `FCX` | 63 | $78.42 | $78.57 | +9.45 | — | +0.00 | +9.45 | +90.72 | — |
| 2026-08-28 | `ASST` | 2 | $23.12 | $22.50 | -1.24 | $21.74 | -1.52 | -2.76 | +3.56 | +2.04 |
| 2026-08-28 | `AMX` | 2 | $23.50 | $23.64 | +0.28 | $23.18 | -0.92 | -0.64 | -0.22 | -1.14 |
| 2026-08-28 | `KEYS` | 4 | — | $324.41 | +0.00 | $319.97 | -17.76 | -17.76 | +0.00 | -17.76 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 5 | — | $240.22 | +0.00 | $236.98 | -16.20 | -16.20 | +0.00 | -16.20 |
| 2026-08-28 | `PLAB` | 46 | — | $30.01 | +0.00 | $27.73 | -104.88 | -104.88 | +0.00 | -104.88 |
| 2026-08-28 | `SEDG` | 42 | — | $32.90 | +0.00 | $31.41 | -62.58 | -62.58 | +0.00 | -62.58 |
| 2026-08-31 | `ASST` | 2 | $21.74 | $22.54 | +1.60 | — | +0.00 | +1.60 | +3.64 | — |
| 2026-08-31 | `AMX` | 2 | $23.18 | $23.37 | +0.38 | — | +0.00 | +0.38 | -0.76 | — |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | $322.70 | +0.84 | +10.92 | -7.68 | -6.84 |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | $132.96 | +5.94 | +16.11 | -85.14 | -79.20 |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | $382.80 | +13.08 | +13.08 | -65.94 | -52.86 |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | $1267.77 | +5.87 | +11.51 | -44.13 | -38.26 |
| 2026-08-31 | `DDOG` | 5 | $236.98 | $233.97 | -15.07 | $237.04 | +15.37 | +0.30 | -31.27 | -15.90 |
| 2026-08-31 | `PLAB` | 46 | $27.73 | $28.04 | +14.26 | $28.14 | +4.60 | +18.86 | -90.62 | -86.02 |
| 2026-08-31 | `SEDG` | 42 | $31.41 | $31.15 | -10.92 | $32.20 | +44.10 | +33.18 | -73.50 | -29.40 |
| 2026-09-01 | `KEYS` | 4 | $322.70 | $321.47 | -4.92 | $319.27 | -8.80 | -13.72 | -11.76 | -20.56 |
| 2026-09-01 | `SMTC` | 9 | $132.96 | $127.63 | -47.97 | $132.27 | +41.76 | -6.21 | -127.17 | -85.41 |
| 2026-09-01 | `CIEN` | 3 | $382.80 | $376.89 | -17.73 | $360.33 | -49.68 | -67.41 | -70.59 | -120.27 |
| 2026-09-01 | `MPWR` | 1 | $1267.77 | $1245.11 | -22.66 | $1225.96 | -19.15 | -41.81 | -60.92 | -80.07 |
| 2026-09-01 | `DDOG` | 5 | $237.04 | $232.88 | -20.80 | $223.84 | -45.20 | -66.00 | -36.70 | -81.90 |
| 2026-09-01 | `PLAB` | 46 | $28.14 | $27.69 | -20.70 | $27.33 | -16.56 | -37.26 | -106.72 | -123.28 |
| 2026-09-01 | `SEDG` | 42 | $32.20 | $31.87 | -13.86 | $32.49 | +26.04 | +12.18 | -43.26 | -17.22 |
| 2026-09-02 | `KEYS` | 4 | $319.27 | $318.04 | -4.92 | — | +0.00 | -4.92 | -25.48 | — |
| 2026-09-02 | `SMTC` | 9 | $132.27 | $133.00 | +6.57 | — | +0.00 | +6.57 | -78.84 | — |
| 2026-09-02 | `CIEN` | 3 | $360.33 | $357.25 | -9.24 | — | +0.00 | -9.24 | -129.51 | — |
| 2026-09-02 | `MPWR` | 1 | $1225.96 | $1224.92 | -1.04 | — | +0.00 | -1.04 | -81.11 | — |
| 2026-09-02 | `DDOG` | 5 | $223.84 | $219.46 | -21.90 | — | +0.00 | -21.90 | -103.80 | — |
| 2026-09-02 | `PLAB` | 46 | $27.33 | $27.41 | +3.68 | — | +0.00 | +3.68 | -119.60 | — |
| 2026-09-02 | `SEDG` | 42 | $32.49 | $32.42 | -2.94 | — | +0.00 | -2.94 | -20.16 | — |
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
| 2026-09-04 | `HPE` | 3 | — | $53.85 | +0.00 | $52.00 | -5.55 | -5.55 | +0.00 | -5.55 |
| 2026-09-04 | `MRX` | 2 | — | $75.65 | +0.00 | $78.27 | +5.24 | +5.24 | +0.00 | +5.24 |
| 2026-09-08 | `AVGO` | 5 | $357.90 | $363.68 | +28.90 | $368.56 | +24.40 | +53.30 | +59.70 | +84.10 |
| 2026-09-08 | `DELL` | 3 | $524.14 | $521.15 | -8.97 | $533.88 | +38.19 | +29.22 | +104.52 | +142.71 |
| 2026-09-08 | `CXW` | 57 | $34.71 | $34.49 | -12.54 | $35.05 | +31.92 | +19.38 | +124.26 | +156.18 |
| 2026-09-08 | `FRNM` | 117 | $16.31 | $16.74 | +50.31 | $15.99 | -87.75 | -37.44 | +101.79 | +14.04 |
| 2026-09-08 | `MMED` | 78 | $23.29 | $23.16 | -10.14 | $23.32 | +12.48 | +2.34 | -56.16 | -43.68 |
| 2026-09-08 | `HPE` | 3 | $52.00 | $52.29 | +0.87 | $56.03 | +11.22 | +12.09 | -4.68 | +6.54 |
| 2026-09-08 | `MRX` | 2 | $78.27 | $78.84 | +1.14 | $76.71 | -4.26 | -3.12 | +6.38 | +2.12 |
| 2026-09-09 | `AVGO` | 5 | $368.56 | $366.23 | -11.65 | — | +0.00 | -11.65 | +72.45 | — |
| 2026-09-09 | `DELL` | 3 | $533.88 | $538.47 | +13.77 | — | +0.00 | +13.77 | +156.48 | — |
| 2026-09-09 | `CXW` | 57 | $35.05 | $35.09 | +2.28 | — | +0.00 | +2.28 | +158.46 | — |
| 2026-09-09 | `FRNM` | 117 | $15.99 | $15.96 | -3.51 | — | +0.00 | -3.51 | +10.53 | — |
| 2026-09-09 | `MMED` | 78 | $23.32 | $23.22 | -7.80 | — | +0.00 | -7.80 | -51.48 | — |
| 2026-09-09 | `HPE` | 3 | $56.03 | $56.94 | +2.73 | $58.90 | +5.88 | +8.61 | +9.27 | +15.15 |
| 2026-09-09 | `MRX` | 2 | $76.71 | $76.60 | -0.22 | $75.72 | -1.76 | -1.98 | +1.90 | +0.14 |
| 2026-09-10 | `HPE` | 3 | $58.90 | $57.80 | -3.30 | — | +0.00 | -3.30 | +11.85 | — |
| 2026-09-10 | `MRX` | 2 | $75.72 | $75.00 | -1.44 | — | +0.00 | -1.44 | -1.30 | — |
| 2026-09-11 | `ORCL` | 29 | — | $164.43 | +0.00 | $150.28 | -410.35 | -410.35 | +0.00 | -410.35 |
| 2026-09-11 | `BTI` | 86 | — | $56.03 | +0.00 | $55.24 | -67.94 | -67.94 | +0.00 | -67.94 |
| 2026-09-14 | `ORCL` | 29 | $150.28 | $141.42 | -256.94 | $144.79 | +97.73 | -159.21 | -667.29 | -569.56 |
| 2026-09-14 | `BTI` | 86 | $55.24 | $57.12 | +161.68 | $57.29 | +14.62 | +176.30 | +93.74 | +108.36 |
| 2026-09-15 | `ORCL` | 29 | $144.79 | $143.46 | -38.57 | $140.35 | -90.19 | -128.76 | -608.13 | -698.32 |
| 2026-09-15 | `BTI` | 86 | $57.29 | $56.46 | -71.38 | $56.52 | +5.16 | -66.22 | +36.98 | +42.14 |
| 2026-09-16 | `ORCL` | 29 | $140.35 | $140.03 | -9.28 | — | +0.00 | -9.28 | -707.60 | — |
| 2026-09-16 | `BTI` | 86 | $56.52 | $56.54 | +1.72 | — | +0.00 | +1.72 | +43.86 | — |
| 2026-09-16 | `WAY` | 114 | — | $26.27 | +0.00 | $26.59 | +36.48 | +36.48 | +0.00 | +36.48 |
| 2026-09-16 | `QCOM` | 15 | — | $189.17 | +0.00 | $184.84 | -64.95 | -64.95 | +0.00 | -64.95 |
| 2026-09-16 | `SM` | 75 | — | $39.99 | +0.00 | $38.16 | -137.25 | -137.25 | +0.00 | -137.25 |
| 2026-09-17 | `WAY` | 114 | $26.59 | $26.51 | -9.12 | $26.51 | +0.00 | -9.12 | +27.36 | +27.36 |
| 2026-09-17 | `QCOM` | 15 | $184.84 | $190.35 | +82.65 | $188.71 | -24.60 | +58.05 | +17.70 | -6.90 |
| 2026-09-17 | `SM` | 75 | $38.16 | $37.57 | -44.25 | $36.97 | -45.00 | -89.25 | -181.50 | -226.50 |
| 2026-09-17 | `AVTR` | 3 | — | $15.81 | +0.00 | $15.86 | +0.15 | +0.15 | +0.00 | +0.15 |
| 2026-09-17 | `GME` | 2 | — | $22.12 | +0.00 | $22.77 | +1.30 | +1.30 | +0.00 | +1.30 |
| 2026-09-18 | `WAY` | 114 | $26.51 | $26.95 | +50.16 | $25.66 | -147.06 | -96.90 | +77.52 | -69.54 |
| 2026-09-18 | `QCOM` | 15 | $188.71 | $191.34 | +39.45 | $177.72 | -204.30 | -164.85 | +32.55 | -171.75 |
| 2026-09-18 | `SM` | 75 | $36.97 | $36.87 | -7.50 | $36.97 | +7.50 | +0.00 | -234.00 | -226.50 |
| 2026-09-18 | `AVTR` | 3 | $15.86 | $15.87 | +0.03 | $15.52 | -1.05 | -1.02 | +0.18 | -0.87 |
| 2026-09-18 | `GME` | 2 | $22.77 | $22.90 | +0.26 | $22.64 | -0.52 | -0.26 | +1.56 | +1.04 |
| 2026-09-18 | `TH` | 1 | — | $20.91 | +0.00 | $21.19 | +0.28 | +0.28 | +0.00 | +0.28 |
| 2026-09-18 | `RARE` | 1 | — | $14.79 | +0.00 | $14.51 | -0.28 | -0.28 | +0.00 | -0.28 |
| 2026-09-18 | `BHVN` | 1 | — | $14.07 | +0.00 | $13.62 | -0.45 | -0.45 | +0.00 | -0.45 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | -13.36 | BHP, APA, AUTL, CRSP, MRK | — | $145.43 | $9,967.91 | BHP×21, APA×44, AUTL×809, CRSP×34, MRK×13 |
| 2026-08-21 | +3.25 | $145.43 | BHP×21, APA×44, AUTL×809, CRSP×34, MRK×13 | $10,081.70 | +113.79 | -35.10 | ABTC | — | $127.93 | $10,046.42 | BHP×21, APA×44, AUTL×809, CRSP×34, MRK×13, ABTC×2 |
| 2026-08-24 | -5.17 | $127.93 | BHP×21, APA×44, AUTL×809, CRSP×34, MRK×13, ABTC×2 | $9,974.82 | -71.60 | -107.45 | — | — | $127.93 | $9,867.37 | BHP×21, APA×44, AUTL×809, CRSP×34, MRK×13, ABTC×2 |
| 2026-08-25 | +1.80 | $127.93 | BHP×21, APA×44, AUTL×809, CRSP×34, MRK×13, ABTC×2 | $9,836.99 | -30.38 | +376.05 | AU, FCX | BHP, APA, AUTL, CRSP, MRK | $77.96 | $10,189.76 | ABTC×2, AU×41, FCX×63 |
| 2026-08-26 | +2.02 | $77.96 | ABTC×2, AU×41, FCX×63 | $10,005.86 | -183.90 | -89.41 | ASST, AMX | ABTC | $5.60 | $9,915.35 | AU×41, FCX×63, ASST×2, AMX×2 |
| 2026-08-27 | — | $5.60 | AU×41, FCX×63, ASST×2, AMX×2 | $9,878.14 | -37.21 | +15.56 | — | — | $5.60 | $9,893.70 | AU×41, FCX×63, ASST×2, AMX×2 |
| 2026-08-28 | +0.75 | $5.60 | AU×41, FCX×63, ASST×2, AMX×2 | $9,934.58 | +40.88 | -414.88 | KEYS, SMTC, CIEN, MPWR, DDOG, PLAB, SEDG | AU, FCX | $779.52 | $9,501.05 | ASST×2, AMX×2, KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, PLAB×46, SEDG×42 |
| 2026-08-31 | -5.85 | $779.52 | ASST×2, AMX×2, KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, PLAB×46, SEDG×42 | $9,517.18 | +16.13 | +89.80 | — | ASST, AMX | $870.37 | $9,606.02 | KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, PLAB×46, SEDG×42 |
| 2026-09-01 | -6.30 | $870.37 | KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, PLAB×46, SEDG×42 | $9,457.38 | -148.64 | -71.59 | — | — | $870.37 | $9,385.79 | KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, PLAB×46, SEDG×42 |
| 2026-09-02 | -3.83 | $870.37 | KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, PLAB×46, SEDG×42 | $9,356.00 | -29.79 | +0.00 | — | KEYS, SMTC, CIEN, MPWR, DDOG, PLAB, SEDG | $9,341.60 | $9,341.60 | — |
| 2026-09-03 | -0.90 | $9,341.60 | — | $9,341.60 | -0.00 | +311.68 | AVGO, DELL, CXW, FRNM, MMED | — | $552.14 | $9,642.55 | AVGO×5, DELL×3, CXW×57, FRNM×117, MMED×78 |
| 2026-09-04 | +2.25 | $552.14 | AVGO×5, DELL×3, CXW×57, FRNM×117, MMED×78 | $9,577.52 | -65.03 | +39.59 | HPE, MRX | — | $236.14 | $9,613.96 | AVGO×5, DELL×3, CXW×57, FRNM×117, MMED×78, HPE×3, MRX×2 |
| 2026-09-08 | -11.47 | $236.14 | AVGO×5, DELL×3, CXW×57, FRNM×117, MMED×78, HPE×3, MRX×2 | $9,663.53 | +49.57 | +26.20 | — | — | $236.14 | $9,689.73 | AVGO×5, DELL×3, CXW×57, FRNM×117, MMED×78, HPE×3, MRX×2 |
| 2026-09-09 | -13.95 | $236.14 | AVGO×5, DELL×3, CXW×57, FRNM×117, MMED×78, HPE×3, MRX×2 | $9,685.33 | -4.40 | +4.12 | — | AVGO, DELL, CXW, FRNM, MMED | $9,350.45 | $9,678.59 | HPE×3, MRX×2 |
| 2026-09-10 | -13.28 | $9,350.45 | HPE×3, MRX×2 | $9,673.85 | -4.74 | +0.00 | — | HPE, MRX | $9,670.56 | $9,670.56 | — |
| 2026-09-11 | +0.50 | $9,670.56 | — | $9,670.56 | -0.00 | -478.29 | ORCL, BTI | — | $79.18 | $9,187.94 | ORCL×29, BTI×86 |
| 2026-09-14 | -11.00 | $79.18 | ORCL×29, BTI×86 | $9,092.68 | -95.26 | +112.35 | — | — | $79.18 | $9,205.03 | ORCL×29, BTI×86 |
| 2026-09-15 | -3.84 | $79.18 | ORCL×29, BTI×86 | $9,095.08 | -109.95 | -85.03 | — | — | $79.18 | $9,010.05 | ORCL×29, BTI×86 |
| 2026-09-16 | +5.30 | $79.18 | ORCL×29, BTI×86 | $9,002.49 | -7.56 | -165.72 | WAY, QCOM, SM | ORCL, BTI | $159.91 | $8,825.77 | WAY×114, QCOM×15, SM×75 |
| 2026-09-17 | +7.38 | $159.91 | WAY×114, QCOM×15, SM×75 | $8,855.05 | +29.28 | -68.15 | AVTR, GME | — | $67.31 | $8,785.97 | WAY×114, QCOM×15, SM×75, AVTR×3, GME×2 |
| 2026-09-18 | +4.86 | $67.31 | WAY×114, QCOM×15, SM×75, AVTR×3, GME×2 | $8,868.37 | +82.40 | -345.88 | TH, RARE, BHVN | — | $17.03 | $8,521.98 | WAY×114, QCOM×15, SM×75, AVTR×3, GME×2, TH×1, RARE×1, BHVN×1 |

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
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 21 | $91.01 | $2.05 | — | $8,086.74 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2000.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 44 | $44.76 | $2.12 | — | $6,115.18 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $2000.00 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 809 | $2.47 | $10.44 | — | $4,106.51 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $2000.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 34 | $58.73 | $2.09 | — | $2,107.60 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $2000.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRK` | 13 | $150.78 | $2.03 | — | $145.43 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list oppset; 🔵; ⚪; ret5=+14.5; leftover $2000.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $145.43 | ▼ close $9,967.91 vs 09:30 $10,000.00 (session -13.36) | 16:00 close · cash $145.43 · equity $9,967.91 vs 09:30 $10,000.00 (-32.09; session marks -13.36) · 5 name(s) marked open→close (per-name table). BHP×21 09:30 $91.01 → close $93.63 +55.02; APA×44 09:30 $44.76 → close $44.39 -16.28; AUTL×809 09:30 $2.47 → close $2.46 -8.09; CRSP×34 09:30 $58.73 → close $58.12 -20.74; MRK×13 09:30 $150.78 → close $148.99 -23.27 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $145.43 | ▲ 09:30 equity $10,081.70 vs yday $9,967.91 (+113.79) | 09:30 open · cash $145.43 (unchanged overnight, no fees) · equity $10,081.70 vs prior close $9,967.91 (+113.79) · 5 name(s) re-marked at the open (per-name table). BHP×21 yday $93.63 → 09:30 $95.72 +43.89; APA×44 yday $44.39 → 09:30 $44.52 +5.72; AUTL×809 yday $2.46 → 09:30 $2.47 +8.09; CRSP×34 yday $58.12 → 09:30 $59.72 +54.40; MRK×13 yday $148.99 → 09:30 $149.12 +1.69 | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 2 | $8.66 | $0.18 | — | $127.93 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $24.24 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.93 | ▼ close $10,046.42 vs 09:30 $10,081.70 (session -35.10) | 16:00 close · cash $127.93 · equity $10,046.42 vs 09:30 $10,081.70 (-35.28; session marks -35.10) · 6 name(s) marked open→close (per-name table). BHP×21 09:30 $95.72 → close $97.03 +27.51; APA×44 09:30 $44.52 → close $43.39 -49.72; AUTL×809 09:30 $2.47 → close $2.41 -48.54; CRSP×34 09:30 $59.72 → close $59.50 -7.48; MRK×13 09:30 $149.12 → close $152.55 +44.59; ABTC×2 09:30 $8.66 → close $7.93 -1.46 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.93 | ▼ 09:30 equity $9,974.82 vs yday $10,046.42 (-71.60) | 09:30 open · cash $127.93 (unchanged overnight, no fees) · equity $9,974.82 vs prior close $10,046.42 (-71.60) · 6 name(s) re-marked at the open (per-name table). BHP×21 yday $97.03 → 09:30 $97.31 +5.88; APA×44 yday $43.39 → 09:30 $42.93 -20.24; AUTL×809 yday $2.41 → 09:30 $2.40 -8.09; CRSP×34 yday $59.50 → 09:30 $58.75 -25.50; MRK×13 yday $152.55 → 09:30 $150.72 -23.79; ABTC×2 yday $7.93 → 09:30 $8.00 +0.14 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.93 | ▼ close $9,867.37 vs 09:30 $9,974.82 (session -107.45) | 16:00 close · cash $127.93 · equity $9,867.37 vs 09:30 $9,974.82 (-107.45; session marks -107.45) · 6 name(s) marked open→close (per-name table). BHP×21 09:30 $97.31 → close $97.13 -3.78; APA×44 09:30 $42.93 → close $42.96 +1.32; AUTL×809 09:30 $2.40 → close $2.34 -48.54; CRSP×34 09:30 $58.75 → close $57.08 -56.95; MRK×13 09:30 $150.72 → close $150.66 -0.78; ABTC×2 09:30 $8.00 → close $8.64 +1.28 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.93 | ▼ 09:30 equity $9,836.99 vs yday $9,867.37 (-30.38) | 09:30 open · cash $127.93 (unchanged overnight, no fees) · equity $9,836.99 vs prior close $9,867.37 (-30.38) · 6 name(s) re-marked at the open (per-name table). BHP×21 yday $97.13 → 09:30 $95.86 -26.67; APA×44 yday $42.96 → 09:30 $41.38 -69.52; AUTL×809 yday $2.34 → 09:30 $2.38 +32.36; CRSP×34 yday $57.08 → 09:30 $57.93 +29.07; MRK×13 yday $150.66 → 09:30 $151.00 +4.42; ABTC×2 yday $8.64 → 09:30 $8.62 -0.04 | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 21 | $95.86 | $2.08 | $+97.72 | $2,138.91 | ▲ +97.72 after sell → book $9,834.91; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 44 | $41.38 | $2.15 | $-152.99 | $3,957.48 | ▼ -152.99 after sell → book $9,832.76; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 809 | $2.38 | $10.59 | $-93.83 | $5,872.32 | ▼ -93.83 after sell → book $9,822.18; vs 09:30 mark -10.58 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 34 | $57.93 | $2.12 | $-31.41 | $7,839.82 | ▼ -31.41 after sell → book $9,820.06; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRK` | 13 | $151.00 | $2.05 | $-1.22 | $9,800.76 | ▼ -1.22 after sell → book $9,818.00; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 41 | $118.52 | $2.11 | — | $4,939.33 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $4900.38 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 63 | $77.13 | $2.18 | — | $77.96 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $4900.38 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $77.96 | ▲ close $10,189.76 vs 09:30 $9,836.99 (session +376.05) | 16:00 close · cash $77.96 · equity $10,189.76 vs 09:30 $9,836.99 (+352.77; session marks +376.05) · 3 name(s) marked open→close (per-name table). ABTC×2 09:30 $8.62 → close $9.24 +1.24; AU×41 09:30 $118.52 → close $123.39 +199.67; FCX×63 09:30 $77.13 → close $79.91 +175.14 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $77.96 | ▼ 09:30 equity $10,005.86 vs yday $10,189.76 (-183.90) | 09:30 open · cash $77.96 (unchanged overnight, no fees) · equity $10,005.86 vs prior close $10,189.76 (-183.90) · 3 name(s) re-marked at the open (per-name table). ABTC×2 yday $9.24 → 09:30 $8.84 -0.80; AU×41 yday $123.39 → 09:30 $119.80 -147.19; FCX×63 yday $79.91 → 09:30 $79.34 -35.91 | — |
| 2026-08-26 09:30 ET | **SELL** | `ABTC` | 2 | $8.84 | $0.20 | $-0.02 | $95.44 | ▼ -0.02 after sell → book $10,005.66; vs 09:30 mark -0.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `ASST` | 2 | $20.72 | $0.42 | — | $53.58 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list oppset; 🔵; ret5=+67.1; leftover $47.72 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AMX` | 2 | $23.75 | $0.48 | — | $5.60 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list oppset; 🔵; ret5=+0.5; leftover $47.72 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.60 | ▼ close $9,915.35 vs 09:30 $10,005.86 (session -89.41) | 16:00 close · cash $5.60 · equity $9,915.35 vs 09:30 $10,005.86 (-90.51; session marks -89.41) · 4 name(s) marked open→close (per-name table). AU×41 09:30 $119.80 → close $118.11 -69.29; FCX×63 09:30 $79.34 → close $79.00 -21.42; ASST×2 09:30 $20.72 → close $21.50 +1.56; AMX×2 09:30 $23.75 → close $23.62 -0.26 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.60 | ▼ 09:30 equity $9,878.14 vs yday $9,915.35 (-37.21) | 09:30 open · cash $5.60 (unchanged overnight, no fees) · equity $9,878.14 vs prior close $9,915.35 (-37.21) · 4 name(s) re-marked at the open (per-name table). AU×41 yday $118.11 → 09:30 $117.41 -28.70; FCX×63 yday $79.00 → 09:30 $78.83 -10.71; ASST×2 yday $21.50 → 09:30 $22.45 +1.90; AMX×2 yday $23.62 → 09:30 $23.77 +0.30 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.60 | ▲ close $9,893.70 vs 09:30 $9,878.14 (session +15.56) | 16:00 close · cash $5.60 · equity $9,893.70 vs 09:30 $9,878.14 (+15.56; session marks +15.56) · 4 name(s) marked open→close (per-name table). AU×41 09:30 $117.41 → close $118.40 +40.59; FCX×63 09:30 $78.83 → close $78.42 -25.83; ASST×2 09:30 $22.45 → close $23.12 +1.34; AMX×2 09:30 $23.77 → close $23.50 -0.54 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.60 | ▲ 09:30 equity $9,934.58 vs yday $9,893.70 (+40.88) | 09:30 open · cash $5.60 (unchanged overnight, no fees) · equity $9,934.58 vs prior close $9,893.70 (+40.88) · 4 name(s) re-marked at the open (per-name table). AU×41 yday $118.40 → 09:30 $119.19 +32.39; FCX×63 yday $78.42 → 09:30 $78.57 +9.45; ASST×2 yday $23.12 → 09:30 $22.50 -1.24; AMX×2 yday $23.50 → 09:30 $23.64 +0.28 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 41 | $119.19 | $2.16 | $+23.19 | $4,890.23 | ▲ +23.19 after sell → book $9,932.42; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 63 | $78.57 | $2.23 | $+86.31 | $9,837.91 | ▲ +86.31 after sell → book $9,930.19; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $8,538.27 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1405.42 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $7,260.41 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $1405.42 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,057.15 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1405.42 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $4,749.13 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1405.42 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $3,546.02 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1405.42 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PLAB` | 46 | $30.01 | $2.13 | — | $2,163.43 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list oppset; 🔵; ret5=-0.9; leftover $1405.42 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 42 | $32.90 | $2.12 | — | $779.52 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1405.42 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $779.52 | ▼ close $9,501.05 vs 09:30 $9,934.58 (session -414.88) | 16:00 close · cash $779.52 · equity $9,501.05 vs 09:30 $9,934.58 (-433.53; session marks -414.88) · 9 name(s) marked open→close (per-name table). ASST×2 09:30 $22.50 → close $21.74 -1.52; AMX×2 09:30 $23.64 → close $23.18 -0.92; KEYS×4 09:30 $324.41 → close $319.97 -17.76; SMTC×9 09:30 $141.76 → close $131.17 -95.31; CIEN×3 09:30 $400.42 → close $378.44 -65.94; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×5 09:30 $240.22 → close $236.98 -16.20; PLAB×46 09:30 $30.01 → close $27.73 -104.88; SEDG×42 09:30 $32.90 → close $31.41 -62.58 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $779.52 | ▲ 09:30 equity $9,517.18 vs yday $9,501.05 (+16.13) | 09:30 open · cash $779.52 (unchanged overnight, no fees) · equity $9,517.18 vs prior close $9,501.05 (+16.13) · 9 name(s) re-marked at the open (per-name table). ASST×2 yday $21.74 → 09:30 $22.54 +1.60; AMX×2 yday $23.18 → 09:30 $23.37 +0.38; KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×5 yday $236.98 → 09:30 $233.97 -15.07; PLAB×46 yday $27.73 → 09:30 $28.04 +14.26; SEDG×42 yday $31.41 → 09:30 $31.15 -10.92 | — |
| 2026-08-31 09:30 ET | **SELL** | `ASST` | 2 | $22.54 | $0.48 | $+2.74 | $824.12 | ▲ +2.74 after sell → book $9,516.71; vs 09:30 mark -0.47 | dropped from list after 3 sess (min 3) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `AMX` | 2 | $23.37 | $0.49 | $-1.73 | $870.37 | ▼ -1.73 after sell → book $9,516.21; vs 09:30 mark -0.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $870.37 | ▲ close $9,606.02 vs 09:30 $9,517.18 (session +89.80) | 16:00 close · cash $870.37 · equity $9,606.02 vs 09:30 $9,517.18 (+88.84; session marks +89.80) · 7 name(s) marked open→close (per-name table). KEYS×4 09:30 $322.49 → close $322.70 +0.84; SMTC×9 09:30 $132.30 → close $132.96 +5.94; CIEN×3 09:30 $378.44 → close $382.80 +13.08; MPWR×1 09:30 $1261.90 → close $1267.77 +5.87; DDOG×5 09:30 $233.97 → close $237.04 +15.37; PLAB×46 09:30 $28.04 → close $28.14 +4.60; SEDG×42 09:30 $31.15 → close $32.20 +44.10 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $870.37 | ▼ 09:30 equity $9,457.38 vs yday $9,606.02 (-148.64) | 09:30 open · cash $870.37 (unchanged overnight, no fees) · equity $9,457.38 vs prior close $9,606.02 (-148.64) · 7 name(s) re-marked at the open (per-name table). KEYS×4 yday $322.70 → 09:30 $321.47 -4.92; SMTC×9 yday $132.96 → 09:30 $127.63 -47.97; CIEN×3 yday $382.80 → 09:30 $376.89 -17.73; MPWR×1 yday $1267.77 → 09:30 $1245.11 -22.66; DDOG×5 yday $237.04 → 09:30 $232.88 -20.80; PLAB×46 yday $28.14 → 09:30 $27.69 -20.70; SEDG×42 yday $32.20 → 09:30 $31.87 -13.86 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $870.37 | ▼ close $9,385.79 vs 09:30 $9,457.38 (session -71.59) | 16:00 close · cash $870.37 · equity $9,385.79 vs 09:30 $9,457.38 (-71.59; session marks -71.59) · 7 name(s) marked open→close (per-name table). KEYS×4 09:30 $321.47 → close $319.27 -8.80; SMTC×9 09:30 $127.63 → close $132.27 +41.76; CIEN×3 09:30 $376.89 → close $360.33 -49.68; MPWR×1 09:30 $1245.11 → close $1225.96 -19.15; DDOG×5 09:30 $232.88 → close $223.84 -45.20; PLAB×46 09:30 $27.69 → close $27.33 -16.56; SEDG×42 09:30 $31.87 → close $32.49 +26.04 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $870.37 | ▼ 09:30 equity $9,356.00 vs yday $9,385.79 (-29.79) | 09:30 open · cash $870.37 (unchanged overnight, no fees) · equity $9,356.00 vs prior close $9,385.79 (-29.79) · 7 name(s) re-marked at the open (per-name table). KEYS×4 yday $319.27 → 09:30 $318.04 -4.92; SMTC×9 yday $132.27 → 09:30 $133.00 +6.57; CIEN×3 yday $360.33 → 09:30 $357.25 -9.24; MPWR×1 yday $1225.96 → 09:30 $1224.92 -1.04; DDOG×5 yday $223.84 → 09:30 $219.46 -21.90; PLAB×46 yday $27.33 → 09:30 $27.41 +3.68; SEDG×42 yday $32.49 → 09:30 $32.42 -2.94 | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 4 | $318.04 | $2.02 | $-29.50 | $2,140.50 | ▼ -29.50 after sell → book $9,353.97; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 9 | $133.00 | $2.04 | $-82.89 | $3,335.47 | ▼ -82.89 after sell → book $9,351.94; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 3 | $357.25 | $2.02 | $-133.53 | $4,405.20 | ▼ -133.53 after sell → book $9,349.92; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 1 | $1224.92 | $2.01 | $-85.12 | $5,628.11 | ▼ -85.12 after sell → book $9,347.91; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 5 | $219.46 | $2.02 | $-107.83 | $6,723.38 | ▼ -107.83 after sell → book $9,345.88; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `PLAB` | 46 | $27.41 | $2.15 | $-123.88 | $7,982.09 | ▼ -123.88 after sell → book $9,343.73; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 42 | $32.42 | $2.14 | $-24.41 | $9,341.60 | ▼ -24.41 after sell → book $9,341.60; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,341.60 | ▲ close $9,341.60 vs 09:30 $9,356.00 (session +0.00) | 16:00 close · cash $9,341.60 · no lots left · equity $9,341.60. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,341.60 | ▲ 09:30 equity $9,341.60 vs yday $9,341.60 (-0.00) | 09:30 open · cash $9,341.60 · no holdings · equity $9,341.60 vs prior close $9,341.60 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 5 | $351.74 | $2.00 | — | $7,580.89 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1868.32 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $6,119.96 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $1868.32 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 57 | $32.31 | $2.16 | — | $4,276.13 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1868.32 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 117 | $15.87 | $2.34 | — | $2,417.00 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1868.32 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 78 | $23.88 | $2.22 | — | $552.14 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+21.9; leftover $1868.32 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $552.14 | ▲ close $9,642.55 vs 09:30 $9,341.60 (session +311.68) | 16:00 close · cash $552.14 · equity $9,642.55 vs 09:30 $9,341.60 (+300.95; session marks +311.68) · 5 name(s) marked open→close (per-name table). AVGO×5 09:30 $351.74 → close $357.16 +27.10; DELL×3 09:30 $486.31 → close $516.39 +90.24; CXW×57 09:30 $32.31 → close $33.66 +76.95; FRNM×117 09:30 $15.87 → close $16.90 +120.51; MMED×78 09:30 $23.88 → close $23.84 -3.12 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $552.14 | ▼ 09:30 equity $9,577.52 vs yday $9,642.55 (-65.03) | 09:30 open · cash $552.14 (unchanged overnight, no fees) · equity $9,577.52 vs prior close $9,642.55 (-65.03) · 5 name(s) re-marked at the open (per-name table). AVGO×5 yday $357.16 → 09:30 $359.70 +12.70; DELL×3 yday $516.39 → 09:30 $513.78 -7.83; CXW×57 yday $33.66 → 09:30 $33.46 -11.40; FRNM×117 yday $16.90 → 09:30 $16.40 -58.50; MMED×78 yday $23.84 → 09:30 $23.84 +0.00 | — |
| 2026-09-04 09:30 ET | **BUY** | `HPE` | 3 | $53.85 | $1.62 | — | $388.96 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list oppset; 🔵; ret5=+0.1; leftover $184.05 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟡 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 2 | $75.65 | $1.52 | — | $236.14 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $184.05 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $236.14 | ▲ close $9,613.96 vs 09:30 $9,577.52 (session +39.59) | 16:00 close · cash $236.14 · equity $9,613.96 vs 09:30 $9,577.52 (+36.44; session marks +39.59) · 7 name(s) marked open→close (per-name table). AVGO×5 09:30 $359.70 → close $357.90 -9.00; DELL×3 09:30 $513.78 → close $524.14 +31.08; CXW×57 09:30 $33.46 → close $34.71 +71.25; FRNM×117 09:30 $16.40 → close $16.31 -10.53; MMED×78 09:30 $23.84 → close $23.29 -42.90; HPE×3 09:30 $53.85 → close $52.00 -5.55; MRX×2 09:30 $75.65 → close $78.27 +5.24 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $236.14 | ▲ 09:30 equity $9,663.53 vs yday $9,613.96 (+49.57) | 09:30 open · cash $236.14 (unchanged overnight, no fees) · equity $9,663.53 vs prior close $9,613.96 (+49.57) · 7 name(s) re-marked at the open (per-name table). AVGO×5 yday $357.90 → 09:30 $363.68 +28.90; DELL×3 yday $524.14 → 09:30 $521.15 -8.97; CXW×57 yday $34.71 → 09:30 $34.49 -12.54; FRNM×117 yday $16.31 → 09:30 $16.74 +50.31; MMED×78 yday $23.29 → 09:30 $23.16 -10.14; HPE×3 yday $52.00 → 09:30 $52.29 +0.87; MRX×2 yday $78.27 → 09:30 $78.84 +1.14 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $236.14 | ▲ close $9,689.73 vs 09:30 $9,663.53 (session +26.20) | 16:00 close · cash $236.14 · equity $9,689.73 vs 09:30 $9,663.53 (+26.20; session marks +26.20) · 7 name(s) marked open→close (per-name table). AVGO×5 09:30 $363.68 → close $368.56 +24.40; DELL×3 09:30 $521.15 → close $533.88 +38.19; CXW×57 09:30 $34.49 → close $35.05 +31.92; FRNM×117 09:30 $16.74 → close $15.99 -87.75; MMED×78 09:30 $23.16 → close $23.32 +12.48; HPE×3 09:30 $52.29 → close $56.03 +11.22; MRX×2 09:30 $78.84 → close $76.71 -4.26 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $236.14 | ▼ 09:30 equity $9,685.33 vs yday $9,689.73 (-4.40) | 09:30 open · cash $236.14 (unchanged overnight, no fees) · equity $9,685.33 vs prior close $9,689.73 (-4.40) · 7 name(s) re-marked at the open (per-name table). AVGO×5 yday $368.56 → 09:30 $366.23 -11.65; DELL×3 yday $533.88 → 09:30 $538.47 +13.77; CXW×57 yday $35.05 → 09:30 $35.09 +2.28; FRNM×117 yday $15.99 → 09:30 $15.96 -3.51; MMED×78 yday $23.32 → 09:30 $23.22 -7.80; HPE×3 yday $56.03 → 09:30 $56.94 +2.73; MRX×2 yday $76.71 → 09:30 $76.60 -0.22 | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 5 | $366.23 | $2.03 | $+68.42 | $2,065.26 | ▲ +68.42 after sell → book $9,683.30; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 3 | $538.47 | $2.02 | $+152.46 | $3,678.65 | ▲ +152.46 after sell → book $9,681.28; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CXW` | 57 | $35.09 | $2.19 | $+154.11 | $5,676.59 | ▲ +154.11 after sell → book $9,679.09; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 117 | $15.96 | $2.38 | $+5.81 | $7,541.54 | ▲ +5.81 after sell → book $9,676.72; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 78 | $23.22 | $2.25 | $-55.96 | $9,350.45 | ▼ -55.96 after sell → book $9,674.47; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,350.45 | ▲ close $9,678.59 vs 09:30 $9,685.33 (session +4.12) | 16:00 close · cash $9,350.45 · equity $9,678.59 vs 09:30 $9,685.33 (-6.74; session marks +4.12) · 2 name(s) marked open→close (per-name table). HPE×3 09:30 $56.94 → close $58.90 +5.88; MRX×2 09:30 $76.60 → close $75.72 -1.76 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,350.45 | ▼ 09:30 equity $9,673.85 vs yday $9,678.59 (-4.74) | 09:30 open · cash $9,350.45 (unchanged overnight, no fees) · equity $9,673.85 vs prior close $9,678.59 (-4.74) · 2 name(s) re-marked at the open (per-name table). HPE×3 yday $58.90 → 09:30 $57.80 -3.30; MRX×2 yday $75.72 → 09:30 $75.00 -1.44 | — |
| 2026-09-10 09:30 ET | **SELL** | `HPE` | 3 | $57.80 | $1.76 | $+8.46 | $9,522.08 | ▲ +8.46 after sell → book $9,672.08; vs 09:30 mark -1.77 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-10 09:30 ET | **SELL** | `MRX` | 2 | $75.00 | $1.53 | $-4.35 | $9,670.56 | ▼ -4.35 after sell → book $9,670.56; vs 09:30 mark -1.52 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,670.56 | ▲ close $9,670.56 vs 09:30 $9,673.85 (session +0.00) | 16:00 close · cash $9,670.56 · no lots left · equity $9,670.56. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,670.56 | ▲ 09:30 equity $9,670.56 vs yday $9,670.56 (-0.00) | 09:30 open · cash $9,670.56 · no holdings · equity $9,670.56 vs prior close $9,670.56 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 29 | $164.43 | $2.08 | — | $4,900.01 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $4835.28 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BTI` | 86 | $56.03 | $2.25 | — | $79.18 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list oppset; 🔵; ret5=-0.8; leftover $4835.28 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.18 | ▼ close $9,187.94 vs 09:30 $9,670.56 (session -478.29) | 16:00 close · cash $79.18 · equity $9,187.94 vs 09:30 $9,670.56 (-482.62; session marks -478.29) · 2 name(s) marked open→close (per-name table). ORCL×29 09:30 $164.43 → close $150.28 -410.35; BTI×86 09:30 $56.03 → close $55.24 -67.94 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.18 | ▼ 09:30 equity $9,092.68 vs yday $9,187.94 (-95.26) | 09:30 open · cash $79.18 (unchanged overnight, no fees) · equity $9,092.68 vs prior close $9,187.94 (-95.26) · 2 name(s) re-marked at the open (per-name table). ORCL×29 yday $150.28 → 09:30 $141.42 -256.94; BTI×86 yday $55.24 → 09:30 $57.12 +161.68 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.18 | ▲ close $9,205.03 vs 09:30 $9,092.68 (session +112.35) | 16:00 close · cash $79.18 · equity $9,205.03 vs 09:30 $9,092.68 (+112.35; session marks +112.35) · 2 name(s) marked open→close (per-name table). ORCL×29 09:30 $141.42 → close $144.79 +97.73; BTI×86 09:30 $57.12 → close $57.29 +14.62 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.18 | ▼ 09:30 equity $9,095.08 vs yday $9,205.03 (-109.95) | 09:30 open · cash $79.18 (unchanged overnight, no fees) · equity $9,095.08 vs prior close $9,205.03 (-109.95) · 2 name(s) re-marked at the open (per-name table). ORCL×29 yday $144.79 → 09:30 $143.46 -38.57; BTI×86 yday $57.29 → 09:30 $56.46 -71.38 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.18 | ▼ close $9,010.05 vs 09:30 $9,095.08 (session -85.03) | 16:00 close · cash $79.18 · equity $9,010.05 vs 09:30 $9,095.08 (-85.03; session marks -85.03) · 2 name(s) marked open→close (per-name table). ORCL×29 09:30 $143.46 → close $140.35 -90.19; BTI×86 09:30 $56.46 → close $56.52 +5.16 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.18 | ▼ 09:30 equity $9,002.49 vs yday $9,010.05 (-7.56) | 09:30 open · cash $79.18 (unchanged overnight, no fees) · equity $9,002.49 vs prior close $9,010.05 (-7.56) · 2 name(s) re-marked at the open (per-name table). ORCL×29 yday $140.35 → 09:30 $140.03 -9.28; BTI×86 yday $56.52 → 09:30 $56.54 +1.72 | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 29 | $140.03 | $2.12 | $-711.80 | $4,137.93 | ▼ -711.80 after sell → book $9,000.37; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BTI` | 86 | $56.54 | $2.30 | $+39.31 | $8,998.07 | ▲ +39.31 after sell → book $8,998.07; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 114 | $26.27 | $2.33 | — | $6,000.96 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2999.36 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 15 | $189.17 | $2.04 | — | $3,161.38 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $2999.36 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 75 | $39.99 | $2.21 | — | $159.91 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2999.36 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $159.91 | ▼ close $8,825.77 vs 09:30 $9,002.49 (session -165.72) | 16:00 close · cash $159.91 · equity $8,825.77 vs 09:30 $9,002.49 (-176.72; session marks -165.72) · 3 name(s) marked open→close (per-name table). WAY×114 09:30 $26.27 → close $26.59 +36.48; QCOM×15 09:30 $189.17 → close $184.84 -64.95; SM×75 09:30 $39.99 → close $38.16 -137.25 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $159.91 | ▲ 09:30 equity $8,855.05 vs yday $8,825.77 (+29.28) | 09:30 open · cash $159.91 (unchanged overnight, no fees) · equity $8,855.05 vs prior close $8,825.77 (+29.28) · 3 name(s) re-marked at the open (per-name table). WAY×114 yday $26.59 → 09:30 $26.51 -9.12; QCOM×15 yday $184.84 → 09:30 $190.35 +82.65; SM×75 yday $38.16 → 09:30 $37.57 -44.25 | — |
| 2026-09-17 09:30 ET | **BUY** | `AVTR` | 3 | $15.81 | $0.48 | — | $112.00 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $53.30 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 2 | $22.12 | $0.45 | — | $67.31 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $53.30 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.31 | ▼ close $8,785.97 vs 09:30 $8,855.05 (session -68.15) | 16:00 close · cash $67.31 · equity $8,785.97 vs 09:30 $8,855.05 (-69.08; session marks -68.15) · 5 name(s) marked open→close (per-name table). WAY×114 09:30 $26.51 → close $26.51 +0.00; QCOM×15 09:30 $190.35 → close $188.71 -24.60; SM×75 09:30 $37.57 → close $36.97 -45.00; AVTR×3 09:30 $15.81 → close $15.86 +0.15; GME×2 09:30 $22.12 → close $22.77 +1.30 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.31 | ▲ 09:30 equity $8,868.37 vs yday $8,785.97 (+82.40) | 09:30 open · cash $67.31 (unchanged overnight, no fees) · equity $8,868.37 vs prior close $8,785.97 (+82.40) · 5 name(s) re-marked at the open (per-name table). WAY×114 yday $26.51 → 09:30 $26.95 +50.16; QCOM×15 yday $188.71 → 09:30 $191.34 +39.45; SM×75 yday $36.97 → 09:30 $36.87 -7.50; AVTR×3 yday $15.86 → 09:30 $15.87 +0.03; GME×2 yday $22.77 → 09:30 $22.90 +0.26 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 1 | $20.91 | $0.21 | — | $46.19 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $22.44 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 1 | $14.79 | $0.15 | — | $31.25 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $22.44 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 1 | $14.07 | $0.14 | — | $17.03 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $22.44 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.03 | ▼ close $8,521.98 vs 09:30 $8,868.37 (session -345.88) | 16:00 close · cash $17.03 · equity $8,521.98 vs 09:30 $8,868.37 (-346.39; session marks -345.88) · 8 name(s) marked open→close (per-name table). WAY×114 09:30 $26.95 → close $25.66 -147.06; QCOM×15 09:30 $191.34 → close $177.72 -204.30; SM×75 09:30 $36.87 → close $36.97 +7.50; AVTR×3 09:30 $15.87 → close $15.52 -1.05; GME×2 09:30 $22.90 → close $22.64 -0.52; TH×1 09:30 $20.91 → close $21.19 +0.28; RARE×1 09:30 $14.79 → close $14.51 -0.28; BHVN×1 09:30 $14.07 → close $13.62 -0.45 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 24.24 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 24.24 < 1 share @ 115.18 |
| 2026-08-21 | `GRAL` | cash | leftover split 24.24 < 1 share @ 78.88 |
| 2026-08-21 | `VIRT` | cash | leftover split 24.24 < 1 share @ 60.66 |
| 2026-08-21 | `MFC` | cash | leftover split 24.24 < 1 share @ 42.48 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `AMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 0.80 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 0.80 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 0.80 < 1 share @ 1746.53 |
| 2026-08-27 | `CM` | cash | leftover split 0.80 < 1 share @ 118.77 |
| 2026-08-27 | `GEN` | cash | leftover split 0.80 < 1 share @ 29.83 |
| 2026-08-27 | `LRCX` | cash | leftover split 0.80 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 0.80 < 1 share @ 222.86 |
| 2026-08-28 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DDOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PLAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-09-01 | `PLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CXW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 184.05 < 1 share @ 263.36 |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CXW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HPE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `MRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-09 | `HPE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BTI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BTI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 53.30 < 1 share @ 170.85 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVTR` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `WAY` | 114 | 2026-09-16 @ $26.27 | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2999.36 |
| `QCOM` | 15 | 2026-09-16 @ $189.17 | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $2999.36 |
| `SM` | 75 | 2026-09-16 @ $39.99 | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2999.36 |
| `AVTR` | 3 | 2026-09-17 @ $15.81 | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $53.30 |
| `GME` | 2 | 2026-09-17 @ $22.12 | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $53.30 |
| `TH` | 1 | 2026-09-18 @ $20.91 | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $22.44 |
| `RARE` | 1 | 2026-09-18 @ $14.79 | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $22.44 |
| `BHVN` | 1 | 2026-09-18 @ $14.07 | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $22.44 |
