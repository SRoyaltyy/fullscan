# Factor mine action — `union_news_or_net4_rw_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `rank_w` · sell `list` · S-boost `none` · OR news + net≥4; leftover weighted by camera rank

Cash book **-2.40%** ($9,760) · signal-only (no cash/fees) was -2.84%. Starts YES **5/28**. Fills 88 · skips 23 · realized $-239.60.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: the morning news packet OR the prior-export headline is green.
- Must-have: camera net (+G −R) is at least 4.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 4.
- Split leftover cash by rank (first name gets the biggest slice).
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
- **Gate** `news_or_headline=True,cam_net_min=4` · **rank** `cond` · **top_n** 4.
- **Size** `rank_w` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,760.43.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `HLIT` | 303 | — | $13.18 | +0.00 | $13.92 | +224.22 | +224.22 | +0.00 | +224.22 |
| 2026-08-14 | `SNDK` | 1 | — | $1646.93 | +0.00 | $1641.11 | -5.82 | -5.82 | +0.00 | -5.82 |
| 2026-08-14 | `ANGX` | 464 | — | $4.31 | +0.00 | $4.37 | +27.84 | +27.84 | +0.00 | +27.84 |
| 2026-08-14 | `ARX` | 51 | — | $19.57 | +0.00 | $19.58 | +0.51 | +0.51 | +0.00 | +0.51 |
| 2026-08-17 | `HLIT` | 303 | $13.92 | $13.84 | -24.24 | — | +0.00 | -24.24 | +199.98 | — |
| 2026-08-17 | `SNDK` | 1 | $1641.11 | $1700.74 | +59.63 | — | +0.00 | +59.63 | +53.81 | — |
| 2026-08-17 | `ANGX` | 464 | $4.37 | $4.60 | +106.72 | — | +0.00 | +106.72 | +134.56 | — |
| 2026-08-17 | `ARX` | 51 | $19.58 | $19.57 | -0.51 | — | +0.00 | -0.51 | +0.00 | — |
| 2026-08-17 | `DVN` | 112 | — | $46.18 | +0.00 | $47.57 | +155.68 | +155.68 | +0.00 | +155.68 |
| 2026-08-17 | `EOG` | 24 | — | $142.77 | +0.00 | $146.15 | +81.12 | +81.12 | +0.00 | +81.12 |
| 2026-08-17 | `FANG` | 8 | — | $202.70 | +0.00 | $206.29 | +28.72 | +28.72 | +0.00 | +28.72 |
| 2026-08-18 | `DVN` | 112 | $47.57 | $48.00 | +48.16 | — | +0.00 | +48.16 | +203.84 | — |
| 2026-08-18 | `EOG` | 24 | $146.15 | $148.04 | +45.36 | — | +0.00 | +45.36 | +126.48 | — |
| 2026-08-18 | `FANG` | 8 | $206.29 | $208.93 | +21.12 | — | +0.00 | +21.12 | +49.84 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 47 | — | $91.01 | +0.00 | $93.63 | +123.14 | +123.14 | +0.00 | +123.14 |
| 2026-08-20 | `APA` | 71 | — | $44.76 | +0.00 | $44.39 | -26.27 | -26.27 | +0.00 | -26.27 |
| 2026-08-20 | `AUTL` | 868 | — | $2.47 | +0.00 | $2.46 | -8.68 | -8.68 | +0.00 | -8.68 |
| 2026-08-20 | `CRSP` | 18 | — | $58.73 | +0.00 | $58.12 | -10.98 | -10.98 | +0.00 | -10.98 |
| 2026-08-21 | `BHP` | 47 | $93.63 | $95.72 | +98.23 | — | +0.00 | +98.23 | +221.37 | — |
| 2026-08-21 | `APA` | 71 | $44.39 | $44.52 | +9.23 | — | +0.00 | +9.23 | -17.04 | — |
| 2026-08-21 | `AUTL` | 868 | $2.46 | $2.47 | +8.68 | $2.41 | -52.08 | -43.40 | +0.00 | -52.08 |
| 2026-08-21 | `CRSP` | 18 | $58.12 | $59.72 | +28.80 | $59.50 | -3.96 | +24.84 | +17.82 | +13.86 |
| 2026-08-21 | `AU` | 43 | — | $119.43 | +0.00 | $121.22 | +76.97 | +76.97 | +0.00 | +76.97 |
| 2026-08-21 | `FUTU` | 22 | — | $115.18 | +0.00 | $123.64 | +186.12 | +186.12 | +0.00 | +186.12 |
| 2026-08-24 | `AUTL` | 868 | $2.41 | $2.40 | -8.68 | — | +0.00 | -8.68 | -60.76 | — |
| 2026-08-24 | `CRSP` | 18 | $59.50 | $58.75 | -13.50 | $57.08 | -30.15 | -43.65 | +0.36 | -29.79 |
| 2026-08-24 | `AU` | 43 | $121.22 | $120.51 | -30.53 | — | +0.00 | -30.53 | +46.44 | — |
| 2026-08-24 | `FUTU` | 22 | $123.64 | $121.00 | -58.08 | — | +0.00 | -58.08 | +128.04 | — |
| 2026-08-25 | `CRSP` | 18 | $57.08 | $57.93 | +15.39 | — | +0.00 | +15.39 | -14.40 | — |
| 2026-08-25 | `AU` | 46 | — | $118.52 | +0.00 | $123.39 | +224.02 | +224.02 | +0.00 | +224.02 |
| 2026-08-25 | `FCX` | 47 | — | $77.13 | +0.00 | $79.91 | +130.66 | +130.66 | +0.00 | +130.66 |
| 2026-08-25 | `EZPW` | 52 | — | $35.05 | +0.00 | $35.23 | +9.36 | +9.36 | +0.00 | +9.36 |
| 2026-08-26 | `AU` | 46 | $123.39 | $119.80 | -165.14 | — | +0.00 | -165.14 | +58.88 | — |
| 2026-08-26 | `FCX` | 47 | $79.91 | $79.34 | -26.79 | — | +0.00 | -26.79 | +103.87 | — |
| 2026-08-26 | `EZPW` | 52 | $35.23 | $35.70 | +24.44 | — | +0.00 | +24.44 | +33.80 | — |
| 2026-08-26 | `FNV` | 27 | — | $267.02 | +0.00 | $267.37 | +9.45 | +9.45 | +0.00 | +9.45 |
| 2026-08-26 | `CM` | 31 | — | $118.50 | +0.00 | $118.20 | -9.30 | -9.30 | +0.00 | -9.30 |
| 2026-08-27 | `FNV` | 27 | $267.37 | $267.23 | -3.78 | — | +0.00 | -3.78 | +5.67 | — |
| 2026-08-27 | `CM` | 31 | $118.20 | $118.77 | +17.67 | $114.84 | -121.83 | -104.16 | +8.37 | -113.46 |
| 2026-08-27 | `ACMR` | 45 | — | $81.65 | +0.00 | $80.49 | -52.20 | -52.20 | +0.00 | -52.20 |
| 2026-08-27 | `MU` | 2 | — | $967.01 | +0.00 | $935.39 | -63.24 | -63.24 | +0.00 | -63.24 |
| 2026-08-28 | `CM` | 31 | $114.84 | $115.66 | +25.42 | — | +0.00 | +25.42 | -88.04 | — |
| 2026-08-28 | `ACMR` | 45 | $80.49 | $79.27 | -54.90 | — | +0.00 | -54.90 | -107.10 | — |
| 2026-08-28 | `MU` | 2 | $935.39 | $919.29 | -32.20 | — | +0.00 | -32.20 | -95.44 | — |
| 2026-08-28 | `KEYS` | 13 | — | $324.41 | +0.00 | $319.97 | -57.72 | -57.72 | +0.00 | -57.72 |
| 2026-08-28 | `SMTC` | 23 | — | $141.76 | +0.00 | $131.17 | -243.57 | -243.57 | +0.00 | -243.57 |
| 2026-08-28 | `CIEN` | 5 | — | $400.42 | +0.00 | $378.44 | -109.90 | -109.90 | +0.00 | -109.90 |
| 2026-08-31 | `KEYS` | 13 | $319.97 | $322.49 | +32.76 | — | +0.00 | +32.76 | -24.96 | — |
| 2026-08-31 | `SMTC` | 23 | $131.17 | $132.30 | +25.99 | — | +0.00 | +25.99 | -217.58 | — |
| 2026-08-31 | `CIEN` | 5 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -109.90 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 11 | — | $351.74 | +0.00 | $357.16 | +59.62 | +59.62 | +0.00 | +59.62 |
| 2026-09-03 | `DELL` | 6 | — | $486.31 | +0.00 | $516.39 | +180.48 | +180.48 | +0.00 | +180.48 |
| 2026-09-03 | `CXW` | 65 | — | $32.31 | +0.00 | $33.66 | +87.75 | +87.75 | +0.00 | +87.75 |
| 2026-09-03 | `FRNM` | 66 | — | $15.87 | +0.00 | $16.90 | +67.98 | +67.98 | +0.00 | +67.98 |
| 2026-09-04 | `AVGO` | 11 | $357.16 | $359.70 | +27.94 | — | +0.00 | +27.94 | +87.56 | — |
| 2026-09-04 | `DELL` | 6 | $516.39 | $513.78 | -15.66 | — | +0.00 | -15.66 | +164.82 | — |
| 2026-09-04 | `CXW` | 65 | $33.66 | $33.46 | -13.00 | — | +0.00 | -13.00 | +74.75 | — |
| 2026-09-04 | `FRNM` | 66 | $16.90 | $16.40 | -33.00 | $16.31 | -5.94 | -38.94 | +34.98 | +29.04 |
| 2026-09-04 | `CRM` | 18 | — | $263.36 | +0.00 | $259.23 | -74.34 | -74.34 | +0.00 | -74.34 |
| 2026-09-04 | `MRX` | 43 | — | $75.65 | +0.00 | $78.27 | +112.66 | +112.66 | +0.00 | +112.66 |
| 2026-09-04 | `BE` | 6 | — | $236.82 | +0.00 | $252.87 | +96.30 | +96.30 | +0.00 | +96.30 |
| 2026-09-08 | `FRNM` | 66 | $16.31 | $16.74 | +28.38 | — | +0.00 | +28.38 | +57.42 | — |
| 2026-09-08 | `CRM` | 18 | $259.23 | $253.72 | -99.18 | — | +0.00 | -99.18 | -173.52 | — |
| 2026-09-08 | `MRX` | 43 | $78.27 | $78.84 | +24.51 | — | +0.00 | +24.51 | +137.17 | — |
| 2026-09-08 | `BE` | 6 | $252.87 | $267.76 | +89.34 | — | +0.00 | +89.34 | +185.64 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 66 | — | $164.43 | +0.00 | $150.28 | -933.90 | -933.90 | +0.00 | -933.90 |
| 2026-09-14 | `ORCL` | 66 | $150.28 | $141.42 | -584.76 | — | +0.00 | -584.76 | -1518.66 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 180 | — | $26.27 | +0.00 | $26.59 | +57.60 | +57.60 | +0.00 | +57.60 |
| 2026-09-16 | `QCOM` | 16 | — | $189.17 | +0.00 | $184.84 | -69.28 | -69.28 | +0.00 | -69.28 |
| 2026-09-16 | `SM` | 39 | — | $39.99 | +0.00 | $38.16 | -71.37 | -71.37 | +0.00 | -71.37 |
| 2026-09-17 | `WAY` | 180 | $26.59 | $26.51 | -14.40 | — | +0.00 | -14.40 | +43.20 | — |
| 2026-09-17 | `QCOM` | 16 | $184.84 | $190.35 | +88.16 | — | +0.00 | +88.16 | +18.88 | — |
| 2026-09-17 | `SM` | 39 | $38.16 | $37.57 | -23.01 | — | +0.00 | -23.01 | -94.38 | — |
| 2026-09-17 | `SMTC` | 22 | — | $170.85 | +0.00 | $178.19 | +161.48 | +161.48 | +0.00 | +161.48 |
| 2026-09-17 | `AVTR` | 179 | — | $15.81 | +0.00 | $15.86 | +8.95 | +8.95 | +0.00 | +8.95 |
| 2026-09-17 | `GME` | 85 | — | $22.12 | +0.00 | $22.77 | +55.25 | +55.25 | +0.00 | +55.25 |
| 2026-09-17 | `JBHT` | 3 | — | $238.60 | +0.00 | $236.80 | -5.40 | -5.40 | +0.00 | -5.40 |
| 2026-09-18 | `SMTC` | 22 | $178.19 | $182.33 | +91.08 | — | +0.00 | +91.08 | +252.56 | — |
| 2026-09-18 | `AVTR` | 179 | $15.86 | $15.87 | +1.79 | — | +0.00 | +1.79 | +10.74 | — |
| 2026-09-18 | `GME` | 85 | $22.77 | $22.90 | +11.05 | $22.64 | -22.10 | -11.05 | +66.30 | +44.20 |
| 2026-09-18 | `JBHT` | 3 | $236.80 | $236.80 | +0.00 | — | +0.00 | +0.00 | -5.40 | — |
| 2026-09-18 | `TH` | 186 | — | $20.91 | +0.00 | $21.19 | +52.08 | +52.08 | +0.00 | +52.08 |
| 2026-09-18 | `RARE` | 175 | — | $14.79 | +0.00 | $14.51 | -49.00 | -49.00 | +0.00 | -49.00 |
| 2026-09-18 | `BHVN` | 92 | — | $14.07 | +0.00 | $13.62 | -41.40 | -41.40 | +0.00 | -41.40 |
| 2026-09-21 | `GME` | 85 | $22.64 | $22.78 | +11.90 | — | +0.00 | +11.90 | +56.10 | — |
| 2026-09-21 | `TH` | 186 | $21.19 | $21.65 | +85.56 | — | +0.00 | +85.56 | +137.64 | — |
| 2026-09-21 | `RARE` | 175 | $14.51 | $14.58 | +12.25 | — | +0.00 | +12.25 | -36.75 | — |
| 2026-09-21 | `BHVN` | 92 | $13.62 | $13.90 | +25.76 | — | +0.00 | +25.76 | -15.64 | — |
| 2026-09-21 | `VICR` | 21 | — | $230.25 | +0.00 | $223.90 | -133.35 | -133.35 | +0.00 | -133.35 |
| 2026-09-21 | `SMTC` | 17 | — | $190.30 | +0.00 | $177.37 | -219.81 | -219.81 | +0.00 | -219.81 |
| 2026-09-21 | `GLXY` | 62 | — | $25.95 | +0.00 | $26.07 | +7.44 | +7.44 | +0.00 | +7.44 |
| 2026-09-22 | `VICR` | 21 | $223.90 | $241.04 | +359.94 | — | +0.00 | +359.94 | +226.59 | — |
| 2026-09-22 | `SMTC` | 17 | $177.37 | $175.00 | -40.29 | — | +0.00 | -40.29 | -260.10 | — |
| 2026-09-22 | `GLXY` | 62 | $26.07 | $25.95 | -7.44 | — | +0.00 | -7.44 | +0.00 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +246.75 | HLIT, SNDK, ANGX, ARX | — | $1,347.59 | $10,232.72 | HLIT×303, SNDK×1, ANGX×464, ARX×51 |
| 2026-08-17 | +2.25 | $1,347.59 | HLIT×303, SNDK×1, ANGX×464, ARX×51 | $10,374.32 | +141.60 | +265.52 | DVN, EOG, FANG | HLIT, SNDK, ANGX, ARX | $133.43 | $10,619.19 | DVN×112, EOG×24, FANG×8 |
| 2026-08-18 | -6.20 | $133.43 | DVN×112, EOG×24, FANG×8 | $10,733.83 | +114.64 | +0.00 | — | DVN, EOG, FANG | $10,727.31 | $10,727.31 | — |
| 2026-08-19 | -7.20 | $10,727.31 | — | $10,727.31 | -0.00 | +0.00 | — | — | $10,727.31 | $10,727.31 | — |
| 2026-08-20 | +1.12 | $10,727.31 | — | $10,727.31 | -0.00 | +77.21 | BHP, APA, AUTL, CRSP | — | $53.20 | $10,786.94 | BHP×47, APA×71, AUTL×868, CRSP×18 |
| 2026-08-21 | +3.25 | $53.20 | BHP×47, APA×71, AUTL×868, CRSP×18 | $10,931.88 | +144.94 | +207.05 | AU, FUTU | BHP, APA | $34.92 | $11,130.34 | AUTL×868, CRSP×18, AU×43, FUTU×22 |
| 2026-08-24 | -5.17 | $34.92 | AUTL×868, CRSP×18, AU×43, FUTU×22 | $11,019.55 | -110.79 | -30.15 | — | AUTL, AU, FUTU | $9,946.43 | $10,973.78 | CRSP×18 |
| 2026-08-25 | +1.80 | $9,946.43 | CRSP×18 | $10,989.17 | +15.39 | +364.04 | AU, FCX, EZPW | CRSP | $81.07 | $11,344.74 | AU×46, FCX×47, EZPW×52 |
| 2026-08-26 | +2.02 | $81.07 | AU×46, FCX×47, EZPW×52 | $11,177.25 | -167.49 | +0.15 | FNV, CM | AU, FCX, EZPW | $283.54 | $11,166.73 | FNV×27, CM×31 |
| 2026-08-27 | — | $283.54 | FNV×27, CM×31 | $11,180.62 | +13.89 | -237.27 | ACMR, MU | FNV | $1,884.22 | $10,937.09 | CM×31, ACMR×45, MU×2 |
| 2026-08-28 | +0.75 | $1,884.22 | CM×31, ACMR×45, MU×2 | $10,875.41 | -61.68 | -411.19 | KEYS, SMTC, CIEN | CM, ACMR, MU | $1,383.10 | $10,451.82 | KEYS×13, SMTC×23, CIEN×5 |
| 2026-08-31 | -5.85 | $1,383.10 | KEYS×13, SMTC×23, CIEN×5 | $10,510.57 | +58.75 | +0.00 | — | KEYS, SMTC, CIEN | $10,504.37 | $10,504.37 | — |
| 2026-09-01 | -6.30 | $10,504.37 | — | $10,504.37 | +0.00 | +0.00 | — | — | $10,504.37 | $10,504.37 | — |
| 2026-09-02 | -3.83 | $10,504.37 | — | $10,504.37 | +0.00 | +0.00 | — | — | $10,504.37 | $10,504.37 | — |
| 2026-09-03 | -0.90 | $10,504.37 | — | $10,504.37 | +0.00 | +395.83 | AVGO, DELL, CXW, FRNM | — | $561.40 | $10,891.80 | AVGO×11, DELL×6, CXW×65, FRNM×66 |
| 2026-09-04 | +2.25 | $561.40 | AVGO×11, DELL×6, CXW×65, FRNM×66 | $10,858.08 | -33.72 | +128.68 | CRM, MRX, BE | AVGO, DELL, CXW | $348.84 | $10,974.27 | FRNM×66, CRM×18, MRX×43, BE×6 |
| 2026-09-08 | -11.47 | $348.84 | FRNM×66, CRM×18, MRX×43, BE×6 | $11,017.32 | +43.05 | +0.00 | — | FRNM, CRM, MRX, BE | $11,008.83 | $11,008.83 | — |
| 2026-09-09 | -13.95 | $11,008.83 | — | $11,008.83 | -0.00 | +0.00 | — | — | $11,008.83 | $11,008.83 | — |
| 2026-09-10 | -13.28 | $11,008.83 | — | $11,008.83 | -0.00 | +0.00 | — | — | $11,008.83 | $11,008.83 | — |
| 2026-09-11 | +0.50 | $11,008.83 | — | $11,008.83 | -0.00 | -933.90 | ORCL | — | $154.26 | $10,072.74 | ORCL×66 |
| 2026-09-14 | -11.00 | $154.26 | ORCL×66 | $9,487.98 | -584.76 | +0.00 | — | ORCL | $9,485.71 | $9,485.71 | — |
| 2026-09-15 | -3.84 | $9,485.71 | — | $9,485.71 | -0.00 | +0.00 | — | — | $9,485.71 | $9,485.71 | — |
| 2026-09-16 | +5.30 | $9,485.71 | — | $9,485.71 | -0.00 | -83.05 | WAY, QCOM, SM | — | $164.10 | $9,395.98 | WAY×180, QCOM×16, SM×39 |
| 2026-09-17 | +7.38 | $164.10 | WAY×180, QCOM×16, SM×39 | $9,446.73 | +50.75 | +220.28 | SMTC, AVTR, GME, JBHT | WAY, QCOM, SM | $246.42 | $9,651.39 | SMTC×22, AVTR×179, GME×85, JBHT×3 |
| 2026-09-18 | +4.86 | $246.42 | SMTC×22, AVTR×179, GME×85, JBHT×3 | $9,755.31 | +103.92 | -60.42 | TH, RARE, BHVN | SMTC, AVTR, JBHT | $22.83 | $9,680.86 | GME×85, TH×186, RARE×175, BHVN×92 |
| 2026-09-21 | +12.87 | $22.83 | GME×85, TH×186, RARE×175, BHVN×92 | $9,816.33 | +135.47 | -345.72 | VICR, SMTC, GLXY | GME, TH, RARE, BHVN | $121.07 | $9,454.60 | VICR×21, SMTC×17, GLXY×62 |
| 2026-09-22 | -0.50 | $121.07 | VICR×21, SMTC×17, GLXY×62 | $9,766.81 | +312.21 | +0.00 | — | VICR, SMTC, GLXY | $9,760.43 | $9,760.43 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 303 | $13.18 | $3.91 | — | $6,002.55 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $4000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 1 | $1646.93 | $1.99 | — | $4,353.63 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $3000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $2,347.80 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 51 | $19.57 | $2.14 | — | $1,347.59 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,347.59 | ▲ close $10,232.72 vs 09:30 $10,000.00 (session +246.75) | 16:00 close · cash $1,347.59 · equity $10,232.72 vs 09:30 $10,000.00 (+232.72; session marks +246.75) · 4 name(s) marked open→close (per-name table). HLIT×303 09:30 $13.18 → close $13.92 +224.22; SNDK×1 09:30 $1646.93 → close $1641.11 -5.82; ANGX×464 09:30 $4.31 → close $4.37 +27.84; ARX×51 09:30 $19.57 → close $19.58 +0.51 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,347.59 | ▲ 09:30 equity $10,374.32 vs yday $10,232.72 (+141.60) | 09:30 open · cash $1,347.59 (unchanged overnight, no fees) · equity $10,374.32 vs prior close $10,232.72 (+141.60) · 4 name(s) re-marked at the open (per-name table). HLIT×303 yday $13.92 → 09:30 $13.84 -24.24; SNDK×1 yday $1641.11 → 09:30 $1700.74 +59.63; ANGX×464 yday $4.37 → 09:30 $4.60 +106.72; ARX×51 yday $19.58 → 09:30 $19.57 -0.51 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 303 | $13.84 | $3.99 | $+192.08 | $5,537.12 | ▲ +192.08 after sell → book $10,370.33; vs 09:30 mark -3.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SNDK` | 1 | $1700.74 | $2.02 | $+49.81 | $7,235.85 | ▲ +49.81 after sell → book $10,368.32; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 464 | $4.60 | $6.08 | $+122.49 | $9,364.17 | ▲ +122.49 after sell → book $10,362.24; vs 09:30 mark -6.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 51 | $19.57 | $2.16 | $-4.31 | $10,360.07 | ▼ -4.31 after sell → book $10,360.07; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 112 | $46.18 | $2.33 | — | $5,185.59 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+6.7; leftover $5180.04 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 24 | $142.77 | $2.06 | — | $1,757.04 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+5.8; leftover $3453.36 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 8 | $202.70 | $2.01 | — | $133.43 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+8.3; leftover $1726.68 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.43 | ▲ close $10,619.19 vs 09:30 $10,374.32 (session +265.52) | 16:00 close · cash $133.43 · equity $10,619.19 vs 09:30 $10,374.32 (+244.87; session marks +265.52) · 3 name(s) marked open→close (per-name table). DVN×112 09:30 $46.18 → close $47.57 +155.68; EOG×24 09:30 $142.77 → close $146.15 +81.12; FANG×8 09:30 $202.70 → close $206.29 +28.72 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.43 | ▲ 09:30 equity $10,733.83 vs yday $10,619.19 (+114.64) | 09:30 open · cash $133.43 (unchanged overnight, no fees) · equity $10,733.83 vs prior close $10,619.19 (+114.64) · 3 name(s) re-marked at the open (per-name table). DVN×112 yday $47.57 → 09:30 $48.00 +48.16; EOG×24 yday $146.15 → 09:30 $148.04 +45.36; FANG×8 yday $206.29 → 09:30 $208.93 +21.12 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 112 | $48.00 | $2.39 | $+199.13 | $5,507.04 | ▲ +199.13 after sell → book $10,731.44; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 24 | $148.04 | $2.10 | $+122.32 | $9,057.90 | ▲ +122.32 after sell → book $10,729.34; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 8 | $208.93 | $2.04 | $+45.79 | $10,727.31 | ▲ +45.79 after sell → book $10,727.31; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,727.31 | ▲ close $10,727.31 vs 09:30 $10,733.83 (session +0.00) | 16:00 close · cash $10,727.31 · no lots left · equity $10,727.31. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,727.31 | ▲ 09:30 equity $10,727.31 vs yday $10,727.31 (-0.00) | 09:30 open · cash $10,727.31 · no holdings · equity $10,727.31 vs prior close $10,727.31 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,727.31 | ▲ close $10,727.31 vs 09:30 $10,727.31 (session +0.00) | 16:00 close · cash $10,727.31 · no lots left · equity $10,727.31. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,727.31 | ▲ 09:30 equity $10,727.31 vs yday $10,727.31 (-0.00) | 09:30 open · cash $10,727.31 · no holdings · equity $10,727.31 vs prior close $10,727.31 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 47 | $91.01 | $2.13 | — | $6,447.70 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $4290.92 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 71 | $44.76 | $2.20 | — | $3,267.54 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $3218.19 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 868 | $2.47 | $11.20 | — | $1,112.38 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $2145.46 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 18 | $58.73 | $2.04 | — | $53.20 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1072.73 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.20 | ▲ close $10,786.94 vs 09:30 $10,727.31 (session +77.21) | 16:00 close · cash $53.20 · equity $10,786.94 vs 09:30 $10,727.31 (+59.63; session marks +77.21) · 4 name(s) marked open→close (per-name table). BHP×47 09:30 $91.01 → close $93.63 +123.14; APA×71 09:30 $44.76 → close $44.39 -26.27; AUTL×868 09:30 $2.47 → close $2.46 -8.68; CRSP×18 09:30 $58.73 → close $58.12 -10.98 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.20 | ▲ 09:30 equity $10,931.88 vs yday $10,786.94 (+144.94) | 09:30 open · cash $53.20 (unchanged overnight, no fees) · equity $10,931.88 vs prior close $10,786.94 (+144.94) · 4 name(s) re-marked at the open (per-name table). BHP×47 yday $93.63 → 09:30 $95.72 +98.23; APA×71 yday $44.39 → 09:30 $44.52 +9.23; AUTL×868 yday $2.46 → 09:30 $2.47 +8.68; CRSP×18 yday $58.12 → 09:30 $59.72 +28.80 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 47 | $95.72 | $2.18 | $+217.06 | $4,549.86 | ▲ +217.06 after sell → book $10,929.70; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 71 | $44.52 | $2.24 | $-21.48 | $7,708.54 | ▼ -21.48 after sell → book $10,927.46; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 43 | $119.43 | $2.12 | — | $2,570.93 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $5139.03 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 22 | $115.18 | $2.06 | — | $34.92 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $2569.51 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.92 | ▲ close $11,130.34 vs 09:30 $10,931.88 (session +207.05) | 16:00 close · cash $34.92 · equity $11,130.34 vs 09:30 $10,931.88 (+198.46; session marks +207.05) · 4 name(s) marked open→close (per-name table). AUTL×868 09:30 $2.47 → close $2.41 -52.08; CRSP×18 09:30 $59.72 → close $59.50 -3.96; AU×43 09:30 $119.43 → close $121.22 +76.97; FUTU×22 09:30 $115.18 → close $123.64 +186.12 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.92 | ▼ 09:30 equity $11,019.55 vs yday $11,130.34 (-110.79) | 09:30 open · cash $34.92 (unchanged overnight, no fees) · equity $11,019.55 vs prior close $11,130.34 (-110.79) · 4 name(s) re-marked at the open (per-name table). AUTL×868 yday $2.41 → 09:30 $2.40 -8.68; CRSP×18 yday $59.50 → 09:30 $58.75 -13.50; AU×43 yday $121.22 → 09:30 $120.51 -30.53; FUTU×22 yday $123.64 → 09:30 $121.00 -58.08 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 868 | $2.40 | $11.36 | $-83.32 | $2,106.76 | ▼ -83.32 after sell → book $11,008.19; vs 09:30 mark -11.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 43 | $120.51 | $2.17 | $+42.15 | $7,286.52 | ▲ +42.15 after sell → book $11,006.02; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 22 | $121.00 | $2.09 | $+123.90 | $9,946.43 | ▲ +123.90 after sell → book $11,003.93; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,946.43 | ▼ close $10,973.78 vs 09:30 $11,019.55 (session -30.15) | 16:00 close · cash $9,946.43 · equity $10,973.78 vs 09:30 $11,019.55 (-45.77; session marks -30.15) · 1 name(s) marked open→close (per-name table). CRSP×18 09:30 $58.75 → close $57.08 -30.15 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,946.43 | ▲ 09:30 equity $10,989.17 vs yday $10,973.78 (+15.39) | 09:30 open · cash $9,946.43 (unchanged overnight, no fees) · equity $10,989.17 vs prior close $10,973.78 (+15.39) · 1 name(s) re-marked at the open (per-name table). CRSP×18 yday $57.08 → 09:30 $57.93 +15.39 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 18 | $57.93 | $2.06 | $-18.51 | $10,987.11 | ▼ -18.51 after sell → book $10,987.11; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 46 | $118.52 | $2.13 | — | $5,533.06 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $5493.55 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 47 | $77.13 | $2.13 | — | $1,905.82 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3662.37 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 52 | $35.05 | $2.15 | — | $81.07 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1831.18 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.07 | ▲ close $11,344.74 vs 09:30 $10,989.17 (session +364.04) | 16:00 close · cash $81.07 · equity $11,344.74 vs 09:30 $10,989.17 (+355.57; session marks +364.04) · 3 name(s) marked open→close (per-name table). AU×46 09:30 $118.52 → close $123.39 +224.02; FCX×47 09:30 $77.13 → close $79.91 +130.66; EZPW×52 09:30 $35.05 → close $35.23 +9.36 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.07 | ▼ 09:30 equity $11,177.25 vs yday $11,344.74 (-167.49) | 09:30 open · cash $81.07 (unchanged overnight, no fees) · equity $11,177.25 vs prior close $11,344.74 (-167.49) · 3 name(s) re-marked at the open (per-name table). AU×46 yday $123.39 → 09:30 $119.80 -165.14; FCX×47 yday $79.91 → 09:30 $79.34 -26.79; EZPW×52 yday $35.23 → 09:30 $35.70 +24.44 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 46 | $119.80 | $2.18 | $+54.57 | $5,589.69 | ▲ +54.57 after sell → book $11,175.07; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 47 | $79.34 | $2.17 | $+99.57 | $9,316.50 | ▲ +99.57 after sell → book $11,172.90; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 52 | $35.70 | $2.17 | $+29.48 | $11,170.73 | ▲ +29.48 after sell → book $11,170.73; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 27 | $267.02 | $2.07 | — | $3,959.12 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $7447.15 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 31 | $118.50 | $2.08 | — | $283.54 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $3723.58 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $283.54 | ▲ close $11,166.73 vs 09:30 $11,177.25 (session +0.15) | 16:00 close · cash $283.54 · equity $11,166.73 vs 09:30 $11,177.25 (-10.52; session marks +0.15) · 2 name(s) marked open→close (per-name table). FNV×27 09:30 $267.02 → close $267.37 +9.45; CM×31 09:30 $118.50 → close $118.20 -9.30 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $283.54 | ▲ 09:30 equity $11,180.62 vs yday $11,166.73 (+13.89) | 09:30 open · cash $283.54 (unchanged overnight, no fees) · equity $11,180.62 vs prior close $11,166.73 (+13.89) · 2 name(s) re-marked at the open (per-name table). FNV×27 yday $267.37 → 09:30 $267.23 -3.78; CM×31 yday $118.20 → 09:30 $118.77 +17.67 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 27 | $267.23 | $2.14 | $+1.46 | $7,496.61 | ▲ +1.46 after sell → book $11,178.48; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 45 | $81.65 | $2.12 | — | $3,820.23 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $3748.30 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 2 | $967.01 | $2.00 | — | $1,884.22 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $2498.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,884.22 | ▼ close $10,937.09 vs 09:30 $11,180.62 (session -237.27) | 16:00 close · cash $1,884.22 · equity $10,937.09 vs 09:30 $11,180.62 (-243.53; session marks -237.27) · 3 name(s) marked open→close (per-name table). CM×31 09:30 $118.77 → close $114.84 -121.83; ACMR×45 09:30 $81.65 → close $80.49 -52.20; MU×2 09:30 $967.01 → close $935.39 -63.24 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,884.22 | ▼ 09:30 equity $10,875.41 vs yday $10,937.09 (-61.68) | 09:30 open · cash $1,884.22 (unchanged overnight, no fees) · equity $10,875.41 vs prior close $10,937.09 (-61.68) · 3 name(s) re-marked at the open (per-name table). CM×31 yday $114.84 → 09:30 $115.66 +25.42; ACMR×45 yday $80.49 → 09:30 $79.27 -54.90; MU×2 yday $935.39 → 09:30 $919.29 -32.20 | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 31 | $115.66 | $2.12 | $-92.24 | $5,467.55 | ▼ -92.24 after sell → book $10,873.28; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 45 | $79.27 | $2.16 | $-111.39 | $9,032.54 | ▼ -111.39 after sell → book $10,871.12; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 2 | $919.29 | $2.02 | $-99.46 | $10,869.10 | ▼ -99.46 after sell → book $10,869.10; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 13 | $324.41 | $2.03 | — | $6,649.74 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $4347.64 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 23 | $141.76 | $2.06 | — | $3,387.20 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $3260.73 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 5 | $400.42 | $2.00 | — | $1,383.10 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $2173.82 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,383.10 | ▼ close $10,451.82 vs 09:30 $10,875.41 (session -411.19) | 16:00 close · cash $1,383.10 · equity $10,451.82 vs 09:30 $10,875.41 (-423.59; session marks -411.19) · 3 name(s) marked open→close (per-name table). KEYS×13 09:30 $324.41 → close $319.97 -57.72; SMTC×23 09:30 $141.76 → close $131.17 -243.57; CIEN×5 09:30 $400.42 → close $378.44 -109.90 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,383.10 | ▲ 09:30 equity $10,510.57 vs yday $10,451.82 (+58.75) | 09:30 open · cash $1,383.10 (unchanged overnight, no fees) · equity $10,510.57 vs prior close $10,451.82 (+58.75) · 3 name(s) re-marked at the open (per-name table). KEYS×13 yday $319.97 → 09:30 $322.49 +32.76; SMTC×23 yday $131.17 → 09:30 $132.30 +25.99; CIEN×5 yday $378.44 → 09:30 $378.44 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 13 | $322.49 | $2.07 | $-29.06 | $5,573.39 | ▼ -29.06 after sell → book $10,508.49; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 23 | $132.30 | $2.09 | $-221.73 | $8,614.20 | ▼ -221.73 after sell → book $10,506.40; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 5 | $378.44 | $2.03 | $-113.94 | $10,504.37 | ▼ -113.94 after sell → book $10,504.37; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,504.37 | ▲ close $10,504.37 vs 09:30 $10,510.57 (session +0.00) | 16:00 close · cash $10,504.37 · no lots left · equity $10,504.37. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,504.37 | ▲ 09:30 equity $10,504.37 vs yday $10,504.37 (+0.00) | 09:30 open · cash $10,504.37 · no holdings · equity $10,504.37 vs prior close $10,504.37 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,504.37 | ▲ close $10,504.37 vs 09:30 $10,504.37 (session +0.00) | 16:00 close · cash $10,504.37 · no lots left · equity $10,504.37. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,504.37 | ▲ 09:30 equity $10,504.37 vs yday $10,504.37 (+0.00) | 09:30 open · cash $10,504.37 · no holdings · equity $10,504.37 vs prior close $10,504.37 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,504.37 | ▲ close $10,504.37 vs 09:30 $10,504.37 (session +0.00) | 16:00 close · cash $10,504.37 · no lots left · equity $10,504.37. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,504.37 | ▲ 09:30 equity $10,504.37 vs yday $10,504.37 (+0.00) | 09:30 open · cash $10,504.37 · no holdings · equity $10,504.37 vs prior close $10,504.37 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 11 | $351.74 | $2.02 | — | $6,633.21 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $4201.75 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 6 | $486.31 | $2.01 | — | $3,713.34 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $3151.31 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 65 | $32.31 | $2.19 | — | $1,611.00 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $2100.87 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 66 | $15.87 | $2.19 | — | $561.40 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1050.44 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $561.40 | ▲ close $10,891.80 vs 09:30 $10,504.37 (session +395.83) | 16:00 close · cash $561.40 · equity $10,891.80 vs 09:30 $10,504.37 (+387.43; session marks +395.83) · 4 name(s) marked open→close (per-name table). AVGO×11 09:30 $351.74 → close $357.16 +59.62; DELL×6 09:30 $486.31 → close $516.39 +180.48; CXW×65 09:30 $32.31 → close $33.66 +87.75; FRNM×66 09:30 $15.87 → close $16.90 +67.98 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $561.40 | ▼ 09:30 equity $10,858.08 vs yday $10,891.80 (-33.72) | 09:30 open · cash $561.40 (unchanged overnight, no fees) · equity $10,858.08 vs prior close $10,891.80 (-33.72) · 4 name(s) re-marked at the open (per-name table). AVGO×11 yday $357.16 → 09:30 $359.70 +27.94; DELL×6 yday $516.39 → 09:30 $513.78 -15.66; CXW×65 yday $33.66 → 09:30 $33.46 -13.00; FRNM×66 yday $16.90 → 09:30 $16.40 -33.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 11 | $359.70 | $2.06 | $+83.47 | $4,516.03 | ▲ +83.47 after sell → book $10,856.01; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 6 | $513.78 | $2.04 | $+160.77 | $7,596.67 | ▲ +160.77 after sell → book $10,853.97; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 65 | $33.46 | $2.21 | $+70.35 | $9,769.36 | ▲ +70.35 after sell → book $10,851.76; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 18 | $263.36 | $2.04 | — | $5,026.83 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $4884.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 43 | $75.65 | $2.12 | — | $1,771.76 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $3256.45 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 6 | $236.82 | $2.01 | — | $348.84 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+8.1; leftover $1628.23 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $348.84 | ▲ close $10,974.27 vs 09:30 $10,858.08 (session +128.68) | 16:00 close · cash $348.84 · equity $10,974.27 vs 09:30 $10,858.08 (+116.19; session marks +128.68) · 4 name(s) marked open→close (per-name table). FRNM×66 09:30 $16.40 → close $16.31 -5.94; CRM×18 09:30 $263.36 → close $259.23 -74.34; MRX×43 09:30 $75.65 → close $78.27 +112.66; BE×6 09:30 $236.82 → close $252.87 +96.30 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $348.84 | ▲ 09:30 equity $11,017.32 vs yday $10,974.27 (+43.05) | 09:30 open · cash $348.84 (unchanged overnight, no fees) · equity $11,017.32 vs prior close $10,974.27 (+43.05) · 4 name(s) re-marked at the open (per-name table). FRNM×66 yday $16.31 → 09:30 $16.74 +28.38; CRM×18 yday $259.23 → 09:30 $253.72 -99.18; MRX×43 yday $78.27 → 09:30 $78.84 +24.51; BE×6 yday $252.87 → 09:30 $267.76 +89.34 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 66 | $16.74 | $2.21 | $+53.02 | $1,451.47 | ▲ +53.02 after sell → book $11,015.11; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 18 | $253.72 | $2.09 | $-177.65 | $6,016.34 | ▼ -177.65 after sell → book $11,013.02; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 43 | $78.84 | $2.16 | $+132.89 | $9,404.30 | ▲ +132.89 after sell → book $11,010.86; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 6 | $267.76 | $2.03 | $+181.60 | $11,008.83 | ▲ +181.60 after sell → book $11,008.83; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,008.83 | ▲ close $11,008.83 vs 09:30 $11,017.32 (session +0.00) | 16:00 close · cash $11,008.83 · no lots left · equity $11,008.83. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,008.83 | ▲ 09:30 equity $11,008.83 vs yday $11,008.83 (-0.00) | 09:30 open · cash $11,008.83 · no holdings · equity $11,008.83 vs prior close $11,008.83 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,008.83 | ▲ close $11,008.83 vs 09:30 $11,008.83 (session +0.00) | 16:00 close · cash $11,008.83 · no lots left · equity $11,008.83. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,008.83 | ▲ 09:30 equity $11,008.83 vs yday $11,008.83 (-0.00) | 09:30 open · cash $11,008.83 · no holdings · equity $11,008.83 vs prior close $11,008.83 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,008.83 | ▲ close $11,008.83 vs 09:30 $11,008.83 (session +0.00) | 16:00 close · cash $11,008.83 · no lots left · equity $11,008.83. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,008.83 | ▲ 09:30 equity $11,008.83 vs yday $11,008.83 (-0.00) | 09:30 open · cash $11,008.83 · no holdings · equity $11,008.83 vs prior close $11,008.83 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 66 | $164.43 | $2.19 | — | $154.26 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $11008.83 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $154.26 | ▼ close $10,072.74 vs 09:30 $11,008.83 (session -933.90) | 16:00 close · cash $154.26 · equity $10,072.74 vs 09:30 $11,008.83 (-936.09; session marks -933.90) · 1 name(s) marked open→close (per-name table). ORCL×66 09:30 $164.43 → close $150.28 -933.90 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.26 | ▼ 09:30 equity $9,487.98 vs yday $10,072.74 (-584.76) | 09:30 open · cash $154.26 (unchanged overnight, no fees) · equity $9,487.98 vs prior close $10,072.74 (-584.76) · 1 name(s) re-marked at the open (per-name table). ORCL×66 yday $150.28 → 09:30 $141.42 -584.76 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 66 | $141.42 | $2.27 | $-1523.12 | $9,485.71 | ▼ -1,523.12 after sell → book $9,485.71; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,485.71 | ▲ close $9,485.71 vs 09:30 $9,487.98 (session +0.00) | 16:00 close · cash $9,485.71 · no lots left · equity $9,485.71. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,485.71 | ▲ 09:30 equity $9,485.71 vs yday $9,485.71 (-0.00) | 09:30 open · cash $9,485.71 · no holdings · equity $9,485.71 vs prior close $9,485.71 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,485.71 | ▲ close $9,485.71 vs 09:30 $9,485.71 (session +0.00) | 16:00 close · cash $9,485.71 · no lots left · equity $9,485.71. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,485.71 | ▲ 09:30 equity $9,485.71 vs yday $9,485.71 (-0.00) | 09:30 open · cash $9,485.71 · no holdings · equity $9,485.71 vs prior close $9,485.71 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 180 | $26.27 | $2.53 | — | $4,754.58 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $4742.85 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 16 | $189.17 | $2.04 | — | $1,725.82 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $3161.90 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 39 | $39.99 | $2.11 | — | $164.10 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $1580.95 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $164.10 | ▼ close $9,395.98 vs 09:30 $9,485.71 (session -83.05) | 16:00 close · cash $164.10 · equity $9,395.98 vs 09:30 $9,485.71 (-89.73; session marks -83.05) · 3 name(s) marked open→close (per-name table). WAY×180 09:30 $26.27 → close $26.59 +57.60; QCOM×16 09:30 $189.17 → close $184.84 -69.28; SM×39 09:30 $39.99 → close $38.16 -71.37 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $164.10 | ▲ 09:30 equity $9,446.73 vs yday $9,395.98 (+50.75) | 09:30 open · cash $164.10 (unchanged overnight, no fees) · equity $9,446.73 vs prior close $9,395.98 (+50.75) · 3 name(s) re-marked at the open (per-name table). WAY×180 yday $26.59 → 09:30 $26.51 -14.40; QCOM×16 yday $184.84 → 09:30 $190.35 +88.16; SM×39 yday $38.16 → 09:30 $37.57 -23.01 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 180 | $26.51 | $2.60 | $+38.07 | $4,933.30 | ▲ +38.07 after sell → book $9,444.13; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 16 | $190.35 | $2.07 | $+14.77 | $7,976.83 | ▲ +14.77 after sell → book $9,442.06; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 39 | $37.57 | $2.13 | $-98.62 | $9,439.93 | ▼ -98.62 after sell → book $9,439.93; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 22 | $170.85 | $2.06 | — | $5,679.18 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $3775.97 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AVTR` | 179 | $15.81 | $2.53 | — | $2,846.66 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $2831.98 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 85 | $22.12 | $2.25 | — | $964.22 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1887.99 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 3 | $238.60 | $2.00 | — | $246.42 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_mover; ret5=-11.6; leftover $943.99 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $246.42 | ▲ close $9,651.39 vs 09:30 $9,446.73 (session +220.28) | 16:00 close · cash $246.42 · equity $9,651.39 vs 09:30 $9,446.73 (+204.66; session marks +220.28) · 4 name(s) marked open→close (per-name table). SMTC×22 09:30 $170.85 → close $178.19 +161.48; AVTR×179 09:30 $15.81 → close $15.86 +8.95; GME×85 09:30 $22.12 → close $22.77 +55.25; JBHT×3 09:30 $238.60 → close $236.80 -5.40 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $246.42 | ▲ 09:30 equity $9,755.31 vs yday $9,651.39 (+103.92) | 09:30 open · cash $246.42 (unchanged overnight, no fees) · equity $9,755.31 vs prior close $9,651.39 (+103.92) · 4 name(s) re-marked at the open (per-name table). SMTC×22 yday $178.19 → 09:30 $182.33 +91.08; AVTR×179 yday $15.86 → 09:30 $15.87 +1.79; GME×85 yday $22.77 → 09:30 $22.90 +11.05; JBHT×3 yday $236.80 → 09:30 $236.80 +0.00 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 22 | $182.33 | $2.10 | $+248.41 | $4,255.58 | ▲ +248.41 after sell → book $9,753.21; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 179 | $15.87 | $2.58 | $+5.63 | $7,093.73 | ▲ +5.63 after sell → book $9,750.63; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 3 | $236.80 | $2.02 | $-9.42 | $7,802.11 | ▼ -9.42 after sell → book $9,748.61; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 186 | $20.91 | $2.55 | — | $3,910.30 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $3901.05 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 175 | $14.79 | $2.52 | — | $1,319.54 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $2600.70 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 92 | $14.07 | $2.27 | — | $22.83 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1300.35 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.83 | ▼ close $9,680.86 vs 09:30 $9,755.31 (session -60.42) | 16:00 close · cash $22.83 · equity $9,680.86 vs 09:30 $9,755.31 (-74.45; session marks -60.42) · 4 name(s) marked open→close (per-name table). GME×85 09:30 $22.90 → close $22.64 -22.10; TH×186 09:30 $20.91 → close $21.19 +52.08; RARE×175 09:30 $14.79 → close $14.51 -49.00; BHVN×92 09:30 $14.07 → close $13.62 -41.40 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.83 | ▲ 09:30 equity $9,816.33 vs yday $9,680.86 (+135.47) | 09:30 open · cash $22.83 (unchanged overnight, no fees) · equity $9,816.33 vs prior close $9,680.86 (+135.47) · 4 name(s) re-marked at the open (per-name table). GME×85 yday $22.64 → 09:30 $22.78 +11.90; TH×186 yday $21.19 → 09:30 $21.65 +85.56; RARE×175 yday $14.51 → 09:30 $14.58 +12.25; BHVN×92 yday $13.62 → 09:30 $13.90 +25.76 | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 85 | $22.78 | $2.27 | $+51.58 | $1,956.86 | ▲ +51.58 after sell → book $9,814.06; vs 09:30 mark -2.27 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 186 | $21.65 | $2.61 | $+132.48 | $5,981.14 | ▲ +132.48 after sell → book $9,811.44; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 175 | $14.58 | $2.56 | $-41.83 | $8,530.08 | ▼ -41.83 after sell → book $9,808.88; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 92 | $13.90 | $2.29 | $-20.20 | $9,806.59 | ▼ -20.20 after sell → book $9,806.59; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 21 | $230.25 | $2.05 | — | $4,969.29 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+12.5; leftover $4903.29 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 17 | $190.30 | $2.04 | — | $1,732.14 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+10.6; leftover $3268.86 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 62 | $25.95 | $2.18 | — | $121.07 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1634.43 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $121.07 | ▼ close $9,454.60 vs 09:30 $9,816.33 (session -345.72) | 16:00 close · cash $121.07 · equity $9,454.60 vs 09:30 $9,816.33 (-361.73; session marks -345.72) · 3 name(s) marked open→close (per-name table). VICR×21 09:30 $230.25 → close $223.90 -133.35; SMTC×17 09:30 $190.30 → close $177.37 -219.81; GLXY×62 09:30 $25.95 → close $26.07 +7.44 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $121.07 | ▲ 09:30 equity $9,766.81 vs yday $9,454.60 (+312.21) | 09:30 open · cash $121.07 (unchanged overnight, no fees) · equity $9,766.81 vs prior close $9,454.60 (+312.21) · 3 name(s) re-marked at the open (per-name table). VICR×21 yday $223.90 → 09:30 $241.04 +359.94; SMTC×17 yday $177.37 → 09:30 $175.00 -40.29; GLXY×62 yday $26.07 → 09:30 $25.95 -7.44 | — |
| 2026-09-22 09:30 ET | **SELL** | `VICR` | 21 | $241.04 | $2.10 | $+222.43 | $5,180.81 | ▲ +222.43 after sell → book $9,764.71; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SMTC` | 17 | $175.00 | $2.07 | $-264.22 | $8,153.73 | ▼ -264.22 after sell → book $9,762.63; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `GLXY` | 62 | $25.95 | $2.20 | $-4.38 | $9,760.43 | ▼ -4.38 after sell → book $9,760.43; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,760.43 | ▲ close $9,760.43 vs 09:30 $9,766.81 (session +0.00) | 16:00 close · cash $9,760.43 · no lots left · equity $9,760.43. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1249.43 < 1 share @ 1746.53 |
| 2026-08-28 | `MPWR` | cash | leftover split 1086.91 < 1 share @ 1306.03 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
