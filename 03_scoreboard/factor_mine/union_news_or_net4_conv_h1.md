# Factor mine action — `union_news_or_net4_conv_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `conviction` · sell `list` · S-boost `none` · OR news + net≥4; 70% leftover if #1 net ≥ 5

Cash book **+1.49%** ($10,149) · signal-only (no cash/fees) was -7.33%. Starts YES **7/29**. Fills 87 · skips 24 · realized $+793.85.

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
- **Gate** `news_or_headline=True,cam_net_min=4` · **rank** `cond` · **top_n** 4.
- **Size** `conviction` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $1.41.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `HLIT` | 531 | — | $13.18 | +0.00 | $13.92 | +392.94 | +392.94 | +0.00 | +392.94 |
| 2026-08-14 | `ANGX` | 232 | — | $4.31 | +0.00 | $4.37 | +13.92 | +13.92 | +0.00 | +13.92 |
| 2026-08-14 | `ARX` | 51 | — | $19.57 | +0.00 | $19.58 | +0.51 | +0.51 | +0.00 | +0.51 |
| 2026-08-17 | `HLIT` | 531 | $13.92 | $13.84 | -42.48 | — | +0.00 | -42.48 | +350.46 | — |
| 2026-08-17 | `ANGX` | 232 | $4.37 | $4.60 | +53.36 | — | +0.00 | +53.36 | +67.28 | — |
| 2026-08-17 | `ARX` | 51 | $19.58 | $19.57 | -0.51 | — | +0.00 | -0.51 | +0.00 | — |
| 2026-08-17 | `DVN` | 112 | — | $46.18 | +0.00 | $47.57 | +155.68 | +155.68 | +0.00 | +155.68 |
| 2026-08-17 | `EOG` | 24 | — | $142.77 | +0.00 | $146.15 | +81.12 | +81.12 | +0.00 | +81.12 |
| 2026-08-17 | `FANG` | 8 | — | $202.70 | +0.00 | $206.29 | +28.72 | +28.72 | +0.00 | +28.72 |
| 2026-08-18 | `DVN` | 112 | $47.57 | $48.00 | +48.16 | — | +0.00 | +48.16 | +203.84 | — |
| 2026-08-18 | `EOG` | 24 | $146.15 | $148.04 | +45.36 | — | +0.00 | +45.36 | +126.48 | — |
| 2026-08-18 | `FANG` | 8 | $206.29 | $208.93 | +21.12 | — | +0.00 | +21.12 | +49.84 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 82 | — | $91.01 | +0.00 | $93.63 | +214.84 | +214.84 | +0.00 | +214.84 |
| 2026-08-20 | `APA` | 24 | — | $44.76 | +0.00 | $44.39 | -8.88 | -8.88 | +0.00 | -8.88 |
| 2026-08-20 | `AUTL` | 435 | — | $2.47 | +0.00 | $2.46 | -4.35 | -4.35 | +0.00 | -4.35 |
| 2026-08-20 | `CRSP` | 18 | — | $58.73 | +0.00 | $58.12 | -10.98 | -10.98 | +0.00 | -10.98 |
| 2026-08-21 | `BHP` | 82 | $93.63 | $95.72 | +171.38 | — | +0.00 | +171.38 | +386.22 | — |
| 2026-08-21 | `APA` | 24 | $44.39 | $44.52 | +3.12 | — | +0.00 | +3.12 | -5.76 | — |
| 2026-08-21 | `AUTL` | 435 | $2.46 | $2.47 | +4.35 | $2.41 | -26.10 | -21.75 | +0.00 | -26.10 |
| 2026-08-21 | `CRSP` | 18 | $58.12 | $59.72 | +28.80 | $59.50 | -3.96 | +24.84 | +17.82 | +13.86 |
| 2026-08-21 | `AU` | 52 | — | $119.43 | +0.00 | $121.22 | +93.08 | +93.08 | +0.00 | +93.08 |
| 2026-08-21 | `FUTU` | 23 | — | $115.18 | +0.00 | $123.64 | +194.58 | +194.58 | +0.00 | +194.58 |
| 2026-08-24 | `AUTL` | 435 | $2.41 | $2.40 | -4.35 | — | +0.00 | -4.35 | -30.45 | — |
| 2026-08-24 | `CRSP` | 18 | $59.50 | $58.75 | -13.50 | $57.08 | -30.15 | -43.65 | +0.36 | -29.79 |
| 2026-08-24 | `AU` | 52 | $121.22 | $120.51 | -36.92 | — | +0.00 | -36.92 | +56.16 | — |
| 2026-08-24 | `FUTU` | 23 | $123.64 | $121.00 | -60.72 | — | +0.00 | -60.72 | +133.86 | — |
| 2026-08-25 | `CRSP` | 18 | $57.08 | $57.93 | +15.39 | — | +0.00 | +15.39 | -14.40 | — |
| 2026-08-25 | `AU` | 66 | — | $118.52 | +0.00 | $123.39 | +321.42 | +321.42 | +0.00 | +321.42 |
| 2026-08-25 | `FCX` | 21 | — | $77.13 | +0.00 | $79.91 | +58.38 | +58.38 | +0.00 | +58.38 |
| 2026-08-25 | `EZPW` | 48 | — | $35.05 | +0.00 | $35.23 | +8.64 | +8.64 | +0.00 | +8.64 |
| 2026-08-26 | `AU` | 66 | $123.39 | $119.80 | -236.94 | — | +0.00 | -236.94 | +84.48 | — |
| 2026-08-26 | `FCX` | 21 | $79.91 | $79.34 | -11.97 | — | +0.00 | -11.97 | +46.41 | — |
| 2026-08-26 | `EZPW` | 48 | $35.23 | $35.70 | +22.56 | — | +0.00 | +22.56 | +31.20 | — |
| 2026-08-26 | `FNV` | 29 | — | $267.02 | +0.00 | $267.37 | +10.15 | +10.15 | +0.00 | +10.15 |
| 2026-08-26 | `CM` | 28 | — | $118.50 | +0.00 | $118.20 | -8.40 | -8.40 | +0.00 | -8.40 |
| 2026-08-27 | `FNV` | 29 | $267.37 | $267.23 | -4.06 | — | +0.00 | -4.06 | +6.09 | — |
| 2026-08-27 | `CM` | 28 | $118.20 | $118.77 | +15.96 | $114.84 | -110.04 | -94.08 | +7.56 | -102.48 |
| 2026-08-27 | `ACMR` | 69 | — | $81.65 | +0.00 | $80.49 | -80.04 | -80.04 | +0.00 | -80.04 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-28 | `CM` | 28 | $114.84 | $115.66 | +22.96 | — | +0.00 | +22.96 | -79.52 | — |
| 2026-08-28 | `ACMR` | 69 | $80.49 | $79.27 | -84.18 | — | +0.00 | -84.18 | -164.22 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `KEYS` | 23 | — | $324.41 | +0.00 | $319.97 | -102.12 | -102.12 | +0.00 | -102.12 |
| 2026-08-28 | `SMTC` | 7 | — | $141.76 | +0.00 | $131.17 | -74.13 | -74.13 | +0.00 | -74.13 |
| 2026-08-28 | `CIEN` | 2 | — | $400.42 | +0.00 | $378.44 | -43.96 | -43.96 | +0.00 | -43.96 |
| 2026-08-31 | `KEYS` | 23 | $319.97 | $322.49 | +57.96 | — | +0.00 | +57.96 | -44.16 | — |
| 2026-08-31 | `SMTC` | 7 | $131.17 | $132.30 | +7.91 | — | +0.00 | +7.91 | -66.22 | — |
| 2026-08-31 | `CIEN` | 2 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -43.96 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 21 | — | $351.74 | +0.00 | $357.16 | +113.82 | +113.82 | +0.00 | +113.82 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `CXW` | 33 | — | $32.31 | +0.00 | $33.66 | +44.55 | +44.55 | +0.00 | +44.55 |
| 2026-09-03 | `FRNM` | 68 | — | $15.87 | +0.00 | $16.90 | +70.04 | +70.04 | +0.00 | +70.04 |
| 2026-09-04 | `AVGO` | 21 | $357.16 | $359.70 | +53.34 | — | +0.00 | +53.34 | +167.16 | — |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | — | +0.00 | -5.22 | +54.94 | — |
| 2026-09-04 | `CXW` | 33 | $33.66 | $33.46 | -6.60 | — | +0.00 | -6.60 | +37.95 | — |
| 2026-09-04 | `FRNM` | 68 | $16.90 | $16.40 | -34.00 | $16.31 | -6.12 | -40.12 | +36.04 | +29.92 |
| 2026-09-04 | `CRM` | 26 | — | $263.36 | +0.00 | $259.23 | -107.38 | -107.38 | +0.00 | -107.38 |
| 2026-09-04 | `MRX` | 20 | — | $75.65 | +0.00 | $78.27 | +52.40 | +52.40 | +0.00 | +52.40 |
| 2026-09-04 | `BE` | 6 | — | $236.82 | +0.00 | $252.87 | +96.30 | +96.30 | +0.00 | +96.30 |
| 2026-09-08 | `FRNM` | 68 | $16.31 | $16.74 | +29.24 | — | +0.00 | +29.24 | +59.16 | — |
| 2026-09-08 | `CRM` | 26 | $259.23 | $253.72 | -143.26 | — | +0.00 | -143.26 | -250.64 | — |
| 2026-09-08 | `MRX` | 20 | $78.27 | $78.84 | +11.40 | — | +0.00 | +11.40 | +63.80 | — |
| 2026-09-08 | `BE` | 6 | $252.87 | $267.76 | +89.34 | — | +0.00 | +89.34 | +185.64 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 68 | — | $164.43 | +0.00 | $150.28 | -962.20 | -962.20 | +0.00 | -962.20 |
| 2026-09-14 | `ORCL` | 68 | $150.28 | $141.42 | -602.48 | — | +0.00 | -602.48 | -1564.68 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 257 | — | $26.27 | +0.00 | $26.59 | +82.24 | +82.24 | +0.00 | +82.24 |
| 2026-09-16 | `QCOM` | 7 | — | $189.17 | +0.00 | $184.84 | -30.31 | -30.31 | +0.00 | -30.31 |
| 2026-09-16 | `SM` | 36 | — | $39.99 | +0.00 | $38.16 | -65.88 | -65.88 | +0.00 | -65.88 |
| 2026-09-17 | `WAY` | 257 | $26.59 | $26.51 | -20.56 | — | +0.00 | -20.56 | +61.68 | — |
| 2026-09-17 | `QCOM` | 7 | $184.84 | $190.35 | +38.57 | — | +0.00 | +38.57 | +8.26 | — |
| 2026-09-17 | `SM` | 36 | $38.16 | $37.57 | -21.24 | — | +0.00 | -21.24 | -87.12 | — |
| 2026-09-17 | `SMTC` | 39 | — | $170.85 | +0.00 | $178.19 | +286.26 | +286.26 | +0.00 | +286.26 |
| 2026-09-17 | `CLS` | 2 | — | $337.75 | +0.00 | $329.94 | -15.62 | -15.62 | +0.00 | -15.62 |
| 2026-09-17 | `GME` | 43 | — | $22.12 | +0.00 | $22.77 | +27.95 | +27.95 | +0.00 | +27.95 |
| 2026-09-17 | `JBHT` | 4 | — | $238.60 | +0.00 | $236.80 | -7.20 | -7.20 | +0.00 | -7.20 |
| 2026-09-18 | `SMTC` | 39 | $178.19 | $182.33 | +161.46 | — | +0.00 | +161.46 | +447.72 | — |
| 2026-09-18 | `CLS` | 2 | $329.94 | $332.06 | +4.24 | $332.63 | +1.14 | +5.38 | -11.38 | -10.24 |
| 2026-09-18 | `GME` | 43 | $22.77 | $22.90 | +5.59 | $22.64 | -11.18 | -5.59 | +33.54 | +22.36 |
| 2026-09-18 | `JBHT` | 4 | $236.80 | $236.80 | +0.00 | — | +0.00 | +0.00 | -7.20 | — |
| 2026-09-18 | `TH` | 281 | — | $20.91 | +0.00 | $21.19 | +78.68 | +78.68 | +0.00 | +78.68 |
| 2026-09-18 | `RARE` | 170 | — | $14.79 | +0.00 | $14.51 | -47.60 | -47.60 | +0.00 | -47.60 |
| 2026-09-21 | `CLS` | 2 | $332.63 | $341.45 | +17.64 | — | +0.00 | +17.64 | +7.40 | — |
| 2026-09-21 | `GME` | 43 | $22.64 | $22.78 | +6.02 | — | +0.00 | +6.02 | +28.38 | — |
| 2026-09-21 | `TH` | 281 | $21.19 | $21.65 | +129.26 | — | +0.00 | +129.26 | +207.94 | — |
| 2026-09-21 | `RARE` | 170 | $14.51 | $14.58 | +11.90 | — | +0.00 | +11.90 | -35.70 | — |
| 2026-09-21 | `VICR` | 31 | — | $230.25 | +0.00 | $223.90 | -196.85 | -196.85 | +0.00 | -196.85 |
| 2026-09-21 | `SMTC` | 8 | — | $190.30 | +0.00 | $177.37 | -103.44 | -103.44 | +0.00 | -103.44 |
| 2026-09-21 | `GLXY` | 59 | — | $25.95 | +0.00 | $26.07 | +7.08 | +7.08 | +0.00 | +7.08 |
| 2026-09-22 | `VICR` | 31 | $223.90 | $252.37 | +882.57 | — | +0.00 | +882.57 | +685.72 | — |
| 2026-09-22 | `SMTC` | 8 | $177.37 | $177.30 | -0.56 | — | +0.00 | -0.56 | -104.00 | — |
| 2026-09-22 | `GLXY` | 59 | $26.07 | $25.67 | -23.60 | — | +0.00 | -23.60 | -16.52 | — |
| 2026-09-23 | `PGEN` | 950 | — | $7.95 | +0.00 | $7.44 | -484.50 | -484.50 | +0.00 | -484.50 |
| 2026-09-23 | `SGRY` | 103 | — | $15.70 | +0.00 | $14.56 | -117.42 | -117.42 | +0.00 | -117.42 |
| 2026-09-23 | `VERI` | 1225 | — | $1.30 | +0.00 | $1.29 | -12.25 | -12.25 | +0.00 | -12.25 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +407.37 | HLIT, ANGX, ARX | — | $991.44 | $10,395.38 | HLIT×531, ANGX×232, ARX×51 |
| 2026-08-17 | +2.25 | $991.44 | HLIT×531, ANGX×232, ARX×51 | $10,405.75 | +10.37 | +265.52 | DVN, EOG, FANG | HLIT, ANGX, ARX | $166.91 | $10,652.67 | DVN×112, EOG×24, FANG×8 |
| 2026-08-18 | -6.20 | $166.91 | DVN×112, EOG×24, FANG×8 | $10,767.31 | +114.64 | +0.00 | — | DVN, EOG, FANG | $10,760.79 | $10,760.79 | — |
| 2026-08-19 | -7.20 | $10,760.79 | — | $10,760.79 | -0.00 | +0.00 | — | — | $10,760.79 | $10,760.79 | — |
| 2026-08-20 | +1.12 | $10,760.79 | — | $10,760.79 | -0.00 | +190.63 | BHP, APA, AUTL, CRSP | — | $80.18 | $10,939.46 | BHP×82, APA×24, AUTL×435, CRSP×18 |
| 2026-08-21 | +3.25 | $80.18 | BHP×82, APA×24, AUTL×435, CRSP×18 | $11,147.11 | +207.65 | +257.60 | AU, FUTU | BHP, APA | $129.60 | $11,396.11 | AUTL×435, CRSP×18, AU×52, FUTU×23 |
| 2026-08-24 | -5.17 | $129.60 | AUTL×435, CRSP×18, AU×52, FUTU×23 | $11,280.62 | -115.49 | -30.15 | — | AUTL, AU, FUTU | $10,213.13 | $11,240.48 | CRSP×18 |
| 2026-08-25 | +1.80 | $10,213.13 | CRSP×18 | $11,255.87 | +15.39 | +388.44 | AU, FCX, EZPW | CRSP | $122.98 | $11,635.87 | AU×66, FCX×21, EZPW×48 |
| 2026-08-26 | +2.02 | $122.98 | AU×66, FCX×21, EZPW×48 | $11,409.52 | -226.35 | +1.75 | FNV, CM | AU, FCX, EZPW | $337.30 | $11,400.63 | FNV×29, CM×28 |
| 2026-08-27 | — | $337.30 | FNV×29, CM×28 | $11,412.53 | +11.90 | -221.70 | ACMR, MU | FNV | $1,479.77 | $11,184.49 | CM×28, ACMR×69, MU×1 |
| 2026-08-28 | +0.75 | $1,479.77 | CM×28, ACMR×69, MU×1 | $11,107.17 | -77.32 | -220.21 | KEYS, SMTC, CIEN | CM, ACMR, MU | $1,840.14 | $10,874.52 | KEYS×23, SMTC×7, CIEN×2 |
| 2026-08-31 | -5.85 | $1,840.14 | KEYS×23, SMTC×7, CIEN×2 | $10,940.39 | +65.87 | +0.00 | — | KEYS, SMTC, CIEN | $10,934.21 | $10,934.21 | — |
| 2026-09-01 | -6.30 | $10,934.21 | — | $10,934.21 | +0.00 | +0.00 | — | — | $10,934.21 | $10,934.21 | — |
| 2026-09-02 | -3.83 | $10,934.21 | — | $10,934.21 | +0.00 | +0.00 | — | — | $10,934.21 | $10,934.21 | — |
| 2026-09-03 | -0.90 | $10,934.21 | — | $10,934.21 | +0.00 | +288.57 | AVGO, DELL, CXW, FRNM | — | $421.33 | $11,214.45 | AVGO×21, DELL×2, CXW×33, FRNM×68 |
| 2026-09-04 | +2.25 | $421.33 | AVGO×21, DELL×2, CXW×33, FRNM×68 | $11,221.97 | +7.52 | +35.20 | CRM, MRX, BE | AVGO, DELL, CXW | $313.11 | $11,244.79 | FRNM×68, CRM×26, MRX×20, BE×6 |
| 2026-09-08 | -11.47 | $313.11 | FRNM×68, CRM×26, MRX×20, BE×6 | $11,231.51 | -13.28 | +0.00 | — | FRNM, CRM, MRX, BE | $11,223.06 | $11,223.06 | — |
| 2026-09-09 | -13.95 | $11,223.06 | — | $11,223.06 | +0.00 | +0.00 | — | — | $11,223.06 | $11,223.06 | — |
| 2026-09-10 | -13.28 | $11,223.06 | — | $11,223.06 | +0.00 | +0.00 | — | — | $11,223.06 | $11,223.06 | — |
| 2026-09-11 | +0.50 | $11,223.06 | — | $11,223.06 | +0.00 | -962.20 | ORCL | — | $39.63 | $10,258.67 | ORCL×68 |
| 2026-09-14 | -11.00 | $39.63 | ORCL×68 | $9,656.19 | -602.48 | +0.00 | — | ORCL | $9,653.91 | $9,653.91 | — |
| 2026-09-15 | -3.84 | $9,653.91 | — | $9,653.91 | -0.00 | +0.00 | — | — | $9,653.91 | $9,653.91 | — |
| 2026-09-16 | +5.30 | $9,653.91 | — | $9,653.91 | -0.00 | -13.95 | WAY, QCOM, SM | — | $131.26 | $9,632.53 | WAY×257, QCOM×7, SM×36 |
| 2026-09-17 | +7.38 | $131.26 | WAY×257, QCOM×7, SM×36 | $9,629.30 | -3.23 | +291.39 | SMTC, CLS, GME, JBHT | WAY, QCOM, SM | $369.31 | $9,904.91 | SMTC×39, CLS×2, GME×43, JBHT×4 |
| 2026-09-18 | +4.86 | $369.31 | SMTC×39, CLS×2, GME×43, JBHT×4 | $10,076.20 | +171.29 | +21.04 | TH, RARE | SMTC, JBHT | $27.05 | $10,086.92 | CLS×2, GME×43, TH×281, RARE×170 |
| 2026-09-21 | +12.87 | $27.05 | CLS×2, GME×43, TH×281, RARE×170 | $10,251.74 | +164.82 | -293.21 | VICR, SMTC, GLXY | CLS, GME, TH, RARE | $43.85 | $9,941.84 | VICR×31, SMTC×8, GLXY×59 |
| 2026-09-22 | -0.50 | $43.85 | VICR×31, SMTC×8, GLXY×59 | $10,800.25 | +858.41 | +0.00 | — | VICR, SMTC, GLXY | $10,793.87 | $10,793.87 | — |
| 2026-09-23 | +2.29 | $10,793.87 | — | $10,793.87 | -0.00 | -614.17 | PGEN, SGRY, VERI | — | $1.41 | $10,149.34 | PGEN×950, SGRY×103, VERI×1225 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 531 | $13.18 | $6.85 | — | $2,994.57 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $7000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 232 | $4.31 | $2.99 | — | $1,991.66 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 51 | $19.57 | $2.14 | — | $991.44 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $991.44 | ▲ close $10,395.38 vs 09:30 $10,000.00 (session +407.37) | 16:00 close · cash $991.44 · equity $10,395.38 vs 09:30 $10,000.00 (+395.38; session marks +407.37) · 3 name(s) marked open→close (per-name table). HLIT×531 09:30 $13.18 → close $13.92 +392.94; ANGX×232 09:30 $4.31 → close $4.37 +13.92; ARX×51 09:30 $19.57 → close $19.58 +0.51 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $991.44 | ▲ 09:30 equity $10,405.75 vs yday $10,395.38 (+10.37) | 09:30 open · cash $991.44 (unchanged overnight, no fees) · equity $10,405.75 vs prior close $10,395.38 (+10.37) · 3 name(s) re-marked at the open (per-name table). HLIT×531 yday $13.92 → 09:30 $13.84 -42.48; ANGX×232 yday $4.37 → 09:30 $4.60 +53.36; ARX×51 yday $19.58 → 09:30 $19.57 -0.51 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 531 | $13.84 | $7.00 | $+336.61 | $8,333.49 | ▲ +336.61 after sell → book $10,398.76; vs 09:30 mark -6.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 232 | $4.60 | $3.04 | $+61.25 | $9,397.65 | ▲ +61.25 after sell → book $10,395.72; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 51 | $19.57 | $2.16 | $-4.31 | $10,393.55 | ▼ -4.31 after sell → book $10,393.55; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 112 | $46.18 | $2.33 | — | $5,219.07 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+6.7; leftover $5196.78 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 24 | $142.77 | $2.06 | — | $1,790.53 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+5.8; leftover $3464.52 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 8 | $202.70 | $2.01 | — | $166.91 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+8.3; leftover $1732.26 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $166.91 | ▲ close $10,652.67 vs 09:30 $10,405.75 (session +265.52) | 16:00 close · cash $166.91 · equity $10,652.67 vs 09:30 $10,405.75 (+246.92; session marks +265.52) · 3 name(s) marked open→close (per-name table). DVN×112 09:30 $46.18 → close $47.57 +155.68; EOG×24 09:30 $142.77 → close $146.15 +81.12; FANG×8 09:30 $202.70 → close $206.29 +28.72 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $166.91 | ▲ 09:30 equity $10,767.31 vs yday $10,652.67 (+114.64) | 09:30 open · cash $166.91 (unchanged overnight, no fees) · equity $10,767.31 vs prior close $10,652.67 (+114.64) · 3 name(s) re-marked at the open (per-name table). DVN×112 yday $47.57 → 09:30 $48.00 +48.16; EOG×24 yday $146.15 → 09:30 $148.04 +45.36; FANG×8 yday $206.29 → 09:30 $208.93 +21.12 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 112 | $48.00 | $2.39 | $+199.13 | $5,540.52 | ▲ +199.13 after sell → book $10,764.92; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 24 | $148.04 | $2.10 | $+122.32 | $9,091.38 | ▲ +122.32 after sell → book $10,762.82; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 8 | $208.93 | $2.04 | $+45.79 | $10,760.79 | ▲ +45.79 after sell → book $10,760.79; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,760.79 | ▲ close $10,760.79 vs 09:30 $10,767.31 (session +0.00) | 16:00 close · cash $10,760.79 · no lots left · equity $10,760.79. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,760.79 | ▲ 09:30 equity $10,760.79 vs yday $10,760.79 (-0.00) | 09:30 open · cash $10,760.79 · no holdings · equity $10,760.79 vs prior close $10,760.79 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,760.79 | ▲ close $10,760.79 vs 09:30 $10,760.79 (session +0.00) | 16:00 close · cash $10,760.79 · no lots left · equity $10,760.79. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,760.79 | ▲ 09:30 equity $10,760.79 vs yday $10,760.79 (-0.00) | 09:30 open · cash $10,760.79 · no holdings · equity $10,760.79 vs prior close $10,760.79 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 82 | $91.01 | $2.24 | — | $3,295.73 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $7532.55 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 24 | $44.76 | $2.06 | — | $2,219.43 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1076.08 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 435 | $2.47 | $5.61 | — | $1,139.37 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1076.08 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 18 | $58.73 | $2.04 | — | $80.18 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1076.08 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $80.18 | ▲ close $10,939.46 vs 09:30 $10,760.79 (session +190.63) | 16:00 close · cash $80.18 · equity $10,939.46 vs 09:30 $10,760.79 (+178.67; session marks +190.63) · 4 name(s) marked open→close (per-name table). BHP×82 09:30 $91.01 → close $93.63 +214.84; APA×24 09:30 $44.76 → close $44.39 -8.88; AUTL×435 09:30 $2.47 → close $2.46 -4.35; CRSP×18 09:30 $58.73 → close $58.12 -10.98 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.18 | ▲ 09:30 equity $11,147.11 vs yday $10,939.46 (+207.65) | 09:30 open · cash $80.18 (unchanged overnight, no fees) · equity $11,147.11 vs prior close $10,939.46 (+207.65) · 4 name(s) re-marked at the open (per-name table). BHP×82 yday $93.63 → 09:30 $95.72 +171.38; APA×24 yday $44.39 → 09:30 $44.52 +3.12; AUTL×435 yday $2.46 → 09:30 $2.47 +4.35; CRSP×18 yday $58.12 → 09:30 $59.72 +28.80 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 82 | $95.72 | $2.31 | $+381.67 | $7,926.91 | ▲ +381.67 after sell → book $11,144.80; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 24 | $44.52 | $2.08 | $-9.90 | $8,993.31 | ▼ -9.90 after sell → book $11,142.72; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 52 | $119.43 | $2.15 | — | $2,780.80 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $6295.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 23 | $115.18 | $2.06 | — | $129.60 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $2697.99 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.60 | ▲ close $11,396.11 vs 09:30 $11,147.11 (session +257.60) | 16:00 close · cash $129.60 · equity $11,396.11 vs 09:30 $11,147.11 (+249.00; session marks +257.60) · 4 name(s) marked open→close (per-name table). AUTL×435 09:30 $2.47 → close $2.41 -26.10; CRSP×18 09:30 $59.72 → close $59.50 -3.96; AU×52 09:30 $119.43 → close $121.22 +93.08; FUTU×23 09:30 $115.18 → close $123.64 +194.58 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.60 | ▼ 09:30 equity $11,280.62 vs yday $11,396.11 (-115.49) | 09:30 open · cash $129.60 (unchanged overnight, no fees) · equity $11,280.62 vs prior close $11,396.11 (-115.49) · 4 name(s) re-marked at the open (per-name table). AUTL×435 yday $2.41 → 09:30 $2.40 -4.35; CRSP×18 yday $59.50 → 09:30 $58.75 -13.50; AU×52 yday $121.22 → 09:30 $120.51 -36.92; FUTU×23 yday $123.64 → 09:30 $121.00 -60.72 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 435 | $2.40 | $5.69 | $-41.76 | $1,167.91 | ▼ -41.76 after sell → book $11,274.93; vs 09:30 mark -5.69 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 52 | $120.51 | $2.21 | $+51.81 | $7,432.22 | ▲ +51.81 after sell → book $11,272.72; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 23 | $121.00 | $2.09 | $+129.71 | $10,213.13 | ▲ +129.71 after sell → book $11,270.63; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,213.13 | ▼ close $11,240.48 vs 09:30 $11,280.62 (session -30.15) | 16:00 close · cash $10,213.13 · equity $11,240.48 vs 09:30 $11,280.62 (-40.14; session marks -30.15) · 1 name(s) marked open→close (per-name table). CRSP×18 09:30 $58.75 → close $57.08 -30.15 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,213.13 | ▲ 09:30 equity $11,255.87 vs yday $11,240.48 (+15.39) | 09:30 open · cash $10,213.13 (unchanged overnight, no fees) · equity $11,255.87 vs prior close $11,240.48 (+15.39) · 1 name(s) re-marked at the open (per-name table). CRSP×18 yday $57.08 → 09:30 $57.93 +15.39 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 18 | $57.93 | $2.06 | $-18.51 | $11,253.81 | ▼ -18.51 after sell → book $11,253.81; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 66 | $118.52 | $2.19 | — | $3,429.30 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $7877.67 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 21 | $77.13 | $2.05 | — | $1,807.52 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1688.07 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 48 | $35.05 | $2.13 | — | $122.98 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1688.07 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.98 | ▲ close $11,635.87 vs 09:30 $11,255.87 (session +388.44) | 16:00 close · cash $122.98 · equity $11,635.87 vs 09:30 $11,255.87 (+380.00; session marks +388.44) · 3 name(s) marked open→close (per-name table). AU×66 09:30 $118.52 → close $123.39 +321.42; FCX×21 09:30 $77.13 → close $79.91 +58.38; EZPW×48 09:30 $35.05 → close $35.23 +8.64 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.98 | ▼ 09:30 equity $11,409.52 vs yday $11,635.87 (-226.35) | 09:30 open · cash $122.98 (unchanged overnight, no fees) · equity $11,409.52 vs prior close $11,635.87 (-226.35) · 3 name(s) re-marked at the open (per-name table). AU×66 yday $123.39 → 09:30 $119.80 -236.94; FCX×21 yday $79.91 → 09:30 $79.34 -11.97; EZPW×48 yday $35.23 → 09:30 $35.70 +22.56 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 66 | $119.80 | $2.26 | $+80.03 | $8,027.52 | ▲ +80.03 after sell → book $11,407.26; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 21 | $79.34 | $2.08 | $+42.28 | $9,691.58 | ▲ +42.28 after sell → book $11,405.18; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 48 | $35.70 | $2.16 | $+26.91 | $11,403.03 | ▲ +26.91 after sell → book $11,403.03; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 29 | $267.02 | $2.08 | — | $3,657.37 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $7982.12 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 28 | $118.50 | $2.07 | — | $337.30 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $3420.91 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $337.30 | ▲ close $11,400.63 vs 09:30 $11,409.52 (session +1.75) | 16:00 close · cash $337.30 · equity $11,400.63 vs 09:30 $11,409.52 (-8.89; session marks +1.75) · 2 name(s) marked open→close (per-name table). FNV×29 09:30 $267.02 → close $267.37 +10.15; CM×28 09:30 $118.50 → close $118.20 -8.40 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $337.30 | ▲ 09:30 equity $11,412.53 vs yday $11,400.63 (+11.90) | 09:30 open · cash $337.30 (unchanged overnight, no fees) · equity $11,412.53 vs prior close $11,400.63 (+11.90) · 2 name(s) re-marked at the open (per-name table). FNV×29 yday $267.37 → 09:30 $267.23 -4.06; CM×28 yday $118.20 → 09:30 $118.77 +15.96 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 29 | $267.23 | $2.15 | $+1.86 | $8,084.82 | ▲ +1.86 after sell → book $11,410.38; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 69 | $81.65 | $2.20 | — | $2,448.77 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $5659.37 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $1,479.77 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1212.72 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,479.77 | ▼ close $11,184.49 vs 09:30 $11,412.53 (session -221.70) | 16:00 close · cash $1,479.77 · equity $11,184.49 vs 09:30 $11,412.53 (-228.04; session marks -221.70) · 3 name(s) marked open→close (per-name table). CM×28 09:30 $118.77 → close $114.84 -110.04; ACMR×69 09:30 $81.65 → close $80.49 -80.04; MU×1 09:30 $967.01 → close $935.39 -31.62 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,479.77 | ▼ 09:30 equity $11,107.17 vs yday $11,184.49 (-77.32) | 09:30 open · cash $1,479.77 (unchanged overnight, no fees) · equity $11,107.17 vs prior close $11,184.49 (-77.32) · 3 name(s) re-marked at the open (per-name table). CM×28 yday $114.84 → 09:30 $115.66 +22.96; ACMR×69 yday $80.49 → 09:30 $79.27 -84.18; MU×1 yday $935.39 → 09:30 $919.29 -16.10 | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 28 | $115.66 | $2.11 | $-83.70 | $4,716.14 | ▼ -83.70 after sell → book $11,105.06; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 69 | $79.27 | $2.25 | $-168.67 | $10,183.51 | ▼ -168.67 after sell → book $11,102.80; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $11,100.79 | ▼ -51.73 after sell → book $11,100.79; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 23 | $324.41 | $2.06 | — | $3,637.30 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $7770.55 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 7 | $141.76 | $2.01 | — | $2,642.97 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1110.08 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 2 | $400.42 | $2.00 | — | $1,840.14 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1110.08 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,840.14 | ▼ close $10,874.52 vs 09:30 $11,107.17 (session -220.21) | 16:00 close · cash $1,840.14 · equity $10,874.52 vs 09:30 $11,107.17 (-232.65; session marks -220.21) · 3 name(s) marked open→close (per-name table). KEYS×23 09:30 $324.41 → close $319.97 -102.12; SMTC×7 09:30 $141.76 → close $131.17 -74.13; CIEN×2 09:30 $400.42 → close $378.44 -43.96 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,840.14 | ▲ 09:30 equity $10,940.39 vs yday $10,874.52 (+65.87) | 09:30 open · cash $1,840.14 (unchanged overnight, no fees) · equity $10,940.39 vs prior close $10,874.52 (+65.87) · 3 name(s) re-marked at the open (per-name table). KEYS×23 yday $319.97 → 09:30 $322.49 +57.96; SMTC×7 yday $131.17 → 09:30 $132.30 +7.91; CIEN×2 yday $378.44 → 09:30 $378.44 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 23 | $322.49 | $2.13 | $-48.35 | $9,255.28 | ▼ -48.35 after sell → book $10,938.26; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 7 | $132.30 | $2.03 | $-70.26 | $10,179.35 | ▼ -70.26 after sell → book $10,936.23; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 2 | $378.44 | $2.02 | $-47.97 | $10,934.21 | ▼ -47.97 after sell → book $10,934.21; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,934.21 | ▲ close $10,934.21 vs 09:30 $10,940.39 (session +0.00) | 16:00 close · cash $10,934.21 · no lots left · equity $10,934.21. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,934.21 | ▲ 09:30 equity $10,934.21 vs yday $10,934.21 (+0.00) | 09:30 open · cash $10,934.21 · no holdings · equity $10,934.21 vs prior close $10,934.21 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,934.21 | ▲ close $10,934.21 vs 09:30 $10,934.21 (session +0.00) | 16:00 close · cash $10,934.21 · no lots left · equity $10,934.21. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,934.21 | ▲ 09:30 equity $10,934.21 vs yday $10,934.21 (+0.00) | 09:30 open · cash $10,934.21 · no holdings · equity $10,934.21 vs prior close $10,934.21 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,934.21 | ▲ close $10,934.21 vs 09:30 $10,934.21 (session +0.00) | 16:00 close · cash $10,934.21 · no lots left · equity $10,934.21. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,934.21 | ▲ 09:30 equity $10,934.21 vs yday $10,934.21 (+0.00) | 09:30 open · cash $10,934.21 · no holdings · equity $10,934.21 vs prior close $10,934.21 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 21 | $351.74 | $2.05 | — | $3,545.62 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $7653.95 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $2,571.00 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1093.42 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 33 | $32.31 | $2.09 | — | $1,502.68 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1093.42 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 68 | $15.87 | $2.19 | — | $421.33 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1093.42 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $421.33 | ▲ close $11,214.45 vs 09:30 $10,934.21 (session +288.57) | 16:00 close · cash $421.33 · equity $11,214.45 vs 09:30 $10,934.21 (+280.24; session marks +288.57) · 4 name(s) marked open→close (per-name table). AVGO×21 09:30 $351.74 → close $357.16 +113.82; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×33 09:30 $32.31 → close $33.66 +44.55; FRNM×68 09:30 $15.87 → close $16.90 +70.04 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $421.33 | ▲ 09:30 equity $11,221.97 vs yday $11,214.45 (+7.52) | 09:30 open · cash $421.33 (unchanged overnight, no fees) · equity $11,221.97 vs prior close $11,214.45 (+7.52) · 4 name(s) re-marked at the open (per-name table). AVGO×21 yday $357.16 → 09:30 $359.70 +53.34; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×33 yday $33.66 → 09:30 $33.46 -6.60; FRNM×68 yday $16.90 → 09:30 $16.40 -34.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 21 | $359.70 | $2.12 | $+162.98 | $7,972.90 | ▲ +162.98 after sell → book $11,219.84; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $8,998.45 | ▲ +50.93 after sell → book $11,217.83; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 33 | $33.46 | $2.11 | $+33.75 | $10,100.52 | ▲ +33.75 after sell → book $11,215.72; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 26 | $263.36 | $2.07 | — | $3,251.09 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $7070.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 20 | $75.65 | $2.05 | — | $1,736.04 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1515.08 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 6 | $236.82 | $2.01 | — | $313.11 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+8.1; leftover $1515.08 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $313.11 | ▲ close $11,244.79 vs 09:30 $11,221.97 (session +35.20) | 16:00 close · cash $313.11 · equity $11,244.79 vs 09:30 $11,221.97 (+22.82; session marks +35.20) · 4 name(s) marked open→close (per-name table). FRNM×68 09:30 $16.40 → close $16.31 -6.12; CRM×26 09:30 $263.36 → close $259.23 -107.38; MRX×20 09:30 $75.65 → close $78.27 +52.40; BE×6 09:30 $236.82 → close $252.87 +96.30 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $313.11 | ▼ 09:30 equity $11,231.51 vs yday $11,244.79 (-13.28) | 09:30 open · cash $313.11 (unchanged overnight, no fees) · equity $11,231.51 vs prior close $11,244.79 (-13.28) · 4 name(s) re-marked at the open (per-name table). FRNM×68 yday $16.31 → 09:30 $16.74 +29.24; CRM×26 yday $259.23 → 09:30 $253.72 -143.26; MRX×20 yday $78.27 → 09:30 $78.84 +11.40; BE×6 yday $252.87 → 09:30 $267.76 +89.34 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 68 | $16.74 | $2.22 | $+54.75 | $1,449.22 | ▲ +54.75 after sell → book $11,229.30; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 26 | $253.72 | $2.13 | $-254.84 | $8,043.81 | ▼ -254.84 after sell → book $11,227.17; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 20 | $78.84 | $2.07 | $+59.68 | $9,618.54 | ▲ +59.68 after sell → book $11,225.10; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 6 | $267.76 | $2.03 | $+181.60 | $11,223.06 | ▲ +181.60 after sell → book $11,223.06; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,223.06 | ▲ close $11,223.06 vs 09:30 $11,231.51 (session +0.00) | 16:00 close · cash $11,223.06 · no lots left · equity $11,223.06. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,223.06 | ▲ 09:30 equity $11,223.06 vs yday $11,223.06 (+0.00) | 09:30 open · cash $11,223.06 · no holdings · equity $11,223.06 vs prior close $11,223.06 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,223.06 | ▲ close $11,223.06 vs 09:30 $11,223.06 (session +0.00) | 16:00 close · cash $11,223.06 · no lots left · equity $11,223.06. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,223.06 | ▲ 09:30 equity $11,223.06 vs yday $11,223.06 (+0.00) | 09:30 open · cash $11,223.06 · no holdings · equity $11,223.06 vs prior close $11,223.06 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,223.06 | ▲ close $11,223.06 vs 09:30 $11,223.06 (session +0.00) | 16:00 close · cash $11,223.06 · no lots left · equity $11,223.06. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,223.06 | ▲ 09:30 equity $11,223.06 vs yday $11,223.06 (+0.00) | 09:30 open · cash $11,223.06 · no holdings · equity $11,223.06 vs prior close $11,223.06 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 68 | $164.43 | $2.19 | — | $39.63 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $11223.06 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.63 | ▼ close $10,258.67 vs 09:30 $11,223.06 (session -962.20) | 16:00 close · cash $39.63 · equity $10,258.67 vs 09:30 $11,223.06 (-964.39; session marks -962.20) · 1 name(s) marked open→close (per-name table). ORCL×68 09:30 $164.43 → close $150.28 -962.20 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.63 | ▼ 09:30 equity $9,656.19 vs yday $10,258.67 (-602.48) | 09:30 open · cash $39.63 (unchanged overnight, no fees) · equity $9,656.19 vs prior close $10,258.67 (-602.48) · 1 name(s) re-marked at the open (per-name table). ORCL×68 yday $150.28 → 09:30 $141.42 -602.48 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 68 | $141.42 | $2.28 | $-1569.16 | $9,653.91 | ▼ -1,569.16 after sell → book $9,653.91; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,653.91 | ▲ close $9,653.91 vs 09:30 $9,656.19 (session +0.00) | 16:00 close · cash $9,653.91 · no lots left · equity $9,653.91. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,653.91 | ▲ 09:30 equity $9,653.91 vs yday $9,653.91 (-0.00) | 09:30 open · cash $9,653.91 · no holdings · equity $9,653.91 vs prior close $9,653.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,653.91 | ▲ close $9,653.91 vs 09:30 $9,653.91 (session +0.00) | 16:00 close · cash $9,653.91 · no lots left · equity $9,653.91. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,653.91 | ▲ 09:30 equity $9,653.91 vs yday $9,653.91 (-0.00) | 09:30 open · cash $9,653.91 · no holdings · equity $9,653.91 vs prior close $9,653.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 257 | $26.27 | $3.32 | — | $2,899.20 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $6757.74 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 7 | $189.17 | $2.01 | — | $1,573.00 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $1448.09 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 36 | $39.99 | $2.10 | — | $131.26 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $1448.09 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.26 | ▼ close $9,632.53 vs 09:30 $9,653.91 (session -13.95) | 16:00 close · cash $131.26 · equity $9,632.53 vs 09:30 $9,653.91 (-21.38; session marks -13.95) · 3 name(s) marked open→close (per-name table). WAY×257 09:30 $26.27 → close $26.59 +82.24; QCOM×7 09:30 $189.17 → close $184.84 -30.31; SM×36 09:30 $39.99 → close $38.16 -65.88 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.26 | ▼ 09:30 equity $9,629.30 vs yday $9,632.53 (-3.23) | 09:30 open · cash $131.26 (unchanged overnight, no fees) · equity $9,629.30 vs prior close $9,632.53 (-3.23) · 3 name(s) re-marked at the open (per-name table). WAY×257 yday $26.59 → 09:30 $26.51 -20.56; QCOM×7 yday $184.84 → 09:30 $190.35 +38.57; SM×36 yday $38.16 → 09:30 $37.57 -21.24 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 257 | $26.51 | $3.41 | $+54.95 | $6,940.92 | ▲ +54.95 after sell → book $9,625.89; vs 09:30 mark -3.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 7 | $190.35 | $2.03 | $+4.22 | $8,271.34 | ▲ +4.22 after sell → book $9,623.86; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 36 | $37.57 | $2.12 | $-91.34 | $9,621.74 | ▼ -91.34 after sell → book $9,621.74; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 39 | $170.85 | $2.11 | — | $2,956.48 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $6735.22 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `CLS` | 2 | $337.75 | $2.00 | — | $2,278.99 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+10.2; leftover $962.17 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 43 | $22.12 | $2.12 | — | $1,325.71 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $962.17 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 4 | $238.60 | $2.00 | — | $369.31 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_mover; ret5=-11.6; leftover $962.17 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $369.31 | ▲ close $9,904.91 vs 09:30 $9,629.30 (session +291.39) | 16:00 close · cash $369.31 · equity $9,904.91 vs 09:30 $9,629.30 (+275.61; session marks +291.39) · 4 name(s) marked open→close (per-name table). SMTC×39 09:30 $170.85 → close $178.19 +286.26; CLS×2 09:30 $337.75 → close $329.94 -15.62; GME×43 09:30 $22.12 → close $22.77 +27.95; JBHT×4 09:30 $238.60 → close $236.80 -7.20 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $369.31 | ▲ 09:30 equity $10,076.20 vs yday $9,904.91 (+171.29) | 09:30 open · cash $369.31 (unchanged overnight, no fees) · equity $10,076.20 vs prior close $9,904.91 (+171.29) · 4 name(s) re-marked at the open (per-name table). SMTC×39 yday $178.19 → 09:30 $182.33 +161.46; CLS×2 yday $329.94 → 09:30 $332.06 +4.24; GME×43 yday $22.77 → 09:30 $22.90 +5.59; JBHT×4 yday $236.80 → 09:30 $236.80 +0.00 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 39 | $182.33 | $2.17 | $+443.44 | $7,478.00 | ▲ +443.44 after sell → book $10,074.02; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 4 | $236.80 | $2.02 | $-11.22 | $8,423.18 | ▼ -11.22 after sell → book $10,072.00; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 281 | $20.91 | $3.62 | — | $2,543.85 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $5896.23 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 170 | $14.79 | $2.50 | — | $27.05 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $2526.95 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.05 | ▲ close $10,086.92 vs 09:30 $10,076.20 (session +21.04) | 16:00 close · cash $27.05 · equity $10,086.92 vs 09:30 $10,076.20 (+10.72; session marks +21.04) · 4 name(s) marked open→close (per-name table). CLS×2 09:30 $332.06 → close $332.63 +1.14; GME×43 09:30 $22.90 → close $22.64 -11.18; TH×281 09:30 $20.91 → close $21.19 +78.68; RARE×170 09:30 $14.79 → close $14.51 -47.60 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.05 | ▲ 09:30 equity $10,251.74 vs yday $10,086.92 (+164.82) | 09:30 open · cash $27.05 (unchanged overnight, no fees) · equity $10,251.74 vs prior close $10,086.92 (+164.82) · 4 name(s) re-marked at the open (per-name table). CLS×2 yday $332.63 → 09:30 $341.45 +17.64; GME×43 yday $22.64 → 09:30 $22.78 +6.02; TH×281 yday $21.19 → 09:30 $21.65 +129.26; RARE×170 yday $14.51 → 09:30 $14.58 +11.90 | — |
| 2026-09-21 09:30 ET | **SELL** | `CLS` | 2 | $341.45 | $2.02 | $+3.39 | $707.93 | ▲ +3.39 after sell → book $10,249.72; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 43 | $22.78 | $2.14 | $+24.12 | $1,685.33 | ▲ +24.12 after sell → book $10,247.58; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 281 | $21.65 | $3.72 | $+200.59 | $7,765.26 | ▲ +200.59 after sell → book $10,243.86; vs 09:30 mark -3.72 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 170 | $14.58 | $2.55 | $-40.75 | $10,241.31 | ▼ -40.75 after sell → book $10,241.31; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 31 | $230.25 | $2.08 | — | $3,101.48 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+12.5; leftover $7168.92 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 8 | $190.30 | $2.01 | — | $1,577.07 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+10.6; leftover $1536.20 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 59 | $25.95 | $2.17 | — | $43.85 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1536.20 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.85 | ▼ close $9,941.84 vs 09:30 $10,251.74 (session -293.21) | 16:00 close · cash $43.85 · equity $9,941.84 vs 09:30 $10,251.74 (-309.90; session marks -293.21) · 3 name(s) marked open→close (per-name table). VICR×31 09:30 $230.25 → close $223.90 -196.85; SMTC×8 09:30 $190.30 → close $177.37 -103.44; GLXY×59 09:30 $25.95 → close $26.07 +7.08 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.85 | ▲ 09:30 equity $10,800.25 vs yday $9,941.84 (+858.41) | 09:30 open · cash $43.85 (unchanged overnight, no fees) · equity $10,800.25 vs prior close $9,941.84 (+858.41) · 3 name(s) re-marked at the open (per-name table). VICR×31 yday $223.90 → 09:30 $252.37 +882.57; SMTC×8 yday $177.37 → 09:30 $177.30 -0.56; GLXY×59 yday $26.07 → 09:30 $25.67 -23.60 | — |
| 2026-09-22 09:30 ET | **SELL** | `VICR` | 31 | $252.37 | $2.16 | $+681.48 | $7,865.16 | ▲ +681.48 after sell → book $10,798.09; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SMTC` | 8 | $177.30 | $2.04 | $-108.05 | $9,281.53 | ▼ -108.05 after sell → book $10,796.06; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `GLXY` | 59 | $25.67 | $2.19 | $-20.88 | $10,793.87 | ▼ -20.88 after sell → book $10,793.87; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,793.87 | ▲ close $10,793.87 vs 09:30 $10,800.25 (session +0.00) | 16:00 close · cash $10,793.87 · no lots left · equity $10,793.87. | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,793.87 | ▲ 09:30 equity $10,793.87 vs yday $10,793.87 (-0.00) | 09:30 open · cash $10,793.87 · no holdings · equity $10,793.87 vs prior close $10,793.87 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 950 | $7.95 | $12.26 | — | $3,229.11 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $7555.71 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 103 | $15.70 | $2.30 | — | $1,609.71 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.5; leftover $1619.08 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 1225 | $1.30 | $15.80 | — | $1.41 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+15.3; leftover $1619.08 | join🟢 sector🟡 gen🟢 news🟢 digest🟡 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.41 | ▼ close $10,149.34 vs 09:30 $10,793.87 (session -614.17) | 16:00 close · cash $1.41 · equity $10,149.34 vs 09:30 $10,793.87 (-644.53; session marks -614.17) · 3 name(s) marked open→close (per-name table). PGEN×950 09:30 $7.95 → close $7.44 -484.50; SGRY×103 09:30 $15.70 → close $14.56 -117.42; VERI×1225 09:30 $1.30 → close $1.29 -12.25 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1000.00 < 1 share @ 1646.93 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1212.72 < 1 share @ 1746.53 |
| 2026-08-28 | `MPWR` | cash | leftover split 1110.08 < 1 share @ 1306.03 |
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

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `PGEN` | 950 | 2026-09-23 @ $7.95 | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $7555.71 |
| `SGRY` | 103 | 2026-09-23 @ $15.70 | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.5; leftover $1619.08 |
| `VERI` | 1225 | 2026-09-23 @ $1.30 | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+15.3; leftover $1619.08 |
