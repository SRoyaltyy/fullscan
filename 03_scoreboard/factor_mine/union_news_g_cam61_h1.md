# Factor mine action — `union_news_g_cam61_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · merged news🟢 and cameras +6 −≤1

Cash book **-20.11%** ($7,989) · signal-only (no cash/fees) was -19.77%. Starts YES **0/29**. Fills 78 · skips 13 · realized $-1414.64.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=good,n_pos_min=6,cam_bad_max=1` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $3.73.

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
| 2026-08-21 | `BHP` | 27 | $93.63 | $95.72 | +56.43 | — | +0.00 | +56.43 | +127.17 | — |
| 2026-08-21 | `APA` | 55 | $44.39 | $44.52 | +7.15 | — | +0.00 | +7.15 | -13.20 | — |
| 2026-08-21 | `AUTL` | 1012 | $2.46 | $2.47 | +10.12 | $2.41 | -60.72 | -50.60 | +0.00 | -60.72 |
| 2026-08-21 | `CRSP` | 42 | $58.12 | $59.72 | +67.20 | $59.50 | -9.24 | +57.96 | +41.58 | +32.34 |
| 2026-08-21 | `AU` | 7 | — | $119.43 | +0.00 | $121.22 | +12.53 | +12.53 | +0.00 | +12.53 |
| 2026-08-21 | `FUTU` | 7 | — | $115.18 | +0.00 | $123.64 | +59.22 | +59.22 | +0.00 | +59.22 |
| 2026-08-21 | `GRAL` | 10 | — | $78.88 | +0.00 | $79.54 | +6.60 | +6.60 | +0.00 | +6.60 |
| 2026-08-21 | `ABTC` | 98 | — | $8.66 | +0.00 | $7.93 | -71.54 | -71.54 | +0.00 | -71.54 |
| 2026-08-21 | `HIVE` | 263 | — | $3.24 | +0.00 | $3.03 | -55.23 | -55.23 | +0.00 | -55.23 |
| 2026-08-21 | `MARA` | 72 | — | $11.70 | +0.00 | $11.26 | -31.68 | -31.68 | +0.00 | -31.68 |
| 2026-08-24 | `AUTL` | 1012 | $2.41 | $2.40 | -10.12 | — | +0.00 | -10.12 | -70.84 | — |
| 2026-08-24 | `CRSP` | 42 | $59.50 | $58.75 | -31.50 | $57.08 | -70.35 | -101.85 | +0.84 | -69.51 |
| 2026-08-24 | `AU` | 7 | $121.22 | $120.51 | -4.97 | — | +0.00 | -4.97 | +7.56 | — |
| 2026-08-24 | `FUTU` | 7 | $123.64 | $121.00 | -18.48 | — | +0.00 | -18.48 | +40.74 | — |
| 2026-08-24 | `GRAL` | 10 | $79.54 | $81.87 | +23.30 | — | +0.00 | +23.30 | +29.90 | — |
| 2026-08-24 | `ABTC` | 98 | $7.93 | $8.00 | +6.86 | — | +0.00 | +6.86 | -64.68 | — |
| 2026-08-24 | `HIVE` | 263 | $3.03 | $2.99 | -10.52 | — | +0.00 | -10.52 | -65.75 | — |
| 2026-08-24 | `MARA` | 72 | $11.26 | $11.17 | -6.48 | — | +0.00 | -6.48 | -38.16 | — |
| 2026-08-25 | `CRSP` | 42 | $57.08 | $57.93 | +35.91 | — | +0.00 | +35.91 | -33.60 | — |
| 2026-08-25 | `AU` | 41 | — | $118.52 | +0.00 | $123.39 | +199.67 | +199.67 | +0.00 | +199.67 |
| 2026-08-25 | `FCX` | 63 | — | $77.13 | +0.00 | $79.91 | +175.14 | +175.14 | +0.00 | +175.14 |
| 2026-08-26 | `AU` | 41 | $123.39 | $119.80 | -147.19 | — | +0.00 | -147.19 | +52.48 | — |
| 2026-08-26 | `FCX` | 63 | $79.91 | $79.34 | -35.91 | — | +0.00 | -35.91 | +139.23 | — |
| 2026-08-26 | `CM` | 84 | — | $118.50 | +0.00 | $118.20 | -25.20 | -25.20 | +0.00 | -25.20 |
| 2026-08-27 | `CM` | 84 | $118.20 | $118.77 | +47.88 | $114.84 | -330.12 | -282.24 | +22.68 | -307.44 |
| 2026-08-28 | `CM` | 84 | $114.84 | $115.66 | +68.88 | — | +0.00 | +68.88 | -238.56 | — |
| 2026-08-28 | `KEYS` | 5 | — | $324.41 | +0.00 | $319.97 | -22.20 | -22.20 | +0.00 | -22.20 |
| 2026-08-28 | `SMTC` | 11 | — | $141.76 | +0.00 | $131.17 | -116.49 | -116.49 | +0.00 | -116.49 |
| 2026-08-28 | `CIEN` | 4 | — | $400.42 | +0.00 | $378.44 | -87.92 | -87.92 | +0.00 | -87.92 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 6 | — | $240.22 | +0.00 | $236.98 | -19.44 | -19.44 | +0.00 | -19.44 |
| 2026-08-28 | `SEDG` | 49 | — | $32.90 | +0.00 | $31.41 | -73.01 | -73.01 | +0.00 | -73.01 |
| 2026-08-31 | `KEYS` | 5 | $319.97 | $322.49 | +12.60 | — | +0.00 | +12.60 | -9.60 | — |
| 2026-08-31 | `SMTC` | 11 | $131.17 | $132.30 | +12.43 | — | +0.00 | +12.43 | -104.06 | — |
| 2026-08-31 | `CIEN` | 4 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -87.92 | — |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | — | +0.00 | +5.64 | -44.13 | — |
| 2026-08-31 | `DDOG` | 6 | $236.98 | $233.97 | -18.09 | — | +0.00 | -18.09 | -37.53 | — |
| 2026-08-31 | `SEDG` | 49 | $31.41 | $31.15 | -12.74 | — | +0.00 | -12.74 | -85.75 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 5 | — | $351.74 | +0.00 | $357.16 | +27.10 | +27.10 | +0.00 | +27.10 |
| 2026-09-03 | `DELL` | 3 | — | $486.31 | +0.00 | $516.39 | +90.24 | +90.24 | +0.00 | +90.24 |
| 2026-09-03 | `CXW` | 58 | — | $32.31 | +0.00 | $33.66 | +78.30 | +78.30 | +0.00 | +78.30 |
| 2026-09-03 | `FRNM` | 118 | — | $15.87 | +0.00 | $16.90 | +121.54 | +121.54 | +0.00 | +121.54 |
| 2026-09-03 | `MMED` | 78 | — | $23.88 | +0.00 | $23.84 | -3.12 | -3.12 | +0.00 | -3.12 |
| 2026-09-04 | `AVGO` | 5 | $357.16 | $359.70 | +12.70 | — | +0.00 | +12.70 | +39.80 | — |
| 2026-09-04 | `DELL` | 3 | $516.39 | $513.78 | -7.83 | — | +0.00 | -7.83 | +82.41 | — |
| 2026-09-04 | `CXW` | 58 | $33.66 | $33.46 | -11.60 | — | +0.00 | -11.60 | +66.70 | — |
| 2026-09-04 | `FRNM` | 118 | $16.90 | $16.40 | -59.00 | $16.31 | -10.62 | -69.62 | +62.54 | +51.92 |
| 2026-09-04 | `MMED` | 78 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -3.12 | — |
| 2026-09-04 | `CRM` | 14 | — | $263.36 | +0.00 | $259.23 | -57.82 | -57.82 | +0.00 | -57.82 |
| 2026-09-04 | `MRX` | 50 | — | $75.65 | +0.00 | $78.27 | +131.00 | +131.00 | +0.00 | +131.00 |
| 2026-09-08 | `FRNM` | 118 | $16.31 | $16.74 | +50.74 | — | +0.00 | +50.74 | +102.66 | — |
| 2026-09-08 | `CRM` | 14 | $259.23 | $253.72 | -77.14 | — | +0.00 | -77.14 | -134.96 | — |
| 2026-09-08 | `MRX` | 50 | $78.27 | $78.84 | +28.50 | — | +0.00 | +28.50 | +159.50 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 58 | — | $164.43 | +0.00 | $150.28 | -820.70 | -820.70 | +0.00 | -820.70 |
| 2026-09-14 | `ORCL` | 58 | $150.28 | $141.42 | -513.88 | — | +0.00 | -513.88 | -1334.58 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 105 | — | $26.27 | +0.00 | $26.59 | +33.60 | +33.60 | +0.00 | +33.60 |
| 2026-09-16 | `QCOM` | 14 | — | $189.17 | +0.00 | $184.84 | -60.62 | -60.62 | +0.00 | -60.62 |
| 2026-09-16 | `SM` | 69 | — | $39.99 | +0.00 | $38.16 | -126.27 | -126.27 | +0.00 | -126.27 |
| 2026-09-17 | `WAY` | 105 | $26.59 | $26.51 | -8.40 | — | +0.00 | -8.40 | +25.20 | — |
| 2026-09-17 | `QCOM` | 14 | $184.84 | $190.35 | +77.14 | — | +0.00 | +77.14 | +16.52 | — |
| 2026-09-17 | `SM` | 69 | $38.16 | $37.57 | -40.71 | — | +0.00 | -40.71 | -166.98 | — |
| 2026-09-17 | `SMTC` | 16 | — | $170.85 | +0.00 | $178.19 | +117.44 | +117.44 | +0.00 | +117.44 |
| 2026-09-17 | `CLS` | 8 | — | $337.75 | +0.00 | $329.94 | -62.48 | -62.48 | +0.00 | -62.48 |
| 2026-09-17 | `GME` | 123 | — | $22.12 | +0.00 | $22.77 | +79.95 | +79.95 | +0.00 | +79.95 |
| 2026-09-18 | `SMTC` | 16 | $178.19 | $182.33 | +66.24 | — | +0.00 | +66.24 | +183.68 | — |
| 2026-09-18 | `CLS` | 8 | $329.94 | $332.06 | +16.96 | $332.63 | +4.56 | +21.52 | -45.52 | -40.96 |
| 2026-09-18 | `GME` | 123 | $22.77 | $22.90 | +15.99 | $22.64 | -31.98 | -15.99 | +95.94 | +63.96 |
| 2026-09-18 | `TH` | 47 | — | $20.91 | +0.00 | $21.19 | +13.16 | +13.16 | +0.00 | +13.16 |
| 2026-09-18 | `RARE` | 66 | — | $14.79 | +0.00 | $14.51 | -18.48 | -18.48 | +0.00 | -18.48 |
| 2026-09-18 | `BHVN` | 70 | — | $14.07 | +0.00 | $13.62 | -31.50 | -31.50 | +0.00 | -31.50 |
| 2026-09-21 | `CLS` | 8 | $332.63 | $341.45 | +70.56 | — | +0.00 | +70.56 | +29.60 | — |
| 2026-09-21 | `GME` | 123 | $22.64 | $22.78 | +17.22 | — | +0.00 | +17.22 | +81.18 | — |
| 2026-09-21 | `TH` | 47 | $21.19 | $21.65 | +21.62 | — | +0.00 | +21.62 | +34.78 | — |
| 2026-09-21 | `RARE` | 66 | $14.51 | $14.58 | +4.62 | — | +0.00 | +4.62 | -13.86 | — |
| 2026-09-21 | `BHVN` | 70 | $13.62 | $13.90 | +19.60 | — | +0.00 | +19.60 | -11.90 | — |
| 2026-09-21 | `VICR` | 18 | — | $230.25 | +0.00 | $223.90 | -114.30 | -114.30 | +0.00 | -114.30 |
| 2026-09-21 | `SMTC` | 22 | — | $190.30 | +0.00 | $177.37 | -284.46 | -284.46 | +0.00 | -284.46 |
| 2026-09-22 | `VICR` | 18 | $223.90 | $252.37 | +512.46 | — | +0.00 | +512.46 | +398.16 | — |
| 2026-09-22 | `SMTC` | 22 | $177.37 | $177.30 | -1.54 | — | +0.00 | -1.54 | -286.00 | — |
| 2026-09-23 | `PGEN` | 539 | — | $7.95 | +0.00 | $7.44 | -274.89 | -274.89 | +0.00 | -274.89 |
| 2026-09-23 | `SGRY` | 273 | — | $15.70 | +0.00 | $14.56 | -311.22 | -311.22 | +0.00 | -311.22 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +14.65 | BHP, APA, AUTL, CRSP | — | $95.23 | $9,995.25 | BHP×27, APA×55, AUTL×1012, CRSP×42 |
| 2026-08-21 | +3.25 | $95.23 | BHP×27, APA×55, AUTL×1012, CRSP×42 | $10,136.15 | +140.90 | -150.06 | AU, FUTU, GRAL, ABTC, HIVE, MARA | BHP, APA | $135.79 | $9,967.88 | AUTL×1012, CRSP×42, AU×7, FUTU×7, GRAL×10, ABTC×98, HIVE×263, MARA×72 |
| 2026-08-24 | -5.17 | $135.79 | AUTL×1012, CRSP×42, AU×7, FUTU×7, GRAL×10, ABTC×98, HIVE×263, MARA×72 | $9,915.97 | -51.91 | -70.35 | — | AUTL, AU, FUTU, GRAL, ABTC, HIVE, MARA | $7,421.14 | $9,818.29 | CRSP×42 |
| 2026-08-25 | +1.80 | $7,421.14 | CRSP×42 | $9,854.20 | +35.91 | +374.81 | AU, FCX | CRSP | $129.26 | $10,222.58 | AU×41, FCX×63 |
| 2026-08-26 | +2.02 | $129.26 | AU×41, FCX×63 | $10,039.48 | -183.10 | -25.20 | CM | AU, FCX | $78.84 | $10,007.64 | CM×84 |
| 2026-08-27 | — | $78.84 | CM×84 | $10,055.52 | +47.88 | -330.12 | — | — | $78.84 | $9,725.40 | CM×84 |
| 2026-08-28 | +0.75 | $78.84 | CM×84 | $9,794.28 | +68.88 | -368.83 | KEYS, SMTC, CIEN, MPWR, DDOG, SEDG | CM | $637.24 | $9,410.95 | KEYS×5, SMTC×11, CIEN×4, MPWR×1, DDOG×6, SEDG×49 |
| 2026-08-31 | -5.85 | $637.24 | KEYS×5, SMTC×11, CIEN×4, MPWR×1, DDOG×6, SEDG×49 | $9,410.79 | -0.16 | +0.00 | — | KEYS, SMTC, CIEN, MPWR, DDOG, SEDG | $9,398.49 | $9,398.49 | — |
| 2026-09-01 | -6.30 | $9,398.49 | — | $9,398.49 | +0.00 | +0.00 | — | — | $9,398.49 | $9,398.49 | — |
| 2026-09-02 | -3.83 | $9,398.49 | — | $9,398.49 | +0.00 | +0.00 | — | — | $9,398.49 | $9,398.49 | — |
| 2026-09-03 | -0.90 | $9,398.49 | — | $9,398.49 | +0.00 | +314.06 | AVGO, DELL, CXW, FRNM, MMED | — | $560.85 | $9,701.82 | AVGO×5, DELL×3, CXW×58, FRNM×118, MMED×78 |
| 2026-09-04 | +2.25 | $560.85 | AVGO×5, DELL×3, CXW×58, FRNM×118, MMED×78 | $9,636.09 | -65.73 | +62.56 | CRM, MRX | AVGO, DELL, CXW, MMED | $218.68 | $9,685.98 | FRNM×118, CRM×14, MRX×50 |
| 2026-09-08 | -11.47 | $218.68 | FRNM×118, CRM×14, MRX×50 | $9,688.08 | +2.10 | +0.00 | — | FRNM, CRM, MRX | $9,681.45 | $9,681.45 | — |
| 2026-09-09 | -13.95 | $9,681.45 | — | $9,681.45 | +0.00 | +0.00 | — | — | $9,681.45 | $9,681.45 | — |
| 2026-09-10 | -13.28 | $9,681.45 | — | $9,681.45 | +0.00 | +0.00 | — | — | $9,681.45 | $9,681.45 | — |
| 2026-09-11 | +0.50 | $9,681.45 | — | $9,681.45 | +0.00 | -820.70 | ORCL | — | $142.35 | $8,858.59 | ORCL×58 |
| 2026-09-14 | -11.00 | $142.35 | ORCL×58 | $8,344.71 | -513.88 | +0.00 | — | ORCL | $8,342.47 | $8,342.47 | — |
| 2026-09-15 | -3.84 | $8,342.47 | — | $8,342.47 | -0.00 | +0.00 | — | — | $8,342.47 | $8,342.47 | — |
| 2026-09-16 | +5.30 | $8,342.47 | — | $8,342.47 | -0.00 | -153.29 | WAY, QCOM, SM | — | $169.89 | $8,182.64 | WAY×105, QCOM×14, SM×69 |
| 2026-09-17 | +7.38 | $169.89 | WAY×105, QCOM×14, SM×69 | $8,210.67 | +28.03 | +134.91 | SMTC, CLS, GME | WAY, QCOM, SM | $41.27 | $8,332.54 | SMTC×16, CLS×8, GME×123 |
| 2026-09-18 | +4.86 | $41.27 | SMTC×16, CLS×8, GME×123 | $8,431.73 | +99.19 | -64.24 | TH, RARE, BHVN | SMTC | $6.14 | $8,358.89 | CLS×8, GME×123, TH×47, RARE×66, BHVN×70 |
| 2026-09-21 | +12.87 | $6.14 | CLS×8, GME×123, TH×47, RARE×66, BHVN×70 | $8,492.51 | +133.62 | -398.76 | VICR, SMTC | CLS, GME, TH, RARE, BHVN | $146.29 | $8,078.63 | VICR×18, SMTC×22 |
| 2026-09-22 | -0.50 | $146.29 | VICR×18, SMTC×22 | $8,589.55 | +510.92 | +0.00 | — | VICR, SMTC | $8,585.36 | $8,585.36 | — |
| 2026-09-23 | +2.29 | $8,585.36 | — | $8,585.36 | -0.00 | -586.11 | PGEN, SGRY | — | $3.73 | $7,988.77 | PGEN×539, SGRY×273 |

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
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 27 | $95.72 | $2.10 | $+123.00 | $2,677.57 | ▲ +123.00 after sell → book $10,134.05; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 55 | $44.52 | $2.18 | $-17.54 | $5,123.99 | ▼ -17.54 after sell → book $10,131.87; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 7 | $119.43 | $2.01 | — | $4,285.97 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $854.00 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 7 | $115.18 | $2.01 | — | $3,477.69 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $854.00 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 10 | $78.88 | $2.02 | — | $2,686.87 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $854.00 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 98 | $8.66 | $2.28 | — | $1,835.91 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $854.00 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 263 | $3.24 | $3.39 | — | $980.40 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $854.00 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 72 | $11.70 | $2.21 | — | $135.79 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $854.00 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $135.79 | ▼ close $9,967.88 vs 09:30 $10,136.15 (session -150.06) | 16:00 close · cash $135.79 · equity $9,967.88 vs 09:30 $10,136.15 (-168.27; session marks -150.06) · 8 name(s) marked open→close (per-name table). AUTL×1012 09:30 $2.47 → close $2.41 -60.72; CRSP×42 09:30 $59.72 → close $59.50 -9.24; AU×7 09:30 $119.43 → close $121.22 +12.53; FUTU×7 09:30 $115.18 → close $123.64 +59.22; GRAL×10 09:30 $78.88 → close $79.54 +6.60; ABTC×98 09:30 $8.66 → close $7.93 -71.54; HIVE×263 09:30 $3.24 → close $3.03 -55.23; MARA×72 09:30 $11.70 → close $11.26 -31.68 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $135.79 | ▼ 09:30 equity $9,915.97 vs yday $9,967.88 (-51.91) | 09:30 open · cash $135.79 (unchanged overnight, no fees) · equity $9,915.97 vs prior close $9,967.88 (-51.91) · 8 name(s) re-marked at the open (per-name table). AUTL×1012 yday $2.41 → 09:30 $2.40 -10.12; CRSP×42 yday $59.50 → 09:30 $58.75 -31.50; AU×7 yday $121.22 → 09:30 $120.51 -4.97; FUTU×7 yday $123.64 → 09:30 $121.00 -18.48; GRAL×10 yday $79.54 → 09:30 $81.87 +23.30; ABTC×98 yday $7.93 → 09:30 $8.00 +6.86; HIVE×263 yday $3.03 → 09:30 $2.99 -10.52; MARA×72 yday $11.26 → 09:30 $11.17 -6.48 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 1012 | $2.40 | $13.24 | $-97.14 | $2,551.35 | ▼ -97.14 after sell → book $9,902.73; vs 09:30 mark -13.24 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 7 | $120.51 | $2.03 | $+3.52 | $3,392.89 | ▲ +3.52 after sell → book $9,900.70; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 7 | $121.00 | $2.03 | $+36.70 | $4,237.86 | ▲ +36.70 after sell → book $9,898.67; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 10 | $81.87 | $2.04 | $+25.84 | $5,054.52 | ▲ +25.84 after sell → book $9,896.63; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 98 | $8.00 | $2.31 | $-69.27 | $5,836.21 | ▼ -69.27 after sell → book $9,894.32; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 263 | $2.99 | $3.45 | $-72.59 | $6,619.13 | ▼ -72.59 after sell → book $9,890.87; vs 09:30 mark -3.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 72 | $11.17 | $2.23 | $-42.59 | $7,421.14 | ▼ -42.59 after sell → book $9,888.64; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,421.14 | ▼ close $9,818.29 vs 09:30 $9,915.97 (session -70.35) | 16:00 close · cash $7,421.14 · equity $9,818.29 vs 09:30 $9,915.97 (-97.68; session marks -70.35) · 1 name(s) marked open→close (per-name table). CRSP×42 09:30 $58.75 → close $57.08 -70.35 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,421.14 | ▲ 09:30 equity $9,854.20 vs yday $9,818.29 (+35.91) | 09:30 open · cash $7,421.14 (unchanged overnight, no fees) · equity $9,854.20 vs prior close $9,818.29 (+35.91) · 1 name(s) re-marked at the open (per-name table). CRSP×42 yday $57.08 → 09:30 $57.93 +35.91 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 42 | $57.93 | $2.15 | $-37.86 | $9,852.06 | ▼ -37.86 after sell → book $9,852.06; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 41 | $118.52 | $2.11 | — | $4,990.62 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $4926.03 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 63 | $77.13 | $2.18 | — | $129.26 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $4926.03 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.26 | ▲ close $10,222.58 vs 09:30 $9,854.20 (session +374.81) | 16:00 close · cash $129.26 · equity $10,222.58 vs 09:30 $9,854.20 (+368.38; session marks +374.81) · 2 name(s) marked open→close (per-name table). AU×41 09:30 $118.52 → close $123.39 +199.67; FCX×63 09:30 $77.13 → close $79.91 +175.14 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.26 | ▼ 09:30 equity $10,039.48 vs yday $10,222.58 (-183.10) | 09:30 open · cash $129.26 (unchanged overnight, no fees) · equity $10,039.48 vs prior close $10,222.58 (-183.10) · 2 name(s) re-marked at the open (per-name table). AU×41 yday $123.39 → 09:30 $119.80 -147.19; FCX×63 yday $79.91 → 09:30 $79.34 -35.91 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 41 | $119.80 | $2.16 | $+48.20 | $5,038.89 | ▲ +48.20 after sell → book $10,037.31; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 63 | $79.34 | $2.23 | $+134.82 | $10,035.08 | ▲ +134.82 after sell → book $10,035.08; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 84 | $118.50 | $2.24 | — | $78.84 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $10035.08 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.84 | ▼ close $10,007.64 vs 09:30 $10,039.48 (session -25.20) | 16:00 close · cash $78.84 · equity $10,007.64 vs 09:30 $10,039.48 (-31.84; session marks -25.20) · 1 name(s) marked open→close (per-name table). CM×84 09:30 $118.50 → close $118.20 -25.20 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.84 | ▲ 09:30 equity $10,055.52 vs yday $10,007.64 (+47.88) | 09:30 open · cash $78.84 (unchanged overnight, no fees) · equity $10,055.52 vs prior close $10,007.64 (+47.88) · 1 name(s) re-marked at the open (per-name table). CM×84 yday $118.20 → 09:30 $118.77 +47.88 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.84 | ▼ close $9,725.40 vs 09:30 $10,055.52 (session -330.12) | 16:00 close · cash $78.84 · equity $9,725.40 vs 09:30 $10,055.52 (-330.12; session marks -330.12) · 1 name(s) marked open→close (per-name table). CM×84 09:30 $118.77 → close $114.84 -330.12 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.84 | ▲ 09:30 equity $9,794.28 vs yday $9,725.40 (+68.88) | 09:30 open · cash $78.84 (unchanged overnight, no fees) · equity $9,794.28 vs prior close $9,725.40 (+68.88) · 1 name(s) re-marked at the open (per-name table). CM×84 yday $114.84 → 09:30 $115.66 +68.88 | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 84 | $115.66 | $2.33 | $-243.14 | $9,791.95 | ▼ -243.14 after sell → book $9,791.95; vs 09:30 mark -2.33 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 5 | $324.41 | $2.00 | — | $8,167.89 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1631.99 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 11 | $141.76 | $2.02 | — | $6,606.51 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1631.99 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 4 | $400.42 | $2.00 | — | $5,002.83 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1631.99 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $3,694.81 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1631.99 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 6 | $240.22 | $2.01 | — | $2,251.48 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1631.99 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 49 | $32.90 | $2.14 | — | $637.24 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1631.99 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $637.24 | ▼ close $9,410.95 vs 09:30 $9,794.28 (session -368.83) | 16:00 close · cash $637.24 · equity $9,410.95 vs 09:30 $9,794.28 (-383.33; session marks -368.83) · 6 name(s) marked open→close (per-name table). KEYS×5 09:30 $324.41 → close $319.97 -22.20; SMTC×11 09:30 $141.76 → close $131.17 -116.49; CIEN×4 09:30 $400.42 → close $378.44 -87.92; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×6 09:30 $240.22 → close $236.98 -19.44; SEDG×49 09:30 $32.90 → close $31.41 -73.01 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $637.24 | ▼ 09:30 equity $9,410.79 vs yday $9,410.95 (-0.16) | 09:30 open · cash $637.24 (unchanged overnight, no fees) · equity $9,410.79 vs prior close $9,410.95 (-0.16) · 6 name(s) re-marked at the open (per-name table). KEYS×5 yday $319.97 → 09:30 $322.49 +12.60; SMTC×11 yday $131.17 → 09:30 $132.30 +12.43; CIEN×4 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×6 yday $236.98 → 09:30 $233.97 -18.09; SEDG×49 yday $31.41 → 09:30 $31.15 -12.74 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 5 | $322.49 | $2.03 | $-13.63 | $2,247.66 | ▼ -13.63 after sell → book $9,408.76; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 11 | $132.30 | $2.04 | $-108.13 | $3,700.92 | ▼ -108.13 after sell → book $9,406.72; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 4 | $378.44 | $2.02 | $-91.95 | $5,212.65 | ▼ -91.95 after sell → book $9,404.69; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $6,472.54 | ▼ -48.14 after sell → book $9,402.68; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 6 | $233.97 | $2.03 | $-41.57 | $7,874.30 | ▼ -41.57 after sell → book $9,400.65; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 49 | $31.15 | $2.16 | $-90.05 | $9,398.49 | ▼ -90.05 after sell → book $9,398.49; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,398.49 | ▲ close $9,398.49 vs 09:30 $9,410.79 (session +0.00) | 16:00 close · cash $9,398.49 · no lots left · equity $9,398.49. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,398.49 | ▲ 09:30 equity $9,398.49 vs yday $9,398.49 (+0.00) | 09:30 open · cash $9,398.49 · no holdings · equity $9,398.49 vs prior close $9,398.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,398.49 | ▲ close $9,398.49 vs 09:30 $9,398.49 (session +0.00) | 16:00 close · cash $9,398.49 · no lots left · equity $9,398.49. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,398.49 | ▲ 09:30 equity $9,398.49 vs yday $9,398.49 (+0.00) | 09:30 open · cash $9,398.49 · no holdings · equity $9,398.49 vs prior close $9,398.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,398.49 | ▲ close $9,398.49 vs 09:30 $9,398.49 (session +0.00) | 16:00 close · cash $9,398.49 · no lots left · equity $9,398.49. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,398.49 | ▲ 09:30 equity $9,398.49 vs yday $9,398.49 (+0.00) | 09:30 open · cash $9,398.49 · no holdings · equity $9,398.49 vs prior close $9,398.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 5 | $351.74 | $2.00 | — | $7,637.79 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1879.70 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $6,176.86 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1879.70 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 58 | $32.31 | $2.16 | — | $4,300.71 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1879.70 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 118 | $15.87 | $2.34 | — | $2,425.71 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1879.70 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 78 | $23.88 | $2.22 | — | $560.85 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1879.70 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $560.85 | ▲ close $9,701.82 vs 09:30 $9,398.49 (session +314.06) | 16:00 close · cash $560.85 · equity $9,701.82 vs 09:30 $9,398.49 (+303.33; session marks +314.06) · 5 name(s) marked open→close (per-name table). AVGO×5 09:30 $351.74 → close $357.16 +27.10; DELL×3 09:30 $486.31 → close $516.39 +90.24; CXW×58 09:30 $32.31 → close $33.66 +78.30; FRNM×118 09:30 $15.87 → close $16.90 +121.54; MMED×78 09:30 $23.88 → close $23.84 -3.12 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $560.85 | ▼ 09:30 equity $9,636.09 vs yday $9,701.82 (-65.73) | 09:30 open · cash $560.85 (unchanged overnight, no fees) · equity $9,636.09 vs prior close $9,701.82 (-65.73) · 5 name(s) re-marked at the open (per-name table). AVGO×5 yday $357.16 → 09:30 $359.70 +12.70; DELL×3 yday $516.39 → 09:30 $513.78 -7.83; CXW×58 yday $33.66 → 09:30 $33.46 -11.60; FRNM×118 yday $16.90 → 09:30 $16.40 -59.00; MMED×78 yday $23.84 → 09:30 $23.84 +0.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 5 | $359.70 | $2.03 | $+35.77 | $2,357.32 | ▲ +35.77 after sell → book $9,634.06; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 3 | $513.78 | $2.02 | $+78.39 | $3,896.64 | ▲ +78.39 after sell → book $9,632.04; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 58 | $33.46 | $2.19 | $+62.35 | $5,835.13 | ▲ +62.35 after sell → book $9,629.85; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 78 | $23.84 | $2.25 | $-7.60 | $7,692.39 | ▼ -7.60 after sell → book $9,627.59; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 14 | $263.36 | $2.03 | — | $4,003.32 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $3846.20 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 50 | $75.65 | $2.14 | — | $218.68 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $3846.20 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $218.68 | ▲ close $9,685.98 vs 09:30 $9,636.09 (session +62.56) | 16:00 close · cash $218.68 · equity $9,685.98 vs 09:30 $9,636.09 (+49.89; session marks +62.56) · 3 name(s) marked open→close (per-name table). FRNM×118 09:30 $16.40 → close $16.31 -10.62; CRM×14 09:30 $263.36 → close $259.23 -57.82; MRX×50 09:30 $75.65 → close $78.27 +131.00 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $218.68 | ▲ 09:30 equity $9,688.08 vs yday $9,685.98 (+2.10) | 09:30 open · cash $218.68 (unchanged overnight, no fees) · equity $9,688.08 vs prior close $9,685.98 (+2.10) · 3 name(s) re-marked at the open (per-name table). FRNM×118 yday $16.31 → 09:30 $16.74 +50.74; CRM×14 yday $259.23 → 09:30 $253.72 -77.14; MRX×50 yday $78.27 → 09:30 $78.84 +28.50 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 118 | $16.74 | $2.38 | $+97.94 | $2,191.62 | ▲ +97.94 after sell → book $9,685.70; vs 09:30 mark -2.38 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 14 | $253.72 | $2.07 | $-139.06 | $5,741.63 | ▼ -139.06 after sell → book $9,683.63; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 50 | $78.84 | $2.18 | $+155.18 | $9,681.45 | ▲ +155.18 after sell → book $9,681.45; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,681.45 | ▲ close $9,681.45 vs 09:30 $9,688.08 (session +0.00) | 16:00 close · cash $9,681.45 · no lots left · equity $9,681.45. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,681.45 | ▲ 09:30 equity $9,681.45 vs yday $9,681.45 (+0.00) | 09:30 open · cash $9,681.45 · no holdings · equity $9,681.45 vs prior close $9,681.45 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,681.45 | ▲ close $9,681.45 vs 09:30 $9,681.45 (session +0.00) | 16:00 close · cash $9,681.45 · no lots left · equity $9,681.45. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,681.45 | ▲ 09:30 equity $9,681.45 vs yday $9,681.45 (+0.00) | 09:30 open · cash $9,681.45 · no holdings · equity $9,681.45 vs prior close $9,681.45 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,681.45 | ▲ close $9,681.45 vs 09:30 $9,681.45 (session +0.00) | 16:00 close · cash $9,681.45 · no lots left · equity $9,681.45. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,681.45 | ▲ 09:30 equity $9,681.45 vs yday $9,681.45 (+0.00) | 09:30 open · cash $9,681.45 · no holdings · equity $9,681.45 vs prior close $9,681.45 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 58 | $164.43 | $2.16 | — | $142.35 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $9681.45 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.35 | ▼ close $8,858.59 vs 09:30 $9,681.45 (session -820.70) | 16:00 close · cash $142.35 · equity $8,858.59 vs 09:30 $9,681.45 (-822.86; session marks -820.70) · 1 name(s) marked open→close (per-name table). ORCL×58 09:30 $164.43 → close $150.28 -820.70 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.35 | ▼ 09:30 equity $8,344.71 vs yday $8,858.59 (-513.88) | 09:30 open · cash $142.35 (unchanged overnight, no fees) · equity $8,344.71 vs prior close $8,858.59 (-513.88) · 1 name(s) re-marked at the open (per-name table). ORCL×58 yday $150.28 → 09:30 $141.42 -513.88 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 58 | $141.42 | $2.24 | $-1338.98 | $8,342.47 | ▼ -1,338.98 after sell → book $8,342.47; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,342.47 | ▲ close $8,342.47 vs 09:30 $8,344.71 (session +0.00) | 16:00 close · cash $8,342.47 · no lots left · equity $8,342.47. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,342.47 | ▲ 09:30 equity $8,342.47 vs yday $8,342.47 (-0.00) | 09:30 open · cash $8,342.47 · no holdings · equity $8,342.47 vs prior close $8,342.47 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,342.47 | ▲ close $8,342.47 vs 09:30 $8,342.47 (session +0.00) | 16:00 close · cash $8,342.47 · no lots left · equity $8,342.47. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,342.47 | ▲ 09:30 equity $8,342.47 vs yday $8,342.47 (-0.00) | 09:30 open · cash $8,342.47 · no holdings · equity $8,342.47 vs prior close $8,342.47 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 105 | $26.27 | $2.31 | — | $5,581.81 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2780.82 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 14 | $189.17 | $2.03 | — | $2,931.40 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2780.82 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 69 | $39.99 | $2.20 | — | $169.89 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2780.82 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $169.89 | ▼ close $8,182.64 vs 09:30 $8,342.47 (session -153.29) | 16:00 close · cash $169.89 · equity $8,182.64 vs 09:30 $8,342.47 (-159.83; session marks -153.29) · 3 name(s) marked open→close (per-name table). WAY×105 09:30 $26.27 → close $26.59 +33.60; QCOM×14 09:30 $189.17 → close $184.84 -60.62; SM×69 09:30 $39.99 → close $38.16 -126.27 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $169.89 | ▲ 09:30 equity $8,210.67 vs yday $8,182.64 (+28.03) | 09:30 open · cash $169.89 (unchanged overnight, no fees) · equity $8,210.67 vs prior close $8,182.64 (+28.03) · 3 name(s) re-marked at the open (per-name table). WAY×105 yday $26.59 → 09:30 $26.51 -8.40; QCOM×14 yday $184.84 → 09:30 $190.35 +77.14; SM×69 yday $38.16 → 09:30 $37.57 -40.71 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 105 | $26.51 | $2.34 | $+20.55 | $2,951.10 | ▲ +20.55 after sell → book $8,208.33; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 14 | $190.35 | $2.06 | $+12.42 | $5,613.94 | ▲ +12.42 after sell → book $8,206.27; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 69 | $37.57 | $2.23 | $-171.41 | $8,204.04 | ▼ -171.41 after sell → book $8,204.04; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 16 | $170.85 | $2.04 | — | $5,468.40 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $2734.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `CLS` | 8 | $337.75 | $2.01 | — | $2,764.38 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+10.2; leftover $2734.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 123 | $22.12 | $2.36 | — | $41.27 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $2734.68 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.27 | ▲ close $8,332.54 vs 09:30 $8,210.67 (session +134.91) | 16:00 close · cash $41.27 · equity $8,332.54 vs 09:30 $8,210.67 (+121.87; session marks +134.91) · 3 name(s) marked open→close (per-name table). SMTC×16 09:30 $170.85 → close $178.19 +117.44; CLS×8 09:30 $337.75 → close $329.94 -62.48; GME×123 09:30 $22.12 → close $22.77 +79.95 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.27 | ▲ 09:30 equity $8,431.73 vs yday $8,332.54 (+99.19) | 09:30 open · cash $41.27 (unchanged overnight, no fees) · equity $8,431.73 vs prior close $8,332.54 (+99.19) · 3 name(s) re-marked at the open (per-name table). SMTC×16 yday $178.19 → 09:30 $182.33 +66.24; CLS×8 yday $329.94 → 09:30 $332.06 +16.96; GME×123 yday $22.77 → 09:30 $22.90 +15.99 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 16 | $182.33 | $2.07 | $+179.57 | $2,956.47 | ▲ +179.57 after sell → book $8,429.65; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 47 | $20.91 | $2.13 | — | $1,971.57 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $985.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 66 | $14.79 | $2.19 | — | $993.24 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $985.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 70 | $14.07 | $2.20 | — | $6.14 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $985.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.14 | ▼ close $8,358.89 vs 09:30 $8,431.73 (session -64.24) | 16:00 close · cash $6.14 · equity $8,358.89 vs 09:30 $8,431.73 (-72.84; session marks -64.24) · 5 name(s) marked open→close (per-name table). CLS×8 09:30 $332.06 → close $332.63 +4.56; GME×123 09:30 $22.90 → close $22.64 -31.98; TH×47 09:30 $20.91 → close $21.19 +13.16; RARE×66 09:30 $14.79 → close $14.51 -18.48; BHVN×70 09:30 $14.07 → close $13.62 -31.50 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.14 | ▲ 09:30 equity $8,492.51 vs yday $8,358.89 (+133.62) | 09:30 open · cash $6.14 (unchanged overnight, no fees) · equity $8,492.51 vs prior close $8,358.89 (+133.62) · 5 name(s) re-marked at the open (per-name table). CLS×8 yday $332.63 → 09:30 $341.45 +70.56; GME×123 yday $22.64 → 09:30 $22.78 +17.22; TH×47 yday $21.19 → 09:30 $21.65 +21.62; RARE×66 yday $14.51 → 09:30 $14.58 +4.62; BHVN×70 yday $13.62 → 09:30 $13.90 +19.60 | — |
| 2026-09-21 09:30 ET | **SELL** | `CLS` | 8 | $341.45 | $2.05 | $+25.54 | $2,735.70 | ▲ +25.54 after sell → book $8,490.47; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 123 | $22.78 | $2.40 | $+76.42 | $5,535.24 | ▲ +76.42 after sell → book $8,488.07; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 47 | $21.65 | $2.15 | $+30.50 | $6,550.64 | ▲ +30.50 after sell → book $8,485.92; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 66 | $14.58 | $2.21 | $-18.26 | $7,510.71 | ▼ -18.26 after sell → book $8,483.71; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 70 | $13.90 | $2.22 | $-16.32 | $8,481.49 | ▼ -16.32 after sell → book $8,481.49; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 18 | $230.25 | $2.04 | — | $4,334.94 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+12.5; leftover $4240.74 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 22 | $190.30 | $2.06 | — | $146.29 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+10.6; leftover $4240.74 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $146.29 | ▼ close $8,078.63 vs 09:30 $8,492.51 (session -398.76) | 16:00 close · cash $146.29 · equity $8,078.63 vs 09:30 $8,492.51 (-413.88; session marks -398.76) · 2 name(s) marked open→close (per-name table). VICR×18 09:30 $230.25 → close $223.90 -114.30; SMTC×22 09:30 $190.30 → close $177.37 -284.46 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $146.29 | ▲ 09:30 equity $8,589.55 vs yday $8,078.63 (+510.92) | 09:30 open · cash $146.29 (unchanged overnight, no fees) · equity $8,589.55 vs prior close $8,078.63 (+510.92) · 2 name(s) re-marked at the open (per-name table). VICR×18 yday $223.90 → 09:30 $252.37 +512.46; SMTC×22 yday $177.37 → 09:30 $177.30 -1.54 | — |
| 2026-09-22 09:30 ET | **SELL** | `VICR` | 18 | $252.37 | $2.09 | $+394.03 | $4,686.86 | ▲ +394.03 after sell → book $8,587.46; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SMTC` | 22 | $177.30 | $2.10 | $-290.15 | $8,585.36 | ▼ -290.15 after sell → book $8,585.36; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,585.36 | ▲ close $8,585.36 vs 09:30 $8,589.55 (session +0.00) | 16:00 close · cash $8,585.36 · no lots left · equity $8,585.36. | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,585.36 | ▲ 09:30 equity $8,585.36 vs yday $8,585.36 (-0.00) | 09:30 open · cash $8,585.36 · no holdings · equity $8,585.36 vs prior close $8,585.36 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 539 | $7.95 | $6.95 | — | $4,293.35 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $4292.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 273 | $15.70 | $3.52 | — | $3.73 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.5; leftover $4292.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.73 | ▼ close $7,988.77 vs 09:30 $8,585.36 (session -586.11) | 16:00 close · cash $3.73 · equity $7,988.77 vs 09:30 $8,585.36 (-596.59; session marks -586.11) · 2 name(s) marked open→close (per-name table). PGEN×539 09:30 $7.95 → close $7.44 -274.89; SGRY×273 09:30 $15.70 → close $14.56 -311.22 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ACMR` | cash | leftover split 13.14 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 13.14 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 13.14 < 1 share @ 1746.53 |
| 2026-08-27 | `GEN` | cash | leftover split 13.14 < 1 share @ 29.83 |
| 2026-08-27 | `LRCX` | cash | leftover split 13.14 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 13.14 < 1 share @ 222.86 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `PGEN` | 539 | 2026-09-23 @ $7.95 | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $4292.68 |
| `SGRY` | 273 | 2026-09-23 @ $15.70 | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.5; leftover $4292.68 |
