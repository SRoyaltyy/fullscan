# Factor mine action — `union_news_or_net4_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 4

Cash book **-10.27%** ($8,973) · signal-only (no cash/fees) was -4.68%. Starts YES **0/27**. Fills 126 · skips 25 · realized $-782.13.

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
- Must-have: the morning news packet OR the prior-export headline is green.
- Must-have: camera net (+G −R) is at least 4.
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
- **Gate** `news_or_headline=True,cam_net_min=4` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $14.68.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `HLIT` | 126 | — | $13.18 | +0.00 | $13.92 | +93.24 | +93.24 | +0.00 | +93.24 |
| 2026-08-14 | `SNDK` | 1 | — | $1646.93 | +0.00 | $1641.11 | -5.82 | -5.82 | +0.00 | -5.82 |
| 2026-08-14 | `ANGX` | 386 | — | $4.31 | +0.00 | $4.37 | +23.16 | +23.16 | +0.00 | +23.16 |
| 2026-08-14 | `ARX` | 85 | — | $19.57 | +0.00 | $19.58 | +0.85 | +0.85 | +0.00 | +0.85 |
| 2026-08-14 | `MH` | 123 | — | $13.55 | +0.00 | $13.10 | -55.35 | -55.35 | +0.00 | -55.35 |
| 2026-08-14 | `VELO` | 108 | — | $15.38 | +0.00 | $16.16 | +84.24 | +84.24 | +0.00 | +84.24 |
| 2026-08-17 | `HLIT` | 126 | $13.92 | $13.84 | -10.08 | — | +0.00 | -10.08 | +83.16 | — |
| 2026-08-17 | `SNDK` | 1 | $1641.11 | $1700.74 | +59.63 | — | +0.00 | +59.63 | +53.81 | — |
| 2026-08-17 | `ANGX` | 386 | $4.37 | $4.60 | +88.78 | — | +0.00 | +88.78 | +111.94 | — |
| 2026-08-17 | `ARX` | 85 | $19.58 | $19.57 | -0.85 | — | +0.00 | -0.85 | +0.00 | — |
| 2026-08-17 | `MH` | 123 | $13.10 | $13.16 | +7.38 | — | +0.00 | +7.38 | -47.97 | — |
| 2026-08-17 | `VELO` | 108 | $16.16 | $16.05 | -11.88 | — | +0.00 | -11.88 | +72.36 | — |
| 2026-08-17 | `DVN` | 73 | — | $46.18 | +0.00 | $47.57 | +101.47 | +101.47 | +0.00 | +101.47 |
| 2026-08-17 | `EOG` | 23 | — | $142.77 | +0.00 | $146.15 | +77.74 | +77.74 | +0.00 | +77.74 |
| 2026-08-17 | `FANG` | 16 | — | $202.70 | +0.00 | $206.29 | +57.44 | +57.44 | +0.00 | +57.44 |
| 2026-08-18 | `DVN` | 73 | $47.57 | $48.00 | +31.39 | — | +0.00 | +31.39 | +132.86 | — |
| 2026-08-18 | `EOG` | 23 | $146.15 | $148.04 | +43.47 | — | +0.00 | +43.47 | +121.21 | — |
| 2026-08-18 | `FANG` | 16 | $206.29 | $208.93 | +42.24 | — | +0.00 | +42.24 | +99.68 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 16 | — | $91.01 | +0.00 | $93.63 | +41.92 | +41.92 | +0.00 | +41.92 |
| 2026-08-20 | `APA` | 33 | — | $44.76 | +0.00 | $44.39 | -12.21 | -12.21 | +0.00 | -12.21 |
| 2026-08-20 | `AUTL` | 612 | — | $2.47 | +0.00 | $2.46 | -6.12 | -6.12 | +0.00 | -6.12 |
| 2026-08-20 | `CRSP` | 25 | — | $58.73 | +0.00 | $58.12 | -15.25 | -15.25 | +0.00 | -15.25 |
| 2026-08-20 | `ASST` | 94 | — | $16.00 | +0.00 | $16.13 | +12.22 | +12.22 | +0.00 | +12.22 |
| 2026-08-20 | `MRNA` | 10 | — | $150.14 | +0.00 | $133.32 | -168.20 | -168.20 | +0.00 | -168.20 |
| 2026-08-20 | `ZLAB` | 56 | — | $26.57 | +0.00 | $26.02 | -30.80 | -30.80 | +0.00 | -30.80 |
| 2026-08-21 | `BHP` | 16 | $93.63 | $95.72 | +33.44 | — | +0.00 | +33.44 | +75.36 | — |
| 2026-08-21 | `APA` | 33 | $44.39 | $44.52 | +4.29 | — | +0.00 | +4.29 | -7.92 | — |
| 2026-08-21 | `AUTL` | 612 | $2.46 | $2.47 | +6.12 | $2.41 | -36.72 | -30.60 | +0.00 | -36.72 |
| 2026-08-21 | `CRSP` | 25 | $58.12 | $59.72 | +40.00 | $59.50 | -5.50 | +34.50 | +24.75 | +19.25 |
| 2026-08-21 | `ASST` | 94 | $16.13 | $17.66 | +143.82 | — | +0.00 | +143.82 | +156.04 | — |
| 2026-08-21 | `MRNA` | 10 | $133.32 | $133.11 | -2.10 | — | +0.00 | -2.10 | -170.30 | — |
| 2026-08-21 | `ZLAB` | 56 | $26.02 | $26.25 | +12.88 | — | +0.00 | +12.88 | -17.92 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `FUTU` | 11 | — | $115.18 | +0.00 | $123.64 | +93.06 | +93.06 | +0.00 | +93.06 |
| 2026-08-21 | `GRAL` | 16 | — | $78.88 | +0.00 | $79.54 | +10.56 | +10.56 | +0.00 | +10.56 |
| 2026-08-21 | `ABTC` | 146 | — | $8.66 | +0.00 | $7.93 | -106.58 | -106.58 | +0.00 | -106.58 |
| 2026-08-21 | `HIVE` | 391 | — | $3.24 | +0.00 | $3.03 | -82.11 | -82.11 | +0.00 | -82.11 |
| 2026-08-21 | `MARA` | 108 | — | $11.70 | +0.00 | $11.26 | -47.52 | -47.52 | +0.00 | -47.52 |
| 2026-08-24 | `AUTL` | 612 | $2.41 | $2.40 | -6.12 | — | +0.00 | -6.12 | -42.84 | — |
| 2026-08-24 | `CRSP` | 25 | $59.50 | $58.75 | -18.75 | $57.08 | -41.87 | -60.62 | +0.50 | -41.37 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `FUTU` | 11 | $123.64 | $121.00 | -29.04 | — | +0.00 | -29.04 | +64.02 | — |
| 2026-08-24 | `GRAL` | 16 | $79.54 | $81.87 | +37.28 | — | +0.00 | +37.28 | +47.84 | — |
| 2026-08-24 | `ABTC` | 146 | $7.93 | $8.00 | +10.22 | — | +0.00 | +10.22 | -96.36 | — |
| 2026-08-24 | `HIVE` | 391 | $3.03 | $2.99 | -15.64 | — | +0.00 | -15.64 | -97.75 | — |
| 2026-08-24 | `MARA` | 108 | $11.26 | $11.17 | -9.72 | — | +0.00 | -9.72 | -57.24 | — |
| 2026-08-25 | `CRSP` | 25 | $57.08 | $57.93 | +21.37 | — | +0.00 | +21.37 | -20.00 | — |
| 2026-08-25 | `AU` | 29 | — | $118.52 | +0.00 | $123.39 | +141.23 | +141.23 | +0.00 | +141.23 |
| 2026-08-25 | `FCX` | 44 | — | $77.13 | +0.00 | $79.91 | +122.32 | +122.32 | +0.00 | +122.32 |
| 2026-08-25 | `EZPW` | 98 | — | $35.05 | +0.00 | $35.23 | +17.64 | +17.64 | +0.00 | +17.64 |
| 2026-08-26 | `AU` | 29 | $123.39 | $119.80 | -104.11 | — | +0.00 | -104.11 | +37.12 | — |
| 2026-08-26 | `FCX` | 44 | $79.91 | $79.34 | -25.08 | — | +0.00 | -25.08 | +97.24 | — |
| 2026-08-26 | `EZPW` | 98 | $35.23 | $35.70 | +46.06 | — | +0.00 | +46.06 | +63.70 | — |
| 2026-08-26 | `FNV` | 19 | — | $267.02 | +0.00 | $267.37 | +6.65 | +6.65 | +0.00 | +6.65 |
| 2026-08-26 | `CM` | 44 | — | $118.50 | +0.00 | $118.20 | -13.20 | -13.20 | +0.00 | -13.20 |
| 2026-08-27 | `FNV` | 19 | $267.37 | $267.23 | -2.66 | — | +0.00 | -2.66 | +3.99 | — |
| 2026-08-27 | `CM` | 44 | $118.20 | $118.77 | +25.08 | $114.84 | -172.92 | -147.84 | +11.88 | -161.04 |
| 2026-08-27 | `ACMR` | 9 | — | $81.65 | +0.00 | $80.49 | -10.44 | -10.44 | +0.00 | -10.44 |
| 2026-08-27 | `GEN` | 25 | — | $29.83 | +0.00 | $30.50 | +16.75 | +16.75 | +0.00 | +16.75 |
| 2026-08-27 | `LRCX` | 2 | — | $318.88 | +0.00 | $318.58 | -0.60 | -0.60 | +0.00 | -0.60 |
| 2026-08-27 | `NVDA` | 3 | — | $222.86 | +0.00 | $227.98 | +15.36 | +15.36 | +0.00 | +15.36 |
| 2026-08-27 | `ADSK` | 2 | — | $261.47 | +0.00 | $270.58 | +18.22 | +18.22 | +0.00 | +18.22 |
| 2026-08-28 | `CM` | 44 | $114.84 | $115.66 | +36.08 | — | +0.00 | +36.08 | -124.96 | — |
| 2026-08-28 | `ACMR` | 9 | $80.49 | $79.27 | -10.98 | — | +0.00 | -10.98 | -21.42 | — |
| 2026-08-28 | `GEN` | 25 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +16.75 | — |
| 2026-08-28 | `LRCX` | 2 | $318.58 | $318.03 | -1.10 | — | +0.00 | -1.10 | -1.70 | — |
| 2026-08-28 | `NVDA` | 3 | $227.98 | $227.36 | -1.86 | — | +0.00 | -1.86 | +13.50 | — |
| 2026-08-28 | `ADSK` | 2 | $270.58 | $261.16 | -18.84 | $260.66 | -1.00 | -19.84 | -0.62 | -1.62 |
| 2026-08-28 | `KEYS` | 4 | — | $324.41 | +0.00 | $319.97 | -17.76 | -17.76 | +0.00 | -17.76 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 5 | — | $240.22 | +0.00 | $236.98 | -16.20 | -16.20 | +0.00 | -16.20 |
| 2026-08-28 | `SEDG` | 42 | — | $32.90 | +0.00 | $31.41 | -62.58 | -62.58 | +0.00 | -62.58 |
| 2026-08-28 | `TLS` | 292 | — | $4.82 | +0.00 | $4.79 | -8.76 | -8.76 | +0.00 | -8.76 |
| 2026-08-31 | `ADSK` | 2 | $260.66 | $257.71 | -5.90 | — | +0.00 | -5.90 | -7.52 | — |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | — | +0.00 | +10.08 | -7.68 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -65.94 | — |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | — | +0.00 | +5.64 | -44.13 | — |
| 2026-08-31 | `DDOG` | 5 | $236.98 | $233.97 | -15.07 | — | +0.00 | -15.07 | -31.27 | — |
| 2026-08-31 | `SEDG` | 42 | $31.41 | $31.15 | -10.92 | — | +0.00 | -10.92 | -73.50 | — |
| 2026-08-31 | `TLS` | 292 | $4.79 | $4.81 | +5.84 | — | +0.00 | +5.84 | -2.92 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 4 | — | $351.74 | +0.00 | $357.16 | +21.68 | +21.68 | +0.00 | +21.68 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `CXW` | 44 | — | $32.31 | +0.00 | $33.66 | +59.40 | +59.40 | +0.00 | +59.40 |
| 2026-09-03 | `FRNM` | 90 | — | $15.87 | +0.00 | $16.90 | +92.70 | +92.70 | +0.00 | +92.70 |
| 2026-09-03 | `MMED` | 60 | — | $23.88 | +0.00 | $23.84 | -2.40 | -2.40 | +0.00 | -2.40 |
| 2026-09-03 | `DE` | 2 | — | $703.25 | +0.00 | $694.41 | -17.68 | -17.68 | +0.00 | -17.68 |
| 2026-09-03 | `HPE` | 30 | — | $47.60 | +0.00 | $54.44 | +205.20 | +205.20 | +0.00 | +205.20 |
| 2026-09-04 | `AVGO` | 4 | $357.16 | $359.70 | +10.16 | — | +0.00 | +10.16 | +31.84 | — |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | — | +0.00 | -5.22 | +54.94 | — |
| 2026-09-04 | `CXW` | 44 | $33.66 | $33.46 | -8.80 | — | +0.00 | -8.80 | +50.60 | — |
| 2026-09-04 | `FRNM` | 90 | $16.90 | $16.40 | -45.00 | $16.31 | -8.10 | -53.10 | +47.70 | +39.60 |
| 2026-09-04 | `MMED` | 60 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.40 | — |
| 2026-09-04 | `DE` | 2 | $694.41 | $692.03 | -4.76 | — | +0.00 | -4.76 | -22.44 | — |
| 2026-09-04 | `HPE` | 30 | $54.44 | $53.85 | -17.70 | — | +0.00 | -17.70 | +187.50 | — |
| 2026-09-04 | `CRM` | 8 | — | $263.36 | +0.00 | $259.23 | -33.04 | -33.04 | +0.00 | -33.04 |
| 2026-09-04 | `MRX` | 29 | — | $75.65 | +0.00 | $78.27 | +75.98 | +75.98 | +0.00 | +75.98 |
| 2026-09-04 | `BE` | 9 | — | $236.82 | +0.00 | $252.87 | +144.45 | +144.45 | +0.00 | +144.45 |
| 2026-09-04 | `BAK` | 1145 | — | $1.94 | +0.00 | $1.89 | -57.25 | -57.25 | +0.00 | -57.25 |
| 2026-09-08 | `FRNM` | 90 | $16.31 | $16.74 | +38.70 | — | +0.00 | +38.70 | +78.30 | — |
| 2026-09-08 | `CRM` | 8 | $259.23 | $253.72 | -44.08 | — | +0.00 | -44.08 | -77.12 | — |
| 2026-09-08 | `MRX` | 29 | $78.27 | $78.84 | +16.53 | — | +0.00 | +16.53 | +92.51 | — |
| 2026-09-08 | `BE` | 9 | $252.87 | $267.76 | +134.01 | — | +0.00 | +134.01 | +278.46 | — |
| 2026-09-08 | `BAK` | 1145 | $1.89 | $1.94 | +57.25 | — | +0.00 | +57.25 | +0.00 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 64 | — | $164.43 | +0.00 | $150.28 | -905.60 | -905.60 | +0.00 | -905.60 |
| 2026-09-14 | `ORCL` | 64 | $150.28 | $141.42 | -567.04 | — | +0.00 | -567.04 | -1472.64 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 116 | — | $26.27 | +0.00 | $26.59 | +37.12 | +37.12 | +0.00 | +37.12 |
| 2026-09-16 | `QCOM` | 16 | — | $189.17 | +0.00 | $184.84 | -69.28 | -69.28 | +0.00 | -69.28 |
| 2026-09-16 | `SM` | 76 | — | $39.99 | +0.00 | $38.16 | -139.08 | -139.08 | +0.00 | -139.08 |
| 2026-09-17 | `WAY` | 116 | $26.59 | $26.51 | -9.28 | — | +0.00 | -9.28 | +27.84 | — |
| 2026-09-17 | `QCOM` | 16 | $184.84 | $190.35 | +88.16 | — | +0.00 | +88.16 | +18.88 | — |
| 2026-09-17 | `SM` | 76 | $38.16 | $37.57 | -44.84 | — | +0.00 | -44.84 | -183.92 | — |
| 2026-09-17 | `SMTC` | 13 | — | $170.85 | +0.00 | $178.19 | +95.42 | +95.42 | +0.00 | +95.42 |
| 2026-09-17 | `CLS` | 6 | — | $337.75 | +0.00 | $329.94 | -46.86 | -46.86 | +0.00 | -46.86 |
| 2026-09-17 | `GME` | 101 | — | $22.12 | +0.00 | $22.77 | +65.65 | +65.65 | +0.00 | +65.65 |
| 2026-09-17 | `JBHT` | 9 | — | $238.60 | +0.00 | $236.80 | -16.20 | -16.20 | +0.00 | -16.20 |
| 2026-09-18 | `SMTC` | 13 | $178.19 | $182.33 | +53.82 | — | +0.00 | +53.82 | +149.24 | — |
| 2026-09-18 | `CLS` | 6 | $329.94 | $332.06 | +12.72 | $332.63 | +3.42 | +16.14 | -34.14 | -30.72 |
| 2026-09-18 | `GME` | 101 | $22.77 | $22.90 | +13.13 | $22.64 | -26.26 | -13.13 | +78.78 | +52.52 |
| 2026-09-18 | `JBHT` | 9 | $236.80 | $236.80 | +0.00 | — | +0.00 | +0.00 | -16.20 | — |
| 2026-09-18 | `TH` | 77 | — | $20.91 | +0.00 | $21.19 | +21.56 | +21.56 | +0.00 | +21.56 |
| 2026-09-18 | `RARE` | 109 | — | $14.79 | +0.00 | $14.51 | -30.52 | -30.52 | +0.00 | -30.52 |
| 2026-09-18 | `BHVN` | 115 | — | $14.07 | +0.00 | $13.62 | -51.75 | -51.75 | +0.00 | -51.75 |
| 2026-09-21 | `CLS` | 6 | $332.63 | $341.45 | +52.92 | — | +0.00 | +52.92 | +22.20 | — |
| 2026-09-21 | `GME` | 101 | $22.64 | $22.78 | +14.14 | — | +0.00 | +14.14 | +66.66 | — |
| 2026-09-21 | `TH` | 77 | $21.19 | $21.55 | +27.72 | — | +0.00 | +27.72 | +49.28 | — |
| 2026-09-21 | `RARE` | 109 | $14.51 | $14.60 | +9.81 | — | +0.00 | +9.81 | -20.71 | — |
| 2026-09-21 | `BHVN` | 115 | $13.62 | $13.90 | +32.20 | — | +0.00 | +32.20 | -19.55 | — |
| 2026-09-21 | `VICR` | 10 | — | $230.25 | +0.00 | $223.90 | -63.50 | -63.50 | +0.00 | -63.50 |
| 2026-09-21 | `SMTC` | 12 | — | $190.30 | +0.00 | $177.37 | -155.16 | -155.16 | +0.00 | -155.16 |
| 2026-09-21 | `ALVO` | 389 | — | $5.92 | +0.00 | $5.88 | -15.56 | -15.56 | +0.00 | -15.56 |
| 2026-09-21 | `SION` | 384 | — | $5.99 | +0.00 | $6.00 | +3.84 | +3.84 | +0.00 | +3.84 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +140.32 | HLIT, SNDK, ANGX, ARX, MH, VELO | — | $21.33 | $10,124.06 | HLIT×126, SNDK×1, ANGX×386, ARX×85, MH×123, VELO×108 |
| 2026-08-17 | +2.25 | $21.33 | HLIT×126, SNDK×1, ANGX×386, ARX×85, MH×123, VELO×108 | $10,257.05 | +132.99 | +236.65 | DVN, EOG, FANG | HLIT, SNDK, ANGX, ARX, MH, VELO | $336.20 | $10,470.90 | DVN×73, EOG×23, FANG×16 |
| 2026-08-18 | -6.20 | $336.20 | DVN×73, EOG×23, FANG×16 | $10,588.00 | +117.10 | +0.00 | — | DVN, EOG, FANG | $10,581.58 | $10,581.58 | — |
| 2026-08-19 | -7.20 | $10,581.58 | — | $10,581.58 | +0.00 | +0.00 | — | — | $10,581.58 | $10,581.58 | — |
| 2026-08-20 | +1.12 | $10,581.58 | — | $10,581.58 | +0.00 | -178.44 | BHP, APA, AUTL, CRSP, ASST, MRNA, ZLAB | — | $154.60 | $10,382.61 | BHP×16, APA×33, AUTL×612, CRSP×25, ASST×94, MRNA×10, ZLAB×56 |
| 2026-08-21 | +3.25 | $154.60 | BHP×16, APA×33, AUTL×612, CRSP×25, ASST×94, MRNA×10, ZLAB×56 | $10,621.06 | +238.45 | -156.91 | AU, FUTU, GRAL, ABTC, HIVE, MARA | BHP, APA, ASST, MRNA, ZLAB | $71.70 | $10,437.59 | AUTL×612, CRSP×25, AU×10, FUTU×11, GRAL×16, ABTC×146, HIVE×391, MARA×108 |
| 2026-08-24 | -5.17 | $71.70 | AUTL×612, CRSP×25, AU×10, FUTU×11, GRAL×16, ABTC×146, HIVE×391, MARA×108 | $10,398.72 | -38.87 | -41.87 | — | AUTL, AU, FUTU, GRAL, ABTC, HIVE, MARA | $8,905.89 | $10,332.77 | CRSP×25 |
| 2026-08-25 | +1.80 | $8,905.89 | CRSP×25 | $10,354.14 | +21.37 | +281.19 | AU, FCX, EZPW | CRSP | $79.87 | $10,626.76 | AU×29, FCX×44, EZPW×98 |
| 2026-08-26 | +2.02 | $79.87 | AU×29, FCX×44, EZPW×98 | $10,543.63 | -83.13 | -6.55 | FNV, CM | AU, FCX, EZPW | $245.48 | $10,526.31 | FNV×19, CM×44 |
| 2026-08-27 | — | $245.48 | FNV×19, CM×44 | $10,548.73 | +22.42 | -133.63 | ACMR, GEN, LRCX, NVDA, ADSK | FNV | $2,000.80 | $10,402.93 | CM×44, ACMR×9, GEN×25, LRCX×2, NVDA×3, ADSK×2 |
| 2026-08-28 | +0.75 | $2,000.80 | CM×44, ACMR×9, GEN×25, LRCX×2, NVDA×3, ADSK×2 | $10,406.23 | +3.30 | -317.32 | KEYS, SMTC, CIEN, MPWR, DDOG, SEDG, TLS | CM, ACMR, GEN, LRCX, NVDA | $786.57 | $10,062.68 | ADSK×2, KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, SEDG×42, TLS×292 |
| 2026-08-31 | -5.85 | $786.57 | ADSK×2, KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, SEDG×42, TLS×292 | $10,062.52 | -0.16 | +0.00 | — | ADSK, KEYS, SMTC, CIEN, MPWR, DDOG, SEDG, TLS | $10,044.42 | $10,044.42 | — |
| 2026-09-01 | -6.30 | $10,044.42 | — | $10,044.42 | +0.00 | +0.00 | — | — | $10,044.42 | $10,044.42 | — |
| 2026-09-02 | -3.83 | $10,044.42 | — | $10,044.42 | +0.00 | +0.00 | — | — | $10,044.42 | $10,044.42 | — |
| 2026-09-03 | -0.90 | $10,044.42 | — | $10,044.42 | +0.00 | +419.06 | AVGO, DELL, CXW, FRNM, MMED, DE, HPE | — | $532.98 | $10,448.86 | AVGO×4, DELL×2, CXW×44, FRNM×90, MMED×60, DE×2, HPE×30 |
| 2026-09-04 | +2.25 | $532.98 | AVGO×4, DELL×2, CXW×44, FRNM×90, MMED×60, DE×2, HPE×30 | $10,377.54 | -71.32 | +122.04 | CRM, MRX, BE, BAK | AVGO, DELL, CXW, MMED, DE, HPE | $214.75 | $10,466.20 | FRNM×90, CRM×8, MRX×29, BE×9, BAK×1145 |
| 2026-09-08 | -11.47 | $214.75 | FRNM×90, CRM×8, MRX×29, BE×9, BAK×1145 | $10,668.61 | +202.41 | +0.00 | — | FRNM, CRM, MRX, BE, BAK | $10,645.16 | $10,645.16 | — |
| 2026-09-09 | -13.95 | $10,645.16 | — | $10,645.16 | -0.00 | +0.00 | — | — | $10,645.16 | $10,645.16 | — |
| 2026-09-10 | -13.28 | $10,645.16 | — | $10,645.16 | -0.00 | +0.00 | — | — | $10,645.16 | $10,645.16 | — |
| 2026-09-11 | +0.50 | $10,645.16 | — | $10,645.16 | -0.00 | -905.60 | ORCL | — | $119.45 | $9,737.37 | ORCL×64 |
| 2026-09-14 | -11.00 | $119.45 | ORCL×64 | $9,170.33 | -567.04 | +0.00 | — | ORCL | $9,168.07 | $9,168.07 | — |
| 2026-09-15 | -3.84 | $9,168.07 | — | $9,168.07 | -0.00 | +0.00 | — | — | $9,168.07 | $9,168.07 | — |
| 2026-09-16 | +5.30 | $9,168.07 | — | $9,168.07 | -0.00 | -171.24 | WAY, QCOM, SM | — | $48.19 | $8,990.23 | WAY×116, QCOM×16, SM×76 |
| 2026-09-17 | +7.38 | $48.19 | WAY×116, QCOM×16, SM×76 | $9,024.27 | +34.04 | +98.01 | SMTC, CLS, GME, JBHT | WAY, QCOM, SM | $380.15 | $9,107.23 | SMTC×13, CLS×6, GME×101, JBHT×9 |
| 2026-09-18 | +4.86 | $380.15 | SMTC×13, CLS×6, GME×101, JBHT×9 | $9,186.90 | +79.67 | -83.55 | TH, RARE, BHVN | SMTC, JBHT | $30.44 | $9,092.38 | CLS×6, GME×101, TH×77, RARE×109, BHVN×115 |
| 2026-09-21 | +12.87 | $30.44 | CLS×6, GME×101, TH×77, RARE×109, BHVN×115 | $9,229.17 | +136.79 | -230.38 | VICR, SMTC, ALVO, SION | CLS, GME, TH, RARE, BHVN | $14.68 | $8,973.44 | VICR×10, SMTC×12, ALVO×389, SION×384 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 126 | $13.18 | $2.37 | — | $8,336.95 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 1 | $1646.93 | $1.99 | — | $6,688.03 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 386 | $4.31 | $4.98 | — | $5,019.39 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 85 | $19.57 | $2.25 | — | $3,353.69 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 123 | $13.55 | $2.36 | — | $1,684.69 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 108 | $15.38 | $2.31 | — | $21.33 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.33 | ▲ close $10,124.06 vs 09:30 $10,000.00 (session +140.32) | 16:00 close · cash $21.33 · equity $10,124.06 vs 09:30 $10,000.00 (+124.06; session marks +140.32) · 6 name(s) marked open→close (per-name table). HLIT×126 09:30 $13.18 → close $13.92 +93.24; SNDK×1 09:30 $1646.93 → close $1641.11 -5.82; ANGX×386 09:30 $4.31 → close $4.37 +23.16; ARX×85 09:30 $19.57 → close $19.58 +0.85; MH×123 09:30 $13.55 → close $13.10 -55.35; VELO×108 09:30 $15.38 → close $16.16 +84.24 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.33 | ▲ 09:30 equity $10,257.05 vs yday $10,124.06 (+132.99) | 09:30 open · cash $21.33 (unchanged overnight, no fees) · equity $10,257.05 vs prior close $10,124.06 (+132.99) · 6 name(s) re-marked at the open (per-name table). HLIT×126 yday $13.92 → 09:30 $13.84 -10.08; SNDK×1 yday $1641.11 → 09:30 $1700.74 +59.63; ANGX×386 yday $4.37 → 09:30 $4.60 +88.78; ARX×85 yday $19.58 → 09:30 $19.57 -0.85; MH×123 yday $13.10 → 09:30 $13.16 +7.38; VELO×108 yday $16.16 → 09:30 $16.05 -11.88 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 126 | $13.84 | $2.40 | $+78.39 | $1,762.77 | ▲ +78.39 after sell → book $10,254.64; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SNDK` | 1 | $1700.74 | $2.02 | $+49.81 | $3,461.50 | ▲ +49.81 after sell → book $10,252.63; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 386 | $4.60 | $5.06 | $+101.90 | $5,232.04 | ▲ +101.90 after sell → book $10,247.57; vs 09:30 mark -5.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 85 | $19.57 | $2.27 | $-4.52 | $6,893.22 | ▼ -4.52 after sell → book $10,245.30; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 123 | $13.16 | $2.39 | $-52.72 | $8,509.50 | ▼ -52.72 after sell → book $10,242.90; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VELO` | 108 | $16.05 | $2.35 | $+67.70 | $10,240.56 | ▲ +67.70 after sell → book $10,240.56; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 73 | $46.18 | $2.21 | — | $6,867.21 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+6.7; leftover $3413.52 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 23 | $142.77 | $2.06 | — | $3,581.44 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+5.8; leftover $3413.52 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 16 | $202.70 | $2.04 | — | $336.20 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+8.3; leftover $3413.52 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $336.20 | ▲ close $10,470.90 vs 09:30 $10,257.05 (session +236.65) | 16:00 close · cash $336.20 · equity $10,470.90 vs 09:30 $10,257.05 (+213.85; session marks +236.65) · 3 name(s) marked open→close (per-name table). DVN×73 09:30 $46.18 → close $47.57 +101.47; EOG×23 09:30 $142.77 → close $146.15 +77.74; FANG×16 09:30 $202.70 → close $206.29 +57.44 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $336.20 | ▲ 09:30 equity $10,588.00 vs yday $10,470.90 (+117.10) | 09:30 open · cash $336.20 (unchanged overnight, no fees) · equity $10,588.00 vs prior close $10,470.90 (+117.10) · 3 name(s) re-marked at the open (per-name table). DVN×73 yday $47.57 → 09:30 $48.00 +31.39; EOG×23 yday $146.15 → 09:30 $148.04 +43.47; FANG×16 yday $206.29 → 09:30 $208.93 +42.24 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 73 | $48.00 | $2.25 | $+128.40 | $3,837.95 | ▲ +128.40 after sell → book $10,585.75; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 23 | $148.04 | $2.10 | $+117.05 | $7,240.78 | ▲ +117.05 after sell → book $10,583.66; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 16 | $208.93 | $2.07 | $+95.57 | $10,581.58 | ▲ +95.57 after sell → book $10,581.58; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,581.58 | ▲ close $10,581.58 vs 09:30 $10,588.00 (session +0.00) | 16:00 close · cash $10,581.58 · no lots left · equity $10,581.58. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,581.58 | ▲ 09:30 equity $10,581.58 vs yday $10,581.58 (+0.00) | 09:30 open · cash $10,581.58 · no holdings · equity $10,581.58 vs prior close $10,581.58 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,581.58 | ▲ close $10,581.58 vs 09:30 $10,581.58 (session +0.00) | 16:00 close · cash $10,581.58 · no lots left · equity $10,581.58. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,581.58 | ▲ 09:30 equity $10,581.58 vs yday $10,581.58 (+0.00) | 09:30 open · cash $10,581.58 · no holdings · equity $10,581.58 vs prior close $10,581.58 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 16 | $91.01 | $2.04 | — | $9,123.38 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1511.65 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 33 | $44.76 | $2.09 | — | $7,644.22 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1511.65 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 612 | $2.47 | $7.89 | — | $6,124.68 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1511.65 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 25 | $58.73 | $2.06 | — | $4,654.37 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1511.65 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 94 | $16.00 | $2.27 | — | $3,148.09 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1511.65 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 10 | $150.14 | $2.02 | — | $1,644.67 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1511.65 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 56 | $26.57 | $2.16 | — | $154.60 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1511.65 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $154.60 | ▼ close $10,382.61 vs 09:30 $10,581.58 (session -178.44) | 16:00 close · cash $154.60 · equity $10,382.61 vs 09:30 $10,581.58 (-198.97; session marks -178.44) · 7 name(s) marked open→close (per-name table). BHP×16 09:30 $91.01 → close $93.63 +41.92; APA×33 09:30 $44.76 → close $44.39 -12.21; AUTL×612 09:30 $2.47 → close $2.46 -6.12; CRSP×25 09:30 $58.73 → close $58.12 -15.25; ASST×94 09:30 $16.00 → close $16.13 +12.22; MRNA×10 09:30 $150.14 → close $133.32 -168.20; ZLAB×56 09:30 $26.57 → close $26.02 -30.80 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.60 | ▲ 09:30 equity $10,621.06 vs yday $10,382.61 (+238.45) | 09:30 open · cash $154.60 (unchanged overnight, no fees) · equity $10,621.06 vs prior close $10,382.61 (+238.45) · 7 name(s) re-marked at the open (per-name table). BHP×16 yday $93.63 → 09:30 $95.72 +33.44; APA×33 yday $44.39 → 09:30 $44.52 +4.29; AUTL×612 yday $2.46 → 09:30 $2.47 +6.12; CRSP×25 yday $58.12 → 09:30 $59.72 +40.00; ASST×94 yday $16.13 → 09:30 $17.66 +143.82; MRNA×10 yday $133.32 → 09:30 $133.11 -2.10; ZLAB×56 yday $26.02 → 09:30 $26.25 +12.88 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 16 | $95.72 | $2.06 | $+71.26 | $1,684.06 | ▲ +71.26 after sell → book $10,619.00; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 33 | $44.52 | $2.11 | $-12.12 | $3,151.10 | ▼ -12.12 after sell → book $10,616.88; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 94 | $17.66 | $2.30 | $+151.47 | $4,808.84 | ▲ +151.47 after sell → book $10,614.58; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 10 | $133.11 | $2.04 | $-174.36 | $6,137.90 | ▼ -174.36 after sell → book $10,612.54; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 56 | $26.25 | $2.18 | $-22.26 | $7,605.72 | ▼ -22.26 after sell → book $10,610.36; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,409.40 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1267.62 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $5,140.40 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1267.62 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 16 | $78.88 | $2.04 | — | $3,876.28 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1267.62 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 146 | $8.66 | $2.43 | — | $2,609.49 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1267.62 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 391 | $3.24 | $5.04 | — | $1,337.61 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $1267.62 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 108 | $11.70 | $2.31 | — | $71.70 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1267.62 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.70 | ▼ close $10,437.59 vs 09:30 $10,621.06 (session -156.91) | 16:00 close · cash $71.70 · equity $10,437.59 vs 09:30 $10,621.06 (-183.47; session marks -156.91) · 8 name(s) marked open→close (per-name table). AUTL×612 09:30 $2.47 → close $2.41 -36.72; CRSP×25 09:30 $59.72 → close $59.50 -5.50; AU×10 09:30 $119.43 → close $121.22 +17.90; FUTU×11 09:30 $115.18 → close $123.64 +93.06; GRAL×16 09:30 $78.88 → close $79.54 +10.56; ABTC×146 09:30 $8.66 → close $7.93 -106.58; HIVE×391 09:30 $3.24 → close $3.03 -82.11; MARA×108 09:30 $11.70 → close $11.26 -47.52 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.70 | ▼ 09:30 equity $10,398.72 vs yday $10,437.59 (-38.87) | 09:30 open · cash $71.70 (unchanged overnight, no fees) · equity $10,398.72 vs prior close $10,437.59 (-38.87) · 8 name(s) re-marked at the open (per-name table). AUTL×612 yday $2.41 → 09:30 $2.40 -6.12; CRSP×25 yday $59.50 → 09:30 $58.75 -18.75; AU×10 yday $121.22 → 09:30 $120.51 -7.10; FUTU×11 yday $123.64 → 09:30 $121.00 -29.04; GRAL×16 yday $79.54 → 09:30 $81.87 +37.28; ABTC×146 yday $7.93 → 09:30 $8.00 +10.22; HIVE×391 yday $3.03 → 09:30 $2.99 -15.64; MARA×108 yday $11.26 → 09:30 $11.17 -9.72 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 612 | $2.40 | $8.01 | $-58.74 | $1,532.49 | ▼ -58.74 after sell → book $10,390.71; vs 09:30 mark -8.01 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $2,735.55 | ▲ +6.74 after sell → book $10,388.67; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $121.00 | $2.04 | $+59.95 | $4,064.50 | ▲ +59.95 after sell → book $10,386.62; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 16 | $81.87 | $2.06 | $+43.74 | $5,372.37 | ▲ +43.74 after sell → book $10,384.57; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 146 | $8.00 | $2.46 | $-101.25 | $6,537.90 | ▼ -101.25 after sell → book $10,382.10; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 391 | $2.99 | $5.12 | $-107.91 | $7,701.88 | ▼ -107.91 after sell → book $10,376.99; vs 09:30 mark -5.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 108 | $11.17 | $2.34 | $-61.90 | $8,905.89 | ▼ -61.90 after sell → book $10,374.64; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,905.89 | ▼ close $10,332.77 vs 09:30 $10,398.72 (session -41.87) | 16:00 close · cash $8,905.89 · equity $10,332.77 vs 09:30 $10,398.72 (-65.95; session marks -41.87) · 1 name(s) marked open→close (per-name table). CRSP×25 09:30 $58.75 → close $57.08 -41.87 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,905.89 | ▲ 09:30 equity $10,354.14 vs yday $10,332.77 (+21.37) | 09:30 open · cash $8,905.89 (unchanged overnight, no fees) · equity $10,354.14 vs prior close $10,332.77 (+21.37) · 1 name(s) re-marked at the open (per-name table). CRSP×25 yday $57.08 → 09:30 $57.93 +21.37 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 25 | $57.93 | $2.09 | $-24.15 | $10,352.06 | ▼ -24.15 after sell → book $10,352.06; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 29 | $118.52 | $2.08 | — | $6,912.90 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $3450.69 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 44 | $77.13 | $2.12 | — | $3,517.06 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3450.69 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 98 | $35.05 | $2.28 | — | $79.87 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $3450.69 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.87 | ▲ close $10,626.76 vs 09:30 $10,354.14 (session +281.19) | 16:00 close · cash $79.87 · equity $10,626.76 vs 09:30 $10,354.14 (+272.62; session marks +281.19) · 3 name(s) marked open→close (per-name table). AU×29 09:30 $118.52 → close $123.39 +141.23; FCX×44 09:30 $77.13 → close $79.91 +122.32; EZPW×98 09:30 $35.05 → close $35.23 +17.64 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.87 | ▼ 09:30 equity $10,543.63 vs yday $10,626.76 (-83.13) | 09:30 open · cash $79.87 (unchanged overnight, no fees) · equity $10,543.63 vs prior close $10,626.76 (-83.13) · 3 name(s) re-marked at the open (per-name table). AU×29 yday $123.39 → 09:30 $119.80 -104.11; FCX×44 yday $79.91 → 09:30 $79.34 -25.08; EZPW×98 yday $35.23 → 09:30 $35.70 +46.06 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 29 | $119.80 | $2.11 | $+32.93 | $3,551.96 | ▲ +32.93 after sell → book $10,541.52; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 44 | $79.34 | $2.16 | $+92.96 | $7,040.76 | ▲ +92.96 after sell → book $10,539.36; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 98 | $35.70 | $2.33 | $+59.09 | $10,537.03 | ▲ +59.09 after sell → book $10,537.03; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 19 | $267.02 | $2.05 | — | $5,461.60 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $5268.52 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 44 | $118.50 | $2.12 | — | $245.48 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $5268.52 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $245.48 | ▼ close $10,526.31 vs 09:30 $10,543.63 (session -6.55) | 16:00 close · cash $245.48 · equity $10,526.31 vs 09:30 $10,543.63 (-17.32; session marks -6.55) · 2 name(s) marked open→close (per-name table). FNV×19 09:30 $267.02 → close $267.37 +6.65; CM×44 09:30 $118.50 → close $118.20 -13.20 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $245.48 | ▲ 09:30 equity $10,548.73 vs yday $10,526.31 (+22.42) | 09:30 open · cash $245.48 (unchanged overnight, no fees) · equity $10,548.73 vs prior close $10,526.31 (+22.42) · 2 name(s) re-marked at the open (per-name table). FNV×19 yday $267.37 → 09:30 $267.23 -2.66; CM×44 yday $118.20 → 09:30 $118.77 +25.08 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 19 | $267.23 | $2.10 | $-0.15 | $5,320.75 | ▼ -0.15 after sell → book $10,546.63; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 9 | $81.65 | $2.02 | — | $4,583.89 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $760.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 25 | $29.83 | $2.06 | — | $3,836.07 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+7.6; leftover $760.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 2 | $318.88 | $2.00 | — | $3,196.32 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $760.11 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 3 | $222.86 | $2.00 | — | $2,525.74 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $760.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 2 | $261.47 | $2.00 | — | $2,000.80 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list overnight,overnight_mega; 🔵; ret5=+1.4; leftover $760.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,000.80 | ▼ close $10,402.93 vs 09:30 $10,548.73 (session -133.63) | 16:00 close · cash $2,000.80 · equity $10,402.93 vs 09:30 $10,548.73 (-145.80; session marks -133.63) · 6 name(s) marked open→close (per-name table). CM×44 09:30 $118.77 → close $114.84 -172.92; ACMR×9 09:30 $81.65 → close $80.49 -10.44; GEN×25 09:30 $29.83 → close $30.50 +16.75; LRCX×2 09:30 $318.88 → close $318.58 -0.60; NVDA×3 09:30 $222.86 → close $227.98 +15.36; ADSK×2 09:30 $261.47 → close $270.58 +18.22 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,000.80 | ▲ 09:30 equity $10,406.23 vs yday $10,402.93 (+3.30) | 09:30 open · cash $2,000.80 (unchanged overnight, no fees) · equity $10,406.23 vs prior close $10,402.93 (+3.30) · 6 name(s) re-marked at the open (per-name table). CM×44 yday $114.84 → 09:30 $115.66 +36.08; ACMR×9 yday $80.49 → 09:30 $79.27 -10.98; GEN×25 yday $30.50 → 09:30 $30.50 +0.00; LRCX×2 yday $318.58 → 09:30 $318.03 -1.10; NVDA×3 yday $227.98 → 09:30 $227.36 -1.86; ADSK×2 yday $270.58 → 09:30 $261.16 -18.84 | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 44 | $115.66 | $2.17 | $-129.25 | $7,087.67 | ▼ -129.25 after sell → book $10,404.06; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 9 | $79.27 | $2.04 | $-25.47 | $7,799.06 | ▼ -25.47 after sell → book $10,402.02; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 25 | $30.50 | $2.08 | $+12.60 | $8,559.48 | ▲ +12.60 after sell → book $10,399.94; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 2 | $318.03 | $2.02 | $-5.71 | $9,193.52 | ▼ -5.71 after sell → book $10,397.92; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 3 | $227.36 | $2.02 | $+9.48 | $9,873.58 | ▲ +9.48 after sell → book $10,395.90; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $8,573.94 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1410.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $7,296.08 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1410.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,092.82 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1410.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $4,784.80 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1410.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $3,581.70 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1410.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 42 | $32.90 | $2.12 | — | $2,197.78 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1410.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 292 | $4.82 | $3.77 | — | $786.57 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1410.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $786.57 | ▼ close $10,062.68 vs 09:30 $10,406.23 (session -317.32) | 16:00 close · cash $786.57 · equity $10,062.68 vs 09:30 $10,406.23 (-343.55; session marks -317.32) · 8 name(s) marked open→close (per-name table). ADSK×2 09:30 $261.16 → close $260.66 -1.00; KEYS×4 09:30 $324.41 → close $319.97 -17.76; SMTC×9 09:30 $141.76 → close $131.17 -95.31; CIEN×3 09:30 $400.42 → close $378.44 -65.94; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×5 09:30 $240.22 → close $236.98 -16.20; SEDG×42 09:30 $32.90 → close $31.41 -62.58; TLS×292 09:30 $4.82 → close $4.79 -8.76 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $786.57 | ▼ 09:30 equity $10,062.52 vs yday $10,062.68 (-0.16) | 09:30 open · cash $786.57 (unchanged overnight, no fees) · equity $10,062.52 vs prior close $10,062.68 (-0.16) · 8 name(s) re-marked at the open (per-name table). ADSK×2 yday $260.66 → 09:30 $257.71 -5.90; KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×5 yday $236.98 → 09:30 $233.97 -15.07; SEDG×42 yday $31.41 → 09:30 $31.15 -10.92; TLS×292 yday $4.79 → 09:30 $4.81 +5.84 | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 2 | $257.71 | $2.02 | $-11.53 | $1,299.98 | ▼ -11.53 after sell → book $10,060.50; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $2,587.91 | ▼ -11.70 after sell → book $10,058.48; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $3,776.58 | ▼ -89.19 after sell → book $10,056.44; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $4,909.88 | ▼ -69.96 after sell → book $10,054.42; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $6,169.77 | ▼ -48.14 after sell → book $10,052.41; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 5 | $233.97 | $2.02 | $-35.31 | $7,337.57 | ▼ -35.31 after sell → book $10,050.39; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 42 | $31.15 | $2.14 | $-77.75 | $8,643.73 | ▼ -77.75 after sell → book $10,048.25; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 292 | $4.81 | $3.83 | $-10.51 | $10,044.42 | ▼ -10.51 after sell → book $10,044.42; vs 09:30 mark -3.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,044.42 | ▲ close $10,044.42 vs 09:30 $10,062.52 (session +0.00) | 16:00 close · cash $10,044.42 · no lots left · equity $10,044.42. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,044.42 | ▲ 09:30 equity $10,044.42 vs yday $10,044.42 (+0.00) | 09:30 open · cash $10,044.42 · no holdings · equity $10,044.42 vs prior close $10,044.42 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,044.42 | ▲ close $10,044.42 vs 09:30 $10,044.42 (session +0.00) | 16:00 close · cash $10,044.42 · no lots left · equity $10,044.42. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,044.42 | ▲ 09:30 equity $10,044.42 vs yday $10,044.42 (+0.00) | 09:30 open · cash $10,044.42 · no holdings · equity $10,044.42 vs prior close $10,044.42 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,044.42 | ▲ close $10,044.42 vs 09:30 $10,044.42 (session +0.00) | 16:00 close · cash $10,044.42 · no lots left · equity $10,044.42. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,044.42 | ▲ 09:30 equity $10,044.42 vs yday $10,044.42 (+0.00) | 09:30 open · cash $10,044.42 · no holdings · equity $10,044.42 vs prior close $10,044.42 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 4 | $351.74 | $2.00 | — | $8,635.46 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1434.92 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,660.84 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1434.92 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 44 | $32.31 | $2.12 | — | $6,237.08 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1434.92 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 90 | $15.87 | $2.26 | — | $4,806.52 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1434.92 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 60 | $23.88 | $2.17 | — | $3,371.55 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1434.92 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $1,963.06 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1434.92 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 30 | $47.60 | $2.08 | — | $532.98 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1434.92 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $532.98 | ▲ close $10,448.86 vs 09:30 $10,044.42 (session +419.06) | 16:00 close · cash $532.98 · equity $10,448.86 vs 09:30 $10,044.42 (+404.44; session marks +419.06) · 7 name(s) marked open→close (per-name table). AVGO×4 09:30 $351.74 → close $357.16 +21.68; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×44 09:30 $32.31 → close $33.66 +59.40; FRNM×90 09:30 $15.87 → close $16.90 +92.70; MMED×60 09:30 $23.88 → close $23.84 -2.40; DE×2 09:30 $703.25 → close $694.41 -17.68; HPE×30 09:30 $47.60 → close $54.44 +205.20 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $532.98 | ▼ 09:30 equity $10,377.54 vs yday $10,448.86 (-71.32) | 09:30 open · cash $532.98 (unchanged overnight, no fees) · equity $10,377.54 vs prior close $10,448.86 (-71.32) · 7 name(s) re-marked at the open (per-name table). AVGO×4 yday $357.16 → 09:30 $359.70 +10.16; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×44 yday $33.66 → 09:30 $33.46 -8.80; FRNM×90 yday $16.90 → 09:30 $16.40 -45.00; MMED×60 yday $23.84 → 09:30 $23.84 +0.00; DE×2 yday $694.41 → 09:30 $692.03 -4.76; HPE×30 yday $54.44 → 09:30 $53.85 -17.70 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 4 | $359.70 | $2.02 | $+27.81 | $1,969.75 | ▲ +27.81 after sell → book $10,375.51; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $2,995.30 | ▲ +50.93 after sell → book $10,373.50; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 44 | $33.46 | $2.14 | $+46.33 | $4,465.39 | ▲ +46.33 after sell → book $10,371.35; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 60 | $23.84 | $2.19 | $-6.76 | $5,893.60 | ▼ -6.76 after sell → book $10,369.16; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 2 | $692.03 | $2.02 | $-26.45 | $7,275.64 | ▼ -26.45 after sell → book $10,367.14; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 30 | $53.85 | $2.10 | $+183.32 | $8,889.04 | ▲ +183.32 after sell → book $10,365.04; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 8 | $263.36 | $2.01 | — | $6,780.15 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $2222.26 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 29 | $75.65 | $2.08 | — | $4,584.22 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $2222.26 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 9 | $236.82 | $2.02 | — | $2,450.82 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+8.1; leftover $2222.26 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 1145 | $1.94 | $14.77 | — | $214.75 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $2222.26 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $214.75 | ▲ close $10,466.20 vs 09:30 $10,377.54 (session +122.04) | 16:00 close · cash $214.75 · equity $10,466.20 vs 09:30 $10,377.54 (+88.66; session marks +122.04) · 5 name(s) marked open→close (per-name table). FRNM×90 09:30 $16.40 → close $16.31 -8.10; CRM×8 09:30 $263.36 → close $259.23 -33.04; MRX×29 09:30 $75.65 → close $78.27 +75.98; BE×9 09:30 $236.82 → close $252.87 +144.45; BAK×1145 09:30 $1.94 → close $1.89 -57.25 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $214.75 | ▲ 09:30 equity $10,668.61 vs yday $10,466.20 (+202.41) | 09:30 open · cash $214.75 (unchanged overnight, no fees) · equity $10,668.61 vs prior close $10,466.20 (+202.41) · 5 name(s) re-marked at the open (per-name table). FRNM×90 yday $16.31 → 09:30 $16.74 +38.70; CRM×8 yday $259.23 → 09:30 $253.72 -44.08; MRX×29 yday $78.27 → 09:30 $78.84 +16.53; BE×9 yday $252.87 → 09:30 $267.76 +134.01; BAK×1145 yday $1.89 → 09:30 $1.94 +57.25 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 90 | $16.74 | $2.29 | $+73.75 | $1,719.07 | ▲ +73.75 after sell → book $10,666.33; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 8 | $253.72 | $2.04 | $-81.17 | $3,746.79 | ▼ -81.17 after sell → book $10,664.29; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 29 | $78.84 | $2.11 | $+88.33 | $6,031.04 | ▲ +88.33 after sell → book $10,662.18; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 9 | $267.76 | $2.05 | $+274.40 | $8,438.83 | ▲ +274.40 after sell → book $10,660.13; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 1145 | $1.94 | $14.98 | $-29.75 | $10,645.16 | ▼ -29.75 after sell → book $10,645.16; vs 09:30 mark -14.97 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,645.16 | ▲ close $10,645.16 vs 09:30 $10,668.61 (session +0.00) | 16:00 close · cash $10,645.16 · no lots left · equity $10,645.16. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,645.16 | ▲ 09:30 equity $10,645.16 vs yday $10,645.16 (-0.00) | 09:30 open · cash $10,645.16 · no holdings · equity $10,645.16 vs prior close $10,645.16 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,645.16 | ▲ close $10,645.16 vs 09:30 $10,645.16 (session +0.00) | 16:00 close · cash $10,645.16 · no lots left · equity $10,645.16. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,645.16 | ▲ 09:30 equity $10,645.16 vs yday $10,645.16 (-0.00) | 09:30 open · cash $10,645.16 · no holdings · equity $10,645.16 vs prior close $10,645.16 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,645.16 | ▲ close $10,645.16 vs 09:30 $10,645.16 (session +0.00) | 16:00 close · cash $10,645.16 · no lots left · equity $10,645.16. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,645.16 | ▲ 09:30 equity $10,645.16 vs yday $10,645.16 (-0.00) | 09:30 open · cash $10,645.16 · no holdings · equity $10,645.16 vs prior close $10,645.16 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 64 | $164.43 | $2.18 | — | $119.45 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $10645.16 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $119.45 | ▼ close $9,737.37 vs 09:30 $10,645.16 (session -905.60) | 16:00 close · cash $119.45 · equity $9,737.37 vs 09:30 $10,645.16 (-907.79; session marks -905.60) · 1 name(s) marked open→close (per-name table). ORCL×64 09:30 $164.43 → close $150.28 -905.60 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $119.45 | ▼ 09:30 equity $9,170.33 vs yday $9,737.37 (-567.04) | 09:30 open · cash $119.45 (unchanged overnight, no fees) · equity $9,170.33 vs prior close $9,737.37 (-567.04) · 1 name(s) re-marked at the open (per-name table). ORCL×64 yday $150.28 → 09:30 $141.42 -567.04 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 64 | $141.42 | $2.27 | $-1477.09 | $9,168.07 | ▼ -1,477.09 after sell → book $9,168.07; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,168.07 | ▲ close $9,168.07 vs 09:30 $9,170.33 (session +0.00) | 16:00 close · cash $9,168.07 · no lots left · equity $9,168.07. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,168.07 | ▲ 09:30 equity $9,168.07 vs yday $9,168.07 (-0.00) | 09:30 open · cash $9,168.07 · no holdings · equity $9,168.07 vs prior close $9,168.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,168.07 | ▲ close $9,168.07 vs 09:30 $9,168.07 (session +0.00) | 16:00 close · cash $9,168.07 · no lots left · equity $9,168.07. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,168.07 | ▲ 09:30 equity $9,168.07 vs yday $9,168.07 (-0.00) | 09:30 open · cash $9,168.07 · no holdings · equity $9,168.07 vs prior close $9,168.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 116 | $26.27 | $2.34 | — | $6,118.41 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $3056.02 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 16 | $189.17 | $2.04 | — | $3,089.65 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $3056.02 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 76 | $39.99 | $2.22 | — | $48.19 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $3056.02 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.19 | ▼ close $8,990.23 vs 09:30 $9,168.07 (session -171.24) | 16:00 close · cash $48.19 · equity $8,990.23 vs 09:30 $9,168.07 (-177.84; session marks -171.24) · 3 name(s) marked open→close (per-name table). WAY×116 09:30 $26.27 → close $26.59 +37.12; QCOM×16 09:30 $189.17 → close $184.84 -69.28; SM×76 09:30 $39.99 → close $38.16 -139.08 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.19 | ▲ 09:30 equity $9,024.27 vs yday $8,990.23 (+34.04) | 09:30 open · cash $48.19 (unchanged overnight, no fees) · equity $9,024.27 vs prior close $8,990.23 (+34.04) · 3 name(s) re-marked at the open (per-name table). WAY×116 yday $26.59 → 09:30 $26.51 -9.28; QCOM×16 yday $184.84 → 09:30 $190.35 +88.16; SM×76 yday $38.16 → 09:30 $37.57 -44.84 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 116 | $26.51 | $2.38 | $+23.12 | $3,120.97 | ▲ +23.12 after sell → book $9,021.89; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 16 | $190.35 | $2.07 | $+14.77 | $6,164.50 | ▲ +14.77 after sell → book $9,019.82; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 76 | $37.57 | $2.25 | $-188.39 | $9,017.57 | ▼ -188.39 after sell → book $9,017.57; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 13 | $170.85 | $2.03 | — | $6,794.49 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $2254.39 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `CLS` | 6 | $337.75 | $2.01 | — | $4,765.98 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+10.2; leftover $2254.39 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 101 | $22.12 | $2.29 | — | $2,529.57 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $2254.39 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 9 | $238.60 | $2.02 | — | $380.15 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_mover; ret5=-11.6; leftover $2254.39 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $380.15 | ▲ close $9,107.23 vs 09:30 $9,024.27 (session +98.01) | 16:00 close · cash $380.15 · equity $9,107.23 vs 09:30 $9,024.27 (+82.96; session marks +98.01) · 4 name(s) marked open→close (per-name table). SMTC×13 09:30 $170.85 → close $178.19 +95.42; CLS×6 09:30 $337.75 → close $329.94 -46.86; GME×101 09:30 $22.12 → close $22.77 +65.65; JBHT×9 09:30 $238.60 → close $236.80 -16.20 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $380.15 | ▲ 09:30 equity $9,186.90 vs yday $9,107.23 (+79.67) | 09:30 open · cash $380.15 (unchanged overnight, no fees) · equity $9,186.90 vs prior close $9,107.23 (+79.67) · 4 name(s) re-marked at the open (per-name table). SMTC×13 yday $178.19 → 09:30 $182.33 +53.82; CLS×6 yday $329.94 → 09:30 $332.06 +12.72; GME×101 yday $22.77 → 09:30 $22.90 +13.13; JBHT×9 yday $236.80 → 09:30 $236.80 +0.00 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 13 | $182.33 | $2.06 | $+145.15 | $2,748.38 | ▲ +145.15 after sell → book $9,184.84; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 9 | $236.80 | $2.04 | $-20.26 | $4,877.54 | ▼ -20.26 after sell → book $9,182.80; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 77 | $20.91 | $2.22 | — | $3,265.25 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1625.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 109 | $14.79 | $2.32 | — | $1,650.82 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1625.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 115 | $14.07 | $2.33 | — | $30.44 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1625.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.44 | ▼ close $9,092.38 vs 09:30 $9,186.90 (session -83.55) | 16:00 close · cash $30.44 · equity $9,092.38 vs 09:30 $9,186.90 (-94.52; session marks -83.55) · 5 name(s) marked open→close (per-name table). CLS×6 09:30 $332.06 → close $332.63 +3.42; GME×101 09:30 $22.90 → close $22.64 -26.26; TH×77 09:30 $20.91 → close $21.19 +21.56; RARE×109 09:30 $14.79 → close $14.51 -30.52; BHVN×115 09:30 $14.07 → close $13.62 -51.75 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.44 | ▲ 09:30 equity $9,229.17 vs yday $9,092.38 (+136.79) | 09:30 open · cash $30.44 (unchanged overnight, no fees) · equity $9,229.17 vs prior close $9,092.38 (+136.79) · 5 name(s) re-marked at the open (per-name table). CLS×6 yday $332.63 → 09:30 $341.45 +52.92; GME×101 yday $22.64 → 09:30 $22.78 +14.14; TH×77 yday $21.19 → 09:30 $21.55 +27.72; RARE×109 yday $14.51 → 09:30 $14.60 +9.81; BHVN×115 yday $13.62 → 09:30 $13.90 +32.20 | — |
| 2026-09-21 09:30 ET | **SELL** | `CLS` | 6 | $341.45 | $2.03 | $+18.16 | $2,077.10 | ▲ +18.16 after sell → book $9,227.13; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 101 | $22.78 | $2.33 | $+62.04 | $4,375.55 | ▲ +62.04 after sell → book $9,224.80; vs 09:30 mark -2.33 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 77 | $21.55 | $2.25 | $+44.81 | $6,032.66 | ▲ +44.81 after sell → book $9,222.56; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 109 | $14.60 | $2.35 | $-25.37 | $7,621.71 | ▼ -25.37 after sell → book $9,220.21; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 115 | $13.90 | $2.37 | $-24.25 | $9,217.84 | ▼ -24.25 after sell → book $9,217.84; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 10 | $230.25 | $2.02 | — | $6,913.32 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+12.5; leftover $2304.46 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 12 | $190.30 | $2.03 | — | $4,627.69 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+10.6; leftover $2304.46 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `ALVO` | 389 | $5.92 | $5.02 | — | $2,319.80 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+11.0; leftover $2304.46 | join🔴 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 384 | $5.99 | $4.95 | — | $14.68 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_mover; 🔵; ret5=-24.1; leftover $2304.46 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.68 | ▼ close $8,973.44 vs 09:30 $9,229.17 (session -230.38) | 16:00 close · cash $14.68 · equity $8,973.44 vs 09:30 $9,229.17 (-255.73; session marks -230.38) · 4 name(s) marked open→close (per-name table). VICR×10 09:30 $230.25 → close $223.90 -63.50; SMTC×12 09:30 $190.30 → close $177.37 -155.16; ALVO×389 09:30 $5.92 → close $5.88 -15.56; SION×384 09:30 $5.99 → close $6.00 +3.84 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `MU` | cash | leftover split 760.11 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 760.11 < 1 share @ 1746.53 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `VICR` | 10 | 2026-09-21 @ $230.25 | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+12.5; leftover $2304.46 |
| `SMTC` | 12 | 2026-09-21 @ $190.30 | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+10.6; leftover $2304.46 |
| `ALVO` | 389 | 2026-09-21 @ $5.92 | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+11.0; leftover $2304.46 |
| `SION` | 384 | 2026-09-21 @ $5.99 | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_mover; 🔵; ret5=-24.1; leftover $2304.46 |
