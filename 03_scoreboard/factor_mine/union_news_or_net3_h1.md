# Factor mine action — `union_news_or_net3_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 3

Cash book **-16.09%** ($8,391) · signal-only (no cash/fees) was -7.86%. Starts YES **0/27**. Fills 134 · skips 29 · realized $-1381.34.

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
- Must-have: camera net (+G −R) is at least 3.
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
- **Gate** `news_or_headline=True,cam_net_min=3` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $136.83.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `HLIT` | 94 | — | $13.18 | +0.00 | $13.92 | +69.56 | +69.56 | +0.00 | +69.56 |
| 2026-08-14 | `ANGX` | 290 | — | $4.31 | +0.00 | $4.37 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-08-14 | `ARX` | 63 | — | $19.57 | +0.00 | $19.58 | +0.63 | +0.63 | +0.00 | +0.63 |
| 2026-08-14 | `MH` | 92 | — | $13.55 | +0.00 | $13.10 | -41.40 | -41.40 | +0.00 | -41.40 |
| 2026-08-14 | `VELO` | 81 | — | $15.38 | +0.00 | $16.16 | +63.18 | +63.18 | +0.00 | +63.18 |
| 2026-08-14 | `NRG` | 10 | — | $120.00 | +0.00 | $126.24 | +62.40 | +62.40 | +0.00 | +62.40 |
| 2026-08-14 | `S` | 52 | — | $23.77 | +0.00 | $23.11 | -34.58 | -34.58 | +0.00 | -34.58 |
| 2026-08-17 | `HLIT` | 94 | $13.92 | $13.84 | -7.52 | — | +0.00 | -7.52 | +62.04 | — |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | — | +0.00 | +66.70 | +84.10 | — |
| 2026-08-17 | `ARX` | 63 | $19.58 | $19.57 | -0.63 | — | +0.00 | -0.63 | +0.00 | — |
| 2026-08-17 | `MH` | 92 | $13.10 | $13.16 | +5.52 | — | +0.00 | +5.52 | -35.88 | — |
| 2026-08-17 | `VELO` | 81 | $16.16 | $16.05 | -8.91 | — | +0.00 | -8.91 | +54.27 | — |
| 2026-08-17 | `NRG` | 10 | $126.24 | $127.40 | +11.60 | — | +0.00 | +11.60 | +74.00 | — |
| 2026-08-17 | `S` | 52 | $23.11 | $22.50 | -31.72 | — | +0.00 | -31.72 | -66.30 | — |
| 2026-08-17 | `DVN` | 54 | — | $46.18 | +0.00 | $47.57 | +75.06 | +75.06 | +0.00 | +75.06 |
| 2026-08-17 | `EOG` | 17 | — | $142.77 | +0.00 | $146.15 | +57.46 | +57.46 | +0.00 | +57.46 |
| 2026-08-17 | `FANG` | 12 | — | $202.70 | +0.00 | $206.29 | +43.08 | +43.08 | +0.00 | +43.08 |
| 2026-08-17 | `OUST` | 51 | — | $49.00 | +0.00 | $48.13 | -44.37 | -44.37 | +0.00 | -44.37 |
| 2026-08-18 | `DVN` | 54 | $47.57 | $48.00 | +23.22 | — | +0.00 | +23.22 | +98.28 | — |
| 2026-08-18 | `EOG` | 17 | $146.15 | $148.04 | +32.13 | — | +0.00 | +32.13 | +89.59 | — |
| 2026-08-18 | `FANG` | 12 | $206.29 | $208.93 | +31.68 | — | +0.00 | +31.68 | +74.76 | — |
| 2026-08-18 | `OUST` | 51 | $48.13 | $45.09 | -155.04 | — | +0.00 | -155.04 | -199.41 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `APA` | 28 | — | $44.76 | +0.00 | $44.39 | -10.36 | -10.36 | +0.00 | -10.36 |
| 2026-08-20 | `AUTL` | 515 | — | $2.47 | +0.00 | $2.46 | -5.15 | -5.15 | +0.00 | -5.15 |
| 2026-08-20 | `CRSP` | 21 | — | $58.73 | +0.00 | $58.12 | -12.81 | -12.81 | +0.00 | -12.81 |
| 2026-08-20 | `ASST` | 79 | — | $16.00 | +0.00 | $16.13 | +10.27 | +10.27 | +0.00 | +10.27 |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `ZLAB` | 47 | — | $26.57 | +0.00 | $26.02 | -25.85 | -25.85 | +0.00 | -25.85 |
| 2026-08-20 | `HUMA` | 1800 | — | $0.71 | +0.00 | $0.68 | -46.80 | -46.80 | +0.00 | -46.80 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `APA` | 28 | $44.39 | $44.52 | +3.64 | — | +0.00 | +3.64 | -6.72 | — |
| 2026-08-21 | `AUTL` | 515 | $2.46 | $2.47 | +5.15 | $2.41 | -30.90 | -25.75 | +0.00 | -30.90 |
| 2026-08-21 | `CRSP` | 21 | $58.12 | $59.72 | +33.60 | $59.50 | -4.62 | +28.98 | +20.79 | +16.17 |
| 2026-08-21 | `ASST` | 79 | $16.13 | $17.66 | +120.87 | — | +0.00 | +120.87 | +131.14 | — |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | — | +0.00 | -1.68 | -136.24 | — |
| 2026-08-21 | `ZLAB` | 47 | $26.02 | $26.25 | +10.81 | — | +0.00 | +10.81 | -15.04 | — |
| 2026-08-21 | `HUMA` | 1800 | $0.68 | $0.67 | -12.60 | — | +0.00 | -12.60 | -59.40 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `FUTU` | 10 | — | $115.18 | +0.00 | $123.64 | +84.60 | +84.60 | +0.00 | +84.60 |
| 2026-08-21 | `GRAL` | 16 | — | $78.88 | +0.00 | $79.54 | +10.56 | +10.56 | +0.00 | +10.56 |
| 2026-08-21 | `ABTC` | 146 | — | $8.66 | +0.00 | $7.93 | -106.58 | -106.58 | +0.00 | -106.58 |
| 2026-08-21 | `HIVE` | 390 | — | $3.24 | +0.00 | $3.03 | -81.90 | -81.90 | +0.00 | -81.90 |
| 2026-08-21 | `MARA` | 108 | — | $11.70 | +0.00 | $11.26 | -47.52 | -47.52 | +0.00 | -47.52 |
| 2026-08-24 | `AUTL` | 515 | $2.41 | $2.40 | -5.15 | — | +0.00 | -5.15 | -36.05 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | $57.08 | -35.17 | -50.92 | +0.42 | -34.75 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `FUTU` | 10 | $123.64 | $121.00 | -26.40 | — | +0.00 | -26.40 | +58.20 | — |
| 2026-08-24 | `GRAL` | 16 | $79.54 | $81.87 | +37.28 | — | +0.00 | +37.28 | +47.84 | — |
| 2026-08-24 | `ABTC` | 146 | $7.93 | $8.00 | +10.22 | — | +0.00 | +10.22 | -96.36 | — |
| 2026-08-24 | `HIVE` | 390 | $3.03 | $2.99 | -15.60 | — | +0.00 | -15.60 | -97.50 | — |
| 2026-08-24 | `MARA` | 108 | $11.26 | $11.17 | -9.72 | — | +0.00 | -9.72 | -57.24 | — |
| 2026-08-25 | `CRSP` | 21 | $57.08 | $57.93 | +17.95 | — | +0.00 | +17.95 | -16.80 | — |
| 2026-08-25 | `AU` | 27 | — | $118.52 | +0.00 | $123.39 | +131.49 | +131.49 | +0.00 | +131.49 |
| 2026-08-25 | `FCX` | 42 | — | $77.13 | +0.00 | $79.91 | +116.76 | +116.76 | +0.00 | +116.76 |
| 2026-08-25 | `EZPW` | 93 | — | $35.05 | +0.00 | $35.23 | +16.74 | +16.74 | +0.00 | +16.74 |
| 2026-08-26 | `AU` | 27 | $123.39 | $119.80 | -96.93 | — | +0.00 | -96.93 | +34.56 | — |
| 2026-08-26 | `FCX` | 42 | $79.91 | $79.34 | -23.94 | — | +0.00 | -23.94 | +92.82 | — |
| 2026-08-26 | `EZPW` | 93 | $35.23 | $35.70 | +43.71 | — | +0.00 | +43.71 | +60.45 | — |
| 2026-08-26 | `FNV` | 18 | — | $267.02 | +0.00 | $267.37 | +6.30 | +6.30 | +0.00 | +6.30 |
| 2026-08-26 | `CM` | 42 | — | $118.50 | +0.00 | $118.20 | -12.60 | -12.60 | +0.00 | -12.60 |
| 2026-08-27 | `FNV` | 18 | $267.37 | $267.23 | -2.52 | — | +0.00 | -2.52 | +3.78 | — |
| 2026-08-27 | `CM` | 42 | $118.20 | $118.77 | +23.94 | $114.84 | -165.06 | -141.12 | +11.34 | -153.72 |
| 2026-08-27 | `ACMR` | 8 | — | $81.65 | +0.00 | $80.49 | -9.28 | -9.28 | +0.00 | -9.28 |
| 2026-08-27 | `GEN` | 24 | — | $29.83 | +0.00 | $30.50 | +16.08 | +16.08 | +0.00 | +16.08 |
| 2026-08-27 | `LRCX` | 2 | — | $318.88 | +0.00 | $318.58 | -0.60 | -0.60 | +0.00 | -0.60 |
| 2026-08-27 | `NVDA` | 3 | — | $222.86 | +0.00 | $227.98 | +15.36 | +15.36 | +0.00 | +15.36 |
| 2026-08-27 | `ADSK` | 2 | — | $261.47 | +0.00 | $270.58 | +18.22 | +18.22 | +0.00 | +18.22 |
| 2026-08-28 | `CM` | 42 | $114.84 | $115.66 | +34.44 | — | +0.00 | +34.44 | -119.28 | — |
| 2026-08-28 | `ACMR` | 8 | $80.49 | $79.27 | -9.76 | — | +0.00 | -9.76 | -19.04 | — |
| 2026-08-28 | `GEN` | 24 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +16.08 | — |
| 2026-08-28 | `LRCX` | 2 | $318.58 | $318.03 | -1.10 | — | +0.00 | -1.10 | -1.70 | — |
| 2026-08-28 | `NVDA` | 3 | $227.98 | $227.36 | -1.86 | — | +0.00 | -1.86 | +13.50 | — |
| 2026-08-28 | `ADSK` | 2 | $270.58 | $261.16 | -18.84 | $260.66 | -1.00 | -19.84 | -0.62 | -1.62 |
| 2026-08-28 | `KEYS` | 4 | — | $324.41 | +0.00 | $319.97 | -17.76 | -17.76 | +0.00 | -17.76 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 5 | — | $240.22 | +0.00 | $236.98 | -16.20 | -16.20 | +0.00 | -16.20 |
| 2026-08-28 | `SEDG` | 40 | — | $32.90 | +0.00 | $31.41 | -59.60 | -59.60 | +0.00 | -59.60 |
| 2026-08-28 | `TLS` | 278 | — | $4.82 | +0.00 | $4.79 | -8.34 | -8.34 | +0.00 | -8.34 |
| 2026-08-31 | `ADSK` | 2 | $260.66 | $257.71 | -5.90 | — | +0.00 | -5.90 | -7.52 | — |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | — | +0.00 | +10.08 | -7.68 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -65.94 | — |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | — | +0.00 | +5.64 | -44.13 | — |
| 2026-08-31 | `DDOG` | 5 | $236.98 | $233.97 | -15.07 | — | +0.00 | -15.07 | -31.27 | — |
| 2026-08-31 | `SEDG` | 40 | $31.41 | $31.15 | -10.40 | — | +0.00 | -10.40 | -70.00 | — |
| 2026-08-31 | `TLS` | 278 | $4.79 | $4.81 | +5.56 | — | +0.00 | +5.56 | -2.78 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 3 | — | $351.74 | +0.00 | $357.16 | +16.26 | +16.26 | +0.00 | +16.26 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `CXW` | 42 | — | $32.31 | +0.00 | $33.66 | +56.70 | +56.70 | +0.00 | +56.70 |
| 2026-09-03 | `FRNM` | 86 | — | $15.87 | +0.00 | $16.90 | +88.58 | +88.58 | +0.00 | +88.58 |
| 2026-09-03 | `MMED` | 57 | — | $23.88 | +0.00 | $23.84 | -2.28 | -2.28 | +0.00 | -2.28 |
| 2026-09-03 | `DE` | 1 | — | $703.25 | +0.00 | $694.41 | -8.84 | -8.84 | +0.00 | -8.84 |
| 2026-09-03 | `HPE` | 28 | — | $47.60 | +0.00 | $54.44 | +191.52 | +191.52 | +0.00 | +191.52 |
| 2026-09-04 | `AVGO` | 3 | $357.16 | $359.70 | +7.62 | — | +0.00 | +7.62 | +23.88 | — |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | — | +0.00 | -5.22 | +54.94 | — |
| 2026-09-04 | `CXW` | 42 | $33.66 | $33.46 | -8.40 | — | +0.00 | -8.40 | +48.30 | — |
| 2026-09-04 | `FRNM` | 86 | $16.90 | $16.40 | -43.00 | $16.31 | -7.74 | -50.74 | +45.58 | +37.84 |
| 2026-09-04 | `MMED` | 57 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.28 | — |
| 2026-09-04 | `DE` | 1 | $694.41 | $692.03 | -2.38 | — | +0.00 | -2.38 | -11.22 | — |
| 2026-09-04 | `HPE` | 28 | $54.44 | $53.85 | -16.52 | — | +0.00 | -16.52 | +175.00 | — |
| 2026-09-04 | `CRM` | 8 | — | $263.36 | +0.00 | $259.23 | -33.04 | -33.04 | +0.00 | -33.04 |
| 2026-09-04 | `MRX` | 27 | — | $75.65 | +0.00 | $78.27 | +70.74 | +70.74 | +0.00 | +70.74 |
| 2026-09-04 | `BE` | 8 | — | $236.82 | +0.00 | $252.87 | +128.40 | +128.40 | +0.00 | +128.40 |
| 2026-09-04 | `BAK` | 1089 | — | $1.94 | +0.00 | $1.89 | -54.45 | -54.45 | +0.00 | -54.45 |
| 2026-09-08 | `FRNM` | 86 | $16.31 | $16.74 | +36.98 | — | +0.00 | +36.98 | +74.82 | — |
| 2026-09-08 | `CRM` | 8 | $259.23 | $253.72 | -44.08 | — | +0.00 | -44.08 | -77.12 | — |
| 2026-09-08 | `MRX` | 27 | $78.27 | $78.84 | +15.39 | $76.71 | -57.51 | -42.12 | +86.13 | +28.62 |
| 2026-09-08 | `BE` | 8 | $252.87 | $267.76 | +119.12 | — | +0.00 | +119.12 | +247.52 | — |
| 2026-09-08 | `BAK` | 1089 | $1.89 | $1.94 | +54.45 | — | +0.00 | +54.45 | +0.00 | — |
| 2026-09-09 | `MRX` | 27 | $76.71 | $76.60 | -2.97 | — | +0.00 | -2.97 | +25.65 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 61 | — | $164.43 | +0.00 | $150.28 | -863.15 | -863.15 | +0.00 | -863.15 |
| 2026-09-14 | `ORCL` | 61 | $150.28 | $141.42 | -540.46 | — | +0.00 | -540.46 | -1403.61 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 109 | — | $26.27 | +0.00 | $26.59 | +34.88 | +34.88 | +0.00 | +34.88 |
| 2026-09-16 | `QCOM` | 15 | — | $189.17 | +0.00 | $184.84 | -64.95 | -64.95 | +0.00 | -64.95 |
| 2026-09-16 | `SM` | 72 | — | $39.99 | +0.00 | $38.16 | -131.76 | -131.76 | +0.00 | -131.76 |
| 2026-09-17 | `WAY` | 109 | $26.59 | $26.51 | -8.72 | — | +0.00 | -8.72 | +26.16 | — |
| 2026-09-17 | `QCOM` | 15 | $184.84 | $190.35 | +82.65 | — | +0.00 | +82.65 | +17.70 | — |
| 2026-09-17 | `SM` | 72 | $38.16 | $37.57 | -42.48 | — | +0.00 | -42.48 | -174.24 | — |
| 2026-09-17 | `SMTC` | 9 | — | $170.85 | +0.00 | $178.19 | +66.06 | +66.06 | +0.00 | +66.06 |
| 2026-09-17 | `CLS` | 5 | — | $337.75 | +0.00 | $329.94 | -39.05 | -39.05 | +0.00 | -39.05 |
| 2026-09-17 | `GME` | 76 | — | $22.12 | +0.00 | $22.77 | +49.40 | +49.40 | +0.00 | +49.40 |
| 2026-09-17 | `JBHT` | 7 | — | $238.60 | +0.00 | $236.80 | -12.60 | -12.60 | +0.00 | -12.60 |
| 2026-09-17 | `LITE` | 1 | — | $934.88 | +0.00 | $893.61 | -41.27 | -41.27 | +0.00 | -41.27 |
| 2026-09-18 | `SMTC` | 9 | $178.19 | $182.33 | +37.26 | — | +0.00 | +37.26 | +103.32 | — |
| 2026-09-18 | `CLS` | 5 | $329.94 | $332.06 | +10.60 | $332.63 | +2.85 | +13.45 | -28.45 | -25.60 |
| 2026-09-18 | `GME` | 76 | $22.77 | $22.90 | +9.88 | $22.64 | -19.76 | -9.88 | +59.28 | +39.52 |
| 2026-09-18 | `JBHT` | 7 | $236.80 | $236.80 | +0.00 | — | +0.00 | +0.00 | -12.60 | — |
| 2026-09-18 | `LITE` | 1 | $893.61 | $915.66 | +22.05 | — | +0.00 | +22.05 | -19.22 | — |
| 2026-09-18 | `TH` | 82 | — | $20.91 | +0.00 | $21.19 | +22.96 | +22.96 | +0.00 | +22.96 |
| 2026-09-18 | `RARE` | 116 | — | $14.79 | +0.00 | $14.51 | -32.48 | -32.48 | +0.00 | -32.48 |
| 2026-09-18 | `BHVN` | 122 | — | $14.07 | +0.00 | $13.62 | -54.90 | -54.90 | +0.00 | -54.90 |
| 2026-09-21 | `CLS` | 5 | $332.63 | $341.45 | +44.10 | — | +0.00 | +44.10 | +18.50 | — |
| 2026-09-21 | `GME` | 76 | $22.64 | $22.78 | +10.64 | — | +0.00 | +10.64 | +50.16 | — |
| 2026-09-21 | `TH` | 82 | $21.19 | $21.65 | +37.72 | — | +0.00 | +37.72 | +60.68 | — |
| 2026-09-21 | `RARE` | 116 | $14.51 | $14.58 | +8.12 | — | +0.00 | +8.12 | -24.36 | — |
| 2026-09-21 | `BHVN` | 122 | $13.62 | $13.90 | +34.16 | — | +0.00 | +34.16 | -20.74 | — |
| 2026-09-21 | `VICR` | 9 | — | $230.25 | +0.00 | $223.90 | -57.15 | -57.15 | +0.00 | -57.15 |
| 2026-09-21 | `SMTC` | 11 | — | $190.30 | +0.00 | $177.37 | -142.23 | -142.23 | +0.00 | -142.23 |
| 2026-09-21 | `ALVO` | 363 | — | $5.92 | +0.00 | $5.88 | -14.52 | -14.52 | +0.00 | -14.52 |
| 2026-09-21 | `SION` | 359 | — | $6.00 | +0.00 | $6.00 | +0.00 | +0.00 | +0.00 | +0.00 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +137.19 | HLIT, ANGX, ARX, MH, VELO, NRG, S | — | $1,332.73 | $10,120.33 | HLIT×94, ANGX×290, ARX×63, MH×92, VELO×81, NRG×10, S×52 |
| 2026-08-17 | +2.25 | $1,332.73 | HLIT×94, ANGX×290, ARX×63, MH×92, VELO×81, NRG×10, S×52 | $10,155.37 | +35.04 | +131.23 | DVN, EOG, FANG, OUST | HLIT, ANGX, ARX, MH, VELO, NRG, S | $277.75 | $10,261.19 | DVN×54, EOG×17, FANG×12, OUST×51 |
| 2026-08-18 | -6.20 | $277.75 | DVN×54, EOG×17, FANG×12, OUST×51 | $10,193.18 | -68.01 | +0.00 | — | DVN, EOG, FANG, OUST | $10,184.70 | $10,184.70 | — |
| 2026-08-19 | -7.20 | $10,184.70 | — | $10,184.70 | -0.00 | +0.00 | — | — | $10,184.70 | $10,184.70 | — |
| 2026-08-20 | +1.12 | $10,184.70 | — | $10,184.70 | -0.00 | -191.20 | BHP, APA, AUTL, CRSP, ASST, MRNA, ZLAB, HUMA | — | $219.10 | $9,956.20 | BHP×13, APA×28, AUTL×515, CRSP×21, ASST×79, MRNA×8, ZLAB×47, HUMA×1800 |
| 2026-08-21 | +3.25 | $219.10 | BHP×13, APA×28, AUTL×515, CRSP×21, ASST×79, MRNA×8, ZLAB×47, HUMA×1800 | $10,143.16 | +186.96 | -158.46 | AU, FUTU, GRAL, ABTC, HIVE, MARA | BHP, APA, ASST, MRNA, ZLAB, HUMA | $172.98 | $9,940.43 | AUTL×515, CRSP×21, AU×10, FUTU×10, GRAL×16, ABTC×146, HIVE×390, MARA×108 |
| 2026-08-24 | -5.17 | $172.98 | AUTL×515, CRSP×21, AU×10, FUTU×10, GRAL×16, ABTC×146, HIVE×390, MARA×108 | $9,908.21 | -32.22 | -35.17 | — | AUTL, AU, FUTU, GRAL, ABTC, HIVE, MARA | $8,651.67 | $9,850.25 | CRSP×21 |
| 2026-08-25 | +1.80 | $8,651.67 | CRSP×21 | $9,868.20 | +17.95 | +264.99 | AU, FCX, EZPW | CRSP | $160.52 | $10,124.66 | AU×27, FCX×42, EZPW×93 |
| 2026-08-26 | +2.02 | $160.52 | AU×27, FCX×42, EZPW×93 | $10,047.50 | -77.16 | -6.30 | FNV, CM | AU, FCX, EZPW | $253.41 | $10,030.47 | FNV×18, CM×42 |
| 2026-08-27 | — | $253.41 | FNV×18, CM×42 | $10,051.89 | +21.42 | -125.28 | ACMR, GEN, LRCX, NVDA, ADSK | FNV | $1,852.99 | $9,914.45 | CM×42, ACMR×8, GEN×24, LRCX×2, NVDA×3, ADSK×2 |
| 2026-08-28 | +0.75 | $1,852.99 | CM×42, ACMR×8, GEN×24, LRCX×2, NVDA×3, ADSK×2 | $9,917.33 | +2.88 | -313.92 | KEYS, SMTC, CIEN, MPWR, DDOG, SEDG, TLS | CM, ACMR, GEN, LRCX, NVDA | $431.16 | $9,577.39 | ADSK×2, KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, SEDG×40, TLS×278 |
| 2026-08-31 | -5.85 | $431.16 | ADSK×2, KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, SEDG×40, TLS×278 | $9,577.46 | +0.07 | +0.00 | — | ADSK, KEYS, SMTC, CIEN, MPWR, DDOG, SEDG, TLS | $9,559.55 | $9,559.55 | — |
| 2026-09-01 | -6.30 | $9,559.55 | — | $9,559.55 | +0.00 | +0.00 | — | — | $9,559.55 | $9,559.55 | — |
| 2026-09-02 | -3.83 | $9,559.55 | — | $9,559.55 | +0.00 | +0.00 | — | — | $9,559.55 | $9,559.55 | — |
| 2026-09-03 | -0.90 | $9,559.55 | — | $9,559.55 | +0.00 | +402.10 | AVGO, DELL, CXW, FRNM, MMED, DE, HPE | — | $1,398.08 | $9,947.07 | AVGO×3, DELL×2, CXW×42, FRNM×86, MMED×57, DE×1, HPE×28 |
| 2026-09-04 | +2.25 | $1,398.08 | AVGO×3, DELL×2, CXW×42, FRNM×86, MMED×57, DE×1, HPE×28 | $9,879.17 | -67.90 | +103.91 | CRM, MRX, BE, BAK | AVGO, DELL, CXW, MMED, DE, HPE | $279.51 | $9,950.47 | FRNM×86, CRM×8, MRX×27, BE×8, BAK×1089 |
| 2026-09-08 | -11.47 | $279.51 | FRNM×86, CRM×8, MRX×27, BE×8, BAK×1089 | $10,132.33 | +181.86 | -57.51 | — | FRNM, CRM, BE, BAK | $7,983.05 | $10,054.22 | MRX×27 |
| 2026-09-09 | -13.95 | $7,983.05 | MRX×27 | $10,051.25 | -2.97 | +0.00 | — | MRX | $10,049.15 | $10,049.15 | — |
| 2026-09-10 | -13.28 | $10,049.15 | — | $10,049.15 | -0.00 | +0.00 | — | — | $10,049.15 | $10,049.15 | — |
| 2026-09-11 | +0.50 | $10,049.15 | — | $10,049.15 | -0.00 | -863.15 | ORCL | — | $16.75 | $9,183.83 | ORCL×61 |
| 2026-09-14 | -11.00 | $16.75 | ORCL×61 | $8,643.37 | -540.46 | +0.00 | — | ORCL | $8,641.11 | $8,641.11 | — |
| 2026-09-15 | -3.84 | $8,641.11 | — | $8,641.11 | +0.00 | +0.00 | — | — | $8,641.11 | $8,641.11 | — |
| 2026-09-16 | +5.30 | $8,641.11 | — | $8,641.11 | +0.00 | -161.83 | WAY, QCOM, SM | — | $54.30 | $8,472.73 | WAY×109, QCOM×15, SM×72 |
| 2026-09-17 | +7.38 | $54.30 | WAY×109, QCOM×15, SM×72 | $8,504.18 | +31.45 | +22.54 | SMTC, CLS, GME, JBHT, LITE | WAY, QCOM, SM | $974.67 | $8,509.81 | SMTC×9, CLS×5, GME×76, JBHT×7, LITE×1 |
| 2026-09-18 | +4.86 | $974.67 | SMTC×9, CLS×5, GME×76, JBHT×7, LITE×1 | $8,589.60 | +79.79 | -81.33 | TH, RARE, BHVN | SMTC, JBHT, LITE | $29.08 | $8,495.25 | CLS×5, GME×76, TH×82, RARE×116, BHVN×122 |
| 2026-09-21 | +12.87 | $29.08 | CLS×5, GME×76, TH×82, RARE×116, BHVN×122 | $8,629.99 | +134.74 | -213.90 | VICR, SMTC, ALVO, SION | CLS, GME, TH, RARE, BHVN | $136.83 | $8,391.44 | VICR×9, SMTC×11, ALVO×363, SION×359 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $8,758.81 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $7,505.17 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $6,270.08 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $5,021.21 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 81 | $15.38 | $2.23 | — | $3,773.20 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $2,571.18 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `S` | 52 | $23.77 | $2.15 | — | $1,332.73 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,332.73 | ▲ close $10,120.33 vs 09:30 $10,000.00 (session +137.19) | 16:00 close · cash $1,332.73 · equity $10,120.33 vs 09:30 $10,000.00 (+120.33; session marks +137.19) · 7 name(s) marked open→close (per-name table). HLIT×94 09:30 $13.18 → close $13.92 +69.56; ANGX×290 09:30 $4.31 → close $4.37 +17.40; ARX×63 09:30 $19.57 → close $19.58 +0.63; MH×92 09:30 $13.55 → close $13.10 -41.40; VELO×81 09:30 $15.38 → close $16.16 +63.18; NRG×10 09:30 $120.00 → close $126.24 +62.40; S×52 09:30 $23.77 → close $23.11 -34.58 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,332.73 | ▲ 09:30 equity $10,155.37 vs yday $10,120.33 (+35.04) | 09:30 open · cash $1,332.73 (unchanged overnight, no fees) · equity $10,155.37 vs prior close $10,120.33 (+35.04) · 7 name(s) re-marked at the open (per-name table). HLIT×94 yday $13.92 → 09:30 $13.84 -7.52; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; MH×92 yday $13.10 → 09:30 $13.16 +5.52; VELO×81 yday $16.16 → 09:30 $16.05 -8.91; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; S×52 yday $23.11 → 09:30 $22.50 -31.72 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $2,631.40 | ▲ +57.47 after sell → book $10,153.08; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,961.60 | ▲ +76.56 after sell → book $10,149.28; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $5,192.31 | ▼ -4.38 after sell → book $10,147.08; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $6,400.73 | ▼ -40.44 after sell → book $10,144.78; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VELO` | 81 | $16.05 | $2.26 | $+49.78 | $7,698.53 | ▲ +49.78 after sell → book $10,142.53; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $8,970.49 | ▲ +69.94 after sell → book $10,140.49; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `S` | 52 | $22.50 | $2.17 | $-70.61 | $10,138.32 | ▼ -70.61 after sell → book $10,138.32; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 54 | $46.18 | $2.15 | — | $7,642.45 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+6.7; leftover $2534.58 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 17 | $142.77 | $2.04 | — | $5,213.32 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+5.8; leftover $2534.58 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 12 | $202.70 | $2.03 | — | $2,778.89 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+8.3; leftover $2534.58 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 51 | $49.00 | $2.14 | — | $277.75 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $2534.58 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $277.75 | ▲ close $10,261.19 vs 09:30 $10,155.37 (session +131.23) | 16:00 close · cash $277.75 · equity $10,261.19 vs 09:30 $10,155.37 (+105.82; session marks +131.23) · 4 name(s) marked open→close (per-name table). DVN×54 09:30 $46.18 → close $47.57 +75.06; EOG×17 09:30 $142.77 → close $146.15 +57.46; FANG×12 09:30 $202.70 → close $206.29 +43.08; OUST×51 09:30 $49.00 → close $48.13 -44.37 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $277.75 | ▼ 09:30 equity $10,193.18 vs yday $10,261.19 (-68.01) | 09:30 open · cash $277.75 (unchanged overnight, no fees) · equity $10,193.18 vs prior close $10,261.19 (-68.01) · 4 name(s) re-marked at the open (per-name table). DVN×54 yday $47.57 → 09:30 $48.00 +23.22; EOG×17 yday $146.15 → 09:30 $148.04 +32.13; FANG×12 yday $206.29 → 09:30 $208.93 +31.68; OUST×51 yday $48.13 → 09:30 $45.09 -155.04 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 54 | $48.00 | $2.18 | $+93.95 | $2,867.57 | ▲ +93.95 after sell → book $10,191.00; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 17 | $148.04 | $2.07 | $+85.48 | $5,382.18 | ▲ +85.48 after sell → book $10,188.93; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 12 | $208.93 | $2.06 | $+70.68 | $7,887.28 | ▲ +70.68 after sell → book $10,186.87; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 51 | $45.09 | $2.17 | $-203.72 | $10,184.70 | ▼ -203.72 after sell → book $10,184.70; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,184.70 | ▲ close $10,184.70 vs 09:30 $10,193.18 (session +0.00) | 16:00 close · cash $10,184.70 · no lots left · equity $10,184.70. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,184.70 | ▲ 09:30 equity $10,184.70 vs yday $10,184.70 (-0.00) | 09:30 open · cash $10,184.70 · no holdings · equity $10,184.70 vs prior close $10,184.70 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,184.70 | ▲ close $10,184.70 vs 09:30 $10,184.70 (session +0.00) | 16:00 close · cash $10,184.70 · no lots left · equity $10,184.70. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,184.70 | ▲ 09:30 equity $10,184.70 vs yday $10,184.70 (-0.00) | 09:30 open · cash $10,184.70 · no holdings · equity $10,184.70 vs prior close $10,184.70 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,999.54 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1273.09 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $7,744.19 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1273.09 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 515 | $2.47 | $6.64 | — | $6,465.49 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1273.09 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $5,230.11 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1273.09 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 79 | $16.00 | $2.23 | — | $3,963.88 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1273.09 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $2,760.75 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1273.09 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 47 | $26.57 | $2.13 | — | $1,509.83 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1273.09 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1800 | $0.71 | $18.13 | — | $219.10 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1273.09 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $219.10 | ▼ close $9,956.20 vs 09:30 $10,184.70 (session -191.20) | 16:00 close · cash $219.10 · equity $9,956.20 vs 09:30 $10,184.70 (-228.50; session marks -191.20) · 8 name(s) marked open→close (per-name table). BHP×13 09:30 $91.01 → close $93.63 +34.06; APA×28 09:30 $44.76 → close $44.39 -10.36; AUTL×515 09:30 $2.47 → close $2.46 -5.15; CRSP×21 09:30 $58.73 → close $58.12 -12.81; ASST×79 09:30 $16.00 → close $16.13 +10.27; MRNA×8 09:30 $150.14 → close $133.32 -134.56; ZLAB×47 09:30 $26.57 → close $26.02 -25.85; HUMA×1800 09:30 $0.71 → close $0.68 -46.80 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $219.10 | ▲ 09:30 equity $10,143.16 vs yday $9,956.20 (+186.96) | 09:30 open · cash $219.10 (unchanged overnight, no fees) · equity $10,143.16 vs prior close $9,956.20 (+186.96) · 8 name(s) re-marked at the open (per-name table). BHP×13 yday $93.63 → 09:30 $95.72 +27.17; APA×28 yday $44.39 → 09:30 $44.52 +3.64; AUTL×515 yday $2.46 → 09:30 $2.47 +5.15; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; ASST×79 yday $16.13 → 09:30 $17.66 +120.87; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; ZLAB×47 yday $26.02 → 09:30 $26.25 +10.81; HUMA×1800 yday $0.68 → 09:30 $0.67 -12.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,461.41 | ▲ +57.15 after sell → book $10,141.11; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $2,705.88 | ▼ -10.89 after sell → book $10,139.02; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 79 | $17.66 | $2.25 | $+126.66 | $4,098.77 | ▲ +126.66 after sell → book $10,136.77; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $5,161.61 | ▼ -140.29 after sell → book $10,134.73; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 47 | $26.25 | $2.15 | $-19.32 | $6,393.21 | ▼ -19.32 after sell → book $10,132.58; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1800 | $0.67 | $17.84 | $-95.37 | $7,588.57 | ▼ -95.37 after sell → book $10,114.74; vs 09:30 mark -17.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,392.25 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1264.76 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $5,238.43 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1264.76 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 16 | $78.88 | $2.04 | — | $3,974.31 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1264.76 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 146 | $8.66 | $2.43 | — | $2,707.52 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1264.76 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 390 | $3.24 | $5.03 | — | $1,438.89 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $1264.76 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 108 | $11.70 | $2.31 | — | $172.98 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1264.76 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $172.98 | ▼ close $9,940.43 vs 09:30 $10,143.16 (session -158.46) | 16:00 close · cash $172.98 · equity $9,940.43 vs 09:30 $10,143.16 (-202.73; session marks -158.46) · 8 name(s) marked open→close (per-name table). AUTL×515 09:30 $2.47 → close $2.41 -30.90; CRSP×21 09:30 $59.72 → close $59.50 -4.62; AU×10 09:30 $119.43 → close $121.22 +17.90; FUTU×10 09:30 $115.18 → close $123.64 +84.60; GRAL×16 09:30 $78.88 → close $79.54 +10.56; ABTC×146 09:30 $8.66 → close $7.93 -106.58; HIVE×390 09:30 $3.24 → close $3.03 -81.90; MARA×108 09:30 $11.70 → close $11.26 -47.52 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $172.98 | ▼ 09:30 equity $9,908.21 vs yday $9,940.43 (-32.22) | 09:30 open · cash $172.98 (unchanged overnight, no fees) · equity $9,908.21 vs prior close $9,940.43 (-32.22) · 8 name(s) re-marked at the open (per-name table). AUTL×515 yday $2.41 → 09:30 $2.40 -5.15; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; AU×10 yday $121.22 → 09:30 $120.51 -7.10; FUTU×10 yday $123.64 → 09:30 $121.00 -26.40; GRAL×16 yday $79.54 → 09:30 $81.87 +37.28; ABTC×146 yday $7.93 → 09:30 $8.00 +10.22; HIVE×390 yday $3.03 → 09:30 $2.99 -15.60; MARA×108 yday $11.26 → 09:30 $11.17 -9.72 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 515 | $2.40 | $6.74 | $-49.43 | $1,402.24 | ▼ -49.43 after sell → book $9,901.47; vs 09:30 mark -6.74 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $2,605.30 | ▲ +6.74 after sell → book $9,899.43; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $121.00 | $2.04 | $+54.14 | $3,813.26 | ▲ +54.14 after sell → book $9,897.39; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 16 | $81.87 | $2.06 | $+43.74 | $5,121.12 | ▲ +43.74 after sell → book $9,895.33; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 146 | $8.00 | $2.46 | $-101.25 | $6,286.66 | ▼ -101.25 after sell → book $9,892.87; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 390 | $2.99 | $5.11 | $-107.64 | $7,447.65 | ▼ -107.64 after sell → book $9,887.76; vs 09:30 mark -5.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 108 | $11.17 | $2.34 | $-61.90 | $8,651.67 | ▼ -61.90 after sell → book $9,885.42; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,651.67 | ▼ close $9,850.25 vs 09:30 $9,908.21 (session -35.17) | 16:00 close · cash $8,651.67 · equity $9,850.25 vs 09:30 $9,908.21 (-57.96; session marks -35.17) · 1 name(s) marked open→close (per-name table). CRSP×21 09:30 $58.75 → close $57.08 -35.17 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,651.67 | ▲ 09:30 equity $9,868.20 vs yday $9,850.25 (+17.95) | 09:30 open · cash $8,651.67 (unchanged overnight, no fees) · equity $9,868.20 vs prior close $9,850.25 (+17.95) · 1 name(s) re-marked at the open (per-name table). CRSP×21 yday $57.08 → 09:30 $57.93 +17.95 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-20.93 | $9,866.13 | ▼ -20.93 after sell → book $9,866.13; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 27 | $118.52 | $2.07 | — | $6,664.02 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $3288.71 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 42 | $77.13 | $2.12 | — | $3,422.44 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3288.71 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 93 | $35.05 | $2.27 | — | $160.52 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $3288.71 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.52 | ▲ close $10,124.66 vs 09:30 $9,868.20 (session +264.99) | 16:00 close · cash $160.52 · equity $10,124.66 vs 09:30 $9,868.20 (+256.46; session marks +264.99) · 3 name(s) marked open→close (per-name table). AU×27 09:30 $118.52 → close $123.39 +131.49; FCX×42 09:30 $77.13 → close $79.91 +116.76; EZPW×93 09:30 $35.05 → close $35.23 +16.74 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.52 | ▼ 09:30 equity $10,047.50 vs yday $10,124.66 (-77.16) | 09:30 open · cash $160.52 (unchanged overnight, no fees) · equity $10,047.50 vs prior close $10,124.66 (-77.16) · 3 name(s) re-marked at the open (per-name table). AU×27 yday $123.39 → 09:30 $119.80 -96.93; FCX×42 yday $79.91 → 09:30 $79.34 -23.94; EZPW×93 yday $35.23 → 09:30 $35.70 +43.71 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 27 | $119.80 | $2.11 | $+30.38 | $3,393.02 | ▲ +30.38 after sell → book $10,045.40; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 42 | $79.34 | $2.15 | $+88.55 | $6,723.14 | ▲ +88.55 after sell → book $10,043.24; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 93 | $35.70 | $2.31 | $+55.87 | $10,040.93 | ▲ +55.87 after sell → book $10,040.93; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 18 | $267.02 | $2.04 | — | $5,232.53 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $5020.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 42 | $118.50 | $2.12 | — | $253.41 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $5020.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $253.41 | ▼ close $10,030.47 vs 09:30 $10,047.50 (session -6.30) | 16:00 close · cash $253.41 · equity $10,030.47 vs 09:30 $10,047.50 (-17.03; session marks -6.30) · 2 name(s) marked open→close (per-name table). FNV×18 09:30 $267.02 → close $267.37 +6.30; CM×42 09:30 $118.50 → close $118.20 -12.60 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $253.41 | ▲ 09:30 equity $10,051.89 vs yday $10,030.47 (+21.42) | 09:30 open · cash $253.41 (unchanged overnight, no fees) · equity $10,051.89 vs prior close $10,030.47 (+21.42) · 2 name(s) re-marked at the open (per-name table). FNV×18 yday $267.37 → 09:30 $267.23 -2.52; CM×42 yday $118.20 → 09:30 $118.77 +23.94 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 18 | $267.23 | $2.09 | $-0.36 | $5,061.46 | ▼ -0.36 after sell → book $10,049.80; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 8 | $81.65 | $2.01 | — | $4,406.25 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $723.07 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 24 | $29.83 | $2.06 | — | $3,688.26 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+7.6; leftover $723.07 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 2 | $318.88 | $2.00 | — | $3,048.51 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $723.07 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 3 | $222.86 | $2.00 | — | $2,377.93 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $723.07 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 2 | $261.47 | $2.00 | — | $1,852.99 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list overnight,overnight_mega; 🔵; ret5=+1.4; leftover $723.07 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,852.99 | ▼ close $9,914.45 vs 09:30 $10,051.89 (session -125.28) | 16:00 close · cash $1,852.99 · equity $9,914.45 vs 09:30 $10,051.89 (-137.44; session marks -125.28) · 6 name(s) marked open→close (per-name table). CM×42 09:30 $118.77 → close $114.84 -165.06; ACMR×8 09:30 $81.65 → close $80.49 -9.28; GEN×24 09:30 $29.83 → close $30.50 +16.08; LRCX×2 09:30 $318.88 → close $318.58 -0.60; NVDA×3 09:30 $222.86 → close $227.98 +15.36; ADSK×2 09:30 $261.47 → close $270.58 +18.22 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,852.99 | ▲ 09:30 equity $9,917.33 vs yday $9,914.45 (+2.88) | 09:30 open · cash $1,852.99 (unchanged overnight, no fees) · equity $9,917.33 vs prior close $9,914.45 (+2.88) · 6 name(s) re-marked at the open (per-name table). CM×42 yday $114.84 → 09:30 $115.66 +34.44; ACMR×8 yday $80.49 → 09:30 $79.27 -9.76; GEN×24 yday $30.50 → 09:30 $30.50 +0.00; LRCX×2 yday $318.58 → 09:30 $318.03 -1.10; NVDA×3 yday $227.98 → 09:30 $227.36 -1.86; ADSK×2 yday $270.58 → 09:30 $261.16 -18.84 | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 42 | $115.66 | $2.16 | $-123.56 | $6,708.55 | ▼ -123.56 after sell → book $9,915.17; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 8 | $79.27 | $2.03 | $-23.09 | $7,340.67 | ▼ -23.09 after sell → book $9,913.13; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 24 | $30.50 | $2.08 | $+11.94 | $8,070.59 | ▲ +11.94 after sell → book $9,911.05; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 2 | $318.03 | $2.02 | $-5.71 | $8,704.64 | ▼ -5.71 after sell → book $9,909.04; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 3 | $227.36 | $2.02 | $+9.48 | $9,384.70 | ▲ +9.48 after sell → book $9,907.02; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $8,085.06 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1340.67 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $6,807.20 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1340.67 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $5,603.94 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1340.67 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $4,295.92 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1340.67 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $3,092.81 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1340.67 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 40 | $32.90 | $2.11 | — | $1,774.70 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1340.67 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 278 | $4.82 | $3.59 | — | $431.16 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1340.67 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $431.16 | ▼ close $9,577.39 vs 09:30 $9,917.33 (session -313.92) | 16:00 close · cash $431.16 · equity $9,577.39 vs 09:30 $9,917.33 (-339.94; session marks -313.92) · 8 name(s) marked open→close (per-name table). ADSK×2 09:30 $261.16 → close $260.66 -1.00; KEYS×4 09:30 $324.41 → close $319.97 -17.76; SMTC×9 09:30 $141.76 → close $131.17 -95.31; CIEN×3 09:30 $400.42 → close $378.44 -65.94; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×5 09:30 $240.22 → close $236.98 -16.20; SEDG×40 09:30 $32.90 → close $31.41 -59.60; TLS×278 09:30 $4.82 → close $4.79 -8.34 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $431.16 | ▲ 09:30 equity $9,577.46 vs yday $9,577.39 (+0.07) | 09:30 open · cash $431.16 (unchanged overnight, no fees) · equity $9,577.46 vs prior close $9,577.39 (+0.07) · 8 name(s) re-marked at the open (per-name table). ADSK×2 yday $260.66 → 09:30 $257.71 -5.90; KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×5 yday $236.98 → 09:30 $233.97 -15.07; SEDG×40 yday $31.41 → 09:30 $31.15 -10.40; TLS×278 yday $4.79 → 09:30 $4.81 +5.56 | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 2 | $257.71 | $2.02 | $-11.53 | $944.56 | ▼ -11.53 after sell → book $9,575.44; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $2,232.50 | ▼ -11.70 after sell → book $9,573.42; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $3,421.16 | ▼ -89.19 after sell → book $9,571.38; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $4,554.46 | ▼ -69.96 after sell → book $9,569.37; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $5,814.35 | ▼ -48.14 after sell → book $9,567.35; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 5 | $233.97 | $2.02 | $-35.31 | $6,982.15 | ▼ -35.31 after sell → book $9,565.33; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 40 | $31.15 | $2.13 | $-74.24 | $8,226.02 | ▼ -74.24 after sell → book $9,563.20; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 278 | $4.81 | $3.64 | $-10.01 | $9,559.55 | ▼ -10.01 after sell → book $9,559.55; vs 09:30 mark -3.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,559.55 | ▲ close $9,559.55 vs 09:30 $9,577.46 (session +0.00) | 16:00 close · cash $9,559.55 · no lots left · equity $9,559.55. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,559.55 | ▲ 09:30 equity $9,559.55 vs yday $9,559.55 (+0.00) | 09:30 open · cash $9,559.55 · no holdings · equity $9,559.55 vs prior close $9,559.55 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,559.55 | ▲ close $9,559.55 vs 09:30 $9,559.55 (session +0.00) | 16:00 close · cash $9,559.55 · no lots left · equity $9,559.55. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,559.55 | ▲ 09:30 equity $9,559.55 vs yday $9,559.55 (+0.00) | 09:30 open · cash $9,559.55 · no holdings · equity $9,559.55 vs prior close $9,559.55 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,559.55 | ▲ close $9,559.55 vs 09:30 $9,559.55 (session +0.00) | 16:00 close · cash $9,559.55 · no lots left · equity $9,559.55. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,559.55 | ▲ 09:30 equity $9,559.55 vs yday $9,559.55 (+0.00) | 09:30 open · cash $9,559.55 · no holdings · equity $9,559.55 vs prior close $9,559.55 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,502.34 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1365.65 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,527.72 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1365.65 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 42 | $32.31 | $2.12 | — | $6,168.58 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1365.65 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 86 | $15.87 | $2.25 | — | $4,801.52 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1365.65 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 57 | $23.88 | $2.16 | — | $3,438.19 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1365.65 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $2,732.95 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1365.65 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 28 | $47.60 | $2.07 | — | $1,398.08 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1365.65 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,398.08 | ▲ close $9,947.07 vs 09:30 $9,559.55 (session +402.10) | 16:00 close · cash $1,398.08 · equity $9,947.07 vs 09:30 $9,559.55 (+387.52; session marks +402.10) · 7 name(s) marked open→close (per-name table). AVGO×3 09:30 $351.74 → close $357.16 +16.26; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×42 09:30 $32.31 → close $33.66 +56.70; FRNM×86 09:30 $15.87 → close $16.90 +88.58; MMED×57 09:30 $23.88 → close $23.84 -2.28; DE×1 09:30 $703.25 → close $694.41 -8.84; HPE×28 09:30 $47.60 → close $54.44 +191.52 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,398.08 | ▼ 09:30 equity $9,879.17 vs yday $9,947.07 (-67.90) | 09:30 open · cash $1,398.08 (unchanged overnight, no fees) · equity $9,879.17 vs prior close $9,947.07 (-67.90) · 7 name(s) re-marked at the open (per-name table). AVGO×3 yday $357.16 → 09:30 $359.70 +7.62; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×42 yday $33.66 → 09:30 $33.46 -8.40; FRNM×86 yday $16.90 → 09:30 $16.40 -43.00; MMED×57 yday $23.84 → 09:30 $23.84 +0.00; DE×1 yday $694.41 → 09:30 $692.03 -2.38; HPE×28 yday $54.44 → 09:30 $53.85 -16.52 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $2,475.16 | ▲ +19.86 after sell → book $9,877.15; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $3,500.70 | ▲ +50.93 after sell → book $9,875.13; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 42 | $33.46 | $2.14 | $+44.05 | $4,903.89 | ▲ +44.05 after sell → book $9,873.00; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 57 | $23.84 | $2.18 | $-6.62 | $6,260.58 | ▼ -6.62 after sell → book $9,870.81; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $6,950.60 | ▼ -15.23 after sell → book $9,868.80; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 28 | $53.85 | $2.10 | $+170.83 | $8,456.30 | ▲ +170.83 after sell → book $9,866.70; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 8 | $263.36 | $2.01 | — | $6,347.41 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $2114.08 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 27 | $75.65 | $2.07 | — | $4,302.79 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $2114.08 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 8 | $236.82 | $2.01 | — | $2,406.22 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; ret5=+8.1; leftover $2114.08 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 1089 | $1.94 | $14.05 | — | $279.51 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $2114.08 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $279.51 | ▲ close $9,950.47 vs 09:30 $9,879.17 (session +103.91) | 16:00 close · cash $279.51 · equity $9,950.47 vs 09:30 $9,879.17 (+71.30; session marks +103.91) · 5 name(s) marked open→close (per-name table). FRNM×86 09:30 $16.40 → close $16.31 -7.74; CRM×8 09:30 $263.36 → close $259.23 -33.04; MRX×27 09:30 $75.65 → close $78.27 +70.74; BE×8 09:30 $236.82 → close $252.87 +128.40; BAK×1089 09:30 $1.94 → close $1.89 -54.45 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $279.51 | ▲ 09:30 equity $10,132.33 vs yday $9,950.47 (+181.86) | 09:30 open · cash $279.51 (unchanged overnight, no fees) · equity $10,132.33 vs prior close $9,950.47 (+181.86) · 5 name(s) re-marked at the open (per-name table). FRNM×86 yday $16.31 → 09:30 $16.74 +36.98; CRM×8 yday $259.23 → 09:30 $253.72 -44.08; MRX×27 yday $78.27 → 09:30 $78.84 +15.39; BE×8 yday $252.87 → 09:30 $267.76 +119.12; BAK×1089 yday $1.89 → 09:30 $1.94 +54.45 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 86 | $16.74 | $2.27 | $+70.30 | $1,716.87 | ▲ +70.30 after sell → book $10,130.05; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 8 | $253.72 | $2.04 | $-81.17 | $3,744.59 | ▼ -81.17 after sell → book $10,128.01; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 8 | $267.76 | $2.04 | $+243.46 | $5,884.63 | ▲ +243.46 after sell → book $10,125.97; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 1089 | $1.94 | $14.25 | $-28.29 | $7,983.05 | ▼ -28.29 after sell → book $10,111.73; vs 09:30 mark -14.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,983.05 | ▼ close $10,054.22 vs 09:30 $10,132.33 (session -57.51) | 16:00 close · cash $7,983.05 · equity $10,054.22 vs 09:30 $10,132.33 (-78.11; session marks -57.51) · 1 name(s) marked open→close (per-name table). MRX×27 09:30 $78.84 → close $76.71 -57.51 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,983.05 | ▼ 09:30 equity $10,051.25 vs yday $10,054.22 (-2.97) | 09:30 open · cash $7,983.05 (unchanged overnight, no fees) · equity $10,051.25 vs prior close $10,054.22 (-2.97) · 1 name(s) re-marked at the open (per-name table). MRX×27 yday $76.71 → 09:30 $76.60 -2.97 | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 27 | $76.60 | $2.10 | $+21.48 | $10,049.15 | ▲ +21.48 after sell → book $10,049.15; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,049.15 | ▲ close $10,049.15 vs 09:30 $10,051.25 (session +0.00) | 16:00 close · cash $10,049.15 · no lots left · equity $10,049.15. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,049.15 | ▲ 09:30 equity $10,049.15 vs yday $10,049.15 (-0.00) | 09:30 open · cash $10,049.15 · no holdings · equity $10,049.15 vs prior close $10,049.15 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,049.15 | ▲ close $10,049.15 vs 09:30 $10,049.15 (session +0.00) | 16:00 close · cash $10,049.15 · no lots left · equity $10,049.15. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,049.15 | ▲ 09:30 equity $10,049.15 vs yday $10,049.15 (-0.00) | 09:30 open · cash $10,049.15 · no holdings · equity $10,049.15 vs prior close $10,049.15 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 61 | $164.43 | $2.17 | — | $16.75 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $10049.15 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.75 | ▼ close $9,183.83 vs 09:30 $10,049.15 (session -863.15) | 16:00 close · cash $16.75 · equity $9,183.83 vs 09:30 $10,049.15 (-865.32; session marks -863.15) · 1 name(s) marked open→close (per-name table). ORCL×61 09:30 $164.43 → close $150.28 -863.15 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.75 | ▼ 09:30 equity $8,643.37 vs yday $9,183.83 (-540.46) | 09:30 open · cash $16.75 (unchanged overnight, no fees) · equity $8,643.37 vs prior close $9,183.83 (-540.46) · 1 name(s) re-marked at the open (per-name table). ORCL×61 yday $150.28 → 09:30 $141.42 -540.46 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 61 | $141.42 | $2.25 | $-1408.04 | $8,641.11 | ▼ -1,408.04 after sell → book $8,641.11; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,641.11 | ▲ close $8,641.11 vs 09:30 $8,643.37 (session +0.00) | 16:00 close · cash $8,641.11 · no lots left · equity $8,641.11. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,641.11 | ▲ 09:30 equity $8,641.11 vs yday $8,641.11 (+0.00) | 09:30 open · cash $8,641.11 · no holdings · equity $8,641.11 vs prior close $8,641.11 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,641.11 | ▲ close $8,641.11 vs 09:30 $8,641.11 (session +0.00) | 16:00 close · cash $8,641.11 · no lots left · equity $8,641.11. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,641.11 | ▲ 09:30 equity $8,641.11 vs yday $8,641.11 (+0.00) | 09:30 open · cash $8,641.11 · no holdings · equity $8,641.11 vs prior close $8,641.11 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 109 | $26.27 | $2.32 | — | $5,775.37 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2880.37 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 15 | $189.17 | $2.04 | — | $2,935.78 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2880.37 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 72 | $39.99 | $2.21 | — | $54.30 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2880.37 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.30 | ▼ close $8,472.73 vs 09:30 $8,641.11 (session -161.83) | 16:00 close · cash $54.30 · equity $8,472.73 vs 09:30 $8,641.11 (-168.38; session marks -161.83) · 3 name(s) marked open→close (per-name table). WAY×109 09:30 $26.27 → close $26.59 +34.88; QCOM×15 09:30 $189.17 → close $184.84 -64.95; SM×72 09:30 $39.99 → close $38.16 -131.76 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.30 | ▲ 09:30 equity $8,504.18 vs yday $8,472.73 (+31.45) | 09:30 open · cash $54.30 (unchanged overnight, no fees) · equity $8,504.18 vs prior close $8,472.73 (+31.45) · 3 name(s) re-marked at the open (per-name table). WAY×109 yday $26.59 → 09:30 $26.51 -8.72; QCOM×15 yday $184.84 → 09:30 $190.35 +82.65; SM×72 yday $38.16 → 09:30 $37.57 -42.48 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 109 | $26.51 | $2.36 | $+21.48 | $2,941.53 | ▲ +21.48 after sell → book $8,501.82; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 15 | $190.35 | $2.07 | $+13.60 | $5,794.71 | ▲ +13.60 after sell → book $8,499.75; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 72 | $37.57 | $2.24 | $-178.69 | $8,497.51 | ▼ -178.69 after sell → book $8,497.51; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 9 | $170.85 | $2.02 | — | $6,957.84 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1699.50 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `CLS` | 5 | $337.75 | $2.00 | — | $5,267.09 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+10.2; leftover $1699.50 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 76 | $22.12 | $2.22 | — | $3,583.75 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1699.50 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 7 | $238.60 | $2.01 | — | $1,911.54 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_mover; ret5=-11.6; leftover $1699.50 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 1 | $934.88 | $1.99 | — | $974.67 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; ret5=-7.0; leftover $1699.50 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $974.67 | ▲ close $8,509.81 vs 09:30 $8,504.18 (session +22.54) | 16:00 close · cash $974.67 · equity $8,509.81 vs 09:30 $8,504.18 (+5.63; session marks +22.54) · 5 name(s) marked open→close (per-name table). SMTC×9 09:30 $170.85 → close $178.19 +66.06; CLS×5 09:30 $337.75 → close $329.94 -39.05; GME×76 09:30 $22.12 → close $22.77 +49.40; JBHT×7 09:30 $238.60 → close $236.80 -12.60; LITE×1 09:30 $934.88 → close $893.61 -41.27 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $974.67 | ▲ 09:30 equity $8,589.60 vs yday $8,509.81 (+79.79) | 09:30 open · cash $974.67 (unchanged overnight, no fees) · equity $8,589.60 vs prior close $8,509.81 (+79.79) · 5 name(s) re-marked at the open (per-name table). SMTC×9 yday $178.19 → 09:30 $182.33 +37.26; CLS×5 yday $329.94 → 09:30 $332.06 +10.60; GME×76 yday $22.77 → 09:30 $22.90 +9.88; JBHT×7 yday $236.80 → 09:30 $236.80 +0.00; LITE×1 yday $893.61 → 09:30 $915.66 +22.05 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 9 | $182.33 | $2.04 | $+99.26 | $2,613.60 | ▲ +99.26 after sell → book $8,587.56; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 7 | $236.80 | $2.03 | $-16.65 | $4,269.16 | ▼ -16.65 after sell → book $8,585.52; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 1 | $915.66 | $2.01 | $-23.23 | $5,182.81 | ▼ -23.23 after sell → book $8,583.51; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 82 | $20.91 | $2.24 | — | $3,465.95 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1727.60 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 116 | $14.79 | $2.34 | — | $1,747.97 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1727.60 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 122 | $14.07 | $2.36 | — | $29.08 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1727.60 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.08 | ▼ close $8,495.25 vs 09:30 $8,589.60 (session -81.33) | 16:00 close · cash $29.08 · equity $8,495.25 vs 09:30 $8,589.60 (-94.35; session marks -81.33) · 5 name(s) marked open→close (per-name table). CLS×5 09:30 $332.06 → close $332.63 +2.85; GME×76 09:30 $22.90 → close $22.64 -19.76; TH×82 09:30 $20.91 → close $21.19 +22.96; RARE×116 09:30 $14.79 → close $14.51 -32.48; BHVN×122 09:30 $14.07 → close $13.62 -54.90 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.08 | ▲ 09:30 equity $8,629.99 vs yday $8,495.25 (+134.74) | 09:30 open · cash $29.08 (unchanged overnight, no fees) · equity $8,629.99 vs prior close $8,495.25 (+134.74) · 5 name(s) re-marked at the open (per-name table). CLS×5 yday $332.63 → 09:30 $341.45 +44.10; GME×76 yday $22.64 → 09:30 $22.78 +10.64; TH×82 yday $21.19 → 09:30 $21.65 +37.72; RARE×116 yday $14.51 → 09:30 $14.58 +8.12; BHVN×122 yday $13.62 → 09:30 $13.90 +34.16 | — |
| 2026-09-21 09:30 ET | **SELL** | `CLS` | 5 | $341.45 | $2.03 | $+14.47 | $1,734.30 | ▲ +14.47 after sell → book $8,627.96; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 76 | $22.78 | $2.24 | $+45.70 | $3,463.34 | ▲ +45.70 after sell → book $8,625.72; vs 09:30 mark -2.24 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 82 | $21.65 | $2.26 | $+56.18 | $5,236.37 | ▲ +56.18 after sell → book $8,623.45; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 116 | $14.58 | $2.37 | $-29.07 | $6,925.28 | ▼ -29.07 after sell → book $8,621.08; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 122 | $13.90 | $2.39 | $-25.49 | $8,618.69 | ▼ -25.49 after sell → book $8,618.69; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 9 | $230.25 | $2.02 | — | $6,544.42 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; ret5=+12.5; leftover $2154.67 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 11 | $190.30 | $2.02 | — | $4,449.10 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; ret5=+10.6; leftover $2154.67 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `ALVO` | 363 | $5.92 | $4.68 | — | $2,295.46 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; ret5=+11.0; leftover $2154.67 | join🔴 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 359 | $6.00 | $4.63 | — | $136.83 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_mover; 🔵; ret5=-24.1; leftover $2154.67 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $136.83 | ▼ close $8,391.44 vs 09:30 $8,629.99 (session -213.90) | 16:00 close · cash $136.83 · equity $8,391.44 vs 09:30 $8,629.99 (-238.55; session marks -213.90) · 4 name(s) marked open→close (per-name table). VICR×9 09:30 $230.25 → close $223.90 -57.15; SMTC×11 09:30 $190.30 → close $177.37 -142.23; ALVO×363 09:30 $5.92 → close $5.88 -14.52; SION×359 09:30 $6.00 → close $6.00 +0.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `MU` | cash | leftover split 723.07 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 723.07 < 1 share @ 1746.53 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
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
| `VICR` | 9 | 2026-09-21 @ $230.25 | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; ret5=+12.5; leftover $2154.67 |
| `SMTC` | 11 | 2026-09-21 @ $190.30 | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; ret5=+10.6; leftover $2154.67 |
| `ALVO` | 363 | 2026-09-21 @ $5.92 | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; ret5=+11.0; leftover $2154.67 |
| `SION` | 359 | 2026-09-21 @ $6.00 | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_mover; 🔵; ret5=-24.1; leftover $2154.67 |
