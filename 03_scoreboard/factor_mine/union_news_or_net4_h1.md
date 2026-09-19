# Factor mine action — `union_news_or_net4_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 4

Cash book **-8.06%** ($9,194) · signal-only (no cash/fees) was -1.79%. Starts YES **0/26**. Fills 98 · skips 23 · realized $-805.78.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,194.20.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `HLIT` | 151 | — | $13.18 | +0.00 | $13.92 | +111.74 | +111.74 | +0.00 | +111.74 |
| 2026-08-14 | `SNDK` | 1 | — | $1646.93 | +0.00 | $1641.11 | -5.82 | -5.82 | +0.00 | -5.82 |
| 2026-08-14 | `ANGX` | 464 | — | $4.31 | +0.00 | $4.37 | +27.84 | +27.84 | +0.00 | +27.84 |
| 2026-08-14 | `MH` | 147 | — | $13.55 | +0.00 | $13.10 | -66.15 | -66.15 | +0.00 | -66.15 |
| 2026-08-14 | `VELO` | 130 | — | $15.38 | +0.00 | $16.16 | +101.40 | +101.40 | +0.00 | +101.40 |
| 2026-08-17 | `HLIT` | 151 | $13.92 | $13.84 | -12.08 | — | +0.00 | -12.08 | +99.66 | — |
| 2026-08-17 | `SNDK` | 1 | $1641.11 | $1700.74 | +59.63 | — | +0.00 | +59.63 | +53.81 | — |
| 2026-08-17 | `ANGX` | 464 | $4.37 | $4.60 | +106.72 | — | +0.00 | +106.72 | +134.56 | — |
| 2026-08-17 | `MH` | 147 | $13.10 | $13.16 | +8.82 | — | +0.00 | +8.82 | -57.33 | — |
| 2026-08-17 | `VELO` | 130 | $16.16 | $16.05 | -14.30 | — | +0.00 | -14.30 | +87.10 | — |
| 2026-08-17 | `DVN` | 74 | — | $46.18 | +0.00 | $47.57 | +102.86 | +102.86 | +0.00 | +102.86 |
| 2026-08-17 | `EOG` | 24 | — | $142.77 | +0.00 | $146.15 | +81.12 | +81.12 | +0.00 | +81.12 |
| 2026-08-17 | `FANG` | 16 | — | $202.70 | +0.00 | $206.29 | +57.44 | +57.44 | +0.00 | +57.44 |
| 2026-08-18 | `DVN` | 74 | $47.57 | $48.00 | +31.82 | — | +0.00 | +31.82 | +134.68 | — |
| 2026-08-18 | `EOG` | 24 | $146.15 | $148.04 | +45.36 | — | +0.00 | +45.36 | +126.48 | — |
| 2026-08-18 | `FANG` | 16 | $206.29 | $208.93 | +42.24 | — | +0.00 | +42.24 | +99.68 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 16 | — | $91.01 | +0.00 | $93.63 | +41.92 | +41.92 | +0.00 | +41.92 |
| 2026-08-20 | `APA` | 33 | — | $44.76 | +0.00 | $44.39 | -12.21 | -12.21 | +0.00 | -12.21 |
| 2026-08-20 | `AUTL` | 615 | — | $2.47 | +0.00 | $2.46 | -6.15 | -6.15 | +0.00 | -6.15 |
| 2026-08-20 | `CRSP` | 25 | — | $58.73 | +0.00 | $58.12 | -15.25 | -15.25 | +0.00 | -15.25 |
| 2026-08-20 | `ASST` | 94 | — | $16.00 | +0.00 | $16.13 | +12.22 | +12.22 | +0.00 | +12.22 |
| 2026-08-20 | `MRNA` | 10 | — | $150.14 | +0.00 | $133.32 | -168.20 | -168.20 | +0.00 | -168.20 |
| 2026-08-20 | `ZLAB` | 57 | — | $26.57 | +0.00 | $26.02 | -31.35 | -31.35 | +0.00 | -31.35 |
| 2026-08-21 | `BHP` | 16 | $93.63 | $95.72 | +33.44 | — | +0.00 | +33.44 | +75.36 | — |
| 2026-08-21 | `APA` | 33 | $44.39 | $44.52 | +4.29 | — | +0.00 | +4.29 | -7.92 | — |
| 2026-08-21 | `AUTL` | 615 | $2.46 | $2.47 | +6.15 | $2.41 | -36.90 | -30.75 | +0.00 | -36.90 |
| 2026-08-21 | `CRSP` | 25 | $58.12 | $59.72 | +40.00 | $59.50 | -5.50 | +34.50 | +24.75 | +19.25 |
| 2026-08-21 | `ASST` | 94 | $16.13 | $17.66 | +143.82 | — | +0.00 | +143.82 | +156.04 | — |
| 2026-08-21 | `MRNA` | 10 | $133.32 | $133.11 | -2.10 | — | +0.00 | -2.10 | -170.30 | — |
| 2026-08-21 | `ZLAB` | 57 | $26.02 | $26.25 | +13.11 | — | +0.00 | +13.11 | -18.24 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `FUTU` | 11 | — | $115.18 | +0.00 | $123.64 | +93.06 | +93.06 | +0.00 | +93.06 |
| 2026-08-21 | `GRAL` | 16 | — | $78.88 | +0.00 | $79.54 | +10.56 | +10.56 | +0.00 | +10.56 |
| 2026-08-21 | `ABTC` | 147 | — | $8.66 | +0.00 | $7.93 | -107.31 | -107.31 | +0.00 | -107.31 |
| 2026-08-21 | `HIVE` | 393 | — | $3.24 | +0.00 | $3.03 | -82.53 | -82.53 | +0.00 | -82.53 |
| 2026-08-21 | `MARA` | 108 | — | $11.70 | +0.00 | $11.26 | -47.52 | -47.52 | +0.00 | -47.52 |
| 2026-08-24 | `AUTL` | 615 | $2.41 | $2.40 | -6.15 | — | +0.00 | -6.15 | -43.05 | — |
| 2026-08-24 | `CRSP` | 25 | $59.50 | $58.75 | -18.75 | $57.08 | -41.87 | -60.62 | +0.50 | -41.37 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `FUTU` | 11 | $123.64 | $121.00 | -29.04 | — | +0.00 | -29.04 | +64.02 | — |
| 2026-08-24 | `GRAL` | 16 | $79.54 | $81.87 | +37.28 | — | +0.00 | +37.28 | +47.84 | — |
| 2026-08-24 | `ABTC` | 147 | $7.93 | $8.00 | +10.29 | — | +0.00 | +10.29 | -97.02 | — |
| 2026-08-24 | `HIVE` | 393 | $3.03 | $2.99 | -15.72 | — | +0.00 | -15.72 | -98.25 | — |
| 2026-08-24 | `MARA` | 108 | $11.26 | $11.17 | -9.72 | — | +0.00 | -9.72 | -57.24 | — |
| 2026-08-25 | `CRSP` | 25 | $57.08 | $57.93 | +21.37 | — | +0.00 | +21.37 | -20.00 | — |
| 2026-08-25 | `AU` | 29 | — | $118.52 | +0.00 | $123.39 | +141.23 | +141.23 | +0.00 | +141.23 |
| 2026-08-25 | `FCX` | 44 | — | $77.13 | +0.00 | $79.91 | +122.32 | +122.32 | +0.00 | +122.32 |
| 2026-08-25 | `EZPW` | 98 | — | $35.05 | +0.00 | $35.23 | +17.64 | +17.64 | +0.00 | +17.64 |
| 2026-08-26 | `AU` | 29 | $123.39 | $119.80 | -104.11 | — | +0.00 | -104.11 | +37.12 | — |
| 2026-08-26 | `FCX` | 44 | $79.91 | $79.34 | -25.08 | — | +0.00 | -25.08 | +97.24 | — |
| 2026-08-26 | `EZPW` | 98 | $35.23 | $35.70 | +46.06 | — | +0.00 | +46.06 | +63.70 | — |
| 2026-08-26 | `FNV` | 39 | — | $267.02 | +0.00 | $267.37 | +13.65 | +13.65 | +0.00 | +13.65 |
| 2026-08-27 | `FNV` | 39 | $267.37 | $267.23 | -5.46 | — | +0.00 | -5.46 | +8.19 | — |
| 2026-08-27 | `ACMR` | 25 | — | $81.65 | +0.00 | $80.49 | -29.00 | -29.00 | +0.00 | -29.00 |
| 2026-08-27 | `MU` | 2 | — | $967.01 | +0.00 | $935.39 | -63.24 | -63.24 | +0.00 | -63.24 |
| 2026-08-27 | `ASML` | 1 | — | $1746.53 | +0.00 | $1735.01 | -11.52 | -11.52 | +0.00 | -11.52 |
| 2026-08-27 | `LRCX` | 6 | — | $318.88 | +0.00 | $318.58 | -1.80 | -1.80 | +0.00 | -1.80 |
| 2026-08-27 | `NVDA` | 9 | — | $222.86 | +0.00 | $227.98 | +46.08 | +46.08 | +0.00 | +46.08 |
| 2026-08-28 | `ACMR` | 25 | $80.49 | $79.27 | -30.50 | — | +0.00 | -30.50 | -59.50 | — |
| 2026-08-28 | `MU` | 2 | $935.39 | $919.29 | -32.20 | — | +0.00 | -32.20 | -95.44 | — |
| 2026-08-28 | `ASML` | 1 | $1735.01 | $1734.75 | -0.26 | — | +0.00 | -0.26 | -11.78 | — |
| 2026-08-28 | `LRCX` | 6 | $318.58 | $318.03 | -3.30 | — | +0.00 | -3.30 | -5.10 | — |
| 2026-08-28 | `NVDA` | 9 | $227.98 | $227.36 | -5.58 | — | +0.00 | -5.58 | +40.50 | — |
| 2026-08-28 | `KEYS` | 4 | — | $324.41 | +0.00 | $319.97 | -17.76 | -17.76 | +0.00 | -17.76 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `DDOG` | 5 | — | $240.22 | +0.00 | $236.98 | -16.20 | -16.20 | +0.00 | -16.20 |
| 2026-08-28 | `ADSK` | 4 | — | $261.16 | +0.00 | $260.66 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-08-28 | `SEDG` | 39 | — | $32.90 | +0.00 | $31.41 | -58.11 | -58.11 | +0.00 | -58.11 |
| 2026-08-28 | `TLS` | 270 | — | $4.82 | +0.00 | $4.79 | -8.10 | -8.10 | +0.00 | -8.10 |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | — | +0.00 | +10.08 | -7.68 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -65.94 | — |
| 2026-08-31 | `DDOG` | 5 | $236.98 | $233.97 | -15.07 | — | +0.00 | -15.07 | -31.27 | — |
| 2026-08-31 | `ADSK` | 4 | $260.66 | $257.71 | -11.80 | — | +0.00 | -11.80 | -13.80 | — |
| 2026-08-31 | `SEDG` | 39 | $31.41 | $31.15 | -10.14 | — | +0.00 | -10.14 | -68.25 | — |
| 2026-08-31 | `TLS` | 270 | $4.79 | $4.81 | +5.40 | — | +0.00 | +5.40 | -2.70 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 4 | — | $351.74 | +0.00 | $357.16 | +21.68 | +21.68 | +0.00 | +21.68 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `CXW` | 44 | — | $32.31 | +0.00 | $33.66 | +59.40 | +59.40 | +0.00 | +59.40 |
| 2026-09-03 | `FRNM` | 91 | — | $15.87 | +0.00 | $16.90 | +93.73 | +93.73 | +0.00 | +93.73 |
| 2026-09-03 | `MMED` | 60 | — | $23.88 | +0.00 | $23.84 | -2.40 | -2.40 | +0.00 | -2.40 |
| 2026-09-03 | `DE` | 2 | — | $703.25 | +0.00 | $694.41 | -17.68 | -17.68 | +0.00 | -17.68 |
| 2026-09-03 | `HPE` | 30 | — | $47.60 | +0.00 | $54.44 | +205.20 | +205.20 | +0.00 | +205.20 |
| 2026-09-04 | `AVGO` | 4 | $357.16 | $359.70 | +10.16 | — | +0.00 | +10.16 | +31.84 | — |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | — | +0.00 | -5.22 | +54.94 | — |
| 2026-09-04 | `CXW` | 44 | $33.66 | $33.46 | -8.80 | — | +0.00 | -8.80 | +50.60 | — |
| 2026-09-04 | `FRNM` | 91 | $16.90 | $16.40 | -45.50 | $16.31 | -8.19 | -53.69 | +48.23 | +40.04 |
| 2026-09-04 | `MMED` | 60 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.40 | — |
| 2026-09-04 | `DE` | 2 | $694.41 | $692.03 | -4.76 | — | +0.00 | -4.76 | -22.44 | — |
| 2026-09-04 | `HPE` | 30 | $54.44 | $53.85 | -17.70 | — | +0.00 | -17.70 | +187.50 | — |
| 2026-09-04 | `CRM` | 8 | — | $263.36 | +0.00 | $259.23 | -33.04 | -33.04 | +0.00 | -33.04 |
| 2026-09-04 | `MRX` | 29 | — | $75.65 | +0.00 | $78.27 | +75.98 | +75.98 | +0.00 | +75.98 |
| 2026-09-04 | `BE` | 9 | — | $236.82 | +0.00 | $252.87 | +144.45 | +144.45 | +0.00 | +144.45 |
| 2026-09-04 | `BAK` | 1155 | — | $1.94 | +0.00 | $1.89 | -57.75 | -57.75 | +0.00 | -57.75 |
| 2026-09-08 | `FRNM` | 91 | $16.31 | $16.74 | +39.13 | — | +0.00 | +39.13 | +79.17 | — |
| 2026-09-08 | `CRM` | 8 | $259.23 | $253.72 | -44.08 | — | +0.00 | -44.08 | -77.12 | — |
| 2026-09-08 | `MRX` | 29 | $78.27 | $78.84 | +16.53 | $76.71 | -61.77 | -45.24 | +92.51 | +30.74 |
| 2026-09-08 | `BE` | 9 | $252.87 | $267.76 | +134.01 | — | +0.00 | +134.01 | +278.46 | — |
| 2026-09-08 | `BAK` | 1155 | $1.89 | $1.94 | +57.75 | — | +0.00 | +57.75 | +0.00 | — |
| 2026-09-09 | `MRX` | 29 | $76.71 | $76.60 | -3.19 | — | +0.00 | -3.19 | +27.55 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 64 | — | $164.43 | +0.00 | $150.28 | -905.60 | -905.60 | +0.00 | -905.60 |
| 2026-09-14 | `ORCL` | 64 | $150.28 | $141.42 | -567.04 | — | +0.00 | -567.04 | -1472.64 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +169.01 | HLIT, SNDK, ANGX, MH, VELO | — | $356.57 | $10,153.78 | HLIT×151, SNDK×1, ANGX×464, MH×147, VELO×130 |
| 2026-08-17 | +2.25 | $356.57 | HLIT×151, SNDK×1, ANGX×464, MH×147, VELO×130 | $10,302.57 | +148.79 | +241.42 | DVN, EOG, FANG | HLIT, SNDK, ANGX, MH, VELO | $193.79 | $10,522.21 | DVN×74, EOG×24, FANG×16 |
| 2026-08-18 | -6.20 | $193.79 | DVN×74, EOG×24, FANG×16 | $10,641.63 | +119.42 | +0.00 | — | DVN, EOG, FANG | $10,635.20 | $10,635.20 | — |
| 2026-08-19 | -7.20 | $10,635.20 | — | $10,635.20 | +0.00 | +0.00 | — | — | $10,635.20 | $10,635.20 | — |
| 2026-08-20 | +1.12 | $10,635.20 | — | $10,635.20 | +0.00 | -179.02 | BHP, APA, AUTL, CRSP, ASST, MRNA, ZLAB | — | $174.19 | $10,435.60 | BHP×16, APA×33, AUTL×615, CRSP×25, ASST×94, MRNA×10, ZLAB×57 |
| 2026-08-21 | +3.25 | $174.19 | BHP×16, APA×33, AUTL×615, CRSP×25, ASST×94, MRNA×10, ZLAB×57 | $10,674.31 | +238.71 | -158.24 | AU, FUTU, GRAL, ABTC, HIVE, MARA | BHP, APA, ASST, MRNA, ZLAB | $102.37 | $10,489.48 | AUTL×615, CRSP×25, AU×10, FUTU×11, GRAL×16, ABTC×147, HIVE×393, MARA×108 |
| 2026-08-24 | -5.17 | $102.37 | AUTL×615, CRSP×25, AU×10, FUTU×11, GRAL×16, ABTC×147, HIVE×393, MARA×108 | $10,450.57 | -38.91 | -41.87 | — | AUTL, AU, FUTU, GRAL, ABTC, HIVE, MARA | $8,957.68 | $10,384.56 | CRSP×25 |
| 2026-08-25 | +1.80 | $8,957.68 | CRSP×25 | $10,405.93 | +21.37 | +281.19 | AU, FCX, EZPW | CRSP | $131.66 | $10,678.55 | AU×29, FCX×44, EZPW×98 |
| 2026-08-26 | +2.02 | $131.66 | AU×29, FCX×44, EZPW×98 | $10,595.42 | -83.13 | +13.65 | FNV | AU, FCX, EZPW | $172.93 | $10,600.36 | FNV×39 |
| 2026-08-27 | — | $172.93 | FNV×39 | $10,594.90 | -5.46 | -59.48 | ACMR, MU, ASML, LRCX, NVDA | FNV | $941.80 | $10,523.14 | ACMR×25, MU×2, ASML×1, LRCX×6, NVDA×9 |
| 2026-08-28 | +0.75 | $941.80 | ACMR×25, MU×2, ASML×1, LRCX×6, NVDA×9 | $10,451.30 | -71.84 | -263.42 | KEYS, SMTC, CIEN, DDOG, ADSK, SEDG, TLS | ACMR, MU, ASML, LRCX, NVDA | $1,820.50 | $10,162.06 | KEYS×4, SMTC×9, CIEN×3, DDOG×5, ADSK×4, SEDG×39, TLS×270 |
| 2026-08-31 | -5.85 | $1,820.50 | KEYS×4, SMTC×9, CIEN×3, DDOG×5, ADSK×4, SEDG×39, TLS×270 | $10,150.70 | -11.36 | +0.00 | — | KEYS, SMTC, CIEN, DDOG, ADSK, SEDG, TLS | $10,134.91 | $10,134.91 | — |
| 2026-09-01 | -6.30 | $10,134.91 | — | $10,134.91 | -0.00 | +0.00 | — | — | $10,134.91 | $10,134.91 | — |
| 2026-09-02 | -3.83 | $10,134.91 | — | $10,134.91 | -0.00 | +0.00 | — | — | $10,134.91 | $10,134.91 | — |
| 2026-09-03 | -0.90 | $10,134.91 | — | $10,134.91 | -0.00 | +420.09 | AVGO, DELL, CXW, FRNM, MMED, DE, HPE | — | $607.59 | $10,540.37 | AVGO×4, DELL×2, CXW×44, FRNM×91, MMED×60, DE×2, HPE×30 |
| 2026-09-04 | +2.25 | $607.59 | AVGO×4, DELL×2, CXW×44, FRNM×91, MMED×60, DE×2, HPE×30 | $10,468.55 | -71.82 | +121.45 | CRM, MRX, BE, BAK | AVGO, DELL, CXW, MMED, DE, HPE | $269.83 | $10,556.49 | FRNM×91, CRM×8, MRX×29, BE×9, BAK×1155 |
| 2026-09-08 | -11.47 | $269.83 | FRNM×91, CRM×8, MRX×29, BE×9, BAK×1155 | $10,759.83 | +203.34 | -61.77 | — | FRNM, CRM, BE, BAK | $8,451.99 | $10,676.58 | MRX×29 |
| 2026-09-09 | -13.95 | $8,451.99 | MRX×29 | $10,673.39 | -3.19 | +0.00 | — | MRX | $10,671.28 | $10,671.28 | — |
| 2026-09-10 | -13.28 | $10,671.28 | — | $10,671.28 | +0.00 | +0.00 | — | — | $10,671.28 | $10,671.28 | — |
| 2026-09-11 | +0.50 | $10,671.28 | — | $10,671.28 | +0.00 | -905.60 | ORCL | — | $145.58 | $9,763.50 | ORCL×64 |
| 2026-09-14 | -11.00 | $145.58 | ORCL×64 | $9,196.46 | -567.04 | +0.00 | — | ORCL | $9,194.20 | $9,194.20 | — |
| 2026-09-15 | -3.84 | $9,194.20 | — | $9,194.20 | -0.00 | +0.00 | — | — | $9,194.20 | $9,194.20 | — |
| 2026-09-16 | +5.30 | $9,194.20 | — | $9,194.20 | -0.00 | +0.00 | — | — | $9,194.20 | $9,194.20 | — |
| 2026-09-17 | +7.38 | $9,194.20 | — | $9,194.20 | -0.00 | +0.00 | — | — | $9,194.20 | $9,194.20 | — |
| 2026-09-18 | +4.86 | $9,194.20 | — | $9,194.20 | -0.00 | +0.00 | — | — | $9,194.20 | $9,194.20 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 151 | $13.18 | $2.44 | — | $8,007.38 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 1 | $1646.93 | $1.99 | — | $6,358.45 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $4,352.63 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 147 | $13.55 | $2.43 | — | $2,358.35 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 130 | $15.38 | $2.38 | — | $356.57 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $356.57 | ▲ close $10,153.78 vs 09:30 $10,000.00 (session +169.01) | 16:00 close · cash $356.57 · equity $10,153.78 vs 09:30 $10,000.00 (+153.78; session marks +169.01) · 5 name(s) marked open→close (per-name table). HLIT×151 09:30 $13.18 → close $13.92 +111.74; SNDK×1 09:30 $1646.93 → close $1641.11 -5.82; ANGX×464 09:30 $4.31 → close $4.37 +27.84; MH×147 09:30 $13.55 → close $13.10 -66.15; VELO×130 09:30 $15.38 → close $16.16 +101.40 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $356.57 | ▲ 09:30 equity $10,302.57 vs yday $10,153.78 (+148.79) | 09:30 open · cash $356.57 (unchanged overnight, no fees) · equity $10,302.57 vs prior close $10,153.78 (+148.79) · 5 name(s) re-marked at the open (per-name table). HLIT×151 yday $13.92 → 09:30 $13.84 -12.08; SNDK×1 yday $1641.11 → 09:30 $1700.74 +59.63; ANGX×464 yday $4.37 → 09:30 $4.60 +106.72; MH×147 yday $13.10 → 09:30 $13.16 +8.82; VELO×130 yday $16.16 → 09:30 $16.05 -14.30 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 151 | $13.84 | $2.48 | $+94.73 | $2,443.92 | ▲ +94.73 after sell → book $10,300.09; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SNDK` | 1 | $1700.74 | $2.02 | $+49.81 | $4,142.65 | ▲ +49.81 after sell → book $10,298.07; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 464 | $4.60 | $6.08 | $+122.49 | $6,270.97 | ▲ +122.49 after sell → book $10,291.99; vs 09:30 mark -6.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 147 | $13.16 | $2.47 | $-62.23 | $8,203.02 | ▼ -62.23 after sell → book $10,289.52; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VELO` | 130 | $16.05 | $2.42 | $+82.30 | $10,287.10 | ▲ +82.30 after sell → book $10,287.10; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 74 | $46.18 | $2.21 | — | $6,867.57 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+6.7; leftover $3429.03 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 24 | $142.77 | $2.06 | — | $3,439.03 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+5.8; leftover $3429.03 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 16 | $202.70 | $2.04 | — | $193.79 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+8.3; leftover $3429.03 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $193.79 | ▲ close $10,522.21 vs 09:30 $10,302.57 (session +241.42) | 16:00 close · cash $193.79 · equity $10,522.21 vs 09:30 $10,302.57 (+219.64; session marks +241.42) · 3 name(s) marked open→close (per-name table). DVN×74 09:30 $46.18 → close $47.57 +102.86; EOG×24 09:30 $142.77 → close $146.15 +81.12; FANG×16 09:30 $202.70 → close $206.29 +57.44 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $193.79 | ▲ 09:30 equity $10,641.63 vs yday $10,522.21 (+119.42) | 09:30 open · cash $193.79 (unchanged overnight, no fees) · equity $10,641.63 vs prior close $10,522.21 (+119.42) · 3 name(s) re-marked at the open (per-name table). DVN×74 yday $47.57 → 09:30 $48.00 +31.82; EOG×24 yday $146.15 → 09:30 $148.04 +45.36; FANG×16 yday $206.29 → 09:30 $208.93 +42.24 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 74 | $48.00 | $2.25 | $+130.22 | $3,743.54 | ▲ +130.22 after sell → book $10,639.38; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 24 | $148.04 | $2.10 | $+122.32 | $7,294.40 | ▲ +122.32 after sell → book $10,637.28; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 16 | $208.93 | $2.07 | $+95.57 | $10,635.20 | ▲ +95.57 after sell → book $10,635.20; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,635.20 | ▲ close $10,635.20 vs 09:30 $10,641.63 (session +0.00) | 16:00 close · cash $10,635.20 · no lots left · equity $10,635.20. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,635.20 | ▲ 09:30 equity $10,635.20 vs yday $10,635.20 (+0.00) | 09:30 open · cash $10,635.20 · no holdings · equity $10,635.20 vs prior close $10,635.20 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,635.20 | ▲ close $10,635.20 vs 09:30 $10,635.20 (session +0.00) | 16:00 close · cash $10,635.20 · no lots left · equity $10,635.20. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,635.20 | ▲ 09:30 equity $10,635.20 vs yday $10,635.20 (+0.00) | 09:30 open · cash $10,635.20 · no holdings · equity $10,635.20 vs prior close $10,635.20 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 16 | $91.01 | $2.04 | — | $9,177.00 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1519.31 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 33 | $44.76 | $2.09 | — | $7,697.84 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1519.31 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 615 | $2.47 | $7.93 | — | $6,170.85 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1519.31 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 25 | $58.73 | $2.06 | — | $4,700.54 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1519.31 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 94 | $16.00 | $2.27 | — | $3,194.26 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1519.31 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 10 | $150.14 | $2.02 | — | $1,690.84 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1519.31 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 57 | $26.57 | $2.16 | — | $174.19 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1519.31 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.19 | ▼ close $10,435.60 vs 09:30 $10,635.20 (session -179.02) | 16:00 close · cash $174.19 · equity $10,435.60 vs 09:30 $10,635.20 (-199.60; session marks -179.02) · 7 name(s) marked open→close (per-name table). BHP×16 09:30 $91.01 → close $93.63 +41.92; APA×33 09:30 $44.76 → close $44.39 -12.21; AUTL×615 09:30 $2.47 → close $2.46 -6.15; CRSP×25 09:30 $58.73 → close $58.12 -15.25; ASST×94 09:30 $16.00 → close $16.13 +12.22; MRNA×10 09:30 $150.14 → close $133.32 -168.20; ZLAB×57 09:30 $26.57 → close $26.02 -31.35 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.19 | ▲ 09:30 equity $10,674.31 vs yday $10,435.60 (+238.71) | 09:30 open · cash $174.19 (unchanged overnight, no fees) · equity $10,674.31 vs prior close $10,435.60 (+238.71) · 7 name(s) re-marked at the open (per-name table). BHP×16 yday $93.63 → 09:30 $95.72 +33.44; APA×33 yday $44.39 → 09:30 $44.52 +4.29; AUTL×615 yday $2.46 → 09:30 $2.47 +6.15; CRSP×25 yday $58.12 → 09:30 $59.72 +40.00; ASST×94 yday $16.13 → 09:30 $17.66 +143.82; MRNA×10 yday $133.32 → 09:30 $133.11 -2.10; ZLAB×57 yday $26.02 → 09:30 $26.25 +13.11 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 16 | $95.72 | $2.06 | $+71.26 | $1,703.65 | ▲ +71.26 after sell → book $10,672.25; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 33 | $44.52 | $2.11 | $-12.12 | $3,170.70 | ▼ -12.12 after sell → book $10,670.14; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 94 | $17.66 | $2.30 | $+151.47 | $4,828.44 | ▲ +151.47 after sell → book $10,667.84; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 10 | $133.11 | $2.04 | $-174.36 | $6,157.50 | ▼ -174.36 after sell → book $10,665.80; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 57 | $26.25 | $2.18 | $-22.58 | $7,651.57 | ▼ -22.58 after sell → book $10,663.62; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,455.25 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1275.26 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $5,186.25 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1275.26 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 16 | $78.88 | $2.04 | — | $3,922.13 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1275.26 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 147 | $8.66 | $2.43 | — | $2,646.68 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1275.26 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 393 | $3.24 | $5.07 | — | $1,368.29 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $1275.26 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 108 | $11.70 | $2.31 | — | $102.37 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1275.26 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $102.37 | ▼ close $10,489.48 vs 09:30 $10,674.31 (session -158.24) | 16:00 close · cash $102.37 · equity $10,489.48 vs 09:30 $10,674.31 (-184.83; session marks -158.24) · 8 name(s) marked open→close (per-name table). AUTL×615 09:30 $2.47 → close $2.41 -36.90; CRSP×25 09:30 $59.72 → close $59.50 -5.50; AU×10 09:30 $119.43 → close $121.22 +17.90; FUTU×11 09:30 $115.18 → close $123.64 +93.06; GRAL×16 09:30 $78.88 → close $79.54 +10.56; ABTC×147 09:30 $8.66 → close $7.93 -107.31; HIVE×393 09:30 $3.24 → close $3.03 -82.53; MARA×108 09:30 $11.70 → close $11.26 -47.52 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $102.37 | ▼ 09:30 equity $10,450.57 vs yday $10,489.48 (-38.91) | 09:30 open · cash $102.37 (unchanged overnight, no fees) · equity $10,450.57 vs prior close $10,489.48 (-38.91) · 8 name(s) re-marked at the open (per-name table). AUTL×615 yday $2.41 → 09:30 $2.40 -6.15; CRSP×25 yday $59.50 → 09:30 $58.75 -18.75; AU×10 yday $121.22 → 09:30 $120.51 -7.10; FUTU×11 yday $123.64 → 09:30 $121.00 -29.04; GRAL×16 yday $79.54 → 09:30 $81.87 +37.28; ABTC×147 yday $7.93 → 09:30 $8.00 +10.29; HIVE×393 yday $3.03 → 09:30 $2.99 -15.72; MARA×108 yday $11.26 → 09:30 $11.17 -9.72 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 615 | $2.40 | $8.05 | $-59.03 | $1,570.32 | ▼ -59.03 after sell → book $10,442.52; vs 09:30 mark -8.05 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $2,773.38 | ▲ +6.74 after sell → book $10,440.48; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $121.00 | $2.04 | $+59.95 | $4,102.34 | ▲ +59.95 after sell → book $10,438.44; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 16 | $81.87 | $2.06 | $+43.74 | $5,410.20 | ▲ +43.74 after sell → book $10,436.38; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 147 | $8.00 | $2.47 | $-101.92 | $6,583.74 | ▼ -101.92 after sell → book $10,433.92; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 393 | $2.99 | $5.14 | $-108.46 | $7,753.66 | ▼ -108.46 after sell → book $10,428.77; vs 09:30 mark -5.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 108 | $11.17 | $2.34 | $-61.90 | $8,957.68 | ▼ -61.90 after sell → book $10,426.43; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,957.68 | ▼ close $10,384.56 vs 09:30 $10,450.57 (session -41.87) | 16:00 close · cash $8,957.68 · equity $10,384.56 vs 09:30 $10,450.57 (-66.01; session marks -41.87) · 1 name(s) marked open→close (per-name table). CRSP×25 09:30 $58.75 → close $57.08 -41.87 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,957.68 | ▲ 09:30 equity $10,405.93 vs yday $10,384.56 (+21.37) | 09:30 open · cash $8,957.68 (unchanged overnight, no fees) · equity $10,405.93 vs prior close $10,384.56 (+21.37) · 1 name(s) re-marked at the open (per-name table). CRSP×25 yday $57.08 → 09:30 $57.93 +21.37 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 25 | $57.93 | $2.09 | $-24.15 | $10,403.84 | ▼ -24.15 after sell → book $10,403.84; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 29 | $118.52 | $2.08 | — | $6,964.69 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $3467.95 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 44 | $77.13 | $2.12 | — | $3,568.85 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3467.95 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 98 | $35.05 | $2.28 | — | $131.66 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $3467.95 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.66 | ▲ close $10,678.55 vs 09:30 $10,405.93 (session +281.19) | 16:00 close · cash $131.66 · equity $10,678.55 vs 09:30 $10,405.93 (+272.62; session marks +281.19) · 3 name(s) marked open→close (per-name table). AU×29 09:30 $118.52 → close $123.39 +141.23; FCX×44 09:30 $77.13 → close $79.91 +122.32; EZPW×98 09:30 $35.05 → close $35.23 +17.64 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.66 | ▼ 09:30 equity $10,595.42 vs yday $10,678.55 (-83.13) | 09:30 open · cash $131.66 (unchanged overnight, no fees) · equity $10,595.42 vs prior close $10,678.55 (-83.13) · 3 name(s) re-marked at the open (per-name table). AU×29 yday $123.39 → 09:30 $119.80 -104.11; FCX×44 yday $79.91 → 09:30 $79.34 -25.08; EZPW×98 yday $35.23 → 09:30 $35.70 +46.06 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 29 | $119.80 | $2.11 | $+32.93 | $3,603.75 | ▲ +32.93 after sell → book $10,593.31; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 44 | $79.34 | $2.16 | $+92.96 | $7,092.55 | ▲ +92.96 after sell → book $10,591.15; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 98 | $35.70 | $2.33 | $+59.09 | $10,588.82 | ▲ +59.09 after sell → book $10,588.82; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 39 | $267.02 | $2.11 | — | $172.93 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $10588.82 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $172.93 | ▲ close $10,600.36 vs 09:30 $10,595.42 (session +13.65) | 16:00 close · cash $172.93 · equity $10,600.36 vs 09:30 $10,595.42 (+4.94; session marks +13.65) · 1 name(s) marked open→close (per-name table). FNV×39 09:30 $267.02 → close $267.37 +13.65 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $172.93 | ▼ 09:30 equity $10,594.90 vs yday $10,600.36 (-5.46) | 09:30 open · cash $172.93 (unchanged overnight, no fees) · equity $10,594.90 vs prior close $10,600.36 (-5.46) · 1 name(s) re-marked at the open (per-name table). FNV×39 yday $267.37 → 09:30 $267.23 -5.46 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 39 | $267.23 | $2.20 | $+3.88 | $10,592.70 | ▲ +3.88 after sell → book $10,592.70; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 25 | $81.65 | $2.06 | — | $8,549.39 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $2118.54 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 2 | $967.01 | $2.00 | — | $6,613.37 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $2118.54 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ASML` | 1 | $1746.53 | $1.99 | — | $4,864.85 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=-0.3; leftover $2118.54 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 6 | $318.88 | $2.01 | — | $2,949.56 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $2118.54 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 9 | $222.86 | $2.02 | — | $941.80 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $2118.54 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $941.80 | ▼ close $10,523.14 vs 09:30 $10,594.90 (session -59.48) | 16:00 close · cash $941.80 · equity $10,523.14 vs 09:30 $10,594.90 (-71.76; session marks -59.48) · 5 name(s) marked open→close (per-name table). ACMR×25 09:30 $81.65 → close $80.49 -29.00; MU×2 09:30 $967.01 → close $935.39 -63.24; ASML×1 09:30 $1746.53 → close $1735.01 -11.52; LRCX×6 09:30 $318.88 → close $318.58 -1.80; NVDA×9 09:30 $222.86 → close $227.98 +46.08 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $941.80 | ▼ 09:30 equity $10,451.30 vs yday $10,523.14 (-71.84) | 09:30 open · cash $941.80 (unchanged overnight, no fees) · equity $10,451.30 vs prior close $10,523.14 (-71.84) · 5 name(s) re-marked at the open (per-name table). ACMR×25 yday $80.49 → 09:30 $79.27 -30.50; MU×2 yday $935.39 → 09:30 $919.29 -32.20; ASML×1 yday $1735.01 → 09:30 $1734.75 -0.26; LRCX×6 yday $318.58 → 09:30 $318.03 -3.30; NVDA×9 yday $227.98 → 09:30 $227.36 -5.58 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 25 | $79.27 | $2.09 | $-63.66 | $2,921.46 | ▼ -63.66 after sell → book $10,449.21; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 2 | $919.29 | $2.02 | $-99.46 | $4,758.02 | ▼ -99.46 after sell → book $10,447.19; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ASML` | 1 | $1734.75 | $2.02 | $-15.79 | $6,490.75 | ▼ -15.79 after sell → book $10,445.17; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 6 | $318.03 | $2.03 | $-9.14 | $8,396.90 | ▼ -9.14 after sell → book $10,443.14; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 9 | $227.36 | $2.04 | $+36.44 | $10,441.10 | ▲ +36.44 after sell → book $10,441.10; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $9,141.45 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1305.14 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $7,863.60 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1305.14 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,660.34 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1305.14 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $5,457.23 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1305.14 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $4,410.59 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; ret5=+7.8; leftover $1305.14 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $32.90 | $2.11 | — | $3,125.38 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1305.14 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 270 | $4.82 | $3.48 | — | $1,820.50 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1305.14 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,820.50 | ▼ close $10,162.06 vs 09:30 $10,451.30 (session -263.42) | 16:00 close · cash $1,820.50 · equity $10,162.06 vs 09:30 $10,451.30 (-289.24; session marks -263.42) · 7 name(s) marked open→close (per-name table). KEYS×4 09:30 $324.41 → close $319.97 -17.76; SMTC×9 09:30 $141.76 → close $131.17 -95.31; CIEN×3 09:30 $400.42 → close $378.44 -65.94; DDOG×5 09:30 $240.22 → close $236.98 -16.20; ADSK×4 09:30 $261.16 → close $260.66 -2.00; SEDG×39 09:30 $32.90 → close $31.41 -58.11; TLS×270 09:30 $4.82 → close $4.79 -8.10 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,820.50 | ▼ 09:30 equity $10,150.70 vs yday $10,162.06 (-11.36) | 09:30 open · cash $1,820.50 (unchanged overnight, no fees) · equity $10,150.70 vs prior close $10,162.06 (-11.36) · 7 name(s) re-marked at the open (per-name table). KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; DDOG×5 yday $236.98 → 09:30 $233.97 -15.07; ADSK×4 yday $260.66 → 09:30 $257.71 -11.80; SEDG×39 yday $31.41 → 09:30 $31.15 -10.14; TLS×270 yday $4.79 → 09:30 $4.81 +5.40 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $3,108.44 | ▼ -11.70 after sell → book $10,148.67; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $4,297.10 | ▼ -89.19 after sell → book $10,146.64; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $5,430.40 | ▼ -69.96 after sell → book $10,144.62; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 5 | $233.97 | $2.02 | $-35.31 | $6,598.20 | ▼ -35.31 after sell → book $10,142.59; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $257.71 | $2.02 | $-17.82 | $7,627.02 | ▼ -17.82 after sell → book $10,140.57; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 39 | $31.15 | $2.13 | $-72.48 | $8,839.74 | ▼ -72.48 after sell → book $10,138.44; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 270 | $4.81 | $3.54 | $-9.72 | $10,134.91 | ▼ -9.72 after sell → book $10,134.91; vs 09:30 mark -3.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,134.91 | ▲ close $10,134.91 vs 09:30 $10,150.70 (session +0.00) | 16:00 close · cash $10,134.91 · no lots left · equity $10,134.91. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,134.91 | ▲ 09:30 equity $10,134.91 vs yday $10,134.91 (-0.00) | 09:30 open · cash $10,134.91 · no holdings · equity $10,134.91 vs prior close $10,134.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,134.91 | ▲ close $10,134.91 vs 09:30 $10,134.91 (session +0.00) | 16:00 close · cash $10,134.91 · no lots left · equity $10,134.91. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,134.91 | ▲ 09:30 equity $10,134.91 vs yday $10,134.91 (-0.00) | 09:30 open · cash $10,134.91 · no holdings · equity $10,134.91 vs prior close $10,134.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,134.91 | ▲ close $10,134.91 vs 09:30 $10,134.91 (session +0.00) | 16:00 close · cash $10,134.91 · no lots left · equity $10,134.91. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,134.91 | ▲ 09:30 equity $10,134.91 vs yday $10,134.91 (-0.00) | 09:30 open · cash $10,134.91 · no holdings · equity $10,134.91 vs prior close $10,134.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 4 | $351.74 | $2.00 | — | $8,725.94 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1447.84 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,751.33 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1447.84 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 44 | $32.31 | $2.12 | — | $6,327.57 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1447.84 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 91 | $15.87 | $2.26 | — | $4,881.13 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1447.84 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 60 | $23.88 | $2.17 | — | $3,446.16 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1447.84 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $2,037.67 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1447.84 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 30 | $47.60 | $2.08 | — | $607.59 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1447.84 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $607.59 | ▲ close $10,540.37 vs 09:30 $10,134.91 (session +420.09) | 16:00 close · cash $607.59 · equity $10,540.37 vs 09:30 $10,134.91 (+405.46; session marks +420.09) · 7 name(s) marked open→close (per-name table). AVGO×4 09:30 $351.74 → close $357.16 +21.68; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×44 09:30 $32.31 → close $33.66 +59.40; FRNM×91 09:30 $15.87 → close $16.90 +93.73; MMED×60 09:30 $23.88 → close $23.84 -2.40; DE×2 09:30 $703.25 → close $694.41 -17.68; HPE×30 09:30 $47.60 → close $54.44 +205.20 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $607.59 | ▼ 09:30 equity $10,468.55 vs yday $10,540.37 (-71.82) | 09:30 open · cash $607.59 (unchanged overnight, no fees) · equity $10,468.55 vs prior close $10,540.37 (-71.82) · 7 name(s) re-marked at the open (per-name table). AVGO×4 yday $357.16 → 09:30 $359.70 +10.16; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×44 yday $33.66 → 09:30 $33.46 -8.80; FRNM×91 yday $16.90 → 09:30 $16.40 -45.50; MMED×60 yday $23.84 → 09:30 $23.84 +0.00; DE×2 yday $694.41 → 09:30 $692.03 -4.76; HPE×30 yday $54.44 → 09:30 $53.85 -17.70 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 4 | $359.70 | $2.02 | $+27.81 | $2,044.36 | ▲ +27.81 after sell → book $10,466.52; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $3,069.91 | ▲ +50.93 after sell → book $10,464.51; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 44 | $33.46 | $2.14 | $+46.33 | $4,540.00 | ▲ +46.33 after sell → book $10,462.36; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 60 | $23.84 | $2.19 | $-6.76 | $5,968.21 | ▼ -6.76 after sell → book $10,460.17; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 2 | $692.03 | $2.02 | $-26.45 | $7,350.26 | ▼ -26.45 after sell → book $10,458.16; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 30 | $53.85 | $2.10 | $+183.32 | $8,963.65 | ▲ +183.32 after sell → book $10,456.05; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 8 | $263.36 | $2.01 | — | $6,854.76 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $2240.91 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 29 | $75.65 | $2.08 | — | $4,658.83 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $2240.91 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 9 | $236.82 | $2.02 | — | $2,525.43 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+8.1; leftover $2240.91 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 1155 | $1.94 | $14.90 | — | $269.83 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $2240.91 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $269.83 | ▲ close $10,556.49 vs 09:30 $10,468.55 (session +121.45) | 16:00 close · cash $269.83 · equity $10,556.49 vs 09:30 $10,468.55 (+87.94; session marks +121.45) · 5 name(s) marked open→close (per-name table). FRNM×91 09:30 $16.40 → close $16.31 -8.19; CRM×8 09:30 $263.36 → close $259.23 -33.04; MRX×29 09:30 $75.65 → close $78.27 +75.98; BE×9 09:30 $236.82 → close $252.87 +144.45; BAK×1155 09:30 $1.94 → close $1.89 -57.75 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $269.83 | ▲ 09:30 equity $10,759.83 vs yday $10,556.49 (+203.34) | 09:30 open · cash $269.83 (unchanged overnight, no fees) · equity $10,759.83 vs prior close $10,556.49 (+203.34) · 5 name(s) re-marked at the open (per-name table). FRNM×91 yday $16.31 → 09:30 $16.74 +39.13; CRM×8 yday $259.23 → 09:30 $253.72 -44.08; MRX×29 yday $78.27 → 09:30 $78.84 +16.53; BE×9 yday $252.87 → 09:30 $267.76 +134.01; BAK×1155 yday $1.89 → 09:30 $1.94 +57.75 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 91 | $16.74 | $2.29 | $+74.62 | $1,790.88 | ▲ +74.62 after sell → book $10,757.54; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 8 | $253.72 | $2.04 | $-81.17 | $3,818.60 | ▼ -81.17 after sell → book $10,755.50; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 9 | $267.76 | $2.05 | $+274.40 | $6,226.40 | ▲ +274.40 after sell → book $10,753.46; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 1155 | $1.94 | $15.11 | $-30.01 | $8,451.99 | ▼ -30.01 after sell → book $10,738.35; vs 09:30 mark -15.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,451.99 | ▼ close $10,676.58 vs 09:30 $10,759.83 (session -61.77) | 16:00 close · cash $8,451.99 · equity $10,676.58 vs 09:30 $10,759.83 (-83.25; session marks -61.77) · 1 name(s) marked open→close (per-name table). MRX×29 09:30 $78.84 → close $76.71 -61.77 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,451.99 | ▼ 09:30 equity $10,673.39 vs yday $10,676.58 (-3.19) | 09:30 open · cash $8,451.99 (unchanged overnight, no fees) · equity $10,673.39 vs prior close $10,676.58 (-3.19) · 1 name(s) re-marked at the open (per-name table). MRX×29 yday $76.71 → 09:30 $76.60 -3.19 | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 29 | $76.60 | $2.10 | $+23.37 | $10,671.28 | ▲ +23.37 after sell → book $10,671.28; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,671.28 | ▲ close $10,671.28 vs 09:30 $10,673.39 (session +0.00) | 16:00 close · cash $10,671.28 · no lots left · equity $10,671.28. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,671.28 | ▲ 09:30 equity $10,671.28 vs yday $10,671.28 (+0.00) | 09:30 open · cash $10,671.28 · no holdings · equity $10,671.28 vs prior close $10,671.28 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,671.28 | ▲ close $10,671.28 vs 09:30 $10,671.28 (session +0.00) | 16:00 close · cash $10,671.28 · no lots left · equity $10,671.28. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,671.28 | ▲ 09:30 equity $10,671.28 vs yday $10,671.28 (+0.00) | 09:30 open · cash $10,671.28 · no holdings · equity $10,671.28 vs prior close $10,671.28 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 64 | $164.43 | $2.18 | — | $145.58 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $10671.28 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $145.58 | ▼ close $9,763.50 vs 09:30 $10,671.28 (session -905.60) | 16:00 close · cash $145.58 · equity $9,763.50 vs 09:30 $10,671.28 (-907.78; session marks -905.60) · 1 name(s) marked open→close (per-name table). ORCL×64 09:30 $164.43 → close $150.28 -905.60 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $145.58 | ▼ 09:30 equity $9,196.46 vs yday $9,763.50 (-567.04) | 09:30 open · cash $145.58 (unchanged overnight, no fees) · equity $9,196.46 vs prior close $9,763.50 (-567.04) · 1 name(s) re-marked at the open (per-name table). ORCL×64 yday $150.28 → 09:30 $141.42 -567.04 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 64 | $141.42 | $2.27 | $-1477.09 | $9,194.20 | ▼ -1,477.09 after sell → book $9,194.20; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,194.20 | ▲ close $9,194.20 vs 09:30 $9,196.46 (session +0.00) | 16:00 close · cash $9,194.20 · no lots left · equity $9,194.20. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,194.20 | ▲ 09:30 equity $9,194.20 vs yday $9,194.20 (-0.00) | 09:30 open · cash $9,194.20 · no holdings · equity $9,194.20 vs prior close $9,194.20 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,194.20 | ▲ close $9,194.20 vs 09:30 $9,194.20 (session +0.00) | 16:00 close · cash $9,194.20 · no lots left · equity $9,194.20. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,194.20 | ▲ 09:30 equity $9,194.20 vs yday $9,194.20 (-0.00) | 09:30 open · cash $9,194.20 · no holdings · equity $9,194.20 vs prior close $9,194.20 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,194.20 | ▲ close $9,194.20 vs 09:30 $9,194.20 (session +0.00) | 16:00 close · cash $9,194.20 · no lots left · equity $9,194.20. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,194.20 | ▲ 09:30 equity $9,194.20 vs yday $9,194.20 (-0.00) | 09:30 open · cash $9,194.20 · no holdings · equity $9,194.20 vs prior close $9,194.20 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,194.20 | ▲ close $9,194.20 vs 09:30 $9,194.20 (session +0.00) | 16:00 close · cash $9,194.20 · no lots left · equity $9,194.20. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,194.20 | ▲ 09:30 equity $9,194.20 vs yday $9,194.20 (-0.00) | 09:30 open · cash $9,194.20 · no holdings · equity $9,194.20 vs prior close $9,194.20 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,194.20 | ▲ close $9,194.20 vs 09:30 $9,194.20 (session +0.00) | 16:00 close · cash $9,194.20 · no lots left · equity $9,194.20. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-28 | `MPWR` | cash | leftover split 1305.14 < 1 share @ 1306.03 |
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
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
