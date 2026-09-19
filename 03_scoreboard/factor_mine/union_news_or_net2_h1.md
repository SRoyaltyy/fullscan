# Factor mine action — `union_news_or_net2_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 2

Cash book **-1.27%** ($9,873) · signal-only (no cash/fees) was +2.21%. Starts YES **7/26**. Fills 130 · skips 39 · realized $-218.69.

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
- Must-have: camera net (+G −R) is at least 2.
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
- **Gate** `news_or_headline=True,cam_net_min=2` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $12.03.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `HLIT` | 94 | — | $13.18 | +0.00 | $13.92 | +69.56 | +69.56 | +0.00 | +69.56 |
| 2026-08-14 | `ANGX` | 290 | — | $4.31 | +0.00 | $4.37 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-08-14 | `MH` | 92 | — | $13.55 | +0.00 | $13.10 | -41.40 | -41.40 | +0.00 | -41.40 |
| 2026-08-14 | `VELO` | 81 | — | $15.38 | +0.00 | $16.16 | +63.18 | +63.18 | +0.00 | +63.18 |
| 2026-08-14 | `ARX` | 63 | — | $19.57 | +0.00 | $19.58 | +0.63 | +0.63 | +0.00 | +0.63 |
| 2026-08-14 | `NRG` | 10 | — | $120.00 | +0.00 | $126.24 | +62.40 | +62.40 | +0.00 | +62.40 |
| 2026-08-14 | `S` | 52 | — | $23.77 | +0.00 | $23.11 | -34.58 | -34.58 | +0.00 | -34.58 |
| 2026-08-17 | `HLIT` | 94 | $13.92 | $13.84 | -7.52 | — | +0.00 | -7.52 | +62.04 | — |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | — | +0.00 | +66.70 | +84.10 | — |
| 2026-08-17 | `MH` | 92 | $13.10 | $13.16 | +5.52 | — | +0.00 | +5.52 | -35.88 | — |
| 2026-08-17 | `VELO` | 81 | $16.16 | $16.05 | -8.91 | — | +0.00 | -8.91 | +54.27 | — |
| 2026-08-17 | `ARX` | 63 | $19.58 | $19.57 | -0.63 | — | +0.00 | -0.63 | +0.00 | — |
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
| 2026-08-20 | `TEAM` | 7 | — | $173.90 | +0.00 | $174.91 | +7.07 | +7.07 | +0.00 | +7.07 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `APA` | 28 | $44.39 | $44.52 | +3.64 | — | +0.00 | +3.64 | -6.72 | — |
| 2026-08-21 | `AUTL` | 515 | $2.46 | $2.47 | +5.15 | $2.41 | -30.90 | -25.75 | +0.00 | -30.90 |
| 2026-08-21 | `CRSP` | 21 | $58.12 | $59.72 | +33.60 | $59.50 | -4.62 | +28.98 | +20.79 | +16.17 |
| 2026-08-21 | `ASST` | 79 | $16.13 | $17.66 | +120.87 | — | +0.00 | +120.87 | +131.14 | — |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | — | +0.00 | -1.68 | -136.24 | — |
| 2026-08-21 | `ZLAB` | 47 | $26.02 | $26.25 | +10.81 | — | +0.00 | +10.81 | -15.04 | — |
| 2026-08-21 | `TEAM` | 7 | $174.91 | $174.22 | -4.83 | — | +0.00 | -4.83 | +2.24 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `FUTU` | 11 | — | $115.18 | +0.00 | $123.64 | +93.06 | +93.06 | +0.00 | +93.06 |
| 2026-08-21 | `GRAL` | 16 | — | $78.88 | +0.00 | $79.54 | +10.56 | +10.56 | +0.00 | +10.56 |
| 2026-08-21 | `ABTC` | 147 | — | $8.66 | +0.00 | $7.93 | -107.31 | -107.31 | +0.00 | -107.31 |
| 2026-08-21 | `HIVE` | 395 | — | $3.24 | +0.00 | $3.03 | -82.95 | -82.95 | +0.00 | -82.95 |
| 2026-08-21 | `MARA` | 109 | — | $11.70 | +0.00 | $11.26 | -47.96 | -47.96 | +0.00 | -47.96 |
| 2026-08-24 | `AUTL` | 515 | $2.41 | $2.40 | -5.15 | — | +0.00 | -5.15 | -36.05 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | $57.08 | -35.17 | -50.92 | +0.42 | -34.75 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `FUTU` | 11 | $123.64 | $121.00 | -29.04 | — | +0.00 | -29.04 | +64.02 | — |
| 2026-08-24 | `GRAL` | 16 | $79.54 | $81.87 | +37.28 | — | +0.00 | +37.28 | +47.84 | — |
| 2026-08-24 | `ABTC` | 147 | $7.93 | $8.00 | +10.29 | — | +0.00 | +10.29 | -97.02 | — |
| 2026-08-24 | `HIVE` | 395 | $3.03 | $2.99 | -15.80 | — | +0.00 | -15.80 | -98.75 | — |
| 2026-08-24 | `MARA` | 109 | $11.26 | $11.17 | -9.81 | — | +0.00 | -9.81 | -57.77 | — |
| 2026-08-25 | `CRSP` | 21 | $57.08 | $57.93 | +17.95 | — | +0.00 | +17.95 | -16.80 | — |
| 2026-08-25 | `AU` | 16 | — | $118.52 | +0.00 | $123.39 | +77.92 | +77.92 | +0.00 | +77.92 |
| 2026-08-25 | `FCX` | 25 | — | $77.13 | +0.00 | $79.91 | +69.50 | +69.50 | +0.00 | +69.50 |
| 2026-08-25 | `EZPW` | 56 | — | $35.05 | +0.00 | $35.23 | +10.08 | +10.08 | +0.00 | +10.08 |
| 2026-08-25 | `RUM` | 211 | — | $9.42 | +0.00 | $10.23 | +170.91 | +170.91 | +0.00 | +170.91 |
| 2026-08-25 | `ZYME` | 69 | — | $28.86 | +0.00 | $27.47 | -95.91 | -95.91 | +0.00 | -95.91 |
| 2026-08-26 | `AU` | 16 | $123.39 | $119.80 | -57.44 | — | +0.00 | -57.44 | +20.48 | — |
| 2026-08-26 | `FCX` | 25 | $79.91 | $79.34 | -14.25 | — | +0.00 | -14.25 | +55.25 | — |
| 2026-08-26 | `EZPW` | 56 | $35.23 | $35.70 | +26.32 | — | +0.00 | +26.32 | +36.40 | — |
| 2026-08-26 | `RUM` | 211 | $10.23 | $10.07 | -33.76 | — | +0.00 | -33.76 | +137.15 | — |
| 2026-08-26 | `ZYME` | 69 | $27.47 | $27.56 | +6.21 | — | +0.00 | +6.21 | -89.70 | — |
| 2026-08-26 | `FNV` | 37 | — | $267.02 | +0.00 | $267.37 | +12.95 | +12.95 | +0.00 | +12.95 |
| 2026-08-27 | `FNV` | 37 | $267.37 | $267.23 | -5.18 | — | +0.00 | -5.18 | +7.77 | — |
| 2026-08-27 | `ACMR` | 24 | — | $81.65 | +0.00 | $80.49 | -27.84 | -27.84 | +0.00 | -27.84 |
| 2026-08-27 | `MU` | 2 | — | $967.01 | +0.00 | $935.39 | -63.24 | -63.24 | +0.00 | -63.24 |
| 2026-08-27 | `ASML` | 1 | — | $1746.53 | +0.00 | $1735.01 | -11.52 | -11.52 | +0.00 | -11.52 |
| 2026-08-27 | `LRCX` | 6 | — | $318.88 | +0.00 | $318.58 | -1.80 | -1.80 | +0.00 | -1.80 |
| 2026-08-27 | `NVDA` | 9 | — | $222.86 | +0.00 | $227.98 | +46.08 | +46.08 | +0.00 | +46.08 |
| 2026-08-28 | `ACMR` | 24 | $80.49 | $79.27 | -29.28 | — | +0.00 | -29.28 | -57.12 | — |
| 2026-08-28 | `MU` | 2 | $935.39 | $919.29 | -32.20 | — | +0.00 | -32.20 | -95.44 | — |
| 2026-08-28 | `ASML` | 1 | $1735.01 | $1734.75 | -0.26 | — | +0.00 | -0.26 | -11.78 | — |
| 2026-08-28 | `LRCX` | 6 | $318.58 | $318.03 | -3.30 | — | +0.00 | -3.30 | -5.10 | — |
| 2026-08-28 | `NVDA` | 9 | $227.98 | $227.36 | -5.58 | — | +0.00 | -5.58 | +40.50 | — |
| 2026-08-28 | `KEYS` | 3 | — | $324.41 | +0.00 | $319.97 | -13.32 | -13.32 | +0.00 | -13.32 |
| 2026-08-28 | `SMTC` | 8 | — | $141.76 | +0.00 | $131.17 | -84.72 | -84.72 | +0.00 | -84.72 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `DDOG` | 5 | — | $240.22 | +0.00 | $236.98 | -16.20 | -16.20 | +0.00 | -16.20 |
| 2026-08-28 | `ADSK` | 4 | — | $261.16 | +0.00 | $260.66 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-08-28 | `SEDG` | 37 | — | $32.90 | +0.00 | $31.41 | -55.13 | -55.13 | +0.00 | -55.13 |
| 2026-08-28 | `TLS` | 258 | — | $4.82 | +0.00 | $4.79 | -7.74 | -7.74 | +0.00 | -7.74 |
| 2026-08-31 | `KEYS` | 3 | $319.97 | $322.49 | +7.56 | — | +0.00 | +7.56 | -5.76 | — |
| 2026-08-31 | `SMTC` | 8 | $131.17 | $132.30 | +9.04 | — | +0.00 | +9.04 | -75.68 | — |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -65.94 | — |
| 2026-08-31 | `DDOG` | 5 | $236.98 | $233.97 | -15.07 | — | +0.00 | -15.07 | -31.27 | — |
| 2026-08-31 | `ADSK` | 4 | $260.66 | $257.71 | -11.80 | — | +0.00 | -11.80 | -13.80 | — |
| 2026-08-31 | `SEDG` | 37 | $31.41 | $31.15 | -9.62 | — | +0.00 | -9.62 | -64.75 | — |
| 2026-08-31 | `TLS` | 258 | $4.79 | $4.81 | +5.16 | — | +0.00 | +5.16 | -2.58 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 3 | — | $351.74 | +0.00 | $357.16 | +16.26 | +16.26 | +0.00 | +16.26 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `CXW` | 37 | — | $32.31 | +0.00 | $33.66 | +49.95 | +49.95 | +0.00 | +49.95 |
| 2026-09-03 | `FRNM` | 76 | — | $15.87 | +0.00 | $16.90 | +78.28 | +78.28 | +0.00 | +78.28 |
| 2026-09-03 | `MMED` | 50 | — | $23.88 | +0.00 | $23.84 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-09-03 | `DE` | 1 | — | $703.25 | +0.00 | $694.41 | -8.84 | -8.84 | +0.00 | -8.84 |
| 2026-09-03 | `HPE` | 25 | — | $47.60 | +0.00 | $54.44 | +171.00 | +171.00 | +0.00 | +171.00 |
| 2026-09-03 | `CNXC` | 36 | — | $32.88 | +0.00 | $32.85 | -1.08 | -1.08 | +0.00 | -1.08 |
| 2026-09-04 | `AVGO` | 3 | $357.16 | $359.70 | +7.62 | — | +0.00 | +7.62 | +23.88 | — |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | — | +0.00 | -5.22 | +54.94 | — |
| 2026-09-04 | `CXW` | 37 | $33.66 | $33.46 | -7.40 | — | +0.00 | -7.40 | +42.55 | — |
| 2026-09-04 | `FRNM` | 76 | $16.90 | $16.40 | -38.00 | $16.31 | -6.84 | -44.84 | +40.28 | +33.44 |
| 2026-09-04 | `MMED` | 50 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.00 | — |
| 2026-09-04 | `DE` | 1 | $694.41 | $692.03 | -2.38 | — | +0.00 | -2.38 | -11.22 | — |
| 2026-09-04 | `HPE` | 25 | $54.44 | $53.85 | -14.75 | — | +0.00 | -14.75 | +156.25 | — |
| 2026-09-04 | `CNXC` | 36 | $32.85 | $32.48 | -13.32 | — | +0.00 | -13.32 | -14.40 | — |
| 2026-09-04 | `CRM` | 8 | — | $263.36 | +0.00 | $259.23 | -33.04 | -33.04 | +0.00 | -33.04 |
| 2026-09-04 | `MRX` | 28 | — | $75.65 | +0.00 | $78.27 | +73.36 | +73.36 | +0.00 | +73.36 |
| 2026-09-04 | `BE` | 9 | — | $236.82 | +0.00 | $252.87 | +144.45 | +144.45 | +0.00 | +144.45 |
| 2026-09-04 | `BAK` | 1118 | — | $1.94 | +0.00 | $1.89 | -55.90 | -55.90 | +0.00 | -55.90 |
| 2026-09-08 | `FRNM` | 76 | $16.31 | $16.74 | +32.68 | — | +0.00 | +32.68 | +66.12 | — |
| 2026-09-08 | `CRM` | 8 | $259.23 | $253.72 | -44.08 | — | +0.00 | -44.08 | -77.12 | — |
| 2026-09-08 | `MRX` | 28 | $78.27 | $78.84 | +15.96 | $76.71 | -59.64 | -43.68 | +89.32 | +29.68 |
| 2026-09-08 | `BE` | 9 | $252.87 | $267.76 | +134.01 | — | +0.00 | +134.01 | +278.46 | — |
| 2026-09-08 | `BAK` | 1118 | $1.89 | $1.94 | +55.90 | — | +0.00 | +55.90 | +0.00 | — |
| 2026-09-09 | `MRX` | 28 | $76.71 | $76.60 | -3.08 | — | +0.00 | -3.08 | +26.60 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 20 | — | $164.43 | +0.00 | $150.28 | -283.00 | -283.00 | +0.00 | -283.00 |
| 2026-09-11 | `ADBE` | 13 | — | $242.17 | +0.00 | $252.23 | +130.78 | +130.78 | +0.00 | +130.78 |
| 2026-09-11 | `BAK` | 1593 | — | $2.12 | +0.00 | $2.08 | -63.72 | -63.72 | +0.00 | -63.72 |
| 2026-09-14 | `ORCL` | 20 | $150.28 | $141.42 | -177.20 | — | +0.00 | -177.20 | -460.20 | — |
| 2026-09-14 | `ADBE` | 13 | $252.23 | $261.51 | +120.64 | — | +0.00 | +120.64 | +251.42 | — |
| 2026-09-14 | `BAK` | 1593 | $2.08 | $2.05 | -47.79 | — | +0.00 | -47.79 | -111.51 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 185 | — | $26.27 | +0.00 | $26.59 | +59.20 | +59.20 | +0.00 | +59.20 |
| 2026-09-16 | `CLS` | 15 | — | $320.20 | +0.00 | $323.83 | +54.45 | +54.45 | +0.00 | +54.45 |
| 2026-09-17 | `WAY` | 185 | $26.59 | $26.51 | -14.80 | — | +0.00 | -14.80 | +44.40 | — |
| 2026-09-17 | `CLS` | 15 | $323.83 | $337.75 | +208.80 | $329.94 | -117.15 | +91.65 | +263.25 | +146.10 |
| 2026-09-17 | `SMTC` | 4 | — | $170.85 | +0.00 | $178.19 | +29.36 | +29.36 | +0.00 | +29.36 |
| 2026-09-17 | `GME` | 37 | — | $22.12 | +0.00 | $22.77 | +24.05 | +24.05 | +0.00 | +24.05 |
| 2026-09-17 | `JBHT` | 3 | — | $238.60 | +0.00 | $236.80 | -5.40 | -5.40 | +0.00 | -5.40 |
| 2026-09-17 | `TNDM` | 46 | — | $17.72 | +0.00 | $17.23 | -22.54 | -22.54 | +0.00 | -22.54 |
| 2026-09-17 | `SION` | 114 | — | $7.27 | +0.00 | $7.18 | -10.26 | -10.26 | +0.00 | -10.26 |
| 2026-09-18 | `CLS` | 15 | $329.94 | $332.06 | +31.80 | $332.63 | +8.55 | +40.35 | +177.90 | +186.45 |
| 2026-09-18 | `SMTC` | 4 | $178.19 | $182.33 | +16.56 | — | +0.00 | +16.56 | +45.92 | — |
| 2026-09-18 | `GME` | 37 | $22.77 | $22.90 | +4.81 | $22.64 | -9.62 | -4.81 | +28.86 | +19.24 |
| 2026-09-18 | `JBHT` | 3 | $236.80 | $236.80 | +0.00 | — | +0.00 | +0.00 | -5.40 | — |
| 2026-09-18 | `TNDM` | 46 | $17.23 | $17.13 | -4.60 | — | +0.00 | -4.60 | -27.14 | — |
| 2026-09-18 | `SION` | 114 | $7.18 | $7.12 | -6.84 | — | +0.00 | -6.84 | -17.10 | — |
| 2026-09-18 | `RARE` | 140 | — | $14.79 | +0.00 | $14.51 | -39.20 | -39.20 | +0.00 | -39.20 |
| 2026-09-18 | `BHVN` | 147 | — | $14.07 | +0.00 | $13.62 | -66.15 | -66.15 | +0.00 | -66.15 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +137.19 | HLIT, ANGX, MH, VELO, ARX, NRG, S | — | $1,332.73 | $10,120.33 | HLIT×94, ANGX×290, MH×92, VELO×81, ARX×63, NRG×10, S×52 |
| 2026-08-17 | +2.25 | $1,332.73 | HLIT×94, ANGX×290, MH×92, VELO×81, ARX×63, NRG×10, S×52 | $10,155.37 | +35.04 | +131.23 | DVN, EOG, FANG, OUST | HLIT, ANGX, MH, VELO, ARX, NRG, S | $277.75 | $10,261.19 | DVN×54, EOG×17, FANG×12, OUST×51 |
| 2026-08-18 | -6.20 | $277.75 | DVN×54, EOG×17, FANG×12, OUST×51 | $10,193.18 | -68.01 | +0.00 | — | DVN, EOG, FANG, OUST | $10,184.70 | $10,184.70 | — |
| 2026-08-19 | -7.20 | $10,184.70 | — | $10,184.70 | -0.00 | +0.00 | — | — | $10,184.70 | $10,184.70 | — |
| 2026-08-20 | +1.12 | $10,184.70 | — | $10,184.70 | -0.00 | -137.33 | BHP, APA, AUTL, CRSP, ASST, MRNA, ZLAB, TEAM | — | $290.52 | $10,026.19 | BHP×13, APA×28, AUTL×515, CRSP×21, ASST×79, MRNA×8, ZLAB×47, TEAM×7 |
| 2026-08-21 | +3.25 | $290.52 | BHP×13, APA×28, AUTL×515, CRSP×21, ASST×79, MRNA×8, ZLAB×47, TEAM×7 | $10,220.92 | +194.73 | -152.22 | AU, FUTU, GRAL, ABTC, HIVE, MARA | BHP, APA, ASST, MRNA, ZLAB, TEAM | $114.73 | $10,040.16 | AUTL×515, CRSP×21, AU×10, FUTU×11, GRAL×16, ABTC×147, HIVE×395, MARA×109 |
| 2026-08-24 | -5.17 | $114.73 | AUTL×515, CRSP×21, AU×10, FUTU×11, GRAL×16, ABTC×147, HIVE×395, MARA×109 | $10,005.08 | -35.08 | -35.17 | — | AUTL, AU, FUTU, GRAL, ABTC, HIVE, MARA | $8,748.47 | $9,947.04 | CRSP×21 |
| 2026-08-25 | +1.80 | $8,748.47 | CRSP×21 | $9,965.00 | +17.96 | +232.50 | AU, FCX, EZPW, RUM, ZYME | CRSP | $185.42 | $10,184.25 | AU×16, FCX×25, EZPW×56, RUM×211, ZYME×69 |
| 2026-08-26 | +2.02 | $185.42 | AU×16, FCX×25, EZPW×56, RUM×211, ZYME×69 | $10,111.33 | -72.92 | +12.95 | FNV | AU, FCX, EZPW, RUM, ZYME | $218.15 | $10,110.84 | FNV×37 |
| 2026-08-27 | — | $218.15 | FNV×37 | $10,105.66 | -5.18 | -58.32 | ACMR, MU, ASML, LRCX, NVDA | FNV | $534.22 | $10,035.07 | ACMR×24, MU×2, ASML×1, LRCX×6, NVDA×9 |
| 2026-08-28 | +0.75 | $534.22 | ACMR×24, MU×2, ASML×1, LRCX×6, NVDA×9 | $9,964.45 | -70.62 | -245.05 | KEYS, SMTC, CIEN, DDOG, ADSK, SEDG, TLS | ACMR, MU, ASML, LRCX, NVDA | $1,923.63 | $9,693.75 | KEYS×3, SMTC×8, CIEN×3, DDOG×5, ADSK×4, SEDG×37, TLS×258 |
| 2026-08-31 | -5.85 | $1,923.63 | KEYS×3, SMTC×8, CIEN×3, DDOG×5, ADSK×4, SEDG×37, TLS×258 | $9,679.02 | -14.73 | +0.00 | — | KEYS, SMTC, CIEN, DDOG, ADSK, SEDG, TLS | $9,663.40 | $9,663.40 | — |
| 2026-09-01 | -6.30 | $9,663.40 | — | $9,663.40 | -0.00 | +0.00 | — | — | $9,663.40 | $9,663.40 | — |
| 2026-09-02 | -3.83 | $9,663.40 | — | $9,663.40 | -0.00 | +0.00 | — | — | $9,663.40 | $9,663.40 | — |
| 2026-09-03 | -0.90 | $9,663.40 | — | $9,663.40 | -0.00 | +363.73 | AVGO, DELL, CXW, FRNM, MMED, DE, HPE, CNXC | — | $946.43 | $10,010.52 | AVGO×3, DELL×2, CXW×37, FRNM×76, MMED×50, DE×1, HPE×25, CNXC×36 |
| 2026-09-04 | +2.25 | $946.43 | AVGO×3, DELL×2, CXW×37, FRNM×76, MMED×50, DE×1, HPE×25, CNXC×36 | $9,937.07 | -73.45 | +122.03 | CRM, MRX, BE, BAK | AVGO, DELL, CXW, MMED, DE, HPE, CNXC | $130.23 | $10,024.04 | FRNM×76, CRM×8, MRX×28, BE×9, BAK×1118 |
| 2026-09-08 | -11.47 | $130.23 | FRNM×76, CRM×8, MRX×28, BE×9, BAK×1118 | $10,218.51 | +194.47 | -59.64 | — | FRNM, CRM, BE, BAK | $7,990.03 | $10,137.91 | MRX×28 |
| 2026-09-09 | -13.95 | $7,990.03 | MRX×28 | $10,134.83 | -3.08 | +0.00 | — | MRX | $10,132.73 | $10,132.73 | — |
| 2026-09-10 | -13.28 | $10,132.73 | — | $10,132.73 | +0.00 | +0.00 | — | — | $10,132.73 | $10,132.73 | — |
| 2026-09-11 | +0.50 | $10,132.73 | — | $10,132.73 | +0.00 | -215.94 | ORCL, ADBE, BAK | — | $294.13 | $9,892.16 | ORCL×20, ADBE×13, BAK×1593 |
| 2026-09-14 | -11.00 | $294.13 | ORCL×20, ADBE×13, BAK×1593 | $9,787.81 | -104.35 | +0.00 | — | ORCL, ADBE, BAK | $9,762.83 | $9,762.83 | — |
| 2026-09-15 | -3.84 | $9,762.83 | — | $9,762.83 | -0.00 | +0.00 | — | — | $9,762.83 | $9,762.83 | — |
| 2026-09-16 | +5.30 | $9,762.83 | — | $9,762.83 | -0.00 | +113.65 | WAY, CLS | — | $95.30 | $9,871.90 | WAY×185, CLS×15 |
| 2026-09-17 | +7.38 | $95.30 | WAY×185, CLS×15 | $10,065.90 | +194.00 | -101.94 | SMTC, GME, JBHT, TNDM, SION | WAY | $1,124.93 | $9,950.78 | CLS×15, SMTC×4, GME×37, JBHT×3, TNDM×46, SION×114 |
| 2026-09-18 | +4.86 | $1,124.93 | CLS×15, SMTC×4, GME×37, JBHT×3, TNDM×46, SION×114 | $9,992.51 | +41.73 | -106.42 | RARE, BHVN | SMTC, JBHT, TNDM, SION | $12.03 | $9,872.70 | CLS×15, GME×37, RARE×140, BHVN×147 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $8,758.81 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $7,505.17 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $6,256.30 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 81 | $15.38 | $2.23 | — | $5,008.29 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $3,773.20 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $2,571.18 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `S` | 52 | $23.77 | $2.15 | — | $1,332.73 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,332.73 | ▲ close $10,120.33 vs 09:30 $10,000.00 (session +137.19) | 16:00 close · cash $1,332.73 · equity $10,120.33 vs 09:30 $10,000.00 (+120.33; session marks +137.19) · 7 name(s) marked open→close (per-name table). HLIT×94 09:30 $13.18 → close $13.92 +69.56; ANGX×290 09:30 $4.31 → close $4.37 +17.40; MH×92 09:30 $13.55 → close $13.10 -41.40; VELO×81 09:30 $15.38 → close $16.16 +63.18; ARX×63 09:30 $19.57 → close $19.58 +0.63; NRG×10 09:30 $120.00 → close $126.24 +62.40; S×52 09:30 $23.77 → close $23.11 -34.58 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,332.73 | ▲ 09:30 equity $10,155.37 vs yday $10,120.33 (+35.04) | 09:30 open · cash $1,332.73 (unchanged overnight, no fees) · equity $10,155.37 vs prior close $10,120.33 (+35.04) · 7 name(s) re-marked at the open (per-name table). HLIT×94 yday $13.92 → 09:30 $13.84 -7.52; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; MH×92 yday $13.10 → 09:30 $13.16 +5.52; VELO×81 yday $16.16 → 09:30 $16.05 -8.91; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; S×52 yday $23.11 → 09:30 $22.50 -31.72 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $2,631.40 | ▲ +57.47 after sell → book $10,153.08; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,961.60 | ▲ +76.56 after sell → book $10,149.28; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $5,170.02 | ▼ -40.44 after sell → book $10,146.98; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VELO` | 81 | $16.05 | $2.26 | $+49.78 | $6,467.82 | ▲ +49.78 after sell → book $10,144.73; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $7,698.53 | ▼ -4.38 after sell → book $10,142.53; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $8,970.49 | ▲ +69.94 after sell → book $10,140.49; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `S` | 52 | $22.50 | $2.17 | $-70.61 | $10,138.32 | ▼ -70.61 after sell → book $10,138.32; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 54 | $46.18 | $2.15 | — | $7,642.45 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+6.7; leftover $2534.58 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 17 | $142.77 | $2.04 | — | $5,213.32 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+5.8; leftover $2534.58 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 12 | $202.70 | $2.03 | — | $2,778.89 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+8.3; leftover $2534.58 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 51 | $49.00 | $2.14 | — | $277.75 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $2534.58 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
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
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,999.54 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1273.09 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $7,744.19 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1273.09 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 515 | $2.47 | $6.64 | — | $6,465.49 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1273.09 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $5,230.11 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1273.09 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 79 | $16.00 | $2.23 | — | $3,963.88 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1273.09 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $2,760.75 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1273.09 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 47 | $26.57 | $2.13 | — | $1,509.83 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1273.09 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TEAM` | 7 | $173.90 | $2.01 | — | $290.52 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+12.2; leftover $1273.09 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $290.52 | ▼ close $10,026.19 vs 09:30 $10,184.70 (session -137.33) | 16:00 close · cash $290.52 · equity $10,026.19 vs 09:30 $10,184.70 (-158.51; session marks -137.33) · 8 name(s) marked open→close (per-name table). BHP×13 09:30 $91.01 → close $93.63 +34.06; APA×28 09:30 $44.76 → close $44.39 -10.36; AUTL×515 09:30 $2.47 → close $2.46 -5.15; CRSP×21 09:30 $58.73 → close $58.12 -12.81; ASST×79 09:30 $16.00 → close $16.13 +10.27; MRNA×8 09:30 $150.14 → close $133.32 -134.56; ZLAB×47 09:30 $26.57 → close $26.02 -25.85; TEAM×7 09:30 $173.90 → close $174.91 +7.07 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $290.52 | ▲ 09:30 equity $10,220.92 vs yday $10,026.19 (+194.73) | 09:30 open · cash $290.52 (unchanged overnight, no fees) · equity $10,220.92 vs prior close $10,026.19 (+194.73) · 8 name(s) re-marked at the open (per-name table). BHP×13 yday $93.63 → 09:30 $95.72 +27.17; APA×28 yday $44.39 → 09:30 $44.52 +3.64; AUTL×515 yday $2.46 → 09:30 $2.47 +5.15; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; ASST×79 yday $16.13 → 09:30 $17.66 +120.87; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; ZLAB×47 yday $26.02 → 09:30 $26.25 +10.81; TEAM×7 yday $174.91 → 09:30 $174.22 -4.83 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,532.83 | ▲ +57.15 after sell → book $10,218.87; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $2,777.29 | ▼ -10.89 after sell → book $10,216.77; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 79 | $17.66 | $2.25 | $+126.66 | $4,170.18 | ▲ +126.66 after sell → book $10,214.52; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $5,233.03 | ▼ -140.29 after sell → book $10,212.49; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 47 | $26.25 | $2.15 | $-19.32 | $6,464.63 | ▼ -19.32 after sell → book $10,210.34; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TEAM` | 7 | $174.22 | $2.03 | $-1.80 | $7,682.14 | ▼ -1.80 after sell → book $10,208.31; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,485.82 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1280.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $5,216.81 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1280.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 16 | $78.88 | $2.04 | — | $3,952.69 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1280.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 147 | $8.66 | $2.43 | — | $2,677.24 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1280.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 395 | $3.24 | $5.10 | — | $1,392.35 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $1280.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 109 | $11.70 | $2.32 | — | $114.73 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1280.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $114.73 | ▼ close $10,040.16 vs 09:30 $10,220.92 (session -152.22) | 16:00 close · cash $114.73 · equity $10,040.16 vs 09:30 $10,220.92 (-180.76; session marks -152.22) · 8 name(s) marked open→close (per-name table). AUTL×515 09:30 $2.47 → close $2.41 -30.90; CRSP×21 09:30 $59.72 → close $59.50 -4.62; AU×10 09:30 $119.43 → close $121.22 +17.90; FUTU×11 09:30 $115.18 → close $123.64 +93.06; GRAL×16 09:30 $78.88 → close $79.54 +10.56; ABTC×147 09:30 $8.66 → close $7.93 -107.31; HIVE×395 09:30 $3.24 → close $3.03 -82.95; MARA×109 09:30 $11.70 → close $11.26 -47.96 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $114.73 | ▼ 09:30 equity $10,005.08 vs yday $10,040.16 (-35.08) | 09:30 open · cash $114.73 (unchanged overnight, no fees) · equity $10,005.08 vs prior close $10,040.16 (-35.08) · 8 name(s) re-marked at the open (per-name table). AUTL×515 yday $2.41 → 09:30 $2.40 -5.15; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; AU×10 yday $121.22 → 09:30 $120.51 -7.10; FUTU×11 yday $123.64 → 09:30 $121.00 -29.04; GRAL×16 yday $79.54 → 09:30 $81.87 +37.28; ABTC×147 yday $7.93 → 09:30 $8.00 +10.29; HIVE×395 yday $3.03 → 09:30 $2.99 -15.80; MARA×109 yday $11.26 → 09:30 $11.17 -9.81 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 515 | $2.40 | $6.74 | $-49.43 | $1,343.99 | ▼ -49.43 after sell → book $9,998.34; vs 09:30 mark -6.74 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $2,547.05 | ▲ +6.74 after sell → book $9,996.30; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $121.00 | $2.04 | $+59.95 | $3,876.01 | ▲ +59.95 after sell → book $9,994.26; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 16 | $81.87 | $2.06 | $+43.74 | $5,183.87 | ▲ +43.74 after sell → book $9,992.20; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 147 | $8.00 | $2.47 | $-101.92 | $6,357.40 | ▼ -101.92 after sell → book $9,989.73; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 395 | $2.99 | $5.17 | $-109.02 | $7,533.28 | ▼ -109.02 after sell → book $9,984.56; vs 09:30 mark -5.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 109 | $11.17 | $2.35 | $-62.43 | $8,748.47 | ▼ -62.43 after sell → book $9,982.22; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,748.47 | ▼ close $9,947.04 vs 09:30 $10,005.08 (session -35.17) | 16:00 close · cash $8,748.47 · equity $9,947.04 vs 09:30 $10,005.08 (-58.04; session marks -35.17) · 1 name(s) marked open→close (per-name table). CRSP×21 09:30 $58.75 → close $57.08 -35.17 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,748.47 | ▲ 09:30 equity $9,965.00 vs yday $9,947.04 (+17.96) | 09:30 open · cash $8,748.47 (unchanged overnight, no fees) · equity $9,965.00 vs prior close $9,947.04 (+17.96) · 1 name(s) re-marked at the open (per-name table). CRSP×21 yday $57.08 → 09:30 $57.93 +17.95 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-20.93 | $9,962.93 | ▼ -20.93 after sell → book $9,962.93; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 16 | $118.52 | $2.04 | — | $8,064.57 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1992.59 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 25 | $77.13 | $2.06 | — | $6,134.25 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1992.59 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 56 | $35.05 | $2.16 | — | $4,169.29 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1992.59 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 211 | $9.42 | $2.72 | — | $2,178.95 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1992.59 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 69 | $28.86 | $2.20 | — | $185.42 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+13.7; leftover $1992.59 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $185.42 | ▲ close $10,184.25 vs 09:30 $9,965.00 (session +232.50) | 16:00 close · cash $185.42 · equity $10,184.25 vs 09:30 $9,965.00 (+219.25; session marks +232.50) · 5 name(s) marked open→close (per-name table). AU×16 09:30 $118.52 → close $123.39 +77.92; FCX×25 09:30 $77.13 → close $79.91 +69.50; EZPW×56 09:30 $35.05 → close $35.23 +10.08; RUM×211 09:30 $9.42 → close $10.23 +170.91; ZYME×69 09:30 $28.86 → close $27.47 -95.91 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $185.42 | ▼ 09:30 equity $10,111.33 vs yday $10,184.25 (-72.92) | 09:30 open · cash $185.42 (unchanged overnight, no fees) · equity $10,111.33 vs prior close $10,184.25 (-72.92) · 5 name(s) re-marked at the open (per-name table). AU×16 yday $123.39 → 09:30 $119.80 -57.44; FCX×25 yday $79.91 → 09:30 $79.34 -14.25; EZPW×56 yday $35.23 → 09:30 $35.70 +26.32; RUM×211 yday $10.23 → 09:30 $10.07 -33.76; ZYME×69 yday $27.47 → 09:30 $27.56 +6.21 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 16 | $119.80 | $2.06 | $+16.38 | $2,100.15 | ▲ +16.38 after sell → book $10,109.26; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 25 | $79.34 | $2.09 | $+51.09 | $4,081.56 | ▲ +51.09 after sell → book $10,107.17; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 56 | $35.70 | $2.18 | $+32.06 | $6,078.58 | ▲ +32.06 after sell → book $10,104.99; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 211 | $10.07 | $2.77 | $+131.65 | $8,200.57 | ▲ +131.65 after sell → book $10,102.21; vs 09:30 mark -2.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 69 | $27.56 | $2.22 | $-94.12 | $10,099.99 | ▼ -94.12 after sell → book $10,099.99; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 37 | $267.02 | $2.10 | — | $218.15 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $10099.99 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $218.15 | ▲ close $10,110.84 vs 09:30 $10,111.33 (session +12.95) | 16:00 close · cash $218.15 · equity $10,110.84 vs 09:30 $10,111.33 (-0.49; session marks +12.95) · 1 name(s) marked open→close (per-name table). FNV×37 09:30 $267.02 → close $267.37 +12.95 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $218.15 | ▼ 09:30 equity $10,105.66 vs yday $10,110.84 (-5.18) | 09:30 open · cash $218.15 (unchanged overnight, no fees) · equity $10,105.66 vs prior close $10,110.84 (-5.18) · 1 name(s) re-marked at the open (per-name table). FNV×37 yday $267.37 → 09:30 $267.23 -5.18 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 37 | $267.23 | $2.19 | $+3.48 | $10,103.47 | ▲ +3.48 after sell → book $10,103.47; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 24 | $81.65 | $2.06 | — | $8,141.81 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $2020.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 2 | $967.01 | $2.00 | — | $6,205.79 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $2020.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ASML` | 1 | $1746.53 | $1.99 | — | $4,457.27 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=-0.3; leftover $2020.69 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 6 | $318.88 | $2.01 | — | $2,541.98 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $2020.69 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 9 | $222.86 | $2.02 | — | $534.22 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $2020.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $534.22 | ▼ close $10,035.07 vs 09:30 $10,105.66 (session -58.32) | 16:00 close · cash $534.22 · equity $10,035.07 vs 09:30 $10,105.66 (-70.59; session marks -58.32) · 5 name(s) marked open→close (per-name table). ACMR×24 09:30 $81.65 → close $80.49 -27.84; MU×2 09:30 $967.01 → close $935.39 -63.24; ASML×1 09:30 $1746.53 → close $1735.01 -11.52; LRCX×6 09:30 $318.88 → close $318.58 -1.80; NVDA×9 09:30 $222.86 → close $227.98 +46.08 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $534.22 | ▼ 09:30 equity $9,964.45 vs yday $10,035.07 (-70.62) | 09:30 open · cash $534.22 (unchanged overnight, no fees) · equity $9,964.45 vs prior close $10,035.07 (-70.62) · 5 name(s) re-marked at the open (per-name table). ACMR×24 yday $80.49 → 09:30 $79.27 -29.28; MU×2 yday $935.39 → 09:30 $919.29 -32.20; ASML×1 yday $1735.01 → 09:30 $1734.75 -0.26; LRCX×6 yday $318.58 → 09:30 $318.03 -3.30; NVDA×9 yday $227.98 → 09:30 $227.36 -5.58 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 24 | $79.27 | $2.09 | $-61.27 | $2,434.62 | ▼ -61.27 after sell → book $9,962.37; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 2 | $919.29 | $2.02 | $-99.46 | $4,271.17 | ▼ -99.46 after sell → book $9,960.34; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ASML` | 1 | $1734.75 | $2.02 | $-15.79 | $6,003.91 | ▼ -15.79 after sell → book $9,958.33; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 6 | $318.03 | $2.03 | $-9.14 | $7,910.05 | ▼ -9.14 after sell → book $9,956.29; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 9 | $227.36 | $2.04 | $+36.44 | $9,954.25 | ▲ +36.44 after sell → book $9,954.25; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $8,979.02 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1244.28 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $7,842.93 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1244.28 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,639.67 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1244.28 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $5,436.56 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1244.28 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $4,389.92 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; ret5=+7.8; leftover $1244.28 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 37 | $32.90 | $2.10 | — | $3,170.52 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1244.28 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 258 | $4.82 | $3.33 | — | $1,923.63 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1244.28 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,923.63 | ▼ close $9,693.75 vs 09:30 $9,964.45 (session -245.05) | 16:00 close · cash $1,923.63 · equity $9,693.75 vs 09:30 $9,964.45 (-270.70; session marks -245.05) · 7 name(s) marked open→close (per-name table). KEYS×3 09:30 $324.41 → close $319.97 -13.32; SMTC×8 09:30 $141.76 → close $131.17 -84.72; CIEN×3 09:30 $400.42 → close $378.44 -65.94; DDOG×5 09:30 $240.22 → close $236.98 -16.20; ADSK×4 09:30 $261.16 → close $260.66 -2.00; SEDG×37 09:30 $32.90 → close $31.41 -55.13; TLS×258 09:30 $4.82 → close $4.79 -7.74 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,923.63 | ▼ 09:30 equity $9,679.02 vs yday $9,693.75 (-14.73) | 09:30 open · cash $1,923.63 (unchanged overnight, no fees) · equity $9,679.02 vs prior close $9,693.75 (-14.73) · 7 name(s) re-marked at the open (per-name table). KEYS×3 yday $319.97 → 09:30 $322.49 +7.56; SMTC×8 yday $131.17 → 09:30 $132.30 +9.04; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; DDOG×5 yday $236.98 → 09:30 $233.97 -15.07; ADSK×4 yday $260.66 → 09:30 $257.71 -11.80; SEDG×37 yday $31.41 → 09:30 $31.15 -9.62; TLS×258 yday $4.79 → 09:30 $4.81 +5.16 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 3 | $322.49 | $2.02 | $-9.78 | $2,889.08 | ▼ -9.78 after sell → book $9,677.00; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $132.30 | $2.03 | $-79.73 | $3,945.45 | ▼ -79.73 after sell → book $9,674.96; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $5,078.75 | ▼ -69.96 after sell → book $9,672.95; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 5 | $233.97 | $2.02 | $-35.31 | $6,246.55 | ▼ -35.31 after sell → book $9,670.92; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $257.71 | $2.02 | $-17.82 | $7,275.37 | ▼ -17.82 after sell → book $9,668.90; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 37 | $31.15 | $2.12 | $-68.97 | $8,425.80 | ▼ -68.97 after sell → book $9,666.78; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 258 | $4.81 | $3.38 | $-9.29 | $9,663.40 | ▼ -9.29 after sell → book $9,663.40; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,663.40 | ▲ close $9,663.40 vs 09:30 $9,679.02 (session +0.00) | 16:00 close · cash $9,663.40 · no lots left · equity $9,663.40. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,663.40 | ▲ 09:30 equity $9,663.40 vs yday $9,663.40 (-0.00) | 09:30 open · cash $9,663.40 · no holdings · equity $9,663.40 vs prior close $9,663.40 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,663.40 | ▲ close $9,663.40 vs 09:30 $9,663.40 (session +0.00) | 16:00 close · cash $9,663.40 · no lots left · equity $9,663.40. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,663.40 | ▲ 09:30 equity $9,663.40 vs yday $9,663.40 (-0.00) | 09:30 open · cash $9,663.40 · no holdings · equity $9,663.40 vs prior close $9,663.40 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,663.40 | ▲ close $9,663.40 vs 09:30 $9,663.40 (session +0.00) | 16:00 close · cash $9,663.40 · no lots left · equity $9,663.40. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,663.40 | ▲ 09:30 equity $9,663.40 vs yday $9,663.40 (-0.00) | 09:30 open · cash $9,663.40 · no holdings · equity $9,663.40 vs prior close $9,663.40 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,606.18 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1207.92 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,631.56 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1207.92 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 37 | $32.31 | $2.10 | — | $6,433.99 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1207.92 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 76 | $15.87 | $2.22 | — | $5,225.65 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1207.92 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 50 | $23.88 | $2.14 | — | $4,029.51 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1207.92 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $3,324.27 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1207.92 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 25 | $47.60 | $2.06 | — | $2,132.20 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1207.92 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 36 | $32.88 | $2.10 | — | $946.43 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ret5=+16.2; leftover $1207.92 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $946.43 | ▲ close $10,010.52 vs 09:30 $9,663.40 (session +363.73) | 16:00 close · cash $946.43 · equity $10,010.52 vs 09:30 $9,663.40 (+347.12; session marks +363.73) · 8 name(s) marked open→close (per-name table). AVGO×3 09:30 $351.74 → close $357.16 +16.26; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×37 09:30 $32.31 → close $33.66 +49.95; FRNM×76 09:30 $15.87 → close $16.90 +78.28; MMED×50 09:30 $23.88 → close $23.84 -2.00; DE×1 09:30 $703.25 → close $694.41 -8.84; HPE×25 09:30 $47.60 → close $54.44 +171.00; CNXC×36 09:30 $32.88 → close $32.85 -1.08 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $946.43 | ▼ 09:30 equity $9,937.07 vs yday $10,010.52 (-73.45) | 09:30 open · cash $946.43 (unchanged overnight, no fees) · equity $9,937.07 vs prior close $10,010.52 (-73.45) · 8 name(s) re-marked at the open (per-name table). AVGO×3 yday $357.16 → 09:30 $359.70 +7.62; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×37 yday $33.66 → 09:30 $33.46 -7.40; FRNM×76 yday $16.90 → 09:30 $16.40 -38.00; MMED×50 yday $23.84 → 09:30 $23.84 +0.00; DE×1 yday $694.41 → 09:30 $692.03 -2.38; HPE×25 yday $54.44 → 09:30 $53.85 -14.75; CNXC×36 yday $32.85 → 09:30 $32.48 -13.32 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $2,023.51 | ▲ +19.86 after sell → book $9,935.05; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $3,049.05 | ▲ +50.93 after sell → book $9,933.03; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 37 | $33.46 | $2.12 | $+38.33 | $4,284.95 | ▲ +38.33 after sell → book $9,930.91; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 50 | $23.84 | $2.16 | $-6.30 | $5,474.79 | ▼ -6.30 after sell → book $9,928.75; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $6,164.81 | ▼ -15.23 after sell → book $9,926.74; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 25 | $53.85 | $2.09 | $+152.10 | $7,508.97 | ▲ +152.10 after sell → book $9,924.65; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 36 | $32.48 | $2.12 | $-18.62 | $8,676.13 | ▼ -18.62 after sell → book $9,922.53; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 8 | $263.36 | $2.01 | — | $6,567.24 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $2169.03 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 28 | $75.65 | $2.07 | — | $4,446.97 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $2169.03 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 9 | $236.82 | $2.02 | — | $2,313.57 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; ret5=+8.1; leftover $2169.03 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 1118 | $1.94 | $14.42 | — | $130.23 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $2169.03 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $130.23 | ▲ close $10,024.04 vs 09:30 $9,937.07 (session +122.03) | 16:00 close · cash $130.23 · equity $10,024.04 vs 09:30 $9,937.07 (+86.97; session marks +122.03) · 5 name(s) marked open→close (per-name table). FRNM×76 09:30 $16.40 → close $16.31 -6.84; CRM×8 09:30 $263.36 → close $259.23 -33.04; MRX×28 09:30 $75.65 → close $78.27 +73.36; BE×9 09:30 $236.82 → close $252.87 +144.45; BAK×1118 09:30 $1.94 → close $1.89 -55.90 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $130.23 | ▲ 09:30 equity $10,218.51 vs yday $10,024.04 (+194.47) | 09:30 open · cash $130.23 (unchanged overnight, no fees) · equity $10,218.51 vs prior close $10,024.04 (+194.47) · 5 name(s) re-marked at the open (per-name table). FRNM×76 yday $16.31 → 09:30 $16.74 +32.68; CRM×8 yday $259.23 → 09:30 $253.72 -44.08; MRX×28 yday $78.27 → 09:30 $78.84 +15.96; BE×9 yday $252.87 → 09:30 $267.76 +134.01; BAK×1118 yday $1.89 → 09:30 $1.94 +55.90 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 76 | $16.74 | $2.24 | $+61.66 | $1,400.23 | ▲ +61.66 after sell → book $10,216.27; vs 09:30 mark -2.24 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 8 | $253.72 | $2.04 | $-81.17 | $3,427.95 | ▼ -81.17 after sell → book $10,214.23; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 9 | $267.76 | $2.05 | $+274.40 | $5,835.74 | ▲ +274.40 after sell → book $10,212.18; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 1118 | $1.94 | $14.63 | $-29.05 | $7,990.03 | ▼ -29.05 after sell → book $10,197.55; vs 09:30 mark -14.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,990.03 | ▼ close $10,137.91 vs 09:30 $10,218.51 (session -59.64) | 16:00 close · cash $7,990.03 · equity $10,137.91 vs 09:30 $10,218.51 (-80.60; session marks -59.64) · 1 name(s) marked open→close (per-name table). MRX×28 09:30 $78.84 → close $76.71 -59.64 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,990.03 | ▼ 09:30 equity $10,134.83 vs yday $10,137.91 (-3.08) | 09:30 open · cash $7,990.03 (unchanged overnight, no fees) · equity $10,134.83 vs prior close $10,137.91 (-3.08) · 1 name(s) re-marked at the open (per-name table). MRX×28 yday $76.71 → 09:30 $76.60 -3.08 | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 28 | $76.60 | $2.10 | $+22.42 | $10,132.73 | ▲ +22.42 after sell → book $10,132.73; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,132.73 | ▲ close $10,132.73 vs 09:30 $10,134.83 (session +0.00) | 16:00 close · cash $10,132.73 · no lots left · equity $10,132.73. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,132.73 | ▲ 09:30 equity $10,132.73 vs yday $10,132.73 (+0.00) | 09:30 open · cash $10,132.73 · no holdings · equity $10,132.73 vs prior close $10,132.73 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,132.73 | ▲ close $10,132.73 vs 09:30 $10,132.73 (session +0.00) | 16:00 close · cash $10,132.73 · no lots left · equity $10,132.73. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,132.73 | ▲ 09:30 equity $10,132.73 vs yday $10,132.73 (+0.00) | 09:30 open · cash $10,132.73 · no holdings · equity $10,132.73 vs prior close $10,132.73 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 20 | $164.43 | $2.05 | — | $6,842.08 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $3377.58 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 13 | $242.17 | $2.03 | — | $3,691.84 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; ret5=-11.1; leftover $3377.58 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 1593 | $2.12 | $20.55 | — | $294.13 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $3377.58 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $294.13 | ▼ close $9,892.16 vs 09:30 $10,132.73 (session -215.94) | 16:00 close · cash $294.13 · equity $9,892.16 vs 09:30 $10,132.73 (-240.57; session marks -215.94) · 3 name(s) marked open→close (per-name table). ORCL×20 09:30 $164.43 → close $150.28 -283.00; ADBE×13 09:30 $242.17 → close $252.23 +130.78; BAK×1593 09:30 $2.12 → close $2.08 -63.72 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $294.13 | ▼ 09:30 equity $9,787.81 vs yday $9,892.16 (-104.35) | 09:30 open · cash $294.13 (unchanged overnight, no fees) · equity $9,787.81 vs prior close $9,892.16 (-104.35) · 3 name(s) re-marked at the open (per-name table). ORCL×20 yday $150.28 → 09:30 $141.42 -177.20; ADBE×13 yday $252.23 → 09:30 $261.51 +120.64; BAK×1593 yday $2.08 → 09:30 $2.05 -47.79 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 20 | $141.42 | $2.08 | $-464.33 | $3,120.45 | ▼ -464.33 after sell → book $9,785.73; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 13 | $261.51 | $2.07 | $+247.32 | $6,518.02 | ▲ +247.32 after sell → book $9,783.67; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 1593 | $2.05 | $20.84 | $-152.90 | $9,762.83 | ▼ -152.90 after sell → book $9,762.83; vs 09:30 mark -20.84 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,762.83 | ▲ close $9,762.83 vs 09:30 $9,787.81 (session +0.00) | 16:00 close · cash $9,762.83 · no lots left · equity $9,762.83. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,762.83 | ▲ 09:30 equity $9,762.83 vs yday $9,762.83 (-0.00) | 09:30 open · cash $9,762.83 · no holdings · equity $9,762.83 vs prior close $9,762.83 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,762.83 | ▲ close $9,762.83 vs 09:30 $9,762.83 (session +0.00) | 16:00 close · cash $9,762.83 · no lots left · equity $9,762.83. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,762.83 | ▲ 09:30 equity $9,762.83 vs yday $9,762.83 (-0.00) | 09:30 open · cash $9,762.83 · no holdings · equity $9,762.83 vs prior close $9,762.83 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 185 | $26.27 | $2.54 | — | $4,900.33 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ret5=-7.4; leftover $4881.41 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `CLS` | 15 | $320.20 | $2.04 | — | $95.30 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; ret5=+10.2; leftover $4881.41 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.30 | ▲ close $9,871.90 vs 09:30 $9,762.83 (session +113.65) | 16:00 close · cash $95.30 · equity $9,871.90 vs 09:30 $9,762.83 (+109.07; session marks +113.65) · 2 name(s) marked open→close (per-name table). WAY×185 09:30 $26.27 → close $26.59 +59.20; CLS×15 09:30 $320.20 → close $323.83 +54.45 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.30 | ▲ 09:30 equity $10,065.90 vs yday $9,871.90 (+194.00) | 09:30 open · cash $95.30 (unchanged overnight, no fees) · equity $10,065.90 vs prior close $9,871.90 (+194.00) · 2 name(s) re-marked at the open (per-name table). WAY×185 yday $26.59 → 09:30 $26.51 -14.80; CLS×15 yday $323.83 → 09:30 $337.75 +208.80 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 185 | $26.51 | $2.61 | $+39.24 | $4,997.03 | ▲ +39.24 after sell → book $10,063.28; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 4 | $170.85 | $2.00 | — | $4,311.63 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+23.5; leftover $832.84 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 37 | $22.12 | $2.10 | — | $3,491.09 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+10.0; leftover $832.84 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 3 | $238.60 | $2.00 | — | $2,773.29 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_mover; ret5=+3.5; leftover $832.84 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 46 | $17.72 | $2.13 | — | $1,956.04 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ret5=-16.3; leftover $832.84 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SION` | 114 | $7.27 | $2.33 | — | $1,124.93 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+16.3; leftover $832.84 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,124.93 | ▼ close $9,950.78 vs 09:30 $10,065.90 (session -101.94) | 16:00 close · cash $1,124.93 · equity $9,950.78 vs 09:30 $10,065.90 (-115.12; session marks -101.94) · 6 name(s) marked open→close (per-name table). CLS×15 09:30 $337.75 → close $329.94 -117.15; SMTC×4 09:30 $170.85 → close $178.19 +29.36; GME×37 09:30 $22.12 → close $22.77 +24.05; JBHT×3 09:30 $238.60 → close $236.80 -5.40; TNDM×46 09:30 $17.72 → close $17.23 -22.54; SION×114 09:30 $7.27 → close $7.18 -10.26 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,124.93 | ▲ 09:30 equity $9,992.51 vs yday $9,950.78 (+41.73) | 09:30 open · cash $1,124.93 (unchanged overnight, no fees) · equity $9,992.51 vs prior close $9,950.78 (+41.73) · 6 name(s) re-marked at the open (per-name table). CLS×15 yday $329.94 → 09:30 $332.06 +31.80; SMTC×4 yday $178.19 → 09:30 $182.33 +16.56; GME×37 yday $22.77 → 09:30 $22.90 +4.81; JBHT×3 yday $236.80 → 09:30 $236.80 +0.00; TNDM×46 yday $17.23 → 09:30 $17.13 -4.60; SION×114 yday $7.18 → 09:30 $7.12 -6.84 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 4 | $182.33 | $2.02 | $+41.90 | $1,852.23 | ▲ +41.90 after sell → book $9,990.49; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 3 | $236.80 | $2.02 | $-9.42 | $2,560.61 | ▼ -9.42 after sell → book $9,988.47; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 46 | $17.13 | $2.15 | $-31.42 | $3,346.44 | ▼ -31.42 after sell → book $9,986.32; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SION` | 114 | $7.12 | $2.36 | $-21.79 | $4,155.76 | ▼ -21.79 after sell → book $9,983.96; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 140 | $14.79 | $2.41 | — | $2,082.75 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ⚪; ret5=-3.7; leftover $2077.88 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 147 | $14.07 | $2.43 | — | $12.03 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ret5=-19.9; leftover $2077.88 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.03 | ▼ close $9,872.70 vs 09:30 $9,992.51 (session -106.42) | 16:00 close · cash $12.03 · equity $9,872.70 vs 09:30 $9,992.51 (-119.81; session marks -106.42) · 4 name(s) marked open→close (per-name table). CLS×15 09:30 $332.06 → close $332.63 +8.55; GME×37 09:30 $22.90 → close $22.64 -9.62; RARE×140 09:30 $14.79 → close $14.51 -39.20; BHVN×147 09:30 $14.07 → close $13.62 -66.15 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-28 | `MPWR` | cash | leftover split 1244.28 < 1 share @ 1306.03 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `LITE` | cash | leftover split 832.84 < 1 share @ 934.88 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CLS` | 15 | 2026-09-16 @ $320.20 | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; ret5=+10.2; leftover $4881.41 |
| `GME` | 37 | 2026-09-17 @ $22.12 | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+10.0; leftover $832.84 |
| `RARE` | 140 | 2026-09-18 @ $14.79 | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ⚪; ret5=-3.7; leftover $2077.88 |
| `BHVN` | 147 | 2026-09-18 @ $14.07 | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ret5=-19.9; leftover $2077.88 |
