# Factor mine action — `union_news_or_net2_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 2

Cash book **-1.62%** ($9,838) · signal-only (no cash/fees) was -1.69%. Starts YES **4/22**. Fills 116 · skips 34 · realized $-162.33.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,837.70.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ANGX` | 290 | — | $4.31 | +0.00 | $4.37 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-08-14 | `ARX` | 63 | — | $19.57 | +0.00 | $19.58 | +0.63 | +0.63 | +0.00 | +0.63 |
| 2026-08-14 | `HLIT` | 94 | — | $13.18 | +0.00 | $13.92 | +69.56 | +69.56 | +0.00 | +69.56 |
| 2026-08-14 | `MH` | 92 | — | $13.55 | +0.00 | $13.10 | -41.40 | -41.40 | +0.00 | -41.40 |
| 2026-08-14 | `NRG` | 10 | — | $120.00 | +0.00 | $126.24 | +62.40 | +62.40 | +0.00 | +62.40 |
| 2026-08-14 | `TLN` | 3 | — | $359.83 | +0.00 | $362.74 | +8.73 | +8.73 | +0.00 | +8.73 |
| 2026-08-14 | `VST` | 8 | — | $146.90 | +0.00 | $148.13 | +9.84 | +9.84 | +0.00 | +9.84 |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | — | +0.00 | +66.70 | +84.10 | — |
| 2026-08-17 | `ARX` | 63 | $19.58 | $19.57 | -0.63 | — | +0.00 | -0.63 | +0.00 | — |
| 2026-08-17 | `HLIT` | 94 | $13.92 | $13.84 | -7.52 | — | +0.00 | -7.52 | +62.04 | — |
| 2026-08-17 | `MH` | 92 | $13.10 | $13.16 | +5.52 | — | +0.00 | +5.52 | -35.88 | — |
| 2026-08-17 | `NRG` | 10 | $126.24 | $127.40 | +11.60 | — | +0.00 | +11.60 | +74.00 | — |
| 2026-08-17 | `TLN` | 3 | $362.74 | $367.88 | +15.42 | — | +0.00 | +15.42 | +24.15 | — |
| 2026-08-17 | `VST` | 8 | $148.13 | $149.37 | +9.92 | — | +0.00 | +9.92 | +19.76 | — |
| 2026-08-17 | `DVN` | 55 | — | $46.18 | +0.00 | $47.57 | +76.45 | +76.45 | +0.00 | +76.45 |
| 2026-08-17 | `EOG` | 17 | — | $142.77 | +0.00 | $146.15 | +57.46 | +57.46 | +0.00 | +57.46 |
| 2026-08-17 | `FANG` | 12 | — | $202.70 | +0.00 | $206.29 | +43.08 | +43.08 | +0.00 | +43.08 |
| 2026-08-17 | `OUST` | 52 | — | $49.00 | +0.00 | $48.13 | -45.24 | -45.24 | +0.00 | -45.24 |
| 2026-08-18 | `DVN` | 55 | $47.57 | $48.00 | +23.65 | — | +0.00 | +23.65 | +100.10 | — |
| 2026-08-18 | `EOG` | 17 | $146.15 | $148.04 | +32.13 | — | +0.00 | +32.13 | +89.59 | — |
| 2026-08-18 | `FANG` | 12 | $206.29 | $208.93 | +31.68 | — | +0.00 | +31.68 | +74.76 | — |
| 2026-08-18 | `OUST` | 52 | $48.13 | $45.09 | -158.08 | — | +0.00 | -158.08 | -203.32 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 14 | — | $91.01 | +0.00 | $93.63 | +36.68 | +36.68 | +0.00 | +36.68 |
| 2026-08-20 | `APA` | 28 | — | $44.76 | +0.00 | $44.39 | -10.36 | -10.36 | +0.00 | -10.36 |
| 2026-08-20 | `AUTL` | 518 | — | $2.47 | +0.00 | $2.46 | -5.18 | -5.18 | +0.00 | -5.18 |
| 2026-08-20 | `CRSP` | 21 | — | $58.73 | +0.00 | $58.12 | -12.81 | -12.81 | +0.00 | -12.81 |
| 2026-08-20 | `ASST` | 79 | — | $16.00 | +0.00 | $16.13 | +10.27 | +10.27 | +0.00 | +10.27 |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `ZLAB` | 48 | — | $26.57 | +0.00 | $26.02 | -26.40 | -26.40 | +0.00 | -26.40 |
| 2026-08-20 | `TEAM` | 7 | — | $173.90 | +0.00 | $174.91 | +7.07 | +7.07 | +0.00 | +7.07 |
| 2026-08-21 | `BHP` | 14 | $93.63 | $95.72 | +29.26 | — | +0.00 | +29.26 | +65.94 | — |
| 2026-08-21 | `APA` | 28 | $44.39 | $44.52 | +3.64 | — | +0.00 | +3.64 | -6.72 | — |
| 2026-08-21 | `AUTL` | 518 | $2.46 | $2.47 | +5.18 | $2.41 | -31.08 | -25.90 | +0.00 | -31.08 |
| 2026-08-21 | `CRSP` | 21 | $58.12 | $59.72 | +33.60 | $59.50 | -4.62 | +28.98 | +20.79 | +16.17 |
| 2026-08-21 | `ASST` | 79 | $16.13 | $17.66 | +120.87 | — | +0.00 | +120.87 | +131.14 | — |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | — | +0.00 | -1.68 | -136.24 | — |
| 2026-08-21 | `ZLAB` | 48 | $26.02 | $26.25 | +11.04 | — | +0.00 | +11.04 | -15.36 | — |
| 2026-08-21 | `TEAM` | 7 | $174.91 | $174.22 | -4.83 | — | +0.00 | -4.83 | +2.24 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `FUTU` | 11 | — | $115.18 | +0.00 | $123.64 | +93.06 | +93.06 | +0.00 | +93.06 |
| 2026-08-21 | `GRAL` | 16 | — | $78.88 | +0.00 | $79.54 | +10.56 | +10.56 | +0.00 | +10.56 |
| 2026-08-21 | `ABTC` | 148 | — | $8.66 | +0.00 | $7.93 | -108.04 | -108.04 | +0.00 | -108.04 |
| 2026-08-21 | `HIVE` | 397 | — | $3.24 | +0.00 | $3.03 | -83.37 | -83.37 | +0.00 | -83.37 |
| 2026-08-21 | `MARA` | 110 | — | $11.70 | +0.00 | $11.26 | -48.40 | -48.40 | +0.00 | -48.40 |
| 2026-08-24 | `AUTL` | 518 | $2.41 | $2.40 | -5.18 | — | +0.00 | -5.18 | -36.26 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | $57.08 | -35.17 | -50.92 | +0.42 | -34.75 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `FUTU` | 11 | $123.64 | $121.00 | -29.04 | — | +0.00 | -29.04 | +64.02 | — |
| 2026-08-24 | `GRAL` | 16 | $79.54 | $81.87 | +37.28 | — | +0.00 | +37.28 | +47.84 | — |
| 2026-08-24 | `ABTC` | 148 | $7.93 | $8.00 | +10.36 | — | +0.00 | +10.36 | -97.68 | — |
| 2026-08-24 | `HIVE` | 397 | $3.03 | $2.99 | -15.88 | — | +0.00 | -15.88 | -99.25 | — |
| 2026-08-24 | `MARA` | 110 | $11.26 | $11.17 | -9.90 | — | +0.00 | -9.90 | -58.30 | — |
| 2026-08-25 | `CRSP` | 21 | $57.08 | $57.93 | +17.95 | — | +0.00 | +17.95 | -16.80 | — |
| 2026-08-25 | `AU` | 16 | — | $118.52 | +0.00 | $123.39 | +77.92 | +77.92 | +0.00 | +77.92 |
| 2026-08-25 | `FCX` | 25 | — | $77.13 | +0.00 | $79.91 | +69.50 | +69.50 | +0.00 | +69.50 |
| 2026-08-25 | `EZPW` | 57 | — | $35.05 | +0.00 | $35.23 | +10.26 | +10.26 | +0.00 | +10.26 |
| 2026-08-25 | `RUM` | 212 | — | $9.42 | +0.00 | $10.23 | +171.72 | +171.72 | +0.00 | +171.72 |
| 2026-08-25 | `ZYME` | 69 | — | $28.86 | +0.00 | $27.47 | -95.91 | -95.91 | +0.00 | -95.91 |
| 2026-08-26 | `AU` | 16 | $123.39 | $119.80 | -57.44 | — | +0.00 | -57.44 | +20.48 | — |
| 2026-08-26 | `FCX` | 25 | $79.91 | $79.34 | -14.25 | — | +0.00 | -14.25 | +55.25 | — |
| 2026-08-26 | `EZPW` | 57 | $35.23 | $35.70 | +26.79 | — | +0.00 | +26.79 | +37.05 | — |
| 2026-08-26 | `RUM` | 212 | $10.23 | $10.07 | -33.92 | — | +0.00 | -33.92 | +137.80 | — |
| 2026-08-26 | `ZYME` | 69 | $27.47 | $27.56 | +6.21 | — | +0.00 | +6.21 | -89.70 | — |
| 2026-08-26 | `FNV` | 38 | — | $267.02 | +0.00 | $267.37 | +13.30 | +13.30 | +0.00 | +13.30 |
| 2026-08-27 | `FNV` | 38 | $267.37 | $267.23 | -5.32 | — | +0.00 | -5.32 | +7.98 | — |
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
| 2026-08-28 | `SEDG` | 38 | — | $32.90 | +0.00 | $31.41 | -56.62 | -56.62 | +0.00 | -56.62 |
| 2026-08-28 | `TLS` | 259 | — | $4.82 | +0.00 | $4.79 | -7.77 | -7.77 | +0.00 | -7.77 |
| 2026-08-31 | `KEYS` | 3 | $319.97 | $322.49 | +7.56 | — | +0.00 | +7.56 | -5.76 | — |
| 2026-08-31 | `SMTC` | 8 | $131.17 | $132.30 | +9.04 | — | +0.00 | +9.04 | -75.68 | — |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -65.94 | — |
| 2026-08-31 | `DDOG` | 5 | $236.98 | $233.97 | -15.07 | — | +0.00 | -15.07 | -31.27 | — |
| 2026-08-31 | `ADSK` | 4 | $260.66 | $257.71 | -11.80 | — | +0.00 | -11.80 | -13.80 | — |
| 2026-08-31 | `SEDG` | 38 | $31.41 | $31.15 | -9.88 | — | +0.00 | -9.88 | -66.50 | — |
| 2026-08-31 | `TLS` | 259 | $4.79 | $4.81 | +5.18 | — | +0.00 | +5.18 | -2.59 | — |
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
| 2026-09-04 | `BAK` | 1125 | — | $1.94 | +0.00 | $1.89 | -56.25 | -56.25 | +0.00 | -56.25 |
| 2026-09-08 | `FRNM` | 76 | $16.31 | $16.74 | +32.68 | — | +0.00 | +32.68 | +66.12 | — |
| 2026-09-08 | `CRM` | 8 | $259.23 | $253.72 | -44.08 | — | +0.00 | -44.08 | -77.12 | — |
| 2026-09-08 | `MRX` | 28 | $78.27 | $78.84 | +15.96 | $76.71 | -59.64 | -43.68 | +89.32 | +29.68 |
| 2026-09-08 | `BE` | 9 | $252.87 | $267.76 | +134.01 | — | +0.00 | +134.01 | +278.46 | — |
| 2026-09-08 | `BAK` | 1125 | $1.89 | $1.94 | +56.25 | — | +0.00 | +56.25 | +0.00 | — |
| 2026-09-09 | `MRX` | 28 | $76.71 | $76.60 | -3.08 | — | +0.00 | -3.08 | +26.60 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 20 | — | $164.43 | +0.00 | $150.28 | -283.00 | -283.00 | +0.00 | -283.00 |
| 2026-09-11 | `ADBE` | 14 | — | $242.17 | +0.00 | $252.23 | +140.84 | +140.84 | +0.00 | +140.84 |
| 2026-09-11 | `BAK` | 1602 | — | $2.12 | +0.00 | $2.08 | -64.08 | -64.08 | +0.00 | -64.08 |
| 2026-09-14 | `ORCL` | 20 | $150.28 | $141.42 | -177.20 | — | +0.00 | -177.20 | -460.20 | — |
| 2026-09-14 | `ADBE` | 14 | $252.23 | $261.51 | +129.92 | — | +0.00 | +129.92 | +270.76 | — |
| 2026-09-14 | `BAK` | 1602 | $2.08 | $2.05 | -48.06 | — | +0.00 | -48.06 | -112.14 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +127.16 | ANGX, ARX, HLIT, MH, NRG, TLN, VST | — | $1,560.49 | $10,110.67 | ANGX×290, ARX×63, HLIT×94, MH×92, NRG×10, TLN×3, VST×8 |
| 2026-08-17 | +2.25 | $1,560.49 | ANGX×290, ARX×63, HLIT×94, MH×92, NRG×10, TLN×3, VST×8 | $10,211.68 | +101.01 | +131.75 | DVN, EOG, FANG, OUST | ANGX, ARX, HLIT, MH, NRG, TLN, VST | $239.24 | $10,318.38 | DVN×55, EOG×17, FANG×12, OUST×52 |
| 2026-08-18 | -6.20 | $239.24 | DVN×55, EOG×17, FANG×12, OUST×52 | $10,247.76 | -70.62 | +0.00 | — | DVN, EOG, FANG, OUST | $10,239.27 | $10,239.27 | — |
| 2026-08-19 | -7.20 | $10,239.27 | — | $10,239.27 | +0.00 | +0.00 | — | — | $10,239.27 | $10,239.27 | — |
| 2026-08-20 | +1.12 | $10,239.27 | — | $10,239.27 | +0.00 | -135.29 | BHP, APA, AUTL, CRSP, ASST, MRNA, ZLAB, TEAM | — | $220.05 | $10,082.75 | BHP×14, APA×28, AUTL×518, CRSP×21, ASST×79, MRNA×8, ZLAB×48, TEAM×7 |
| 2026-08-21 | +3.25 | $220.05 | BHP×14, APA×28, AUTL×518, CRSP×21, ASST×79, MRNA×8, ZLAB×48, TEAM×7 | $10,279.83 | +197.08 | -153.99 | AU, FUTU, GRAL, ABTC, HIVE, MARA | BHP, APA, ASST, MRNA, ZLAB, TEAM | $139.36 | $10,097.27 | AUTL×518, CRSP×21, AU×10, FUTU×11, GRAL×16, ABTC×148, HIVE×397, MARA×110 |
| 2026-08-24 | -5.17 | $139.36 | AUTL×518, CRSP×21, AU×10, FUTU×11, GRAL×16, ABTC×148, HIVE×397, MARA×110 | $10,062.06 | -35.21 | -35.17 | — | AUTL, AU, FUTU, GRAL, ABTC, HIVE, MARA | $8,805.38 | $10,003.95 | CRSP×21 |
| 2026-08-25 | +1.80 | $8,805.38 | CRSP×21 | $10,021.91 | +17.96 | +233.49 | AU, FCX, EZPW, RUM, ZYME | CRSP | $197.84 | $10,242.13 | AU×16, FCX×25, EZPW×57, RUM×212, ZYME×69 |
| 2026-08-26 | +2.02 | $197.84 | AU×16, FCX×25, EZPW×57, RUM×212, ZYME×69 | $10,169.52 | -72.61 | +13.30 | FNV | AU, FCX, EZPW, RUM, ZYME | $9.30 | $10,169.36 | FNV×38 |
| 2026-08-27 | — | $9.30 | FNV×38 | $10,164.04 | -5.32 | -58.32 | ACMR, MU, ASML, LRCX, NVDA | FNV | $592.60 | $10,093.45 | ACMR×24, MU×2, ASML×1, LRCX×6, NVDA×9 |
| 2026-08-28 | +0.75 | $592.60 | ACMR×24, MU×2, ASML×1, LRCX×6, NVDA×9 | $10,022.83 | -70.62 | -246.57 | KEYS, SMTC, CIEN, DDOG, ADSK, SEDG, TLS | ACMR, MU, ASML, LRCX, NVDA | $1,944.27 | $9,750.59 | KEYS×3, SMTC×8, CIEN×3, DDOG×5, ADSK×4, SEDG×38, TLS×259 |
| 2026-08-31 | -5.85 | $1,944.27 | KEYS×3, SMTC×8, CIEN×3, DDOG×5, ADSK×4, SEDG×38, TLS×259 | $9,735.62 | -14.97 | +0.00 | — | KEYS, SMTC, CIEN, DDOG, ADSK, SEDG, TLS | $9,719.98 | $9,719.98 | — |
| 2026-09-01 | -6.30 | $9,719.98 | — | $9,719.98 | +0.00 | +0.00 | — | — | $9,719.98 | $9,719.98 | — |
| 2026-09-02 | -3.83 | $9,719.98 | — | $9,719.98 | +0.00 | +0.00 | — | — | $9,719.98 | $9,719.98 | — |
| 2026-09-03 | -0.90 | $9,719.98 | — | $9,719.98 | +0.00 | +363.73 | AVGO, DELL, CXW, FRNM, MMED, DE, HPE, CNXC | — | $1,003.01 | $10,067.10 | AVGO×3, DELL×2, CXW×37, FRNM×76, MMED×50, DE×1, HPE×25, CNXC×36 |
| 2026-09-04 | +2.25 | $1,003.01 | AVGO×3, DELL×2, CXW×37, FRNM×76, MMED×50, DE×1, HPE×25, CNXC×36 | $9,993.65 | -73.45 | +121.68 | CRM, MRX, BE, BAK | AVGO, DELL, CXW, MMED, DE, HPE, CNXC | $173.14 | $10,080.18 | FRNM×76, CRM×8, MRX×28, BE×9, BAK×1125 |
| 2026-09-08 | -11.47 | $173.14 | FRNM×76, CRM×8, MRX×28, BE×9, BAK×1125 | $10,275.00 | +194.82 | -59.64 | — | FRNM, CRM, BE, BAK | $8,046.44 | $10,194.32 | MRX×28 |
| 2026-09-09 | -13.95 | $8,046.44 | MRX×28 | $10,191.24 | -3.08 | +0.00 | — | MRX | $10,189.14 | $10,189.14 | — |
| 2026-09-10 | -13.28 | $10,189.14 | — | $10,189.14 | -0.00 | +0.00 | — | — | $10,189.14 | $10,189.14 | — |
| 2026-09-11 | +0.50 | $10,189.14 | — | $10,189.14 | -0.00 | -206.24 | ORCL, ADBE, BAK | — | $89.17 | $9,958.15 | ORCL×20, ADBE×14, BAK×1602 |
| 2026-09-14 | -11.00 | $89.17 | ORCL×20, ADBE×14, BAK×1602 | $9,862.81 | -95.34 | +0.00 | — | ORCL, ADBE, BAK | $9,837.70 | $9,837.70 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $8,746.36 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $7,511.27 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $6,270.08 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $5,021.21 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $3,819.19 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $2,737.70 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $1,560.49 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,560.49 | ▲ close $10,110.67 vs 09:30 $10,000.00 (session +127.16) | 16:00 close · cash $1,560.49 · equity $10,110.67 vs 09:30 $10,000.00 (+110.67; session marks +127.16) · 7 name(s) marked open→close (per-name table). ANGX×290 09:30 $4.31 → close $4.37 +17.40; ARX×63 09:30 $19.57 → close $19.58 +0.63; HLIT×94 09:30 $13.18 → close $13.92 +69.56; MH×92 09:30 $13.55 → close $13.10 -41.40; NRG×10 09:30 $120.00 → close $126.24 +62.40; TLN×3 09:30 $359.83 → close $362.74 +8.73; VST×8 09:30 $146.90 → close $148.13 +9.84 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,560.49 | ▲ 09:30 equity $10,211.68 vs yday $10,110.67 (+101.01) | 09:30 open · cash $1,560.49 (unchanged overnight, no fees) · equity $10,211.68 vs prior close $10,110.67 (+101.01) · 7 name(s) re-marked at the open (per-name table). ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; HLIT×94 yday $13.92 → 09:30 $13.84 -7.52; MH×92 yday $13.10 → 09:30 $13.16 +5.52; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92 | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $2,890.69 | ▲ +76.56 after sell → book $10,207.88; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $4,121.40 | ▼ -4.38 after sell → book $10,205.68; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $5,420.06 | ▲ +57.47 after sell → book $10,203.38; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $6,628.49 | ▼ -40.44 after sell → book $10,201.09; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $7,900.45 | ▲ +69.94 after sell → book $10,199.05; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $9,002.07 | ▲ +20.13 after sell → book $10,197.03; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $10,195.00 | ▲ +15.71 after sell → book $10,195.00; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 55 | $46.18 | $2.15 | — | $7,652.94 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+6.7; leftover $2548.75 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 17 | $142.77 | $2.04 | — | $5,223.81 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+5.8; leftover $2548.75 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 12 | $202.70 | $2.03 | — | $2,789.39 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+8.3; leftover $2548.75 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 52 | $49.00 | $2.15 | — | $239.24 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $2548.75 | join🟡 sector🟢 gen🟢 news🟢 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $239.24 | ▲ close $10,318.38 vs 09:30 $10,211.68 (session +131.75) | 16:00 close · cash $239.24 · equity $10,318.38 vs 09:30 $10,211.68 (+106.70; session marks +131.75) · 4 name(s) marked open→close (per-name table). DVN×55 09:30 $46.18 → close $47.57 +76.45; EOG×17 09:30 $142.77 → close $146.15 +57.46; FANG×12 09:30 $202.70 → close $206.29 +43.08; OUST×52 09:30 $49.00 → close $48.13 -45.24 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $239.24 | ▼ 09:30 equity $10,247.76 vs yday $10,318.38 (-70.62) | 09:30 open · cash $239.24 (unchanged overnight, no fees) · equity $10,247.76 vs prior close $10,318.38 (-70.62) · 4 name(s) re-marked at the open (per-name table). DVN×55 yday $47.57 → 09:30 $48.00 +23.65; EOG×17 yday $146.15 → 09:30 $148.04 +32.13; FANG×12 yday $206.29 → 09:30 $208.93 +31.68; OUST×52 yday $48.13 → 09:30 $45.09 -158.08 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 55 | $48.00 | $2.19 | $+95.76 | $2,877.05 | ▲ +95.76 after sell → book $10,245.57; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 17 | $148.04 | $2.07 | $+85.48 | $5,391.66 | ▲ +85.48 after sell → book $10,243.50; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 12 | $208.93 | $2.06 | $+70.68 | $7,896.77 | ▲ +70.68 after sell → book $10,241.45; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 52 | $45.09 | $2.17 | $-207.64 | $10,239.27 | ▼ -207.64 after sell → book $10,239.27; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,239.27 | ▲ close $10,239.27 vs 09:30 $10,247.76 (session +0.00) | 16:00 close · cash $10,239.27 · no lots left · equity $10,239.27. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,239.27 | ▲ 09:30 equity $10,239.27 vs yday $10,239.27 (+0.00) | 09:30 open · cash $10,239.27 · no holdings · equity $10,239.27 vs prior close $10,239.27 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,239.27 | ▲ close $10,239.27 vs 09:30 $10,239.27 (session +0.00) | 16:00 close · cash $10,239.27 · no lots left · equity $10,239.27. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,239.27 | ▲ 09:30 equity $10,239.27 vs yday $10,239.27 (+0.00) | 09:30 open · cash $10,239.27 · no holdings · equity $10,239.27 vs prior close $10,239.27 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,963.10 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1279.91 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $7,707.75 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1279.91 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 518 | $2.47 | $6.68 | — | $6,421.60 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1279.91 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $5,186.22 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1279.91 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 79 | $16.00 | $2.23 | — | $3,919.99 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1279.91 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $2,716.86 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1279.91 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 48 | $26.57 | $2.13 | — | $1,439.36 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1279.91 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TEAM` | 7 | $173.90 | $2.01 | — | $220.05 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+12.2; leftover $1279.91 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $220.05 | ▼ close $10,082.75 vs 09:30 $10,239.27 (session -135.29) | 16:00 close · cash $220.05 · equity $10,082.75 vs 09:30 $10,239.27 (-156.52; session marks -135.29) · 8 name(s) marked open→close (per-name table). BHP×14 09:30 $91.01 → close $93.63 +36.68; APA×28 09:30 $44.76 → close $44.39 -10.36; AUTL×518 09:30 $2.47 → close $2.46 -5.18; CRSP×21 09:30 $58.73 → close $58.12 -12.81; ASST×79 09:30 $16.00 → close $16.13 +10.27; MRNA×8 09:30 $150.14 → close $133.32 -134.56; ZLAB×48 09:30 $26.57 → close $26.02 -26.40; TEAM×7 09:30 $173.90 → close $174.91 +7.07 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $220.05 | ▲ 09:30 equity $10,279.83 vs yday $10,082.75 (+197.08) | 09:30 open · cash $220.05 (unchanged overnight, no fees) · equity $10,279.83 vs prior close $10,082.75 (+197.08) · 8 name(s) re-marked at the open (per-name table). BHP×14 yday $93.63 → 09:30 $95.72 +29.26; APA×28 yday $44.39 → 09:30 $44.52 +3.64; AUTL×518 yday $2.46 → 09:30 $2.47 +5.18; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; ASST×79 yday $16.13 → 09:30 $17.66 +120.87; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; ZLAB×48 yday $26.02 → 09:30 $26.25 +11.04; TEAM×7 yday $174.91 → 09:30 $174.22 -4.83 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 14 | $95.72 | $2.05 | $+61.86 | $1,558.08 | ▲ +61.86 after sell → book $10,277.78; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $2,802.55 | ▼ -10.89 after sell → book $10,275.69; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 79 | $17.66 | $2.25 | $+126.66 | $4,195.44 | ▲ +126.66 after sell → book $10,273.44; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $5,258.28 | ▼ -140.29 after sell → book $10,271.40; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 48 | $26.25 | $2.15 | $-19.65 | $6,516.13 | ▼ -19.65 after sell → book $10,269.25; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TEAM` | 7 | $174.22 | $2.03 | $-1.80 | $7,733.64 | ▼ -1.80 after sell → book $10,267.22; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,537.32 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1288.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $5,268.31 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1288.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 16 | $78.88 | $2.04 | — | $4,004.20 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1288.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 148 | $8.66 | $2.43 | — | $2,720.08 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1288.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 397 | $3.24 | $5.12 | — | $1,428.68 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $1288.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 110 | $11.70 | $2.32 | — | $139.36 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1288.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $139.36 | ▼ close $10,097.27 vs 09:30 $10,279.83 (session -153.99) | 16:00 close · cash $139.36 · equity $10,097.27 vs 09:30 $10,279.83 (-182.56; session marks -153.99) · 8 name(s) marked open→close (per-name table). AUTL×518 09:30 $2.47 → close $2.41 -31.08; CRSP×21 09:30 $59.72 → close $59.50 -4.62; AU×10 09:30 $119.43 → close $121.22 +17.90; FUTU×11 09:30 $115.18 → close $123.64 +93.06; GRAL×16 09:30 $78.88 → close $79.54 +10.56; ABTC×148 09:30 $8.66 → close $7.93 -108.04; HIVE×397 09:30 $3.24 → close $3.03 -83.37; MARA×110 09:30 $11.70 → close $11.26 -48.40 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $139.36 | ▼ 09:30 equity $10,062.06 vs yday $10,097.27 (-35.21) | 09:30 open · cash $139.36 (unchanged overnight, no fees) · equity $10,062.06 vs prior close $10,097.27 (-35.21) · 8 name(s) re-marked at the open (per-name table). AUTL×518 yday $2.41 → 09:30 $2.40 -5.18; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; AU×10 yday $121.22 → 09:30 $120.51 -7.10; FUTU×11 yday $123.64 → 09:30 $121.00 -29.04; GRAL×16 yday $79.54 → 09:30 $81.87 +37.28; ABTC×148 yday $7.93 → 09:30 $8.00 +10.36; HIVE×397 yday $3.03 → 09:30 $2.99 -15.88; MARA×110 yday $11.26 → 09:30 $11.17 -9.90 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 518 | $2.40 | $6.78 | $-49.72 | $1,375.78 | ▼ -49.72 after sell → book $10,055.28; vs 09:30 mark -6.78 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $2,578.84 | ▲ +6.74 after sell → book $10,053.24; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $121.00 | $2.04 | $+59.95 | $3,907.80 | ▲ +59.95 after sell → book $10,051.20; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 16 | $81.87 | $2.06 | $+43.74 | $5,215.66 | ▲ +43.74 after sell → book $10,049.14; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 148 | $8.00 | $2.47 | $-102.58 | $6,397.19 | ▼ -102.58 after sell → book $10,046.67; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 397 | $2.99 | $5.20 | $-109.57 | $7,579.02 | ▼ -109.57 after sell → book $10,041.47; vs 09:30 mark -5.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 110 | $11.17 | $2.35 | $-62.97 | $8,805.38 | ▼ -62.97 after sell → book $10,039.13; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,805.38 | ▼ close $10,003.95 vs 09:30 $10,062.06 (session -35.17) | 16:00 close · cash $8,805.38 · equity $10,003.95 vs 09:30 $10,062.06 (-58.11; session marks -35.17) · 1 name(s) marked open→close (per-name table). CRSP×21 09:30 $58.75 → close $57.08 -35.17 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,805.38 | ▲ 09:30 equity $10,021.91 vs yday $10,003.95 (+17.96) | 09:30 open · cash $8,805.38 (unchanged overnight, no fees) · equity $10,021.91 vs prior close $10,003.95 (+17.96) · 1 name(s) re-marked at the open (per-name table). CRSP×21 yday $57.08 → 09:30 $57.93 +17.95 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-20.93 | $10,019.83 | ▼ -20.93 after sell → book $10,019.83; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 16 | $118.52 | $2.04 | — | $8,121.48 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $2003.97 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 25 | $77.13 | $2.06 | — | $6,191.16 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $2003.97 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 57 | $35.05 | $2.16 | — | $4,191.15 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $2003.97 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 212 | $9.42 | $2.73 | — | $2,191.37 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $2003.97 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 69 | $28.86 | $2.20 | — | $197.84 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+13.7; leftover $2003.97 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $197.84 | ▲ close $10,242.13 vs 09:30 $10,021.91 (session +233.49) | 16:00 close · cash $197.84 · equity $10,242.13 vs 09:30 $10,021.91 (+220.22; session marks +233.49) · 5 name(s) marked open→close (per-name table). AU×16 09:30 $118.52 → close $123.39 +77.92; FCX×25 09:30 $77.13 → close $79.91 +69.50; EZPW×57 09:30 $35.05 → close $35.23 +10.26; RUM×212 09:30 $9.42 → close $10.23 +171.72; ZYME×69 09:30 $28.86 → close $27.47 -95.91 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $197.84 | ▼ 09:30 equity $10,169.52 vs yday $10,242.13 (-72.61) | 09:30 open · cash $197.84 (unchanged overnight, no fees) · equity $10,169.52 vs prior close $10,242.13 (-72.61) · 5 name(s) re-marked at the open (per-name table). AU×16 yday $123.39 → 09:30 $119.80 -57.44; FCX×25 yday $79.91 → 09:30 $79.34 -14.25; EZPW×57 yday $35.23 → 09:30 $35.70 +26.79; RUM×212 yday $10.23 → 09:30 $10.07 -33.92; ZYME×69 yday $27.47 → 09:30 $27.56 +6.21 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 16 | $119.80 | $2.06 | $+16.38 | $2,112.57 | ▲ +16.38 after sell → book $10,167.45; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 25 | $79.34 | $2.09 | $+51.09 | $4,093.98 | ▲ +51.09 after sell → book $10,165.36; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 57 | $35.70 | $2.19 | $+32.70 | $6,126.70 | ▲ +32.70 after sell → book $10,163.18; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 212 | $10.07 | $2.79 | $+132.28 | $8,258.75 | ▲ +132.28 after sell → book $10,160.39; vs 09:30 mark -2.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 69 | $27.56 | $2.22 | $-94.12 | $10,158.17 | ▼ -94.12 after sell → book $10,158.17; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 38 | $267.02 | $2.10 | — | $9.30 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $10158.17 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.30 | ▲ close $10,169.36 vs 09:30 $10,169.52 (session +13.30) | 16:00 close · cash $9.30 · equity $10,169.36 vs 09:30 $10,169.52 (-0.16; session marks +13.30) · 1 name(s) marked open→close (per-name table). FNV×38 09:30 $267.02 → close $267.37 +13.30 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.30 | ▼ 09:30 equity $10,164.04 vs yday $10,169.36 (-5.32) | 09:30 open · cash $9.30 (unchanged overnight, no fees) · equity $10,164.04 vs prior close $10,169.36 (-5.32) · 1 name(s) re-marked at the open (per-name table). FNV×38 yday $267.37 → 09:30 $267.23 -5.32 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 38 | $267.23 | $2.20 | $+3.68 | $10,161.85 | ▲ +3.68 after sell → book $10,161.85; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 24 | $81.65 | $2.06 | — | $8,200.18 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $2032.37 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 2 | $967.01 | $2.00 | — | $6,264.17 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $2032.37 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ASML` | 1 | $1746.53 | $1.99 | — | $4,515.64 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=-0.3; leftover $2032.37 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 6 | $318.88 | $2.01 | — | $2,600.36 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $2032.37 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 9 | $222.86 | $2.02 | — | $592.60 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $2032.37 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $592.60 | ▼ close $10,093.45 vs 09:30 $10,164.04 (session -58.32) | 16:00 close · cash $592.60 · equity $10,093.45 vs 09:30 $10,164.04 (-70.59; session marks -58.32) · 5 name(s) marked open→close (per-name table). ACMR×24 09:30 $81.65 → close $80.49 -27.84; MU×2 09:30 $967.01 → close $935.39 -63.24; ASML×1 09:30 $1746.53 → close $1735.01 -11.52; LRCX×6 09:30 $318.88 → close $318.58 -1.80; NVDA×9 09:30 $222.86 → close $227.98 +46.08 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $592.60 | ▼ 09:30 equity $10,022.83 vs yday $10,093.45 (-70.62) | 09:30 open · cash $592.60 (unchanged overnight, no fees) · equity $10,022.83 vs prior close $10,093.45 (-70.62) · 5 name(s) re-marked at the open (per-name table). ACMR×24 yday $80.49 → 09:30 $79.27 -29.28; MU×2 yday $935.39 → 09:30 $919.29 -32.20; ASML×1 yday $1735.01 → 09:30 $1734.75 -0.26; LRCX×6 yday $318.58 → 09:30 $318.03 -3.30; NVDA×9 yday $227.98 → 09:30 $227.36 -5.58 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 24 | $79.27 | $2.09 | $-61.27 | $2,492.99 | ▼ -61.27 after sell → book $10,020.74; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 2 | $919.29 | $2.02 | $-99.46 | $4,329.55 | ▼ -99.46 after sell → book $10,018.72; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ASML` | 1 | $1734.75 | $2.02 | $-15.79 | $6,062.29 | ▼ -15.79 after sell → book $10,016.71; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 6 | $318.03 | $2.03 | $-9.14 | $7,968.43 | ▼ -9.14 after sell → book $10,014.67; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 9 | $227.36 | $2.04 | $+36.44 | $10,012.63 | ▲ +36.44 after sell → book $10,012.63; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $9,037.40 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1251.58 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $7,901.31 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1251.58 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,698.05 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1251.58 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $5,494.94 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1251.58 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $4,448.30 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; ret5=+7.8; leftover $1251.58 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 38 | $32.90 | $2.10 | — | $3,196.00 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1251.58 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 259 | $4.82 | $3.34 | — | $1,944.27 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1251.58 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,944.27 | ▼ close $9,750.59 vs 09:30 $10,022.83 (session -246.57) | 16:00 close · cash $1,944.27 · equity $9,750.59 vs 09:30 $10,022.83 (-272.24; session marks -246.57) · 7 name(s) marked open→close (per-name table). KEYS×3 09:30 $324.41 → close $319.97 -13.32; SMTC×8 09:30 $141.76 → close $131.17 -84.72; CIEN×3 09:30 $400.42 → close $378.44 -65.94; DDOG×5 09:30 $240.22 → close $236.98 -16.20; ADSK×4 09:30 $261.16 → close $260.66 -2.00; SEDG×38 09:30 $32.90 → close $31.41 -56.62; TLS×259 09:30 $4.82 → close $4.79 -7.77 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,944.27 | ▼ 09:30 equity $9,735.62 vs yday $9,750.59 (-14.97) | 09:30 open · cash $1,944.27 (unchanged overnight, no fees) · equity $9,735.62 vs prior close $9,750.59 (-14.97) · 7 name(s) re-marked at the open (per-name table). KEYS×3 yday $319.97 → 09:30 $322.49 +7.56; SMTC×8 yday $131.17 → 09:30 $132.30 +9.04; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; DDOG×5 yday $236.98 → 09:30 $233.97 -15.07; ADSK×4 yday $260.66 → 09:30 $257.71 -11.80; SEDG×38 yday $31.41 → 09:30 $31.15 -9.88; TLS×259 yday $4.79 → 09:30 $4.81 +5.18 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 3 | $322.49 | $2.02 | $-9.78 | $2,909.73 | ▼ -9.78 after sell → book $9,733.60; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $132.30 | $2.03 | $-79.73 | $3,966.09 | ▼ -79.73 after sell → book $9,731.57; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $5,099.39 | ▼ -69.96 after sell → book $9,729.55; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 5 | $233.97 | $2.02 | $-35.31 | $6,267.19 | ▼ -35.31 after sell → book $9,727.52; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $257.71 | $2.02 | $-17.82 | $7,296.01 | ▼ -17.82 after sell → book $9,725.50; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 38 | $31.15 | $2.12 | $-70.73 | $8,477.59 | ▼ -70.73 after sell → book $9,723.38; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 259 | $4.81 | $3.39 | $-9.33 | $9,719.98 | ▼ -9.33 after sell → book $9,719.98; vs 09:30 mark -3.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,719.98 | ▲ close $9,719.98 vs 09:30 $9,735.62 (session +0.00) | 16:00 close · cash $9,719.98 · no lots left · equity $9,719.98. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,719.98 | ▲ 09:30 equity $9,719.98 vs yday $9,719.98 (+0.00) | 09:30 open · cash $9,719.98 · no holdings · equity $9,719.98 vs prior close $9,719.98 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,719.98 | ▲ close $9,719.98 vs 09:30 $9,719.98 (session +0.00) | 16:00 close · cash $9,719.98 · no lots left · equity $9,719.98. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,719.98 | ▲ 09:30 equity $9,719.98 vs yday $9,719.98 (+0.00) | 09:30 open · cash $9,719.98 · no holdings · equity $9,719.98 vs prior close $9,719.98 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,719.98 | ▲ close $9,719.98 vs 09:30 $9,719.98 (session +0.00) | 16:00 close · cash $9,719.98 · no lots left · equity $9,719.98. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,719.98 | ▲ 09:30 equity $9,719.98 vs yday $9,719.98 (+0.00) | 09:30 open · cash $9,719.98 · no holdings · equity $9,719.98 vs prior close $9,719.98 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,662.76 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1215.00 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,688.15 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1215.00 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 37 | $32.31 | $2.10 | — | $6,490.58 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1215.00 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 76 | $15.87 | $2.22 | — | $5,282.24 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1215.00 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 50 | $23.88 | $2.14 | — | $4,086.10 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1215.00 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $3,380.86 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1215.00 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 25 | $47.60 | $2.06 | — | $2,188.79 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1215.00 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 36 | $32.88 | $2.10 | — | $1,003.01 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ret5=+16.2; leftover $1215.00 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,003.01 | ▲ close $10,067.10 vs 09:30 $9,719.98 (session +363.73) | 16:00 close · cash $1,003.01 · equity $10,067.10 vs 09:30 $9,719.98 (+347.12; session marks +363.73) · 8 name(s) marked open→close (per-name table). AVGO×3 09:30 $351.74 → close $357.16 +16.26; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×37 09:30 $32.31 → close $33.66 +49.95; FRNM×76 09:30 $15.87 → close $16.90 +78.28; MMED×50 09:30 $23.88 → close $23.84 -2.00; DE×1 09:30 $703.25 → close $694.41 -8.84; HPE×25 09:30 $47.60 → close $54.44 +171.00; CNXC×36 09:30 $32.88 → close $32.85 -1.08 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,003.01 | ▼ 09:30 equity $9,993.65 vs yday $10,067.10 (-73.45) | 09:30 open · cash $1,003.01 (unchanged overnight, no fees) · equity $9,993.65 vs prior close $10,067.10 (-73.45) · 8 name(s) re-marked at the open (per-name table). AVGO×3 yday $357.16 → 09:30 $359.70 +7.62; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×37 yday $33.66 → 09:30 $33.46 -7.40; FRNM×76 yday $16.90 → 09:30 $16.40 -38.00; MMED×50 yday $23.84 → 09:30 $23.84 +0.00; DE×1 yday $694.41 → 09:30 $692.03 -2.38; HPE×25 yday $54.44 → 09:30 $53.85 -14.75; CNXC×36 yday $32.85 → 09:30 $32.48 -13.32 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $2,080.09 | ▲ +19.86 after sell → book $9,991.63; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $3,105.64 | ▲ +50.93 after sell → book $9,989.62; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 37 | $33.46 | $2.12 | $+38.33 | $4,341.54 | ▲ +38.33 after sell → book $9,987.50; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 50 | $23.84 | $2.16 | $-6.30 | $5,531.38 | ▼ -6.30 after sell → book $9,985.34; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $6,221.39 | ▼ -15.23 after sell → book $9,983.32; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 25 | $53.85 | $2.09 | $+152.10 | $7,565.56 | ▲ +152.10 after sell → book $9,981.24; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 36 | $32.48 | $2.12 | $-18.62 | $8,732.72 | ▼ -18.62 after sell → book $9,979.12; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 8 | $263.36 | $2.01 | — | $6,623.83 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $2183.18 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 28 | $75.65 | $2.07 | — | $4,503.55 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $2183.18 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 9 | $236.82 | $2.02 | — | $2,370.15 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; ret5=+8.1; leftover $2183.18 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 1125 | $1.94 | $14.51 | — | $173.14 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $2183.18 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $173.14 | ▲ close $10,080.18 vs 09:30 $9,993.65 (session +121.68) | 16:00 close · cash $173.14 · equity $10,080.18 vs 09:30 $9,993.65 (+86.53; session marks +121.68) · 5 name(s) marked open→close (per-name table). FRNM×76 09:30 $16.40 → close $16.31 -6.84; CRM×8 09:30 $263.36 → close $259.23 -33.04; MRX×28 09:30 $75.65 → close $78.27 +73.36; BE×9 09:30 $236.82 → close $252.87 +144.45; BAK×1125 09:30 $1.94 → close $1.89 -56.25 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.14 | ▲ 09:30 equity $10,275.00 vs yday $10,080.18 (+194.82) | 09:30 open · cash $173.14 (unchanged overnight, no fees) · equity $10,275.00 vs prior close $10,080.18 (+194.82) · 5 name(s) re-marked at the open (per-name table). FRNM×76 yday $16.31 → 09:30 $16.74 +32.68; CRM×8 yday $259.23 → 09:30 $253.72 -44.08; MRX×28 yday $78.27 → 09:30 $78.84 +15.96; BE×9 yday $252.87 → 09:30 $267.76 +134.01; BAK×1125 yday $1.89 → 09:30 $1.94 +56.25 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 76 | $16.74 | $2.24 | $+61.66 | $1,443.14 | ▲ +61.66 after sell → book $10,272.76; vs 09:30 mark -2.24 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 8 | $253.72 | $2.04 | $-81.17 | $3,470.86 | ▼ -81.17 after sell → book $10,270.72; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 9 | $267.76 | $2.05 | $+274.40 | $5,878.65 | ▲ +274.40 after sell → book $10,268.67; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 1125 | $1.94 | $14.72 | $-29.23 | $8,046.44 | ▼ -29.23 after sell → book $10,253.96; vs 09:30 mark -14.71 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,046.44 | ▼ close $10,194.32 vs 09:30 $10,275.00 (session -59.64) | 16:00 close · cash $8,046.44 · equity $10,194.32 vs 09:30 $10,275.00 (-80.68; session marks -59.64) · 1 name(s) marked open→close (per-name table). MRX×28 09:30 $78.84 → close $76.71 -59.64 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,046.44 | ▼ 09:30 equity $10,191.24 vs yday $10,194.32 (-3.08) | 09:30 open · cash $8,046.44 (unchanged overnight, no fees) · equity $10,191.24 vs prior close $10,194.32 (-3.08) · 1 name(s) re-marked at the open (per-name table). MRX×28 yday $76.71 → 09:30 $76.60 -3.08 | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 28 | $76.60 | $2.10 | $+22.42 | $10,189.14 | ▲ +22.42 after sell → book $10,189.14; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,189.14 | ▲ close $10,189.14 vs 09:30 $10,191.24 (session +0.00) | 16:00 close · cash $10,189.14 · no lots left · equity $10,189.14. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,189.14 | ▲ 09:30 equity $10,189.14 vs yday $10,189.14 (-0.00) | 09:30 open · cash $10,189.14 · no holdings · equity $10,189.14 vs prior close $10,189.14 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,189.14 | ▲ close $10,189.14 vs 09:30 $10,189.14 (session +0.00) | 16:00 close · cash $10,189.14 · no lots left · equity $10,189.14. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,189.14 | ▲ 09:30 equity $10,189.14 vs yday $10,189.14 (-0.00) | 09:30 open · cash $10,189.14 · no holdings · equity $10,189.14 vs prior close $10,189.14 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 20 | $164.43 | $2.05 | — | $6,898.49 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $3396.38 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 14 | $242.17 | $2.03 | — | $3,506.07 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; ret5=-11.1; leftover $3396.38 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 1602 | $2.12 | $20.67 | — | $89.17 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $3396.38 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $89.17 | ▼ close $9,958.15 vs 09:30 $10,189.14 (session -206.24) | 16:00 close · cash $89.17 · equity $9,958.15 vs 09:30 $10,189.14 (-230.99; session marks -206.24) · 3 name(s) marked open→close (per-name table). ORCL×20 09:30 $164.43 → close $150.28 -283.00; ADBE×14 09:30 $242.17 → close $252.23 +140.84; BAK×1602 09:30 $2.12 → close $2.08 -64.08 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $89.17 | ▼ 09:30 equity $9,862.81 vs yday $9,958.15 (-95.34) | 09:30 open · cash $89.17 (unchanged overnight, no fees) · equity $9,862.81 vs prior close $9,958.15 (-95.34) · 3 name(s) re-marked at the open (per-name table). ORCL×20 yday $150.28 → 09:30 $141.42 -177.20; ADBE×14 yday $252.23 → 09:30 $261.51 +129.92; BAK×1602 yday $2.08 → 09:30 $2.05 -48.06 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 20 | $141.42 | $2.08 | $-464.33 | $2,915.49 | ▼ -464.33 after sell → book $9,860.73; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 14 | $261.51 | $2.07 | $+266.66 | $6,574.55 | ▲ +266.66 after sell → book $9,858.65; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 1602 | $2.05 | $20.96 | $-153.76 | $9,837.70 | ▼ -153.76 after sell → book $9,837.70; vs 09:30 mark -20.95 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,837.70 | ▲ close $9,837.70 vs 09:30 $9,862.81 (session +0.00) | 16:00 close · cash $9,837.70 · no lots left · equity $9,837.70. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-28 | `MPWR` | cash | leftover split 1251.58 < 1 share @ 1306.03 |
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
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
