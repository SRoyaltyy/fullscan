# Factor mine action — `union_news_or_net3_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 3

Cash book **-4.11%** ($9,590) · signal-only (no cash/fees) was -2.54%. Starts YES **0/26**. Fills 144 · skips 34 · realized $-335.16.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $23.50.

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
| 2026-08-17 | `DVN` | 36 | — | $46.18 | +0.00 | $47.57 | +50.04 | +50.04 | +0.00 | +50.04 |
| 2026-08-17 | `EOG` | 11 | — | $142.77 | +0.00 | $146.15 | +37.18 | +37.18 | +0.00 | +37.18 |
| 2026-08-17 | `FANG` | 8 | — | $202.70 | +0.00 | $206.29 | +28.72 | +28.72 | +0.00 | +28.72 |
| 2026-08-17 | `GLOB` | 45 | — | $37.18 | +0.00 | $36.26 | -41.40 | -41.40 | +0.00 | -41.40 |
| 2026-08-17 | `TPG` | 32 | — | $52.67 | +0.00 | $51.77 | -28.80 | -28.80 | +0.00 | -28.80 |
| 2026-08-17 | `OUST` | 34 | — | $49.00 | +0.00 | $48.13 | -29.58 | -29.58 | +0.00 | -29.58 |
| 2026-08-18 | `DVN` | 36 | $47.57 | $48.00 | +15.48 | — | +0.00 | +15.48 | +65.52 | — |
| 2026-08-18 | `EOG` | 11 | $146.15 | $148.04 | +20.79 | — | +0.00 | +20.79 | +57.97 | — |
| 2026-08-18 | `FANG` | 8 | $206.29 | $208.93 | +21.12 | — | +0.00 | +21.12 | +49.84 | — |
| 2026-08-18 | `GLOB` | 45 | $36.26 | $36.98 | +32.40 | — | +0.00 | +32.40 | -9.00 | — |
| 2026-08-18 | `TPG` | 32 | $51.77 | $51.77 | +0.00 | — | +0.00 | +0.00 | -28.80 | — |
| 2026-08-18 | `OUST` | 34 | $48.13 | $45.09 | -103.36 | — | +0.00 | -103.36 | -132.94 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `APA` | 28 | — | $44.76 | +0.00 | $44.39 | -10.36 | -10.36 | +0.00 | -10.36 |
| 2026-08-20 | `AUTL` | 511 | — | $2.47 | +0.00 | $2.46 | -5.11 | -5.11 | +0.00 | -5.11 |
| 2026-08-20 | `CRSP` | 21 | — | $58.73 | +0.00 | $58.12 | -12.81 | -12.81 | +0.00 | -12.81 |
| 2026-08-20 | `MRK` | 8 | — | $150.78 | +0.00 | $148.99 | -14.32 | -14.32 | +0.00 | -14.32 |
| 2026-08-20 | `ASST` | 79 | — | $16.00 | +0.00 | $16.13 | +10.27 | +10.27 | +0.00 | +10.27 |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `ZLAB` | 47 | — | $26.57 | +0.00 | $26.02 | -25.85 | -25.85 | +0.00 | -25.85 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `APA` | 28 | $44.39 | $44.52 | +3.64 | — | +0.00 | +3.64 | -6.72 | — |
| 2026-08-21 | `AUTL` | 511 | $2.46 | $2.47 | +5.11 | $2.41 | -30.66 | -25.55 | +0.00 | -30.66 |
| 2026-08-21 | `CRSP` | 21 | $58.12 | $59.72 | +33.60 | $59.50 | -4.62 | +28.98 | +20.79 | +16.17 |
| 2026-08-21 | `MRK` | 8 | $148.99 | $149.12 | +1.04 | — | +0.00 | +1.04 | -13.28 | — |
| 2026-08-21 | `ASST` | 79 | $16.13 | $17.66 | +120.87 | — | +0.00 | +120.87 | +131.14 | — |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | — | +0.00 | -1.68 | -136.24 | — |
| 2026-08-21 | `ZLAB` | 47 | $26.02 | $26.25 | +10.81 | — | +0.00 | +10.81 | -15.04 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `FUTU` | 11 | — | $115.18 | +0.00 | $123.64 | +93.06 | +93.06 | +0.00 | +93.06 |
| 2026-08-21 | `GRAL` | 16 | — | $78.88 | +0.00 | $79.54 | +10.56 | +10.56 | +0.00 | +10.56 |
| 2026-08-21 | `VIRT` | 20 | — | $60.66 | +0.00 | $67.93 | +145.40 | +145.40 | +0.00 | +145.40 |
| 2026-08-21 | `MFC` | 29 | — | $42.48 | +0.00 | $42.51 | +0.87 | +0.87 | +0.00 | +0.87 |
| 2026-08-21 | `ABTC` | 146 | — | $8.66 | +0.00 | $7.93 | -106.58 | -106.58 | +0.00 | -106.58 |
| 2026-08-24 | `AUTL` | 511 | $2.41 | $2.40 | -5.11 | — | +0.00 | -5.11 | -35.77 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | $57.08 | -35.17 | -50.92 | +0.42 | -34.75 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `FUTU` | 11 | $123.64 | $121.00 | -29.04 | — | +0.00 | -29.04 | +64.02 | — |
| 2026-08-24 | `GRAL` | 16 | $79.54 | $81.87 | +37.28 | — | +0.00 | +37.28 | +47.84 | — |
| 2026-08-24 | `VIRT` | 20 | $67.93 | $66.80 | -22.60 | — | +0.00 | -22.60 | +122.80 | — |
| 2026-08-24 | `MFC` | 29 | $42.51 | $42.31 | -5.80 | — | +0.00 | -5.80 | -4.93 | — |
| 2026-08-24 | `ABTC` | 146 | $7.93 | $8.00 | +10.22 | — | +0.00 | +10.22 | -96.36 | — |
| 2026-08-25 | `CRSP` | 21 | $57.08 | $57.93 | +17.95 | — | +0.00 | +17.95 | -16.80 | — |
| 2026-08-25 | `AU` | 21 | — | $118.52 | +0.00 | $123.39 | +102.27 | +102.27 | +0.00 | +102.27 |
| 2026-08-25 | `FCX` | 32 | — | $77.13 | +0.00 | $79.91 | +88.96 | +88.96 | +0.00 | +88.96 |
| 2026-08-25 | `EZPW` | 72 | — | $35.05 | +0.00 | $35.23 | +12.96 | +12.96 | +0.00 | +12.96 |
| 2026-08-25 | `AMX` | 106 | — | $23.80 | +0.00 | $23.75 | -5.30 | -5.30 | +0.00 | -5.30 |
| 2026-08-26 | `AU` | 21 | $123.39 | $119.80 | -75.39 | — | +0.00 | -75.39 | +26.88 | — |
| 2026-08-26 | `FCX` | 32 | $79.91 | $79.34 | -18.24 | — | +0.00 | -18.24 | +70.72 | — |
| 2026-08-26 | `EZPW` | 72 | $35.23 | $35.70 | +33.84 | — | +0.00 | +33.84 | +46.80 | — |
| 2026-08-26 | `AMX` | 106 | $23.75 | $23.75 | +0.00 | $23.62 | -13.78 | -13.78 | -5.30 | -19.08 |
| 2026-08-26 | `FNV` | 14 | — | $267.02 | +0.00 | $267.37 | +4.90 | +4.90 | +0.00 | +4.90 |
| 2026-08-26 | `ASST` | 187 | — | $20.72 | +0.00 | $21.50 | +145.86 | +145.86 | +0.00 | +145.86 |
| 2026-08-27 | `AMX` | 106 | $23.62 | $23.77 | +15.90 | — | +0.00 | +15.90 | -3.18 | — |
| 2026-08-27 | `FNV` | 14 | $267.37 | $267.23 | -1.96 | — | +0.00 | -1.96 | +2.94 | — |
| 2026-08-27 | `ASST` | 187 | $21.50 | $22.45 | +177.65 | — | +0.00 | +177.65 | +323.51 | — |
| 2026-08-27 | `ACMR` | 16 | — | $81.65 | +0.00 | $80.49 | -18.56 | -18.56 | +0.00 | -18.56 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `CM` | 11 | — | $118.77 | +0.00 | $114.84 | -43.23 | -43.23 | +0.00 | -43.23 |
| 2026-08-27 | `GEN` | 44 | — | $29.83 | +0.00 | $30.50 | +29.48 | +29.48 | +0.00 | +29.48 |
| 2026-08-27 | `LRCX` | 4 | — | $318.88 | +0.00 | $318.58 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-27 | `NVDA` | 5 | — | $222.86 | +0.00 | $227.98 | +25.60 | +25.60 | +0.00 | +25.60 |
| 2026-08-27 | `AXTI` | 18 | — | $70.30 | +0.00 | $66.92 | -60.84 | -60.84 | +0.00 | -60.84 |
| 2026-08-28 | `ACMR` | 16 | $80.49 | $79.27 | -19.52 | — | +0.00 | -19.52 | -38.08 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `CM` | 11 | $114.84 | $115.66 | +9.02 | — | +0.00 | +9.02 | -34.21 | — |
| 2026-08-28 | `GEN` | 44 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +29.48 | — |
| 2026-08-28 | `LRCX` | 4 | $318.58 | $318.03 | -2.20 | — | +0.00 | -2.20 | -3.40 | — |
| 2026-08-28 | `NVDA` | 5 | $227.98 | $227.36 | -3.10 | — | +0.00 | -3.10 | +22.50 | — |
| 2026-08-28 | `AXTI` | 18 | $66.92 | $65.29 | -29.34 | — | +0.00 | -29.34 | -90.18 | — |
| 2026-08-28 | `KEYS` | 4 | — | $324.41 | +0.00 | $319.97 | -17.76 | -17.76 | +0.00 | -17.76 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `DDOG` | 5 | — | $240.22 | +0.00 | $236.98 | -16.20 | -16.20 | +0.00 | -16.20 |
| 2026-08-28 | `PLAB` | 43 | — | $30.01 | +0.00 | $27.73 | -98.04 | -98.04 | +0.00 | -98.04 |
| 2026-08-28 | `ADSK` | 4 | — | $261.16 | +0.00 | $260.66 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-08-28 | `SEDG` | 39 | — | $32.90 | +0.00 | $31.41 | -58.11 | -58.11 | +0.00 | -58.11 |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | — | +0.00 | +10.08 | -7.68 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -65.94 | — |
| 2026-08-31 | `DDOG` | 5 | $236.98 | $233.97 | -15.07 | — | +0.00 | -15.07 | -31.27 | — |
| 2026-08-31 | `PLAB` | 43 | $27.73 | $28.04 | +13.33 | — | +0.00 | +13.33 | -84.71 | — |
| 2026-08-31 | `ADSK` | 4 | $260.66 | $257.71 | -11.80 | — | +0.00 | -11.80 | -13.80 | — |
| 2026-08-31 | `SEDG` | 39 | $31.41 | $31.15 | -10.14 | — | +0.00 | -10.14 | -68.25 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 4 | — | $351.74 | +0.00 | $357.16 | +21.68 | +21.68 | +0.00 | +21.68 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `CXW` | 44 | — | $32.31 | +0.00 | $33.66 | +59.40 | +59.40 | +0.00 | +59.40 |
| 2026-09-03 | `FRNM` | 90 | — | $15.87 | +0.00 | $16.90 | +92.70 | +92.70 | +0.00 | +92.70 |
| 2026-09-03 | `MMED` | 59 | — | $23.88 | +0.00 | $23.84 | -2.36 | -2.36 | +0.00 | -2.36 |
| 2026-09-03 | `DE` | 2 | — | $703.25 | +0.00 | $694.41 | -17.68 | -17.68 | +0.00 | -17.68 |
| 2026-09-03 | `HPE` | 30 | — | $47.60 | +0.00 | $54.44 | +205.20 | +205.20 | +0.00 | +205.20 |
| 2026-09-04 | `AVGO` | 4 | $357.16 | $359.70 | +10.16 | — | +0.00 | +10.16 | +31.84 | — |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | — | +0.00 | -5.22 | +54.94 | — |
| 2026-09-04 | `CXW` | 44 | $33.66 | $33.46 | -8.80 | — | +0.00 | -8.80 | +50.60 | — |
| 2026-09-04 | `FRNM` | 90 | $16.90 | $16.40 | -45.00 | $16.31 | -8.10 | -53.10 | +47.70 | +39.60 |
| 2026-09-04 | `MMED` | 59 | $23.84 | $23.84 | +0.00 | $23.29 | -32.45 | -32.45 | -2.36 | -34.81 |
| 2026-09-04 | `DE` | 2 | $694.41 | $692.03 | -4.76 | — | +0.00 | -4.76 | -22.44 | — |
| 2026-09-04 | `HPE` | 30 | $54.44 | $53.85 | -17.70 | $52.00 | -55.50 | -73.20 | +187.50 | +132.00 |
| 2026-09-04 | `CRM` | 4 | — | $263.36 | +0.00 | $259.23 | -16.52 | -16.52 | +0.00 | -16.52 |
| 2026-09-04 | `MRX` | 15 | — | $75.65 | +0.00 | $78.27 | +39.30 | +39.30 | +0.00 | +39.30 |
| 2026-09-04 | `BE` | 4 | — | $236.82 | +0.00 | $252.87 | +64.20 | +64.20 | +0.00 | +64.20 |
| 2026-09-04 | `AMX` | 50 | — | $23.03 | +0.00 | $23.00 | -1.50 | -1.50 | +0.00 | -1.50 |
| 2026-09-04 | `BAK` | 603 | — | $1.94 | +0.00 | $1.89 | -30.15 | -30.15 | +0.00 | -30.15 |
| 2026-09-08 | `FRNM` | 90 | $16.31 | $16.74 | +38.70 | — | +0.00 | +38.70 | +78.30 | — |
| 2026-09-08 | `MMED` | 59 | $23.29 | $23.16 | -7.67 | — | +0.00 | -7.67 | -42.48 | — |
| 2026-09-08 | `HPE` | 30 | $52.00 | $52.29 | +8.70 | — | +0.00 | +8.70 | +140.70 | — |
| 2026-09-08 | `CRM` | 4 | $259.23 | $253.72 | -22.04 | — | +0.00 | -22.04 | -38.56 | — |
| 2026-09-08 | `MRX` | 15 | $78.27 | $78.84 | +8.55 | $76.71 | -31.95 | -23.40 | +47.85 | +15.90 |
| 2026-09-08 | `BE` | 4 | $252.87 | $267.76 | +59.56 | — | +0.00 | +59.56 | +123.76 | — |
| 2026-09-08 | `AMX` | 50 | $23.00 | $23.15 | +7.50 | — | +0.00 | +7.50 | +6.00 | — |
| 2026-09-08 | `BAK` | 603 | $1.89 | $1.94 | +30.15 | — | +0.00 | +30.15 | +0.00 | — |
| 2026-09-09 | `MRX` | 15 | $76.71 | $76.60 | -1.65 | — | +0.00 | -1.65 | +14.25 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 31 | — | $164.43 | +0.00 | $150.28 | -438.65 | -438.65 | +0.00 | -438.65 |
| 2026-09-11 | `BTI` | 92 | — | $56.03 | +0.00 | $55.24 | -72.68 | -72.68 | +0.00 | -72.68 |
| 2026-09-14 | `ORCL` | 31 | $150.28 | $141.42 | -274.66 | — | +0.00 | -274.66 | -713.31 | — |
| 2026-09-14 | `BTI` | 92 | $55.24 | $57.12 | +172.96 | — | +0.00 | +172.96 | +100.28 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 74 | — | $26.27 | +0.00 | $26.59 | +23.68 | +23.68 | +0.00 | +23.68 |
| 2026-09-16 | `QCOM` | 10 | — | $189.17 | +0.00 | $184.84 | -43.30 | -43.30 | +0.00 | -43.30 |
| 2026-09-16 | `SM` | 48 | — | $39.99 | +0.00 | $38.16 | -87.84 | -87.84 | +0.00 | -87.84 |
| 2026-09-16 | `AMX` | 84 | — | $23.18 | +0.00 | $22.98 | -16.80 | -16.80 | +0.00 | -16.80 |
| 2026-09-16 | `AVTR` | 125 | — | $15.53 | +0.00 | $15.61 | +10.00 | +10.00 | +0.00 | +10.00 |
| 2026-09-17 | `WAY` | 74 | $26.59 | $26.51 | -5.92 | — | +0.00 | -5.92 | +17.76 | — |
| 2026-09-17 | `QCOM` | 10 | $184.84 | $190.35 | +55.10 | — | +0.00 | +55.10 | +11.80 | — |
| 2026-09-17 | `SM` | 48 | $38.16 | $37.57 | -28.32 | — | +0.00 | -28.32 | -116.16 | — |
| 2026-09-17 | `AMX` | 84 | $22.98 | $23.09 | +9.24 | — | +0.00 | +9.24 | -7.56 | — |
| 2026-09-17 | `AVTR` | 125 | $15.61 | $15.81 | +25.00 | $15.86 | +6.25 | +31.25 | +35.00 | +41.25 |
| 2026-09-17 | `SMTC` | 8 | — | $170.85 | +0.00 | $178.19 | +58.72 | +58.72 | +0.00 | +58.72 |
| 2026-09-17 | `GME` | 69 | — | $22.12 | +0.00 | $22.77 | +44.85 | +44.85 | +0.00 | +44.85 |
| 2026-09-17 | `JBHT` | 6 | — | $238.60 | +0.00 | $236.80 | -10.80 | -10.80 | +0.00 | -10.80 |
| 2026-09-17 | `SRRK` | 31 | — | $49.52 | +0.00 | $49.02 | -15.50 | -15.50 | +0.00 | -15.50 |
| 2026-09-17 | `LITE` | 1 | — | $934.88 | +0.00 | $893.61 | -41.27 | -41.27 | +0.00 | -41.27 |
| 2026-09-18 | `AVTR` | 125 | $15.86 | $15.87 | +1.25 | — | +0.00 | +1.25 | +42.50 | — |
| 2026-09-18 | `SMTC` | 8 | $178.19 | $182.33 | +33.12 | — | +0.00 | +33.12 | +91.84 | — |
| 2026-09-18 | `GME` | 69 | $22.77 | $22.90 | +8.97 | $22.64 | -17.94 | -8.97 | +53.82 | +35.88 |
| 2026-09-18 | `JBHT` | 6 | $236.80 | $236.80 | +0.00 | — | +0.00 | +0.00 | -10.80 | — |
| 2026-09-18 | `SRRK` | 31 | $49.02 | $48.02 | -31.00 | — | +0.00 | -31.00 | -46.50 | — |
| 2026-09-18 | `LITE` | 1 | $893.61 | $915.66 | +22.05 | — | +0.00 | +22.05 | -19.22 | — |
| 2026-09-18 | `TH` | 129 | — | $20.91 | +0.00 | $21.19 | +36.12 | +36.12 | +0.00 | +36.12 |
| 2026-09-18 | `RARE` | 183 | — | $14.79 | +0.00 | $14.51 | -51.24 | -51.24 | +0.00 | -51.24 |
| 2026-09-18 | `BHVN` | 192 | — | $14.07 | +0.00 | $13.62 | -86.40 | -86.40 | +0.00 | -86.40 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +137.19 | HLIT, ANGX, ARX, MH, VELO, NRG, S | — | $1,332.73 | $10,120.33 | HLIT×94, ANGX×290, ARX×63, MH×92, VELO×81, NRG×10, S×52 |
| 2026-08-17 | +2.25 | $1,332.73 | HLIT×94, ANGX×290, ARX×63, MH×92, VELO×81, NRG×10, S×52 | $10,155.37 | +35.04 | +16.16 | DVN, EOG, FANG, GLOB, TPG, OUST | HLIT, ANGX, ARX, MH, VELO, NRG, S | $246.79 | $10,142.04 | DVN×36, EOG×11, FANG×8, GLOB×45, TPG×32, OUST×34 |
| 2026-08-18 | -6.20 | $246.79 | DVN×36, EOG×11, FANG×8, GLOB×45, TPG×32, OUST×34 | $10,128.47 | -13.57 | +0.00 | — | DVN, EOG, FANG, GLOB, TPG, OUST | $10,115.90 | $10,115.90 | — |
| 2026-08-19 | -7.20 | $10,115.90 | — | $10,115.90 | -0.00 | +0.00 | — | — | $10,115.90 | $10,115.90 | — |
| 2026-08-20 | +1.12 | $10,115.90 | — | $10,115.90 | -0.00 | -158.68 | BHP, APA, AUTL, CRSP, MRK, ASST, MRNA, ZLAB | — | $242.70 | $9,936.08 | BHP×13, APA×28, AUTL×511, CRSP×21, MRK×8, ASST×79, MRNA×8, ZLAB×47 |
| 2026-08-21 | +3.25 | $242.70 | BHP×13, APA×28, AUTL×511, CRSP×21, MRK×8, ASST×79, MRNA×8, ZLAB×47 | $10,136.64 | +200.56 | +125.93 | AU, FUTU, GRAL, VIRT, MFC, ABTC | BHP, APA, MRK, ASST, MRNA, ZLAB | $162.26 | $10,237.32 | AUTL×511, CRSP×21, AU×10, FUTU×11, GRAL×16, VIRT×20, MFC×29, ABTC×146 |
| 2026-08-24 | -5.17 | $162.26 | AUTL×511, CRSP×21, AU×10, FUTU×11, GRAL×16, VIRT×20, MFC×29, ABTC×146 | $10,199.42 | -37.90 | -35.17 | — | AUTL, AU, FUTU, GRAL, VIRT, MFC, ABTC | $8,946.21 | $10,144.79 | CRSP×21 |
| 2026-08-25 | +1.80 | $8,946.21 | CRSP×21 | $10,162.74 | +17.95 | +198.89 | AU, FCX, EZPW, AMX | CRSP | $148.54 | $10,350.91 | AU×21, FCX×32, EZPW×72, AMX×106 |
| 2026-08-26 | +2.02 | $148.54 | AU×21, FCX×32, EZPW×72, AMX×106 | $10,291.12 | -59.79 | +136.98 | FNV, ASST | AU, FCX, EZPW | $149.68 | $10,417.08 | AMX×106, FNV×14, ASST×187 |
| 2026-08-27 | — | $149.68 | AMX×106, FNV×14, ASST×187 | $10,608.67 | +191.59 | -100.37 | ACMR, MU, CM, GEN, LRCX, NVDA, AXTI | AMX, FNV, ASST | $2,039.79 | $10,487.04 | ACMR×16, MU×1, CM×11, GEN×44, LRCX×4, NVDA×5, AXTI×18 |
| 2026-08-28 | +0.75 | $2,039.79 | ACMR×16, MU×1, CM×11, GEN×44, LRCX×4, NVDA×5, AXTI×18 | $10,425.80 | -61.24 | -353.36 | KEYS, SMTC, CIEN, DDOG, PLAB, ADSK, SEDG | ACMR, MU, CM, GEN, LRCX, NVDA, AXTI | $1,803.17 | $10,043.82 | KEYS×4, SMTC×9, CIEN×3, DDOG×5, PLAB×43, ADSK×4, SEDG×39 |
| 2026-08-31 | -5.85 | $1,803.17 | KEYS×4, SMTC×9, CIEN×3, DDOG×5, PLAB×43, ADSK×4, SEDG×39 | $10,040.38 | -3.44 | +0.00 | — | KEYS, SMTC, CIEN, DDOG, PLAB, ADSK, SEDG | $10,025.99 | $10,025.99 | — |
| 2026-09-01 | -6.30 | $10,025.99 | — | $10,025.99 | +0.00 | +0.00 | — | — | $10,025.99 | $10,025.99 | — |
| 2026-09-02 | -3.83 | $10,025.99 | — | $10,025.99 | +0.00 | +0.00 | — | — | $10,025.99 | $10,025.99 | — |
| 2026-09-03 | -0.90 | $10,025.99 | — | $10,025.99 | +0.00 | +419.10 | AVGO, DELL, CXW, FRNM, MMED, DE, HPE | — | $538.43 | $10,430.47 | AVGO×4, DELL×2, CXW×44, FRNM×90, MMED×59, DE×2, HPE×30 |
| 2026-09-04 | +2.25 | $538.43 | AVGO×4, DELL×2, CXW×44, FRNM×90, MMED×59, DE×2, HPE×30 | $10,359.15 | -71.32 | -40.72 | CRM, MRX, BE, AMX, BAK | AVGO, DELL, CXW, DE | $380.14 | $10,294.27 | FRNM×90, MMED×59, HPE×30, CRM×4, MRX×15, BE×4, AMX×50, BAK×603 |
| 2026-09-08 | -11.47 | $380.14 | FRNM×90, MMED×59, HPE×30, CRM×4, MRX×15, BE×4, AMX×50, BAK×603 | $10,417.72 | +123.45 | -31.95 | — | FRNM, MMED, HPE, CRM, BE, AMX, BAK | $9,214.45 | $10,365.10 | MRX×15 |
| 2026-09-09 | -13.95 | $9,214.45 | MRX×15 | $10,363.45 | -1.65 | +0.00 | — | MRX | $10,361.40 | $10,361.40 | — |
| 2026-09-10 | -13.28 | $10,361.40 | — | $10,361.40 | -0.00 | +0.00 | — | — | $10,361.40 | $10,361.40 | — |
| 2026-09-11 | +0.50 | $10,361.40 | — | $10,361.40 | -0.00 | -511.33 | ORCL, BTI | — | $104.96 | $9,845.72 | ORCL×31, BTI×92 |
| 2026-09-14 | -11.00 | $104.96 | ORCL×31, BTI×92 | $9,744.02 | -101.70 | +0.00 | — | ORCL, BTI | $9,739.57 | $9,739.57 | — |
| 2026-09-15 | -3.84 | $9,739.57 | — | $9,739.57 | -0.00 | +0.00 | — | — | $9,739.57 | $9,739.57 | — |
| 2026-09-16 | +5.30 | $9,739.57 | — | $9,739.57 | -0.00 | -114.26 | WAY, QCOM, SM, AMX, AVTR | — | $85.02 | $9,614.33 | WAY×74, QCOM×10, SM×48, AMX×84, AVTR×125 |
| 2026-09-17 | +7.38 | $85.02 | WAY×74, QCOM×10, SM×48, AMX×84, AVTR×125 | $9,669.43 | +55.10 | +42.25 | SMTC, GME, JBHT, SRRK, LITE | WAY, QCOM, SM, AMX | $879.49 | $9,692.67 | AVTR×125, SMTC×8, GME×69, JBHT×6, SRRK×31, LITE×1 |
| 2026-09-18 | +4.86 | $879.49 | AVTR×125, SMTC×8, GME×69, JBHT×6, SRRK×31, LITE×1 | $9,727.06 | +34.39 | -119.46 | TH, RARE, BHVN | AVTR, SMTC, JBHT, SRRK, LITE | $23.50 | $9,589.54 | GME×69, TH×129, RARE×183, BHVN×192 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $8,758.81 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $7,505.17 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $6,270.08 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $5,021.21 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 81 | $15.38 | $2.23 | — | $3,773.20 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $2,571.18 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `S` | 52 | $23.77 | $2.15 | — | $1,332.73 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,332.73 | ▲ close $10,120.33 vs 09:30 $10,000.00 (session +137.19) | 16:00 close · cash $1,332.73 · equity $10,120.33 vs 09:30 $10,000.00 (+120.33; session marks +137.19) · 7 name(s) marked open→close (per-name table). HLIT×94 09:30 $13.18 → close $13.92 +69.56; ANGX×290 09:30 $4.31 → close $4.37 +17.40; ARX×63 09:30 $19.57 → close $19.58 +0.63; MH×92 09:30 $13.55 → close $13.10 -41.40; VELO×81 09:30 $15.38 → close $16.16 +63.18; NRG×10 09:30 $120.00 → close $126.24 +62.40; S×52 09:30 $23.77 → close $23.11 -34.58 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,332.73 | ▲ 09:30 equity $10,155.37 vs yday $10,120.33 (+35.04) | 09:30 open · cash $1,332.73 (unchanged overnight, no fees) · equity $10,155.37 vs prior close $10,120.33 (+35.04) · 7 name(s) re-marked at the open (per-name table). HLIT×94 yday $13.92 → 09:30 $13.84 -7.52; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; MH×92 yday $13.10 → 09:30 $13.16 +5.52; VELO×81 yday $16.16 → 09:30 $16.05 -8.91; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; S×52 yday $23.11 → 09:30 $22.50 -31.72 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $2,631.40 | ▲ +57.47 after sell → book $10,153.08; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,961.60 | ▲ +76.56 after sell → book $10,149.28; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $5,192.31 | ▼ -4.38 after sell → book $10,147.08; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $6,400.73 | ▼ -40.44 after sell → book $10,144.78; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VELO` | 81 | $16.05 | $2.26 | $+49.78 | $7,698.53 | ▲ +49.78 after sell → book $10,142.53; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $8,970.49 | ▲ +69.94 after sell → book $10,140.49; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `S` | 52 | $22.50 | $2.17 | $-70.61 | $10,138.32 | ▼ -70.61 after sell → book $10,138.32; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 36 | $46.18 | $2.10 | — | $8,473.74 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+6.7; leftover $1689.72 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 11 | $142.77 | $2.02 | — | $6,901.25 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+5.8; leftover $1689.72 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 8 | $202.70 | $2.01 | — | $5,277.64 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+8.3; leftover $1689.72 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `GLOB` | 45 | $37.18 | $2.12 | — | $3,602.41 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list oppset; ⚪; ret5=-0.1; leftover $1689.72 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TPG` | 32 | $52.67 | $2.09 | — | $1,914.89 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list oppset; 🔵; ⚪; ret5=+9.4; leftover $1689.72 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 34 | $49.00 | $2.09 | — | $246.79 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $1689.72 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $246.79 | ▲ close $10,142.04 vs 09:30 $10,155.37 (session +16.16) | 16:00 close · cash $246.79 · equity $10,142.04 vs 09:30 $10,155.37 (-13.33; session marks +16.16) · 6 name(s) marked open→close (per-name table). DVN×36 09:30 $46.18 → close $47.57 +50.04; EOG×11 09:30 $142.77 → close $146.15 +37.18; FANG×8 09:30 $202.70 → close $206.29 +28.72; GLOB×45 09:30 $37.18 → close $36.26 -41.40; TPG×32 09:30 $52.67 → close $51.77 -28.80; OUST×34 09:30 $49.00 → close $48.13 -29.58 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $246.79 | ▼ 09:30 equity $10,128.47 vs yday $10,142.04 (-13.57) | 09:30 open · cash $246.79 (unchanged overnight, no fees) · equity $10,128.47 vs prior close $10,142.04 (-13.57) · 6 name(s) re-marked at the open (per-name table). DVN×36 yday $47.57 → 09:30 $48.00 +15.48; EOG×11 yday $146.15 → 09:30 $148.04 +20.79; FANG×8 yday $206.29 → 09:30 $208.93 +21.12; GLOB×45 yday $36.26 → 09:30 $36.98 +32.40; TPG×32 yday $51.77 → 09:30 $51.77 +0.00; OUST×34 yday $48.13 → 09:30 $45.09 -103.36 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 36 | $48.00 | $2.12 | $+61.30 | $1,972.67 | ▲ +61.30 after sell → book $10,126.35; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 11 | $148.04 | $2.05 | $+53.90 | $3,599.07 | ▲ +53.90 after sell → book $10,124.31; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 8 | $208.93 | $2.04 | $+45.79 | $5,268.47 | ▲ +45.79 after sell → book $10,122.27; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `GLOB` | 45 | $36.98 | $2.15 | $-13.27 | $6,930.42 | ▼ -13.27 after sell → book $10,120.12; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 32 | $51.77 | $2.11 | $-33.00 | $8,584.95 | ▼ -33.00 after sell → book $10,118.01; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 34 | $45.09 | $2.11 | $-137.15 | $10,115.90 | ▼ -137.15 after sell → book $10,115.90; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,115.90 | ▲ close $10,115.90 vs 09:30 $10,128.47 (session +0.00) | 16:00 close · cash $10,115.90 · no lots left · equity $10,115.90. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,115.90 | ▲ 09:30 equity $10,115.90 vs yday $10,115.90 (-0.00) | 09:30 open · cash $10,115.90 · no holdings · equity $10,115.90 vs prior close $10,115.90 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,115.90 | ▲ close $10,115.90 vs 09:30 $10,115.90 (session +0.00) | 16:00 close · cash $10,115.90 · no lots left · equity $10,115.90. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,115.90 | ▲ 09:30 equity $10,115.90 vs yday $10,115.90 (-0.00) | 09:30 open · cash $10,115.90 · no holdings · equity $10,115.90 vs prior close $10,115.90 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,930.74 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1264.49 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $7,675.38 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1264.49 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 511 | $2.47 | $6.59 | — | $6,406.62 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1264.49 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $5,171.24 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1264.49 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRK` | 8 | $150.78 | $2.01 | — | $3,962.98 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list oppset; 🔵; ⚪; ret5=+14.5; leftover $1264.49 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 79 | $16.00 | $2.23 | — | $2,696.76 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1264.49 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $1,493.62 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ret5=+173.9; leftover $1264.49 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 47 | $26.57 | $2.13 | — | $242.70 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1264.49 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $242.70 | ▼ close $9,936.08 vs 09:30 $10,115.90 (session -158.68) | 16:00 close · cash $242.70 · equity $9,936.08 vs 09:30 $10,115.90 (-179.82; session marks -158.68) · 8 name(s) marked open→close (per-name table). BHP×13 09:30 $91.01 → close $93.63 +34.06; APA×28 09:30 $44.76 → close $44.39 -10.36; AUTL×511 09:30 $2.47 → close $2.46 -5.11; CRSP×21 09:30 $58.73 → close $58.12 -12.81; MRK×8 09:30 $150.78 → close $148.99 -14.32; ASST×79 09:30 $16.00 → close $16.13 +10.27; MRNA×8 09:30 $150.14 → close $133.32 -134.56; ZLAB×47 09:30 $26.57 → close $26.02 -25.85 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $242.70 | ▲ 09:30 equity $10,136.64 vs yday $9,936.08 (+200.56) | 09:30 open · cash $242.70 (unchanged overnight, no fees) · equity $10,136.64 vs prior close $9,936.08 (+200.56) · 8 name(s) re-marked at the open (per-name table). BHP×13 yday $93.63 → 09:30 $95.72 +27.17; APA×28 yday $44.39 → 09:30 $44.52 +3.64; AUTL×511 yday $2.46 → 09:30 $2.47 +5.11; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; MRK×8 yday $148.99 → 09:30 $149.12 +1.04; ASST×79 yday $16.13 → 09:30 $17.66 +120.87; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; ZLAB×47 yday $26.02 → 09:30 $26.25 +10.81 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,485.01 | ▲ +57.15 after sell → book $10,134.59; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $2,729.48 | ▼ -10.89 after sell → book $10,132.50; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRK` | 8 | $149.12 | $2.03 | $-17.33 | $3,920.41 | ▼ -17.33 after sell → book $10,130.47; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 79 | $17.66 | $2.25 | $+126.66 | $5,313.29 | ▲ +126.66 after sell → book $10,128.21; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $6,376.14 | ▼ -140.29 after sell → book $10,126.18; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 47 | $26.25 | $2.15 | $-19.32 | $7,607.74 | ▼ -19.32 after sell → book $10,124.03; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,411.42 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1267.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $5,142.42 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1267.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 16 | $78.88 | $2.04 | — | $3,878.30 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1267.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `VIRT` | 20 | $60.66 | $2.05 | — | $2,663.05 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list oppset; 🔵; ⚪; ret5=+7.0; leftover $1267.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MFC` | 29 | $42.48 | $2.08 | — | $1,429.05 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list oppset; 🔵; ⚪; ret5=-3.3; leftover $1267.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 146 | $8.66 | $2.43 | — | $162.26 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1267.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $162.26 | ▲ close $10,237.32 vs 09:30 $10,136.64 (session +125.93) | 16:00 close · cash $162.26 · equity $10,237.32 vs 09:30 $10,136.64 (+100.68; session marks +125.93) · 8 name(s) marked open→close (per-name table). AUTL×511 09:30 $2.47 → close $2.41 -30.66; CRSP×21 09:30 $59.72 → close $59.50 -4.62; AU×10 09:30 $119.43 → close $121.22 +17.90; FUTU×11 09:30 $115.18 → close $123.64 +93.06; GRAL×16 09:30 $78.88 → close $79.54 +10.56; VIRT×20 09:30 $60.66 → close $67.93 +145.40; MFC×29 09:30 $42.48 → close $42.51 +0.87; ABTC×146 09:30 $8.66 → close $7.93 -106.58 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $162.26 | ▼ 09:30 equity $10,199.42 vs yday $10,237.32 (-37.90) | 09:30 open · cash $162.26 (unchanged overnight, no fees) · equity $10,199.42 vs prior close $10,237.32 (-37.90) · 8 name(s) re-marked at the open (per-name table). AUTL×511 yday $2.41 → 09:30 $2.40 -5.11; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; AU×10 yday $121.22 → 09:30 $120.51 -7.10; FUTU×11 yday $123.64 → 09:30 $121.00 -29.04; GRAL×16 yday $79.54 → 09:30 $81.87 +37.28; VIRT×20 yday $67.93 → 09:30 $66.80 -22.60; MFC×29 yday $42.51 → 09:30 $42.31 -5.80; ABTC×146 yday $7.93 → 09:30 $8.00 +10.22 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 511 | $2.40 | $6.69 | $-49.05 | $1,381.98 | ▼ -49.05 after sell → book $10,192.74; vs 09:30 mark -6.68 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $2,585.04 | ▲ +6.74 after sell → book $10,190.70; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $121.00 | $2.04 | $+59.95 | $3,913.99 | ▲ +59.95 after sell → book $10,188.65; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 16 | $81.87 | $2.06 | $+43.74 | $5,221.85 | ▲ +43.74 after sell → book $10,186.59; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `VIRT` | 20 | $66.80 | $2.07 | $+118.68 | $6,555.78 | ▲ +118.68 after sell → book $10,184.52; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `MFC` | 29 | $42.31 | $2.10 | $-9.10 | $7,780.68 | ▼ -9.10 after sell → book $10,182.43; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 146 | $8.00 | $2.46 | $-101.25 | $8,946.21 | ▼ -101.25 after sell → book $10,179.96; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,946.21 | ▼ close $10,144.79 vs 09:30 $10,199.42 (session -35.17) | 16:00 close · cash $8,946.21 · equity $10,144.79 vs 09:30 $10,199.42 (-54.63; session marks -35.17) · 1 name(s) marked open→close (per-name table). CRSP×21 09:30 $58.75 → close $57.08 -35.17 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,946.21 | ▲ 09:30 equity $10,162.74 vs yday $10,144.79 (+17.95) | 09:30 open · cash $8,946.21 (unchanged overnight, no fees) · equity $10,162.74 vs prior close $10,144.79 (+17.95) · 1 name(s) re-marked at the open (per-name table). CRSP×21 yday $57.08 → 09:30 $57.93 +17.95 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-20.93 | $10,160.67 | ▼ -20.93 after sell → book $10,160.67; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 21 | $118.52 | $2.05 | — | $7,669.70 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $2540.17 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 32 | $77.13 | $2.09 | — | $5,199.45 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $2540.17 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 72 | $35.05 | $2.21 | — | $2,673.65 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,oppset; 🔵; ⚪; ret5=+19.7; leftover $2540.17 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMX` | 106 | $23.80 | $2.31 | — | $148.54 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list oppset; 🔵; ret5=+0.5; leftover $2540.17 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $148.54 | ▲ close $10,350.91 vs 09:30 $10,162.74 (session +198.89) | 16:00 close · cash $148.54 · equity $10,350.91 vs 09:30 $10,162.74 (+188.17; session marks +198.89) · 4 name(s) marked open→close (per-name table). AU×21 09:30 $118.52 → close $123.39 +102.27; FCX×32 09:30 $77.13 → close $79.91 +88.96; EZPW×72 09:30 $35.05 → close $35.23 +12.96; AMX×106 09:30 $23.80 → close $23.75 -5.30 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $148.54 | ▼ 09:30 equity $10,291.12 vs yday $10,350.91 (-59.79) | 09:30 open · cash $148.54 (unchanged overnight, no fees) · equity $10,291.12 vs prior close $10,350.91 (-59.79) · 4 name(s) re-marked at the open (per-name table). AU×21 yday $123.39 → 09:30 $119.80 -75.39; FCX×32 yday $79.91 → 09:30 $79.34 -18.24; EZPW×72 yday $35.23 → 09:30 $35.70 +33.84; AMX×106 yday $23.75 → 09:30 $23.75 +0.00 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 21 | $119.80 | $2.08 | $+22.74 | $2,662.26 | ▲ +22.74 after sell → book $10,289.04; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 32 | $79.34 | $2.12 | $+66.52 | $5,199.02 | ▲ +66.52 after sell → book $10,286.92; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 72 | $35.70 | $2.24 | $+42.36 | $7,767.18 | ▲ +42.36 after sell → book $10,284.68; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 14 | $267.02 | $2.03 | — | $4,026.87 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $3883.59 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ASST` | 187 | $20.72 | $2.55 | — | $149.68 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list oppset; 🔵; ret5=+67.1; leftover $3883.59 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $149.68 | ▲ close $10,417.08 vs 09:30 $10,291.12 (session +136.98) | 16:00 close · cash $149.68 · equity $10,417.08 vs 09:30 $10,291.12 (+125.96; session marks +136.98) · 3 name(s) marked open→close (per-name table). AMX×106 09:30 $23.75 → close $23.62 -13.78; FNV×14 09:30 $267.02 → close $267.37 +4.90; ASST×187 09:30 $20.72 → close $21.50 +145.86 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.68 | ▲ 09:30 equity $10,608.67 vs yday $10,417.08 (+191.59) | 09:30 open · cash $149.68 (unchanged overnight, no fees) · equity $10,608.67 vs prior close $10,417.08 (+191.59) · 3 name(s) re-marked at the open (per-name table). AMX×106 yday $23.62 → 09:30 $23.77 +15.90; FNV×14 yday $267.37 → 09:30 $267.23 -1.96; ASST×187 yday $21.50 → 09:30 $22.45 +177.65 | — |
| 2026-08-27 09:30 ET | **SELL** | `AMX` | 106 | $23.77 | $2.35 | $-7.83 | $2,666.95 | ▼ -7.83 after sell → book $10,606.32; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 14 | $267.23 | $2.07 | $-1.16 | $6,406.10 | ▼ -1.16 after sell → book $10,604.25; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 187 | $22.45 | $2.62 | $+318.34 | $10,601.63 | ▲ +318.34 after sell → book $10,601.63; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 16 | $81.65 | $2.04 | — | $9,293.20 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1325.20 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $8,324.19 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1325.20 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 11 | $118.77 | $2.02 | — | $7,015.70 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list earn_react; ret5=+0.3; leftover $1325.20 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 44 | $29.83 | $2.12 | — | $5,701.06 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+7.6; leftover $1325.20 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 4 | $318.88 | $2.00 | — | $4,423.54 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1325.20 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 5 | $222.86 | $2.00 | — | $3,307.23 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $1325.20 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 18 | $70.30 | $2.04 | — | $2,039.79 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ret5=-11.2; leftover $1325.20 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,039.79 | ▼ close $10,487.04 vs 09:30 $10,608.67 (session -100.37) | 16:00 close · cash $2,039.79 · equity $10,487.04 vs 09:30 $10,608.67 (-121.63; session marks -100.37) · 7 name(s) marked open→close (per-name table). ACMR×16 09:30 $81.65 → close $80.49 -18.56; MU×1 09:30 $967.01 → close $935.39 -31.62; CM×11 09:30 $118.77 → close $114.84 -43.23; GEN×44 09:30 $29.83 → close $30.50 +29.48; LRCX×4 09:30 $318.88 → close $318.58 -1.20; NVDA×5 09:30 $222.86 → close $227.98 +25.60; AXTI×18 09:30 $70.30 → close $66.92 -60.84 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,039.79 | ▼ 09:30 equity $10,425.80 vs yday $10,487.04 (-61.24) | 09:30 open · cash $2,039.79 (unchanged overnight, no fees) · equity $10,425.80 vs prior close $10,487.04 (-61.24) · 7 name(s) re-marked at the open (per-name table). ACMR×16 yday $80.49 → 09:30 $79.27 -19.52; MU×1 yday $935.39 → 09:30 $919.29 -16.10; CM×11 yday $114.84 → 09:30 $115.66 +9.02; GEN×44 yday $30.50 → 09:30 $30.50 +0.00; LRCX×4 yday $318.58 → 09:30 $318.03 -2.20; NVDA×5 yday $227.98 → 09:30 $227.36 -3.10; AXTI×18 yday $66.92 → 09:30 $65.29 -29.34 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 16 | $79.27 | $2.06 | $-42.18 | $3,306.05 | ▼ -42.18 after sell → book $10,423.74; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $4,223.33 | ▼ -51.73 after sell → book $10,421.73; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 11 | $115.66 | $2.04 | $-38.28 | $5,493.54 | ▼ -38.28 after sell → book $10,419.68; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 44 | $30.50 | $2.14 | $+25.22 | $6,833.40 | ▲ +25.22 after sell → book $10,417.54; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 4 | $318.03 | $2.02 | $-7.42 | $8,103.50 | ▼ -7.42 after sell → book $10,415.52; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 5 | $227.36 | $2.02 | $+18.47 | $9,238.27 | ▲ +18.47 after sell → book $10,413.49; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 18 | $65.29 | $2.06 | $-94.29 | $10,411.43 | ▼ -94.29 after sell → book $10,411.43; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $9,111.79 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1301.43 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $7,833.93 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $1301.43 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,630.67 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1301.43 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $5,427.57 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1301.43 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PLAB` | 43 | $30.01 | $2.12 | — | $4,135.02 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list oppset; 🔵; ret5=-0.9; leftover $1301.43 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $3,088.38 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list earn_react; ret5=+7.8; leftover $1301.43 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $32.90 | $2.11 | — | $1,803.17 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1301.43 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,803.17 | ▼ close $10,043.82 vs 09:30 $10,425.80 (session -353.36) | 16:00 close · cash $1,803.17 · equity $10,043.82 vs 09:30 $10,425.80 (-381.98; session marks -353.36) · 7 name(s) marked open→close (per-name table). KEYS×4 09:30 $324.41 → close $319.97 -17.76; SMTC×9 09:30 $141.76 → close $131.17 -95.31; CIEN×3 09:30 $400.42 → close $378.44 -65.94; DDOG×5 09:30 $240.22 → close $236.98 -16.20; PLAB×43 09:30 $30.01 → close $27.73 -98.04; ADSK×4 09:30 $261.16 → close $260.66 -2.00; SEDG×39 09:30 $32.90 → close $31.41 -58.11 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,803.17 | ▼ 09:30 equity $10,040.38 vs yday $10,043.82 (-3.44) | 09:30 open · cash $1,803.17 (unchanged overnight, no fees) · equity $10,040.38 vs prior close $10,043.82 (-3.44) · 7 name(s) re-marked at the open (per-name table). KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; DDOG×5 yday $236.98 → 09:30 $233.97 -15.07; PLAB×43 yday $27.73 → 09:30 $28.04 +13.33; ADSK×4 yday $260.66 → 09:30 $257.71 -11.80; SEDG×39 yday $31.41 → 09:30 $31.15 -10.14 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $3,091.11 | ▼ -11.70 after sell → book $10,038.36; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $4,279.77 | ▼ -89.19 after sell → book $10,036.32; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $5,413.07 | ▼ -69.96 after sell → book $10,034.30; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 5 | $233.97 | $2.02 | $-35.31 | $6,580.87 | ▼ -35.31 after sell → book $10,032.28; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PLAB` | 43 | $28.04 | $2.14 | $-88.97 | $7,784.45 | ▼ -88.97 after sell → book $10,030.14; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $257.71 | $2.02 | $-17.82 | $8,813.27 | ▼ -17.82 after sell → book $10,028.12; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 39 | $31.15 | $2.13 | $-72.48 | $10,025.99 | ▼ -72.48 after sell → book $10,025.99; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,025.99 | ▲ close $10,025.99 vs 09:30 $10,040.38 (session +0.00) | 16:00 close · cash $10,025.99 · no lots left · equity $10,025.99. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,025.99 | ▲ 09:30 equity $10,025.99 vs yday $10,025.99 (+0.00) | 09:30 open · cash $10,025.99 · no holdings · equity $10,025.99 vs prior close $10,025.99 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,025.99 | ▲ close $10,025.99 vs 09:30 $10,025.99 (session +0.00) | 16:00 close · cash $10,025.99 · no lots left · equity $10,025.99. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,025.99 | ▲ 09:30 equity $10,025.99 vs yday $10,025.99 (+0.00) | 09:30 open · cash $10,025.99 · no holdings · equity $10,025.99 vs prior close $10,025.99 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,025.99 | ▲ close $10,025.99 vs 09:30 $10,025.99 (session +0.00) | 16:00 close · cash $10,025.99 · no lots left · equity $10,025.99. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,025.99 | ▲ 09:30 equity $10,025.99 vs yday $10,025.99 (+0.00) | 09:30 open · cash $10,025.99 · no holdings · equity $10,025.99 vs prior close $10,025.99 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 4 | $351.74 | $2.00 | — | $8,617.03 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1432.28 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,642.41 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $1432.28 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 44 | $32.31 | $2.12 | — | $6,218.65 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1432.28 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 90 | $15.87 | $2.26 | — | $4,788.09 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1432.28 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 59 | $23.88 | $2.17 | — | $3,377.00 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+21.9; leftover $1432.28 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $1,968.51 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1432.28 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 30 | $47.60 | $2.08 | — | $538.43 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list earn_react,oppset; 🔵; ret5=-6.2; leftover $1432.28 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $538.43 | ▲ close $10,430.47 vs 09:30 $10,025.99 (session +419.10) | 16:00 close · cash $538.43 · equity $10,430.47 vs 09:30 $10,025.99 (+404.48; session marks +419.10) · 7 name(s) marked open→close (per-name table). AVGO×4 09:30 $351.74 → close $357.16 +21.68; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×44 09:30 $32.31 → close $33.66 +59.40; FRNM×90 09:30 $15.87 → close $16.90 +92.70; MMED×59 09:30 $23.88 → close $23.84 -2.36; DE×2 09:30 $703.25 → close $694.41 -17.68; HPE×30 09:30 $47.60 → close $54.44 +205.20 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $538.43 | ▼ 09:30 equity $10,359.15 vs yday $10,430.47 (-71.32) | 09:30 open · cash $538.43 (unchanged overnight, no fees) · equity $10,359.15 vs prior close $10,430.47 (-71.32) · 7 name(s) re-marked at the open (per-name table). AVGO×4 yday $357.16 → 09:30 $359.70 +10.16; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×44 yday $33.66 → 09:30 $33.46 -8.80; FRNM×90 yday $16.90 → 09:30 $16.40 -45.00; MMED×59 yday $23.84 → 09:30 $23.84 +0.00; DE×2 yday $694.41 → 09:30 $692.03 -4.76; HPE×30 yday $54.44 → 09:30 $53.85 -17.70 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 4 | $359.70 | $2.02 | $+27.81 | $1,975.21 | ▲ +27.81 after sell → book $10,357.13; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $3,000.75 | ▲ +50.93 after sell → book $10,355.11; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 44 | $33.46 | $2.14 | $+46.33 | $4,470.85 | ▲ +46.33 after sell → book $10,352.97; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 2 | $692.03 | $2.02 | $-26.45 | $5,852.89 | ▼ -26.45 after sell → book $10,350.95; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $4,797.45 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1170.58 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 15 | $75.65 | $2.04 | — | $3,660.66 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1170.58 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 4 | $236.82 | $2.00 | — | $2,711.38 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; ret5=+8.1; leftover $1170.58 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `AMX` | 50 | $23.03 | $2.14 | — | $1,557.74 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list oppset; 🔵; ret5=-1.4; leftover $1170.58 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 603 | $1.94 | $7.78 | — | $380.14 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $1170.58 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $380.14 | ▼ close $10,294.27 vs 09:30 $10,359.15 (session -40.72) | 16:00 close · cash $380.14 · equity $10,294.27 vs 09:30 $10,359.15 (-64.88; session marks -40.72) · 8 name(s) marked open→close (per-name table). FRNM×90 09:30 $16.40 → close $16.31 -8.10; MMED×59 09:30 $23.84 → close $23.29 -32.45; HPE×30 09:30 $53.85 → close $52.00 -55.50; CRM×4 09:30 $263.36 → close $259.23 -16.52; MRX×15 09:30 $75.65 → close $78.27 +39.30; BE×4 09:30 $236.82 → close $252.87 +64.20; AMX×50 09:30 $23.03 → close $23.00 -1.50; BAK×603 09:30 $1.94 → close $1.89 -30.15 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $380.14 | ▲ 09:30 equity $10,417.72 vs yday $10,294.27 (+123.45) | 09:30 open · cash $380.14 (unchanged overnight, no fees) · equity $10,417.72 vs prior close $10,294.27 (+123.45) · 8 name(s) re-marked at the open (per-name table). FRNM×90 yday $16.31 → 09:30 $16.74 +38.70; MMED×59 yday $23.29 → 09:30 $23.16 -7.67; HPE×30 yday $52.00 → 09:30 $52.29 +8.70; CRM×4 yday $259.23 → 09:30 $253.72 -22.04; MRX×15 yday $78.27 → 09:30 $78.84 +8.55; BE×4 yday $252.87 → 09:30 $267.76 +59.56; AMX×50 yday $23.00 → 09:30 $23.15 +7.50; BAK×603 yday $1.89 → 09:30 $1.94 +30.15 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 90 | $16.74 | $2.29 | $+73.75 | $1,884.45 | ▲ +73.75 after sell → book $10,415.43; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MMED` | 59 | $23.16 | $2.19 | $-46.83 | $3,248.71 | ▼ -46.83 after sell → book $10,413.25; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HPE` | 30 | $52.29 | $2.10 | $+136.52 | $4,815.30 | ▲ +136.52 after sell → book $10,411.14; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $5,828.16 | ▼ -42.58 after sell → book $10,409.12; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 4 | $267.76 | $2.02 | $+119.74 | $6,897.18 | ▲ +119.74 after sell → book $10,407.10; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AMX` | 50 | $23.15 | $2.16 | $+1.70 | $8,052.52 | ▲ +1.70 after sell → book $10,404.94; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 603 | $1.94 | $7.89 | $-15.67 | $9,214.45 | ▼ -15.67 after sell → book $10,397.05; vs 09:30 mark -7.89 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,214.45 | ▼ close $10,365.10 vs 09:30 $10,417.72 (session -31.95) | 16:00 close · cash $9,214.45 · equity $10,365.10 vs 09:30 $10,417.72 (-52.62; session marks -31.95) · 1 name(s) marked open→close (per-name table). MRX×15 09:30 $78.84 → close $76.71 -31.95 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,214.45 | ▼ 09:30 equity $10,363.45 vs yday $10,365.10 (-1.65) | 09:30 open · cash $9,214.45 (unchanged overnight, no fees) · equity $10,363.45 vs prior close $10,365.10 (-1.65) · 1 name(s) re-marked at the open (per-name table). MRX×15 yday $76.71 → 09:30 $76.60 -1.65 | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 15 | $76.60 | $2.06 | $+10.16 | $10,361.40 | ▲ +10.16 after sell → book $10,361.40; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,361.40 | ▲ close $10,361.40 vs 09:30 $10,363.45 (session +0.00) | 16:00 close · cash $10,361.40 · no lots left · equity $10,361.40. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,361.40 | ▲ 09:30 equity $10,361.40 vs yday $10,361.40 (-0.00) | 09:30 open · cash $10,361.40 · no holdings · equity $10,361.40 vs prior close $10,361.40 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,361.40 | ▲ close $10,361.40 vs 09:30 $10,361.40 (session +0.00) | 16:00 close · cash $10,361.40 · no lots left · equity $10,361.40. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,361.40 | ▲ 09:30 equity $10,361.40 vs yday $10,361.40 (-0.00) | 09:30 open · cash $10,361.40 · no holdings · equity $10,361.40 vs prior close $10,361.40 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 31 | $164.43 | $2.08 | — | $5,261.98 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $5180.70 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BTI` | 92 | $56.03 | $2.27 | — | $104.96 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list oppset; 🔵; ret5=-0.8; leftover $5180.70 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $104.96 | ▼ close $9,845.72 vs 09:30 $10,361.40 (session -511.33) | 16:00 close · cash $104.96 · equity $9,845.72 vs 09:30 $10,361.40 (-515.68; session marks -511.33) · 2 name(s) marked open→close (per-name table). ORCL×31 09:30 $164.43 → close $150.28 -438.65; BTI×92 09:30 $56.03 → close $55.24 -72.68 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.96 | ▼ 09:30 equity $9,744.02 vs yday $9,845.72 (-101.70) | 09:30 open · cash $104.96 (unchanged overnight, no fees) · equity $9,744.02 vs prior close $9,845.72 (-101.70) · 2 name(s) re-marked at the open (per-name table). ORCL×31 yday $150.28 → 09:30 $141.42 -274.66; BTI×92 yday $55.24 → 09:30 $57.12 +172.96 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 31 | $141.42 | $2.13 | $-717.52 | $4,486.85 | ▼ -717.52 after sell → book $9,741.89; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-14 09:30 ET | **SELL** | `BTI` | 92 | $57.12 | $2.32 | $+95.69 | $9,739.57 | ▲ +95.69 after sell → book $9,739.57; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,739.57 | ▲ close $9,739.57 vs 09:30 $9,744.02 (session +0.00) | 16:00 close · cash $9,739.57 · no lots left · equity $9,739.57. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,739.57 | ▲ 09:30 equity $9,739.57 vs yday $9,739.57 (-0.00) | 09:30 open · cash $9,739.57 · no holdings · equity $9,739.57 vs prior close $9,739.57 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,739.57 | ▲ close $9,739.57 vs 09:30 $9,739.57 (session +0.00) | 16:00 close · cash $9,739.57 · no lots left · equity $9,739.57. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,739.57 | ▲ 09:30 equity $9,739.57 vs yday $9,739.57 (-0.00) | 09:30 open · cash $9,739.57 · no holdings · equity $9,739.57 vs prior close $9,739.57 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 74 | $26.27 | $2.21 | — | $7,793.37 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $1947.91 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 10 | $189.17 | $2.02 | — | $5,899.65 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $1947.91 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 48 | $39.99 | $2.13 | — | $3,978.00 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $1947.91 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `AMX` | 84 | $23.18 | $2.24 | — | $2,028.64 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list oppset; 🔵; ret5=-0.2; leftover $1947.91 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `AVTR` | 125 | $15.53 | $2.37 | — | $85.02 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; ret5=+4.9; leftover $1947.91 | join🟡 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟡 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.02 | ▼ close $9,614.33 vs 09:30 $9,739.57 (session -114.26) | 16:00 close · cash $85.02 · equity $9,614.33 vs 09:30 $9,739.57 (-125.24; session marks -114.26) · 5 name(s) marked open→close (per-name table). WAY×74 09:30 $26.27 → close $26.59 +23.68; QCOM×10 09:30 $189.17 → close $184.84 -43.30; SM×48 09:30 $39.99 → close $38.16 -87.84; AMX×84 09:30 $23.18 → close $22.98 -16.80; AVTR×125 09:30 $15.53 → close $15.61 +10.00 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.02 | ▲ 09:30 equity $9,669.43 vs yday $9,614.33 (+55.10) | 09:30 open · cash $85.02 (unchanged overnight, no fees) · equity $9,669.43 vs prior close $9,614.33 (+55.10) · 5 name(s) re-marked at the open (per-name table). WAY×74 yday $26.59 → 09:30 $26.51 -5.92; QCOM×10 yday $184.84 → 09:30 $190.35 +55.10; SM×48 yday $38.16 → 09:30 $37.57 -28.32; AMX×84 yday $22.98 → 09:30 $23.09 +9.24; AVTR×125 yday $15.61 → 09:30 $15.81 +25.00 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 74 | $26.51 | $2.24 | $+13.31 | $2,044.52 | ▲ +13.31 after sell → book $9,667.19; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 10 | $190.35 | $2.05 | $+7.73 | $3,945.98 | ▲ +7.73 after sell → book $9,665.15; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 48 | $37.57 | $2.16 | $-120.45 | $5,747.18 | ▼ -120.45 after sell → book $9,662.99; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AMX` | 84 | $23.09 | $2.27 | $-12.07 | $7,684.47 | ▼ -12.07 after sell → book $9,660.72; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 8 | $170.85 | $2.01 | — | $6,315.65 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1536.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 69 | $22.12 | $2.20 | — | $4,787.18 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1536.89 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 6 | $238.60 | $2.01 | — | $3,353.57 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_mover,oppset; ret5=-11.6; leftover $1536.89 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SRRK` | 31 | $49.52 | $2.08 | — | $1,816.36 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list oppset; 🔵; ret5=+1.1; leftover $1536.89 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 1 | $934.88 | $1.99 | — | $879.49 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; ret5=-7.0; leftover $1536.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $879.49 | ▲ close $9,692.67 vs 09:30 $9,669.43 (session +42.25) | 16:00 close · cash $879.49 · equity $9,692.67 vs 09:30 $9,669.43 (+23.24; session marks +42.25) · 6 name(s) marked open→close (per-name table). AVTR×125 09:30 $15.81 → close $15.86 +6.25; SMTC×8 09:30 $170.85 → close $178.19 +58.72; GME×69 09:30 $22.12 → close $22.77 +44.85; JBHT×6 09:30 $238.60 → close $236.80 -10.80; SRRK×31 09:30 $49.52 → close $49.02 -15.50; LITE×1 09:30 $934.88 → close $893.61 -41.27 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $879.49 | ▲ 09:30 equity $9,727.06 vs yday $9,692.67 (+34.39) | 09:30 open · cash $879.49 (unchanged overnight, no fees) · equity $9,727.06 vs prior close $9,692.67 (+34.39) · 6 name(s) re-marked at the open (per-name table). AVTR×125 yday $15.86 → 09:30 $15.87 +1.25; SMTC×8 yday $178.19 → 09:30 $182.33 +33.12; GME×69 yday $22.77 → 09:30 $22.90 +8.97; JBHT×6 yday $236.80 → 09:30 $236.80 +0.00; SRRK×31 yday $49.02 → 09:30 $48.02 -31.00; LITE×1 yday $893.61 → 09:30 $915.66 +22.05 | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 125 | $15.87 | $2.40 | $+37.73 | $2,860.84 | ▲ +37.73 after sell → book $9,724.66; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 8 | $182.33 | $2.04 | $+87.79 | $4,317.44 | ▲ +87.79 after sell → book $9,722.62; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 6 | $236.80 | $2.03 | $-14.84 | $5,736.22 | ▼ -14.84 after sell → book $9,720.60; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SRRK` | 31 | $48.02 | $2.10 | $-50.69 | $7,222.73 | ▼ -50.69 after sell → book $9,718.49; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 1 | $915.66 | $2.01 | $-23.23 | $8,136.38 | ▼ -23.23 after sell → book $9,716.48; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 129 | $20.91 | $2.38 | — | $5,436.61 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2712.13 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 183 | $14.79 | $2.54 | — | $2,727.50 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $2712.13 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 192 | $14.07 | $2.57 | — | $23.50 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2712.13 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.50 | ▼ close $9,589.54 vs 09:30 $9,727.06 (session -119.46) | 16:00 close · cash $23.50 · equity $9,589.54 vs 09:30 $9,727.06 (-137.52; session marks -119.46) · 4 name(s) marked open→close (per-name table). GME×69 09:30 $22.90 → close $22.64 -17.94; TH×129 09:30 $20.91 → close $21.19 +36.12; RARE×183 09:30 $14.79 → close $14.51 -51.24; BHVN×192 09:30 $14.07 → close $13.62 -86.40 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1325.20 < 1 share @ 1746.53 |
| 2026-08-28 | `MPWR` | cash | leftover split 1301.43 < 1 share @ 1306.03 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ESI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SOLS` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-10 | `FNV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CVE` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SRRK` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GME` | 69 | 2026-09-17 @ $22.12 | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1536.89 |
| `TH` | 129 | 2026-09-18 @ $20.91 | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2712.13 |
| `RARE` | 183 | 2026-09-18 @ $14.79 | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $2712.13 |
| `BHVN` | 192 | 2026-09-18 @ $14.07 | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2712.13 |
