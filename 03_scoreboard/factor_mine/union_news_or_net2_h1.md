# Factor mine action — `union_news_or_net2_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 2

Cash book **+0.41%** ($10,041) · signal-only (no cash/fees) was +5.17%. Starts YES **6/26**. Fills 164 · skips 49 · realized $+136.71.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $24.09.

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
| 2026-08-25 | `AU` | 14 | — | $118.52 | +0.00 | $123.39 | +68.18 | +68.18 | +0.00 | +68.18 |
| 2026-08-25 | `FCX` | 21 | — | $77.13 | +0.00 | $79.91 | +58.38 | +58.38 | +0.00 | +58.38 |
| 2026-08-25 | `EZPW` | 48 | — | $35.05 | +0.00 | $35.23 | +8.64 | +8.64 | +0.00 | +8.64 |
| 2026-08-25 | `AMX` | 71 | — | $23.80 | +0.00 | $23.75 | -3.55 | -3.55 | +0.00 | -3.55 |
| 2026-08-25 | `RUM` | 179 | — | $9.42 | +0.00 | $10.23 | +144.99 | +144.99 | +0.00 | +144.99 |
| 2026-08-25 | `ZYME` | 58 | — | $28.86 | +0.00 | $27.47 | -80.62 | -80.62 | +0.00 | -80.62 |
| 2026-08-26 | `AU` | 14 | $123.39 | $119.80 | -50.26 | — | +0.00 | -50.26 | +17.92 | — |
| 2026-08-26 | `FCX` | 21 | $79.91 | $79.34 | -11.97 | — | +0.00 | -11.97 | +46.41 | — |
| 2026-08-26 | `EZPW` | 48 | $35.23 | $35.70 | +22.56 | — | +0.00 | +22.56 | +31.20 | — |
| 2026-08-26 | `AMX` | 71 | $23.75 | $23.75 | +0.00 | $23.62 | -9.23 | -9.23 | -3.55 | -12.78 |
| 2026-08-26 | `RUM` | 179 | $10.23 | $10.07 | -28.64 | — | +0.00 | -28.64 | +116.35 | — |
| 2026-08-26 | `ZYME` | 58 | $27.47 | $27.56 | +5.22 | $29.30 | +101.21 | +106.43 | -75.40 | +25.81 |
| 2026-08-26 | `FNV` | 13 | — | $267.02 | +0.00 | $267.37 | +4.55 | +4.55 | +0.00 | +4.55 |
| 2026-08-26 | `ASST` | 168 | — | $20.72 | +0.00 | $21.50 | +131.04 | +131.04 | +0.00 | +131.04 |
| 2026-08-27 | `AMX` | 71 | $23.62 | $23.77 | +10.65 | — | +0.00 | +10.65 | -2.13 | — |
| 2026-08-27 | `ZYME` | 58 | $29.30 | $29.33 | +1.45 | — | +0.00 | +1.45 | +27.26 | — |
| 2026-08-27 | `FNV` | 13 | $267.37 | $267.23 | -1.82 | — | +0.00 | -1.82 | +2.73 | — |
| 2026-08-27 | `ASST` | 168 | $21.50 | $22.45 | +159.60 | — | +0.00 | +159.60 | +290.64 | — |
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
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 5 | — | $240.22 | +0.00 | $236.98 | -16.20 | -16.20 | +0.00 | -16.20 |
| 2026-08-28 | `PLAB` | 43 | — | $30.01 | +0.00 | $27.73 | -98.04 | -98.04 | +0.00 | -98.04 |
| 2026-08-28 | `ADSK` | 5 | — | $261.16 | +0.00 | $260.66 | -2.50 | -2.50 | +0.00 | -2.50 |
| 2026-08-28 | `SEDG` | 39 | — | $32.90 | +0.00 | $31.41 | -58.11 | -58.11 | +0.00 | -58.11 |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | — | +0.00 | +10.08 | -7.68 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -65.94 | — |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | — | +0.00 | +5.64 | -44.13 | — |
| 2026-08-31 | `DDOG` | 5 | $236.98 | $233.97 | -15.07 | — | +0.00 | -15.07 | -31.27 | — |
| 2026-08-31 | `PLAB` | 43 | $27.73 | $28.04 | +13.33 | — | +0.00 | +13.33 | -84.71 | — |
| 2026-08-31 | `ADSK` | 5 | $260.66 | $257.71 | -14.75 | — | +0.00 | -14.75 | -17.25 | — |
| 2026-08-31 | `SEDG` | 39 | $31.41 | $31.15 | -10.14 | — | +0.00 | -10.14 | -68.25 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 3 | — | $351.74 | +0.00 | $357.16 | +16.26 | +16.26 | +0.00 | +16.26 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `CXW` | 38 | — | $32.31 | +0.00 | $33.66 | +51.30 | +51.30 | +0.00 | +51.30 |
| 2026-09-03 | `FRNM` | 78 | — | $15.87 | +0.00 | $16.90 | +80.34 | +80.34 | +0.00 | +80.34 |
| 2026-09-03 | `MMED` | 52 | — | $23.88 | +0.00 | $23.84 | -2.08 | -2.08 | +0.00 | -2.08 |
| 2026-09-03 | `DE` | 1 | — | $703.25 | +0.00 | $694.41 | -8.84 | -8.84 | +0.00 | -8.84 |
| 2026-09-03 | `HPE` | 26 | — | $47.60 | +0.00 | $54.44 | +177.84 | +177.84 | +0.00 | +177.84 |
| 2026-09-03 | `CNXC` | 38 | — | $32.88 | +0.00 | $32.85 | -1.14 | -1.14 | +0.00 | -1.14 |
| 2026-09-04 | `AVGO` | 3 | $357.16 | $359.70 | +7.62 | — | +0.00 | +7.62 | +23.88 | — |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | — | +0.00 | -5.22 | +54.94 | — |
| 2026-09-04 | `CXW` | 38 | $33.66 | $33.46 | -7.60 | — | +0.00 | -7.60 | +43.70 | — |
| 2026-09-04 | `FRNM` | 78 | $16.90 | $16.40 | -39.00 | $16.31 | -7.02 | -46.02 | +41.34 | +34.32 |
| 2026-09-04 | `MMED` | 52 | $23.84 | $23.84 | +0.00 | $23.29 | -28.60 | -28.60 | -2.08 | -30.68 |
| 2026-09-04 | `DE` | 1 | $694.41 | $692.03 | -2.38 | — | +0.00 | -2.38 | -11.22 | — |
| 2026-09-04 | `HPE` | 26 | $54.44 | $53.85 | -15.34 | $52.00 | -48.10 | -63.44 | +162.50 | +114.40 |
| 2026-09-04 | `CNXC` | 38 | $32.85 | $32.48 | -14.06 | — | +0.00 | -14.06 | -15.20 | — |
| 2026-09-04 | `CRM` | 4 | — | $263.36 | +0.00 | $259.23 | -16.52 | -16.52 | +0.00 | -16.52 |
| 2026-09-04 | `MRX` | 16 | — | $75.65 | +0.00 | $78.27 | +41.92 | +41.92 | +0.00 | +41.92 |
| 2026-09-04 | `BE` | 5 | — | $236.82 | +0.00 | $252.87 | +80.25 | +80.25 | +0.00 | +80.25 |
| 2026-09-04 | `AMX` | 55 | — | $23.03 | +0.00 | $23.00 | -1.65 | -1.65 | +0.00 | -1.65 |
| 2026-09-04 | `BAK` | 657 | — | $1.94 | +0.00 | $1.89 | -32.85 | -32.85 | +0.00 | -32.85 |
| 2026-09-08 | `FRNM` | 78 | $16.31 | $16.74 | +33.54 | — | +0.00 | +33.54 | +67.86 | — |
| 2026-09-08 | `MMED` | 52 | $23.29 | $23.16 | -6.76 | — | +0.00 | -6.76 | -37.44 | — |
| 2026-09-08 | `HPE` | 26 | $52.00 | $52.29 | +7.54 | — | +0.00 | +7.54 | +121.94 | — |
| 2026-09-08 | `CRM` | 4 | $259.23 | $253.72 | -22.04 | — | +0.00 | -22.04 | -38.56 | — |
| 2026-09-08 | `MRX` | 16 | $78.27 | $78.84 | +9.12 | $76.71 | -34.08 | -24.96 | +51.04 | +16.96 |
| 2026-09-08 | `BE` | 5 | $252.87 | $267.76 | +74.45 | — | +0.00 | +74.45 | +154.70 | — |
| 2026-09-08 | `AMX` | 55 | $23.00 | $23.15 | +8.25 | — | +0.00 | +8.25 | +6.60 | — |
| 2026-09-08 | `BAK` | 657 | $1.89 | $1.94 | +32.85 | — | +0.00 | +32.85 | +0.00 | — |
| 2026-09-09 | `MRX` | 16 | $76.71 | $76.60 | -1.76 | — | +0.00 | -1.76 | +15.20 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 10 | — | $164.43 | +0.00 | $150.28 | -141.50 | -141.50 | +0.00 | -141.50 |
| 2026-09-11 | `BTI` | 30 | — | $56.03 | +0.00 | $55.24 | -23.70 | -23.70 | +0.00 | -23.70 |
| 2026-09-11 | `ADBE` | 7 | — | $242.17 | +0.00 | $252.23 | +70.42 | +70.42 | +0.00 | +70.42 |
| 2026-09-11 | `CNQ` | 34 | — | $49.94 | +0.00 | $50.07 | +4.42 | +4.42 | +0.00 | +4.42 |
| 2026-09-11 | `AVTR` | 114 | — | $15.01 | +0.00 | $14.81 | -22.80 | -22.80 | +0.00 | -22.80 |
| 2026-09-11 | `BAK` | 813 | — | $2.12 | +0.00 | $2.08 | -32.52 | -32.52 | +0.00 | -32.52 |
| 2026-09-14 | `ORCL` | 10 | $150.28 | $141.42 | -88.60 | — | +0.00 | -88.60 | -230.10 | — |
| 2026-09-14 | `BTI` | 30 | $55.24 | $57.12 | +56.40 | — | +0.00 | +56.40 | +32.70 | — |
| 2026-09-14 | `ADBE` | 7 | $252.23 | $261.51 | +64.96 | — | +0.00 | +64.96 | +135.38 | — |
| 2026-09-14 | `CNQ` | 34 | $50.07 | $50.76 | +23.46 | — | +0.00 | +23.46 | +27.88 | — |
| 2026-09-14 | `AVTR` | 114 | $14.81 | $14.87 | +6.84 | — | +0.00 | +6.84 | -15.96 | — |
| 2026-09-14 | `BAK` | 813 | $2.08 | $2.05 | -24.39 | — | +0.00 | -24.39 | -56.91 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 77 | — | $26.27 | +0.00 | $26.59 | +24.64 | +24.64 | +0.00 | +24.64 |
| 2026-09-16 | `QCOM` | 10 | — | $189.17 | +0.00 | $184.84 | -43.30 | -43.30 | +0.00 | -43.30 |
| 2026-09-16 | `SM` | 51 | — | $39.99 | +0.00 | $38.16 | -93.33 | -93.33 | +0.00 | -93.33 |
| 2026-09-16 | `AMX` | 87 | — | $23.18 | +0.00 | $22.98 | -17.40 | -17.40 | +0.00 | -17.40 |
| 2026-09-16 | `AVTR` | 131 | — | $15.53 | +0.00 | $15.61 | +10.48 | +10.48 | +0.00 | +10.48 |
| 2026-09-17 | `WAY` | 77 | $26.59 | $26.51 | -6.16 | — | +0.00 | -6.16 | +18.48 | — |
| 2026-09-17 | `QCOM` | 10 | $184.84 | $190.35 | +55.10 | — | +0.00 | +55.10 | +11.80 | — |
| 2026-09-17 | `SM` | 51 | $38.16 | $37.57 | -30.09 | — | +0.00 | -30.09 | -123.42 | — |
| 2026-09-17 | `AMX` | 87 | $22.98 | $23.09 | +9.57 | — | +0.00 | +9.57 | -7.83 | — |
| 2026-09-17 | `AVTR` | 131 | $15.61 | $15.81 | +26.20 | $15.86 | +6.55 | +32.75 | +36.68 | +43.23 |
| 2026-09-17 | `SMTC` | 6 | — | $170.85 | +0.00 | $178.19 | +44.04 | +44.04 | +0.00 | +44.04 |
| 2026-09-17 | `GME` | 51 | — | $22.12 | +0.00 | $22.77 | +33.15 | +33.15 | +0.00 | +33.15 |
| 2026-09-17 | `JBHT` | 4 | — | $238.60 | +0.00 | $236.80 | -7.20 | -7.20 | +0.00 | -7.20 |
| 2026-09-17 | `SRRK` | 23 | — | $49.52 | +0.00 | $49.02 | -11.50 | -11.50 | +0.00 | -11.50 |
| 2026-09-17 | `LITE` | 1 | — | $934.88 | +0.00 | $893.61 | -41.27 | -41.27 | +0.00 | -41.27 |
| 2026-09-17 | `ALVO` | 220 | — | $5.22 | +0.00 | $5.34 | +26.40 | +26.40 | +0.00 | +26.40 |
| 2026-09-17 | `KEY` | 54 | — | $20.98 | +0.00 | $20.95 | -1.62 | -1.62 | +0.00 | -1.62 |
| 2026-09-18 | `AVTR` | 131 | $15.86 | $15.87 | +1.31 | — | +0.00 | +1.31 | +44.54 | — |
| 2026-09-18 | `SMTC` | 6 | $178.19 | $182.33 | +24.84 | — | +0.00 | +24.84 | +68.88 | — |
| 2026-09-18 | `GME` | 51 | $22.77 | $22.90 | +6.63 | $22.64 | -13.26 | -6.63 | +39.78 | +26.52 |
| 2026-09-18 | `JBHT` | 4 | $236.80 | $236.80 | +0.00 | — | +0.00 | +0.00 | -7.20 | — |
| 2026-09-18 | `SRRK` | 23 | $49.02 | $48.02 | -23.00 | — | +0.00 | -23.00 | -34.50 | — |
| 2026-09-18 | `LITE` | 1 | $893.61 | $915.66 | +22.05 | — | +0.00 | +22.05 | -19.22 | — |
| 2026-09-18 | `ALVO` | 220 | $5.34 | $5.40 | +13.20 | — | +0.00 | +13.20 | +39.60 | — |
| 2026-09-18 | `KEY` | 54 | $20.95 | $20.90 | -2.70 | — | +0.00 | -2.70 | -4.32 | — |
| 2026-09-18 | `TH` | 143 | — | $20.91 | +0.00 | $21.19 | +40.04 | +40.04 | +0.00 | +40.04 |
| 2026-09-18 | `RARE` | 202 | — | $14.79 | +0.00 | $14.51 | -56.56 | -56.56 | +0.00 | -56.56 |
| 2026-09-18 | `BHVN` | 213 | — | $14.07 | +0.00 | $13.62 | -95.85 | -95.85 | +0.00 | -95.85 |

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
| 2026-08-25 | +1.80 | $8,946.21 | CRSP×21 | $10,162.74 | +17.95 | +196.02 | AU, FCX, EZPW, AMX, RUM, ZYME | CRSP | $136.29 | $10,343.58 | AU×14, FCX×21, EZPW×48, AMX×71, RUM×179, ZYME×58 |
| 2026-08-26 | +2.02 | $136.29 | AU×14, FCX×21, EZPW×48, AMX×71, RUM×179, ZYME×58 | $10,280.49 | -63.09 | +227.57 | FNV, ASST | AU, FCX, EZPW, RUM | $30.15 | $10,494.67 | AMX×71, ZYME×58, FNV×13, ASST×168 |
| 2026-08-27 | — | $30.15 | AMX×71, ZYME×58, FNV×13, ASST×168 | $10,664.55 | +169.88 | -100.37 | ACMR, MU, CM, GEN, LRCX, NVDA, AXTI | AMX, ZYME, FNV, ASST | $2,093.67 | $10,540.92 | ACMR×16, MU×1, CM×11, GEN×44, LRCX×4, NVDA×5, AXTI×18 |
| 2026-08-28 | +0.75 | $2,093.67 | ACMR×16, MU×1, CM×11, GEN×44, LRCX×4, NVDA×5, AXTI×18 | $10,479.68 | -61.24 | -403.63 | KEYS, SMTC, CIEN, MPWR, DDOG, PLAB, ADSK, SEDG | ACMR, MU, CM, GEN, LRCX, NVDA, AXTI | $287.87 | $10,045.44 | KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, PLAB×43, ADSK×5, SEDG×39 |
| 2026-08-31 | -5.85 | $287.87 | KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, PLAB×43, ADSK×5, SEDG×39 | $10,044.69 | -0.75 | +0.00 | — | KEYS, SMTC, CIEN, MPWR, DDOG, PLAB, ADSK, SEDG | $10,028.29 | $10,028.29 | — |
| 2026-09-01 | -6.30 | $10,028.29 | — | $10,028.29 | -0.00 | +0.00 | — | — | $10,028.29 | $10,028.29 | — |
| 2026-09-02 | -3.83 | $10,028.29 | — | $10,028.29 | -0.00 | +0.00 | — | — | $10,028.29 | $10,028.29 | — |
| 2026-09-03 | -0.90 | $10,028.29 | — | $10,028.29 | -0.00 | +373.84 | AVGO, DELL, CXW, FRNM, MMED, DE, HPE, CNXC | — | $1,086.12 | $10,385.49 | AVGO×3, DELL×2, CXW×38, FRNM×78, MMED×52, DE×1, HPE×26, CNXC×38 |
| 2026-09-04 | +2.25 | $1,086.12 | AVGO×3, DELL×2, CXW×38, FRNM×78, MMED×52, DE×1, HPE×26, CNXC×38 | $10,309.51 | -75.98 | -12.57 | CRM, MRX, BE, AMX, BAK | AVGO, DELL, CXW, DE, CNXC | $374.39 | $10,269.97 | FRNM×78, MMED×52, HPE×26, CRM×4, MRX×16, BE×5, AMX×55, BAK×657 |
| 2026-09-08 | -11.47 | $374.39 | FRNM×78, MMED×52, HPE×26, CRM×4, MRX×16, BE×5, AMX×55, BAK×657 | $10,406.92 | +136.95 | -34.08 | — | FRNM, MMED, HPE, CRM, BE, AMX, BAK | $9,124.16 | $10,351.52 | MRX×16 |
| 2026-09-09 | -13.95 | $9,124.16 | MRX×16 | $10,349.76 | -1.76 | +0.00 | — | MRX | $10,347.70 | $10,347.70 | — |
| 2026-09-10 | -13.28 | $10,347.70 | — | $10,347.70 | +0.00 | +0.00 | — | — | $10,347.70 | $10,347.70 | — |
| 2026-09-11 | +0.50 | $10,347.70 | — | $10,347.70 | +0.00 | -145.68 | ORCL, BTI, ADBE, CNQ, AVTR, BAK | — | $173.63 | $10,181.00 | ORCL×10, BTI×30, ADBE×7, CNQ×34, AVTR×114, BAK×813 |
| 2026-09-14 | -11.00 | $173.63 | ORCL×10, BTI×30, ADBE×7, CNQ×34, AVTR×114, BAK×813 | $10,219.67 | +38.67 | +0.00 | — | ORCL, BTI, ADBE, CNQ, AVTR, BAK | $10,198.37 | $10,198.37 | — |
| 2026-09-15 | -3.84 | $10,198.37 | — | $10,198.37 | +0.00 | +0.00 | — | — | $10,198.37 | $10,198.37 | — |
| 2026-09-16 | +5.30 | $10,198.37 | — | $10,198.37 | +0.00 | -118.91 | WAY, QCOM, SM, AMX, AVTR | — | $182.28 | $10,068.44 | WAY×77, QCOM×10, SM×51, AMX×87, AVTR×131 |
| 2026-09-17 | +7.38 | $182.28 | WAY×77, QCOM×10, SM×51, AMX×87, AVTR×131 | $10,123.06 | +54.62 | +48.55 | SMTC, GME, JBHT, SRRK, LITE, ALVO, KEY | WAY, QCOM, SM, AMX | $565.23 | $10,147.67 | AVTR×131, SMTC×6, GME×51, JBHT×4, SRRK×23, LITE×1, ALVO×220, KEY×54 |
| 2026-09-18 | +4.86 | $565.23 | AVTR×131, SMTC×6, GME×51, JBHT×4, SRRK×23, LITE×1, ALVO×220, KEY×54 | $10,190.00 | +42.33 | -125.63 | TH, RARE, BHVN | AVTR, SMTC, JBHT, SRRK, LITE, ALVO, KEY | $24.09 | $10,040.98 | GME×51, TH×143, RARE×202, BHVN×213 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $8,758.81 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $7,505.17 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $6,270.08 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $5,021.21 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 81 | $15.38 | $2.23 | — | $3,773.20 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $2,571.18 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `S` | 52 | $23.77 | $2.15 | — | $1,332.73 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,332.73 | ▲ close $10,120.33 vs 09:30 $10,000.00 (session +137.19) | 16:00 close · cash $1,332.73 · equity $10,120.33 vs 09:30 $10,000.00 (+120.33; session marks +137.19) · 7 name(s) marked open→close (per-name table). HLIT×94 09:30 $13.18 → close $13.92 +69.56; ANGX×290 09:30 $4.31 → close $4.37 +17.40; ARX×63 09:30 $19.57 → close $19.58 +0.63; MH×92 09:30 $13.55 → close $13.10 -41.40; VELO×81 09:30 $15.38 → close $16.16 +63.18; NRG×10 09:30 $120.00 → close $126.24 +62.40; S×52 09:30 $23.77 → close $23.11 -34.58 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,332.73 | ▲ 09:30 equity $10,155.37 vs yday $10,120.33 (+35.04) | 09:30 open · cash $1,332.73 (unchanged overnight, no fees) · equity $10,155.37 vs prior close $10,120.33 (+35.04) · 7 name(s) re-marked at the open (per-name table). HLIT×94 yday $13.92 → 09:30 $13.84 -7.52; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; MH×92 yday $13.10 → 09:30 $13.16 +5.52; VELO×81 yday $16.16 → 09:30 $16.05 -8.91; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; S×52 yday $23.11 → 09:30 $22.50 -31.72 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $2,631.40 | ▲ +57.47 after sell → book $10,153.08; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,961.60 | ▲ +76.56 after sell → book $10,149.28; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $5,192.31 | ▼ -4.38 after sell → book $10,147.08; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $6,400.73 | ▼ -40.44 after sell → book $10,144.78; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VELO` | 81 | $16.05 | $2.26 | $+49.78 | $7,698.53 | ▲ +49.78 after sell → book $10,142.53; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $8,970.49 | ▲ +69.94 after sell → book $10,140.49; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `S` | 52 | $22.50 | $2.17 | $-70.61 | $10,138.32 | ▼ -70.61 after sell → book $10,138.32; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 36 | $46.18 | $2.10 | — | $8,473.74 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+6.7; leftover $1689.72 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 11 | $142.77 | $2.02 | — | $6,901.25 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+5.8; leftover $1689.72 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 8 | $202.70 | $2.01 | — | $5,277.64 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+8.3; leftover $1689.72 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `GLOB` | 45 | $37.18 | $2.12 | — | $3,602.41 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list oppset; ⚪; ret5=-0.1; leftover $1689.72 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TPG` | 32 | $52.67 | $2.09 | — | $1,914.89 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list oppset; 🔵; ⚪; ret5=+9.4; leftover $1689.72 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 34 | $49.00 | $2.09 | — | $246.79 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $1689.72 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
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
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,930.74 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1264.49 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $7,675.38 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1264.49 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 511 | $2.47 | $6.59 | — | $6,406.62 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1264.49 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $5,171.24 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1264.49 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRK` | 8 | $150.78 | $2.01 | — | $3,962.98 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list oppset; 🔵; ⚪; ret5=+14.5; leftover $1264.49 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 79 | $16.00 | $2.23 | — | $2,696.76 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1264.49 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $1,493.62 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ret5=+173.9; leftover $1264.49 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 47 | $26.57 | $2.13 | — | $242.70 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1264.49 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $242.70 | ▼ close $9,936.08 vs 09:30 $10,115.90 (session -158.68) | 16:00 close · cash $242.70 · equity $9,936.08 vs 09:30 $10,115.90 (-179.82; session marks -158.68) · 8 name(s) marked open→close (per-name table). BHP×13 09:30 $91.01 → close $93.63 +34.06; APA×28 09:30 $44.76 → close $44.39 -10.36; AUTL×511 09:30 $2.47 → close $2.46 -5.11; CRSP×21 09:30 $58.73 → close $58.12 -12.81; MRK×8 09:30 $150.78 → close $148.99 -14.32; ASST×79 09:30 $16.00 → close $16.13 +10.27; MRNA×8 09:30 $150.14 → close $133.32 -134.56; ZLAB×47 09:30 $26.57 → close $26.02 -25.85 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $242.70 | ▲ 09:30 equity $10,136.64 vs yday $9,936.08 (+200.56) | 09:30 open · cash $242.70 (unchanged overnight, no fees) · equity $10,136.64 vs prior close $9,936.08 (+200.56) · 8 name(s) re-marked at the open (per-name table). BHP×13 yday $93.63 → 09:30 $95.72 +27.17; APA×28 yday $44.39 → 09:30 $44.52 +3.64; AUTL×511 yday $2.46 → 09:30 $2.47 +5.11; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; MRK×8 yday $148.99 → 09:30 $149.12 +1.04; ASST×79 yday $16.13 → 09:30 $17.66 +120.87; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; ZLAB×47 yday $26.02 → 09:30 $26.25 +10.81 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,485.01 | ▲ +57.15 after sell → book $10,134.59; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $2,729.48 | ▼ -10.89 after sell → book $10,132.50; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRK` | 8 | $149.12 | $2.03 | $-17.33 | $3,920.41 | ▼ -17.33 after sell → book $10,130.47; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 79 | $17.66 | $2.25 | $+126.66 | $5,313.29 | ▲ +126.66 after sell → book $10,128.21; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $6,376.14 | ▼ -140.29 after sell → book $10,126.18; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 47 | $26.25 | $2.15 | $-19.32 | $7,607.74 | ▼ -19.32 after sell → book $10,124.03; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,411.42 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1267.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $5,142.42 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1267.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 16 | $78.88 | $2.04 | — | $3,878.30 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1267.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `VIRT` | 20 | $60.66 | $2.05 | — | $2,663.05 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list oppset; 🔵; ⚪; ret5=+7.0; leftover $1267.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MFC` | 29 | $42.48 | $2.08 | — | $1,429.05 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list oppset; 🔵; ⚪; ret5=-3.3; leftover $1267.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 146 | $8.66 | $2.43 | — | $162.26 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1267.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
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
| 2026-08-25 09:30 ET | **BUY** | `AU` | 14 | $118.52 | $2.03 | — | $8,499.36 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1693.45 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 21 | $77.13 | $2.05 | — | $6,877.58 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1693.45 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 48 | $35.05 | $2.13 | — | $5,193.04 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,oppset; 🔵; ⚪; ret5=+19.7; leftover $1693.45 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMX` | 71 | $23.80 | $2.20 | — | $3,501.04 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list oppset; 🔵; ret5=+0.5; leftover $1693.45 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 179 | $9.42 | $2.53 | — | $1,812.33 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,ohlc_hot,oppset; 🔵; ret5=+13.6; leftover $1693.45 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 58 | $28.86 | $2.16 | — | $136.29 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+13.7; leftover $1693.45 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $136.29 | ▲ close $10,343.58 vs 09:30 $10,162.74 (session +196.02) | 16:00 close · cash $136.29 · equity $10,343.58 vs 09:30 $10,162.74 (+180.84; session marks +196.02) · 6 name(s) marked open→close (per-name table). AU×14 09:30 $118.52 → close $123.39 +68.18; FCX×21 09:30 $77.13 → close $79.91 +58.38; EZPW×48 09:30 $35.05 → close $35.23 +8.64; AMX×71 09:30 $23.80 → close $23.75 -3.55; RUM×179 09:30 $9.42 → close $10.23 +144.99; ZYME×58 09:30 $28.86 → close $27.47 -80.62 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $136.29 | ▼ 09:30 equity $10,280.49 vs yday $10,343.58 (-63.09) | 09:30 open · cash $136.29 (unchanged overnight, no fees) · equity $10,280.49 vs prior close $10,343.58 (-63.09) · 6 name(s) re-marked at the open (per-name table). AU×14 yday $123.39 → 09:30 $119.80 -50.26; FCX×21 yday $79.91 → 09:30 $79.34 -11.97; EZPW×48 yday $35.23 → 09:30 $35.70 +22.56; AMX×71 yday $23.75 → 09:30 $23.75 +0.00; RUM×179 yday $10.23 → 09:30 $10.07 -28.64; ZYME×58 yday $27.47 → 09:30 $27.56 +5.22 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 14 | $119.80 | $2.06 | $+13.83 | $1,811.43 | ▲ +13.83 after sell → book $10,278.43; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 21 | $79.34 | $2.08 | $+42.28 | $3,475.50 | ▲ +42.28 after sell → book $10,276.36; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 48 | $35.70 | $2.16 | $+26.91 | $5,186.94 | ▲ +26.91 after sell → book $10,274.20; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 179 | $10.07 | $2.57 | $+111.25 | $6,986.90 | ▲ +111.25 after sell → book $10,271.63; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 13 | $267.02 | $2.03 | — | $3,513.61 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $3493.45 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ASST` | 168 | $20.72 | $2.49 | — | $30.15 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list oppset; 🔵; ret5=+67.1; leftover $3493.45 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.15 | ▲ close $10,494.67 vs 09:30 $10,280.49 (session +227.57) | 16:00 close · cash $30.15 · equity $10,494.67 vs 09:30 $10,280.49 (+214.18; session marks +227.57) · 4 name(s) marked open→close (per-name table). AMX×71 09:30 $23.75 → close $23.62 -9.23; ZYME×58 09:30 $27.56 → close $29.30 +101.21; FNV×13 09:30 $267.02 → close $267.37 +4.55; ASST×168 09:30 $20.72 → close $21.50 +131.04 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.15 | ▲ 09:30 equity $10,664.55 vs yday $10,494.67 (+169.88) | 09:30 open · cash $30.15 (unchanged overnight, no fees) · equity $10,664.55 vs prior close $10,494.67 (+169.88) · 4 name(s) re-marked at the open (per-name table). AMX×71 yday $23.62 → 09:30 $23.77 +10.65; ZYME×58 yday $29.30 → 09:30 $29.33 +1.45; FNV×13 yday $267.37 → 09:30 $267.23 -1.82; ASST×168 yday $21.50 → 09:30 $22.45 +159.60 | — |
| 2026-08-27 09:30 ET | **SELL** | `AMX` | 71 | $23.77 | $2.23 | $-6.56 | $1,715.60 | ▼ -6.56 after sell → book $10,662.33; vs 09:30 mark -2.22 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZYME` | 58 | $29.33 | $2.19 | $+22.91 | $3,414.55 | ▲ +22.91 after sell → book $10,660.14; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 13 | $267.23 | $2.07 | $-1.37 | $6,886.47 | ▼ -1.37 after sell → book $10,658.07; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 168 | $22.45 | $2.55 | $+285.59 | $10,655.52 | ▲ +285.59 after sell → book $10,655.52; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 16 | $81.65 | $2.04 | — | $9,347.08 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1331.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $8,378.08 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1331.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 11 | $118.77 | $2.02 | — | $7,069.59 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; ret5=+0.3; leftover $1331.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 44 | $29.83 | $2.12 | — | $5,754.94 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+7.6; leftover $1331.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 4 | $318.88 | $2.00 | — | $4,477.42 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1331.94 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 5 | $222.86 | $2.00 | — | $3,361.12 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $1331.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 18 | $70.30 | $2.04 | — | $2,093.67 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ret5=-11.2; leftover $1331.94 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,093.67 | ▼ close $10,540.92 vs 09:30 $10,664.55 (session -100.37) | 16:00 close · cash $2,093.67 · equity $10,540.92 vs 09:30 $10,664.55 (-123.63; session marks -100.37) · 7 name(s) marked open→close (per-name table). ACMR×16 09:30 $81.65 → close $80.49 -18.56; MU×1 09:30 $967.01 → close $935.39 -31.62; CM×11 09:30 $118.77 → close $114.84 -43.23; GEN×44 09:30 $29.83 → close $30.50 +29.48; LRCX×4 09:30 $318.88 → close $318.58 -1.20; NVDA×5 09:30 $222.86 → close $227.98 +25.60; AXTI×18 09:30 $70.30 → close $66.92 -60.84 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,093.67 | ▼ 09:30 equity $10,479.68 vs yday $10,540.92 (-61.24) | 09:30 open · cash $2,093.67 (unchanged overnight, no fees) · equity $10,479.68 vs prior close $10,540.92 (-61.24) · 7 name(s) re-marked at the open (per-name table). ACMR×16 yday $80.49 → 09:30 $79.27 -19.52; MU×1 yday $935.39 → 09:30 $919.29 -16.10; CM×11 yday $114.84 → 09:30 $115.66 +9.02; GEN×44 yday $30.50 → 09:30 $30.50 +0.00; LRCX×4 yday $318.58 → 09:30 $318.03 -2.20; NVDA×5 yday $227.98 → 09:30 $227.36 -3.10; AXTI×18 yday $66.92 → 09:30 $65.29 -29.34 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 16 | $79.27 | $2.06 | $-42.18 | $3,359.94 | ▼ -42.18 after sell → book $10,477.63; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $4,277.21 | ▼ -51.73 after sell → book $10,475.61; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 11 | $115.66 | $2.04 | $-38.28 | $5,547.43 | ▼ -38.28 after sell → book $10,473.57; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 44 | $30.50 | $2.14 | $+25.22 | $6,887.29 | ▲ +25.22 after sell → book $10,471.43; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 4 | $318.03 | $2.02 | $-7.42 | $8,157.38 | ▼ -7.42 after sell → book $10,469.40; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 5 | $227.36 | $2.02 | $+18.47 | $9,292.16 | ▲ +18.47 after sell → book $10,467.38; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 18 | $65.29 | $2.06 | $-94.29 | $10,465.31 | ▼ -94.29 after sell → book $10,465.31; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $9,165.67 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1308.16 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $7,887.82 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $1308.16 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,684.56 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1308.16 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $5,376.53 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1308.16 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $4,173.43 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1308.16 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PLAB` | 43 | $30.01 | $2.12 | — | $2,880.88 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list oppset; 🔵; ret5=-0.9; leftover $1308.16 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $1,573.07 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; ret5=+7.8; leftover $1308.16 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $32.90 | $2.11 | — | $287.87 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1308.16 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $287.87 | ▼ close $10,045.44 vs 09:30 $10,479.68 (session -403.63) | 16:00 close · cash $287.87 · equity $10,045.44 vs 09:30 $10,479.68 (-434.24; session marks -403.63) · 8 name(s) marked open→close (per-name table). KEYS×4 09:30 $324.41 → close $319.97 -17.76; SMTC×9 09:30 $141.76 → close $131.17 -95.31; CIEN×3 09:30 $400.42 → close $378.44 -65.94; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×5 09:30 $240.22 → close $236.98 -16.20; PLAB×43 09:30 $30.01 → close $27.73 -98.04; ADSK×5 09:30 $261.16 → close $260.66 -2.50; SEDG×39 09:30 $32.90 → close $31.41 -58.11 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $287.87 | ▼ 09:30 equity $10,044.69 vs yday $10,045.44 (-0.75) | 09:30 open · cash $287.87 (unchanged overnight, no fees) · equity $10,044.69 vs prior close $10,045.44 (-0.75) · 8 name(s) re-marked at the open (per-name table). KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×5 yday $236.98 → 09:30 $233.97 -15.07; PLAB×43 yday $27.73 → 09:30 $28.04 +13.33; ADSK×5 yday $260.66 → 09:30 $257.71 -14.75; SEDG×39 yday $31.41 → 09:30 $31.15 -10.14 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $1,575.81 | ▼ -11.70 after sell → book $10,042.67; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $2,764.47 | ▼ -89.19 after sell → book $10,040.63; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $3,897.77 | ▼ -69.96 after sell → book $10,038.61; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $5,157.66 | ▼ -48.14 after sell → book $10,036.60; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 5 | $233.97 | $2.02 | $-35.31 | $6,325.46 | ▼ -35.31 after sell → book $10,034.58; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PLAB` | 43 | $28.04 | $2.14 | $-88.97 | $7,529.04 | ▼ -88.97 after sell → book $10,032.44; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 5 | $257.71 | $2.03 | $-21.28 | $8,815.56 | ▼ -21.28 after sell → book $10,030.41; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 39 | $31.15 | $2.13 | $-72.48 | $10,028.29 | ▼ -72.48 after sell → book $10,028.29; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,028.29 | ▲ close $10,028.29 vs 09:30 $10,044.69 (session +0.00) | 16:00 close · cash $10,028.29 · no lots left · equity $10,028.29. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,028.29 | ▲ 09:30 equity $10,028.29 vs yday $10,028.29 (-0.00) | 09:30 open · cash $10,028.29 · no holdings · equity $10,028.29 vs prior close $10,028.29 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,028.29 | ▲ close $10,028.29 vs 09:30 $10,028.29 (session +0.00) | 16:00 close · cash $10,028.29 · no lots left · equity $10,028.29. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,028.29 | ▲ 09:30 equity $10,028.29 vs yday $10,028.29 (-0.00) | 09:30 open · cash $10,028.29 · no holdings · equity $10,028.29 vs prior close $10,028.29 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,028.29 | ▲ close $10,028.29 vs 09:30 $10,028.29 (session +0.00) | 16:00 close · cash $10,028.29 · no lots left · equity $10,028.29. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,028.29 | ▲ 09:30 equity $10,028.29 vs yday $10,028.29 (-0.00) | 09:30 open · cash $10,028.29 · no holdings · equity $10,028.29 vs prior close $10,028.29 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,971.07 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1253.54 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,996.45 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $1253.54 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 38 | $32.31 | $2.10 | — | $6,766.57 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1253.54 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 78 | $15.87 | $2.22 | — | $5,526.48 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1253.54 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 52 | $23.88 | $2.15 | — | $4,282.58 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+21.9; leftover $1253.54 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $3,577.33 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1253.54 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 26 | $47.60 | $2.07 | — | $2,337.67 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react,oppset; 🔵; ret5=-6.2; leftover $1253.54 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 38 | $32.88 | $2.10 | — | $1,086.12 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ret5=+16.2; leftover $1253.54 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,086.12 | ▲ close $10,385.49 vs 09:30 $10,028.29 (session +373.84) | 16:00 close · cash $1,086.12 · equity $10,385.49 vs 09:30 $10,028.29 (+357.20; session marks +373.84) · 8 name(s) marked open→close (per-name table). AVGO×3 09:30 $351.74 → close $357.16 +16.26; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×38 09:30 $32.31 → close $33.66 +51.30; FRNM×78 09:30 $15.87 → close $16.90 +80.34; MMED×52 09:30 $23.88 → close $23.84 -2.08; DE×1 09:30 $703.25 → close $694.41 -8.84; HPE×26 09:30 $47.60 → close $54.44 +177.84; CNXC×38 09:30 $32.88 → close $32.85 -1.14 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,086.12 | ▼ 09:30 equity $10,309.51 vs yday $10,385.49 (-75.98) | 09:30 open · cash $1,086.12 (unchanged overnight, no fees) · equity $10,309.51 vs prior close $10,385.49 (-75.98) · 8 name(s) re-marked at the open (per-name table). AVGO×3 yday $357.16 → 09:30 $359.70 +7.62; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×38 yday $33.66 → 09:30 $33.46 -7.60; FRNM×78 yday $16.90 → 09:30 $16.40 -39.00; MMED×52 yday $23.84 → 09:30 $23.84 +0.00; DE×1 yday $694.41 → 09:30 $692.03 -2.38; HPE×26 yday $54.44 → 09:30 $53.85 -15.34; CNXC×38 yday $32.85 → 09:30 $32.48 -14.06 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $2,163.20 | ▲ +19.86 after sell → book $10,307.49; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $3,188.75 | ▲ +50.93 after sell → book $10,305.48; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 38 | $33.46 | $2.12 | $+39.47 | $4,458.10 | ▲ +39.47 after sell → book $10,303.35; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $5,148.12 | ▼ -15.23 after sell → book $10,301.34; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 38 | $32.48 | $2.12 | $-19.43 | $6,380.24 | ▼ -19.43 after sell → book $10,299.22; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $5,324.79 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1276.05 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 16 | $75.65 | $2.04 | — | $4,112.36 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1276.05 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 5 | $236.82 | $2.00 | — | $2,926.25 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; ret5=+8.1; leftover $1276.05 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `AMX` | 55 | $23.03 | $2.15 | — | $1,657.45 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list oppset; 🔵; ret5=-1.4; leftover $1276.05 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 657 | $1.94 | $8.48 | — | $374.39 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $1276.05 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $374.39 | ▼ close $10,269.97 vs 09:30 $10,309.51 (session -12.57) | 16:00 close · cash $374.39 · equity $10,269.97 vs 09:30 $10,309.51 (-39.54; session marks -12.57) · 8 name(s) marked open→close (per-name table). FRNM×78 09:30 $16.40 → close $16.31 -7.02; MMED×52 09:30 $23.84 → close $23.29 -28.60; HPE×26 09:30 $53.85 → close $52.00 -48.10; CRM×4 09:30 $263.36 → close $259.23 -16.52; MRX×16 09:30 $75.65 → close $78.27 +41.92; BE×5 09:30 $236.82 → close $252.87 +80.25; AMX×55 09:30 $23.03 → close $23.00 -1.65; BAK×657 09:30 $1.94 → close $1.89 -32.85 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $374.39 | ▲ 09:30 equity $10,406.92 vs yday $10,269.97 (+136.95) | 09:30 open · cash $374.39 (unchanged overnight, no fees) · equity $10,406.92 vs prior close $10,269.97 (+136.95) · 8 name(s) re-marked at the open (per-name table). FRNM×78 yday $16.31 → 09:30 $16.74 +33.54; MMED×52 yday $23.29 → 09:30 $23.16 -6.76; HPE×26 yday $52.00 → 09:30 $52.29 +7.54; CRM×4 yday $259.23 → 09:30 $253.72 -22.04; MRX×16 yday $78.27 → 09:30 $78.84 +9.12; BE×5 yday $252.87 → 09:30 $267.76 +74.45; AMX×55 yday $23.00 → 09:30 $23.15 +8.25; BAK×657 yday $1.89 → 09:30 $1.94 +32.85 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 78 | $16.74 | $2.25 | $+63.39 | $1,677.86 | ▲ +63.39 after sell → book $10,404.67; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MMED` | 52 | $23.16 | $2.17 | $-41.75 | $2,880.02 | ▼ -41.75 after sell → book $10,402.51; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HPE` | 26 | $52.29 | $2.09 | $+117.78 | $4,237.47 | ▲ +117.78 after sell → book $10,400.42; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $5,250.33 | ▼ -42.58 after sell → book $10,398.40; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 5 | $267.76 | $2.03 | $+150.67 | $6,587.10 | ▲ +150.67 after sell → book $10,396.37; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AMX` | 55 | $23.15 | $2.18 | $+2.27 | $7,858.17 | ▲ +2.27 after sell → book $10,394.19; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 657 | $1.94 | $8.59 | $-17.07 | $9,124.16 | ▼ -17.07 after sell → book $10,385.60; vs 09:30 mark -8.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,124.16 | ▼ close $10,351.52 vs 09:30 $10,406.92 (session -34.08) | 16:00 close · cash $9,124.16 · equity $10,351.52 vs 09:30 $10,406.92 (-55.40; session marks -34.08) · 1 name(s) marked open→close (per-name table). MRX×16 09:30 $78.84 → close $76.71 -34.08 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,124.16 | ▼ 09:30 equity $10,349.76 vs yday $10,351.52 (-1.76) | 09:30 open · cash $9,124.16 (unchanged overnight, no fees) · equity $10,349.76 vs prior close $10,351.52 (-1.76) · 1 name(s) re-marked at the open (per-name table). MRX×16 yday $76.71 → 09:30 $76.60 -1.76 | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 16 | $76.60 | $2.06 | $+11.10 | $10,347.70 | ▲ +11.10 after sell → book $10,347.70; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,347.70 | ▲ close $10,347.70 vs 09:30 $10,349.76 (session +0.00) | 16:00 close · cash $10,347.70 · no lots left · equity $10,347.70. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,347.70 | ▲ 09:30 equity $10,347.70 vs yday $10,347.70 (+0.00) | 09:30 open · cash $10,347.70 · no holdings · equity $10,347.70 vs prior close $10,347.70 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,347.70 | ▲ close $10,347.70 vs 09:30 $10,347.70 (session +0.00) | 16:00 close · cash $10,347.70 · no lots left · equity $10,347.70. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,347.70 | ▲ 09:30 equity $10,347.70 vs yday $10,347.70 (+0.00) | 09:30 open · cash $10,347.70 · no holdings · equity $10,347.70 vs prior close $10,347.70 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 10 | $164.43 | $2.02 | — | $8,701.38 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1724.62 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BTI` | 30 | $56.03 | $2.08 | — | $7,018.40 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list oppset; 🔵; ret5=-0.8; leftover $1724.62 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 7 | $242.17 | $2.01 | — | $5,321.20 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; ret5=-11.1; leftover $1724.62 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CNQ` | 34 | $49.94 | $2.09 | — | $3,621.15 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list oppset; ret5=+1.7; leftover $1724.62 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 114 | $15.01 | $2.33 | — | $1,907.68 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $1724.62 | join🟢 sector🟡 gen🟡 news🟢 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 813 | $2.12 | $10.49 | — | $173.63 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1724.62 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $173.63 | ▼ close $10,181.00 vs 09:30 $10,347.70 (session -145.68) | 16:00 close · cash $173.63 · equity $10,181.00 vs 09:30 $10,347.70 (-166.70; session marks -145.68) · 6 name(s) marked open→close (per-name table). ORCL×10 09:30 $164.43 → close $150.28 -141.50; BTI×30 09:30 $56.03 → close $55.24 -23.70; ADBE×7 09:30 $242.17 → close $252.23 +70.42; CNQ×34 09:30 $49.94 → close $50.07 +4.42; AVTR×114 09:30 $15.01 → close $14.81 -22.80; BAK×813 09:30 $2.12 → close $2.08 -32.52 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.63 | ▲ 09:30 equity $10,219.67 vs yday $10,181.00 (+38.67) | 09:30 open · cash $173.63 (unchanged overnight, no fees) · equity $10,219.67 vs prior close $10,181.00 (+38.67) · 6 name(s) re-marked at the open (per-name table). ORCL×10 yday $150.28 → 09:30 $141.42 -88.60; BTI×30 yday $55.24 → 09:30 $57.12 +56.40; ADBE×7 yday $252.23 → 09:30 $261.51 +64.96; CNQ×34 yday $50.07 → 09:30 $50.76 +23.46; AVTR×114 yday $14.81 → 09:30 $14.87 +6.84; BAK×813 yday $2.08 → 09:30 $2.05 -24.39 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 10 | $141.42 | $2.04 | $-234.16 | $1,585.79 | ▼ -234.16 after sell → book $10,217.63; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-14 09:30 ET | **SELL** | `BTI` | 30 | $57.12 | $2.10 | $+28.52 | $3,297.28 | ▲ +28.52 after sell → book $10,215.52; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 7 | $261.51 | $2.04 | $+131.33 | $5,125.82 | ▲ +131.33 after sell → book $10,213.49; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CNQ` | 34 | $50.76 | $2.12 | $+23.67 | $6,849.54 | ▲ +23.67 after sell → book $10,211.37; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AVTR` | 114 | $14.87 | $2.36 | $-20.66 | $8,542.36 | ▼ -20.66 after sell → book $10,209.01; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🔴 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 813 | $2.05 | $10.64 | $-78.03 | $10,198.37 | ▼ -78.03 after sell → book $10,198.37; vs 09:30 mark -10.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,198.37 | ▲ close $10,198.37 vs 09:30 $10,219.67 (session +0.00) | 16:00 close · cash $10,198.37 · no lots left · equity $10,198.37. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,198.37 | ▲ 09:30 equity $10,198.37 vs yday $10,198.37 (+0.00) | 09:30 open · cash $10,198.37 · no holdings · equity $10,198.37 vs prior close $10,198.37 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,198.37 | ▲ close $10,198.37 vs 09:30 $10,198.37 (session +0.00) | 16:00 close · cash $10,198.37 · no lots left · equity $10,198.37. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,198.37 | ▲ 09:30 equity $10,198.37 vs yday $10,198.37 (+0.00) | 09:30 open · cash $10,198.37 · no holdings · equity $10,198.37 vs prior close $10,198.37 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 77 | $26.27 | $2.22 | — | $8,173.36 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2039.67 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 10 | $189.17 | $2.02 | — | $6,279.64 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $2039.67 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 51 | $39.99 | $2.14 | — | $4,238.01 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2039.67 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `AMX` | 87 | $23.18 | $2.25 | — | $2,219.10 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list oppset; 🔵; ret5=-0.2; leftover $2039.67 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `AVTR` | 131 | $15.53 | $2.38 | — | $182.28 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; ret5=+4.9; leftover $2039.67 | join🟡 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟡 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $182.28 | ▼ close $10,068.44 vs 09:30 $10,198.37 (session -118.91) | 16:00 close · cash $182.28 · equity $10,068.44 vs 09:30 $10,198.37 (-129.93; session marks -118.91) · 5 name(s) marked open→close (per-name table). WAY×77 09:30 $26.27 → close $26.59 +24.64; QCOM×10 09:30 $189.17 → close $184.84 -43.30; SM×51 09:30 $39.99 → close $38.16 -93.33; AMX×87 09:30 $23.18 → close $22.98 -17.40; AVTR×131 09:30 $15.53 → close $15.61 +10.48 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $182.28 | ▲ 09:30 equity $10,123.06 vs yday $10,068.44 (+54.62) | 09:30 open · cash $182.28 (unchanged overnight, no fees) · equity $10,123.06 vs prior close $10,068.44 (+54.62) · 5 name(s) re-marked at the open (per-name table). WAY×77 yday $26.59 → 09:30 $26.51 -6.16; QCOM×10 yday $184.84 → 09:30 $190.35 +55.10; SM×51 yday $38.16 → 09:30 $37.57 -30.09; AMX×87 yday $22.98 → 09:30 $23.09 +9.57; AVTR×131 yday $15.61 → 09:30 $15.81 +26.20 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 77 | $26.51 | $2.25 | $+14.01 | $2,221.30 | ▲ +14.01 after sell → book $10,120.81; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 10 | $190.35 | $2.05 | $+7.73 | $4,122.76 | ▲ +7.73 after sell → book $10,118.77; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 51 | $37.57 | $2.17 | $-127.73 | $6,036.66 | ▼ -127.73 after sell → book $10,116.60; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AMX` | 87 | $23.09 | $2.28 | $-12.36 | $8,043.21 | ▼ -12.36 after sell → book $10,114.32; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 6 | $170.85 | $2.01 | — | $7,016.10 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1149.03 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 51 | $22.12 | $2.14 | — | $5,885.84 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1149.03 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 4 | $238.60 | $2.00 | — | $4,929.44 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_mover,oppset; ret5=-11.6; leftover $1149.03 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SRRK` | 23 | $49.52 | $2.06 | — | $3,788.42 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list oppset; 🔵; ret5=+1.1; leftover $1149.03 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 1 | $934.88 | $1.99 | — | $2,851.54 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; ret5=-7.0; leftover $1149.03 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ALVO` | 220 | $5.22 | $2.84 | — | $1,700.31 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list oppset; 🔵; ret5=-2.1; leftover $1149.03 | join🔴 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `KEY` | 54 | $20.98 | $2.15 | — | $565.23 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list oppset; 🔵; ret5=+0.8; leftover $1149.03 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $565.23 | ▲ close $10,147.67 vs 09:30 $10,123.06 (session +48.55) | 16:00 close · cash $565.23 · equity $10,147.67 vs 09:30 $10,123.06 (+24.61; session marks +48.55) · 8 name(s) marked open→close (per-name table). AVTR×131 09:30 $15.81 → close $15.86 +6.55; SMTC×6 09:30 $170.85 → close $178.19 +44.04; GME×51 09:30 $22.12 → close $22.77 +33.15; JBHT×4 09:30 $238.60 → close $236.80 -7.20; SRRK×23 09:30 $49.52 → close $49.02 -11.50; LITE×1 09:30 $934.88 → close $893.61 -41.27; ALVO×220 09:30 $5.22 → close $5.34 +26.40; KEY×54 09:30 $20.98 → close $20.95 -1.62 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $565.23 | ▲ 09:30 equity $10,190.00 vs yday $10,147.67 (+42.33) | 09:30 open · cash $565.23 (unchanged overnight, no fees) · equity $10,190.00 vs prior close $10,147.67 (+42.33) · 8 name(s) re-marked at the open (per-name table). AVTR×131 yday $15.86 → 09:30 $15.87 +1.31; SMTC×6 yday $178.19 → 09:30 $182.33 +24.84; GME×51 yday $22.77 → 09:30 $22.90 +6.63; JBHT×4 yday $236.80 → 09:30 $236.80 +0.00; SRRK×23 yday $49.02 → 09:30 $48.02 -23.00; LITE×1 yday $893.61 → 09:30 $915.66 +22.05; ALVO×220 yday $5.34 → 09:30 $5.40 +13.20; KEY×54 yday $20.95 → 09:30 $20.90 -2.70 | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 131 | $15.87 | $2.42 | $+39.74 | $2,641.78 | ▲ +39.74 after sell → book $10,187.58; vs 09:30 mark -2.42 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 6 | $182.33 | $2.03 | $+64.84 | $3,733.73 | ▲ +64.84 after sell → book $10,185.55; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 4 | $236.80 | $2.02 | $-11.22 | $4,678.91 | ▼ -11.22 after sell → book $10,183.53; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SRRK` | 23 | $48.02 | $2.08 | $-38.64 | $5,781.29 | ▼ -38.64 after sell → book $10,181.45; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 1 | $915.66 | $2.01 | $-23.23 | $6,694.94 | ▼ -23.23 after sell → book $10,179.44; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ALVO` | 220 | $5.40 | $2.88 | $+33.88 | $7,880.06 | ▲ +33.88 after sell → book $10,176.56; vs 09:30 mark -2.88 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `KEY` | 54 | $20.90 | $2.17 | $-8.64 | $9,006.48 | ▼ -8.64 after sell → book $10,174.38; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 143 | $20.91 | $2.42 | — | $6,013.94 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $3002.16 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 202 | $14.79 | $2.61 | — | $3,023.75 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $3002.16 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 213 | $14.07 | $2.75 | — | $24.09 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $3002.16 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.09 | ▼ close $10,040.98 vs 09:30 $10,190.00 (session -125.63) | 16:00 close · cash $24.09 · equity $10,040.98 vs 09:30 $10,190.00 (-149.02; session marks -125.63) · 4 name(s) marked open→close (per-name table). GME×51 09:30 $22.90 → close $22.64 -13.26; TH×143 09:30 $20.91 → close $21.19 +40.04; RARE×202 09:30 $14.79 → close $14.51 -56.56; BHVN×213 09:30 $14.07 → close $13.62 -95.85 | — |

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
| 2026-08-24 | `AMX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1331.94 < 1 share @ 1746.53 |
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
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MAMA` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FNV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CGNT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `QCOM` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CVE` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SRRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `AVTR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GME` | 51 | 2026-09-17 @ $22.12 | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1149.03 |
| `TH` | 143 | 2026-09-18 @ $20.91 | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $3002.16 |
| `RARE` | 202 | 2026-09-18 @ $14.79 | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $3002.16 |
| `BHVN` | 213 | 2026-09-18 @ $14.07 | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $3002.16 |
