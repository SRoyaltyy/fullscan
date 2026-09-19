# Factor mine action — `union_news_or_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢

Cash book **+2.01%** ($10,201) · signal-only (no cash/fees) was +18.15%. Starts YES **14/26**. Fills 160 · skips 65 · realized $+190.80.

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
- **Gate** `news_or_headline=True` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $3.16.

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
| 2026-08-17 | `DVN` | 43 | — | $46.18 | +0.00 | $47.57 | +59.77 | +59.77 | +0.00 | +59.77 |
| 2026-08-17 | `EOG` | 14 | — | $142.77 | +0.00 | $146.15 | +47.32 | +47.32 | +0.00 | +47.32 |
| 2026-08-17 | `FANG` | 10 | — | $202.70 | +0.00 | $206.29 | +35.90 | +35.90 | +0.00 | +35.90 |
| 2026-08-17 | `OUST` | 41 | — | $49.00 | +0.00 | $48.13 | -35.67 | -35.67 | +0.00 | -35.67 |
| 2026-08-17 | `CELC` | 21 | — | $92.99 | +0.00 | $92.44 | -11.55 | -11.55 | +0.00 | -11.55 |
| 2026-08-18 | `DVN` | 43 | $47.57 | $48.00 | +18.49 | — | +0.00 | +18.49 | +78.26 | — |
| 2026-08-18 | `EOG` | 14 | $146.15 | $148.04 | +26.46 | — | +0.00 | +26.46 | +73.78 | — |
| 2026-08-18 | `FANG` | 10 | $206.29 | $208.93 | +26.40 | — | +0.00 | +26.40 | +62.30 | — |
| 2026-08-18 | `OUST` | 41 | $48.13 | $45.09 | -124.64 | — | +0.00 | -124.64 | -160.31 | — |
| 2026-08-18 | `CELC` | 21 | $92.44 | $92.38 | -1.26 | — | +0.00 | -1.26 | -12.81 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `APA` | 28 | — | $44.76 | +0.00 | $44.39 | -10.36 | -10.36 | +0.00 | -10.36 |
| 2026-08-20 | `AUTL` | 514 | — | $2.47 | +0.00 | $2.46 | -5.14 | -5.14 | +0.00 | -5.14 |
| 2026-08-20 | `CRSP` | 21 | — | $58.73 | +0.00 | $58.12 | -12.81 | -12.81 | +0.00 | -12.81 |
| 2026-08-20 | `ASST` | 79 | — | $16.00 | +0.00 | $16.13 | +10.27 | +10.27 | +0.00 | +10.27 |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `ZLAB` | 47 | — | $26.57 | +0.00 | $26.02 | -25.85 | -25.85 | +0.00 | -25.85 |
| 2026-08-20 | `TEAM` | 7 | — | $173.90 | +0.00 | $174.91 | +7.07 | +7.07 | +0.00 | +7.07 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `APA` | 28 | $44.39 | $44.52 | +3.64 | — | +0.00 | +3.64 | -6.72 | — |
| 2026-08-21 | `AUTL` | 514 | $2.46 | $2.47 | +5.14 | $2.41 | -30.84 | -25.70 | +0.00 | -30.84 |
| 2026-08-21 | `CRSP` | 21 | $58.12 | $59.72 | +33.60 | $59.50 | -4.62 | +28.98 | +20.79 | +16.17 |
| 2026-08-21 | `ASST` | 79 | $16.13 | $17.66 | +120.87 | — | +0.00 | +120.87 | +131.14 | — |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | — | +0.00 | -1.68 | -136.24 | — |
| 2026-08-21 | `ZLAB` | 47 | $26.02 | $26.25 | +10.81 | — | +0.00 | +10.81 | -15.04 | — |
| 2026-08-21 | `TEAM` | 7 | $174.91 | $174.22 | -4.83 | — | +0.00 | -4.83 | +2.24 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `FUTU` | 11 | — | $115.18 | +0.00 | $123.64 | +93.06 | +93.06 | +0.00 | +93.06 |
| 2026-08-21 | `GRAL` | 16 | — | $78.88 | +0.00 | $79.54 | +10.56 | +10.56 | +0.00 | +10.56 |
| 2026-08-21 | `ABTC` | 147 | — | $8.66 | +0.00 | $7.93 | -107.31 | -107.31 | +0.00 | -107.31 |
| 2026-08-21 | `HIVE` | 393 | — | $3.24 | +0.00 | $3.03 | -82.53 | -82.53 | +0.00 | -82.53 |
| 2026-08-21 | `MARA` | 109 | — | $11.70 | +0.00 | $11.26 | -47.96 | -47.96 | +0.00 | -47.96 |
| 2026-08-24 | `AUTL` | 514 | $2.41 | $2.40 | -5.14 | — | +0.00 | -5.14 | -35.98 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | $57.08 | -35.17 | -50.92 | +0.42 | -34.75 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `FUTU` | 11 | $123.64 | $121.00 | -29.04 | — | +0.00 | -29.04 | +64.02 | — |
| 2026-08-24 | `GRAL` | 16 | $79.54 | $81.87 | +37.28 | — | +0.00 | +37.28 | +47.84 | — |
| 2026-08-24 | `ABTC` | 147 | $7.93 | $8.00 | +10.29 | — | +0.00 | +10.29 | -97.02 | — |
| 2026-08-24 | `HIVE` | 393 | $3.03 | $2.99 | -15.72 | — | +0.00 | -15.72 | -98.25 | — |
| 2026-08-24 | `MARA` | 109 | $11.26 | $11.17 | -9.81 | — | +0.00 | -9.81 | -57.77 | — |
| 2026-08-25 | `CRSP` | 21 | $57.08 | $57.93 | +17.95 | — | +0.00 | +17.95 | -16.80 | — |
| 2026-08-25 | `AU` | 11 | — | $118.52 | +0.00 | $123.39 | +53.57 | +53.57 | +0.00 | +53.57 |
| 2026-08-25 | `FCX` | 18 | — | $77.13 | +0.00 | $79.91 | +50.04 | +50.04 | +0.00 | +50.04 |
| 2026-08-25 | `EZPW` | 40 | — | $35.05 | +0.00 | $35.23 | +7.20 | +7.20 | +0.00 | +7.20 |
| 2026-08-25 | `RUM` | 150 | — | $9.42 | +0.00 | $10.23 | +121.50 | +121.50 | +0.00 | +121.50 |
| 2026-08-25 | `ZYME` | 49 | — | $28.86 | +0.00 | $27.47 | -68.11 | -68.11 | +0.00 | -68.11 |
| 2026-08-25 | `REAX` | 58 | — | $24.11 | +0.00 | $28.43 | +250.56 | +250.56 | +0.00 | +250.56 |
| 2026-08-25 | `EOLS` | 162 | — | $8.72 | +0.00 | $8.97 | +41.31 | +41.31 | +0.00 | +41.31 |
| 2026-08-26 | `AU` | 11 | $123.39 | $119.80 | -39.49 | — | +0.00 | -39.49 | +14.08 | — |
| 2026-08-26 | `FCX` | 18 | $79.91 | $79.34 | -10.26 | — | +0.00 | -10.26 | +39.78 | — |
| 2026-08-26 | `EZPW` | 40 | $35.23 | $35.70 | +18.80 | — | +0.00 | +18.80 | +26.00 | — |
| 2026-08-26 | `RUM` | 150 | $10.23 | $10.07 | -24.00 | — | +0.00 | -24.00 | +97.50 | — |
| 2026-08-26 | `ZYME` | 49 | $27.47 | $27.56 | +4.41 | — | +0.00 | +4.41 | -63.70 | — |
| 2026-08-26 | `REAX` | 58 | $28.43 | $26.61 | -105.56 | — | +0.00 | -105.56 | +145.00 | — |
| 2026-08-26 | `EOLS` | 162 | $8.97 | $8.86 | -18.63 | — | +0.00 | -18.63 | +22.68 | — |
| 2026-08-26 | `FNV` | 7 | — | $267.02 | +0.00 | $267.37 | +2.45 | +2.45 | +0.00 | +2.45 |
| 2026-08-26 | `TRLV` | 181 | — | $11.22 | +0.00 | $11.43 | +38.01 | +38.01 | +0.00 | +38.01 |
| 2026-08-26 | `CAPR` | 245 | — | $8.29 | +0.00 | $9.36 | +262.15 | +262.15 | +0.00 | +262.15 |
| 2026-08-26 | `FWRD` | 117 | — | $17.41 | +0.00 | $17.63 | +25.74 | +25.74 | +0.00 | +25.74 |
| 2026-08-26 | `FLNC` | 183 | — | $11.12 | +0.00 | $11.08 | -7.32 | -7.32 | +0.00 | -7.32 |
| 2026-08-27 | `FNV` | 7 | $267.37 | $267.23 | -0.98 | — | +0.00 | -0.98 | +1.47 | — |
| 2026-08-27 | `TRLV` | 181 | $11.43 | $11.38 | -9.05 | — | +0.00 | -9.05 | +28.96 | — |
| 2026-08-27 | `CAPR` | 245 | $9.36 | $9.19 | -41.65 | — | +0.00 | -41.65 | +220.50 | — |
| 2026-08-27 | `FWRD` | 117 | $17.63 | $17.60 | -3.51 | — | +0.00 | -3.51 | +22.23 | — |
| 2026-08-27 | `FLNC` | 183 | $11.08 | $11.52 | +80.52 | — | +0.00 | +80.52 | +73.20 | — |
| 2026-08-27 | `ACMR` | 21 | — | $81.65 | +0.00 | $80.49 | -24.36 | -24.36 | +0.00 | -24.36 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `ASML` | 1 | — | $1746.53 | +0.00 | $1735.01 | -11.52 | -11.52 | +0.00 | -11.52 |
| 2026-08-27 | `LRCX` | 5 | — | $318.88 | +0.00 | $318.58 | -1.50 | -1.50 | +0.00 | -1.50 |
| 2026-08-27 | `NVDA` | 7 | — | $222.86 | +0.00 | $227.98 | +35.84 | +35.84 | +0.00 | +35.84 |
| 2026-08-27 | `RRC` | 42 | — | $41.44 | +0.00 | $41.64 | +8.40 | +8.40 | +0.00 | +8.40 |
| 2026-08-28 | `ACMR` | 21 | $80.49 | $79.27 | -25.62 | — | +0.00 | -25.62 | -49.98 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `ASML` | 1 | $1735.01 | $1734.75 | -0.26 | — | +0.00 | -0.26 | -11.78 | — |
| 2026-08-28 | `LRCX` | 5 | $318.58 | $318.03 | -2.75 | — | +0.00 | -2.75 | -4.25 | — |
| 2026-08-28 | `NVDA` | 7 | $227.98 | $227.36 | -4.34 | — | +0.00 | -4.34 | +31.50 | — |
| 2026-08-28 | `RRC` | 42 | $41.64 | $41.74 | +4.20 | — | +0.00 | +4.20 | +12.60 | — |
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
| 2026-09-03 | `AVGO` | 3 | — | $351.74 | +0.00 | $357.16 | +16.26 | +16.26 | +0.00 | +16.26 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `CXW` | 39 | — | $32.31 | +0.00 | $33.66 | +52.65 | +52.65 | +0.00 | +52.65 |
| 2026-09-03 | `FRNM` | 79 | — | $15.87 | +0.00 | $16.90 | +81.37 | +81.37 | +0.00 | +81.37 |
| 2026-09-03 | `MMED` | 52 | — | $23.88 | +0.00 | $23.84 | -2.08 | -2.08 | +0.00 | -2.08 |
| 2026-09-03 | `DE` | 1 | — | $703.25 | +0.00 | $694.41 | -8.84 | -8.84 | +0.00 | -8.84 |
| 2026-09-03 | `HPE` | 26 | — | $47.60 | +0.00 | $54.44 | +177.84 | +177.84 | +0.00 | +177.84 |
| 2026-09-03 | `CNXC` | 38 | — | $32.88 | +0.00 | $32.85 | -1.14 | -1.14 | +0.00 | -1.14 |
| 2026-09-04 | `AVGO` | 3 | $357.16 | $359.70 | +7.62 | — | +0.00 | +7.62 | +23.88 | — |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | — | +0.00 | -5.22 | +54.94 | — |
| 2026-09-04 | `CXW` | 39 | $33.66 | $33.46 | -7.80 | — | +0.00 | -7.80 | +44.85 | — |
| 2026-09-04 | `FRNM` | 79 | $16.90 | $16.40 | -39.50 | $16.31 | -7.11 | -46.61 | +41.87 | +34.76 |
| 2026-09-04 | `MMED` | 52 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.08 | — |
| 2026-09-04 | `DE` | 1 | $694.41 | $692.03 | -2.38 | — | +0.00 | -2.38 | -11.22 | — |
| 2026-09-04 | `HPE` | 26 | $54.44 | $53.85 | -15.34 | — | +0.00 | -15.34 | +162.50 | — |
| 2026-09-04 | `CNXC` | 38 | $32.85 | $32.48 | -14.06 | — | +0.00 | -14.06 | -15.20 | — |
| 2026-09-04 | `CRM` | 6 | — | $263.36 | +0.00 | $259.23 | -24.78 | -24.78 | +0.00 | -24.78 |
| 2026-09-04 | `MRX` | 24 | — | $75.65 | +0.00 | $78.27 | +62.88 | +62.88 | +0.00 | +62.88 |
| 2026-09-04 | `BE` | 7 | — | $236.82 | +0.00 | $252.87 | +112.35 | +112.35 | +0.00 | +112.35 |
| 2026-09-04 | `BAK` | 936 | — | $1.94 | +0.00 | $1.89 | -46.80 | -46.80 | +0.00 | -46.80 |
| 2026-09-04 | `MSTR` | 13 | — | $137.35 | +0.00 | $142.80 | +70.85 | +70.85 | +0.00 | +70.85 |
| 2026-09-08 | `FRNM` | 79 | $16.31 | $16.74 | +33.97 | — | +0.00 | +33.97 | +68.73 | — |
| 2026-09-08 | `CRM` | 6 | $259.23 | $253.72 | -33.06 | — | +0.00 | -33.06 | -57.84 | — |
| 2026-09-08 | `MRX` | 24 | $78.27 | $78.84 | +13.68 | $76.71 | -51.12 | -37.44 | +76.56 | +25.44 |
| 2026-09-08 | `BE` | 7 | $252.87 | $267.76 | +104.23 | — | +0.00 | +104.23 | +216.58 | — |
| 2026-09-08 | `BAK` | 936 | $1.89 | $1.94 | +46.80 | — | +0.00 | +46.80 | +0.00 | — |
| 2026-09-08 | `MSTR` | 13 | $142.80 | $137.62 | -67.34 | $136.52 | -14.30 | -81.64 | +3.51 | -10.79 |
| 2026-09-09 | `MRX` | 24 | $76.71 | $76.60 | -2.64 | — | +0.00 | -2.64 | +22.80 | — |
| 2026-09-09 | `MSTR` | 13 | $136.52 | $141.82 | +68.90 | — | +0.00 | +68.90 | +58.11 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 12 | — | $164.43 | +0.00 | $150.28 | -169.80 | -169.80 | +0.00 | -169.80 |
| 2026-09-11 | `ADBE` | 8 | — | $242.17 | +0.00 | $252.23 | +80.48 | +80.48 | +0.00 | +80.48 |
| 2026-09-11 | `BAK` | 1000 | — | $2.12 | +0.00 | $2.08 | -40.00 | -40.00 | +0.00 | -40.00 |
| 2026-09-11 | `AMTX` | 1039 | — | $2.04 | +0.00 | $2.01 | -31.17 | -31.17 | +0.00 | -31.17 |
| 2026-09-11 | `RH` | 15 | — | $135.71 | +0.00 | $134.07 | -24.60 | -24.60 | +0.00 | -24.60 |
| 2026-09-14 | `ORCL` | 12 | $150.28 | $141.42 | -106.32 | — | +0.00 | -106.32 | -276.12 | — |
| 2026-09-14 | `ADBE` | 8 | $252.23 | $261.51 | +74.24 | — | +0.00 | +74.24 | +154.72 | — |
| 2026-09-14 | `BAK` | 1000 | $2.08 | $2.05 | -30.00 | — | +0.00 | -30.00 | -70.00 | — |
| 2026-09-14 | `AMTX` | 1039 | $2.01 | $2.01 | +0.00 | — | +0.00 | +0.00 | -31.17 | — |
| 2026-09-14 | `RH` | 15 | $134.07 | $131.40 | -40.05 | — | +0.00 | -40.05 | -64.65 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 97 | — | $26.27 | +0.00 | $26.59 | +31.04 | +31.04 | +0.00 | +31.04 |
| 2026-09-16 | `SM` | 64 | — | $39.99 | +0.00 | $38.16 | -117.12 | -117.12 | +0.00 | -117.12 |
| 2026-09-16 | `CLS` | 8 | — | $320.20 | +0.00 | $323.83 | +29.04 | +29.04 | +0.00 | +29.04 |
| 2026-09-16 | `SION` | 368 | — | $6.95 | +0.00 | $7.04 | +33.12 | +33.12 | +0.00 | +33.12 |
| 2026-09-17 | `WAY` | 97 | $26.59 | $26.51 | -7.76 | — | +0.00 | -7.76 | +23.28 | — |
| 2026-09-17 | `SM` | 64 | $38.16 | $37.57 | -37.76 | — | +0.00 | -37.76 | -154.88 | — |
| 2026-09-17 | `CLS` | 8 | $323.83 | $337.75 | +111.36 | $329.94 | -62.48 | +48.88 | +140.40 | +77.92 |
| 2026-09-17 | `SION` | 368 | $7.04 | $7.27 | +84.64 | — | +0.00 | +84.64 | +117.76 | — |
| 2026-09-17 | `SMTC` | 7 | — | $170.85 | +0.00 | $178.19 | +51.38 | +51.38 | +0.00 | +51.38 |
| 2026-09-17 | `GME` | 57 | — | $22.12 | +0.00 | $22.77 | +37.05 | +37.05 | +0.00 | +37.05 |
| 2026-09-17 | `JBHT` | 5 | — | $238.60 | +0.00 | $236.80 | -9.00 | -9.00 | +0.00 | -9.00 |
| 2026-09-17 | `LITE` | 1 | — | $934.88 | +0.00 | $893.61 | -41.27 | -41.27 | +0.00 | -41.27 |
| 2026-09-17 | `TNDM` | 71 | — | $17.72 | +0.00 | $17.23 | -34.79 | -34.79 | +0.00 | -34.79 |
| 2026-09-17 | `BAK` | 720 | — | $1.77 | +0.00 | $1.79 | +14.40 | +14.40 | +0.00 | +14.40 |
| 2026-09-18 | `CLS` | 8 | $329.94 | $332.06 | +16.96 | $332.63 | +4.56 | +21.52 | +94.88 | +99.44 |
| 2026-09-18 | `SMTC` | 7 | $178.19 | $182.33 | +28.98 | — | +0.00 | +28.98 | +80.36 | — |
| 2026-09-18 | `GME` | 57 | $22.77 | $22.90 | +7.41 | $22.64 | -14.82 | -7.41 | +44.46 | +29.64 |
| 2026-09-18 | `JBHT` | 5 | $236.80 | $236.80 | +0.00 | — | +0.00 | +0.00 | -9.00 | — |
| 2026-09-18 | `LITE` | 1 | $893.61 | $915.66 | +22.05 | — | +0.00 | +22.05 | -19.22 | — |
| 2026-09-18 | `TNDM` | 71 | $17.23 | $17.13 | -7.10 | — | +0.00 | -7.10 | -41.89 | — |
| 2026-09-18 | `BAK` | 720 | $1.79 | $1.77 | -14.40 | — | +0.00 | -14.40 | +0.00 | — |
| 2026-09-18 | `TH` | 76 | — | $20.91 | +0.00 | $21.19 | +21.28 | +21.28 | +0.00 | +21.28 |
| 2026-09-18 | `RARE` | 107 | — | $14.79 | +0.00 | $14.51 | -29.96 | -29.96 | +0.00 | -29.96 |
| 2026-09-18 | `BHVN` | 113 | — | $14.07 | +0.00 | $13.62 | -50.85 | -50.85 | +0.00 | -50.85 |
| 2026-09-18 | `FLNC` | 211 | — | $7.54 | +0.00 | $7.32 | -45.36 | -45.36 | +0.00 | -45.36 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +137.19 | HLIT, ANGX, MH, VELO, ARX, NRG, S | — | $1,332.73 | $10,120.33 | HLIT×94, ANGX×290, MH×92, VELO×81, ARX×63, NRG×10, S×52 |
| 2026-08-17 | +2.25 | $1,332.73 | HLIT×94, ANGX×290, MH×92, VELO×81, ARX×63, NRG×10, S×52 | $10,155.37 | +35.04 | +95.77 | DVN, EOG, FANG, OUST, CELC | HLIT, ANGX, MH, VELO, ARX, NRG, S | $154.67 | $10,223.75 | DVN×43, EOG×14, FANG×10, OUST×41, CELC×21 |
| 2026-08-18 | -6.20 | $154.67 | DVN×43, EOG×14, FANG×10, OUST×41, CELC×21 | $10,169.20 | -54.55 | +0.00 | — | DVN, EOG, FANG, OUST, CELC | $10,158.74 | $10,158.74 | — |
| 2026-08-19 | -7.20 | $10,158.74 | — | $10,158.74 | -0.00 | +0.00 | — | — | $10,158.74 | $10,158.74 | — |
| 2026-08-20 | +1.12 | $10,158.74 | — | $10,158.74 | -0.00 | -137.32 | BHP, APA, AUTL, CRSP, ASST, MRNA, ZLAB, TEAM | — | $267.04 | $10,000.25 | BHP×13, APA×28, AUTL×514, CRSP×21, ASST×79, MRNA×8, ZLAB×47, TEAM×7 |
| 2026-08-21 | +3.25 | $267.04 | BHP×13, APA×28, AUTL×514, CRSP×21, ASST×79, MRNA×8, ZLAB×47, TEAM×7 | $10,194.97 | +194.72 | -151.74 | AU, FUTU, GRAL, ABTC, HIVE, MARA | BHP, APA, ASST, MRNA, ZLAB, TEAM | $97.76 | $10,014.72 | AUTL×514, CRSP×21, AU×10, FUTU×11, GRAL×16, ABTC×147, HIVE×393, MARA×109 |
| 2026-08-24 | -5.17 | $97.76 | AUTL×514, CRSP×21, AU×10, FUTU×11, GRAL×16, ABTC×147, HIVE×393, MARA×109 | $9,979.73 | -34.99 | -35.17 | — | AUTL, AU, FUTU, GRAL, ABTC, HIVE, MARA | $8,723.16 | $9,921.73 | CRSP×21 |
| 2026-08-25 | +1.80 | $8,723.16 | CRSP×21 | $9,939.69 | +17.96 | +456.07 | AU, FCX, EZPW, RUM, ZYME, REAX, EOLS | CRSP | $190.00 | $10,378.29 | AU×11, FCX×18, EZPW×40, RUM×150, ZYME×49, REAX×58, EOLS×162 |
| 2026-08-26 | +2.02 | $190.00 | AU×11, FCX×18, EZPW×40, RUM×150, ZYME×49, REAX×58, EOLS×162 | $10,203.56 | -174.73 | +321.03 | FNV, TRLV, CAPR, FWRD, FLNC | AU, FCX, EZPW, RUM, ZYME, REAX, EOLS | $172.46 | $10,496.43 | FNV×7, TRLV×181, CAPR×245, FWRD×117, FLNC×183 |
| 2026-08-27 | — | $172.46 | FNV×7, TRLV×181, CAPR×245, FWRD×117, FLNC×183 | $10,521.76 | +25.33 | -24.76 | ACMR, MU, ASML, LRCX, NVDA, RRC | FNV, TRLV, CAPR, FWRD, FLNC | $1,173.70 | $10,472.03 | ACMR×21, MU×1, ASML×1, LRCX×5, NVDA×7, RRC×42 |
| 2026-08-28 | +0.75 | $1,173.70 | ACMR×21, MU×1, ASML×1, LRCX×5, NVDA×7, RRC×42 | $10,427.16 | -44.87 | -263.42 | KEYS, SMTC, CIEN, DDOG, ADSK, SEDG, TLS | ACMR, MU, ASML, LRCX, NVDA, RRC | $1,794.26 | $10,135.82 | KEYS×4, SMTC×9, CIEN×3, DDOG×5, ADSK×4, SEDG×39, TLS×270 |
| 2026-08-31 | -5.85 | $1,794.26 | KEYS×4, SMTC×9, CIEN×3, DDOG×5, ADSK×4, SEDG×39, TLS×270 | $10,124.45 | -11.37 | +0.00 | — | KEYS, SMTC, CIEN, DDOG, ADSK, SEDG, TLS | $10,108.66 | $10,108.66 | — |
| 2026-09-01 | -6.30 | $10,108.66 | — | $10,108.66 | +0.00 | +0.00 | — | — | $10,108.66 | $10,108.66 | — |
| 2026-09-02 | -3.83 | $10,108.66 | — | $10,108.66 | +0.00 | +0.00 | — | — | $10,108.66 | $10,108.66 | — |
| 2026-09-03 | -0.90 | $10,108.66 | — | $10,108.66 | +0.00 | +376.22 | AVGO, DELL, CXW, FRNM, MMED, DE, HPE, CNXC | — | $1,118.31 | $10,468.24 | AVGO×3, DELL×2, CXW×39, FRNM×79, MMED×52, DE×1, HPE×26, CNXC×38 |
| 2026-09-04 | +2.25 | $1,118.31 | AVGO×3, DELL×2, CXW×39, FRNM×79, MMED×52, DE×1, HPE×26, CNXC×38 | $10,391.56 | -76.68 | +167.39 | CRM, MRX, BE, BAK, MSTR | AVGO, DELL, CXW, MMED, DE, HPE, CNXC | $406.33 | $10,524.21 | FRNM×79, CRM×6, MRX×24, BE×7, BAK×936, MSTR×13 |
| 2026-09-08 | -11.47 | $406.33 | FRNM×79, CRM×6, MRX×24, BE×7, BAK×936, MSTR×13 | $10,622.49 | +98.28 | -65.42 | — | FRNM, CRM, BE, BAK | $6,922.71 | $10,538.51 | MRX×24, MSTR×13 |
| 2026-09-09 | -13.95 | $6,922.71 | MRX×24, MSTR×13 | $10,604.77 | +66.26 | +0.00 | — | MRX, MSTR | $10,600.63 | $10,600.63 | — |
| 2026-09-10 | -13.28 | $10,600.63 | — | $10,600.63 | +0.00 | +0.00 | — | — | $10,600.63 | $10,600.63 | — |
| 2026-09-11 | +0.50 | $10,600.63 | — | $10,600.63 | +0.00 | -185.09 | ORCL, ADBE, BAK, AMTX, RH | — | $382.52 | $10,383.16 | ORCL×12, ADBE×8, BAK×1000, AMTX×1039, RH×15 |
| 2026-09-14 | -11.00 | $382.52 | ORCL×12, ADBE×8, BAK×1000, AMTX×1039, RH×15 | $10,281.03 | -102.13 | +0.00 | — | ORCL, ADBE, BAK, AMTX, RH | $10,248.21 | $10,248.21 | — |
| 2026-09-15 | -3.84 | $10,248.21 | — | $10,248.21 | -0.00 | +0.00 | — | — | $10,248.21 | $10,248.21 | — |
| 2026-09-16 | +5.30 | $10,248.21 | — | $10,248.21 | -0.00 | -23.92 | WAY, SM, CLS, SION | — | $10.23 | $10,213.06 | WAY×97, SM×64, CLS×8, SION×368 |
| 2026-09-17 | +7.38 | $10.23 | WAY×97, SM×64, CLS×8, SION×368 | $10,363.54 | +150.48 | -44.71 | SMTC, GME, JBHT, LITE, TNDM, BAK | WAY, SM, SION | $515.33 | $10,289.81 | CLS×8, SMTC×7, GME×57, JBHT×5, LITE×1, TNDM×71, BAK×720 |
| 2026-09-18 | +4.86 | $515.33 | CLS×8, SMTC×7, GME×57, JBHT×5, LITE×1, TNDM×71, BAK×720 | $10,343.71 | +53.90 | -115.15 | TH, RARE, BHVN, FLNC | SMTC, JBHT, LITE, TNDM, BAK | $3.16 | $10,201.27 | CLS×8, GME×57, TH×76, RARE×107, BHVN×113, FLNC×211 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $8,758.81 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $7,505.17 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $6,256.30 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 81 | $15.38 | $2.23 | — | $5,008.29 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $3,773.20 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $2,571.18 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `S` | 52 | $23.77 | $2.15 | — | $1,332.73 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,332.73 | ▲ close $10,120.33 vs 09:30 $10,000.00 (session +137.19) | 16:00 close · cash $1,332.73 · equity $10,120.33 vs 09:30 $10,000.00 (+120.33; session marks +137.19) · 7 name(s) marked open→close (per-name table). HLIT×94 09:30 $13.18 → close $13.92 +69.56; ANGX×290 09:30 $4.31 → close $4.37 +17.40; MH×92 09:30 $13.55 → close $13.10 -41.40; VELO×81 09:30 $15.38 → close $16.16 +63.18; ARX×63 09:30 $19.57 → close $19.58 +0.63; NRG×10 09:30 $120.00 → close $126.24 +62.40; S×52 09:30 $23.77 → close $23.11 -34.58 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,332.73 | ▲ 09:30 equity $10,155.37 vs yday $10,120.33 (+35.04) | 09:30 open · cash $1,332.73 (unchanged overnight, no fees) · equity $10,155.37 vs prior close $10,120.33 (+35.04) · 7 name(s) re-marked at the open (per-name table). HLIT×94 yday $13.92 → 09:30 $13.84 -7.52; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; MH×92 yday $13.10 → 09:30 $13.16 +5.52; VELO×81 yday $16.16 → 09:30 $16.05 -8.91; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; S×52 yday $23.11 → 09:30 $22.50 -31.72 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $2,631.40 | ▲ +57.47 after sell → book $10,153.08; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,961.60 | ▲ +76.56 after sell → book $10,149.28; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $5,170.02 | ▼ -40.44 after sell → book $10,146.98; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VELO` | 81 | $16.05 | $2.26 | $+49.78 | $6,467.82 | ▲ +49.78 after sell → book $10,144.73; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $7,698.53 | ▼ -4.38 after sell → book $10,142.53; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $8,970.49 | ▲ +69.94 after sell → book $10,140.49; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `S` | 52 | $22.50 | $2.17 | $-70.61 | $10,138.32 | ▼ -70.61 after sell → book $10,138.32; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 43 | $46.18 | $2.12 | — | $8,150.46 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ret5=+6.7; leftover $2027.66 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 14 | $142.77 | $2.03 | — | $6,149.65 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ret5=+5.8; leftover $2027.66 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 10 | $202.70 | $2.02 | — | $4,120.63 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ret5=+8.3; leftover $2027.66 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 41 | $49.00 | $2.11 | — | $2,109.52 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $2027.66 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 21 | $92.99 | $2.05 | — | $154.67 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer; ret5=-0.8; leftover $2027.66 | join🟡 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $154.67 | ▲ close $10,223.75 vs 09:30 $10,155.37 (session +95.77) | 16:00 close · cash $154.67 · equity $10,223.75 vs 09:30 $10,155.37 (+68.38; session marks +95.77) · 5 name(s) marked open→close (per-name table). DVN×43 09:30 $46.18 → close $47.57 +59.77; EOG×14 09:30 $142.77 → close $146.15 +47.32; FANG×10 09:30 $202.70 → close $206.29 +35.90; OUST×41 09:30 $49.00 → close $48.13 -35.67; CELC×21 09:30 $92.99 → close $92.44 -11.55 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.67 | ▼ 09:30 equity $10,169.20 vs yday $10,223.75 (-54.55) | 09:30 open · cash $154.67 (unchanged overnight, no fees) · equity $10,169.20 vs prior close $10,223.75 (-54.55) · 5 name(s) re-marked at the open (per-name table). DVN×43 yday $47.57 → 09:30 $48.00 +18.49; EOG×14 yday $146.15 → 09:30 $148.04 +26.46; FANG×10 yday $206.29 → 09:30 $208.93 +26.40; OUST×41 yday $48.13 → 09:30 $45.09 -124.64; CELC×21 yday $92.44 → 09:30 $92.38 -1.26 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 43 | $48.00 | $2.15 | $+74.00 | $2,216.53 | ▲ +74.00 after sell → book $10,167.06; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 14 | $148.04 | $2.06 | $+69.69 | $4,287.03 | ▲ +69.69 after sell → book $10,165.00; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 10 | $208.93 | $2.05 | $+58.23 | $6,374.28 | ▲ +58.23 after sell → book $10,162.95; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 41 | $45.09 | $2.14 | $-164.56 | $8,220.84 | ▼ -164.56 after sell → book $10,160.82; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 21 | $92.38 | $2.08 | $-16.94 | $10,158.74 | ▼ -16.94 after sell → book $10,158.74; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,158.74 | ▲ close $10,158.74 vs 09:30 $10,169.20 (session +0.00) | 16:00 close · cash $10,158.74 · no lots left · equity $10,158.74. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,158.74 | ▲ 09:30 equity $10,158.74 vs yday $10,158.74 (-0.00) | 09:30 open · cash $10,158.74 · no holdings · equity $10,158.74 vs prior close $10,158.74 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,158.74 | ▲ close $10,158.74 vs 09:30 $10,158.74 (session +0.00) | 16:00 close · cash $10,158.74 · no lots left · equity $10,158.74. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,158.74 | ▲ 09:30 equity $10,158.74 vs yday $10,158.74 (-0.00) | 09:30 open · cash $10,158.74 · no holdings · equity $10,158.74 vs prior close $10,158.74 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,973.58 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1269.84 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $7,718.22 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1269.84 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 514 | $2.47 | $6.63 | — | $6,442.01 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1269.84 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $5,206.63 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1269.84 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 79 | $16.00 | $2.23 | — | $3,940.40 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1269.84 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $2,737.27 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1269.84 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 47 | $26.57 | $2.13 | — | $1,486.35 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1269.84 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TEAM` | 7 | $173.90 | $2.01 | — | $267.04 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+12.2; leftover $1269.84 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $267.04 | ▼ close $10,000.25 vs 09:30 $10,158.74 (session -137.32) | 16:00 close · cash $267.04 · equity $10,000.25 vs 09:30 $10,158.74 (-158.49; session marks -137.32) · 8 name(s) marked open→close (per-name table). BHP×13 09:30 $91.01 → close $93.63 +34.06; APA×28 09:30 $44.76 → close $44.39 -10.36; AUTL×514 09:30 $2.47 → close $2.46 -5.14; CRSP×21 09:30 $58.73 → close $58.12 -12.81; ASST×79 09:30 $16.00 → close $16.13 +10.27; MRNA×8 09:30 $150.14 → close $133.32 -134.56; ZLAB×47 09:30 $26.57 → close $26.02 -25.85; TEAM×7 09:30 $173.90 → close $174.91 +7.07 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $267.04 | ▲ 09:30 equity $10,194.97 vs yday $10,000.25 (+194.72) | 09:30 open · cash $267.04 (unchanged overnight, no fees) · equity $10,194.97 vs prior close $10,000.25 (+194.72) · 8 name(s) re-marked at the open (per-name table). BHP×13 yday $93.63 → 09:30 $95.72 +27.17; APA×28 yday $44.39 → 09:30 $44.52 +3.64; AUTL×514 yday $2.46 → 09:30 $2.47 +5.14; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; ASST×79 yday $16.13 → 09:30 $17.66 +120.87; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; ZLAB×47 yday $26.02 → 09:30 $26.25 +10.81; TEAM×7 yday $174.91 → 09:30 $174.22 -4.83 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,509.35 | ▲ +57.15 after sell → book $10,192.92; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $2,753.81 | ▼ -10.89 after sell → book $10,190.82; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 79 | $17.66 | $2.25 | $+126.66 | $4,146.70 | ▲ +126.66 after sell → book $10,188.57; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $5,209.55 | ▼ -140.29 after sell → book $10,186.54; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 47 | $26.25 | $2.15 | $-19.32 | $6,441.15 | ▼ -19.32 after sell → book $10,184.39; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TEAM` | 7 | $174.22 | $2.03 | $-1.80 | $7,658.66 | ▼ -1.80 after sell → book $10,182.36; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,462.34 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1276.44 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $5,193.33 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1276.44 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 16 | $78.88 | $2.04 | — | $3,929.22 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1276.44 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 147 | $8.66 | $2.43 | — | $2,653.77 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1276.44 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 393 | $3.24 | $5.07 | — | $1,375.38 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $1276.44 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 109 | $11.70 | $2.32 | — | $97.76 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1276.44 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.76 | ▼ close $10,014.72 vs 09:30 $10,194.97 (session -151.74) | 16:00 close · cash $97.76 · equity $10,014.72 vs 09:30 $10,194.97 (-180.25; session marks -151.74) · 8 name(s) marked open→close (per-name table). AUTL×514 09:30 $2.47 → close $2.41 -30.84; CRSP×21 09:30 $59.72 → close $59.50 -4.62; AU×10 09:30 $119.43 → close $121.22 +17.90; FUTU×11 09:30 $115.18 → close $123.64 +93.06; GRAL×16 09:30 $78.88 → close $79.54 +10.56; ABTC×147 09:30 $8.66 → close $7.93 -107.31; HIVE×393 09:30 $3.24 → close $3.03 -82.53; MARA×109 09:30 $11.70 → close $11.26 -47.96 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.76 | ▼ 09:30 equity $9,979.73 vs yday $10,014.72 (-34.99) | 09:30 open · cash $97.76 (unchanged overnight, no fees) · equity $9,979.73 vs prior close $10,014.72 (-34.99) · 8 name(s) re-marked at the open (per-name table). AUTL×514 yday $2.41 → 09:30 $2.40 -5.14; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; AU×10 yday $121.22 → 09:30 $120.51 -7.10; FUTU×11 yday $123.64 → 09:30 $121.00 -29.04; GRAL×16 yday $79.54 → 09:30 $81.87 +37.28; ABTC×147 yday $7.93 → 09:30 $8.00 +10.29; HIVE×393 yday $3.03 → 09:30 $2.99 -15.72; MARA×109 yday $11.26 → 09:30 $11.17 -9.81 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 514 | $2.40 | $6.73 | $-49.34 | $1,324.63 | ▼ -49.34 after sell → book $9,973.00; vs 09:30 mark -6.73 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $2,527.69 | ▲ +6.74 after sell → book $9,970.96; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $121.00 | $2.04 | $+59.95 | $3,856.65 | ▲ +59.95 after sell → book $9,968.92; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 16 | $81.87 | $2.06 | $+43.74 | $5,164.51 | ▲ +43.74 after sell → book $9,966.86; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 147 | $8.00 | $2.47 | $-101.92 | $6,338.05 | ▼ -101.92 after sell → book $9,964.40; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 393 | $2.99 | $5.14 | $-108.46 | $7,507.97 | ▼ -108.46 after sell → book $9,959.25; vs 09:30 mark -5.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 109 | $11.17 | $2.35 | $-62.43 | $8,723.16 | ▼ -62.43 after sell → book $9,956.91; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,723.16 | ▼ close $9,921.73 vs 09:30 $9,979.73 (session -35.17) | 16:00 close · cash $8,723.16 · equity $9,921.73 vs 09:30 $9,979.73 (-58.00; session marks -35.17) · 1 name(s) marked open→close (per-name table). CRSP×21 09:30 $58.75 → close $57.08 -35.17 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,723.16 | ▲ 09:30 equity $9,939.69 vs yday $9,921.73 (+17.96) | 09:30 open · cash $8,723.16 (unchanged overnight, no fees) · equity $9,939.69 vs prior close $9,921.73 (+17.96) · 1 name(s) re-marked at the open (per-name table). CRSP×21 yday $57.08 → 09:30 $57.93 +17.95 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-20.93 | $9,937.61 | ▼ -20.93 after sell → book $9,937.61; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 11 | $118.52 | $2.02 | — | $8,631.87 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1419.66 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 18 | $77.13 | $2.04 | — | $7,241.49 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1419.66 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 40 | $35.05 | $2.11 | — | $5,837.38 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1419.66 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 150 | $9.42 | $2.44 | — | $4,421.94 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1419.66 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 49 | $28.86 | $2.14 | — | $3,005.66 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+13.7; leftover $1419.66 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 58 | $24.11 | $2.16 | — | $1,605.11 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; ret5=+891.7; leftover $1419.66 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 162 | $8.72 | $2.48 | — | $190.00 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+13.0; leftover $1419.66 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $190.00 | ▲ close $10,378.29 vs 09:30 $9,939.69 (session +456.07) | 16:00 close · cash $190.00 · equity $10,378.29 vs 09:30 $9,939.69 (+438.60; session marks +456.07) · 7 name(s) marked open→close (per-name table). AU×11 09:30 $118.52 → close $123.39 +53.57; FCX×18 09:30 $77.13 → close $79.91 +50.04; EZPW×40 09:30 $35.05 → close $35.23 +7.20; RUM×150 09:30 $9.42 → close $10.23 +121.50; ZYME×49 09:30 $28.86 → close $27.47 -68.11; REAX×58 09:30 $24.11 → close $28.43 +250.56; EOLS×162 09:30 $8.72 → close $8.97 +41.31 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $190.00 | ▼ 09:30 equity $10,203.56 vs yday $10,378.29 (-174.73) | 09:30 open · cash $190.00 (unchanged overnight, no fees) · equity $10,203.56 vs prior close $10,378.29 (-174.73) · 7 name(s) re-marked at the open (per-name table). AU×11 yday $123.39 → 09:30 $119.80 -39.49; FCX×18 yday $79.91 → 09:30 $79.34 -10.26; EZPW×40 yday $35.23 → 09:30 $35.70 +18.80; RUM×150 yday $10.23 → 09:30 $10.07 -24.00; ZYME×49 yday $27.47 → 09:30 $27.56 +4.41; REAX×58 yday $28.43 → 09:30 $26.61 -105.56; EOLS×162 yday $8.97 → 09:30 $8.86 -18.63 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 11 | $119.80 | $2.04 | $+10.01 | $1,505.75 | ▲ +10.01 after sell → book $10,201.51; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 18 | $79.34 | $2.07 | $+35.67 | $2,931.81 | ▲ +35.67 after sell → book $10,199.45; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 40 | $35.70 | $2.13 | $+21.76 | $4,357.68 | ▲ +21.76 after sell → book $10,197.32; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 150 | $10.07 | $2.48 | $+92.58 | $5,865.70 | ▲ +92.58 after sell → book $10,194.84; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 49 | $27.56 | $2.16 | $-67.99 | $7,213.98 | ▼ -67.99 after sell → book $10,192.68; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 58 | $26.61 | $2.19 | $+140.65 | $8,755.18 | ▲ +140.65 after sell → book $10,190.50; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 162 | $8.86 | $2.51 | $+17.69 | $10,187.98 | ▲ +17.69 after sell → book $10,187.98; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 7 | $267.02 | $2.01 | — | $8,316.83 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $2037.60 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 181 | $11.22 | $2.53 | — | $6,283.48 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+16.8; leftover $2037.60 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 245 | $8.29 | $3.16 | — | $4,249.27 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $2037.60 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 117 | $17.41 | $2.34 | — | $2,209.96 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; ret5=-9.2; leftover $2037.60 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 183 | $11.12 | $2.54 | — | $172.46 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $2037.60 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $172.46 | ▲ close $10,496.43 vs 09:30 $10,203.56 (session +321.03) | 16:00 close · cash $172.46 · equity $10,496.43 vs 09:30 $10,203.56 (+292.87; session marks +321.03) · 5 name(s) marked open→close (per-name table). FNV×7 09:30 $267.02 → close $267.37 +2.45; TRLV×181 09:30 $11.22 → close $11.43 +38.01; CAPR×245 09:30 $8.29 → close $9.36 +262.15; FWRD×117 09:30 $17.41 → close $17.63 +25.74; FLNC×183 09:30 $11.12 → close $11.08 -7.32 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $172.46 | ▲ 09:30 equity $10,521.76 vs yday $10,496.43 (+25.33) | 09:30 open · cash $172.46 (unchanged overnight, no fees) · equity $10,521.76 vs prior close $10,496.43 (+25.33) · 5 name(s) re-marked at the open (per-name table). FNV×7 yday $267.37 → 09:30 $267.23 -0.98; TRLV×181 yday $11.43 → 09:30 $11.38 -9.05; CAPR×245 yday $9.36 → 09:30 $9.19 -41.65; FWRD×117 yday $17.63 → 09:30 $17.60 -3.51; FLNC×183 yday $11.08 → 09:30 $11.52 +80.52 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 7 | $267.23 | $2.04 | $-2.58 | $2,041.03 | ▼ -2.58 after sell → book $10,519.72; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 181 | $11.38 | $2.58 | $+23.85 | $4,098.23 | ▲ +23.85 after sell → book $10,517.14; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 245 | $9.19 | $3.22 | $+214.12 | $6,346.56 | ▲ +214.12 after sell → book $10,513.92; vs 09:30 mark -3.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FWRD` | 117 | $17.60 | $2.38 | $+17.51 | $8,403.39 | ▲ +17.51 after sell → book $10,511.55; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 183 | $11.52 | $2.59 | $+68.07 | $10,508.96 | ▲ +68.07 after sell → book $10,508.96; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 21 | $81.65 | $2.05 | — | $8,792.26 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1751.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $7,823.25 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1751.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ASML` | 1 | $1746.53 | $1.99 | — | $6,074.73 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=-0.3; leftover $1751.49 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 5 | $318.88 | $2.00 | — | $4,478.33 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1751.49 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 7 | $222.86 | $2.01 | — | $2,916.30 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $1751.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 42 | $41.44 | $2.12 | — | $1,173.70 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; ret5=+3.1; leftover $1751.49 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,173.70 | ▼ close $10,472.03 vs 09:30 $10,521.76 (session -24.76) | 16:00 close · cash $1,173.70 · equity $10,472.03 vs 09:30 $10,521.76 (-49.73; session marks -24.76) · 6 name(s) marked open→close (per-name table). ACMR×21 09:30 $81.65 → close $80.49 -24.36; MU×1 09:30 $967.01 → close $935.39 -31.62; ASML×1 09:30 $1746.53 → close $1735.01 -11.52; LRCX×5 09:30 $318.88 → close $318.58 -1.50; NVDA×7 09:30 $222.86 → close $227.98 +35.84; RRC×42 09:30 $41.44 → close $41.64 +8.40 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,173.70 | ▼ 09:30 equity $10,427.16 vs yday $10,472.03 (-44.87) | 09:30 open · cash $1,173.70 (unchanged overnight, no fees) · equity $10,427.16 vs prior close $10,472.03 (-44.87) · 6 name(s) re-marked at the open (per-name table). ACMR×21 yday $80.49 → 09:30 $79.27 -25.62; MU×1 yday $935.39 → 09:30 $919.29 -16.10; ASML×1 yday $1735.01 → 09:30 $1734.75 -0.26; LRCX×5 yday $318.58 → 09:30 $318.03 -2.75; NVDA×7 yday $227.98 → 09:30 $227.36 -4.34; RRC×42 yday $41.64 → 09:30 $41.74 +4.20 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 21 | $79.27 | $2.08 | $-54.11 | $2,836.29 | ▼ -54.11 after sell → book $10,425.08; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $3,753.57 | ▼ -51.73 after sell → book $10,423.07; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ASML` | 1 | $1734.75 | $2.02 | $-15.79 | $5,486.30 | ▼ -15.79 after sell → book $10,421.05; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 5 | $318.03 | $2.03 | $-8.28 | $7,074.43 | ▼ -8.28 after sell → book $10,419.03; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 7 | $227.36 | $2.03 | $+27.46 | $8,663.91 | ▲ +27.46 after sell → book $10,416.99; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 42 | $41.74 | $2.14 | $+8.34 | $10,414.85 | ▲ +8.34 after sell → book $10,414.85; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $9,115.21 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1301.86 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $7,837.35 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1301.86 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,634.09 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1301.86 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $5,430.99 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1301.86 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $4,384.35 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; ret5=+7.8; leftover $1301.86 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $32.90 | $2.11 | — | $3,099.14 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1301.86 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 270 | $4.82 | $3.48 | — | $1,794.26 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1301.86 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,794.26 | ▼ close $10,135.82 vs 09:30 $10,427.16 (session -263.42) | 16:00 close · cash $1,794.26 · equity $10,135.82 vs 09:30 $10,427.16 (-291.34; session marks -263.42) · 7 name(s) marked open→close (per-name table). KEYS×4 09:30 $324.41 → close $319.97 -17.76; SMTC×9 09:30 $141.76 → close $131.17 -95.31; CIEN×3 09:30 $400.42 → close $378.44 -65.94; DDOG×5 09:30 $240.22 → close $236.98 -16.20; ADSK×4 09:30 $261.16 → close $260.66 -2.00; SEDG×39 09:30 $32.90 → close $31.41 -58.11; TLS×270 09:30 $4.82 → close $4.79 -8.10 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,794.26 | ▼ 09:30 equity $10,124.45 vs yday $10,135.82 (-11.37) | 09:30 open · cash $1,794.26 (unchanged overnight, no fees) · equity $10,124.45 vs prior close $10,135.82 (-11.37) · 7 name(s) re-marked at the open (per-name table). KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; DDOG×5 yday $236.98 → 09:30 $233.97 -15.07; ADSK×4 yday $260.66 → 09:30 $257.71 -11.80; SEDG×39 yday $31.41 → 09:30 $31.15 -10.14; TLS×270 yday $4.79 → 09:30 $4.81 +5.40 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $3,082.19 | ▼ -11.70 after sell → book $10,122.43; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $4,270.86 | ▼ -89.19 after sell → book $10,120.39; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $5,404.16 | ▼ -69.96 after sell → book $10,118.37; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 5 | $233.97 | $2.02 | $-35.31 | $6,571.96 | ▼ -35.31 after sell → book $10,116.35; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $257.71 | $2.02 | $-17.82 | $7,600.78 | ▼ -17.82 after sell → book $10,114.33; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 39 | $31.15 | $2.13 | $-72.48 | $8,813.50 | ▼ -72.48 after sell → book $10,112.20; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 270 | $4.81 | $3.54 | $-9.72 | $10,108.66 | ▼ -9.72 after sell → book $10,108.66; vs 09:30 mark -3.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,108.66 | ▲ close $10,108.66 vs 09:30 $10,124.45 (session +0.00) | 16:00 close · cash $10,108.66 · no lots left · equity $10,108.66. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,108.66 | ▲ 09:30 equity $10,108.66 vs yday $10,108.66 (+0.00) | 09:30 open · cash $10,108.66 · no holdings · equity $10,108.66 vs prior close $10,108.66 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,108.66 | ▲ close $10,108.66 vs 09:30 $10,108.66 (session +0.00) | 16:00 close · cash $10,108.66 · no lots left · equity $10,108.66. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,108.66 | ▲ 09:30 equity $10,108.66 vs yday $10,108.66 (+0.00) | 09:30 open · cash $10,108.66 · no holdings · equity $10,108.66 vs prior close $10,108.66 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,108.66 | ▲ close $10,108.66 vs 09:30 $10,108.66 (session +0.00) | 16:00 close · cash $10,108.66 · no lots left · equity $10,108.66. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,108.66 | ▲ 09:30 equity $10,108.66 vs yday $10,108.66 (+0.00) | 09:30 open · cash $10,108.66 · no holdings · equity $10,108.66 vs prior close $10,108.66 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $9,051.44 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1263.58 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $8,076.83 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1263.58 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 39 | $32.31 | $2.11 | — | $6,814.63 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1263.58 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 79 | $15.87 | $2.23 | — | $5,558.67 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1263.58 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 52 | $23.88 | $2.15 | — | $4,314.77 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1263.58 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $3,609.52 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1263.58 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 26 | $47.60 | $2.07 | — | $2,369.86 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1263.58 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 38 | $32.88 | $2.10 | — | $1,118.31 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ret5=+16.2; leftover $1263.58 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,118.31 | ▲ close $10,468.24 vs 09:30 $10,108.66 (session +376.22) | 16:00 close · cash $1,118.31 · equity $10,468.24 vs 09:30 $10,108.66 (+359.58; session marks +376.22) · 8 name(s) marked open→close (per-name table). AVGO×3 09:30 $351.74 → close $357.16 +16.26; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×39 09:30 $32.31 → close $33.66 +52.65; FRNM×79 09:30 $15.87 → close $16.90 +81.37; MMED×52 09:30 $23.88 → close $23.84 -2.08; DE×1 09:30 $703.25 → close $694.41 -8.84; HPE×26 09:30 $47.60 → close $54.44 +177.84; CNXC×38 09:30 $32.88 → close $32.85 -1.14 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,118.31 | ▼ 09:30 equity $10,391.56 vs yday $10,468.24 (-76.68) | 09:30 open · cash $1,118.31 (unchanged overnight, no fees) · equity $10,391.56 vs prior close $10,468.24 (-76.68) · 8 name(s) re-marked at the open (per-name table). AVGO×3 yday $357.16 → 09:30 $359.70 +7.62; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×39 yday $33.66 → 09:30 $33.46 -7.80; FRNM×79 yday $16.90 → 09:30 $16.40 -39.50; MMED×52 yday $23.84 → 09:30 $23.84 +0.00; DE×1 yday $694.41 → 09:30 $692.03 -2.38; HPE×26 yday $54.44 → 09:30 $53.85 -15.34; CNXC×38 yday $32.85 → 09:30 $32.48 -14.06 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $2,195.39 | ▲ +19.86 after sell → book $10,389.54; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $3,220.94 | ▲ +50.93 after sell → book $10,387.53; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 39 | $33.46 | $2.13 | $+40.62 | $4,523.75 | ▲ +40.62 after sell → book $10,385.40; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 52 | $23.84 | $2.17 | $-6.39 | $5,761.26 | ▼ -6.39 after sell → book $10,383.23; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $6,451.28 | ▼ -15.23 after sell → book $10,381.22; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 26 | $53.85 | $2.09 | $+158.34 | $7,849.29 | ▲ +158.34 after sell → book $10,379.13; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 38 | $32.48 | $2.12 | $-19.43 | $9,081.41 | ▼ -19.43 after sell → book $10,377.01; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 6 | $263.36 | $2.01 | — | $7,499.24 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1816.28 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 24 | $75.65 | $2.06 | — | $5,681.58 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1816.28 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 7 | $236.82 | $2.01 | — | $4,021.83 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+8.1; leftover $1816.28 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 936 | $1.94 | $12.07 | — | $2,193.91 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $1816.28 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 13 | $137.35 | $2.03 | — | $406.33 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+5.4; leftover $1816.28 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $406.33 | ▲ close $10,524.21 vs 09:30 $10,391.56 (session +167.39) | 16:00 close · cash $406.33 · equity $10,524.21 vs 09:30 $10,391.56 (+132.65; session marks +167.39) · 6 name(s) marked open→close (per-name table). FRNM×79 09:30 $16.40 → close $16.31 -7.11; CRM×6 09:30 $263.36 → close $259.23 -24.78; MRX×24 09:30 $75.65 → close $78.27 +62.88; BE×7 09:30 $236.82 → close $252.87 +112.35; BAK×936 09:30 $1.94 → close $1.89 -46.80; MSTR×13 09:30 $137.35 → close $142.80 +70.85 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $406.33 | ▲ 09:30 equity $10,622.49 vs yday $10,524.21 (+98.28) | 09:30 open · cash $406.33 (unchanged overnight, no fees) · equity $10,622.49 vs prior close $10,524.21 (+98.28) · 6 name(s) re-marked at the open (per-name table). FRNM×79 yday $16.31 → 09:30 $16.74 +33.97; CRM×6 yday $259.23 → 09:30 $253.72 -33.06; MRX×24 yday $78.27 → 09:30 $78.84 +13.68; BE×7 yday $252.87 → 09:30 $267.76 +104.23; BAK×936 yday $1.89 → 09:30 $1.94 +46.80; MSTR×13 yday $142.80 → 09:30 $137.62 -67.34 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 79 | $16.74 | $2.25 | $+64.25 | $1,726.54 | ▲ +64.25 after sell → book $10,620.24; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 6 | $253.72 | $2.03 | $-61.88 | $3,246.83 | ▼ -61.88 after sell → book $10,618.21; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 7 | $267.76 | $2.04 | $+212.53 | $5,119.12 | ▲ +212.53 after sell → book $10,616.18; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 936 | $1.94 | $12.24 | $-24.32 | $6,922.71 | ▼ -24.32 after sell → book $10,603.93; vs 09:30 mark -12.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,922.71 | ▼ close $10,538.51 vs 09:30 $10,622.49 (session -65.42) | 16:00 close · cash $6,922.71 · equity $10,538.51 vs 09:30 $10,622.49 (-83.98; session marks -65.42) · 2 name(s) marked open→close (per-name table). MRX×24 09:30 $78.84 → close $76.71 -51.12; MSTR×13 09:30 $137.62 → close $136.52 -14.30 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,922.71 | ▲ 09:30 equity $10,604.77 vs yday $10,538.51 (+66.26) | 09:30 open · cash $6,922.71 (unchanged overnight, no fees) · equity $10,604.77 vs prior close $10,538.51 (+66.26) · 2 name(s) re-marked at the open (per-name table). MRX×24 yday $76.71 → 09:30 $76.60 -2.64; MSTR×13 yday $136.52 → 09:30 $141.82 +68.90 | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 24 | $76.60 | $2.09 | $+18.65 | $8,759.02 | ▲ +18.65 after sell → book $10,602.68; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 13 | $141.82 | $2.05 | $+54.03 | $10,600.63 | ▲ +54.03 after sell → book $10,600.63; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,600.63 | ▲ close $10,600.63 vs 09:30 $10,604.77 (session +0.00) | 16:00 close · cash $10,600.63 · no lots left · equity $10,600.63. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,600.63 | ▲ 09:30 equity $10,600.63 vs yday $10,600.63 (+0.00) | 09:30 open · cash $10,600.63 · no holdings · equity $10,600.63 vs prior close $10,600.63 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,600.63 | ▲ close $10,600.63 vs 09:30 $10,600.63 (session +0.00) | 16:00 close · cash $10,600.63 · no lots left · equity $10,600.63. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,600.63 | ▲ 09:30 equity $10,600.63 vs yday $10,600.63 (+0.00) | 09:30 open · cash $10,600.63 · no holdings · equity $10,600.63 vs prior close $10,600.63 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 12 | $164.43 | $2.03 | — | $8,625.44 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $2120.13 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 8 | $242.17 | $2.01 | — | $6,686.07 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; ret5=-11.1; leftover $2120.13 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 1000 | $2.12 | $12.90 | — | $4,553.17 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $2120.13 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 1039 | $2.04 | $13.40 | — | $2,420.21 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $2120.13 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 15 | $135.71 | $2.04 | — | $382.52 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; ret5=-9.2; leftover $2120.13 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $382.52 | ▼ close $10,383.16 vs 09:30 $10,600.63 (session -185.09) | 16:00 close · cash $382.52 · equity $10,383.16 vs 09:30 $10,600.63 (-217.47; session marks -185.09) · 5 name(s) marked open→close (per-name table). ORCL×12 09:30 $164.43 → close $150.28 -169.80; ADBE×8 09:30 $242.17 → close $252.23 +80.48; BAK×1000 09:30 $2.12 → close $2.08 -40.00; AMTX×1039 09:30 $2.04 → close $2.01 -31.17; RH×15 09:30 $135.71 → close $134.07 -24.60 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $382.52 | ▼ 09:30 equity $10,281.03 vs yday $10,383.16 (-102.13) | 09:30 open · cash $382.52 (unchanged overnight, no fees) · equity $10,281.03 vs prior close $10,383.16 (-102.13) · 5 name(s) re-marked at the open (per-name table). ORCL×12 yday $150.28 → 09:30 $141.42 -106.32; ADBE×8 yday $252.23 → 09:30 $261.51 +74.24; BAK×1000 yday $2.08 → 09:30 $2.05 -30.00; AMTX×1039 yday $2.01 → 09:30 $2.01 +0.00; RH×15 yday $134.07 → 09:30 $131.40 -40.05 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 12 | $141.42 | $2.05 | $-280.20 | $2,077.51 | ▼ -280.20 after sell → book $10,278.98; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 8 | $261.51 | $2.04 | $+150.67 | $4,167.55 | ▲ +150.67 after sell → book $10,276.94; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 1000 | $2.05 | $13.08 | $-95.98 | $6,204.47 | ▼ -95.98 after sell → book $10,263.86; vs 09:30 mark -13.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 1039 | $2.01 | $13.59 | $-58.17 | $8,279.27 | ▼ -58.17 after sell → book $10,250.27; vs 09:30 mark -13.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 15 | $131.40 | $2.06 | $-68.75 | $10,248.21 | ▼ -68.75 after sell → book $10,248.21; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,248.21 | ▲ close $10,248.21 vs 09:30 $10,281.03 (session +0.00) | 16:00 close · cash $10,248.21 · no lots left · equity $10,248.21. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,248.21 | ▲ 09:30 equity $10,248.21 vs yday $10,248.21 (-0.00) | 09:30 open · cash $10,248.21 · no holdings · equity $10,248.21 vs prior close $10,248.21 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,248.21 | ▲ close $10,248.21 vs 09:30 $10,248.21 (session +0.00) | 16:00 close · cash $10,248.21 · no lots left · equity $10,248.21. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,248.21 | ▲ 09:30 equity $10,248.21 vs yday $10,248.21 (-0.00) | 09:30 open · cash $10,248.21 · no holdings · equity $10,248.21 vs prior close $10,248.21 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 97 | $26.27 | $2.28 | — | $7,697.74 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2562.05 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 64 | $39.99 | $2.18 | — | $5,136.19 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2562.05 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `CLS` | 8 | $320.20 | $2.01 | — | $2,572.58 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+10.2; leftover $2562.05 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 368 | $6.95 | $4.75 | — | $10.23 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; ret5=-5.8; leftover $2562.05 | join🔴 sector🟢 gen🟢 news🟢 digest🔴 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.23 | ▼ close $10,213.06 vs 09:30 $10,248.21 (session -23.92) | 16:00 close · cash $10.23 · equity $10,213.06 vs 09:30 $10,248.21 (-35.15; session marks -23.92) · 4 name(s) marked open→close (per-name table). WAY×97 09:30 $26.27 → close $26.59 +31.04; SM×64 09:30 $39.99 → close $38.16 -117.12; CLS×8 09:30 $320.20 → close $323.83 +29.04; SION×368 09:30 $6.95 → close $7.04 +33.12 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.23 | ▲ 09:30 equity $10,363.54 vs yday $10,213.06 (+150.48) | 09:30 open · cash $10.23 (unchanged overnight, no fees) · equity $10,363.54 vs prior close $10,213.06 (+150.48) · 4 name(s) re-marked at the open (per-name table). WAY×97 yday $26.59 → 09:30 $26.51 -7.76; SM×64 yday $38.16 → 09:30 $37.57 -37.76; CLS×8 yday $323.83 → 09:30 $337.75 +111.36; SION×368 yday $7.04 → 09:30 $7.27 +84.64 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 97 | $26.51 | $2.32 | $+18.68 | $2,579.38 | ▲ +18.68 after sell → book $10,361.22; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 64 | $37.57 | $2.21 | $-159.27 | $4,981.65 | ▼ -159.27 after sell → book $10,359.01; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 368 | $7.27 | $4.83 | $+108.18 | $7,652.18 | ▲ +108.18 after sell → book $10,354.18; vs 09:30 mark -4.83 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 7 | $170.85 | $2.01 | — | $6,454.22 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1275.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 57 | $22.12 | $2.16 | — | $5,191.22 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1275.36 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 5 | $238.60 | $2.00 | — | $3,996.22 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; ret5=+3.5; leftover $1275.36 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 1 | $934.88 | $1.99 | — | $3,059.34 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; ret5=+9.4; leftover $1275.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 71 | $17.72 | $2.20 | — | $1,799.02 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ret5=-8.3; leftover $1275.36 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 720 | $1.77 | $9.29 | — | $515.33 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; ret5=-10.2; leftover $1275.36 | join🔴 sector🟡 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $515.33 | ▼ close $10,289.81 vs 09:30 $10,363.54 (session -44.71) | 16:00 close · cash $515.33 · equity $10,289.81 vs 09:30 $10,363.54 (-73.73; session marks -44.71) · 7 name(s) marked open→close (per-name table). CLS×8 09:30 $337.75 → close $329.94 -62.48; SMTC×7 09:30 $170.85 → close $178.19 +51.38; GME×57 09:30 $22.12 → close $22.77 +37.05; JBHT×5 09:30 $238.60 → close $236.80 -9.00; LITE×1 09:30 $934.88 → close $893.61 -41.27; TNDM×71 09:30 $17.72 → close $17.23 -34.79; BAK×720 09:30 $1.77 → close $1.79 +14.40 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $515.33 | ▲ 09:30 equity $10,343.71 vs yday $10,289.81 (+53.90) | 09:30 open · cash $515.33 (unchanged overnight, no fees) · equity $10,343.71 vs prior close $10,289.81 (+53.90) · 7 name(s) re-marked at the open (per-name table). CLS×8 yday $329.94 → 09:30 $332.06 +16.96; SMTC×7 yday $178.19 → 09:30 $182.33 +28.98; GME×57 yday $22.77 → 09:30 $22.90 +7.41; JBHT×5 yday $236.80 → 09:30 $236.80 +0.00; LITE×1 yday $893.61 → 09:30 $915.66 +22.05; TNDM×71 yday $17.23 → 09:30 $17.13 -7.10; BAK×720 yday $1.79 → 09:30 $1.77 -14.40 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 7 | $182.33 | $2.03 | $+76.32 | $1,789.61 | ▲ +76.32 after sell → book $10,341.68; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 5 | $236.80 | $2.02 | $-13.03 | $2,971.59 | ▼ -13.03 after sell → book $10,339.66; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 1 | $915.66 | $2.01 | $-23.23 | $3,885.23 | ▼ -23.23 after sell → book $10,337.64; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 71 | $17.13 | $2.22 | $-46.32 | $5,099.24 | ▼ -46.32 after sell → book $10,335.42; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 720 | $1.77 | $9.42 | $-18.71 | $6,364.22 | ▼ -18.71 after sell → book $10,326.00; vs 09:30 mark -9.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 76 | $20.91 | $2.22 | — | $4,772.84 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1591.06 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 107 | $14.79 | $2.31 | — | $3,188.00 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=-3.7; leftover $1591.06 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 113 | $14.07 | $2.33 | — | $1,595.76 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1591.06 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 211 | $7.54 | $2.72 | — | $3.16 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; 🔵; ret5=-20.9; leftover $1591.06 | join🟡 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.16 | ▼ close $10,201.27 vs 09:30 $10,343.71 (session -115.15) | 16:00 close · cash $3.16 · equity $10,201.27 vs 09:30 $10,343.71 (-142.44; session marks -115.15) · 6 name(s) marked open→close (per-name table). CLS×8 09:30 $332.06 → close $332.63 +4.56; GME×57 09:30 $22.90 → close $22.64 -14.82; TH×76 09:30 $20.91 → close $21.19 +21.28; RARE×107 09:30 $14.79 → close $14.51 -29.96; BHVN×113 09:30 $14.07 → close $13.62 -50.85; FLNC×211 09:30 $7.54 → close $7.32 -45.36 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AUTL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-28 | `MPWR` | cash | leftover split 1301.86 < 1 share @ 1306.03 |
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
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `WLTH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `AVTR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CLS` | 8 | 2026-09-16 @ $320.20 | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+10.2; leftover $2562.05 |
| `GME` | 57 | 2026-09-17 @ $22.12 | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1275.36 |
| `TH` | 76 | 2026-09-18 @ $20.91 | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1591.06 |
| `RARE` | 107 | 2026-09-18 @ $14.79 | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=-3.7; leftover $1591.06 |
| `BHVN` | 113 | 2026-09-18 @ $14.07 | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1591.06 |
| `FLNC` | 211 | 2026-09-18 @ $7.54 | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; 🔵; ret5=-20.9; leftover $1591.06 |
