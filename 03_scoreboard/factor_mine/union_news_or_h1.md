# Factor mine action — `union_news_or_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢

Cash book **+2.64%** ($10,264) · signal-only (no cash/fees) was +14.08%. Starts YES **14/22**. Fills 140 · skips 61 · realized $+264.31.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,264.32.

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
| 2026-08-17 | `DVN` | 44 | — | $46.18 | +0.00 | $47.57 | +61.16 | +61.16 | +0.00 | +61.16 |
| 2026-08-17 | `EOG` | 14 | — | $142.77 | +0.00 | $146.15 | +47.32 | +47.32 | +0.00 | +47.32 |
| 2026-08-17 | `FANG` | 10 | — | $202.70 | +0.00 | $206.29 | +35.90 | +35.90 | +0.00 | +35.90 |
| 2026-08-17 | `OUST` | 41 | — | $49.00 | +0.00 | $48.13 | -35.67 | -35.67 | +0.00 | -35.67 |
| 2026-08-17 | `CELC` | 21 | — | $92.99 | +0.00 | $92.44 | -11.55 | -11.55 | +0.00 | -11.55 |
| 2026-08-18 | `DVN` | 44 | $47.57 | $48.00 | +18.92 | — | +0.00 | +18.92 | +80.08 | — |
| 2026-08-18 | `EOG` | 14 | $146.15 | $148.04 | +26.46 | — | +0.00 | +26.46 | +73.78 | — |
| 2026-08-18 | `FANG` | 10 | $206.29 | $208.93 | +26.40 | — | +0.00 | +26.40 | +62.30 | — |
| 2026-08-18 | `OUST` | 41 | $48.13 | $45.09 | -124.64 | — | +0.00 | -124.64 | -160.31 | — |
| 2026-08-18 | `CELC` | 21 | $92.44 | $92.38 | -1.26 | — | +0.00 | -1.26 | -12.81 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 14 | — | $91.01 | +0.00 | $93.63 | +36.68 | +36.68 | +0.00 | +36.68 |
| 2026-08-20 | `APA` | 28 | — | $44.76 | +0.00 | $44.39 | -10.36 | -10.36 | +0.00 | -10.36 |
| 2026-08-20 | `AUTL` | 517 | — | $2.47 | +0.00 | $2.46 | -5.17 | -5.17 | +0.00 | -5.17 |
| 2026-08-20 | `CRSP` | 21 | — | $58.73 | +0.00 | $58.12 | -12.81 | -12.81 | +0.00 | -12.81 |
| 2026-08-20 | `ASST` | 79 | — | $16.00 | +0.00 | $16.13 | +10.27 | +10.27 | +0.00 | +10.27 |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `ZLAB` | 48 | — | $26.57 | +0.00 | $26.02 | -26.40 | -26.40 | +0.00 | -26.40 |
| 2026-08-20 | `TEAM` | 7 | — | $173.90 | +0.00 | $174.91 | +7.07 | +7.07 | +0.00 | +7.07 |
| 2026-08-21 | `BHP` | 14 | $93.63 | $95.72 | +29.26 | — | +0.00 | +29.26 | +65.94 | — |
| 2026-08-21 | `APA` | 28 | $44.39 | $44.52 | +3.64 | — | +0.00 | +3.64 | -6.72 | — |
| 2026-08-21 | `AUTL` | 517 | $2.46 | $2.47 | +5.17 | $2.41 | -31.02 | -25.85 | +0.00 | -31.02 |
| 2026-08-21 | `CRSP` | 21 | $58.12 | $59.72 | +33.60 | $59.50 | -4.62 | +28.98 | +20.79 | +16.17 |
| 2026-08-21 | `ASST` | 79 | $16.13 | $17.66 | +120.87 | — | +0.00 | +120.87 | +131.14 | — |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | — | +0.00 | -1.68 | -136.24 | — |
| 2026-08-21 | `ZLAB` | 48 | $26.02 | $26.25 | +11.04 | — | +0.00 | +11.04 | -15.36 | — |
| 2026-08-21 | `TEAM` | 7 | $174.91 | $174.22 | -4.83 | — | +0.00 | -4.83 | +2.24 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `FUTU` | 11 | — | $115.18 | +0.00 | $123.64 | +93.06 | +93.06 | +0.00 | +93.06 |
| 2026-08-21 | `GRAL` | 16 | — | $78.88 | +0.00 | $79.54 | +10.56 | +10.56 | +0.00 | +10.56 |
| 2026-08-21 | `ABTC` | 148 | — | $8.66 | +0.00 | $7.93 | -108.04 | -108.04 | +0.00 | -108.04 |
| 2026-08-21 | `HIVE` | 396 | — | $3.24 | +0.00 | $3.03 | -83.16 | -83.16 | +0.00 | -83.16 |
| 2026-08-21 | `MARA` | 109 | — | $11.70 | +0.00 | $11.26 | -47.96 | -47.96 | +0.00 | -47.96 |
| 2026-08-24 | `AUTL` | 517 | $2.41 | $2.40 | -5.17 | — | +0.00 | -5.17 | -36.19 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | $57.08 | -35.17 | -50.92 | +0.42 | -34.75 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `FUTU` | 11 | $123.64 | $121.00 | -29.04 | — | +0.00 | -29.04 | +64.02 | — |
| 2026-08-24 | `GRAL` | 16 | $79.54 | $81.87 | +37.28 | — | +0.00 | +37.28 | +47.84 | — |
| 2026-08-24 | `ABTC` | 148 | $7.93 | $8.00 | +10.36 | — | +0.00 | +10.36 | -97.68 | — |
| 2026-08-24 | `HIVE` | 396 | $3.03 | $2.99 | -15.84 | — | +0.00 | -15.84 | -99.00 | — |
| 2026-08-24 | `MARA` | 109 | $11.26 | $11.17 | -9.81 | — | +0.00 | -9.81 | -57.77 | — |
| 2026-08-25 | `CRSP` | 21 | $57.08 | $57.93 | +17.95 | — | +0.00 | +17.95 | -16.80 | — |
| 2026-08-25 | `AU` | 12 | — | $118.52 | +0.00 | $123.39 | +58.44 | +58.44 | +0.00 | +58.44 |
| 2026-08-25 | `FCX` | 18 | — | $77.13 | +0.00 | $79.91 | +50.04 | +50.04 | +0.00 | +50.04 |
| 2026-08-25 | `EZPW` | 40 | — | $35.05 | +0.00 | $35.23 | +7.20 | +7.20 | +0.00 | +7.20 |
| 2026-08-25 | `RUM` | 151 | — | $9.42 | +0.00 | $10.23 | +122.31 | +122.31 | +0.00 | +122.31 |
| 2026-08-25 | `ZYME` | 49 | — | $28.86 | +0.00 | $27.47 | -68.11 | -68.11 | +0.00 | -68.11 |
| 2026-08-25 | `REAX` | 59 | — | $24.11 | +0.00 | $28.43 | +254.88 | +254.88 | +0.00 | +254.88 |
| 2026-08-25 | `EOLS` | 163 | — | $8.72 | +0.00 | $8.97 | +41.56 | +41.56 | +0.00 | +41.56 |
| 2026-08-26 | `AU` | 12 | $123.39 | $119.80 | -43.08 | — | +0.00 | -43.08 | +15.36 | — |
| 2026-08-26 | `FCX` | 18 | $79.91 | $79.34 | -10.26 | — | +0.00 | -10.26 | +39.78 | — |
| 2026-08-26 | `EZPW` | 40 | $35.23 | $35.70 | +18.80 | — | +0.00 | +18.80 | +26.00 | — |
| 2026-08-26 | `RUM` | 151 | $10.23 | $10.07 | -24.16 | — | +0.00 | -24.16 | +98.15 | — |
| 2026-08-26 | `ZYME` | 49 | $27.47 | $27.56 | +4.41 | — | +0.00 | +4.41 | -63.70 | — |
| 2026-08-26 | `REAX` | 59 | $28.43 | $26.61 | -107.38 | — | +0.00 | -107.38 | +147.50 | — |
| 2026-08-26 | `EOLS` | 163 | $8.97 | $8.86 | -18.75 | — | +0.00 | -18.75 | +22.82 | — |
| 2026-08-26 | `FNV` | 7 | — | $267.02 | +0.00 | $267.37 | +2.45 | +2.45 | +0.00 | +2.45 |
| 2026-08-26 | `TRLV` | 182 | — | $11.22 | +0.00 | $11.43 | +38.22 | +38.22 | +0.00 | +38.22 |
| 2026-08-26 | `CAPR` | 247 | — | $8.29 | +0.00 | $9.36 | +264.29 | +264.29 | +0.00 | +264.29 |
| 2026-08-26 | `FWRD` | 117 | — | $17.41 | +0.00 | $17.63 | +25.74 | +25.74 | +0.00 | +25.74 |
| 2026-08-26 | `FLNC` | 184 | — | $11.12 | +0.00 | $11.08 | -7.36 | -7.36 | +0.00 | -7.36 |
| 2026-08-27 | `FNV` | 7 | $267.37 | $267.23 | -0.98 | — | +0.00 | -0.98 | +1.47 | — |
| 2026-08-27 | `TRLV` | 182 | $11.43 | $11.38 | -9.10 | — | +0.00 | -9.10 | +29.12 | — |
| 2026-08-27 | `CAPR` | 247 | $9.36 | $9.19 | -41.99 | — | +0.00 | -41.99 | +222.30 | — |
| 2026-08-27 | `FWRD` | 117 | $17.63 | $17.60 | -3.51 | — | +0.00 | -3.51 | +22.23 | — |
| 2026-08-27 | `FLNC` | 184 | $11.08 | $11.52 | +80.96 | — | +0.00 | +80.96 | +73.60 | — |
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
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 5 | — | $240.22 | +0.00 | $236.98 | -16.20 | -16.20 | +0.00 | -16.20 |
| 2026-08-28 | `ADSK` | 5 | — | $261.16 | +0.00 | $260.66 | -2.50 | -2.50 | +0.00 | -2.50 |
| 2026-08-28 | `SEDG` | 39 | — | $32.90 | +0.00 | $31.41 | -58.11 | -58.11 | +0.00 | -58.11 |
| 2026-08-28 | `TLS` | 271 | — | $4.82 | +0.00 | $4.79 | -8.13 | -8.13 | +0.00 | -8.13 |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | — | +0.00 | +10.08 | -7.68 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -65.94 | — |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | — | +0.00 | +5.64 | -44.13 | — |
| 2026-08-31 | `DDOG` | 5 | $236.98 | $233.97 | -15.07 | — | +0.00 | -15.07 | -31.27 | — |
| 2026-08-31 | `ADSK` | 5 | $260.66 | $257.71 | -14.75 | — | +0.00 | -14.75 | -17.25 | — |
| 2026-08-31 | `SEDG` | 39 | $31.41 | $31.15 | -10.14 | — | +0.00 | -10.14 | -68.25 | — |
| 2026-08-31 | `TLS` | 271 | $4.79 | $4.81 | +5.42 | — | +0.00 | +5.42 | -2.71 | — |
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
| 2026-09-04 | `BAK` | 937 | — | $1.94 | +0.00 | $1.89 | -46.85 | -46.85 | +0.00 | -46.85 |
| 2026-09-04 | `MSTR` | 13 | — | $137.35 | +0.00 | $142.80 | +70.85 | +70.85 | +0.00 | +70.85 |
| 2026-09-08 | `FRNM` | 79 | $16.31 | $16.74 | +33.97 | — | +0.00 | +33.97 | +68.73 | — |
| 2026-09-08 | `CRM` | 6 | $259.23 | $253.72 | -33.06 | — | +0.00 | -33.06 | -57.84 | — |
| 2026-09-08 | `MRX` | 24 | $78.27 | $78.84 | +13.68 | $76.71 | -51.12 | -37.44 | +76.56 | +25.44 |
| 2026-09-08 | `BE` | 7 | $252.87 | $267.76 | +104.23 | — | +0.00 | +104.23 | +216.58 | — |
| 2026-09-08 | `BAK` | 937 | $1.89 | $1.94 | +46.85 | — | +0.00 | +46.85 | +0.00 | — |
| 2026-09-08 | `MSTR` | 13 | $142.80 | $137.62 | -67.34 | $136.52 | -14.30 | -81.64 | +3.51 | -10.79 |
| 2026-09-09 | `MRX` | 24 | $76.71 | $76.60 | -2.64 | — | +0.00 | -2.64 | +22.80 | — |
| 2026-09-09 | `MSTR` | 13 | $136.52 | $141.82 | +68.90 | — | +0.00 | +68.90 | +58.11 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 12 | — | $164.43 | +0.00 | $150.28 | -169.80 | -169.80 | +0.00 | -169.80 |
| 2026-09-11 | `ADBE` | 8 | — | $242.17 | +0.00 | $252.23 | +80.48 | +80.48 | +0.00 | +80.48 |
| 2026-09-11 | `BAK` | 1001 | — | $2.12 | +0.00 | $2.08 | -40.04 | -40.04 | +0.00 | -40.04 |
| 2026-09-11 | `AMTX` | 1040 | — | $2.04 | +0.00 | $2.01 | -31.20 | -31.20 | +0.00 | -31.20 |
| 2026-09-11 | `RH` | 15 | — | $135.71 | +0.00 | $134.07 | -24.60 | -24.60 | +0.00 | -24.60 |
| 2026-09-14 | `ORCL` | 12 | $150.28 | $141.42 | -106.32 | — | +0.00 | -106.32 | -276.12 | — |
| 2026-09-14 | `ADBE` | 8 | $252.23 | $261.51 | +74.24 | — | +0.00 | +74.24 | +154.72 | — |
| 2026-09-14 | `BAK` | 1001 | $2.08 | $2.05 | -30.03 | — | +0.00 | -30.03 | -70.07 | — |
| 2026-09-14 | `AMTX` | 1040 | $2.01 | $2.01 | +0.00 | — | +0.00 | +0.00 | -31.20 | — |
| 2026-09-14 | `RH` | 15 | $134.07 | $131.40 | -40.05 | — | +0.00 | -40.05 | -64.65 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +127.16 | ANGX, ARX, HLIT, MH, NRG, TLN, VST | — | $1,560.49 | $10,110.67 | ANGX×290, ARX×63, HLIT×94, MH×92, NRG×10, TLN×3, VST×8 |
| 2026-08-17 | +2.25 | $1,560.49 | ANGX×290, ARX×63, HLIT×94, MH×92, NRG×10, TLN×3, VST×8 | $10,211.68 | +101.01 | +97.16 | DVN, EOG, FANG, OUST, CELC | ANGX, ARX, HLIT, MH, NRG, TLN, VST | $165.17 | $10,281.82 | DVN×44, EOG×14, FANG×10, OUST×41, CELC×21 |
| 2026-08-18 | -6.20 | $165.17 | DVN×44, EOG×14, FANG×10, OUST×41, CELC×21 | $10,227.70 | -54.12 | +0.00 | — | DVN, EOG, FANG, OUST, CELC | $10,217.23 | $10,217.23 | — |
| 2026-08-19 | -7.20 | $10,217.23 | — | $10,217.23 | -0.00 | +0.00 | — | — | $10,217.23 | $10,217.23 | — |
| 2026-08-20 | +1.12 | $10,217.23 | — | $10,217.23 | -0.00 | -135.28 | BHP, APA, AUTL, CRSP, ASST, MRNA, ZLAB, TEAM | — | $200.49 | $10,060.73 | BHP×14, APA×28, AUTL×517, CRSP×21, ASST×79, MRNA×8, ZLAB×48, TEAM×7 |
| 2026-08-21 | +3.25 | $200.49 | BHP×14, APA×28, AUTL×517, CRSP×21, ASST×79, MRNA×8, ZLAB×48, TEAM×7 | $10,257.80 | +197.07 | -153.28 | AU, FUTU, GRAL, ABTC, HIVE, MARA | BHP, APA, ASST, MRNA, ZLAB, TEAM | $134.75 | $10,075.96 | AUTL×517, CRSP×21, AU×10, FUTU×11, GRAL×16, ABTC×148, HIVE×396, MARA×109 |
| 2026-08-24 | -5.17 | $134.75 | AUTL×517, CRSP×21, AU×10, FUTU×11, GRAL×16, ABTC×148, HIVE×396, MARA×109 | $10,040.89 | -35.07 | -35.17 | — | AUTL, AU, FUTU, GRAL, ABTC, HIVE, MARA | $8,784.24 | $9,982.81 | CRSP×21 |
| 2026-08-25 | +1.80 | $8,784.24 | CRSP×21 | $10,000.77 | +17.96 | +466.32 | AU, FCX, EZPW, RUM, ZYME, REAX, EOLS | CRSP | $90.30 | $10,449.62 | AU×12, FCX×18, EZPW×40, RUM×151, ZYME×49, REAX×59, EOLS×163 |
| 2026-08-26 | +2.02 | $90.30 | AU×12, FCX×18, EZPW×40, RUM×151, ZYME×49, REAX×59, EOLS×163 | $10,269.20 | -180.42 | +323.34 | FNV, TRLV, CAPR, FWRD, FLNC | AU, FCX, EZPW, RUM, ZYME, REAX, EOLS | $199.14 | $10,564.34 | FNV×7, TRLV×182, CAPR×247, FWRD×117, FLNC×184 |
| 2026-08-27 | — | $199.14 | FNV×7, TRLV×182, CAPR×247, FWRD×117, FLNC×184 | $10,589.72 | +25.38 | -24.76 | ACMR, MU, ASML, LRCX, NVDA, RRC | FNV, TRLV, CAPR, FWRD, FLNC | $1,241.62 | $10,539.95 | ACMR×21, MU×1, ASML×1, LRCX×5, NVDA×7, RRC×42 |
| 2026-08-28 | +0.75 | $1,241.62 | ACMR×21, MU×1, ASML×1, LRCX×5, NVDA×7, RRC×42 | $10,495.08 | -44.87 | -313.72 | KEYS, SMTC, CIEN, MPWR, DDOG, ADSK, SEDG, TLS | ACMR, MU, ASML, LRCX, NVDA, RRC | $288.16 | $10,151.43 | KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, ADSK×5, SEDG×39, TLS×271 |
| 2026-08-31 | -5.85 | $288.16 | KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, ADSK×5, SEDG×39, TLS×271 | $10,142.78 | -8.65 | +0.00 | — | KEYS, SMTC, CIEN, MPWR, DDOG, ADSK, SEDG, TLS | $10,124.96 | $10,124.96 | — |
| 2026-09-01 | -6.30 | $10,124.96 | — | $10,124.96 | -0.00 | +0.00 | — | — | $10,124.96 | $10,124.96 | — |
| 2026-09-02 | -3.83 | $10,124.96 | — | $10,124.96 | -0.00 | +0.00 | — | — | $10,124.96 | $10,124.96 | — |
| 2026-09-03 | -0.90 | $10,124.96 | — | $10,124.96 | -0.00 | +376.22 | AVGO, DELL, CXW, FRNM, MMED, DE, HPE, CNXC | — | $1,134.61 | $10,484.54 | AVGO×3, DELL×2, CXW×39, FRNM×79, MMED×52, DE×1, HPE×26, CNXC×38 |
| 2026-09-04 | +2.25 | $1,134.61 | AVGO×3, DELL×2, CXW×39, FRNM×79, MMED×52, DE×1, HPE×26, CNXC×38 | $10,407.86 | -76.68 | +167.34 | CRM, MRX, BE, BAK, MSTR | AVGO, DELL, CXW, MMED, DE, HPE, CNXC | $420.67 | $10,540.44 | FRNM×79, CRM×6, MRX×24, BE×7, BAK×937, MSTR×13 |
| 2026-09-08 | -11.47 | $420.67 | FRNM×79, CRM×6, MRX×24, BE×7, BAK×937, MSTR×13 | $10,638.77 | +98.33 | -65.42 | — | FRNM, CRM, BE, BAK | $6,938.98 | $10,554.78 | MRX×24, MSTR×13 |
| 2026-09-09 | -13.95 | $6,938.98 | MRX×24, MSTR×13 | $10,621.04 | +66.26 | +0.00 | — | MRX, MSTR | $10,616.90 | $10,616.90 | — |
| 2026-09-10 | -13.28 | $10,616.90 | — | $10,616.90 | +0.00 | +0.00 | — | — | $10,616.90 | $10,616.90 | — |
| 2026-09-11 | +0.50 | $10,616.90 | — | $10,616.90 | +0.00 | -185.16 | ORCL, ADBE, BAK, AMTX, RH | — | $394.61 | $10,399.34 | ORCL×12, ADBE×8, BAK×1001, AMTX×1040, RH×15 |
| 2026-09-14 | -11.00 | $394.61 | ORCL×12, ADBE×8, BAK×1001, AMTX×1040, RH×15 | $10,297.18 | -102.16 | +0.00 | — | ORCL, ADBE, BAK, AMTX, RH | $10,264.32 | $10,264.32 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $8,746.36 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $7,511.27 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $6,270.08 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $5,021.21 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $3,819.19 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $2,737.70 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $1,560.49 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,560.49 | ▲ close $10,110.67 vs 09:30 $10,000.00 (session +127.16) | 16:00 close · cash $1,560.49 · equity $10,110.67 vs 09:30 $10,000.00 (+110.67; session marks +127.16) · 7 name(s) marked open→close (per-name table). ANGX×290 09:30 $4.31 → close $4.37 +17.40; ARX×63 09:30 $19.57 → close $19.58 +0.63; HLIT×94 09:30 $13.18 → close $13.92 +69.56; MH×92 09:30 $13.55 → close $13.10 -41.40; NRG×10 09:30 $120.00 → close $126.24 +62.40; TLN×3 09:30 $359.83 → close $362.74 +8.73; VST×8 09:30 $146.90 → close $148.13 +9.84 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,560.49 | ▲ 09:30 equity $10,211.68 vs yday $10,110.67 (+101.01) | 09:30 open · cash $1,560.49 (unchanged overnight, no fees) · equity $10,211.68 vs prior close $10,110.67 (+101.01) · 7 name(s) re-marked at the open (per-name table). ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; HLIT×94 yday $13.92 → 09:30 $13.84 -7.52; MH×92 yday $13.10 → 09:30 $13.16 +5.52; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92 | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $2,890.69 | ▲ +76.56 after sell → book $10,207.88; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $4,121.40 | ▼ -4.38 after sell → book $10,205.68; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $5,420.06 | ▲ +57.47 after sell → book $10,203.38; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $6,628.49 | ▼ -40.44 after sell → book $10,201.09; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $7,900.45 | ▲ +69.94 after sell → book $10,199.05; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $9,002.07 | ▲ +20.13 after sell → book $10,197.03; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $10,195.00 | ▲ +15.71 after sell → book $10,195.00; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 44 | $46.18 | $2.12 | — | $8,160.96 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ret5=+6.7; leftover $2039.00 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 14 | $142.77 | $2.03 | — | $6,160.14 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ret5=+5.8; leftover $2039.00 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 10 | $202.70 | $2.02 | — | $4,131.12 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ret5=+8.3; leftover $2039.00 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 41 | $49.00 | $2.11 | — | $2,120.01 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $2039.00 | join🟡 sector🟢 gen🟢 news🟢 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 21 | $92.99 | $2.05 | — | $165.17 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer; ret5=-0.8; leftover $2039.00 | join🟡 sector🔴 gen🟢 news🟢 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $165.17 | ▲ close $10,281.82 vs 09:30 $10,211.68 (session +97.16) | 16:00 close · cash $165.17 · equity $10,281.82 vs 09:30 $10,211.68 (+70.14; session marks +97.16) · 5 name(s) marked open→close (per-name table). DVN×44 09:30 $46.18 → close $47.57 +61.16; EOG×14 09:30 $142.77 → close $146.15 +47.32; FANG×10 09:30 $202.70 → close $206.29 +35.90; OUST×41 09:30 $49.00 → close $48.13 -35.67; CELC×21 09:30 $92.99 → close $92.44 -11.55 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $165.17 | ▼ 09:30 equity $10,227.70 vs yday $10,281.82 (-54.12) | 09:30 open · cash $165.17 (unchanged overnight, no fees) · equity $10,227.70 vs prior close $10,281.82 (-54.12) · 5 name(s) re-marked at the open (per-name table). DVN×44 yday $47.57 → 09:30 $48.00 +18.92; EOG×14 yday $146.15 → 09:30 $148.04 +26.46; FANG×10 yday $206.29 → 09:30 $208.93 +26.40; OUST×41 yday $48.13 → 09:30 $45.09 -124.64; CELC×21 yday $92.44 → 09:30 $92.38 -1.26 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 44 | $48.00 | $2.15 | $+75.81 | $2,275.02 | ▲ +75.81 after sell → book $10,225.55; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 14 | $148.04 | $2.06 | $+69.69 | $4,345.52 | ▲ +69.69 after sell → book $10,223.49; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 10 | $208.93 | $2.05 | $+58.23 | $6,432.77 | ▲ +58.23 after sell → book $10,221.44; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 41 | $45.09 | $2.14 | $-164.56 | $8,279.33 | ▼ -164.56 after sell → book $10,219.31; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 21 | $92.38 | $2.08 | $-16.94 | $10,217.23 | ▼ -16.94 after sell → book $10,217.23; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,217.23 | ▲ close $10,217.23 vs 09:30 $10,227.70 (session +0.00) | 16:00 close · cash $10,217.23 · no lots left · equity $10,217.23. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,217.23 | ▲ 09:30 equity $10,217.23 vs yday $10,217.23 (-0.00) | 09:30 open · cash $10,217.23 · no holdings · equity $10,217.23 vs prior close $10,217.23 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,217.23 | ▲ close $10,217.23 vs 09:30 $10,217.23 (session +0.00) | 16:00 close · cash $10,217.23 · no lots left · equity $10,217.23. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,217.23 | ▲ 09:30 equity $10,217.23 vs yday $10,217.23 (-0.00) | 09:30 open · cash $10,217.23 · no holdings · equity $10,217.23 vs prior close $10,217.23 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,941.05 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $7,685.70 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1277.15 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 517 | $2.47 | $6.67 | — | $6,402.04 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $5,166.66 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 79 | $16.00 | $2.23 | — | $3,900.43 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $2,697.30 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1277.15 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 48 | $26.57 | $2.13 | — | $1,419.80 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TEAM` | 7 | $173.90 | $2.01 | — | $200.49 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+12.2; leftover $1277.15 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $200.49 | ▼ close $10,060.73 vs 09:30 $10,217.23 (session -135.28) | 16:00 close · cash $200.49 · equity $10,060.73 vs 09:30 $10,217.23 (-156.50; session marks -135.28) · 8 name(s) marked open→close (per-name table). BHP×14 09:30 $91.01 → close $93.63 +36.68; APA×28 09:30 $44.76 → close $44.39 -10.36; AUTL×517 09:30 $2.47 → close $2.46 -5.17; CRSP×21 09:30 $58.73 → close $58.12 -12.81; ASST×79 09:30 $16.00 → close $16.13 +10.27; MRNA×8 09:30 $150.14 → close $133.32 -134.56; ZLAB×48 09:30 $26.57 → close $26.02 -26.40; TEAM×7 09:30 $173.90 → close $174.91 +7.07 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $200.49 | ▲ 09:30 equity $10,257.80 vs yday $10,060.73 (+197.07) | 09:30 open · cash $200.49 (unchanged overnight, no fees) · equity $10,257.80 vs prior close $10,060.73 (+197.07) · 8 name(s) re-marked at the open (per-name table). BHP×14 yday $93.63 → 09:30 $95.72 +29.26; APA×28 yday $44.39 → 09:30 $44.52 +3.64; AUTL×517 yday $2.46 → 09:30 $2.47 +5.17; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; ASST×79 yday $16.13 → 09:30 $17.66 +120.87; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; ZLAB×48 yday $26.02 → 09:30 $26.25 +11.04; TEAM×7 yday $174.91 → 09:30 $174.22 -4.83 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 14 | $95.72 | $2.05 | $+61.86 | $1,538.52 | ▲ +61.86 after sell → book $10,255.75; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $2,782.99 | ▼ -10.89 after sell → book $10,253.66; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 79 | $17.66 | $2.25 | $+126.66 | $4,175.87 | ▲ +126.66 after sell → book $10,251.40; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $5,238.72 | ▼ -140.29 after sell → book $10,249.37; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 48 | $26.25 | $2.15 | $-19.65 | $6,496.57 | ▼ -19.65 after sell → book $10,247.22; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TEAM` | 7 | $174.22 | $2.03 | $-1.80 | $7,714.08 | ▼ -1.80 after sell → book $10,245.19; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,517.76 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1285.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $5,248.75 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1285.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 16 | $78.88 | $2.04 | — | $3,984.63 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1285.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 148 | $8.66 | $2.43 | — | $2,700.52 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1285.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 396 | $3.24 | $5.11 | — | $1,412.37 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $1285.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 109 | $11.70 | $2.32 | — | $134.75 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1285.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $134.75 | ▼ close $10,075.96 vs 09:30 $10,257.80 (session -153.28) | 16:00 close · cash $134.75 · equity $10,075.96 vs 09:30 $10,257.80 (-181.84; session marks -153.28) · 8 name(s) marked open→close (per-name table). AUTL×517 09:30 $2.47 → close $2.41 -31.02; CRSP×21 09:30 $59.72 → close $59.50 -4.62; AU×10 09:30 $119.43 → close $121.22 +17.90; FUTU×11 09:30 $115.18 → close $123.64 +93.06; GRAL×16 09:30 $78.88 → close $79.54 +10.56; ABTC×148 09:30 $8.66 → close $7.93 -108.04; HIVE×396 09:30 $3.24 → close $3.03 -83.16; MARA×109 09:30 $11.70 → close $11.26 -47.96 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $134.75 | ▼ 09:30 equity $10,040.89 vs yday $10,075.96 (-35.07) | 09:30 open · cash $134.75 (unchanged overnight, no fees) · equity $10,040.89 vs prior close $10,075.96 (-35.07) · 8 name(s) re-marked at the open (per-name table). AUTL×517 yday $2.41 → 09:30 $2.40 -5.17; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; AU×10 yday $121.22 → 09:30 $120.51 -7.10; FUTU×11 yday $123.64 → 09:30 $121.00 -29.04; GRAL×16 yday $79.54 → 09:30 $81.87 +37.28; ABTC×148 yday $7.93 → 09:30 $8.00 +10.36; HIVE×396 yday $3.03 → 09:30 $2.99 -15.84; MARA×109 yday $11.26 → 09:30 $11.17 -9.81 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 517 | $2.40 | $6.77 | $-49.62 | $1,368.79 | ▼ -49.62 after sell → book $10,034.13; vs 09:30 mark -6.76 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $2,571.85 | ▲ +6.74 after sell → book $10,032.09; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $121.00 | $2.04 | $+59.95 | $3,900.81 | ▲ +59.95 after sell → book $10,030.05; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 16 | $81.87 | $2.06 | $+43.74 | $5,208.67 | ▲ +43.74 after sell → book $10,027.99; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 148 | $8.00 | $2.47 | $-102.58 | $6,390.20 | ▼ -102.58 after sell → book $10,025.52; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 396 | $2.99 | $5.18 | $-109.29 | $7,569.05 | ▼ -109.29 after sell → book $10,020.33; vs 09:30 mark -5.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 109 | $11.17 | $2.35 | $-62.43 | $8,784.24 | ▼ -62.43 after sell → book $10,017.99; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,784.24 | ▼ close $9,982.81 vs 09:30 $10,040.89 (session -35.17) | 16:00 close · cash $8,784.24 · equity $9,982.81 vs 09:30 $10,040.89 (-58.08; session marks -35.17) · 1 name(s) marked open→close (per-name table). CRSP×21 09:30 $58.75 → close $57.08 -35.17 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,784.24 | ▲ 09:30 equity $10,000.77 vs yday $9,982.81 (+17.96) | 09:30 open · cash $8,784.24 (unchanged overnight, no fees) · equity $10,000.77 vs prior close $9,982.81 (+17.96) · 1 name(s) re-marked at the open (per-name table). CRSP×21 yday $57.08 → 09:30 $57.93 +17.95 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-20.93 | $9,998.70 | ▼ -20.93 after sell → book $9,998.70; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 12 | $118.52 | $2.03 | — | $8,574.43 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1428.39 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 18 | $77.13 | $2.04 | — | $7,184.05 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1428.39 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 40 | $35.05 | $2.11 | — | $5,779.94 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1428.39 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 151 | $9.42 | $2.44 | — | $4,355.07 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1428.39 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 49 | $28.86 | $2.14 | — | $2,938.80 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+13.7; leftover $1428.39 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 59 | $24.11 | $2.17 | — | $1,514.14 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; ret5=+891.7; leftover $1428.39 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 163 | $8.72 | $2.48 | — | $90.30 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+13.0; leftover $1428.39 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.30 | ▲ close $10,449.62 vs 09:30 $10,000.77 (session +466.32) | 16:00 close · cash $90.30 · equity $10,449.62 vs 09:30 $10,000.77 (+448.85; session marks +466.32) · 7 name(s) marked open→close (per-name table). AU×12 09:30 $118.52 → close $123.39 +58.44; FCX×18 09:30 $77.13 → close $79.91 +50.04; EZPW×40 09:30 $35.05 → close $35.23 +7.20; RUM×151 09:30 $9.42 → close $10.23 +122.31; ZYME×49 09:30 $28.86 → close $27.47 -68.11; REAX×59 09:30 $24.11 → close $28.43 +254.88; EOLS×163 09:30 $8.72 → close $8.97 +41.56 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.30 | ▼ 09:30 equity $10,269.20 vs yday $10,449.62 (-180.42) | 09:30 open · cash $90.30 (unchanged overnight, no fees) · equity $10,269.20 vs prior close $10,449.62 (-180.42) · 7 name(s) re-marked at the open (per-name table). AU×12 yday $123.39 → 09:30 $119.80 -43.08; FCX×18 yday $79.91 → 09:30 $79.34 -10.26; EZPW×40 yday $35.23 → 09:30 $35.70 +18.80; RUM×151 yday $10.23 → 09:30 $10.07 -24.16; ZYME×49 yday $27.47 → 09:30 $27.56 +4.41; REAX×59 yday $28.43 → 09:30 $26.61 -107.38; EOLS×163 yday $8.97 → 09:30 $8.86 -18.75 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 12 | $119.80 | $2.05 | $+11.29 | $1,525.85 | ▲ +11.29 after sell → book $10,267.15; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 18 | $79.34 | $2.07 | $+35.67 | $2,951.91 | ▲ +35.67 after sell → book $10,265.09; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 40 | $35.70 | $2.13 | $+21.76 | $4,377.78 | ▲ +21.76 after sell → book $10,262.96; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 151 | $10.07 | $2.48 | $+93.23 | $5,895.87 | ▲ +93.23 after sell → book $10,260.48; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 49 | $27.56 | $2.16 | $-67.99 | $7,244.15 | ▼ -67.99 after sell → book $10,258.32; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 59 | $26.61 | $2.19 | $+143.14 | $8,811.95 | ▲ +143.14 after sell → book $10,256.13; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 163 | $8.86 | $2.52 | $+17.82 | $10,253.61 | ▲ +17.82 after sell → book $10,253.61; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 7 | $267.02 | $2.01 | — | $8,382.46 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $2050.72 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 182 | $11.22 | $2.54 | — | $6,337.88 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+16.8; leftover $2050.72 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 247 | $8.29 | $3.19 | — | $4,287.07 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $2050.72 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 117 | $17.41 | $2.34 | — | $2,247.76 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; ret5=-9.2; leftover $2050.72 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 184 | $11.12 | $2.54 | — | $199.14 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $2050.72 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $199.14 | ▲ close $10,564.34 vs 09:30 $10,269.20 (session +323.34) | 16:00 close · cash $199.14 · equity $10,564.34 vs 09:30 $10,269.20 (+295.14; session marks +323.34) · 5 name(s) marked open→close (per-name table). FNV×7 09:30 $267.02 → close $267.37 +2.45; TRLV×182 09:30 $11.22 → close $11.43 +38.22; CAPR×247 09:30 $8.29 → close $9.36 +264.29; FWRD×117 09:30 $17.41 → close $17.63 +25.74; FLNC×184 09:30 $11.12 → close $11.08 -7.36 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $199.14 | ▲ 09:30 equity $10,589.72 vs yday $10,564.34 (+25.38) | 09:30 open · cash $199.14 (unchanged overnight, no fees) · equity $10,589.72 vs prior close $10,564.34 (+25.38) · 5 name(s) re-marked at the open (per-name table). FNV×7 yday $267.37 → 09:30 $267.23 -0.98; TRLV×182 yday $11.43 → 09:30 $11.38 -9.10; CAPR×247 yday $9.36 → 09:30 $9.19 -41.99; FWRD×117 yday $17.63 → 09:30 $17.60 -3.51; FLNC×184 yday $11.08 → 09:30 $11.52 +80.96 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 7 | $267.23 | $2.04 | $-2.58 | $2,067.71 | ▼ -2.58 after sell → book $10,587.68; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 182 | $11.38 | $2.58 | $+24.00 | $4,136.29 | ▲ +24.00 after sell → book $10,585.10; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 247 | $9.19 | $3.25 | $+215.87 | $6,402.97 | ▲ +215.87 after sell → book $10,581.85; vs 09:30 mark -3.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FWRD` | 117 | $17.60 | $2.38 | $+17.51 | $8,459.79 | ▲ +17.51 after sell → book $10,579.47; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 184 | $11.52 | $2.59 | $+68.47 | $10,576.88 | ▲ +68.47 after sell → book $10,576.88; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 21 | $81.65 | $2.05 | — | $8,860.18 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1762.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $7,891.18 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1762.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ASML` | 1 | $1746.53 | $1.99 | — | $6,142.66 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=-0.3; leftover $1762.81 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 5 | $318.88 | $2.00 | — | $4,546.25 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1762.81 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 7 | $222.86 | $2.01 | — | $2,984.22 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $1762.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 42 | $41.44 | $2.12 | — | $1,241.62 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; ret5=+3.1; leftover $1762.81 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,241.62 | ▼ close $10,539.95 vs 09:30 $10,589.72 (session -24.76) | 16:00 close · cash $1,241.62 · equity $10,539.95 vs 09:30 $10,589.72 (-49.77; session marks -24.76) · 6 name(s) marked open→close (per-name table). ACMR×21 09:30 $81.65 → close $80.49 -24.36; MU×1 09:30 $967.01 → close $935.39 -31.62; ASML×1 09:30 $1746.53 → close $1735.01 -11.52; LRCX×5 09:30 $318.88 → close $318.58 -1.50; NVDA×7 09:30 $222.86 → close $227.98 +35.84; RRC×42 09:30 $41.44 → close $41.64 +8.40 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,241.62 | ▼ 09:30 equity $10,495.08 vs yday $10,539.95 (-44.87) | 09:30 open · cash $1,241.62 (unchanged overnight, no fees) · equity $10,495.08 vs prior close $10,539.95 (-44.87) · 6 name(s) re-marked at the open (per-name table). ACMR×21 yday $80.49 → 09:30 $79.27 -25.62; MU×1 yday $935.39 → 09:30 $919.29 -16.10; ASML×1 yday $1735.01 → 09:30 $1734.75 -0.26; LRCX×5 yday $318.58 → 09:30 $318.03 -2.75; NVDA×7 yday $227.98 → 09:30 $227.36 -4.34; RRC×42 yday $41.64 → 09:30 $41.74 +4.20 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 21 | $79.27 | $2.08 | $-54.11 | $2,904.22 | ▼ -54.11 after sell → book $10,493.01; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $3,821.49 | ▼ -51.73 after sell → book $10,490.99; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ASML` | 1 | $1734.75 | $2.02 | $-15.79 | $5,554.23 | ▼ -15.79 after sell → book $10,488.98; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 5 | $318.03 | $2.03 | $-8.28 | $7,142.35 | ▼ -8.28 after sell → book $10,486.95; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 7 | $227.36 | $2.03 | $+27.46 | $8,731.84 | ▲ +27.46 after sell → book $10,484.92; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 42 | $41.74 | $2.14 | $+8.34 | $10,482.78 | ▲ +8.34 after sell → book $10,482.78; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $9,183.13 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1310.35 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $7,905.28 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1310.35 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,702.02 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1310.35 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $5,393.99 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1310.35 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $4,190.89 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1310.35 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $2,883.08 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; ret5=+7.8; leftover $1310.35 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $32.90 | $2.11 | — | $1,597.88 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1310.35 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 271 | $4.82 | $3.50 | — | $288.16 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1310.35 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $288.16 | ▼ close $10,151.43 vs 09:30 $10,495.08 (session -313.72) | 16:00 close · cash $288.16 · equity $10,151.43 vs 09:30 $10,495.08 (-343.65; session marks -313.72) · 8 name(s) marked open→close (per-name table). KEYS×4 09:30 $324.41 → close $319.97 -17.76; SMTC×9 09:30 $141.76 → close $131.17 -95.31; CIEN×3 09:30 $400.42 → close $378.44 -65.94; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×5 09:30 $240.22 → close $236.98 -16.20; ADSK×5 09:30 $261.16 → close $260.66 -2.50; SEDG×39 09:30 $32.90 → close $31.41 -58.11; TLS×271 09:30 $4.82 → close $4.79 -8.13 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $288.16 | ▼ 09:30 equity $10,142.78 vs yday $10,151.43 (-8.65) | 09:30 open · cash $288.16 (unchanged overnight, no fees) · equity $10,142.78 vs prior close $10,151.43 (-8.65) · 8 name(s) re-marked at the open (per-name table). KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×5 yday $236.98 → 09:30 $233.97 -15.07; ADSK×5 yday $260.66 → 09:30 $257.71 -14.75; SEDG×39 yday $31.41 → 09:30 $31.15 -10.14; TLS×271 yday $4.79 → 09:30 $4.81 +5.42 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $1,576.10 | ▼ -11.70 after sell → book $10,140.75; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $2,764.76 | ▼ -89.19 after sell → book $10,138.72; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $3,898.06 | ▼ -69.96 after sell → book $10,136.70; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $5,157.95 | ▼ -48.14 after sell → book $10,134.69; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 5 | $233.97 | $2.02 | $-35.31 | $6,325.75 | ▼ -35.31 after sell → book $10,132.66; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 5 | $257.71 | $2.03 | $-21.28 | $7,612.28 | ▼ -21.28 after sell → book $10,130.64; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 39 | $31.15 | $2.13 | $-72.48 | $8,825.00 | ▼ -72.48 after sell → book $10,128.51; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 271 | $4.81 | $3.55 | $-9.76 | $10,124.96 | ▼ -9.76 after sell → book $10,124.96; vs 09:30 mark -3.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,124.96 | ▲ close $10,124.96 vs 09:30 $10,142.78 (session +0.00) | 16:00 close · cash $10,124.96 · no lots left · equity $10,124.96. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,124.96 | ▲ 09:30 equity $10,124.96 vs yday $10,124.96 (-0.00) | 09:30 open · cash $10,124.96 · no holdings · equity $10,124.96 vs prior close $10,124.96 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,124.96 | ▲ close $10,124.96 vs 09:30 $10,124.96 (session +0.00) | 16:00 close · cash $10,124.96 · no lots left · equity $10,124.96. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,124.96 | ▲ 09:30 equity $10,124.96 vs yday $10,124.96 (-0.00) | 09:30 open · cash $10,124.96 · no holdings · equity $10,124.96 vs prior close $10,124.96 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,124.96 | ▲ close $10,124.96 vs 09:30 $10,124.96 (session +0.00) | 16:00 close · cash $10,124.96 · no lots left · equity $10,124.96. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,124.96 | ▲ 09:30 equity $10,124.96 vs yday $10,124.96 (-0.00) | 09:30 open · cash $10,124.96 · no holdings · equity $10,124.96 vs prior close $10,124.96 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $9,067.74 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1265.62 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $8,093.12 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1265.62 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 39 | $32.31 | $2.11 | — | $6,830.92 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1265.62 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 79 | $15.87 | $2.23 | — | $5,574.97 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1265.62 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 52 | $23.88 | $2.15 | — | $4,331.06 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1265.62 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $3,625.82 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1265.62 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 26 | $47.60 | $2.07 | — | $2,386.15 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1265.62 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 38 | $32.88 | $2.10 | — | $1,134.61 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ret5=+16.2; leftover $1265.62 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,134.61 | ▲ close $10,484.54 vs 09:30 $10,124.96 (session +376.22) | 16:00 close · cash $1,134.61 · equity $10,484.54 vs 09:30 $10,124.96 (+359.58; session marks +376.22) · 8 name(s) marked open→close (per-name table). AVGO×3 09:30 $351.74 → close $357.16 +16.26; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×39 09:30 $32.31 → close $33.66 +52.65; FRNM×79 09:30 $15.87 → close $16.90 +81.37; MMED×52 09:30 $23.88 → close $23.84 -2.08; DE×1 09:30 $703.25 → close $694.41 -8.84; HPE×26 09:30 $47.60 → close $54.44 +177.84; CNXC×38 09:30 $32.88 → close $32.85 -1.14 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,134.61 | ▼ 09:30 equity $10,407.86 vs yday $10,484.54 (-76.68) | 09:30 open · cash $1,134.61 (unchanged overnight, no fees) · equity $10,407.86 vs prior close $10,484.54 (-76.68) · 8 name(s) re-marked at the open (per-name table). AVGO×3 yday $357.16 → 09:30 $359.70 +7.62; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×39 yday $33.66 → 09:30 $33.46 -7.80; FRNM×79 yday $16.90 → 09:30 $16.40 -39.50; MMED×52 yday $23.84 → 09:30 $23.84 +0.00; DE×1 yday $694.41 → 09:30 $692.03 -2.38; HPE×26 yday $54.44 → 09:30 $53.85 -15.34; CNXC×38 yday $32.85 → 09:30 $32.48 -14.06 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $2,211.69 | ▲ +19.86 after sell → book $10,405.84; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $3,237.23 | ▲ +50.93 after sell → book $10,403.82; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 39 | $33.46 | $2.13 | $+40.62 | $4,540.04 | ▲ +40.62 after sell → book $10,401.69; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 52 | $23.84 | $2.17 | $-6.39 | $5,777.56 | ▼ -6.39 after sell → book $10,399.53; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $6,467.58 | ▼ -15.23 after sell → book $10,397.52; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 26 | $53.85 | $2.09 | $+158.34 | $7,865.59 | ▲ +158.34 after sell → book $10,395.43; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 38 | $32.48 | $2.12 | $-19.43 | $9,097.70 | ▼ -19.43 after sell → book $10,393.30; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 6 | $263.36 | $2.01 | — | $7,515.53 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1819.54 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 24 | $75.65 | $2.06 | — | $5,697.87 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1819.54 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 7 | $236.82 | $2.01 | — | $4,038.12 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+8.1; leftover $1819.54 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 937 | $1.94 | $12.09 | — | $2,208.25 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $1819.54 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 13 | $137.35 | $2.03 | — | $420.67 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+5.4; leftover $1819.54 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $420.67 | ▲ close $10,540.44 vs 09:30 $10,407.86 (session +167.34) | 16:00 close · cash $420.67 · equity $10,540.44 vs 09:30 $10,407.86 (+132.58; session marks +167.34) · 6 name(s) marked open→close (per-name table). FRNM×79 09:30 $16.40 → close $16.31 -7.11; CRM×6 09:30 $263.36 → close $259.23 -24.78; MRX×24 09:30 $75.65 → close $78.27 +62.88; BE×7 09:30 $236.82 → close $252.87 +112.35; BAK×937 09:30 $1.94 → close $1.89 -46.85; MSTR×13 09:30 $137.35 → close $142.80 +70.85 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $420.67 | ▲ 09:30 equity $10,638.77 vs yday $10,540.44 (+98.33) | 09:30 open · cash $420.67 (unchanged overnight, no fees) · equity $10,638.77 vs prior close $10,540.44 (+98.33) · 6 name(s) re-marked at the open (per-name table). FRNM×79 yday $16.31 → 09:30 $16.74 +33.97; CRM×6 yday $259.23 → 09:30 $253.72 -33.06; MRX×24 yday $78.27 → 09:30 $78.84 +13.68; BE×7 yday $252.87 → 09:30 $267.76 +104.23; BAK×937 yday $1.89 → 09:30 $1.94 +46.85; MSTR×13 yday $142.80 → 09:30 $137.62 -67.34 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 79 | $16.74 | $2.25 | $+64.25 | $1,740.88 | ▲ +64.25 after sell → book $10,636.52; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 6 | $253.72 | $2.03 | $-61.88 | $3,261.17 | ▼ -61.88 after sell → book $10,634.49; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 7 | $267.76 | $2.04 | $+212.53 | $5,133.46 | ▲ +212.53 after sell → book $10,632.46; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 937 | $1.94 | $12.26 | $-24.34 | $6,938.98 | ▼ -24.34 after sell → book $10,620.20; vs 09:30 mark -12.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,938.98 | ▼ close $10,554.78 vs 09:30 $10,638.77 (session -65.42) | 16:00 close · cash $6,938.98 · equity $10,554.78 vs 09:30 $10,638.77 (-83.99; session marks -65.42) · 2 name(s) marked open→close (per-name table). MRX×24 09:30 $78.84 → close $76.71 -51.12; MSTR×13 09:30 $137.62 → close $136.52 -14.30 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,938.98 | ▲ 09:30 equity $10,621.04 vs yday $10,554.78 (+66.26) | 09:30 open · cash $6,938.98 (unchanged overnight, no fees) · equity $10,621.04 vs prior close $10,554.78 (+66.26) · 2 name(s) re-marked at the open (per-name table). MRX×24 yday $76.71 → 09:30 $76.60 -2.64; MSTR×13 yday $136.52 → 09:30 $141.82 +68.90 | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 24 | $76.60 | $2.09 | $+18.65 | $8,775.29 | ▲ +18.65 after sell → book $10,618.95; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 13 | $141.82 | $2.05 | $+54.03 | $10,616.90 | ▲ +54.03 after sell → book $10,616.90; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,616.90 | ▲ close $10,616.90 vs 09:30 $10,621.04 (session +0.00) | 16:00 close · cash $10,616.90 · no lots left · equity $10,616.90. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,616.90 | ▲ 09:30 equity $10,616.90 vs yday $10,616.90 (+0.00) | 09:30 open · cash $10,616.90 · no holdings · equity $10,616.90 vs prior close $10,616.90 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,616.90 | ▲ close $10,616.90 vs 09:30 $10,616.90 (session +0.00) | 16:00 close · cash $10,616.90 · no lots left · equity $10,616.90. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,616.90 | ▲ 09:30 equity $10,616.90 vs yday $10,616.90 (+0.00) | 09:30 open · cash $10,616.90 · no holdings · equity $10,616.90 vs prior close $10,616.90 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 12 | $164.43 | $2.03 | — | $8,641.71 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $2123.38 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 8 | $242.17 | $2.01 | — | $6,702.34 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; ret5=-11.1; leftover $2123.38 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 1001 | $2.12 | $12.91 | — | $4,567.31 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $2123.38 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 1040 | $2.04 | $13.42 | — | $2,432.29 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $2123.38 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 15 | $135.71 | $2.04 | — | $394.61 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; ret5=-9.2; leftover $2123.38 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $394.61 | ▼ close $10,399.34 vs 09:30 $10,616.90 (session -185.16) | 16:00 close · cash $394.61 · equity $10,399.34 vs 09:30 $10,616.90 (-217.56; session marks -185.16) · 5 name(s) marked open→close (per-name table). ORCL×12 09:30 $164.43 → close $150.28 -169.80; ADBE×8 09:30 $242.17 → close $252.23 +80.48; BAK×1001 09:30 $2.12 → close $2.08 -40.04; AMTX×1040 09:30 $2.04 → close $2.01 -31.20; RH×15 09:30 $135.71 → close $134.07 -24.60 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $394.61 | ▼ 09:30 equity $10,297.18 vs yday $10,399.34 (-102.16) | 09:30 open · cash $394.61 (unchanged overnight, no fees) · equity $10,297.18 vs prior close $10,399.34 (-102.16) · 5 name(s) re-marked at the open (per-name table). ORCL×12 yday $150.28 → 09:30 $141.42 -106.32; ADBE×8 yday $252.23 → 09:30 $261.51 +74.24; BAK×1001 yday $2.08 → 09:30 $2.05 -30.03; AMTX×1040 yday $2.01 → 09:30 $2.01 +0.00; RH×15 yday $134.07 → 09:30 $131.40 -40.05 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 12 | $141.42 | $2.05 | $-280.20 | $2,089.60 | ▼ -280.20 after sell → book $10,295.13; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 8 | $261.51 | $2.04 | $+150.67 | $4,179.64 | ▲ +150.67 after sell → book $10,293.09; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 1001 | $2.05 | $13.10 | $-96.08 | $6,218.59 | ▼ -96.08 after sell → book $10,279.99; vs 09:30 mark -13.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 1040 | $2.01 | $13.61 | $-58.22 | $8,295.39 | ▼ -58.22 after sell → book $10,266.39; vs 09:30 mark -13.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 15 | $131.40 | $2.06 | $-68.75 | $10,264.32 | ▼ -68.75 after sell → book $10,264.32; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,264.32 | ▲ close $10,264.32 vs 09:30 $10,297.18 (session +0.00) | 16:00 close · cash $10,264.32 · no lots left · equity $10,264.32. | — |

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
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
