# Factor mine action — `union_news_g_cond_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · merged news🟢, rank +G−R

Cash book **+2.33%** ($10,233) · signal-only (no cash/fees) was +13.46%. Starts YES **14/22**. Fills 136 · skips 63 · realized $+233.30.

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
- **Gate** `news=good` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,233.32.

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
| 2026-08-20 | `HUMA` | 1806 | — | $0.71 | +0.00 | $0.68 | -46.96 | -46.96 | +0.00 | -46.96 |
| 2026-08-21 | `BHP` | 14 | $93.63 | $95.72 | +29.26 | — | +0.00 | +29.26 | +65.94 | — |
| 2026-08-21 | `APA` | 28 | $44.39 | $44.52 | +3.64 | — | +0.00 | +3.64 | -6.72 | — |
| 2026-08-21 | `AUTL` | 517 | $2.46 | $2.47 | +5.17 | $2.41 | -31.02 | -25.85 | +0.00 | -31.02 |
| 2026-08-21 | `CRSP` | 21 | $58.12 | $59.72 | +33.60 | $59.50 | -4.62 | +28.98 | +20.79 | +16.17 |
| 2026-08-21 | `ASST` | 79 | $16.13 | $17.66 | +120.87 | — | +0.00 | +120.87 | +131.14 | — |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | — | +0.00 | -1.68 | -136.24 | — |
| 2026-08-21 | `ZLAB` | 48 | $26.02 | $26.25 | +11.04 | — | +0.00 | +11.04 | -15.36 | — |
| 2026-08-21 | `HUMA` | 1806 | $0.68 | $0.67 | -12.64 | — | +0.00 | -12.64 | -59.60 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `FUTU` | 11 | — | $115.18 | +0.00 | $123.64 | +93.06 | +93.06 | +0.00 | +93.06 |
| 2026-08-21 | `GRAL` | 16 | — | $78.88 | +0.00 | $79.54 | +10.56 | +10.56 | +0.00 | +10.56 |
| 2026-08-21 | `ABTC` | 146 | — | $8.66 | +0.00 | $7.93 | -106.58 | -106.58 | +0.00 | -106.58 |
| 2026-08-21 | `HIVE` | 391 | — | $3.24 | +0.00 | $3.03 | -82.11 | -82.11 | +0.00 | -82.11 |
| 2026-08-21 | `MARA` | 108 | — | $11.70 | +0.00 | $11.26 | -47.52 | -47.52 | +0.00 | -47.52 |
| 2026-08-24 | `AUTL` | 517 | $2.41 | $2.40 | -5.17 | — | +0.00 | -5.17 | -36.19 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | $57.08 | -35.17 | -50.92 | +0.42 | -34.75 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `FUTU` | 11 | $123.64 | $121.00 | -29.04 | — | +0.00 | -29.04 | +64.02 | — |
| 2026-08-24 | `GRAL` | 16 | $79.54 | $81.87 | +37.28 | — | +0.00 | +37.28 | +47.84 | — |
| 2026-08-24 | `ABTC` | 146 | $7.93 | $8.00 | +10.22 | — | +0.00 | +10.22 | -96.36 | — |
| 2026-08-24 | `HIVE` | 391 | $3.03 | $2.99 | -15.64 | — | +0.00 | -15.64 | -97.75 | — |
| 2026-08-24 | `MARA` | 108 | $11.26 | $11.17 | -9.72 | — | +0.00 | -9.72 | -57.24 | — |
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
| 2026-08-26 | `FWRD` | 116 | — | $17.41 | +0.00 | $17.63 | +25.52 | +25.52 | +0.00 | +25.52 |
| 2026-08-26 | `FLNC` | 182 | — | $11.12 | +0.00 | $11.08 | -7.28 | -7.28 | +0.00 | -7.28 |
| 2026-08-27 | `FNV` | 7 | $267.37 | $267.23 | -0.98 | — | +0.00 | -0.98 | +1.47 | — |
| 2026-08-27 | `TRLV` | 181 | $11.43 | $11.38 | -9.05 | — | +0.00 | -9.05 | +28.96 | — |
| 2026-08-27 | `CAPR` | 245 | $9.36 | $9.19 | -41.65 | — | +0.00 | -41.65 | +220.50 | — |
| 2026-08-27 | `FWRD` | 116 | $17.63 | $17.60 | -3.48 | — | +0.00 | -3.48 | +22.04 | — |
| 2026-08-27 | `FLNC` | 182 | $11.08 | $11.52 | +80.08 | — | +0.00 | +80.08 | +72.80 | — |
| 2026-08-27 | `ACMR` | 21 | — | $81.65 | +0.00 | $80.49 | -24.36 | -24.36 | +0.00 | -24.36 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `LRCX` | 5 | — | $318.88 | +0.00 | $318.58 | -1.50 | -1.50 | +0.00 | -1.50 |
| 2026-08-27 | `NVDA` | 7 | — | $222.86 | +0.00 | $227.98 | +35.84 | +35.84 | +0.00 | +35.84 |
| 2026-08-27 | `RRC` | 42 | — | $41.44 | +0.00 | $41.64 | +8.40 | +8.40 | +0.00 | +8.40 |
| 2026-08-28 | `ACMR` | 21 | $80.49 | $79.27 | -25.62 | — | +0.00 | -25.62 | -49.98 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `LRCX` | 5 | $318.58 | $318.03 | -2.75 | — | +0.00 | -2.75 | -4.25 | — |
| 2026-08-28 | `NVDA` | 7 | $227.98 | $227.36 | -4.34 | — | +0.00 | -4.34 | +31.50 | — |
| 2026-08-28 | `RRC` | 42 | $41.64 | $41.74 | +4.20 | — | +0.00 | +4.20 | +12.60 | — |
| 2026-08-28 | `KEYS` | 4 | — | $324.41 | +0.00 | $319.97 | -17.76 | -17.76 | +0.00 | -17.76 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `DDOG` | 5 | — | $240.22 | +0.00 | $236.98 | -16.20 | -16.20 | +0.00 | -16.20 |
| 2026-08-28 | `ADSK` | 4 | — | $261.16 | +0.00 | $260.66 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-08-28 | `SEDG` | 39 | — | $32.90 | +0.00 | $31.41 | -58.11 | -58.11 | +0.00 | -58.11 |
| 2026-08-28 | `TLS` | 269 | — | $4.82 | +0.00 | $4.79 | -8.07 | -8.07 | +0.00 | -8.07 |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | — | +0.00 | +10.08 | -7.68 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -65.94 | — |
| 2026-08-31 | `DDOG` | 5 | $236.98 | $233.97 | -15.07 | — | +0.00 | -15.07 | -31.27 | — |
| 2026-08-31 | `ADSK` | 4 | $260.66 | $257.71 | -11.80 | — | +0.00 | -11.80 | -13.80 | — |
| 2026-08-31 | `SEDG` | 39 | $31.41 | $31.15 | -10.14 | — | +0.00 | -10.14 | -68.25 | — |
| 2026-08-31 | `TLS` | 269 | $4.79 | $4.81 | +5.38 | — | +0.00 | +5.38 | -2.69 | — |
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
| 2026-09-04 | `MRX` | 23 | — | $75.65 | +0.00 | $78.27 | +60.26 | +60.26 | +0.00 | +60.26 |
| 2026-09-04 | `BE` | 7 | — | $236.82 | +0.00 | $252.87 | +112.35 | +112.35 | +0.00 | +112.35 |
| 2026-09-04 | `BAK` | 934 | — | $1.94 | +0.00 | $1.89 | -46.70 | -46.70 | +0.00 | -46.70 |
| 2026-09-04 | `MSTR` | 13 | — | $137.35 | +0.00 | $142.80 | +70.85 | +70.85 | +0.00 | +70.85 |
| 2026-09-08 | `FRNM` | 79 | $16.31 | $16.74 | +33.97 | — | +0.00 | +33.97 | +68.73 | — |
| 2026-09-08 | `CRM` | 6 | $259.23 | $253.72 | -33.06 | — | +0.00 | -33.06 | -57.84 | — |
| 2026-09-08 | `MRX` | 23 | $78.27 | $78.84 | +13.11 | $76.71 | -48.99 | -35.88 | +73.37 | +24.38 |
| 2026-09-08 | `BE` | 7 | $252.87 | $267.76 | +104.23 | — | +0.00 | +104.23 | +216.58 | — |
| 2026-09-08 | `BAK` | 934 | $1.89 | $1.94 | +46.70 | — | +0.00 | +46.70 | +0.00 | — |
| 2026-09-08 | `MSTR` | 13 | $142.80 | $137.62 | -67.34 | $136.52 | -14.30 | -81.64 | +3.51 | -10.79 |
| 2026-09-09 | `MRX` | 23 | $76.71 | $76.60 | -2.53 | — | +0.00 | -2.53 | +21.85 | — |
| 2026-09-09 | `MSTR` | 13 | $136.52 | $141.82 | +68.90 | — | +0.00 | +68.90 | +58.11 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 12 | — | $164.43 | +0.00 | $150.28 | -169.80 | -169.80 | +0.00 | -169.80 |
| 2026-09-11 | `ADBE` | 8 | — | $242.17 | +0.00 | $252.23 | +80.48 | +80.48 | +0.00 | +80.48 |
| 2026-09-11 | `BAK` | 998 | — | $2.12 | +0.00 | $2.08 | -39.92 | -39.92 | +0.00 | -39.92 |
| 2026-09-11 | `AMTX` | 1037 | — | $2.04 | +0.00 | $2.01 | -31.11 | -31.11 | +0.00 | -31.11 |
| 2026-09-11 | `RH` | 15 | — | $135.71 | +0.00 | $134.07 | -24.60 | -24.60 | +0.00 | -24.60 |
| 2026-09-14 | `ORCL` | 12 | $150.28 | $141.42 | -106.32 | — | +0.00 | -106.32 | -276.12 | — |
| 2026-09-14 | `ADBE` | 8 | $252.23 | $261.51 | +74.24 | — | +0.00 | +74.24 | +154.72 | — |
| 2026-09-14 | `BAK` | 998 | $2.08 | $2.05 | -29.94 | — | +0.00 | -29.94 | -69.86 | — |
| 2026-09-14 | `AMTX` | 1037 | $2.01 | $2.01 | +0.00 | — | +0.00 | +0.00 | -31.11 | — |
| 2026-09-14 | `RH` | 15 | $134.07 | $131.40 | -40.05 | — | +0.00 | -40.05 | -64.65 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +127.16 | ANGX, ARX, HLIT, MH, NRG, TLN, VST | — | $1,560.49 | $10,110.67 | ANGX×290, ARX×63, HLIT×94, MH×92, NRG×10, TLN×3, VST×8 |
| 2026-08-17 | +2.25 | $1,560.49 | ANGX×290, ARX×63, HLIT×94, MH×92, NRG×10, TLN×3, VST×8 | $10,211.68 | +101.01 | +97.16 | DVN, EOG, FANG, OUST, CELC | ANGX, ARX, HLIT, MH, NRG, TLN, VST | $165.17 | $10,281.82 | DVN×44, EOG×14, FANG×10, OUST×41, CELC×21 |
| 2026-08-18 | -6.20 | $165.17 | DVN×44, EOG×14, FANG×10, OUST×41, CELC×21 | $10,227.70 | -54.12 | +0.00 | — | DVN, EOG, FANG, OUST, CELC | $10,217.23 | $10,217.23 | — |
| 2026-08-19 | -7.20 | $10,217.23 | — | $10,217.23 | -0.00 | +0.00 | — | — | $10,217.23 | $10,217.23 | — |
| 2026-08-20 | +1.12 | $10,217.23 | — | $10,217.23 | -0.00 | -189.31 | BHP, APA, AUTL, CRSP, ASST, MRNA, ZLAB, HUMA | — | $124.77 | $9,990.53 | BHP×14, APA×28, AUTL×517, CRSP×21, ASST×79, MRNA×8, ZLAB×48, HUMA×1806 |
| 2026-08-21 | +3.25 | $124.77 | BHP×14, APA×28, AUTL×517, CRSP×21, ASST×79, MRNA×8, ZLAB×48, HUMA×1806 | $10,179.79 | +189.26 | -150.33 | AU, FUTU, GRAL, ABTC, HIVE, MARA | BHP, APA, ASST, MRNA, ZLAB, HUMA | $86.17 | $9,985.11 | AUTL×517, CRSP×21, AU×10, FUTU×11, GRAL×16, ABTC×146, HIVE×391, MARA×108 |
| 2026-08-24 | -5.17 | $86.17 | AUTL×517, CRSP×21, AU×10, FUTU×11, GRAL×16, ABTC×146, HIVE×391, MARA×108 | $9,950.19 | -34.92 | -35.17 | — | AUTL, AU, FUTU, GRAL, ABTC, HIVE, MARA | $8,693.61 | $9,892.18 | CRSP×21 |
| 2026-08-25 | +1.80 | $8,693.61 | CRSP×21 | $9,910.14 | +17.96 | +456.07 | AU, FCX, EZPW, RUM, ZYME, REAX, EOLS | CRSP | $160.45 | $10,348.74 | AU×11, FCX×18, EZPW×40, RUM×150, ZYME×49, REAX×58, EOLS×162 |
| 2026-08-26 | +2.02 | $160.45 | AU×11, FCX×18, EZPW×40, RUM×150, ZYME×49, REAX×58, EOLS×162 | $10,174.01 | -174.73 | +320.85 | FNV, TRLV, CAPR, FWRD, FLNC | AU, FCX, EZPW, RUM, ZYME, REAX, EOLS | $171.44 | $10,466.70 | FNV×7, TRLV×181, CAPR×245, FWRD×116, FLNC×182 |
| 2026-08-27 | — | $171.44 | FNV×7, TRLV×181, CAPR×245, FWRD×116, FLNC×182 | $10,491.62 | +24.92 | -13.24 | ACMR, MU, LRCX, NVDA, RRC | FNV, TRLV, CAPR, FWRD, FLNC | $2,892.10 | $10,455.42 | ACMR×21, MU×1, LRCX×5, NVDA×7, RRC×42 |
| 2026-08-28 | +0.75 | $2,892.10 | ACMR×21, MU×1, LRCX×5, NVDA×7, RRC×42 | $10,410.81 | -44.61 | -263.39 | KEYS, SMTC, CIEN, DDOG, ADSK, SEDG, TLS | ACMR, MU, LRCX, NVDA, RRC | $1,784.75 | $10,121.52 | KEYS×4, SMTC×9, CIEN×3, DDOG×5, ADSK×4, SEDG×39, TLS×269 |
| 2026-08-31 | -5.85 | $1,784.75 | KEYS×4, SMTC×9, CIEN×3, DDOG×5, ADSK×4, SEDG×39, TLS×269 | $10,110.14 | -11.38 | +0.00 | — | KEYS, SMTC, CIEN, DDOG, ADSK, SEDG, TLS | $10,094.36 | $10,094.36 | — |
| 2026-09-01 | -6.30 | $10,094.36 | — | $10,094.36 | -0.00 | +0.00 | — | — | $10,094.36 | $10,094.36 | — |
| 2026-09-02 | -3.83 | $10,094.36 | — | $10,094.36 | -0.00 | +0.00 | — | — | $10,094.36 | $10,094.36 | — |
| 2026-09-03 | -0.90 | $10,094.36 | — | $10,094.36 | -0.00 | +376.22 | AVGO, DELL, CXW, FRNM, MMED, DE, HPE, CNXC | — | $1,104.01 | $10,453.94 | AVGO×3, DELL×2, CXW×39, FRNM×79, MMED×52, DE×1, HPE×26, CNXC×38 |
| 2026-09-04 | +2.25 | $1,104.01 | AVGO×3, DELL×2, CXW×39, FRNM×79, MMED×52, DE×1, HPE×26, CNXC×38 | $10,377.26 | -76.68 | +164.87 | CRM, MRX, BE, BAK, MSTR | AVGO, DELL, CXW, MMED, DE, HPE, CNXC | $471.59 | $10,507.42 | FRNM×79, CRM×6, MRX×23, BE×7, BAK×934, MSTR×13 |
| 2026-09-08 | -11.47 | $471.59 | FRNM×79, CRM×6, MRX×23, BE×7, BAK×934, MSTR×13 | $10,605.03 | +97.61 | -63.29 | — | FRNM, CRM, BE, BAK | $6,984.11 | $10,523.20 | MRX×23, MSTR×13 |
| 2026-09-09 | -13.95 | $6,984.11 | MRX×23, MSTR×13 | $10,589.57 | +66.37 | +0.00 | — | MRX, MSTR | $10,585.44 | $10,585.44 | — |
| 2026-09-10 | -13.28 | $10,585.44 | — | $10,585.44 | -0.00 | +0.00 | — | — | $10,585.44 | $10,585.44 | — |
| 2026-09-11 | +0.50 | $10,585.44 | — | $10,585.44 | -0.00 | -184.95 | ORCL, ADBE, BAK, AMTX, RH | — | $375.70 | $10,368.16 | ORCL×12, ADBE×8, BAK×998, AMTX×1037, RH×15 |
| 2026-09-14 | -11.00 | $375.70 | ORCL×12, ADBE×8, BAK×998, AMTX×1037, RH×15 | $10,266.09 | -102.07 | +0.00 | — | ORCL, ADBE, BAK, AMTX, RH | $10,233.32 | $10,233.32 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $8,746.36 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $7,511.27 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $6,270.08 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $5,021.21 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $3,819.19 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $2,737.70 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $1,560.49 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,560.49 | ▲ close $10,110.67 vs 09:30 $10,000.00 (session +127.16) | 16:00 close · cash $1,560.49 · equity $10,110.67 vs 09:30 $10,000.00 (+110.67; session marks +127.16) · 7 name(s) marked open→close (per-name table). ANGX×290 09:30 $4.31 → close $4.37 +17.40; ARX×63 09:30 $19.57 → close $19.58 +0.63; HLIT×94 09:30 $13.18 → close $13.92 +69.56; MH×92 09:30 $13.55 → close $13.10 -41.40; NRG×10 09:30 $120.00 → close $126.24 +62.40; TLN×3 09:30 $359.83 → close $362.74 +8.73; VST×8 09:30 $146.90 → close $148.13 +9.84 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,560.49 | ▲ 09:30 equity $10,211.68 vs yday $10,110.67 (+101.01) | 09:30 open · cash $1,560.49 (unchanged overnight, no fees) · equity $10,211.68 vs prior close $10,110.67 (+101.01) · 7 name(s) re-marked at the open (per-name table). ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; HLIT×94 yday $13.92 → 09:30 $13.84 -7.52; MH×92 yday $13.10 → 09:30 $13.16 +5.52; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92 | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $2,890.69 | ▲ +76.56 after sell → book $10,207.88; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $4,121.40 | ▼ -4.38 after sell → book $10,205.68; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $5,420.06 | ▲ +57.47 after sell → book $10,203.38; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $6,628.49 | ▼ -40.44 after sell → book $10,201.09; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $7,900.45 | ▲ +69.94 after sell → book $10,199.05; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $9,002.07 | ▲ +20.13 after sell → book $10,197.03; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $10,195.00 | ▲ +15.71 after sell → book $10,195.00; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 44 | $46.18 | $2.12 | — | $8,160.96 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ret5=+6.7; leftover $2039.00 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 14 | $142.77 | $2.03 | — | $6,160.14 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ret5=+5.8; leftover $2039.00 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 10 | $202.70 | $2.02 | — | $4,131.12 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ret5=+8.3; leftover $2039.00 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 41 | $49.00 | $2.11 | — | $2,120.01 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $2039.00 | join🟡 sector🟢 gen🟢 news🟢 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 21 | $92.99 | $2.05 | — | $165.17 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer; ret5=-0.8; leftover $2039.00 | join🟡 sector🔴 gen🟢 news🟢 judge🟡 vol🟡 buy🟡 |
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
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,941.05 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $7,685.70 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1277.15 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 517 | $2.47 | $6.67 | — | $6,402.04 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $5,166.66 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 79 | $16.00 | $2.23 | — | $3,900.43 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $2,697.30 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1277.15 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 48 | $26.57 | $2.13 | — | $1,419.80 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1806 | $0.71 | $18.19 | — | $124.77 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1277.15 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.77 | ▼ close $9,990.53 vs 09:30 $10,217.23 (session -189.31) | 16:00 close · cash $124.77 · equity $9,990.53 vs 09:30 $10,217.23 (-226.70; session marks -189.31) · 8 name(s) marked open→close (per-name table). BHP×14 09:30 $91.01 → close $93.63 +36.68; APA×28 09:30 $44.76 → close $44.39 -10.36; AUTL×517 09:30 $2.47 → close $2.46 -5.17; CRSP×21 09:30 $58.73 → close $58.12 -12.81; ASST×79 09:30 $16.00 → close $16.13 +10.27; MRNA×8 09:30 $150.14 → close $133.32 -134.56; ZLAB×48 09:30 $26.57 → close $26.02 -26.40; HUMA×1806 09:30 $0.71 → close $0.68 -46.96 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.77 | ▲ 09:30 equity $10,179.79 vs yday $9,990.53 (+189.26) | 09:30 open · cash $124.77 (unchanged overnight, no fees) · equity $10,179.79 vs prior close $9,990.53 (+189.26) · 8 name(s) re-marked at the open (per-name table). BHP×14 yday $93.63 → 09:30 $95.72 +29.26; APA×28 yday $44.39 → 09:30 $44.52 +3.64; AUTL×517 yday $2.46 → 09:30 $2.47 +5.17; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; ASST×79 yday $16.13 → 09:30 $17.66 +120.87; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; ZLAB×48 yday $26.02 → 09:30 $26.25 +11.04; HUMA×1806 yday $0.68 → 09:30 $0.67 -12.64 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 14 | $95.72 | $2.05 | $+61.86 | $1,462.80 | ▲ +61.86 after sell → book $10,177.74; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $2,707.27 | ▼ -10.89 after sell → book $10,175.64; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 79 | $17.66 | $2.25 | $+126.66 | $4,100.16 | ▲ +126.66 after sell → book $10,173.39; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $5,163.00 | ▼ -140.29 after sell → book $10,171.36; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 48 | $26.25 | $2.15 | $-19.65 | $6,420.85 | ▼ -19.65 after sell → book $10,169.20; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1806 | $0.67 | $17.90 | $-95.68 | $7,620.19 | ▼ -95.68 after sell → book $10,151.30; vs 09:30 mark -17.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,423.87 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1270.03 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $5,154.87 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1270.03 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 16 | $78.88 | $2.04 | — | $3,890.75 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1270.03 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 146 | $8.66 | $2.43 | — | $2,623.96 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1270.03 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 391 | $3.24 | $5.04 | — | $1,352.08 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $1270.03 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 108 | $11.70 | $2.31 | — | $86.17 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1270.03 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.17 | ▼ close $9,985.11 vs 09:30 $10,179.79 (session -150.33) | 16:00 close · cash $86.17 · equity $9,985.11 vs 09:30 $10,179.79 (-194.68; session marks -150.33) · 8 name(s) marked open→close (per-name table). AUTL×517 09:30 $2.47 → close $2.41 -31.02; CRSP×21 09:30 $59.72 → close $59.50 -4.62; AU×10 09:30 $119.43 → close $121.22 +17.90; FUTU×11 09:30 $115.18 → close $123.64 +93.06; GRAL×16 09:30 $78.88 → close $79.54 +10.56; ABTC×146 09:30 $8.66 → close $7.93 -106.58; HIVE×391 09:30 $3.24 → close $3.03 -82.11; MARA×108 09:30 $11.70 → close $11.26 -47.52 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.17 | ▼ 09:30 equity $9,950.19 vs yday $9,985.11 (-34.92) | 09:30 open · cash $86.17 (unchanged overnight, no fees) · equity $9,950.19 vs prior close $9,985.11 (-34.92) · 8 name(s) re-marked at the open (per-name table). AUTL×517 yday $2.41 → 09:30 $2.40 -5.17; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; AU×10 yday $121.22 → 09:30 $120.51 -7.10; FUTU×11 yday $123.64 → 09:30 $121.00 -29.04; GRAL×16 yday $79.54 → 09:30 $81.87 +37.28; ABTC×146 yday $7.93 → 09:30 $8.00 +10.22; HIVE×391 yday $3.03 → 09:30 $2.99 -15.64; MARA×108 yday $11.26 → 09:30 $11.17 -9.72 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 517 | $2.40 | $6.77 | $-49.62 | $1,320.20 | ▼ -49.62 after sell → book $9,943.42; vs 09:30 mark -6.77 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $2,523.26 | ▲ +6.74 after sell → book $9,941.38; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $121.00 | $2.04 | $+59.95 | $3,852.22 | ▲ +59.95 after sell → book $9,939.34; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 16 | $81.87 | $2.06 | $+43.74 | $5,160.08 | ▲ +43.74 after sell → book $9,937.28; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 146 | $8.00 | $2.46 | $-101.25 | $6,325.62 | ▼ -101.25 after sell → book $9,934.82; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 391 | $2.99 | $5.12 | $-107.91 | $7,489.59 | ▼ -107.91 after sell → book $9,929.70; vs 09:30 mark -5.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 108 | $11.17 | $2.34 | $-61.90 | $8,693.61 | ▼ -61.90 after sell → book $9,927.36; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,693.61 | ▼ close $9,892.18 vs 09:30 $9,950.19 (session -35.17) | 16:00 close · cash $8,693.61 · equity $9,892.18 vs 09:30 $9,950.19 (-58.01; session marks -35.17) · 1 name(s) marked open→close (per-name table). CRSP×21 09:30 $58.75 → close $57.08 -35.17 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,693.61 | ▲ 09:30 equity $9,910.14 vs yday $9,892.18 (+17.96) | 09:30 open · cash $8,693.61 (unchanged overnight, no fees) · equity $9,910.14 vs prior close $9,892.18 (+17.96) · 1 name(s) re-marked at the open (per-name table). CRSP×21 yday $57.08 → 09:30 $57.93 +17.95 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-20.93 | $9,908.06 | ▼ -20.93 after sell → book $9,908.06; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 11 | $118.52 | $2.02 | — | $8,602.32 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1415.44 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 18 | $77.13 | $2.04 | — | $7,211.94 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1415.44 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 40 | $35.05 | $2.11 | — | $5,807.83 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1415.44 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 150 | $9.42 | $2.44 | — | $4,392.39 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1415.44 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 49 | $28.86 | $2.14 | — | $2,976.11 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+13.7; leftover $1415.44 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 58 | $24.11 | $2.16 | — | $1,575.56 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_mover; ret5=+891.7; leftover $1415.44 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 162 | $8.72 | $2.48 | — | $160.45 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+13.0; leftover $1415.44 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.45 | ▲ close $10,348.74 vs 09:30 $9,910.14 (session +456.07) | 16:00 close · cash $160.45 · equity $10,348.74 vs 09:30 $9,910.14 (+438.60; session marks +456.07) · 7 name(s) marked open→close (per-name table). AU×11 09:30 $118.52 → close $123.39 +53.57; FCX×18 09:30 $77.13 → close $79.91 +50.04; EZPW×40 09:30 $35.05 → close $35.23 +7.20; RUM×150 09:30 $9.42 → close $10.23 +121.50; ZYME×49 09:30 $28.86 → close $27.47 -68.11; REAX×58 09:30 $24.11 → close $28.43 +250.56; EOLS×162 09:30 $8.72 → close $8.97 +41.31 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.45 | ▼ 09:30 equity $10,174.01 vs yday $10,348.74 (-174.73) | 09:30 open · cash $160.45 (unchanged overnight, no fees) · equity $10,174.01 vs prior close $10,348.74 (-174.73) · 7 name(s) re-marked at the open (per-name table). AU×11 yday $123.39 → 09:30 $119.80 -39.49; FCX×18 yday $79.91 → 09:30 $79.34 -10.26; EZPW×40 yday $35.23 → 09:30 $35.70 +18.80; RUM×150 yday $10.23 → 09:30 $10.07 -24.00; ZYME×49 yday $27.47 → 09:30 $27.56 +4.41; REAX×58 yday $28.43 → 09:30 $26.61 -105.56; EOLS×162 yday $8.97 → 09:30 $8.86 -18.63 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 11 | $119.80 | $2.04 | $+10.01 | $1,476.21 | ▲ +10.01 after sell → book $10,171.97; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 18 | $79.34 | $2.07 | $+35.67 | $2,902.26 | ▲ +35.67 after sell → book $10,169.90; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 40 | $35.70 | $2.13 | $+21.76 | $4,328.13 | ▲ +21.76 after sell → book $10,167.77; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 150 | $10.07 | $2.48 | $+92.58 | $5,836.15 | ▲ +92.58 after sell → book $10,165.29; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 49 | $27.56 | $2.16 | $-67.99 | $7,184.43 | ▼ -67.99 after sell → book $10,163.13; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 58 | $26.61 | $2.19 | $+140.65 | $8,725.63 | ▲ +140.65 after sell → book $10,160.95; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 162 | $8.86 | $2.51 | $+17.69 | $10,158.43 | ▲ +17.69 after sell → book $10,158.43; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 7 | $267.02 | $2.01 | — | $8,287.28 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $2031.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 181 | $11.22 | $2.53 | — | $6,253.93 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+16.8; leftover $2031.69 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 245 | $8.29 | $3.16 | — | $4,219.72 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $2031.69 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 116 | $17.41 | $2.34 | — | $2,197.82 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_mover; ret5=-9.2; leftover $2031.69 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 182 | $11.12 | $2.54 | — | $171.44 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $2031.69 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $171.44 | ▲ close $10,466.70 vs 09:30 $10,174.01 (session +320.85) | 16:00 close · cash $171.44 · equity $10,466.70 vs 09:30 $10,174.01 (+292.69; session marks +320.85) · 5 name(s) marked open→close (per-name table). FNV×7 09:30 $267.02 → close $267.37 +2.45; TRLV×181 09:30 $11.22 → close $11.43 +38.01; CAPR×245 09:30 $8.29 → close $9.36 +262.15; FWRD×116 09:30 $17.41 → close $17.63 +25.52; FLNC×182 09:30 $11.12 → close $11.08 -7.28 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $171.44 | ▲ 09:30 equity $10,491.62 vs yday $10,466.70 (+24.92) | 09:30 open · cash $171.44 (unchanged overnight, no fees) · equity $10,491.62 vs prior close $10,466.70 (+24.92) · 5 name(s) re-marked at the open (per-name table). FNV×7 yday $267.37 → 09:30 $267.23 -0.98; TRLV×181 yday $11.43 → 09:30 $11.38 -9.05; CAPR×245 yday $9.36 → 09:30 $9.19 -41.65; FWRD×116 yday $17.63 → 09:30 $17.60 -3.48; FLNC×182 yday $11.08 → 09:30 $11.52 +80.08 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 7 | $267.23 | $2.04 | $-2.58 | $2,040.02 | ▼ -2.58 after sell → book $10,489.59; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 181 | $11.38 | $2.58 | $+23.85 | $4,097.22 | ▲ +23.85 after sell → book $10,487.01; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 245 | $9.19 | $3.22 | $+214.12 | $6,345.55 | ▲ +214.12 after sell → book $10,483.79; vs 09:30 mark -3.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FWRD` | 116 | $17.60 | $2.37 | $+17.33 | $8,384.78 | ▲ +17.33 after sell → book $10,481.42; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 182 | $11.52 | $2.58 | $+67.68 | $10,478.83 | ▲ +67.68 after sell → book $10,478.83; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 21 | $81.65 | $2.05 | — | $8,762.13 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1746.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $7,793.13 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1746.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 5 | $318.88 | $2.00 | — | $6,196.72 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1746.47 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 7 | $222.86 | $2.01 | — | $4,634.69 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $1746.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 42 | $41.44 | $2.12 | — | $2,892.10 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; ret5=+3.1; leftover $1746.47 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,892.10 | ▼ close $10,455.42 vs 09:30 $10,491.62 (session -13.24) | 16:00 close · cash $2,892.10 · equity $10,455.42 vs 09:30 $10,491.62 (-36.20; session marks -13.24) · 5 name(s) marked open→close (per-name table). ACMR×21 09:30 $81.65 → close $80.49 -24.36; MU×1 09:30 $967.01 → close $935.39 -31.62; LRCX×5 09:30 $318.88 → close $318.58 -1.50; NVDA×7 09:30 $222.86 → close $227.98 +35.84; RRC×42 09:30 $41.44 → close $41.64 +8.40 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,892.10 | ▼ 09:30 equity $10,410.81 vs yday $10,455.42 (-44.61) | 09:30 open · cash $2,892.10 (unchanged overnight, no fees) · equity $10,410.81 vs prior close $10,455.42 (-44.61) · 5 name(s) re-marked at the open (per-name table). ACMR×21 yday $80.49 → 09:30 $79.27 -25.62; MU×1 yday $935.39 → 09:30 $919.29 -16.10; LRCX×5 yday $318.58 → 09:30 $318.03 -2.75; NVDA×7 yday $227.98 → 09:30 $227.36 -4.34; RRC×42 yday $41.64 → 09:30 $41.74 +4.20 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 21 | $79.27 | $2.08 | $-54.11 | $4,554.69 | ▼ -54.11 after sell → book $10,408.73; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $5,471.97 | ▼ -51.73 after sell → book $10,406.72; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 5 | $318.03 | $2.03 | $-8.28 | $7,060.09 | ▼ -8.28 after sell → book $10,404.69; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 7 | $227.36 | $2.03 | $+27.46 | $8,649.57 | ▲ +27.46 after sell → book $10,402.65; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 42 | $41.74 | $2.14 | $+8.34 | $10,400.51 | ▲ +8.34 after sell → book $10,400.51; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $9,100.87 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1300.06 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $7,823.02 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1300.06 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,619.76 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1300.06 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $5,416.65 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1300.06 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $4,370.01 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; ret5=+7.8; leftover $1300.06 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $32.90 | $2.11 | — | $3,084.80 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1300.06 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 269 | $4.82 | $3.47 | — | $1,784.75 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1300.06 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,784.75 | ▼ close $10,121.52 vs 09:30 $10,410.81 (session -263.39) | 16:00 close · cash $1,784.75 · equity $10,121.52 vs 09:30 $10,410.81 (-289.29; session marks -263.39) · 7 name(s) marked open→close (per-name table). KEYS×4 09:30 $324.41 → close $319.97 -17.76; SMTC×9 09:30 $141.76 → close $131.17 -95.31; CIEN×3 09:30 $400.42 → close $378.44 -65.94; DDOG×5 09:30 $240.22 → close $236.98 -16.20; ADSK×4 09:30 $261.16 → close $260.66 -2.00; SEDG×39 09:30 $32.90 → close $31.41 -58.11; TLS×269 09:30 $4.82 → close $4.79 -8.07 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,784.75 | ▼ 09:30 equity $10,110.14 vs yday $10,121.52 (-11.38) | 09:30 open · cash $1,784.75 (unchanged overnight, no fees) · equity $10,110.14 vs prior close $10,121.52 (-11.38) · 7 name(s) re-marked at the open (per-name table). KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; DDOG×5 yday $236.98 → 09:30 $233.97 -15.07; ADSK×4 yday $260.66 → 09:30 $257.71 -11.80; SEDG×39 yday $31.41 → 09:30 $31.15 -10.14; TLS×269 yday $4.79 → 09:30 $4.81 +5.38 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $3,072.69 | ▼ -11.70 after sell → book $10,108.12; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $4,261.35 | ▼ -89.19 after sell → book $10,106.08; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $5,394.65 | ▼ -69.96 after sell → book $10,104.06; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 5 | $233.97 | $2.02 | $-35.31 | $6,562.45 | ▼ -35.31 after sell → book $10,102.03; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $257.71 | $2.02 | $-17.82 | $7,591.27 | ▼ -17.82 after sell → book $10,100.01; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 39 | $31.15 | $2.13 | $-72.48 | $8,804.00 | ▼ -72.48 after sell → book $10,097.89; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 269 | $4.81 | $3.53 | $-9.69 | $10,094.36 | ▼ -9.69 after sell → book $10,094.36; vs 09:30 mark -3.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,094.36 | ▲ close $10,094.36 vs 09:30 $10,110.14 (session +0.00) | 16:00 close · cash $10,094.36 · no lots left · equity $10,094.36. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,094.36 | ▲ 09:30 equity $10,094.36 vs yday $10,094.36 (-0.00) | 09:30 open · cash $10,094.36 · no holdings · equity $10,094.36 vs prior close $10,094.36 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,094.36 | ▲ close $10,094.36 vs 09:30 $10,094.36 (session +0.00) | 16:00 close · cash $10,094.36 · no lots left · equity $10,094.36. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,094.36 | ▲ 09:30 equity $10,094.36 vs yday $10,094.36 (-0.00) | 09:30 open · cash $10,094.36 · no holdings · equity $10,094.36 vs prior close $10,094.36 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,094.36 | ▲ close $10,094.36 vs 09:30 $10,094.36 (session +0.00) | 16:00 close · cash $10,094.36 · no lots left · equity $10,094.36. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,094.36 | ▲ 09:30 equity $10,094.36 vs yday $10,094.36 (-0.00) | 09:30 open · cash $10,094.36 · no holdings · equity $10,094.36 vs prior close $10,094.36 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $9,037.14 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1261.79 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $8,062.52 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1261.79 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 39 | $32.31 | $2.11 | — | $6,800.33 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1261.79 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 79 | $15.87 | $2.23 | — | $5,544.37 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1261.79 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 52 | $23.88 | $2.15 | — | $4,300.46 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1261.79 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $3,595.22 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1261.79 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 26 | $47.60 | $2.07 | — | $2,355.55 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1261.79 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 38 | $32.88 | $2.10 | — | $1,104.01 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ret5=+16.2; leftover $1261.79 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,104.01 | ▲ close $10,453.94 vs 09:30 $10,094.36 (session +376.22) | 16:00 close · cash $1,104.01 · equity $10,453.94 vs 09:30 $10,094.36 (+359.58; session marks +376.22) · 8 name(s) marked open→close (per-name table). AVGO×3 09:30 $351.74 → close $357.16 +16.26; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×39 09:30 $32.31 → close $33.66 +52.65; FRNM×79 09:30 $15.87 → close $16.90 +81.37; MMED×52 09:30 $23.88 → close $23.84 -2.08; DE×1 09:30 $703.25 → close $694.41 -8.84; HPE×26 09:30 $47.60 → close $54.44 +177.84; CNXC×38 09:30 $32.88 → close $32.85 -1.14 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,104.01 | ▼ 09:30 equity $10,377.26 vs yday $10,453.94 (-76.68) | 09:30 open · cash $1,104.01 (unchanged overnight, no fees) · equity $10,377.26 vs prior close $10,453.94 (-76.68) · 8 name(s) re-marked at the open (per-name table). AVGO×3 yday $357.16 → 09:30 $359.70 +7.62; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×39 yday $33.66 → 09:30 $33.46 -7.80; FRNM×79 yday $16.90 → 09:30 $16.40 -39.50; MMED×52 yday $23.84 → 09:30 $23.84 +0.00; DE×1 yday $694.41 → 09:30 $692.03 -2.38; HPE×26 yday $54.44 → 09:30 $53.85 -15.34; CNXC×38 yday $32.85 → 09:30 $32.48 -14.06 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $2,181.09 | ▲ +19.86 after sell → book $10,375.24; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $3,206.63 | ▲ +50.93 after sell → book $10,373.22; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 39 | $33.46 | $2.13 | $+40.62 | $4,509.45 | ▲ +40.62 after sell → book $10,371.10; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 52 | $23.84 | $2.17 | $-6.39 | $5,746.96 | ▼ -6.39 after sell → book $10,368.93; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $6,436.98 | ▼ -15.23 after sell → book $10,366.92; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 26 | $53.85 | $2.09 | $+158.34 | $7,834.99 | ▲ +158.34 after sell → book $10,364.83; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 38 | $32.48 | $2.12 | $-19.43 | $9,067.11 | ▼ -19.43 after sell → book $10,362.71; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 6 | $263.36 | $2.01 | — | $7,484.94 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1813.42 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 23 | $75.65 | $2.06 | — | $5,742.93 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1813.42 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 7 | $236.82 | $2.01 | — | $4,083.18 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; ret5=+8.1; leftover $1813.42 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 934 | $1.94 | $12.05 | — | $2,259.17 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $1813.42 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 13 | $137.35 | $2.03 | — | $471.59 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; ret5=+5.4; leftover $1813.42 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $471.59 | ▲ close $10,507.42 vs 09:30 $10,377.26 (session +164.87) | 16:00 close · cash $471.59 · equity $10,507.42 vs 09:30 $10,377.26 (+130.16; session marks +164.87) · 6 name(s) marked open→close (per-name table). FRNM×79 09:30 $16.40 → close $16.31 -7.11; CRM×6 09:30 $263.36 → close $259.23 -24.78; MRX×23 09:30 $75.65 → close $78.27 +60.26; BE×7 09:30 $236.82 → close $252.87 +112.35; BAK×934 09:30 $1.94 → close $1.89 -46.70; MSTR×13 09:30 $137.35 → close $142.80 +70.85 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $471.59 | ▲ 09:30 equity $10,605.03 vs yday $10,507.42 (+97.61) | 09:30 open · cash $471.59 (unchanged overnight, no fees) · equity $10,605.03 vs prior close $10,507.42 (+97.61) · 6 name(s) re-marked at the open (per-name table). FRNM×79 yday $16.31 → 09:30 $16.74 +33.97; CRM×6 yday $259.23 → 09:30 $253.72 -33.06; MRX×23 yday $78.27 → 09:30 $78.84 +13.11; BE×7 yday $252.87 → 09:30 $267.76 +104.23; BAK×934 yday $1.89 → 09:30 $1.94 +46.70; MSTR×13 yday $142.80 → 09:30 $137.62 -67.34 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 79 | $16.74 | $2.25 | $+64.25 | $1,791.80 | ▲ +64.25 after sell → book $10,602.78; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 6 | $253.72 | $2.03 | $-61.88 | $3,312.09 | ▼ -61.88 after sell → book $10,600.75; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 7 | $267.76 | $2.04 | $+212.53 | $5,184.37 | ▲ +212.53 after sell → book $10,598.71; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 934 | $1.94 | $12.22 | $-24.27 | $6,984.11 | ▼ -24.27 after sell → book $10,586.49; vs 09:30 mark -12.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,984.11 | ▼ close $10,523.20 vs 09:30 $10,605.03 (session -63.29) | 16:00 close · cash $6,984.11 · equity $10,523.20 vs 09:30 $10,605.03 (-81.83; session marks -63.29) · 2 name(s) marked open→close (per-name table). MRX×23 09:30 $78.84 → close $76.71 -48.99; MSTR×13 09:30 $137.62 → close $136.52 -14.30 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,984.11 | ▲ 09:30 equity $10,589.57 vs yday $10,523.20 (+66.37) | 09:30 open · cash $6,984.11 (unchanged overnight, no fees) · equity $10,589.57 vs prior close $10,523.20 (+66.37) · 2 name(s) re-marked at the open (per-name table). MRX×23 yday $76.71 → 09:30 $76.60 -2.53; MSTR×13 yday $136.52 → 09:30 $141.82 +68.90 | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 23 | $76.60 | $2.08 | $+17.71 | $8,743.83 | ▲ +17.71 after sell → book $10,587.49; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 13 | $141.82 | $2.05 | $+54.03 | $10,585.44 | ▲ +54.03 after sell → book $10,585.44; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,585.44 | ▲ close $10,585.44 vs 09:30 $10,589.57 (session +0.00) | 16:00 close · cash $10,585.44 · no lots left · equity $10,585.44. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,585.44 | ▲ 09:30 equity $10,585.44 vs yday $10,585.44 (-0.00) | 09:30 open · cash $10,585.44 · no holdings · equity $10,585.44 vs prior close $10,585.44 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,585.44 | ▲ close $10,585.44 vs 09:30 $10,585.44 (session +0.00) | 16:00 close · cash $10,585.44 · no lots left · equity $10,585.44. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,585.44 | ▲ 09:30 equity $10,585.44 vs yday $10,585.44 (-0.00) | 09:30 open · cash $10,585.44 · no holdings · equity $10,585.44 vs prior close $10,585.44 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 12 | $164.43 | $2.03 | — | $8,610.25 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $2117.09 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 8 | $242.17 | $2.01 | — | $6,670.88 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; ret5=-11.1; leftover $2117.09 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 998 | $2.12 | $12.87 | — | $4,542.24 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $2117.09 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 1037 | $2.04 | $13.38 | — | $2,413.39 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $2117.09 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 15 | $135.71 | $2.04 | — | $375.70 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; ret5=-9.2; leftover $2117.09 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $375.70 | ▼ close $10,368.16 vs 09:30 $10,585.44 (session -184.95) | 16:00 close · cash $375.70 · equity $10,368.16 vs 09:30 $10,585.44 (-217.28; session marks -184.95) · 5 name(s) marked open→close (per-name table). ORCL×12 09:30 $164.43 → close $150.28 -169.80; ADBE×8 09:30 $242.17 → close $252.23 +80.48; BAK×998 09:30 $2.12 → close $2.08 -39.92; AMTX×1037 09:30 $2.04 → close $2.01 -31.11; RH×15 09:30 $135.71 → close $134.07 -24.60 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $375.70 | ▼ 09:30 equity $10,266.09 vs yday $10,368.16 (-102.07) | 09:30 open · cash $375.70 (unchanged overnight, no fees) · equity $10,266.09 vs prior close $10,368.16 (-102.07) · 5 name(s) re-marked at the open (per-name table). ORCL×12 yday $150.28 → 09:30 $141.42 -106.32; ADBE×8 yday $252.23 → 09:30 $261.51 +74.24; BAK×998 yday $2.08 → 09:30 $2.05 -29.94; AMTX×1037 yday $2.01 → 09:30 $2.01 +0.00; RH×15 yday $134.07 → 09:30 $131.40 -40.05 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 12 | $141.42 | $2.05 | $-280.20 | $2,070.69 | ▼ -280.20 after sell → book $10,264.04; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 8 | $261.51 | $2.04 | $+150.67 | $4,160.73 | ▲ +150.67 after sell → book $10,262.00; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 998 | $2.05 | $13.06 | $-95.79 | $6,193.57 | ▼ -95.79 after sell → book $10,248.94; vs 09:30 mark -13.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 1037 | $2.01 | $13.57 | $-58.05 | $8,264.38 | ▼ -58.05 after sell → book $10,235.38; vs 09:30 mark -13.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 15 | $131.40 | $2.06 | $-68.75 | $10,233.32 | ▼ -68.75 after sell → book $10,233.32; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,233.32 | ▲ close $10,233.32 vs 09:30 $10,266.09 (session +0.00) | 16:00 close · cash $10,233.32 · no lots left · equity $10,233.32. | — |

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
| 2026-08-27 | `ASML` | cash | leftover split 1746.47 < 1 share @ 1746.53 |
| 2026-08-28 | `MPWR` | cash | leftover split 1300.06 < 1 share @ 1306.03 |
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
