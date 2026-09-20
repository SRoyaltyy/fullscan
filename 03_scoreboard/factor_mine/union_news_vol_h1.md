# Factor mine action — `union_news_vol_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+5.14%** ($10,514) · signal-only (no cash/fees) was +16.13%. Starts YES **10/26**. Fills 123 · skips 38 · realized $+607.92.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the news camera (does the morning packet like the headline?) is green.
- Must-have: the volume camera (is this name unusually active?) is green.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Keep the first 8 names in list order.
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
- **Gate** `news=good,vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $76.38.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ANGX` | 464 | — | $4.31 | +0.00 | $4.37 | +27.84 | +27.84 | +0.00 | +27.84 |
| 2026-08-14 | `ARX` | 102 | — | $19.57 | +0.00 | $19.58 | +1.02 | +1.02 | +0.00 | +1.02 |
| 2026-08-14 | `SNDK` | 1 | — | $1646.93 | +0.00 | $1641.11 | -5.82 | -5.82 | +0.00 | -5.82 |
| 2026-08-14 | `MH` | 147 | — | $13.55 | +0.00 | $13.10 | -66.15 | -66.15 | +0.00 | -66.15 |
| 2026-08-14 | `HLIT` | 151 | — | $13.18 | +0.00 | $13.92 | +111.74 | +111.74 | +0.00 | +111.74 |
| 2026-08-17 | `ANGX` | 464 | $4.37 | $4.60 | +106.72 | — | +0.00 | +106.72 | +134.56 | — |
| 2026-08-17 | `ARX` | 102 | $19.58 | $19.57 | -1.02 | — | +0.00 | -1.02 | +0.00 | — |
| 2026-08-17 | `SNDK` | 1 | $1641.11 | $1700.74 | +59.63 | — | +0.00 | +59.63 | +53.81 | — |
| 2026-08-17 | `MH` | 147 | $13.10 | $13.16 | +8.82 | — | +0.00 | +8.82 | -57.33 | — |
| 2026-08-17 | `HLIT` | 151 | $13.92 | $13.84 | -12.08 | — | +0.00 | -12.08 | +99.66 | — |
| 2026-08-17 | `GLOB` | 137 | — | $37.18 | +0.00 | $36.26 | -126.04 | -126.04 | +0.00 | -126.04 |
| 2026-08-17 | `TPG` | 96 | — | $52.67 | +0.00 | $51.77 | -86.40 | -86.40 | +0.00 | -86.40 |
| 2026-08-18 | `GLOB` | 137 | $36.26 | $36.98 | +98.64 | — | +0.00 | +98.64 | -27.40 | — |
| 2026-08-18 | `TPG` | 96 | $51.77 | $51.77 | +0.00 | — | +0.00 | +0.00 | -86.40 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `HUMA` | 1781 | — | $0.71 | +0.00 | $0.68 | -46.31 | -46.31 | +0.00 | -46.31 |
| 2026-08-20 | `BTGO` | 190 | — | $6.61 | +0.00 | $6.60 | -0.95 | -0.95 | +0.00 | -0.95 |
| 2026-08-20 | `ASST` | 78 | — | $16.00 | +0.00 | $16.13 | +10.14 | +10.14 | +0.00 | +10.14 |
| 2026-08-20 | `ZLAB` | 47 | — | $26.57 | +0.00 | $26.02 | -25.85 | -25.85 | +0.00 | -25.85 |
| 2026-08-20 | `CRSP` | 21 | — | $58.73 | +0.00 | $58.12 | -12.81 | -12.81 | +0.00 | -12.81 |
| 2026-08-20 | `APA` | 28 | — | $44.76 | +0.00 | $44.39 | -10.36 | -10.36 | +0.00 | -10.36 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | — | +0.00 | -1.68 | -136.24 | — |
| 2026-08-21 | `HUMA` | 1781 | $0.68 | $0.67 | -12.47 | — | +0.00 | -12.47 | -58.77 | — |
| 2026-08-21 | `BTGO` | 190 | $6.60 | $6.95 | +66.50 | — | +0.00 | +66.50 | +65.55 | — |
| 2026-08-21 | `ASST` | 78 | $16.13 | $17.66 | +119.34 | — | +0.00 | +119.34 | +129.48 | — |
| 2026-08-21 | `ZLAB` | 47 | $26.02 | $26.25 | +10.81 | — | +0.00 | +10.81 | -15.04 | — |
| 2026-08-21 | `CRSP` | 21 | $58.12 | $59.72 | +33.60 | $59.50 | -4.62 | +28.98 | +20.79 | +16.17 |
| 2026-08-21 | `APA` | 28 | $44.39 | $44.52 | +3.64 | — | +0.00 | +3.64 | -6.72 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUTL` | 510 | — | $2.47 | +0.00 | $2.41 | -30.60 | -30.60 | +0.00 | -30.60 |
| 2026-08-21 | `FUTU` | 10 | — | $115.18 | +0.00 | $123.64 | +84.60 | +84.60 | +0.00 | +84.60 |
| 2026-08-21 | `DE` | 2 | — | $623.26 | +0.00 | $647.47 | +48.42 | +48.42 | +0.00 | +48.42 |
| 2026-08-21 | `MARA` | 107 | — | $11.70 | +0.00 | $11.26 | -47.08 | -47.08 | +0.00 | -47.08 |
| 2026-08-21 | `BTDR` | 113 | — | $11.10 | +0.00 | $11.37 | +31.07 | +31.07 | +0.00 | +31.07 |
| 2026-08-21 | `HIVE` | 388 | — | $3.24 | +0.00 | $3.03 | -81.48 | -81.48 | +0.00 | -81.48 |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | $57.08 | -35.17 | -50.92 | +0.42 | -34.75 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUTL` | 510 | $2.41 | $2.40 | -5.10 | — | +0.00 | -5.10 | -35.70 | — |
| 2026-08-24 | `FUTU` | 10 | $123.64 | $121.00 | -26.40 | — | +0.00 | -26.40 | +58.20 | — |
| 2026-08-24 | `DE` | 2 | $647.47 | $653.04 | +11.14 | — | +0.00 | +11.14 | +59.56 | — |
| 2026-08-24 | `MARA` | 107 | $11.26 | $11.17 | -9.63 | — | +0.00 | -9.63 | -56.71 | — |
| 2026-08-24 | `BTDR` | 113 | $11.37 | $11.48 | +12.43 | — | +0.00 | +12.43 | +43.50 | — |
| 2026-08-24 | `HIVE` | 388 | $3.03 | $2.99 | -15.52 | — | +0.00 | -15.52 | -97.00 | — |
| 2026-08-25 | `CRSP` | 21 | $57.08 | $57.93 | +17.95 | — | +0.00 | +17.95 | -16.80 | — |
| 2026-08-25 | `RUM` | 151 | — | $9.42 | +0.00 | $10.23 | +122.31 | +122.31 | +0.00 | +122.31 |
| 2026-08-25 | `EZPW` | 40 | — | $35.05 | +0.00 | $35.23 | +7.20 | +7.20 | +0.00 | +7.20 |
| 2026-08-25 | `REAX` | 59 | — | $24.11 | +0.00 | $28.43 | +254.88 | +254.88 | +0.00 | +254.88 |
| 2026-08-25 | `AU` | 12 | — | $118.52 | +0.00 | $123.39 | +58.44 | +58.44 | +0.00 | +58.44 |
| 2026-08-25 | `FCX` | 18 | — | $77.13 | +0.00 | $79.91 | +50.04 | +50.04 | +0.00 | +50.04 |
| 2026-08-25 | `AMX` | 59 | — | $23.80 | +0.00 | $23.75 | -2.95 | -2.95 | +0.00 | -2.95 |
| 2026-08-26 | `RUM` | 151 | $10.23 | $10.07 | -24.16 | — | +0.00 | -24.16 | +98.15 | — |
| 2026-08-26 | `EZPW` | 40 | $35.23 | $35.70 | +18.80 | — | +0.00 | +18.80 | +26.00 | — |
| 2026-08-26 | `REAX` | 59 | $28.43 | $26.61 | -107.38 | — | +0.00 | -107.38 | +147.50 | — |
| 2026-08-26 | `AU` | 12 | $123.39 | $119.80 | -43.08 | — | +0.00 | -43.08 | +15.36 | — |
| 2026-08-26 | `FCX` | 18 | $79.91 | $79.34 | -10.26 | — | +0.00 | -10.26 | +39.78 | — |
| 2026-08-26 | `AMX` | 59 | $23.75 | $23.75 | +0.00 | — | +0.00 | +0.00 | -2.95 | — |
| 2026-08-26 | `ASST` | 495 | — | $20.72 | +0.00 | $21.50 | +386.10 | +386.10 | +0.00 | +386.10 |
| 2026-08-27 | `ASST` | 495 | $21.50 | $22.45 | +470.25 | — | +0.00 | +470.25 | +856.35 | — |
| 2026-08-28 | `SEDG` | 42 | — | $32.90 | +0.00 | $31.41 | -62.58 | -62.58 | +0.00 | -62.58 |
| 2026-08-28 | `CAPR` | 142 | — | $9.73 | +0.00 | $9.59 | -19.88 | -19.88 | +0.00 | -19.88 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `ERAS` | 72 | — | $19.25 | +0.00 | $18.03 | -87.84 | -87.84 | +0.00 | -87.84 |
| 2026-08-28 | `BBWI` | 74 | — | $18.75 | +0.00 | $19.22 | +34.78 | +34.78 | +0.00 | +34.78 |
| 2026-08-28 | `ZYME` | 48 | — | $28.91 | +0.00 | $28.27 | -30.72 | -30.72 | +0.00 | -30.72 |
| 2026-08-28 | `TH` | 73 | — | $19.00 | +0.00 | $18.55 | -32.85 | -32.85 | +0.00 | -32.85 |
| 2026-08-28 | `PLAB` | 46 | — | $30.01 | +0.00 | $27.73 | -104.88 | -104.88 | +0.00 | -104.88 |
| 2026-08-31 | `SEDG` | 42 | $31.41 | $31.15 | -10.92 | — | +0.00 | -10.92 | -73.50 | — |
| 2026-08-31 | `CAPR` | 142 | $9.59 | $9.50 | -12.78 | — | +0.00 | -12.78 | -32.66 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `ERAS` | 72 | $18.03 | $17.87 | -11.52 | — | +0.00 | -11.52 | -99.36 | — |
| 2026-08-31 | `BBWI` | 74 | $19.22 | $19.25 | +2.22 | — | +0.00 | +2.22 | +37.00 | — |
| 2026-08-31 | `ZYME` | 48 | $28.27 | $28.06 | -10.08 | — | +0.00 | -10.08 | -40.80 | — |
| 2026-08-31 | `TH` | 73 | $18.55 | $18.12 | -31.03 | $18.52 | +28.83 | -2.20 | -63.88 | -35.04 |
| 2026-08-31 | `PLAB` | 46 | $27.73 | $28.04 | +14.26 | — | +0.00 | +14.26 | -90.62 | — |
| 2026-09-01 | `TH` | 73 | $18.52 | $18.45 | -5.11 | — | +0.00 | -5.11 | -40.15 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `MMED` | 89 | — | $23.88 | +0.00 | $23.84 | -3.56 | -3.56 | +0.00 | -3.56 |
| 2026-09-03 | `DE` | 3 | — | $703.25 | +0.00 | $694.41 | -26.52 | -26.52 | +0.00 | -26.52 |
| 2026-09-03 | `FRNM` | 134 | — | $15.87 | +0.00 | $16.90 | +138.02 | +138.02 | +0.00 | +138.02 |
| 2026-09-03 | `DELL` | 4 | — | $486.31 | +0.00 | $516.39 | +120.32 | +120.32 | +0.00 | +120.32 |
| 2026-09-03 | `CXW` | 65 | — | $32.31 | +0.00 | $33.66 | +87.75 | +87.75 | +0.00 | +87.75 |
| 2026-09-04 | `MMED` | 89 | $23.84 | $23.84 | +0.00 | $23.29 | -48.95 | -48.95 | -3.56 | -52.51 |
| 2026-09-04 | `DE` | 3 | $694.41 | $692.03 | -7.14 | — | +0.00 | -7.14 | -33.66 | — |
| 2026-09-04 | `FRNM` | 134 | $16.90 | $16.40 | -67.00 | $16.31 | -12.06 | -79.06 | +71.02 | +58.96 |
| 2026-09-04 | `DELL` | 4 | $516.39 | $513.78 | -10.44 | — | +0.00 | -10.44 | +109.88 | — |
| 2026-09-04 | `CXW` | 65 | $33.66 | $33.46 | -13.00 | — | +0.00 | -13.00 | +74.75 | — |
| 2026-09-04 | `BAK` | 1684 | — | $1.94 | +0.00 | $1.89 | -84.20 | -84.20 | +0.00 | -84.20 |
| 2026-09-04 | `HPE` | 60 | — | $53.85 | +0.00 | $52.00 | -111.00 | -111.00 | +0.00 | -111.00 |
| 2026-09-08 | `MMED` | 89 | $23.29 | $23.16 | -11.57 | — | +0.00 | -11.57 | -64.08 | — |
| 2026-09-08 | `FRNM` | 134 | $16.31 | $16.74 | +57.62 | — | +0.00 | +57.62 | +116.58 | — |
| 2026-09-08 | `BAK` | 1684 | $1.89 | $1.94 | +84.20 | — | +0.00 | +84.20 | +0.00 | — |
| 2026-09-08 | `HPE` | 60 | $52.00 | $52.29 | +17.40 | — | +0.00 | +17.40 | -93.60 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 13 | — | $164.43 | +0.00 | $150.28 | -183.95 | -183.95 | +0.00 | -183.95 |
| 2026-09-11 | `ADBE` | 8 | — | $242.17 | +0.00 | $252.23 | +80.48 | +80.48 | +0.00 | +80.48 |
| 2026-09-11 | `RH` | 15 | — | $135.71 | +0.00 | $134.07 | -24.60 | -24.60 | +0.00 | -24.60 |
| 2026-09-11 | `CNQ` | 42 | — | $49.94 | +0.00 | $50.07 | +5.46 | +5.46 | +0.00 | +5.46 |
| 2026-09-11 | `BTI` | 38 | — | $56.03 | +0.00 | $55.24 | -30.02 | -30.02 | +0.00 | -30.02 |
| 2026-09-14 | `ORCL` | 13 | $150.28 | $141.42 | -115.18 | — | +0.00 | -115.18 | -299.13 | — |
| 2026-09-14 | `ADBE` | 8 | $252.23 | $261.51 | +74.24 | — | +0.00 | +74.24 | +154.72 | — |
| 2026-09-14 | `RH` | 15 | $134.07 | $131.40 | -40.05 | — | +0.00 | -40.05 | -64.65 | — |
| 2026-09-14 | `CNQ` | 42 | $50.07 | $50.76 | +28.98 | — | +0.00 | +28.98 | +34.44 | — |
| 2026-09-14 | `BTI` | 38 | $55.24 | $57.12 | +71.44 | — | +0.00 | +71.44 | +41.42 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 100 | — | $26.27 | +0.00 | $26.59 | +32.00 | +32.00 | +0.00 | +32.00 |
| 2026-09-16 | `ASND` | 10 | — | $239.70 | +0.00 | $247.69 | +79.90 | +79.90 | +0.00 | +79.90 |
| 2026-09-16 | `SRRK` | 52 | — | $50.01 | +0.00 | $49.37 | -33.28 | -33.28 | +0.00 | -33.28 |
| 2026-09-16 | `AMX` | 113 | — | $23.18 | +0.00 | $22.98 | -22.60 | -22.60 | +0.00 | -22.60 |
| 2026-09-17 | `WAY` | 100 | $26.59 | $26.51 | -8.00 | — | +0.00 | -8.00 | +24.00 | — |
| 2026-09-17 | `ASND` | 10 | $247.69 | $249.23 | +15.40 | — | +0.00 | +15.40 | +95.30 | — |
| 2026-09-17 | `SRRK` | 52 | $49.37 | $49.52 | +7.80 | $49.02 | -26.00 | -18.20 | -25.48 | -51.48 |
| 2026-09-17 | `AMX` | 113 | $22.98 | $23.09 | +12.43 | — | +0.00 | +12.43 | -10.17 | — |
| 2026-09-17 | `SMTC` | 7 | — | $170.85 | +0.00 | $178.19 | +51.38 | +51.38 | +0.00 | +51.38 |
| 2026-09-17 | `TNDM` | 75 | — | $17.72 | +0.00 | $17.23 | -36.75 | -36.75 | +0.00 | -36.75 |
| 2026-09-17 | `JBHT` | 5 | — | $238.60 | +0.00 | $236.80 | -9.00 | -9.00 | +0.00 | -9.00 |
| 2026-09-17 | `FANG` | 7 | — | $191.08 | +0.00 | $196.94 | +41.02 | +41.02 | +0.00 | +41.02 |
| 2026-09-17 | `ALVO` | 256 | — | $5.22 | +0.00 | $5.34 | +30.72 | +30.72 | +0.00 | +30.72 |
| 2026-09-17 | `KEY` | 63 | — | $20.98 | +0.00 | $20.95 | -1.89 | -1.89 | +0.00 | -1.89 |
| 2026-09-18 | `SRRK` | 52 | $49.02 | $48.02 | -52.00 | — | +0.00 | -52.00 | -103.48 | — |
| 2026-09-18 | `SMTC` | 7 | $178.19 | $182.33 | +28.98 | — | +0.00 | +28.98 | +80.36 | — |
| 2026-09-18 | `TNDM` | 75 | $17.23 | $17.13 | -7.50 | — | +0.00 | -7.50 | -44.25 | — |
| 2026-09-18 | `JBHT` | 5 | $236.80 | $236.80 | +0.00 | — | +0.00 | +0.00 | -9.00 | — |
| 2026-09-18 | `FANG` | 7 | $196.94 | $196.94 | +0.00 | — | +0.00 | +0.00 | +41.02 | — |
| 2026-09-18 | `ALVO` | 256 | $5.34 | $5.40 | +15.36 | — | +0.00 | +15.36 | +46.08 | — |
| 2026-09-18 | `KEY` | 63 | $20.95 | $20.90 | -3.15 | — | +0.00 | -3.15 | -5.04 | — |
| 2026-09-18 | `BHVN` | 150 | — | $14.07 | +0.00 | $13.62 | -67.50 | -67.50 | +0.00 | -67.50 |
| 2026-09-18 | `RARE` | 143 | — | $14.79 | +0.00 | $14.51 | -40.04 | -40.04 | +0.00 | -40.04 |
| 2026-09-18 | `FLNC` | 281 | — | $7.54 | +0.00 | $7.32 | -60.41 | -60.41 | +0.00 | -60.41 |
| 2026-09-18 | `TH` | 101 | — | $20.91 | +0.00 | $21.19 | +28.28 | +28.28 | +0.00 | +28.28 |
| 2026-09-18 | `JXN` | 16 | — | $129.00 | +0.00 | $132.67 | +58.72 | +58.72 | +0.00 | +58.72 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +68.63 | ANGX, ARX, SNDK, MH, HLIT | — | $359.91 | $10,053.48 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 |
| 2026-08-17 | +2.25 | $359.91 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | $10,215.56 | +162.08 | -212.44 | GLOB, TPG | ANGX, ARX, SNDK, MH, HLIT | $45.52 | $9,983.06 | GLOB×137, TPG×96 |
| 2026-08-18 | -6.20 | $45.52 | GLOB×137, TPG×96 | $10,081.70 | +98.64 | +0.00 | — | GLOB, TPG | $10,076.90 | $10,076.90 | — |
| 2026-08-19 | -7.20 | $10,076.90 | — | $10,076.90 | -0.00 | +0.00 | — | — | $10,076.90 | $10,076.90 | — |
| 2026-08-20 | +1.12 | $10,076.90 | — | $10,076.90 | -0.00 | -186.64 | BHP, MRNA, HUMA, BTGO, ASST, ZLAB, CRSP, APA | — | $162.11 | $9,857.24 | BHP×13, MRNA×8, HUMA×1781, BTGO×190, ASST×78, ZLAB×47, CRSP×21, APA×28 |
| 2026-08-21 | +3.25 | $162.11 | BHP×13, MRNA×8, HUMA×1781, BTGO×190, ASST×78, ZLAB×47, CRSP×21, APA×28 | $10,104.16 | +246.92 | +18.21 | AU, AUTL, FUTU, DE, MARA, BTDR, HIVE | BHP, MRNA, HUMA, BTGO, ASST, ZLAB, APA | $181.87 | $10,069.28 | CRSP×21, AU×10, AUTL×510, FUTU×10, DE×2, MARA×107, BTDR×113, HIVE×388 |
| 2026-08-24 | -5.17 | $181.87 | CRSP×21, AU×10, AUTL×510, FUTU×10, DE×2, MARA×107, BTDR×113, HIVE×388 | $10,013.35 | -55.93 | -35.17 | — | AU, AUTL, FUTU, DE, MARA, BTDR, HIVE | $8,757.05 | $9,955.63 | CRSP×21 |
| 2026-08-25 | +1.80 | $8,757.05 | CRSP×21 | $9,973.58 | +17.95 | +489.92 | RUM, EZPW, REAX, AU, FCX, AMX | CRSP | $1,496.86 | $10,448.47 | RUM×151, EZPW×40, REAX×59, AU×12, FCX×18, AMX×59 |
| 2026-08-26 | +2.02 | $1,496.86 | RUM×151, EZPW×40, REAX×59, AU×12, FCX×18, AMX×59 | $10,282.39 | -166.08 | +386.10 | ASST | RUM, EZPW, REAX, AU, FCX, AMX | $6.51 | $10,649.01 | ASST×495 |
| 2026-08-27 | — | $6.51 | ASST×495 | $11,119.26 | +470.25 | +0.00 | — | ASST | $11,112.70 | $11,112.70 | — |
| 2026-08-28 | +0.75 | $11,112.70 | — | $11,112.70 | -0.00 | -399.28 | SEDG, CAPR, SMTC, ERAS, BBWI, ZYME, TH, PLAB | — | $127.32 | $10,695.98 | SEDG×42, CAPR×142, SMTC×9, ERAS×72, BBWI×74, ZYME×48, TH×73, PLAB×46 |
| 2026-08-31 | -5.85 | $127.32 | SEDG×42, CAPR×142, SMTC×9, ERAS×72, BBWI×74, ZYME×48, TH×73, PLAB×46 | $10,646.31 | -49.67 | +28.83 | — | SEDG, CAPR, SMTC, ERAS, BBWI, ZYME, PLAB | $9,307.79 | $10,659.75 | TH×73 |
| 2026-09-01 | -6.30 | $9,307.79 | TH×73 | $10,654.64 | -5.11 | +0.00 | — | TH | $10,652.41 | $10,652.41 | — |
| 2026-09-02 | -3.83 | $10,652.41 | — | $10,652.41 | -0.00 | +0.00 | — | — | $10,652.41 | $10,652.41 | — |
| 2026-09-03 | -0.90 | $10,652.41 | — | $10,652.41 | -0.00 | +316.01 | MMED, DE, FRNM, DELL, CXW | — | $234.53 | $10,957.58 | MMED×89, DE×3, FRNM×134, DELL×4, CXW×65 |
| 2026-09-04 | +2.25 | $234.53 | MMED×89, DE×3, FRNM×134, DELL×4, CXW×65 | $10,860.00 | -97.58 | -256.21 | BAK, HPE | DE, DELL, CXW | $12.52 | $10,573.63 | MMED×89, FRNM×134, BAK×1684, HPE×60 |
| 2026-09-08 | -11.47 | $12.52 | MMED×89, FRNM×134, BAK×1684, HPE×60 | $10,721.28 | +147.65 | +0.00 | — | MMED, FRNM, BAK, HPE | $10,692.33 | $10,692.33 | — |
| 2026-09-09 | -13.95 | $10,692.33 | — | $10,692.33 | -0.00 | +0.00 | — | — | $10,692.33 | $10,692.33 | — |
| 2026-09-10 | -13.28 | $10,692.33 | — | $10,692.33 | -0.00 | +0.00 | — | — | $10,692.33 | $10,692.33 | — |
| 2026-09-11 | +0.50 | $10,692.33 | — | $10,692.33 | -0.00 | -152.63 | ORCL, ADBE, RH, CNQ, BTI | — | $344.81 | $10,529.40 | ORCL×13, ADBE×8, RH×15, CNQ×42, BTI×38 |
| 2026-09-14 | -11.00 | $344.81 | ORCL×13, ADBE×8, RH×15, CNQ×42, BTI×38 | $10,548.83 | +19.43 | +0.00 | — | ORCL, ADBE, RH, CNQ, BTI | $10,538.40 | $10,538.40 | — |
| 2026-09-15 | -3.84 | $10,538.40 | — | $10,538.40 | -0.00 | +0.00 | — | — | $10,538.40 | $10,538.40 | — |
| 2026-09-16 | +5.30 | $10,538.40 | — | $10,538.40 | -0.00 | +56.02 | WAY, ASND, SRRK, AMX | — | $285.75 | $10,585.63 | WAY×100, ASND×10, SRRK×52, AMX×113 |
| 2026-09-17 | +7.38 | $285.75 | WAY×100, ASND×10, SRRK×52, AMX×113 | $10,613.26 | +27.63 | +49.48 | SMTC, TNDM, JBHT, FANG, ALVO, KEY | WAY, ASND, AMX | $304.18 | $10,642.27 | SRRK×52, SMTC×7, TNDM×75, JBHT×5, FANG×7, ALVO×256, KEY×63 |
| 2026-09-18 | +4.86 | $304.18 | SRRK×52, SMTC×7, TNDM×75, JBHT×5, FANG×7, ALVO×256, KEY×63 | $10,623.96 | -18.31 | -80.95 | BHVN, RARE, FLNC, TH, JXN | SRRK, SMTC, TNDM, JBHT, FANG, ALVO, KEY | $76.38 | $10,514.14 | BHVN×150, RARE×143, FLNC×281, TH×101, JXN×16 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $7,994.17 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 102 | $19.57 | $2.30 | — | $5,995.74 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+58.7; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 1 | $1646.93 | $1.99 | — | $4,346.82 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 147 | $13.55 | $2.43 | — | $2,352.53 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 151 | $13.18 | $2.44 | — | $359.91 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $359.91 | ▲ close $10,053.48 vs 09:30 $10,000.00 (session +68.63) | 16:00 close · cash $359.91 · equity $10,053.48 vs 09:30 $10,000.00 (+53.48; session marks +68.63) · 5 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.31 → close $4.37 +27.84; ARX×102 09:30 $19.57 → close $19.58 +1.02; SNDK×1 09:30 $1646.93 → close $1641.11 -5.82; MH×147 09:30 $13.55 → close $13.10 -66.15; HLIT×151 09:30 $13.18 → close $13.92 +111.74 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $359.91 | ▲ 09:30 equity $10,215.56 vs yday $10,053.48 (+162.08) | 09:30 open · cash $359.91 (unchanged overnight, no fees) · equity $10,215.56 vs prior close $10,053.48 (+162.08) · 5 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.37 → 09:30 $4.60 +106.72; ARX×102 yday $19.58 → 09:30 $19.57 -1.02; SNDK×1 yday $1641.11 → 09:30 $1700.74 +59.63; MH×147 yday $13.10 → 09:30 $13.16 +8.82; HLIT×151 yday $13.92 → 09:30 $13.84 -12.08 | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 464 | $4.60 | $6.08 | $+122.49 | $2,488.23 | ▲ +122.49 after sell → book $10,209.48; vs 09:30 mark -6.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 102 | $19.57 | $2.33 | $-4.62 | $4,482.04 | ▼ -4.62 after sell → book $10,207.15; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `SNDK` | 1 | $1700.74 | $2.02 | $+49.81 | $6,180.77 | ▲ +49.81 after sell → book $10,205.13; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 147 | $13.16 | $2.47 | $-62.23 | $8,112.82 | ▼ -62.23 after sell → book $10,202.66; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 151 | $13.84 | $2.48 | $+94.73 | $10,200.18 | ▲ +94.73 after sell → book $10,200.18; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `GLOB` | 137 | $37.18 | $2.40 | — | $5,104.11 | — | combo gate; gate news=good,vol=good; list oppset; ⚪; ret5=-0.1; leftover $5100.09 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TPG` | 96 | $52.67 | $2.28 | — | $45.52 | — | combo gate; gate news=good,vol=good; list oppset; 🔵; ⚪; ret5=+9.4; leftover $5100.09 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.52 | ▼ close $9,983.06 vs 09:30 $10,215.56 (session -212.44) | 16:00 close · cash $45.52 · equity $9,983.06 vs 09:30 $10,215.56 (-232.50; session marks -212.44) · 2 name(s) marked open→close (per-name table). GLOB×137 09:30 $37.18 → close $36.26 -126.04; TPG×96 09:30 $52.67 → close $51.77 -86.40 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.52 | ▲ 09:30 equity $10,081.70 vs yday $9,983.06 (+98.64) | 09:30 open · cash $45.52 (unchanged overnight, no fees) · equity $10,081.70 vs prior close $9,983.06 (+98.64) · 2 name(s) re-marked at the open (per-name table). GLOB×137 yday $36.26 → 09:30 $36.98 +98.64; TPG×96 yday $51.77 → 09:30 $51.77 +0.00 | — |
| 2026-08-18 09:30 ET | **SELL** | `GLOB` | 137 | $36.98 | $2.46 | $-32.27 | $5,109.31 | ▼ -32.27 after sell → book $10,079.23; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 96 | $51.77 | $2.33 | $-91.01 | $10,076.90 | ▼ -91.01 after sell → book $10,076.90; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,076.90 | ▲ close $10,076.90 vs 09:30 $10,081.70 (session +0.00) | 16:00 close · cash $10,076.90 · no lots left · equity $10,076.90. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,076.90 | ▲ 09:30 equity $10,076.90 vs yday $10,076.90 (-0.00) | 09:30 open · cash $10,076.90 · no holdings · equity $10,076.90 vs prior close $10,076.90 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,076.90 | ▲ close $10,076.90 vs 09:30 $10,076.90 (session +0.00) | 16:00 close · cash $10,076.90 · no lots left · equity $10,076.90. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,076.90 | ▲ 09:30 equity $10,076.90 vs yday $10,076.90 (-0.00) | 09:30 open · cash $10,076.90 · no holdings · equity $10,076.90 vs prior close $10,076.90 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,891.74 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1259.61 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $7,688.61 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover,oppset; 🔵; ret5=+173.9; leftover $1259.61 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1781 | $0.71 | $17.93 | — | $6,411.50 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1259.61 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 190 | $6.61 | $2.56 | — | $5,153.99 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1259.61 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 78 | $16.00 | $2.22 | — | $3,903.77 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1259.61 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 47 | $26.57 | $2.13 | — | $2,652.85 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+4.8; leftover $1259.61 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $1,417.47 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1259.61 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $162.11 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ret5=+8.7; leftover $1259.61 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $162.11 | ▼ close $9,857.24 vs 09:30 $10,076.90 (session -186.64) | 16:00 close · cash $162.11 · equity $9,857.24 vs 09:30 $10,076.90 (-219.66; session marks -186.64) · 8 name(s) marked open→close (per-name table). BHP×13 09:30 $91.01 → close $93.63 +34.06; MRNA×8 09:30 $150.14 → close $133.32 -134.56; HUMA×1781 09:30 $0.71 → close $0.68 -46.31; BTGO×190 09:30 $6.61 → close $6.60 -0.95; ASST×78 09:30 $16.00 → close $16.13 +10.14; ZLAB×47 09:30 $26.57 → close $26.02 -25.85; CRSP×21 09:30 $58.73 → close $58.12 -12.81; APA×28 09:30 $44.76 → close $44.39 -10.36 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $162.11 | ▲ 09:30 equity $10,104.16 vs yday $9,857.24 (+246.92) | 09:30 open · cash $162.11 (unchanged overnight, no fees) · equity $10,104.16 vs prior close $9,857.24 (+246.92) · 8 name(s) re-marked at the open (per-name table). BHP×13 yday $93.63 → 09:30 $95.72 +27.17; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; HUMA×1781 yday $0.68 → 09:30 $0.67 -12.47; BTGO×190 yday $6.60 → 09:30 $6.95 +66.50; ASST×78 yday $16.13 → 09:30 $17.66 +119.34; ZLAB×47 yday $26.02 → 09:30 $26.25 +10.81; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; APA×28 yday $44.39 → 09:30 $44.52 +3.64 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,404.42 | ▲ +57.15 after sell → book $10,102.11; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $2,467.27 | ▼ -140.29 after sell → book $10,100.07; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1781 | $0.67 | $17.65 | $-94.36 | $3,650.01 | ▼ -94.36 after sell → book $10,082.42; vs 09:30 mark -17.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 190 | $6.95 | $2.60 | $+60.39 | $4,967.91 | ▲ +60.39 after sell → book $10,079.82; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 78 | $17.66 | $2.25 | $+125.01 | $6,343.14 | ▲ +125.01 after sell → book $10,077.57; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 47 | $26.25 | $2.15 | $-19.32 | $7,574.74 | ▼ -19.32 after sell → book $10,075.42; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $8,819.20 | ▼ -10.89 after sell → book $10,073.33; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $7,622.89 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1259.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 510 | $2.47 | $6.58 | — | $6,356.61 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1259.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $5,202.79 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1259.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $3,954.27 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1259.89 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 107 | $11.70 | $2.31 | — | $2,700.06 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1259.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 113 | $11.10 | $2.33 | — | $1,444.00 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+19.1; leftover $1259.89 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 388 | $3.24 | $5.01 | — | $181.87 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1259.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $181.87 | ▲ close $10,069.28 vs 09:30 $10,104.16 (session +18.21) | 16:00 close · cash $181.87 · equity $10,069.28 vs 09:30 $10,104.16 (-34.88; session marks +18.21) · 8 name(s) marked open→close (per-name table). CRSP×21 09:30 $59.72 → close $59.50 -4.62; AU×10 09:30 $119.43 → close $121.22 +17.90; AUTL×510 09:30 $2.47 → close $2.41 -30.60; FUTU×10 09:30 $115.18 → close $123.64 +84.60; DE×2 09:30 $623.26 → close $647.47 +48.42; MARA×107 09:30 $11.70 → close $11.26 -47.08; BTDR×113 09:30 $11.10 → close $11.37 +31.07; HIVE×388 09:30 $3.24 → close $3.03 -81.48 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $181.87 | ▼ 09:30 equity $10,013.35 vs yday $10,069.28 (-55.93) | 09:30 open · cash $181.87 (unchanged overnight, no fees) · equity $10,013.35 vs prior close $10,069.28 (-55.93) · 8 name(s) re-marked at the open (per-name table). CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUTL×510 yday $2.41 → 09:30 $2.40 -5.10; FUTU×10 yday $123.64 → 09:30 $121.00 -26.40; DE×2 yday $647.47 → 09:30 $653.04 +11.14; MARA×107 yday $11.26 → 09:30 $11.17 -9.63; BTDR×113 yday $11.37 → 09:30 $11.48 +12.43; HIVE×388 yday $3.03 → 09:30 $2.99 -15.52 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,384.93 | ▲ +6.74 after sell → book $10,011.31; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 510 | $2.40 | $6.67 | $-48.95 | $2,602.26 | ▼ -48.95 after sell → book $10,004.64; vs 09:30 mark -6.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $121.00 | $2.04 | $+54.14 | $3,810.22 | ▲ +54.14 after sell → book $10,002.60; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $5,114.28 | ▲ +55.55 after sell → book $10,000.58; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 107 | $11.17 | $2.34 | $-61.36 | $6,307.13 | ▼ -61.36 after sell → book $9,998.24; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 113 | $11.48 | $2.36 | $+38.82 | $7,602.01 | ▲ +38.82 after sell → book $9,995.88; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 388 | $2.99 | $5.08 | $-107.08 | $8,757.05 | ▼ -107.08 after sell → book $9,990.80; vs 09:30 mark -5.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,757.05 | ▼ close $9,955.63 vs 09:30 $10,013.35 (session -35.17) | 16:00 close · cash $8,757.05 · equity $9,955.63 vs 09:30 $10,013.35 (-57.72; session marks -35.17) · 1 name(s) marked open→close (per-name table). CRSP×21 09:30 $58.75 → close $57.08 -35.17 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,757.05 | ▲ 09:30 equity $9,973.58 vs yday $9,955.63 (+17.95) | 09:30 open · cash $8,757.05 (unchanged overnight, no fees) · equity $9,973.58 vs prior close $9,955.63 (+17.95) · 1 name(s) re-marked at the open (per-name table). CRSP×21 yday $57.08 → 09:30 $57.93 +17.95 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-20.93 | $9,971.51 | ▼ -20.93 after sell → book $9,971.51; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 151 | $9.42 | $2.44 | — | $8,546.65 | — | combo gate; gate news=good,vol=good; list yday_gainer,ohlc_hot,oppset; 🔵; ret5=+13.6; leftover $1424.50 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 40 | $35.05 | $2.11 | — | $7,142.54 | — | combo gate; gate news=good,vol=good; list yday_gainer,oppset; 🔵; ⚪; ret5=+19.7; leftover $1424.50 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 59 | $24.11 | $2.17 | — | $5,717.88 | — | combo gate; gate news=good,vol=good; list yday_mover; ret5=+891.7; leftover $1424.50 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 12 | $118.52 | $2.03 | — | $4,293.61 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1424.50 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 18 | $77.13 | $2.04 | — | $2,903.23 | — | combo gate; gate news=good,vol=good; list mover_buy; ⚪; ret5=+13.8; leftover $1424.50 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMX` | 59 | $23.80 | $2.17 | — | $1,496.86 | — | combo gate; gate news=good,vol=good; list oppset; 🔵; ret5=+0.5; leftover $1424.50 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,496.86 | ▲ close $10,448.47 vs 09:30 $9,973.58 (session +489.92) | 16:00 close · cash $1,496.86 · equity $10,448.47 vs 09:30 $9,973.58 (+474.89; session marks +489.92) · 6 name(s) marked open→close (per-name table). RUM×151 09:30 $9.42 → close $10.23 +122.31; EZPW×40 09:30 $35.05 → close $35.23 +7.20; REAX×59 09:30 $24.11 → close $28.43 +254.88; AU×12 09:30 $118.52 → close $123.39 +58.44; FCX×18 09:30 $77.13 → close $79.91 +50.04; AMX×59 09:30 $23.80 → close $23.75 -2.95 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,496.86 | ▼ 09:30 equity $10,282.39 vs yday $10,448.47 (-166.08) | 09:30 open · cash $1,496.86 (unchanged overnight, no fees) · equity $10,282.39 vs prior close $10,448.47 (-166.08) · 6 name(s) re-marked at the open (per-name table). RUM×151 yday $10.23 → 09:30 $10.07 -24.16; EZPW×40 yday $35.23 → 09:30 $35.70 +18.80; REAX×59 yday $28.43 → 09:30 $26.61 -107.38; AU×12 yday $123.39 → 09:30 $119.80 -43.08; FCX×18 yday $79.91 → 09:30 $79.34 -10.26; AMX×59 yday $23.75 → 09:30 $23.75 +0.00 | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 151 | $10.07 | $2.48 | $+93.23 | $3,014.95 | ▲ +93.23 after sell → book $10,279.91; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 40 | $35.70 | $2.13 | $+21.76 | $4,440.82 | ▲ +21.76 after sell → book $10,277.78; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 59 | $26.61 | $2.19 | $+143.14 | $6,008.62 | ▲ +143.14 after sell → book $10,275.59; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 12 | $119.80 | $2.05 | $+11.29 | $7,444.17 | ▲ +11.29 after sell → book $10,273.54; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 18 | $79.34 | $2.07 | $+35.67 | $8,870.23 | ▲ +35.67 after sell → book $10,271.48; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AMX` | 59 | $23.75 | $2.19 | $-7.31 | $10,269.29 | ▼ -7.31 after sell → book $10,269.29; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ASST` | 495 | $20.72 | $6.39 | — | $6.51 | — | combo gate; gate news=good,vol=good; list oppset; 🔵; ret5=+67.1; leftover $10269.29 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.51 | ▲ close $10,649.01 vs 09:30 $10,282.39 (session +386.10) | 16:00 close · cash $6.51 · equity $10,649.01 vs 09:30 $10,282.39 (+366.62; session marks +386.10) · 1 name(s) marked open→close (per-name table). ASST×495 09:30 $20.72 → close $21.50 +386.10 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.51 | ▲ 09:30 equity $11,119.26 vs yday $10,649.01 (+470.25) | 09:30 open · cash $6.51 (unchanged overnight, no fees) · equity $11,119.26 vs prior close $10,649.01 (+470.25) · 1 name(s) re-marked at the open (per-name table). ASST×495 yday $21.50 → 09:30 $22.45 +470.25 | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 495 | $22.45 | $6.56 | $+843.41 | $11,112.70 | ▲ +843.41 after sell → book $11,112.70; vs 09:30 mark -6.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,112.70 | ▲ close $11,112.70 vs 09:30 $11,119.26 (session +0.00) | 16:00 close · cash $11,112.70 · no lots left · equity $11,112.70. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,112.70 | ▲ 09:30 equity $11,112.70 vs yday $11,112.70 (-0.00) | 09:30 open · cash $11,112.70 · no holdings · equity $11,112.70 vs prior close $11,112.70 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 42 | $32.90 | $2.12 | — | $9,728.78 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1389.09 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 142 | $9.73 | $2.42 | — | $8,344.71 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; ret5=+47.1; leftover $1389.09 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $7,066.85 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $1389.09 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 72 | $19.25 | $2.21 | — | $5,678.64 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+14.1; leftover $1389.09 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 74 | $18.75 | $2.21 | — | $4,288.93 | — | combo gate; gate news=good,vol=good; list yday_gainer,oppset; ret5=-5.0; leftover $1389.09 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 48 | $28.91 | $2.13 | — | $2,899.12 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+9.2; leftover $1389.09 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 73 | $19.00 | $2.21 | — | $1,509.91 | — | combo gate; gate news=good,vol=good; list ohlc_hot; ret5=+7.5; leftover $1389.09 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PLAB` | 46 | $30.01 | $2.13 | — | $127.32 | — | combo gate; gate news=good,vol=good; list oppset; 🔵; ret5=-0.9; leftover $1389.09 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.32 | ▼ close $10,695.98 vs 09:30 $11,112.70 (session -399.28) | 16:00 close · cash $127.32 · equity $10,695.98 vs 09:30 $11,112.70 (-416.72; session marks -399.28) · 8 name(s) marked open→close (per-name table). SEDG×42 09:30 $32.90 → close $31.41 -62.58; CAPR×142 09:30 $9.73 → close $9.59 -19.88; SMTC×9 09:30 $141.76 → close $131.17 -95.31; ERAS×72 09:30 $19.25 → close $18.03 -87.84; BBWI×74 09:30 $18.75 → close $19.22 +34.78; ZYME×48 09:30 $28.91 → close $28.27 -30.72; TH×73 09:30 $19.00 → close $18.55 -32.85; PLAB×46 09:30 $30.01 → close $27.73 -104.88 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.32 | ▼ 09:30 equity $10,646.31 vs yday $10,695.98 (-49.67) | 09:30 open · cash $127.32 (unchanged overnight, no fees) · equity $10,646.31 vs prior close $10,695.98 (-49.67) · 8 name(s) re-marked at the open (per-name table). SEDG×42 yday $31.41 → 09:30 $31.15 -10.92; CAPR×142 yday $9.59 → 09:30 $9.50 -12.78; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; ERAS×72 yday $18.03 → 09:30 $17.87 -11.52; BBWI×74 yday $19.22 → 09:30 $19.25 +2.22; ZYME×48 yday $28.27 → 09:30 $28.06 -10.08; TH×73 yday $18.55 → 09:30 $18.12 -31.03; PLAB×46 yday $27.73 → 09:30 $28.04 +14.26 | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 42 | $31.15 | $2.14 | $-77.75 | $1,433.48 | ▼ -77.75 after sell → book $10,644.17; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 142 | $9.50 | $2.45 | $-37.53 | $2,780.03 | ▼ -37.53 after sell → book $10,641.72; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $3,968.70 | ▼ -89.19 after sell → book $10,639.68; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 72 | $17.87 | $2.23 | $-103.79 | $5,253.11 | ▼ -103.79 after sell → book $10,637.45; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 74 | $19.25 | $2.24 | $+32.55 | $6,675.37 | ▲ +32.55 after sell → book $10,635.22; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 48 | $28.06 | $2.15 | $-45.09 | $8,020.10 | ▼ -45.09 after sell → book $10,633.06; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PLAB` | 46 | $28.04 | $2.15 | $-94.90 | $9,307.79 | ▼ -94.90 after sell → book $10,630.91; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,307.79 | ▲ close $10,659.75 vs 09:30 $10,646.31 (session +28.83) | 16:00 close · cash $9,307.79 · equity $10,659.75 vs 09:30 $10,646.31 (+13.44; session marks +28.83) · 1 name(s) marked open→close (per-name table). TH×73 09:30 $18.12 → close $18.52 +28.83 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,307.79 | ▼ 09:30 equity $10,654.64 vs yday $10,659.75 (-5.11) | 09:30 open · cash $9,307.79 (unchanged overnight, no fees) · equity $10,654.64 vs prior close $10,659.75 (-5.11) · 1 name(s) re-marked at the open (per-name table). TH×73 yday $18.52 → 09:30 $18.45 -5.11 | — |
| 2026-09-01 09:30 ET | **SELL** | `TH` | 73 | $18.45 | $2.23 | $-44.59 | $10,652.41 | ▼ -44.59 after sell → book $10,652.41; vs 09:30 mark -2.23 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,652.41 | ▲ close $10,652.41 vs 09:30 $10,654.64 (session +0.00) | 16:00 close · cash $10,652.41 · no lots left · equity $10,652.41. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,652.41 | ▲ 09:30 equity $10,652.41 vs yday $10,652.41 (-0.00) | 09:30 open · cash $10,652.41 · no holdings · equity $10,652.41 vs prior close $10,652.41 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,652.41 | ▲ close $10,652.41 vs 09:30 $10,652.41 (session +0.00) | 16:00 close · cash $10,652.41 · no lots left · equity $10,652.41. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,652.41 | ▲ 09:30 equity $10,652.41 vs yday $10,652.41 (-0.00) | 09:30 open · cash $10,652.41 · no holdings · equity $10,652.41 vs prior close $10,652.41 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 89 | $23.88 | $2.26 | — | $8,524.83 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+21.9; leftover $2130.48 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 3 | $703.25 | $2.00 | — | $6,413.08 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $2130.48 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 134 | $15.87 | $2.39 | — | $4,284.11 | — | combo gate; gate news=good,vol=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $2130.48 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 4 | $486.31 | $2.00 | — | $2,336.87 | — | combo gate; gate news=good,vol=good; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $2130.48 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 65 | $32.31 | $2.19 | — | $234.53 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $2130.48 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $234.53 | ▲ close $10,957.58 vs 09:30 $10,652.41 (session +316.01) | 16:00 close · cash $234.53 · equity $10,957.58 vs 09:30 $10,652.41 (+305.17; session marks +316.01) · 5 name(s) marked open→close (per-name table). MMED×89 09:30 $23.88 → close $23.84 -3.56; DE×3 09:30 $703.25 → close $694.41 -26.52; FRNM×134 09:30 $15.87 → close $16.90 +138.02; DELL×4 09:30 $486.31 → close $516.39 +120.32; CXW×65 09:30 $32.31 → close $33.66 +87.75 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $234.53 | ▼ 09:30 equity $10,860.00 vs yday $10,957.58 (-97.58) | 09:30 open · cash $234.53 (unchanged overnight, no fees) · equity $10,860.00 vs prior close $10,957.58 (-97.58) · 5 name(s) re-marked at the open (per-name table). MMED×89 yday $23.84 → 09:30 $23.84 +0.00; DE×3 yday $694.41 → 09:30 $692.03 -7.14; FRNM×134 yday $16.90 → 09:30 $16.40 -67.00; DELL×4 yday $516.39 → 09:30 $513.78 -10.44; CXW×65 yday $33.66 → 09:30 $33.46 -13.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 3 | $692.03 | $2.03 | $-37.68 | $2,308.60 | ▼ -37.68 after sell → book $10,857.98; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 4 | $513.78 | $2.03 | $+105.85 | $4,361.69 | ▲ +105.85 after sell → book $10,855.95; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 65 | $33.46 | $2.21 | $+70.35 | $6,534.38 | ▲ +70.35 after sell → book $10,853.74; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 1684 | $1.94 | $21.72 | — | $3,245.69 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; ret5=+18.3; leftover $3267.19 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HPE` | 60 | $53.85 | $2.17 | — | $12.52 | — | combo gate; gate news=good,vol=good; list oppset; 🔵; ret5=+0.1; leftover $3267.19 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟡 vol🟢 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.52 | ▼ close $10,573.63 vs 09:30 $10,860.00 (session -256.21) | 16:00 close · cash $12.52 · equity $10,573.63 vs 09:30 $10,860.00 (-286.37; session marks -256.21) · 4 name(s) marked open→close (per-name table). MMED×89 09:30 $23.84 → close $23.29 -48.95; FRNM×134 09:30 $16.40 → close $16.31 -12.06; BAK×1684 09:30 $1.94 → close $1.89 -84.20; HPE×60 09:30 $53.85 → close $52.00 -111.00 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.52 | ▲ 09:30 equity $10,721.28 vs yday $10,573.63 (+147.65) | 09:30 open · cash $12.52 (unchanged overnight, no fees) · equity $10,721.28 vs prior close $10,573.63 (+147.65) · 4 name(s) re-marked at the open (per-name table). MMED×89 yday $23.29 → 09:30 $23.16 -11.57; FRNM×134 yday $16.31 → 09:30 $16.74 +57.62; BAK×1684 yday $1.89 → 09:30 $1.94 +84.20; HPE×60 yday $52.00 → 09:30 $52.29 +17.40 | — |
| 2026-09-08 09:30 ET | **SELL** | `MMED` | 89 | $23.16 | $2.29 | $-68.63 | $2,071.47 | ▼ -68.63 after sell → book $10,718.99; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 134 | $16.74 | $2.43 | $+111.76 | $4,312.20 | ▲ +111.76 after sell → book $10,716.56; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 1684 | $1.94 | $22.03 | $-43.75 | $7,557.13 | ▼ -43.75 after sell → book $10,694.53; vs 09:30 mark -22.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HPE` | 60 | $52.29 | $2.21 | $-97.98 | $10,692.33 | ▼ -97.98 after sell → book $10,692.33; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,692.33 | ▲ close $10,692.33 vs 09:30 $10,721.28 (session +0.00) | 16:00 close · cash $10,692.33 · no lots left · equity $10,692.33. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,692.33 | ▲ 09:30 equity $10,692.33 vs yday $10,692.33 (-0.00) | 09:30 open · cash $10,692.33 · no holdings · equity $10,692.33 vs prior close $10,692.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,692.33 | ▲ close $10,692.33 vs 09:30 $10,692.33 (session +0.00) | 16:00 close · cash $10,692.33 · no lots left · equity $10,692.33. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,692.33 | ▲ 09:30 equity $10,692.33 vs yday $10,692.33 (-0.00) | 09:30 open · cash $10,692.33 · no holdings · equity $10,692.33 vs prior close $10,692.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,692.33 | ▲ close $10,692.33 vs 09:30 $10,692.33 (session +0.00) | 16:00 close · cash $10,692.33 · no lots left · equity $10,692.33. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,692.33 | ▲ 09:30 equity $10,692.33 vs yday $10,692.33 (-0.00) | 09:30 open · cash $10,692.33 · no holdings · equity $10,692.33 vs prior close $10,692.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 13 | $164.43 | $2.03 | — | $8,552.71 | — | combo gate; gate news=good,vol=good; list flatten,earn_react; ⚪; ret5=+4.9; leftover $2138.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 8 | $242.17 | $2.01 | — | $6,613.33 | — | combo gate; gate news=good,vol=good; list earn_react; ret5=-11.1; leftover $2138.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 15 | $135.71 | $2.04 | — | $4,575.65 | — | combo gate; gate news=good,vol=good; list earn_react,oppset; ret5=-9.2; leftover $2138.47 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CNQ` | 42 | $49.94 | $2.12 | — | $2,476.05 | — | combo gate; gate news=good,vol=good; list oppset; ret5=+1.7; leftover $2138.47 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BTI` | 38 | $56.03 | $2.10 | — | $344.81 | — | combo gate; gate news=good,vol=good; list oppset; 🔵; ret5=-0.8; leftover $2138.47 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $344.81 | ▼ close $10,529.40 vs 09:30 $10,692.33 (session -152.63) | 16:00 close · cash $344.81 · equity $10,529.40 vs 09:30 $10,692.33 (-162.93; session marks -152.63) · 5 name(s) marked open→close (per-name table). ORCL×13 09:30 $164.43 → close $150.28 -183.95; ADBE×8 09:30 $242.17 → close $252.23 +80.48; RH×15 09:30 $135.71 → close $134.07 -24.60; CNQ×42 09:30 $49.94 → close $50.07 +5.46; BTI×38 09:30 $56.03 → close $55.24 -30.02 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $344.81 | ▲ 09:30 equity $10,548.83 vs yday $10,529.40 (+19.43) | 09:30 open · cash $344.81 (unchanged overnight, no fees) · equity $10,548.83 vs prior close $10,529.40 (+19.43) · 5 name(s) re-marked at the open (per-name table). ORCL×13 yday $150.28 → 09:30 $141.42 -115.18; ADBE×8 yday $252.23 → 09:30 $261.51 +74.24; RH×15 yday $134.07 → 09:30 $131.40 -40.05; CNQ×42 yday $50.07 → 09:30 $50.76 +28.98; BTI×38 yday $55.24 → 09:30 $57.12 +71.44 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 13 | $141.42 | $2.05 | $-303.21 | $2,181.22 | ▼ -303.21 after sell → book $10,546.78; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 8 | $261.51 | $2.04 | $+150.67 | $4,271.25 | ▲ +150.67 after sell → book $10,544.73; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 15 | $131.40 | $2.06 | $-68.75 | $6,240.19 | ▼ -68.75 after sell → book $10,542.67; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `CNQ` | 42 | $50.76 | $2.14 | $+30.18 | $8,369.97 | ▲ +30.18 after sell → book $10,540.53; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BTI` | 38 | $57.12 | $2.13 | $+37.18 | $10,538.40 | ▲ +37.18 after sell → book $10,538.40; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,538.40 | ▲ close $10,538.40 vs 09:30 $10,548.83 (session +0.00) | 16:00 close · cash $10,538.40 · no lots left · equity $10,538.40. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,538.40 | ▲ 09:30 equity $10,538.40 vs yday $10,538.40 (-0.00) | 09:30 open · cash $10,538.40 · no holdings · equity $10,538.40 vs prior close $10,538.40 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,538.40 | ▲ close $10,538.40 vs 09:30 $10,538.40 (session +0.00) | 16:00 close · cash $10,538.40 · no lots left · equity $10,538.40. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,538.40 | ▲ 09:30 equity $10,538.40 vs yday $10,538.40 (-0.00) | 09:30 open · cash $10,538.40 · no holdings · equity $10,538.40 vs prior close $10,538.40 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 100 | $26.27 | $2.29 | — | $7,909.11 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+10.0; leftover $2634.60 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `ASND` | 10 | $239.70 | $2.02 | — | $5,510.09 | — | combo gate; gate news=good,vol=good; list oppset; ret5=-11.2; leftover $2634.60 | join🟡 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SRRK` | 52 | $50.01 | $2.15 | — | $2,907.42 | — | combo gate; gate news=good,vol=good; list oppset; ret5=+1.1; leftover $2634.60 | join🟡 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `AMX` | 113 | $23.18 | $2.33 | — | $285.75 | — | combo gate; gate news=good,vol=good; list oppset; 🔵; ret5=-0.2; leftover $2634.60 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $285.75 | ▲ close $10,585.63 vs 09:30 $10,538.40 (session +56.02) | 16:00 close · cash $285.75 · equity $10,585.63 vs 09:30 $10,538.40 (+47.23; session marks +56.02) · 4 name(s) marked open→close (per-name table). WAY×100 09:30 $26.27 → close $26.59 +32.00; ASND×10 09:30 $239.70 → close $247.69 +79.90; SRRK×52 09:30 $50.01 → close $49.37 -33.28; AMX×113 09:30 $23.18 → close $22.98 -22.60 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $285.75 | ▲ 09:30 equity $10,613.26 vs yday $10,585.63 (+27.63) | 09:30 open · cash $285.75 (unchanged overnight, no fees) · equity $10,613.26 vs prior close $10,585.63 (+27.63) · 4 name(s) re-marked at the open (per-name table). WAY×100 yday $26.59 → 09:30 $26.51 -8.00; ASND×10 yday $247.69 → 09:30 $249.23 +15.40; SRRK×52 yday $49.37 → 09:30 $49.52 +7.80; AMX×113 yday $22.98 → 09:30 $23.09 +12.43 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 100 | $26.51 | $2.33 | $+19.38 | $2,934.43 | ▲ +19.38 after sell → book $10,610.94; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ASND` | 10 | $249.23 | $2.05 | $+91.23 | $5,424.68 | ▲ +91.23 after sell → book $10,608.89; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AMX` | 113 | $23.09 | $2.37 | $-14.87 | $8,031.48 | ▼ -14.87 after sell → book $10,606.52; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 7 | $170.85 | $2.01 | — | $6,833.52 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1338.58 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 75 | $17.72 | $2.21 | — | $5,502.30 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=-8.3; leftover $1338.58 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 5 | $238.60 | $2.00 | — | $4,307.30 | — | combo gate; gate news=good,vol=good; list yday_mover,oppset; ret5=-11.6; leftover $1338.58 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `FANG` | 7 | $191.08 | $2.01 | — | $2,967.73 | — | combo gate; gate news=good,vol=good; list oppset; ret5=-4.0; leftover $1338.58 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ALVO` | 256 | $5.22 | $3.30 | — | $1,628.10 | — | combo gate; gate news=good,vol=good; list oppset; 🔵; ret5=-2.1; leftover $1338.58 | join🔴 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `KEY` | 63 | $20.98 | $2.18 | — | $304.18 | — | combo gate; gate news=good,vol=good; list oppset; 🔵; ret5=+0.8; leftover $1338.58 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $304.18 | ▲ close $10,642.27 vs 09:30 $10,613.26 (session +49.48) | 16:00 close · cash $304.18 · equity $10,642.27 vs 09:30 $10,613.26 (+29.01; session marks +49.48) · 7 name(s) marked open→close (per-name table). SRRK×52 09:30 $49.52 → close $49.02 -26.00; SMTC×7 09:30 $170.85 → close $178.19 +51.38; TNDM×75 09:30 $17.72 → close $17.23 -36.75; JBHT×5 09:30 $238.60 → close $236.80 -9.00; FANG×7 09:30 $191.08 → close $196.94 +41.02; ALVO×256 09:30 $5.22 → close $5.34 +30.72; KEY×63 09:30 $20.98 → close $20.95 -1.89 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $304.18 | ▼ 09:30 equity $10,623.96 vs yday $10,642.27 (-18.31) | 09:30 open · cash $304.18 (unchanged overnight, no fees) · equity $10,623.96 vs prior close $10,642.27 (-18.31) · 7 name(s) re-marked at the open (per-name table). SRRK×52 yday $49.02 → 09:30 $48.02 -52.00; SMTC×7 yday $178.19 → 09:30 $182.33 +28.98; TNDM×75 yday $17.23 → 09:30 $17.13 -7.50; JBHT×5 yday $236.80 → 09:30 $236.80 +0.00; FANG×7 yday $196.94 → 09:30 $196.94 +0.00; ALVO×256 yday $5.34 → 09:30 $5.40 +15.36; KEY×63 yday $20.95 → 09:30 $20.90 -3.15 | — |
| 2026-09-18 09:30 ET | **SELL** | `SRRK` | 52 | $48.02 | $2.18 | $-107.80 | $2,799.05 | ▼ -107.80 after sell → book $10,621.79; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 7 | $182.33 | $2.03 | $+76.32 | $4,073.33 | ▲ +76.32 after sell → book $10,619.76; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 75 | $17.13 | $2.24 | $-48.70 | $5,355.84 | ▼ -48.70 after sell → book $10,617.52; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 5 | $236.80 | $2.02 | $-13.03 | $6,537.82 | ▼ -13.03 after sell → book $10,615.50; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FANG` | 7 | $196.94 | $2.03 | $+36.98 | $7,914.36 | ▲ +36.98 after sell → book $10,613.46; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ALVO` | 256 | $5.40 | $3.36 | $+39.42 | $9,293.41 | ▲ +39.42 after sell → book $10,610.11; vs 09:30 mark -3.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `KEY` | 63 | $20.90 | $2.20 | $-9.42 | $10,607.91 | ▼ -9.42 after sell → book $10,607.91; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 150 | $14.07 | $2.44 | — | $8,494.97 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2121.58 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 143 | $14.79 | $2.42 | — | $6,377.58 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $2121.58 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 281 | $7.54 | $3.62 | — | $4,256.62 | — | combo gate; gate news=good,vol=good; list yday_mover,oppset; 🔵; ret5=-20.9; leftover $2121.58 | join🟡 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 101 | $20.91 | $2.29 | — | $2,142.42 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2121.58 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `JXN` | 16 | $129.00 | $2.04 | — | $76.38 | — | combo gate; gate news=good,vol=good; list oppset; ret5=+2.4; leftover $2121.58 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.38 | ▼ close $10,514.14 vs 09:30 $10,623.96 (session -80.95) | 16:00 close · cash $76.38 · equity $10,514.14 vs 09:30 $10,623.96 (-109.82; session marks -80.95) · 5 name(s) marked open→close (per-name table). BHVN×150 09:30 $14.07 → close $13.62 -67.50; RARE×143 09:30 $14.79 → close $14.51 -40.04; FLNC×281 09:30 $7.54 → close $7.32 -60.41; TH×101 09:30 $20.91 → close $21.19 +28.28; JXN×16 09:30 $129.00 → close $132.67 +58.72 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AUTL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HDSN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AMX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PURR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `TWO` | no_price | no 09:30 open |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `WLTH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CGNT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SRRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CVE` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `BHVN` | 150 | 2026-09-18 @ $14.07 | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2121.58 |
| `RARE` | 143 | 2026-09-18 @ $14.79 | combo gate; gate news=good,vol=good; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $2121.58 |
| `FLNC` | 281 | 2026-09-18 @ $7.54 | combo gate; gate news=good,vol=good; list yday_mover,oppset; 🔵; ret5=-20.9; leftover $2121.58 |
| `TH` | 101 | 2026-09-18 @ $20.91 | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2121.58 |
| `JXN` | 16 | 2026-09-18 @ $129.00 | combo gate; gate news=good,vol=good; list oppset; ret5=+2.4; leftover $2121.58 |
