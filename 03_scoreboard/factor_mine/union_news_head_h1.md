# Factor mine action — `union_news_head_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · prior-export headline🟢 only

Cash book **-1.69%** ($9,832) · signal-only (no cash/fees) was +16.70%. Starts YES **9/26**. Fills 144 · skips 63 · realized $-108.94.

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
- Must-have: the prior-export headline is green.
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
- **Gate** `headline=good` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $122.38.

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
| 2026-08-14 | `S` | 52 | — | $23.77 | +0.00 | $23.11 | -34.58 | -34.58 | +0.00 | -34.58 |
| 2026-08-14 | `ZS` | 6 | — | $190.00 | +0.00 | $183.60 | -38.40 | -38.40 | +0.00 | -38.40 |
| 2026-08-17 | `HLIT` | 94 | $13.92 | $13.84 | -7.52 | — | +0.00 | -7.52 | +62.04 | — |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | — | +0.00 | +66.70 | +84.10 | — |
| 2026-08-17 | `ARX` | 63 | $19.58 | $19.57 | -0.63 | — | +0.00 | -0.63 | +0.00 | — |
| 2026-08-17 | `MH` | 92 | $13.10 | $13.16 | +5.52 | — | +0.00 | +5.52 | -35.88 | — |
| 2026-08-17 | `VELO` | 81 | $16.16 | $16.05 | -8.91 | — | +0.00 | -8.91 | +54.27 | — |
| 2026-08-17 | `S` | 52 | $23.11 | $22.50 | -31.72 | — | +0.00 | -31.72 | -66.30 | — |
| 2026-08-17 | `ZS` | 6 | $183.60 | $188.38 | +28.65 | — | +0.00 | +28.65 | -9.75 | — |
| 2026-08-17 | `GLOB` | 67 | — | $37.18 | +0.00 | $36.26 | -61.64 | -61.64 | +0.00 | -61.64 |
| 2026-08-17 | `TPG` | 47 | — | $52.67 | +0.00 | $51.77 | -42.30 | -42.30 | +0.00 | -42.30 |
| 2026-08-17 | `OUST` | 51 | — | $49.00 | +0.00 | $48.13 | -44.37 | -44.37 | +0.00 | -44.37 |
| 2026-08-17 | `CELC` | 27 | — | $92.99 | +0.00 | $92.44 | -14.85 | -14.85 | +0.00 | -14.85 |
| 2026-08-18 | `GLOB` | 67 | $36.26 | $36.98 | +48.24 | — | +0.00 | +48.24 | -13.40 | — |
| 2026-08-18 | `TPG` | 47 | $51.77 | $51.77 | +0.00 | — | +0.00 | +0.00 | -42.30 | — |
| 2026-08-18 | `OUST` | 51 | $48.13 | $45.09 | -155.04 | — | +0.00 | -155.04 | -199.41 | — |
| 2026-08-18 | `CELC` | 27 | $92.44 | $92.38 | -1.62 | — | +0.00 | -1.62 | -16.47 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `AUTL` | 494 | — | $2.47 | +0.00 | $2.46 | -4.94 | -4.94 | +0.00 | -4.94 |
| 2026-08-20 | `CRSP` | 20 | — | $58.73 | +0.00 | $58.12 | -12.20 | -12.20 | +0.00 | -12.20 |
| 2026-08-20 | `MRK` | 8 | — | $150.78 | +0.00 | $148.99 | -14.32 | -14.32 | +0.00 | -14.32 |
| 2026-08-20 | `ASST` | 76 | — | $16.00 | +0.00 | $16.13 | +9.88 | +9.88 | +0.00 | +9.88 |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `ZLAB` | 45 | — | $26.57 | +0.00 | $26.02 | -24.75 | -24.75 | +0.00 | -24.75 |
| 2026-08-20 | `TEAM` | 7 | — | $173.90 | +0.00 | $174.91 | +7.07 | +7.07 | +0.00 | +7.07 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `AUTL` | 494 | $2.46 | $2.47 | +4.94 | $2.41 | -29.64 | -24.70 | +0.00 | -29.64 |
| 2026-08-21 | `CRSP` | 20 | $58.12 | $59.72 | +32.00 | $59.50 | -4.40 | +27.60 | +19.80 | +15.40 |
| 2026-08-21 | `MRK` | 8 | $148.99 | $149.12 | +1.04 | — | +0.00 | +1.04 | -13.28 | — |
| 2026-08-21 | `ASST` | 76 | $16.13 | $17.66 | +116.28 | — | +0.00 | +116.28 | +126.16 | — |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | — | +0.00 | -1.68 | -136.24 | — |
| 2026-08-21 | `ZLAB` | 45 | $26.02 | $26.25 | +10.35 | — | +0.00 | +10.35 | -14.40 | — |
| 2026-08-21 | `TEAM` | 7 | $174.91 | $174.22 | -4.83 | — | +0.00 | -4.83 | +2.24 | — |
| 2026-08-21 | `FUTU` | 10 | — | $115.18 | +0.00 | $123.64 | +84.60 | +84.60 | +0.00 | +84.60 |
| 2026-08-21 | `GRAL` | 15 | — | $78.88 | +0.00 | $79.54 | +9.90 | +9.90 | +0.00 | +9.90 |
| 2026-08-21 | `VIRT` | 20 | — | $60.66 | +0.00 | $67.93 | +145.40 | +145.40 | +0.00 | +145.40 |
| 2026-08-21 | `ABTC` | 141 | — | $8.66 | +0.00 | $7.93 | -102.93 | -102.93 | +0.00 | -102.93 |
| 2026-08-21 | `HIVE` | 378 | — | $3.24 | +0.00 | $3.03 | -79.38 | -79.38 | +0.00 | -79.38 |
| 2026-08-21 | `MARA` | 104 | — | $11.70 | +0.00 | $11.26 | -45.76 | -45.76 | +0.00 | -45.76 |
| 2026-08-24 | `AUTL` | 494 | $2.41 | $2.40 | -4.94 | — | +0.00 | -4.94 | -34.58 | — |
| 2026-08-24 | `CRSP` | 20 | $59.50 | $58.75 | -15.00 | $57.08 | -33.50 | -48.50 | +0.40 | -33.10 |
| 2026-08-24 | `FUTU` | 10 | $123.64 | $121.00 | -26.40 | — | +0.00 | -26.40 | +58.20 | — |
| 2026-08-24 | `GRAL` | 15 | $79.54 | $81.87 | +34.95 | — | +0.00 | +34.95 | +44.85 | — |
| 2026-08-24 | `VIRT` | 20 | $67.93 | $66.80 | -22.60 | — | +0.00 | -22.60 | +122.80 | — |
| 2026-08-24 | `ABTC` | 141 | $7.93 | $8.00 | +9.87 | — | +0.00 | +9.87 | -93.06 | — |
| 2026-08-24 | `HIVE` | 378 | $3.03 | $2.99 | -15.12 | — | +0.00 | -15.12 | -94.50 | — |
| 2026-08-24 | `MARA` | 104 | $11.26 | $11.17 | -9.36 | — | +0.00 | -9.36 | -55.12 | — |
| 2026-08-25 | `CRSP` | 20 | $57.08 | $57.93 | +17.10 | — | +0.00 | +17.10 | -16.00 | — |
| 2026-08-25 | `EZPW` | 45 | — | $35.05 | +0.00 | $35.23 | +8.10 | +8.10 | +0.00 | +8.10 |
| 2026-08-25 | `RUM` | 170 | — | $9.42 | +0.00 | $10.23 | +137.70 | +137.70 | +0.00 | +137.70 |
| 2026-08-25 | `ZYME` | 55 | — | $28.86 | +0.00 | $27.47 | -76.45 | -76.45 | +0.00 | -76.45 |
| 2026-08-25 | `REAX` | 66 | — | $24.11 | +0.00 | $28.43 | +285.12 | +285.12 | +0.00 | +285.12 |
| 2026-08-25 | `EOLS` | 184 | — | $8.72 | +0.00 | $8.97 | +46.92 | +46.92 | +0.00 | +46.92 |
| 2026-08-26 | `EZPW` | 45 | $35.23 | $35.70 | +21.15 | — | +0.00 | +21.15 | +29.25 | — |
| 2026-08-26 | `RUM` | 170 | $10.23 | $10.07 | -27.20 | — | +0.00 | -27.20 | +110.50 | — |
| 2026-08-26 | `ZYME` | 55 | $27.47 | $27.56 | +4.95 | $29.30 | +95.98 | +100.93 | -71.50 | +24.48 |
| 2026-08-26 | `REAX` | 66 | $28.43 | $26.61 | -120.12 | — | +0.00 | -120.12 | +165.00 | — |
| 2026-08-26 | `EOLS` | 184 | $8.97 | $8.86 | -21.16 | — | +0.00 | -21.16 | +25.76 | — |
| 2026-08-26 | `FNV` | 5 | — | $267.02 | +0.00 | $267.37 | +1.75 | +1.75 | +0.00 | +1.75 |
| 2026-08-26 | `ASST` | 67 | — | $20.72 | +0.00 | $21.50 | +52.26 | +52.26 | +0.00 | +52.26 |
| 2026-08-26 | `TRLV` | 124 | — | $11.22 | +0.00 | $11.43 | +26.04 | +26.04 | +0.00 | +26.04 |
| 2026-08-26 | `CAPR` | 168 | — | $8.29 | +0.00 | $9.36 | +179.76 | +179.76 | +0.00 | +179.76 |
| 2026-08-26 | `FWRD` | 80 | — | $17.41 | +0.00 | $17.63 | +17.60 | +17.60 | +0.00 | +17.60 |
| 2026-08-26 | `FLNC` | 125 | — | $11.12 | +0.00 | $11.08 | -5.00 | -5.00 | +0.00 | -5.00 |
| 2026-08-27 | `ZYME` | 55 | $29.30 | $29.33 | +1.37 | — | +0.00 | +1.37 | +25.85 | — |
| 2026-08-27 | `FNV` | 5 | $267.37 | $267.23 | -0.70 | — | +0.00 | -0.70 | +1.05 | — |
| 2026-08-27 | `ASST` | 67 | $21.50 | $22.45 | +63.65 | — | +0.00 | +63.65 | +115.91 | — |
| 2026-08-27 | `TRLV` | 124 | $11.43 | $11.38 | -6.20 | $11.03 | -43.40 | -49.60 | +19.84 | -23.56 |
| 2026-08-27 | `CAPR` | 168 | $9.36 | $9.19 | -28.56 | $10.06 | +146.16 | +117.60 | +151.20 | +297.36 |
| 2026-08-27 | `FWRD` | 80 | $17.63 | $17.60 | -2.40 | $17.70 | +8.00 | +5.60 | +15.20 | +23.20 |
| 2026-08-27 | `FLNC` | 125 | $11.08 | $11.52 | +55.00 | $11.43 | -11.25 | +43.75 | +50.00 | +38.75 |
| 2026-08-27 | `GEN` | 50 | — | $29.83 | +0.00 | $30.50 | +33.50 | +33.50 | +0.00 | +33.50 |
| 2026-08-27 | `SRRK` | 25 | — | $60.00 | +0.00 | $59.26 | -18.50 | -18.50 | +0.00 | -18.50 |
| 2026-08-28 | `TRLV` | 124 | $11.03 | $11.00 | -3.72 | — | +0.00 | -3.72 | -27.28 | — |
| 2026-08-28 | `CAPR` | 168 | $10.06 | $9.73 | -55.44 | — | +0.00 | -55.44 | +241.92 | — |
| 2026-08-28 | `FWRD` | 80 | $17.70 | $17.70 | +0.00 | — | +0.00 | +0.00 | +23.20 | — |
| 2026-08-28 | `FLNC` | 125 | $11.43 | $11.27 | -20.00 | — | +0.00 | -20.00 | +18.75 | — |
| 2026-08-28 | `GEN` | 50 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +33.50 | — |
| 2026-08-28 | `SRRK` | 25 | $59.26 | $58.75 | -12.75 | — | +0.00 | -12.75 | -31.25 | — |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `PLAB` | 43 | — | $30.01 | +0.00 | $27.73 | -98.04 | -98.04 | +0.00 | -98.04 |
| 2026-08-28 | `SEDG` | 39 | — | $32.90 | +0.00 | $31.41 | -58.11 | -58.11 | +0.00 | -58.11 |
| 2026-08-28 | `TLS` | 267 | — | $4.82 | +0.00 | $4.79 | -8.01 | -8.01 | +0.00 | -8.01 |
| 2026-08-28 | `TH` | 67 | — | $19.00 | +0.00 | $18.55 | -30.15 | -30.15 | +0.00 | -30.15 |
| 2026-08-28 | `OPTX` | 149 | — | $8.61 | +0.00 | $8.52 | -13.41 | -13.41 | +0.00 | -13.41 |
| 2026-08-28 | `HEI` | 3 | — | $339.95 | +0.00 | $336.53 | -10.26 | -10.26 | +0.00 | -10.26 |
| 2026-08-28 | `KSS` | 70 | — | $18.25 | +0.00 | $17.50 | -52.50 | -52.50 | +0.00 | -52.50 |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `PLAB` | 43 | $27.73 | $28.04 | +13.33 | — | +0.00 | +13.33 | -84.71 | — |
| 2026-08-31 | `SEDG` | 39 | $31.41 | $31.15 | -10.14 | — | +0.00 | -10.14 | -68.25 | — |
| 2026-08-31 | `TLS` | 267 | $4.79 | $4.81 | +5.34 | — | +0.00 | +5.34 | -2.67 | — |
| 2026-08-31 | `TH` | 67 | $18.55 | $18.12 | -28.48 | $18.52 | +26.46 | -2.02 | -58.62 | -32.16 |
| 2026-08-31 | `OPTX` | 149 | $8.52 | $8.52 | +0.00 | — | +0.00 | +0.00 | -13.41 | — |
| 2026-08-31 | `HEI` | 3 | $336.53 | $334.88 | -4.95 | — | +0.00 | -4.95 | -15.21 | — |
| 2026-08-31 | `KSS` | 70 | $17.50 | $17.26 | -16.80 | — | +0.00 | -16.80 | -69.30 | — |
| 2026-09-01 | `TH` | 67 | $18.52 | $18.45 | -4.69 | — | +0.00 | -4.69 | -36.85 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `CXW` | 51 | — | $32.31 | +0.00 | $33.66 | +68.85 | +68.85 | +0.00 | +68.85 |
| 2026-09-03 | `FRNM` | 104 | — | $15.87 | +0.00 | $16.90 | +107.12 | +107.12 | +0.00 | +107.12 |
| 2026-09-03 | `MMED` | 69 | — | $23.88 | +0.00 | $23.84 | -2.76 | -2.76 | +0.00 | -2.76 |
| 2026-09-03 | `DE` | 2 | — | $703.25 | +0.00 | $694.41 | -17.68 | -17.68 | +0.00 | -17.68 |
| 2026-09-03 | `CNXC` | 50 | — | $32.88 | +0.00 | $32.85 | -1.50 | -1.50 | +0.00 | -1.50 |
| 2026-09-03 | `OPTX` | 217 | — | $7.59 | +0.00 | $7.76 | +36.89 | +36.89 | +0.00 | +36.89 |
| 2026-09-04 | `CXW` | 51 | $33.66 | $33.46 | -10.20 | — | +0.00 | -10.20 | +58.65 | — |
| 2026-09-04 | `FRNM` | 104 | $16.90 | $16.40 | -52.00 | $16.31 | -9.36 | -61.36 | +55.12 | +45.76 |
| 2026-09-04 | `MMED` | 69 | $23.84 | $23.84 | +0.00 | $23.29 | -37.95 | -37.95 | -2.76 | -40.71 |
| 2026-09-04 | `DE` | 2 | $694.41 | $692.03 | -4.76 | — | +0.00 | -4.76 | -22.44 | — |
| 2026-09-04 | `CNXC` | 50 | $32.85 | $32.48 | -18.50 | — | +0.00 | -18.50 | -20.00 | — |
| 2026-09-04 | `OPTX` | 217 | $7.76 | $7.79 | +6.51 | — | +0.00 | +6.51 | +43.40 | — |
| 2026-09-04 | `MRX` | 43 | — | $75.65 | +0.00 | $78.27 | +112.66 | +112.66 | +0.00 | +112.66 |
| 2026-09-04 | `BAK` | 1713 | — | $1.94 | +0.00 | $1.89 | -85.65 | -85.65 | +0.00 | -85.65 |
| 2026-09-08 | `FRNM` | 104 | $16.31 | $16.74 | +44.72 | — | +0.00 | +44.72 | +90.48 | — |
| 2026-09-08 | `MMED` | 69 | $23.29 | $23.16 | -8.97 | — | +0.00 | -8.97 | -49.68 | — |
| 2026-09-08 | `MRX` | 43 | $78.27 | $78.84 | +24.51 | $76.71 | -91.59 | -67.08 | +137.17 | +45.58 |
| 2026-09-08 | `BAK` | 1713 | $1.89 | $1.94 | +85.65 | — | +0.00 | +85.65 | +0.00 | — |
| 2026-09-09 | `MRX` | 43 | $76.71 | $76.60 | -4.73 | — | +0.00 | -4.73 | +40.85 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 10 | — | $164.43 | +0.00 | $150.28 | -141.50 | -141.50 | +0.00 | -141.50 |
| 2026-09-11 | `ADBE` | 6 | — | $242.17 | +0.00 | $252.23 | +60.36 | +60.36 | +0.00 | +60.36 |
| 2026-09-11 | `AVTR` | 110 | — | $15.01 | +0.00 | $14.81 | -22.00 | -22.00 | +0.00 | -22.00 |
| 2026-09-11 | `BAK` | 784 | — | $2.12 | +0.00 | $2.08 | -31.36 | -31.36 | +0.00 | -31.36 |
| 2026-09-11 | `AMTX` | 815 | — | $2.04 | +0.00 | $2.01 | -24.45 | -24.45 | +0.00 | -24.45 |
| 2026-09-11 | `RH` | 12 | — | $135.71 | +0.00 | $134.07 | -19.68 | -19.68 | +0.00 | -19.68 |
| 2026-09-14 | `ORCL` | 10 | $150.28 | $141.42 | -88.60 | — | +0.00 | -88.60 | -230.10 | — |
| 2026-09-14 | `ADBE` | 6 | $252.23 | $261.51 | +55.68 | — | +0.00 | +55.68 | +116.04 | — |
| 2026-09-14 | `AVTR` | 110 | $14.81 | $14.87 | +6.60 | $15.16 | +31.90 | +38.50 | -15.40 | +16.50 |
| 2026-09-14 | `BAK` | 784 | $2.08 | $2.05 | -23.52 | — | +0.00 | -23.52 | -54.88 | — |
| 2026-09-14 | `AMTX` | 815 | $2.01 | $2.01 | +0.00 | — | +0.00 | +0.00 | -24.45 | — |
| 2026-09-14 | `RH` | 12 | $134.07 | $131.40 | -32.04 | — | +0.00 | -32.04 | -51.72 | — |
| 2026-09-15 | `AVTR` | 110 | $15.16 | $15.22 | +6.60 | $15.44 | +24.20 | +30.80 | +23.10 | +47.30 |
| 2026-09-16 | `AVTR` | 110 | $15.44 | $15.53 | +9.90 | $15.61 | +8.80 | +18.70 | +57.20 | +66.00 |
| 2026-09-16 | `WAY` | 76 | — | $26.27 | +0.00 | $26.59 | +24.32 | +24.32 | +0.00 | +24.32 |
| 2026-09-16 | `ASND` | 8 | — | $239.70 | +0.00 | $247.69 | +63.92 | +63.92 | +0.00 | +63.92 |
| 2026-09-16 | `SRRK` | 40 | — | $50.01 | +0.00 | $49.37 | -25.60 | -25.60 | +0.00 | -25.60 |
| 2026-09-16 | `SION` | 288 | — | $6.95 | +0.00 | $7.04 | +25.92 | +25.92 | +0.00 | +25.92 |
| 2026-09-17 | `AVTR` | 110 | $15.61 | $15.81 | +22.00 | $15.86 | +5.50 | +27.50 | +88.00 | +93.50 |
| 2026-09-17 | `WAY` | 76 | $26.59 | $26.51 | -6.08 | — | +0.00 | -6.08 | +18.24 | — |
| 2026-09-17 | `ASND` | 8 | $247.69 | $249.23 | +12.32 | — | +0.00 | +12.32 | +76.24 | — |
| 2026-09-17 | `SRRK` | 40 | $49.37 | $49.52 | +6.00 | $49.02 | -20.00 | -14.00 | -19.60 | -39.60 |
| 2026-09-17 | `SION` | 288 | $7.04 | $7.27 | +66.24 | — | +0.00 | +66.24 | +92.16 | — |
| 2026-09-17 | `SMTC` | 6 | — | $170.85 | +0.00 | $178.19 | +44.04 | +44.04 | +0.00 | +44.04 |
| 2026-09-17 | `GME` | 46 | — | $22.12 | +0.00 | $22.77 | +29.90 | +29.90 | +0.00 | +29.90 |
| 2026-09-17 | `JBHT` | 4 | — | $238.60 | +0.00 | $236.80 | -7.20 | -7.20 | +0.00 | -7.20 |
| 2026-09-17 | `ALVO` | 197 | — | $5.22 | +0.00 | $5.34 | +23.64 | +23.64 | +0.00 | +23.64 |
| 2026-09-17 | `KEY` | 49 | — | $20.98 | +0.00 | $20.95 | -1.47 | -1.47 | +0.00 | -1.47 |
| 2026-09-17 | `TNDM` | 58 | — | $17.72 | +0.00 | $17.23 | -28.42 | -28.42 | +0.00 | -28.42 |
| 2026-09-18 | `AVTR` | 110 | $15.86 | $15.87 | +1.10 | — | +0.00 | +1.10 | +94.60 | — |
| 2026-09-18 | `SRRK` | 40 | $49.02 | $48.02 | -40.00 | — | +0.00 | -40.00 | -79.60 | — |
| 2026-09-18 | `SMTC` | 6 | $178.19 | $182.33 | +24.84 | — | +0.00 | +24.84 | +68.88 | — |
| 2026-09-18 | `GME` | 46 | $22.77 | $22.90 | +5.98 | $22.64 | -11.96 | -5.98 | +35.88 | +23.92 |
| 2026-09-18 | `JBHT` | 4 | $236.80 | $236.80 | +0.00 | — | +0.00 | +0.00 | -7.20 | — |
| 2026-09-18 | `ALVO` | 197 | $5.34 | $5.40 | +11.82 | — | +0.00 | +11.82 | +35.46 | — |
| 2026-09-18 | `KEY` | 49 | $20.95 | $20.90 | -2.45 | — | +0.00 | -2.45 | -3.92 | — |
| 2026-09-18 | `TNDM` | 58 | $17.23 | $17.13 | -5.80 | — | +0.00 | -5.80 | -34.22 | — |
| 2026-09-18 | `TH` | 84 | — | $20.91 | +0.00 | $21.19 | +23.52 | +23.52 | +0.00 | +23.52 |
| 2026-09-18 | `RARE` | 119 | — | $14.79 | +0.00 | $14.51 | -33.32 | -33.32 | +0.00 | -33.32 |
| 2026-09-18 | `BHVN` | 126 | — | $14.07 | +0.00 | $13.62 | -56.70 | -56.70 | +0.00 | -56.70 |
| 2026-09-18 | `JXN` | 13 | — | $129.00 | +0.00 | $132.67 | +47.71 | +47.71 | +0.00 | +47.71 |
| 2026-09-18 | `FLNC` | 235 | — | $7.54 | +0.00 | $7.32 | -50.52 | -50.52 | +0.00 | -50.52 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +36.39 | HLIT, ANGX, ARX, MH, VELO, S, ZS | — | $1,392.75 | $10,019.55 | HLIT×94, ANGX×290, ARX×63, MH×92, VELO×81, S×52, ZS×6 |
| 2026-08-17 | +2.25 | $1,392.75 | HLIT×94, ANGX×290, ARX×63, MH×92, VELO×81, S×52, ZS×6 | $10,071.64 | +52.09 | -163.16 | GLOB, TPG, OUST, CELC | HLIT, ANGX, ARX, MH, VELO, S, ZS | $69.78 | $9,882.90 | GLOB×67, TPG×47, OUST×51, CELC×27 |
| 2026-08-18 | -6.20 | $69.78 | GLOB×67, TPG×47, OUST×51, CELC×27 | $9,774.48 | -108.42 | +0.00 | — | GLOB, TPG, OUST, CELC | $9,765.82 | $9,765.82 | — |
| 2026-08-19 | -7.20 | $9,765.82 | — | $9,765.82 | +0.00 | +0.00 | — | — | $9,765.82 | $9,765.82 | — |
| 2026-08-20 | +1.12 | $9,765.82 | — | $9,765.82 | +0.00 | -139.76 | BHP, AUTL, CRSP, MRK, ASST, MRNA, ZLAB, TEAM | — | $130.77 | $9,605.23 | BHP×13, AUTL×494, CRSP×20, MRK×8, ASST×76, MRNA×8, ZLAB×45, TEAM×7 |
| 2026-08-21 | +3.25 | $130.77 | BHP×13, AUTL×494, CRSP×20, MRK×8, ASST×76, MRNA×8, ZLAB×45, TEAM×7 | $9,790.50 | +185.27 | -22.21 | FUTU, GRAL, VIRT, ABTC, HIVE, MARA | BHP, MRK, ASST, MRNA, ZLAB, TEAM | $136.91 | $9,740.06 | AUTL×494, CRSP×20, FUTU×10, GRAL×15, VIRT×20, ABTC×141, HIVE×378, MARA×104 |
| 2026-08-24 | -5.17 | $136.91 | AUTL×494, CRSP×20, FUTU×10, GRAL×15, VIRT×20, ABTC×141, HIVE×378, MARA×104 | $9,691.46 | -48.60 | -33.50 | — | AUTL, FUTU, GRAL, VIRT, ABTC, HIVE, MARA | $8,494.11 | $9,635.61 | CRSP×20 |
| 2026-08-25 | +1.80 | $8,494.11 | CRSP×20 | $9,652.71 | +17.10 | +401.39 | EZPW, RUM, ZYME, REAX, EOLS | CRSP | $1,677.44 | $10,040.52 | EZPW×45, RUM×170, ZYME×55, REAX×66, EOLS×184 |
| 2026-08-26 | +2.02 | $1,677.44 | EZPW×45, RUM×170, ZYME×55, REAX×66, EOLS×184 | $9,898.14 | -142.38 | +368.39 | FNV, ASST, TRLV, CAPR, FWRD, FLNC | EZPW, RUM, REAX, EOLS | $69.06 | $10,243.39 | ZYME×55, FNV×5, ASST×67, TRLV×124, CAPR×168, FWRD×80, FLNC×125 |
| 2026-08-27 | — | $69.06 | ZYME×55, FNV×5, ASST×67, TRLV×124, CAPR×168, FWRD×80, FLNC×125 | $10,325.55 | +82.16 | +114.51 | GEN, SRRK | ZYME, FNV, ASST | $1,520.39 | $10,429.44 | TRLV×124, CAPR×168, FWRD×80, FLNC×125, GEN×50, SRRK×25 |
| 2026-08-28 | +0.75 | $1,520.39 | TRLV×124, CAPR×168, FWRD×80, FLNC×125, GEN×50, SRRK×25 | $10,337.53 | -91.91 | -365.79 | SMTC, PLAB, SEDG, TLS, TH, OPTX, HEI, KSS | TRLV, CAPR, FWRD, FLNC, GEN, SRRK | $315.63 | $9,939.39 | SMTC×9, PLAB×43, SEDG×39, TLS×267, TH×67, OPTX×149, HEI×3, KSS×70 |
| 2026-08-31 | -5.85 | $315.63 | SMTC×9, PLAB×43, SEDG×39, TLS×267, TH×67, OPTX×149, HEI×3, KSS×70 | $9,907.87 | -31.52 | +26.46 | — | SMTC, PLAB, SEDG, TLS, OPTX, HEI, KSS | $8,676.98 | $9,917.82 | TH×67 |
| 2026-09-01 | -6.30 | $8,676.98 | TH×67 | $9,913.13 | -4.69 | +0.00 | — | TH | $9,910.92 | $9,910.92 | — |
| 2026-09-02 | -3.83 | $9,910.92 | — | $9,910.92 | -0.00 | +0.00 | — | — | $9,910.92 | $9,910.92 | — |
| 2026-09-03 | -0.90 | $9,910.92 | — | $9,910.92 | -0.00 | +190.92 | CXW, FRNM, MMED, DE, CNXC, OPTX | — | $253.80 | $10,088.26 | CXW×51, FRNM×104, MMED×69, DE×2, CNXC×50, OPTX×217 |
| 2026-09-04 | +2.25 | $253.80 | CXW×51, FRNM×104, MMED×69, DE×2, CNXC×50, OPTX×217 | $10,009.31 | -78.95 | -20.30 | MRX, BAK | CXW, DE, CNXC, OPTX | $49.17 | $9,955.60 | FRNM×104, MMED×69, MRX×43, BAK×1713 |
| 2026-09-08 | -11.47 | $49.17 | FRNM×104, MMED×69, MRX×43, BAK×1713 | $10,101.51 | +145.91 | -91.59 | — | FRNM, MMED, BAK | $6,684.43 | $9,982.96 | MRX×43 |
| 2026-09-09 | -13.95 | $6,684.43 | MRX×43 | $9,978.23 | -4.73 | +0.00 | — | MRX | $9,976.07 | $9,976.07 | — |
| 2026-09-10 | -13.28 | $9,976.07 | — | $9,976.07 | -0.00 | +0.00 | — | — | $9,976.07 | $9,976.07 | — |
| 2026-09-11 | +0.50 | $9,976.07 | — | $9,976.07 | -0.00 | -178.63 | ORCL, ADBE, AVTR, BAK, AMTX, RH | — | $245.45 | $9,768.44 | ORCL×10, ADBE×6, AVTR×110, BAK×784, AMTX×815, RH×12 |
| 2026-09-14 | -11.00 | $245.45 | ORCL×10, ADBE×6, AVTR×110, BAK×784, AMTX×815, RH×12 | $9,686.56 | -81.88 | +31.90 | — | ORCL, ADBE, BAK, AMTX, RH | $8,023.82 | $9,691.42 | AVTR×110 |
| 2026-09-15 | -3.84 | $8,023.82 | AVTR×110 | $9,698.02 | +6.60 | +24.20 | — | — | $8,023.82 | $9,722.22 | AVTR×110 |
| 2026-09-16 | +5.30 | $8,023.82 | AVTR×110 | $9,732.12 | +9.90 | +97.36 | WAY, ASND, SRRK, SION | — | $97.64 | $9,819.42 | AVTR×110, WAY×76, ASND×8, SRRK×40, SION×288 |
| 2026-09-17 | +7.38 | $97.64 | AVTR×110, WAY×76, ASND×8, SRRK×40, SION×288 | $9,919.90 | +100.48 | +45.99 | SMTC, GME, JBHT, ALVO, KEY, TNDM | WAY, ASND, SION | $97.78 | $9,944.81 | AVTR×110, SRRK×40, SMTC×6, GME×46, JBHT×4, ALVO×197, KEY×49, TNDM×58 |
| 2026-09-18 | +4.86 | $97.78 | AVTR×110, SRRK×40, SMTC×6, GME×46, JBHT×4, ALVO×197, KEY×49, TNDM×58 | $9,940.30 | -4.51 | -81.27 | TH, RARE, BHVN, JXN, FLNC | AVTR, SRRK, SMTC, JBHT, ALVO, KEY, TNDM | $122.38 | $9,831.50 | GME×46, TH×84, RARE×119, BHVN×126, JXN×13, FLNC×235 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $8,758.81 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $7,505.17 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $6,270.08 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $5,021.21 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 81 | $15.38 | $2.23 | — | $3,773.20 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `S` | 52 | $23.77 | $2.15 | — | $2,534.75 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZS` | 6 | $190.00 | $2.01 | — | $1,392.75 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+15.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,392.75 | ▲ close $10,019.55 vs 09:30 $10,000.00 (session +36.39) | 16:00 close · cash $1,392.75 · equity $10,019.55 vs 09:30 $10,000.00 (+19.55; session marks +36.39) · 7 name(s) marked open→close (per-name table). HLIT×94 09:30 $13.18 → close $13.92 +69.56; ANGX×290 09:30 $4.31 → close $4.37 +17.40; ARX×63 09:30 $19.57 → close $19.58 +0.63; MH×92 09:30 $13.55 → close $13.10 -41.40; VELO×81 09:30 $15.38 → close $16.16 +63.18; S×52 09:30 $23.77 → close $23.11 -34.58; ZS×6 09:30 $190.00 → close $183.60 -38.40 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,392.75 | ▲ 09:30 equity $10,071.64 vs yday $10,019.55 (+52.09) | 09:30 open · cash $1,392.75 (unchanged overnight, no fees) · equity $10,071.64 vs prior close $10,019.55 (+52.09) · 7 name(s) re-marked at the open (per-name table). HLIT×94 yday $13.92 → 09:30 $13.84 -7.52; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; MH×92 yday $13.10 → 09:30 $13.16 +5.52; VELO×81 yday $16.16 → 09:30 $16.05 -8.91; S×52 yday $23.11 → 09:30 $22.50 -31.72; ZS×6 yday $183.60 → 09:30 $188.38 +28.65 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $2,691.41 | ▲ +57.47 after sell → book $10,069.34; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $4,021.61 | ▲ +76.56 after sell → book $10,065.54; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $5,252.32 | ▼ -4.38 after sell → book $10,063.34; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $6,460.75 | ▼ -40.44 after sell → book $10,061.05; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VELO` | 81 | $16.05 | $2.26 | $+49.78 | $7,758.54 | ▲ +49.78 after sell → book $10,058.79; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `S` | 52 | $22.50 | $2.17 | $-70.61 | $8,926.37 | ▼ -70.61 after sell → book $10,056.62; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZS` | 6 | $188.38 | $2.03 | $-13.79 | $10,054.60 | ▼ -13.79 after sell → book $10,054.60; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `GLOB` | 67 | $37.18 | $2.19 | — | $7,561.34 | — | prior-export headline🟢 only; gate headline=good; rank cond; list oppset; ⚪; ret5=-0.1; leftover $2513.65 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TPG` | 47 | $52.67 | $2.13 | — | $5,083.72 | — | prior-export headline🟢 only; gate headline=good; rank cond; list oppset; 🔵; ⚪; ret5=+9.4; leftover $2513.65 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 51 | $49.00 | $2.14 | — | $2,582.58 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $2513.65 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 27 | $92.99 | $2.07 | — | $69.78 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer; ret5=-0.8; leftover $2513.65 | join🟡 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $69.78 | ▼ close $9,882.90 vs 09:30 $10,071.64 (session -163.16) | 16:00 close · cash $69.78 · equity $9,882.90 vs 09:30 $10,071.64 (-188.74; session marks -163.16) · 4 name(s) marked open→close (per-name table). GLOB×67 09:30 $37.18 → close $36.26 -61.64; TPG×47 09:30 $52.67 → close $51.77 -42.30; OUST×51 09:30 $49.00 → close $48.13 -44.37; CELC×27 09:30 $92.99 → close $92.44 -14.85 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.78 | ▼ 09:30 equity $9,774.48 vs yday $9,882.90 (-108.42) | 09:30 open · cash $69.78 (unchanged overnight, no fees) · equity $9,774.48 vs prior close $9,882.90 (-108.42) · 4 name(s) re-marked at the open (per-name table). GLOB×67 yday $36.26 → 09:30 $36.98 +48.24; TPG×47 yday $51.77 → 09:30 $51.77 +0.00; OUST×51 yday $48.13 → 09:30 $45.09 -155.04; CELC×27 yday $92.44 → 09:30 $92.38 -1.62 | — |
| 2026-08-18 09:30 ET | **SELL** | `GLOB` | 67 | $36.98 | $2.22 | $-17.81 | $2,545.22 | ▼ -17.81 after sell → book $9,772.26; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 47 | $51.77 | $2.16 | $-46.59 | $4,976.25 | ▼ -46.59 after sell → book $9,770.10; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 51 | $45.09 | $2.17 | $-203.72 | $7,273.67 | ▼ -203.72 after sell → book $9,767.93; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 27 | $92.38 | $2.10 | $-20.64 | $9,765.82 | ▼ -20.64 after sell → book $9,765.82; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,765.82 | ▲ close $9,765.82 vs 09:30 $9,774.48 (session +0.00) | 16:00 close · cash $9,765.82 · no lots left · equity $9,765.82. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,765.82 | ▲ 09:30 equity $9,765.82 vs yday $9,765.82 (+0.00) | 09:30 open · cash $9,765.82 · no holdings · equity $9,765.82 vs prior close $9,765.82 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,765.82 | ▲ close $9,765.82 vs 09:30 $9,765.82 (session +0.00) | 16:00 close · cash $9,765.82 · no lots left · equity $9,765.82. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,765.82 | ▲ 09:30 equity $9,765.82 vs yday $9,765.82 (+0.00) | 09:30 open · cash $9,765.82 · no holdings · equity $9,765.82 vs prior close $9,765.82 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,580.67 | — | prior-export headline🟢 only; gate headline=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1220.73 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 494 | $2.47 | $6.37 | — | $7,354.11 | — | prior-export headline🟢 only; gate headline=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1220.73 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 20 | $58.73 | $2.05 | — | $6,177.46 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1220.73 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRK` | 8 | $150.78 | $2.01 | — | $4,969.21 | — | prior-export headline🟢 only; gate headline=good; rank cond; list oppset; 🔵; ⚪; ret5=+14.5; leftover $1220.73 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 76 | $16.00 | $2.22 | — | $3,750.99 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1220.73 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $2,547.86 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ret5=+173.9; leftover $1220.73 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 45 | $26.57 | $2.12 | — | $1,350.08 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1220.73 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TEAM` | 7 | $173.90 | $2.01 | — | $130.77 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+12.2; leftover $1220.73 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $130.77 | ▼ close $9,605.23 vs 09:30 $9,765.82 (session -139.76) | 16:00 close · cash $130.77 · equity $9,605.23 vs 09:30 $9,765.82 (-160.59; session marks -139.76) · 8 name(s) marked open→close (per-name table). BHP×13 09:30 $91.01 → close $93.63 +34.06; AUTL×494 09:30 $2.47 → close $2.46 -4.94; CRSP×20 09:30 $58.73 → close $58.12 -12.20; MRK×8 09:30 $150.78 → close $148.99 -14.32; ASST×76 09:30 $16.00 → close $16.13 +9.88; MRNA×8 09:30 $150.14 → close $133.32 -134.56; ZLAB×45 09:30 $26.57 → close $26.02 -24.75; TEAM×7 09:30 $173.90 → close $174.91 +7.07 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $130.77 | ▲ 09:30 equity $9,790.50 vs yday $9,605.23 (+185.27) | 09:30 open · cash $130.77 (unchanged overnight, no fees) · equity $9,790.50 vs prior close $9,605.23 (+185.27) · 8 name(s) re-marked at the open (per-name table). BHP×13 yday $93.63 → 09:30 $95.72 +27.17; AUTL×494 yday $2.46 → 09:30 $2.47 +4.94; CRSP×20 yday $58.12 → 09:30 $59.72 +32.00; MRK×8 yday $148.99 → 09:30 $149.12 +1.04; ASST×76 yday $16.13 → 09:30 $17.66 +116.28; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; ZLAB×45 yday $26.02 → 09:30 $26.25 +10.35; TEAM×7 yday $174.91 → 09:30 $174.22 -4.83 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,373.08 | ▲ +57.15 after sell → book $9,788.45; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRK` | 8 | $149.12 | $2.03 | $-17.33 | $2,564.01 | ▼ -17.33 after sell → book $9,786.42; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 76 | $17.66 | $2.24 | $+121.70 | $3,903.93 | ▲ +121.70 after sell → book $9,784.18; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $4,966.77 | ▼ -140.29 after sell → book $9,782.14; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 45 | $26.25 | $2.15 | $-18.67 | $6,145.88 | ▼ -18.67 after sell → book $9,780.00; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TEAM` | 7 | $174.22 | $2.03 | $-1.80 | $7,363.39 | ▼ -1.80 after sell → book $9,777.97; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $6,209.57 | — | prior-export headline🟢 only; gate headline=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1227.23 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 15 | $78.88 | $2.04 | — | $5,024.33 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1227.23 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `VIRT` | 20 | $60.66 | $2.05 | — | $3,809.08 | — | prior-export headline🟢 only; gate headline=good; rank cond; list oppset; 🔵; ⚪; ret5=+7.0; leftover $1227.23 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 141 | $8.66 | $2.41 | — | $2,585.61 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1227.23 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 378 | $3.24 | $4.88 | — | $1,356.01 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $1227.23 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 104 | $11.70 | $2.30 | — | $136.91 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1227.23 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $136.91 | ▼ close $9,740.06 vs 09:30 $9,790.50 (session -22.21) | 16:00 close · cash $136.91 · equity $9,740.06 vs 09:30 $9,790.50 (-50.44; session marks -22.21) · 8 name(s) marked open→close (per-name table). AUTL×494 09:30 $2.47 → close $2.41 -29.64; CRSP×20 09:30 $59.72 → close $59.50 -4.40; FUTU×10 09:30 $115.18 → close $123.64 +84.60; GRAL×15 09:30 $78.88 → close $79.54 +9.90; VIRT×20 09:30 $60.66 → close $67.93 +145.40; ABTC×141 09:30 $8.66 → close $7.93 -102.93; HIVE×378 09:30 $3.24 → close $3.03 -79.38; MARA×104 09:30 $11.70 → close $11.26 -45.76 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $136.91 | ▼ 09:30 equity $9,691.46 vs yday $9,740.06 (-48.60) | 09:30 open · cash $136.91 (unchanged overnight, no fees) · equity $9,691.46 vs prior close $9,740.06 (-48.60) · 8 name(s) re-marked at the open (per-name table). AUTL×494 yday $2.41 → 09:30 $2.40 -4.94; CRSP×20 yday $59.50 → 09:30 $58.75 -15.00; FUTU×10 yday $123.64 → 09:30 $121.00 -26.40; GRAL×15 yday $79.54 → 09:30 $81.87 +34.95; VIRT×20 yday $67.93 → 09:30 $66.80 -22.60; ABTC×141 yday $7.93 → 09:30 $8.00 +9.87; HIVE×378 yday $3.03 → 09:30 $2.99 -15.12; MARA×104 yday $11.26 → 09:30 $11.17 -9.36 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 494 | $2.40 | $6.46 | $-47.42 | $1,316.05 | ▼ -47.42 after sell → book $9,685.00; vs 09:30 mark -6.46 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $121.00 | $2.04 | $+54.14 | $2,524.01 | ▲ +54.14 after sell → book $9,682.96; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 15 | $81.87 | $2.06 | $+40.76 | $3,750.00 | ▲ +40.76 after sell → book $9,680.90; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `VIRT` | 20 | $66.80 | $2.07 | $+118.68 | $5,083.93 | ▲ +118.68 after sell → book $9,678.83; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 141 | $8.00 | $2.45 | $-97.92 | $6,209.48 | ▼ -97.92 after sell → book $9,676.38; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 378 | $2.99 | $4.95 | $-104.33 | $7,334.76 | ▼ -104.33 after sell → book $9,671.44; vs 09:30 mark -4.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 104 | $11.17 | $2.33 | $-59.75 | $8,494.11 | ▼ -59.75 after sell → book $9,669.11; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,494.11 | ▼ close $9,635.61 vs 09:30 $9,691.46 (session -33.50) | 16:00 close · cash $8,494.11 · equity $9,635.61 vs 09:30 $9,691.46 (-55.85; session marks -33.50) · 1 name(s) marked open→close (per-name table). CRSP×20 09:30 $58.75 → close $57.08 -33.50 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,494.11 | ▲ 09:30 equity $9,652.71 vs yday $9,635.61 (+17.10) | 09:30 open · cash $8,494.11 (unchanged overnight, no fees) · equity $9,652.71 vs prior close $9,635.61 (+17.10) · 1 name(s) re-marked at the open (per-name table). CRSP×20 yday $57.08 → 09:30 $57.93 +17.10 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 20 | $57.93 | $2.07 | $-20.12 | $9,650.64 | ▼ -20.12 after sell → book $9,650.64; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 45 | $35.05 | $2.12 | — | $8,071.26 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,oppset; 🔵; ⚪; ret5=+19.7; leftover $1608.44 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 170 | $9.42 | $2.50 | — | $6,467.36 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,ohlc_hot,oppset; 🔵; ret5=+13.6; leftover $1608.44 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 55 | $28.86 | $2.15 | — | $4,877.91 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+13.7; leftover $1608.44 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 66 | $24.11 | $2.19 | — | $3,284.46 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=+891.7; leftover $1608.44 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 184 | $8.72 | $2.54 | — | $1,677.44 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+13.0; leftover $1608.44 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,677.44 | ▲ close $10,040.52 vs 09:30 $9,652.71 (session +401.39) | 16:00 close · cash $1,677.44 · equity $10,040.52 vs 09:30 $9,652.71 (+387.81; session marks +401.39) · 5 name(s) marked open→close (per-name table). EZPW×45 09:30 $35.05 → close $35.23 +8.10; RUM×170 09:30 $9.42 → close $10.23 +137.70; ZYME×55 09:30 $28.86 → close $27.47 -76.45; REAX×66 09:30 $24.11 → close $28.43 +285.12; EOLS×184 09:30 $8.72 → close $8.97 +46.92 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,677.44 | ▼ 09:30 equity $9,898.14 vs yday $10,040.52 (-142.38) | 09:30 open · cash $1,677.44 (unchanged overnight, no fees) · equity $9,898.14 vs prior close $10,040.52 (-142.38) · 5 name(s) re-marked at the open (per-name table). EZPW×45 yday $35.23 → 09:30 $35.70 +21.15; RUM×170 yday $10.23 → 09:30 $10.07 -27.20; ZYME×55 yday $27.47 → 09:30 $27.56 +4.95; REAX×66 yday $28.43 → 09:30 $26.61 -120.12; EOLS×184 yday $8.97 → 09:30 $8.86 -21.16 | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 45 | $35.70 | $2.15 | $+24.98 | $3,281.79 | ▲ +24.98 after sell → book $9,895.99; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 170 | $10.07 | $2.54 | $+105.46 | $4,991.15 | ▲ +105.46 after sell → book $9,893.45; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 66 | $26.61 | $2.21 | $+160.60 | $6,745.19 | ▲ +160.60 after sell → book $9,891.23; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 184 | $8.86 | $2.59 | $+20.63 | $8,372.85 | ▲ +20.63 after sell → book $9,888.65; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 5 | $267.02 | $2.00 | — | $7,035.74 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $1395.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ASST` | 67 | $20.72 | $2.19 | — | $5,645.31 | — | prior-export headline🟢 only; gate headline=good; rank cond; list oppset; 🔵; ret5=+67.1; leftover $1395.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 124 | $11.22 | $2.36 | — | $4,251.67 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+16.8; leftover $1395.47 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 168 | $8.29 | $2.49 | — | $2,856.46 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover,ohlc_hot,oppset; 🔵; ret5=+17.1; leftover $1395.47 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 80 | $17.41 | $2.23 | — | $1,461.43 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-9.2; leftover $1395.47 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 125 | $11.12 | $2.37 | — | $69.06 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1395.47 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $69.06 | ▲ close $10,243.39 vs 09:30 $9,898.14 (session +368.39) | 16:00 close · cash $69.06 · equity $10,243.39 vs 09:30 $9,898.14 (+345.25; session marks +368.39) · 7 name(s) marked open→close (per-name table). ZYME×55 09:30 $27.56 → close $29.30 +95.98; FNV×5 09:30 $267.02 → close $267.37 +1.75; ASST×67 09:30 $20.72 → close $21.50 +52.26; TRLV×124 09:30 $11.22 → close $11.43 +26.04; CAPR×168 09:30 $8.29 → close $9.36 +179.76; FWRD×80 09:30 $17.41 → close $17.63 +17.60; FLNC×125 09:30 $11.12 → close $11.08 -5.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.06 | ▲ 09:30 equity $10,325.55 vs yday $10,243.39 (+82.16) | 09:30 open · cash $69.06 (unchanged overnight, no fees) · equity $10,325.55 vs prior close $10,243.39 (+82.16) · 7 name(s) re-marked at the open (per-name table). ZYME×55 yday $29.30 → 09:30 $29.33 +1.37; FNV×5 yday $267.37 → 09:30 $267.23 -0.70; ASST×67 yday $21.50 → 09:30 $22.45 +63.65; TRLV×124 yday $11.43 → 09:30 $11.38 -6.20; CAPR×168 yday $9.36 → 09:30 $9.19 -28.56; FWRD×80 yday $17.63 → 09:30 $17.60 -2.40; FLNC×125 yday $11.08 → 09:30 $11.52 +55.00 | — |
| 2026-08-27 09:30 ET | **SELL** | `ZYME` | 55 | $29.33 | $2.18 | $+21.52 | $1,680.03 | ▲ +21.52 after sell → book $10,323.37; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 5 | $267.23 | $2.03 | $-2.98 | $3,014.16 | ▼ -2.98 after sell → book $10,321.35; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 67 | $22.45 | $2.21 | $+111.50 | $4,516.09 | ▲ +111.50 after sell → book $10,319.13; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 50 | $29.83 | $2.14 | — | $3,022.45 | — | prior-export headline🟢 only; gate headline=good; rank cond; list mover_buy; 🔵; ret5=+7.6; leftover $1505.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SRRK` | 25 | $60.00 | $2.06 | — | $1,520.39 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+6.2; leftover $1505.36 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,520.39 | ▲ close $10,429.44 vs 09:30 $10,325.55 (session +114.51) | 16:00 close · cash $1,520.39 · equity $10,429.44 vs 09:30 $10,325.55 (+103.89; session marks +114.51) · 6 name(s) marked open→close (per-name table). TRLV×124 09:30 $11.38 → close $11.03 -43.40; CAPR×168 09:30 $9.19 → close $10.06 +146.16; FWRD×80 09:30 $17.60 → close $17.70 +8.00; FLNC×125 09:30 $11.52 → close $11.43 -11.25; GEN×50 09:30 $29.83 → close $30.50 +33.50; SRRK×25 09:30 $60.00 → close $59.26 -18.50 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,520.39 | ▼ 09:30 equity $10,337.53 vs yday $10,429.44 (-91.91) | 09:30 open · cash $1,520.39 (unchanged overnight, no fees) · equity $10,337.53 vs prior close $10,429.44 (-91.91) · 6 name(s) re-marked at the open (per-name table). TRLV×124 yday $11.03 → 09:30 $11.00 -3.72; CAPR×168 yday $10.06 → 09:30 $9.73 -55.44; FWRD×80 yday $17.70 → 09:30 $17.70 +0.00; FLNC×125 yday $11.43 → 09:30 $11.27 -20.00; GEN×50 yday $30.50 → 09:30 $30.50 +0.00; SRRK×25 yday $59.26 → 09:30 $58.75 -12.75 | — |
| 2026-08-28 09:30 ET | **SELL** | `TRLV` | 124 | $11.00 | $2.39 | $-32.04 | $2,881.99 | ▼ -32.04 after sell → book $10,335.13; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 168 | $9.73 | $2.54 | $+236.89 | $4,514.10 | ▲ +236.89 after sell → book $10,332.60; vs 09:30 mark -2.53 | dropped from list after 2 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `FWRD` | 80 | $17.70 | $2.25 | $+18.72 | $5,927.84 | ▲ +18.72 after sell → book $10,330.34; vs 09:30 mark -2.26 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 125 | $11.27 | $2.40 | $+13.99 | $7,334.20 | ▲ +13.99 after sell → book $10,327.95; vs 09:30 mark -2.39 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 50 | $30.50 | $2.16 | $+29.20 | $8,857.04 | ▲ +29.20 after sell → book $10,325.79; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SRRK` | 25 | $58.75 | $2.09 | $-35.40 | $10,323.70 | ▼ -35.40 after sell → book $10,323.70; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $9,045.84 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $1290.46 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PLAB` | 43 | $30.01 | $2.12 | — | $7,753.29 | — | prior-export headline🟢 only; gate headline=good; rank cond; list oppset; 🔵; ret5=-0.9; leftover $1290.46 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $32.90 | $2.11 | — | $6,468.09 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1290.46 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 267 | $4.82 | $3.44 | — | $5,177.70 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1290.46 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 67 | $19.00 | $2.19 | — | $3,902.51 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+7.5; leftover $1290.46 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 149 | $8.61 | $2.44 | — | $2,617.18 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer; ret5=-0.7; leftover $1290.46 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HEI` | 3 | $339.95 | $2.00 | — | $1,595.33 | — | prior-export headline🟢 only; gate headline=good; rank cond; list oppset; ret5=-3.9; leftover $1290.46 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KSS` | 70 | $18.25 | $2.20 | — | $315.63 | — | prior-export headline🟢 only; gate headline=good; rank cond; list oppset; ret5=+4.7; leftover $1290.46 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟡 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $315.63 | ▼ close $9,939.39 vs 09:30 $10,337.53 (session -365.79) | 16:00 close · cash $315.63 · equity $9,939.39 vs 09:30 $10,337.53 (-398.14; session marks -365.79) · 8 name(s) marked open→close (per-name table). SMTC×9 09:30 $141.76 → close $131.17 -95.31; PLAB×43 09:30 $30.01 → close $27.73 -98.04; SEDG×39 09:30 $32.90 → close $31.41 -58.11; TLS×267 09:30 $4.82 → close $4.79 -8.01; TH×67 09:30 $19.00 → close $18.55 -30.15; OPTX×149 09:30 $8.61 → close $8.52 -13.41; HEI×3 09:30 $339.95 → close $336.53 -10.26; KSS×70 09:30 $18.25 → close $17.50 -52.50 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $315.63 | ▼ 09:30 equity $9,907.87 vs yday $9,939.39 (-31.52) | 09:30 open · cash $315.63 (unchanged overnight, no fees) · equity $9,907.87 vs prior close $9,939.39 (-31.52) · 8 name(s) re-marked at the open (per-name table). SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; PLAB×43 yday $27.73 → 09:30 $28.04 +13.33; SEDG×39 yday $31.41 → 09:30 $31.15 -10.14; TLS×267 yday $4.79 → 09:30 $4.81 +5.34; TH×67 yday $18.55 → 09:30 $18.12 -28.48; OPTX×149 yday $8.52 → 09:30 $8.52 +0.00; HEI×3 yday $336.53 → 09:30 $334.88 -4.95; KSS×70 yday $17.50 → 09:30 $17.26 -16.80 | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $1,504.30 | ▼ -89.19 after sell → book $9,905.83; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PLAB` | 43 | $28.04 | $2.14 | $-88.97 | $2,707.88 | ▼ -88.97 after sell → book $9,903.69; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 39 | $31.15 | $2.13 | $-72.48 | $3,920.60 | ▼ -72.48 after sell → book $9,901.57; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 267 | $4.81 | $3.50 | $-9.61 | $5,201.37 | ▼ -9.61 after sell → book $9,898.07; vs 09:30 mark -3.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 149 | $8.52 | $2.47 | $-18.32 | $6,468.38 | ▼ -18.32 after sell → book $9,895.60; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HEI` | 3 | $334.88 | $2.02 | $-19.23 | $7,471.00 | ▼ -19.23 after sell → book $9,893.58; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KSS` | 70 | $17.26 | $2.22 | $-73.72 | $8,676.98 | ▼ -73.72 after sell → book $9,891.35; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,676.98 | ▲ close $9,917.82 vs 09:30 $9,907.87 (session +26.46) | 16:00 close · cash $8,676.98 · equity $9,917.82 vs 09:30 $9,907.87 (+9.95; session marks +26.46) · 1 name(s) marked open→close (per-name table). TH×67 09:30 $18.12 → close $18.52 +26.46 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,676.98 | ▼ 09:30 equity $9,913.13 vs yday $9,917.82 (-4.69) | 09:30 open · cash $8,676.98 (unchanged overnight, no fees) · equity $9,913.13 vs prior close $9,917.82 (-4.69) · 1 name(s) re-marked at the open (per-name table). TH×67 yday $18.52 → 09:30 $18.45 -4.69 | — |
| 2026-09-01 09:30 ET | **SELL** | `TH` | 67 | $18.45 | $2.21 | $-41.25 | $9,910.92 | ▼ -41.25 after sell → book $9,910.92; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,910.92 | ▲ close $9,910.92 vs 09:30 $9,913.13 (session +0.00) | 16:00 close · cash $9,910.92 · no lots left · equity $9,910.92. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,910.92 | ▲ 09:30 equity $9,910.92 vs yday $9,910.92 (-0.00) | 09:30 open · cash $9,910.92 · no holdings · equity $9,910.92 vs prior close $9,910.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,910.92 | ▲ close $9,910.92 vs 09:30 $9,910.92 (session +0.00) | 16:00 close · cash $9,910.92 · no lots left · equity $9,910.92. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,910.92 | ▲ 09:30 equity $9,910.92 vs yday $9,910.92 (-0.00) | 09:30 open · cash $9,910.92 · no holdings · equity $9,910.92 vs prior close $9,910.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 51 | $32.31 | $2.14 | — | $8,260.96 | — | prior-export headline🟢 only; gate headline=good; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1651.82 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 104 | $15.87 | $2.30 | — | $6,608.18 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1651.82 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 69 | $23.88 | $2.20 | — | $4,958.27 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+21.9; leftover $1651.82 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $3,549.77 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1651.82 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 50 | $32.88 | $2.14 | — | $1,903.63 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=+16.2; leftover $1651.82 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 217 | $7.59 | $2.80 | — | $253.80 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-11.5; leftover $1651.82 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $253.80 | ▲ close $10,088.26 vs 09:30 $9,910.92 (session +190.92) | 16:00 close · cash $253.80 · equity $10,088.26 vs 09:30 $9,910.92 (+177.34; session marks +190.92) · 6 name(s) marked open→close (per-name table). CXW×51 09:30 $32.31 → close $33.66 +68.85; FRNM×104 09:30 $15.87 → close $16.90 +107.12; MMED×69 09:30 $23.88 → close $23.84 -2.76; DE×2 09:30 $703.25 → close $694.41 -17.68; CNXC×50 09:30 $32.88 → close $32.85 -1.50; OPTX×217 09:30 $7.59 → close $7.76 +36.89 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $253.80 | ▼ 09:30 equity $10,009.31 vs yday $10,088.26 (-78.95) | 09:30 open · cash $253.80 (unchanged overnight, no fees) · equity $10,009.31 vs prior close $10,088.26 (-78.95) · 6 name(s) re-marked at the open (per-name table). CXW×51 yday $33.66 → 09:30 $33.46 -10.20; FRNM×104 yday $16.90 → 09:30 $16.40 -52.00; MMED×69 yday $23.84 → 09:30 $23.84 +0.00; DE×2 yday $694.41 → 09:30 $692.03 -4.76; CNXC×50 yday $32.85 → 09:30 $32.48 -18.50; OPTX×217 yday $7.76 → 09:30 $7.79 +6.51 | — |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 51 | $33.46 | $2.17 | $+54.34 | $1,958.09 | ▲ +54.34 after sell → book $10,007.14; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 2 | $692.03 | $2.02 | $-26.45 | $3,340.14 | ▼ -26.45 after sell → book $10,005.13; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 50 | $32.48 | $2.16 | $-24.30 | $4,961.97 | ▼ -24.30 after sell → book $10,002.96; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 217 | $7.79 | $2.85 | $+37.75 | $6,649.55 | ▲ +37.75 after sell → book $10,000.11; vs 09:30 mark -2.85 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 43 | $75.65 | $2.12 | — | $3,394.49 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $3324.78 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 1713 | $1.94 | $22.10 | — | $49.17 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $3324.78 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.17 | ▼ close $9,955.60 vs 09:30 $10,009.31 (session -20.30) | 16:00 close · cash $49.17 · equity $9,955.60 vs 09:30 $10,009.31 (-53.71; session marks -20.30) · 4 name(s) marked open→close (per-name table). FRNM×104 09:30 $16.40 → close $16.31 -9.36; MMED×69 09:30 $23.84 → close $23.29 -37.95; MRX×43 09:30 $75.65 → close $78.27 +112.66; BAK×1713 09:30 $1.94 → close $1.89 -85.65 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.17 | ▲ 09:30 equity $10,101.51 vs yday $9,955.60 (+145.91) | 09:30 open · cash $49.17 (unchanged overnight, no fees) · equity $10,101.51 vs prior close $9,955.60 (+145.91) · 4 name(s) re-marked at the open (per-name table). FRNM×104 yday $16.31 → 09:30 $16.74 +44.72; MMED×69 yday $23.29 → 09:30 $23.16 -8.97; MRX×43 yday $78.27 → 09:30 $78.84 +24.51; BAK×1713 yday $1.89 → 09:30 $1.94 +85.65 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 104 | $16.74 | $2.33 | $+85.84 | $1,787.79 | ▲ +85.84 after sell → book $10,099.17; vs 09:30 mark -2.34 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MMED` | 69 | $23.16 | $2.22 | $-54.10 | $3,383.61 | ▼ -54.10 after sell → book $10,096.95; vs 09:30 mark -2.22 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 1713 | $1.94 | $22.41 | $-44.51 | $6,684.43 | ▼ -44.51 after sell → book $10,074.55; vs 09:30 mark -22.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,684.43 | ▼ close $9,982.96 vs 09:30 $10,101.51 (session -91.59) | 16:00 close · cash $6,684.43 · equity $9,982.96 vs 09:30 $10,101.51 (-118.55; session marks -91.59) · 1 name(s) marked open→close (per-name table). MRX×43 09:30 $78.84 → close $76.71 -91.59 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,684.43 | ▼ 09:30 equity $9,978.23 vs yday $9,982.96 (-4.73) | 09:30 open · cash $6,684.43 (unchanged overnight, no fees) · equity $9,978.23 vs prior close $9,982.96 (-4.73) · 1 name(s) re-marked at the open (per-name table). MRX×43 yday $76.71 → 09:30 $76.60 -4.73 | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 43 | $76.60 | $2.16 | $+36.58 | $9,976.07 | ▲ +36.58 after sell → book $9,976.07; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,976.07 | ▲ close $9,976.07 vs 09:30 $9,978.23 (session +0.00) | 16:00 close · cash $9,976.07 · no lots left · equity $9,976.07. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,976.07 | ▲ 09:30 equity $9,976.07 vs yday $9,976.07 (-0.00) | 09:30 open · cash $9,976.07 · no holdings · equity $9,976.07 vs prior close $9,976.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,976.07 | ▲ close $9,976.07 vs 09:30 $9,976.07 (session +0.00) | 16:00 close · cash $9,976.07 · no lots left · equity $9,976.07. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,976.07 | ▲ 09:30 equity $9,976.07 vs yday $9,976.07 (-0.00) | 09:30 open · cash $9,976.07 · no holdings · equity $9,976.07 vs prior close $9,976.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 10 | $164.43 | $2.02 | — | $8,329.75 | — | prior-export headline🟢 only; gate headline=good; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1662.68 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 6 | $242.17 | $2.01 | — | $6,874.72 | — | prior-export headline🟢 only; gate headline=good; rank cond; list earn_react; ret5=-11.1; leftover $1662.68 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 110 | $15.01 | $2.32 | — | $5,221.30 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $1662.68 | join🟢 sector🟡 gen🟡 news🟢 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 784 | $2.12 | $10.11 | — | $3,549.11 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1662.68 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 815 | $2.04 | $10.51 | — | $1,875.99 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1662.68 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 12 | $135.71 | $2.03 | — | $245.45 | — | prior-export headline🟢 only; gate headline=good; rank cond; list earn_react,oppset; ret5=-9.2; leftover $1662.68 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $245.45 | ▼ close $9,768.44 vs 09:30 $9,976.07 (session -178.63) | 16:00 close · cash $245.45 · equity $9,768.44 vs 09:30 $9,976.07 (-207.63; session marks -178.63) · 6 name(s) marked open→close (per-name table). ORCL×10 09:30 $164.43 → close $150.28 -141.50; ADBE×6 09:30 $242.17 → close $252.23 +60.36; AVTR×110 09:30 $15.01 → close $14.81 -22.00; BAK×784 09:30 $2.12 → close $2.08 -31.36; AMTX×815 09:30 $2.04 → close $2.01 -24.45; RH×12 09:30 $135.71 → close $134.07 -19.68 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $245.45 | ▼ 09:30 equity $9,686.56 vs yday $9,768.44 (-81.88) | 09:30 open · cash $245.45 (unchanged overnight, no fees) · equity $9,686.56 vs prior close $9,768.44 (-81.88) · 6 name(s) re-marked at the open (per-name table). ORCL×10 yday $150.28 → 09:30 $141.42 -88.60; ADBE×6 yday $252.23 → 09:30 $261.51 +55.68; AVTR×110 yday $14.81 → 09:30 $14.87 +6.60; BAK×784 yday $2.08 → 09:30 $2.05 -23.52; AMTX×815 yday $2.01 → 09:30 $2.01 +0.00; RH×12 yday $134.07 → 09:30 $131.40 -32.04 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 10 | $141.42 | $2.04 | $-234.16 | $1,657.61 | ▼ -234.16 after sell → book $9,684.52; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 6 | $261.51 | $2.03 | $+112.00 | $3,224.64 | ▲ +112.00 after sell → book $9,682.49; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 784 | $2.05 | $10.26 | $-75.25 | $4,821.58 | ▼ -75.25 after sell → book $9,672.23; vs 09:30 mark -10.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 815 | $2.01 | $10.66 | $-45.63 | $6,449.07 | ▼ -45.63 after sell → book $9,661.57; vs 09:30 mark -10.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 12 | $131.40 | $2.05 | $-55.79 | $8,023.82 | ▼ -55.79 after sell → book $9,659.52; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,023.82 | ▲ close $9,691.42 vs 09:30 $9,686.56 (session +31.90) | 16:00 close · cash $8,023.82 · equity $9,691.42 vs 09:30 $9,686.56 (+4.86; session marks +31.90) · 1 name(s) marked open→close (per-name table). AVTR×110 09:30 $14.87 → close $15.16 +31.90 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,023.82 | ▲ 09:30 equity $9,698.02 vs yday $9,691.42 (+6.60) | 09:30 open · cash $8,023.82 (unchanged overnight, no fees) · equity $9,698.02 vs prior close $9,691.42 (+6.60) · 1 name(s) re-marked at the open (per-name table). AVTR×110 yday $15.16 → 09:30 $15.22 +6.60 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,023.82 | ▲ close $9,722.22 vs 09:30 $9,698.02 (session +24.20) | 16:00 close · cash $8,023.82 · equity $9,722.22 vs 09:30 $9,698.02 (+24.20; session marks +24.20) · 1 name(s) marked open→close (per-name table). AVTR×110 09:30 $15.22 → close $15.44 +24.20 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,023.82 | ▲ 09:30 equity $9,732.12 vs yday $9,722.22 (+9.90) | 09:30 open · cash $8,023.82 (unchanged overnight, no fees) · equity $9,732.12 vs prior close $9,722.22 (+9.90) · 1 name(s) re-marked at the open (per-name table). AVTR×110 yday $15.44 → 09:30 $15.53 +9.90 | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 76 | $26.27 | $2.22 | — | $6,025.08 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2005.95 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `ASND` | 8 | $239.70 | $2.01 | — | $4,105.47 | — | prior-export headline🟢 only; gate headline=good; rank cond; list oppset; ret5=-11.2; leftover $2005.95 | join🟡 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SRRK` | 40 | $50.01 | $2.11 | — | $2,102.96 | — | prior-export headline🟢 only; gate headline=good; rank cond; list oppset; ret5=+1.1; leftover $2005.95 | join🟡 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 288 | $6.95 | $3.72 | — | $97.64 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-5.8; leftover $2005.95 | join🔴 sector🟢 gen🟢 news🟢 digest🔴 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.64 | ▲ close $9,819.42 vs 09:30 $9,732.12 (session +97.36) | 16:00 close · cash $97.64 · equity $9,819.42 vs 09:30 $9,732.12 (+87.30; session marks +97.36) · 5 name(s) marked open→close (per-name table). AVTR×110 09:30 $15.53 → close $15.61 +8.80; WAY×76 09:30 $26.27 → close $26.59 +24.32; ASND×8 09:30 $239.70 → close $247.69 +63.92; SRRK×40 09:30 $50.01 → close $49.37 -25.60; SION×288 09:30 $6.95 → close $7.04 +25.92 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.64 | ▲ 09:30 equity $9,919.90 vs yday $9,819.42 (+100.48) | 09:30 open · cash $97.64 (unchanged overnight, no fees) · equity $9,919.90 vs prior close $9,819.42 (+100.48) · 5 name(s) re-marked at the open (per-name table). AVTR×110 yday $15.61 → 09:30 $15.81 +22.00; WAY×76 yday $26.59 → 09:30 $26.51 -6.08; ASND×8 yday $247.69 → 09:30 $249.23 +12.32; SRRK×40 yday $49.37 → 09:30 $49.52 +6.00; SION×288 yday $7.04 → 09:30 $7.27 +66.24 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 76 | $26.51 | $2.25 | $+13.78 | $2,110.16 | ▲ +13.78 after sell → book $9,917.66; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ASND` | 8 | $249.23 | $2.04 | $+72.19 | $4,101.96 | ▲ +72.19 after sell → book $9,915.62; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 288 | $7.27 | $3.78 | $+84.66 | $6,191.94 | ▲ +84.66 after sell → book $9,911.84; vs 09:30 mark -3.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 6 | $170.85 | $2.01 | — | $5,164.83 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1031.99 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 46 | $22.12 | $2.13 | — | $4,145.18 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1031.99 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 4 | $238.60 | $2.00 | — | $3,188.78 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover,oppset; ret5=-11.6; leftover $1031.99 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ALVO` | 197 | $5.22 | $2.58 | — | $2,157.86 | — | prior-export headline🟢 only; gate headline=good; rank cond; list oppset; 🔵; ret5=-2.1; leftover $1031.99 | join🔴 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `KEY` | 49 | $20.98 | $2.14 | — | $1,127.70 | — | prior-export headline🟢 only; gate headline=good; rank cond; list oppset; 🔵; ret5=+0.8; leftover $1031.99 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 58 | $17.72 | $2.16 | — | $97.78 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=-8.3; leftover $1031.99 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.78 | ▲ close $9,944.81 vs 09:30 $9,919.90 (session +45.99) | 16:00 close · cash $97.78 · equity $9,944.81 vs 09:30 $9,919.90 (+24.91; session marks +45.99) · 8 name(s) marked open→close (per-name table). AVTR×110 09:30 $15.81 → close $15.86 +5.50; SRRK×40 09:30 $49.52 → close $49.02 -20.00; SMTC×6 09:30 $170.85 → close $178.19 +44.04; GME×46 09:30 $22.12 → close $22.77 +29.90; JBHT×4 09:30 $238.60 → close $236.80 -7.20; ALVO×197 09:30 $5.22 → close $5.34 +23.64; KEY×49 09:30 $20.98 → close $20.95 -1.47; TNDM×58 09:30 $17.72 → close $17.23 -28.42 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.78 | ▼ 09:30 equity $9,940.30 vs yday $9,944.81 (-4.51) | 09:30 open · cash $97.78 (unchanged overnight, no fees) · equity $9,940.30 vs prior close $9,944.81 (-4.51) · 8 name(s) re-marked at the open (per-name table). AVTR×110 yday $15.86 → 09:30 $15.87 +1.10; SRRK×40 yday $49.02 → 09:30 $48.02 -40.00; SMTC×6 yday $178.19 → 09:30 $182.33 +24.84; GME×46 yday $22.77 → 09:30 $22.90 +5.98; JBHT×4 yday $236.80 → 09:30 $236.80 +0.00; ALVO×197 yday $5.34 → 09:30 $5.40 +11.82; KEY×49 yday $20.95 → 09:30 $20.90 -2.45; TNDM×58 yday $17.23 → 09:30 $17.13 -5.80 | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 110 | $15.87 | $2.35 | $+89.93 | $1,841.12 | ▲ +89.93 after sell → book $9,937.94; vs 09:30 mark -2.36 | dropped from list after 5 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SRRK` | 40 | $48.02 | $2.14 | $-83.85 | $3,759.79 | ▼ -83.85 after sell → book $9,935.81; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 6 | $182.33 | $2.03 | $+64.84 | $4,851.74 | ▲ +64.84 after sell → book $9,933.78; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 4 | $236.80 | $2.02 | $-11.22 | $5,796.92 | ▼ -11.22 after sell → book $9,931.76; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ALVO` | 197 | $5.40 | $2.62 | $+30.26 | $6,858.09 | ▲ +30.26 after sell → book $9,929.13; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `KEY` | 49 | $20.90 | $2.16 | $-8.21 | $7,880.04 | ▼ -8.21 after sell → book $9,926.98; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 58 | $17.13 | $2.18 | $-38.57 | $8,871.39 | ▼ -38.57 after sell → book $9,924.79; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 84 | $20.91 | $2.24 | — | $7,112.71 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1774.28 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 119 | $14.79 | $2.35 | — | $5,350.35 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $1774.28 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 126 | $14.07 | $2.37 | — | $3,575.17 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1774.28 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `JXN` | 13 | $129.00 | $2.03 | — | $1,896.14 | — | prior-export headline🟢 only; gate headline=good; rank cond; list oppset; ret5=+2.4; leftover $1774.28 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 235 | $7.54 | $3.03 | — | $122.38 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover,oppset; 🔵; ret5=-20.9; leftover $1774.28 | join🟡 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.38 | ▼ close $9,831.50 vs 09:30 $9,940.30 (session -81.27) | 16:00 close · cash $122.38 · equity $9,831.50 vs 09:30 $9,940.30 (-108.80; session marks -81.27) · 6 name(s) marked open→close (per-name table). GME×46 09:30 $22.90 → close $22.64 -11.96; TH×84 09:30 $20.91 → close $21.19 +23.52; RARE×119 09:30 $14.79 → close $14.51 -33.32; BHVN×126 09:30 $14.07 → close $13.62 -56.70; JXN×13 09:30 $129.00 → close $132.67 +47.71; FLNC×235 09:30 $7.54 → close $7.32 -50.52 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AUTL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HDSN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PURR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `TWO` | no_price | no 09:30 open |
| 2026-08-27 | `ASML` | cash | leftover split 1505.36 < 1 share @ 1746.53 |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ESI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SOLS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BEKE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BNTX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ROIV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `LULU` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TAC` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MAMA` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `RPRX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CGNT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `WLTH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AVTR` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SRRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QCOM` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GME` | 46 | 2026-09-17 @ $22.12 | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1031.99 |
| `TH` | 84 | 2026-09-18 @ $20.91 | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1774.28 |
| `RARE` | 119 | 2026-09-18 @ $14.79 | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $1774.28 |
| `BHVN` | 126 | 2026-09-18 @ $14.07 | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1774.28 |
| `JXN` | 13 | 2026-09-18 @ $129.00 | prior-export headline🟢 only; gate headline=good; rank cond; list oppset; ret5=+2.4; leftover $1774.28 |
| `FLNC` | 235 | 2026-09-18 @ $7.54 | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover,oppset; 🔵; ret5=-20.9; leftover $1774.28 |
