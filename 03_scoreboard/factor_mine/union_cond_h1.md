# Factor mine action — `union_cond_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · rank by cond

Cash book **-1.69%** ($9,831) · signal-only (no cash/fees) was -1.31%. Starts YES **3/19**. Fills 168 · skips 63 · realized $-168.78.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
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
- **Gate** `none (list as ranked)` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,831.24.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 20 | — | $59.80 | +0.00 | $60.23 | +8.60 | +8.60 | +0.00 | +8.60 |
| 2026-08-13 | `HIMS` | 42 | — | $29.74 | +0.00 | $28.77 | -40.74 | -40.74 | +0.00 | -40.74 |
| 2026-08-13 | `INO` | 1543 | — | $0.81 | +0.00 | $0.90 | +138.87 | +138.87 | +0.00 | +138.87 |
| 2026-08-13 | `SLS` | 106 | — | $11.70 | +0.00 | $12.36 | +69.96 | +69.96 | +0.00 | +69.96 |
| 2026-08-13 | `TGTX` | 25 | — | $49.70 | +0.00 | $47.94 | -44.00 | -44.00 | +0.00 | -44.00 |
| 2026-08-13 | `TNDM` | 53 | — | $23.33 | +0.00 | $23.13 | -10.60 | -10.60 | +0.00 | -10.60 |
| 2026-08-13 | `VOR` | 56 | — | $22.01 | +0.00 | $23.29 | +71.68 | +71.68 | +0.00 | +71.68 |
| 2026-08-13 | `IREN` | 27 | — | $45.98 | +0.00 | $44.76 | -32.94 | -32.94 | +0.00 | -32.94 |
| 2026-08-14 | `BTSG` | 20 | $60.23 | $59.65 | -11.60 | — | +0.00 | -11.60 | -3.00 | — |
| 2026-08-14 | `HIMS` | 42 | $28.77 | $29.15 | +15.96 | — | +0.00 | +15.96 | -24.78 | — |
| 2026-08-14 | `INO` | 1543 | $0.90 | $0.93 | +46.29 | — | +0.00 | +46.29 | +185.16 | — |
| 2026-08-14 | `SLS` | 106 | $12.36 | $12.40 | +4.24 | — | +0.00 | +4.24 | +74.20 | — |
| 2026-08-14 | `TGTX` | 25 | $47.94 | $47.27 | -16.75 | — | +0.00 | -16.75 | -60.75 | — |
| 2026-08-14 | `TNDM` | 53 | $23.13 | $22.92 | -11.13 | — | +0.00 | -11.13 | -21.73 | — |
| 2026-08-14 | `VOR` | 56 | $23.29 | $23.33 | +2.24 | — | +0.00 | +2.24 | +73.92 | — |
| 2026-08-14 | `IREN` | 27 | $44.76 | $44.09 | -18.09 | — | +0.00 | -18.09 | -51.03 | — |
| 2026-08-14 | `AMPY` | 255 | — | $4.94 | +0.00 | $4.78 | -40.80 | -40.80 | +0.00 | -40.80 |
| 2026-08-14 | `BRUN` | 48 | — | $26.25 | +0.00 | $22.93 | -159.12 | -159.12 | +0.00 | -159.12 |
| 2026-08-14 | `CLBT` | 116 | — | $10.83 | +0.00 | $11.14 | +35.96 | +35.96 | +0.00 | +35.96 |
| 2026-08-14 | `EU` | 1070 | — | $1.18 | +0.00 | $1.21 | +32.10 | +32.10 | +0.00 | +32.10 |
| 2026-08-14 | `HLIT` | 95 | — | $13.18 | +0.00 | $13.92 | +70.30 | +70.30 | +0.00 | +70.30 |
| 2026-08-14 | `MNTN` | 101 | — | $12.50 | +0.00 | $12.52 | +2.02 | +2.02 | +0.00 | +2.02 |
| 2026-08-14 | `QMCO` | 51 | — | $24.68 | +0.00 | $26.11 | +72.93 | +72.93 | +0.00 | +72.93 |
| 2026-08-14 | `QMLS` | 173 | — | $7.29 | +0.00 | $7.32 | +5.19 | +5.19 | +0.00 | +5.19 |
| 2026-08-17 | `AMPY` | 255 | $4.78 | $4.86 | +20.40 | — | +0.00 | +20.40 | -20.40 | — |
| 2026-08-17 | `BRUN` | 48 | $22.93 | $23.00 | +3.36 | — | +0.00 | +3.36 | -155.76 | — |
| 2026-08-17 | `CLBT` | 116 | $11.14 | $11.19 | +5.80 | — | +0.00 | +5.80 | +41.76 | — |
| 2026-08-17 | `EU` | 1070 | $1.21 | $1.21 | +0.00 | — | +0.00 | +0.00 | +32.10 | — |
| 2026-08-17 | `HLIT` | 95 | $13.92 | $13.84 | -7.60 | — | +0.00 | -7.60 | +62.70 | — |
| 2026-08-17 | `MNTN` | 101 | $12.52 | $12.40 | -12.12 | — | +0.00 | -12.12 | -10.10 | — |
| 2026-08-17 | `QMCO` | 51 | $26.11 | $24.83 | -65.28 | — | +0.00 | -65.28 | +7.65 | — |
| 2026-08-17 | `QMLS` | 173 | $7.32 | $7.24 | -13.84 | — | +0.00 | -13.84 | -8.65 | — |
| 2026-08-17 | `DVN` | 27 | — | $46.18 | +0.00 | $47.57 | +37.53 | +37.53 | +0.00 | +37.53 |
| 2026-08-17 | `EOG` | 8 | — | $142.77 | +0.00 | $146.15 | +27.04 | +27.04 | +0.00 | +27.04 |
| 2026-08-17 | `FANG` | 6 | — | $202.70 | +0.00 | $206.29 | +21.54 | +21.54 | +0.00 | +21.54 |
| 2026-08-17 | `BORR` | 272 | — | $4.59 | +0.00 | $4.50 | -24.48 | -24.48 | +0.00 | -24.48 |
| 2026-08-17 | `LPTH` | 83 | — | $14.94 | +0.00 | $14.80 | -11.62 | -11.62 | +0.00 | -11.62 |
| 2026-08-17 | `VERI` | 1086 | — | $1.15 | +0.00 | $1.08 | -70.59 | -70.59 | +0.00 | -70.59 |
| 2026-08-17 | `AAOI` | 8 | — | $152.64 | +0.00 | $154.89 | +18.00 | +18.00 | +0.00 | +18.00 |
| 2026-08-17 | `ABX` | 136 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-18 | `DVN` | 27 | $47.57 | $48.00 | +11.61 | — | +0.00 | +11.61 | +49.14 | — |
| 2026-08-18 | `EOG` | 8 | $146.15 | $148.04 | +15.12 | — | +0.00 | +15.12 | +42.16 | — |
| 2026-08-18 | `FANG` | 6 | $206.29 | $208.93 | +15.84 | — | +0.00 | +15.84 | +37.38 | — |
| 2026-08-18 | `BORR` | 272 | $4.50 | $4.56 | +16.32 | — | +0.00 | +16.32 | -8.16 | — |
| 2026-08-18 | `LPTH` | 83 | $14.80 | $14.01 | -65.57 | — | +0.00 | -65.57 | -77.19 | — |
| 2026-08-18 | `VERI` | 1086 | $1.08 | $1.05 | -38.01 | — | +0.00 | -38.01 | -108.60 | — |
| 2026-08-18 | `AAOI` | 8 | $154.89 | $146.20 | -69.52 | — | +0.00 | -69.52 | -51.52 | — |
| 2026-08-18 | `ABX` | 136 | $9.12 | $9.03 | -12.24 | — | +0.00 | -12.24 | -12.24 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 59 | — | $20.55 | +0.00 | $21.19 | +37.76 | +37.76 | +0.00 | +37.76 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 59 | — | $20.65 | +0.00 | $21.11 | +27.14 | +27.14 | +0.00 | +27.14 |
| 2026-08-20 | `HDSN` | 212 | — | $5.77 | +0.00 | $5.57 | -42.40 | -42.40 | +0.00 | -42.40 |
| 2026-08-20 | `IAG` | 62 | — | $19.63 | +0.00 | $20.50 | +53.94 | +53.94 | +0.00 | +53.94 |
| 2026-08-20 | `KGC` | 41 | — | $29.63 | +0.00 | $31.43 | +73.80 | +73.80 | +0.00 | +73.80 |
| 2026-08-20 | `NFGC` | 700 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 59 | $21.19 | $21.90 | +41.89 | — | +0.00 | +41.89 | +79.65 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `CDE` | 59 | $21.11 | $21.75 | +37.76 | — | +0.00 | +37.76 | +64.90 | — |
| 2026-08-21 | `HDSN` | 212 | $5.57 | $5.67 | +21.20 | — | +0.00 | +21.20 | -21.20 | — |
| 2026-08-21 | `IAG` | 62 | $20.50 | $21.17 | +41.54 | — | +0.00 | +41.54 | +95.48 | — |
| 2026-08-21 | `KGC` | 41 | $31.43 | $32.17 | +30.34 | — | +0.00 | +30.34 | +104.14 | — |
| 2026-08-21 | `NFGC` | 700 | $1.75 | $1.79 | +28.00 | — | +0.00 | +28.00 | +28.00 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 74 | — | $17.20 | +0.00 | $16.65 | -40.70 | -40.70 | +0.00 | -40.70 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 115 | — | $11.13 | +0.00 | $13.45 | +266.80 | +266.80 | +0.00 | +266.80 |
| 2026-08-21 | `AUTL` | 518 | — | $2.47 | +0.00 | $2.41 | -31.08 | -31.08 | +0.00 | -31.08 |
| 2026-08-21 | `CRDL` | 663 | — | $1.93 | +0.00 | $1.86 | -46.41 | -46.41 | +0.00 | -46.41 |
| 2026-08-21 | `CRSP` | 21 | — | $59.72 | +0.00 | $59.50 | -4.62 | -4.62 | +0.00 | -4.62 |
| 2026-08-21 | `CYPH` | 970 | — | $1.32 | +0.00 | $1.42 | +97.00 | +97.00 | +0.00 | +97.00 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUPH` | 74 | $16.65 | $16.57 | -5.92 | — | +0.00 | -5.92 | -46.62 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 115 | $13.45 | $13.33 | -13.80 | — | +0.00 | -13.80 | +253.00 | — |
| 2026-08-24 | `AUTL` | 518 | $2.41 | $2.40 | -5.18 | — | +0.00 | -5.18 | -36.26 | — |
| 2026-08-24 | `CRDL` | 663 | $1.86 | $1.88 | +13.26 | — | +0.00 | +13.26 | -33.15 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | $57.08 | -35.17 | -50.92 | -20.37 | -55.54 |
| 2026-08-24 | `CYPH` | 970 | $1.42 | $1.83 | +397.70 | — | +0.00 | +397.70 | +494.70 | — |
| 2026-08-25 | `CRSP` | 21 | $57.08 | $57.93 | +17.95 | — | +0.00 | +17.95 | -37.59 | — |
| 2026-08-25 | `AU` | 11 | — | $118.52 | +0.00 | $123.39 | +53.57 | +53.57 | +0.00 | +53.57 |
| 2026-08-25 | `ERO` | 35 | — | $38.01 | +0.00 | $40.40 | +83.65 | +83.65 | +0.00 | +83.65 |
| 2026-08-25 | `FCX` | 17 | — | $77.13 | +0.00 | $79.91 | +47.26 | +47.26 | +0.00 | +47.26 |
| 2026-08-25 | `CNH` | 113 | — | $11.90 | +0.00 | $11.56 | -38.42 | -38.42 | +0.00 | -38.42 |
| 2026-08-25 | `HMY` | 60 | — | $22.41 | +0.00 | $22.95 | +32.40 | +32.40 | +0.00 | +32.40 |
| 2026-08-25 | `MOS` | 56 | — | $23.77 | +0.00 | $24.27 | +28.00 | +28.00 | +0.00 | +28.00 |
| 2026-08-25 | `RHI` | 30 | — | $43.76 | +0.00 | $44.90 | +34.20 | +34.20 | +0.00 | +34.20 |
| 2026-08-25 | `SUZ` | 150 | — | $8.98 | +0.00 | $9.03 | +7.50 | +7.50 | +0.00 | +7.50 |
| 2026-08-26 | `AU` | 11 | $123.39 | $119.80 | -39.49 | — | +0.00 | -39.49 | +14.08 | — |
| 2026-08-26 | `ERO` | 35 | $40.40 | $40.51 | +3.85 | — | +0.00 | +3.85 | +87.50 | — |
| 2026-08-26 | `FCX` | 17 | $79.91 | $79.34 | -9.69 | — | +0.00 | -9.69 | +37.57 | — |
| 2026-08-26 | `CNH` | 113 | $11.56 | $11.54 | -2.26 | — | +0.00 | -2.26 | -40.68 | — |
| 2026-08-26 | `HMY` | 60 | $22.95 | $22.39 | -33.60 | — | +0.00 | -33.60 | -1.20 | — |
| 2026-08-26 | `MOS` | 56 | $24.27 | $24.84 | +31.92 | $24.16 | -38.08 | -6.16 | +59.92 | +21.84 |
| 2026-08-26 | `RHI` | 30 | $44.90 | $44.33 | -17.10 | — | +0.00 | -17.10 | +17.10 | — |
| 2026-08-26 | `SUZ` | 150 | $9.03 | $9.03 | +0.00 | — | +0.00 | +0.00 | +7.50 | — |
| 2026-08-26 | `FNV` | 5 | — | $267.02 | +0.00 | $267.37 | +1.75 | +1.75 | +0.00 | +1.75 |
| 2026-08-26 | `FIGR` | 33 | — | $40.50 | +0.00 | $37.08 | -112.86 | -112.86 | +0.00 | -112.86 |
| 2026-08-26 | `FUTU` | 10 | — | $124.67 | +0.00 | $127.34 | +26.70 | +26.70 | +0.00 | +26.70 |
| 2026-08-26 | `SLQT` | 2336 | — | $0.58 | +0.00 | $0.55 | -77.09 | -77.09 | +0.00 | -77.09 |
| 2026-08-26 | `TIGR` | 261 | — | $5.21 | +0.00 | $5.46 | +65.25 | +65.25 | +0.00 | +65.25 |
| 2026-08-26 | `BTG` | 236 | — | $5.75 | +0.00 | $5.74 | -2.36 | -2.36 | +0.00 | -2.36 |
| 2026-08-26 | `NEXA` | 87 | — | $15.64 | +0.00 | $15.58 | -5.22 | -5.22 | +0.00 | -5.22 |
| 2026-08-27 | `MOS` | 56 | $24.16 | $24.00 | -8.96 | $23.76 | -13.44 | -22.40 | +12.88 | -0.56 |
| 2026-08-27 | `FNV` | 5 | $267.37 | $267.23 | -0.70 | — | +0.00 | -0.70 | +1.05 | — |
| 2026-08-27 | `FIGR` | 33 | $37.08 | $37.42 | +11.22 | — | +0.00 | +11.22 | -101.64 | — |
| 2026-08-27 | `FUTU` | 10 | $127.34 | $128.00 | +6.60 | — | +0.00 | +6.60 | +33.30 | — |
| 2026-08-27 | `SLQT` | 2336 | $0.55 | $0.53 | -46.72 | — | +0.00 | -46.72 | -123.81 | — |
| 2026-08-27 | `TIGR` | 261 | $5.46 | $5.49 | +7.83 | — | +0.00 | +7.83 | +73.08 | — |
| 2026-08-27 | `BTG` | 236 | $5.74 | $5.73 | -2.36 | — | +0.00 | -2.36 | -4.72 | — |
| 2026-08-27 | `NEXA` | 87 | $15.58 | $14.90 | -59.16 | — | +0.00 | -59.16 | -64.38 | — |
| 2026-08-27 | `ACMR` | 16 | — | $81.65 | +0.00 | $80.49 | -18.56 | -18.56 | +0.00 | -18.56 |
| 2026-08-27 | `GGB` | 290 | — | $4.57 | +0.00 | $4.70 | +37.70 | +37.70 | +0.00 | +37.70 |
| 2026-08-27 | `MT` | 17 | — | $74.54 | +0.00 | $74.63 | +1.53 | +1.53 | +0.00 | +1.53 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `SLI` | 509 | — | $2.60 | +0.00 | $2.64 | +20.36 | +20.36 | +0.00 | +20.36 |
| 2026-08-27 | `TX` | 23 | — | $55.25 | +0.00 | $55.83 | +13.34 | +13.34 | +0.00 | +13.34 |
| 2026-08-27 | `ANET` | 6 | — | $205.90 | +0.00 | $201.09 | -28.86 | -28.86 | +0.00 | -28.86 |
| 2026-08-28 | `MOS` | 56 | $23.76 | $23.95 | +10.64 | — | +0.00 | +10.64 | +10.08 | — |
| 2026-08-28 | `ACMR` | 16 | $80.49 | $79.27 | -19.52 | — | +0.00 | -19.52 | -38.08 | — |
| 2026-08-28 | `GGB` | 290 | $4.70 | $4.67 | -8.70 | — | +0.00 | -8.70 | +29.00 | — |
| 2026-08-28 | `MT` | 17 | $74.63 | $75.39 | +12.92 | — | +0.00 | +12.92 | +14.45 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `SLI` | 509 | $2.64 | $2.68 | +20.36 | — | +0.00 | +20.36 | +40.72 | — |
| 2026-08-28 | `TX` | 23 | $55.83 | $55.97 | +3.22 | — | +0.00 | +3.22 | +16.56 | — |
| 2026-08-28 | `ANET` | 6 | $201.09 | $200.00 | -6.54 | — | +0.00 | -6.54 | -35.40 | — |
| 2026-08-28 | `KEYS` | 4 | — | $324.41 | +0.00 | $319.97 | -17.76 | -17.76 | +0.00 | -17.76 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `AVT` | 14 | — | $91.49 | +0.00 | $88.63 | -40.04 | -40.04 | +0.00 | -40.04 |
| 2026-08-28 | `CGNX` | 21 | — | $62.82 | +0.00 | $60.46 | -49.56 | -49.56 | +0.00 | -49.56 |
| 2026-08-28 | `COHR` | 4 | — | $289.44 | +0.00 | $279.20 | -40.96 | -40.96 | +0.00 | -40.96 |
| 2026-08-28 | `LSCC` | 11 | — | $119.76 | +0.00 | $114.40 | -58.96 | -58.96 | +0.00 | -58.96 |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | — | +0.00 | +10.08 | -7.68 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -65.94 | — |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | — | +0.00 | +5.64 | -44.13 | — |
| 2026-08-31 | `AVT` | 14 | $88.63 | $89.39 | +10.64 | — | +0.00 | +10.64 | -29.40 | — |
| 2026-08-31 | `CGNX` | 21 | $60.46 | $60.46 | +0.00 | — | +0.00 | +0.00 | -49.56 | — |
| 2026-08-31 | `COHR` | 4 | $279.20 | $280.25 | +4.20 | — | +0.00 | +4.20 | -36.76 | — |
| 2026-08-31 | `LSCC` | 11 | $114.40 | $115.56 | +12.76 | — | +0.00 | +12.76 | -46.20 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ARCT` | 75 | — | $16.77 | +0.00 | $15.56 | -90.75 | -90.75 | +0.00 | -90.75 |
| 2026-09-03 | `BMEA` | 658 | — | $1.93 | +0.00 | $1.91 | -13.16 | -13.16 | +0.00 | -13.16 |
| 2026-09-03 | `CRDL` | 582 | — | $2.18 | +0.00 | $2.16 | -11.64 | -11.64 | +0.00 | -11.64 |
| 2026-09-03 | `HRMY` | 29 | — | $42.93 | +0.00 | $41.86 | -31.03 | -31.03 | +0.00 | -31.03 |
| 2026-09-03 | `NVAX` | 121 | — | $10.42 | +0.00 | $10.34 | -9.68 | -9.68 | +0.00 | -9.68 |
| 2026-09-03 | `PBH` | 23 | — | $53.45 | +0.00 | $52.56 | -20.47 | -20.47 | +0.00 | -20.47 |
| 2026-09-03 | `PCRX` | 47 | — | $26.74 | +0.00 | $26.60 | -6.58 | -6.58 | +0.00 | -6.58 |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-04 | `ARCT` | 75 | $15.56 | $15.61 | +3.75 | — | +0.00 | +3.75 | -87.00 | — |
| 2026-09-04 | `BMEA` | 658 | $1.91 | $1.90 | -6.58 | $2.03 | +85.54 | +78.96 | -19.74 | +65.80 |
| 2026-09-04 | `CRDL` | 582 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -11.64 | — |
| 2026-09-04 | `HRMY` | 29 | $41.86 | $41.50 | -10.44 | $42.25 | +21.75 | +11.31 | -41.47 | -19.72 |
| 2026-09-04 | `NVAX` | 121 | $10.34 | $10.50 | +19.36 | — | +0.00 | +19.36 | +9.68 | — |
| 2026-09-04 | `PBH` | 23 | $52.56 | $51.80 | -17.48 | — | +0.00 | -17.48 | -37.95 | — |
| 2026-09-04 | `PCRX` | 47 | $26.60 | $26.38 | -10.34 | — | +0.00 | -10.34 | -16.92 | — |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `CABA` | 357 | — | $3.46 | +0.00 | $3.47 | +3.57 | +3.57 | +0.00 | +3.57 |
| 2026-09-04 | `ALEC` | 491 | — | $2.52 | +0.00 | $2.46 | -29.46 | -29.46 | +0.00 | -29.46 |
| 2026-09-04 | `ATRC` | 23 | — | $52.03 | +0.00 | $51.52 | -11.73 | -11.73 | +0.00 | -11.73 |
| 2026-09-04 | `BHC` | 184 | — | $6.71 | +0.00 | $6.56 | -27.60 | -27.60 | +0.00 | -27.60 |
| 2026-09-04 | `CRM` | 4 | — | $263.36 | +0.00 | $259.23 | -16.52 | -16.52 | +0.00 | -16.52 |
| 2026-09-04 | `MLYS` | 44 | — | $28.00 | +0.00 | $28.21 | +9.24 | +9.24 | +0.00 | +9.24 |
| 2026-09-08 | `BMEA` | 658 | $2.03 | $2.00 | -19.74 | — | +0.00 | -19.74 | +46.06 | — |
| 2026-09-08 | `HRMY` | 29 | $42.25 | $42.20 | -1.45 | — | +0.00 | -1.45 | -21.17 | — |
| 2026-09-08 | `CABA` | 357 | $3.47 | $3.43 | -14.28 | — | +0.00 | -14.28 | -10.71 | — |
| 2026-09-08 | `ALEC` | 491 | $2.46 | $2.38 | -39.28 | — | +0.00 | -39.28 | -68.74 | — |
| 2026-09-08 | `ATRC` | 23 | $51.52 | $54.31 | +64.17 | — | +0.00 | +64.17 | +52.44 | — |
| 2026-09-08 | `BHC` | 184 | $6.56 | $6.57 | +1.84 | — | +0.00 | +1.84 | -25.76 | — |
| 2026-09-08 | `CRM` | 4 | $259.23 | $253.72 | -22.04 | — | +0.00 | -22.04 | -38.56 | — |
| 2026-09-08 | `MLYS` | 44 | $28.21 | $28.03 | -7.92 | — | +0.00 | -7.92 | +1.32 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +160.83 | BTSG, HIMS, INO, SLS, TGTX, TNDM, VOR, IREN | — | $79.84 | $10,128.79 | BTSG×20, HIMS×42, INO×1543, SLS×106, TGTX×25, TNDM×53, VOR×56, IREN×27 |
| 2026-08-14 | +5.50 | $79.84 | BTSG×20, HIMS×42, INO×1543, SLS×106, TGTX×25, TNDM×53, VOR×56, IREN×27 | $10,139.95 | +11.16 | +18.58 | AMPY, BRUN, CLBT, EU, HLIT, MNTN, QMCO, QMLS | BTSG, HIMS, INO, SLS, TGTX, TNDM, VOR, IREN | $2.06 | $10,093.43 | AMPY×255, BRUN×48, CLBT×116, EU×1070, HLIT×95, MNTN×101, QMCO×51, QMLS×173 |
| 2026-08-17 | +2.25 | $2.06 | AMPY×255, BRUN×48, CLBT×116, EU×1070, HLIT×95, MNTN×101, QMCO×51, QMLS×173 | $10,024.15 | -69.28 | -2.58 | DVN, EOG, FANG, BORR, LPTH, VERI, AAOI, ABX | AMPY, BRUN, CLBT, EU, HLIT, MNTN, QMCO, QMLS | $158.64 | $9,960.12 | DVN×27, EOG×8, FANG×6, BORR×272, LPTH×83, VERI×1086, AAOI×8, ABX×136 |
| 2026-08-18 | -6.20 | $158.64 | DVN×27, EOG×8, FANG×6, BORR×272, LPTH×83, VERI×1086, AAOI×8, ABX×136 | $9,833.67 | -126.45 | +0.00 | — | DVN, EOG, FANG, BORR, LPTH, VERI, AAOI, ABX | $9,803.03 | $9,803.03 | — |
| 2026-08-19 | -7.20 | $9,803.03 | — | $9,803.03 | -0.00 | +0.00 | — | — | $9,803.03 | $9,803.03 | — |
| 2026-08-20 | +1.12 | $9,803.03 | — | $9,803.03 | -0.00 | +229.98 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $128.22 | $10,008.58 | AG×59, BHP×13, CDE×59, HDSN×212, IAG×62, KGC×41, NFGC×700, WPM×8 |
| 2026-08-21 | +3.25 | $128.22 | AG×59, BHP×13, CDE×59, HDSN×212, IAG×62, KGC×41, NFGC×700, WPM×8 | $10,272.08 | +263.50 | +257.69 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $286.86 | $10,466.67 | AU×10, AUPH×74, AEM×5, ARCT×115, AUTL×518, CRDL×663, CRSP×21, CYPH×970 |
| 2026-08-24 | -5.17 | $286.86 | AU×10, AUPH×74, AEM×5, ARCT×115, AUTL×518, CRDL×663, CRSP×21, CYPH×970 | $10,834.73 | +368.06 | -35.17 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CYPH | $9,564.17 | $10,762.75 | CRSP×21 |
| 2026-08-25 | +1.80 | $9,564.17 | CRSP×21 | $10,780.70 | +17.95 | +248.16 | AU, ERO, FCX, CNH, HMY, MOS, RHI, SUZ | CRSP | $135.79 | $11,009.45 | AU×11, ERO×35, FCX×17, CNH×113, HMY×60, MOS×56, RHI×30, SUZ×150 |
| 2026-08-26 | +2.02 | $135.79 | AU×11, ERO×35, FCX×17, CNH×113, HMY×60, MOS×56, RHI×30, SUZ×150 | $10,943.08 | -66.37 | -141.91 | FNV, FIGR, FUTU, SLQT, TIGR, BTG, NEXA | AU, ERO, FCX, CNH, HMY, RHI, SUZ | $143.62 | $10,750.43 | MOS×56, FNV×5, FIGR×33, FUTU×10, SLQT×2336, TIGR×261, BTG×236, NEXA×87 |
| 2026-08-27 | — | $143.62 | MOS×56, FNV×5, FIGR×33, FUTU×10, SLQT×2336, TIGR×261, BTG×236, NEXA×87 | $10,658.18 | -92.25 | -19.55 | ACMR, GGB, MT, MU, SLI, TX, ANET | FNV, FIGR, FUTU, SLQT, TIGR, BTG, NEXA | $563.54 | $10,583.43 | MOS×56, ACMR×16, GGB×290, MT×17, MU×1, SLI×509, TX×23, ANET×6 |
| 2026-08-28 | +0.75 | $563.54 | MOS×56, ACMR×16, GGB×290, MT×17, MU×1, SLI×509, TX×23, ANET×6 | $10,579.71 | -3.72 | -418.30 | KEYS, SMTC, CIEN, MPWR, AVT, CGNX, COHR, LSCC | MOS, ACMR, GGB, MT, MU, SLI, TX, ANET | $384.74 | $10,122.41 | KEYS×4, SMTC×9, CIEN×3, MPWR×1, AVT×14, CGNX×21, COHR×4, LSCC×11 |
| 2026-08-31 | -5.85 | $384.74 | KEYS×4, SMTC×9, CIEN×3, MPWR×1, AVT×14, CGNX×21, COHR×4, LSCC×11 | $10,175.90 | +53.49 | +0.00 | — | KEYS, SMTC, CIEN, MPWR, AVT, CGNX, COHR, LSCC | $10,159.61 | $10,159.61 | — |
| 2026-09-01 | -6.30 | $10,159.61 | — | $10,159.61 | +0.00 | +0.00 | — | — | $10,159.61 | $10,159.61 | — |
| 2026-09-02 | -3.83 | $10,159.61 | — | $10,159.61 | +0.00 | +0.00 | — | — | $10,159.61 | $10,159.61 | — |
| 2026-09-03 | -0.90 | $10,159.61 | — | $10,159.61 | +0.00 | -199.69 | ARCT, BMEA, CRDL, HRMY, NVAX, PBH, PCRX, RVTY | — | $150.35 | $9,931.08 | ARCT×75, BMEA×658, CRDL×582, HRMY×29, NVAX×121, PBH×23, PCRX×47, RVTY×9 |
| 2026-09-04 | +2.25 | $150.35 | ARCT×75, BMEA×658, CRDL×582, HRMY×29, NVAX×121, PBH×23, PCRX×47, RVTY×9 | $9,903.95 | -27.13 | +34.79 | CABA, ALEC, ATRC, BHC, CRM, MLYS | ARCT, CRDL, NVAX, PBH, PCRX, RVTY | $222.77 | $9,900.57 | BMEA×658, HRMY×29, CABA×357, ALEC×491, ATRC×23, BHC×184, CRM×4, MLYS×44 |
| 2026-09-08 | -11.47 | $222.77 | BMEA×658, HRMY×29, CABA×357, ALEC×491, ATRC×23, BHC×184, CRM×4, MLYS×44 | $9,861.87 | -38.70 | +0.00 | — | BMEA, HRMY, CABA, ALEC, ATRC, BHC, CRM, MLYS | $9,831.24 | $9,831.24 | — |
| 2026-09-09 | -13.95 | $9,831.24 | — | $9,831.24 | -0.00 | +0.00 | — | — | $9,831.24 | $9,831.24 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | rank by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $7,550.75 | — | rank by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $6,283.80 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $5,041.29 | — | rank by cond; rank cond; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $3,796.72 | — | rank by cond; rank cond; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $2,558.08 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $1,323.37 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $79.84 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.84 | ▲ close $10,128.79 vs 09:30 $10,000.00 (session +160.83) | 16:00 close · cash $79.84 · equity $10,128.79 vs 09:30 $10,000.00 (+128.79; session marks +160.83) · 8 name(s) marked open→close (per-name table). BTSG×20 09:30 $59.80 → close $60.23 +8.60; HIMS×42 09:30 $29.74 → close $28.77 -40.74; INO×1543 09:30 $0.81 → close $0.90 +138.87; SLS×106 09:30 $11.70 → close $12.36 +69.96; TGTX×25 09:30 $49.70 → close $47.94 -44.00; TNDM×53 09:30 $23.33 → close $23.13 -10.60; VOR×56 09:30 $22.01 → close $23.29 +71.68; IREN×27 09:30 $45.98 → close $44.76 -32.94 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.84 | ▲ 09:30 equity $10,139.95 vs yday $10,128.79 (+11.16) | 09:30 open · cash $79.84 (unchanged overnight, no fees) · equity $10,139.95 vs prior close $10,128.79 (+11.16) · 8 name(s) re-marked at the open (per-name table). BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; IREN×27 yday $44.76 → 09:30 $44.09 -18.09 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $1,270.77 | ▼ -7.12 after sell → book $10,137.88; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $2,492.93 | ▼ -29.03 after sell → book $10,135.74; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $3,908.67 | ▲ +148.79 after sell → book $10,116.49; vs 09:30 mark -19.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $5,220.74 | ▲ +69.56 after sell → book $10,114.16; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $6,400.40 | ▼ -64.90 after sell → book $10,112.07; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $7,612.99 | ▼ -26.05 after sell → book $10,109.90; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $8,917.29 | ▲ +69.58 after sell → book $10,107.72; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $10,105.63 | ▼ -55.19 after sell → book $10,105.63; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `AMPY` | 255 | $4.94 | $3.29 | — | $8,842.64 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.4; leftover $1263.20 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 48 | $26.25 | $2.13 | — | $7,580.75 | — | rank by cond; rank cond; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $1263.20 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 116 | $10.83 | $2.34 | — | $6,322.13 | — | rank by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-30.1; leftover $1263.20 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 1070 | $1.18 | $13.80 | — | $5,045.73 | — | rank by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1263.20 | join🟢 sector🟢 gen🟢 news🔴 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 95 | $13.18 | $2.27 | — | $3,791.35 | — | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1263.20 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MNTN` | 101 | $12.50 | $2.29 | — | $2,526.56 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1263.20 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 51 | $24.68 | $2.14 | — | $1,265.74 | — | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1263.20 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 173 | $7.29 | $2.51 | — | $2.06 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1263.20 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.06 | ▲ close $10,093.43 vs 09:30 $10,139.95 (session +18.58) | 16:00 close · cash $2.06 · equity $10,093.43 vs 09:30 $10,139.95 (-46.52; session marks +18.58) · 8 name(s) marked open→close (per-name table). AMPY×255 09:30 $4.94 → close $4.78 -40.80; BRUN×48 09:30 $26.25 → close $22.93 -159.12; CLBT×116 09:30 $10.83 → close $11.14 +35.96; EU×1070 09:30 $1.18 → close $1.21 +32.10; HLIT×95 09:30 $13.18 → close $13.92 +70.30; MNTN×101 09:30 $12.50 → close $12.52 +2.02; QMCO×51 09:30 $24.68 → close $26.11 +72.93; QMLS×173 09:30 $7.29 → close $7.32 +5.19 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.06 | ▼ 09:30 equity $10,024.15 vs yday $10,093.43 (-69.28) | 09:30 open · cash $2.06 (unchanged overnight, no fees) · equity $10,024.15 vs prior close $10,093.43 (-69.28) · 8 name(s) re-marked at the open (per-name table). AMPY×255 yday $4.78 → 09:30 $4.86 +20.40; BRUN×48 yday $22.93 → 09:30 $23.00 +3.36; CLBT×116 yday $11.14 → 09:30 $11.19 +5.80; EU×1070 yday $1.21 → 09:30 $1.21 +0.00; HLIT×95 yday $13.92 → 09:30 $13.84 -7.60; MNTN×101 yday $12.52 → 09:30 $12.40 -12.12; QMCO×51 yday $26.11 → 09:30 $24.83 -65.28; QMLS×173 yday $7.32 → 09:30 $7.24 -13.84 | — |
| 2026-08-17 09:30 ET | **SELL** | `AMPY` | 255 | $4.86 | $3.34 | $-27.03 | $1,238.02 | ▼ -27.03 after sell → book $10,020.81; vs 09:30 mark -3.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 48 | $23.00 | $2.15 | $-160.05 | $2,339.86 | ▼ -160.05 after sell → book $10,018.65; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 116 | $11.19 | $2.37 | $+37.05 | $3,635.54 | ▲ +37.05 after sell → book $10,016.29; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `EU` | 1070 | $1.21 | $13.99 | $+4.31 | $4,916.24 | ▲ +4.31 after sell → book $10,002.29; vs 09:30 mark -14.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 95 | $13.84 | $2.30 | $+58.12 | $6,228.74 | ▲ +58.12 after sell → book $9,999.99; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MNTN` | 101 | $12.40 | $2.32 | $-14.71 | $7,478.82 | ▼ -14.71 after sell → book $9,997.67; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 51 | $24.83 | $2.16 | $+3.34 | $8,742.99 | ▲ +3.34 after sell → book $9,995.51; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 173 | $7.24 | $2.55 | $-13.71 | $9,992.96 | ▼ -13.71 after sell → book $9,992.96; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,744.03 | — | rank by cond; rank cond; list flatten; 🔵; ret5=+6.7; leftover $1249.12 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,599.86 | — | rank by cond; rank cond; list flatten; 🔵; ret5=+5.8; leftover $1249.12 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,381.65 | — | rank by cond; rank cond; list flatten; 🔵; ret5=+8.3; leftover $1249.12 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 272 | $4.59 | $3.51 | — | $5,129.66 | — | rank by cond; rank cond; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $1249.12 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 83 | $14.94 | $2.24 | — | $3,887.40 | — | rank by cond; rank cond; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1249.12 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `VERI` | 1086 | $1.15 | $14.01 | — | $2,624.49 | — | rank by cond; rank cond; list yday_mover; ⚪; ret5=-12.2; leftover $1249.12 | join🟡 sector🟢 gen🟢 news🔴 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `AAOI` | 8 | $152.64 | $2.01 | — | $1,401.36 | — | rank by cond; rank cond; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+10.8; leftover $1249.12 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 136 | $9.12 | $2.40 | — | $158.64 | — | rank by cond; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1249.12 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.64 | ▼ close $9,960.12 vs 09:30 $10,024.15 (session -2.58) | 16:00 close · cash $158.64 · equity $9,960.12 vs 09:30 $10,024.15 (-64.03; session marks -2.58) · 8 name(s) marked open→close (per-name table). DVN×27 09:30 $46.18 → close $47.57 +37.53; EOG×8 09:30 $142.77 → close $146.15 +27.04; FANG×6 09:30 $202.70 → close $206.29 +21.54; BORR×272 09:30 $4.59 → close $4.50 -24.48; LPTH×83 09:30 $14.94 → close $14.80 -11.62; VERI×1086 09:30 $1.15 → close $1.08 -70.59; AAOI×8 09:30 $152.64 → close $154.89 +18.00; ABX×136 09:30 $9.12 → close $9.12 +0.00 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.64 | ▼ 09:30 equity $9,833.67 vs yday $9,960.12 (-126.45) | 09:30 open · cash $158.64 (unchanged overnight, no fees) · equity $9,833.67 vs prior close $9,960.12 (-126.45) · 8 name(s) re-marked at the open (per-name table). DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; BORR×272 yday $4.50 → 09:30 $4.56 +16.32; LPTH×83 yday $14.80 → 09:30 $14.01 -65.57; VERI×1086 yday $1.08 → 09:30 $1.05 -38.01; AAOI×8 yday $154.89 → 09:30 $146.20 -69.52; ABX×136 yday $9.12 → 09:30 $9.03 -12.24 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,452.55 | ▲ +44.98 after sell → book $9,831.58; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,634.84 | ▲ +38.11 after sell → book $9,829.55; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,886.39 | ▲ +33.34 after sell → book $9,827.52; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 272 | $4.56 | $3.56 | $-15.23 | $5,123.14 | ▼ -15.23 after sell → book $9,823.95; vs 09:30 mark -3.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 83 | $14.01 | $2.26 | $-81.69 | $6,283.71 | ▼ -81.69 after sell → book $9,821.69; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERI` | 1086 | $1.05 | $14.20 | $-136.81 | $7,409.81 | ▼ -136.81 after sell → book $9,807.49; vs 09:30 mark -14.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `AAOI` | 8 | $146.20 | $2.03 | $-55.57 | $8,577.38 | ▼ -55.57 after sell → book $9,805.46; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 136 | $9.03 | $2.43 | $-17.07 | $9,803.03 | ▼ -17.07 after sell → book $9,803.03; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,803.03 | ▲ close $9,803.03 vs 09:30 $9,833.67 (session +0.00) | 16:00 close · cash $9,803.03 · no lots left · equity $9,803.03. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,803.03 | ▲ 09:30 equity $9,803.03 vs yday $9,803.03 (-0.00) | 09:30 open · cash $9,803.03 · no holdings · equity $9,803.03 vs prior close $9,803.03 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,803.03 | ▲ close $9,803.03 vs 09:30 $9,803.03 (session +0.00) | 16:00 close · cash $9,803.03 · no lots left · equity $9,803.03. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,803.03 | ▲ 09:30 equity $9,803.03 vs yday $9,803.03 (-0.00) | 09:30 open · cash $9,803.03 · no holdings · equity $9,803.03 vs prior close $9,803.03 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 59 | $20.55 | $2.17 | — | $8,588.41 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1225.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,403.25 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1225.38 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 59 | $20.65 | $2.17 | — | $6,182.73 | — | rank by cond; rank cond; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1225.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 212 | $5.77 | $2.73 | — | $4,956.76 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1225.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 62 | $19.63 | $2.18 | — | $3,737.52 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1225.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $2,520.58 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1225.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 700 | $1.75 | $9.03 | — | $1,286.55 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1225.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $128.22 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1225.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.22 | ▲ close $10,008.58 vs 09:30 $9,803.03 (session +229.98) | 16:00 close · cash $128.22 · equity $10,008.58 vs 09:30 $9,803.03 (+205.55; session marks +229.98) · 8 name(s) marked open→close (per-name table). AG×59 09:30 $20.55 → close $21.19 +37.76; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×59 09:30 $20.65 → close $21.11 +27.14; HDSN×212 09:30 $5.77 → close $5.57 -42.40; IAG×62 09:30 $19.63 → close $20.50 +53.94; KGC×41 09:30 $29.63 → close $31.43 +73.80; NFGC×700 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.22 | ▲ 09:30 equity $10,272.08 vs yday $10,008.58 (+263.50) | 09:30 open · cash $128.22 (unchanged overnight, no fees) · equity $10,272.08 vs prior close $10,008.58 (+263.50) · 8 name(s) re-marked at the open (per-name table). AG×59 yday $21.19 → 09:30 $21.90 +41.89; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×59 yday $21.11 → 09:30 $21.75 +37.76; HDSN×212 yday $5.57 → 09:30 $5.67 +21.20; IAG×62 yday $20.50 → 09:30 $21.17 +41.54; KGC×41 yday $31.43 → 09:30 $32.17 +30.34; NFGC×700 yday $1.75 → 09:30 $1.79 +28.00; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 59 | $21.90 | $2.19 | $+75.30 | $1,418.13 | ▲ +75.30 after sell → book $10,269.89; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,660.44 | ▲ +57.15 after sell → book $10,267.84; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 59 | $21.75 | $2.19 | $+60.55 | $3,941.50 | ▲ +60.55 after sell → book $10,265.65; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 212 | $5.67 | $2.78 | $-26.71 | $5,140.76 | ▼ -26.71 after sell → book $10,262.87; vs 09:30 mark -2.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 62 | $21.17 | $2.20 | $+91.11 | $6,451.10 | ▲ +91.11 after sell → book $10,260.67; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $7,767.94 | ▲ +99.89 after sell → book $10,258.54; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 700 | $1.79 | $9.16 | $+9.81 | $9,011.79 | ▲ +9.81 after sell → book $10,249.39; vs 09:30 mark -9.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,247.35 | ▲ +77.23 after sell → book $10,247.35; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,051.03 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1280.92 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 74 | $17.20 | $2.21 | — | $7,776.02 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1280.92 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,692.51 | — | rank by cond; rank cond; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1280.92 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 115 | $11.13 | $2.33 | — | $5,410.23 | — | rank by cond; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1280.92 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 518 | $2.47 | $6.68 | — | $4,124.09 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1280.92 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 663 | $1.93 | $8.55 | — | $2,835.94 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1280.92 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,579.77 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1280.92 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 970 | $1.32 | $12.51 | — | $286.86 | — | rank by cond; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1280.92 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $286.86 | ▲ close $10,466.67 vs 09:30 $10,272.08 (session +257.69) | 16:00 close · cash $286.86 · equity $10,466.67 vs 09:30 $10,272.08 (+194.59; session marks +257.69) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×74 09:30 $17.20 → close $16.65 -40.70; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×115 09:30 $11.13 → close $13.45 +266.80; AUTL×518 09:30 $2.47 → close $2.41 -31.08; CRDL×663 09:30 $1.93 → close $1.86 -46.41; CRSP×21 09:30 $59.72 → close $59.50 -4.62; CYPH×970 09:30 $1.32 → close $1.42 +97.00 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $286.86 | ▲ 09:30 equity $10,834.73 vs yday $10,466.67 (+368.06) | 09:30 open · cash $286.86 (unchanged overnight, no fees) · equity $10,834.73 vs prior close $10,466.67 (+368.06) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×74 yday $16.65 → 09:30 $16.57 -5.92; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×115 yday $13.45 → 09:30 $13.33 -13.80; AUTL×518 yday $2.41 → 09:30 $2.40 -5.18; CRDL×663 yday $1.86 → 09:30 $1.88 +13.26; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; CYPH×970 yday $1.42 → 09:30 $1.83 +397.70 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,489.92 | ▲ +6.74 after sell → book $10,832.69; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 74 | $16.57 | $2.23 | $-51.07 | $2,713.86 | ▼ -51.07 after sell → book $10,830.45; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,796.99 | ▼ -0.38 after sell → book $10,828.43; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 115 | $13.33 | $2.37 | $+248.30 | $5,327.57 | ▲ +248.30 after sell → book $10,826.06; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 518 | $2.40 | $6.78 | $-49.72 | $6,563.99 | ▼ -49.72 after sell → book $10,819.28; vs 09:30 mark -6.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 663 | $1.88 | $8.67 | $-50.38 | $7,801.76 | ▼ -50.38 after sell → book $10,810.61; vs 09:30 mark -8.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 970 | $1.83 | $12.69 | $+469.50 | $9,564.17 | ▲ +469.50 after sell → book $10,797.92; vs 09:30 mark -12.69 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,564.17 | ▼ close $10,762.75 vs 09:30 $10,834.73 (session -35.17) | 16:00 close · cash $9,564.17 · equity $10,762.75 vs 09:30 $10,834.73 (-71.98; session marks -35.17) · 1 name(s) marked open→close (per-name table). CRSP×21 09:30 $58.75 → close $57.08 -35.17 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,564.17 | ▲ 09:30 equity $10,780.70 vs yday $10,762.75 (+17.95) | 09:30 open · cash $9,564.17 (unchanged overnight, no fees) · equity $10,780.70 vs prior close $10,762.75 (+17.95) · 1 name(s) re-marked at the open (per-name table). CRSP×21 yday $57.08 → 09:30 $57.93 +17.95 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-41.72 | $10,778.63 | ▼ -41.72 after sell → book $10,778.63; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 11 | $118.52 | $2.02 | — | $9,472.89 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1347.33 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 35 | $38.01 | $2.10 | — | $8,140.44 | — | rank by cond; rank cond; list mover_buy; ⚪; ret5=+10.4; leftover $1347.33 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 17 | $77.13 | $2.04 | — | $6,827.19 | — | rank by cond; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1347.33 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CNH` | 113 | $11.90 | $2.33 | — | $5,480.16 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1347.33 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HMY` | 60 | $22.41 | $2.17 | — | $4,133.39 | — | rank by cond; rank cond; list mover_buy; ⚪; ret5=+13.9; leftover $1347.33 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 56 | $23.77 | $2.16 | — | $2,800.11 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+13.0; leftover $1347.33 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 30 | $43.76 | $2.08 | — | $1,485.23 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $1347.33 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 150 | $8.98 | $2.44 | — | $135.79 | — | rank by cond; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; leftover $1347.33 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $135.79 | ▲ close $11,009.45 vs 09:30 $10,780.70 (session +248.16) | 16:00 close · cash $135.79 · equity $11,009.45 vs 09:30 $10,780.70 (+228.75; session marks +248.16) · 8 name(s) marked open→close (per-name table). AU×11 09:30 $118.52 → close $123.39 +53.57; ERO×35 09:30 $38.01 → close $40.40 +83.65; FCX×17 09:30 $77.13 → close $79.91 +47.26; CNH×113 09:30 $11.90 → close $11.56 -38.42; HMY×60 09:30 $22.41 → close $22.95 +32.40; MOS×56 09:30 $23.77 → close $24.27 +28.00; RHI×30 09:30 $43.76 → close $44.90 +34.20; SUZ×150 09:30 $8.98 → close $9.03 +7.50 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $135.79 | ▼ 09:30 equity $10,943.08 vs yday $11,009.45 (-66.37) | 09:30 open · cash $135.79 (unchanged overnight, no fees) · equity $10,943.08 vs prior close $11,009.45 (-66.37) · 8 name(s) re-marked at the open (per-name table). AU×11 yday $123.39 → 09:30 $119.80 -39.49; ERO×35 yday $40.40 → 09:30 $40.51 +3.85; FCX×17 yday $79.91 → 09:30 $79.34 -9.69; CNH×113 yday $11.56 → 09:30 $11.54 -2.26; HMY×60 yday $22.95 → 09:30 $22.39 -33.60; MOS×56 yday $24.27 → 09:30 $24.84 +31.92; RHI×30 yday $44.90 → 09:30 $44.33 -17.10; SUZ×150 yday $9.03 → 09:30 $9.03 +0.00 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 11 | $119.80 | $2.04 | $+10.01 | $1,451.55 | ▲ +10.01 after sell → book $10,941.04; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ERO` | 35 | $40.51 | $2.12 | $+83.29 | $2,867.28 | ▲ +83.29 after sell → book $10,938.92; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 17 | $79.34 | $2.06 | $+33.47 | $4,214.00 | ▲ +33.47 after sell → book $10,936.86; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CNH` | 113 | $11.54 | $2.36 | $-45.37 | $5,515.66 | ▼ -45.37 after sell → book $10,934.50; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `HMY` | 60 | $22.39 | $2.19 | $-5.56 | $6,856.87 | ▼ -5.56 after sell → book $10,932.31; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 30 | $44.33 | $2.10 | $+12.92 | $8,184.67 | ▲ +12.92 after sell → book $10,930.21; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 150 | $9.03 | $2.48 | $+2.58 | $9,536.70 | ▲ +2.58 after sell → book $10,927.74; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 5 | $267.02 | $2.00 | — | $8,199.59 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $1362.39 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FIGR` | 33 | $40.50 | $2.09 | — | $6,861.00 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+15.8; leftover $1362.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `FUTU` | 10 | $124.67 | $2.02 | — | $5,612.28 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+15.7; leftover $1362.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 2336 | $0.58 | $20.63 | — | $4,229.77 | — | rank by cond; rank cond; list yday_mover; 🔵; ret5=-27.5; leftover $1362.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 261 | $5.21 | $3.37 | — | $2,866.59 | — | rank by cond; rank cond; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $1362.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BTG` | 236 | $5.75 | $3.04 | — | $1,506.55 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+17.9; leftover $1362.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NEXA` | 87 | $15.64 | $2.25 | — | $143.62 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+17.3; leftover $1362.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $143.62 | ▼ close $10,750.43 vs 09:30 $10,943.08 (session -141.91) | 16:00 close · cash $143.62 · equity $10,750.43 vs 09:30 $10,943.08 (-192.65; session marks -141.91) · 8 name(s) marked open→close (per-name table). MOS×56 09:30 $24.84 → close $24.16 -38.08; FNV×5 09:30 $267.02 → close $267.37 +1.75; FIGR×33 09:30 $40.50 → close $37.08 -112.86; FUTU×10 09:30 $124.67 → close $127.34 +26.70; SLQT×2336 09:30 $0.58 → close $0.55 -77.09; TIGR×261 09:30 $5.21 → close $5.46 +65.25; BTG×236 09:30 $5.75 → close $5.74 -2.36; NEXA×87 09:30 $15.64 → close $15.58 -5.22 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $143.62 | ▼ 09:30 equity $10,658.18 vs yday $10,750.43 (-92.25) | 09:30 open · cash $143.62 (unchanged overnight, no fees) · equity $10,658.18 vs prior close $10,750.43 (-92.25) · 8 name(s) re-marked at the open (per-name table). MOS×56 yday $24.16 → 09:30 $24.00 -8.96; FNV×5 yday $267.37 → 09:30 $267.23 -0.70; FIGR×33 yday $37.08 → 09:30 $37.42 +11.22; FUTU×10 yday $127.34 → 09:30 $128.00 +6.60; SLQT×2336 yday $0.55 → 09:30 $0.53 -46.72; TIGR×261 yday $5.46 → 09:30 $5.49 +7.83; BTG×236 yday $5.74 → 09:30 $5.73 -2.36; NEXA×87 yday $15.58 → 09:30 $14.90 -59.16 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 5 | $267.23 | $2.03 | $-2.98 | $1,477.74 | ▼ -2.98 after sell → book $10,656.15; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FIGR` | 33 | $37.42 | $2.11 | $-105.84 | $2,710.49 | ▼ -105.84 after sell → book $10,654.04; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FUTU` | 10 | $128.00 | $2.04 | $+29.24 | $3,988.45 | ▲ +29.24 after sell → book $10,652.00; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 2336 | $0.53 | $19.79 | $-164.22 | $5,206.74 | ▼ -164.22 after sell → book $10,632.21; vs 09:30 mark -19.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 261 | $5.49 | $3.42 | $+66.29 | $6,636.21 | ▲ +66.29 after sell → book $10,628.79; vs 09:30 mark -3.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTG` | 236 | $5.73 | $3.09 | $-10.86 | $7,985.40 | ▼ -10.86 after sell → book $10,625.70; vs 09:30 mark -3.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NEXA` | 87 | $14.90 | $2.28 | $-68.91 | $9,279.42 | ▼ -68.91 after sell → book $10,623.42; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 16 | $81.65 | $2.04 | — | $7,970.98 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1325.63 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 290 | $4.57 | $3.74 | — | $6,641.94 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+1.1; leftover $1325.63 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 17 | $74.54 | $2.04 | — | $5,372.72 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=-0.1; leftover $1325.63 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $4,403.72 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1325.63 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 509 | $2.60 | $6.57 | — | $3,073.75 | — | rank by cond; rank cond; list flatten; ret5=+13.0; leftover $1325.63 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 23 | $55.25 | $2.06 | — | $1,800.94 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+2.1; leftover $1325.63 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $205.90 | $2.01 | — | $563.54 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+8.5; leftover $1325.63 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $563.54 | ▼ close $10,583.43 vs 09:30 $10,658.18 (session -19.55) | 16:00 close · cash $563.54 · equity $10,583.43 vs 09:30 $10,658.18 (-74.75; session marks -19.55) · 8 name(s) marked open→close (per-name table). MOS×56 09:30 $24.00 → close $23.76 -13.44; ACMR×16 09:30 $81.65 → close $80.49 -18.56; GGB×290 09:30 $4.57 → close $4.70 +37.70; MT×17 09:30 $74.54 → close $74.63 +1.53; MU×1 09:30 $967.01 → close $935.39 -31.62; SLI×509 09:30 $2.60 → close $2.64 +20.36; TX×23 09:30 $55.25 → close $55.83 +13.34; ANET×6 09:30 $205.90 → close $201.09 -28.86 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $563.54 | ▼ 09:30 equity $10,579.71 vs yday $10,583.43 (-3.72) | 09:30 open · cash $563.54 (unchanged overnight, no fees) · equity $10,579.71 vs prior close $10,583.43 (-3.72) · 8 name(s) re-marked at the open (per-name table). MOS×56 yday $23.76 → 09:30 $23.95 +10.64; ACMR×16 yday $80.49 → 09:30 $79.27 -19.52; GGB×290 yday $4.70 → 09:30 $4.67 -8.70; MT×17 yday $74.63 → 09:30 $75.39 +12.92; MU×1 yday $935.39 → 09:30 $919.29 -16.10; SLI×509 yday $2.64 → 09:30 $2.68 +20.36; TX×23 yday $55.83 → 09:30 $55.97 +3.22; ANET×6 yday $201.09 → 09:30 $200.00 -6.54 | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 56 | $23.95 | $2.18 | $+5.74 | $1,902.56 | ▲ +5.74 after sell → book $10,577.53; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 16 | $79.27 | $2.06 | $-42.18 | $3,168.82 | ▼ -42.18 after sell → book $10,575.47; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 290 | $4.67 | $3.80 | $+21.46 | $4,519.32 | ▲ +21.46 after sell → book $10,571.67; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 17 | $75.39 | $2.06 | $+10.35 | $5,798.89 | ▲ +10.35 after sell → book $10,569.61; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $6,716.17 | ▼ -51.73 after sell → book $10,567.60; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 509 | $2.68 | $6.66 | $+27.49 | $8,073.62 | ▲ +27.49 after sell → book $10,560.93; vs 09:30 mark -6.67 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 23 | $55.97 | $2.08 | $+12.42 | $9,358.85 | ▲ +12.42 after sell → book $10,558.85; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $200.00 | $2.03 | $-39.44 | $10,556.83 | ▼ -39.44 after sell → book $10,556.83; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $9,257.18 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1319.60 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $7,979.33 | — | rank by cond; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1319.60 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,776.07 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1319.60 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $5,468.05 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1319.60 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.49 | $2.03 | — | $4,185.15 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1319.60 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 21 | $62.82 | $2.05 | — | $2,863.88 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1319.60 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $1,704.12 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1319.60 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 11 | $119.76 | $2.02 | — | $384.74 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1319.60 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $384.74 | ▼ close $10,122.41 vs 09:30 $10,579.71 (session -418.30) | 16:00 close · cash $384.74 · equity $10,122.41 vs 09:30 $10,579.71 (-457.30; session marks -418.30) · 8 name(s) marked open→close (per-name table). KEYS×4 09:30 $324.41 → close $319.97 -17.76; SMTC×9 09:30 $141.76 → close $131.17 -95.31; CIEN×3 09:30 $400.42 → close $378.44 -65.94; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; AVT×14 09:30 $91.49 → close $88.63 -40.04; CGNX×21 09:30 $62.82 → close $60.46 -49.56; COHR×4 09:30 $289.44 → close $279.20 -40.96; LSCC×11 09:30 $119.76 → close $114.40 -58.96 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $384.74 | ▲ 09:30 equity $10,175.90 vs yday $10,122.41 (+53.49) | 09:30 open · cash $384.74 (unchanged overnight, no fees) · equity $10,175.90 vs prior close $10,122.41 (+53.49) · 8 name(s) re-marked at the open (per-name table). KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; AVT×14 yday $88.63 → 09:30 $89.39 +10.64; CGNX×21 yday $60.46 → 09:30 $60.46 +0.00; COHR×4 yday $279.20 → 09:30 $280.25 +4.20; LSCC×11 yday $114.40 → 09:30 $115.56 +12.76 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $1,672.67 | ▼ -11.70 after sell → book $10,173.87; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $2,861.34 | ▼ -89.19 after sell → book $10,171.84; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $3,994.64 | ▼ -69.96 after sell → book $10,169.82; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $5,254.52 | ▼ -48.14 after sell → book $10,167.80; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 14 | $89.39 | $2.05 | $-33.48 | $6,503.93 | ▼ -33.48 after sell → book $10,165.75; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 21 | $60.46 | $2.07 | $-53.69 | $7,771.52 | ▼ -53.69 after sell → book $10,163.68; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $280.25 | $2.02 | $-40.78 | $8,890.50 | ▼ -40.78 after sell → book $10,161.66; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 11 | $115.56 | $2.04 | $-50.27 | $10,159.61 | ▼ -50.27 after sell → book $10,159.61; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,159.61 | ▲ close $10,159.61 vs 09:30 $10,175.90 (session +0.00) | 16:00 close · cash $10,159.61 · no lots left · equity $10,159.61. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,159.61 | ▲ 09:30 equity $10,159.61 vs yday $10,159.61 (+0.00) | 09:30 open · cash $10,159.61 · no holdings · equity $10,159.61 vs prior close $10,159.61 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,159.61 | ▲ close $10,159.61 vs 09:30 $10,159.61 (session +0.00) | 16:00 close · cash $10,159.61 · no lots left · equity $10,159.61. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,159.61 | ▲ 09:30 equity $10,159.61 vs yday $10,159.61 (+0.00) | 09:30 open · cash $10,159.61 · no holdings · equity $10,159.61 vs prior close $10,159.61 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,159.61 | ▲ close $10,159.61 vs 09:30 $10,159.61 (session +0.00) | 16:00 close · cash $10,159.61 · no lots left · equity $10,159.61. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,159.61 | ▲ 09:30 equity $10,159.61 vs yday $10,159.61 (+0.00) | 09:30 open · cash $10,159.61 · no holdings · equity $10,159.61 vs prior close $10,159.61 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 75 | $16.77 | $2.21 | — | $8,899.65 | — | rank by cond; rank cond; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1269.95 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 658 | $1.93 | $8.49 | — | $7,621.22 | — | rank by cond; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1269.95 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 582 | $2.18 | $7.51 | — | $6,344.95 | — | rank by cond; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1269.95 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $5,097.91 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1269.95 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 121 | $10.42 | $2.35 | — | $3,834.73 | — | rank by cond; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1269.95 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PBH` | 23 | $53.45 | $2.06 | — | $2,603.32 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.3; leftover $1269.95 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `PCRX` | 47 | $26.74 | $2.13 | — | $1,344.41 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.6; leftover $1269.95 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $150.35 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1269.95 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $150.35 | ▼ close $9,931.08 vs 09:30 $10,159.61 (session -199.69) | 16:00 close · cash $150.35 · equity $9,931.08 vs 09:30 $10,159.61 (-228.53; session marks -199.69) · 8 name(s) marked open→close (per-name table). ARCT×75 09:30 $16.77 → close $15.56 -90.75; BMEA×658 09:30 $1.93 → close $1.91 -13.16; CRDL×582 09:30 $2.18 → close $2.16 -11.64; HRMY×29 09:30 $42.93 → close $41.86 -31.03; NVAX×121 09:30 $10.42 → close $10.34 -9.68; PBH×23 09:30 $53.45 → close $52.56 -20.47; PCRX×47 09:30 $26.74 → close $26.60 -6.58; RVTY×9 09:30 $132.45 → close $130.63 -16.38 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $150.35 | ▼ 09:30 equity $9,903.95 vs yday $9,931.08 (-27.13) | 09:30 open · cash $150.35 (unchanged overnight, no fees) · equity $9,903.95 vs prior close $9,931.08 (-27.13) · 8 name(s) re-marked at the open (per-name table). ARCT×75 yday $15.56 → 09:30 $15.61 +3.75; BMEA×658 yday $1.91 → 09:30 $1.90 -6.58; CRDL×582 yday $2.16 → 09:30 $2.16 +0.00; HRMY×29 yday $41.86 → 09:30 $41.50 -10.44; NVAX×121 yday $10.34 → 09:30 $10.50 +19.36; PBH×23 yday $52.56 → 09:30 $51.80 -17.48; PCRX×47 yday $26.60 → 09:30 $26.38 -10.34; RVTY×9 yday $130.63 → 09:30 $130.03 -5.40 | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 75 | $15.61 | $2.24 | $-91.45 | $1,318.86 | ▼ -91.45 after sell → book $9,901.71; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 582 | $2.16 | $7.61 | $-26.76 | $2,568.36 | ▼ -26.76 after sell → book $9,894.09; vs 09:30 mark -7.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 121 | $10.50 | $2.38 | $+4.94 | $3,836.48 | ▲ +4.94 after sell → book $9,891.71; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBH` | 23 | $51.80 | $2.08 | $-42.09 | $5,025.80 | ▼ -42.09 after sell → book $9,889.63; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PCRX` | 47 | $26.38 | $2.15 | $-21.20 | $6,263.51 | ▼ -21.20 after sell → book $9,887.48; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $7,431.74 | ▼ -25.83 after sell → book $9,885.44; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 357 | $3.46 | $4.61 | — | $6,191.92 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1238.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 491 | $2.52 | $6.33 | — | $4,948.26 | — | rank by cond; rank cond; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1238.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 23 | $52.03 | $2.06 | — | $3,749.52 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $1238.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 184 | $6.71 | $2.54 | — | $2,512.33 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1238.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $1,456.89 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1238.62 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 44 | $28.00 | $2.12 | — | $222.77 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+8.7; leftover $1238.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $222.77 | ▲ close $9,900.57 vs 09:30 $9,903.95 (session +34.79) | 16:00 close · cash $222.77 · equity $9,900.57 vs 09:30 $9,903.95 (-3.38; session marks +34.79) · 8 name(s) marked open→close (per-name table). BMEA×658 09:30 $1.90 → close $2.03 +85.54; HRMY×29 09:30 $41.50 → close $42.25 +21.75; CABA×357 09:30 $3.46 → close $3.47 +3.57; ALEC×491 09:30 $2.52 → close $2.46 -29.46; ATRC×23 09:30 $52.03 → close $51.52 -11.73; BHC×184 09:30 $6.71 → close $6.56 -27.60; CRM×4 09:30 $263.36 → close $259.23 -16.52; MLYS×44 09:30 $28.00 → close $28.21 +9.24 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $222.77 | ▼ 09:30 equity $9,861.87 vs yday $9,900.57 (-38.70) | 09:30 open · cash $222.77 (unchanged overnight, no fees) · equity $9,861.87 vs prior close $9,900.57 (-38.70) · 8 name(s) re-marked at the open (per-name table). BMEA×658 yday $2.03 → 09:30 $2.00 -19.74; HRMY×29 yday $42.25 → 09:30 $42.20 -1.45; CABA×357 yday $3.47 → 09:30 $3.43 -14.28; ALEC×491 yday $2.46 → 09:30 $2.38 -39.28; ATRC×23 yday $51.52 → 09:30 $54.31 +64.17; BHC×184 yday $6.56 → 09:30 $6.57 +1.84; CRM×4 yday $259.23 → 09:30 $253.72 -22.04; MLYS×44 yday $28.21 → 09:30 $28.03 -7.92 | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 658 | $2.00 | $8.61 | $+28.96 | $1,530.16 | ▲ +28.96 after sell → book $9,853.26; vs 09:30 mark -8.61 | dropped from list after 2 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `HRMY` | 29 | $42.20 | $2.10 | $-25.34 | $2,751.86 | ▼ -25.34 after sell → book $9,851.16; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 357 | $3.43 | $4.67 | $-19.99 | $3,971.70 | ▼ -19.99 after sell → book $9,846.49; vs 09:30 mark -4.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 491 | $2.38 | $6.43 | $-81.50 | $5,133.85 | ▼ -81.50 after sell → book $9,840.06; vs 09:30 mark -6.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 23 | $54.31 | $2.08 | $+48.30 | $6,380.91 | ▲ +48.30 after sell → book $9,837.99; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 184 | $6.57 | $2.58 | $-30.88 | $7,587.20 | ▼ -30.88 after sell → book $9,835.40; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $8,600.06 | ▼ -42.58 after sell → book $9,833.38; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MLYS` | 44 | $28.03 | $2.14 | $-2.94 | $9,831.24 | ▼ -2.94 after sell → book $9,831.24; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,831.24 | ▲ close $9,831.24 vs 09:30 $9,861.87 (session +0.00) | 16:00 close · cash $9,831.24 · no lots left · equity $9,831.24. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,831.24 | ▲ 09:30 equity $9,831.24 vs yday $9,831.24 (-0.00) | 09:30 open · cash $9,831.24 · no holdings · equity $9,831.24 vs prior close $9,831.24 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,831.24 | ▲ close $9,831.24 vs 09:30 $9,831.24 (session +0.00) | 16:00 close · cash $9,831.24 · no lots left · equity $9,831.24. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DNN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BHP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BSBR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `EBAY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NOK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TME` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRGP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACIW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `AVPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CHKP` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALAB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ANET` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `APA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CHRD` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CRK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ACB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ADM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASND` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASTH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CHEF` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `VNT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `GLW` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SWKS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OBE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SKHY` | hard_red | hard-red S=-13.95 sit; no new buys |
