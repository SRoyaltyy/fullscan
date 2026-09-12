# Factor mine action — `union_cond_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · rank by cond

Cash book **-2.57%** ($9,743) · signal-only (no cash/fees) was +14.02%. Starts YES **7/21**. Fills 166 · skips 79 · realized $-305.66.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,329.50.

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
| 2026-08-14 | `BRUN` | 48 | — | $26.25 | +0.00 | $22.93 | -159.12 | -159.12 | +0.00 | -159.12 |
| 2026-08-14 | `CLBT` | 116 | — | $10.83 | +0.00 | $11.14 | +35.96 | +35.96 | +0.00 | +35.96 |
| 2026-08-14 | `HLIT` | 95 | — | $13.18 | +0.00 | $13.92 | +70.30 | +70.30 | +0.00 | +70.30 |
| 2026-08-14 | `MNTN` | 101 | — | $12.50 | +0.00 | $12.52 | +2.02 | +2.02 | +0.00 | +2.02 |
| 2026-08-14 | `QMCO` | 51 | — | $24.68 | +0.00 | $26.11 | +72.93 | +72.93 | +0.00 | +72.93 |
| 2026-08-14 | `QMLS` | 173 | — | $7.29 | +0.00 | $7.32 | +5.19 | +5.19 | +0.00 | +5.19 |
| 2026-08-14 | `SECZ` | 216 | — | $5.84 | +0.00 | $5.61 | -49.68 | -49.68 | +0.00 | -49.68 |
| 2026-08-17 | `BRUN` | 48 | $22.93 | $23.00 | +3.36 | — | +0.00 | +3.36 | -155.76 | — |
| 2026-08-17 | `CLBT` | 116 | $11.14 | $11.19 | +5.80 | — | +0.00 | +5.80 | +41.76 | — |
| 2026-08-17 | `HLIT` | 95 | $13.92 | $13.84 | -7.60 | — | +0.00 | -7.60 | +62.70 | — |
| 2026-08-17 | `MNTN` | 101 | $12.52 | $12.40 | -12.12 | — | +0.00 | -12.12 | -10.10 | — |
| 2026-08-17 | `QMCO` | 51 | $26.11 | $24.83 | -65.28 | — | +0.00 | -65.28 | +7.65 | — |
| 2026-08-17 | `QMLS` | 173 | $7.32 | $7.24 | -13.84 | — | +0.00 | -13.84 | -8.65 | — |
| 2026-08-17 | `SECZ` | 216 | $5.61 | $5.45 | -34.56 | — | +0.00 | -34.56 | -84.24 | — |
| 2026-08-17 | `LPTH` | 83 | — | $14.94 | +0.00 | $14.80 | -11.62 | -11.62 | +0.00 | -11.62 |
| 2026-08-17 | `VERI` | 1078 | — | $1.15 | +0.00 | $1.08 | -70.07 | -70.07 | +0.00 | -70.07 |
| 2026-08-17 | `DVN` | 26 | — | $46.18 | +0.00 | $47.57 | +36.14 | +36.14 | +0.00 | +36.14 |
| 2026-08-17 | `EOG` | 8 | — | $142.77 | +0.00 | $146.15 | +27.04 | +27.04 | +0.00 | +27.04 |
| 2026-08-17 | `FANG` | 6 | — | $202.70 | +0.00 | $206.29 | +21.54 | +21.54 | +0.00 | +21.54 |
| 2026-08-17 | `AAOI` | 8 | — | $152.64 | +0.00 | $154.89 | +18.00 | +18.00 | +0.00 | +18.00 |
| 2026-08-17 | `ABX` | 136 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `ALOY` | 84 | — | $14.66 | +0.00 | $13.86 | -67.62 | -67.62 | +0.00 | -67.62 |
| 2026-08-18 | `LPTH` | 83 | $14.80 | $14.01 | -65.57 | — | +0.00 | -65.57 | -77.19 | — |
| 2026-08-18 | `VERI` | 1078 | $1.08 | $1.05 | -37.73 | — | +0.00 | -37.73 | -107.80 | — |
| 2026-08-18 | `DVN` | 26 | $47.57 | $48.00 | +11.18 | — | +0.00 | +11.18 | +47.32 | — |
| 2026-08-18 | `EOG` | 8 | $146.15 | $148.04 | +15.12 | — | +0.00 | +15.12 | +42.16 | — |
| 2026-08-18 | `FANG` | 6 | $206.29 | $208.93 | +15.84 | — | +0.00 | +15.84 | +37.38 | — |
| 2026-08-18 | `AAOI` | 8 | $154.89 | $146.20 | -69.52 | — | +0.00 | -69.52 | -51.52 | — |
| 2026-08-18 | `ABX` | 136 | $9.12 | $9.03 | -12.24 | — | +0.00 | -12.24 | -12.24 | — |
| 2026-08-18 | `ALOY` | 84 | $13.86 | $13.19 | -55.86 | — | +0.00 | -55.86 | -123.48 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 58 | — | $20.55 | +0.00 | $21.19 | +37.12 | +37.12 | +0.00 | +37.12 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 58 | — | $20.65 | +0.00 | $21.11 | +26.68 | +26.68 | +0.00 | +26.68 |
| 2026-08-20 | `HDSN` | 208 | — | $5.77 | +0.00 | $5.57 | -41.60 | -41.60 | +0.00 | -41.60 |
| 2026-08-20 | `IAG` | 61 | — | $19.63 | +0.00 | $20.50 | +53.07 | +53.07 | +0.00 | +53.07 |
| 2026-08-20 | `KGC` | 40 | — | $29.63 | +0.00 | $31.43 | +72.00 | +72.00 | +0.00 | +72.00 |
| 2026-08-20 | `NFGC` | 687 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 58 | $21.19 | $21.90 | +41.18 | — | +0.00 | +41.18 | +78.30 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `CDE` | 58 | $21.11 | $21.75 | +37.12 | — | +0.00 | +37.12 | +63.80 | — |
| 2026-08-21 | `HDSN` | 208 | $5.57 | $5.67 | +20.80 | — | +0.00 | +20.80 | -20.80 | — |
| 2026-08-21 | `IAG` | 61 | $20.50 | $21.17 | +40.87 | — | +0.00 | +40.87 | +93.94 | — |
| 2026-08-21 | `KGC` | 40 | $31.43 | $32.17 | +29.60 | — | +0.00 | +29.60 | +101.60 | — |
| 2026-08-21 | `NFGC` | 687 | $1.75 | $1.79 | +27.48 | — | +0.00 | +27.48 | +27.48 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 73 | — | $17.20 | +0.00 | $16.65 | -40.15 | -40.15 | +0.00 | -40.15 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 112 | — | $11.13 | +0.00 | $13.45 | +259.84 | +259.84 | +0.00 | +259.84 |
| 2026-08-21 | `AUTL` | 509 | — | $2.47 | +0.00 | $2.41 | -30.54 | -30.54 | +0.00 | -30.54 |
| 2026-08-21 | `CRDL` | 651 | — | $1.93 | +0.00 | $1.86 | -45.57 | -45.57 | +0.00 | -45.57 |
| 2026-08-21 | `CRSP` | 21 | — | $59.72 | +0.00 | $59.50 | -4.62 | -4.62 | +0.00 | -4.62 |
| 2026-08-21 | `CYPH` | 952 | — | $1.32 | +0.00 | $1.42 | +95.20 | +95.20 | +0.00 | +95.20 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUPH` | 73 | $16.65 | $16.57 | -5.84 | — | +0.00 | -5.84 | -45.99 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 112 | $13.45 | $13.33 | -13.44 | — | +0.00 | -13.44 | +246.40 | — |
| 2026-08-24 | `AUTL` | 509 | $2.41 | $2.40 | -5.09 | — | +0.00 | -5.09 | -35.63 | — |
| 2026-08-24 | `CRDL` | 651 | $1.86 | $1.88 | +13.02 | — | +0.00 | +13.02 | -32.55 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | $57.08 | -35.17 | -50.92 | -20.37 | -55.54 |
| 2026-08-24 | `CYPH` | 952 | $1.42 | $1.83 | +390.32 | — | +0.00 | +390.32 | +485.52 | — |
| 2026-08-25 | `CRSP` | 21 | $57.08 | $57.93 | +17.95 | — | +0.00 | +17.95 | -37.59 | — |
| 2026-08-25 | `AU` | 11 | — | $118.52 | +0.00 | $123.39 | +53.57 | +53.57 | +0.00 | +53.57 |
| 2026-08-25 | `ERO` | 34 | — | $38.01 | +0.00 | $40.40 | +81.26 | +81.26 | +0.00 | +81.26 |
| 2026-08-25 | `FCX` | 17 | — | $77.13 | +0.00 | $79.91 | +47.26 | +47.26 | +0.00 | +47.26 |
| 2026-08-25 | `CNH` | 111 | — | $11.90 | +0.00 | $11.56 | -37.74 | -37.74 | +0.00 | -37.74 |
| 2026-08-25 | `HMY` | 59 | — | $22.41 | +0.00 | $22.95 | +31.86 | +31.86 | +0.00 | +31.86 |
| 2026-08-25 | `MOS` | 55 | — | $23.77 | +0.00 | $24.27 | +27.50 | +27.50 | +0.00 | +27.50 |
| 2026-08-25 | `RHI` | 30 | — | $43.76 | +0.00 | $44.90 | +34.20 | +34.20 | +0.00 | +34.20 |
| 2026-08-25 | `SUZ` | 147 | — | $8.98 | +0.00 | $9.03 | +7.35 | +7.35 | +0.00 | +7.35 |
| 2026-08-26 | `AU` | 11 | $123.39 | $119.80 | -39.49 | — | +0.00 | -39.49 | +14.08 | — |
| 2026-08-26 | `ERO` | 34 | $40.40 | $40.51 | +3.74 | — | +0.00 | +3.74 | +85.00 | — |
| 2026-08-26 | `FCX` | 17 | $79.91 | $79.34 | -9.69 | — | +0.00 | -9.69 | +37.57 | — |
| 2026-08-26 | `CNH` | 111 | $11.56 | $11.54 | -2.22 | — | +0.00 | -2.22 | -39.96 | — |
| 2026-08-26 | `HMY` | 59 | $22.95 | $22.39 | -33.04 | — | +0.00 | -33.04 | -1.18 | — |
| 2026-08-26 | `MOS` | 55 | $24.27 | $24.84 | +31.35 | $24.16 | -37.40 | -6.05 | +58.85 | +21.45 |
| 2026-08-26 | `RHI` | 30 | $44.90 | $44.33 | -17.10 | — | +0.00 | -17.10 | +17.10 | — |
| 2026-08-26 | `SUZ` | 147 | $9.03 | $9.03 | +0.00 | — | +0.00 | +0.00 | +7.35 | — |
| 2026-08-26 | `FNV` | 5 | — | $267.02 | +0.00 | $267.37 | +1.75 | +1.75 | +0.00 | +1.75 |
| 2026-08-26 | `FIGR` | 33 | — | $40.50 | +0.00 | $37.08 | -112.86 | -112.86 | +0.00 | -112.86 |
| 2026-08-26 | `FUTU` | 10 | — | $124.67 | +0.00 | $127.34 | +26.70 | +26.70 | +0.00 | +26.70 |
| 2026-08-26 | `SLQT` | 2293 | — | $0.58 | +0.00 | $0.55 | -75.67 | -75.67 | +0.00 | -75.67 |
| 2026-08-26 | `TIGR` | 256 | — | $5.21 | +0.00 | $5.46 | +64.00 | +64.00 | +0.00 | +64.00 |
| 2026-08-26 | `BTG` | 232 | — | $5.75 | +0.00 | $5.74 | -2.32 | -2.32 | +0.00 | -2.32 |
| 2026-08-26 | `NEXA` | 85 | — | $15.64 | +0.00 | $15.58 | -5.10 | -5.10 | +0.00 | -5.10 |
| 2026-08-27 | `MOS` | 55 | $24.16 | $24.00 | -8.80 | $23.76 | -13.20 | -22.00 | +12.65 | -0.55 |
| 2026-08-27 | `FNV` | 5 | $267.37 | $267.23 | -0.70 | — | +0.00 | -0.70 | +1.05 | — |
| 2026-08-27 | `FIGR` | 33 | $37.08 | $37.42 | +11.22 | — | +0.00 | +11.22 | -101.64 | — |
| 2026-08-27 | `FUTU` | 10 | $127.34 | $128.00 | +6.60 | — | +0.00 | +6.60 | +33.30 | — |
| 2026-08-27 | `SLQT` | 2293 | $0.55 | $0.53 | -45.86 | — | +0.00 | -45.86 | -121.53 | — |
| 2026-08-27 | `TIGR` | 256 | $5.46 | $5.49 | +7.68 | — | +0.00 | +7.68 | +71.68 | — |
| 2026-08-27 | `BTG` | 232 | $5.74 | $5.73 | -2.32 | — | +0.00 | -2.32 | -4.64 | — |
| 2026-08-27 | `NEXA` | 85 | $15.58 | $14.90 | -57.80 | — | +0.00 | -57.80 | -62.90 | — |
| 2026-08-27 | `ACMR` | 15 | — | $81.65 | +0.00 | $80.49 | -17.40 | -17.40 | +0.00 | -17.40 |
| 2026-08-27 | `GGB` | 284 | — | $4.57 | +0.00 | $4.70 | +36.92 | +36.92 | +0.00 | +36.92 |
| 2026-08-27 | `MT` | 17 | — | $74.54 | +0.00 | $74.63 | +1.53 | +1.53 | +0.00 | +1.53 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `SLI` | 500 | — | $2.60 | +0.00 | $2.64 | +20.00 | +20.00 | +0.00 | +20.00 |
| 2026-08-27 | `TX` | 23 | — | $55.25 | +0.00 | $55.83 | +13.34 | +13.34 | +0.00 | +13.34 |
| 2026-08-27 | `ANET` | 6 | — | $205.90 | +0.00 | $201.09 | -28.86 | -28.86 | +0.00 | -28.86 |
| 2026-08-28 | `MOS` | 55 | $23.76 | $23.95 | +10.45 | — | +0.00 | +10.45 | +9.90 | — |
| 2026-08-28 | `ACMR` | 15 | $80.49 | $79.27 | -18.30 | — | +0.00 | -18.30 | -35.70 | — |
| 2026-08-28 | `GGB` | 284 | $4.70 | $4.67 | -8.52 | — | +0.00 | -8.52 | +28.40 | — |
| 2026-08-28 | `MT` | 17 | $74.63 | $75.39 | +12.92 | — | +0.00 | +12.92 | +14.45 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `SLI` | 500 | $2.64 | $2.68 | +20.00 | — | +0.00 | +20.00 | +40.00 | — |
| 2026-08-28 | `TX` | 23 | $55.83 | $55.97 | +3.22 | — | +0.00 | +3.22 | +16.56 | — |
| 2026-08-28 | `ANET` | 6 | $201.09 | $200.00 | -6.54 | — | +0.00 | -6.54 | -35.40 | — |
| 2026-08-28 | `KEYS` | 3 | — | $324.41 | +0.00 | $319.97 | -13.32 | -13.32 | +0.00 | -13.32 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `AVT` | 14 | — | $91.49 | +0.00 | $88.63 | -40.04 | -40.04 | +0.00 | -40.04 |
| 2026-08-28 | `CGNX` | 20 | — | $62.82 | +0.00 | $60.46 | -47.20 | -47.20 | +0.00 | -47.20 |
| 2026-08-28 | `COHR` | 4 | — | $289.44 | +0.00 | $279.20 | -40.96 | -40.96 | +0.00 | -40.96 |
| 2026-08-28 | `LSCC` | 10 | — | $119.76 | +0.00 | $114.40 | -53.60 | -53.60 | +0.00 | -53.60 |
| 2026-08-31 | `KEYS` | 3 | $319.97 | $322.49 | +7.56 | — | +0.00 | +7.56 | -5.76 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -65.94 | — |
| 2026-08-31 | `AVT` | 14 | $88.63 | $89.39 | +10.64 | — | +0.00 | +10.64 | -29.40 | — |
| 2026-08-31 | `CGNX` | 20 | $60.46 | $60.46 | +0.00 | — | +0.00 | +0.00 | -47.20 | — |
| 2026-08-31 | `COHR` | 4 | $279.20 | $280.25 | +4.20 | — | +0.00 | +4.20 | -36.76 | — |
| 2026-08-31 | `LSCC` | 10 | $114.40 | $115.56 | +11.60 | — | +0.00 | +11.60 | -42.00 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ARCT` | 74 | — | $16.77 | +0.00 | $15.56 | -89.54 | -89.54 | +0.00 | -89.54 |
| 2026-09-03 | `BMEA` | 648 | — | $1.93 | +0.00 | $1.91 | -12.96 | -12.96 | +0.00 | -12.96 |
| 2026-09-03 | `CRDL` | 574 | — | $2.18 | +0.00 | $2.16 | -11.48 | -11.48 | +0.00 | -11.48 |
| 2026-09-03 | `HRMY` | 29 | — | $42.93 | +0.00 | $41.86 | -31.03 | -31.03 | +0.00 | -31.03 |
| 2026-09-03 | `NVAX` | 120 | — | $10.42 | +0.00 | $10.34 | -9.60 | -9.60 | +0.00 | -9.60 |
| 2026-09-03 | `PBH` | 23 | — | $53.45 | +0.00 | $52.56 | -20.47 | -20.47 | +0.00 | -20.47 |
| 2026-09-03 | `PCRX` | 46 | — | $26.74 | +0.00 | $26.60 | -6.44 | -6.44 | +0.00 | -6.44 |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-04 | `ARCT` | 74 | $15.56 | $15.61 | +3.70 | — | +0.00 | +3.70 | -85.84 | — |
| 2026-09-04 | `BMEA` | 648 | $1.91 | $1.90 | -6.48 | $2.03 | +84.24 | +77.76 | -19.44 | +64.80 |
| 2026-09-04 | `CRDL` | 574 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -11.48 | — |
| 2026-09-04 | `HRMY` | 29 | $41.86 | $41.50 | -10.44 | $42.25 | +21.75 | +11.31 | -41.47 | -19.72 |
| 2026-09-04 | `NVAX` | 120 | $10.34 | $10.50 | +19.20 | — | +0.00 | +19.20 | +9.60 | — |
| 2026-09-04 | `PBH` | 23 | $52.56 | $51.80 | -17.48 | — | +0.00 | -17.48 | -37.95 | — |
| 2026-09-04 | `PCRX` | 46 | $26.60 | $26.38 | -10.12 | — | +0.00 | -10.12 | -16.56 | — |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `CABA` | 352 | — | $3.46 | +0.00 | $3.47 | +3.52 | +3.52 | +0.00 | +3.52 |
| 2026-09-04 | `ALEC` | 483 | — | $2.52 | +0.00 | $2.46 | -28.98 | -28.98 | +0.00 | -28.98 |
| 2026-09-04 | `ATRC` | 23 | — | $52.03 | +0.00 | $51.52 | -11.73 | -11.73 | +0.00 | -11.73 |
| 2026-09-04 | `BHC` | 181 | — | $6.71 | +0.00 | $6.56 | -27.15 | -27.15 | +0.00 | -27.15 |
| 2026-09-04 | `CRM` | 4 | — | $263.36 | +0.00 | $259.23 | -16.52 | -16.52 | +0.00 | -16.52 |
| 2026-09-04 | `MLYS` | 43 | — | $28.00 | +0.00 | $28.21 | +9.03 | +9.03 | +0.00 | +9.03 |
| 2026-09-08 | `BMEA` | 648 | $2.03 | $2.00 | -19.44 | — | +0.00 | -19.44 | +45.36 | — |
| 2026-09-08 | `HRMY` | 29 | $42.25 | $42.20 | -1.45 | — | +0.00 | -1.45 | -21.17 | — |
| 2026-09-08 | `CABA` | 352 | $3.47 | $3.43 | -14.08 | — | +0.00 | -14.08 | -10.56 | — |
| 2026-09-08 | `ALEC` | 483 | $2.46 | $2.38 | -38.64 | — | +0.00 | -38.64 | -67.62 | — |
| 2026-09-08 | `ATRC` | 23 | $51.52 | $54.31 | +64.17 | — | +0.00 | +64.17 | +52.44 | — |
| 2026-09-08 | `BHC` | 181 | $6.56 | $6.57 | +1.81 | — | +0.00 | +1.81 | -25.34 | — |
| 2026-09-08 | `CRM` | 4 | $259.23 | $253.72 | -22.04 | — | +0.00 | -22.04 | -38.56 | — |
| 2026-09-08 | `MLYS` | 43 | $28.21 | $28.03 | -7.74 | — | +0.00 | -7.74 | +1.29 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `HPE` | 21 | — | $56.37 | +0.00 | $62.09 | +120.12 | +120.12 | +0.00 | +120.12 |
| 2026-09-11 | `SEDG` | 32 | — | $36.78 | +0.00 | $34.68 | -67.20 | -67.20 | +0.00 | -67.20 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +160.83 | BTSG, HIMS, INO, SLS, TGTX, TNDM, VOR, IREN | — | $79.84 | $10,128.79 | BTSG×20, HIMS×42, INO×1543, SLS×106, TGTX×25, TNDM×53, VOR×56, IREN×27 |
| 2026-08-14 | +5.50 | $79.84 | BTSG×20, HIMS×42, INO×1543, SLS×106, TGTX×25, TNDM×53, VOR×56, IREN×27 | $10,139.95 | +11.16 | -22.40 | BRUN, CLBT, HLIT, MNTN, QMCO, QMLS, SECZ | BTSG, HIMS, INO, SLS, TGTX, TNDM, VOR, IREN | $1,277.23 | $10,066.76 | BRUN×48, CLBT×116, HLIT×95, MNTN×101, QMCO×51, QMLS×173, SECZ×216 |
| 2026-08-17 | +2.25 | $1,277.23 | BRUN×48, CLBT×116, HLIT×95, MNTN×101, QMCO×51, QMLS×173, SECZ×216 | $9,942.52 | -124.24 | -46.59 | LPTH, VERI, DVN, EOG, FANG, AAOI, ABX, ALOY | BRUN, CLBT, HLIT, MNTN, QMCO, QMLS, SECZ | $165.30 | $9,850.35 | LPTH×83, VERI×1078, DVN×26, EOG×8, FANG×6, AAOI×8, ABX×136, ALOY×84 |
| 2026-08-18 | -6.20 | $165.30 | LPTH×83, VERI×1078, DVN×26, EOG×8, FANG×6, AAOI×8, ABX×136, ALOY×84 | $9,651.57 | -198.78 | +0.00 | — | LPTH, VERI, DVN, EOG, FANG, AAOI, ABX, ALOY | $9,622.33 | $9,622.33 | — |
| 2026-08-19 | -7.20 | $9,622.33 | — | $9,622.33 | +0.00 | +0.00 | — | — | $9,622.33 | $9,622.33 | — |
| 2026-08-20 | +1.12 | $9,622.33 | — | $9,622.33 | +0.00 | +227.01 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $84.04 | $9,825.14 | AG×58, BHP×13, CDE×58, HDSN×208, IAG×61, KGC×40, NFGC×687, WPM×8 |
| 2026-08-21 | +3.25 | $84.04 | AG×58, BHP×13, CDE×58, HDSN×208, IAG×61, KGC×40, NFGC×687, WPM×8 | $10,084.96 | +259.82 | +250.86 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $220.24 | $10,273.48 | AU×10, AUPH×73, AEM×5, ARCT×112, AUTL×509, CRDL×651, CRSP×21, CYPH×952 |
| 2026-08-24 | -5.17 | $220.24 | AU×10, AUPH×73, AEM×5, ARCT×112, AUTL×509, CRDL×651, CRSP×21, CYPH×952 | $10,634.45 | +360.97 | -35.17 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CYPH | $9,364.41 | $10,562.99 | CRSP×21 |
| 2026-08-25 | +1.80 | $9,364.41 | CRSP×21 | $10,580.94 | +17.95 | +245.26 | AU, ERO, FCX, CNH, HMY, MOS, RHI, SUZ | CRSP | $70.99 | $10,806.82 | AU×11, ERO×34, FCX×17, CNH×111, HMY×59, MOS×55, RHI×30, SUZ×147 |
| 2026-08-26 | +2.02 | $70.99 | AU×11, ERO×34, FCX×17, CNH×111, HMY×59, MOS×55, RHI×30, SUZ×147 | $10,740.37 | -66.45 | -140.90 | FNV, FIGR, FUTU, SLQT, TIGR, BTG, NEXA | AU, ERO, FCX, CNH, HMY, RHI, SUZ | $71.66 | $10,549.24 | MOS×55, FNV×5, FIGR×33, FUTU×10, SLQT×2293, TIGR×256, BTG×232, NEXA×85 |
| 2026-08-27 | — | $71.66 | MOS×55, FNV×5, FIGR×33, FUTU×10, SLQT×2293, TIGR×256, BTG×232, NEXA×85 | $10,459.26 | -89.98 | -19.29 | ACMR, GGB, MT, MU, SLI, TX, ANET | FNV, FIGR, FUTU, SLQT, TIGR, BTG, NEXA | $521.78 | $10,385.46 | MOS×55, ACMR×15, GGB×284, MT×17, MU×1, SLI×500, TX×23, ANET×6 |
| 2026-08-28 | +0.75 | $521.78 | MOS×55, ACMR×15, GGB×284, MT×17, MU×1, SLI×500, TX×23, ANET×6 | $10,382.59 | -2.87 | -356.37 | KEYS, SMTC, CIEN, AVT, CGNX, COHR, LSCC | MOS, ACMR, GGB, MT, MU, SLI, TX, ANET | $2,002.84 | $9,989.42 | KEYS×3, SMTC×9, CIEN×3, AVT×14, CGNX×20, COHR×4, LSCC×10 |
| 2026-08-31 | -5.85 | $2,002.84 | KEYS×3, SMTC×9, CIEN×3, AVT×14, CGNX×20, COHR×4, LSCC×10 | $10,033.59 | +44.17 | +0.00 | — | KEYS, SMTC, CIEN, AVT, CGNX, COHR, LSCC | $10,019.33 | $10,019.33 | — |
| 2026-09-01 | -6.30 | $10,019.33 | — | $10,019.33 | +0.00 | +0.00 | — | — | $10,019.33 | $10,019.33 | — |
| 2026-09-02 | -3.83 | $10,019.33 | — | $10,019.33 | +0.00 | +0.00 | — | — | $10,019.33 | $10,019.33 | — |
| 2026-09-03 | -0.90 | $10,019.33 | — | $10,019.33 | +0.00 | -197.90 | ARCT, BMEA, CRDL, HRMY, NVAX, PBH, PCRX, RVTY | — | $100.98 | $9,792.83 | ARCT×74, BMEA×648, CRDL×574, HRMY×29, NVAX×120, PBH×23, PCRX×46, RVTY×9 |
| 2026-09-04 | +2.25 | $100.98 | ARCT×74, BMEA×648, CRDL×574, HRMY×29, NVAX×120, PBH×23, PCRX×46, RVTY×9 | $9,765.81 | -27.02 | +34.16 | CABA, ALEC, ATRC, BHC, CRM, MLYS | ARCT, CRDL, NVAX, PBH, PCRX, RVTY | $189.51 | $9,762.09 | BMEA×648, HRMY×29, CABA×352, ALEC×483, ATRC×23, BHC×181, CRM×4, MLYS×43 |
| 2026-09-08 | -11.47 | $189.51 | BMEA×648, HRMY×29, CABA×352, ALEC×483, ATRC×23, BHC×181, CRM×4, MLYS×43 | $9,724.68 | -37.41 | +0.00 | — | BMEA, HRMY, CABA, ALEC, ATRC, BHC, CRM, MLYS | $9,694.37 | $9,694.37 | — |
| 2026-09-09 | -13.95 | $9,694.37 | — | $9,694.37 | -0.00 | +0.00 | — | — | $9,694.37 | $9,694.37 | — |
| 2026-09-10 | -13.28 | $9,694.37 | — | $9,694.37 | -0.00 | +0.00 | — | — | $9,694.37 | $9,694.37 | — |
| 2026-09-11 | +0.50 | $9,694.37 | — | $9,694.37 | -0.00 | +52.92 | HPE, SEDG | — | $7,329.50 | $9,743.15 | HPE×21, SEDG×32 |

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
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 48 | $26.25 | $2.13 | — | $8,843.74 | — | rank by cond; rank cond; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $1263.20 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 116 | $10.83 | $2.34 | — | $7,585.12 | — | rank by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-30.1; leftover $1263.20 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 95 | $13.18 | $2.27 | — | $6,330.75 | — | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1263.20 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MNTN` | 101 | $12.50 | $2.29 | — | $5,065.95 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1263.20 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 51 | $24.68 | $2.14 | — | $3,805.13 | — | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1263.20 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 173 | $7.29 | $2.51 | — | $2,541.45 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1263.20 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SECZ` | 216 | $5.84 | $2.79 | — | $1,277.23 | — | rank by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-20.7; leftover $1263.20 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,277.23 | ▼ close $10,066.76 vs 09:30 $10,139.95 (session -22.40) | 16:00 close · cash $1,277.23 · equity $10,066.76 vs 09:30 $10,139.95 (-73.19; session marks -22.40) · 7 name(s) marked open→close (per-name table). BRUN×48 09:30 $26.25 → close $22.93 -159.12; CLBT×116 09:30 $10.83 → close $11.14 +35.96; HLIT×95 09:30 $13.18 → close $13.92 +70.30; MNTN×101 09:30 $12.50 → close $12.52 +2.02; QMCO×51 09:30 $24.68 → close $26.11 +72.93; QMLS×173 09:30 $7.29 → close $7.32 +5.19; SECZ×216 09:30 $5.84 → close $5.61 -49.68 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,277.23 | ▼ 09:30 equity $9,942.52 vs yday $10,066.76 (-124.24) | 09:30 open · cash $1,277.23 (unchanged overnight, no fees) · equity $9,942.52 vs prior close $10,066.76 (-124.24) · 7 name(s) re-marked at the open (per-name table). BRUN×48 yday $22.93 → 09:30 $23.00 +3.36; CLBT×116 yday $11.14 → 09:30 $11.19 +5.80; HLIT×95 yday $13.92 → 09:30 $13.84 -7.60; MNTN×101 yday $12.52 → 09:30 $12.40 -12.12; QMCO×51 yday $26.11 → 09:30 $24.83 -65.28; QMLS×173 yday $7.32 → 09:30 $7.24 -13.84; SECZ×216 yday $5.61 → 09:30 $5.45 -34.56 | — |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 48 | $23.00 | $2.15 | $-160.05 | $2,379.07 | ▼ -160.05 after sell → book $9,940.36; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 116 | $11.19 | $2.37 | $+37.05 | $3,674.74 | ▲ +37.05 after sell → book $9,937.99; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 95 | $13.84 | $2.30 | $+58.12 | $4,987.24 | ▲ +58.12 after sell → book $9,935.69; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MNTN` | 101 | $12.40 | $2.32 | $-14.71 | $6,237.32 | ▼ -14.71 after sell → book $9,933.37; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 51 | $24.83 | $2.16 | $+3.34 | $7,501.49 | ▲ +3.34 after sell → book $9,931.21; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 173 | $7.24 | $2.55 | $-13.71 | $8,751.46 | ▼ -13.71 after sell → book $9,928.66; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SECZ` | 216 | $5.45 | $2.83 | $-89.86 | $9,925.83 | ▼ -89.86 after sell → book $9,925.83; vs 09:30 mark -2.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 83 | $14.94 | $2.24 | — | $8,683.57 | — | rank by cond; rank cond; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1240.73 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `VERI` | 1078 | $1.15 | $13.91 | — | $7,429.96 | — | rank by cond; rank cond; list yday_mover; ⚪; ret5=-12.2; leftover $1240.73 | join🟡 sector🟢 gen🟢 news🔴 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 26 | $46.18 | $2.07 | — | $6,227.22 | — | rank by cond; rank cond; list flatten; 🔵; ret5=+6.7; leftover $1240.73 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $5,083.04 | — | rank by cond; rank cond; list flatten; 🔵; ret5=+5.8; leftover $1240.73 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $3,864.83 | — | rank by cond; rank cond; list flatten; 🔵; ret5=+8.3; leftover $1240.73 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `AAOI` | 8 | $152.64 | $2.01 | — | $2,641.70 | — | rank by cond; rank cond; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+10.8; leftover $1240.73 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 136 | $9.12 | $2.40 | — | $1,398.98 | — | rank by cond; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1240.73 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 84 | $14.66 | $2.24 | — | $165.30 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1240.73 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $165.30 | ▼ close $9,850.35 vs 09:30 $9,942.52 (session -46.59) | 16:00 close · cash $165.30 · equity $9,850.35 vs 09:30 $9,942.52 (-92.17; session marks -46.59) · 8 name(s) marked open→close (per-name table). LPTH×83 09:30 $14.94 → close $14.80 -11.62; VERI×1078 09:30 $1.15 → close $1.08 -70.07; DVN×26 09:30 $46.18 → close $47.57 +36.14; EOG×8 09:30 $142.77 → close $146.15 +27.04; FANG×6 09:30 $202.70 → close $206.29 +21.54; AAOI×8 09:30 $152.64 → close $154.89 +18.00; ABX×136 09:30 $9.12 → close $9.12 +0.00; ALOY×84 09:30 $14.66 → close $13.86 -67.62 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $165.30 | ▼ 09:30 equity $9,651.57 vs yday $9,850.35 (-198.78) | 09:30 open · cash $165.30 (unchanged overnight, no fees) · equity $9,651.57 vs prior close $9,850.35 (-198.78) · 8 name(s) re-marked at the open (per-name table). LPTH×83 yday $14.80 → 09:30 $14.01 -65.57; VERI×1078 yday $1.08 → 09:30 $1.05 -37.73; DVN×26 yday $47.57 → 09:30 $48.00 +11.18; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; AAOI×8 yday $154.89 → 09:30 $146.20 -69.52; ABX×136 yday $9.12 → 09:30 $9.03 -12.24; ALOY×84 yday $13.86 → 09:30 $13.19 -55.86 | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 83 | $14.01 | $2.26 | $-81.69 | $1,325.87 | ▼ -81.69 after sell → book $9,649.31; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERI` | 1078 | $1.05 | $14.10 | $-135.80 | $2,443.67 | ▼ -135.80 after sell → book $9,635.21; vs 09:30 mark -14.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 26 | $48.00 | $2.09 | $+43.16 | $3,689.58 | ▲ +43.16 after sell → book $9,633.12; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $4,871.87 | ▲ +38.11 after sell → book $9,631.09; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $6,123.42 | ▲ +33.34 after sell → book $9,629.06; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `AAOI` | 8 | $146.20 | $2.03 | $-55.57 | $7,290.99 | ▼ -55.57 after sell → book $9,627.03; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 136 | $9.03 | $2.43 | $-17.07 | $8,516.64 | ▼ -17.07 after sell → book $9,624.60; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 84 | $13.19 | $2.27 | $-127.99 | $9,622.33 | ▼ -127.99 after sell → book $9,622.33; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,622.33 | ▲ close $9,622.33 vs 09:30 $9,651.57 (session +0.00) | 16:00 close · cash $9,622.33 · no lots left · equity $9,622.33. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,622.33 | ▲ 09:30 equity $9,622.33 vs yday $9,622.33 (+0.00) | 09:30 open · cash $9,622.33 · no holdings · equity $9,622.33 vs prior close $9,622.33 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,622.33 | ▲ close $9,622.33 vs 09:30 $9,622.33 (session +0.00) | 16:00 close · cash $9,622.33 · no lots left · equity $9,622.33. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,622.33 | ▲ 09:30 equity $9,622.33 vs yday $9,622.33 (+0.00) | 09:30 open · cash $9,622.33 · no holdings · equity $9,622.33 vs prior close $9,622.33 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,428.27 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1202.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,243.11 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1202.79 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 58 | $20.65 | $2.16 | — | $6,043.24 | — | rank by cond; rank cond; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1202.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 208 | $5.77 | $2.68 | — | $4,840.40 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1202.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $3,640.80 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1202.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $2,453.49 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1202.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 687 | $1.75 | $8.86 | — | $1,242.38 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1202.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $84.04 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1202.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $84.04 | ▲ close $9,825.14 vs 09:30 $9,622.33 (session +227.01) | 16:00 close · cash $84.04 · equity $9,825.14 vs 09:30 $9,622.33 (+202.81; session marks +227.01) · 8 name(s) marked open→close (per-name table). AG×58 09:30 $20.55 → close $21.19 +37.12; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×58 09:30 $20.65 → close $21.11 +26.68; HDSN×208 09:30 $5.77 → close $5.57 -41.60; IAG×61 09:30 $19.63 → close $20.50 +53.07; KGC×40 09:30 $29.63 → close $31.43 +72.00; NFGC×687 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $84.04 | ▲ 09:30 equity $10,084.96 vs yday $9,825.14 (+259.82) | 09:30 open · cash $84.04 (unchanged overnight, no fees) · equity $10,084.96 vs prior close $9,825.14 (+259.82) · 8 name(s) re-marked at the open (per-name table). AG×58 yday $21.19 → 09:30 $21.90 +41.18; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×58 yday $21.11 → 09:30 $21.75 +37.12; HDSN×208 yday $5.57 → 09:30 $5.67 +20.80; IAG×61 yday $20.50 → 09:30 $21.17 +40.87; KGC×40 yday $31.43 → 09:30 $32.17 +29.60; NFGC×687 yday $1.75 → 09:30 $1.79 +27.48; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 58 | $21.90 | $2.18 | $+73.95 | $1,352.06 | ▲ +73.95 after sell → book $10,082.78; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,594.37 | ▲ +57.15 after sell → book $10,080.73; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 58 | $21.75 | $2.18 | $+59.45 | $3,853.69 | ▲ +59.45 after sell → book $10,078.55; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 208 | $5.67 | $2.73 | $-26.21 | $5,030.32 | ▼ -26.21 after sell → book $10,075.82; vs 09:30 mark -2.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 61 | $21.17 | $2.19 | $+89.57 | $6,319.49 | ▲ +89.57 after sell → book $10,073.62; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $7,604.16 | ▲ +97.36 after sell → book $10,071.49; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 687 | $1.79 | $8.99 | $+9.63 | $8,824.91 | ▲ +9.63 after sell → book $10,062.51; vs 09:30 mark -8.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,060.47 | ▲ +77.23 after sell → book $10,060.47; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,864.15 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1257.56 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 73 | $17.20 | $2.21 | — | $7,606.34 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1257.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,522.84 | — | rank by cond; rank cond; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1257.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 112 | $11.13 | $2.33 | — | $5,273.95 | — | rank by cond; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1257.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 509 | $2.47 | $6.57 | — | $4,010.16 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1257.56 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 651 | $1.93 | $8.40 | — | $2,745.33 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1257.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,489.16 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1257.56 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 952 | $1.32 | $12.28 | — | $220.24 | — | rank by cond; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1257.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $220.24 | ▲ close $10,273.48 vs 09:30 $10,084.96 (session +250.86) | 16:00 close · cash $220.24 · equity $10,273.48 vs 09:30 $10,084.96 (+188.52; session marks +250.86) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×73 09:30 $17.20 → close $16.65 -40.15; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×112 09:30 $11.13 → close $13.45 +259.84; AUTL×509 09:30 $2.47 → close $2.41 -30.54; CRDL×651 09:30 $1.93 → close $1.86 -45.57; CRSP×21 09:30 $59.72 → close $59.50 -4.62; CYPH×952 09:30 $1.32 → close $1.42 +95.20 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $220.24 | ▲ 09:30 equity $10,634.45 vs yday $10,273.48 (+360.97) | 09:30 open · cash $220.24 (unchanged overnight, no fees) · equity $10,634.45 vs prior close $10,273.48 (+360.97) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×73 yday $16.65 → 09:30 $16.57 -5.84; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×112 yday $13.45 → 09:30 $13.33 -13.44; AUTL×509 yday $2.41 → 09:30 $2.40 -5.09; CRDL×651 yday $1.86 → 09:30 $1.88 +13.02; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; CYPH×952 yday $1.42 → 09:30 $1.83 +390.32 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,423.30 | ▲ +6.74 after sell → book $10,632.41; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 73 | $16.57 | $2.23 | $-50.43 | $2,630.67 | ▼ -50.43 after sell → book $10,630.17; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,713.80 | ▼ -0.38 after sell → book $10,628.15; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 112 | $13.33 | $2.36 | $+241.72 | $5,204.40 | ▲ +241.72 after sell → book $10,625.79; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 509 | $2.40 | $6.66 | $-48.86 | $6,419.34 | ▼ -48.86 after sell → book $10,619.13; vs 09:30 mark -6.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 651 | $1.88 | $8.52 | $-49.46 | $7,634.71 | ▼ -49.46 after sell → book $10,610.62; vs 09:30 mark -8.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 952 | $1.83 | $12.45 | $+460.79 | $9,364.41 | ▲ +460.79 after sell → book $10,598.16; vs 09:30 mark -12.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,364.41 | ▼ close $10,562.99 vs 09:30 $10,634.45 (session -35.17) | 16:00 close · cash $9,364.41 · equity $10,562.99 vs 09:30 $10,634.45 (-71.46; session marks -35.17) · 1 name(s) marked open→close (per-name table). CRSP×21 09:30 $58.75 → close $57.08 -35.17 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,364.41 | ▲ 09:30 equity $10,580.94 vs yday $10,562.99 (+17.95) | 09:30 open · cash $9,364.41 (unchanged overnight, no fees) · equity $10,580.94 vs prior close $10,562.99 (+17.95) · 1 name(s) re-marked at the open (per-name table). CRSP×21 yday $57.08 → 09:30 $57.93 +17.95 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-41.72 | $10,578.87 | ▼ -41.72 after sell → book $10,578.87; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 11 | $118.52 | $2.02 | — | $9,273.13 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1322.36 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 34 | $38.01 | $2.09 | — | $7,978.70 | — | rank by cond; rank cond; list mover_buy; ⚪; ret5=+10.4; leftover $1322.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 17 | $77.13 | $2.04 | — | $6,665.44 | — | rank by cond; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1322.36 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CNH` | 111 | $11.90 | $2.32 | — | $5,342.22 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1322.36 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HMY` | 59 | $22.41 | $2.17 | — | $4,017.86 | — | rank by cond; rank cond; list mover_buy; ⚪; ret5=+13.9; leftover $1322.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $23.77 | $2.15 | — | $2,708.36 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+13.0; leftover $1322.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 30 | $43.76 | $2.08 | — | $1,393.48 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $1322.36 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 147 | $8.98 | $2.43 | — | $70.99 | — | rank by cond; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; leftover $1322.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $70.99 | ▲ close $10,806.82 vs 09:30 $10,580.94 (session +245.26) | 16:00 close · cash $70.99 · equity $10,806.82 vs 09:30 $10,580.94 (+225.88; session marks +245.26) · 8 name(s) marked open→close (per-name table). AU×11 09:30 $118.52 → close $123.39 +53.57; ERO×34 09:30 $38.01 → close $40.40 +81.26; FCX×17 09:30 $77.13 → close $79.91 +47.26; CNH×111 09:30 $11.90 → close $11.56 -37.74; HMY×59 09:30 $22.41 → close $22.95 +31.86; MOS×55 09:30 $23.77 → close $24.27 +27.50; RHI×30 09:30 $43.76 → close $44.90 +34.20; SUZ×147 09:30 $8.98 → close $9.03 +7.35 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $70.99 | ▼ 09:30 equity $10,740.37 vs yday $10,806.82 (-66.45) | 09:30 open · cash $70.99 (unchanged overnight, no fees) · equity $10,740.37 vs prior close $10,806.82 (-66.45) · 8 name(s) re-marked at the open (per-name table). AU×11 yday $123.39 → 09:30 $119.80 -39.49; ERO×34 yday $40.40 → 09:30 $40.51 +3.74; FCX×17 yday $79.91 → 09:30 $79.34 -9.69; CNH×111 yday $11.56 → 09:30 $11.54 -2.22; HMY×59 yday $22.95 → 09:30 $22.39 -33.04; MOS×55 yday $24.27 → 09:30 $24.84 +31.35; RHI×30 yday $44.90 → 09:30 $44.33 -17.10; SUZ×147 yday $9.03 → 09:30 $9.03 +0.00 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 11 | $119.80 | $2.04 | $+10.01 | $1,386.74 | ▲ +10.01 after sell → book $10,738.32; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ERO` | 34 | $40.51 | $2.11 | $+80.79 | $2,761.97 | ▲ +80.79 after sell → book $10,736.21; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 17 | $79.34 | $2.06 | $+33.47 | $4,108.69 | ▲ +33.47 after sell → book $10,734.15; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CNH` | 111 | $11.54 | $2.35 | $-44.63 | $5,387.28 | ▼ -44.63 after sell → book $10,731.80; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `HMY` | 59 | $22.39 | $2.19 | $-5.53 | $6,706.10 | ▼ -5.53 after sell → book $10,729.61; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 30 | $44.33 | $2.10 | $+12.92 | $8,033.90 | ▲ +12.92 after sell → book $10,727.51; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 147 | $9.03 | $2.47 | $+2.45 | $9,358.84 | ▲ +2.45 after sell → book $10,725.04; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 5 | $267.02 | $2.00 | — | $8,021.74 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $1336.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FIGR` | 33 | $40.50 | $2.09 | — | $6,683.15 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+15.8; leftover $1336.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `FUTU` | 10 | $124.67 | $2.02 | — | $5,434.43 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+15.7; leftover $1336.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 2293 | $0.58 | $20.25 | — | $4,077.36 | — | rank by cond; rank cond; list yday_mover; 🔵; ret5=-27.5; leftover $1336.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 256 | $5.21 | $3.30 | — | $2,740.30 | — | rank by cond; rank cond; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $1336.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BTG` | 232 | $5.75 | $2.99 | — | $1,403.31 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+17.9; leftover $1336.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NEXA` | 85 | $15.64 | $2.25 | — | $71.66 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+17.3; leftover $1336.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.66 | ▼ close $10,549.24 vs 09:30 $10,740.37 (session -140.90) | 16:00 close · cash $71.66 · equity $10,549.24 vs 09:30 $10,740.37 (-191.13; session marks -140.90) · 8 name(s) marked open→close (per-name table). MOS×55 09:30 $24.84 → close $24.16 -37.40; FNV×5 09:30 $267.02 → close $267.37 +1.75; FIGR×33 09:30 $40.50 → close $37.08 -112.86; FUTU×10 09:30 $124.67 → close $127.34 +26.70; SLQT×2293 09:30 $0.58 → close $0.55 -75.67; TIGR×256 09:30 $5.21 → close $5.46 +64.00; BTG×232 09:30 $5.75 → close $5.74 -2.32; NEXA×85 09:30 $15.64 → close $15.58 -5.10 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.66 | ▼ 09:30 equity $10,459.26 vs yday $10,549.24 (-89.98) | 09:30 open · cash $71.66 (unchanged overnight, no fees) · equity $10,459.26 vs prior close $10,549.24 (-89.98) · 8 name(s) re-marked at the open (per-name table). MOS×55 yday $24.16 → 09:30 $24.00 -8.80; FNV×5 yday $267.37 → 09:30 $267.23 -0.70; FIGR×33 yday $37.08 → 09:30 $37.42 +11.22; FUTU×10 yday $127.34 → 09:30 $128.00 +6.60; SLQT×2293 yday $0.55 → 09:30 $0.53 -45.86; TIGR×256 yday $5.46 → 09:30 $5.49 +7.68; BTG×232 yday $5.74 → 09:30 $5.73 -2.32; NEXA×85 yday $15.58 → 09:30 $14.90 -57.80 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 5 | $267.23 | $2.03 | $-2.98 | $1,405.79 | ▼ -2.98 after sell → book $10,457.24; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FIGR` | 33 | $37.42 | $2.11 | $-105.84 | $2,638.54 | ▼ -105.84 after sell → book $10,455.13; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FUTU` | 10 | $128.00 | $2.04 | $+29.24 | $3,916.50 | ▲ +29.24 after sell → book $10,453.09; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 2293 | $0.53 | $19.42 | $-161.20 | $5,112.37 | ▼ -161.20 after sell → book $10,433.67; vs 09:30 mark -19.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 256 | $5.49 | $3.36 | $+65.02 | $6,514.45 | ▲ +65.02 after sell → book $10,430.31; vs 09:30 mark -3.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTG` | 232 | $5.73 | $3.04 | $-10.67 | $7,840.77 | ▼ -10.67 after sell → book $10,427.27; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NEXA` | 85 | $14.90 | $2.27 | $-67.41 | $9,105.00 | ▼ -67.41 after sell → book $10,425.00; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 15 | $81.65 | $2.04 | — | $7,878.21 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1300.71 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 284 | $4.57 | $3.66 | — | $6,576.67 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+1.1; leftover $1300.71 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 17 | $74.54 | $2.04 | — | $5,307.45 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=-0.1; leftover $1300.71 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $4,338.45 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1300.71 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 500 | $2.60 | $6.45 | — | $3,032.00 | — | rank by cond; rank cond; list flatten; ret5=+13.0; leftover $1300.71 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 23 | $55.25 | $2.06 | — | $1,759.19 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+2.1; leftover $1300.71 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $205.90 | $2.01 | — | $521.78 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+8.5; leftover $1300.71 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $521.78 | ▼ close $10,385.46 vs 09:30 $10,459.26 (session -19.29) | 16:00 close · cash $521.78 · equity $10,385.46 vs 09:30 $10,459.26 (-73.80; session marks -19.29) · 8 name(s) marked open→close (per-name table). MOS×55 09:30 $24.00 → close $23.76 -13.20; ACMR×15 09:30 $81.65 → close $80.49 -17.40; GGB×284 09:30 $4.57 → close $4.70 +36.92; MT×17 09:30 $74.54 → close $74.63 +1.53; MU×1 09:30 $967.01 → close $935.39 -31.62; SLI×500 09:30 $2.60 → close $2.64 +20.00; TX×23 09:30 $55.25 → close $55.83 +13.34; ANET×6 09:30 $205.90 → close $201.09 -28.86 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $521.78 | ▼ 09:30 equity $10,382.59 vs yday $10,385.46 (-2.87) | 09:30 open · cash $521.78 (unchanged overnight, no fees) · equity $10,382.59 vs prior close $10,385.46 (-2.87) · 8 name(s) re-marked at the open (per-name table). MOS×55 yday $23.76 → 09:30 $23.95 +10.45; ACMR×15 yday $80.49 → 09:30 $79.27 -18.30; GGB×284 yday $4.70 → 09:30 $4.67 -8.52; MT×17 yday $74.63 → 09:30 $75.39 +12.92; MU×1 yday $935.39 → 09:30 $919.29 -16.10; SLI×500 yday $2.64 → 09:30 $2.68 +20.00; TX×23 yday $55.83 → 09:30 $55.97 +3.22; ANET×6 yday $201.09 → 09:30 $200.00 -6.54 | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 55 | $23.95 | $2.18 | $+5.57 | $1,836.85 | ▲ +5.57 after sell → book $10,380.41; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 15 | $79.27 | $2.06 | $-39.79 | $3,023.85 | ▼ -39.79 after sell → book $10,378.36; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 284 | $4.67 | $3.72 | $+21.01 | $4,346.41 | ▲ +21.01 after sell → book $10,374.64; vs 09:30 mark -3.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 17 | $75.39 | $2.06 | $+10.35 | $5,625.98 | ▲ +10.35 after sell → book $10,372.58; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $6,543.25 | ▼ -51.73 after sell → book $10,370.56; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 500 | $2.68 | $6.54 | $+27.01 | $7,876.71 | ▲ +27.01 after sell → book $10,364.02; vs 09:30 mark -6.54 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 23 | $55.97 | $2.08 | $+12.42 | $9,161.94 | ▲ +12.42 after sell → book $10,361.94; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $200.00 | $2.03 | $-39.44 | $10,359.91 | ▼ -39.44 after sell → book $10,359.91; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $9,384.68 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1294.99 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $8,106.83 | — | rank by cond; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1294.99 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,903.57 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1294.99 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.49 | $2.03 | — | $5,620.68 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1294.99 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 20 | $62.82 | $2.05 | — | $4,362.23 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1294.99 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $3,202.46 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1294.99 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 10 | $119.76 | $2.02 | — | $2,002.84 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1294.99 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,002.84 | ▼ close $9,989.42 vs 09:30 $10,382.59 (session -356.37) | 16:00 close · cash $2,002.84 · equity $9,989.42 vs 09:30 $10,382.59 (-393.17; session marks -356.37) · 7 name(s) marked open→close (per-name table). KEYS×3 09:30 $324.41 → close $319.97 -13.32; SMTC×9 09:30 $141.76 → close $131.17 -95.31; CIEN×3 09:30 $400.42 → close $378.44 -65.94; AVT×14 09:30 $91.49 → close $88.63 -40.04; CGNX×20 09:30 $62.82 → close $60.46 -47.20; COHR×4 09:30 $289.44 → close $279.20 -40.96; LSCC×10 09:30 $119.76 → close $114.40 -53.60 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,002.84 | ▲ 09:30 equity $10,033.59 vs yday $9,989.42 (+44.17) | 09:30 open · cash $2,002.84 (unchanged overnight, no fees) · equity $10,033.59 vs prior close $9,989.42 (+44.17) · 7 name(s) re-marked at the open (per-name table). KEYS×3 yday $319.97 → 09:30 $322.49 +7.56; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; AVT×14 yday $88.63 → 09:30 $89.39 +10.64; CGNX×20 yday $60.46 → 09:30 $60.46 +0.00; COHR×4 yday $279.20 → 09:30 $280.25 +4.20; LSCC×10 yday $114.40 → 09:30 $115.56 +11.60 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 3 | $322.49 | $2.02 | $-9.78 | $2,968.29 | ▼ -9.78 after sell → book $10,031.57; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $4,156.96 | ▼ -89.19 after sell → book $10,029.54; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $5,290.26 | ▼ -69.96 after sell → book $10,027.52; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 14 | $89.39 | $2.05 | $-33.48 | $6,539.67 | ▼ -33.48 after sell → book $10,025.47; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 20 | $60.46 | $2.07 | $-51.32 | $7,746.80 | ▼ -51.32 after sell → book $10,023.40; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $280.25 | $2.02 | $-40.78 | $8,865.77 | ▼ -40.78 after sell → book $10,021.37; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 10 | $115.56 | $2.04 | $-46.06 | $10,019.33 | ▼ -46.06 after sell → book $10,019.33; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,019.33 | ▲ close $10,019.33 vs 09:30 $10,033.59 (session +0.00) | 16:00 close · cash $10,019.33 · no lots left · equity $10,019.33. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,019.33 | ▲ 09:30 equity $10,019.33 vs yday $10,019.33 (+0.00) | 09:30 open · cash $10,019.33 · no holdings · equity $10,019.33 vs prior close $10,019.33 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,019.33 | ▲ close $10,019.33 vs 09:30 $10,019.33 (session +0.00) | 16:00 close · cash $10,019.33 · no lots left · equity $10,019.33. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,019.33 | ▲ 09:30 equity $10,019.33 vs yday $10,019.33 (+0.00) | 09:30 open · cash $10,019.33 · no holdings · equity $10,019.33 vs prior close $10,019.33 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,019.33 | ▲ close $10,019.33 vs 09:30 $10,019.33 (session +0.00) | 16:00 close · cash $10,019.33 · no lots left · equity $10,019.33. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,019.33 | ▲ 09:30 equity $10,019.33 vs yday $10,019.33 (+0.00) | 09:30 open · cash $10,019.33 · no holdings · equity $10,019.33 vs prior close $10,019.33 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 74 | $16.77 | $2.21 | — | $8,776.14 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1252.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 648 | $1.93 | $8.36 | — | $7,517.14 | — | rank by cond; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1252.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 574 | $2.18 | $7.40 | — | $6,258.42 | — | rank by cond; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1252.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $5,011.37 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1252.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 120 | $10.42 | $2.35 | — | $3,758.62 | — | rank by cond; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1252.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PBH` | 23 | $53.45 | $2.06 | — | $2,527.21 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.3; leftover $1252.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `PCRX` | 46 | $26.74 | $2.13 | — | $1,295.04 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.6; leftover $1252.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $100.98 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1252.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $100.98 | ▼ close $9,792.83 vs 09:30 $10,019.33 (session -197.90) | 16:00 close · cash $100.98 · equity $9,792.83 vs 09:30 $10,019.33 (-226.50; session marks -197.90) · 8 name(s) marked open→close (per-name table). ARCT×74 09:30 $16.77 → close $15.56 -89.54; BMEA×648 09:30 $1.93 → close $1.91 -12.96; CRDL×574 09:30 $2.18 → close $2.16 -11.48; HRMY×29 09:30 $42.93 → close $41.86 -31.03; NVAX×120 09:30 $10.42 → close $10.34 -9.60; PBH×23 09:30 $53.45 → close $52.56 -20.47; PCRX×46 09:30 $26.74 → close $26.60 -6.44; RVTY×9 09:30 $132.45 → close $130.63 -16.38 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $100.98 | ▼ 09:30 equity $9,765.81 vs yday $9,792.83 (-27.02) | 09:30 open · cash $100.98 (unchanged overnight, no fees) · equity $9,765.81 vs prior close $9,792.83 (-27.02) · 8 name(s) re-marked at the open (per-name table). ARCT×74 yday $15.56 → 09:30 $15.61 +3.70; BMEA×648 yday $1.91 → 09:30 $1.90 -6.48; CRDL×574 yday $2.16 → 09:30 $2.16 +0.00; HRMY×29 yday $41.86 → 09:30 $41.50 -10.44; NVAX×120 yday $10.34 → 09:30 $10.50 +19.20; PBH×23 yday $52.56 → 09:30 $51.80 -17.48; PCRX×46 yday $26.60 → 09:30 $26.38 -10.12; RVTY×9 yday $130.63 → 09:30 $130.03 -5.40 | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 74 | $15.61 | $2.23 | $-90.29 | $1,253.88 | ▼ -90.29 after sell → book $9,763.57; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 574 | $2.16 | $7.51 | $-26.39 | $2,486.21 | ▼ -26.39 after sell → book $9,756.06; vs 09:30 mark -7.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 120 | $10.50 | $2.38 | $+4.87 | $3,743.83 | ▲ +4.87 after sell → book $9,753.68; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBH` | 23 | $51.80 | $2.08 | $-42.09 | $4,933.15 | ▼ -42.09 after sell → book $9,751.60; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PCRX` | 46 | $26.38 | $2.15 | $-20.84 | $6,144.49 | ▼ -20.84 after sell → book $9,749.46; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $7,312.72 | ▼ -25.83 after sell → book $9,747.42; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 352 | $3.46 | $4.54 | — | $6,090.26 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1218.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 483 | $2.52 | $6.23 | — | $4,866.87 | — | rank by cond; rank cond; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1218.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 23 | $52.03 | $2.06 | — | $3,668.12 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $1218.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 181 | $6.71 | $2.53 | — | $2,451.08 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1218.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $1,395.63 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1218.79 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 43 | $28.00 | $2.12 | — | $189.51 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+8.7; leftover $1218.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $189.51 | ▲ close $9,762.09 vs 09:30 $9,765.81 (session +34.16) | 16:00 close · cash $189.51 · equity $9,762.09 vs 09:30 $9,765.81 (-3.72; session marks +34.16) · 8 name(s) marked open→close (per-name table). BMEA×648 09:30 $1.90 → close $2.03 +84.24; HRMY×29 09:30 $41.50 → close $42.25 +21.75; CABA×352 09:30 $3.46 → close $3.47 +3.52; ALEC×483 09:30 $2.52 → close $2.46 -28.98; ATRC×23 09:30 $52.03 → close $51.52 -11.73; BHC×181 09:30 $6.71 → close $6.56 -27.15; CRM×4 09:30 $263.36 → close $259.23 -16.52; MLYS×43 09:30 $28.00 → close $28.21 +9.03 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $189.51 | ▼ 09:30 equity $9,724.68 vs yday $9,762.09 (-37.41) | 09:30 open · cash $189.51 (unchanged overnight, no fees) · equity $9,724.68 vs prior close $9,762.09 (-37.41) · 8 name(s) re-marked at the open (per-name table). BMEA×648 yday $2.03 → 09:30 $2.00 -19.44; HRMY×29 yday $42.25 → 09:30 $42.20 -1.45; CABA×352 yday $3.47 → 09:30 $3.43 -14.08; ALEC×483 yday $2.46 → 09:30 $2.38 -38.64; ATRC×23 yday $51.52 → 09:30 $54.31 +64.17; BHC×181 yday $6.56 → 09:30 $6.57 +1.81; CRM×4 yday $259.23 → 09:30 $253.72 -22.04; MLYS×43 yday $28.21 → 09:30 $28.03 -7.74 | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 648 | $2.00 | $8.48 | $+28.52 | $1,477.04 | ▲ +28.52 after sell → book $9,716.21; vs 09:30 mark -8.47 | dropped from list after 2 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `HRMY` | 29 | $42.20 | $2.10 | $-25.34 | $2,698.74 | ▼ -25.34 after sell → book $9,714.11; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 352 | $3.43 | $4.61 | $-19.71 | $3,901.49 | ▼ -19.71 after sell → book $9,709.50; vs 09:30 mark -4.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 483 | $2.38 | $6.32 | $-80.17 | $5,044.71 | ▼ -80.17 after sell → book $9,703.18; vs 09:30 mark -6.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 23 | $54.31 | $2.08 | $+48.30 | $6,291.76 | ▲ +48.30 after sell → book $9,701.10; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 181 | $6.57 | $2.57 | $-30.45 | $7,478.36 | ▼ -30.45 after sell → book $9,698.53; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $8,491.22 | ▼ -42.58 after sell → book $9,696.51; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MLYS` | 43 | $28.03 | $2.14 | $-2.97 | $9,694.37 | ▼ -2.97 after sell → book $9,694.37; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,694.37 | ▲ close $9,694.37 vs 09:30 $9,724.68 (session +0.00) | 16:00 close · cash $9,694.37 · no lots left · equity $9,694.37. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,694.37 | ▲ 09:30 equity $9,694.37 vs yday $9,694.37 (-0.00) | 09:30 open · cash $9,694.37 · no holdings · equity $9,694.37 vs prior close $9,694.37 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,694.37 | ▲ close $9,694.37 vs 09:30 $9,694.37 (session +0.00) | 16:00 close · cash $9,694.37 · no lots left · equity $9,694.37. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,694.37 | ▲ 09:30 equity $9,694.37 vs yday $9,694.37 (-0.00) | 09:30 open · cash $9,694.37 · no holdings · equity $9,694.37 vs prior close $9,694.37 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,694.37 | ▲ close $9,694.37 vs 09:30 $9,694.37 (session +0.00) | 16:00 close · cash $9,694.37 · no lots left · equity $9,694.37. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,694.37 | ▲ 09:30 equity $9,694.37 vs yday $9,694.37 (-0.00) | 09:30 open · cash $9,694.37 · no holdings · equity $9,694.37 vs prior close $9,694.37 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `HPE` | 21 | $56.37 | $2.05 | — | $8,508.54 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+15.8; leftover $1211.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SEDG` | 32 | $36.78 | $2.09 | — | $7,329.50 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+13.1; leftover $1211.80 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,329.50 | ▲ close $9,743.15 vs 09:30 $9,694.37 (session +52.92) | 16:00 close · cash $7,329.50 · equity $9,743.15 vs 09:30 $9,694.37 (+48.78; session marks +52.92) · 2 name(s) marked open→close (per-name table). HPE×21 09:30 $56.37 → close $62.09 +120.12; SEDG×32 09:30 $36.78 → close $34.68 -67.20 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1263.20 < 1 share @ 1646.93 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OCUL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PLX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `RLX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
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
| 2026-08-28 | `MPWR` | cash | leftover split 1294.99 < 1 share @ 1306.03 |
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
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SKHY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DOCN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `CVI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INTC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `MXL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SWKS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ARBE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `ORCL` | no_price | no 09:30 open |
| 2026-09-11 | `SWKS` | no_price | no 09:30 open |
| 2026-09-11 | `QRVO` | no_price | no 09:30 open |
| 2026-09-11 | `BAND` | no_price | no 09:30 open |
| 2026-09-11 | `COHU` | no_price | no 09:30 open |
| 2026-09-11 | `DOCN` | no_price | no 09:30 open |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `HPE` | 21 | 2026-09-11 @ $56.37 | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+15.8; leftover $1211.80 |
| `SEDG` | 32 | 2026-09-11 @ $36.78 | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+13.1; leftover $1211.80 |
