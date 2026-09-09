# Factor mine action — `union_cond_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · rank by cond

Cash book **-5.22%** ($9,478) · signal-only (no cash/fees) was -5.51%. Starts YES **3/18**. Fills 166 · skips 56 · realized $-521.82.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,478.15.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 20 | — | $59.80 | +0.00 | $60.23 | +8.60 | +8.60 | +0.00 | +8.60 |
| 2026-08-13 | `HIMS` | 42 | — | $29.74 | +0.00 | $28.77 | -40.74 | -40.74 | +0.00 | -40.74 |
| 2026-08-13 | `INO` | 1543 | — | $0.81 | +0.00 | $0.90 | +138.87 | +138.87 | +0.00 | +138.87 |
| 2026-08-13 | `IREN` | 27 | — | $45.98 | +0.00 | $44.76 | -32.94 | -32.94 | +0.00 | -32.94 |
| 2026-08-13 | `SLS` | 106 | — | $11.70 | +0.00 | $12.36 | +69.96 | +69.96 | +0.00 | +69.96 |
| 2026-08-13 | `TGTX` | 25 | — | $49.70 | +0.00 | $47.94 | -44.00 | -44.00 | +0.00 | -44.00 |
| 2026-08-13 | `TNDM` | 53 | — | $23.33 | +0.00 | $23.13 | -10.60 | -10.60 | +0.00 | -10.60 |
| 2026-08-13 | `TPG` | 24 | — | $50.62 | +0.00 | $54.62 | +95.92 | +95.92 | +0.00 | +95.92 |
| 2026-08-14 | `BTSG` | 20 | $60.23 | $59.65 | -11.60 | — | +0.00 | -11.60 | -3.00 | — |
| 2026-08-14 | `HIMS` | 42 | $28.77 | $29.15 | +15.96 | — | +0.00 | +15.96 | -24.78 | — |
| 2026-08-14 | `INO` | 1543 | $0.90 | $0.93 | +46.29 | — | +0.00 | +46.29 | +185.16 | — |
| 2026-08-14 | `IREN` | 27 | $44.76 | $44.09 | -18.09 | — | +0.00 | -18.09 | -51.03 | — |
| 2026-08-14 | `SLS` | 106 | $12.36 | $12.40 | +4.24 | — | +0.00 | +4.24 | +74.20 | — |
| 2026-08-14 | `TGTX` | 25 | $47.94 | $47.27 | -16.75 | — | +0.00 | -16.75 | -60.75 | — |
| 2026-08-14 | `TNDM` | 53 | $23.13 | $22.92 | -11.13 | — | +0.00 | -11.13 | -21.73 | — |
| 2026-08-14 | `TPG` | 24 | $54.62 | $55.29 | +16.08 | — | +0.00 | +16.08 | +112.00 | — |
| 2026-08-14 | `ARX` | 64 | — | $19.57 | +0.00 | $19.58 | +0.64 | +0.64 | +0.00 | +0.64 |
| 2026-08-14 | `BETR` | 85 | — | $14.80 | +0.00 | $13.73 | -90.95 | -90.95 | +0.00 | -90.95 |
| 2026-08-14 | `BTBT` | 845 | — | $1.50 | +0.00 | $1.57 | +59.15 | +59.15 | +0.00 | +59.15 |
| 2026-08-14 | `FIGR` | 39 | — | $32.12 | +0.00 | $31.43 | -26.91 | -26.91 | +0.00 | -26.91 |
| 2026-08-14 | `ADUR` | 76 | — | $16.50 | +0.00 | $16.17 | -25.08 | -25.08 | +0.00 | -25.08 |
| 2026-08-14 | `AIRO` | 114 | — | $11.12 | +0.00 | $9.57 | -176.70 | -176.70 | +0.00 | -176.70 |
| 2026-08-14 | `AMPY` | 256 | — | $4.94 | +0.00 | $4.78 | -40.96 | -40.96 | +0.00 | -40.96 |
| 2026-08-14 | `ANGX` | 294 | — | $4.31 | +0.00 | $4.37 | +17.64 | +17.64 | +0.00 | +17.64 |
| 2026-08-17 | `ARX` | 64 | $19.58 | $19.57 | -0.64 | — | +0.00 | -0.64 | +0.00 | — |
| 2026-08-17 | `BETR` | 85 | $13.73 | $13.67 | -5.10 | — | +0.00 | -5.10 | -96.05 | — |
| 2026-08-17 | `BTBT` | 845 | $1.57 | $1.52 | -42.25 | — | +0.00 | -42.25 | +16.90 | — |
| 2026-08-17 | `FIGR` | 39 | $31.43 | $32.16 | +28.47 | — | +0.00 | +28.47 | +1.56 | — |
| 2026-08-17 | `ADUR` | 76 | $16.17 | $15.73 | -33.44 | — | +0.00 | -33.44 | -58.52 | — |
| 2026-08-17 | `AIRO` | 114 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -176.70 | — |
| 2026-08-17 | `AMPY` | 256 | $4.78 | $4.86 | +20.48 | — | +0.00 | +20.48 | -20.48 | — |
| 2026-08-17 | `ANGX` | 294 | $4.37 | $4.60 | +67.62 | — | +0.00 | +67.62 | +85.26 | — |
| 2026-08-17 | `ABX` | 134 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `INV` | 759 | — | $1.62 | +0.00 | $1.39 | -178.37 | -178.37 | +0.00 | -178.37 |
| 2026-08-17 | `NU` | 79 | — | $15.40 | +0.00 | $14.74 | -52.14 | -52.14 | +0.00 | -52.14 |
| 2026-08-17 | `XHG` | 293 | — | $4.19 | +0.00 | $3.91 | -82.04 | -82.04 | +0.00 | -82.04 |
| 2026-08-17 | `DVN` | 26 | — | $46.18 | +0.00 | $47.57 | +36.14 | +36.14 | +0.00 | +36.14 |
| 2026-08-17 | `EOG` | 8 | — | $142.77 | +0.00 | $146.15 | +27.04 | +27.04 | +0.00 | +27.04 |
| 2026-08-17 | `FANG` | 6 | — | $202.70 | +0.00 | $206.29 | +21.54 | +21.54 | +0.00 | +21.54 |
| 2026-08-17 | `ALOY` | 83 | — | $14.66 | +0.00 | $13.86 | -66.81 | -66.81 | +0.00 | -66.81 |
| 2026-08-18 | `ABX` | 134 | $9.12 | $9.03 | -12.06 | — | +0.00 | -12.06 | -12.06 | — |
| 2026-08-18 | `INV` | 759 | $1.39 | $1.32 | -45.54 | — | +0.00 | -45.54 | -223.91 | — |
| 2026-08-18 | `NU` | 79 | $14.74 | $14.53 | -16.59 | — | +0.00 | -16.59 | -68.73 | — |
| 2026-08-18 | `XHG` | 293 | $3.91 | $3.94 | +8.79 | — | +0.00 | +8.79 | -73.25 | — |
| 2026-08-18 | `DVN` | 26 | $47.57 | $48.00 | +11.18 | — | +0.00 | +11.18 | +47.32 | — |
| 2026-08-18 | `EOG` | 8 | $146.15 | $148.04 | +15.12 | — | +0.00 | +15.12 | +42.16 | — |
| 2026-08-18 | `FANG` | 6 | $206.29 | $208.93 | +15.84 | — | +0.00 | +15.84 | +37.38 | — |
| 2026-08-18 | `ALOY` | 83 | $13.86 | $13.19 | -55.20 | — | +0.00 | -55.20 | -122.01 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 57 | — | $20.55 | +0.00 | $21.19 | +36.48 | +36.48 | +0.00 | +36.48 |
| 2026-08-20 | `BHP` | 12 | — | $91.01 | +0.00 | $93.63 | +31.44 | +31.44 | +0.00 | +31.44 |
| 2026-08-20 | `CDE` | 56 | — | $20.65 | +0.00 | $21.11 | +25.76 | +25.76 | +0.00 | +25.76 |
| 2026-08-20 | `HDSN` | 203 | — | $5.77 | +0.00 | $5.57 | -40.60 | -40.60 | +0.00 | -40.60 |
| 2026-08-20 | `IAG` | 59 | — | $19.63 | +0.00 | $20.50 | +51.33 | +51.33 | +0.00 | +51.33 |
| 2026-08-20 | `KGC` | 39 | — | $29.63 | +0.00 | $31.43 | +70.20 | +70.20 | +0.00 | +70.20 |
| 2026-08-20 | `NFGC` | 672 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 57 | $21.19 | $21.90 | +40.47 | — | +0.00 | +40.47 | +76.95 | — |
| 2026-08-21 | `BHP` | 12 | $93.63 | $95.72 | +25.08 | — | +0.00 | +25.08 | +56.52 | — |
| 2026-08-21 | `CDE` | 56 | $21.11 | $21.75 | +35.84 | — | +0.00 | +35.84 | +61.60 | — |
| 2026-08-21 | `HDSN` | 203 | $5.57 | $5.67 | +20.30 | — | +0.00 | +20.30 | -20.30 | — |
| 2026-08-21 | `IAG` | 59 | $20.50 | $21.17 | +39.53 | — | +0.00 | +39.53 | +90.86 | — |
| 2026-08-21 | `KGC` | 39 | $31.43 | $32.17 | +28.86 | — | +0.00 | +28.86 | +99.06 | — |
| 2026-08-21 | `NFGC` | 672 | $1.75 | $1.79 | +26.88 | — | +0.00 | +26.88 | +26.88 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 71 | — | $17.20 | +0.00 | $16.65 | -39.05 | -39.05 | +0.00 | -39.05 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 110 | — | $11.13 | +0.00 | $13.45 | +255.20 | +255.20 | +0.00 | +255.20 |
| 2026-08-21 | `AUTL` | 497 | — | $2.47 | +0.00 | $2.41 | -29.82 | -29.82 | +0.00 | -29.82 |
| 2026-08-21 | `CRDL` | 637 | — | $1.93 | +0.00 | $1.86 | -44.59 | -44.59 | +0.00 | -44.59 |
| 2026-08-21 | `CRSP` | 20 | — | $59.72 | +0.00 | $59.50 | -4.40 | -4.40 | +0.00 | -4.40 |
| 2026-08-21 | `CYPH` | 931 | — | $1.32 | +0.00 | $1.42 | +93.10 | +93.10 | +0.00 | +93.10 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUPH` | 71 | $16.65 | $16.57 | -5.68 | — | +0.00 | -5.68 | -44.73 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 110 | $13.45 | $13.33 | -13.20 | — | +0.00 | -13.20 | +242.00 | — |
| 2026-08-24 | `AUTL` | 497 | $2.41 | $2.40 | -4.97 | — | +0.00 | -4.97 | -34.79 | — |
| 2026-08-24 | `CRDL` | 637 | $1.86 | $1.88 | +12.74 | — | +0.00 | +12.74 | -31.85 | — |
| 2026-08-24 | `CRSP` | 20 | $59.50 | $58.75 | -15.00 | $57.08 | -33.50 | -48.50 | -19.40 | -52.90 |
| 2026-08-24 | `CYPH` | 931 | $1.42 | $1.83 | +381.71 | — | +0.00 | +381.71 | +474.81 | — |
| 2026-08-25 | `CRSP` | 20 | $57.08 | $57.93 | +17.10 | — | +0.00 | +17.10 | -35.80 | — |
| 2026-08-25 | `AU` | 10 | — | $118.52 | +0.00 | $123.39 | +48.70 | +48.70 | +0.00 | +48.70 |
| 2026-08-25 | `ERO` | 34 | — | $38.01 | +0.00 | $40.40 | +81.26 | +81.26 | +0.00 | +81.26 |
| 2026-08-25 | `FCX` | 16 | — | $77.13 | +0.00 | $79.91 | +44.48 | +44.48 | +0.00 | +44.48 |
| 2026-08-25 | `CNH` | 108 | — | $11.90 | +0.00 | $11.56 | -36.72 | -36.72 | +0.00 | -36.72 |
| 2026-08-25 | `HMY` | 57 | — | $22.41 | +0.00 | $22.95 | +30.78 | +30.78 | +0.00 | +30.78 |
| 2026-08-25 | `MOS` | 54 | — | $23.77 | +0.00 | $24.27 | +27.00 | +27.00 | +0.00 | +27.00 |
| 2026-08-25 | `RHI` | 29 | — | $43.76 | +0.00 | $44.90 | +33.06 | +33.06 | +0.00 | +33.06 |
| 2026-08-25 | `SUZ` | 143 | — | $8.98 | +0.00 | $9.03 | +7.15 | +7.15 | +0.00 | +7.15 |
| 2026-08-26 | `AU` | 10 | $123.39 | $119.80 | -35.90 | — | +0.00 | -35.90 | +12.80 | — |
| 2026-08-26 | `ERO` | 34 | $40.40 | $40.51 | +3.74 | — | +0.00 | +3.74 | +85.00 | — |
| 2026-08-26 | `FCX` | 16 | $79.91 | $79.34 | -9.12 | — | +0.00 | -9.12 | +35.36 | — |
| 2026-08-26 | `CNH` | 108 | $11.56 | $11.54 | -2.16 | — | +0.00 | -2.16 | -38.88 | — |
| 2026-08-26 | `HMY` | 57 | $22.95 | $22.39 | -31.92 | — | +0.00 | -31.92 | -1.14 | — |
| 2026-08-26 | `MOS` | 54 | $24.27 | $24.84 | +30.78 | $24.16 | -36.72 | -5.94 | +57.78 | +21.06 |
| 2026-08-26 | `RHI` | 29 | $44.90 | $44.33 | -16.53 | — | +0.00 | -16.53 | +16.53 | — |
| 2026-08-26 | `SUZ` | 143 | $9.03 | $9.03 | +0.00 | — | +0.00 | +0.00 | +7.15 | — |
| 2026-08-26 | `FNV` | 4 | — | $267.02 | +0.00 | $267.37 | +1.40 | +1.40 | +0.00 | +1.40 |
| 2026-08-26 | `FIGR` | 32 | — | $40.50 | +0.00 | $37.08 | -109.44 | -109.44 | +0.00 | -109.44 |
| 2026-08-26 | `FUTU` | 10 | — | $124.67 | +0.00 | $127.34 | +26.70 | +26.70 | +0.00 | +26.70 |
| 2026-08-26 | `SLQT` | 2240 | — | $0.58 | +0.00 | $0.55 | -73.92 | -73.92 | +0.00 | -73.92 |
| 2026-08-26 | `TIGR` | 250 | — | $5.21 | +0.00 | $5.46 | +62.50 | +62.50 | +0.00 | +62.50 |
| 2026-08-26 | `BTG` | 227 | — | $5.75 | +0.00 | $5.74 | -2.27 | -2.27 | +0.00 | -2.27 |
| 2026-08-26 | `NEXA` | 83 | — | $15.64 | +0.00 | $15.58 | -4.98 | -4.98 | +0.00 | -4.98 |
| 2026-08-27 | `MOS` | 54 | $24.16 | $24.00 | -8.64 | $23.76 | -12.96 | -21.60 | +12.42 | -0.54 |
| 2026-08-27 | `FNV` | 4 | $267.37 | $267.23 | -0.56 | — | +0.00 | -0.56 | +0.84 | — |
| 2026-08-27 | `FIGR` | 32 | $37.08 | $37.42 | +10.88 | — | +0.00 | +10.88 | -98.56 | — |
| 2026-08-27 | `FUTU` | 10 | $127.34 | $128.00 | +6.60 | — | +0.00 | +6.60 | +33.30 | — |
| 2026-08-27 | `SLQT` | 2240 | $0.55 | $0.53 | -44.80 | — | +0.00 | -44.80 | -118.72 | — |
| 2026-08-27 | `TIGR` | 250 | $5.46 | $5.49 | +7.50 | — | +0.00 | +7.50 | +70.00 | — |
| 2026-08-27 | `BTG` | 227 | $5.74 | $5.73 | -2.27 | — | +0.00 | -2.27 | -4.54 | — |
| 2026-08-27 | `NEXA` | 83 | $15.58 | $14.90 | -56.44 | — | +0.00 | -56.44 | -61.42 | — |
| 2026-08-27 | `ACMR` | 15 | — | $81.65 | +0.00 | $80.49 | -17.40 | -17.40 | +0.00 | -17.40 |
| 2026-08-27 | `GGB` | 278 | — | $4.57 | +0.00 | $4.70 | +36.14 | +36.14 | +0.00 | +36.14 |
| 2026-08-27 | `MT` | 17 | — | $74.54 | +0.00 | $74.63 | +1.53 | +1.53 | +0.00 | +1.53 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `SLI` | 488 | — | $2.60 | +0.00 | $2.64 | +19.52 | +19.52 | +0.00 | +19.52 |
| 2026-08-27 | `TX` | 23 | — | $55.25 | +0.00 | $55.83 | +13.34 | +13.34 | +0.00 | +13.34 |
| 2026-08-27 | `ANET` | 6 | — | $205.90 | +0.00 | $201.09 | -28.86 | -28.86 | +0.00 | -28.86 |
| 2026-08-28 | `MOS` | 54 | $23.76 | $23.95 | +10.26 | — | +0.00 | +10.26 | +9.72 | — |
| 2026-08-28 | `ACMR` | 15 | $80.49 | $79.27 | -18.30 | — | +0.00 | -18.30 | -35.70 | — |
| 2026-08-28 | `GGB` | 278 | $4.70 | $4.67 | -8.34 | — | +0.00 | -8.34 | +27.80 | — |
| 2026-08-28 | `MT` | 17 | $74.63 | $75.39 | +12.92 | — | +0.00 | +12.92 | +14.45 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `SLI` | 488 | $2.64 | $2.68 | +19.52 | — | +0.00 | +19.52 | +39.04 | — |
| 2026-08-28 | `TX` | 23 | $55.83 | $55.97 | +3.22 | — | +0.00 | +3.22 | +16.56 | — |
| 2026-08-28 | `ANET` | 6 | $201.09 | $200.00 | -6.54 | — | +0.00 | -6.54 | -35.40 | — |
| 2026-08-28 | `KEYS` | 3 | — | $324.41 | +0.00 | $319.97 | -13.32 | -13.32 | +0.00 | -13.32 |
| 2026-08-28 | `SMTC` | 8 | — | $141.76 | +0.00 | $131.17 | -84.72 | -84.72 | +0.00 | -84.72 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `AVT` | 13 | — | $91.49 | +0.00 | $88.63 | -37.18 | -37.18 | +0.00 | -37.18 |
| 2026-08-28 | `CGNX` | 20 | — | $62.82 | +0.00 | $60.46 | -47.20 | -47.20 | +0.00 | -47.20 |
| 2026-08-28 | `COHR` | 4 | — | $289.44 | +0.00 | $279.20 | -40.96 | -40.96 | +0.00 | -40.96 |
| 2026-08-28 | `LSCC` | 10 | — | $119.76 | +0.00 | $114.40 | -53.60 | -53.60 | +0.00 | -53.60 |
| 2026-08-31 | `KEYS` | 3 | $319.97 | $322.49 | +7.56 | — | +0.00 | +7.56 | -5.76 | — |
| 2026-08-31 | `SMTC` | 8 | $131.17 | $132.30 | +9.04 | — | +0.00 | +9.04 | -75.68 | — |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -65.94 | — |
| 2026-08-31 | `AVT` | 13 | $88.63 | $89.39 | +9.88 | — | +0.00 | +9.88 | -27.30 | — |
| 2026-08-31 | `CGNX` | 20 | $60.46 | $60.46 | +0.00 | — | +0.00 | +0.00 | -47.20 | — |
| 2026-08-31 | `COHR` | 4 | $279.20 | $280.25 | +4.20 | — | +0.00 | +4.20 | -36.76 | — |
| 2026-08-31 | `LSCC` | 10 | $114.40 | $115.56 | +11.60 | — | +0.00 | +11.60 | -42.00 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ARCT` | 73 | — | $16.77 | +0.00 | $15.56 | -88.33 | -88.33 | +0.00 | -88.33 |
| 2026-09-03 | `BMEA` | 634 | — | $1.93 | +0.00 | $1.91 | -12.68 | -12.68 | +0.00 | -12.68 |
| 2026-09-03 | `CRDL` | 561 | — | $2.18 | +0.00 | $2.16 | -11.22 | -11.22 | +0.00 | -11.22 |
| 2026-09-03 | `HRMY` | 28 | — | $42.93 | +0.00 | $41.86 | -29.96 | -29.96 | +0.00 | -29.96 |
| 2026-09-03 | `NVAX` | 117 | — | $10.42 | +0.00 | $10.34 | -9.36 | -9.36 | +0.00 | -9.36 |
| 2026-09-03 | `PBH` | 22 | — | $53.45 | +0.00 | $52.56 | -19.58 | -19.58 | +0.00 | -19.58 |
| 2026-09-03 | `PCRX` | 45 | — | $26.74 | +0.00 | $26.60 | -6.30 | -6.30 | +0.00 | -6.30 |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-04 | `ARCT` | 73 | $15.56 | $15.61 | +3.65 | — | +0.00 | +3.65 | -84.68 | — |
| 2026-09-04 | `BMEA` | 634 | $1.91 | $1.90 | -6.34 | $2.03 | +82.42 | +76.08 | -19.02 | +63.40 |
| 2026-09-04 | `CRDL` | 561 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -11.22 | — |
| 2026-09-04 | `HRMY` | 28 | $41.86 | $41.50 | -10.08 | $42.25 | +21.00 | +10.92 | -40.04 | -19.04 |
| 2026-09-04 | `NVAX` | 117 | $10.34 | $10.50 | +18.72 | — | +0.00 | +18.72 | +9.36 | — |
| 2026-09-04 | `PBH` | 22 | $52.56 | $51.80 | -16.72 | — | +0.00 | -16.72 | -36.30 | — |
| 2026-09-04 | `PCRX` | 45 | $26.60 | $26.38 | -9.90 | — | +0.00 | -9.90 | -16.20 | — |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `CABA` | 345 | — | $3.46 | +0.00 | $3.47 | +3.45 | +3.45 | +0.00 | +3.45 |
| 2026-09-04 | `ALEC` | 473 | — | $2.52 | +0.00 | $2.46 | -28.38 | -28.38 | +0.00 | -28.38 |
| 2026-09-04 | `ATRC` | 22 | — | $52.03 | +0.00 | $51.52 | -11.22 | -11.22 | +0.00 | -11.22 |
| 2026-09-04 | `BHC` | 178 | — | $6.71 | +0.00 | $6.56 | -26.70 | -26.70 | +0.00 | -26.70 |
| 2026-09-04 | `CRM` | 4 | — | $263.36 | +0.00 | $259.23 | -16.52 | -16.52 | +0.00 | -16.52 |
| 2026-09-04 | `MLYS` | 42 | — | $28.00 | +0.00 | $28.21 | +8.82 | +8.82 | +0.00 | +8.82 |
| 2026-09-08 | `BMEA` | 634 | $2.03 | $2.00 | -19.02 | — | +0.00 | -19.02 | +44.38 | — |
| 2026-09-08 | `HRMY` | 28 | $42.25 | $42.20 | -1.40 | — | +0.00 | -1.40 | -20.44 | — |
| 2026-09-08 | `CABA` | 345 | $3.47 | $3.43 | -13.80 | — | +0.00 | -13.80 | -10.35 | — |
| 2026-09-08 | `ALEC` | 473 | $2.46 | $2.38 | -37.84 | — | +0.00 | -37.84 | -66.22 | — |
| 2026-09-08 | `ATRC` | 22 | $51.52 | $54.31 | +61.38 | — | +0.00 | +61.38 | +50.16 | — |
| 2026-09-08 | `BHC` | 178 | $6.56 | $6.57 | +1.78 | — | +0.00 | +1.78 | -24.92 | — |
| 2026-09-08 | `CRM` | 4 | $259.23 | $253.72 | -22.04 | — | +0.00 | -22.04 | -38.56 | — |
| 2026-09-08 | `MLYS` | 42 | $28.21 | $28.03 | -7.56 | — | +0.00 | -7.56 | +1.26 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +185.07 | BTSG, HIMS, INO, IREN, SLS, TGTX, TNDM, TPG | — | $97.53 | $10,153.12 | BTSG×20, HIMS×42, INO×1543, IREN×27, SLS×106, TGTX×25, TNDM×53, TPG×24 |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, HIMS×42, INO×1543, IREN×27, SLS×106, TGTX×25, TNDM×53, TPG×24 | $10,178.12 | +25.00 | -283.17 | ARX, BETR, BTBT, FIGR, ADUR, AIRO, AMPY, ANGX | BTSG, HIMS, INO, IREN, SLS, TGTX, TNDM, TPG | $30.71 | $9,831.66 | ARX×64, BETR×85, BTBT×845, FIGR×39, ADUR×76, AIRO×114, AMPY×256, ANGX×294 |
| 2026-08-17 | +2.25 | $30.71 | ARX×64, BETR×85, BTBT×845, FIGR×39, ADUR×76, AIRO×114, AMPY×256, ANGX×294 | $9,866.80 | +35.14 | -294.64 | ABX, INV, NU, XHG, DVN, EOG, FANG, ALOY | ARX, BETR, BTBT, FIGR, ADUR, AIRO, AMPY, ANGX | $139.07 | $9,516.18 | ABX×134, INV×759, NU×79, XHG×293, DVN×26, EOG×8, FANG×6, ALOY×83 |
| 2026-08-18 | -6.20 | $139.07 | ABX×134, INV×759, NU×79, XHG×293, DVN×26, EOG×8, FANG×6, ALOY×83 | $9,437.73 | -78.45 | +0.00 | — | ABX, INV, NU, XHG, DVN, EOG, FANG, ALOY | $9,410.87 | $9,410.87 | — |
| 2026-08-19 | -7.20 | $9,410.87 | — | $9,410.87 | +0.00 | +0.00 | — | — | $9,410.87 | $9,410.87 | — |
| 2026-08-20 | +1.12 | $9,410.87 | — | $9,410.87 | +0.00 | +220.29 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $149.71 | $9,607.24 | AG×57, BHP×12, CDE×56, HDSN×203, IAG×59, KGC×39, NFGC×672, WPM×8 |
| 2026-08-21 | +3.25 | $149.71 | AG×57, BHP×12, CDE×56, HDSN×203, IAG×59, KGC×39, NFGC×672, WPM×8 | $9,859.80 | +252.56 | +247.14 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $196.74 | $10,045.50 | AU×10, AUPH×71, AEM×5, ARCT×110, AUTL×497, CRDL×637, CRSP×20, CYPH×931 |
| 2026-08-24 | -5.17 | $196.74 | AU×10, AUPH×71, AEM×5, ARCT×110, AUTL×497, CRDL×637, CRSP×20, CYPH×931 | $10,398.85 | +353.35 | -33.50 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CYPH | $9,188.20 | $10,329.70 | CRSP×20 |
| 2026-08-25 | +1.80 | $9,188.20 | CRSP×20 | $10,346.80 | +17.10 | +235.71 | AU, ERO, FCX, CNH, HMY, MOS, RHI, SUZ | CRSP | $216.50 | $10,563.16 | AU×10, ERO×34, FCX×16, CNH×108, HMY×57, MOS×54, RHI×29, SUZ×143 |
| 2026-08-26 | +2.02 | $216.50 | AU×10, ERO×34, FCX×16, CNH×108, HMY×57, MOS×54, RHI×29, SUZ×143 | $10,502.05 | -61.11 | -136.73 | FNV, FIGR, FUTU, SLQT, TIGR, BTG, NEXA | AU, ERO, FCX, CNH, HMY, RHI, SUZ | $288.56 | $10,315.76 | MOS×54, FNV×4, FIGR×32, FUTU×10, SLQT×2240, TIGR×250, BTG×227, NEXA×83 |
| 2026-08-27 | — | $288.56 | MOS×54, FNV×4, FIGR×32, FUTU×10, SLQT×2240, TIGR×250, BTG×227, NEXA×83 | $10,228.03 | -87.73 | -20.31 | ACMR, GGB, MT, MU, SLI, TX, ANET | FNV, FIGR, FUTU, SLQT, TIGR, BTG, NEXA | $374.00 | $10,154.04 | MOS×54, ACMR×15, GGB×278, MT×17, MU×1, SLI×488, TX×23, ANET×6 |
| 2026-08-28 | +0.75 | $374.00 | MOS×54, ACMR×15, GGB×278, MT×17, MU×1, SLI×488, TX×23, ANET×6 | $10,150.68 | -3.36 | -342.92 | KEYS, SMTC, CIEN, AVT, CGNX, COHR, LSCC | MOS, ACMR, GGB, MT, MU, SLI, TX, ANET | $2,004.43 | $9,771.21 | KEYS×3, SMTC×8, CIEN×3, AVT×13, CGNX×20, COHR×4, LSCC×10 |
| 2026-08-31 | -5.85 | $2,004.43 | KEYS×3, SMTC×8, CIEN×3, AVT×13, CGNX×20, COHR×4, LSCC×10 | $9,813.49 | +42.28 | +0.00 | — | KEYS, SMTC, CIEN, AVT, CGNX, COHR, LSCC | $9,799.24 | $9,799.24 | — |
| 2026-09-01 | -6.30 | $9,799.24 | — | $9,799.24 | -0.00 | +0.00 | — | — | $9,799.24 | $9,799.24 | — |
| 2026-09-02 | -3.83 | $9,799.24 | — | $9,799.24 | -0.00 | +0.00 | — | — | $9,799.24 | $9,799.24 | — |
| 2026-09-03 | -0.90 | $9,799.24 | — | $9,799.24 | -0.00 | -193.81 | ARCT, BMEA, CRDL, HRMY, NVAX, PBH, PCRX, RVTY | — | $107.76 | $9,577.19 | ARCT×73, BMEA×634, CRDL×561, HRMY×28, NVAX×117, PBH×22, PCRX×45, RVTY×9 |
| 2026-09-04 | +2.25 | $107.76 | ARCT×73, BMEA×634, CRDL×561, HRMY×28, NVAX×117, PBH×22, PCRX×45, RVTY×9 | $9,551.12 | -26.07 | +32.87 | CABA, ALEC, ATRC, BHC, CRM, MLYS | ARCT, CRDL, NVAX, PBH, PCRX, RVTY | $192.93 | $9,546.54 | BMEA×634, HRMY×28, CABA×345, ALEC×473, ATRC×22, BHC×178, CRM×4, MLYS×42 |
| 2026-09-08 | -11.47 | $192.93 | BMEA×634, HRMY×28, CABA×345, ALEC×473, ATRC×22, BHC×178, CRM×4, MLYS×42 | $9,508.04 | -38.50 | +0.00 | — | BMEA, HRMY, CABA, ALEC, ATRC, BHC, CRM, MLYS | $9,478.15 | $9,478.15 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | rank by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $7,550.75 | — | rank by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $6,283.80 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $5,040.27 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,797.76 | — | rank by cond; rank cond; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $2,553.19 | — | rank by cond; rank cond; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $1,314.55 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $97.53 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | 16:00 close · cash $97.53 · equity $10,153.12 vs 09:30 $10,000.00 (+153.12; session marks +185.07) · 8 name(s) marked open→close (per-name table). BTSG×20 09:30 $59.80 → close $60.23 +8.60; HIMS×42 09:30 $29.74 → close $28.77 -40.74; INO×1543 09:30 $0.81 → close $0.90 +138.87; IREN×27 09:30 $45.98 → close $44.76 -32.94; SLS×106 09:30 $11.70 → close $12.36 +69.96; TGTX×25 09:30 $49.70 → close $47.94 -44.00; TNDM×53 09:30 $23.33 → close $23.13 -10.60; TPG×24 09:30 $50.62 → close $54.62 +95.92 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) · 8 name(s) re-marked at the open (per-name table). BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; TPG×24 yday $54.62 → 09:30 $55.29 +16.08 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $1,288.46 | ▼ -7.12 after sell → book $10,176.05; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $2,510.63 | ▼ -29.03 after sell → book $10,173.92; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $3,926.37 | ▲ +148.79 after sell → book $10,154.67; vs 09:30 mark -19.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $5,114.71 | ▼ -55.19 after sell → book $10,152.58; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $6,426.78 | ▲ +69.56 after sell → book $10,150.25; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $7,606.44 | ▼ -64.90 after sell → book $10,148.16; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $8,819.03 | ▼ -26.05 after sell → book $10,145.99; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $10,143.91 | ▲ +107.86 after sell → book $10,143.91; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 64 | $19.57 | $2.18 | — | $8,889.25 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 85 | $14.80 | $2.25 | — | $7,629.00 | — | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 845 | $1.50 | $10.90 | — | $6,350.60 | — | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `FIGR` | 39 | $32.12 | $2.11 | — | $5,095.81 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 76 | $16.50 | $2.22 | — | $3,839.60 | — | rank by cond; rank cond; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 114 | $11.12 | $2.33 | — | $2,569.58 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AMPY` | 256 | $4.94 | $3.30 | — | $1,301.64 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.4; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 294 | $4.31 | $3.79 | — | $30.71 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.71 | ▼ close $9,831.66 vs 09:30 $10,178.12 (session -283.17) | 16:00 close · cash $30.71 · equity $9,831.66 vs 09:30 $10,178.12 (-346.46; session marks -283.17) · 8 name(s) marked open→close (per-name table). ARX×64 09:30 $19.57 → close $19.58 +0.64; BETR×85 09:30 $14.80 → close $13.73 -90.95; BTBT×845 09:30 $1.50 → close $1.57 +59.15; FIGR×39 09:30 $32.12 → close $31.43 -26.91; ADUR×76 09:30 $16.50 → close $16.17 -25.08; AIRO×114 09:30 $11.12 → close $9.57 -176.70; AMPY×256 09:30 $4.94 → close $4.78 -40.96; ANGX×294 09:30 $4.31 → close $4.37 +17.64 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.71 | ▲ 09:30 equity $9,866.80 vs yday $9,831.66 (+35.14) | 09:30 open · cash $30.71 (unchanged overnight, no fees) · equity $9,866.80 vs prior close $9,831.66 (+35.14) · 8 name(s) re-marked at the open (per-name table). ARX×64 yday $19.58 → 09:30 $19.57 -0.64; BETR×85 yday $13.73 → 09:30 $13.67 -5.10; BTBT×845 yday $1.57 → 09:30 $1.52 -42.25; FIGR×39 yday $31.43 → 09:30 $32.16 +28.47; ADUR×76 yday $16.17 → 09:30 $15.73 -33.44; AIRO×114 yday $9.57 → 09:30 $9.57 +0.00; AMPY×256 yday $4.78 → 09:30 $4.86 +20.48; ANGX×294 yday $4.37 → 09:30 $4.60 +67.62 | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 64 | $19.57 | $2.20 | $-4.38 | $1,280.99 | ▼ -4.38 after sell → book $9,864.60; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 85 | $13.67 | $2.27 | $-100.56 | $2,440.67 | ▼ -100.56 after sell → book $9,862.33; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 845 | $1.52 | $11.05 | $-5.05 | $3,714.02 | ▼ -5.05 after sell → book $9,851.28; vs 09:30 mark -11.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `FIGR` | 39 | $32.16 | $2.13 | $-2.67 | $4,966.13 | ▼ -2.67 after sell → book $9,849.15; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 76 | $15.73 | $2.24 | $-62.98 | $6,159.37 | ▼ -62.98 after sell → book $9,846.91; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 114 | $9.57 | $2.36 | $-181.39 | $7,247.99 | ▼ -181.39 after sell → book $9,844.55; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AMPY` | 256 | $4.86 | $3.35 | $-27.14 | $8,488.79 | ▼ -27.14 after sell → book $9,841.19; vs 09:30 mark -3.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 294 | $4.60 | $3.85 | $+77.62 | $9,837.34 | ▲ +77.62 after sell → book $9,837.34; vs 09:30 mark -3.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 134 | $9.12 | $2.39 | — | $8,612.87 | — | rank by cond; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1229.67 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 759 | $1.62 | $9.79 | — | $7,373.50 | — | rank by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $1229.67 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NU` | 79 | $15.40 | $2.23 | — | $6,154.67 | — | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $1229.67 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 293 | $4.19 | $3.78 | — | $4,923.22 | — | rank by cond; rank cond; list yday_mover; ⚪; ret5=+291.8; leftover $1229.67 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 26 | $46.18 | $2.07 | — | $3,720.47 | — | rank by cond; rank cond; list flatten; 🔵; ret5=+6.7; leftover $1229.67 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $2,576.30 | — | rank by cond; rank cond; list flatten; 🔵; ret5=+5.8; leftover $1229.67 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $1,358.09 | — | rank by cond; rank cond; list flatten; 🔵; ret5=+8.3; leftover $1229.67 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 83 | $14.66 | $2.24 | — | $139.07 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1229.67 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $139.07 | ▼ close $9,516.18 vs 09:30 $9,866.80 (session -294.64) | 16:00 close · cash $139.07 · equity $9,516.18 vs 09:30 $9,866.80 (-350.62; session marks -294.64) · 8 name(s) marked open→close (per-name table). ABX×134 09:30 $9.12 → close $9.12 +0.00; INV×759 09:30 $1.62 → close $1.39 -178.37; NU×79 09:30 $15.40 → close $14.74 -52.14; XHG×293 09:30 $4.19 → close $3.91 -82.04; DVN×26 09:30 $46.18 → close $47.57 +36.14; EOG×8 09:30 $142.77 → close $146.15 +27.04; FANG×6 09:30 $202.70 → close $206.29 +21.54; ALOY×83 09:30 $14.66 → close $13.86 -66.81 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $139.07 | ▼ 09:30 equity $9,437.73 vs yday $9,516.18 (-78.45) | 09:30 open · cash $139.07 (unchanged overnight, no fees) · equity $9,437.73 vs prior close $9,516.18 (-78.45) · 8 name(s) re-marked at the open (per-name table). ABX×134 yday $9.12 → 09:30 $9.03 -12.06; INV×759 yday $1.39 → 09:30 $1.32 -45.54; NU×79 yday $14.74 → 09:30 $14.53 -16.59; XHG×293 yday $3.91 → 09:30 $3.94 +8.79; DVN×26 yday $47.57 → 09:30 $48.00 +11.18; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; ALOY×83 yday $13.86 → 09:30 $13.19 -55.20 | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 134 | $9.03 | $2.42 | $-16.88 | $1,346.67 | ▼ -16.88 after sell → book $9,435.30; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INV` | 759 | $1.32 | $9.93 | $-243.62 | $2,342.42 | ▼ -243.62 after sell → book $9,425.38; vs 09:30 mark -9.92 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NU` | 79 | $14.53 | $2.25 | $-73.21 | $3,488.04 | ▼ -73.21 after sell → book $9,423.13; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 293 | $3.94 | $3.84 | $-80.87 | $4,638.62 | ▼ -80.87 after sell → book $9,419.29; vs 09:30 mark -3.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 26 | $48.00 | $2.09 | $+43.16 | $5,884.53 | ▲ +43.16 after sell → book $9,417.20; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $7,066.82 | ▲ +38.11 after sell → book $9,415.17; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $8,318.37 | ▲ +33.34 after sell → book $9,413.14; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 83 | $13.19 | $2.26 | $-126.51 | $9,410.87 | ▼ -126.51 after sell → book $9,410.87; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,410.87 | ▲ close $9,410.87 vs 09:30 $9,437.73 (session +0.00) | 16:00 close · cash $9,410.87 · no lots left · equity $9,410.87. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,410.87 | ▲ 09:30 equity $9,410.87 vs yday $9,410.87 (+0.00) | 09:30 open · cash $9,410.87 · no holdings · equity $9,410.87 vs prior close $9,410.87 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,410.87 | ▲ close $9,410.87 vs 09:30 $9,410.87 (session +0.00) | 16:00 close · cash $9,410.87 · no lots left · equity $9,410.87. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,410.87 | ▲ 09:30 equity $9,410.87 vs yday $9,410.87 (+0.00) | 09:30 open · cash $9,410.87 · no holdings · equity $9,410.87 vs prior close $9,410.87 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 57 | $20.55 | $2.16 | — | $8,237.36 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1176.36 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $7,143.22 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1176.36 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $5,984.66 | — | rank by cond; rank cond; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1176.36 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 203 | $5.77 | $2.62 | — | $4,810.73 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1176.36 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $3,650.39 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1176.36 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $2,492.72 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1176.36 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 672 | $1.75 | $8.67 | — | $1,308.05 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1176.36 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $149.71 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1176.36 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $149.71 | ▲ close $9,607.24 vs 09:30 $9,410.87 (session +220.29) | 16:00 close · cash $149.71 · equity $9,607.24 vs 09:30 $9,410.87 (+196.37; session marks +220.29) · 8 name(s) marked open→close (per-name table). AG×57 09:30 $20.55 → close $21.19 +36.48; BHP×12 09:30 $91.01 → close $93.63 +31.44; CDE×56 09:30 $20.65 → close $21.11 +25.76; HDSN×203 09:30 $5.77 → close $5.57 -40.60; IAG×59 09:30 $19.63 → close $20.50 +51.33; KGC×39 09:30 $29.63 → close $31.43 +70.20; NFGC×672 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.71 | ▲ 09:30 equity $9,859.80 vs yday $9,607.24 (+252.56) | 09:30 open · cash $149.71 (unchanged overnight, no fees) · equity $9,859.80 vs prior close $9,607.24 (+252.56) · 8 name(s) re-marked at the open (per-name table). AG×57 yday $21.19 → 09:30 $21.90 +40.47; BHP×12 yday $93.63 → 09:30 $95.72 +25.08; CDE×56 yday $21.11 → 09:30 $21.75 +35.84; HDSN×203 yday $5.57 → 09:30 $5.67 +20.30; IAG×59 yday $20.50 → 09:30 $21.17 +39.53; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×672 yday $1.75 → 09:30 $1.79 +26.88; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 57 | $21.90 | $2.18 | $+72.61 | $1,395.83 | ▲ +72.61 after sell → book $9,857.62; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 12 | $95.72 | $2.05 | $+52.45 | $2,542.43 | ▲ +52.45 after sell → book $9,855.58; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 56 | $21.75 | $2.18 | $+57.26 | $3,758.25 | ▲ +57.26 after sell → book $9,853.40; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 203 | $5.67 | $2.66 | $-25.58 | $4,906.60 | ▼ -25.58 after sell → book $9,850.74; vs 09:30 mark -2.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 59 | $21.17 | $2.19 | $+86.51 | $6,153.44 | ▲ +86.51 after sell → book $9,848.55; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 39 | $32.17 | $2.13 | $+94.83 | $7,405.94 | ▲ +94.83 after sell → book $9,846.42; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 672 | $1.79 | $8.79 | $+9.42 | $8,600.03 | ▲ +9.42 after sell → book $9,837.63; vs 09:30 mark -8.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $9,835.60 | ▲ +77.23 after sell → book $9,835.60; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,639.28 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1229.45 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 71 | $17.20 | $2.20 | — | $7,415.88 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1229.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,332.37 | — | rank by cond; rank cond; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1229.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 110 | $11.13 | $2.32 | — | $5,105.75 | — | rank by cond; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1229.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 497 | $2.47 | $6.41 | — | $3,871.75 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1229.45 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 637 | $1.93 | $8.22 | — | $2,634.12 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1229.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 20 | $59.72 | $2.05 | — | $1,437.67 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1229.45 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 931 | $1.32 | $12.01 | — | $196.74 | — | rank by cond; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1229.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $196.74 | ▲ close $10,045.50 vs 09:30 $9,859.80 (session +247.14) | 16:00 close · cash $196.74 · equity $10,045.50 vs 09:30 $9,859.80 (+185.70; session marks +247.14) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×71 09:30 $17.20 → close $16.65 -39.05; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×110 09:30 $11.13 → close $13.45 +255.20; AUTL×497 09:30 $2.47 → close $2.41 -29.82; CRDL×637 09:30 $1.93 → close $1.86 -44.59; CRSP×20 09:30 $59.72 → close $59.50 -4.40; CYPH×931 09:30 $1.32 → close $1.42 +93.10 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $196.74 | ▲ 09:30 equity $10,398.85 vs yday $10,045.50 (+353.35) | 09:30 open · cash $196.74 (unchanged overnight, no fees) · equity $10,398.85 vs prior close $10,045.50 (+353.35) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×71 yday $16.65 → 09:30 $16.57 -5.68; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×110 yday $13.45 → 09:30 $13.33 -13.20; AUTL×497 yday $2.41 → 09:30 $2.40 -4.97; CRDL×637 yday $1.86 → 09:30 $1.88 +12.74; CRSP×20 yday $59.50 → 09:30 $58.75 -15.00; CYPH×931 yday $1.42 → 09:30 $1.83 +381.71 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,399.80 | ▲ +6.74 after sell → book $10,396.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 71 | $16.57 | $2.22 | $-49.16 | $2,574.05 | ▼ -49.16 after sell → book $10,394.59; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,657.17 | ▼ -0.38 after sell → book $10,392.56; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 110 | $13.33 | $2.35 | $+237.33 | $5,121.12 | ▲ +237.33 after sell → book $10,390.21; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 497 | $2.40 | $6.50 | $-47.71 | $6,307.42 | ▼ -47.71 after sell → book $10,383.71; vs 09:30 mark -6.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 637 | $1.88 | $8.33 | $-48.40 | $7,496.65 | ▼ -48.40 after sell → book $10,375.38; vs 09:30 mark -8.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 931 | $1.83 | $12.18 | $+450.62 | $9,188.20 | ▲ +450.62 after sell → book $10,363.20; vs 09:30 mark -12.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,188.20 | ▼ close $10,329.70 vs 09:30 $10,398.85 (session -33.50) | 16:00 close · cash $9,188.20 · equity $10,329.70 vs 09:30 $10,398.85 (-69.15; session marks -33.50) · 1 name(s) marked open→close (per-name table). CRSP×20 09:30 $58.75 → close $57.08 -33.50 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,188.20 | ▲ 09:30 equity $10,346.80 vs yday $10,329.70 (+17.10) | 09:30 open · cash $9,188.20 (unchanged overnight, no fees) · equity $10,346.80 vs prior close $10,329.70 (+17.10) · 1 name(s) re-marked at the open (per-name table). CRSP×20 yday $57.08 → 09:30 $57.93 +17.10 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 20 | $57.93 | $2.07 | $-39.92 | $10,344.73 | ▼ -39.92 after sell → book $10,344.73; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 10 | $118.52 | $2.02 | — | $9,157.51 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1293.09 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 34 | $38.01 | $2.09 | — | $7,863.08 | — | rank by cond; rank cond; list mover_buy; ⚪; ret5=+10.4; leftover $1293.09 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 16 | $77.13 | $2.04 | — | $6,626.96 | — | rank by cond; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1293.09 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CNH` | 108 | $11.90 | $2.31 | — | $5,339.44 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1293.09 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HMY` | 57 | $22.41 | $2.16 | — | $4,059.91 | — | rank by cond; rank cond; list mover_buy; ⚪; ret5=+13.9; leftover $1293.09 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 54 | $23.77 | $2.15 | — | $2,774.18 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+13.0; leftover $1293.09 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 29 | $43.76 | $2.08 | — | $1,503.06 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $1293.09 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 143 | $8.98 | $2.42 | — | $216.50 | — | rank by cond; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; leftover $1293.09 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $216.50 | ▲ close $10,563.16 vs 09:30 $10,346.80 (session +235.71) | 16:00 close · cash $216.50 · equity $10,563.16 vs 09:30 $10,346.80 (+216.36; session marks +235.71) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $118.52 → close $123.39 +48.70; ERO×34 09:30 $38.01 → close $40.40 +81.26; FCX×16 09:30 $77.13 → close $79.91 +44.48; CNH×108 09:30 $11.90 → close $11.56 -36.72; HMY×57 09:30 $22.41 → close $22.95 +30.78; MOS×54 09:30 $23.77 → close $24.27 +27.00; RHI×29 09:30 $43.76 → close $44.90 +33.06; SUZ×143 09:30 $8.98 → close $9.03 +7.15 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $216.50 | ▼ 09:30 equity $10,502.05 vs yday $10,563.16 (-61.11) | 09:30 open · cash $216.50 (unchanged overnight, no fees) · equity $10,502.05 vs prior close $10,563.16 (-61.11) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $123.39 → 09:30 $119.80 -35.90; ERO×34 yday $40.40 → 09:30 $40.51 +3.74; FCX×16 yday $79.91 → 09:30 $79.34 -9.12; CNH×108 yday $11.56 → 09:30 $11.54 -2.16; HMY×57 yday $22.95 → 09:30 $22.39 -31.92; MOS×54 yday $24.27 → 09:30 $24.84 +30.78; RHI×29 yday $44.90 → 09:30 $44.33 -16.53; SUZ×143 yday $9.03 → 09:30 $9.03 +0.00 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 10 | $119.80 | $2.04 | $+8.74 | $1,412.46 | ▲ +8.74 after sell → book $10,500.01; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ERO` | 34 | $40.51 | $2.11 | $+80.79 | $2,787.69 | ▲ +80.79 after sell → book $10,497.90; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 16 | $79.34 | $2.06 | $+31.26 | $4,055.07 | ▲ +31.26 after sell → book $10,495.84; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CNH` | 108 | $11.54 | $2.34 | $-43.54 | $5,299.05 | ▼ -43.54 after sell → book $10,493.50; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `HMY` | 57 | $22.39 | $2.18 | $-5.48 | $6,573.10 | ▼ -5.48 after sell → book $10,491.32; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 29 | $44.33 | $2.10 | $+12.36 | $7,856.57 | ▲ +12.36 after sell → book $10,489.22; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 143 | $9.03 | $2.45 | $+2.28 | $9,145.41 | ▲ +2.28 after sell → book $10,486.77; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 4 | $267.02 | $2.00 | — | $8,075.33 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $1306.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FIGR` | 32 | $40.50 | $2.09 | — | $6,777.24 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+15.8; leftover $1306.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `FUTU` | 10 | $124.67 | $2.02 | — | $5,528.52 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+15.7; leftover $1306.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 2240 | $0.58 | $19.78 | — | $4,202.82 | — | rank by cond; rank cond; list yday_mover; 🔵; ret5=-27.5; leftover $1306.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 250 | $5.21 | $3.23 | — | $2,897.10 | — | rank by cond; rank cond; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $1306.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BTG` | 227 | $5.75 | $2.93 | — | $1,588.92 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+17.9; leftover $1306.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NEXA` | 83 | $15.64 | $2.24 | — | $288.56 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+17.3; leftover $1306.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $288.56 | ▼ close $10,315.76 vs 09:30 $10,502.05 (session -136.73) | 16:00 close · cash $288.56 · equity $10,315.76 vs 09:30 $10,502.05 (-186.29; session marks -136.73) · 8 name(s) marked open→close (per-name table). MOS×54 09:30 $24.84 → close $24.16 -36.72; FNV×4 09:30 $267.02 → close $267.37 +1.40; FIGR×32 09:30 $40.50 → close $37.08 -109.44; FUTU×10 09:30 $124.67 → close $127.34 +26.70; SLQT×2240 09:30 $0.58 → close $0.55 -73.92; TIGR×250 09:30 $5.21 → close $5.46 +62.50; BTG×227 09:30 $5.75 → close $5.74 -2.27; NEXA×83 09:30 $15.64 → close $15.58 -4.98 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $288.56 | ▼ 09:30 equity $10,228.03 vs yday $10,315.76 (-87.73) | 09:30 open · cash $288.56 (unchanged overnight, no fees) · equity $10,228.03 vs prior close $10,315.76 (-87.73) · 8 name(s) re-marked at the open (per-name table). MOS×54 yday $24.16 → 09:30 $24.00 -8.64; FNV×4 yday $267.37 → 09:30 $267.23 -0.56; FIGR×32 yday $37.08 → 09:30 $37.42 +10.88; FUTU×10 yday $127.34 → 09:30 $128.00 +6.60; SLQT×2240 yday $0.55 → 09:30 $0.53 -44.80; TIGR×250 yday $5.46 → 09:30 $5.49 +7.50; BTG×227 yday $5.74 → 09:30 $5.73 -2.27; NEXA×83 yday $15.58 → 09:30 $14.90 -56.44 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 4 | $267.23 | $2.02 | $-3.18 | $1,355.46 | ▼ -3.18 after sell → book $10,226.01; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FIGR` | 32 | $37.42 | $2.11 | $-102.75 | $2,550.79 | ▼ -102.75 after sell → book $10,223.90; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FUTU` | 10 | $128.00 | $2.04 | $+29.24 | $3,828.75 | ▲ +29.24 after sell → book $10,221.86; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 2240 | $0.53 | $18.97 | $-157.47 | $4,996.98 | ▼ -157.47 after sell → book $10,202.89; vs 09:30 mark -18.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 250 | $5.49 | $3.28 | $+63.50 | $6,366.20 | ▲ +63.50 after sell → book $10,199.61; vs 09:30 mark -3.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTG` | 227 | $5.73 | $2.98 | $-10.44 | $7,663.93 | ▼ -10.44 after sell → book $10,196.63; vs 09:30 mark -2.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NEXA` | 83 | $14.90 | $2.26 | $-65.92 | $8,898.37 | ▼ -65.92 after sell → book $10,194.37; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 15 | $81.65 | $2.04 | — | $7,671.59 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1271.20 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 278 | $4.57 | $3.59 | — | $6,397.54 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+1.1; leftover $1271.20 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 17 | $74.54 | $2.04 | — | $5,128.32 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=-0.1; leftover $1271.20 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $4,159.32 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1271.20 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 488 | $2.60 | $6.30 | — | $2,884.22 | — | rank by cond; rank cond; list flatten; ret5=+13.0; leftover $1271.20 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 23 | $55.25 | $2.06 | — | $1,611.41 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+2.1; leftover $1271.20 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $205.90 | $2.01 | — | $374.00 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+8.5; leftover $1271.20 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $374.00 | ▼ close $10,154.04 vs 09:30 $10,228.03 (session -20.31) | 16:00 close · cash $374.00 · equity $10,154.04 vs 09:30 $10,228.03 (-73.99; session marks -20.31) · 8 name(s) marked open→close (per-name table). MOS×54 09:30 $24.00 → close $23.76 -12.96; ACMR×15 09:30 $81.65 → close $80.49 -17.40; GGB×278 09:30 $4.57 → close $4.70 +36.14; MT×17 09:30 $74.54 → close $74.63 +1.53; MU×1 09:30 $967.01 → close $935.39 -31.62; SLI×488 09:30 $2.60 → close $2.64 +19.52; TX×23 09:30 $55.25 → close $55.83 +13.34; ANET×6 09:30 $205.90 → close $201.09 -28.86 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $374.00 | ▼ 09:30 equity $10,150.68 vs yday $10,154.04 (-3.36) | 09:30 open · cash $374.00 (unchanged overnight, no fees) · equity $10,150.68 vs prior close $10,154.04 (-3.36) · 8 name(s) re-marked at the open (per-name table). MOS×54 yday $23.76 → 09:30 $23.95 +10.26; ACMR×15 yday $80.49 → 09:30 $79.27 -18.30; GGB×278 yday $4.70 → 09:30 $4.67 -8.34; MT×17 yday $74.63 → 09:30 $75.39 +12.92; MU×1 yday $935.39 → 09:30 $919.29 -16.10; SLI×488 yday $2.64 → 09:30 $2.68 +19.52; TX×23 yday $55.83 → 09:30 $55.97 +3.22; ANET×6 yday $201.09 → 09:30 $200.00 -6.54 | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 54 | $23.95 | $2.17 | $+5.40 | $1,665.13 | ▲ +5.40 after sell → book $10,148.51; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 15 | $79.27 | $2.06 | $-39.79 | $2,852.13 | ▼ -39.79 after sell → book $10,146.46; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 278 | $4.67 | $3.64 | $+20.57 | $4,146.74 | ▲ +20.57 after sell → book $10,142.81; vs 09:30 mark -3.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 17 | $75.39 | $2.06 | $+10.35 | $5,426.31 | ▲ +10.35 after sell → book $10,140.75; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $6,343.59 | ▼ -51.73 after sell → book $10,138.74; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 488 | $2.68 | $6.39 | $+26.36 | $7,645.04 | ▲ +26.36 after sell → book $10,132.35; vs 09:30 mark -6.39 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 23 | $55.97 | $2.08 | $+12.42 | $8,930.27 | ▲ +12.42 after sell → book $10,130.27; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $200.00 | $2.03 | $-39.44 | $10,128.25 | ▼ -39.44 after sell → book $10,128.25; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $9,153.02 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1266.03 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $8,016.92 | — | rank by cond; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1266.03 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,813.66 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1266.03 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 13 | $91.49 | $2.03 | — | $5,622.26 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1266.03 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 20 | $62.82 | $2.05 | — | $4,363.81 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1266.03 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $3,204.05 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1266.03 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 10 | $119.76 | $2.02 | — | $2,004.43 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1266.03 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,004.43 | ▼ close $9,771.21 vs 09:30 $10,150.68 (session -342.92) | 16:00 close · cash $2,004.43 · equity $9,771.21 vs 09:30 $10,150.68 (-379.47; session marks -342.92) · 7 name(s) marked open→close (per-name table). KEYS×3 09:30 $324.41 → close $319.97 -13.32; SMTC×8 09:30 $141.76 → close $131.17 -84.72; CIEN×3 09:30 $400.42 → close $378.44 -65.94; AVT×13 09:30 $91.49 → close $88.63 -37.18; CGNX×20 09:30 $62.82 → close $60.46 -47.20; COHR×4 09:30 $289.44 → close $279.20 -40.96; LSCC×10 09:30 $119.76 → close $114.40 -53.60 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,004.43 | ▲ 09:30 equity $9,813.49 vs yday $9,771.21 (+42.28) | 09:30 open · cash $2,004.43 (unchanged overnight, no fees) · equity $9,813.49 vs prior close $9,771.21 (+42.28) · 7 name(s) re-marked at the open (per-name table). KEYS×3 yday $319.97 → 09:30 $322.49 +7.56; SMTC×8 yday $131.17 → 09:30 $132.30 +9.04; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; AVT×13 yday $88.63 → 09:30 $89.39 +9.88; CGNX×20 yday $60.46 → 09:30 $60.46 +0.00; COHR×4 yday $279.20 → 09:30 $280.25 +4.20; LSCC×10 yday $114.40 → 09:30 $115.56 +11.60 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 3 | $322.49 | $2.02 | $-9.78 | $2,969.88 | ▼ -9.78 after sell → book $9,811.47; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $132.30 | $2.03 | $-79.73 | $4,026.25 | ▼ -79.73 after sell → book $9,809.44; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $5,159.55 | ▼ -69.96 after sell → book $9,807.42; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 13 | $89.39 | $2.05 | $-31.38 | $6,319.57 | ▼ -31.38 after sell → book $9,805.37; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 20 | $60.46 | $2.07 | $-51.32 | $7,526.70 | ▼ -51.32 after sell → book $9,803.30; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $280.25 | $2.02 | $-40.78 | $8,645.68 | ▼ -40.78 after sell → book $9,801.28; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 10 | $115.56 | $2.04 | $-46.06 | $9,799.24 | ▼ -46.06 after sell → book $9,799.24; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,799.24 | ▲ close $9,799.24 vs 09:30 $9,813.49 (session +0.00) | 16:00 close · cash $9,799.24 · no lots left · equity $9,799.24. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,799.24 | ▲ 09:30 equity $9,799.24 vs yday $9,799.24 (-0.00) | 09:30 open · cash $9,799.24 · no holdings · equity $9,799.24 vs prior close $9,799.24 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,799.24 | ▲ close $9,799.24 vs 09:30 $9,799.24 (session +0.00) | 16:00 close · cash $9,799.24 · no lots left · equity $9,799.24. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,799.24 | ▲ 09:30 equity $9,799.24 vs yday $9,799.24 (-0.00) | 09:30 open · cash $9,799.24 · no holdings · equity $9,799.24 vs prior close $9,799.24 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,799.24 | ▲ close $9,799.24 vs 09:30 $9,799.24 (session +0.00) | 16:00 close · cash $9,799.24 · no lots left · equity $9,799.24. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,799.24 | ▲ 09:30 equity $9,799.24 vs yday $9,799.24 (-0.00) | 09:30 open · cash $9,799.24 · no holdings · equity $9,799.24 vs prior close $9,799.24 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 73 | $16.77 | $2.21 | — | $8,572.82 | — | rank by cond; rank cond; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1224.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 634 | $1.93 | $8.18 | — | $7,341.02 | — | rank by cond; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1224.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 561 | $2.18 | $7.24 | — | $6,110.81 | — | rank by cond; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1224.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 28 | $42.93 | $2.07 | — | $4,906.69 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1224.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 117 | $10.42 | $2.34 | — | $3,685.21 | — | rank by cond; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1224.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PBH` | 22 | $53.45 | $2.06 | — | $2,507.25 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.3; leftover $1224.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `PCRX` | 45 | $26.74 | $2.12 | — | $1,301.83 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.6; leftover $1224.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $107.76 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1224.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.76 | ▼ close $9,577.19 vs 09:30 $9,799.24 (session -193.81) | 16:00 close · cash $107.76 · equity $9,577.19 vs 09:30 $9,799.24 (-222.05; session marks -193.81) · 8 name(s) marked open→close (per-name table). ARCT×73 09:30 $16.77 → close $15.56 -88.33; BMEA×634 09:30 $1.93 → close $1.91 -12.68; CRDL×561 09:30 $2.18 → close $2.16 -11.22; HRMY×28 09:30 $42.93 → close $41.86 -29.96; NVAX×117 09:30 $10.42 → close $10.34 -9.36; PBH×22 09:30 $53.45 → close $52.56 -19.58; PCRX×45 09:30 $26.74 → close $26.60 -6.30; RVTY×9 09:30 $132.45 → close $130.63 -16.38 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.76 | ▼ 09:30 equity $9,551.12 vs yday $9,577.19 (-26.07) | 09:30 open · cash $107.76 (unchanged overnight, no fees) · equity $9,551.12 vs prior close $9,577.19 (-26.07) · 8 name(s) re-marked at the open (per-name table). ARCT×73 yday $15.56 → 09:30 $15.61 +3.65; BMEA×634 yday $1.91 → 09:30 $1.90 -6.34; CRDL×561 yday $2.16 → 09:30 $2.16 +0.00; HRMY×28 yday $41.86 → 09:30 $41.50 -10.08; NVAX×117 yday $10.34 → 09:30 $10.50 +18.72; PBH×22 yday $52.56 → 09:30 $51.80 -16.72; PCRX×45 yday $26.60 → 09:30 $26.38 -9.90; RVTY×9 yday $130.63 → 09:30 $130.03 -5.40 | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 73 | $15.61 | $2.23 | $-89.12 | $1,245.06 | ▼ -89.12 after sell → book $9,548.89; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 561 | $2.16 | $7.34 | $-25.80 | $2,449.48 | ▼ -25.80 after sell → book $9,541.55; vs 09:30 mark -7.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 117 | $10.50 | $2.37 | $+4.65 | $3,675.61 | ▲ +4.65 after sell → book $9,539.18; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBH` | 22 | $51.80 | $2.08 | $-40.43 | $4,813.13 | ▼ -40.43 after sell → book $9,537.10; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PCRX` | 45 | $26.38 | $2.15 | $-20.47 | $5,998.09 | ▼ -20.47 after sell → book $9,534.96; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $7,166.32 | ▼ -25.83 after sell → book $9,532.92; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 345 | $3.46 | $4.45 | — | $5,968.17 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1194.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 473 | $2.52 | $6.10 | — | $4,770.11 | — | rank by cond; rank cond; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1194.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 22 | $52.03 | $2.06 | — | $3,623.39 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $1194.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 178 | $6.71 | $2.52 | — | $2,426.49 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1194.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $1,371.05 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1194.39 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 42 | $28.00 | $2.12 | — | $192.93 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+8.7; leftover $1194.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $192.93 | ▲ close $9,546.54 vs 09:30 $9,551.12 (session +32.87) | 16:00 close · cash $192.93 · equity $9,546.54 vs 09:30 $9,551.12 (-4.58; session marks +32.87) · 8 name(s) marked open→close (per-name table). BMEA×634 09:30 $1.90 → close $2.03 +82.42; HRMY×28 09:30 $41.50 → close $42.25 +21.00; CABA×345 09:30 $3.46 → close $3.47 +3.45; ALEC×473 09:30 $2.52 → close $2.46 -28.38; ATRC×22 09:30 $52.03 → close $51.52 -11.22; BHC×178 09:30 $6.71 → close $6.56 -26.70; CRM×4 09:30 $263.36 → close $259.23 -16.52; MLYS×42 09:30 $28.00 → close $28.21 +8.82 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $192.93 | ▼ 09:30 equity $9,508.04 vs yday $9,546.54 (-38.50) | 09:30 open · cash $192.93 (unchanged overnight, no fees) · equity $9,508.04 vs prior close $9,546.54 (-38.50) · 8 name(s) re-marked at the open (per-name table). BMEA×634 yday $2.03 → 09:30 $2.00 -19.02; HRMY×28 yday $42.25 → 09:30 $42.20 -1.40; CABA×345 yday $3.47 → 09:30 $3.43 -13.80; ALEC×473 yday $2.46 → 09:30 $2.38 -37.84; ATRC×22 yday $51.52 → 09:30 $54.31 +61.38; BHC×178 yday $6.56 → 09:30 $6.57 +1.78; CRM×4 yday $259.23 → 09:30 $253.72 -22.04; MLYS×42 yday $28.21 → 09:30 $28.03 -7.56 | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 634 | $2.00 | $8.29 | $+27.91 | $1,452.64 | ▲ +27.91 after sell → book $9,499.75; vs 09:30 mark -8.29 | dropped from list after 2 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `HRMY` | 28 | $42.20 | $2.09 | $-24.61 | $2,632.14 | ▼ -24.61 after sell → book $9,497.65; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 345 | $3.43 | $4.52 | $-19.32 | $3,810.98 | ▼ -19.32 after sell → book $9,493.14; vs 09:30 mark -4.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 473 | $2.38 | $6.19 | $-78.51 | $4,930.53 | ▼ -78.51 after sell → book $9,486.95; vs 09:30 mark -6.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 22 | $54.31 | $2.08 | $+46.03 | $6,123.27 | ▲ +46.03 after sell → book $9,484.87; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 178 | $6.57 | $2.56 | $-30.01 | $7,290.17 | ▼ -30.01 after sell → book $9,482.31; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $8,303.02 | ▼ -42.58 after sell → book $9,480.28; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MLYS` | 42 | $28.03 | $2.14 | $-2.99 | $9,478.15 | ▼ -2.99 after sell → book $9,478.15; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,478.15 | ▲ close $9,478.15 vs 09:30 $9,508.04 (session +0.00) | 16:00 close · cash $9,478.15 · no lots left · equity $9,478.15. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
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
| 2026-08-28 | `MPWR` | cash | leftover split 1266.03 < 1 share @ 1306.03 |
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
