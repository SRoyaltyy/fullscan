# Factor mine action — `union_news_head_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · prior-export headline🟢 only

Cash book **-14.88%** ($8,512) · signal-only (no cash/fees) was +183.55%. Starts YES **0/22**. Fills 93 · skips 137 · realized $-1240.91.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `headline=good` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $261.88.

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
| 2026-08-14 | `VELO` | 81 | — | $15.38 | +0.00 | $16.16 | +63.18 | +63.18 | +0.00 | +63.18 |
| 2026-08-14 | `FWRD` | 62 | — | $20.11 | +0.00 | $18.96 | -71.30 | -71.30 | +0.00 | -71.30 |
| 2026-08-14 | `S` | 52 | — | $23.77 | +0.00 | $23.11 | -34.58 | -34.58 | +0.00 | -34.58 |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | $4.71 | +31.90 | +98.60 | +84.10 | +116.00 |
| 2026-08-17 | `ARX` | 63 | $19.58 | $19.57 | -0.63 | $19.54 | -1.89 | -2.52 | +0.00 | -1.89 |
| 2026-08-17 | `HLIT` | 94 | $13.92 | $13.84 | -7.52 | $13.43 | -38.54 | -46.06 | +62.04 | +23.50 |
| 2026-08-17 | `MH` | 92 | $13.10 | $13.16 | +5.52 | $12.77 | -35.88 | -30.36 | -35.88 | -71.76 |
| 2026-08-17 | `VELO` | 81 | $16.16 | $16.05 | -8.91 | $15.60 | -36.86 | -45.77 | +54.27 | +17.41 |
| 2026-08-17 | `FWRD` | 62 | $18.96 | $19.20 | +14.88 | $19.32 | +7.44 | +22.32 | -56.42 | -48.98 |
| 2026-08-17 | `S` | 52 | $23.11 | $22.50 | -31.72 | $21.82 | -35.36 | -67.08 | -66.30 | -101.66 |
| 2026-08-17 | `OUST` | 13 | — | $49.00 | +0.00 | $48.13 | -11.31 | -11.31 | +0.00 | -11.31 |
| 2026-08-17 | `CELC` | 6 | — | $92.99 | +0.00 | $92.44 | -3.30 | -3.30 | +0.00 | -3.30 |
| 2026-08-18 | `ANGX` | 290 | $4.71 | $4.79 | +23.20 | $4.85 | +17.40 | +40.60 | +139.20 | +156.60 |
| 2026-08-18 | `ARX` | 63 | $19.54 | $19.57 | +1.89 | $19.56 | -0.63 | +1.26 | +0.00 | -0.63 |
| 2026-08-18 | `HLIT` | 94 | $13.43 | $12.93 | -47.00 | $12.73 | -18.80 | -65.80 | -23.50 | -42.30 |
| 2026-08-18 | `MH` | 92 | $12.77 | $13.00 | +21.16 | $13.12 | +11.04 | +32.20 | -50.60 | -39.56 |
| 2026-08-18 | `VELO` | 81 | $15.60 | $14.73 | -70.07 | $14.63 | -8.10 | -78.17 | -52.65 | -60.75 |
| 2026-08-18 | `FWRD` | 62 | $19.32 | $19.46 | +8.68 | $19.18 | -17.36 | -8.68 | -40.30 | -57.66 |
| 2026-08-18 | `S` | 52 | $21.82 | $21.65 | -8.84 | $22.49 | +43.68 | +34.84 | -110.50 | -66.82 |
| 2026-08-18 | `OUST` | 13 | $48.13 | $45.09 | -39.52 | $42.99 | -27.30 | -66.82 | -50.83 | -78.13 |
| 2026-08-18 | `CELC` | 6 | $92.44 | $92.38 | -0.36 | $93.24 | +5.16 | +4.80 | -3.66 | +1.50 |
| 2026-08-19 | `ANGX` | 290 | $4.85 | $4.79 | -17.40 | $4.60 | -55.10 | -72.50 | +139.20 | +84.10 |
| 2026-08-19 | `ARX` | 63 | $19.56 | $19.58 | +1.26 | — | +0.00 | +1.26 | +0.63 | — |
| 2026-08-19 | `HLIT` | 94 | $12.73 | $12.90 | +15.98 | — | +0.00 | +15.98 | -26.32 | — |
| 2026-08-19 | `MH` | 92 | $13.12 | $13.01 | -10.12 | — | +0.00 | -10.12 | -49.68 | — |
| 2026-08-19 | `VELO` | 81 | $14.63 | $14.51 | -9.72 | — | +0.00 | -9.72 | -70.47 | — |
| 2026-08-19 | `FWRD` | 62 | $19.18 | $19.47 | +17.98 | — | +0.00 | +17.98 | -39.68 | — |
| 2026-08-19 | `S` | 52 | $22.49 | $22.37 | -6.24 | — | +0.00 | -6.24 | -73.06 | — |
| 2026-08-19 | `OUST` | 13 | $42.99 | $43.00 | +0.13 | $40.06 | -38.22 | -38.09 | -78.00 | -116.22 |
| 2026-08-19 | `CELC` | 6 | $93.24 | $95.50 | +13.56 | $93.65 | -11.10 | +2.46 | +15.06 | +3.96 |
| 2026-08-20 | `ANGX` | 290 | $4.60 | $4.57 | -8.70 | — | +0.00 | -8.70 | +75.40 | — |
| 2026-08-20 | `OUST` | 13 | $40.06 | $40.63 | +7.41 | — | +0.00 | +7.41 | -108.81 | — |
| 2026-08-20 | `CELC` | 6 | $93.65 | $92.90 | -4.50 | — | +0.00 | -4.50 | -0.54 | — |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `AUTL` | 489 | — | $2.47 | +0.00 | $2.46 | -4.89 | -4.89 | +0.00 | -4.89 |
| 2026-08-20 | `CRSP` | 20 | — | $58.73 | +0.00 | $58.12 | -12.20 | -12.20 | +0.00 | -12.20 |
| 2026-08-20 | `ASST` | 75 | — | $16.00 | +0.00 | $16.13 | +9.75 | +9.75 | +0.00 | +9.75 |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `ZLAB` | 45 | — | $26.57 | +0.00 | $26.02 | -24.75 | -24.75 | +0.00 | -24.75 |
| 2026-08-20 | `TEAM` | 6 | — | $173.90 | +0.00 | $174.91 | +6.06 | +6.06 | +0.00 | +6.06 |
| 2026-08-20 | `HUMA` | 1708 | — | $0.71 | +0.00 | $0.68 | -44.41 | -44.41 | +0.00 | -44.41 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | $97.03 | +17.03 | +44.20 | +61.23 | +78.26 |
| 2026-08-21 | `AUTL` | 489 | $2.46 | $2.47 | +4.89 | $2.41 | -29.34 | -24.45 | +0.00 | -29.34 |
| 2026-08-21 | `CRSP` | 20 | $58.12 | $59.72 | +32.00 | $59.50 | -4.40 | +27.60 | +19.80 | +15.40 |
| 2026-08-21 | `ASST` | 75 | $16.13 | $17.66 | +114.75 | $18.22 | +42.00 | +156.75 | +124.50 | +166.50 |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | $145.13 | +96.16 | +94.48 | -136.24 | -40.08 |
| 2026-08-21 | `ZLAB` | 45 | $26.02 | $26.25 | +10.35 | $26.01 | -10.80 | -0.45 | -14.40 | -25.20 |
| 2026-08-21 | `TEAM` | 6 | $174.91 | $174.22 | -4.14 | $171.81 | -14.46 | -18.60 | +1.92 | -12.54 |
| 2026-08-21 | `HUMA` | 1708 | $0.68 | $0.67 | -11.96 | $0.64 | -54.66 | -66.62 | -56.36 | -111.02 |
| 2026-08-21 | `ABTC` | 4 | — | $8.66 | +0.00 | $7.93 | -2.92 | -2.92 | +0.00 | -2.92 |
| 2026-08-21 | `HIVE` | 11 | — | $3.24 | +0.00 | $3.03 | -2.31 | -2.31 | +0.00 | -2.31 |
| 2026-08-21 | `MARA` | 3 | — | $11.70 | +0.00 | $11.26 | -1.32 | -1.32 | +0.00 | -1.32 |
| 2026-08-21 | `BTDR` | 3 | — | $11.10 | +0.00 | $11.37 | +0.82 | +0.82 | +0.00 | +0.82 |
| 2026-08-24 | `BHP` | 13 | $97.03 | $97.31 | +3.64 | $97.13 | -2.34 | +1.30 | +81.90 | +79.56 |
| 2026-08-24 | `AUTL` | 489 | $2.41 | $2.40 | -4.89 | $2.34 | -29.34 | -34.23 | -34.23 | -63.57 |
| 2026-08-24 | `CRSP` | 20 | $59.50 | $58.75 | -15.00 | $57.08 | -33.50 | -48.50 | +0.40 | -33.10 |
| 2026-08-24 | `ASST` | 75 | $18.22 | $18.76 | +40.50 | $19.73 | +72.75 | +113.25 | +207.00 | +279.75 |
| 2026-08-24 | `MRNA` | 8 | $145.13 | $142.70 | -19.44 | $138.89 | -30.48 | -49.92 | -59.52 | -90.00 |
| 2026-08-24 | `ZLAB` | 45 | $26.01 | $25.43 | -26.10 | $25.64 | +9.45 | -16.65 | -51.30 | -41.85 |
| 2026-08-24 | `TEAM` | 6 | $171.81 | $169.30 | -15.06 | $171.33 | +12.18 | -2.88 | -27.60 | -15.42 |
| 2026-08-24 | `HUMA` | 1708 | $0.64 | $0.68 | +56.36 | $0.66 | -22.20 | +34.16 | -54.66 | -76.86 |
| 2026-08-24 | `ABTC` | 4 | $7.93 | $8.00 | +0.28 | $8.64 | +2.56 | +2.84 | -2.64 | -0.08 |
| 2026-08-24 | `HIVE` | 11 | $3.03 | $2.99 | -0.44 | $2.86 | -1.43 | -1.87 | -2.75 | -4.18 |
| 2026-08-24 | `MARA` | 3 | $11.26 | $11.17 | -0.27 | $11.18 | +0.03 | -0.24 | -1.59 | -1.56 |
| 2026-08-24 | `BTDR` | 3 | $11.37 | $11.48 | +0.33 | $10.91 | -1.71 | -1.38 | +1.15 | -0.56 |
| 2026-08-25 | `BHP` | 13 | $97.13 | $95.86 | -16.51 | — | +0.00 | -16.51 | +63.05 | — |
| 2026-08-25 | `AUTL` | 489 | $2.34 | $2.38 | +19.56 | — | +0.00 | +19.56 | -44.01 | — |
| 2026-08-25 | `CRSP` | 20 | $57.08 | $57.93 | +17.10 | — | +0.00 | +17.10 | -16.00 | — |
| 2026-08-25 | `ASST` | 75 | $19.73 | $19.04 | -51.75 | — | +0.00 | -51.75 | +228.00 | — |
| 2026-08-25 | `MRNA` | 8 | $138.89 | $143.50 | +36.88 | — | +0.00 | +36.88 | -53.12 | — |
| 2026-08-25 | `ZLAB` | 45 | $25.64 | $26.04 | +18.00 | — | +0.00 | +18.00 | -23.85 | — |
| 2026-08-25 | `TEAM` | 6 | $171.33 | $170.64 | -4.14 | — | +0.00 | -4.14 | -19.56 | — |
| 2026-08-25 | `HUMA` | 1708 | $0.66 | $0.66 | +1.71 | — | +0.00 | +1.71 | -75.15 | — |
| 2026-08-25 | `ABTC` | 4 | $8.64 | $8.62 | -0.08 | $9.24 | +2.48 | +2.40 | -0.16 | +2.32 |
| 2026-08-25 | `HIVE` | 11 | $2.86 | $2.87 | +0.11 | $3.02 | +1.65 | +1.76 | -4.07 | -2.42 |
| 2026-08-25 | `MARA` | 3 | $11.18 | $11.07 | -0.33 | $11.83 | +2.28 | +1.95 | -1.89 | +0.39 |
| 2026-08-25 | `BTDR` | 3 | $10.91 | $10.70 | -0.63 | $11.29 | +1.77 | +1.14 | -1.19 | +0.58 |
| 2026-08-25 | `EZPW` | 54 | — | $35.05 | +0.00 | $35.23 | +9.72 | +9.72 | +0.00 | +9.72 |
| 2026-08-25 | `RUM` | 201 | — | $9.42 | +0.00 | $10.23 | +162.81 | +162.81 | +0.00 | +162.81 |
| 2026-08-25 | `ZYME` | 65 | — | $28.86 | +0.00 | $27.47 | -90.35 | -90.35 | +0.00 | -90.35 |
| 2026-08-25 | `REAX` | 78 | — | $24.11 | +0.00 | $28.43 | +336.96 | +336.96 | +0.00 | +336.96 |
| 2026-08-25 | `EOLS` | 218 | — | $8.72 | +0.00 | $8.97 | +55.59 | +55.59 | +0.00 | +55.59 |
| 2026-08-26 | `ABTC` | 4 | $9.24 | $8.84 | -1.60 | — | +0.00 | -1.60 | +0.72 | — |
| 2026-08-26 | `HIVE` | 11 | $3.02 | $2.95 | -0.77 | — | +0.00 | -0.77 | -3.19 | — |
| 2026-08-26 | `MARA` | 3 | $11.83 | $11.56 | -0.81 | — | +0.00 | -0.81 | -0.42 | — |
| 2026-08-26 | `BTDR` | 3 | $11.29 | $11.05 | -0.72 | — | +0.00 | -0.72 | -0.13 | — |
| 2026-08-26 | `EZPW` | 54 | $35.23 | $35.70 | +25.38 | $33.90 | -97.20 | -71.82 | +35.10 | -62.10 |
| 2026-08-26 | `RUM` | 201 | $10.23 | $10.07 | -32.16 | $9.38 | -139.70 | -171.86 | +130.65 | -9.04 |
| 2026-08-26 | `ZYME` | 65 | $27.47 | $27.56 | +5.85 | $29.30 | +113.43 | +119.28 | -84.50 | +28.93 |
| 2026-08-26 | `REAX` | 78 | $28.43 | $26.61 | -141.96 | $26.59 | -1.56 | -143.52 | +195.00 | +193.44 |
| 2026-08-26 | `EOLS` | 218 | $8.97 | $8.86 | -25.07 | $8.78 | -17.44 | -42.51 | +30.52 | +13.08 |
| 2026-08-26 | `TRLV` | 3 | — | $11.22 | +0.00 | $11.43 | +0.63 | +0.63 | +0.00 | +0.63 |
| 2026-08-26 | `CAPR` | 4 | — | $8.29 | +0.00 | $9.36 | +4.28 | +4.28 | +0.00 | +4.28 |
| 2026-08-26 | `FWRD` | 2 | — | $17.41 | +0.00 | $17.63 | +0.44 | +0.44 | +0.00 | +0.44 |
| 2026-08-26 | `FLNC` | 3 | — | $11.12 | +0.00 | $11.08 | -0.12 | -0.12 | +0.00 | -0.12 |
| 2026-08-27 | `EZPW` | 54 | $33.90 | $33.50 | -21.60 | $34.41 | +49.14 | +27.54 | -83.70 | -34.56 |
| 2026-08-27 | `RUM` | 201 | $9.38 | $9.51 | +27.13 | $9.43 | -16.08 | +11.05 | +18.09 | +2.01 |
| 2026-08-27 | `ZYME` | 65 | $29.30 | $29.33 | +1.62 | $29.01 | -20.80 | -19.18 | +30.55 | +9.75 |
| 2026-08-27 | `REAX` | 78 | $26.59 | $25.91 | -53.04 | $23.63 | -177.84 | -230.88 | +140.40 | -37.44 |
| 2026-08-27 | `EOLS` | 218 | $8.78 | $8.78 | +0.00 | $8.88 | +21.80 | +21.80 | +13.08 | +34.88 |
| 2026-08-27 | `TRLV` | 3 | $11.43 | $11.38 | -0.15 | $11.03 | -1.05 | -1.20 | +0.48 | -0.57 |
| 2026-08-27 | `CAPR` | 4 | $9.36 | $9.19 | -0.68 | $10.06 | +3.48 | +2.80 | +3.60 | +7.08 |
| 2026-08-27 | `FWRD` | 2 | $17.63 | $17.60 | -0.06 | $17.70 | +0.20 | +0.14 | +0.38 | +0.58 |
| 2026-08-27 | `FLNC` | 3 | $11.08 | $11.52 | +1.32 | $11.43 | -0.27 | +1.05 | +1.20 | +0.93 |
| 2026-08-28 | `EZPW` | 54 | $34.41 | $34.50 | +4.86 | — | +0.00 | +4.86 | -29.70 | — |
| 2026-08-28 | `RUM` | 201 | $9.43 | $9.30 | -26.13 | — | +0.00 | -26.13 | -24.12 | — |
| 2026-08-28 | `ZYME` | 65 | $29.01 | $28.91 | -6.50 | — | +0.00 | -6.50 | +3.25 | — |
| 2026-08-28 | `REAX` | 78 | $23.63 | $23.40 | -17.94 | — | +0.00 | -17.94 | -55.38 | — |
| 2026-08-28 | `EOLS` | 218 | $8.88 | $8.84 | -8.72 | — | +0.00 | -8.72 | +26.16 | — |
| 2026-08-28 | `TRLV` | 3 | $11.03 | $11.00 | -0.09 | $11.82 | +2.46 | +2.37 | -0.66 | +1.80 |
| 2026-08-28 | `CAPR` | 4 | $10.06 | $9.73 | -1.32 | $9.59 | -0.56 | -1.88 | +5.76 | +5.20 |
| 2026-08-28 | `FWRD` | 2 | $17.70 | $17.70 | +0.00 | $16.99 | -1.42 | -1.42 | +0.58 | -0.84 |
| 2026-08-28 | `FLNC` | 3 | $11.43 | $11.27 | -0.48 | $10.88 | -1.17 | -1.65 | +0.45 | -0.72 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `SEDG` | 40 | — | $32.90 | +0.00 | $31.41 | -59.60 | -59.60 | +0.00 | -59.60 |
| 2026-08-28 | `TLS` | 278 | — | $4.82 | +0.00 | $4.79 | -8.34 | -8.34 | +0.00 | -8.34 |
| 2026-08-28 | `TH` | 70 | — | $19.00 | +0.00 | $18.55 | -31.50 | -31.50 | +0.00 | -31.50 |
| 2026-08-28 | `OPTX` | 156 | — | $8.61 | +0.00 | $8.52 | -14.04 | -14.04 | +0.00 | -14.04 |
| 2026-08-28 | `BBWI` | 71 | — | $18.75 | +0.00 | $19.22 | +33.37 | +33.37 | +0.00 | +33.37 |
| 2026-08-28 | `ERAS` | 69 | — | $19.25 | +0.00 | $18.03 | -84.18 | -84.18 | +0.00 | -84.18 |
| 2026-08-31 | `TRLV` | 3 | $11.82 | $11.80 | -0.06 | — | +0.00 | -0.06 | +1.74 | — |
| 2026-08-31 | `CAPR` | 4 | $9.59 | $9.50 | -0.36 | — | +0.00 | -0.36 | +4.84 | — |
| 2026-08-31 | `FWRD` | 2 | $16.99 | $17.03 | +0.08 | — | +0.00 | +0.08 | -0.76 | — |
| 2026-08-31 | `FLNC` | 3 | $10.88 | $10.82 | -0.18 | — | +0.00 | -0.18 | -0.90 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | $132.96 | +5.94 | +16.11 | -85.14 | -79.20 |
| 2026-08-31 | `SEDG` | 40 | $31.41 | $31.15 | -10.40 | $32.20 | +42.00 | +31.60 | -70.00 | -28.00 |
| 2026-08-31 | `TLS` | 278 | $4.79 | $4.81 | +5.56 | $4.82 | +2.78 | +8.34 | -2.78 | +0.00 |
| 2026-08-31 | `TH` | 70 | $18.55 | $18.12 | -29.75 | $18.52 | +27.65 | -2.10 | -61.25 | -33.60 |
| 2026-08-31 | `OPTX` | 156 | $8.52 | $8.52 | +0.00 | $8.19 | -51.48 | -51.48 | -14.04 | -65.52 |
| 2026-08-31 | `BBWI` | 71 | $19.22 | $19.25 | +2.13 | $19.25 | +0.00 | +2.13 | +35.50 | +35.50 |
| 2026-08-31 | `ERAS` | 69 | $18.03 | $17.87 | -11.04 | $17.88 | +0.69 | -10.35 | -95.22 | -94.53 |
| 2026-09-01 | `SMTC` | 9 | $132.96 | $127.63 | -47.97 | $132.27 | +41.76 | -6.21 | -127.17 | -85.41 |
| 2026-09-01 | `SEDG` | 40 | $32.20 | $31.87 | -13.20 | $32.49 | +24.80 | +11.60 | -41.20 | -16.40 |
| 2026-09-01 | `TLS` | 278 | $4.82 | $4.77 | -13.90 | $4.72 | -13.90 | -27.80 | -13.90 | -27.80 |
| 2026-09-01 | `TH` | 70 | $18.52 | $18.45 | -4.90 | $18.07 | -26.60 | -31.50 | -38.50 | -65.10 |
| 2026-09-01 | `OPTX` | 156 | $8.19 | $7.94 | -39.00 | $7.28 | -102.96 | -141.96 | -104.52 | -207.48 |
| 2026-09-01 | `BBWI` | 71 | $19.25 | $18.77 | -34.08 | $18.61 | -11.36 | -45.44 | +1.42 | -9.94 |
| 2026-09-01 | `ERAS` | 69 | $17.88 | $17.58 | -20.70 | $16.76 | -56.58 | -77.28 | -115.23 | -171.81 |
| 2026-09-02 | `SMTC` | 9 | $132.27 | $133.00 | +6.57 | — | +0.00 | +6.57 | -78.84 | — |
| 2026-09-02 | `SEDG` | 40 | $32.49 | $32.42 | -2.80 | — | +0.00 | -2.80 | -19.20 | — |
| 2026-09-02 | `TLS` | 278 | $4.72 | $4.73 | +2.78 | — | +0.00 | +2.78 | -25.02 | — |
| 2026-09-02 | `TH` | 70 | $18.07 | $17.98 | -6.30 | — | +0.00 | -6.30 | -71.40 | — |
| 2026-09-02 | `OPTX` | 156 | $7.28 | $7.25 | -4.68 | — | +0.00 | -4.68 | -212.16 | — |
| 2026-09-02 | `BBWI` | 71 | $18.61 | $18.41 | -14.20 | — | +0.00 | -14.20 | -24.14 | — |
| 2026-09-02 | `ERAS` | 69 | $16.76 | $16.97 | +14.49 | — | +0.00 | +14.49 | -157.32 | — |
| 2026-09-03 | `CXW` | 46 | — | $32.31 | +0.00 | $33.66 | +62.10 | +62.10 | +0.00 | +62.10 |
| 2026-09-03 | `FRNM` | 93 | — | $15.87 | +0.00 | $16.90 | +95.79 | +95.79 | +0.00 | +95.79 |
| 2026-09-03 | `MMED` | 62 | — | $23.88 | +0.00 | $23.84 | -2.48 | -2.48 | +0.00 | -2.48 |
| 2026-09-03 | `DE` | 2 | — | $703.25 | +0.00 | $694.41 | -17.68 | -17.68 | +0.00 | -17.68 |
| 2026-09-03 | `CNXC` | 45 | — | $32.88 | +0.00 | $32.85 | -1.35 | -1.35 | +0.00 | -1.35 |
| 2026-09-03 | `OPTX` | 195 | — | $7.59 | +0.00 | $7.76 | +33.15 | +33.15 | +0.00 | +33.15 |
| 2026-09-04 | `CXW` | 46 | $33.66 | $33.46 | -9.20 | $34.71 | +57.50 | +48.30 | +52.90 | +110.40 |
| 2026-09-04 | `FRNM` | 93 | $16.90 | $16.40 | -46.50 | $16.31 | -8.37 | -54.87 | +49.29 | +40.92 |
| 2026-09-04 | `MMED` | 62 | $23.84 | $23.84 | +0.00 | $23.29 | -34.10 | -34.10 | -2.48 | -36.58 |
| 2026-09-04 | `DE` | 2 | $694.41 | $692.03 | -4.76 | $693.53 | +3.00 | -1.76 | -22.44 | -19.44 |
| 2026-09-04 | `CNXC` | 45 | $32.85 | $32.48 | -16.65 | $32.16 | -14.40 | -31.05 | -18.00 | -32.40 |
| 2026-09-04 | `OPTX` | 195 | $7.76 | $7.79 | +5.85 | $7.57 | -42.90 | -37.05 | +39.00 | -3.90 |
| 2026-09-04 | `BAK` | 26 | — | $1.94 | +0.00 | $1.89 | -1.30 | -1.30 | +0.00 | -1.30 |
| 2026-09-08 | `CXW` | 46 | $34.71 | $34.49 | -10.12 | $35.05 | +25.76 | +15.64 | +100.28 | +126.04 |
| 2026-09-08 | `FRNM` | 93 | $16.31 | $16.74 | +39.99 | $15.99 | -69.75 | -29.76 | +80.91 | +11.16 |
| 2026-09-08 | `MMED` | 62 | $23.29 | $23.16 | -8.06 | $23.32 | +9.92 | +1.86 | -44.64 | -34.72 |
| 2026-09-08 | `DE` | 2 | $693.53 | $687.21 | -12.64 | $680.73 | -12.96 | -25.60 | -32.08 | -45.04 |
| 2026-09-08 | `CNXC` | 45 | $32.16 | $31.77 | -17.55 | $28.27 | -157.50 | -175.05 | -49.95 | -207.45 |
| 2026-09-08 | `OPTX` | 195 | $7.57 | $7.67 | +19.50 | $7.76 | +17.55 | +37.05 | +15.60 | +33.15 |
| 2026-09-08 | `BAK` | 26 | $1.89 | $1.94 | +1.30 | $1.92 | -0.52 | +0.78 | +0.00 | -0.52 |
| 2026-09-09 | `CXW` | 46 | $35.05 | $35.09 | +1.84 | — | +0.00 | +1.84 | +127.88 | — |
| 2026-09-09 | `FRNM` | 93 | $15.99 | $15.96 | -2.79 | — | +0.00 | -2.79 | +8.37 | — |
| 2026-09-09 | `MMED` | 62 | $23.32 | $23.22 | -6.20 | — | +0.00 | -6.20 | -40.92 | — |
| 2026-09-09 | `DE` | 2 | $680.73 | $681.32 | +1.18 | — | +0.00 | +1.18 | -43.86 | — |
| 2026-09-09 | `CNXC` | 45 | $28.27 | $28.13 | -6.30 | — | +0.00 | -6.30 | -213.75 | — |
| 2026-09-09 | `OPTX` | 195 | $7.76 | $7.72 | -7.80 | — | +0.00 | -7.80 | +25.35 | — |
| 2026-09-09 | `BAK` | 26 | $1.92 | $2.02 | +2.47 | $1.97 | -1.17 | +1.30 | +1.95 | +0.78 |
| 2026-09-10 | `BAK` | 26 | $1.97 | $1.97 | +0.00 | — | +0.00 | +0.00 | +0.78 | — |
| 2026-09-11 | `ORCL` | 10 | — | $164.43 | +0.00 | $150.28 | -141.50 | -141.50 | +0.00 | -141.50 |
| 2026-09-11 | `ADBE` | 7 | — | $242.17 | +0.00 | $252.23 | +70.42 | +70.42 | +0.00 | +70.42 |
| 2026-09-11 | `BAK` | 826 | — | $2.12 | +0.00 | $2.08 | -33.04 | -33.04 | +0.00 | -33.04 |
| 2026-09-11 | `AMTX` | 858 | — | $2.04 | +0.00 | $2.01 | -25.74 | -25.74 | +0.00 | -25.74 |
| 2026-09-11 | `RH` | 12 | — | $135.71 | +0.00 | $134.07 | -19.68 | -19.68 | +0.00 | -19.68 |
| 2026-09-14 | `ORCL` | 10 | $150.28 | $141.42 | -88.60 | $144.72 | +33.00 | -55.60 | -230.10 | -197.10 |
| 2026-09-14 | `ADBE` | 7 | $252.23 | $261.51 | +64.96 | $265.60 | +28.63 | +93.59 | +135.38 | +164.01 |
| 2026-09-14 | `BAK` | 826 | $2.08 | $2.05 | -24.78 | $2.01 | -33.04 | -57.82 | -57.82 | -90.86 |
| 2026-09-14 | `AMTX` | 858 | $2.01 | $2.01 | +0.00 | $1.95 | -51.48 | -51.48 | -25.74 | -77.22 |
| 2026-09-14 | `RH` | 12 | $134.07 | $131.40 | -32.04 | $134.17 | +33.24 | +1.20 | -51.72 | -18.48 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +3.49 | ANGX, ARX, HLIT, MH, VELO, FWRD, S | — | $1,285.76 | $9,986.48 | ANGX×290, ARX×63, HLIT×94, MH×92, VELO×81, FWRD×62, S×52 |
| 2026-08-17 | +2.25 | $1,285.76 | ANGX×290, ARX×63, HLIT×94, MH×92, VELO×81, FWRD×62, S×52 | $10,024.80 | +38.32 | -123.80 | OUST, CELC | — | $86.78 | $9,896.97 | ANGX×290, ARX×63, HLIT×94, MH×92, VELO×81, FWRD×62, S×52, OUST×13, CELC×6 |
| 2026-08-18 | -6.20 | $86.78 | ANGX×290, ARX×63, HLIT×94, MH×92, VELO×81, FWRD×62, S×52, OUST×13, CELC×6 | $9,786.11 | -110.86 | +5.09 | — | — | $86.78 | $9,791.20 | ANGX×290, ARX×63, HLIT×94, MH×92, VELO×81, FWRD×62, S×52, OUST×13, CELC×6 |
| 2026-08-19 | -7.20 | $86.78 | ANGX×290, ARX×63, HLIT×94, MH×92, VELO×81, FWRD×62, S×52, OUST×13, CELC×6 | $9,796.63 | +5.43 | -104.42 | — | ARX, HLIT, MH, VELO, FWRD, S | $7,262.12 | $9,678.80 | ANGX×290, OUST×13, CELC×6 |
| 2026-08-20 | +1.12 | $7,262.12 | ANGX×290, OUST×13, CELC×6 | $9,673.01 | -5.79 | -170.94 | BHP, AUTL, CRSP, ASST, MRNA, ZLAB, TEAM, HUMA | ANGX, OUST, CELC | $215.90 | $9,458.25 | BHP×13, AUTL×489, CRSP×20, ASST×75, MRNA×8, ZLAB×45, TEAM×6, HUMA×1708 |
| 2026-08-21 | +3.25 | $215.90 | BHP×13, AUTL×489, CRSP×20, ASST×75, MRNA×8, ZLAB×45, TEAM×6, HUMA×1708 | $9,629.63 | +171.38 | +35.80 | ABTC, HIVE, MARA, BTDR | — | $75.79 | $9,663.99 | BHP×13, AUTL×489, CRSP×20, ASST×75, MRNA×8, ZLAB×45, TEAM×6, HUMA×1708, ABTC×4, HIVE×11, MARA×3, BTDR×3 |
| 2026-08-24 | -5.17 | $75.79 | BHP×13, AUTL×489, CRSP×20, ASST×75, MRNA×8, ZLAB×45, TEAM×6, HUMA×1708, ABTC×4, HIVE×11, MARA×3, BTDR×3 | $9,683.91 | +19.92 | -24.03 | — | — | $75.79 | $9,659.87 | BHP×13, AUTL×489, CRSP×20, ASST×75, MRNA×8, ZLAB×45, TEAM×6, HUMA×1708, ABTC×4, HIVE×11, MARA×3, BTDR×3 |
| 2026-08-25 | +1.80 | $75.79 | BHP×13, AUTL×489, CRSP×20, ASST×75, MRNA×8, ZLAB×45, TEAM×6, HUMA×1708, ABTC×4, HIVE×11, MARA×3, BTDR×3 | $9,679.79 | +19.92 | +482.91 | EZPW, RUM, ZYME, REAX, EOLS | BHP, AUTL, CRSP, ASST, MRNA, ZLAB, TEAM, HUMA | $57.19 | $10,115.02 | ABTC×4, HIVE×11, MARA×3, BTDR×3, EZPW×54, RUM×201, ZYME×65, REAX×78, EOLS×218 |
| 2026-08-26 | +2.02 | $57.19 | ABTC×4, HIVE×11, MARA×3, BTDR×3, EZPW×54, RUM×201, ZYME×65, REAX×78, EOLS×218 | $9,943.16 | -171.86 | -137.24 | TRLV, CAPR, FWRD, FLNC | ABTC, HIVE, MARA, BTDR | $54.95 | $9,803.04 | EZPW×54, RUM×201, ZYME×65, REAX×78, EOLS×218, TRLV×3, CAPR×4, FWRD×2, FLNC×3 |
| 2026-08-27 | — | $54.95 | EZPW×54, RUM×201, ZYME×65, REAX×78, EOLS×218, TRLV×3, CAPR×4, FWRD×2, FLNC×3 | $9,757.59 | -45.45 | -141.42 | — | — | $54.95 | $9,616.17 | EZPW×54, RUM×201, ZYME×65, REAX×78, EOLS×218, TRLV×3, CAPR×4, FWRD×2, FLNC×3 |
| 2026-08-28 | +0.75 | $54.95 | EZPW×54, RUM×201, ZYME×65, REAX×78, EOLS×218, TRLV×3, CAPR×4, FWRD×2, FLNC×3 | $9,559.85 | -56.32 | -260.29 | SMTC, SEDG, TLS, TH, OPTX, BBWI, ERAS | EZPW, RUM, ZYME, REAX, EOLS | $125.34 | $9,270.64 | TRLV×3, CAPR×4, FWRD×2, FLNC×3, SMTC×9, SEDG×40, TLS×278, TH×70, OPTX×156, BBWI×71, ERAS×69 |
| 2026-08-31 | -5.85 | $125.34 | TRLV×3, CAPR×4, FWRD×2, FLNC×3, SMTC×9, SEDG×40, TLS×278, TH×70, OPTX×156, BBWI×71, ERAS×69 | $9,236.79 | -33.85 | +27.58 | — | TRLV, CAPR, FWRD, FLNC | $263.74 | $9,262.85 | SMTC×9, SEDG×40, TLS×278, TH×70, OPTX×156, BBWI×71, ERAS×69 |
| 2026-09-01 | -6.30 | $263.74 | SMTC×9, SEDG×40, TLS×278, TH×70, OPTX×156, BBWI×71, ERAS×69 | $9,089.10 | -173.75 | -144.84 | — | — | $263.74 | $8,944.26 | SMTC×9, SEDG×40, TLS×278, TH×70, OPTX×156, BBWI×71, ERAS×69 |
| 2026-09-02 | -3.83 | $263.74 | SMTC×9, SEDG×40, TLS×278, TH×70, OPTX×156, BBWI×71, ERAS×69 | $8,940.12 | -4.14 | +0.00 | — | SMTC, SEDG, TLS, TH, OPTX, BBWI, ERAS | $8,923.15 | $8,923.15 | — |
| 2026-09-03 | -0.90 | $8,923.15 | — | $8,923.15 | +0.00 | +169.53 | CXW, FRNM, MMED, DE, CNXC, OPTX | — | $101.00 | $9,079.41 | CXW×46, FRNM×93, MMED×62, DE×2, CNXC×45, OPTX×195 |
| 2026-09-04 | +2.25 | $101.00 | CXW×46, FRNM×93, MMED×62, DE×2, CNXC×45, OPTX×195 | $9,008.15 | -71.26 | -40.57 | BAK | — | $49.98 | $8,967.00 | CXW×46, FRNM×93, MMED×62, DE×2, CNXC×45, OPTX×195, BAK×26 |
| 2026-09-08 | -11.47 | $49.98 | CXW×46, FRNM×93, MMED×62, DE×2, CNXC×45, OPTX×195, BAK×26 | $8,979.42 | +12.42 | -187.50 | — | — | $49.98 | $8,791.92 | CXW×46, FRNM×93, MMED×62, DE×2, CNXC×45, OPTX×195, BAK×26 |
| 2026-09-09 | -13.95 | $49.98 | CXW×46, FRNM×93, MMED×62, DE×2, CNXC×45, OPTX×195, BAK×26 | $8,774.32 | -17.60 | -1.17 | — | CXW, FRNM, MMED, DE, CNXC, OPTX | $8,708.51 | $8,759.73 | BAK×26 |
| 2026-09-10 | -13.28 | $8,708.51 | BAK×26 | $8,759.73 | -0.00 | +0.00 | — | BAK | $8,759.12 | $8,759.12 | — |
| 2026-09-11 | +0.50 | $8,759.12 | — | $8,759.12 | -0.00 | -149.54 | ORCL, ADBE, BAK, AMTX, RH | — | $261.88 | $8,581.79 | ORCL×10, ADBE×7, BAK×826, AMTX×858, RH×12 |
| 2026-09-14 | -11.00 | $261.88 | ORCL×10, ADBE×7, BAK×826, AMTX×858, RH×12 | $8,501.33 | -80.46 | +10.35 | — | — | $261.88 | $8,511.68 | ORCL×10, ADBE×7, BAK×826, AMTX×858, RH×12 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $8,746.36 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $7,511.27 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $6,270.08 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $5,021.21 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 81 | $15.38 | $2.23 | — | $3,773.20 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `FWRD` | 62 | $20.11 | $2.18 | — | $2,524.20 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+13.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `S` | 52 | $23.77 | $2.15 | — | $1,285.76 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟡 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,285.76 | ▲ close $9,986.48 vs 09:30 $10,000.00 (session +3.49) | 16:00 close · cash $1,285.76 · equity $9,986.48 vs 09:30 $10,000.00 (-13.52; session marks +3.49) · 7 name(s) marked open→close (per-name table). ANGX×290 09:30 $4.31 → close $4.37 +17.40; ARX×63 09:30 $19.57 → close $19.58 +0.63; HLIT×94 09:30 $13.18 → close $13.92 +69.56; MH×92 09:30 $13.55 → close $13.10 -41.40; VELO×81 09:30 $15.38 → close $16.16 +63.18; FWRD×62 09:30 $20.11 → close $18.96 -71.30; S×52 09:30 $23.77 → close $23.11 -34.58 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,285.76 | ▲ 09:30 equity $10,024.80 vs yday $9,986.48 (+38.32) | 09:30 open · cash $1,285.76 (unchanged overnight, no fees) · equity $10,024.80 vs prior close $9,986.48 (+38.32) · 7 name(s) re-marked at the open (per-name table). ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; HLIT×94 yday $13.92 → 09:30 $13.84 -7.52; MH×92 yday $13.10 → 09:30 $13.16 +5.52; VELO×81 yday $16.16 → 09:30 $16.05 -8.91; FWRD×62 yday $18.96 → 09:30 $19.20 +14.88; S×52 yday $23.11 → 09:30 $22.50 -31.72 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 13 | $49.00 | $2.03 | — | $646.73 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $642.88 | join🟡 sector🟢 gen🟢 news🟢 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 6 | $92.99 | $2.01 | — | $86.78 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer; ret5=-0.8; leftover $642.88 | join🟡 sector🔴 gen🟢 news🟢 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.78 | ▼ close $9,896.97 vs 09:30 $10,024.80 (session -123.80) | 16:00 close · cash $86.78 · equity $9,896.97 vs 09:30 $10,024.80 (-127.83; session marks -123.80) · 9 name(s) marked open→close (per-name table). ANGX×290 09:30 $4.60 → close $4.71 +31.90; ARX×63 09:30 $19.57 → close $19.54 -1.89; HLIT×94 09:30 $13.84 → close $13.43 -38.54; MH×92 09:30 $13.16 → close $12.77 -35.88; VELO×81 09:30 $16.05 → close $15.60 -36.86; FWRD×62 09:30 $19.20 → close $19.32 +7.44; S×52 09:30 $22.50 → close $21.82 -35.36; OUST×13 09:30 $49.00 → close $48.13 -11.31; CELC×6 09:30 $92.99 → close $92.44 -3.30 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.78 | ▼ 09:30 equity $9,786.11 vs yday $9,896.97 (-110.86) | 09:30 open · cash $86.78 (unchanged overnight, no fees) · equity $9,786.11 vs prior close $9,896.97 (-110.86) · 9 name(s) re-marked at the open (per-name table). ANGX×290 yday $4.71 → 09:30 $4.79 +23.20; ARX×63 yday $19.54 → 09:30 $19.57 +1.89; HLIT×94 yday $13.43 → 09:30 $12.93 -47.00; MH×92 yday $12.77 → 09:30 $13.00 +21.16; VELO×81 yday $15.60 → 09:30 $14.73 -70.07; FWRD×62 yday $19.32 → 09:30 $19.46 +8.68; S×52 yday $21.82 → 09:30 $21.65 -8.84; OUST×13 yday $48.13 → 09:30 $45.09 -39.52; CELC×6 yday $92.44 → 09:30 $92.38 -0.36 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.78 | ▲ close $9,791.20 vs 09:30 $9,786.11 (session +5.09) | 16:00 close · cash $86.78 · equity $9,791.20 vs 09:30 $9,786.11 (+5.09; session marks +5.09) · 9 name(s) marked open→close (per-name table). ANGX×290 09:30 $4.79 → close $4.85 +17.40; ARX×63 09:30 $19.57 → close $19.56 -0.63; HLIT×94 09:30 $12.93 → close $12.73 -18.80; MH×92 09:30 $13.00 → close $13.12 +11.04; VELO×81 09:30 $14.73 → close $14.63 -8.10; FWRD×62 09:30 $19.46 → close $19.18 -17.36; S×52 09:30 $21.65 → close $22.49 +43.68; OUST×13 09:30 $45.09 → close $42.99 -27.30; CELC×6 09:30 $92.38 → close $93.24 +5.16 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.78 | ▲ 09:30 equity $9,796.63 vs yday $9,791.20 (+5.43) | 09:30 open · cash $86.78 (unchanged overnight, no fees) · equity $9,796.63 vs prior close $9,791.20 (+5.43) · 9 name(s) re-marked at the open (per-name table). ANGX×290 yday $4.85 → 09:30 $4.79 -17.40; ARX×63 yday $19.56 → 09:30 $19.58 +1.26; HLIT×94 yday $12.73 → 09:30 $12.90 +15.98; MH×92 yday $13.12 → 09:30 $13.01 -10.12; VELO×81 yday $14.63 → 09:30 $14.51 -9.72; FWRD×62 yday $19.18 → 09:30 $19.47 +17.98; S×52 yday $22.49 → 09:30 $22.37 -6.24; OUST×13 yday $42.99 → 09:30 $43.00 +0.13; CELC×6 yday $93.24 → 09:30 $95.50 +13.56 | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 63 | $19.58 | $2.20 | $-3.75 | $1,318.12 | ▼ -3.75 after sell → book $9,794.43; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `HLIT` | 94 | $12.90 | $2.30 | $-30.89 | $2,528.42 | ▼ -30.89 after sell → book $9,792.13; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MH` | 92 | $13.01 | $2.29 | $-54.24 | $3,723.05 | ▼ -54.24 after sell → book $9,789.84; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VELO` | 81 | $14.51 | $2.26 | $-74.96 | $4,896.11 | ▼ -74.96 after sell → book $9,787.59; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `FWRD` | 62 | $19.47 | $2.20 | $-44.05 | $6,101.05 | ▼ -44.05 after sell → book $9,785.39; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `S` | 52 | $22.37 | $2.17 | $-77.37 | $7,262.12 | ▼ -77.37 after sell → book $9,783.22; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,262.12 | ▼ close $9,678.80 vs 09:30 $9,796.63 (session -104.42) | 16:00 close · cash $7,262.12 · equity $9,678.80 vs 09:30 $9,796.63 (-117.83; session marks -104.42) · 3 name(s) marked open→close (per-name table). ANGX×290 09:30 $4.79 → close $4.60 -55.10; OUST×13 09:30 $43.00 → close $40.06 -38.22; CELC×6 09:30 $95.50 → close $93.65 -11.10 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,262.12 | ▼ 09:30 equity $9,673.01 vs yday $9,678.80 (-5.79) | 09:30 open · cash $7,262.12 (unchanged overnight, no fees) · equity $9,673.01 vs prior close $9,678.80 (-5.79) · 3 name(s) re-marked at the open (per-name table). ANGX×290 yday $4.60 → 09:30 $4.57 -8.70; OUST×13 yday $40.06 → 09:30 $40.63 +7.41; CELC×6 yday $93.65 → 09:30 $92.90 -4.50 | — |
| 2026-08-20 09:30 ET | **SELL** | `ANGX` | 290 | $4.57 | $3.80 | $+67.86 | $8,583.62 | ▲ +67.86 after sell → book $9,669.21; vs 09:30 mark -3.80 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `OUST` | 13 | $40.63 | $2.05 | $-112.89 | $9,109.76 | ▼ -112.89 after sell → book $9,667.16; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `CELC` | 6 | $92.90 | $2.03 | $-4.58 | $9,665.14 | ▼ -4.58 after sell → book $9,665.14; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,479.98 | — | prior-export headline🟢 only; gate headline=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1208.14 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 489 | $2.47 | $6.31 | — | $7,265.84 | — | prior-export headline🟢 only; gate headline=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1208.14 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 20 | $58.73 | $2.05 | — | $6,089.19 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1208.14 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 75 | $16.00 | $2.21 | — | $4,886.97 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1208.14 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $3,683.84 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1208.14 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 45 | $26.57 | $2.12 | — | $2,486.07 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1208.14 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TEAM` | 6 | $173.90 | $2.01 | — | $1,440.66 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+12.2; leftover $1208.14 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1708 | $0.71 | $17.20 | — | $215.90 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1208.14 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $215.90 | ▼ close $9,458.25 vs 09:30 $9,673.01 (session -170.94) | 16:00 close · cash $215.90 · equity $9,458.25 vs 09:30 $9,673.01 (-214.76; session marks -170.94) · 8 name(s) marked open→close (per-name table). BHP×13 09:30 $91.01 → close $93.63 +34.06; AUTL×489 09:30 $2.47 → close $2.46 -4.89; CRSP×20 09:30 $58.73 → close $58.12 -12.20; ASST×75 09:30 $16.00 → close $16.13 +9.75; MRNA×8 09:30 $150.14 → close $133.32 -134.56; ZLAB×45 09:30 $26.57 → close $26.02 -24.75; TEAM×6 09:30 $173.90 → close $174.91 +6.06; HUMA×1708 09:30 $0.71 → close $0.68 -44.41 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $215.90 | ▲ 09:30 equity $9,629.63 vs yday $9,458.25 (+171.38) | 09:30 open · cash $215.90 (unchanged overnight, no fees) · equity $9,629.63 vs prior close $9,458.25 (+171.38) · 8 name(s) re-marked at the open (per-name table). BHP×13 yday $93.63 → 09:30 $95.72 +27.17; AUTL×489 yday $2.46 → 09:30 $2.47 +4.89; CRSP×20 yday $58.12 → 09:30 $59.72 +32.00; ASST×75 yday $16.13 → 09:30 $17.66 +114.75; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; ZLAB×45 yday $26.02 → 09:30 $26.25 +10.35; TEAM×6 yday $174.91 → 09:30 $174.22 -4.14; HUMA×1708 yday $0.68 → 09:30 $0.67 -11.96 | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 4 | $8.66 | $0.36 | — | $180.90 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $35.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 11 | $3.24 | $0.39 | — | $144.87 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $35.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 3 | $11.70 | $0.36 | — | $109.41 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $35.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 3 | $11.10 | $0.34 | — | $75.79 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ret5=+19.1; leftover $35.98 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.79 | ▲ close $9,663.99 vs 09:30 $9,629.63 (session +35.80) | 16:00 close · cash $75.79 · equity $9,663.99 vs 09:30 $9,629.63 (+34.36; session marks +35.80) · 12 name(s) marked open→close (per-name table). BHP×13 09:30 $95.72 → close $97.03 +17.03; AUTL×489 09:30 $2.47 → close $2.41 -29.34; CRSP×20 09:30 $59.72 → close $59.50 -4.40; ASST×75 09:30 $17.66 → close $18.22 +42.00; MRNA×8 09:30 $133.11 → close $145.13 +96.16; ZLAB×45 09:30 $26.25 → close $26.01 -10.80; TEAM×6 09:30 $174.22 → close $171.81 -14.46; HUMA×1708 09:30 $0.67 → close $0.64 -54.66; ABTC×4 09:30 $8.66 → close $7.93 -2.92; HIVE×11 09:30 $3.24 → close $3.03 -2.31; MARA×3 09:30 $11.70 → close $11.26 -1.32; BTDR×3 09:30 $11.10 → close $11.37 +0.82 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.79 | ▲ 09:30 equity $9,683.91 vs yday $9,663.99 (+19.92) | 09:30 open · cash $75.79 (unchanged overnight, no fees) · equity $9,683.91 vs prior close $9,663.99 (+19.92) · 12 name(s) re-marked at the open (per-name table). BHP×13 yday $97.03 → 09:30 $97.31 +3.64; AUTL×489 yday $2.41 → 09:30 $2.40 -4.89; CRSP×20 yday $59.50 → 09:30 $58.75 -15.00; ASST×75 yday $18.22 → 09:30 $18.76 +40.50; MRNA×8 yday $145.13 → 09:30 $142.70 -19.44; ZLAB×45 yday $26.01 → 09:30 $25.43 -26.10; TEAM×6 yday $171.81 → 09:30 $169.30 -15.06; HUMA×1708 yday $0.64 → 09:30 $0.68 +56.36; ABTC×4 yday $7.93 → 09:30 $8.00 +0.28; HIVE×11 yday $3.03 → 09:30 $2.99 -0.44; MARA×3 yday $11.26 → 09:30 $11.17 -0.27; BTDR×3 yday $11.37 → 09:30 $11.48 +0.33 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.79 | ▼ close $9,659.87 vs 09:30 $9,683.91 (session -24.03) | 16:00 close · cash $75.79 · equity $9,659.87 vs 09:30 $9,683.91 (-24.04; session marks -24.03) · 12 name(s) marked open→close (per-name table). BHP×13 09:30 $97.31 → close $97.13 -2.34; AUTL×489 09:30 $2.40 → close $2.34 -29.34; CRSP×20 09:30 $58.75 → close $57.08 -33.50; ASST×75 09:30 $18.76 → close $19.73 +72.75; MRNA×8 09:30 $142.70 → close $138.89 -30.48; ZLAB×45 09:30 $25.43 → close $25.64 +9.45; TEAM×6 09:30 $169.30 → close $171.33 +12.18; HUMA×1708 09:30 $0.68 → close $0.66 -22.20; ABTC×4 09:30 $8.00 → close $8.64 +2.56; HIVE×11 09:30 $2.99 → close $2.86 -1.43; MARA×3 09:30 $11.17 → close $11.18 +0.03; BTDR×3 09:30 $11.48 → close $10.91 -1.71 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.79 | ▲ 09:30 equity $9,679.79 vs yday $9,659.87 (+19.92) | 09:30 open · cash $75.79 (unchanged overnight, no fees) · equity $9,679.79 vs prior close $9,659.87 (+19.92) · 12 name(s) re-marked at the open (per-name table). BHP×13 yday $97.13 → 09:30 $95.86 -16.51; AUTL×489 yday $2.34 → 09:30 $2.38 +19.56; CRSP×20 yday $57.08 → 09:30 $57.93 +17.10; ASST×75 yday $19.73 → 09:30 $19.04 -51.75; MRNA×8 yday $138.89 → 09:30 $143.50 +36.88; ZLAB×45 yday $25.64 → 09:30 $26.04 +18.00; TEAM×6 yday $171.33 → 09:30 $170.64 -4.14; HUMA×1708 yday $0.66 → 09:30 $0.66 +1.71; ABTC×4 yday $8.64 → 09:30 $8.62 -0.08; HIVE×11 yday $2.86 → 09:30 $2.87 +0.11; MARA×3 yday $11.18 → 09:30 $11.07 -0.33; BTDR×3 yday $10.91 → 09:30 $10.70 -0.63 | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.86 | $2.05 | $+58.97 | $1,319.92 | ▲ +58.97 after sell → book $9,677.74; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 489 | $2.38 | $6.40 | $-56.72 | $2,477.34 | ▼ -56.72 after sell → book $9,671.34; vs 09:30 mark -6.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 20 | $57.93 | $2.07 | $-20.12 | $3,633.87 | ▼ -20.12 after sell → book $9,669.27; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ASST` | 75 | $19.04 | $2.24 | $+223.55 | $5,059.63 | ▲ +223.55 after sell → book $9,667.03; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $143.50 | $2.03 | $-57.17 | $6,205.60 | ▼ -57.17 after sell → book $9,665.00; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ZLAB` | 45 | $26.04 | $2.15 | $-28.12 | $7,375.25 | ▼ -28.12 after sell → book $9,662.85; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TEAM` | 6 | $170.64 | $2.03 | $-23.60 | $8,397.06 | ▼ -23.60 after sell → book $9,660.83; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HUMA` | 1708 | $0.66 | $16.74 | $-109.09 | $9,512.72 | ▼ -109.09 after sell → book $9,644.08; vs 09:30 mark -16.75 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 54 | $35.05 | $2.15 | — | $7,617.87 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1902.54 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 201 | $9.42 | $2.60 | — | $5,721.85 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1902.54 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 65 | $28.86 | $2.19 | — | $3,843.77 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+13.7; leftover $1902.54 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 78 | $24.11 | $2.22 | — | $1,960.97 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=+891.7; leftover $1902.54 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 218 | $8.72 | $2.81 | — | $57.19 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+13.0; leftover $1902.54 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.19 | ▲ close $10,115.02 vs 09:30 $9,679.79 (session +482.91) | 16:00 close · cash $57.19 · equity $10,115.02 vs 09:30 $9,679.79 (+435.23; session marks +482.91) · 9 name(s) marked open→close (per-name table). ABTC×4 09:30 $8.62 → close $9.24 +2.48; HIVE×11 09:30 $2.87 → close $3.02 +1.65; MARA×3 09:30 $11.07 → close $11.83 +2.28; BTDR×3 09:30 $10.70 → close $11.29 +1.77; EZPW×54 09:30 $35.05 → close $35.23 +9.72; RUM×201 09:30 $9.42 → close $10.23 +162.81; ZYME×65 09:30 $28.86 → close $27.47 -90.35; REAX×78 09:30 $24.11 → close $28.43 +336.96; EOLS×218 09:30 $8.72 → close $8.97 +55.59 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.19 | ▼ 09:30 equity $9,943.16 vs yday $10,115.02 (-171.86) | 09:30 open · cash $57.19 (unchanged overnight, no fees) · equity $9,943.16 vs prior close $10,115.02 (-171.86) · 9 name(s) re-marked at the open (per-name table). ABTC×4 yday $9.24 → 09:30 $8.84 -1.60; HIVE×11 yday $3.02 → 09:30 $2.95 -0.77; MARA×3 yday $11.83 → 09:30 $11.56 -0.81; BTDR×3 yday $11.29 → 09:30 $11.05 -0.72; EZPW×54 yday $35.23 → 09:30 $35.70 +25.38; RUM×201 yday $10.23 → 09:30 $10.07 -32.16; ZYME×65 yday $27.47 → 09:30 $27.56 +5.85; REAX×78 yday $28.43 → 09:30 $26.61 -141.96; EOLS×218 yday $8.97 → 09:30 $8.86 -25.07 | — |
| 2026-08-26 09:30 ET | **SELL** | `ABTC` | 4 | $8.84 | $0.39 | $-0.02 | $92.17 | ▼ -0.02 after sell → book $9,942.78; vs 09:30 mark -0.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `HIVE` | 11 | $2.95 | $0.38 | $-3.96 | $124.24 | ▼ -3.96 after sell → book $9,942.40; vs 09:30 mark -0.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MARA` | 3 | $11.56 | $0.38 | $-1.16 | $158.54 | ▼ -1.16 after sell → book $9,942.02; vs 09:30 mark -0.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BTDR` | 3 | $11.05 | $0.36 | $-0.84 | $191.33 | ▼ -0.84 after sell → book $9,941.66; vs 09:30 mark -0.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 3 | $11.22 | $0.35 | — | $157.33 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+16.8; leftover $38.27 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 4 | $8.29 | $0.34 | — | $123.83 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $38.27 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 2 | $17.41 | $0.35 | — | $88.65 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-9.2; leftover $38.27 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 3 | $11.12 | $0.34 | — | $54.95 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $38.27 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.95 | ▼ close $9,803.04 vs 09:30 $9,943.16 (session -137.24) | 16:00 close · cash $54.95 · equity $9,803.04 vs 09:30 $9,943.16 (-140.12; session marks -137.24) · 9 name(s) marked open→close (per-name table). EZPW×54 09:30 $35.70 → close $33.90 -97.20; RUM×201 09:30 $10.07 → close $9.38 -139.70; ZYME×65 09:30 $27.56 → close $29.30 +113.43; REAX×78 09:30 $26.61 → close $26.59 -1.56; EOLS×218 09:30 $8.86 → close $8.78 -17.44; TRLV×3 09:30 $11.22 → close $11.43 +0.63; CAPR×4 09:30 $8.29 → close $9.36 +4.28; FWRD×2 09:30 $17.41 → close $17.63 +0.44; FLNC×3 09:30 $11.12 → close $11.08 -0.12 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.95 | ▼ 09:30 equity $9,757.59 vs yday $9,803.04 (-45.45) | 09:30 open · cash $54.95 (unchanged overnight, no fees) · equity $9,757.59 vs prior close $9,803.04 (-45.45) · 9 name(s) re-marked at the open (per-name table). EZPW×54 yday $33.90 → 09:30 $33.50 -21.60; RUM×201 yday $9.38 → 09:30 $9.51 +27.13; ZYME×65 yday $29.30 → 09:30 $29.33 +1.62; REAX×78 yday $26.59 → 09:30 $25.91 -53.04; EOLS×218 yday $8.78 → 09:30 $8.78 +0.00; TRLV×3 yday $11.43 → 09:30 $11.38 -0.15; CAPR×4 yday $9.36 → 09:30 $9.19 -0.68; FWRD×2 yday $17.63 → 09:30 $17.60 -0.06; FLNC×3 yday $11.08 → 09:30 $11.52 +1.32 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.95 | ▼ close $9,616.17 vs 09:30 $9,757.59 (session -141.42) | 16:00 close · cash $54.95 · equity $9,616.17 vs 09:30 $9,757.59 (-141.42; session marks -141.42) · 9 name(s) marked open→close (per-name table). EZPW×54 09:30 $33.50 → close $34.41 +49.14; RUM×201 09:30 $9.51 → close $9.43 -16.08; ZYME×65 09:30 $29.33 → close $29.01 -20.80; REAX×78 09:30 $25.91 → close $23.63 -177.84; EOLS×218 09:30 $8.78 → close $8.88 +21.80; TRLV×3 09:30 $11.38 → close $11.03 -1.05; CAPR×4 09:30 $9.19 → close $10.06 +3.48; FWRD×2 09:30 $17.60 → close $17.70 +0.20; FLNC×3 09:30 $11.52 → close $11.43 -0.27 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.95 | ▼ 09:30 equity $9,559.85 vs yday $9,616.17 (-56.32) | 09:30 open · cash $54.95 (unchanged overnight, no fees) · equity $9,559.85 vs prior close $9,616.17 (-56.32) · 9 name(s) re-marked at the open (per-name table). EZPW×54 yday $34.41 → 09:30 $34.50 +4.86; RUM×201 yday $9.43 → 09:30 $9.30 -26.13; ZYME×65 yday $29.01 → 09:30 $28.91 -6.50; REAX×78 yday $23.63 → 09:30 $23.40 -17.94; EOLS×218 yday $8.88 → 09:30 $8.84 -8.72; TRLV×3 yday $11.03 → 09:30 $11.00 -0.09; CAPR×4 yday $10.06 → 09:30 $9.73 -1.32; FWRD×2 yday $17.70 → 09:30 $17.70 +0.00; FLNC×3 yday $11.43 → 09:30 $11.27 -0.48 | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 54 | $34.50 | $2.18 | $-34.03 | $1,915.77 | ▼ -34.03 after sell → book $9,557.67; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 201 | $9.30 | $2.65 | $-29.36 | $3,782.43 | ▼ -29.36 after sell → book $9,555.03; vs 09:30 mark -2.64 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZYME` | 65 | $28.91 | $2.21 | $-1.15 | $5,659.36 | ▼ -1.15 after sell → book $9,552.81; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `REAX` | 78 | $23.40 | $2.25 | $-59.86 | $7,482.31 | ▼ -59.86 after sell → book $9,550.56; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EOLS` | 218 | $8.84 | $2.86 | $+20.48 | $9,406.57 | ▲ +20.48 after sell → book $9,547.70; vs 09:30 mark -2.86 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $8,128.71 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1343.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 40 | $32.90 | $2.11 | — | $6,810.60 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1343.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 278 | $4.82 | $3.59 | — | $5,467.06 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1343.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 70 | $19.00 | $2.20 | — | $4,134.86 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+7.5; leftover $1343.80 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 156 | $8.61 | $2.46 | — | $2,789.24 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer; ret5=-0.7; leftover $1343.80 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 71 | $18.75 | $2.20 | — | $1,455.78 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ret5=-5.0; leftover $1343.80 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 69 | $19.25 | $2.20 | — | $125.34 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ret5=+14.1; leftover $1343.80 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $125.34 | ▼ close $9,270.64 vs 09:30 $9,559.85 (session -260.29) | 16:00 close · cash $125.34 · equity $9,270.64 vs 09:30 $9,559.85 (-289.21; session marks -260.29) · 11 name(s) marked open→close (per-name table). TRLV×3 09:30 $11.00 → close $11.82 +2.46; CAPR×4 09:30 $9.73 → close $9.59 -0.56; FWRD×2 09:30 $17.70 → close $16.99 -1.42; FLNC×3 09:30 $11.27 → close $10.88 -1.17; SMTC×9 09:30 $141.76 → close $131.17 -95.31; SEDG×40 09:30 $32.90 → close $31.41 -59.60; TLS×278 09:30 $4.82 → close $4.79 -8.34; TH×70 09:30 $19.00 → close $18.55 -31.50; OPTX×156 09:30 $8.61 → close $8.52 -14.04; BBWI×71 09:30 $18.75 → close $19.22 +33.37; ERAS×69 09:30 $19.25 → close $18.03 -84.18 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $125.34 | ▼ 09:30 equity $9,236.79 vs yday $9,270.64 (-33.85) | 09:30 open · cash $125.34 (unchanged overnight, no fees) · equity $9,236.79 vs prior close $9,270.64 (-33.85) · 11 name(s) re-marked at the open (per-name table). TRLV×3 yday $11.82 → 09:30 $11.80 -0.06; CAPR×4 yday $9.59 → 09:30 $9.50 -0.36; FWRD×2 yday $16.99 → 09:30 $17.03 +0.08; FLNC×3 yday $10.88 → 09:30 $10.82 -0.18; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; SEDG×40 yday $31.41 → 09:30 $31.15 -10.40; TLS×278 yday $4.79 → 09:30 $4.81 +5.56; TH×70 yday $18.55 → 09:30 $18.12 -29.75; OPTX×156 yday $8.52 → 09:30 $8.52 +0.00; BBWI×71 yday $19.22 → 09:30 $19.25 +2.13; ERAS×69 yday $18.03 → 09:30 $17.87 -11.04 | — |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 3 | $11.80 | $0.38 | $+1.01 | $160.35 | ▲ +1.01 after sell → book $9,236.40; vs 09:30 mark -0.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 4 | $9.50 | $0.41 | $+4.08 | $197.94 | ▲ +4.08 after sell → book $9,235.99; vs 09:30 mark -0.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FWRD` | 2 | $17.03 | $0.37 | $-1.48 | $231.64 | ▼ -1.48 after sell → book $9,235.63; vs 09:30 mark -0.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FLNC` | 3 | $10.82 | $0.35 | $-1.60 | $263.74 | ▼ -1.60 after sell → book $9,235.27; vs 09:30 mark -0.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $263.74 | ▲ close $9,262.85 vs 09:30 $9,236.79 (session +27.58) | 16:00 close · cash $263.74 · equity $9,262.85 vs 09:30 $9,236.79 (+26.06; session marks +27.58) · 7 name(s) marked open→close (per-name table). SMTC×9 09:30 $132.30 → close $132.96 +5.94; SEDG×40 09:30 $31.15 → close $32.20 +42.00; TLS×278 09:30 $4.81 → close $4.82 +2.78; TH×70 09:30 $18.12 → close $18.52 +27.65; OPTX×156 09:30 $8.52 → close $8.19 -51.48; BBWI×71 09:30 $19.25 → close $19.25 +0.00; ERAS×69 09:30 $17.87 → close $17.88 +0.69 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $263.74 | ▼ 09:30 equity $9,089.10 vs yday $9,262.85 (-173.75) | 09:30 open · cash $263.74 (unchanged overnight, no fees) · equity $9,089.10 vs prior close $9,262.85 (-173.75) · 7 name(s) re-marked at the open (per-name table). SMTC×9 yday $132.96 → 09:30 $127.63 -47.97; SEDG×40 yday $32.20 → 09:30 $31.87 -13.20; TLS×278 yday $4.82 → 09:30 $4.77 -13.90; TH×70 yday $18.52 → 09:30 $18.45 -4.90; OPTX×156 yday $8.19 → 09:30 $7.94 -39.00; BBWI×71 yday $19.25 → 09:30 $18.77 -34.08; ERAS×69 yday $17.88 → 09:30 $17.58 -20.70 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $263.74 | ▼ close $8,944.26 vs 09:30 $9,089.10 (session -144.84) | 16:00 close · cash $263.74 · equity $8,944.26 vs 09:30 $9,089.10 (-144.84; session marks -144.84) · 7 name(s) marked open→close (per-name table). SMTC×9 09:30 $127.63 → close $132.27 +41.76; SEDG×40 09:30 $31.87 → close $32.49 +24.80; TLS×278 09:30 $4.77 → close $4.72 -13.90; TH×70 09:30 $18.45 → close $18.07 -26.60; OPTX×156 09:30 $7.94 → close $7.28 -102.96; BBWI×71 09:30 $18.77 → close $18.61 -11.36; ERAS×69 09:30 $17.58 → close $16.76 -56.58 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $263.74 | ▼ 09:30 equity $8,940.12 vs yday $8,944.26 (-4.14) | 09:30 open · cash $263.74 (unchanged overnight, no fees) · equity $8,940.12 vs prior close $8,944.26 (-4.14) · 7 name(s) re-marked at the open (per-name table). SMTC×9 yday $132.27 → 09:30 $133.00 +6.57; SEDG×40 yday $32.49 → 09:30 $32.42 -2.80; TLS×278 yday $4.72 → 09:30 $4.73 +2.78; TH×70 yday $18.07 → 09:30 $17.98 -6.30; OPTX×156 yday $7.28 → 09:30 $7.25 -4.68; BBWI×71 yday $18.61 → 09:30 $18.41 -14.20; ERAS×69 yday $16.76 → 09:30 $16.97 +14.49 | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 9 | $133.00 | $2.04 | $-82.89 | $1,458.71 | ▼ -82.89 after sell → book $8,938.09; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 40 | $32.42 | $2.13 | $-23.44 | $2,753.38 | ▼ -23.44 after sell → book $8,935.96; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TLS` | 278 | $4.73 | $3.64 | $-32.25 | $4,064.67 | ▼ -32.25 after sell → book $8,932.31; vs 09:30 mark -3.65 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TH` | 70 | $17.98 | $2.22 | $-75.82 | $5,321.05 | ▼ -75.82 after sell → book $8,930.09; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `OPTX` | 156 | $7.25 | $2.49 | $-217.11 | $6,449.56 | ▼ -217.11 after sell → book $8,927.60; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBWI` | 71 | $18.41 | $2.23 | $-28.57 | $7,754.44 | ▼ -28.57 after sell → book $8,925.37; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERAS` | 69 | $16.97 | $2.22 | $-161.74 | $8,923.15 | ▼ -161.74 after sell → book $8,923.15; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,923.15 | ▲ close $8,923.15 vs 09:30 $8,940.12 (session +0.00) | 16:00 close · cash $8,923.15 · no lots left · equity $8,923.15. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,923.15 | ▲ 09:30 equity $8,923.15 vs yday $8,923.15 (+0.00) | 09:30 open · cash $8,923.15 · no holdings · equity $8,923.15 vs prior close $8,923.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 46 | $32.31 | $2.13 | — | $7,434.77 | — | prior-export headline🟢 only; gate headline=good; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1487.19 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 93 | $15.87 | $2.27 | — | $5,956.59 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1487.19 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 62 | $23.88 | $2.18 | — | $4,473.85 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1487.19 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $3,065.35 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1487.19 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 45 | $32.88 | $2.12 | — | $1,583.63 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=+16.2; leftover $1487.19 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 195 | $7.59 | $2.58 | — | $101.00 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-11.5; leftover $1487.19 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $101.00 | ▲ close $9,079.41 vs 09:30 $8,923.15 (session +169.53) | 16:00 close · cash $101.00 · equity $9,079.41 vs 09:30 $8,923.15 (+156.26; session marks +169.53) · 6 name(s) marked open→close (per-name table). CXW×46 09:30 $32.31 → close $33.66 +62.10; FRNM×93 09:30 $15.87 → close $16.90 +95.79; MMED×62 09:30 $23.88 → close $23.84 -2.48; DE×2 09:30 $703.25 → close $694.41 -17.68; CNXC×45 09:30 $32.88 → close $32.85 -1.35; OPTX×195 09:30 $7.59 → close $7.76 +33.15 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $101.00 | ▼ 09:30 equity $9,008.15 vs yday $9,079.41 (-71.26) | 09:30 open · cash $101.00 (unchanged overnight, no fees) · equity $9,008.15 vs prior close $9,079.41 (-71.26) · 6 name(s) re-marked at the open (per-name table). CXW×46 yday $33.66 → 09:30 $33.46 -9.20; FRNM×93 yday $16.90 → 09:30 $16.40 -46.50; MMED×62 yday $23.84 → 09:30 $23.84 +0.00; DE×2 yday $694.41 → 09:30 $692.03 -4.76; CNXC×45 yday $32.85 → 09:30 $32.48 -16.65; OPTX×195 yday $7.76 → 09:30 $7.79 +5.85 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 26 | $1.94 | $0.58 | — | $49.98 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $50.50 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.98 | ▼ close $8,967.00 vs 09:30 $9,008.15 (session -40.57) | 16:00 close · cash $49.98 · equity $8,967.00 vs 09:30 $9,008.15 (-41.15; session marks -40.57) · 7 name(s) marked open→close (per-name table). CXW×46 09:30 $33.46 → close $34.71 +57.50; FRNM×93 09:30 $16.40 → close $16.31 -8.37; MMED×62 09:30 $23.84 → close $23.29 -34.10; DE×2 09:30 $692.03 → close $693.53 +3.00; CNXC×45 09:30 $32.48 → close $32.16 -14.40; OPTX×195 09:30 $7.79 → close $7.57 -42.90; BAK×26 09:30 $1.94 → close $1.89 -1.30 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.98 | ▲ 09:30 equity $8,979.42 vs yday $8,967.00 (+12.42) | 09:30 open · cash $49.98 (unchanged overnight, no fees) · equity $8,979.42 vs prior close $8,967.00 (+12.42) · 7 name(s) re-marked at the open (per-name table). CXW×46 yday $34.71 → 09:30 $34.49 -10.12; FRNM×93 yday $16.31 → 09:30 $16.74 +39.99; MMED×62 yday $23.29 → 09:30 $23.16 -8.06; DE×2 yday $693.53 → 09:30 $687.21 -12.64; CNXC×45 yday $32.16 → 09:30 $31.77 -17.55; OPTX×195 yday $7.57 → 09:30 $7.67 +19.50; BAK×26 yday $1.89 → 09:30 $1.94 +1.30 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.98 | ▼ close $8,791.92 vs 09:30 $8,979.42 (session -187.50) | 16:00 close · cash $49.98 · equity $8,791.92 vs 09:30 $8,979.42 (-187.50; session marks -187.50) · 7 name(s) marked open→close (per-name table). CXW×46 09:30 $34.49 → close $35.05 +25.76; FRNM×93 09:30 $16.74 → close $15.99 -69.75; MMED×62 09:30 $23.16 → close $23.32 +9.92; DE×2 09:30 $687.21 → close $680.73 -12.96; CNXC×45 09:30 $31.77 → close $28.27 -157.50; OPTX×195 09:30 $7.67 → close $7.76 +17.55; BAK×26 09:30 $1.94 → close $1.92 -0.52 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.98 | ▼ 09:30 equity $8,774.32 vs yday $8,791.92 (-17.60) | 09:30 open · cash $49.98 (unchanged overnight, no fees) · equity $8,774.32 vs prior close $8,791.92 (-17.60) · 7 name(s) re-marked at the open (per-name table). CXW×46 yday $35.05 → 09:30 $35.09 +1.84; FRNM×93 yday $15.99 → 09:30 $15.96 -2.79; MMED×62 yday $23.32 → 09:30 $23.22 -6.20; DE×2 yday $680.73 → 09:30 $681.32 +1.18; CNXC×45 yday $28.27 → 09:30 $28.13 -6.30; OPTX×195 yday $7.76 → 09:30 $7.72 -7.80; BAK×26 yday $1.92 → 09:30 $2.02 +2.47 | — |
| 2026-09-09 09:30 ET | **SELL** | `CXW` | 46 | $35.09 | $2.15 | $+123.60 | $1,661.97 | ▲ +123.60 after sell → book $8,772.17; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 93 | $15.96 | $2.30 | $+3.80 | $3,143.95 | ▲ +3.80 after sell → book $8,769.87; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 62 | $23.22 | $2.20 | $-45.29 | $4,581.40 | ▼ -45.29 after sell → book $8,767.68; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DE` | 2 | $681.32 | $2.02 | $-47.87 | $5,942.02 | ▼ -47.87 after sell → book $8,765.66; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CNXC` | 45 | $28.13 | $2.15 | $-218.02 | $7,205.72 | ▼ -218.02 after sell → book $8,763.51; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `OPTX` | 195 | $7.72 | $2.62 | $+20.16 | $8,708.51 | ▲ +20.16 after sell → book $8,760.90; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,708.51 | ▼ close $8,759.73 vs 09:30 $8,774.32 (session -1.17) | 16:00 close · cash $8,708.51 · equity $8,759.73 vs 09:30 $8,774.32 (-14.59; session marks -1.17) · 1 name(s) marked open→close (per-name table). BAK×26 09:30 $2.02 → close $1.97 -1.17 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,708.51 | ▲ 09:30 equity $8,759.73 vs yday $8,759.73 (-0.00) | 09:30 open · cash $8,708.51 (unchanged overnight, no fees) · equity $8,759.73 vs prior close $8,759.73 (-0.00) · 1 name(s) re-marked at the open (per-name table). BAK×26 yday $1.97 → 09:30 $1.97 +0.00 | — |
| 2026-09-10 09:30 ET | **SELL** | `BAK` | 26 | $1.97 | $0.61 | $-0.41 | $8,759.12 | ▼ -0.41 after sell → book $8,759.12; vs 09:30 mark -0.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,759.12 | ▲ close $8,759.12 vs 09:30 $8,759.73 (session +0.00) | 16:00 close · cash $8,759.12 · no lots left · equity $8,759.12. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,759.12 | ▲ 09:30 equity $8,759.12 vs yday $8,759.12 (-0.00) | 09:30 open · cash $8,759.12 · no holdings · equity $8,759.12 vs prior close $8,759.12 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 10 | $164.43 | $2.02 | — | $7,112.80 | — | prior-export headline🟢 only; gate headline=good; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1751.82 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 7 | $242.17 | $2.01 | — | $5,415.59 | — | prior-export headline🟢 only; gate headline=good; rank cond; list earn_react; ret5=-11.1; leftover $1751.82 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 826 | $2.12 | $10.66 | — | $3,653.82 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1751.82 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 858 | $2.04 | $11.07 | — | $1,892.43 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1751.82 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 12 | $135.71 | $2.03 | — | $261.88 | — | prior-export headline🟢 only; gate headline=good; rank cond; list earn_react; ret5=-9.2; leftover $1751.82 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $261.88 | ▼ close $8,581.79 vs 09:30 $8,759.12 (session -149.54) | 16:00 close · cash $261.88 · equity $8,581.79 vs 09:30 $8,759.12 (-177.33; session marks -149.54) · 5 name(s) marked open→close (per-name table). ORCL×10 09:30 $164.43 → close $150.28 -141.50; ADBE×7 09:30 $242.17 → close $252.23 +70.42; BAK×826 09:30 $2.12 → close $2.08 -33.04; AMTX×858 09:30 $2.04 → close $2.01 -25.74; RH×12 09:30 $135.71 → close $134.07 -19.68 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $261.88 | ▼ 09:30 equity $8,501.33 vs yday $8,581.79 (-80.46) | 09:30 open · cash $261.88 (unchanged overnight, no fees) · equity $8,501.33 vs prior close $8,581.79 (-80.46) · 5 name(s) re-marked at the open (per-name table). ORCL×10 yday $150.28 → 09:30 $141.42 -88.60; ADBE×7 yday $252.23 → 09:30 $261.51 +64.96; BAK×826 yday $2.08 → 09:30 $2.05 -24.78; AMTX×858 yday $2.01 → 09:30 $2.01 +0.00; RH×12 yday $134.07 → 09:30 $131.40 -32.04 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $261.88 | ▲ close $8,511.68 vs 09:30 $8,501.33 (session +10.35) | 16:00 close · cash $261.88 · equity $8,511.68 vs 09:30 $8,501.33 (+10.35; session marks +10.35) · 5 name(s) marked open→close (per-name table). ORCL×10 09:30 $141.42 → close $144.72 +33.00; ADBE×7 09:30 $261.51 → close $265.60 +28.63; BAK×826 09:30 $2.05 → close $2.01 -33.04; AMTX×858 09:30 $2.01 → close $1.95 -51.48; RH×12 09:30 $131.40 → close $134.17 +33.24 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VELO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `FWRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `S` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VELO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `FWRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `S` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OUST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CELC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AUTL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OUST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CELC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ZLAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HUMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `FUTU` | cash | leftover split 35.98 < 1 share @ 115.18 |
| 2026-08-21 | `GRAL` | cash | leftover split 35.98 < 1 share @ 78.88 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ZLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TEAM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HUMA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `REAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EOLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 38.27 < 1 share @ 267.02 |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EOLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `TRLV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `FWRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `FLNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `TRLV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FWRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OPTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `OPTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ERAS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `CXW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CNXC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `OPTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRX` | cash | leftover split 50.50 < 1 share @ 75.65 |
| 2026-09-08 | `CXW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CNXC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `OPTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `WLTH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TXG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ADBE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ORCL` | 10 | 2026-09-11 @ $164.43 | prior-export headline🟢 only; gate headline=good; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1751.82 |
| `ADBE` | 7 | 2026-09-11 @ $242.17 | prior-export headline🟢 only; gate headline=good; rank cond; list earn_react; ret5=-11.1; leftover $1751.82 |
| `BAK` | 826 | 2026-09-11 @ $2.12 | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1751.82 |
| `AMTX` | 858 | 2026-09-11 @ $2.04 | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1751.82 |
| `RH` | 12 | 2026-09-11 @ $135.71 | prior-export headline🟢 only; gate headline=good; rank cond; list earn_react; ret5=-9.2; leftover $1751.82 |
