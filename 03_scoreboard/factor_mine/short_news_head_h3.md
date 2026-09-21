# Factor mine action — `short_news_head_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · short prior-export headline🔴

Cash book **+6.07%** ($10,607) · signal-only (no cash/fees) was -2.05%. Starts YES **12/27**. Fills 71 · skips 84 · realized $+720.33.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the prior-export headline is red.

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a SHORT sleeve: it borrows the name and profits if the price falls. Equity treats the short as a liability (must keep enough to cover).

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `headline=bad` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $26,535.02.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `EU` | 1412 | — | $1.18 | +0.00 | $1.21 | -42.36 | -42.36 | -0.00 | -42.36 |
| 2026-08-14 | `LUNR` | 86 | — | $19.17 | +0.00 | $19.01 | +13.76 | +13.76 | -0.00 | +13.76 |
| 2026-08-14 | `OWL` | 131 | — | $12.70 | +0.00 | $12.22 | +62.22 | +62.22 | -0.00 | +62.22 |
| 2026-08-17 | `EU` | 1412 | $1.21 | $1.21 | +0.00 | $1.13 | +112.96 | +112.96 | -42.36 | +70.60 |
| 2026-08-17 | `LUNR` | 86 | $19.01 | $20.25 | -106.64 | $20.38 | -11.18 | -117.82 | -92.88 | -104.06 |
| 2026-08-17 | `OWL` | 131 | $12.22 | $12.12 | +13.10 | $11.66 | +60.26 | +73.36 | +75.33 | +135.59 |
| 2026-08-17 | `VERI` | 862 | — | $1.15 | +0.00 | $1.08 | +56.03 | +56.03 | -0.00 | +56.03 |
| 2026-08-17 | `ZNTL` | 278 | — | $3.56 | +0.00 | $3.71 | -40.31 | -40.31 | -0.00 | -40.31 |
| 2026-08-17 | `APMD` | 31 | — | $31.70 | +0.00 | $32.55 | -26.35 | -26.35 | -0.00 | -26.35 |
| 2026-08-17 | `HIVE` | 329 | — | $3.01 | +0.00 | $3.07 | -19.74 | -19.74 | -0.00 | -19.74 |
| 2026-08-17 | `RNW` | 145 | — | $6.80 | +0.00 | $6.82 | -2.90 | -2.90 | -0.00 | -2.90 |
| 2026-08-18 | `EU` | 1412 | $1.13 | $1.13 | +0.00 | $1.07 | +84.72 | +84.72 | +70.60 | +155.32 |
| 2026-08-18 | `LUNR` | 86 | $20.38 | $19.31 | +92.02 | $19.31 | +0.00 | +92.02 | -12.04 | -12.04 |
| 2026-08-18 | `OWL` | 131 | $11.66 | $11.54 | +15.72 | $11.59 | -6.55 | +9.17 | +151.31 | +144.76 |
| 2026-08-18 | `VERI` | 862 | $1.08 | $1.05 | +30.17 | $0.99 | +47.41 | +77.58 | +86.20 | +133.61 |
| 2026-08-18 | `ZNTL` | 278 | $3.71 | $3.75 | -12.51 | $3.68 | +19.46 | +6.95 | -52.82 | -33.36 |
| 2026-08-18 | `APMD` | 31 | $32.55 | $32.85 | -9.30 | $31.81 | +32.24 | +22.94 | -35.65 | -3.41 |
| 2026-08-18 | `HIVE` | 329 | $3.07 | $2.96 | +36.19 | $2.78 | +59.22 | +95.41 | +16.45 | +75.67 |
| 2026-08-18 | `RNW` | 145 | $6.82 | $6.83 | -1.45 | $6.82 | +1.45 | +0.00 | -4.35 | -2.90 |
| 2026-08-19 | `EU` | 1412 | $1.07 | $1.07 | +0.00 | — | +0.00 | +0.00 | +155.32 | — |
| 2026-08-19 | `LUNR` | 86 | $19.31 | $18.98 | +28.38 | $18.52 | +39.56 | +67.94 | +16.34 | +55.90 |
| 2026-08-19 | `OWL` | 131 | $11.59 | $11.75 | -20.96 | — | +0.00 | -20.96 | +123.80 | — |
| 2026-08-19 | `VERI` | 862 | $0.99 | $1.00 | -4.31 | $0.97 | +29.31 | +25.00 | +129.30 | +158.61 |
| 2026-08-19 | `ZNTL` | 278 | $3.68 | $3.76 | -22.24 | $3.82 | -16.68 | -38.92 | -55.60 | -72.28 |
| 2026-08-19 | `APMD` | 31 | $31.81 | $32.13 | -9.92 | $32.03 | +3.10 | -6.82 | -13.33 | -10.23 |
| 2026-08-19 | `HIVE` | 329 | $2.78 | $2.78 | +0.00 | $2.82 | -13.16 | -13.16 | +75.67 | +62.51 |
| 2026-08-19 | `RNW` | 145 | $6.82 | $6.84 | -2.90 | $6.80 | +5.80 | +2.90 | -5.80 | -0.00 |
| 2026-08-20 | `LUNR` | 86 | $18.52 | $18.13 | +33.54 | — | +0.00 | +33.54 | +89.44 | — |
| 2026-08-20 | `VERI` | 862 | $0.97 | $0.96 | +2.59 | — | +0.00 | +2.59 | +161.19 | — |
| 2026-08-20 | `ZNTL` | 278 | $3.82 | $4.01 | -54.21 | — | +0.00 | -54.21 | -126.49 | — |
| 2026-08-20 | `APMD` | 31 | $32.03 | $31.87 | +4.96 | — | +0.00 | +4.96 | -5.27 | — |
| 2026-08-20 | `HIVE` | 329 | $2.82 | $2.95 | -42.77 | — | +0.00 | -42.77 | +19.74 | — |
| 2026-08-20 | `RNW` | 145 | $6.80 | $6.81 | -1.45 | — | +0.00 | -1.45 | -1.45 | — |
| 2026-08-20 | `WYFI` | 48 | — | $21.40 | +0.00 | $21.16 | +11.52 | +11.52 | -0.00 | +11.52 |
| 2026-08-20 | `TOYO` | 233 | — | $4.43 | +0.00 | $4.51 | -19.80 | -19.80 | -0.00 | -19.80 |
| 2026-08-20 | `ABCL` | 87 | — | $11.81 | +0.00 | $11.57 | +21.31 | +21.31 | -0.00 | +21.31 |
| 2026-08-20 | `AAP` | 22 | — | $46.85 | +0.00 | $42.39 | +98.12 | +98.12 | -0.00 | +98.12 |
| 2026-08-20 | `AQST` | 223 | — | $4.61 | +0.00 | $4.50 | +25.65 | +25.65 | -0.00 | +25.65 |
| 2026-08-21 | `WYFI` | 48 | $21.16 | $21.54 | -18.24 | $20.72 | +39.36 | +21.12 | -6.72 | +32.64 |
| 2026-08-21 | `TOYO` | 233 | $4.51 | $4.68 | -38.45 | $4.82 | -32.62 | -71.07 | -58.25 | -90.87 |
| 2026-08-21 | `ABCL` | 87 | $11.57 | $11.57 | +0.00 | $11.32 | +21.75 | +21.75 | +21.31 | +43.06 |
| 2026-08-21 | `AAP` | 22 | $42.39 | $42.41 | -0.44 | $42.58 | -3.74 | -4.18 | +97.68 | +93.94 |
| 2026-08-21 | `AQST` | 223 | $4.50 | $4.54 | -10.03 | $4.66 | -26.76 | -36.79 | +15.61 | -11.15 |
| 2026-08-21 | `QTRX` | 417 | — | $3.11 | +0.00 | $2.99 | +50.04 | +50.04 | -0.00 | +50.04 |
| 2026-08-21 | `MRNA` | 9 | — | $133.11 | +0.00 | $145.13 | -108.18 | -108.18 | -0.00 | -108.18 |
| 2026-08-21 | `ARIS` | 62 | — | $20.90 | +0.00 | $20.86 | +2.48 | +2.48 | -0.00 | +2.48 |
| 2026-08-21 | `NOG` | 48 | — | $27.00 | +0.00 | $27.34 | -16.32 | -16.32 | -0.00 | -16.32 |
| 2026-08-24 | `WYFI` | 48 | $20.72 | $20.01 | +34.08 | $20.78 | -36.96 | -2.88 | +66.72 | +29.76 |
| 2026-08-24 | `TOYO` | 233 | $4.82 | $4.58 | +55.92 | $4.38 | +46.60 | +102.52 | -34.95 | +11.65 |
| 2026-08-24 | `ABCL` | 87 | $11.32 | $10.97 | +30.45 | $10.61 | +31.32 | +61.77 | +73.51 | +104.84 |
| 2026-08-24 | `AAP` | 22 | $42.58 | $43.05 | -10.34 | $43.63 | -12.76 | -23.10 | +83.60 | +70.84 |
| 2026-08-24 | `AQST` | 223 | $4.66 | $4.67 | -2.23 | $4.80 | -28.99 | -31.22 | -13.38 | -42.37 |
| 2026-08-24 | `QTRX` | 417 | $2.99 | $2.99 | +0.00 | $2.80 | +79.23 | +79.23 | +50.04 | +129.27 |
| 2026-08-24 | `MRNA` | 9 | $145.13 | $142.70 | +21.87 | $138.89 | +34.29 | +56.16 | -86.31 | -52.02 |
| 2026-08-24 | `ARIS` | 62 | $20.86 | $20.98 | -7.44 | $20.81 | +10.54 | +3.10 | -4.96 | +5.58 |
| 2026-08-24 | `NOG` | 48 | $27.34 | $27.12 | +10.56 | $26.84 | +13.44 | +24.00 | -5.76 | +7.68 |
| 2026-08-25 | `WYFI` | 48 | $20.78 | $20.90 | -5.76 | — | +0.00 | -5.76 | +24.00 | — |
| 2026-08-25 | `TOYO` | 233 | $4.38 | $4.42 | -9.32 | — | +0.00 | -9.32 | +2.33 | — |
| 2026-08-25 | `ABCL` | 87 | $10.61 | $11.00 | -33.93 | — | +0.00 | -33.93 | +70.90 | — |
| 2026-08-25 | `AAP` | 22 | $43.63 | $43.63 | +0.00 | — | +0.00 | +0.00 | +70.84 | — |
| 2026-08-25 | `AQST` | 223 | $4.80 | $4.77 | +6.69 | — | +0.00 | +6.69 | -35.68 | — |
| 2026-08-25 | `QTRX` | 417 | $2.80 | $2.80 | +0.00 | $2.79 | +4.17 | +4.17 | +129.27 | +133.44 |
| 2026-08-25 | `MRNA` | 9 | $138.89 | $143.50 | -41.49 | $158.83 | -137.97 | -179.46 | -93.51 | -231.48 |
| 2026-08-25 | `ARIS` | 62 | $20.81 | $20.45 | +22.32 | $21.18 | -45.26 | -22.94 | +27.90 | -17.36 |
| 2026-08-25 | `NOG` | 48 | $26.84 | $26.06 | +37.44 | $26.42 | -17.28 | +20.16 | +45.12 | +27.84 |
| 2026-08-25 | `AVAH` | 386 | — | $13.62 | +0.00 | $13.59 | +13.51 | +13.51 | -0.00 | +13.51 |
| 2026-08-26 | `QTRX` | 417 | $2.79 | $2.83 | -16.68 | — | +0.00 | -16.68 | +116.76 | — |
| 2026-08-26 | `MRNA` | 9 | $158.83 | $154.20 | +41.67 | — | +0.00 | +41.67 | -189.81 | — |
| 2026-08-26 | `ARIS` | 62 | $21.18 | $20.50 | +42.16 | — | +0.00 | +42.16 | +24.80 | — |
| 2026-08-26 | `NOG` | 48 | $26.42 | $26.00 | +20.16 | — | +0.00 | +20.16 | +48.00 | — |
| 2026-08-26 | `AVAH` | 386 | $13.59 | $13.65 | -23.16 | $13.62 | +11.58 | -11.58 | -9.65 | +1.93 |
| 2026-08-26 | `ABCL` | 212 | — | $12.22 | +0.00 | $12.24 | -4.24 | -4.24 | -0.00 | -4.24 |
| 2026-08-26 | `AQST` | 511 | — | $5.08 | +0.00 | $5.39 | -158.41 | -158.41 | -0.00 | -158.41 |
| 2026-08-27 | `AVAH` | 386 | $13.62 | $13.62 | +0.00 | $13.82 | -77.20 | -77.20 | +1.93 | -75.27 |
| 2026-08-27 | `ABCL` | 212 | $12.24 | $12.25 | -2.12 | $12.40 | -31.80 | -33.92 | -6.36 | -38.16 |
| 2026-08-27 | `AQST` | 511 | $5.39 | $5.39 | +0.00 | $5.16 | +117.53 | +117.53 | -158.41 | -40.88 |
| 2026-08-27 | `MT` | 22 | — | $74.54 | +0.00 | $74.63 | -1.98 | -1.98 | -0.00 | -1.98 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | +31.62 | +31.62 | -0.00 | +31.62 |
| 2026-08-27 | `TX` | 30 | — | $55.25 | +0.00 | $55.83 | -17.40 | -17.40 | -0.00 | -17.40 |
| 2026-08-28 | `AVAH` | 386 | $13.82 | $13.90 | -30.88 | — | +0.00 | -30.88 | -106.15 | — |
| 2026-08-28 | `ABCL` | 212 | $12.40 | $12.30 | +20.14 | $11.35 | +202.46 | +222.60 | -18.02 | +184.44 |
| 2026-08-28 | `AQST` | 511 | $5.16 | $5.11 | +25.55 | $5.02 | +45.99 | +71.54 | -15.33 | +30.66 |
| 2026-08-28 | `MT` | 22 | $74.63 | $75.39 | -16.72 | $74.39 | +22.00 | +5.28 | -18.70 | +3.30 |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | +16.10 | $932.86 | -13.57 | +2.53 | +47.72 | +34.15 |
| 2026-08-28 | `TX` | 30 | $55.83 | $55.97 | -4.20 | $54.84 | +33.90 | +29.70 | -21.60 | +12.30 |
| 2026-08-28 | `SIMO` | 20 | — | $252.24 | +0.00 | $245.81 | +128.60 | +128.60 | -0.00 | +128.60 |
| 2026-08-31 | `ABCL` | 212 | $11.35 | $11.10 | +53.00 | — | +0.00 | +53.00 | +237.44 | — |
| 2026-08-31 | `AQST` | 511 | $5.02 | $4.97 | +22.99 | — | +0.00 | +22.99 | +53.66 | — |
| 2026-08-31 | `MT` | 22 | $74.39 | $75.18 | -17.38 | $74.60 | +12.76 | -4.62 | -14.08 | -1.32 |
| 2026-08-31 | `MU` | 1 | $932.86 | $931.39 | +1.47 | $958.73 | -27.34 | -25.87 | +35.62 | +8.28 |
| 2026-08-31 | `TX` | 30 | $54.84 | $55.26 | -12.60 | $54.82 | +13.20 | +0.60 | -0.30 | +12.90 |
| 2026-08-31 | `SIMO` | 20 | $245.81 | $247.05 | -24.80 | $246.84 | +4.20 | -20.60 | +103.80 | +108.00 |
| 2026-09-01 | `MT` | 22 | $74.60 | $73.22 | +30.36 | — | +0.00 | +30.36 | +29.04 | — |
| 2026-09-01 | `MU` | 1 | $958.73 | $941.13 | +17.60 | — | +0.00 | +17.60 | +25.88 | — |
| 2026-09-01 | `TX` | 30 | $54.82 | $54.76 | +1.80 | — | +0.00 | +1.80 | +14.70 | — |
| 2026-09-01 | `SIMO` | 20 | $246.84 | $240.09 | +135.00 | $237.35 | +54.80 | +189.80 | +243.00 | +297.80 |
| 2026-09-02 | `SIMO` | 20 | $237.35 | $235.71 | +32.80 | — | +0.00 | +32.80 | +330.60 | — |
| 2026-09-03 | `SLN` | 184 | — | $14.85 | +0.00 | $14.79 | +11.04 | +11.04 | -0.00 | +11.04 |
| 2026-09-03 | `OPK` | 1600 | — | $1.71 | +0.00 | $1.61 | +160.00 | +160.00 | -0.00 | +160.00 |
| 2026-09-04 | `SLN` | 184 | $14.79 | $14.63 | +29.44 | $14.59 | +7.36 | +36.80 | +40.48 | +47.84 |
| 2026-09-04 | `OPK` | 1600 | $1.61 | $1.59 | +32.00 | $1.64 | -80.00 | -48.00 | +192.00 | +112.00 |
| 2026-09-04 | `GSM` | 597 | — | $4.67 | +0.00 | $4.67 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-09-04 | `PIPR` | 36 | — | $76.55 | +0.00 | $77.04 | -17.64 | -17.64 | -0.00 | -17.64 |
| 2026-09-08 | `SLN` | 184 | $14.59 | $14.24 | +64.40 | $13.69 | +101.20 | +165.60 | +112.24 | +213.44 |
| 2026-09-08 | `OPK` | 1600 | $1.64 | $1.63 | +16.00 | $1.59 | +64.00 | +80.00 | +128.00 | +192.00 |
| 2026-09-08 | `GSM` | 597 | $4.67 | $4.75 | -47.76 | $4.52 | +137.31 | +89.55 | -47.76 | +89.55 |
| 2026-09-08 | `PIPR` | 36 | $77.04 | $76.64 | +14.40 | $77.34 | -25.20 | -10.80 | -3.24 | -28.44 |
| 2026-09-09 | `SLN` | 184 | $13.69 | $13.60 | +16.56 | — | +0.00 | +16.56 | +230.00 | — |
| 2026-09-09 | `OPK` | 1600 | $1.59 | $1.58 | +16.00 | — | +0.00 | +16.00 | +208.00 | — |
| 2026-09-09 | `GSM` | 597 | $4.52 | $4.52 | +0.00 | $4.49 | +17.91 | +17.91 | +89.55 | +107.46 |
| 2026-09-09 | `PIPR` | 36 | $77.34 | $77.24 | +3.60 | $76.96 | +10.08 | +13.68 | -24.84 | -14.76 |
| 2026-09-10 | `GSM` | 597 | $4.49 | $4.36 | +77.61 | — | +0.00 | +77.61 | +185.07 | — |
| 2026-09-10 | `PIPR` | 36 | $76.96 | $76.79 | +6.12 | — | +0.00 | +6.12 | -8.64 | — |
| 2026-09-11 | `MYGN` | 568 | — | $3.37 | +0.00 | $3.42 | -28.40 | -28.40 | -0.00 | -28.40 |
| 2026-09-11 | `RWT` | 544 | — | $3.52 | +0.00 | $3.55 | -16.32 | -16.32 | -0.00 | -16.32 |
| 2026-09-11 | `CRDL` | 943 | — | $2.03 | +0.00 | $2.00 | +33.00 | +33.00 | -0.00 | +33.00 |
| 2026-09-14 | `MYGN` | 568 | $3.42 | $3.43 | -5.68 | $3.79 | -204.48 | -210.16 | -34.08 | -238.56 |
| 2026-09-14 | `RWT` | 544 | $3.55 | $3.53 | +10.88 | $3.83 | -163.20 | -152.32 | -5.44 | -168.64 |
| 2026-09-14 | `CRDL` | 943 | $2.00 | $1.98 | +14.15 | $1.99 | -9.43 | +4.72 | +47.15 | +37.72 |
| 2026-09-15 | `MYGN` | 568 | $3.79 | $3.80 | -5.68 | $3.88 | -45.44 | -51.12 | -244.24 | -289.68 |
| 2026-09-15 | `RWT` | 544 | $3.83 | $3.80 | +16.32 | $3.91 | -59.84 | -43.52 | -152.32 | -212.16 |
| 2026-09-15 | `CRDL` | 943 | $1.99 | $1.96 | +28.29 | $1.85 | +103.73 | +132.02 | +66.01 | +169.74 |
| 2026-09-16 | `MYGN` | 568 | $3.88 | $3.75 | +73.84 | — | +0.00 | +73.84 | -215.84 | — |
| 2026-09-16 | `RWT` | 544 | $3.91 | $3.98 | -38.08 | — | +0.00 | -38.08 | -250.24 | — |
| 2026-09-16 | `CRDL` | 943 | $1.85 | $1.85 | +0.00 | — | +0.00 | +0.00 | +169.74 | — |
| 2026-09-16 | `BBNX` | 149 | — | $18.61 | +0.00 | $22.18 | -531.93 | -531.93 | -0.00 | -531.93 |
| 2026-09-16 | `GFR` | 407 | — | $6.83 | +0.00 | $6.49 | +138.38 | +138.38 | -0.00 | +138.38 |
| 2026-09-17 | `BBNX` | 149 | $22.18 | $22.46 | -41.72 | $21.43 | +153.47 | +111.75 | -573.65 | -420.18 |
| 2026-09-17 | `GFR` | 407 | $6.49 | $6.48 | +4.07 | $6.66 | -73.26 | -69.19 | +142.45 | +69.19 |
| 2026-09-17 | `BULL` | 336 | — | $7.95 | +0.00 | $7.71 | +80.64 | +80.64 | -0.00 | +80.64 |
| 2026-09-17 | `LEN` | 33 | — | $81.00 | +0.00 | $79.70 | +42.90 | +42.90 | -0.00 | +42.90 |
| 2026-09-18 | `BBNX` | 149 | $21.43 | $21.30 | +19.37 | $21.89 | -87.91 | -68.54 | -400.81 | -488.72 |
| 2026-09-18 | `GFR` | 407 | $6.66 | $6.64 | +8.14 | $6.66 | -8.14 | +0.00 | +77.33 | +69.19 |
| 2026-09-18 | `BULL` | 336 | $7.71 | $7.85 | -47.04 | $8.25 | -134.40 | -181.44 | +33.60 | -100.80 |
| 2026-09-18 | `LEN` | 33 | $79.70 | $78.25 | +47.85 | $76.43 | +60.06 | +107.91 | +90.75 | +150.81 |
| 2026-09-18 | `FIVN` | 158 | — | $34.44 | +0.00 | $32.47 | +311.26 | +311.26 | -0.00 | +311.26 |
| 2026-09-21 | `BBNX` | 149 | $21.89 | $22.11 | -32.78 | — | +0.00 | -32.78 | -521.50 | — |
| 2026-09-21 | `GFR` | 407 | $6.66 | $6.55 | +44.77 | — | +0.00 | +44.77 | +113.96 | — |
| 2026-09-21 | `BULL` | 336 | $8.25 | $8.58 | -110.88 | $8.25 | +110.88 | +0.00 | -211.68 | -100.80 |
| 2026-09-21 | `LEN` | 33 | $76.43 | $76.97 | -17.98 | $78.08 | -36.47 | -54.45 | +132.83 | +96.36 |
| 2026-09-21 | `FIVN` | 158 | $32.47 | $32.98 | -80.58 | $37.01 | -636.74 | -717.32 | +230.68 | -406.06 |
| 2026-09-21 | `AEHL` | 328 | — | $8.26 | +0.00 | $6.92 | +439.52 | +439.52 | -0.00 | +439.52 |
| 2026-09-21 | `AMD` | 4 | — | $583.88 | +0.00 | $615.52 | -126.56 | -126.56 | -0.00 | -126.56 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +33.62 | EU, LUNR, OWL | — | $14,954.53 | $10,010.33 | EU×1412, LUNR×86, OWL×131 |
| 2026-08-17 | +2.25 | $14,954.53 | EU×1412, LUNR×86, OWL×131 | $9,916.79 | -93.54 | +128.77 | VERI, ZNTL, APMD, HIVE, RNW | — | $19,870.58 | $10,021.64 | EU×1412, LUNR×86, OWL×131, VERI×862, ZNTL×278, APMD×31, HIVE×329, RNW×145 |
| 2026-08-18 | -6.20 | $19,870.58 | EU×1412, LUNR×86, OWL×131, VERI×862, ZNTL×278, APMD×31, HIVE×329, RNW×145 | $10,172.48 | +150.84 | +237.95 | — | — | $19,870.58 | $10,410.43 | EU×1412, LUNR×86, OWL×131, VERI×862, ZNTL×278, APMD×31, HIVE×329, RNW×145 |
| 2026-08-19 | -7.20 | $19,870.58 | EU×1412, LUNR×86, OWL×131, VERI×862, ZNTL×278, APMD×31, HIVE×329, RNW×145 | $10,378.48 | -31.95 | +47.93 | — | EU, OWL | $16,799.89 | $10,405.81 | LUNR×86, VERI×862, ZNTL×278, APMD×31, HIVE×329, RNW×145 |
| 2026-08-20 | +1.12 | $16,799.89 | LUNR×86, VERI×862, ZNTL×278, APMD×31, HIVE×329, RNW×145 | $10,348.47 | -57.34 | +136.80 | WYFI, TOYO, ABCL, AAP, AQST | LUNR, VERI, ZNTL, APMD, HIVE, RNW | $15,456.39 | $10,447.16 | WYFI×48, TOYO×233, ABCL×87, AAP×22, AQST×223 |
| 2026-08-21 | +3.25 | $15,456.39 | WYFI×48, TOYO×233, ABCL×87, AAP×22, AQST×223 | $10,380.00 | -67.16 | -73.99 | QTRX, MRNA, ARIS, NOG | — | $20,531.07 | $10,294.03 | WYFI×48, TOYO×233, ABCL×87, AAP×22, AQST×223, QTRX×417, MRNA×9, ARIS×62, NOG×48 |
| 2026-08-24 | -5.17 | $20,531.07 | WYFI×48, TOYO×233, ABCL×87, AAP×22, AQST×223, QTRX×417, MRNA×9, ARIS×62, NOG×48 | $10,426.90 | +132.87 | +136.71 | — | — | $20,531.07 | $10,563.61 | WYFI×48, TOYO×233, ABCL×87, AAP×22, AQST×223, QTRX×417, MRNA×9, ARIS×62, NOG×48 |
| 2026-08-25 | +1.80 | $20,531.07 | WYFI×48, TOYO×233, ABCL×87, AAP×22, AQST×223, QTRX×417, MRNA×9, ARIS×62, NOG×48 | $10,539.56 | -24.05 | -182.83 | AVAH | WYFI, TOYO, ABCL, AAP, AQST | $20,759.14 | $10,339.18 | QTRX×417, MRNA×9, ARIS×62, NOG×48, AVAH×386 |
| 2026-08-26 | +2.02 | $20,759.14 | QTRX×417, MRNA×9, ARIS×62, NOG×48, AVAH×386 | $10,403.33 | +64.15 | -151.07 | ABCL, AQST | QTRX, MRNA, ARIS, NOG | $20,837.41 | $10,230.92 | AVAH×386, ABCL×212, AQST×511 |
| 2026-08-27 | — | $20,837.41 | AVAH×386, ABCL×212, AQST×511 | $10,228.80 | -2.12 | +20.77 | MT, MU, TX | — | $25,095.49 | $10,243.26 | AVAH×386, ABCL×212, AQST×511, MT×22, MU×1, TX×30 |
| 2026-08-28 | +0.75 | $25,095.49 | AVAH×386, ABCL×212, AQST×511, MT×22, MU×1, TX×30 | $10,253.25 | +9.99 | +419.38 | SIMO | AVAH | $24,767.67 | $10,665.41 | ABCL×212, AQST×511, MT×22, MU×1, TX×30, SIMO×20 |
| 2026-08-31 | -5.85 | $24,767.67 | ABCL×212, AQST×511, MT×22, MU×1, TX×30, SIMO×20 | $10,688.09 | +22.68 | +2.82 | — | ABCL, AQST | $19,862.92 | $10,681.59 | MT×22, MU×1, TX×30, SIMO×20 |
| 2026-09-01 | -6.30 | $19,862.92 | MT×22, MU×1, TX×30, SIMO×20 | $10,866.35 | +184.76 | +54.80 | — | MT, MU, TX | $15,662.02 | $10,915.02 | SIMO×20 |
| 2026-09-02 | -3.83 | $15,662.02 | SIMO×20 | $10,947.82 | +32.80 | +0.00 | — | SIMO | $10,945.77 | $10,945.77 | — |
| 2026-09-03 | -0.90 | $10,945.77 | — | $10,945.77 | -0.00 | +171.04 | SLN, OPK | — | $16,390.50 | $11,093.14 | SLN×184, OPK×1600 |
| 2026-09-04 | +2.25 | $16,390.50 | SLN×184, OPK×1600 | $11,154.58 | +61.44 | -90.28 | GSM, PIPR | — | $21,924.18 | $11,054.19 | SLN×184, OPK×1600, GSM×597, PIPR×36 |
| 2026-09-08 | -11.47 | $21,924.18 | SLN×184, OPK×1600, GSM×597, PIPR×36 | $11,101.23 | +47.04 | +277.31 | — | — | $21,924.18 | $11,378.54 | SLN×184, OPK×1600, GSM×597, PIPR×36 |
| 2026-09-09 | -13.95 | $21,924.18 | SLN×184, OPK×1600, GSM×597, PIPR×36 | $11,414.70 | +36.16 | +27.99 | — | SLN, OPK | $16,870.60 | $11,419.51 | GSM×597, PIPR×36 |
| 2026-09-10 | -13.28 | $16,870.60 | GSM×597, PIPR×36 | $11,503.24 | +83.73 | +0.00 | — | GSM, PIPR | $11,493.44 | $11,493.44 | — |
| 2026-09-11 | +0.50 | $11,493.44 | — | $11,493.44 | -0.00 | -11.72 | MYGN, RWT, CRDL | — | $17,209.72 | $11,454.67 | MYGN×568, RWT×544, CRDL×943 |
| 2026-09-14 | -11.00 | $17,209.72 | MYGN×568, RWT×544, CRDL×943 | $11,474.02 | +19.35 | -377.11 | — | — | $17,209.72 | $11,096.91 | MYGN×568, RWT×544, CRDL×943 |
| 2026-09-15 | -3.84 | $17,209.72 | MYGN×568, RWT×544, CRDL×943 | $11,135.84 | +38.93 | -1.55 | — | — | $17,209.72 | $11,134.29 | MYGN×568, RWT×544, CRDL×943 |
| 2026-09-16 | +5.30 | $17,209.72 | MYGN×568, RWT×544, CRDL×943 | $11,170.05 | +35.76 | -393.55 | BBNX, GFR | MYGN, RWT, CRDL | $16,688.26 | $10,742.01 | BBNX×149, GFR×407 |
| 2026-09-17 | +7.38 | $16,688.26 | BBNX×149, GFR×407 | $10,704.36 | -37.65 | +203.75 | BULL, LEN | — | $22,025.78 | $10,901.43 | BBNX×149, GFR×407, BULL×336, LEN×33 |
| 2026-09-18 | +4.86 | $22,025.78 | BBNX×149, GFR×407, BULL×336, LEN×33 | $10,929.75 | +28.32 | +140.87 | FIVN | — | $27,464.62 | $11,067.94 | BBNX×149, GFR×407, BULL×336, LEN×33, FIVN×158 |
| 2026-09-21 | +12.87 | $27,464.62 | BBNX×149, GFR×407, BULL×336, LEN×33, FIVN×158 | $10,870.48 | -197.46 | -249.37 | AEHL, AMD | BBNX, GFR | $26,535.02 | $10,606.96 | BULL×336, LEN×33, FIVN×158, AEHL×328, AMD×4 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1412 | $1.18 | $18.51 | — | $11,647.65 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 86 | $19.17 | $2.32 | — | $13,293.95 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 131 | $12.70 | $2.46 | — | $14,954.53 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ret5=+12.6; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,954.53 | ▲ close $10,010.33 vs 09:30 $10,000.00 (session +33.62) | 16:00 close · cash $14,954.53 · equity $10,010.33 vs 09:30 $10,000.00 (+10.33; session marks +33.62) · 3 name(s) marked open→close (per-name table). EU×1412 09:30 $1.18 → close $1.21 -42.36; LUNR×86 09:30 $19.17 → close $19.01 +13.76; OWL×131 09:30 $12.70 → close $12.22 +62.22 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,954.53 | ▼ 09:30 equity $9,916.79 vs yday $10,010.33 (-93.54) | 09:30 open · cash $14,954.53 (unchanged overnight, no fees) · equity $9,916.79 vs prior close $10,010.33 (-93.54) · 3 name(s) re-marked at the open (per-name table). EU×1412 yday $1.21 → 09:30 $1.21 -0.00; LUNR×86 yday $19.01 → 09:30 $20.25 -106.64; OWL×131 yday $12.22 → 09:30 $12.12 +13.10 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 862 | $1.15 | $11.30 | — | $15,934.53 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; ⚪; ret5=-12.2; leftover $991.68 | join🟡 sector🟢 gen🟢 news🔴 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 278 | $3.56 | $3.67 | — | $16,920.54 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; ret5=-15.6; leftover $991.68 | join🟡 sector🔴 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 31 | $31.70 | $2.13 | — | $17,901.11 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; ret5=+17.6; leftover $991.68 | join🟡 sector🔴 gen🟢 news🔴 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 329 | $3.01 | $4.34 | — | $18,887.07 | — | short prior-export headline🔴; gate headline=bad; list earn_react; ⚪; ret5=-5.3; leftover $991.68 | join🟢 sector🟢 gen🟢 news🔴 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 145 | $6.80 | $2.49 | — | $19,870.58 | — | short prior-export headline🔴; gate headline=bad; list overnight; ⚪; ret5=+10.4; leftover $991.68 | join🟡 sector🟢 gen🟢 news🔴 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,870.58 | ▲ close $10,021.64 vs 09:30 $9,916.79 (session +128.77) | 16:00 close · cash $19,870.58 · equity $10,021.64 vs 09:30 $9,916.79 (+104.85; session marks +128.77) · 8 name(s) marked open→close (per-name table). EU×1412 09:30 $1.21 → close $1.13 +112.96; LUNR×86 09:30 $20.25 → close $20.38 -11.18; OWL×131 09:30 $12.12 → close $11.66 +60.26; VERI×862 09:30 $1.15 → close $1.08 +56.03; ZNTL×278 09:30 $3.56 → close $3.71 -40.31; APMD×31 09:30 $31.70 → close $32.55 -26.35; HIVE×329 09:30 $3.01 → close $3.07 -19.74; RNW×145 09:30 $6.80 → close $6.82 -2.90 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,870.58 | ▲ 09:30 equity $10,172.48 vs yday $10,021.64 (+150.84) | 09:30 open · cash $19,870.58 (unchanged overnight, no fees) · equity $10,172.48 vs prior close $10,021.64 (+150.84) · 8 name(s) re-marked at the open (per-name table). EU×1412 yday $1.13 → 09:30 $1.13 -0.00; LUNR×86 yday $20.38 → 09:30 $19.31 +92.02; OWL×131 yday $11.66 → 09:30 $11.54 +15.72; VERI×862 yday $1.08 → 09:30 $1.05 +30.17; ZNTL×278 yday $3.71 → 09:30 $3.75 -12.51; APMD×31 yday $32.55 → 09:30 $32.85 -9.30; HIVE×329 yday $3.07 → 09:30 $2.96 +36.19; RNW×145 yday $6.82 → 09:30 $6.83 -1.45 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,870.58 | ▲ close $10,410.43 vs 09:30 $10,172.48 (session +237.95) | 16:00 close · cash $19,870.58 · equity $10,410.43 vs 09:30 $10,172.48 (+237.95; session marks +237.95) · 8 name(s) marked open→close (per-name table). EU×1412 09:30 $1.13 → close $1.07 +84.72; LUNR×86 09:30 $19.31 → close $19.31 -0.00; OWL×131 09:30 $11.54 → close $11.59 -6.55; VERI×862 09:30 $1.05 → close $0.99 +47.41; ZNTL×278 09:30 $3.75 → close $3.68 +19.46; APMD×31 09:30 $32.85 → close $31.81 +32.24; HIVE×329 09:30 $2.96 → close $2.78 +59.22; RNW×145 09:30 $6.83 → close $6.82 +1.45 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,870.58 | ▼ 09:30 equity $10,378.48 vs yday $10,410.43 (-31.95) | 09:30 open · cash $19,870.58 (unchanged overnight, no fees) · equity $10,378.48 vs prior close $10,410.43 (-31.95) · 8 name(s) re-marked at the open (per-name table). EU×1412 yday $1.07 → 09:30 $1.07 -0.00; LUNR×86 yday $19.31 → 09:30 $18.98 +28.38; OWL×131 yday $11.59 → 09:30 $11.75 -20.96; VERI×862 yday $0.99 → 09:30 $1.00 -4.31; ZNTL×278 yday $3.68 → 09:30 $3.76 -22.24; APMD×31 yday $31.81 → 09:30 $32.13 -9.92; HIVE×329 yday $2.78 → 09:30 $2.78 -0.00; RNW×145 yday $6.82 → 09:30 $6.84 -2.90 | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 1412 | $1.07 | $18.21 | $+118.60 | $18,341.53 | ▲ +118.60 after sell → book $10,360.27; vs 09:30 mark -18.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 131 | $11.75 | $2.38 | $+118.95 | $16,799.89 | ▲ +118.95 after sell → book $10,357.88; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,799.89 | ▲ close $10,405.81 vs 09:30 $10,378.48 (session +47.93) | 16:00 close · cash $16,799.89 · equity $10,405.81 vs 09:30 $10,378.48 (+27.33; session marks +47.93) · 6 name(s) marked open→close (per-name table). LUNR×86 09:30 $18.98 → close $18.52 +39.56; VERI×862 09:30 $1.00 → close $0.97 +29.31; ZNTL×278 09:30 $3.76 → close $3.82 -16.68; APMD×31 09:30 $32.13 → close $32.03 +3.10; HIVE×329 09:30 $2.78 → close $2.82 -13.16; RNW×145 09:30 $6.84 → close $6.80 +5.80 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,799.89 | ▼ 09:30 equity $10,348.47 vs yday $10,405.81 (-57.34) | 09:30 open · cash $16,799.89 (unchanged overnight, no fees) · equity $10,348.47 vs prior close $10,405.81 (-57.34) · 6 name(s) re-marked at the open (per-name table). LUNR×86 yday $18.52 → 09:30 $18.13 +33.54; VERI×862 yday $0.97 → 09:30 $0.96 +2.59; ZNTL×278 yday $3.82 → 09:30 $4.01 -54.21; APMD×31 yday $32.03 → 09:30 $31.87 +4.96; HIVE×329 yday $2.82 → 09:30 $2.95 -42.77; RNW×145 yday $6.80 → 09:30 $6.81 -1.45 | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 86 | $18.13 | $2.25 | $+84.87 | $15,238.47 | ▲ +84.87 after sell → book $10,346.22; vs 09:30 mark -2.25 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 862 | $0.96 | $10.89 | $+139.01 | $14,397.47 | ▲ +139.01 after sell → book $10,335.33; vs 09:30 mark -10.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 278 | $4.01 | $3.59 | $-133.75 | $13,277.72 | ▼ -133.75 after sell → book $10,331.75; vs 09:30 mark -3.58 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 31 | $31.87 | $2.08 | $-9.48 | $12,287.66 | ▼ -9.48 after sell → book $10,329.66; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 329 | $2.95 | $4.24 | $+11.16 | $11,312.87 | ▲ +11.16 after sell → book $10,325.42; vs 09:30 mark -4.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 145 | $6.81 | $2.42 | $-6.36 | $10,322.99 | ▼ -6.36 after sell → book $10,322.99; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 48 | $21.40 | $2.18 | — | $11,348.01 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; 🔵; ret5=-25.2; leftover $1032.30 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 233 | $4.43 | $3.08 | — | $12,377.12 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; 🔵; ret5=-23.1; leftover $1032.30 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 87 | $11.81 | $2.30 | — | $13,402.72 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1032.30 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 22 | $46.85 | $2.10 | — | $14,431.32 | — | short prior-export headline🔴; gate headline=bad; list earn_react; 🔵; ret5=+5.0; leftover $1032.30 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 223 | $4.61 | $2.95 | — | $15,456.39 | — | short prior-export headline🔴; gate headline=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1032.30 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,456.39 | ▲ close $10,447.16 vs 09:30 $10,348.47 (session +136.80) | 16:00 close · cash $15,456.39 · equity $10,447.16 vs 09:30 $10,348.47 (+98.69; session marks +136.80) · 5 name(s) marked open→close (per-name table). WYFI×48 09:30 $21.40 → close $21.16 +11.52; TOYO×233 09:30 $4.43 → close $4.51 -19.80; ABCL×87 09:30 $11.81 → close $11.57 +21.31; AAP×22 09:30 $46.85 → close $42.39 +98.12; AQST×223 09:30 $4.61 → close $4.50 +25.65 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,456.39 | ▼ 09:30 equity $10,380.00 vs yday $10,447.16 (-67.16) | 09:30 open · cash $15,456.39 (unchanged overnight, no fees) · equity $10,380.00 vs prior close $10,447.16 (-67.16) · 5 name(s) re-marked at the open (per-name table). WYFI×48 yday $21.16 → 09:30 $21.54 -18.24; TOYO×233 yday $4.51 → 09:30 $4.68 -38.45; ABCL×87 yday $11.57 → 09:30 $11.57 -0.00; AAP×22 yday $42.39 → 09:30 $42.41 -0.44; AQST×223 yday $4.50 → 09:30 $4.54 -10.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 417 | $3.11 | $5.49 | — | $16,747.77 | — | short prior-export headline🔴; gate headline=bad; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $1297.50 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 9 | $133.11 | $2.07 | — | $17,943.69 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; leftover $1297.50 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 62 | $20.90 | $2.23 | — | $19,237.26 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1297.50 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 48 | $27.00 | $2.19 | — | $20,531.07 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; ret5=+10.1; leftover $1297.50 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,531.07 | ▼ close $10,294.03 vs 09:30 $10,380.00 (session -73.99) | 16:00 close · cash $20,531.07 · equity $10,294.03 vs 09:30 $10,380.00 (-85.97; session marks -73.99) · 9 name(s) marked open→close (per-name table). WYFI×48 09:30 $21.54 → close $20.72 +39.36; TOYO×233 09:30 $4.68 → close $4.82 -32.62; ABCL×87 09:30 $11.57 → close $11.32 +21.75; AAP×22 09:30 $42.41 → close $42.58 -3.74; AQST×223 09:30 $4.54 → close $4.66 -26.76; QTRX×417 09:30 $3.11 → close $2.99 +50.04; MRNA×9 09:30 $133.11 → close $145.13 -108.18; ARIS×62 09:30 $20.90 → close $20.86 +2.48; NOG×48 09:30 $27.00 → close $27.34 -16.32 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,531.07 | ▲ 09:30 equity $10,426.90 vs yday $10,294.03 (+132.87) | 09:30 open · cash $20,531.07 (unchanged overnight, no fees) · equity $10,426.90 vs prior close $10,294.03 (+132.87) · 9 name(s) re-marked at the open (per-name table). WYFI×48 yday $20.72 → 09:30 $20.01 +34.08; TOYO×233 yday $4.82 → 09:30 $4.58 +55.92; ABCL×87 yday $11.32 → 09:30 $10.97 +30.45; AAP×22 yday $42.58 → 09:30 $43.05 -10.34; AQST×223 yday $4.66 → 09:30 $4.67 -2.23; QTRX×417 yday $2.99 → 09:30 $2.99 -0.00; MRNA×9 yday $145.13 → 09:30 $142.70 +21.87; ARIS×62 yday $20.86 → 09:30 $20.98 -7.44; NOG×48 yday $27.34 → 09:30 $27.12 +10.56 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,531.07 | ▲ close $10,563.61 vs 09:30 $10,426.90 (session +136.71) | 16:00 close · cash $20,531.07 · equity $10,563.61 vs 09:30 $10,426.90 (+136.71; session marks +136.71) · 9 name(s) marked open→close (per-name table). WYFI×48 09:30 $20.01 → close $20.78 -36.96; TOYO×233 09:30 $4.58 → close $4.38 +46.60; ABCL×87 09:30 $10.97 → close $10.61 +31.32; AAP×22 09:30 $43.05 → close $43.63 -12.76; AQST×223 09:30 $4.67 → close $4.80 -28.99; QTRX×417 09:30 $2.99 → close $2.80 +79.23; MRNA×9 09:30 $142.70 → close $138.89 +34.29; ARIS×62 09:30 $20.98 → close $20.81 +10.54; NOG×48 09:30 $27.12 → close $26.84 +13.44 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,531.07 | ▼ 09:30 equity $10,539.56 vs yday $10,563.61 (-24.05) | 09:30 open · cash $20,531.07 (unchanged overnight, no fees) · equity $10,539.56 vs prior close $10,563.61 (-24.05) · 9 name(s) re-marked at the open (per-name table). WYFI×48 yday $20.78 → 09:30 $20.90 -5.76; TOYO×233 yday $4.38 → 09:30 $4.42 -9.32; ABCL×87 yday $10.61 → 09:30 $11.00 -33.93; AAP×22 yday $43.63 → 09:30 $43.63 -0.00; AQST×223 yday $4.80 → 09:30 $4.77 +6.69; QTRX×417 yday $2.80 → 09:30 $2.80 -0.00; MRNA×9 yday $138.89 → 09:30 $143.50 -41.49; ARIS×62 yday $20.81 → 09:30 $20.45 +22.32; NOG×48 yday $26.84 → 09:30 $26.06 +37.44 | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 48 | $20.90 | $2.13 | $+19.68 | $19,525.73 | ▲ +19.68 after sell → book $10,537.42; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 233 | $4.42 | $3.01 | $-3.76 | $18,492.87 | ▼ -3.76 after sell → book $10,534.42; vs 09:30 mark -3.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 87 | $11.00 | $2.25 | $+66.35 | $17,533.62 | ▲ +66.35 after sell → book $10,532.17; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 22 | $43.63 | $2.06 | $+66.68 | $16,571.70 | ▲ +66.68 after sell → book $10,530.11; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 223 | $4.77 | $2.88 | $-41.51 | $15,505.11 | ▼ -41.51 after sell → book $10,527.23; vs 09:30 mark -2.88 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 386 | $13.62 | $5.23 | — | $20,759.14 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $5263.62 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,759.14 | ▼ close $10,339.18 vs 09:30 $10,539.56 (session -182.83) | 16:00 close · cash $20,759.14 · equity $10,339.18 vs 09:30 $10,539.56 (-200.38; session marks -182.83) · 5 name(s) marked open→close (per-name table). QTRX×417 09:30 $2.80 → close $2.79 +4.17; MRNA×9 09:30 $143.50 → close $158.83 -137.97; ARIS×62 09:30 $20.45 → close $21.18 -45.26; NOG×48 09:30 $26.06 → close $26.42 -17.28; AVAH×386 09:30 $13.62 → close $13.59 +13.51 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,759.14 | ▲ 09:30 equity $10,403.33 vs yday $10,339.18 (+64.15) | 09:30 open · cash $20,759.14 (unchanged overnight, no fees) · equity $10,403.33 vs prior close $10,339.18 (+64.15) · 5 name(s) re-marked at the open (per-name table). QTRX×417 yday $2.79 → 09:30 $2.83 -16.68; MRNA×9 yday $158.83 → 09:30 $154.20 +41.67; ARIS×62 yday $21.18 → 09:30 $20.50 +42.16; NOG×48 yday $26.42 → 09:30 $26.00 +20.16; AVAH×386 yday $13.59 → 09:30 $13.65 -23.16 | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 417 | $2.83 | $5.38 | $+105.89 | $19,573.65 | ▲ +105.89 after sell → book $10,397.95; vs 09:30 mark -5.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 9 | $154.20 | $2.02 | $-193.90 | $18,183.83 | ▼ -193.90 after sell → book $10,395.93; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 62 | $20.50 | $2.18 | $+20.39 | $16,910.65 | ▲ +20.39 after sell → book $10,393.75; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 48 | $26.00 | $2.13 | $+43.68 | $15,660.52 | ▲ +43.68 after sell → book $10,391.62; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 212 | $12.22 | $2.86 | — | $18,248.30 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ret5=+12.4; leftover $2597.90 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 511 | $5.08 | $6.77 | — | $20,837.41 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ret5=+17.6; leftover $2597.90 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,837.41 | ▼ close $10,230.92 vs 09:30 $10,403.33 (session -151.07) | 16:00 close · cash $20,837.41 · equity $10,230.92 vs 09:30 $10,403.33 (-172.41; session marks -151.07) · 3 name(s) marked open→close (per-name table). AVAH×386 09:30 $13.65 → close $13.62 +11.58; ABCL×212 09:30 $12.22 → close $12.24 -4.24; AQST×511 09:30 $5.08 → close $5.39 -158.41 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,837.41 | ▼ 09:30 equity $10,228.80 vs yday $10,230.92 (-2.12) | 09:30 open · cash $20,837.41 (unchanged overnight, no fees) · equity $10,228.80 vs prior close $10,230.92 (-2.12) · 3 name(s) re-marked at the open (per-name table). AVAH×386 yday $13.62 → 09:30 $13.62 -0.00; ABCL×212 yday $12.24 → 09:30 $12.25 -2.12; AQST×511 yday $5.39 → 09:30 $5.39 -0.00 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 22 | $74.54 | $2.12 | — | $22,475.16 | — | short prior-export headline🔴; gate headline=bad; list mover_buy; 🔵; ret5=-0.1; leftover $1704.80 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `MU` | 1 | $967.01 | $2.04 | — | $23,440.13 | — | short prior-export headline🔴; gate headline=bad; list mover_buy; 🔵; ret5=+0.1; leftover $1704.80 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 30 | $55.25 | $2.15 | — | $25,095.49 | — | short prior-export headline🔴; gate headline=bad; list mover_buy; 🔵; ret5=+2.1; leftover $1704.80 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25,095.49 | ▲ close $10,243.26 vs 09:30 $10,228.80 (session +20.77) | 16:00 close · cash $25,095.49 · equity $10,243.26 vs 09:30 $10,228.80 (+14.46; session marks +20.77) · 6 name(s) marked open→close (per-name table). AVAH×386 09:30 $13.62 → close $13.82 -77.20; ABCL×212 09:30 $12.25 → close $12.40 -31.80; AQST×511 09:30 $5.39 → close $5.16 +117.53; MT×22 09:30 $74.54 → close $74.63 -1.98; MU×1 09:30 $967.01 → close $935.39 +31.62; TX×30 09:30 $55.25 → close $55.83 -17.40 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25,095.49 | ▲ 09:30 equity $10,253.25 vs yday $10,243.26 (+9.99) | 09:30 open · cash $25,095.49 (unchanged overnight, no fees) · equity $10,253.25 vs prior close $10,243.26 (+9.99) · 6 name(s) re-marked at the open (per-name table). AVAH×386 yday $13.82 → 09:30 $13.90 -30.88; ABCL×212 yday $12.40 → 09:30 $12.30 +20.14; AQST×511 yday $5.16 → 09:30 $5.11 +25.55; MT×22 yday $74.63 → 09:30 $75.39 -16.72; MU×1 yday $935.39 → 09:30 $919.29 +16.10; TX×30 yday $55.83 → 09:30 $55.97 -4.20 | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 386 | $13.90 | $4.98 | $-116.36 | $19,725.11 | ▼ -116.36 after sell → book $10,248.27; vs 09:30 mark -4.98 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 20 | $252.24 | $2.24 | — | $24,767.67 | — | short prior-export headline🔴; gate headline=bad; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $5124.13 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,767.67 | ▲ close $10,665.41 vs 09:30 $10,253.25 (session +419.38) | 16:00 close · cash $24,767.67 · equity $10,665.41 vs 09:30 $10,253.25 (+412.16; session marks +419.38) · 6 name(s) marked open→close (per-name table). ABCL×212 09:30 $12.30 → close $11.35 +202.46; AQST×511 09:30 $5.11 → close $5.02 +45.99; MT×22 09:30 $75.39 → close $74.39 +22.00; MU×1 09:30 $919.29 → close $932.86 -13.57; TX×30 09:30 $55.97 → close $54.84 +33.90; SIMO×20 09:30 $252.24 → close $245.81 +128.60 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,767.67 | ▲ 09:30 equity $10,688.09 vs yday $10,665.41 (+22.68) | 09:30 open · cash $24,767.67 (unchanged overnight, no fees) · equity $10,688.09 vs prior close $10,665.41 (+22.68) · 6 name(s) re-marked at the open (per-name table). ABCL×212 yday $11.35 → 09:30 $11.10 +53.00; AQST×511 yday $5.02 → 09:30 $4.97 +22.99; MT×22 yday $74.39 → 09:30 $75.18 -17.38; MU×1 yday $932.86 → 09:30 $931.39 +1.47; TX×30 yday $54.84 → 09:30 $55.26 -12.60; SIMO×20 yday $245.81 → 09:30 $247.05 -24.80 | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 212 | $11.10 | $2.73 | $+231.84 | $22,411.73 | ▲ +231.84 after sell → book $10,685.36; vs 09:30 mark -2.73 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 511 | $4.97 | $6.59 | $+40.29 | $19,862.92 | ▲ +40.29 after sell → book $10,678.77; vs 09:30 mark -6.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,862.92 | ▲ close $10,681.59 vs 09:30 $10,688.09 (session +2.82) | 16:00 close · cash $19,862.92 · equity $10,681.59 vs 09:30 $10,688.09 (-6.50; session marks +2.82) · 4 name(s) marked open→close (per-name table). MT×22 09:30 $75.18 → close $74.60 +12.76; MU×1 09:30 $931.39 → close $958.73 -27.34; TX×30 09:30 $55.26 → close $54.82 +13.20; SIMO×20 09:30 $247.05 → close $246.84 +4.20 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,862.92 | ▲ 09:30 equity $10,866.35 vs yday $10,681.59 (+184.76) | 09:30 open · cash $19,862.92 (unchanged overnight, no fees) · equity $10,866.35 vs prior close $10,681.59 (+184.76) · 4 name(s) re-marked at the open (per-name table). MT×22 yday $74.60 → 09:30 $73.22 +30.36; MU×1 yday $958.73 → 09:30 $941.13 +17.60; TX×30 yday $54.82 → 09:30 $54.76 +1.80; SIMO×20 yday $246.84 → 09:30 $240.09 +135.00 | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 22 | $73.22 | $2.06 | $+24.86 | $18,250.02 | ▲ +24.86 after sell → book $10,864.29; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `MU` | 1 | $941.13 | $1.99 | $+21.85 | $17,306.90 | ▲ +21.85 after sell → book $10,862.30; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 30 | $54.76 | $2.08 | $+10.47 | $15,662.02 | ▲ +10.47 after sell → book $10,860.22; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,662.02 | ▲ close $10,915.02 vs 09:30 $10,866.35 (session +54.80) | 16:00 close · cash $15,662.02 · equity $10,915.02 vs 09:30 $10,866.35 (+48.67; session marks +54.80) · 1 name(s) marked open→close (per-name table). SIMO×20 09:30 $240.09 → close $237.35 +54.80 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,662.02 | ▲ 09:30 equity $10,947.82 vs yday $10,915.02 (+32.80) | 09:30 open · cash $15,662.02 (unchanged overnight, no fees) · equity $10,947.82 vs prior close $10,915.02 (+32.80) · 1 name(s) re-marked at the open (per-name table). SIMO×20 yday $237.35 → 09:30 $235.71 +32.80 | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 20 | $235.71 | $2.05 | $+326.31 | $10,945.77 | ▲ +326.31 after sell → book $10,945.77; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,945.77 | ▲ close $10,945.77 vs 09:30 $10,947.82 (session +0.00) | 16:00 close · cash $10,945.77 · no lots left · equity $10,945.77. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,945.77 | ▲ 09:30 equity $10,945.77 vs yday $10,945.77 (-0.00) | 09:30 open · cash $10,945.77 · no holdings · equity $10,945.77 vs prior close $10,945.77 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 184 | $14.85 | $2.67 | — | $13,675.50 | — | short prior-export headline🔴; gate headline=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $2736.44 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1600 | $1.71 | $21.00 | — | $16,390.50 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; leftover $2736.44 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,390.50 | ▲ close $11,093.14 vs 09:30 $10,945.77 (session +171.04) | 16:00 close · cash $16,390.50 · equity $11,093.14 vs 09:30 $10,945.77 (+147.37; session marks +171.04) · 2 name(s) marked open→close (per-name table). SLN×184 09:30 $14.85 → close $14.79 +11.04; OPK×1600 09:30 $1.71 → close $1.61 +160.00 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,390.50 | ▲ 09:30 equity $11,154.58 vs yday $11,093.14 (+61.44) | 09:30 open · cash $16,390.50 (unchanged overnight, no fees) · equity $11,154.58 vs prior close $11,093.14 (+61.44) · 2 name(s) re-marked at the open (per-name table). SLN×184 yday $14.79 → 09:30 $14.63 +29.44; OPK×1600 yday $1.61 → 09:30 $1.59 +32.00 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 597 | $4.67 | $7.90 | — | $19,170.59 | — | short prior-export headline🔴; gate headline=bad; list yday_gainer; ret5=+11.9; leftover $2788.64 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 36 | $76.55 | $2.21 | — | $21,924.18 | — | short prior-export headline🔴; gate headline=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; leftover $2788.64 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,924.18 | ▼ close $11,054.19 vs 09:30 $11,154.58 (session -90.28) | 16:00 close · cash $21,924.18 · equity $11,054.19 vs 09:30 $11,154.58 (-100.39; session marks -90.28) · 4 name(s) marked open→close (per-name table). SLN×184 09:30 $14.63 → close $14.59 +7.36; OPK×1600 09:30 $1.59 → close $1.64 -80.00; GSM×597 09:30 $4.67 → close $4.67 -0.00; PIPR×36 09:30 $76.55 → close $77.04 -17.64 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,924.18 | ▲ 09:30 equity $11,101.23 vs yday $11,054.19 (+47.04) | 09:30 open · cash $21,924.18 (unchanged overnight, no fees) · equity $11,101.23 vs prior close $11,054.19 (+47.04) · 4 name(s) re-marked at the open (per-name table). SLN×184 yday $14.59 → 09:30 $14.24 +64.40; OPK×1600 yday $1.64 → 09:30 $1.63 +16.00; GSM×597 yday $4.67 → 09:30 $4.75 -47.76; PIPR×36 yday $77.04 → 09:30 $76.64 +14.40 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,924.18 | ▲ close $11,378.54 vs 09:30 $11,101.23 (session +277.31) | 16:00 close · cash $21,924.18 · equity $11,378.54 vs 09:30 $11,101.23 (+277.31; session marks +277.31) · 4 name(s) marked open→close (per-name table). SLN×184 09:30 $14.24 → close $13.69 +101.20; OPK×1600 09:30 $1.63 → close $1.59 +64.00; GSM×597 09:30 $4.75 → close $4.52 +137.31; PIPR×36 09:30 $76.64 → close $77.34 -25.20 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,924.18 | ▲ 09:30 equity $11,414.70 vs yday $11,378.54 (+36.16) | 09:30 open · cash $21,924.18 (unchanged overnight, no fees) · equity $11,414.70 vs prior close $11,378.54 (+36.16) · 4 name(s) re-marked at the open (per-name table). SLN×184 yday $13.69 → 09:30 $13.60 +16.56; OPK×1600 yday $1.59 → 09:30 $1.58 +16.00; GSM×597 yday $4.52 → 09:30 $4.52 -0.00; PIPR×36 yday $77.34 → 09:30 $77.24 +3.60 | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 184 | $13.60 | $2.54 | $+224.79 | $19,419.24 | ▲ +224.79 after sell → book $11,412.16; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1600 | $1.58 | $20.64 | $+166.36 | $16,870.60 | ▲ +166.36 after sell → book $11,391.52; vs 09:30 mark -20.64 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,870.60 | ▲ close $11,419.51 vs 09:30 $11,414.70 (session +27.99) | 16:00 close · cash $16,870.60 · equity $11,419.51 vs 09:30 $11,414.70 (+4.81; session marks +27.99) · 2 name(s) marked open→close (per-name table). GSM×597 09:30 $4.52 → close $4.49 +17.91; PIPR×36 09:30 $77.24 → close $76.96 +10.08 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,870.60 | ▲ 09:30 equity $11,503.24 vs yday $11,419.51 (+83.73) | 09:30 open · cash $16,870.60 (unchanged overnight, no fees) · equity $11,503.24 vs prior close $11,419.51 (+83.73) · 2 name(s) re-marked at the open (per-name table). GSM×597 yday $4.49 → 09:30 $4.36 +77.61; PIPR×36 yday $76.96 → 09:30 $76.79 +6.12 | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 597 | $4.36 | $7.70 | $+169.47 | $14,259.98 | ▲ +169.47 after sell → book $11,495.54; vs 09:30 mark -7.70 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 36 | $76.79 | $2.10 | $-12.94 | $11,493.44 | ▼ -12.94 after sell → book $11,493.44; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,493.44 | ▲ close $11,493.44 vs 09:30 $11,503.24 (session +0.00) | 16:00 close · cash $11,493.44 · no lots left · equity $11,493.44. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,493.44 | ▲ 09:30 equity $11,493.44 vs yday $11,493.44 (-0.00) | 09:30 open · cash $11,493.44 · no holdings · equity $11,493.44 vs prior close $11,493.44 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 568 | $3.37 | $7.49 | — | $13,400.11 | — | short prior-export headline🔴; gate headline=bad; list yday_gainer,ohlc_hot; 🔵; ret5=+4.0; leftover $1915.57 | join🔴 sector🟡 gen🟡 news🔴 digest🔴 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 544 | $3.52 | $7.18 | — | $15,307.81 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; 🔵; ret5=-19.2; leftover $1915.57 | join🔴 sector🔴 gen🟡 news🔴 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 943 | $2.03 | $12.39 | — | $17,209.72 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; ret5=-8.8; leftover $1915.57 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 judge🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,209.72 | ▼ close $11,454.67 vs 09:30 $11,493.44 (session -11.72) | 16:00 close · cash $17,209.72 · equity $11,454.67 vs 09:30 $11,493.44 (-38.77; session marks -11.72) · 3 name(s) marked open→close (per-name table). MYGN×568 09:30 $3.37 → close $3.42 -28.40; RWT×544 09:30 $3.52 → close $3.55 -16.32; CRDL×943 09:30 $2.03 → close $2.00 +33.00 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,209.72 | ▲ 09:30 equity $11,474.02 vs yday $11,454.67 (+19.35) | 09:30 open · cash $17,209.72 (unchanged overnight, no fees) · equity $11,474.02 vs prior close $11,454.67 (+19.35) · 3 name(s) re-marked at the open (per-name table). MYGN×568 yday $3.42 → 09:30 $3.43 -5.68; RWT×544 yday $3.55 → 09:30 $3.53 +10.88; CRDL×943 yday $2.00 → 09:30 $1.98 +14.15 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,209.72 | ▼ close $11,096.91 vs 09:30 $11,474.02 (session -377.11) | 16:00 close · cash $17,209.72 · equity $11,096.91 vs 09:30 $11,474.02 (-377.11; session marks -377.11) · 3 name(s) marked open→close (per-name table). MYGN×568 09:30 $3.43 → close $3.79 -204.48; RWT×544 09:30 $3.53 → close $3.83 -163.20; CRDL×943 09:30 $1.98 → close $1.99 -9.43 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,209.72 | ▲ 09:30 equity $11,135.84 vs yday $11,096.91 (+38.93) | 09:30 open · cash $17,209.72 (unchanged overnight, no fees) · equity $11,135.84 vs prior close $11,096.91 (+38.93) · 3 name(s) re-marked at the open (per-name table). MYGN×568 yday $3.79 → 09:30 $3.80 -5.68; RWT×544 yday $3.83 → 09:30 $3.80 +16.32; CRDL×943 yday $1.99 → 09:30 $1.96 +28.29 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,209.72 | ▼ close $11,134.29 vs 09:30 $11,135.84 (session -1.55) | 16:00 close · cash $17,209.72 · equity $11,134.29 vs 09:30 $11,135.84 (-1.55; session marks -1.55) · 3 name(s) marked open→close (per-name table). MYGN×568 09:30 $3.80 → close $3.88 -45.44; RWT×544 09:30 $3.80 → close $3.91 -59.84; CRDL×943 09:30 $1.96 → close $1.85 +103.73 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,209.72 | ▲ 09:30 equity $11,170.05 vs yday $11,134.29 (+35.76) | 09:30 open · cash $17,209.72 (unchanged overnight, no fees) · equity $11,170.05 vs prior close $11,134.29 (+35.76) · 3 name(s) re-marked at the open (per-name table). MYGN×568 yday $3.88 → 09:30 $3.75 +73.84; RWT×544 yday $3.91 → 09:30 $3.98 -38.08; CRDL×943 yday $1.85 → 09:30 $1.85 -0.00 | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 568 | $3.75 | $7.33 | $-230.66 | $15,072.39 | ▼ -230.66 after sell → book $11,162.72; vs 09:30 mark -7.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 544 | $3.98 | $7.02 | $-264.43 | $12,900.25 | ▼ -264.43 after sell → book $11,155.70; vs 09:30 mark -7.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 943 | $1.85 | $12.16 | $+145.19 | $11,143.54 | ▲ +145.19 after sell → book $11,143.54; vs 09:30 mark -12.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 149 | $18.61 | $2.56 | — | $13,913.87 | — | short prior-export headline🔴; gate headline=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $2785.88 | join🟢 sector🟢 gen🟢 news🔴 digest🔴 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 407 | $6.83 | $5.42 | — | $16,688.26 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; ret5=+11.2; leftover $2785.88 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,688.26 | ▼ close $10,742.01 vs 09:30 $11,170.05 (session -393.55) | 16:00 close · cash $16,688.26 · equity $10,742.01 vs 09:30 $11,170.05 (-428.04; session marks -393.55) · 2 name(s) marked open→close (per-name table). BBNX×149 09:30 $18.61 → close $22.18 -531.93; GFR×407 09:30 $6.83 → close $6.49 +138.38 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,688.26 | ▼ 09:30 equity $10,704.36 vs yday $10,742.01 (-37.65) | 09:30 open · cash $16,688.26 (unchanged overnight, no fees) · equity $10,704.36 vs prior close $10,742.01 (-37.65) · 2 name(s) re-marked at the open (per-name table). BBNX×149 yday $22.18 → 09:30 $22.46 -41.72; GFR×407 yday $6.49 → 09:30 $6.48 +4.07 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 336 | $7.95 | $4.48 | — | $19,354.98 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $2676.09 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 33 | $81.00 | $2.19 | — | $22,025.78 | — | short prior-export headline🔴; gate headline=bad; list earn_react; 🔵; ret5=-3.0; leftover $2676.09 | join🔴 sector🟢 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,025.78 | ▲ close $10,901.43 vs 09:30 $10,704.36 (session +203.75) | 16:00 close · cash $22,025.78 · equity $10,901.43 vs 09:30 $10,704.36 (+197.07; session marks +203.75) · 4 name(s) marked open→close (per-name table). BBNX×149 09:30 $22.46 → close $21.43 +153.47; GFR×407 09:30 $6.48 → close $6.66 -73.26; BULL×336 09:30 $7.95 → close $7.71 +80.64; LEN×33 09:30 $81.00 → close $79.70 +42.90 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,025.78 | ▲ 09:30 equity $10,929.75 vs yday $10,901.43 (+28.32) | 09:30 open · cash $22,025.78 (unchanged overnight, no fees) · equity $10,929.75 vs prior close $10,901.43 (+28.32) · 4 name(s) re-marked at the open (per-name table). BBNX×149 yday $21.43 → 09:30 $21.30 +19.37; GFR×407 yday $6.66 → 09:30 $6.64 +8.14; BULL×336 yday $7.71 → 09:30 $7.85 -47.04; LEN×33 yday $79.70 → 09:30 $78.25 +47.85 | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 158 | $34.44 | $2.68 | — | $27,464.62 | — | short prior-export headline🔴; gate headline=bad; list flatten; 🔵; ⚪; ret5=-9.6; leftover $5464.88 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27,464.62 | ▲ close $11,067.94 vs 09:30 $10,929.75 (session +140.87) | 16:00 close · cash $27,464.62 · equity $11,067.94 vs 09:30 $10,929.75 (+138.19; session marks +140.87) · 5 name(s) marked open→close (per-name table). BBNX×149 09:30 $21.30 → close $21.89 -87.91; GFR×407 09:30 $6.64 → close $6.66 -8.14; BULL×336 09:30 $7.85 → close $8.25 -134.40; LEN×33 09:30 $78.25 → close $76.43 +60.06; FIVN×158 09:30 $34.44 → close $32.47 +311.26 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27,464.62 | ▼ 09:30 equity $10,870.48 vs yday $11,067.94 (-197.46) | 09:30 open · cash $27,464.62 (unchanged overnight, no fees) · equity $10,870.48 vs prior close $11,067.94 (-197.46) · 5 name(s) re-marked at the open (per-name table). BBNX×149 yday $21.89 → 09:30 $22.11 -32.78; GFR×407 yday $6.66 → 09:30 $6.55 +44.77; BULL×336 yday $8.25 → 09:30 $8.58 -110.88; LEN×33 yday $76.43 → 09:30 $76.97 -17.98; FIVN×158 yday $32.47 → 09:30 $32.98 -80.58 | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 149 | $22.11 | $2.44 | $-526.50 | $24,167.79 | ▼ -526.50 after sell → book $10,868.05; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 407 | $6.55 | $5.25 | $+103.29 | $21,496.69 | ▲ +103.29 after sell → book $10,862.80; vs 09:30 mark -5.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 328 | $8.26 | $4.38 | — | $24,201.59 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; ret5=+23.9; leftover $2715.70 | join🟡 sector🔴 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 4 | $583.88 | $2.09 | — | $26,535.02 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; ret5=+8.5; leftover $2715.70 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26,535.02 | ▼ close $10,606.96 vs 09:30 $10,870.48 (session -249.37) | 16:00 close · cash $26,535.02 · equity $10,606.96 vs 09:30 $10,870.48 (-263.52; session marks -249.37) · 5 name(s) marked open→close (per-name table). BULL×336 09:30 $8.58 → close $8.25 +110.88; LEN×33 09:30 $76.97 → close $78.08 -36.47; FIVN×158 09:30 $32.98 → close $37.01 -636.74; AEHL×328 09:30 $8.26 → close $6.92 +439.52; AMD×4 09:30 $583.88 → close $615.52 -126.56 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `EU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LUNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `OWL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LUNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OWL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ZNTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `VERI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ZNTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `APMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `RNW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `WYFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TOYO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AQST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `WYFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TOYO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARIS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARIS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `NOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `TX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `TX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PIPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `GSM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `PIPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `RWT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `MYGN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RWT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SPCX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `GFR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `GFR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BULL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `LEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BULL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `LEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `BULL` | 336 | 2026-09-17 @ $7.95 | short prior-export headline🔴; gate headline=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $2676.09 |
| `LEN` | 33 | 2026-09-17 @ $81.00 | short prior-export headline🔴; gate headline=bad; list earn_react; 🔵; ret5=-3.0; leftover $2676.09 |
| `FIVN` | 158 | 2026-09-18 @ $34.44 | short prior-export headline🔴; gate headline=bad; list flatten; 🔵; ⚪; ret5=-9.6; leftover $5464.88 |
| `AEHL` | 328 | 2026-09-21 @ $8.26 | short prior-export headline🔴; gate headline=bad; list yday_mover; ret5=+23.9; leftover $2715.70 |
| `AMD` | 4 | 2026-09-21 @ $583.88 | short prior-export headline🔴; gate headline=bad; list ohlc_hot; ret5=+8.5; leftover $2715.70 |
