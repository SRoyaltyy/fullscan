# Factor mine action — `short_news_head_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · short prior-export headline🔴

Cash book **+13.78%** ($11,377) · signal-only (no cash/fees) was +24.01%. Starts YES **17/22**. Fills 51 · skips 63 · realized $+1441.98.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $17,128.42.

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
| 2026-08-17 | `VERI` | 1077 | — | $1.15 | +0.00 | $1.08 | +70.00 | +70.00 | -0.00 | +70.00 |
| 2026-08-17 | `ZNTL` | 348 | — | $3.56 | +0.00 | $3.71 | -50.46 | -50.46 | -0.00 | -50.46 |
| 2026-08-17 | `APMD` | 39 | — | $31.70 | +0.00 | $32.55 | -33.15 | -33.15 | -0.00 | -33.15 |
| 2026-08-17 | `HIVE` | 411 | — | $3.01 | +0.00 | $3.07 | -24.66 | -24.66 | -0.00 | -24.66 |
| 2026-08-18 | `EU` | 1412 | $1.13 | $1.13 | +0.00 | $1.07 | +84.72 | +84.72 | +70.60 | +155.32 |
| 2026-08-18 | `LUNR` | 86 | $20.38 | $19.31 | +92.02 | $19.31 | +0.00 | +92.02 | -12.04 | -12.04 |
| 2026-08-18 | `OWL` | 131 | $11.66 | $11.54 | +15.72 | $11.59 | -6.55 | +9.17 | +151.31 | +144.76 |
| 2026-08-18 | `VERI` | 1077 | $1.08 | $1.05 | +37.69 | $0.99 | +59.24 | +96.93 | +107.70 | +166.93 |
| 2026-08-18 | `ZNTL` | 348 | $3.71 | $3.75 | -15.66 | $3.68 | +24.36 | +8.70 | -66.12 | -41.76 |
| 2026-08-18 | `APMD` | 39 | $32.55 | $32.85 | -11.70 | $31.81 | +40.56 | +28.86 | -44.85 | -4.29 |
| 2026-08-18 | `HIVE` | 411 | $3.07 | $2.96 | +45.21 | $2.78 | +73.98 | +119.19 | +20.55 | +94.53 |
| 2026-08-19 | `EU` | 1412 | $1.07 | $1.07 | +0.00 | — | +0.00 | +0.00 | +155.32 | — |
| 2026-08-19 | `LUNR` | 86 | $19.31 | $18.98 | +28.38 | $18.52 | +39.56 | +67.94 | +16.34 | +55.90 |
| 2026-08-19 | `OWL` | 131 | $11.59 | $11.75 | -20.96 | — | +0.00 | -20.96 | +123.80 | — |
| 2026-08-19 | `VERI` | 1077 | $0.99 | $1.00 | -5.39 | $0.97 | +36.62 | +31.23 | +161.55 | +198.17 |
| 2026-08-19 | `ZNTL` | 348 | $3.68 | $3.76 | -27.84 | $3.82 | -20.88 | -48.72 | -69.60 | -90.48 |
| 2026-08-19 | `APMD` | 39 | $31.81 | $32.13 | -12.48 | $32.03 | +3.90 | -8.58 | -16.77 | -12.87 |
| 2026-08-19 | `HIVE` | 411 | $2.78 | $2.78 | +0.00 | $2.82 | -16.44 | -16.44 | +94.53 | +78.09 |
| 2026-08-20 | `LUNR` | 86 | $18.52 | $18.13 | +33.54 | — | +0.00 | +33.54 | +89.44 | — |
| 2026-08-20 | `VERI` | 1077 | $0.97 | $0.96 | +3.23 | — | +0.00 | +3.23 | +201.40 | — |
| 2026-08-20 | `ZNTL` | 348 | $3.82 | $4.01 | -67.86 | — | +0.00 | -67.86 | -158.34 | — |
| 2026-08-20 | `APMD` | 39 | $32.03 | $31.87 | +6.24 | — | +0.00 | +6.24 | -6.63 | — |
| 2026-08-20 | `HIVE` | 411 | $2.82 | $2.95 | -53.43 | — | +0.00 | -53.43 | +24.66 | — |
| 2026-08-20 | `WYFI` | 48 | — | $21.40 | +0.00 | $21.16 | +11.52 | +11.52 | -0.00 | +11.52 |
| 2026-08-20 | `TOYO` | 233 | — | $4.43 | +0.00 | $4.51 | -19.80 | -19.80 | -0.00 | -19.80 |
| 2026-08-20 | `ABCL` | 87 | — | $11.81 | +0.00 | $11.57 | +21.31 | +21.31 | -0.00 | +21.31 |
| 2026-08-20 | `AAP` | 22 | — | $46.85 | +0.00 | $42.39 | +98.12 | +98.12 | -0.00 | +98.12 |
| 2026-08-20 | `AQST` | 224 | — | $4.61 | +0.00 | $4.50 | +25.76 | +25.76 | -0.00 | +25.76 |
| 2026-08-21 | `WYFI` | 48 | $21.16 | $21.54 | -18.24 | $20.72 | +39.36 | +21.12 | -6.72 | +32.64 |
| 2026-08-21 | `TOYO` | 233 | $4.51 | $4.68 | -38.45 | $4.82 | -32.62 | -71.07 | -58.25 | -90.87 |
| 2026-08-21 | `ABCL` | 87 | $11.57 | $11.57 | +0.00 | $11.32 | +21.75 | +21.75 | +21.31 | +43.06 |
| 2026-08-21 | `AAP` | 22 | $42.39 | $42.41 | -0.44 | $42.58 | -3.74 | -4.18 | +97.68 | +93.94 |
| 2026-08-21 | `AQST` | 224 | $4.50 | $4.54 | -10.08 | $4.66 | -26.88 | -36.96 | +15.68 | -11.20 |
| 2026-08-21 | `QTRX` | 417 | — | $3.11 | +0.00 | $2.99 | +50.04 | +50.04 | -0.00 | +50.04 |
| 2026-08-21 | `MRNA` | 9 | — | $133.11 | +0.00 | $145.13 | -108.18 | -108.18 | -0.00 | -108.18 |
| 2026-08-21 | `ARIS` | 62 | — | $20.90 | +0.00 | $20.86 | +2.48 | +2.48 | -0.00 | +2.48 |
| 2026-08-21 | `NOG` | 48 | — | $27.00 | +0.00 | $27.34 | -16.32 | -16.32 | -0.00 | -16.32 |
| 2026-08-24 | `WYFI` | 48 | $20.72 | $20.01 | +34.08 | $20.78 | -36.96 | -2.88 | +66.72 | +29.76 |
| 2026-08-24 | `TOYO` | 233 | $4.82 | $4.58 | +55.92 | $4.38 | +46.60 | +102.52 | -34.95 | +11.65 |
| 2026-08-24 | `ABCL` | 87 | $11.32 | $10.97 | +30.45 | $10.61 | +31.32 | +61.77 | +73.51 | +104.84 |
| 2026-08-24 | `AAP` | 22 | $42.58 | $43.05 | -10.34 | $43.63 | -12.76 | -23.10 | +83.60 | +70.84 |
| 2026-08-24 | `AQST` | 224 | $4.66 | $4.67 | -2.24 | $4.80 | -29.12 | -31.36 | -13.44 | -42.56 |
| 2026-08-24 | `QTRX` | 417 | $2.99 | $2.99 | +0.00 | $2.80 | +79.23 | +79.23 | +50.04 | +129.27 |
| 2026-08-24 | `MRNA` | 9 | $145.13 | $142.70 | +21.87 | $138.89 | +34.29 | +56.16 | -86.31 | -52.02 |
| 2026-08-24 | `ARIS` | 62 | $20.86 | $20.98 | -7.44 | $20.81 | +10.54 | +3.10 | -4.96 | +5.58 |
| 2026-08-24 | `NOG` | 48 | $27.34 | $27.12 | +10.56 | $26.84 | +13.44 | +24.00 | -5.76 | +7.68 |
| 2026-08-25 | `WYFI` | 48 | $20.78 | $20.90 | -5.76 | — | +0.00 | -5.76 | +24.00 | — |
| 2026-08-25 | `TOYO` | 233 | $4.38 | $4.42 | -9.32 | — | +0.00 | -9.32 | +2.33 | — |
| 2026-08-25 | `ABCL` | 87 | $10.61 | $11.00 | -33.93 | — | +0.00 | -33.93 | +70.90 | — |
| 2026-08-25 | `AAP` | 22 | $43.63 | $43.63 | +0.00 | — | +0.00 | +0.00 | +70.84 | — |
| 2026-08-25 | `AQST` | 224 | $4.80 | $4.77 | +6.72 | — | +0.00 | +6.72 | -35.84 | — |
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
| 2026-08-28 | `AVAH` | 386 | $13.82 | $13.90 | -30.88 | — | +0.00 | -30.88 | -106.15 | — |
| 2026-08-28 | `ABCL` | 212 | $12.40 | $12.30 | +20.14 | $11.35 | +202.46 | +222.60 | -18.02 | +184.44 |
| 2026-08-28 | `AQST` | 511 | $5.16 | $5.11 | +25.55 | $5.02 | +45.99 | +71.54 | -15.33 | +30.66 |
| 2026-08-28 | `SIMO` | 20 | — | $252.24 | +0.00 | $245.81 | +128.60 | +128.60 | -0.00 | +128.60 |
| 2026-08-31 | `ABCL` | 212 | $11.35 | $11.10 | +53.00 | — | +0.00 | +53.00 | +237.44 | — |
| 2026-08-31 | `AQST` | 511 | $5.02 | $4.97 | +22.99 | — | +0.00 | +22.99 | +53.66 | — |
| 2026-08-31 | `SIMO` | 20 | $245.81 | $247.05 | -24.80 | $246.84 | +4.20 | -20.60 | +103.80 | +108.00 |
| 2026-09-01 | `SIMO` | 20 | $246.84 | $240.09 | +135.00 | $237.35 | +54.80 | +189.80 | +243.00 | +297.80 |
| 2026-09-02 | `SIMO` | 20 | $237.35 | $235.71 | +32.80 | — | +0.00 | +32.80 | +330.60 | — |
| 2026-09-03 | `SLN` | 183 | — | $14.85 | +0.00 | $14.79 | +10.98 | +10.98 | -0.00 | +10.98 |
| 2026-09-03 | `OPK` | 1593 | — | $1.71 | +0.00 | $1.61 | +159.30 | +159.30 | -0.00 | +159.30 |
| 2026-09-04 | `SLN` | 183 | $14.79 | $14.63 | +29.28 | $14.59 | +7.32 | +36.60 | +40.26 | +47.58 |
| 2026-09-04 | `OPK` | 1593 | $1.61 | $1.59 | +31.86 | $1.64 | -79.65 | -47.79 | +191.16 | +111.51 |
| 2026-09-04 | `GSM` | 594 | — | $4.67 | +0.00 | $4.67 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-09-04 | `PIPR` | 36 | — | $76.55 | +0.00 | $77.04 | -17.64 | -17.64 | -0.00 | -17.64 |
| 2026-09-08 | `SLN` | 183 | $14.59 | $14.24 | +64.05 | $13.69 | +100.65 | +164.70 | +111.63 | +212.28 |
| 2026-09-08 | `OPK` | 1593 | $1.64 | $1.63 | +15.93 | $1.59 | +63.72 | +79.65 | +127.44 | +191.16 |
| 2026-09-08 | `GSM` | 594 | $4.67 | $4.75 | -47.52 | $4.52 | +136.62 | +89.10 | -47.52 | +89.10 |
| 2026-09-08 | `PIPR` | 36 | $77.04 | $76.64 | +14.40 | $77.34 | -25.20 | -10.80 | -3.24 | -28.44 |
| 2026-09-09 | `SLN` | 183 | $13.69 | $13.60 | +16.47 | — | +0.00 | +16.47 | +228.75 | — |
| 2026-09-09 | `OPK` | 1593 | $1.59 | $1.58 | +15.93 | — | +0.00 | +15.93 | +207.09 | — |
| 2026-09-09 | `GSM` | 594 | $4.52 | $4.52 | +0.00 | $4.49 | +17.82 | +17.82 | +89.10 | +106.92 |
| 2026-09-09 | `PIPR` | 36 | $77.34 | $77.24 | +3.60 | $76.96 | +10.08 | +13.68 | -24.84 | -14.76 |
| 2026-09-10 | `GSM` | 594 | $4.49 | $4.36 | +77.22 | — | +0.00 | +77.22 | +184.14 | — |
| 2026-09-10 | `PIPR` | 36 | $76.96 | $76.79 | +6.12 | — | +0.00 | +6.12 | -8.64 | — |
| 2026-09-11 | `RWT` | 541 | — | $3.52 | +0.00 | $3.55 | -16.23 | -16.23 | -0.00 | -16.23 |
| 2026-09-11 | `CRDL` | 939 | — | $2.03 | +0.00 | $2.00 | +32.86 | +32.86 | -0.00 | +32.86 |
| 2026-09-11 | `BKV` | 76 | — | $24.97 | +0.00 | $24.23 | +56.24 | +56.24 | -0.00 | +56.24 |
| 2026-09-14 | `RWT` | 541 | $3.55 | $3.53 | +10.82 | $3.83 | -162.30 | -151.48 | -5.41 | -167.71 |
| 2026-09-14 | `CRDL` | 939 | $2.00 | $1.96 | +32.87 | $1.99 | -28.17 | +4.70 | +65.73 | +37.56 |
| 2026-09-14 | `BKV` | 76 | $24.23 | $24.26 | -2.28 | $23.82 | +33.44 | +31.16 | +53.96 | +87.40 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +33.62 | EU, LUNR, OWL | — | $14,954.53 | $10,010.33 | EU×1412, LUNR×86, OWL×131 |
| 2026-08-17 | +2.25 | $14,954.53 | EU×1412, LUNR×86, OWL×131 | $9,916.79 | -93.54 | +123.77 | VERI, ZNTL, APMD, HIVE | — | $19,879.09 | $10,014.29 | EU×1412, LUNR×86, OWL×131, VERI×1077, ZNTL×348, APMD×39, HIVE×411 |
| 2026-08-18 | -6.20 | $19,879.09 | EU×1412, LUNR×86, OWL×131, VERI×1077, ZNTL×348, APMD×39, HIVE×411 | $10,177.57 | +163.28 | +276.31 | — | — | $19,879.09 | $10,453.88 | EU×1412, LUNR×86, OWL×131, VERI×1077, ZNTL×348, APMD×39, HIVE×411 |
| 2026-08-19 | -7.20 | $19,879.09 | EU×1412, LUNR×86, OWL×131, VERI×1077, ZNTL×348, APMD×39, HIVE×411 | $10,415.59 | -38.29 | +42.76 | — | EU, OWL | $16,808.40 | $10,437.75 | LUNR×86, VERI×1077, ZNTL×348, APMD×39, HIVE×411 |
| 2026-08-20 | +1.12 | $16,808.40 | LUNR×86, VERI×1077, ZNTL×348, APMD×39, HIVE×411 | $10,359.47 | -78.28 | +136.91 | WYFI, TOYO, ABCL, AAP, AQST | LUNR, VERI, ZNTL, APMD, HIVE | $15,469.72 | $10,456.00 | WYFI×48, TOYO×233, ABCL×87, AAP×22, AQST×224 |
| 2026-08-21 | +3.25 | $15,469.72 | WYFI×48, TOYO×233, ABCL×87, AAP×22, AQST×224 | $10,388.79 | -67.21 | -74.11 | QTRX, MRNA, ARIS, NOG | — | $20,544.39 | $10,302.69 | WYFI×48, TOYO×233, ABCL×87, AAP×22, AQST×224, QTRX×417, MRNA×9, ARIS×62, NOG×48 |
| 2026-08-24 | -5.17 | $20,544.39 | WYFI×48, TOYO×233, ABCL×87, AAP×22, AQST×224, QTRX×417, MRNA×9, ARIS×62, NOG×48 | $10,435.55 | +132.86 | +136.58 | — | — | $20,544.39 | $10,572.13 | WYFI×48, TOYO×233, ABCL×87, AAP×22, AQST×224, QTRX×417, MRNA×9, ARIS×62, NOG×48 |
| 2026-08-25 | +1.80 | $20,544.39 | WYFI×48, TOYO×233, ABCL×87, AAP×22, AQST×224, QTRX×417, MRNA×9, ARIS×62, NOG×48 | $10,548.11 | -24.02 | -182.83 | AVAH | WYFI, TOYO, ABCL, AAP, AQST | $20,767.68 | $10,347.72 | QTRX×417, MRNA×9, ARIS×62, NOG×48, AVAH×386 |
| 2026-08-26 | +2.02 | $20,767.68 | QTRX×417, MRNA×9, ARIS×62, NOG×48, AVAH×386 | $10,411.87 | +64.15 | -151.07 | ABCL, AQST | QTRX, MRNA, ARIS, NOG | $20,845.95 | $10,239.46 | AVAH×386, ABCL×212, AQST×511 |
| 2026-08-27 | — | $20,845.95 | AVAH×386, ABCL×212, AQST×511 | $10,237.34 | -2.12 | +8.53 | — | — | $20,845.95 | $10,245.87 | AVAH×386, ABCL×212, AQST×511 |
| 2026-08-28 | +0.75 | $20,845.95 | AVAH×386, ABCL×212, AQST×511 | $10,260.68 | +14.81 | +377.05 | SIMO | AVAH | $20,518.13 | $10,630.51 | ABCL×212, AQST×511, SIMO×20 |
| 2026-08-31 | -5.85 | $20,518.13 | ABCL×212, AQST×511, SIMO×20 | $10,681.71 | +51.20 | +4.20 | — | ABCL, AQST | $15,613.38 | $10,676.58 | SIMO×20 |
| 2026-09-01 | -6.30 | $15,613.38 | SIMO×20 | $10,811.58 | +135.00 | +54.80 | — | — | $15,613.38 | $10,866.38 | SIMO×20 |
| 2026-09-02 | -3.83 | $15,613.38 | SIMO×20 | $10,899.18 | +32.80 | +0.00 | — | SIMO | $10,897.13 | $10,897.13 | — |
| 2026-09-03 | -0.90 | $10,897.13 | — | $10,897.13 | +0.00 | +170.28 | SLN, OPK | — | $16,315.14 | $11,043.84 | SLN×183, OPK×1593 |
| 2026-09-04 | +2.25 | $16,315.14 | SLN×183, OPK×1593 | $11,104.98 | +61.14 | -89.97 | GSM, PIPR | — | $21,834.85 | $11,004.94 | SLN×183, OPK×1593, GSM×594, PIPR×36 |
| 2026-09-08 | -11.47 | $21,834.85 | SLN×183, OPK×1593, GSM×594, PIPR×36 | $11,051.80 | +46.86 | +275.79 | — | — | $21,834.85 | $11,327.59 | SLN×183, OPK×1593, GSM×594, PIPR×36 |
| 2026-09-09 | -13.95 | $21,834.85 | SLN×183, OPK×1593, GSM×594, PIPR×36 | $11,363.59 | +36.00 | +27.90 | — | SLN, OPK | $16,806.02 | $11,368.40 | GSM×594, PIPR×36 |
| 2026-09-10 | -13.28 | $16,806.02 | GSM×594, PIPR×36 | $11,451.74 | +83.34 | +0.00 | — | GSM, PIPR | $11,441.98 | $11,441.98 | — |
| 2026-09-11 | +0.50 | $11,441.98 | — | $11,441.98 | +0.00 | +72.87 | RWT, CRDL, BKV | — | $17,128.42 | $11,493.09 | RWT×541, CRDL×939, BKV×76 |
| 2026-09-14 | -11.00 | $17,128.42 | RWT×541, CRDL×939, BKV×76 | $11,534.49 | +41.40 | -157.03 | — | — | $17,128.42 | $11,377.46 | RWT×541, CRDL×939, BKV×76 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1412 | $1.18 | $18.51 | — | $11,647.65 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; 🔵; ret5=-0.9; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 judge🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 86 | $19.17 | $2.32 | — | $13,293.95 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 131 | $12.70 | $2.46 | — | $14,954.53 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ret5=+12.6; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,954.53 | ▲ close $10,010.33 vs 09:30 $10,000.00 (session +33.62) | 16:00 close · cash $14,954.53 · equity $10,010.33 vs 09:30 $10,000.00 (+10.33; session marks +33.62) · 3 name(s) marked open→close (per-name table). EU×1412 09:30 $1.18 → close $1.21 -42.36; LUNR×86 09:30 $19.17 → close $19.01 +13.76; OWL×131 09:30 $12.70 → close $12.22 +62.22 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,954.53 | ▼ 09:30 equity $9,916.79 vs yday $10,010.33 (-93.54) | 09:30 open · cash $14,954.53 (unchanged overnight, no fees) · equity $9,916.79 vs prior close $10,010.33 (-93.54) · 3 name(s) re-marked at the open (per-name table). EU×1412 yday $1.21 → 09:30 $1.21 -0.00; LUNR×86 yday $19.01 → 09:30 $20.25 -106.64; OWL×131 yday $12.22 → 09:30 $12.12 +13.10 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 1077 | $1.15 | $14.12 | — | $16,178.97 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; ⚪; ret5=-12.2; leftover $1239.60 | join🟡 sector🟢 gen🟢 news🔴 judge🟡 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 348 | $3.56 | $4.59 | — | $17,413.26 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; ret5=-15.6; leftover $1239.60 | join🟡 sector🔴 gen🟢 news🔴 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 39 | $31.70 | $2.16 | — | $18,647.39 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; ret5=+17.6; leftover $1239.60 | join🟡 sector🔴 gen🟢 news🔴 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 411 | $3.01 | $5.41 | — | $19,879.09 | — | short prior-export headline🔴; gate headline=bad; list earn_react; ⚪; ret5=-5.3; leftover $1239.60 | join🟢 sector🟢 gen🟢 news🔴 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,879.09 | ▲ close $10,014.29 vs 09:30 $9,916.79 (session +123.77) | 16:00 close · cash $19,879.09 · equity $10,014.29 vs 09:30 $9,916.79 (+97.50; session marks +123.77) · 7 name(s) marked open→close (per-name table). EU×1412 09:30 $1.21 → close $1.13 +112.96; LUNR×86 09:30 $20.25 → close $20.38 -11.18; OWL×131 09:30 $12.12 → close $11.66 +60.26; VERI×1077 09:30 $1.15 → close $1.08 +70.00; ZNTL×348 09:30 $3.56 → close $3.71 -50.46; APMD×39 09:30 $31.70 → close $32.55 -33.15; HIVE×411 09:30 $3.01 → close $3.07 -24.66 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,879.09 | ▲ 09:30 equity $10,177.57 vs yday $10,014.29 (+163.28) | 09:30 open · cash $19,879.09 (unchanged overnight, no fees) · equity $10,177.57 vs prior close $10,014.29 (+163.28) · 7 name(s) re-marked at the open (per-name table). EU×1412 yday $1.13 → 09:30 $1.13 -0.00; LUNR×86 yday $20.38 → 09:30 $19.31 +92.02; OWL×131 yday $11.66 → 09:30 $11.54 +15.72; VERI×1077 yday $1.08 → 09:30 $1.05 +37.69; ZNTL×348 yday $3.71 → 09:30 $3.75 -15.66; APMD×39 yday $32.55 → 09:30 $32.85 -11.70; HIVE×411 yday $3.07 → 09:30 $2.96 +45.21 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,879.09 | ▲ close $10,453.88 vs 09:30 $10,177.57 (session +276.31) | 16:00 close · cash $19,879.09 · equity $10,453.88 vs 09:30 $10,177.57 (+276.31; session marks +276.31) · 7 name(s) marked open→close (per-name table). EU×1412 09:30 $1.13 → close $1.07 +84.72; LUNR×86 09:30 $19.31 → close $19.31 -0.00; OWL×131 09:30 $11.54 → close $11.59 -6.55; VERI×1077 09:30 $1.05 → close $0.99 +59.24; ZNTL×348 09:30 $3.75 → close $3.68 +24.36; APMD×39 09:30 $32.85 → close $31.81 +40.56; HIVE×411 09:30 $2.96 → close $2.78 +73.98 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,879.09 | ▼ 09:30 equity $10,415.59 vs yday $10,453.88 (-38.29) | 09:30 open · cash $19,879.09 (unchanged overnight, no fees) · equity $10,415.59 vs prior close $10,453.88 (-38.29) · 7 name(s) re-marked at the open (per-name table). EU×1412 yday $1.07 → 09:30 $1.07 -0.00; LUNR×86 yday $19.31 → 09:30 $18.98 +28.38; OWL×131 yday $11.59 → 09:30 $11.75 -20.96; VERI×1077 yday $0.99 → 09:30 $1.00 -5.39; ZNTL×348 yday $3.68 → 09:30 $3.76 -27.84; APMD×39 yday $31.81 → 09:30 $32.13 -12.48; HIVE×411 yday $2.78 → 09:30 $2.78 -0.00 | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 1412 | $1.07 | $18.21 | $+118.60 | $18,350.04 | ▲ +118.60 after sell → book $10,397.38; vs 09:30 mark -18.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 131 | $11.75 | $2.38 | $+118.95 | $16,808.40 | ▲ +118.95 after sell → book $10,394.99; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,808.40 | ▲ close $10,437.75 vs 09:30 $10,415.59 (session +42.76) | 16:00 close · cash $16,808.40 · equity $10,437.75 vs 09:30 $10,415.59 (+22.16; session marks +42.76) · 5 name(s) marked open→close (per-name table). LUNR×86 09:30 $18.98 → close $18.52 +39.56; VERI×1077 09:30 $1.00 → close $0.97 +36.62; ZNTL×348 09:30 $3.76 → close $3.82 -20.88; APMD×39 09:30 $32.13 → close $32.03 +3.90; HIVE×411 09:30 $2.78 → close $2.82 -16.44 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,808.40 | ▼ 09:30 equity $10,359.47 vs yday $10,437.75 (-78.28) | 09:30 open · cash $16,808.40 (unchanged overnight, no fees) · equity $10,359.47 vs prior close $10,437.75 (-78.28) · 5 name(s) re-marked at the open (per-name table). LUNR×86 yday $18.52 → 09:30 $18.13 +33.54; VERI×1077 yday $0.97 → 09:30 $0.96 +3.23; ZNTL×348 yday $3.82 → 09:30 $4.01 -67.86; APMD×39 yday $32.03 → 09:30 $31.87 +6.24; HIVE×411 yday $2.82 → 09:30 $2.95 -53.43 | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 86 | $18.13 | $2.25 | $+84.87 | $15,246.97 | ▲ +84.87 after sell → book $10,357.22; vs 09:30 mark -2.25 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 1077 | $0.96 | $13.60 | $+173.68 | $14,196.22 | ▲ +173.68 after sell → book $10,343.62; vs 09:30 mark -13.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 348 | $4.01 | $4.49 | $-167.42 | $12,794.51 | ▼ -167.42 after sell → book $10,339.13; vs 09:30 mark -4.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 39 | $31.87 | $2.11 | $-10.90 | $11,549.48 | ▼ -10.90 after sell → book $10,337.03; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 411 | $2.95 | $5.30 | $+13.94 | $10,331.72 | ▲ +13.94 after sell → book $10,331.72; vs 09:30 mark -5.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 48 | $21.40 | $2.18 | — | $11,356.74 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; 🔵; ret5=-25.2; leftover $1033.17 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 233 | $4.43 | $3.08 | — | $12,385.85 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; 🔵; ret5=-23.1; leftover $1033.17 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 87 | $11.81 | $2.30 | — | $13,411.45 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1033.17 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 22 | $46.85 | $2.10 | — | $14,440.05 | — | short prior-export headline🔴; gate headline=bad; list earn_react; 🔵; ret5=+5.0; leftover $1033.17 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 224 | $4.61 | $2.97 | — | $15,469.72 | — | short prior-export headline🔴; gate headline=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1033.17 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,469.72 | ▲ close $10,456.00 vs 09:30 $10,359.47 (session +136.91) | 16:00 close · cash $15,469.72 · equity $10,456.00 vs 09:30 $10,359.47 (+96.53; session marks +136.91) · 5 name(s) marked open→close (per-name table). WYFI×48 09:30 $21.40 → close $21.16 +11.52; TOYO×233 09:30 $4.43 → close $4.51 -19.80; ABCL×87 09:30 $11.81 → close $11.57 +21.31; AAP×22 09:30 $46.85 → close $42.39 +98.12; AQST×224 09:30 $4.61 → close $4.50 +25.76 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,469.72 | ▼ 09:30 equity $10,388.79 vs yday $10,456.00 (-67.21) | 09:30 open · cash $15,469.72 (unchanged overnight, no fees) · equity $10,388.79 vs prior close $10,456.00 (-67.21) · 5 name(s) re-marked at the open (per-name table). WYFI×48 yday $21.16 → 09:30 $21.54 -18.24; TOYO×233 yday $4.51 → 09:30 $4.68 -38.45; ABCL×87 yday $11.57 → 09:30 $11.57 -0.00; AAP×22 yday $42.39 → 09:30 $42.41 -0.44; AQST×224 yday $4.50 → 09:30 $4.54 -10.08 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 417 | $3.11 | $5.49 | — | $16,761.10 | — | short prior-export headline🔴; gate headline=bad; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $1298.60 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 9 | $133.11 | $2.07 | — | $17,957.02 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; leftover $1298.60 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 62 | $20.90 | $2.23 | — | $19,250.58 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1298.60 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 48 | $27.00 | $2.19 | — | $20,544.39 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; ret5=+10.1; leftover $1298.60 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,544.39 | ▼ close $10,302.69 vs 09:30 $10,388.79 (session -74.11) | 16:00 close · cash $20,544.39 · equity $10,302.69 vs 09:30 $10,388.79 (-86.10; session marks -74.11) · 9 name(s) marked open→close (per-name table). WYFI×48 09:30 $21.54 → close $20.72 +39.36; TOYO×233 09:30 $4.68 → close $4.82 -32.62; ABCL×87 09:30 $11.57 → close $11.32 +21.75; AAP×22 09:30 $42.41 → close $42.58 -3.74; AQST×224 09:30 $4.54 → close $4.66 -26.88; QTRX×417 09:30 $3.11 → close $2.99 +50.04; MRNA×9 09:30 $133.11 → close $145.13 -108.18; ARIS×62 09:30 $20.90 → close $20.86 +2.48; NOG×48 09:30 $27.00 → close $27.34 -16.32 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,544.39 | ▲ 09:30 equity $10,435.55 vs yday $10,302.69 (+132.86) | 09:30 open · cash $20,544.39 (unchanged overnight, no fees) · equity $10,435.55 vs prior close $10,302.69 (+132.86) · 9 name(s) re-marked at the open (per-name table). WYFI×48 yday $20.72 → 09:30 $20.01 +34.08; TOYO×233 yday $4.82 → 09:30 $4.58 +55.92; ABCL×87 yday $11.32 → 09:30 $10.97 +30.45; AAP×22 yday $42.58 → 09:30 $43.05 -10.34; AQST×224 yday $4.66 → 09:30 $4.67 -2.24; QTRX×417 yday $2.99 → 09:30 $2.99 -0.00; MRNA×9 yday $145.13 → 09:30 $142.70 +21.87; ARIS×62 yday $20.86 → 09:30 $20.98 -7.44; NOG×48 yday $27.34 → 09:30 $27.12 +10.56 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,544.39 | ▲ close $10,572.13 vs 09:30 $10,435.55 (session +136.58) | 16:00 close · cash $20,544.39 · equity $10,572.13 vs 09:30 $10,435.55 (+136.58; session marks +136.58) · 9 name(s) marked open→close (per-name table). WYFI×48 09:30 $20.01 → close $20.78 -36.96; TOYO×233 09:30 $4.58 → close $4.38 +46.60; ABCL×87 09:30 $10.97 → close $10.61 +31.32; AAP×22 09:30 $43.05 → close $43.63 -12.76; AQST×224 09:30 $4.67 → close $4.80 -29.12; QTRX×417 09:30 $2.99 → close $2.80 +79.23; MRNA×9 09:30 $142.70 → close $138.89 +34.29; ARIS×62 09:30 $20.98 → close $20.81 +10.54; NOG×48 09:30 $27.12 → close $26.84 +13.44 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,544.39 | ▼ 09:30 equity $10,548.11 vs yday $10,572.13 (-24.02) | 09:30 open · cash $20,544.39 (unchanged overnight, no fees) · equity $10,548.11 vs prior close $10,572.13 (-24.02) · 9 name(s) re-marked at the open (per-name table). WYFI×48 yday $20.78 → 09:30 $20.90 -5.76; TOYO×233 yday $4.38 → 09:30 $4.42 -9.32; ABCL×87 yday $10.61 → 09:30 $11.00 -33.93; AAP×22 yday $43.63 → 09:30 $43.63 -0.00; AQST×224 yday $4.80 → 09:30 $4.77 +6.72; QTRX×417 yday $2.80 → 09:30 $2.80 -0.00; MRNA×9 yday $138.89 → 09:30 $143.50 -41.49; ARIS×62 yday $20.81 → 09:30 $20.45 +22.32; NOG×48 yday $26.84 → 09:30 $26.06 +37.44 | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 48 | $20.90 | $2.13 | $+19.68 | $19,539.06 | ▲ +19.68 after sell → book $10,545.98; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 233 | $4.42 | $3.01 | $-3.76 | $18,506.19 | ▼ -3.76 after sell → book $10,542.97; vs 09:30 mark -3.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 87 | $11.00 | $2.25 | $+66.35 | $17,546.94 | ▲ +66.35 after sell → book $10,540.72; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 22 | $43.63 | $2.06 | $+66.68 | $16,585.03 | ▲ +66.68 after sell → book $10,538.67; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 224 | $4.77 | $2.89 | $-41.69 | $15,513.66 | ▼ -41.69 after sell → book $10,535.78; vs 09:30 mark -2.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 386 | $13.62 | $5.23 | — | $20,767.68 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $5267.89 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,767.68 | ▼ close $10,347.72 vs 09:30 $10,548.11 (session -182.83) | 16:00 close · cash $20,767.68 · equity $10,347.72 vs 09:30 $10,548.11 (-200.39; session marks -182.83) · 5 name(s) marked open→close (per-name table). QTRX×417 09:30 $2.80 → close $2.79 +4.17; MRNA×9 09:30 $143.50 → close $158.83 -137.97; ARIS×62 09:30 $20.45 → close $21.18 -45.26; NOG×48 09:30 $26.06 → close $26.42 -17.28; AVAH×386 09:30 $13.62 → close $13.59 +13.51 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,767.68 | ▲ 09:30 equity $10,411.87 vs yday $10,347.72 (+64.15) | 09:30 open · cash $20,767.68 (unchanged overnight, no fees) · equity $10,411.87 vs prior close $10,347.72 (+64.15) · 5 name(s) re-marked at the open (per-name table). QTRX×417 yday $2.79 → 09:30 $2.83 -16.68; MRNA×9 yday $158.83 → 09:30 $154.20 +41.67; ARIS×62 yday $21.18 → 09:30 $20.50 +42.16; NOG×48 yday $26.42 → 09:30 $26.00 +20.16; AVAH×386 yday $13.59 → 09:30 $13.65 -23.16 | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 417 | $2.83 | $5.38 | $+105.89 | $19,582.19 | ▲ +105.89 after sell → book $10,406.49; vs 09:30 mark -5.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 9 | $154.20 | $2.02 | $-193.90 | $18,192.37 | ▼ -193.90 after sell → book $10,404.47; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 62 | $20.50 | $2.18 | $+20.39 | $16,919.20 | ▲ +20.39 after sell → book $10,402.30; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 48 | $26.00 | $2.13 | $+43.68 | $15,669.06 | ▲ +43.68 after sell → book $10,400.16; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 212 | $12.22 | $2.86 | — | $18,256.84 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ret5=+12.4; leftover $2600.04 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 511 | $5.08 | $6.77 | — | $20,845.95 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ret5=+17.6; leftover $2600.04 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,845.95 | ▼ close $10,239.46 vs 09:30 $10,411.87 (session -151.07) | 16:00 close · cash $20,845.95 · equity $10,239.46 vs 09:30 $10,411.87 (-172.41; session marks -151.07) · 3 name(s) marked open→close (per-name table). AVAH×386 09:30 $13.65 → close $13.62 +11.58; ABCL×212 09:30 $12.22 → close $12.24 -4.24; AQST×511 09:30 $5.08 → close $5.39 -158.41 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,845.95 | ▼ 09:30 equity $10,237.34 vs yday $10,239.46 (-2.12) | 09:30 open · cash $20,845.95 (unchanged overnight, no fees) · equity $10,237.34 vs prior close $10,239.46 (-2.12) · 3 name(s) re-marked at the open (per-name table). AVAH×386 yday $13.62 → 09:30 $13.62 -0.00; ABCL×212 yday $12.24 → 09:30 $12.25 -2.12; AQST×511 yday $5.39 → 09:30 $5.39 -0.00 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,845.95 | ▲ close $10,245.87 vs 09:30 $10,237.34 (session +8.53) | 16:00 close · cash $20,845.95 · equity $10,245.87 vs 09:30 $10,237.34 (+8.53; session marks +8.53) · 3 name(s) marked open→close (per-name table). AVAH×386 09:30 $13.62 → close $13.82 -77.20; ABCL×212 09:30 $12.25 → close $12.40 -31.80; AQST×511 09:30 $5.39 → close $5.16 +117.53 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,845.95 | ▲ 09:30 equity $10,260.68 vs yday $10,245.87 (+14.81) | 09:30 open · cash $20,845.95 (unchanged overnight, no fees) · equity $10,260.68 vs prior close $10,245.87 (+14.81) · 3 name(s) re-marked at the open (per-name table). AVAH×386 yday $13.82 → 09:30 $13.90 -30.88; ABCL×212 yday $12.40 → 09:30 $12.30 +20.14; AQST×511 yday $5.16 → 09:30 $5.11 +25.55 | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 386 | $13.90 | $4.98 | $-116.36 | $15,475.57 | ▼ -116.36 after sell → book $10,255.70; vs 09:30 mark -4.98 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 20 | $252.24 | $2.24 | — | $20,518.13 | — | short prior-export headline🔴; gate headline=bad; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $5127.85 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,518.13 | ▲ close $10,630.51 vs 09:30 $10,260.68 (session +377.05) | 16:00 close · cash $20,518.13 · equity $10,630.51 vs 09:30 $10,260.68 (+369.83; session marks +377.05) · 3 name(s) marked open→close (per-name table). ABCL×212 09:30 $12.30 → close $11.35 +202.46; AQST×511 09:30 $5.11 → close $5.02 +45.99; SIMO×20 09:30 $252.24 → close $245.81 +128.60 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,518.13 | ▲ 09:30 equity $10,681.71 vs yday $10,630.51 (+51.20) | 09:30 open · cash $20,518.13 (unchanged overnight, no fees) · equity $10,681.71 vs prior close $10,630.51 (+51.20) · 3 name(s) re-marked at the open (per-name table). ABCL×212 yday $11.35 → 09:30 $11.10 +53.00; AQST×511 yday $5.02 → 09:30 $4.97 +22.99; SIMO×20 yday $245.81 → 09:30 $247.05 -24.80 | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 212 | $11.10 | $2.73 | $+231.84 | $18,162.20 | ▲ +231.84 after sell → book $10,678.97; vs 09:30 mark -2.74 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 511 | $4.97 | $6.59 | $+40.29 | $15,613.38 | ▲ +40.29 after sell → book $10,672.38; vs 09:30 mark -6.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,613.38 | ▲ close $10,676.58 vs 09:30 $10,681.71 (session +4.20) | 16:00 close · cash $15,613.38 · equity $10,676.58 vs 09:30 $10,681.71 (-5.13; session marks +4.20) · 1 name(s) marked open→close (per-name table). SIMO×20 09:30 $247.05 → close $246.84 +4.20 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,613.38 | ▲ 09:30 equity $10,811.58 vs yday $10,676.58 (+135.00) | 09:30 open · cash $15,613.38 (unchanged overnight, no fees) · equity $10,811.58 vs prior close $10,676.58 (+135.00) · 1 name(s) re-marked at the open (per-name table). SIMO×20 yday $246.84 → 09:30 $240.09 +135.00 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,613.38 | ▲ close $10,866.38 vs 09:30 $10,811.58 (session +54.80) | 16:00 close · cash $15,613.38 · equity $10,866.38 vs 09:30 $10,811.58 (+54.80; session marks +54.80) · 1 name(s) marked open→close (per-name table). SIMO×20 09:30 $240.09 → close $237.35 +54.80 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,613.38 | ▲ 09:30 equity $10,899.18 vs yday $10,866.38 (+32.80) | 09:30 open · cash $15,613.38 (unchanged overnight, no fees) · equity $10,899.18 vs prior close $10,866.38 (+32.80) · 1 name(s) re-marked at the open (per-name table). SIMO×20 yday $237.35 → 09:30 $235.71 +32.80 | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 20 | $235.71 | $2.05 | $+326.31 | $10,897.13 | ▲ +326.31 after sell → book $10,897.13; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,897.13 | ▲ close $10,897.13 vs 09:30 $10,899.18 (session +0.00) | 16:00 close · cash $10,897.13 · no lots left · equity $10,897.13. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,897.13 | ▲ 09:30 equity $10,897.13 vs yday $10,897.13 (+0.00) | 09:30 open · cash $10,897.13 · no holdings · equity $10,897.13 vs prior close $10,897.13 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 183 | $14.85 | $2.67 | — | $13,612.02 | — | short prior-export headline🔴; gate headline=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $2724.28 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1593 | $1.71 | $20.91 | — | $16,315.14 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; leftover $2724.28 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,315.14 | ▲ close $11,043.84 vs 09:30 $10,897.13 (session +170.28) | 16:00 close · cash $16,315.14 · equity $11,043.84 vs 09:30 $10,897.13 (+146.71; session marks +170.28) · 2 name(s) marked open→close (per-name table). SLN×183 09:30 $14.85 → close $14.79 +10.98; OPK×1593 09:30 $1.71 → close $1.61 +159.30 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,315.14 | ▲ 09:30 equity $11,104.98 vs yday $11,043.84 (+61.14) | 09:30 open · cash $16,315.14 (unchanged overnight, no fees) · equity $11,104.98 vs prior close $11,043.84 (+61.14) · 2 name(s) re-marked at the open (per-name table). SLN×183 yday $14.79 → 09:30 $14.63 +29.28; OPK×1593 yday $1.61 → 09:30 $1.59 +31.86 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 594 | $4.67 | $7.86 | — | $19,081.26 | — | short prior-export headline🔴; gate headline=bad; list yday_gainer; ret5=+11.9; leftover $2776.24 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 36 | $76.55 | $2.21 | — | $21,834.85 | — | short prior-export headline🔴; gate headline=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; leftover $2776.24 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,834.85 | ▼ close $11,004.94 vs 09:30 $11,104.98 (session -89.97) | 16:00 close · cash $21,834.85 · equity $11,004.94 vs 09:30 $11,104.98 (-100.04; session marks -89.97) · 4 name(s) marked open→close (per-name table). SLN×183 09:30 $14.63 → close $14.59 +7.32; OPK×1593 09:30 $1.59 → close $1.64 -79.65; GSM×594 09:30 $4.67 → close $4.67 -0.00; PIPR×36 09:30 $76.55 → close $77.04 -17.64 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,834.85 | ▲ 09:30 equity $11,051.80 vs yday $11,004.94 (+46.86) | 09:30 open · cash $21,834.85 (unchanged overnight, no fees) · equity $11,051.80 vs prior close $11,004.94 (+46.86) · 4 name(s) re-marked at the open (per-name table). SLN×183 yday $14.59 → 09:30 $14.24 +64.05; OPK×1593 yday $1.64 → 09:30 $1.63 +15.93; GSM×594 yday $4.67 → 09:30 $4.75 -47.52; PIPR×36 yday $77.04 → 09:30 $76.64 +14.40 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,834.85 | ▲ close $11,327.59 vs 09:30 $11,051.80 (session +275.79) | 16:00 close · cash $21,834.85 · equity $11,327.59 vs 09:30 $11,051.80 (+275.79; session marks +275.79) · 4 name(s) marked open→close (per-name table). SLN×183 09:30 $14.24 → close $13.69 +100.65; OPK×1593 09:30 $1.63 → close $1.59 +63.72; GSM×594 09:30 $4.75 → close $4.52 +136.62; PIPR×36 09:30 $76.64 → close $77.34 -25.20 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,834.85 | ▲ 09:30 equity $11,363.59 vs yday $11,327.59 (+36.00) | 09:30 open · cash $21,834.85 (unchanged overnight, no fees) · equity $11,363.59 vs prior close $11,327.59 (+36.00) · 4 name(s) re-marked at the open (per-name table). SLN×183 yday $13.69 → 09:30 $13.60 +16.47; OPK×1593 yday $1.59 → 09:30 $1.58 +15.93; GSM×594 yday $4.52 → 09:30 $4.52 -0.00; PIPR×36 yday $77.34 → 09:30 $77.24 +3.60 | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 183 | $13.60 | $2.54 | $+223.55 | $19,343.51 | ▲ +223.55 after sell → book $11,361.05; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1593 | $1.58 | $20.55 | $+165.63 | $16,806.02 | ▲ +165.63 after sell → book $11,340.50; vs 09:30 mark -20.55 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,806.02 | ▲ close $11,368.40 vs 09:30 $11,363.59 (session +27.90) | 16:00 close · cash $16,806.02 · equity $11,368.40 vs 09:30 $11,363.59 (+4.81; session marks +27.90) · 2 name(s) marked open→close (per-name table). GSM×594 09:30 $4.52 → close $4.49 +17.82; PIPR×36 09:30 $77.24 → close $76.96 +10.08 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,806.02 | ▲ 09:30 equity $11,451.74 vs yday $11,368.40 (+83.34) | 09:30 open · cash $16,806.02 (unchanged overnight, no fees) · equity $11,451.74 vs prior close $11,368.40 (+83.34) · 2 name(s) re-marked at the open (per-name table). GSM×594 yday $4.49 → 09:30 $4.36 +77.22; PIPR×36 yday $76.96 → 09:30 $76.79 +6.12 | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 594 | $4.36 | $7.66 | $+168.62 | $14,208.52 | ▲ +168.62 after sell → book $11,444.08; vs 09:30 mark -7.66 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 36 | $76.79 | $2.10 | $-12.94 | $11,441.98 | ▼ -12.94 after sell → book $11,441.98; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,441.98 | ▲ close $11,441.98 vs 09:30 $11,451.74 (session +0.00) | 16:00 close · cash $11,441.98 · no lots left · equity $11,441.98. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,441.98 | ▲ 09:30 equity $11,441.98 vs yday $11,441.98 (+0.00) | 09:30 open · cash $11,441.98 · no holdings · equity $11,441.98 vs prior close $11,441.98 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 541 | $3.52 | $7.14 | — | $13,339.17 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; 🔵; ret5=-19.2; leftover $1907.00 | join🔴 sector🔴 gen🟡 news🔴 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 939 | $2.03 | $12.34 | — | $15,233.00 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; ret5=-8.8; leftover $1907.00 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 judge🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 76 | $24.97 | $2.30 | — | $17,128.42 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; ret5=+10.8; leftover $1907.00 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,128.42 | ▲ close $11,493.09 vs 09:30 $11,441.98 (session +72.87) | 16:00 close · cash $17,128.42 · equity $11,493.09 vs 09:30 $11,441.98 (+51.11; session marks +72.87) · 3 name(s) marked open→close (per-name table). RWT×541 09:30 $3.52 → close $3.55 -16.23; CRDL×939 09:30 $2.03 → close $2.00 +32.86; BKV×76 09:30 $24.97 → close $24.23 +56.24 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,128.42 | ▲ 09:30 equity $11,534.49 vs yday $11,493.09 (+41.40) | 09:30 open · cash $17,128.42 (unchanged overnight, no fees) · equity $11,534.49 vs prior close $11,493.09 (+41.40) · 3 name(s) re-marked at the open (per-name table). RWT×541 yday $3.55 → 09:30 $3.53 +10.82; CRDL×939 yday $2.00 → 09:30 $1.96 +32.87; BKV×76 yday $24.23 → 09:30 $24.26 -2.28 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,128.42 | ▼ close $11,377.46 vs 09:30 $11,534.49 (session -157.03) | 16:00 close · cash $17,128.42 · equity $11,377.46 vs 09:30 $11,534.49 (-157.03; session marks -157.03) · 3 name(s) marked open→close (per-name table). RWT×541 09:30 $3.53 → close $3.83 -162.30; CRDL×939 09:30 $1.96 → close $1.99 -28.17; BKV×76 09:30 $24.26 → close $23.82 +33.44 | — |

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
| 2026-08-18 | `RNW` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `VERI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ZNTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `APMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
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
| 2026-08-27 | `AQST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-09 | `GSM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `PIPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `RWT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RWT` | 541 | 2026-09-11 @ $3.52 | short prior-export headline🔴; gate headline=bad; list yday_mover; 🔵; ret5=-19.2; leftover $1907.00 |
| `CRDL` | 939 | 2026-09-11 @ $2.03 | short prior-export headline🔴; gate headline=bad; list yday_mover; ret5=-8.8; leftover $1907.00 |
| `BKV` | 76 | 2026-09-11 @ $24.97 | short prior-export headline🔴; gate headline=bad; list ohlc_hot; ret5=+10.8; leftover $1907.00 |
