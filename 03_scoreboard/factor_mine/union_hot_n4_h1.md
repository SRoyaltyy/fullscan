# Factor mine action — `union_hot_n4_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `hot_score` · size `leftover` · sell `list` · S-boost `none` · top 4 by hot

Cash book **+31.97%** ($13,197) · signal-only (no cash/fees) was +71.06%. Starts YES **26/26**. Fills 92 · skips 35 · realized $+1921.38.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how hot the prior tape looked.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by how hot the prior tape looked and keep the top 4.
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
- **Gate** `none (list as ranked)` · **rank** `hot_score` · **top_n** 4.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $85.87.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `IREN` | 54 | — | $45.98 | +0.00 | $44.76 | -65.88 | -65.88 | +0.00 | -65.88 |
| 2026-08-13 | `TNDM` | 107 | — | $23.33 | +0.00 | $23.13 | -21.40 | -21.40 | +0.00 | -21.40 |
| 2026-08-13 | `TPG` | 49 | — | $50.62 | +0.00 | $54.62 | +195.84 | +195.84 | +0.00 | +195.84 |
| 2026-08-13 | `INO` | 3085 | — | $0.81 | +0.00 | $0.90 | +277.65 | +277.65 | +0.00 | +277.65 |
| 2026-08-14 | `IREN` | 54 | $44.76 | $44.09 | -36.18 | — | +0.00 | -36.18 | -102.06 | — |
| 2026-08-14 | `TNDM` | 107 | $23.13 | $22.92 | -22.47 | — | +0.00 | -22.47 | -43.87 | — |
| 2026-08-14 | `TPG` | 49 | $54.62 | $55.29 | +32.83 | — | +0.00 | +32.83 | +228.67 | — |
| 2026-08-14 | `INO` | 3085 | $0.90 | $0.93 | +92.55 | — | +0.00 | +92.55 | +370.20 | — |
| 2026-08-14 | `QMCO` | 105 | — | $24.68 | +0.00 | $26.11 | +150.15 | +150.15 | +0.00 | +150.15 |
| 2026-08-14 | `ARX` | 132 | — | $19.57 | +0.00 | $19.58 | +1.32 | +1.32 | +0.00 | +1.32 |
| 2026-08-14 | `EROC` | 161 | — | $16.05 | +0.00 | $16.72 | +107.87 | +107.87 | +0.00 | +107.87 |
| 2026-08-14 | `ZENA` | 1175 | — | $2.20 | +0.00 | $2.14 | -70.50 | -70.50 | +0.00 | -70.50 |
| 2026-08-17 | `QMCO` | 105 | $26.11 | $24.83 | -134.40 | — | +0.00 | -134.40 | +15.75 | — |
| 2026-08-17 | `ARX` | 132 | $19.58 | $19.57 | -1.32 | — | +0.00 | -1.32 | +0.00 | — |
| 2026-08-17 | `EROC` | 161 | $16.72 | $17.15 | +69.23 | — | +0.00 | +69.23 | +177.10 | — |
| 2026-08-17 | `ZENA` | 1175 | $2.14 | $2.08 | -64.63 | — | +0.00 | -64.63 | -135.13 | — |
| 2026-08-17 | `XHG` | 619 | — | $4.19 | +0.00 | $3.91 | -173.32 | -173.32 | +0.00 | -173.32 |
| 2026-08-17 | `CAPR` | 377 | — | $6.87 | +0.00 | $7.45 | +218.66 | +218.66 | +0.00 | +218.66 |
| 2026-08-17 | `STDN` | 190 | — | $13.64 | +0.00 | $13.31 | -62.70 | -62.70 | +0.00 | -62.70 |
| 2026-08-17 | `HTFL` | 62 | — | $41.23 | +0.00 | $41.94 | +44.02 | +44.02 | +0.00 | +44.02 |
| 2026-08-18 | `XHG` | 619 | $3.91 | $3.94 | +18.57 | — | +0.00 | +18.57 | -154.75 | — |
| 2026-08-18 | `CAPR` | 377 | $7.45 | $7.50 | +18.85 | $7.08 | -158.34 | -139.49 | +237.51 | +79.17 |
| 2026-08-18 | `STDN` | 190 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -62.70 | — |
| 2026-08-18 | `HTFL` | 62 | $41.94 | $41.50 | -27.28 | $45.23 | +231.26 | +203.98 | +16.74 | +248.00 |
| 2026-08-19 | `CAPR` | 377 | $7.08 | $7.19 | +41.47 | — | +0.00 | +41.47 | +120.64 | — |
| 2026-08-19 | `HTFL` | 62 | $45.23 | $46.02 | +48.98 | — | +0.00 | +48.98 | +296.98 | — |
| 2026-08-20 | `MRNA` | 17 | — | $150.14 | +0.00 | $133.32 | -285.94 | -285.94 | +0.00 | -285.94 |
| 2026-08-20 | `AMLX` | 70 | — | $37.49 | +0.00 | $39.66 | +151.90 | +151.90 | +0.00 | +151.90 |
| 2026-08-20 | `CYPH` | 2292 | — | $1.15 | +0.00 | $1.19 | +91.68 | +91.68 | +0.00 | +91.68 |
| 2026-08-20 | `PURR` | 276 | — | $9.55 | +0.00 | $10.08 | +146.28 | +146.28 | +0.00 | +146.28 |
| 2026-08-21 | `MRNA` | 17 | $133.32 | $133.11 | -3.57 | $145.13 | +204.34 | +200.77 | -289.51 | -85.17 |
| 2026-08-21 | `AMLX` | 70 | $39.66 | $39.80 | +9.45 | $38.66 | -79.45 | -70.00 | +161.35 | +81.90 |
| 2026-08-21 | `CYPH` | 2292 | $1.19 | $1.32 | +297.96 | $1.42 | +229.20 | +527.16 | +389.64 | +618.84 |
| 2026-08-21 | `PURR` | 276 | $10.08 | $10.52 | +121.44 | — | +0.00 | +121.44 | +267.72 | — |
| 2026-08-21 | `XHG` | 657 | — | $4.49 | +0.00 | $4.41 | -52.56 | -52.56 | +0.00 | -52.56 |
| 2026-08-24 | `MRNA` | 17 | $145.13 | $142.70 | -41.31 | — | +0.00 | -41.31 | -126.48 | — |
| 2026-08-24 | `AMLX` | 70 | $38.66 | $38.64 | -1.40 | — | +0.00 | -1.40 | +80.50 | — |
| 2026-08-24 | `CYPH` | 2292 | $1.42 | $1.83 | +939.72 | — | +0.00 | +939.72 | +1558.56 | — |
| 2026-08-24 | `XHG` | 657 | $4.41 | $4.32 | -59.13 | — | +0.00 | -59.13 | -111.69 | — |
| 2026-08-25 | `REAX` | 125 | — | $24.11 | +0.00 | $28.43 | +540.00 | +540.00 | +0.00 | +540.00 |
| 2026-08-25 | `CYPH` | 1942 | — | $1.56 | +0.00 | $1.64 | +155.36 | +155.36 | +0.00 | +155.36 |
| 2026-08-25 | `MRNA` | 21 | — | $143.50 | +0.00 | $158.83 | +321.93 | +321.93 | +0.00 | +321.93 |
| 2026-08-25 | `XHG` | 743 | — | $4.07 | +0.00 | $4.02 | -37.15 | -37.15 | +0.00 | -37.15 |
| 2026-08-26 | `REAX` | 125 | $28.43 | $26.61 | -227.50 | — | +0.00 | -227.50 | +312.50 | — |
| 2026-08-26 | `CYPH` | 1942 | $1.64 | $1.60 | -77.68 | — | +0.00 | -77.68 | +77.68 | — |
| 2026-08-26 | `MRNA` | 21 | $158.83 | $154.20 | -97.23 | $149.66 | -95.34 | -192.57 | +224.70 | +129.36 |
| 2026-08-26 | `XHG` | 743 | $4.02 | $3.81 | -156.03 | — | +0.00 | -156.03 | -193.18 | — |
| 2026-08-26 | `BYND` | 217 | — | $14.11 | +0.00 | $14.25 | +30.38 | +30.38 | +0.00 | +30.38 |
| 2026-08-26 | `USDE` | 529 | — | $5.81 | +0.00 | $5.98 | +89.93 | +89.93 | +0.00 | +89.93 |
| 2026-08-26 | `ARCT` | 200 | — | $15.35 | +0.00 | $15.83 | +96.00 | +96.00 | +0.00 | +96.00 |
| 2026-08-27 | `MRNA` | 21 | $149.66 | $144.18 | -115.08 | $142.77 | -29.61 | -144.69 | +14.28 | -15.33 |
| 2026-08-27 | `BYND` | 217 | $14.25 | $14.20 | -10.85 | — | +0.00 | -10.85 | +19.53 | — |
| 2026-08-27 | `USDE` | 529 | $5.98 | $6.50 | +275.08 | — | +0.00 | +275.08 | +365.01 | — |
| 2026-08-27 | `ARCT` | 200 | $15.83 | $15.74 | -18.00 | — | +0.00 | -18.00 | +78.00 | — |
| 2026-08-27 | `XHG` | 793 | — | $4.06 | +0.00 | $3.80 | -206.18 | -206.18 | +0.00 | -206.18 |
| 2026-08-27 | `CAPR` | 350 | — | $9.19 | +0.00 | $10.06 | +304.50 | +304.50 | +0.00 | +304.50 |
| 2026-08-27 | `BZ` | 173 | — | $18.50 | +0.00 | $18.00 | -86.50 | -86.50 | +0.00 | -86.50 |
| 2026-08-28 | `MRNA` | 21 | $142.77 | $137.19 | -117.18 | $137.99 | +16.80 | -100.38 | -132.51 | -115.71 |
| 2026-08-28 | `XHG` | 793 | $3.80 | $3.69 | -87.23 | — | +0.00 | -87.23 | -293.41 | — |
| 2026-08-28 | `CAPR` | 350 | $10.06 | $9.73 | -115.50 | $9.59 | -49.00 | -164.50 | +189.00 | +140.00 |
| 2026-08-28 | `BZ` | 173 | $18.00 | $18.15 | +25.95 | — | +0.00 | +25.95 | -60.55 | — |
| 2026-08-28 | `BYND` | 216 | — | $14.00 | +0.00 | $13.86 | -30.24 | -30.24 | +0.00 | -30.24 |
| 2026-08-28 | `ARCT` | 196 | — | $15.43 | +0.00 | $15.19 | -47.04 | -47.04 | +0.00 | -47.04 |
| 2026-08-31 | `MRNA` | 21 | $137.99 | $134.10 | -81.69 | — | +0.00 | -81.69 | -197.40 | — |
| 2026-08-31 | `CAPR` | 350 | $9.59 | $9.50 | -31.50 | — | +0.00 | -31.50 | +108.50 | — |
| 2026-08-31 | `BYND` | 216 | $13.86 | $13.81 | -10.80 | $13.30 | -110.16 | -120.96 | -41.04 | -151.20 |
| 2026-08-31 | `ARCT` | 196 | $15.19 | $15.00 | -37.24 | — | +0.00 | -37.24 | -84.28 | — |
| 2026-09-01 | `BYND` | 216 | $13.30 | $13.04 | -56.16 | — | +0.00 | -56.16 | -207.36 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `GPRO` | 1670 | — | $1.78 | +0.00 | $1.39 | -651.30 | -651.30 | +0.00 | -651.30 |
| 2026-09-03 | `REAX` | 161 | — | $18.40 | +0.00 | $18.40 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `CNH` | 216 | — | $13.71 | +0.00 | $13.84 | +28.08 | +28.08 | +0.00 | +28.08 |
| 2026-09-03 | `MMED` | 124 | — | $23.88 | +0.00 | $23.84 | -4.96 | -4.96 | +0.00 | -4.96 |
| 2026-09-04 | `GPRO` | 1670 | $1.39 | $1.48 | +150.30 | $1.70 | +367.40 | +517.70 | -501.00 | -133.60 |
| 2026-09-04 | `REAX` | 161 | $18.40 | $18.15 | -40.25 | — | +0.00 | -40.25 | -40.25 | — |
| 2026-09-04 | `CNH` | 216 | $13.84 | $13.89 | +10.80 | — | +0.00 | +10.80 | +38.88 | — |
| 2026-09-04 | `MMED` | 124 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -4.96 | — |
| 2026-09-04 | `CHPT` | 318 | — | $9.28 | +0.00 | $9.89 | +193.98 | +193.98 | +0.00 | +193.98 |
| 2026-09-04 | `DPRO` | 465 | — | $6.36 | +0.00 | $6.12 | -111.60 | -111.60 | +0.00 | -111.60 |
| 2026-09-04 | `ASST` | 117 | — | $25.18 | +0.00 | $27.14 | +229.32 | +229.32 | +0.00 | +229.32 |
| 2026-09-08 | `GPRO` | 1670 | $1.70 | $1.56 | -225.45 | — | +0.00 | -225.45 | -359.05 | — |
| 2026-09-08 | `CHPT` | 318 | $9.89 | $9.91 | +6.36 | $9.37 | -171.72 | -165.36 | +200.34 | +28.62 |
| 2026-09-08 | `DPRO` | 465 | $6.12 | $6.07 | -23.25 | — | +0.00 | -23.25 | -134.85 | — |
| 2026-09-08 | `ASST` | 117 | $27.14 | $26.44 | -81.90 | — | +0.00 | -81.90 | +147.42 | — |
| 2026-09-09 | `CHPT` | 318 | $9.37 | $9.39 | +6.36 | — | +0.00 | +6.36 | +34.98 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `INDP` | 1064 | — | $2.70 | +0.00 | $2.77 | +74.48 | +74.48 | +0.00 | +74.48 |
| 2026-09-11 | `BNC` | 585 | — | $4.91 | +0.00 | $4.80 | -64.35 | -64.35 | +0.00 | -64.35 |
| 2026-09-11 | `IRD` | 466 | — | $6.16 | +0.00 | $6.04 | -55.92 | -55.92 | +0.00 | -55.92 |
| 2026-09-11 | `CMRC` | 907 | — | $3.13 | +0.00 | $3.50 | +340.12 | +340.12 | +0.00 | +340.12 |
| 2026-09-14 | `INDP` | 1064 | $2.77 | $2.80 | +31.92 | $3.14 | +361.76 | +393.68 | +106.40 | +468.16 |
| 2026-09-14 | `BNC` | 585 | $4.80 | $5.03 | +134.55 | $5.27 | +140.40 | +274.95 | +70.20 | +210.60 |
| 2026-09-14 | `IRD` | 466 | $6.04 | $6.02 | -9.32 | — | +0.00 | -9.32 | -65.24 | — |
| 2026-09-14 | `CMRC` | 907 | $3.50 | $3.51 | +4.53 | $3.64 | +117.91 | +122.44 | +344.66 | +462.57 |
| 2026-09-15 | `INDP` | 1064 | $3.14 | $3.40 | +276.64 | $3.64 | +255.36 | +532.00 | +744.80 | +1000.16 |
| 2026-09-15 | `BNC` | 585 | $5.27 | $5.11 | -93.60 | $4.91 | -117.00 | -210.60 | +117.00 | +0.00 |
| 2026-09-15 | `CMRC` | 907 | $3.64 | $3.64 | +0.00 | — | +0.00 | +0.00 | +462.57 | — |
| 2026-09-16 | `INDP` | 1064 | $3.64 | $3.66 | +21.28 | $3.21 | -478.80 | -457.52 | +1021.44 | +542.64 |
| 2026-09-16 | `BNC` | 585 | $4.91 | $4.77 | -81.90 | — | +0.00 | -81.90 | -81.90 | — |
| 2026-09-16 | `HLP` | 1643 | — | $1.80 | +0.00 | $2.07 | +443.61 | +443.61 | +0.00 | +443.61 |
| 2026-09-16 | `SDGR` | 126 | — | $23.29 | +0.00 | $23.93 | +80.64 | +80.64 | +0.00 | +80.64 |
| 2026-09-16 | `SSL` | 202 | — | $14.62 | +0.00 | $14.29 | -66.66 | -66.66 | +0.00 | -66.66 |
| 2026-09-17 | `INDP` | 1064 | $3.21 | $3.30 | +95.76 | $3.93 | +670.32 | +766.08 | +638.40 | +1308.72 |
| 2026-09-17 | `HLP` | 1643 | $2.07 | $2.10 | +49.29 | $2.02 | -131.44 | -82.15 | +492.90 | +361.46 |
| 2026-09-17 | `SDGR` | 126 | $23.93 | $24.09 | +20.16 | — | +0.00 | +20.16 | +100.80 | — |
| 2026-09-17 | `SSL` | 202 | $14.29 | $13.77 | -105.04 | — | +0.00 | -105.04 | -171.70 | — |
| 2026-09-17 | `BBNX` | 129 | — | $22.46 | +0.00 | $21.43 | -132.87 | -132.87 | +0.00 | -132.87 |
| 2026-09-17 | `BRR` | 868 | — | $3.34 | +0.00 | $3.44 | +86.80 | +86.80 | +0.00 | +86.80 |
| 2026-09-18 | `INDP` | 1064 | $3.93 | $3.85 | -85.12 | $3.55 | -319.20 | -404.32 | +1223.60 | +904.40 |
| 2026-09-18 | `HLP` | 1643 | $2.02 | $1.96 | -98.58 | — | +0.00 | -98.58 | +262.88 | — |
| 2026-09-18 | `BBNX` | 129 | $21.43 | $21.30 | -16.77 | — | +0.00 | -16.77 | -149.64 | — |
| 2026-09-18 | `BRR` | 868 | $3.44 | $3.57 | +112.84 | — | +0.00 | +112.84 | +199.64 | — |
| 2026-09-18 | `SDGR` | 102 | — | $29.32 | +0.00 | $29.02 | -30.60 | -30.60 | +0.00 | -30.60 |
| 2026-09-18 | `CYPH` | 992 | — | $3.04 | +0.00 | $3.60 | +560.48 | +560.48 | +0.00 | +560.48 |
| 2026-09-18 | `TEM` | 36 | — | $81.40 | +0.00 | $77.84 | -128.16 | -128.16 | +0.00 | -128.16 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +386.21 | IREN, TNDM, TPG, INO | — | $0.54 | $10,345.37 | IREN×54, TNDM×107, TPG×49, INO×3085 |
| 2026-08-14 | +5.50 | $0.54 | IREN×54, TNDM×107, TPG×49, INO×3085 | $10,412.10 | +66.73 | +188.84 | QMCO, ARX, EROC, ZENA | IREN, TNDM, TPG, INO | $0.91 | $10,533.44 | QMCO×105, ARX×132, EROC×161, ZENA×1175 |
| 2026-08-17 | +2.25 | $0.91 | QMCO×105, ARX×132, EROC×161, ZENA×1175 | $10,402.32 | -131.12 | +26.66 | XHG, CAPR, STDN, HTFL | QMCO, ARX, EROC, ZENA | $30.61 | $10,388.73 | XHG×619, CAPR×377, STDN×190, HTFL×62 |
| 2026-08-18 | -6.20 | $30.61 | XHG×619, CAPR×377, STDN×190, HTFL×62 | $10,398.87 | +10.14 | +72.92 | — | XHG, STDN | $4,987.66 | $10,461.08 | CAPR×377, HTFL×62 |
| 2026-08-19 | -7.20 | $4,987.66 | CAPR×377, HTFL×62 | $10,551.53 | +90.45 | +0.00 | — | CAPR, HTFL | $10,544.37 | $10,544.37 | — |
| 2026-08-20 | +1.12 | $10,544.37 | — | $10,544.37 | -0.00 | +103.92 | MRNA, AMLX, CYPH, PURR | — | $58.72 | $10,610.92 | MRNA×17, AMLX×70, CYPH×2292, PURR×276 |
| 2026-08-21 | +3.25 | $58.72 | MRNA×17, AMLX×70, CYPH×2292, PURR×276 | $11,036.20 | +425.28 | +301.53 | XHG | PURR | $0.21 | $11,325.63 | MRNA×17, AMLX×70, CYPH×2292, XHG×657 |
| 2026-08-24 | -5.17 | $0.21 | MRNA×17, AMLX×70, CYPH×2292, XHG×657 | $12,163.51 | +837.88 | +0.00 | — | MRNA, AMLX, CYPH, XHG | $12,120.61 | $12,120.61 | — |
| 2026-08-25 | +1.80 | $12,120.61 | — | $12,120.61 | +0.00 | +980.14 | REAX, CYPH, MRNA, XHG | — | $0.78 | $13,061.70 | REAX×125, CYPH×1942, MRNA×21, XHG×743 |
| 2026-08-26 | +2.02 | $0.78 | REAX×125, CYPH×1942, MRNA×21, XHG×743 | $12,503.26 | -558.44 | +120.97 | BYND, USDE, ARCT | REAX, CYPH, XHG | $9.94 | $12,574.47 | MRNA×21, BYND×217, USDE×529, ARCT×200 |
| 2026-08-27 | — | $9.94 | MRNA×21, BYND×217, USDE×529, ARCT×200 | $12,705.62 | +131.15 | -17.79 | XHG, CAPR, BZ | BYND, USDE, ARCT | $11.56 | $12,658.13 | MRNA×21, XHG×793, CAPR×350, BZ×173 |
| 2026-08-28 | +0.75 | $11.56 | MRNA×21, XHG×793, CAPR×350, BZ×173 | $12,364.17 | -293.96 | -109.48 | BYND, ARCT | XHG, BZ | $11.09 | $12,236.38 | MRNA×21, CAPR×350, BYND×216, ARCT×196 |
| 2026-08-31 | -5.85 | $11.09 | MRNA×21, CAPR×350, BYND×216, ARCT×196 | $12,075.15 | -161.23 | -110.16 | — | MRNA, CAPR, ARCT | $9,082.87 | $11,955.67 | BYND×216 |
| 2026-09-01 | -6.30 | $9,082.87 | BYND×216 | $11,899.51 | -56.16 | +0.00 | — | BYND | $11,896.67 | $11,896.67 | — |
| 2026-09-02 | -3.83 | $11,896.67 | — | $11,896.67 | -0.00 | +0.00 | — | — | $11,896.67 | $11,896.67 | — |
| 2026-09-03 | -0.90 | $11,896.67 | — | $11,896.67 | -0.00 | -628.18 | GPRO, REAX, CNH, MMED | — | $10.02 | $11,239.32 | GPRO×1670, REAX×161, CNH×216, MMED×124 |
| 2026-09-04 | +2.25 | $10.02 | GPRO×1670, REAX×161, CNH×216, MMED×124 | $11,360.17 | +120.85 | +679.10 | CHPT, DPRO, ASST | REAX, CNH, MMED | $13.85 | $12,019.05 | GPRO×1670, CHPT×318, DPRO×465, ASST×117 |
| 2026-09-08 | -11.47 | $13.85 | GPRO×1670, CHPT×318, DPRO×465, ASST×117 | $11,694.81 | -324.24 | -171.72 | — | GPRO, DPRO, ASST | $8,513.11 | $11,492.77 | CHPT×318 |
| 2026-09-09 | -13.95 | $8,513.11 | CHPT×318 | $11,499.13 | +6.36 | +0.00 | — | CHPT | $11,494.95 | $11,494.95 | — |
| 2026-09-10 | -13.28 | $11,494.95 | — | $11,494.95 | +0.00 | +0.00 | — | — | $11,494.95 | $11,494.95 | — |
| 2026-09-11 | +0.50 | $11,494.95 | — | $11,494.95 | +0.00 | +294.33 | INDP, BNC, IRD, CMRC | — | $1.35 | $11,750.30 | INDP×1064, BNC×585, IRD×466, CMRC×907 |
| 2026-09-14 | -11.00 | $1.35 | INDP×1064, BNC×585, IRD×466, CMRC×907 | $11,911.99 | +161.69 | +620.07 | — | IRD | $2,800.56 | $12,525.95 | INDP×1064, BNC×585, CMRC×907 |
| 2026-09-15 | -3.84 | $2,800.56 | INDP×1064, BNC×585, CMRC×907 | $12,708.99 | +183.04 | +138.36 | — | CMRC | $6,090.16 | $12,835.47 | INDP×1064, BNC×585 |
| 2026-09-16 | +5.30 | $6,090.16 | INDP×1064, BNC×585 | $12,774.85 | -60.62 | -21.21 | HLP, SDGR, SSL | BNC | $1.59 | $12,719.80 | INDP×1064, HLP×1643, SDGR×126, SSL×202 |
| 2026-09-17 | +7.38 | $1.59 | INDP×1064, HLP×1643, SDGR×126, SSL×202 | $12,779.97 | +60.17 | +492.81 | BBNX, BRR | SDGR, SSL | $3.37 | $13,254.14 | INDP×1064, HLP×1643, BBNX×129, BRR×868 |
| 2026-09-18 | +4.86 | $3.37 | INDP×1064, HLP×1643, BBNX×129, BRR×868 | $13,166.51 | -87.63 | +82.52 | SDGR, CYPH, TEM | HLP, BBNX, BRR | $85.87 | $13,196.55 | INDP×1064, SDGR×102, CYPH×992, TEM×36 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $7,514.93 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 107 | $23.33 | $2.31 | — | $5,016.31 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 49 | $50.62 | $2.14 | — | $2,533.63 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3085 | $0.81 | $34.24 | — | $0.54 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+13.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.54 | ▲ close $10,345.37 vs 09:30 $10,000.00 (session +386.21) | 16:00 close · cash $0.54 · equity $10,345.37 vs 09:30 $10,000.00 (+345.37; session marks +386.21) · 4 name(s) marked open→close (per-name table). IREN×54 09:30 $45.98 → close $44.76 -65.88; TNDM×107 09:30 $23.33 → close $23.13 -21.40; TPG×49 09:30 $50.62 → close $54.62 +195.84; INO×3085 09:30 $0.81 → close $0.90 +277.65 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.54 | ▲ 09:30 equity $10,412.10 vs yday $10,345.37 (+66.73) | 09:30 open · cash $0.54 (unchanged overnight, no fees) · equity $10,412.10 vs prior close $10,345.37 (+66.73) · 4 name(s) re-marked at the open (per-name table). IREN×54 yday $44.76 → 09:30 $44.09 -36.18; TNDM×107 yday $23.13 → 09:30 $22.92 -22.47; TPG×49 yday $54.62 → 09:30 $55.29 +32.83; INO×3085 yday $0.90 → 09:30 $0.93 +92.55 | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 54 | $44.09 | $2.18 | $-106.39 | $2,379.22 | ▼ -106.39 after sell → book $10,409.92; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 107 | $22.92 | $2.35 | $-48.53 | $4,829.31 | ▼ -48.53 after sell → book $10,407.57; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 49 | $55.29 | $2.17 | $+224.37 | $7,536.35 | ▲ +224.37 after sell → book $10,405.40; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 3085 | $0.93 | $38.48 | $+297.48 | $10,366.92 | ▲ +297.48 after sell → book $10,366.92; vs 09:30 mark -38.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 105 | $24.68 | $2.31 | — | $7,773.22 | — | top 4 by hot; rank hot_score; list yday_gainer,oppset; 🔵; ⚪; ret5=+111.3; leftover $2591.73 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 132 | $19.57 | $2.39 | — | $5,187.59 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+58.7; leftover $2591.73 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `EROC` | 161 | $16.05 | $2.47 | — | $2,601.07 | — | top 4 by hot; rank hot_score; list oppset; 🔵; ⚪; ret5=+50.4; leftover $2591.73 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 1175 | $2.20 | $15.16 | — | $0.91 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $2591.73 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.91 | ▲ close $10,533.44 vs 09:30 $10,412.10 (session +188.84) | 16:00 close · cash $0.91 · equity $10,533.44 vs 09:30 $10,412.10 (+121.34; session marks +188.84) · 4 name(s) marked open→close (per-name table). QMCO×105 09:30 $24.68 → close $26.11 +150.15; ARX×132 09:30 $19.57 → close $19.58 +1.32; EROC×161 09:30 $16.05 → close $16.72 +107.87; ZENA×1175 09:30 $2.20 → close $2.14 -70.50 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.91 | ▼ 09:30 equity $10,402.32 vs yday $10,533.44 (-131.12) | 09:30 open · cash $0.91 (unchanged overnight, no fees) · equity $10,402.32 vs prior close $10,533.44 (-131.12) · 4 name(s) re-marked at the open (per-name table). QMCO×105 yday $26.11 → 09:30 $24.83 -134.40; ARX×132 yday $19.58 → 09:30 $19.57 -1.32; EROC×161 yday $16.72 → 09:30 $17.15 +69.23; ZENA×1175 yday $2.14 → 09:30 $2.08 -64.63 | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 105 | $24.83 | $2.34 | $+11.10 | $2,605.72 | ▲ +11.10 after sell → book $10,399.98; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 132 | $19.57 | $2.43 | $-4.81 | $5,186.53 | ▼ -4.81 after sell → book $10,397.55; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `EROC` | 161 | $17.15 | $2.52 | $+172.11 | $7,945.16 | ▲ +172.11 after sell → book $10,395.03; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 1175 | $2.08 | $15.37 | $-165.65 | $10,379.66 | ▼ -165.65 after sell → book $10,379.66; vs 09:30 mark -15.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 619 | $4.19 | $7.99 | — | $7,778.06 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; leftover $2594.91 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 377 | $6.87 | $4.86 | — | $5,183.21 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,oppset; ret5=+62.6; leftover $2594.91 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 190 | $13.64 | $2.56 | — | $2,589.05 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $2594.91 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 62 | $41.23 | $2.18 | — | $30.61 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,oppset; ret5=+46.0; leftover $2594.91 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.61 | ▲ close $10,388.73 vs 09:30 $10,402.32 (session +26.66) | 16:00 close · cash $30.61 · equity $10,388.73 vs 09:30 $10,402.32 (-13.59; session marks +26.66) · 4 name(s) marked open→close (per-name table). XHG×619 09:30 $4.19 → close $3.91 -173.32; CAPR×377 09:30 $6.87 → close $7.45 +218.66; STDN×190 09:30 $13.64 → close $13.31 -62.70; HTFL×62 09:30 $41.23 → close $41.94 +44.02 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.61 | ▲ 09:30 equity $10,398.87 vs yday $10,388.73 (+10.14) | 09:30 open · cash $30.61 (unchanged overnight, no fees) · equity $10,398.87 vs prior close $10,388.73 (+10.14) · 4 name(s) re-marked at the open (per-name table). XHG×619 yday $3.91 → 09:30 $3.94 +18.57; CAPR×377 yday $7.45 → 09:30 $7.50 +18.85; STDN×190 yday $13.31 → 09:30 $13.31 +0.00; HTFL×62 yday $41.94 → 09:30 $41.50 -27.28 | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 619 | $3.94 | $8.11 | $-170.84 | $2,461.37 | ▼ -170.84 after sell → book $10,390.77; vs 09:30 mark -8.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 190 | $13.31 | $2.61 | $-67.87 | $4,987.66 | ▼ -67.87 after sell → book $10,388.16; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,987.66 | ▲ close $10,461.08 vs 09:30 $10,398.87 (session +72.92) | 16:00 close · cash $4,987.66 · equity $10,461.08 vs 09:30 $10,398.87 (+62.21; session marks +72.92) · 2 name(s) marked open→close (per-name table). CAPR×377 09:30 $7.50 → close $7.08 -158.34; HTFL×62 09:30 $41.50 → close $45.23 +231.26 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,987.66 | ▲ 09:30 equity $10,551.53 vs yday $10,461.08 (+90.45) | 09:30 open · cash $4,987.66 (unchanged overnight, no fees) · equity $10,551.53 vs prior close $10,461.08 (+90.45) · 2 name(s) re-marked at the open (per-name table). CAPR×377 yday $7.08 → 09:30 $7.19 +41.47; HTFL×62 yday $45.23 → 09:30 $46.02 +48.98 | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 377 | $7.19 | $4.95 | $+110.83 | $7,693.34 | ▲ +110.83 after sell → book $10,546.58; vs 09:30 mark -4.95 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 09:30 ET | **SELL** | `HTFL` | 62 | $46.02 | $2.21 | $+292.59 | $10,544.37 | ▲ +292.59 after sell → book $10,544.37; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,544.37 | ▲ close $10,544.37 vs 09:30 $10,551.53 (session +0.00) | 16:00 close · cash $10,544.37 · no lots left · equity $10,544.37. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,544.37 | ▲ 09:30 equity $10,544.37 vs yday $10,544.37 (-0.00) | 09:30 open · cash $10,544.37 · no holdings · equity $10,544.37 vs prior close $10,544.37 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 17 | $150.14 | $2.04 | — | $7,989.95 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,oppset; 🔵; ret5=+173.9; leftover $2636.09 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AMLX` | 70 | $37.49 | $2.20 | — | $5,363.45 | — | top 4 by hot; rank hot_score; list oppset; 🔵; ⚪; ret5=+65.3; leftover $2636.09 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 2292 | $1.15 | $29.57 | — | $2,698.08 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $2636.09 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `PURR` | 276 | $9.55 | $3.56 | — | $58.72 | — | top 4 by hot; rank hot_score; list oppset; 🔵; ⚪; ret5=+39.1; leftover $2636.09 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $58.72 | ▲ close $10,610.92 vs 09:30 $10,544.37 (session +103.92) | 16:00 close · cash $58.72 · equity $10,610.92 vs 09:30 $10,544.37 (+66.55; session marks +103.92) · 4 name(s) marked open→close (per-name table). MRNA×17 09:30 $150.14 → close $133.32 -285.94; AMLX×70 09:30 $37.49 → close $39.66 +151.90; CYPH×2292 09:30 $1.15 → close $1.19 +91.68; PURR×276 09:30 $9.55 → close $10.08 +146.28 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $58.72 | ▲ 09:30 equity $11,036.20 vs yday $10,610.92 (+425.28) | 09:30 open · cash $58.72 (unchanged overnight, no fees) · equity $11,036.20 vs prior close $10,610.92 (+425.28) · 4 name(s) re-marked at the open (per-name table). MRNA×17 yday $133.32 → 09:30 $133.11 -3.57; AMLX×70 yday $39.66 → 09:30 $39.80 +9.45; CYPH×2292 yday $1.19 → 09:30 $1.32 +297.96; PURR×276 yday $10.08 → 09:30 $10.52 +121.44 | — |
| 2026-08-21 09:30 ET | **SELL** | `PURR` | 276 | $10.52 | $3.63 | $+260.53 | $2,958.61 | ▲ +260.53 after sell → book $11,032.57; vs 09:30 mark -3.63 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 657 | $4.49 | $8.48 | — | $0.21 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; leftover $2958.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.21 | ▲ close $11,325.63 vs 09:30 $11,036.20 (session +301.53) | 16:00 close · cash $0.21 · equity $11,325.63 vs 09:30 $11,036.20 (+289.43; session marks +301.53) · 4 name(s) marked open→close (per-name table). MRNA×17 09:30 $133.11 → close $145.13 +204.34; AMLX×70 09:30 $39.80 → close $38.66 -79.45; CYPH×2292 09:30 $1.32 → close $1.42 +229.20; XHG×657 09:30 $4.49 → close $4.41 -52.56 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.21 | ▲ 09:30 equity $12,163.51 vs yday $11,325.63 (+837.88) | 09:30 open · cash $0.21 (unchanged overnight, no fees) · equity $12,163.51 vs prior close $11,325.63 (+837.88) · 4 name(s) re-marked at the open (per-name table). MRNA×17 yday $145.13 → 09:30 $142.70 -41.31; AMLX×70 yday $38.66 → 09:30 $38.64 -1.40; CYPH×2292 yday $1.42 → 09:30 $1.83 +939.72; XHG×657 yday $4.41 → 09:30 $4.32 -59.13 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 17 | $142.70 | $2.07 | $-130.59 | $2,424.04 | ▼ -130.59 after sell → book $12,161.44; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AMLX` | 70 | $38.64 | $2.23 | $+76.07 | $5,126.60 | ▲ +76.07 after sell → book $12,159.20; vs 09:30 mark -2.24 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 2292 | $1.83 | $29.98 | $+1499.01 | $9,290.98 | ▲ +1,499.01 after sell → book $12,129.22; vs 09:30 mark -29.98 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 657 | $4.32 | $8.61 | $-128.77 | $12,120.61 | ▼ -128.77 after sell → book $12,120.61; vs 09:30 mark -8.61 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,120.61 | ▲ close $12,120.61 vs 09:30 $12,163.51 (session +0.00) | 16:00 close · cash $12,120.61 · no lots left · equity $12,120.61. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,120.61 | ▲ 09:30 equity $12,120.61 vs yday $12,120.61 (+0.00) | 09:30 open · cash $12,120.61 · no holdings · equity $12,120.61 vs prior close $12,120.61 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 125 | $24.11 | $2.37 | — | $9,104.50 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; leftover $3030.15 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 1942 | $1.56 | $25.05 | — | $6,049.93 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $3030.15 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MRNA` | 21 | $143.50 | $2.05 | — | $3,034.37 | — | top 4 by hot; rank hot_score; list oppset; ret5=+115.5; leftover $3030.15 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 743 | $4.07 | $9.58 | — | $0.78 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; leftover $3030.15 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.78 | ▲ close $13,061.70 vs 09:30 $12,120.61 (session +980.14) | 16:00 close · cash $0.78 · equity $13,061.70 vs 09:30 $12,120.61 (+941.09; session marks +980.14) · 4 name(s) marked open→close (per-name table). REAX×125 09:30 $24.11 → close $28.43 +540.00; CYPH×1942 09:30 $1.56 → close $1.64 +155.36; MRNA×21 09:30 $143.50 → close $158.83 +321.93; XHG×743 09:30 $4.07 → close $4.02 -37.15 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.78 | ▼ 09:30 equity $12,503.26 vs yday $13,061.70 (-558.44) | 09:30 open · cash $0.78 (unchanged overnight, no fees) · equity $12,503.26 vs prior close $13,061.70 (-558.44) · 4 name(s) re-marked at the open (per-name table). REAX×125 yday $28.43 → 09:30 $26.61 -227.50; CYPH×1942 yday $1.64 → 09:30 $1.60 -77.68; MRNA×21 yday $158.83 → 09:30 $154.20 -97.23; XHG×743 yday $4.02 → 09:30 $3.81 -156.03 | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 125 | $26.61 | $2.41 | $+307.72 | $3,324.62 | ▲ +307.72 after sell → book $12,500.85; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 1942 | $1.60 | $25.40 | $+27.23 | $6,406.42 | ▲ +27.23 after sell → book $12,475.45; vs 09:30 mark -25.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `XHG` | 743 | $3.81 | $9.73 | $-212.50 | $9,227.52 | ▼ -212.50 after sell → book $12,465.72; vs 09:30 mark -9.73 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 217 | $14.11 | $2.80 | — | $6,162.85 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; leftover $3075.84 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 529 | $5.81 | $6.82 | — | $3,082.53 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $3075.84 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ARCT` | 200 | $15.35 | $2.59 | — | $9.94 | — | top 4 by hot; rank hot_score; list oppset; 🔵; ret5=+88.1; leftover $3075.84 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.94 | ▲ close $12,574.47 vs 09:30 $12,503.26 (session +120.97) | 16:00 close · cash $9.94 · equity $12,574.47 vs 09:30 $12,503.26 (+71.21; session marks +120.97) · 4 name(s) marked open→close (per-name table). MRNA×21 09:30 $154.20 → close $149.66 -95.34; BYND×217 09:30 $14.11 → close $14.25 +30.38; USDE×529 09:30 $5.81 → close $5.98 +89.93; ARCT×200 09:30 $15.35 → close $15.83 +96.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.94 | ▲ 09:30 equity $12,705.62 vs yday $12,574.47 (+131.15) | 09:30 open · cash $9.94 (unchanged overnight, no fees) · equity $12,705.62 vs prior close $12,574.47 (+131.15) · 4 name(s) re-marked at the open (per-name table). MRNA×21 yday $149.66 → 09:30 $144.18 -115.08; BYND×217 yday $14.25 → 09:30 $14.20 -10.85; USDE×529 yday $5.98 → 09:30 $6.50 +275.08; ARCT×200 yday $15.83 → 09:30 $15.74 -18.00 | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 217 | $14.20 | $2.86 | $+13.87 | $3,088.48 | ▲ +13.87 after sell → book $12,702.76; vs 09:30 mark -2.86 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 529 | $6.50 | $6.94 | $+351.25 | $6,520.04 | ▲ +351.25 after sell → book $12,695.82; vs 09:30 mark -6.94 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟡 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 200 | $15.74 | $2.65 | $+72.76 | $9,665.40 | ▲ +72.76 after sell → book $12,693.18; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `XHG` | 793 | $4.06 | $10.23 | — | $6,435.59 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-6.2; leftover $3221.80 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 350 | $9.19 | $4.51 | — | $3,214.57 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; leftover $3221.80 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 173 | $18.50 | $2.51 | — | $11.56 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; leftover $3221.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.56 | ▼ close $12,658.13 vs 09:30 $12,705.62 (session -17.79) | 16:00 close · cash $11.56 · equity $12,658.13 vs 09:30 $12,705.62 (-47.49; session marks -17.79) · 4 name(s) marked open→close (per-name table). MRNA×21 09:30 $144.18 → close $142.77 -29.61; XHG×793 09:30 $4.06 → close $3.80 -206.18; CAPR×350 09:30 $9.19 → close $10.06 +304.50; BZ×173 09:30 $18.50 → close $18.00 -86.50 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.56 | ▼ 09:30 equity $12,364.17 vs yday $12,658.13 (-293.96) | 09:30 open · cash $11.56 (unchanged overnight, no fees) · equity $12,364.17 vs prior close $12,658.13 (-293.96) · 4 name(s) re-marked at the open (per-name table). MRNA×21 yday $142.77 → 09:30 $137.19 -117.18; XHG×793 yday $3.80 → 09:30 $3.69 -87.23; CAPR×350 yday $10.06 → 09:30 $9.73 -115.50; BZ×173 yday $18.00 → 09:30 $18.15 +25.95 | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 793 | $3.69 | $10.38 | $-314.02 | $2,927.35 | ▼ -314.02 after sell → book $12,353.79; vs 09:30 mark -10.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 173 | $18.15 | $2.56 | $-65.62 | $6,064.73 | ▼ -65.62 after sell → book $12,351.22; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 216 | $14.00 | $2.79 | — | $3,037.95 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; leftover $3032.37 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ARCT` | 196 | $15.43 | $2.58 | — | $11.09 | — | top 4 by hot; rank hot_score; list oppset; ret5=+47.1; leftover $3032.37 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.09 | ▼ close $12,236.38 vs 09:30 $12,364.17 (session -109.48) | 16:00 close · cash $11.09 · equity $12,236.38 vs 09:30 $12,364.17 (-127.79; session marks -109.48) · 4 name(s) marked open→close (per-name table). MRNA×21 09:30 $137.19 → close $137.99 +16.80; CAPR×350 09:30 $9.73 → close $9.59 -49.00; BYND×216 09:30 $14.00 → close $13.86 -30.24; ARCT×196 09:30 $15.43 → close $15.19 -47.04 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.09 | ▼ 09:30 equity $12,075.15 vs yday $12,236.38 (-161.23) | 09:30 open · cash $11.09 (unchanged overnight, no fees) · equity $12,075.15 vs prior close $12,236.38 (-161.23) · 4 name(s) re-marked at the open (per-name table). MRNA×21 yday $137.99 → 09:30 $134.10 -81.69; CAPR×350 yday $9.59 → 09:30 $9.50 -31.50; BYND×216 yday $13.86 → 09:30 $13.81 -10.80; ARCT×196 yday $15.19 → 09:30 $15.00 -37.24 | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 21 | $134.10 | $2.09 | $-201.54 | $2,825.10 | ▼ -201.54 after sell → book $12,073.06; vs 09:30 mark -2.09 | dropped from list after 4 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 350 | $9.50 | $4.60 | $+99.39 | $6,145.51 | ▲ +99.39 after sell → book $12,068.47; vs 09:30 mark -4.59 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ARCT` | 196 | $15.00 | $2.63 | $-89.49 | $9,082.87 | ▼ -89.49 after sell → book $12,065.83; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,082.87 | ▼ close $11,955.67 vs 09:30 $12,075.15 (session -110.16) | 16:00 close · cash $9,082.87 · equity $11,955.67 vs 09:30 $12,075.15 (-119.48; session marks -110.16) · 1 name(s) marked open→close (per-name table). BYND×216 09:30 $13.81 → close $13.30 -110.16 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,082.87 | ▼ 09:30 equity $11,899.51 vs yday $11,955.67 (-56.16) | 09:30 open · cash $9,082.87 (unchanged overnight, no fees) · equity $11,899.51 vs prior close $11,955.67 (-56.16) · 1 name(s) re-marked at the open (per-name table). BYND×216 yday $13.30 → 09:30 $13.04 -56.16 | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 216 | $13.04 | $2.84 | $-212.99 | $11,896.67 | ▼ -212.99 after sell → book $11,896.67; vs 09:30 mark -2.84 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,896.67 | ▲ close $11,896.67 vs 09:30 $11,899.51 (session +0.00) | 16:00 close · cash $11,896.67 · no lots left · equity $11,896.67. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,896.67 | ▲ 09:30 equity $11,896.67 vs yday $11,896.67 (-0.00) | 09:30 open · cash $11,896.67 · no holdings · equity $11,896.67 vs prior close $11,896.67 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,896.67 | ▲ close $11,896.67 vs 09:30 $11,896.67 (session +0.00) | 16:00 close · cash $11,896.67 · no lots left · equity $11,896.67. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,896.67 | ▲ 09:30 equity $11,896.67 vs yday $11,896.67 (-0.00) | 09:30 open · cash $11,896.67 · no holdings · equity $11,896.67 vs prior close $11,896.67 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1670 | $1.78 | $21.54 | — | $8,902.52 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; leftover $2974.17 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 161 | $18.40 | $2.47 | — | $5,937.65 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; leftover $2974.17 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 216 | $13.71 | $2.79 | — | $2,973.50 | — | top 4 by hot; rank hot_score; list ohlc_hot,oppset; 🔵; ret5=+17.5; leftover $2974.17 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 124 | $23.88 | $2.36 | — | $10.02 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+21.9; leftover $2974.17 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.02 | ▼ close $11,239.32 vs 09:30 $11,896.67 (session -628.18) | 16:00 close · cash $10.02 · equity $11,239.32 vs 09:30 $11,896.67 (-657.35; session marks -628.18) · 4 name(s) marked open→close (per-name table). GPRO×1670 09:30 $1.78 → close $1.39 -651.30; REAX×161 09:30 $18.40 → close $18.40 +0.00; CNH×216 09:30 $13.71 → close $13.84 +28.08; MMED×124 09:30 $23.88 → close $23.84 -4.96 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.02 | ▲ 09:30 equity $11,360.17 vs yday $11,239.32 (+120.85) | 09:30 open · cash $10.02 (unchanged overnight, no fees) · equity $11,360.17 vs prior close $11,239.32 (+120.85) · 4 name(s) re-marked at the open (per-name table). GPRO×1670 yday $1.39 → 09:30 $1.48 +150.30; REAX×161 yday $18.40 → 09:30 $18.15 -40.25; CNH×216 yday $13.84 → 09:30 $13.89 +10.80; MMED×124 yday $23.84 → 09:30 $23.84 +0.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 161 | $18.15 | $2.52 | $-45.25 | $2,929.65 | ▼ -45.25 after sell → book $11,357.65; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 216 | $13.89 | $2.85 | $+33.25 | $5,927.04 | ▲ +33.25 after sell → book $11,354.80; vs 09:30 mark -2.85 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 124 | $23.84 | $2.41 | $-9.73 | $8,880.80 | ▼ -9.73 after sell → book $11,352.40; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CHPT` | 318 | $9.28 | $4.10 | — | $5,925.65 | — | top 4 by hot; rank hot_score; list oppset; 🔵; ret5=+55.7; leftover $2960.27 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DPRO` | 465 | $6.36 | $6.00 | — | $2,962.26 | — | top 4 by hot; rank hot_score; list oppset; 🔵; ret5=+47.1; leftover $2960.27 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 117 | $25.18 | $2.34 | — | $13.85 | — | top 4 by hot; rank hot_score; list ohlc_hot,oppset; 🔵; ret5=+16.0; leftover $2960.27 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.85 | ▲ close $12,019.05 vs 09:30 $11,360.17 (session +679.10) | 16:00 close · cash $13.85 · equity $12,019.05 vs 09:30 $11,360.17 (+658.88; session marks +679.10) · 4 name(s) marked open→close (per-name table). GPRO×1670 09:30 $1.48 → close $1.70 +367.40; CHPT×318 09:30 $9.28 → close $9.89 +193.98; DPRO×465 09:30 $6.36 → close $6.12 -111.60; ASST×117 09:30 $25.18 → close $27.14 +229.32 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.85 | ▼ 09:30 equity $11,694.81 vs yday $12,019.05 (-324.24) | 09:30 open · cash $13.85 (unchanged overnight, no fees) · equity $11,694.81 vs prior close $12,019.05 (-324.24) · 4 name(s) re-marked at the open (per-name table). GPRO×1670 yday $1.70 → 09:30 $1.56 -225.45; CHPT×318 yday $9.89 → 09:30 $9.91 +6.36; DPRO×465 yday $6.12 → 09:30 $6.07 -23.25; ASST×117 yday $27.14 → 09:30 $26.44 -81.90 | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 1670 | $1.56 | $21.84 | $-402.43 | $2,605.56 | ▼ -402.43 after sell → book $11,672.97; vs 09:30 mark -21.84 | dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DPRO` | 465 | $6.07 | $6.10 | $-146.95 | $5,422.02 | ▼ -146.95 after sell → book $11,666.88; vs 09:30 mark -6.09 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 117 | $26.44 | $2.39 | $+142.69 | $8,513.11 | ▲ +142.69 after sell → book $11,664.49; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,513.11 | ▼ close $11,492.77 vs 09:30 $11,694.81 (session -171.72) | 16:00 close · cash $8,513.11 · equity $11,492.77 vs 09:30 $11,694.81 (-202.04; session marks -171.72) · 1 name(s) marked open→close (per-name table). CHPT×318 09:30 $9.91 → close $9.37 -171.72 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,513.11 | ▲ 09:30 equity $11,499.13 vs yday $11,492.77 (+6.36) | 09:30 open · cash $8,513.11 (unchanged overnight, no fees) · equity $11,499.13 vs prior close $11,492.77 (+6.36) · 1 name(s) re-marked at the open (per-name table). CHPT×318 yday $9.37 → 09:30 $9.39 +6.36 | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 318 | $9.39 | $4.18 | $+26.70 | $11,494.95 | ▲ +26.70 after sell → book $11,494.95; vs 09:30 mark -4.18 | dropped from list after 2 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,494.95 | ▲ close $11,494.95 vs 09:30 $11,499.13 (session +0.00) | 16:00 close · cash $11,494.95 · no lots left · equity $11,494.95. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,494.95 | ▲ 09:30 equity $11,494.95 vs yday $11,494.95 (+0.00) | 09:30 open · cash $11,494.95 · no holdings · equity $11,494.95 vs prior close $11,494.95 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,494.95 | ▲ close $11,494.95 vs 09:30 $11,494.95 (session +0.00) | 16:00 close · cash $11,494.95 · no lots left · equity $11,494.95. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,494.95 | ▲ 09:30 equity $11,494.95 vs yday $11,494.95 (+0.00) | 09:30 open · cash $11,494.95 · no holdings · equity $11,494.95 vs prior close $11,494.95 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 1064 | $2.70 | $13.73 | — | $8,608.43 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $2873.74 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 585 | $4.91 | $7.55 | — | $5,728.53 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $2873.74 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 466 | $6.16 | $6.01 | — | $2,851.96 | — | top 4 by hot; rank hot_score; list yday_gainer,oppset; 🔵; ret5=+36.4; leftover $2873.74 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 907 | $3.13 | $11.70 | — | $1.35 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; leftover $2873.74 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.35 | ▲ close $11,750.30 vs 09:30 $11,494.95 (session +294.33) | 16:00 close · cash $1.35 · equity $11,750.30 vs 09:30 $11,494.95 (+255.35; session marks +294.33) · 4 name(s) marked open→close (per-name table). INDP×1064 09:30 $2.70 → close $2.77 +74.48; BNC×585 09:30 $4.91 → close $4.80 -64.35; IRD×466 09:30 $6.16 → close $6.04 -55.92; CMRC×907 09:30 $3.13 → close $3.50 +340.12 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.35 | ▲ 09:30 equity $11,911.99 vs yday $11,750.30 (+161.69) | 09:30 open · cash $1.35 (unchanged overnight, no fees) · equity $11,911.99 vs prior close $11,750.30 (+161.69) · 4 name(s) re-marked at the open (per-name table). INDP×1064 yday $2.77 → 09:30 $2.80 +31.92; BNC×585 yday $4.80 → 09:30 $5.03 +134.55; IRD×466 yday $6.04 → 09:30 $6.02 -9.32; CMRC×907 yday $3.50 → 09:30 $3.51 +4.53 | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 466 | $6.02 | $6.11 | $-77.36 | $2,800.56 | ▼ -77.36 after sell → book $11,905.88; vs 09:30 mark -6.11 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,800.56 | ▲ close $12,525.95 vs 09:30 $11,911.99 (session +620.07) | 16:00 close · cash $2,800.56 · equity $12,525.95 vs 09:30 $11,911.99 (+613.96; session marks +620.07) · 3 name(s) marked open→close (per-name table). INDP×1064 09:30 $2.80 → close $3.14 +361.76; BNC×585 09:30 $5.03 → close $5.27 +140.40; CMRC×907 09:30 $3.51 → close $3.64 +117.91 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,800.56 | ▲ 09:30 equity $12,708.99 vs yday $12,525.95 (+183.04) | 09:30 open · cash $2,800.56 (unchanged overnight, no fees) · equity $12,708.99 vs prior close $12,525.95 (+183.04) · 3 name(s) re-marked at the open (per-name table). INDP×1064 yday $3.14 → 09:30 $3.40 +276.64; BNC×585 yday $5.27 → 09:30 $5.11 -93.60; CMRC×907 yday $3.64 → 09:30 $3.64 +0.00 | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 907 | $3.64 | $11.88 | $+438.99 | $6,090.16 | ▲ +438.99 after sell → book $12,697.11; vs 09:30 mark -11.88 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,090.16 | ▲ close $12,835.47 vs 09:30 $12,708.99 (session +138.36) | 16:00 close · cash $6,090.16 · equity $12,835.47 vs 09:30 $12,708.99 (+126.48; session marks +138.36) · 2 name(s) marked open→close (per-name table). INDP×1064 09:30 $3.40 → close $3.64 +255.36; BNC×585 09:30 $5.11 → close $4.91 -117.00 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,090.16 | ▼ 09:30 equity $12,774.85 vs yday $12,835.47 (-60.62) | 09:30 open · cash $6,090.16 (unchanged overnight, no fees) · equity $12,774.85 vs prior close $12,835.47 (-60.62) · 2 name(s) re-marked at the open (per-name table). INDP×1064 yday $3.64 → 09:30 $3.66 +21.28; BNC×585 yday $4.91 → 09:30 $4.77 -81.90 | — |
| 2026-09-16 09:30 ET | **SELL** | `BNC` | 585 | $4.77 | $7.67 | $-97.11 | $8,872.94 | ▼ -97.11 after sell → book $12,767.18; vs 09:30 mark -7.67 | dropped from list after 3 sess (min 1) | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 1643 | $1.80 | $21.19 | — | $5,894.35 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $2957.65 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 126 | $23.29 | $2.37 | — | $2,957.44 | — | top 4 by hot; rank hot_score; list yday_gainer,oppset; 🔵; ret5=+16.1; leftover $2957.65 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 202 | $14.62 | $2.61 | — | $1.59 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; leftover $2957.65 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.59 | ▼ close $12,719.80 vs 09:30 $12,774.85 (session -21.21) | 16:00 close · cash $1.59 · equity $12,719.80 vs 09:30 $12,774.85 (-55.05; session marks -21.21) · 4 name(s) marked open→close (per-name table). INDP×1064 09:30 $3.66 → close $3.21 -478.80; HLP×1643 09:30 $1.80 → close $2.07 +443.61; SDGR×126 09:30 $23.29 → close $23.93 +80.64; SSL×202 09:30 $14.62 → close $14.29 -66.66 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.59 | ▲ 09:30 equity $12,779.97 vs yday $12,719.80 (+60.17) | 09:30 open · cash $1.59 (unchanged overnight, no fees) · equity $12,779.97 vs prior close $12,719.80 (+60.17) · 4 name(s) re-marked at the open (per-name table). INDP×1064 yday $3.21 → 09:30 $3.30 +95.76; HLP×1643 yday $2.07 → 09:30 $2.10 +49.29; SDGR×126 yday $23.93 → 09:30 $24.09 +20.16; SSL×202 yday $14.29 → 09:30 $13.77 -105.04 | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 126 | $24.09 | $2.41 | $+96.02 | $3,034.52 | ▲ +96.02 after sell → book $12,777.56; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 202 | $13.77 | $2.66 | $-176.97 | $5,813.40 | ▼ -176.97 after sell → book $12,774.90; vs 09:30 mark -2.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 129 | $22.46 | $2.38 | — | $2,913.68 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,oppset; 🔵; ret5=+27.3; leftover $2906.70 | join🟢 sector🟢 gen🟢 news🔴 digest🔴 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `BRR` | 868 | $3.34 | $11.20 | — | $3.37 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; leftover $2906.70 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.37 | ▲ close $13,254.14 vs 09:30 $12,779.97 (session +492.81) | 16:00 close · cash $3.37 · equity $13,254.14 vs 09:30 $12,779.97 (+474.17; session marks +492.81) · 4 name(s) marked open→close (per-name table). INDP×1064 09:30 $3.30 → close $3.93 +670.32; HLP×1643 09:30 $2.10 → close $2.02 -131.44; BBNX×129 09:30 $22.46 → close $21.43 -132.87; BRR×868 09:30 $3.34 → close $3.44 +86.80 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.37 | ▼ 09:30 equity $13,166.51 vs yday $13,254.14 (-87.63) | 09:30 open · cash $3.37 (unchanged overnight, no fees) · equity $13,166.51 vs prior close $13,254.14 (-87.63) · 4 name(s) re-marked at the open (per-name table). INDP×1064 yday $3.93 → 09:30 $3.85 -85.12; HLP×1643 yday $2.02 → 09:30 $1.96 -98.58; BBNX×129 yday $21.43 → 09:30 $21.30 -16.77; BRR×868 yday $3.44 → 09:30 $3.57 +112.84 | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 1643 | $1.96 | $21.49 | $+220.19 | $3,202.15 | ▲ +220.19 after sell → book $13,145.01; vs 09:30 mark -21.50 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 129 | $21.30 | $2.42 | $-154.44 | $5,947.43 | ▼ -154.44 after sell → book $13,142.59; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRR` | 868 | $3.57 | $11.37 | $+177.08 | $9,034.83 | ▲ +177.08 after sell → book $13,131.23; vs 09:30 mark -11.36 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 102 | $29.32 | $2.30 | — | $6,041.89 | — | top 4 by hot; rank hot_score; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $3011.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 992 | $3.04 | $12.80 | — | $3,018.37 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $3011.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 36 | $81.40 | $2.10 | — | $85.87 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+36.8; leftover $3011.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.87 | ▲ close $13,196.55 vs 09:30 $13,166.51 (session +82.52) | 16:00 close · cash $85.87 · equity $13,196.55 vs 09:30 $13,166.51 (+30.04; session marks +82.52) · 4 name(s) marked open→close (per-name table). INDP×1064 09:30 $3.85 → close $3.55 -319.20; SDGR×102 09:30 $29.32 → close $29.02 -30.60; CYPH×992 09:30 $3.04 → close $3.60 +560.48; TEM×36 09:30 $81.40 → close $77.84 -128.16 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ETON` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `PURR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ROIV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `GPRO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `XHLD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GPRO` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `INDP` | 1064 | 2026-09-11 @ $2.70 | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $2873.74 |
| `SDGR` | 102 | 2026-09-18 @ $29.32 | top 4 by hot; rank hot_score; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $3011.61 |
| `CYPH` | 992 | 2026-09-18 @ $3.04 | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $3011.61 |
| `TEM` | 36 | 2026-09-18 @ $81.40 | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+36.8; leftover $3011.61 |
