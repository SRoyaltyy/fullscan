# Factor mine action — `union_candle_score_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `candle_score` · size `leftover` · sell `list` · S-boost `none` · rank by candle_score

Cash book **-0.83%** ($9,917) · signal-only (no cash/fees) was +2.86%. Starts YES **1/21**. Fills 164 · skips 78 · realized $-60.26.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how clean the prior candles looked.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by how clean the prior candles looked and keep the top 8.
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
- **Gate** `none (list as ranked)` · **rank** `candle_score` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,490.60.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TNDM` | 53 | — | $23.33 | +0.00 | $23.13 | -10.60 | -10.60 | +0.00 | -10.60 |
| 2026-08-13 | `TPG` | 24 | — | $50.62 | +0.00 | $54.62 | +95.92 | +95.92 | +0.00 | +95.92 |
| 2026-08-13 | `HIMS` | 42 | — | $29.74 | +0.00 | $28.77 | -40.74 | -40.74 | +0.00 | -40.74 |
| 2026-08-13 | `IREN` | 27 | — | $45.98 | +0.00 | $44.76 | -32.94 | -32.94 | +0.00 | -32.94 |
| 2026-08-13 | `INO` | 1543 | — | $0.81 | +0.00 | $0.90 | +138.87 | +138.87 | +0.00 | +138.87 |
| 2026-08-13 | `VOR` | 56 | — | $22.01 | +0.00 | $23.29 | +71.68 | +71.68 | +0.00 | +71.68 |
| 2026-08-13 | `BTSG` | 20 | — | $59.80 | +0.00 | $60.23 | +8.60 | +8.60 | +0.00 | +8.60 |
| 2026-08-13 | `SLS` | 106 | — | $11.70 | +0.00 | $12.36 | +69.96 | +69.96 | +0.00 | +69.96 |
| 2026-08-14 | `TNDM` | 53 | $23.13 | $22.92 | -11.13 | — | +0.00 | -11.13 | -21.73 | — |
| 2026-08-14 | `TPG` | 24 | $54.62 | $55.29 | +16.08 | — | +0.00 | +16.08 | +112.00 | — |
| 2026-08-14 | `HIMS` | 42 | $28.77 | $29.15 | +15.96 | — | +0.00 | +15.96 | -24.78 | — |
| 2026-08-14 | `IREN` | 27 | $44.76 | $44.09 | -18.09 | — | +0.00 | -18.09 | -51.03 | — |
| 2026-08-14 | `INO` | 1543 | $0.90 | $0.93 | +46.29 | — | +0.00 | +46.29 | +185.16 | — |
| 2026-08-14 | `VOR` | 56 | $23.29 | $23.33 | +2.24 | — | +0.00 | +2.24 | +73.92 | — |
| 2026-08-14 | `BTSG` | 20 | $60.23 | $59.65 | -11.60 | — | +0.00 | -11.60 | -3.00 | — |
| 2026-08-14 | `SLS` | 106 | $12.36 | $12.40 | +4.24 | — | +0.00 | +4.24 | +74.20 | — |
| 2026-08-14 | `ZS` | 6 | — | $190.00 | +0.00 | $183.60 | -38.40 | -38.40 | +0.00 | -38.40 |
| 2026-08-14 | `BETA` | 50 | — | $25.21 | +0.00 | $24.86 | -17.50 | -17.50 | +0.00 | -17.50 |
| 2026-08-14 | `SATL` | 214 | — | $5.98 | +0.00 | $5.80 | -38.52 | -38.52 | +0.00 | -38.52 |
| 2026-08-14 | `BRZE` | 42 | — | $30.00 | +0.00 | $28.93 | -44.94 | -44.94 | +0.00 | -44.94 |
| 2026-08-14 | `MH` | 94 | — | $13.55 | +0.00 | $13.10 | -42.30 | -42.30 | +0.00 | -42.30 |
| 2026-08-14 | `NMAX` | 129 | — | $9.89 | +0.00 | $10.87 | +125.77 | +125.77 | +0.00 | +125.77 |
| 2026-08-14 | `GLOB` | 33 | — | $38.21 | +0.00 | $37.38 | -27.39 | -27.39 | +0.00 | -27.39 |
| 2026-08-14 | `LUNR` | 67 | — | $19.17 | +0.00 | $19.01 | -10.72 | -10.72 | +0.00 | -10.72 |
| 2026-08-17 | `ZS` | 6 | $183.60 | $188.38 | +28.65 | — | +0.00 | +28.65 | -9.75 | — |
| 2026-08-17 | `BETA` | 50 | $24.86 | $24.61 | -12.50 | — | +0.00 | -12.50 | -30.00 | — |
| 2026-08-17 | `SATL` | 214 | $5.80 | $5.81 | +2.14 | — | +0.00 | +2.14 | -36.38 | — |
| 2026-08-17 | `BRZE` | 42 | $28.93 | $28.44 | -20.58 | — | +0.00 | -20.58 | -65.52 | — |
| 2026-08-17 | `MH` | 94 | $13.10 | $13.16 | +5.64 | — | +0.00 | +5.64 | -36.66 | — |
| 2026-08-17 | `NMAX` | 129 | $10.87 | $10.97 | +12.90 | $10.36 | -78.69 | -65.79 | +138.68 | +59.98 |
| 2026-08-17 | `GLOB` | 33 | $37.38 | $37.18 | -6.60 | — | +0.00 | -6.60 | -33.99 | — |
| 2026-08-17 | `LUNR` | 67 | $19.01 | $20.25 | +83.08 | — | +0.00 | +83.08 | +72.36 | — |
| 2026-08-17 | `NPWR` | 656 | — | $1.92 | +0.00 | $1.73 | -124.64 | -124.64 | +0.00 | -124.64 |
| 2026-08-17 | `JBIO` | 51 | — | $24.60 | +0.00 | $23.45 | -58.65 | -58.65 | +0.00 | -58.65 |
| 2026-08-17 | `HTFL` | 30 | — | $41.23 | +0.00 | $41.94 | +21.30 | +21.30 | +0.00 | +21.30 |
| 2026-08-17 | `SMJF` | 124 | — | $10.10 | +0.00 | $10.45 | +43.40 | +43.40 | +0.00 | +43.40 |
| 2026-08-17 | `STDN` | 92 | — | $13.64 | +0.00 | $13.31 | -30.36 | -30.36 | +0.00 | -30.36 |
| 2026-08-17 | `CLYM` | 77 | — | $16.25 | +0.00 | $17.44 | +91.63 | +91.63 | +0.00 | +91.63 |
| 2026-08-17 | `BORR` | 274 | — | $4.59 | +0.00 | $4.50 | -24.66 | -24.66 | +0.00 | -24.66 |
| 2026-08-18 | `NMAX` | 129 | $10.36 | $10.31 | -6.45 | — | +0.00 | -6.45 | +53.54 | — |
| 2026-08-18 | `NPWR` | 656 | $1.73 | $1.70 | -19.68 | — | +0.00 | -19.68 | -144.32 | — |
| 2026-08-18 | `JBIO` | 51 | $23.45 | $23.07 | -19.38 | — | +0.00 | -19.38 | -78.03 | — |
| 2026-08-18 | `HTFL` | 30 | $41.94 | $41.50 | -13.20 | — | +0.00 | -13.20 | +8.10 | — |
| 2026-08-18 | `SMJF` | 124 | $10.45 | $10.45 | +0.00 | — | +0.00 | +0.00 | +43.40 | — |
| 2026-08-18 | `STDN` | 92 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -30.36 | — |
| 2026-08-18 | `CLYM` | 77 | $17.44 | $16.90 | -41.58 | — | +0.00 | -41.58 | +50.05 | — |
| 2026-08-18 | `BORR` | 274 | $4.50 | $4.56 | +16.44 | — | +0.00 | +16.44 | -8.22 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `IOND` | 18 | — | $65.60 | +0.00 | $68.77 | +57.06 | +57.06 | +0.00 | +57.06 |
| 2026-08-20 | `NBP` | 631 | — | $1.97 | +0.00 | $1.91 | -37.86 | -37.86 | +0.00 | -37.86 |
| 2026-08-20 | `IMMX` | 95 | — | $12.98 | +0.00 | $13.16 | +17.10 | +17.10 | +0.00 | +17.10 |
| 2026-08-20 | `ABCL` | 105 | — | $11.81 | +0.00 | $11.57 | -25.72 | -25.72 | +0.00 | -25.72 |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `ABUS` | 252 | — | $4.92 | +0.00 | $4.77 | -37.80 | -37.80 | +0.00 | -37.80 |
| 2026-08-20 | `CYPH` | 1081 | — | $1.15 | +0.00 | $1.19 | +43.24 | +43.24 | +0.00 | +43.24 |
| 2026-08-20 | `GENB` | 74 | — | $16.76 | +0.00 | $15.99 | -56.98 | -56.98 | +0.00 | -56.98 |
| 2026-08-21 | `IOND` | 18 | $68.77 | $68.41 | -6.48 | — | +0.00 | -6.48 | +50.58 | — |
| 2026-08-21 | `NBP` | 631 | $1.91 | $1.91 | +0.00 | — | +0.00 | +0.00 | -37.86 | — |
| 2026-08-21 | `IMMX` | 95 | $13.16 | $13.36 | +19.00 | — | +0.00 | +19.00 | +36.10 | — |
| 2026-08-21 | `ABCL` | 105 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -25.72 | — |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | — | +0.00 | -1.68 | -136.24 | — |
| 2026-08-21 | `ABUS` | 252 | $4.77 | $5.20 | +108.36 | — | +0.00 | +108.36 | +70.56 | — |
| 2026-08-21 | `CYPH` | 1081 | $1.19 | $1.32 | +140.53 | $1.42 | +108.10 | +248.63 | +183.77 | +291.87 |
| 2026-08-21 | `GENB` | 74 | $15.99 | $16.10 | +8.14 | — | +0.00 | +8.14 | -48.84 | — |
| 2026-08-21 | `SM` | 32 | — | $37.81 | +0.00 | $37.20 | -19.52 | -19.52 | +0.00 | -19.52 |
| 2026-08-21 | `IOVA` | 134 | — | $9.08 | +0.00 | $8.29 | -105.86 | -105.86 | +0.00 | -105.86 |
| 2026-08-21 | `ARIS` | 58 | — | $20.90 | +0.00 | $20.86 | -2.32 | -2.32 | +0.00 | -2.32 |
| 2026-08-21 | `ARCT` | 109 | — | $11.13 | +0.00 | $13.45 | +252.88 | +252.88 | +0.00 | +252.88 |
| 2026-08-21 | `DXYZ` | 35 | — | $34.89 | +0.00 | $34.43 | -16.10 | -16.10 | +0.00 | -16.10 |
| 2026-08-21 | `ILMN` | 5 | — | $212.40 | +0.00 | $219.40 | +35.00 | +35.00 | +0.00 | +35.00 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-24 | `CYPH` | 1081 | $1.42 | $1.83 | +443.21 | — | +0.00 | +443.21 | +735.08 | — |
| 2026-08-24 | `SM` | 32 | $37.20 | $36.61 | -18.88 | — | +0.00 | -18.88 | -38.40 | — |
| 2026-08-24 | `IOVA` | 134 | $8.29 | $8.08 | -28.14 | — | +0.00 | -28.14 | -134.00 | — |
| 2026-08-24 | `ARIS` | 58 | $20.86 | $20.98 | +6.96 | — | +0.00 | +6.96 | +4.64 | — |
| 2026-08-24 | `ARCT` | 109 | $13.45 | $13.33 | -13.08 | — | +0.00 | -13.08 | +239.80 | — |
| 2026-08-24 | `DXYZ` | 35 | $34.43 | $33.10 | -46.55 | — | +0.00 | -46.55 | -62.65 | — |
| 2026-08-24 | `ILMN` | 5 | $219.40 | $215.98 | -17.10 | — | +0.00 | -17.10 | +17.90 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-25 | `ANRO` | 36 | — | $36.52 | +0.00 | $36.31 | -7.56 | -7.56 | +0.00 | -7.56 |
| 2026-08-25 | `SUJA` | 149 | — | $8.79 | +0.00 | $9.33 | +80.46 | +80.46 | +0.00 | +80.46 |
| 2026-08-25 | `WIX` | 15 | — | $83.15 | +0.00 | $86.06 | +43.58 | +43.58 | +0.00 | +43.58 |
| 2026-08-25 | `ALVO` | 250 | — | $5.24 | +0.00 | $5.05 | -47.50 | -47.50 | +0.00 | -47.50 |
| 2026-08-25 | `B` | 27 | — | $47.52 | +0.00 | $48.87 | +36.45 | +36.45 | +0.00 | +36.45 |
| 2026-08-25 | `BMNR` | 55 | — | $23.80 | +0.00 | $24.82 | +56.10 | +56.10 | +0.00 | +56.10 |
| 2026-08-25 | `BRZE` | 42 | — | $30.69 | +0.00 | $30.80 | +4.62 | +4.62 | +0.00 | +4.62 |
| 2026-08-25 | `CELH` | 37 | — | $35.23 | +0.00 | $35.34 | +4.07 | +4.07 | +0.00 | +4.07 |
| 2026-08-26 | `ANRO` | 36 | $36.31 | $35.80 | -18.36 | — | +0.00 | -18.36 | -25.92 | — |
| 2026-08-26 | `SUJA` | 149 | $9.33 | $9.39 | +8.94 | $9.44 | +7.45 | +16.39 | +89.40 | +96.85 |
| 2026-08-26 | `WIX` | 15 | $86.06 | $84.02 | -30.53 | — | +0.00 | -30.53 | +13.05 | — |
| 2026-08-26 | `ALVO` | 250 | $5.05 | $4.98 | -17.50 | — | +0.00 | -17.50 | -65.00 | — |
| 2026-08-26 | `B` | 27 | $48.87 | $48.18 | -18.63 | $47.00 | -31.86 | -50.49 | +17.82 | -14.04 |
| 2026-08-26 | `BMNR` | 55 | $24.82 | $24.24 | -31.90 | — | +0.00 | -31.90 | +24.20 | — |
| 2026-08-26 | `BRZE` | 42 | $30.80 | $29.95 | -35.70 | — | +0.00 | -35.70 | -31.08 | — |
| 2026-08-26 | `CELH` | 37 | $35.34 | $35.25 | -3.33 | — | +0.00 | -3.33 | +0.74 | — |
| 2026-08-26 | `MNRO` | 93 | — | $14.00 | +0.00 | $12.61 | -129.27 | -129.27 | +0.00 | -129.27 |
| 2026-08-26 | `VIR` | 122 | — | $10.60 | +0.00 | $11.08 | +58.56 | +58.56 | +0.00 | +58.56 |
| 2026-08-26 | `NEM` | 9 | — | $132.64 | +0.00 | $131.60 | -9.36 | -9.36 | +0.00 | -9.36 |
| 2026-08-26 | `AQST` | 256 | — | $5.08 | +0.00 | $5.39 | +79.36 | +79.36 | +0.00 | +79.36 |
| 2026-08-26 | `TRLV` | 116 | — | $11.22 | +0.00 | $11.43 | +24.36 | +24.36 | +0.00 | +24.36 |
| 2026-08-26 | `BRR` | 591 | — | $2.20 | +0.00 | $2.17 | -17.73 | -17.73 | +0.00 | -17.73 |
| 2026-08-27 | `SUJA` | 149 | $9.44 | $9.41 | -4.47 | — | +0.00 | -4.47 | +92.38 | — |
| 2026-08-27 | `B` | 27 | $47.00 | $47.07 | +1.89 | — | +0.00 | +1.89 | -12.15 | — |
| 2026-08-27 | `MNRO` | 93 | $12.61 | $12.56 | -4.65 | — | +0.00 | -4.65 | -133.92 | — |
| 2026-08-27 | `VIR` | 122 | $11.08 | $11.00 | -9.76 | — | +0.00 | -9.76 | +48.80 | — |
| 2026-08-27 | `NEM` | 9 | $131.60 | $131.02 | -5.22 | — | +0.00 | -5.22 | -14.58 | — |
| 2026-08-27 | `AQST` | 256 | $5.39 | $5.39 | +0.00 | — | +0.00 | +0.00 | +79.36 | — |
| 2026-08-27 | `TRLV` | 116 | $11.43 | $11.38 | -5.80 | — | +0.00 | -5.80 | +18.56 | — |
| 2026-08-27 | `BRR` | 591 | $2.17 | $2.19 | +11.82 | — | +0.00 | +11.82 | -5.91 | — |
| 2026-08-27 | `GEN` | 43 | — | $29.83 | +0.00 | $30.50 | +28.81 | +28.81 | +0.00 | +28.81 |
| 2026-08-27 | `RRC` | 31 | — | $41.44 | +0.00 | $41.64 | +6.20 | +6.20 | +0.00 | +6.20 |
| 2026-08-27 | `DLO` | 85 | — | $15.33 | +0.00 | $15.14 | -16.15 | -16.15 | +0.00 | -16.15 |
| 2026-08-27 | `ANET` | 6 | — | $205.90 | +0.00 | $201.09 | -28.86 | -28.86 | +0.00 | -28.86 |
| 2026-08-27 | `PGY` | 56 | — | $22.93 | +0.00 | $23.26 | +18.48 | +18.48 | +0.00 | +18.48 |
| 2026-08-27 | `SLI` | 501 | — | $2.60 | +0.00 | $2.64 | +20.04 | +20.04 | +0.00 | +20.04 |
| 2026-08-27 | `PLTR` | 7 | — | $178.75 | +0.00 | $185.93 | +50.26 | +50.26 | +0.00 | +50.26 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-28 | `GEN` | 43 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +28.81 | — |
| 2026-08-28 | `RRC` | 31 | $41.64 | $41.74 | +3.10 | $41.46 | -8.68 | -5.58 | +9.30 | +0.62 |
| 2026-08-28 | `DLO` | 85 | $15.14 | $15.19 | +4.25 | — | +0.00 | +4.25 | -11.90 | — |
| 2026-08-28 | `ANET` | 6 | $201.09 | $200.00 | -6.54 | — | +0.00 | -6.54 | -35.40 | — |
| 2026-08-28 | `PGY` | 56 | $23.26 | $23.21 | -2.80 | — | +0.00 | -2.80 | +15.68 | — |
| 2026-08-28 | `SLI` | 501 | $2.64 | $2.68 | +20.04 | — | +0.00 | +20.04 | +40.08 | — |
| 2026-08-28 | `PLTR` | 7 | $185.93 | $184.95 | -6.86 | — | +0.00 | -6.86 | +43.40 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `EL` | 12 | — | $106.99 | +0.00 | $103.39 | -43.20 | -43.20 | +0.00 | -43.20 |
| 2026-08-28 | `FIG` | 43 | — | $30.18 | +0.00 | $28.82 | -58.48 | -58.48 | +0.00 | -58.48 |
| 2026-08-28 | `ULTA` | 2 | — | $542.00 | +0.00 | $517.50 | -49.00 | -49.00 | +0.00 | -49.00 |
| 2026-08-28 | `BRZE` | 38 | — | $34.06 | +0.00 | $34.53 | +17.86 | +17.86 | +0.00 | +17.86 |
| 2026-08-28 | `CXM` | 165 | — | $7.88 | +0.00 | $8.16 | +46.20 | +46.20 | +0.00 | +46.20 |
| 2026-08-28 | `NEO` | 71 | — | $18.36 | +0.00 | $18.05 | -22.01 | -22.01 | +0.00 | -22.01 |
| 2026-08-28 | `PATH` | 72 | — | $18.12 | +0.00 | $18.15 | +1.80 | +1.80 | +0.00 | +1.80 |
| 2026-08-31 | `RRC` | 31 | $41.46 | $42.00 | +16.74 | — | +0.00 | +16.74 | +17.36 | — |
| 2026-08-31 | `EL` | 12 | $103.39 | $102.70 | -8.28 | — | +0.00 | -8.28 | -51.48 | — |
| 2026-08-31 | `FIG` | 43 | $28.82 | $27.60 | -52.46 | — | +0.00 | -52.46 | -110.94 | — |
| 2026-08-31 | `ULTA` | 2 | $517.50 | $521.10 | +7.20 | — | +0.00 | +7.20 | -41.80 | — |
| 2026-08-31 | `BRZE` | 38 | $34.53 | $34.03 | -19.00 | — | +0.00 | -19.00 | -1.14 | — |
| 2026-08-31 | `CXM` | 165 | $8.16 | $8.17 | +1.65 | — | +0.00 | +1.65 | +47.85 | — |
| 2026-08-31 | `NEO` | 71 | $18.05 | $17.77 | -19.88 | — | +0.00 | -19.88 | -41.89 | — |
| 2026-08-31 | `PATH` | 72 | $18.15 | $18.09 | -4.32 | — | +0.00 | -4.32 | -2.52 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `CTVA` | 14 | — | $90.24 | +0.00 | $88.62 | -22.68 | -22.68 | +0.00 | -22.68 |
| 2026-09-03 | `RSKD` | 191 | — | $6.68 | +0.00 | $6.93 | +47.75 | +47.75 | +0.00 | +47.75 |
| 2026-09-03 | `AGCO` | 9 | — | $127.91 | +0.00 | $125.82 | -18.81 | -18.81 | +0.00 | -18.81 |
| 2026-09-03 | `ASST` | 49 | — | $25.62 | +0.00 | $26.82 | +58.56 | +58.56 | +0.00 | +58.56 |
| 2026-09-03 | `FRNM` | 80 | — | $15.87 | +0.00 | $16.90 | +82.40 | +82.40 | +0.00 | +82.40 |
| 2026-09-03 | `SION` | 174 | — | $7.31 | +0.00 | $6.75 | -97.44 | -97.44 | +0.00 | -97.44 |
| 2026-09-03 | `ARCT` | 76 | — | $16.77 | +0.00 | $15.56 | -91.96 | -91.96 | +0.00 | -91.96 |
| 2026-09-03 | `PYXS` | 343 | — | $3.71 | +0.00 | $3.56 | -49.74 | -49.74 | +0.00 | -49.74 |
| 2026-09-04 | `CTVA` | 14 | $88.62 | $87.64 | -13.72 | — | +0.00 | -13.72 | -36.40 | — |
| 2026-09-04 | `RSKD` | 191 | $6.93 | $6.84 | -17.19 | — | +0.00 | -17.19 | +30.56 | — |
| 2026-09-04 | `AGCO` | 9 | $125.82 | $125.22 | -5.40 | — | +0.00 | -5.40 | -24.21 | — |
| 2026-09-04 | `ASST` | 49 | $26.82 | $25.18 | -80.36 | $27.14 | +96.04 | +15.68 | -21.81 | +74.24 |
| 2026-09-04 | `FRNM` | 80 | $16.90 | $16.40 | -40.00 | $16.31 | -7.20 | -47.20 | +42.40 | +35.20 |
| 2026-09-04 | `SION` | 174 | $6.75 | $6.68 | -12.18 | — | +0.00 | -12.18 | -109.62 | — |
| 2026-09-04 | `ARCT` | 76 | $15.56 | $15.61 | +3.80 | — | +0.00 | +3.80 | -88.16 | — |
| 2026-09-04 | `PYXS` | 343 | $3.56 | $3.53 | -12.01 | — | +0.00 | -12.01 | -61.74 | — |
| 2026-09-04 | `DFDV` | 211 | — | $5.79 | +0.00 | $5.87 | +16.88 | +16.88 | +0.00 | +16.88 |
| 2026-09-04 | `PAGS` | 123 | — | $9.96 | +0.00 | $9.73 | -28.29 | -28.29 | +0.00 | -28.29 |
| 2026-09-04 | `TTD` | 80 | — | $15.18 | +0.00 | $14.43 | -60.00 | -60.00 | +0.00 | -60.00 |
| 2026-09-04 | `TARS` | 14 | — | $82.70 | +0.00 | $90.78 | +113.12 | +113.12 | +0.00 | +113.12 |
| 2026-09-04 | `TDS` | 32 | — | $37.44 | +0.00 | $37.83 | +12.48 | +12.48 | +0.00 | +12.48 |
| 2026-09-04 | `ZETA` | 37 | — | $32.65 | +0.00 | $31.35 | -48.10 | -48.10 | +0.00 | -48.10 |
| 2026-09-08 | `ASST` | 49 | $27.14 | $26.44 | -34.30 | — | +0.00 | -34.30 | +39.94 | — |
| 2026-09-08 | `FRNM` | 80 | $16.31 | $16.74 | +34.40 | — | +0.00 | +34.40 | +69.60 | — |
| 2026-09-08 | `DFDV` | 211 | $5.87 | $5.81 | -12.66 | — | +0.00 | -12.66 | +4.22 | — |
| 2026-09-08 | `PAGS` | 123 | $9.73 | $9.91 | +22.14 | — | +0.00 | +22.14 | -6.15 | — |
| 2026-09-08 | `TTD` | 80 | $14.43 | $14.32 | -8.80 | — | +0.00 | -8.80 | -68.80 | — |
| 2026-09-08 | `TARS` | 14 | $90.78 | $89.67 | -15.54 | — | +0.00 | -15.54 | +97.58 | — |
| 2026-09-08 | `TDS` | 32 | $37.83 | $37.75 | -2.56 | — | +0.00 | -2.56 | +9.92 | — |
| 2026-09-08 | `ZETA` | 37 | $31.35 | $31.08 | -9.99 | — | +0.00 | -9.99 | -58.09 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `CYPH` | 519 | — | $2.39 | +0.00 | $2.27 | -64.88 | -64.88 | +0.00 | -64.88 |
| 2026-09-11 | `IOND` | 15 | — | $80.00 | +0.00 | $83.39 | +50.85 | +50.85 | +0.00 | +50.85 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +300.75 | TNDM, TPG, HIMS, IREN, INO, VOR, BTSG, SLS | — | $107.38 | $10,268.71 | TNDM×53, TPG×24, HIMS×42, IREN×27, INO×1543, VOR×56, BTSG×20, SLS×106 |
| 2026-08-14 | +5.50 | $107.38 | TNDM×53, TPG×24, HIMS×42, IREN×27, INO×1543, VOR×56, BTSG×20, SLS×106 | $10,312.70 | +43.99 | -94.00 | ZS, BETA, SATL, BRZE, MH, NMAX, GLOB, LUNR | TNDM, TPG, HIMS, IREN, INO, VOR, BTSG, SLS | $224.74 | $10,166.44 | ZS×6, BETA×50, SATL×214, BRZE×42, MH×94, NMAX×129, GLOB×33, LUNR×67 |
| 2026-08-17 | +2.25 | $224.74 | ZS×6, BETA×50, SATL×214, BRZE×42, MH×94, NMAX×129, GLOB×33, LUNR×67 | $10,259.17 | +92.73 | -160.67 | NPWR, JBIO, HTFL, SMJF, STDN, CLYM, BORR | ZS, BETA, SATL, BRZE, MH, GLOB, LUNR | $38.01 | $10,059.68 | NMAX×129, NPWR×656, JBIO×51, HTFL×30, SMJF×124, STDN×92, CLYM×77, BORR×274 |
| 2026-08-18 | -6.20 | $38.01 | NMAX×129, NPWR×656, JBIO×51, HTFL×30, SMJF×124, STDN×92, CLYM×77, BORR×274 | $9,975.83 | -83.85 | +0.00 | — | NMAX, NPWR, JBIO, HTFL, SMJF, STDN, CLYM, BORR | $9,950.06 | $9,950.06 | — |
| 2026-08-19 | -7.20 | $9,950.06 | — | $9,950.06 | +0.00 | +0.00 | — | — | $9,950.06 | $9,950.06 | — |
| 2026-08-20 | +1.12 | $9,950.06 | — | $9,950.06 | +0.00 | -175.52 | IOND, NBP, IMMX, ABCL, MRNA, ABUS, CYPH, GENB | — | $91.98 | $9,738.35 | IOND×18, NBP×631, IMMX×95, ABCL×105, MRNA×8, ABUS×252, CYPH×1081, GENB×74 |
| 2026-08-21 | +3.25 | $91.98 | IOND×18, NBP×631, IMMX×95, ABCL×105, MRNA×8, ABUS×252, CYPH×1081, GENB×74 | $10,006.22 | +267.87 | +250.98 | SM, IOVA, ARIS, ARCT, DXYZ, ILMN, AEM | IOND, NBP, IMMX, ABCL, MRNA, ABUS, GENB | $325.06 | $10,219.61 | CYPH×1081, SM×32, IOVA×134, ARIS×58, ARCT×109, DXYZ×35, ILMN×5, AEM×5 |
| 2026-08-24 | -5.17 | $325.06 | CYPH×1081, SM×32, IOVA×134, ARIS×58, ARCT×109, DXYZ×35, ILMN×5, AEM×5 | $10,550.89 | +331.27 | +0.00 | — | CYPH, SM, IOVA, ARIS, ARCT, DXYZ, ILMN, AEM | $10,521.52 | $10,521.52 | — |
| 2026-08-25 | +1.80 | $10,521.52 | — | $10,521.52 | -0.00 | +170.22 | ANRO, SUJA, WIX, ALVO, B, BMNR, BRZE, CELH | — | $137.07 | $10,673.50 | ANRO×36, SUJA×149, WIX×15, ALVO×250, B×27, BMNR×55, BRZE×42, CELH×37 |
| 2026-08-26 | +2.02 | $137.07 | ANRO×36, SUJA×149, WIX×15, ALVO×250, B×27, BMNR×55, BRZE×42, CELH×37 | $10,526.49 | -147.01 | -18.49 | MNRO, VIR, NEM, AQST, TRLV, BRR | ANRO, WIX, ALVO, BMNR, BRZE, CELH | $101.57 | $10,474.21 | SUJA×149, B×27, MNRO×93, VIR×122, NEM×9, AQST×256, TRLV×116, BRR×591 |
| 2026-08-27 | — | $101.57 | SUJA×149, B×27, MNRO×93, VIR×122, NEM×9, AQST×256, TRLV×116, BRR×591 | $10,458.02 | -16.19 | +47.16 | GEN, RRC, DLO, ANET, PGY, SLI, PLTR, MU | SUJA, B, MNRO, VIR, NEM, AQST, TRLV, BRR | $501.48 | $10,459.36 | GEN×43, RRC×31, DLO×85, ANET×6, PGY×56, SLI×501, PLTR×7, MU×1 |
| 2026-08-28 | +0.75 | $501.48 | GEN×43, RRC×31, DLO×85, ANET×6, PGY×56, SLI×501, PLTR×7, MU×1 | $10,454.45 | -4.91 | -115.51 | EL, FIG, ULTA, BRZE, CXM, NEO, PATH | GEN, DLO, ANET, PGY, SLI, PLTR, MU | $257.50 | $10,304.59 | RRC×31, EL×12, FIG×43, ULTA×2, BRZE×38, CXM×165, NEO×71, PATH×72 |
| 2026-08-31 | -5.85 | $257.50 | RRC×31, EL×12, FIG×43, ULTA×2, BRZE×38, CXM×165, NEO×71, PATH×72 | $10,226.24 | -78.35 | +0.00 | — | RRC, EL, FIG, ULTA, BRZE, CXM, NEO, PATH | $10,208.83 | $10,208.83 | — |
| 2026-09-01 | -6.30 | $10,208.83 | — | $10,208.83 | +0.00 | +0.00 | — | — | $10,208.83 | $10,208.83 | — |
| 2026-09-02 | -3.83 | $10,208.83 | — | $10,208.83 | +0.00 | +0.00 | — | — | $10,208.83 | $10,208.83 | — |
| 2026-09-03 | -0.90 | $10,208.83 | — | $10,208.83 | +0.00 | -91.92 | CTVA, RSKD, AGCO, ASST, FRNM, SION, ARCT, PYXS | — | $154.05 | $10,096.78 | CTVA×14, RSKD×191, AGCO×9, ASST×49, FRNM×80, SION×174, ARCT×76, PYXS×343 |
| 2026-09-04 | +2.25 | $154.05 | CTVA×14, RSKD×191, AGCO×9, ASST×49, FRNM×80, SION×174, ARCT×76, PYXS×343 | $9,919.72 | -177.06 | +94.93 | DFDV, PAGS, TTD, TARS, TDS, ZETA | CTVA, RSKD, AGCO, SION, ARCT, PYXS | $119.30 | $9,985.15 | ASST×49, FRNM×80, DFDV×211, PAGS×123, TTD×80, TARS×14, TDS×32, ZETA×37 |
| 2026-09-08 | -11.47 | $119.30 | ASST×49, FRNM×80, DFDV×211, PAGS×123, TTD×80, TARS×14, TDS×32, ZETA×37 | $9,957.84 | -27.31 | +0.00 | — | ASST, FRNM, DFDV, PAGS, TTD, TARS, TDS, ZETA | $9,939.74 | $9,939.74 | — |
| 2026-09-09 | -13.95 | $9,939.74 | — | $9,939.74 | -0.00 | +0.00 | — | — | $9,939.74 | $9,939.74 | — |
| 2026-09-10 | -13.28 | $9,939.74 | — | $9,939.74 | -0.00 | +0.00 | — | — | $9,939.74 | $9,939.74 | — |
| 2026-09-11 | +0.50 | $9,939.74 | — | $9,939.74 | -0.00 | -14.03 | CYPH, IOND | — | $7,490.60 | $9,916.98 | CYPH×519, IOND×15 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $8,761.36 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $7,544.34 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $6,293.15 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $5,049.62 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $3,782.66 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $2,547.94 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $1,349.89 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $107.38 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.38 | ▲ close $10,268.71 vs 09:30 $10,000.00 (session +300.75) | 16:00 close · cash $107.38 · equity $10,268.71 vs 09:30 $10,000.00 (+268.71; session marks +300.75) · 8 name(s) marked open→close (per-name table). TNDM×53 09:30 $23.33 → close $23.13 -10.60; TPG×24 09:30 $50.62 → close $54.62 +95.92; HIMS×42 09:30 $29.74 → close $28.77 -40.74; IREN×27 09:30 $45.98 → close $44.76 -32.94; INO×1543 09:30 $0.81 → close $0.90 +138.87; VOR×56 09:30 $22.01 → close $23.29 +71.68; BTSG×20 09:30 $59.80 → close $60.23 +8.60; SLS×106 09:30 $11.70 → close $12.36 +69.96 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.38 | ▲ 09:30 equity $10,312.70 vs yday $10,268.71 (+43.99) | 09:30 open · cash $107.38 (unchanged overnight, no fees) · equity $10,312.70 vs prior close $10,268.71 (+43.99) · 8 name(s) re-marked at the open (per-name table). TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; SLS×106 yday $12.36 → 09:30 $12.40 +4.24 | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $1,319.97 | ▼ -26.05 after sell → book $10,310.53; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $2,644.85 | ▲ +107.86 after sell → book $10,308.45; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $3,867.01 | ▼ -29.03 after sell → book $10,306.31; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $5,055.35 | ▼ -55.19 after sell → book $10,304.22; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $6,471.10 | ▲ +148.79 after sell → book $10,284.98; vs 09:30 mark -19.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $7,775.40 | ▲ +69.58 after sell → book $10,282.80; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $8,966.33 | ▼ -7.12 after sell → book $10,280.73; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $10,278.39 | ▲ +69.56 after sell → book $10,278.39; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `ZS` | 6 | $190.00 | $2.01 | — | $9,136.38 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+15.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETA` | 50 | $25.21 | $2.14 | — | $7,873.74 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SATL` | 214 | $5.98 | $2.76 | — | $6,591.26 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+16.9; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BRZE` | 42 | $30.00 | $2.12 | — | $5,329.15 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+16.2; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 94 | $13.55 | $2.27 | — | $4,053.18 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 129 | $9.89 | $2.38 | — | $2,774.34 | — | rank by candle_score; rank candle_score; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `GLOB` | 33 | $38.21 | $2.09 | — | $1,511.32 | — | rank by candle_score; rank candle_score; list earn_react; 🔵; ⚪; ret5=+10.0; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 67 | $19.17 | $2.19 | — | $224.74 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🔴 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $224.74 | ▼ close $10,166.44 vs 09:30 $10,312.70 (session -94.00) | 16:00 close · cash $224.74 · equity $10,166.44 vs 09:30 $10,312.70 (-146.26; session marks -94.00) · 8 name(s) marked open→close (per-name table). ZS×6 09:30 $190.00 → close $183.60 -38.40; BETA×50 09:30 $25.21 → close $24.86 -17.50; SATL×214 09:30 $5.98 → close $5.80 -38.52; BRZE×42 09:30 $30.00 → close $28.93 -44.94; MH×94 09:30 $13.55 → close $13.10 -42.30; NMAX×129 09:30 $9.89 → close $10.87 +125.77; GLOB×33 09:30 $38.21 → close $37.38 -27.39; LUNR×67 09:30 $19.17 → close $19.01 -10.72 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $224.74 | ▲ 09:30 equity $10,259.17 vs yday $10,166.44 (+92.73) | 09:30 open · cash $224.74 (unchanged overnight, no fees) · equity $10,259.17 vs prior close $10,166.44 (+92.73) · 8 name(s) re-marked at the open (per-name table). ZS×6 yday $183.60 → 09:30 $188.38 +28.65; BETA×50 yday $24.86 → 09:30 $24.61 -12.50; SATL×214 yday $5.80 → 09:30 $5.81 +2.14; BRZE×42 yday $28.93 → 09:30 $28.44 -20.58; MH×94 yday $13.10 → 09:30 $13.16 +5.64; NMAX×129 yday $10.87 → 09:30 $10.97 +12.90; GLOB×33 yday $37.38 → 09:30 $37.18 -6.60; LUNR×67 yday $19.01 → 09:30 $20.25 +83.08 | — |
| 2026-08-17 09:30 ET | **SELL** | `ZS` | 6 | $188.38 | $2.03 | $-13.79 | $1,352.97 | ▼ -13.79 after sell → book $10,257.15; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETA` | 50 | $24.61 | $2.16 | $-34.30 | $2,581.31 | ▼ -34.30 after sell → book $10,254.99; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SATL` | 214 | $5.81 | $2.81 | $-41.95 | $3,821.84 | ▼ -41.95 after sell → book $10,252.18; vs 09:30 mark -2.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BRZE` | 42 | $28.44 | $2.14 | $-69.77 | $5,014.18 | ▼ -69.77 after sell → book $10,250.04; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 94 | $13.16 | $2.30 | $-41.23 | $6,248.93 | ▼ -41.23 after sell → book $10,247.75; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `GLOB` | 33 | $37.18 | $2.11 | $-38.19 | $7,473.76 | ▼ -38.19 after sell → book $10,245.64; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 67 | $20.25 | $2.21 | $+67.96 | $8,828.29 | ▲ +67.96 after sell → book $10,243.42; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 656 | $1.92 | $8.46 | — | $7,560.31 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1261.18 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `JBIO` | 51 | $24.60 | $2.14 | — | $6,303.57 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+12.5; leftover $1261.18 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 30 | $41.23 | $2.08 | — | $5,064.59 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+46.0; leftover $1261.18 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 124 | $10.10 | $2.36 | — | $3,809.83 | — | rank by candle_score; rank candle_score; list mover_buy; ret5=+22.8; leftover $1261.18 | join🔴 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 92 | $13.64 | $2.27 | — | $2,552.68 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1261.18 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CLYM` | 77 | $16.25 | $2.22 | — | $1,299.21 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; ret5=+16.6; leftover $1261.18 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 274 | $4.59 | $3.53 | — | $38.01 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $1261.18 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.01 | ▼ close $10,059.68 vs 09:30 $10,259.17 (session -160.67) | 16:00 close · cash $38.01 · equity $10,059.68 vs 09:30 $10,259.17 (-199.49; session marks -160.67) · 8 name(s) marked open→close (per-name table). NMAX×129 09:30 $10.97 → close $10.36 -78.69; NPWR×656 09:30 $1.92 → close $1.73 -124.64; JBIO×51 09:30 $24.60 → close $23.45 -58.65; HTFL×30 09:30 $41.23 → close $41.94 +21.30; SMJF×124 09:30 $10.10 → close $10.45 +43.40; STDN×92 09:30 $13.64 → close $13.31 -30.36; CLYM×77 09:30 $16.25 → close $17.44 +91.63; BORR×274 09:30 $4.59 → close $4.50 -24.66 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.01 | ▼ 09:30 equity $9,975.83 vs yday $10,059.68 (-83.85) | 09:30 open · cash $38.01 (unchanged overnight, no fees) · equity $9,975.83 vs prior close $10,059.68 (-83.85) · 8 name(s) re-marked at the open (per-name table). NMAX×129 yday $10.36 → 09:30 $10.31 -6.45; NPWR×656 yday $1.73 → 09:30 $1.70 -19.68; JBIO×51 yday $23.45 → 09:30 $23.07 -19.38; HTFL×30 yday $41.94 → 09:30 $41.50 -13.20; SMJF×124 yday $10.45 → 09:30 $10.45 +0.00; STDN×92 yday $13.31 → 09:30 $13.31 +0.00; CLYM×77 yday $17.44 → 09:30 $16.90 -41.58; BORR×274 yday $4.50 → 09:30 $4.56 +16.44 | — |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 129 | $10.31 | $2.41 | $+48.75 | $1,365.60 | ▲ +48.75 after sell → book $9,973.43; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 656 | $1.70 | $8.58 | $-161.36 | $2,472.21 | ▼ -161.36 after sell → book $9,964.84; vs 09:30 mark -8.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `JBIO` | 51 | $23.07 | $2.16 | $-82.34 | $3,646.62 | ▼ -82.34 after sell → book $9,962.68; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 30 | $41.50 | $2.10 | $+3.92 | $4,889.52 | ▲ +3.92 after sell → book $9,960.58; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `SMJF` | 124 | $10.45 | $2.39 | $+38.64 | $6,182.93 | ▲ +38.64 after sell → book $9,958.19; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 92 | $13.31 | $2.29 | $-34.92 | $7,405.16 | ▼ -34.92 after sell → book $9,955.90; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `CLYM` | 77 | $16.90 | $2.24 | $+45.58 | $8,704.21 | ▲ +45.58 after sell → book $9,953.65; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 274 | $4.56 | $3.59 | $-15.34 | $9,950.06 | ▼ -15.34 after sell → book $9,950.06; vs 09:30 mark -3.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,950.06 | ▲ close $9,950.06 vs 09:30 $9,975.83 (session +0.00) | 16:00 close · cash $9,950.06 · no lots left · equity $9,950.06. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,950.06 | ▲ 09:30 equity $9,950.06 vs yday $9,950.06 (+0.00) | 09:30 open · cash $9,950.06 · no holdings · equity $9,950.06 vs prior close $9,950.06 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,950.06 | ▲ close $9,950.06 vs 09:30 $9,950.06 (session +0.00) | 16:00 close · cash $9,950.06 · no lots left · equity $9,950.06. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,950.06 | ▲ 09:30 equity $9,950.06 vs yday $9,950.06 (+0.00) | 09:30 open · cash $9,950.06 · no holdings · equity $9,950.06 vs prior close $9,950.06 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `IOND` | 18 | $65.60 | $2.04 | — | $8,767.22 | — | rank by candle_score; rank candle_score; list earn_react; 🔵; ⚪; ret5=+3.7; leftover $1243.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NBP` | 631 | $1.97 | $8.14 | — | $7,516.01 | — | rank by candle_score; rank candle_score; list earn_react; 🔵; ret5=+5.9; leftover $1243.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IMMX` | 95 | $12.98 | $2.27 | — | $6,280.63 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1243.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 105 | $11.81 | $2.31 | — | $5,037.75 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1243.76 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $3,834.62 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1243.76 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 252 | $4.92 | $3.25 | — | $2,591.53 | — | rank by candle_score; rank candle_score; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1243.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1081 | $1.15 | $13.94 | — | $1,334.43 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1243.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `GENB` | 74 | $16.76 | $2.21 | — | $91.98 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+12.5; leftover $1243.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $91.98 | ▼ close $9,738.35 vs 09:30 $9,950.06 (session -175.52) | 16:00 close · cash $91.98 · equity $9,738.35 vs 09:30 $9,950.06 (-211.71; session marks -175.52) · 8 name(s) marked open→close (per-name table). IOND×18 09:30 $65.60 → close $68.77 +57.06; NBP×631 09:30 $1.97 → close $1.91 -37.86; IMMX×95 09:30 $12.98 → close $13.16 +17.10; ABCL×105 09:30 $11.81 → close $11.57 -25.72; MRNA×8 09:30 $150.14 → close $133.32 -134.56; ABUS×252 09:30 $4.92 → close $4.77 -37.80; CYPH×1081 09:30 $1.15 → close $1.19 +43.24; GENB×74 09:30 $16.76 → close $15.99 -56.98 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $91.98 | ▲ 09:30 equity $10,006.22 vs yday $9,738.35 (+267.87) | 09:30 open · cash $91.98 (unchanged overnight, no fees) · equity $10,006.22 vs prior close $9,738.35 (+267.87) · 8 name(s) re-marked at the open (per-name table). IOND×18 yday $68.77 → 09:30 $68.41 -6.48; NBP×631 yday $1.91 → 09:30 $1.91 +0.00; IMMX×95 yday $13.16 → 09:30 $13.36 +19.00; ABCL×105 yday $11.57 → 09:30 $11.57 +0.00; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; ABUS×252 yday $4.77 → 09:30 $5.20 +108.36; CYPH×1081 yday $1.19 → 09:30 $1.32 +140.53; GENB×74 yday $15.99 → 09:30 $16.10 +8.14 | — |
| 2026-08-21 09:30 ET | **SELL** | `IOND` | 18 | $68.41 | $2.06 | $+46.47 | $1,321.30 | ▲ +46.47 after sell → book $10,004.16; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NBP` | 631 | $1.91 | $8.25 | $-54.25 | $2,518.25 | ▼ -54.25 after sell → book $9,995.90; vs 09:30 mark -8.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IMMX` | 95 | $13.36 | $2.30 | $+31.52 | $3,785.15 | ▲ +31.52 after sell → book $9,993.60; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 105 | $11.57 | $2.33 | $-30.36 | $4,997.67 | ▼ -30.36 after sell → book $9,991.27; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $6,060.52 | ▼ -140.29 after sell → book $9,989.24; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 252 | $5.20 | $3.30 | $+64.01 | $7,367.61 | ▲ +64.01 after sell → book $9,985.93; vs 09:30 mark -3.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `GENB` | 74 | $16.10 | $2.23 | $-53.29 | $8,556.78 | ▼ -53.29 after sell → book $9,983.70; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `SM` | 32 | $37.81 | $2.09 | — | $7,344.77 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+16.1; leftover $1222.40 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 134 | $9.08 | $2.39 | — | $6,125.66 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1222.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARIS` | 58 | $20.90 | $2.16 | — | $4,911.30 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1222.40 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 109 | $11.13 | $2.32 | — | $3,695.81 | — | rank by candle_score; rank candle_score; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1222.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DXYZ` | 35 | $34.89 | $2.10 | — | $2,472.57 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+8.6; leftover $1222.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ILMN` | 5 | $212.40 | $2.00 | — | $1,408.56 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ⚪; ret5=+10.7; leftover $1222.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $325.06 | — | rank by candle_score; rank candle_score; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1222.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $325.06 | ▲ close $10,219.61 vs 09:30 $10,006.22 (session +250.98) | 16:00 close · cash $325.06 · equity $10,219.61 vs 09:30 $10,006.22 (+213.40; session marks +250.98) · 8 name(s) marked open→close (per-name table). CYPH×1081 09:30 $1.32 → close $1.42 +108.10; SM×32 09:30 $37.81 → close $37.20 -19.52; IOVA×134 09:30 $9.08 → close $8.29 -105.86; ARIS×58 09:30 $20.90 → close $20.86 -2.32; ARCT×109 09:30 $11.13 → close $13.45 +252.88; DXYZ×35 09:30 $34.89 → close $34.43 -16.10; ILMN×5 09:30 $212.40 → close $219.40 +35.00; AEM×5 09:30 $216.30 → close $216.06 -1.20 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $325.06 | ▲ 09:30 equity $10,550.89 vs yday $10,219.61 (+331.27) | 09:30 open · cash $325.06 (unchanged overnight, no fees) · equity $10,550.89 vs prior close $10,219.61 (+331.27) · 8 name(s) re-marked at the open (per-name table). CYPH×1081 yday $1.42 → 09:30 $1.83 +443.21; SM×32 yday $37.20 → 09:30 $36.61 -18.88; IOVA×134 yday $8.29 → 09:30 $8.08 -28.14; ARIS×58 yday $20.86 → 09:30 $20.98 +6.96; ARCT×109 yday $13.45 → 09:30 $13.33 -13.08; DXYZ×35 yday $34.43 → 09:30 $33.10 -46.55; ILMN×5 yday $219.40 → 09:30 $215.98 -17.10; AEM×5 yday $216.06 → 09:30 $217.03 +4.85 | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1081 | $1.83 | $14.14 | $+706.99 | $2,289.14 | ▲ +706.99 after sell → book $10,536.74; vs 09:30 mark -14.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `SM` | 32 | $36.61 | $2.11 | $-42.59 | $3,458.56 | ▼ -42.59 after sell → book $10,534.64; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 134 | $8.08 | $2.42 | $-138.82 | $4,538.85 | ▼ -138.82 after sell → book $10,532.21; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARIS` | 58 | $20.98 | $2.18 | $+0.29 | $5,753.51 | ▲ +0.29 after sell → book $10,530.03; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 109 | $13.33 | $2.35 | $+235.14 | $7,204.13 | ▲ +235.14 after sell → book $10,527.68; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DXYZ` | 35 | $33.10 | $2.12 | $-66.86 | $8,360.52 | ▼ -66.86 after sell → book $10,525.57; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ILMN` | 5 | $215.98 | $2.02 | $+13.87 | $9,438.39 | ▲ +13.87 after sell → book $10,523.54; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $10,521.52 | ▼ -0.38 after sell → book $10,521.52; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,521.52 | ▲ close $10,521.52 vs 09:30 $10,550.89 (session +0.00) | 16:00 close · cash $10,521.52 · no lots left · equity $10,521.52. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,521.52 | ▲ 09:30 equity $10,521.52 vs yday $10,521.52 (-0.00) | 09:30 open · cash $10,521.52 · no holdings · equity $10,521.52 vs prior close $10,521.52 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `ANRO` | 36 | $36.52 | $2.10 | — | $9,204.70 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+7.9; leftover $1315.19 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 149 | $8.79 | $2.44 | — | $7,892.55 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1315.19 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `WIX` | 15 | $83.15 | $2.04 | — | $6,643.27 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+14.5; leftover $1315.19 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 250 | $5.24 | $3.23 | — | $5,330.04 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1315.19 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `B` | 27 | $47.52 | $2.07 | — | $4,044.93 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+13.3; leftover $1315.19 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 55 | $23.80 | $2.15 | — | $2,733.78 | — | rank by candle_score; rank candle_score; list yday_gainer; ret5=+28.9; leftover $1315.19 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BRZE` | 42 | $30.69 | $2.12 | — | $1,442.68 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+12.5; leftover $1315.19 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CELH` | 37 | $35.23 | $2.10 | — | $137.07 | — | rank by candle_score; rank candle_score; list ohlc_hot; ⚪; ret5=+17.0; leftover $1315.19 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $137.07 | ▲ close $10,673.50 vs 09:30 $10,521.52 (session +170.22) | 16:00 close · cash $137.07 · equity $10,673.50 vs 09:30 $10,521.52 (+151.98; session marks +170.22) · 8 name(s) marked open→close (per-name table). ANRO×36 09:30 $36.52 → close $36.31 -7.56; SUJA×149 09:30 $8.79 → close $9.33 +80.46; WIX×15 09:30 $83.15 → close $86.06 +43.58; ALVO×250 09:30 $5.24 → close $5.05 -47.50; B×27 09:30 $47.52 → close $48.87 +36.45; BMNR×55 09:30 $23.80 → close $24.82 +56.10; BRZE×42 09:30 $30.69 → close $30.80 +4.62; CELH×37 09:30 $35.23 → close $35.34 +4.07 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $137.07 | ▼ 09:30 equity $10,526.49 vs yday $10,673.50 (-147.01) | 09:30 open · cash $137.07 (unchanged overnight, no fees) · equity $10,526.49 vs prior close $10,673.50 (-147.01) · 8 name(s) re-marked at the open (per-name table). ANRO×36 yday $36.31 → 09:30 $35.80 -18.36; SUJA×149 yday $9.33 → 09:30 $9.39 +8.94; WIX×15 yday $86.06 → 09:30 $84.02 -30.53; ALVO×250 yday $5.05 → 09:30 $4.98 -17.50; B×27 yday $48.87 → 09:30 $48.18 -18.63; BMNR×55 yday $24.82 → 09:30 $24.24 -31.90; BRZE×42 yday $30.80 → 09:30 $29.95 -35.70; CELH×37 yday $35.34 → 09:30 $35.25 -3.33 | — |
| 2026-08-26 09:30 ET | **SELL** | `ANRO` | 36 | $35.80 | $2.12 | $-30.14 | $1,423.75 | ▼ -30.14 after sell → book $10,524.37; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WIX` | 15 | $84.02 | $2.06 | $+8.96 | $2,682.00 | ▲ +8.96 after sell → book $10,522.32; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 250 | $4.98 | $3.28 | $-71.50 | $3,923.72 | ▼ -71.50 after sell → book $10,519.04; vs 09:30 mark -3.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMNR` | 55 | $24.24 | $2.18 | $+19.87 | $5,254.75 | ▲ +19.87 after sell → book $10,516.87; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BRZE` | 42 | $29.95 | $2.14 | $-35.33 | $6,510.51 | ▼ -35.33 after sell → book $10,514.73; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CELH` | 37 | $35.25 | $2.12 | $-3.48 | $7,812.64 | ▼ -3.48 after sell → book $10,512.61; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 93 | $14.00 | $2.27 | — | $6,508.37 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+17.8; leftover $1302.11 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `VIR` | 122 | $10.60 | $2.36 | — | $5,212.81 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+12.9; leftover $1302.11 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NEM` | 9 | $132.64 | $2.02 | — | $4,017.04 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+16.5; leftover $1302.11 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AQST` | 256 | $5.08 | $3.30 | — | $2,713.25 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+17.6; leftover $1302.11 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 116 | $11.22 | $2.34 | — | $1,409.40 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+16.8; leftover $1302.11 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BRR` | 591 | $2.20 | $7.62 | — | $101.57 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+17.8; leftover $1302.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $101.57 | ▼ close $10,474.21 vs 09:30 $10,526.49 (session -18.49) | 16:00 close · cash $101.57 · equity $10,474.21 vs 09:30 $10,526.49 (-52.28; session marks -18.49) · 8 name(s) marked open→close (per-name table). SUJA×149 09:30 $9.39 → close $9.44 +7.45; B×27 09:30 $48.18 → close $47.00 -31.86; MNRO×93 09:30 $14.00 → close $12.61 -129.27; VIR×122 09:30 $10.60 → close $11.08 +58.56; NEM×9 09:30 $132.64 → close $131.60 -9.36; AQST×256 09:30 $5.08 → close $5.39 +79.36; TRLV×116 09:30 $11.22 → close $11.43 +24.36; BRR×591 09:30 $2.20 → close $2.17 -17.73 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $101.57 | ▼ 09:30 equity $10,458.02 vs yday $10,474.21 (-16.19) | 09:30 open · cash $101.57 (unchanged overnight, no fees) · equity $10,458.02 vs prior close $10,474.21 (-16.19) · 8 name(s) re-marked at the open (per-name table). SUJA×149 yday $9.44 → 09:30 $9.41 -4.47; B×27 yday $47.00 → 09:30 $47.07 +1.89; MNRO×93 yday $12.61 → 09:30 $12.56 -4.65; VIR×122 yday $11.08 → 09:30 $11.00 -9.76; NEM×9 yday $131.60 → 09:30 $131.02 -5.22; AQST×256 yday $5.39 → 09:30 $5.39 +0.00; TRLV×116 yday $11.43 → 09:30 $11.38 -5.80; BRR×591 yday $2.17 → 09:30 $2.19 +11.82 | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 149 | $9.41 | $2.47 | $+87.47 | $1,501.19 | ▲ +87.47 after sell → book $10,455.55; vs 09:30 mark -2.47 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `B` | 27 | $47.07 | $2.09 | $-16.31 | $2,769.99 | ▼ -16.31 after sell → book $10,453.46; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MNRO` | 93 | $12.56 | $2.29 | $-138.48 | $3,935.77 | ▼ -138.48 after sell → book $10,451.16; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `VIR` | 122 | $11.00 | $2.39 | $+44.06 | $5,275.39 | ▲ +44.06 after sell → book $10,448.78; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NEM` | 9 | $131.02 | $2.04 | $-18.63 | $6,452.53 | ▼ -18.63 after sell → book $10,446.74; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AQST` | 256 | $5.39 | $3.36 | $+72.70 | $7,829.01 | ▲ +72.70 after sell → book $10,443.38; vs 09:30 mark -3.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 116 | $11.38 | $2.37 | $+13.85 | $9,146.73 | ▲ +13.85 after sell → book $10,441.02; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BRR` | 591 | $2.19 | $7.73 | $-21.27 | $10,433.28 | ▼ -21.27 after sell → book $10,433.28; vs 09:30 mark -7.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 43 | $29.83 | $2.12 | — | $9,148.47 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=+7.6; leftover $1304.16 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 31 | $41.44 | $2.08 | — | $7,861.75 | — | rank by candle_score; rank candle_score; list flatten; ret5=+3.1; leftover $1304.16 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 85 | $15.33 | $2.25 | — | $6,556.46 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=+7.4; leftover $1304.16 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $205.90 | $2.01 | — | $5,319.05 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=+8.5; leftover $1304.16 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 56 | $22.93 | $2.16 | — | $4,032.81 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=+9.5; leftover $1304.16 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 501 | $2.60 | $6.46 | — | $2,723.75 | — | rank by candle_score; rank candle_score; list flatten; ret5=+13.0; leftover $1304.16 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 7 | $178.75 | $2.01 | — | $1,470.49 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=+1.3; leftover $1304.16 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $501.48 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=+0.1; leftover $1304.16 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $501.48 | ▲ close $10,459.36 vs 09:30 $10,458.02 (session +47.16) | 16:00 close · cash $501.48 · equity $10,459.36 vs 09:30 $10,458.02 (+1.34; session marks +47.16) · 8 name(s) marked open→close (per-name table). GEN×43 09:30 $29.83 → close $30.50 +28.81; RRC×31 09:30 $41.44 → close $41.64 +6.20; DLO×85 09:30 $15.33 → close $15.14 -16.15; ANET×6 09:30 $205.90 → close $201.09 -28.86; PGY×56 09:30 $22.93 → close $23.26 +18.48; SLI×501 09:30 $2.60 → close $2.64 +20.04; PLTR×7 09:30 $178.75 → close $185.93 +50.26; MU×1 09:30 $967.01 → close $935.39 -31.62 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $501.48 | ▼ 09:30 equity $10,454.45 vs yday $10,459.36 (-4.91) | 09:30 open · cash $501.48 (unchanged overnight, no fees) · equity $10,454.45 vs prior close $10,459.36 (-4.91) · 8 name(s) re-marked at the open (per-name table). GEN×43 yday $30.50 → 09:30 $30.50 +0.00; RRC×31 yday $41.64 → 09:30 $41.74 +3.10; DLO×85 yday $15.14 → 09:30 $15.19 +4.25; ANET×6 yday $201.09 → 09:30 $200.00 -6.54; PGY×56 yday $23.26 → 09:30 $23.21 -2.80; SLI×501 yday $2.64 → 09:30 $2.68 +20.04; PLTR×7 yday $185.93 → 09:30 $184.95 -6.86; MU×1 yday $935.39 → 09:30 $919.29 -16.10 | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 43 | $30.50 | $2.14 | $+24.55 | $1,810.84 | ▲ +24.55 after sell → book $10,452.31; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 85 | $15.19 | $2.27 | $-16.41 | $3,099.72 | ▼ -16.41 after sell → book $10,450.04; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $200.00 | $2.03 | $-39.44 | $4,297.70 | ▼ -39.44 after sell → book $10,448.02; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 56 | $23.21 | $2.18 | $+11.34 | $5,595.28 | ▲ +11.34 after sell → book $10,445.84; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 501 | $2.68 | $6.56 | $+27.06 | $6,931.40 | ▲ +27.06 after sell → book $10,439.28; vs 09:30 mark -6.56 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PLTR` | 7 | $184.95 | $2.03 | $+39.36 | $8,224.02 | ▲ +39.36 after sell → book $10,437.25; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $9,141.30 | ▼ -51.73 after sell → book $10,435.24; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `EL` | 12 | $106.99 | $2.03 | — | $7,855.39 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+10.5; leftover $1305.90 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FIG` | 43 | $30.18 | $2.12 | — | $6,555.53 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+12.1; leftover $1305.90 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 2 | $542.00 | $2.00 | — | $5,469.54 | — | rank by candle_score; rank candle_score; list earn_react; ret5=+4.8; leftover $1305.90 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BRZE` | 38 | $34.06 | $2.10 | — | $4,173.15 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+11.0; leftover $1305.90 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CXM` | 165 | $7.88 | $2.48 | — | $2,870.47 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+10.3; leftover $1305.90 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 71 | $18.36 | $2.20 | — | $1,564.70 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+12.8; leftover $1305.90 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PATH` | 72 | $18.12 | $2.21 | — | $257.50 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+15.1; leftover $1305.90 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $257.50 | ▼ close $10,304.59 vs 09:30 $10,454.45 (session -115.51) | 16:00 close · cash $257.50 · equity $10,304.59 vs 09:30 $10,454.45 (-149.86; session marks -115.51) · 8 name(s) marked open→close (per-name table). RRC×31 09:30 $41.74 → close $41.46 -8.68; EL×12 09:30 $106.99 → close $103.39 -43.20; FIG×43 09:30 $30.18 → close $28.82 -58.48; ULTA×2 09:30 $542.00 → close $517.50 -49.00; BRZE×38 09:30 $34.06 → close $34.53 +17.86; CXM×165 09:30 $7.88 → close $8.16 +46.20; NEO×71 09:30 $18.36 → close $18.05 -22.01; PATH×72 09:30 $18.12 → close $18.15 +1.80 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $257.50 | ▼ 09:30 equity $10,226.24 vs yday $10,304.59 (-78.35) | 09:30 open · cash $257.50 (unchanged overnight, no fees) · equity $10,226.24 vs prior close $10,304.59 (-78.35) · 8 name(s) re-marked at the open (per-name table). RRC×31 yday $41.46 → 09:30 $42.00 +16.74; EL×12 yday $103.39 → 09:30 $102.70 -8.28; FIG×43 yday $28.82 → 09:30 $27.60 -52.46; ULTA×2 yday $517.50 → 09:30 $521.10 +7.20; BRZE×38 yday $34.53 → 09:30 $34.03 -19.00; CXM×165 yday $8.16 → 09:30 $8.17 +1.65; NEO×71 yday $18.05 → 09:30 $17.77 -19.88; PATH×72 yday $18.15 → 09:30 $18.09 -4.32 | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 31 | $42.00 | $2.10 | $+13.17 | $1,557.39 | ▲ +13.17 after sell → book $10,224.13; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `EL` | 12 | $102.70 | $2.05 | $-55.55 | $2,787.75 | ▼ -55.55 after sell → book $10,222.09; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FIG` | 43 | $27.60 | $2.14 | $-115.20 | $3,972.41 | ▼ -115.20 after sell → book $10,219.95; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ULTA` | 2 | $521.10 | $2.02 | $-45.81 | $5,012.59 | ▼ -45.81 after sell → book $10,217.93; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BRZE` | 38 | $34.03 | $2.12 | $-5.37 | $6,303.61 | ▼ -5.37 after sell → book $10,215.81; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `CXM` | 165 | $8.17 | $2.52 | $+42.84 | $7,649.14 | ▲ +42.84 after sell → book $10,213.29; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `NEO` | 71 | $17.77 | $2.22 | $-46.32 | $8,908.58 | ▼ -46.32 after sell → book $10,211.06; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PATH` | 72 | $18.09 | $2.23 | $-6.95 | $10,208.83 | ▼ -6.95 after sell → book $10,208.83; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,208.83 | ▲ close $10,208.83 vs 09:30 $10,226.24 (session +0.00) | 16:00 close · cash $10,208.83 · no lots left · equity $10,208.83. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,208.83 | ▲ 09:30 equity $10,208.83 vs yday $10,208.83 (+0.00) | 09:30 open · cash $10,208.83 · no holdings · equity $10,208.83 vs prior close $10,208.83 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,208.83 | ▲ close $10,208.83 vs 09:30 $10,208.83 (session +0.00) | 16:00 close · cash $10,208.83 · no lots left · equity $10,208.83. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,208.83 | ▲ 09:30 equity $10,208.83 vs yday $10,208.83 (+0.00) | 09:30 open · cash $10,208.83 · no holdings · equity $10,208.83 vs prior close $10,208.83 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,208.83 | ▲ close $10,208.83 vs 09:30 $10,208.83 (session +0.00) | 16:00 close · cash $10,208.83 · no lots left · equity $10,208.83. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,208.83 | ▲ 09:30 equity $10,208.83 vs yday $10,208.83 (+0.00) | 09:30 open · cash $10,208.83 · no holdings · equity $10,208.83 vs prior close $10,208.83 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `CTVA` | 14 | $90.24 | $2.03 | — | $8,943.44 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+8.6; leftover $1276.10 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RSKD` | 191 | $6.68 | $2.56 | — | $7,665.00 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+11.4; leftover $1276.10 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 9 | $127.91 | $2.02 | — | $6,511.79 | — | rank by candle_score; rank candle_score; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1276.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ASST` | 49 | $25.62 | $2.14 | — | $5,254.03 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+13.1; leftover $1276.10 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 80 | $15.87 | $2.23 | — | $3,982.20 | — | rank by candle_score; rank candle_score; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1276.10 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 174 | $7.31 | $2.51 | — | $2,707.75 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+18.5; leftover $1276.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 76 | $16.77 | $2.22 | — | $1,431.01 | — | rank by candle_score; rank candle_score; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1276.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PYXS` | 343 | $3.71 | $4.42 | — | $154.05 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+12.3; leftover $1276.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $154.05 | ▼ close $10,096.78 vs 09:30 $10,208.83 (session -91.92) | 16:00 close · cash $154.05 · equity $10,096.78 vs 09:30 $10,208.83 (-112.05; session marks -91.92) · 8 name(s) marked open→close (per-name table). CTVA×14 09:30 $90.24 → close $88.62 -22.68; RSKD×191 09:30 $6.68 → close $6.93 +47.75; AGCO×9 09:30 $127.91 → close $125.82 -18.81; ASST×49 09:30 $25.62 → close $26.82 +58.56; FRNM×80 09:30 $15.87 → close $16.90 +82.40; SION×174 09:30 $7.31 → close $6.75 -97.44; ARCT×76 09:30 $16.77 → close $15.56 -91.96; PYXS×343 09:30 $3.71 → close $3.56 -49.74 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.05 | ▼ 09:30 equity $9,919.72 vs yday $10,096.78 (-177.06) | 09:30 open · cash $154.05 (unchanged overnight, no fees) · equity $9,919.72 vs prior close $10,096.78 (-177.06) · 8 name(s) re-marked at the open (per-name table). CTVA×14 yday $88.62 → 09:30 $87.64 -13.72; RSKD×191 yday $6.93 → 09:30 $6.84 -17.19; AGCO×9 yday $125.82 → 09:30 $125.22 -5.40; ASST×49 yday $26.82 → 09:30 $25.18 -80.36; FRNM×80 yday $16.90 → 09:30 $16.40 -40.00; SION×174 yday $6.75 → 09:30 $6.68 -12.18; ARCT×76 yday $15.56 → 09:30 $15.61 +3.80; PYXS×343 yday $3.56 → 09:30 $3.53 -12.01 | — |
| 2026-09-04 09:30 ET | **SELL** | `CTVA` | 14 | $87.64 | $2.05 | $-40.48 | $1,378.96 | ▼ -40.48 after sell → book $9,917.67; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RSKD` | 191 | $6.84 | $2.61 | $+25.39 | $2,682.80 | ▲ +25.39 after sell → book $9,915.07; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `AGCO` | 9 | $125.22 | $2.04 | $-28.26 | $3,807.74 | ▼ -28.26 after sell → book $9,913.03; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SION` | 174 | $6.68 | $2.55 | $-114.68 | $4,967.51 | ▼ -114.68 after sell → book $9,910.48; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 76 | $15.61 | $2.24 | $-92.62 | $6,151.63 | ▼ -92.62 after sell → book $9,908.24; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PYXS` | 343 | $3.53 | $4.49 | $-70.66 | $7,357.93 | ▼ -70.66 after sell → book $9,903.75; vs 09:30 mark -4.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 211 | $5.79 | $2.72 | — | $6,133.51 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1226.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `PAGS` | 123 | $9.96 | $2.36 | — | $4,906.08 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+11.5; leftover $1226.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TTD` | 80 | $15.18 | $2.23 | — | $3,689.45 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+12.4; leftover $1226.32 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 14 | $82.70 | $2.03 | — | $2,529.61 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1226.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TDS` | 32 | $37.44 | $2.09 | — | $1,329.45 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+14.1; leftover $1226.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ZETA` | 37 | $32.65 | $2.10 | — | $119.30 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+8.1; leftover $1226.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $119.30 | ▲ close $9,985.15 vs 09:30 $9,919.72 (session +94.93) | 16:00 close · cash $119.30 · equity $9,985.15 vs 09:30 $9,919.72 (+65.43; session marks +94.93) · 8 name(s) marked open→close (per-name table). ASST×49 09:30 $25.18 → close $27.14 +96.04; FRNM×80 09:30 $16.40 → close $16.31 -7.20; DFDV×211 09:30 $5.79 → close $5.87 +16.88; PAGS×123 09:30 $9.96 → close $9.73 -28.29; TTD×80 09:30 $15.18 → close $14.43 -60.00; TARS×14 09:30 $82.70 → close $90.78 +113.12; TDS×32 09:30 $37.44 → close $37.83 +12.48; ZETA×37 09:30 $32.65 → close $31.35 -48.10 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $119.30 | ▼ 09:30 equity $9,957.84 vs yday $9,985.15 (-27.31) | 09:30 open · cash $119.30 (unchanged overnight, no fees) · equity $9,957.84 vs prior close $9,985.15 (-27.31) · 8 name(s) re-marked at the open (per-name table). ASST×49 yday $27.14 → 09:30 $26.44 -34.30; FRNM×80 yday $16.31 → 09:30 $16.74 +34.40; DFDV×211 yday $5.87 → 09:30 $5.81 -12.66; PAGS×123 yday $9.73 → 09:30 $9.91 +22.14; TTD×80 yday $14.43 → 09:30 $14.32 -8.80; TARS×14 yday $90.78 → 09:30 $89.67 -15.54; TDS×32 yday $37.83 → 09:30 $37.75 -2.56; ZETA×37 yday $31.35 → 09:30 $31.08 -9.99 | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 49 | $26.44 | $2.16 | $+35.64 | $1,412.70 | ▲ +35.64 after sell → book $9,955.68; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 80 | $16.74 | $2.25 | $+65.12 | $2,749.64 | ▲ +65.12 after sell → book $9,953.42; vs 09:30 mark -2.26 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 211 | $5.81 | $2.77 | $-1.27 | $3,972.79 | ▼ -1.27 after sell → book $9,950.66; vs 09:30 mark -2.76 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `PAGS` | 123 | $9.91 | $2.39 | $-10.90 | $5,189.33 | ▼ -10.90 after sell → book $9,948.27; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TTD` | 80 | $14.32 | $2.25 | $-73.28 | $6,332.68 | ▼ -73.28 after sell → book $9,946.02; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 14 | $89.67 | $2.05 | $+93.50 | $7,586.00 | ▲ +93.50 after sell → book $9,943.96; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TDS` | 32 | $37.75 | $2.11 | $+5.73 | $8,791.90 | ▲ +5.73 after sell → book $9,941.86; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ZETA` | 37 | $31.08 | $2.12 | $-62.31 | $9,939.74 | ▼ -62.31 after sell → book $9,939.74; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,939.74 | ▲ close $9,939.74 vs 09:30 $9,957.84 (session +0.00) | 16:00 close · cash $9,939.74 · no lots left · equity $9,939.74. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,939.74 | ▲ 09:30 equity $9,939.74 vs yday $9,939.74 (-0.00) | 09:30 open · cash $9,939.74 · no holdings · equity $9,939.74 vs prior close $9,939.74 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,939.74 | ▲ close $9,939.74 vs 09:30 $9,939.74 (session +0.00) | 16:00 close · cash $9,939.74 · no lots left · equity $9,939.74. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,939.74 | ▲ 09:30 equity $9,939.74 vs yday $9,939.74 (-0.00) | 09:30 open · cash $9,939.74 · no holdings · equity $9,939.74 vs prior close $9,939.74 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,939.74 | ▲ close $9,939.74 vs 09:30 $9,939.74 (session +0.00) | 16:00 close · cash $9,939.74 · no lots left · equity $9,939.74. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,939.74 | ▲ 09:30 equity $9,939.74 vs yday $9,939.74 (-0.00) | 09:30 open · cash $9,939.74 · no holdings · equity $9,939.74 vs prior close $9,939.74 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `CYPH` | 519 | $2.39 | $6.70 | — | $8,692.63 | — | rank by candle_score; rank candle_score; list yday_mover; 🔵; ret5=+44.1; leftover $1242.47 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `IOND` | 15 | $80.00 | $2.04 | — | $7,490.60 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+14.1; leftover $1242.47 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,490.60 | ▼ close $9,916.98 vs 09:30 $9,939.74 (session -14.03) | 16:00 close · cash $7,490.60 · equity $9,916.98 vs 09:30 $9,939.74 (-22.76; session marks -14.03) · 2 name(s) marked open→close (per-name table). CYPH×519 09:30 $2.39 → close $2.27 -64.88; IOND×15 09:30 $80.00 → close $83.39 +50.85 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ADCT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CERS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KYTX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OVID` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KYMR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `MTDR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PSKY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RDZN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBNX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMTX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ZYME` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `GWRE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TEAM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `APPN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `YEXT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HUBS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SLB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KVYO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `GTLB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ZETA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `XRX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `LAND` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SID` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ACB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZETA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `HAFN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `USDE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TWI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LAND` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LPG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GLW` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SWKS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SKHY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SSL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `INTC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IOND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `PAYP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SWKS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `UROY` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SKHY` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `GLW` | no_price | no 09:30 open |
| 2026-09-11 | `INTC` | no_price | no 09:30 open |
| 2026-09-11 | `CMRC` | no_price | no 09:30 open |
| 2026-09-11 | `PAYP` | no_price | no 09:30 open |
| 2026-09-11 | `QRVO` | no_price | no 09:30 open |
| 2026-09-11 | `SWKS` | no_price | no 09:30 open |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CYPH` | 519 | 2026-09-11 @ $2.39 | rank by candle_score; rank candle_score; list yday_mover; 🔵; ret5=+44.1; leftover $1242.47 |
| `IOND` | 15 | 2026-09-11 @ $80.00 | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+14.1; leftover $1242.47 |
