# Factor mine action — `ohlc_hot_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `ohlc_hot` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-8.38%** ($9,163) · signal-only (no cash/fees) was -0.32%. Starts YES **1/18**. Fills 131 · skips 48 · realized $-873.46.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at names that looked hot on the prior price/volume tape and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: names that looked hot on the prior price/volume tape.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).

### When it buys

- At 09:30, take names on names that looked hot on the prior price/volume tape that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
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

- **Universe** `ohlc_hot` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,006.47.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ADUR` | 75 | — | $16.50 | +0.00 | $16.17 | -24.75 | -24.75 | +0.00 | -24.75 |
| 2026-08-14 | `ANRO` | 39 | — | $31.77 | +0.00 | $32.14 | +14.43 | +14.43 | +0.00 | +14.43 |
| 2026-08-14 | `LIFE` | 35 | — | $35.04 | +0.00 | $34.02 | -35.70 | -35.70 | +0.00 | -35.70 |
| 2026-08-14 | `VOYG` | 28 | — | $44.49 | +0.00 | $42.98 | -42.28 | -42.28 | +0.00 | -42.28 |
| 2026-08-14 | `LUNR` | 65 | — | $19.17 | +0.00 | $19.01 | -10.40 | -10.40 | +0.00 | -10.40 |
| 2026-08-14 | `BETA` | 49 | — | $25.21 | +0.00 | $24.86 | -17.15 | -17.15 | +0.00 | -17.15 |
| 2026-08-14 | `FORM` | 9 | — | $129.48 | +0.00 | $131.60 | +19.08 | +19.08 | +0.00 | +19.08 |
| 2026-08-14 | `ENTG` | 7 | — | $162.45 | +0.00 | $161.76 | -4.83 | -4.83 | +0.00 | -4.83 |
| 2026-08-17 | `ADUR` | 75 | $16.17 | $15.73 | -33.00 | — | +0.00 | -33.00 | -57.75 | — |
| 2026-08-17 | `ANRO` | 39 | $32.14 | $32.15 | +0.39 | — | +0.00 | +0.39 | +14.82 | — |
| 2026-08-17 | `LIFE` | 35 | $34.02 | $34.03 | +0.35 | — | +0.00 | +0.35 | -35.35 | — |
| 2026-08-17 | `VOYG` | 28 | $42.98 | $42.12 | -24.08 | — | +0.00 | -24.08 | -66.36 | — |
| 2026-08-17 | `LUNR` | 65 | $19.01 | $20.25 | +80.60 | $20.38 | +8.45 | +89.05 | +70.20 | +78.65 |
| 2026-08-17 | `BETA` | 49 | $24.86 | $24.61 | -12.25 | — | +0.00 | -12.25 | -29.40 | — |
| 2026-08-17 | `FORM` | 9 | $131.60 | $134.05 | +22.05 | — | +0.00 | +22.05 | +41.13 | — |
| 2026-08-17 | `ENTG` | 7 | $161.76 | $162.04 | +1.96 | — | +0.00 | +1.96 | -2.87 | — |
| 2026-08-17 | `OCC` | 67 | — | $18.24 | +0.00 | $17.12 | -75.04 | -75.04 | +0.00 | -75.04 |
| 2026-08-17 | `ALM` | 75 | — | $16.20 | +0.00 | $16.36 | +12.00 | +12.00 | +0.00 | +12.00 |
| 2026-08-17 | `LPTH` | 82 | — | $14.94 | +0.00 | $14.80 | -11.48 | -11.48 | +0.00 | -11.48 |
| 2026-08-17 | `AAOI` | 8 | — | $152.64 | +0.00 | $154.89 | +18.00 | +18.00 | +0.00 | +18.00 |
| 2026-08-17 | `CLYM` | 75 | — | $16.25 | +0.00 | $17.44 | +89.25 | +89.25 | +0.00 | +89.25 |
| 2026-08-17 | `BORR` | 267 | — | $4.59 | +0.00 | $4.50 | -24.03 | -24.03 | +0.00 | -24.03 |
| 2026-08-17 | `IOVA` | 179 | — | $6.84 | +0.00 | $7.10 | +46.54 | +46.54 | +0.00 | +46.54 |
| 2026-08-18 | `LUNR` | 65 | $20.38 | $19.31 | -69.55 | — | +0.00 | -69.55 | +9.10 | — |
| 2026-08-18 | `OCC` | 67 | $17.12 | $16.20 | -61.64 | — | +0.00 | -61.64 | -136.68 | — |
| 2026-08-18 | `ALM` | 75 | $16.36 | $15.78 | -43.50 | — | +0.00 | -43.50 | -31.50 | — |
| 2026-08-18 | `LPTH` | 82 | $14.80 | $14.01 | -64.78 | — | +0.00 | -64.78 | -76.26 | — |
| 2026-08-18 | `AAOI` | 8 | $154.89 | $146.20 | -69.52 | $131.41 | -118.32 | -187.84 | -51.52 | -169.84 |
| 2026-08-18 | `CLYM` | 75 | $17.44 | $16.90 | -40.50 | — | +0.00 | -40.50 | +48.75 | — |
| 2026-08-18 | `BORR` | 267 | $4.50 | $4.56 | +16.02 | — | +0.00 | +16.02 | -8.01 | — |
| 2026-08-18 | `IOVA` | 179 | $7.10 | $7.00 | -17.90 | $7.03 | +5.37 | -12.53 | +28.64 | +34.01 |
| 2026-08-19 | `AAOI` | 8 | $131.41 | $135.85 | +35.52 | — | +0.00 | +35.52 | -134.32 | — |
| 2026-08-19 | `IOVA` | 179 | $7.03 | $7.20 | +30.43 | — | +0.00 | +30.43 | +64.44 | — |
| 2026-08-20 | `AEM` | 5 | — | $204.45 | +0.00 | $212.04 | +37.95 | +37.95 | +0.00 | +37.95 |
| 2026-08-20 | `TWST` | 8 | — | $136.84 | +0.00 | $136.33 | -4.08 | -4.08 | +0.00 | -4.08 |
| 2026-08-20 | `ABTC` | 140 | — | $8.46 | +0.00 | $8.47 | +1.40 | +1.40 | +0.00 | +1.40 |
| 2026-08-20 | `HL` | 58 | — | $20.25 | +0.00 | $20.82 | +33.06 | +33.06 | +0.00 | +33.06 |
| 2026-08-20 | `SBET` | 157 | — | $7.55 | +0.00 | $7.59 | +6.28 | +6.28 | +0.00 | +6.28 |
| 2026-08-20 | `PPC` | 38 | — | $30.65 | +0.00 | $31.24 | +22.42 | +22.42 | +0.00 | +22.42 |
| 2026-08-20 | `ABCL` | 100 | — | $11.81 | +0.00 | $11.57 | -24.50 | -24.50 | +0.00 | -24.50 |
| 2026-08-20 | `SENS` | 133 | — | $8.91 | +0.00 | $8.82 | -11.97 | -11.97 | +0.00 | -11.97 |
| 2026-08-21 | `AEM` | 5 | $212.04 | $216.30 | +21.30 | $216.06 | -1.20 | +20.10 | +59.25 | +58.05 |
| 2026-08-21 | `TWST` | 8 | $136.33 | $138.43 | +16.80 | — | +0.00 | +16.80 | +12.72 | — |
| 2026-08-21 | `ABTC` | 140 | $8.47 | $8.66 | +26.60 | $7.93 | -102.20 | -75.60 | +28.00 | -74.20 |
| 2026-08-21 | `HL` | 58 | $20.82 | $21.33 | +29.58 | — | +0.00 | +29.58 | +62.64 | — |
| 2026-08-21 | `SBET` | 157 | $7.59 | $7.87 | +43.96 | — | +0.00 | +43.96 | +50.24 | — |
| 2026-08-21 | `PPC` | 38 | $31.24 | $31.13 | -4.18 | — | +0.00 | -4.18 | +18.24 | — |
| 2026-08-21 | `ABCL` | 100 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -24.50 | — |
| 2026-08-21 | `SENS` | 133 | $8.82 | $9.24 | +55.86 | — | +0.00 | +55.86 | +43.89 | — |
| 2026-08-21 | `ORBS` | 1438 | — | $0.86 | +0.00 | $0.88 | +23.01 | +23.01 | +0.00 | +23.01 |
| 2026-08-21 | `GRAL` | 15 | — | $78.88 | +0.00 | $79.54 | +9.90 | +9.90 | +0.00 | +9.90 |
| 2026-08-21 | `MSTR` | 10 | — | $119.69 | +0.00 | $119.25 | -4.40 | -4.40 | +0.00 | -4.40 |
| 2026-08-21 | `TRON` | 640 | — | $1.94 | +0.00 | $2.01 | +44.80 | +44.80 | +0.00 | +44.80 |
| 2026-08-21 | `XHG` | 276 | — | $4.49 | +0.00 | $4.41 | -22.08 | -22.08 | +0.00 | -22.08 |
| 2026-08-21 | `AUGO` | 13 | — | $89.10 | +0.00 | $87.26 | -23.92 | -23.92 | +0.00 | -23.92 |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +62.90 | — |
| 2026-08-24 | `ABTC` | 140 | $7.93 | $8.00 | +9.80 | — | +0.00 | +9.80 | -64.40 | — |
| 2026-08-24 | `ORBS` | 1438 | $0.88 | $0.89 | +14.38 | — | +0.00 | +14.38 | +37.39 | — |
| 2026-08-24 | `GRAL` | 15 | $79.54 | $81.87 | +34.95 | — | +0.00 | +34.95 | +44.85 | — |
| 2026-08-24 | `MSTR` | 10 | $119.25 | $121.84 | +25.90 | — | +0.00 | +25.90 | +21.50 | — |
| 2026-08-24 | `TRON` | 640 | $2.01 | $2.02 | +6.40 | — | +0.00 | +6.40 | +51.20 | — |
| 2026-08-24 | `XHG` | 276 | $4.41 | $4.32 | -24.84 | $4.10 | -60.72 | -85.56 | -46.92 | -107.64 |
| 2026-08-24 | `AUGO` | 13 | $87.26 | $88.60 | +17.42 | — | +0.00 | +17.42 | -6.50 | — |
| 2026-08-25 | `XHG` | 276 | $4.10 | $4.07 | -8.28 | $4.02 | -13.80 | -22.08 | -115.92 | -129.72 |
| 2026-08-25 | `CAPR` | 167 | — | $7.25 | +0.00 | $8.29 | +173.68 | +173.68 | +0.00 | +173.68 |
| 2026-08-25 | `JANX` | 64 | — | $18.72 | +0.00 | $18.68 | -2.56 | -2.56 | +0.00 | -2.56 |
| 2026-08-25 | `RUM` | 128 | — | $9.42 | +0.00 | $10.23 | +103.68 | +103.68 | +0.00 | +103.68 |
| 2026-08-25 | `NIQ` | 63 | — | $19.00 | +0.00 | $19.24 | +15.12 | +15.12 | +0.00 | +15.12 |
| 2026-08-25 | `AVAH` | 89 | — | $13.62 | +0.00 | $13.59 | -3.12 | -3.12 | +0.00 | -3.12 |
| 2026-08-25 | `CELH` | 34 | — | $35.23 | +0.00 | $35.34 | +3.74 | +3.74 | +0.00 | +3.74 |
| 2026-08-25 | `WIX` | 14 | — | $83.15 | +0.00 | $86.06 | +40.67 | +40.67 | +0.00 | +40.67 |
| 2026-08-26 | `XHG` | 276 | $4.02 | $3.81 | -57.96 | $4.06 | +69.00 | +11.04 | -187.68 | -118.68 |
| 2026-08-26 | `CAPR` | 167 | $8.29 | $8.29 | +0.00 | $9.36 | +178.69 | +178.69 | +173.68 | +352.37 |
| 2026-08-26 | `JANX` | 64 | $18.68 | $18.59 | -5.76 | — | +0.00 | -5.76 | -8.32 | — |
| 2026-08-26 | `RUM` | 128 | $10.23 | $10.07 | -20.48 | — | +0.00 | -20.48 | +83.20 | — |
| 2026-08-26 | `NIQ` | 63 | $19.24 | $19.20 | -2.52 | — | +0.00 | -2.52 | +12.60 | — |
| 2026-08-26 | `AVAH` | 89 | $13.59 | $13.65 | +5.34 | — | +0.00 | +5.34 | +2.23 | — |
| 2026-08-26 | `CELH` | 34 | $35.34 | $35.25 | -3.06 | — | +0.00 | -3.06 | +0.68 | — |
| 2026-08-26 | `WIX` | 14 | $86.06 | $84.02 | -28.49 | — | +0.00 | -28.49 | +12.18 | — |
| 2026-08-26 | `KURA` | 90 | — | $13.63 | +0.00 | $13.06 | -51.30 | -51.30 | +0.00 | -51.30 |
| 2026-08-26 | `CNTN` | 535 | — | $2.29 | +0.00 | $2.23 | -32.10 | -32.10 | +0.00 | -32.10 |
| 2026-08-26 | `BYND` | 86 | — | $14.11 | +0.00 | $14.25 | +12.04 | +12.04 | +0.00 | +12.04 |
| 2026-08-26 | `FIGR` | 30 | — | $40.50 | +0.00 | $37.08 | -102.60 | -102.60 | +0.00 | -102.60 |
| 2026-08-26 | `MNRO` | 87 | — | $14.00 | +0.00 | $12.61 | -120.93 | -120.93 | +0.00 | -120.93 |
| 2026-08-26 | `FUTU` | 9 | — | $124.67 | +0.00 | $127.34 | +24.03 | +24.03 | +0.00 | +24.03 |
| 2026-08-27 | `XHG` | 276 | $4.06 | $4.06 | +0.00 | — | +0.00 | +0.00 | -118.68 | — |
| 2026-08-27 | `CAPR` | 167 | $9.36 | $9.19 | -28.39 | — | +0.00 | -28.39 | +323.98 | — |
| 2026-08-27 | `KURA` | 90 | $13.06 | $12.98 | -7.20 | — | +0.00 | -7.20 | -58.50 | — |
| 2026-08-27 | `CNTN` | 535 | $2.23 | $2.21 | -10.70 | — | +0.00 | -10.70 | -42.80 | — |
| 2026-08-27 | `BYND` | 86 | $14.25 | $14.20 | -4.30 | — | +0.00 | -4.30 | +7.74 | — |
| 2026-08-27 | `FIGR` | 30 | $37.08 | $37.42 | +10.20 | — | +0.00 | +10.20 | -92.40 | — |
| 2026-08-27 | `MNRO` | 87 | $12.61 | $12.56 | -4.35 | — | +0.00 | -4.35 | -125.28 | — |
| 2026-08-27 | `FUTU` | 9 | $127.34 | $128.00 | +5.94 | — | +0.00 | +5.94 | +29.97 | — |
| 2026-08-28 | `SLI` | 452 | — | $2.68 | +0.00 | $2.55 | -58.76 | -58.76 | +0.00 | -58.76 |
| 2026-08-28 | `BYND` | 86 | — | $14.00 | +0.00 | $13.86 | -12.04 | -12.04 | +0.00 | -12.04 |
| 2026-08-28 | `MRNA` | 8 | — | $137.19 | +0.00 | $137.99 | +6.40 | +6.40 | +0.00 | +6.40 |
| 2026-08-28 | `SBET` | 140 | — | $8.65 | +0.00 | $8.20 | -63.00 | -63.00 | +0.00 | -63.00 |
| 2026-08-28 | `CRCL` | 13 | — | $92.61 | +0.00 | $87.14 | -71.11 | -71.11 | +0.00 | -71.11 |
| 2026-08-28 | `SNPS` | 2 | — | $461.85 | +0.00 | $442.61 | -38.48 | -38.48 | +0.00 | -38.48 |
| 2026-08-28 | `SRPT` | 56 | — | $21.49 | +0.00 | $20.86 | -35.28 | -35.28 | +0.00 | -35.28 |
| 2026-08-28 | `NEO` | 66 | — | $18.36 | +0.00 | $18.05 | -20.46 | -20.46 | +0.00 | -20.46 |
| 2026-08-31 | `SLI` | 452 | $2.55 | $2.58 | +13.56 | — | +0.00 | +13.56 | -45.20 | — |
| 2026-08-31 | `BYND` | 86 | $13.86 | $13.81 | -4.30 | $13.30 | -43.86 | -48.16 | -16.34 | -60.20 |
| 2026-08-31 | `MRNA` | 8 | $137.99 | $134.10 | -31.12 | $140.34 | +49.92 | +18.80 | -24.72 | +25.20 |
| 2026-08-31 | `SBET` | 140 | $8.20 | $8.24 | +5.60 | — | +0.00 | +5.60 | -57.40 | — |
| 2026-08-31 | `CRCL` | 13 | $87.14 | $87.04 | -1.30 | — | +0.00 | -1.30 | -72.41 | — |
| 2026-08-31 | `SNPS` | 2 | $442.61 | $437.95 | -9.32 | — | +0.00 | -9.32 | -47.80 | — |
| 2026-08-31 | `SRPT` | 56 | $20.86 | $20.56 | -16.80 | — | +0.00 | -16.80 | -52.08 | — |
| 2026-08-31 | `NEO` | 66 | $18.05 | $17.77 | -18.48 | — | +0.00 | -18.48 | -38.94 | — |
| 2026-09-01 | `BYND` | 86 | $13.30 | $13.04 | -22.36 | — | +0.00 | -22.36 | -82.56 | — |
| 2026-09-01 | `MRNA` | 8 | $140.34 | $140.25 | -0.72 | $154.27 | +112.16 | +111.44 | +24.48 | +136.64 |
| 2026-09-02 | `MRNA` | 8 | $154.27 | $151.40 | -22.96 | $150.81 | -4.72 | -27.68 | +113.68 | +108.96 |
| 2026-09-03 | `MRNA` | 8 | $150.81 | $145.94 | -38.92 | — | +0.00 | -38.92 | +70.04 | — |
| 2026-09-03 | `CABA` | 321 | — | $3.63 | +0.00 | $3.48 | -48.15 | -48.15 | +0.00 | -48.15 |
| 2026-09-03 | `VSTM` | 145 | — | $8.03 | +0.00 | $7.98 | -7.25 | -7.25 | +0.00 | -7.25 |
| 2026-09-03 | `ARCT` | 69 | — | $16.77 | +0.00 | $15.56 | -83.49 | -83.49 | +0.00 | -83.49 |
| 2026-09-03 | `SID` | 857 | — | $1.36 | +0.00 | $1.26 | -85.70 | -85.70 | +0.00 | -85.70 |
| 2026-09-03 | `NVAX` | 111 | — | $10.42 | +0.00 | $10.34 | -8.88 | -8.88 | +0.00 | -8.88 |
| 2026-09-03 | `BMEA` | 604 | — | $1.93 | +0.00 | $1.91 | -12.08 | -12.08 | +0.00 | -12.08 |
| 2026-09-03 | `REAX` | 63 | — | $18.40 | +0.00 | $18.40 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `CNH` | 84 | — | $13.71 | +0.00 | $13.84 | +10.92 | +10.92 | +0.00 | +10.92 |
| 2026-09-04 | `CABA` | 321 | $3.48 | $3.46 | -6.42 | — | +0.00 | -6.42 | -54.57 | — |
| 2026-09-04 | `VSTM` | 145 | $7.98 | $7.91 | -10.15 | — | +0.00 | -10.15 | -17.40 | — |
| 2026-09-04 | `ARCT` | 69 | $15.56 | $15.61 | +3.45 | — | +0.00 | +3.45 | -80.04 | — |
| 2026-09-04 | `SID` | 857 | $1.26 | $1.23 | -25.71 | — | +0.00 | -25.71 | -111.41 | — |
| 2026-09-04 | `NVAX` | 111 | $10.34 | $10.50 | +17.76 | — | +0.00 | +17.76 | +8.88 | — |
| 2026-09-04 | `BMEA` | 604 | $1.91 | $1.90 | -6.04 | — | +0.00 | -6.04 | -18.12 | — |
| 2026-09-04 | `REAX` | 63 | $18.40 | $18.15 | -15.75 | — | +0.00 | -15.75 | -15.75 | — |
| 2026-09-04 | `CNH` | 84 | $13.84 | $13.89 | +4.20 | — | +0.00 | +4.20 | +15.12 | — |
| 2026-09-04 | `DELL` | 2 | — | $513.78 | +0.00 | $524.14 | +20.72 | +20.72 | +0.00 | +20.72 |
| 2026-09-04 | `TARS` | 13 | — | $82.70 | +0.00 | $90.78 | +105.04 | +105.04 | +0.00 | +105.04 |
| 2026-09-04 | `ASST` | 44 | — | $25.18 | +0.00 | $27.14 | +86.24 | +86.24 | +0.00 | +86.24 |
| 2026-09-04 | `USDE` | 142 | — | $7.87 | +0.00 | $7.93 | +8.52 | +8.52 | +0.00 | +8.52 |
| 2026-09-04 | `DFDV` | 193 | — | $5.79 | +0.00 | $5.87 | +15.44 | +15.44 | +0.00 | +15.44 |
| 2026-09-04 | `HOOD` | 9 | — | $120.47 | +0.00 | $122.11 | +14.72 | +14.72 | +0.00 | +14.72 |
| 2026-09-04 | `GORO` | 284 | — | $3.95 | +0.00 | $4.15 | +56.80 | +56.80 | +0.00 | +56.80 |
| 2026-09-04 | `RSKD` | 164 | — | $6.84 | +0.00 | $6.51 | -54.12 | -54.12 | +0.00 | -54.12 |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | — | +0.00 | -5.98 | +14.74 | — |
| 2026-09-08 | `TARS` | 13 | $90.78 | $89.67 | -14.43 | — | +0.00 | -14.43 | +90.61 | — |
| 2026-09-08 | `ASST` | 44 | $27.14 | $26.44 | -30.80 | — | +0.00 | -30.80 | +55.44 | — |
| 2026-09-08 | `USDE` | 142 | $7.93 | $7.76 | -24.14 | — | +0.00 | -24.14 | -15.62 | — |
| 2026-09-08 | `DFDV` | 193 | $5.87 | $5.81 | -11.58 | $5.99 | +34.74 | +23.16 | +3.86 | +38.60 |
| 2026-09-08 | `HOOD` | 9 | $122.11 | $125.07 | +26.64 | — | +0.00 | +26.64 | +41.35 | — |
| 2026-09-08 | `GORO` | 284 | $4.15 | $4.13 | -5.68 | — | +0.00 | -5.68 | +51.12 | — |
| 2026-09-08 | `RSKD` | 164 | $6.51 | $6.46 | -8.20 | — | +0.00 | -8.20 | -62.32 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -101.60 | ADUR, ANRO, LIFE, VOYG, LUNR, BETA, FORM, ENTG | — | $250.70 | $9,881.56 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7 |
| 2026-08-17 | +2.25 | $250.70 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7 | $9,917.58 | +36.02 | +63.69 | OCC, ALM, LPTH, AAOI, CLYM, BORR, IOVA | ADUR, ANRO, LIFE, VOYG, BETA, FORM, ENTG | $17.77 | $9,949.63 | LUNR×65, OCC×67, ALM×75, LPTH×82, AAOI×8, CLYM×75, BORR×267, IOVA×179 |
| 2026-08-18 | -6.20 | $17.77 | LUNR×65, OCC×67, ALM×75, LPTH×82, AAOI×8, CLYM×75, BORR×267, IOVA×179 | $9,598.26 | -351.37 | -112.95 | — | LUNR, OCC, ALM, LPTH, CLYM, BORR | $7,161.01 | $9,470.66 | AAOI×8, IOVA×179 |
| 2026-08-19 | -7.20 | $7,161.01 | AAOI×8, IOVA×179 | $9,536.61 | +65.95 | +0.00 | — | AAOI, IOVA | $9,532.01 | $9,532.01 | — |
| 2026-08-20 | +1.12 | $9,532.01 | — | $9,532.01 | -0.00 | +60.56 | AEM, TWST, ABTC, HL, SBET, PPC, ABCL, SENS | — | $321.72 | $9,574.73 | AEM×5, TWST×8, ABTC×140, HL×58, SBET×157, PPC×38, ABCL×100, SENS×133 |
| 2026-08-21 | +3.25 | $321.72 | AEM×5, TWST×8, ABTC×140, HL×58, SBET×157, PPC×38, ABCL×100, SENS×133 | $9,764.65 | +189.92 | -76.09 | ORBS, GRAL, MSTR, TRON, XHG, AUGO | TWST, HL, SBET, PPC, ABCL, SENS | $160.86 | $9,640.34 | AEM×5, ABTC×140, ORBS×1438, GRAL×15, MSTR×10, TRON×640, XHG×276, AUGO×13 |
| 2026-08-24 | -5.17 | $160.86 | AEM×5, ABTC×140, ORBS×1438, GRAL×15, MSTR×10, TRON×640, XHG×276, AUGO×13 | $9,729.20 | +88.86 | -60.72 | — | AEM, ABTC, ORBS, GRAL, MSTR, TRON, AUGO | $8,500.54 | $9,632.14 | XHG×276 |
| 2026-08-25 | +1.80 | $8,500.54 | XHG×276 | $9,623.86 | -8.28 | +317.41 | CAPR, JANX, RUM, NIQ, AVAH, CELH, WIX | — | $98.79 | $9,925.66 | XHG×276, CAPR×167, JANX×64, RUM×128, NIQ×63, AVAH×89, CELH×34, WIX×14 |
| 2026-08-26 | +2.02 | $98.79 | XHG×276, CAPR×167, JANX×64, RUM×128, NIQ×63, AVAH×89, CELH×34, WIX×14 | $9,812.73 | -112.93 | -23.17 | KURA, CNTN, BYND, FIGR, MNRO, FUTU | JANX, RUM, NIQ, AVAH, CELH, WIX | $125.39 | $9,758.55 | XHG×276, CAPR×167, KURA×90, CNTN×535, BYND×86, FIGR×30, MNRO×87, FUTU×9 |
| 2026-08-27 | — | $125.39 | XHG×276, CAPR×167, KURA×90, CNTN×535, BYND×86, FIGR×30, MNRO×87, FUTU×9 | $9,719.75 | -38.80 | +0.00 | — | XHG, CAPR, KURA, CNTN, BYND, FIGR, MNRO, FUTU | $9,695.64 | $9,695.64 | — |
| 2026-08-28 | +0.75 | $9,695.64 | — | $9,695.64 | -0.00 | -292.73 | SLI, BYND, MRNA, SBET, CRCL, SNPS, SRPT, NEO | — | $408.05 | $9,382.03 | SLI×452, BYND×86, MRNA×8, SBET×140, CRCL×13, SNPS×2, SRPT×56, NEO×66 |
| 2026-08-31 | -5.85 | $408.05 | SLI×452, BYND×86, MRNA×8, SBET×140, CRCL×13, SNPS×2, SRPT×56, NEO×66 | $9,319.87 | -62.16 | +6.06 | — | SLI, SBET, CRCL, SNPS, SRPT, NEO | $7,042.60 | $9,309.12 | BYND×86, MRNA×8 |
| 2026-09-01 | -6.30 | $7,042.60 | BYND×86, MRNA×8 | $9,286.04 | -23.08 | +112.16 | — | BYND | $8,161.77 | $9,395.93 | MRNA×8 |
| 2026-09-02 | -3.83 | $8,161.77 | MRNA×8 | $9,372.97 | -22.96 | -4.72 | — | — | $8,161.77 | $9,368.25 | MRNA×8 |
| 2026-09-03 | -0.90 | $8,161.77 | MRNA×8 | $9,329.33 | -38.92 | -234.63 | CABA, VSTM, ARCT, SID, NVAX, BMEA, REAX, CNH | MRNA | $7.53 | $9,058.31 | CABA×321, VSTM×145, ARCT×69, SID×857, NVAX×111, BMEA×604, REAX×63, CNH×84 |
| 2026-09-04 | +2.25 | $7.53 | CABA×321, VSTM×145, ARCT×69, SID×857, NVAX×111, BMEA×604, REAX×63, CNH×84 | $9,019.65 | -38.66 | +253.36 | DELL, TARS, ASST, USDE, DFDV, HOOD, GORO, RSKD | CABA, VSTM, ARCT, SID, NVAX, BMEA, REAX, CNH | $192.12 | $9,218.90 | DELL×2, TARS×13, ASST×44, USDE×142, DFDV×193, HOOD×9, GORO×284, RSKD×164 |
| 2026-09-08 | -11.47 | $192.12 | DELL×2, TARS×13, ASST×44, USDE×142, DFDV×193, HOOD×9, GORO×284, RSKD×164 | $9,144.73 | -74.17 | +34.74 | — | DELL, TARS, ASST, USDE, HOOD, GORO, RSKD | $8,006.47 | $9,162.54 | DFDV×193 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $8,760.28 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANRO` | 39 | $31.77 | $2.11 | — | $7,519.15 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+13.5; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 35 | $35.04 | $2.10 | — | $6,290.65 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VOYG` | 28 | $44.49 | $2.07 | — | $5,042.86 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 65 | $19.17 | $2.19 | — | $3,794.62 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETA` | 49 | $25.21 | $2.14 | — | $2,557.20 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `FORM` | 9 | $129.48 | $2.02 | — | $1,389.86 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+14.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ENTG` | 7 | $162.45 | $2.01 | — | $250.70 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $250.70 | ▼ close $9,881.56 vs 09:30 $10,000.00 (session -101.60) | 16:00 close · cash $250.70 · equity $9,881.56 vs 09:30 $10,000.00 (-118.44; session marks -101.60) · 8 name(s) marked open→close (per-name table). ADUR×75 09:30 $16.50 → close $16.17 -24.75; ANRO×39 09:30 $31.77 → close $32.14 +14.43; LIFE×35 09:30 $35.04 → close $34.02 -35.70; VOYG×28 09:30 $44.49 → close $42.98 -42.28; LUNR×65 09:30 $19.17 → close $19.01 -10.40; BETA×49 09:30 $25.21 → close $24.86 -17.15; FORM×9 09:30 $129.48 → close $131.60 +19.08; ENTG×7 09:30 $162.45 → close $161.76 -4.83 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $250.70 | ▲ 09:30 equity $9,917.58 vs yday $9,881.56 (+36.02) | 09:30 open · cash $250.70 (unchanged overnight, no fees) · equity $9,917.58 vs prior close $9,881.56 (+36.02) · 8 name(s) re-marked at the open (per-name table). ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; ANRO×39 yday $32.14 → 09:30 $32.15 +0.39; LIFE×35 yday $34.02 → 09:30 $34.03 +0.35; VOYG×28 yday $42.98 → 09:30 $42.12 -24.08; LUNR×65 yday $19.01 → 09:30 $20.25 +80.60; BETA×49 yday $24.86 → 09:30 $24.61 -12.25; FORM×9 yday $131.60 → 09:30 $134.05 +22.05; ENTG×7 yday $161.76 → 09:30 $162.04 +1.96 | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $1,428.21 | ▼ -62.20 after sell → book $9,915.34; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANRO` | 39 | $32.15 | $2.13 | $+10.59 | $2,679.93 | ▲ +10.59 after sell → book $9,913.21; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 35 | $34.03 | $2.12 | $-39.56 | $3,868.87 | ▼ -39.56 after sell → book $9,911.10; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VOYG` | 28 | $42.12 | $2.09 | $-70.53 | $5,046.14 | ▼ -70.53 after sell → book $9,909.01; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `BETA` | 49 | $24.61 | $2.16 | $-33.69 | $6,249.87 | ▼ -33.69 after sell → book $9,906.85; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `FORM` | 9 | $134.05 | $2.04 | $+37.08 | $7,454.28 | ▲ +37.08 after sell → book $9,904.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ENTG` | 7 | $162.04 | $2.03 | $-6.91 | $8,586.53 | ▼ -6.91 after sell → book $9,902.78; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 67 | $18.24 | $2.19 | — | $7,362.26 | — | baseline list, no extra gate; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1226.65 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 75 | $16.20 | $2.21 | — | $6,145.04 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1226.65 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 82 | $14.94 | $2.24 | — | $4,917.73 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1226.65 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `AAOI` | 8 | $152.64 | $2.01 | — | $3,694.59 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+10.8; leftover $1226.65 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CLYM` | 75 | $16.25 | $2.21 | — | $2,473.63 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+16.6; leftover $1226.65 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 267 | $4.59 | $3.44 | — | $1,244.66 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $1226.65 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `IOVA` | 179 | $6.84 | $2.53 | — | $17.77 | — | baseline list, no extra gate; list ohlc_hot; ret5=+10.1; leftover $1226.65 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.77 | ▲ close $9,949.63 vs 09:30 $9,917.58 (session +63.69) | 16:00 close · cash $17.77 · equity $9,949.63 vs 09:30 $9,917.58 (+32.05; session marks +63.69) · 8 name(s) marked open→close (per-name table). LUNR×65 09:30 $20.25 → close $20.38 +8.45; OCC×67 09:30 $18.24 → close $17.12 -75.04; ALM×75 09:30 $16.20 → close $16.36 +12.00; LPTH×82 09:30 $14.94 → close $14.80 -11.48; AAOI×8 09:30 $152.64 → close $154.89 +18.00; CLYM×75 09:30 $16.25 → close $17.44 +89.25; BORR×267 09:30 $4.59 → close $4.50 -24.03; IOVA×179 09:30 $6.84 → close $7.10 +46.54 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.77 | ▼ 09:30 equity $9,598.26 vs yday $9,949.63 (-351.37) | 09:30 open · cash $17.77 (unchanged overnight, no fees) · equity $9,598.26 vs prior close $9,949.63 (-351.37) · 8 name(s) re-marked at the open (per-name table). LUNR×65 yday $20.38 → 09:30 $19.31 -69.55; OCC×67 yday $17.12 → 09:30 $16.20 -61.64; ALM×75 yday $16.36 → 09:30 $15.78 -43.50; LPTH×82 yday $14.80 → 09:30 $14.01 -64.78; AAOI×8 yday $154.89 → 09:30 $146.20 -69.52; CLYM×75 yday $17.44 → 09:30 $16.90 -40.50; BORR×267 yday $4.50 → 09:30 $4.56 +16.02; IOVA×179 yday $7.10 → 09:30 $7.00 -17.90 | — |
| 2026-08-18 09:30 ET | **SELL** | `LUNR` | 65 | $19.31 | $2.21 | $+4.71 | $1,270.71 | ▲ +4.71 after sell → book $9,596.05; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 67 | $16.20 | $2.21 | $-141.08 | $2,353.90 | ▼ -141.08 after sell → book $9,593.84; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 75 | $15.78 | $2.24 | $-35.95 | $3,535.16 | ▼ -35.95 after sell → book $9,591.60; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 82 | $14.01 | $2.26 | $-80.76 | $4,681.72 | ▼ -80.76 after sell → book $9,589.34; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CLYM` | 75 | $16.90 | $2.24 | $+44.30 | $5,946.99 | ▲ +44.30 after sell → book $9,587.11; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 267 | $4.56 | $3.50 | $-14.95 | $7,161.01 | ▼ -14.95 after sell → book $9,583.61; vs 09:30 mark -3.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,161.01 | ▼ close $9,470.66 vs 09:30 $9,598.26 (session -112.95) | 16:00 close · cash $7,161.01 · equity $9,470.66 vs 09:30 $9,598.26 (-127.60; session marks -112.95) · 2 name(s) marked open→close (per-name table). AAOI×8 09:30 $146.20 → close $131.41 -118.32; IOVA×179 09:30 $7.00 → close $7.03 +5.37 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,161.01 | ▲ 09:30 equity $9,536.61 vs yday $9,470.66 (+65.95) | 09:30 open · cash $7,161.01 (unchanged overnight, no fees) · equity $9,536.61 vs prior close $9,470.66 (+65.95) · 2 name(s) re-marked at the open (per-name table). AAOI×8 yday $131.41 → 09:30 $135.85 +35.52; IOVA×179 yday $7.03 → 09:30 $7.20 +30.43 | — |
| 2026-08-19 09:30 ET | **SELL** | `AAOI` | 8 | $135.85 | $2.03 | $-138.37 | $8,245.77 | ▼ -138.37 after sell → book $9,534.57; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 peer🔴 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `IOVA` | 179 | $7.20 | $2.57 | $+59.35 | $9,532.01 | ▲ +59.35 after sell → book $9,532.01; vs 09:30 mark -2.56 | dropped from list after 2 sess (min 1) | join🔴 sector🟡 gen🔴 news🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,532.01 | ▲ close $9,532.01 vs 09:30 $9,536.61 (session +0.00) | 16:00 close · cash $9,532.01 · no lots left · equity $9,532.01. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,532.01 | ▲ 09:30 equity $9,532.01 vs yday $9,532.01 (-0.00) | 09:30 open · cash $9,532.01 · no holdings · equity $9,532.01 vs prior close $9,532.01 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AEM` | 5 | $204.45 | $2.00 | — | $8,507.75 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $1191.50 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TWST` | 8 | $136.84 | $2.01 | — | $7,411.02 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+13.7; leftover $1191.50 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABTC` | 140 | $8.46 | $2.41 | — | $6,224.21 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+14.0; leftover $1191.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HL` | 58 | $20.25 | $2.16 | — | $5,047.54 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+13.5; leftover $1191.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SBET` | 157 | $7.55 | $2.46 | — | $3,859.73 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+14.6; leftover $1191.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `PPC` | 38 | $30.65 | $2.10 | — | $2,692.93 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+16.5; leftover $1191.50 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 100 | $11.81 | $2.29 | — | $1,509.14 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1191.50 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 133 | $8.91 | $2.39 | — | $321.72 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1191.50 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $321.72 | ▲ close $9,574.73 vs 09:30 $9,532.01 (session +60.56) | 16:00 close · cash $321.72 · equity $9,574.73 vs 09:30 $9,532.01 (+42.72; session marks +60.56) · 8 name(s) marked open→close (per-name table). AEM×5 09:30 $204.45 → close $212.04 +37.95; TWST×8 09:30 $136.84 → close $136.33 -4.08; ABTC×140 09:30 $8.46 → close $8.47 +1.40; HL×58 09:30 $20.25 → close $20.82 +33.06; SBET×157 09:30 $7.55 → close $7.59 +6.28; PPC×38 09:30 $30.65 → close $31.24 +22.42; ABCL×100 09:30 $11.81 → close $11.57 -24.50; SENS×133 09:30 $8.91 → close $8.82 -11.97 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $321.72 | ▲ 09:30 equity $9,764.65 vs yday $9,574.73 (+189.92) | 09:30 open · cash $321.72 (unchanged overnight, no fees) · equity $9,764.65 vs prior close $9,574.73 (+189.92) · 8 name(s) re-marked at the open (per-name table). AEM×5 yday $212.04 → 09:30 $216.30 +21.30; TWST×8 yday $136.33 → 09:30 $138.43 +16.80; ABTC×140 yday $8.47 → 09:30 $8.66 +26.60; HL×58 yday $20.82 → 09:30 $21.33 +29.58; SBET×157 yday $7.59 → 09:30 $7.87 +43.96; PPC×38 yday $31.24 → 09:30 $31.13 -4.18; ABCL×100 yday $11.57 → 09:30 $11.57 +0.00; SENS×133 yday $8.82 → 09:30 $9.24 +55.86 | — |
| 2026-08-21 09:30 ET | **SELL** | `TWST` | 8 | $138.43 | $2.03 | $+8.67 | $1,427.13 | ▲ +8.67 after sell → book $9,762.62; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HL` | 58 | $21.33 | $2.18 | $+58.29 | $2,662.08 | ▲ +58.29 after sell → book $9,760.43; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `SBET` | 157 | $7.87 | $2.50 | $+45.28 | $3,895.17 | ▲ +45.28 after sell → book $9,757.93; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `PPC` | 38 | $31.13 | $2.12 | $+14.01 | $5,075.99 | ▲ +14.01 after sell → book $9,755.81; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 100 | $11.57 | $2.32 | $-29.11 | $6,230.67 | ▼ -29.11 after sell → book $9,753.49; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 133 | $9.24 | $2.42 | $+39.08 | $7,457.17 | ▲ +39.08 after sell → book $9,751.07; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1438 | $0.86 | $16.74 | — | $6,198.00 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1242.86 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 15 | $78.88 | $2.04 | — | $5,012.77 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1242.86 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MSTR` | 10 | $119.69 | $2.02 | — | $3,813.85 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+15.7; leftover $1242.86 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TRON` | 640 | $1.94 | $8.26 | — | $2,563.99 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+15.4; leftover $1242.86 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 276 | $4.49 | $3.56 | — | $1,321.19 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+12.7; leftover $1242.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUGO` | 13 | $89.10 | $2.03 | — | $160.86 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.8; leftover $1242.86 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.86 | ▼ close $9,640.34 vs 09:30 $9,764.65 (session -76.09) | 16:00 close · cash $160.86 · equity $9,640.34 vs 09:30 $9,764.65 (-124.31; session marks -76.09) · 8 name(s) marked open→close (per-name table). AEM×5 09:30 $216.30 → close $216.06 -1.20; ABTC×140 09:30 $8.66 → close $7.93 -102.20; ORBS×1438 09:30 $0.86 → close $0.88 +23.01; GRAL×15 09:30 $78.88 → close $79.54 +9.90; MSTR×10 09:30 $119.69 → close $119.25 -4.40; TRON×640 09:30 $1.94 → close $2.01 +44.80; XHG×276 09:30 $4.49 → close $4.41 -22.08; AUGO×13 09:30 $89.10 → close $87.26 -23.92 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.86 | ▲ 09:30 equity $9,729.20 vs yday $9,640.34 (+88.86) | 09:30 open · cash $160.86 (unchanged overnight, no fees) · equity $9,729.20 vs prior close $9,640.34 (+88.86) · 8 name(s) re-marked at the open (per-name table). AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ABTC×140 yday $7.93 → 09:30 $8.00 +9.80; ORBS×1438 yday $0.88 → 09:30 $0.89 +14.38; GRAL×15 yday $79.54 → 09:30 $81.87 +34.95; MSTR×10 yday $119.25 → 09:30 $121.84 +25.90; TRON×640 yday $2.01 → 09:30 $2.02 +6.40; XHG×276 yday $4.41 → 09:30 $4.32 -24.84; AUGO×13 yday $87.26 → 09:30 $88.60 +17.42 | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $+58.87 | $1,243.99 | ▲ +58.87 after sell → book $9,727.18; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 140 | $8.00 | $2.44 | $-69.25 | $2,361.54 | ▼ -69.25 after sell → book $9,724.73; vs 09:30 mark -2.45 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1438 | $0.89 | $17.36 | $+3.29 | $3,624.00 | ▲ +3.29 after sell → book $9,707.37; vs 09:30 mark -17.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 15 | $81.87 | $2.06 | $+40.76 | $4,850.00 | ▲ +40.76 after sell → book $9,705.32; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MSTR` | 10 | $121.84 | $2.04 | $+17.44 | $6,066.36 | ▲ +17.44 after sell → book $9,703.28; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TRON` | 640 | $2.02 | $8.37 | $+34.57 | $7,350.78 | ▲ +34.57 after sell → book $9,694.90; vs 09:30 mark -8.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUGO` | 13 | $88.60 | $2.05 | $-10.58 | $8,500.54 | ▼ -10.58 after sell → book $9,692.86; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,500.54 | ▼ close $9,632.14 vs 09:30 $9,729.20 (session -60.72) | 16:00 close · cash $8,500.54 · equity $9,632.14 vs 09:30 $9,729.20 (-97.06; session marks -60.72) · 1 name(s) marked open→close (per-name table). XHG×276 09:30 $4.32 → close $4.10 -60.72 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,500.54 | ▼ 09:30 equity $9,623.86 vs yday $9,632.14 (-8.28) | 09:30 open · cash $8,500.54 (unchanged overnight, no fees) · equity $9,623.86 vs prior close $9,632.14 (-8.28) · 1 name(s) re-marked at the open (per-name table). XHG×276 yday $4.10 → 09:30 $4.07 -8.28 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 167 | $7.25 | $2.49 | — | $7,287.29 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1214.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `JANX` | 64 | $18.72 | $2.18 | — | $6,087.03 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+14.4; leftover $1214.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 128 | $9.42 | $2.37 | — | $4,878.90 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1214.36 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 63 | $19.00 | $2.18 | — | $3,679.72 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+11.2; leftover $1214.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AVAH` | 89 | $13.62 | $2.26 | — | $2,464.84 | — | baseline list, no extra gate; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $1214.36 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CELH` | 34 | $35.23 | $2.09 | — | $1,264.93 | — | baseline list, no extra gate; list ohlc_hot; ⚪; ret5=+17.0; leftover $1214.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `WIX` | 14 | $83.15 | $2.03 | — | $98.79 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+14.5; leftover $1214.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.79 | ▲ close $9,925.66 vs 09:30 $9,623.86 (session +317.41) | 16:00 close · cash $98.79 · equity $9,925.66 vs 09:30 $9,623.86 (+301.80; session marks +317.41) · 8 name(s) marked open→close (per-name table). XHG×276 09:30 $4.07 → close $4.02 -13.80; CAPR×167 09:30 $7.25 → close $8.29 +173.68; JANX×64 09:30 $18.72 → close $18.68 -2.56; RUM×128 09:30 $9.42 → close $10.23 +103.68; NIQ×63 09:30 $19.00 → close $19.24 +15.12; AVAH×89 09:30 $13.62 → close $13.59 -3.12; CELH×34 09:30 $35.23 → close $35.34 +3.74; WIX×14 09:30 $83.15 → close $86.06 +40.67 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.79 | ▼ 09:30 equity $9,812.73 vs yday $9,925.66 (-112.93) | 09:30 open · cash $98.79 (unchanged overnight, no fees) · equity $9,812.73 vs prior close $9,925.66 (-112.93) · 8 name(s) re-marked at the open (per-name table). XHG×276 yday $4.02 → 09:30 $3.81 -57.96; CAPR×167 yday $8.29 → 09:30 $8.29 +0.00; JANX×64 yday $18.68 → 09:30 $18.59 -5.76; RUM×128 yday $10.23 → 09:30 $10.07 -20.48; NIQ×63 yday $19.24 → 09:30 $19.20 -2.52; AVAH×89 yday $13.59 → 09:30 $13.65 +5.34; CELH×34 yday $35.34 → 09:30 $35.25 -3.06; WIX×14 yday $86.06 → 09:30 $84.02 -28.49 | — |
| 2026-08-26 09:30 ET | **SELL** | `JANX` | 64 | $18.59 | $2.20 | $-12.70 | $1,286.35 | ▼ -12.70 after sell → book $9,810.53; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 128 | $10.07 | $2.41 | $+78.42 | $2,572.91 | ▲ +78.42 after sell → book $9,808.13; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `NIQ` | 63 | $19.20 | $2.20 | $+8.22 | $3,780.31 | ▲ +8.22 after sell → book $9,805.93; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AVAH` | 89 | $13.65 | $2.28 | $-2.31 | $4,992.87 | ▼ -2.31 after sell → book $9,803.64; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CELH` | 34 | $35.25 | $2.11 | $-3.52 | $6,189.26 | ▼ -3.52 after sell → book $9,801.53; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WIX` | 14 | $84.02 | $2.05 | $+8.10 | $7,363.49 | ▲ +8.10 after sell → book $9,799.48; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `KURA` | 90 | $13.63 | $2.26 | — | $6,134.53 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+10.2; leftover $1227.25 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CNTN` | 535 | $2.29 | $6.90 | — | $4,902.48 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+14.9; leftover $1227.25 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 86 | $14.11 | $2.25 | — | $3,686.77 | — | baseline list, no extra gate; list ohlc_hot; ret5=+11.4; leftover $1227.25 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FIGR` | 30 | $40.50 | $2.08 | — | $2,469.69 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.8; leftover $1227.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 87 | $14.00 | $2.25 | — | $1,249.44 | — | baseline list, no extra gate; list ohlc_hot; ret5=+17.8; leftover $1227.25 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FUTU` | 9 | $124.67 | $2.02 | — | $125.39 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.7; leftover $1227.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $125.39 | ▼ close $9,758.55 vs 09:30 $9,812.73 (session -23.17) | 16:00 close · cash $125.39 · equity $9,758.55 vs 09:30 $9,812.73 (-54.18; session marks -23.17) · 8 name(s) marked open→close (per-name table). XHG×276 09:30 $3.81 → close $4.06 +69.00; CAPR×167 09:30 $8.29 → close $9.36 +178.69; KURA×90 09:30 $13.63 → close $13.06 -51.30; CNTN×535 09:30 $2.29 → close $2.23 -32.10; BYND×86 09:30 $14.11 → close $14.25 +12.04; FIGR×30 09:30 $40.50 → close $37.08 -102.60; MNRO×87 09:30 $14.00 → close $12.61 -120.93; FUTU×9 09:30 $124.67 → close $127.34 +24.03 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $125.39 | ▼ 09:30 equity $9,719.75 vs yday $9,758.55 (-38.80) | 09:30 open · cash $125.39 (unchanged overnight, no fees) · equity $9,719.75 vs prior close $9,758.55 (-38.80) · 8 name(s) re-marked at the open (per-name table). XHG×276 yday $4.06 → 09:30 $4.06 +0.00; CAPR×167 yday $9.36 → 09:30 $9.19 -28.39; KURA×90 yday $13.06 → 09:30 $12.98 -7.20; CNTN×535 yday $2.23 → 09:30 $2.21 -10.70; BYND×86 yday $14.25 → 09:30 $14.20 -4.30; FIGR×30 yday $37.08 → 09:30 $37.42 +10.20; MNRO×87 yday $12.61 → 09:30 $12.56 -4.35; FUTU×9 yday $127.34 → 09:30 $128.00 +5.94 | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 276 | $4.06 | $3.62 | $-125.86 | $1,242.34 | ▼ -125.86 after sell → book $9,716.14; vs 09:30 mark -3.61 | dropped from list after 4 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 167 | $9.19 | $2.53 | $+318.96 | $2,774.54 | ▲ +318.96 after sell → book $9,713.61; vs 09:30 mark -2.53 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `KURA` | 90 | $12.98 | $2.28 | $-63.04 | $3,940.45 | ▼ -63.04 after sell → book $9,711.32; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CNTN` | 535 | $2.21 | $7.00 | $-56.70 | $5,115.80 | ▼ -56.70 after sell → book $9,704.32; vs 09:30 mark -7.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 86 | $14.20 | $2.27 | $+3.22 | $6,334.73 | ▲ +3.22 after sell → book $9,702.05; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FIGR` | 30 | $37.42 | $2.10 | $-96.58 | $7,455.23 | ▼ -96.58 after sell → book $9,699.95; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MNRO` | 87 | $12.56 | $2.28 | $-129.81 | $8,545.67 | ▼ -129.81 after sell → book $9,697.67; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FUTU` | 9 | $128.00 | $2.04 | $+25.92 | $9,695.64 | ▲ +25.92 after sell → book $9,695.64; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,695.64 | ▲ close $9,695.64 vs 09:30 $9,719.75 (session +0.00) | 16:00 close · cash $9,695.64 · no lots left · equity $9,695.64. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,695.64 | ▲ 09:30 equity $9,695.64 vs yday $9,695.64 (-0.00) | 09:30 open · cash $9,695.64 · no holdings · equity $9,695.64 vs prior close $9,695.64 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SLI` | 452 | $2.68 | $5.83 | — | $8,478.44 | — | baseline list, no extra gate; list flatten,ohlc_hot; ret5=+16.3; leftover $1211.95 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 86 | $14.00 | $2.25 | — | $7,272.20 | — | baseline list, no extra gate; list ohlc_hot; ret5=-3.3; leftover $1211.95 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 8 | $137.19 | $2.01 | — | $6,172.66 | — | baseline list, no extra gate; list ohlc_hot; ret5=+7.1; leftover $1211.95 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SBET` | 140 | $8.65 | $2.41 | — | $4,959.25 | — | baseline list, no extra gate; list ohlc_hot; ret5=+17.0; leftover $1211.95 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CRCL` | 13 | $92.61 | $2.03 | — | $3,753.29 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.6; leftover $1211.95 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 2 | $461.85 | $2.00 | — | $2,827.60 | — | baseline list, no extra gate; list ohlc_hot; ret5=+16.8; leftover $1211.95 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SRPT` | 56 | $21.49 | $2.16 | — | $1,622.00 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.3; leftover $1211.95 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 66 | $18.36 | $2.19 | — | $408.05 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.8; leftover $1211.95 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $408.05 | ▼ close $9,382.03 vs 09:30 $9,695.64 (session -292.73) | 16:00 close · cash $408.05 · equity $9,382.03 vs 09:30 $9,695.64 (-313.61; session marks -292.73) · 8 name(s) marked open→close (per-name table). SLI×452 09:30 $2.68 → close $2.55 -58.76; BYND×86 09:30 $14.00 → close $13.86 -12.04; MRNA×8 09:30 $137.19 → close $137.99 +6.40; SBET×140 09:30 $8.65 → close $8.20 -63.00; CRCL×13 09:30 $92.61 → close $87.14 -71.11; SNPS×2 09:30 $461.85 → close $442.61 -38.48; SRPT×56 09:30 $21.49 → close $20.86 -35.28; NEO×66 09:30 $18.36 → close $18.05 -20.46 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $408.05 | ▼ 09:30 equity $9,319.87 vs yday $9,382.03 (-62.16) | 09:30 open · cash $408.05 (unchanged overnight, no fees) · equity $9,319.87 vs prior close $9,382.03 (-62.16) · 8 name(s) re-marked at the open (per-name table). SLI×452 yday $2.55 → 09:30 $2.58 +13.56; BYND×86 yday $13.86 → 09:30 $13.81 -4.30; MRNA×8 yday $137.99 → 09:30 $134.10 -31.12; SBET×140 yday $8.20 → 09:30 $8.24 +5.60; CRCL×13 yday $87.14 → 09:30 $87.04 -1.30; SNPS×2 yday $442.61 → 09:30 $437.95 -9.32; SRPT×56 yday $20.86 → 09:30 $20.56 -16.80; NEO×66 yday $18.05 → 09:30 $17.77 -18.48 | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 452 | $2.58 | $5.92 | $-56.95 | $1,568.30 | ▼ -56.95 after sell → book $9,313.96; vs 09:30 mark -5.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SBET` | 140 | $8.24 | $2.44 | $-62.25 | $2,719.45 | ▼ -62.25 after sell → book $9,311.51; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRCL` | 13 | $87.04 | $2.05 | $-76.49 | $3,848.92 | ▼ -76.49 after sell → book $9,309.46; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SNPS` | 2 | $437.95 | $2.02 | $-51.81 | $4,722.81 | ▼ -51.81 after sell → book $9,307.45; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `SRPT` | 56 | $20.56 | $2.18 | $-56.42 | $5,871.99 | ▼ -56.42 after sell → book $9,305.27; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `NEO` | 66 | $17.77 | $2.21 | $-43.34 | $7,042.60 | ▼ -43.34 after sell → book $9,303.06; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,042.60 | ▲ close $9,309.12 vs 09:30 $9,319.87 (session +6.06) | 16:00 close · cash $7,042.60 · equity $9,309.12 vs 09:30 $9,319.87 (-10.75; session marks +6.06) · 2 name(s) marked open→close (per-name table). BYND×86 09:30 $13.81 → close $13.30 -43.86; MRNA×8 09:30 $134.10 → close $140.34 +49.92 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,042.60 | ▼ 09:30 equity $9,286.04 vs yday $9,309.12 (-23.08) | 09:30 open · cash $7,042.60 (unchanged overnight, no fees) · equity $9,286.04 vs prior close $9,309.12 (-23.08) · 2 name(s) re-marked at the open (per-name table). BYND×86 yday $13.30 → 09:30 $13.04 -22.36; MRNA×8 yday $140.34 → 09:30 $140.25 -0.72 | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 86 | $13.04 | $2.27 | $-87.08 | $8,161.77 | ▼ -87.08 after sell → book $9,283.77; vs 09:30 mark -2.27 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,161.77 | ▲ close $9,395.93 vs 09:30 $9,286.04 (session +112.16) | 16:00 close · cash $8,161.77 · equity $9,395.93 vs 09:30 $9,286.04 (+109.89; session marks +112.16) · 1 name(s) marked open→close (per-name table). MRNA×8 09:30 $140.25 → close $154.27 +112.16 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,161.77 | ▼ 09:30 equity $9,372.97 vs yday $9,395.93 (-22.96) | 09:30 open · cash $8,161.77 (unchanged overnight, no fees) · equity $9,372.97 vs prior close $9,395.93 (-22.96) · 1 name(s) re-marked at the open (per-name table). MRNA×8 yday $154.27 → 09:30 $151.40 -22.96 | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,161.77 | ▼ close $9,368.25 vs 09:30 $9,372.97 (session -4.72) | 16:00 close · cash $8,161.77 · equity $9,368.25 vs 09:30 $9,372.97 (-4.72; session marks -4.72) · 1 name(s) marked open→close (per-name table). MRNA×8 09:30 $151.40 → close $150.81 -4.72 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,161.77 | ▼ 09:30 equity $9,329.33 vs yday $9,368.25 (-38.92) | 09:30 open · cash $8,161.77 (unchanged overnight, no fees) · equity $9,329.33 vs prior close $9,368.25 (-38.92) · 1 name(s) re-marked at the open (per-name table). MRNA×8 yday $150.81 → 09:30 $145.94 -38.92 | — |
| 2026-09-03 09:30 ET | **SELL** | `MRNA` | 8 | $145.94 | $2.03 | $+65.99 | $9,327.29 | ▲ +65.99 after sell → book $9,327.29; vs 09:30 mark -2.04 | dropped from list after 4 sess (min 1) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 321 | $3.63 | $4.14 | — | $8,157.92 | — | baseline list, no extra gate; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1165.91 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 145 | $8.03 | $2.42 | — | $6,991.15 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1165.91 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 69 | $16.77 | $2.20 | — | $5,831.82 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1165.91 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 857 | $1.36 | $11.06 | — | $4,655.25 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1165.91 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 111 | $10.42 | $2.32 | — | $3,496.30 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1165.91 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 604 | $1.93 | $7.79 | — | $2,322.79 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1165.91 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 63 | $18.40 | $2.18 | — | $1,161.41 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=-32.2; leftover $1165.91 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 84 | $13.71 | $2.24 | — | $7.53 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+17.5; leftover $1165.91 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.53 | ▼ close $9,058.31 vs 09:30 $9,329.33 (session -234.63) | 16:00 close · cash $7.53 · equity $9,058.31 vs 09:30 $9,329.33 (-271.02; session marks -234.63) · 8 name(s) marked open→close (per-name table). CABA×321 09:30 $3.63 → close $3.48 -48.15; VSTM×145 09:30 $8.03 → close $7.98 -7.25; ARCT×69 09:30 $16.77 → close $15.56 -83.49; SID×857 09:30 $1.36 → close $1.26 -85.70; NVAX×111 09:30 $10.42 → close $10.34 -8.88; BMEA×604 09:30 $1.93 → close $1.91 -12.08; REAX×63 09:30 $18.40 → close $18.40 +0.00; CNH×84 09:30 $13.71 → close $13.84 +10.92 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.53 | ▼ 09:30 equity $9,019.65 vs yday $9,058.31 (-38.66) | 09:30 open · cash $7.53 (unchanged overnight, no fees) · equity $9,019.65 vs prior close $9,058.31 (-38.66) · 8 name(s) re-marked at the open (per-name table). CABA×321 yday $3.48 → 09:30 $3.46 -6.42; VSTM×145 yday $7.98 → 09:30 $7.91 -10.15; ARCT×69 yday $15.56 → 09:30 $15.61 +3.45; SID×857 yday $1.26 → 09:30 $1.23 -25.71; NVAX×111 yday $10.34 → 09:30 $10.50 +17.76; BMEA×604 yday $1.91 → 09:30 $1.90 -6.04; REAX×63 yday $18.40 → 09:30 $18.15 -15.75; CNH×84 yday $13.84 → 09:30 $13.89 +4.20 | — |
| 2026-09-04 09:30 ET | **SELL** | `CABA` | 321 | $3.46 | $4.20 | $-62.92 | $1,113.99 | ▼ -62.92 after sell → book $9,015.45; vs 09:30 mark -4.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 145 | $7.91 | $2.46 | $-22.28 | $2,258.48 | ▼ -22.28 after sell → book $9,012.99; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 69 | $15.61 | $2.22 | $-84.46 | $3,333.35 | ▼ -84.46 after sell → book $9,010.77; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SID` | 857 | $1.23 | $11.21 | $-133.67 | $4,376.25 | ▼ -133.67 after sell → book $8,999.56; vs 09:30 mark -11.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 111 | $10.50 | $2.35 | $+4.21 | $5,539.40 | ▲ +4.21 after sell → book $8,997.21; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 604 | $1.90 | $7.90 | $-33.81 | $6,679.10 | ▼ -33.81 after sell → book $8,989.31; vs 09:30 mark -7.90 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 63 | $18.15 | $2.20 | $-20.13 | $7,820.35 | ▼ -20.13 after sell → book $8,987.11; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 84 | $13.89 | $2.27 | $+10.61 | $8,984.84 | ▲ +10.61 after sell → book $8,984.84; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $7,955.29 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1123.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 13 | $82.70 | $2.03 | — | $6,878.16 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1123.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 44 | $25.18 | $2.12 | — | $5,768.12 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+16.0; leftover $1123.11 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 142 | $7.87 | $2.42 | — | $4,648.16 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+8.7; leftover $1123.11 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 193 | $5.79 | $2.57 | — | $3,528.12 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1123.11 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HOOD` | 9 | $120.47 | $2.02 | — | $2,441.83 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+13.6; leftover $1123.11 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GORO` | 284 | $3.95 | $3.66 | — | $1,316.37 | — | baseline list, no extra gate; list ohlc_hot; ret5=+6.9; leftover $1123.11 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `RSKD` | 164 | $6.84 | $2.48 | — | $192.12 | — | baseline list, no extra gate; list ohlc_hot; ret5=+13.2; leftover $1123.11 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $192.12 | ▲ close $9,218.90 vs 09:30 $9,019.65 (session +253.36) | 16:00 close · cash $192.12 · equity $9,218.90 vs 09:30 $9,019.65 (+199.25; session marks +253.36) · 8 name(s) marked open→close (per-name table). DELL×2 09:30 $513.78 → close $524.14 +20.72; TARS×13 09:30 $82.70 → close $90.78 +105.04; ASST×44 09:30 $25.18 → close $27.14 +86.24; USDE×142 09:30 $7.87 → close $7.93 +8.52; DFDV×193 09:30 $5.79 → close $5.87 +15.44; HOOD×9 09:30 $120.47 → close $122.11 +14.72; GORO×284 09:30 $3.95 → close $4.15 +56.80; RSKD×164 09:30 $6.84 → close $6.51 -54.12 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $192.12 | ▼ 09:30 equity $9,144.73 vs yday $9,218.90 (-74.17) | 09:30 open · cash $192.12 (unchanged overnight, no fees) · equity $9,144.73 vs prior close $9,218.90 (-74.17) · 8 name(s) re-marked at the open (per-name table). DELL×2 yday $524.14 → 09:30 $521.15 -5.98; TARS×13 yday $90.78 → 09:30 $89.67 -14.43; ASST×44 yday $27.14 → 09:30 $26.44 -30.80; USDE×142 yday $7.93 → 09:30 $7.76 -24.14; DFDV×193 yday $5.87 → 09:30 $5.81 -11.58; HOOD×9 yday $122.11 → 09:30 $125.07 +26.64; GORO×284 yday $4.15 → 09:30 $4.13 -5.68; RSKD×164 yday $6.51 → 09:30 $6.46 -8.20 | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $1,232.41 | ▲ +10.73 after sell → book $9,142.72; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 13 | $89.67 | $2.05 | $+86.53 | $2,396.07 | ▲ +86.53 after sell → book $9,140.67; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 44 | $26.44 | $2.14 | $+51.18 | $3,557.29 | ▲ +51.18 after sell → book $9,138.53; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 142 | $7.76 | $2.45 | $-20.49 | $4,656.76 | ▼ -20.49 after sell → book $9,136.08; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `HOOD` | 9 | $125.07 | $2.04 | $+37.30 | $5,780.35 | ▲ +37.30 after sell → book $9,134.04; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-08 09:30 ET | **SELL** | `GORO` | 284 | $4.13 | $3.72 | $+43.74 | $6,949.55 | ▲ +43.74 after sell → book $9,130.32; vs 09:30 mark -3.72 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `RSKD` | 164 | $6.46 | $2.52 | $-67.32 | $8,006.47 | ▼ -67.32 after sell → book $9,127.80; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,006.47 | ▲ close $9,162.54 vs 09:30 $9,144.73 (session +34.74) | 16:00 close · cash $8,006.47 · equity $9,162.54 vs 09:30 $9,144.73 (+17.81; session marks +34.74) · 1 name(s) marked open→close (per-name table). DFDV×193 09:30 $5.81 → close $5.99 +34.74 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MRVL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ELMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STDN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRGP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OABI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `XNCR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `UEC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NIQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ARCT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PURR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `REAX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SKYX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DUOL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `BMEA` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GALT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SECZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `VSTM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SKYX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNDK` | hard_red | hard-red S=-11.47 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DFDV` | 193 | 2026-09-04 @ $5.79 | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1123.11 |
