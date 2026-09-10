# Factor mine action — `union_h1_rankw`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `rank_w` · sell `list` · S-boost `none` · rank-weighted leftover

Cash book **+4.01%** ($10,401) · signal-only (no cash/fees) was -0.56%. Starts YES **6/19**. Fills 148 · skips 65 · realized $+401.24.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- Keep the first 8 names in list order.
- Split leftover cash by rank (first name gets the biggest slice).
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
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `rank_w` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,401.19.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 37 | — | $59.80 | +0.00 | $60.23 | +15.91 | +15.91 | +0.00 | +15.91 |
| 2026-08-13 | `IREN` | 42 | — | $45.98 | +0.00 | $44.76 | -51.24 | -51.24 | +0.00 | -51.24 |
| 2026-08-13 | `TPG` | 32 | — | $50.62 | +0.00 | $54.62 | +127.90 | +127.90 | +0.00 | +127.90 |
| 2026-08-13 | `TGTX` | 27 | — | $49.70 | +0.00 | $47.94 | -47.52 | -47.52 | +0.00 | -47.52 |
| 2026-08-13 | `SLS` | 94 | — | $11.70 | +0.00 | $12.36 | +62.04 | +62.04 | +0.00 | +62.04 |
| 2026-08-13 | `HIMS` | 28 | — | $29.74 | +0.00 | $28.77 | -27.16 | -27.16 | +0.00 | -27.16 |
| 2026-08-13 | `INO` | 685 | — | $0.81 | +0.00 | $0.90 | +61.65 | +61.65 | +0.00 | +61.65 |
| 2026-08-13 | `TNDM` | 11 | — | $23.33 | +0.00 | $23.13 | -2.20 | -2.20 | +0.00 | -2.20 |
| 2026-08-14 | `BTSG` | 37 | $60.23 | $59.65 | -21.46 | — | +0.00 | -21.46 | -5.55 | — |
| 2026-08-14 | `IREN` | 42 | $44.76 | $44.09 | -28.14 | — | +0.00 | -28.14 | -79.38 | — |
| 2026-08-14 | `TPG` | 32 | $54.62 | $55.29 | +21.44 | — | +0.00 | +21.44 | +149.34 | — |
| 2026-08-14 | `TGTX` | 27 | $47.94 | $47.27 | -18.09 | — | +0.00 | -18.09 | -65.61 | — |
| 2026-08-14 | `SLS` | 94 | $12.36 | $12.40 | +3.76 | — | +0.00 | +3.76 | +65.80 | — |
| 2026-08-14 | `HIMS` | 28 | $28.77 | $29.15 | +10.64 | — | +0.00 | +10.64 | -16.52 | — |
| 2026-08-14 | `INO` | 685 | $0.90 | $0.93 | +20.55 | — | +0.00 | +20.55 | +82.20 | — |
| 2026-08-14 | `TNDM` | 11 | $23.13 | $22.92 | -2.31 | — | +0.00 | -2.31 | -4.51 | — |
| 2026-08-14 | `TLN` | 6 | — | $359.83 | +0.00 | $362.74 | +17.46 | +17.46 | +0.00 | +17.46 |
| 2026-08-14 | `VST` | 13 | — | $146.90 | +0.00 | $148.13 | +15.99 | +15.99 | +0.00 | +15.99 |
| 2026-08-14 | `NRG` | 13 | — | $120.00 | +0.00 | $126.24 | +81.12 | +81.12 | +0.00 | +81.12 |
| 2026-08-14 | `DAVE` | 4 | — | $330.91 | +0.00 | $334.57 | +14.64 | +14.64 | +0.00 | +14.64 |
| 2026-08-14 | `SLG` | 19 | — | $57.61 | +0.00 | $56.09 | -28.88 | -28.88 | +0.00 | -28.88 |
| 2026-08-14 | `MARA` | 93 | — | $9.01 | +0.00 | $9.20 | +17.67 | +17.67 | +0.00 | +17.67 |
| 2026-08-14 | `LDI` | 597 | — | $0.94 | +0.00 | $0.90 | -23.88 | -23.88 | +0.00 | -23.88 |
| 2026-08-14 | `BTBT` | 186 | — | $1.50 | +0.00 | $1.57 | +13.02 | +13.02 | +0.00 | +13.02 |
| 2026-08-17 | `TLN` | 6 | $362.74 | $367.88 | +30.84 | — | +0.00 | +30.84 | +48.30 | — |
| 2026-08-17 | `VST` | 13 | $148.13 | $149.37 | +16.12 | — | +0.00 | +16.12 | +32.11 | — |
| 2026-08-17 | `NRG` | 13 | $126.24 | $127.40 | +15.08 | — | +0.00 | +15.08 | +96.20 | — |
| 2026-08-17 | `DAVE` | 4 | $334.57 | $336.94 | +9.48 | — | +0.00 | +9.48 | +24.12 | — |
| 2026-08-17 | `SLG` | 19 | $56.09 | $55.37 | -13.68 | — | +0.00 | -13.68 | -42.56 | — |
| 2026-08-17 | `MARA` | 93 | $9.20 | $9.22 | +1.86 | — | +0.00 | +1.86 | +19.53 | — |
| 2026-08-17 | `LDI` | 597 | $0.90 | $0.91 | +5.97 | — | +0.00 | +5.97 | -17.91 | — |
| 2026-08-17 | `BTBT` | 186 | $1.57 | $1.52 | -9.30 | — | +0.00 | -9.30 | +3.72 | — |
| 2026-08-17 | `DVN` | 49 | — | $46.18 | +0.00 | $47.57 | +68.11 | +68.11 | +0.00 | +68.11 |
| 2026-08-17 | `EOG` | 13 | — | $142.77 | +0.00 | $146.15 | +43.94 | +43.94 | +0.00 | +43.94 |
| 2026-08-17 | `FANG` | 8 | — | $202.70 | +0.00 | $206.29 | +28.72 | +28.72 | +0.00 | +28.72 |
| 2026-08-17 | `TMC` | 349 | — | $4.05 | +0.00 | $3.77 | -97.72 | -97.72 | +0.00 | -97.72 |
| 2026-08-17 | `TGB` | 133 | — | $8.46 | +0.00 | $8.77 | +41.23 | +41.23 | +0.00 | +41.23 |
| 2026-08-17 | `ELF` | 9 | — | $90.54 | +0.00 | $93.66 | +28.08 | +28.08 | +0.00 | +28.08 |
| 2026-08-17 | `DNN` | 174 | — | $3.24 | +0.00 | $3.19 | -8.70 | -8.70 | +0.00 | -8.70 |
| 2026-08-17 | `HNST` | 58 | — | $4.81 | +0.00 | $4.70 | -6.38 | -6.38 | +0.00 | -6.38 |
| 2026-08-18 | `DVN` | 49 | $47.57 | $48.00 | +21.07 | — | +0.00 | +21.07 | +89.18 | — |
| 2026-08-18 | `EOG` | 13 | $146.15 | $148.04 | +24.57 | — | +0.00 | +24.57 | +68.51 | — |
| 2026-08-18 | `FANG` | 8 | $206.29 | $208.93 | +21.12 | — | +0.00 | +21.12 | +49.84 | — |
| 2026-08-18 | `TMC` | 349 | $3.77 | $3.72 | -17.45 | — | +0.00 | -17.45 | -115.17 | — |
| 2026-08-18 | `TGB` | 133 | $8.77 | $8.55 | -29.26 | — | +0.00 | -29.26 | +11.97 | — |
| 2026-08-18 | `ELF` | 9 | $93.66 | $93.44 | -1.98 | — | +0.00 | -1.98 | +26.10 | — |
| 2026-08-18 | `DNN` | 174 | $3.19 | $3.11 | -13.92 | — | +0.00 | -13.92 | -22.62 | — |
| 2026-08-18 | `HNST` | 58 | $4.70 | $4.67 | -1.74 | — | +0.00 | -1.74 | -8.12 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 110 | — | $20.55 | +0.00 | $21.19 | +70.40 | +70.40 | +0.00 | +70.40 |
| 2026-08-20 | `BHP` | 21 | — | $91.01 | +0.00 | $93.63 | +55.02 | +55.02 | +0.00 | +55.02 |
| 2026-08-20 | `CDE` | 82 | — | $20.65 | +0.00 | $21.11 | +37.72 | +37.72 | +0.00 | +37.72 |
| 2026-08-20 | `HDSN` | 246 | — | $5.77 | +0.00 | $5.57 | -49.20 | -49.20 | +0.00 | -49.20 |
| 2026-08-20 | `IAG` | 58 | — | $19.63 | +0.00 | $20.50 | +50.46 | +50.46 | +0.00 | +50.46 |
| 2026-08-20 | `KGC` | 28 | — | $29.63 | +0.00 | $31.43 | +50.40 | +50.40 | +0.00 | +50.40 |
| 2026-08-20 | `NFGC` | 325 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 1 | — | $144.54 | +0.00 | $150.25 | +5.71 | +5.71 | +0.00 | +5.71 |
| 2026-08-21 | `AG` | 110 | $21.19 | $21.90 | +78.10 | — | +0.00 | +78.10 | +148.50 | — |
| 2026-08-21 | `BHP` | 21 | $93.63 | $95.72 | +43.89 | — | +0.00 | +43.89 | +98.91 | — |
| 2026-08-21 | `CDE` | 82 | $21.11 | $21.75 | +52.48 | — | +0.00 | +52.48 | +90.20 | — |
| 2026-08-21 | `HDSN` | 246 | $5.57 | $5.67 | +24.60 | — | +0.00 | +24.60 | -24.60 | — |
| 2026-08-21 | `IAG` | 58 | $20.50 | $21.17 | +38.86 | — | +0.00 | +38.86 | +89.32 | — |
| 2026-08-21 | `KGC` | 28 | $31.43 | $32.17 | +20.72 | — | +0.00 | +20.72 | +71.12 | — |
| 2026-08-21 | `NFGC` | 325 | $1.75 | $1.79 | +13.00 | — | +0.00 | +13.00 | +13.00 | — |
| 2026-08-21 | `WPM` | 1 | $150.25 | $154.70 | +4.45 | — | +0.00 | +4.45 | +10.16 | — |
| 2026-08-21 | `AU` | 19 | — | $119.43 | +0.00 | $121.22 | +34.01 | +34.01 | +0.00 | +34.01 |
| 2026-08-21 | `AUPH` | 121 | — | $17.20 | +0.00 | $16.65 | -66.55 | -66.55 | +0.00 | -66.55 |
| 2026-08-21 | `AEM` | 8 | — | $216.30 | +0.00 | $216.06 | -1.92 | -1.92 | +0.00 | -1.92 |
| 2026-08-21 | `ARCT` | 133 | — | $11.13 | +0.00 | $13.45 | +308.56 | +308.56 | +0.00 | +308.56 |
| 2026-08-21 | `AUTL` | 482 | — | $2.47 | +0.00 | $2.41 | -28.92 | -28.92 | +0.00 | -28.92 |
| 2026-08-21 | `CRDL` | 462 | — | $1.93 | +0.00 | $1.86 | -32.34 | -32.34 | +0.00 | -32.34 |
| 2026-08-21 | `CRSP` | 9 | — | $59.72 | +0.00 | $59.50 | -1.98 | -1.98 | +0.00 | -1.98 |
| 2026-08-21 | `CYPH` | 225 | — | $1.32 | +0.00 | $1.42 | +22.50 | +22.50 | +0.00 | +22.50 |
| 2026-08-24 | `AU` | 19 | $121.22 | $120.51 | -13.49 | — | +0.00 | -13.49 | +20.52 | — |
| 2026-08-24 | `AUPH` | 121 | $16.65 | $16.57 | -9.68 | — | +0.00 | -9.68 | -76.23 | — |
| 2026-08-24 | `AEM` | 8 | $216.06 | $217.03 | +7.76 | — | +0.00 | +7.76 | +5.84 | — |
| 2026-08-24 | `ARCT` | 133 | $13.45 | $13.33 | -15.96 | — | +0.00 | -15.96 | +292.60 | — |
| 2026-08-24 | `AUTL` | 482 | $2.41 | $2.40 | -4.82 | — | +0.00 | -4.82 | -33.74 | — |
| 2026-08-24 | `CRDL` | 462 | $1.86 | $1.88 | +9.24 | — | +0.00 | +9.24 | -23.10 | — |
| 2026-08-24 | `CRSP` | 9 | $59.50 | $58.75 | -6.75 | — | +0.00 | -6.75 | -8.73 | — |
| 2026-08-24 | `CYPH` | 225 | $1.42 | $1.83 | +92.25 | — | +0.00 | +92.25 | +114.75 | — |
| 2026-08-25 | `MOS` | 102 | — | $23.77 | +0.00 | $24.27 | +51.00 | +51.00 | +0.00 | +51.00 |
| 2026-08-25 | `OCUL` | 194 | — | $10.98 | +0.00 | $10.88 | -19.40 | -19.40 | +0.00 | -19.40 |
| 2026-08-25 | `INSP` | 29 | — | $61.19 | +0.00 | $61.07 | -3.48 | -3.48 | +0.00 | -3.48 |
| 2026-08-25 | `CRMD` | 182 | — | $8.35 | +0.00 | $8.56 | +38.22 | +38.22 | +0.00 | +38.22 |
| 2026-08-25 | `RZLT` | 246 | — | $4.94 | +0.00 | $5.01 | +17.22 | +17.22 | +0.00 | +17.22 |
| 2026-08-25 | `HCA` | 2 | — | $426.97 | +0.00 | $428.76 | +3.58 | +3.58 | +0.00 | +3.58 |
| 2026-08-25 | `CAPR` | 83 | — | $7.25 | +0.00 | $8.29 | +86.32 | +86.32 | +0.00 | +86.32 |
| 2026-08-25 | `SAFX` | 850 | — | $0.36 | +0.00 | $0.35 | -3.40 | -3.40 | +0.00 | -3.40 |
| 2026-08-26 | `MOS` | 102 | $24.27 | $24.84 | +58.14 | $24.16 | -69.36 | -11.22 | +109.14 | +39.78 |
| 2026-08-26 | `OCUL` | 194 | $10.88 | $10.79 | -17.46 | $10.77 | -3.88 | -21.34 | -36.86 | -40.74 |
| 2026-08-26 | `INSP` | 29 | $61.07 | $60.07 | -29.00 | $61.80 | +50.17 | +21.17 | -32.48 | +17.69 |
| 2026-08-26 | `CRMD` | 182 | $8.56 | $8.60 | +7.28 | $8.39 | -38.22 | -30.94 | +45.50 | +7.28 |
| 2026-08-26 | `RZLT` | 246 | $5.01 | $5.01 | +0.00 | $5.04 | +7.38 | +7.38 | +17.22 | +24.60 |
| 2026-08-26 | `HCA` | 2 | $428.76 | $427.50 | -2.52 | $427.16 | -0.68 | -3.20 | +1.06 | +0.38 |
| 2026-08-26 | `CAPR` | 83 | $8.29 | $8.29 | +0.00 | — | +0.00 | +0.00 | +86.32 | — |
| 2026-08-26 | `SAFX` | 850 | $0.35 | $0.35 | -0.85 | — | +0.00 | -0.85 | -4.25 | — |
| 2026-08-26 | `AVBP` | 23 | — | $31.21 | +0.00 | $31.14 | -1.61 | -1.61 | +0.00 | -1.61 |
| 2026-08-26 | `FLNC` | 32 | — | $11.12 | +0.00 | $11.08 | -1.28 | -1.28 | +0.00 | -1.28 |
| 2026-08-27 | `MOS` | 102 | $24.16 | $24.00 | -16.32 | $23.76 | -24.48 | -40.80 | +23.46 | -1.02 |
| 2026-08-27 | `OCUL` | 194 | $10.77 | $10.63 | -27.16 | — | +0.00 | -27.16 | -67.90 | — |
| 2026-08-27 | `INSP` | 29 | $61.80 | $62.10 | +8.70 | — | +0.00 | +8.70 | +26.39 | — |
| 2026-08-27 | `CRMD` | 182 | $8.39 | $8.49 | +18.20 | — | +0.00 | +18.20 | +25.48 | — |
| 2026-08-27 | `RZLT` | 246 | $5.04 | $5.07 | +7.38 | — | +0.00 | +7.38 | +31.98 | — |
| 2026-08-27 | `HCA` | 2 | $427.16 | $424.61 | -5.10 | — | +0.00 | -5.10 | -4.72 | — |
| 2026-08-27 | `AVBP` | 23 | $31.14 | $30.79 | -8.05 | — | +0.00 | -8.05 | -9.66 | — |
| 2026-08-27 | `FLNC` | 32 | $11.08 | $11.52 | +14.08 | — | +0.00 | +14.08 | +12.80 | — |
| 2026-08-27 | `RRC` | 51 | — | $41.44 | +0.00 | $41.64 | +10.20 | +10.20 | +0.00 | +10.20 |
| 2026-08-27 | `CRK` | 127 | — | $14.42 | +0.00 | $14.62 | +25.40 | +25.40 | +0.00 | +25.40 |
| 2026-08-27 | `SLI` | 589 | — | $2.60 | +0.00 | $2.64 | +23.56 | +23.56 | +0.00 | +23.56 |
| 2026-08-27 | `ACMR` | 15 | — | $81.65 | +0.00 | $80.49 | -17.40 | -17.40 | +0.00 | -17.40 |
| 2026-08-27 | `GGB` | 201 | — | $4.57 | +0.00 | $4.70 | +26.13 | +26.13 | +0.00 | +26.13 |
| 2026-08-27 | `MT` | 8 | — | $74.54 | +0.00 | $74.63 | +0.72 | +0.72 | +0.00 | +0.72 |
| 2026-08-28 | `MOS` | 102 | $23.76 | $23.95 | +19.38 | $23.60 | -35.70 | -16.32 | +18.36 | -17.34 |
| 2026-08-28 | `RRC` | 51 | $41.64 | $41.74 | +5.10 | $41.46 | -14.28 | -9.18 | +15.30 | +1.02 |
| 2026-08-28 | `CRK` | 127 | $14.62 | $14.63 | +1.27 | $14.29 | -43.18 | -41.91 | +26.67 | -16.51 |
| 2026-08-28 | `SLI` | 589 | $2.64 | $2.68 | +23.56 | $2.55 | -76.57 | -53.01 | +47.12 | -29.45 |
| 2026-08-28 | `ACMR` | 15 | $80.49 | $79.27 | -18.30 | — | +0.00 | -18.30 | -35.70 | — |
| 2026-08-28 | `GGB` | 201 | $4.70 | $4.67 | -6.03 | — | +0.00 | -6.03 | +20.10 | — |
| 2026-08-28 | `MT` | 8 | $74.63 | $75.39 | +6.08 | — | +0.00 | +6.08 | +6.80 | — |
| 2026-08-28 | `SEDG` | 37 | — | $32.90 | +0.00 | $31.41 | -55.13 | -55.13 | +0.00 | -55.13 |
| 2026-08-28 | `GRRR` | 58 | — | $15.66 | +0.00 | $14.41 | -72.50 | -72.50 | +0.00 | -72.50 |
| 2026-08-28 | `URBN` | 7 | — | $79.42 | +0.00 | $81.09 | +11.69 | +11.69 | +0.00 | +11.69 |
| 2026-08-28 | `PYXS` | 92 | — | $3.32 | +0.00 | $3.23 | -8.28 | -8.28 | +0.00 | -8.28 |
| 2026-08-31 | `MOS` | 102 | $23.60 | $23.68 | +8.16 | — | +0.00 | +8.16 | -9.18 | — |
| 2026-08-31 | `RRC` | 51 | $41.46 | $42.00 | +27.54 | — | +0.00 | +27.54 | +28.56 | — |
| 2026-08-31 | `CRK` | 127 | $14.29 | $14.54 | +31.75 | — | +0.00 | +31.75 | +15.24 | — |
| 2026-08-31 | `SLI` | 589 | $2.55 | $2.58 | +17.67 | — | +0.00 | +17.67 | -11.78 | — |
| 2026-08-31 | `SEDG` | 37 | $31.41 | $31.15 | -9.62 | — | +0.00 | -9.62 | -64.75 | — |
| 2026-08-31 | `GRRR` | 58 | $14.41 | $14.44 | +1.74 | — | +0.00 | +1.74 | -70.76 | — |
| 2026-08-31 | `URBN` | 7 | $81.09 | $80.44 | -4.55 | — | +0.00 | -4.55 | +7.14 | — |
| 2026-08-31 | `PYXS` | 92 | $3.23 | $3.20 | -2.76 | — | +0.00 | -2.76 | -11.04 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 45 | — | $52.88 | +0.00 | $52.46 | -18.90 | -18.90 | +0.00 | -18.90 |
| 2026-09-03 | `HRMY` | 48 | — | $42.93 | +0.00 | $41.86 | -51.36 | -51.36 | +0.00 | -51.36 |
| 2026-09-03 | `CABA` | 496 | — | $3.63 | +0.00 | $3.48 | -74.40 | -74.40 | +0.00 | -74.40 |
| 2026-09-03 | `VSTM` | 187 | — | $8.03 | +0.00 | $7.98 | -9.35 | -9.35 | +0.00 | -9.35 |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `CRK` | 58 | — | $15.45 | +0.00 | $14.95 | -29.00 | -29.00 | +0.00 | -29.00 |
| 2026-09-03 | `MRNA` | 4 | — | $145.94 | +0.00 | $148.87 | +11.70 | +11.70 | +0.00 | +11.70 |
| 2026-09-03 | `ARCT` | 17 | — | $16.77 | +0.00 | $15.56 | -20.57 | -20.57 | +0.00 | -20.57 |
| 2026-09-04 | `ATRC` | 45 | $52.46 | $52.03 | -19.35 | $51.52 | -22.95 | -42.30 | -38.25 | -61.20 |
| 2026-09-04 | `HRMY` | 48 | $41.86 | $41.50 | -17.28 | — | +0.00 | -17.28 | -68.64 | — |
| 2026-09-04 | `CABA` | 496 | $3.48 | $3.46 | -9.92 | $3.47 | +4.96 | -4.96 | -84.32 | -79.36 |
| 2026-09-04 | `VSTM` | 187 | $7.98 | $7.91 | -13.09 | — | +0.00 | -13.09 | -22.44 | — |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `CRK` | 58 | $14.95 | $15.00 | +2.90 | — | +0.00 | +2.90 | -26.10 | — |
| 2026-09-04 | `MRNA` | 4 | $148.87 | $153.62 | +19.00 | — | +0.00 | +19.00 | +30.70 | — |
| 2026-09-04 | `ARCT` | 17 | $15.56 | $15.61 | +0.85 | — | +0.00 | +0.85 | -19.72 | — |
| 2026-09-04 | `ALEC` | 734 | — | $2.52 | +0.00 | $2.46 | -44.04 | -44.04 | +0.00 | -44.04 |
| 2026-09-04 | `BHC` | 229 | — | $6.71 | +0.00 | $6.56 | -34.35 | -34.35 | +0.00 | -34.35 |
| 2026-09-04 | `BMEA` | 649 | — | $1.90 | +0.00 | $2.03 | +84.37 | +84.37 | +0.00 | +84.37 |
| 2026-09-04 | `OABI` | 193 | — | $4.78 | +0.00 | $4.33 | -86.85 | -86.85 | +0.00 | -86.85 |
| 2026-09-04 | `OPK` | 387 | — | $1.59 | +0.00 | $1.64 | +19.35 | +19.35 | +0.00 | +19.35 |
| 2026-09-04 | `VIR` | 25 | — | $11.31 | +0.00 | $11.38 | +1.87 | +1.87 | +0.00 | +1.87 |
| 2026-09-08 | `ATRC` | 45 | $51.52 | $54.31 | +125.55 | — | +0.00 | +125.55 | +64.35 | — |
| 2026-09-08 | `CABA` | 496 | $3.47 | $3.43 | -19.84 | — | +0.00 | -19.84 | -99.20 | — |
| 2026-09-08 | `ALEC` | 734 | $2.46 | $2.38 | -58.72 | — | +0.00 | -58.72 | -102.76 | — |
| 2026-09-08 | `BHC` | 229 | $6.56 | $6.57 | +2.29 | — | +0.00 | +2.29 | -32.06 | — |
| 2026-09-08 | `BMEA` | 649 | $2.03 | $2.00 | -19.47 | — | +0.00 | -19.47 | +64.90 | — |
| 2026-09-08 | `OABI` | 193 | $4.33 | $4.30 | -5.79 | — | +0.00 | -5.79 | -92.64 | — |
| 2026-09-08 | `OPK` | 387 | $1.64 | $1.63 | -3.87 | — | +0.00 | -3.87 | +15.48 | — |
| 2026-09-08 | `VIR` | 25 | $11.38 | $11.22 | -4.12 | — | +0.00 | -4.12 | -2.25 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +139.38 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $128.05 | $10,117.03 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11 |
| 2026-08-14 | +5.50 | $128.05 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11 | $10,103.42 | -13.61 | +107.14 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $334.42 | $10,164.79 | TLN×6, VST×13, NRG×13, DAVE×4, SLG×19, MARA×93, LDI×597, BTBT×186 |
| 2026-08-17 | +2.25 | $334.42 | TLN×6, VST×13, NRG×13, DAVE×4, SLG×19, MARA×93, LDI×597, BTBT×186 | $10,221.16 | +56.37 | +97.28 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $242.31 | $10,276.25 | DVN×49, EOG×13, FANG×8, TMC×349, TGB×133, ELF×9, DNN×174, HNST×58 |
| 2026-08-18 | -6.20 | $242.31 | DVN×49, EOG×13, FANG×8, TMC×349, TGB×133, ELF×9, DNN×174, HNST×58 | $10,278.66 | +2.41 | +0.00 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | $10,258.63 | $10,258.63 | — |
| 2026-08-19 | -7.20 | $10,258.63 | — | $10,258.63 | +0.00 | +0.00 | — | — | $10,258.63 | $10,258.63 | — |
| 2026-08-20 | +1.12 | $10,258.63 | — | $10,258.63 | +0.00 | +220.51 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $273.07 | $10,459.48 | AG×110, BHP×21, CDE×82, HDSN×246, IAG×58, KGC×28, NFGC×325, WPM×1 |
| 2026-08-21 | +3.25 | $273.07 | AG×110, BHP×21, CDE×82, HDSN×246, IAG×58, KGC×28, NFGC×325, WPM×1 | $10,735.58 | +276.10 | +233.36 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $211.91 | $10,923.01 | AU×19, AUPH×121, AEM×8, ARCT×133, AUTL×482, CRDL×462, CRSP×9, CYPH×225 |
| 2026-08-24 | -5.17 | $211.91 | AU×19, AUPH×121, AEM×8, ARCT×133, AUTL×482, CRDL×462, CRSP×9, CYPH×225 | $10,981.56 | +58.55 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,955.29 | $10,955.29 | — |
| 2026-08-25 | +1.80 | $10,955.29 | — | $10,955.29 | +0.00 | +170.06 | MOS, OCUL, INSP, CRMD, RZLT, HCA, CAPR, SAFX | — | $108.71 | $11,102.87 | MOS×102, OCUL×194, INSP×29, CRMD×182, RZLT×246, HCA×2, CAPR×83, SAFX×850 |
| 2026-08-26 | +2.02 | $108.71 | MOS×102, OCUL×194, INSP×29, CRMD×182, RZLT×246, HCA×2, CAPR×83, SAFX×850 | $11,118.46 | +15.59 | -57.48 | AVBP, FLNC | CAPR, SAFX | $11.05 | $11,048.87 | MOS×102, OCUL×194, INSP×29, CRMD×182, RZLT×246, HCA×2, AVBP×23, FLNC×32 |
| 2026-08-27 | — | $11.05 | MOS×102, OCUL×194, INSP×29, CRMD×182, RZLT×246, HCA×2, AVBP×23, FLNC×32 | $11,040.60 | -8.27 | +44.13 | RRC, CRK, SLI, ACMR, GGB, MT | OCUL, INSP, CRMD, RZLT, HCA, AVBP, FLNC | $341.30 | $11,049.25 | MOS×102, RRC×51, CRK×127, SLI×589, ACMR×15, GGB×201, MT×8 |
| 2026-08-28 | +0.75 | $341.30 | MOS×102, RRC×51, CRK×127, SLI×589, ACMR×15, GGB×201, MT×8 | $11,080.31 | +31.06 | -293.95 | SEDG, GRRR, URBN, PYXS | ACMR, GGB, MT | $69.90 | $10,771.08 | MOS×102, RRC×51, CRK×127, SLI×589, SEDG×37, GRRR×58, URBN×7, PYXS×92 |
| 2026-08-31 | -5.85 | $69.90 | MOS×102, RRC×51, CRK×127, SLI×589, SEDG×37, GRRR×58, URBN×7, PYXS×92 | $10,841.01 | +69.93 | +0.00 | — | MOS, RRC, CRK, SLI, SEDG, GRRR, URBN, PYXS | $10,817.77 | $10,817.77 | — |
| 2026-09-01 | -6.30 | $10,817.77 | — | $10,817.77 | -0.00 | +0.00 | — | — | $10,817.77 | $10,817.77 | — |
| 2026-09-02 | -3.83 | $10,817.77 | — | $10,817.77 | -0.00 | +0.00 | — | — | $10,817.77 | $10,817.77 | — |
| 2026-09-03 | -0.90 | $10,817.77 | — | $10,817.77 | -0.00 | -208.26 | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MRNA, ARCT | — | $96.99 | $10,588.08 | ATRC×45, HRMY×48, CABA×496, VSTM×187, RVTY×9, CRK×58, MRNA×4, ARCT×17 |
| 2026-09-04 | +2.25 | $96.99 | ATRC×45, HRMY×48, CABA×496, VSTM×187, RVTY×9, CRK×58, MRNA×4, ARCT×17 | $10,545.79 | -42.29 | -77.64 | ALEC, BHC, BMEA, OABI, OPK, VIR | HRMY, VSTM, RVTY, CRK, MRNA, ARCT | $4.81 | $10,424.67 | ATRC×45, CABA×496, ALEC×734, BHC×229, BMEA×649, OABI×193, OPK×387, VIR×25 |
| 2026-09-08 | -11.47 | $4.81 | ATRC×45, CABA×496, ALEC×734, BHC×229, BMEA×649, OABI×193, OPK×387, VIR×25 | $10,440.70 | +16.03 | +0.00 | — | ATRC, CABA, ALEC, BHC, BMEA, OABI, OPK, VIR | $10,401.19 | $10,401.19 | — |
| 2026-09-09 | -13.95 | $10,401.19 | — | $10,401.19 | -0.00 | +0.00 | — | — | $10,401.19 | $10,401.19 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 37 | $59.80 | $2.10 | — | $7,785.30 | — | rank-weighted leftover; list flatten; ⚪; ret5=-5.3; leftover $2222.22 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 42 | $45.98 | $2.12 | — | $5,852.02 | — | rank-weighted leftover; list flatten; ⚪; ret5=+12.3; leftover $1944.44 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $4,229.99 | — | rank-weighted leftover; list flatten; ⚪; ret5=+6.2; leftover $1666.67 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 27 | $49.70 | $2.07 | — | $2,886.02 | — | rank-weighted leftover; list flatten; ⚪; ret5=-0.8; leftover $1388.89 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $1,783.95 | — | rank-weighted leftover; list flatten; ⚪; ret5=-0.8; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 28 | $29.74 | $2.07 | — | $949.16 | — | rank-weighted leftover; list flatten; ⚪; ret5=-5.3; leftover $833.33 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 685 | $0.81 | $7.60 | — | $386.70 | — | rank-weighted leftover; list flatten; ⚪; ret5=+13.2; leftover $555.56 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 11 | $23.33 | $2.02 | — | $128.05 | — | rank-weighted leftover; list flatten; ⚪; ret5=+19.7; leftover $277.78 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.05 | ▲ close $10,117.03 vs 09:30 $10,000.00 (session +139.38) | 16:00 close · cash $128.05 · equity $10,117.03 vs 09:30 $10,000.00 (+117.03; session marks +139.38) · 8 name(s) marked open→close (per-name table). BTSG×37 09:30 $59.80 → close $60.23 +15.91; IREN×42 09:30 $45.98 → close $44.76 -51.24; TPG×32 09:30 $50.62 → close $54.62 +127.90; TGTX×27 09:30 $49.70 → close $47.94 -47.52; SLS×94 09:30 $11.70 → close $12.36 +62.04; HIMS×28 09:30 $29.74 → close $28.77 -27.16; INO×685 09:30 $0.81 → close $0.90 +61.65; TNDM×11 09:30 $23.33 → close $23.13 -2.20 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.05 | ▼ 09:30 equity $10,103.42 vs yday $10,117.03 (-13.61) | 09:30 open · cash $128.05 (unchanged overnight, no fees) · equity $10,103.42 vs prior close $10,117.03 (-13.61) · 8 name(s) re-marked at the open (per-name table). BTSG×37 yday $60.23 → 09:30 $59.65 -21.46; IREN×42 yday $44.76 → 09:30 $44.09 -28.14; TPG×32 yday $54.62 → 09:30 $55.29 +21.44; TGTX×27 yday $47.94 → 09:30 $47.27 -18.09; SLS×94 yday $12.36 → 09:30 $12.40 +3.76; HIMS×28 yday $28.77 → 09:30 $29.15 +10.64; INO×685 yday $0.90 → 09:30 $0.93 +20.55; TNDM×11 yday $23.13 → 09:30 $22.92 -2.31 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 37 | $59.65 | $2.13 | $-9.78 | $2,332.97 | ▼ -9.78 after sell → book $10,101.29; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 42 | $44.09 | $2.14 | $-83.64 | $4,182.61 | ▼ -83.64 after sell → book $10,099.15; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 32 | $55.29 | $2.11 | $+145.14 | $5,949.78 | ▲ +145.14 after sell → book $10,097.04; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 27 | $47.27 | $2.09 | $-69.77 | $7,223.98 | ▼ -69.77 after sell → book $10,094.95; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 94 | $12.40 | $2.30 | $+61.23 | $8,387.28 | ▲ +61.23 after sell → book $10,092.65; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 28 | $29.15 | $2.09 | $-20.69 | $9,201.39 | ▼ -20.69 after sell → book $10,090.56; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 685 | $0.93 | $8.55 | $+66.05 | $9,829.89 | ▲ +66.05 after sell → book $10,082.01; vs 09:30 mark -8.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 11 | $22.92 | $2.04 | $-8.58 | $10,079.97 | ▼ -8.58 after sell → book $10,079.97; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 6 | $359.83 | $2.01 | — | $7,918.98 | — | rank-weighted leftover; list flatten; 🔵; ret5=+5.9; leftover $2239.99 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 13 | $146.90 | $2.03 | — | $6,007.25 | — | rank-weighted leftover; list flatten; 🔵; ret5=+3.6; leftover $1959.99 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 13 | $120.00 | $2.03 | — | $4,445.22 | — | rank-weighted leftover; list flatten; 🔵; ret5=+0.6; leftover $1679.99 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 4 | $330.91 | $2.00 | — | $3,119.58 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1400.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 19 | $57.61 | $2.05 | — | $2,022.94 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1120.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 93 | $9.01 | $2.27 | — | $1,182.74 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=-13.5; leftover $840.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 597 | $0.94 | $7.38 | — | $615.97 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.5; leftover $560.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 186 | $1.50 | $2.55 | — | $334.42 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $280.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $334.42 | ▲ close $10,164.79 vs 09:30 $10,103.42 (session +107.14) | 16:00 close · cash $334.42 · equity $10,164.79 vs 09:30 $10,103.42 (+61.37; session marks +107.14) · 8 name(s) marked open→close (per-name table). TLN×6 09:30 $359.83 → close $362.74 +17.46; VST×13 09:30 $146.90 → close $148.13 +15.99; NRG×13 09:30 $120.00 → close $126.24 +81.12; DAVE×4 09:30 $330.91 → close $334.57 +14.64; SLG×19 09:30 $57.61 → close $56.09 -28.88; MARA×93 09:30 $9.01 → close $9.20 +17.67; LDI×597 09:30 $0.94 → close $0.90 -23.88; BTBT×186 09:30 $1.50 → close $1.57 +13.02 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $334.42 | ▲ 09:30 equity $10,221.16 vs yday $10,164.79 (+56.37) | 09:30 open · cash $334.42 (unchanged overnight, no fees) · equity $10,221.16 vs prior close $10,164.79 (+56.37) · 8 name(s) re-marked at the open (per-name table). TLN×6 yday $362.74 → 09:30 $367.88 +30.84; VST×13 yday $148.13 → 09:30 $149.37 +16.12; NRG×13 yday $126.24 → 09:30 $127.40 +15.08; DAVE×4 yday $334.57 → 09:30 $336.94 +9.48; SLG×19 yday $56.09 → 09:30 $55.37 -13.68; MARA×93 yday $9.20 → 09:30 $9.22 +1.86; LDI×597 yday $0.90 → 09:30 $0.91 +5.97; BTBT×186 yday $1.57 → 09:30 $1.52 -9.30 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 6 | $367.88 | $2.04 | $+44.26 | $2,539.66 | ▲ +44.26 after sell → book $10,219.12; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 13 | $149.37 | $2.05 | $+28.03 | $4,479.42 | ▲ +28.03 after sell → book $10,217.07; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 13 | $127.40 | $2.05 | $+92.12 | $6,133.57 | ▲ +92.12 after sell → book $10,215.02; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 4 | $336.94 | $2.02 | $+20.10 | $7,479.31 | ▲ +20.10 after sell → book $10,212.99; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 19 | $55.37 | $2.07 | $-46.67 | $8,529.27 | ▼ -46.67 after sell → book $10,210.93; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 93 | $9.22 | $2.29 | $+14.97 | $9,384.43 | ▲ +14.97 after sell → book $10,208.63; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 597 | $0.91 | $7.31 | $-32.61 | $9,918.60 | ▼ -32.61 after sell → book $10,201.32; vs 09:30 mark -7.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 186 | $1.52 | $2.59 | $-1.42 | $10,198.73 | ▼ -1.42 after sell → book $10,198.73; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 49 | $46.18 | $2.14 | — | $7,933.77 | — | rank-weighted leftover; list flatten; 🔵; ret5=+6.7; leftover $2266.38 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 13 | $142.77 | $2.03 | — | $6,075.73 | — | rank-weighted leftover; list flatten; 🔵; ret5=+5.8; leftover $1983.09 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 8 | $202.70 | $2.01 | — | $4,452.12 | — | rank-weighted leftover; list flatten; 🔵; ret5=+8.3; leftover $1699.79 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 349 | $4.05 | $4.50 | — | $3,034.17 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1416.49 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 133 | $8.46 | $2.39 | — | $1,906.60 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1133.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 9 | $90.54 | $2.02 | — | $1,089.72 | — | rank-weighted leftover; list flatten; ret5=-7.2; leftover $849.89 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 174 | $3.24 | $2.51 | — | $523.45 | — | rank-weighted leftover; list flatten; ⚪; ret5=+0.3; leftover $566.60 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 58 | $4.81 | $2.16 | — | $242.31 | — | rank-weighted leftover; list flatten; ⚪; ret5=-11.4; leftover $283.30 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $242.31 | ▲ close $10,276.25 vs 09:30 $10,221.16 (session +97.28) | 16:00 close · cash $242.31 · equity $10,276.25 vs 09:30 $10,221.16 (+55.09; session marks +97.28) · 8 name(s) marked open→close (per-name table). DVN×49 09:30 $46.18 → close $47.57 +68.11; EOG×13 09:30 $142.77 → close $146.15 +43.94; FANG×8 09:30 $202.70 → close $206.29 +28.72; TMC×349 09:30 $4.05 → close $3.77 -97.72; TGB×133 09:30 $8.46 → close $8.77 +41.23; ELF×9 09:30 $90.54 → close $93.66 +28.08; DNN×174 09:30 $3.24 → close $3.19 -8.70; HNST×58 09:30 $4.81 → close $4.70 -6.38 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $242.31 | ▲ 09:30 equity $10,278.66 vs yday $10,276.25 (+2.41) | 09:30 open · cash $242.31 (unchanged overnight, no fees) · equity $10,278.66 vs prior close $10,276.25 (+2.41) · 8 name(s) re-marked at the open (per-name table). DVN×49 yday $47.57 → 09:30 $48.00 +21.07; EOG×13 yday $146.15 → 09:30 $148.04 +24.57; FANG×8 yday $206.29 → 09:30 $208.93 +21.12; TMC×349 yday $3.77 → 09:30 $3.72 -17.45; TGB×133 yday $8.77 → 09:30 $8.55 -29.26; ELF×9 yday $93.66 → 09:30 $93.44 -1.98; DNN×174 yday $3.19 → 09:30 $3.11 -13.92; HNST×58 yday $4.70 → 09:30 $4.67 -1.74 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 49 | $48.00 | $2.17 | $+84.88 | $2,592.14 | ▲ +84.88 after sell → book $10,276.49; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 13 | $148.04 | $2.05 | $+64.43 | $4,514.60 | ▲ +64.43 after sell → book $10,274.43; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 8 | $208.93 | $2.04 | $+45.79 | $6,184.01 | ▲ +45.79 after sell → book $10,272.40; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 349 | $3.72 | $4.57 | $-124.24 | $7,477.72 | ▼ -124.24 after sell → book $10,267.83; vs 09:30 mark -4.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 133 | $8.55 | $2.42 | $+7.16 | $8,612.45 | ▲ +7.16 after sell → book $10,265.41; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 9 | $93.44 | $2.04 | $+22.05 | $9,451.37 | ▲ +22.05 after sell → book $10,263.37; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 174 | $3.11 | $2.55 | $-27.68 | $9,989.96 | ▼ -27.68 after sell → book $10,260.82; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 58 | $4.67 | $2.18 | $-12.47 | $10,258.63 | ▼ -12.47 after sell → book $10,258.63; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,258.63 | ▲ close $10,258.63 vs 09:30 $10,278.66 (session +0.00) | 16:00 close · cash $10,258.63 · no lots left · equity $10,258.63. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,258.63 | ▲ 09:30 equity $10,258.63 vs yday $10,258.63 (+0.00) | 09:30 open · cash $10,258.63 · no holdings · equity $10,258.63 vs prior close $10,258.63 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,258.63 | ▲ close $10,258.63 vs 09:30 $10,258.63 (session +0.00) | 16:00 close · cash $10,258.63 · no lots left · equity $10,258.63. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,258.63 | ▲ 09:30 equity $10,258.63 vs yday $10,258.63 (+0.00) | 09:30 open · cash $10,258.63 · no holdings · equity $10,258.63 vs prior close $10,258.63 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 110 | $20.55 | $2.32 | — | $7,995.81 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $2279.70 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 21 | $91.01 | $2.05 | — | $6,082.55 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1994.73 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 82 | $20.65 | $2.24 | — | $4,387.02 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1709.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 246 | $5.77 | $3.17 | — | $2,964.42 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1424.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 58 | $19.63 | $2.16 | — | $1,823.72 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1139.85 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 28 | $29.63 | $2.07 | — | $992.00 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $854.89 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 325 | $1.75 | $4.19 | — | $419.06 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $569.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 1 | $144.54 | $1.45 | — | $273.07 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $284.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $273.07 | ▲ close $10,459.48 vs 09:30 $10,258.63 (session +220.51) | 16:00 close · cash $273.07 · equity $10,459.48 vs 09:30 $10,258.63 (+200.85; session marks +220.51) · 8 name(s) marked open→close (per-name table). AG×110 09:30 $20.55 → close $21.19 +70.40; BHP×21 09:30 $91.01 → close $93.63 +55.02; CDE×82 09:30 $20.65 → close $21.11 +37.72; HDSN×246 09:30 $5.77 → close $5.57 -49.20; IAG×58 09:30 $19.63 → close $20.50 +50.46; KGC×28 09:30 $29.63 → close $31.43 +50.40; NFGC×325 09:30 $1.75 → close $1.75 +0.00; WPM×1 09:30 $144.54 → close $150.25 +5.71 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $273.07 | ▲ 09:30 equity $10,735.58 vs yday $10,459.48 (+276.10) | 09:30 open · cash $273.07 (unchanged overnight, no fees) · equity $10,735.58 vs prior close $10,459.48 (+276.10) · 8 name(s) re-marked at the open (per-name table). AG×110 yday $21.19 → 09:30 $21.90 +78.10; BHP×21 yday $93.63 → 09:30 $95.72 +43.89; CDE×82 yday $21.11 → 09:30 $21.75 +52.48; HDSN×246 yday $5.57 → 09:30 $5.67 +24.60; IAG×58 yday $20.50 → 09:30 $21.17 +38.86; KGC×28 yday $31.43 → 09:30 $32.17 +20.72; NFGC×325 yday $1.75 → 09:30 $1.79 +13.00; WPM×1 yday $150.25 → 09:30 $154.70 +4.45 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 110 | $21.90 | $2.36 | $+143.82 | $2,679.72 | ▲ +143.82 after sell → book $10,733.23; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 21 | $95.72 | $2.08 | $+94.78 | $4,687.76 | ▲ +94.78 after sell → book $10,731.15; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 82 | $21.75 | $2.26 | $+85.70 | $6,468.99 | ▲ +85.70 after sell → book $10,728.88; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 246 | $5.67 | $3.23 | $-31.00 | $7,860.59 | ▼ -31.00 after sell → book $10,725.66; vs 09:30 mark -3.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 58 | $21.17 | $2.18 | $+84.97 | $9,086.26 | ▲ +84.97 after sell → book $10,723.47; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 28 | $32.17 | $2.09 | $+66.95 | $9,984.93 | ▲ +66.95 after sell → book $10,721.38; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 325 | $1.79 | $4.26 | $+4.55 | $10,562.42 | ▲ +4.55 after sell → book $10,717.12; vs 09:30 mark -4.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 1 | $154.70 | $1.57 | $+7.14 | $10,715.55 | ▲ +7.14 after sell → book $10,715.55; vs 09:30 mark -1.57 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 19 | $119.43 | $2.05 | — | $8,444.34 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $2381.23 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 121 | $17.20 | $2.35 | — | $6,360.78 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $2083.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 8 | $216.30 | $2.01 | — | $4,628.37 | — | rank-weighted leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1785.93 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 133 | $11.13 | $2.39 | — | $3,145.69 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1488.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 482 | $2.47 | $6.22 | — | $1,948.93 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1190.62 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 462 | $1.93 | $5.96 | — | $1,051.31 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $892.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 9 | $59.72 | $2.02 | — | $511.81 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $595.31 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 225 | $1.32 | $2.90 | — | $211.91 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $297.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $211.91 | ▲ close $10,923.01 vs 09:30 $10,735.58 (session +233.36) | 16:00 close · cash $211.91 · equity $10,923.01 vs 09:30 $10,735.58 (+187.43; session marks +233.36) · 8 name(s) marked open→close (per-name table). AU×19 09:30 $119.43 → close $121.22 +34.01; AUPH×121 09:30 $17.20 → close $16.65 -66.55; AEM×8 09:30 $216.30 → close $216.06 -1.92; ARCT×133 09:30 $11.13 → close $13.45 +308.56; AUTL×482 09:30 $2.47 → close $2.41 -28.92; CRDL×462 09:30 $1.93 → close $1.86 -32.34; CRSP×9 09:30 $59.72 → close $59.50 -1.98; CYPH×225 09:30 $1.32 → close $1.42 +22.50 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $211.91 | ▲ 09:30 equity $10,981.56 vs yday $10,923.01 (+58.55) | 09:30 open · cash $211.91 (unchanged overnight, no fees) · equity $10,981.56 vs prior close $10,923.01 (+58.55) · 8 name(s) re-marked at the open (per-name table). AU×19 yday $121.22 → 09:30 $120.51 -13.49; AUPH×121 yday $16.65 → 09:30 $16.57 -9.68; AEM×8 yday $216.06 → 09:30 $217.03 +7.76; ARCT×133 yday $13.45 → 09:30 $13.33 -15.96; AUTL×482 yday $2.41 → 09:30 $2.40 -4.82; CRDL×462 yday $1.86 → 09:30 $1.88 +9.24; CRSP×9 yday $59.50 → 09:30 $58.75 -6.75; CYPH×225 yday $1.42 → 09:30 $1.83 +92.25 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 19 | $120.51 | $2.08 | $+16.40 | $2,499.53 | ▲ +16.40 after sell → book $10,979.49; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 121 | $16.57 | $2.39 | $-80.97 | $4,502.11 | ▼ -80.97 after sell → book $10,977.10; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 8 | $217.03 | $2.04 | $+1.79 | $6,236.31 | ▲ +1.79 after sell → book $10,975.06; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 133 | $13.33 | $2.43 | $+287.79 | $8,006.77 | ▲ +287.79 after sell → book $10,972.63; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 482 | $2.40 | $6.31 | $-46.27 | $9,157.27 | ▼ -46.27 after sell → book $10,966.33; vs 09:30 mark -6.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 462 | $1.88 | $6.05 | $-35.11 | $10,019.78 | ▼ -35.11 after sell → book $10,960.28; vs 09:30 mark -6.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 9 | $58.75 | $2.04 | $-12.78 | $10,546.49 | ▼ -12.78 after sell → book $10,958.24; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 225 | $1.83 | $2.95 | $+108.90 | $10,955.29 | ▲ +108.90 after sell → book $10,955.29; vs 09:30 mark -2.95 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,955.29 | ▲ close $10,955.29 vs 09:30 $10,981.56 (session +0.00) | 16:00 close · cash $10,955.29 · no lots left · equity $10,955.29. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,955.29 | ▲ 09:30 equity $10,955.29 vs yday $10,955.29 (+0.00) | 09:30 open · cash $10,955.29 · no holdings · equity $10,955.29 vs prior close $10,955.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 102 | $23.77 | $2.30 | — | $8,528.46 | — | rank-weighted leftover; list flatten; ⚪; ret5=+13.0; leftover $2434.51 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 194 | $10.98 | $2.57 | — | $6,395.77 | — | rank-weighted leftover; list flatten; 🔵; ret5=+1.2; leftover $2130.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 29 | $61.19 | $2.08 | — | $4,619.18 | — | rank-weighted leftover; list flatten; 🔵; ret5=+7.4; leftover $1825.88 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 182 | $8.35 | $2.54 | — | $3,096.94 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1521.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 246 | $4.94 | $3.17 | — | $1,878.53 | — | rank-weighted leftover; list flatten; ret5=+7.1; leftover $1217.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $426.97 | $2.00 | — | $1,022.59 | — | rank-weighted leftover; list flatten; ret5=+6.0; leftover $912.94 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 83 | $7.25 | $2.24 | — | $418.60 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $608.63 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 850 | $0.36 | $5.59 | — | $108.71 | — | rank-weighted leftover; list probable,yday_gainer; ret5=-15.6; leftover $304.31 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $108.71 | ▲ close $11,102.87 vs 09:30 $10,955.29 (session +170.06) | 16:00 close · cash $108.71 · equity $11,102.87 vs 09:30 $10,955.29 (+147.58; session marks +170.06) · 8 name(s) marked open→close (per-name table). MOS×102 09:30 $23.77 → close $24.27 +51.00; OCUL×194 09:30 $10.98 → close $10.88 -19.40; INSP×29 09:30 $61.19 → close $61.07 -3.48; CRMD×182 09:30 $8.35 → close $8.56 +38.22; RZLT×246 09:30 $4.94 → close $5.01 +17.22; HCA×2 09:30 $426.97 → close $428.76 +3.58; CAPR×83 09:30 $7.25 → close $8.29 +86.32; SAFX×850 09:30 $0.36 → close $0.35 -3.40 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $108.71 | ▲ 09:30 equity $11,118.46 vs yday $11,102.87 (+15.59) | 09:30 open · cash $108.71 (unchanged overnight, no fees) · equity $11,118.46 vs prior close $11,102.87 (+15.59) · 8 name(s) re-marked at the open (per-name table). MOS×102 yday $24.27 → 09:30 $24.84 +58.14; OCUL×194 yday $10.88 → 09:30 $10.79 -17.46; INSP×29 yday $61.07 → 09:30 $60.07 -29.00; CRMD×182 yday $8.56 → 09:30 $8.60 +7.28; RZLT×246 yday $5.01 → 09:30 $5.01 +0.00; HCA×2 yday $428.76 → 09:30 $427.50 -2.52; CAPR×83 yday $8.29 → 09:30 $8.29 +0.00; SAFX×850 yday $0.35 → 09:30 $0.35 -0.85 | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 83 | $8.29 | $2.26 | $+81.82 | $794.52 | ▲ +81.82 after sell → book $11,116.20; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 850 | $0.35 | $5.70 | $-15.54 | $1,088.87 | ▼ -15.54 after sell → book $11,110.50; vs 09:30 mark -5.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 23 | $31.21 | $2.06 | — | $368.98 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $725.91 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 32 | $11.12 | $2.09 | — | $11.05 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $362.96 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.05 | ▼ close $11,048.87 vs 09:30 $11,118.46 (session -57.48) | 16:00 close · cash $11.05 · equity $11,048.87 vs 09:30 $11,118.46 (-69.59; session marks -57.48) · 8 name(s) marked open→close (per-name table). MOS×102 09:30 $24.84 → close $24.16 -69.36; OCUL×194 09:30 $10.79 → close $10.77 -3.88; INSP×29 09:30 $60.07 → close $61.80 +50.17; CRMD×182 09:30 $8.60 → close $8.39 -38.22; RZLT×246 09:30 $5.01 → close $5.04 +7.38; HCA×2 09:30 $427.50 → close $427.16 -0.68; AVBP×23 09:30 $31.21 → close $31.14 -1.61; FLNC×32 09:30 $11.12 → close $11.08 -1.28 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.05 | ▼ 09:30 equity $11,040.60 vs yday $11,048.87 (-8.27) | 09:30 open · cash $11.05 (unchanged overnight, no fees) · equity $11,040.60 vs prior close $11,048.87 (-8.27) · 8 name(s) re-marked at the open (per-name table). MOS×102 yday $24.16 → 09:30 $24.00 -16.32; OCUL×194 yday $10.77 → 09:30 $10.63 -27.16; INSP×29 yday $61.80 → 09:30 $62.10 +8.70; CRMD×182 yday $8.39 → 09:30 $8.49 +18.20; RZLT×246 yday $5.04 → 09:30 $5.07 +7.38; HCA×2 yday $427.16 → 09:30 $424.61 -5.10; AVBP×23 yday $31.14 → 09:30 $30.79 -8.05; FLNC×32 yday $11.08 → 09:30 $11.52 +14.08 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 194 | $10.63 | $2.62 | $-73.09 | $2,070.65 | ▼ -73.09 after sell → book $11,037.98; vs 09:30 mark -2.62 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 29 | $62.10 | $2.10 | $+22.21 | $3,869.45 | ▲ +22.21 after sell → book $11,035.88; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 182 | $8.49 | $2.58 | $+20.37 | $5,412.05 | ▲ +20.37 after sell → book $11,033.30; vs 09:30 mark -2.58 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 246 | $5.07 | $3.22 | $+25.58 | $6,656.05 | ▲ +25.58 after sell → book $11,030.08; vs 09:30 mark -3.22 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 2 | $424.61 | $2.02 | $-8.73 | $7,503.25 | ▼ -8.73 after sell → book $11,028.06; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 23 | $30.79 | $2.08 | $-13.80 | $8,209.34 | ▼ -13.80 after sell → book $11,025.98; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 32 | $11.52 | $2.11 | $+8.61 | $8,575.88 | ▲ +8.61 after sell → book $11,023.88; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 51 | $41.44 | $2.14 | — | $6,460.29 | — | rank-weighted leftover; list flatten; ret5=+3.1; leftover $2143.97 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 127 | $14.42 | $2.37 | — | $4,626.58 | — | rank-weighted leftover; list flatten; ret5=+7.1; leftover $1837.69 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 589 | $2.60 | $7.60 | — | $3,087.58 | — | rank-weighted leftover; list flatten; ret5=+13.0; leftover $1531.41 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 15 | $81.65 | $2.04 | — | $1,860.80 | — | rank-weighted leftover; list mover_buy; 🔵; ret5=+2.0; leftover $1225.13 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 201 | $4.57 | $2.60 | — | $939.63 | — | rank-weighted leftover; list mover_buy; 🔵; ret5=+1.1; leftover $918.84 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 8 | $74.54 | $2.01 | — | $341.30 | — | rank-weighted leftover; list mover_buy; 🔵; ret5=-0.1; leftover $612.56 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $341.30 | ▲ close $11,049.25 vs 09:30 $11,040.60 (session +44.13) | 16:00 close · cash $341.30 · equity $11,049.25 vs 09:30 $11,040.60 (+8.65; session marks +44.13) · 7 name(s) marked open→close (per-name table). MOS×102 09:30 $24.00 → close $23.76 -24.48; RRC×51 09:30 $41.44 → close $41.64 +10.20; CRK×127 09:30 $14.42 → close $14.62 +25.40; SLI×589 09:30 $2.60 → close $2.64 +23.56; ACMR×15 09:30 $81.65 → close $80.49 -17.40; GGB×201 09:30 $4.57 → close $4.70 +26.13; MT×8 09:30 $74.54 → close $74.63 +0.72 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $341.30 | ▲ 09:30 equity $11,080.31 vs yday $11,049.25 (+31.06) | 09:30 open · cash $341.30 (unchanged overnight, no fees) · equity $11,080.31 vs prior close $11,049.25 (+31.06) · 7 name(s) re-marked at the open (per-name table). MOS×102 yday $23.76 → 09:30 $23.95 +19.38; RRC×51 yday $41.64 → 09:30 $41.74 +5.10; CRK×127 yday $14.62 → 09:30 $14.63 +1.27; SLI×589 yday $2.64 → 09:30 $2.68 +23.56; ACMR×15 yday $80.49 → 09:30 $79.27 -18.30; GGB×201 yday $4.70 → 09:30 $4.67 -6.03; MT×8 yday $74.63 → 09:30 $75.39 +6.08 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 15 | $79.27 | $2.06 | $-39.79 | $1,528.29 | ▼ -39.79 after sell → book $11,078.25; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 201 | $4.67 | $2.64 | $+14.86 | $2,464.32 | ▲ +14.86 after sell → book $11,075.61; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 8 | $75.39 | $2.03 | $+2.75 | $3,065.41 | ▲ +2.75 after sell → book $11,073.58; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 37 | $32.90 | $2.10 | — | $1,846.01 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1226.16 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 58 | $15.66 | $2.16 | — | $935.56 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $919.62 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 7 | $79.42 | $2.01 | — | $377.61 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $613.08 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 92 | $3.32 | $2.27 | — | $69.90 | — | rank-weighted leftover; list probable,yday_gainer; ret5=+6.4; leftover $306.54 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $69.90 | ▼ close $10,771.08 vs 09:30 $11,080.31 (session -293.95) | 16:00 close · cash $69.90 · equity $10,771.08 vs 09:30 $11,080.31 (-309.23; session marks -293.95) · 8 name(s) marked open→close (per-name table). MOS×102 09:30 $23.95 → close $23.60 -35.70; RRC×51 09:30 $41.74 → close $41.46 -14.28; CRK×127 09:30 $14.63 → close $14.29 -43.18; SLI×589 09:30 $2.68 → close $2.55 -76.57; SEDG×37 09:30 $32.90 → close $31.41 -55.13; GRRR×58 09:30 $15.66 → close $14.41 -72.50; URBN×7 09:30 $79.42 → close $81.09 +11.69; PYXS×92 09:30 $3.32 → close $3.23 -8.28 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.90 | ▲ 09:30 equity $10,841.01 vs yday $10,771.08 (+69.93) | 09:30 open · cash $69.90 (unchanged overnight, no fees) · equity $10,841.01 vs prior close $10,771.08 (+69.93) · 8 name(s) re-marked at the open (per-name table). MOS×102 yday $23.60 → 09:30 $23.68 +8.16; RRC×51 yday $41.46 → 09:30 $42.00 +27.54; CRK×127 yday $14.29 → 09:30 $14.54 +31.75; SLI×589 yday $2.55 → 09:30 $2.58 +17.67; SEDG×37 yday $31.41 → 09:30 $31.15 -9.62; GRRR×58 yday $14.41 → 09:30 $14.44 +1.74; URBN×7 yday $81.09 → 09:30 $80.44 -4.55; PYXS×92 yday $3.23 → 09:30 $3.20 -2.76 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 102 | $23.68 | $2.33 | $-13.81 | $2,482.93 | ▼ -13.81 after sell → book $10,838.68; vs 09:30 mark -2.33 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 51 | $42.00 | $2.17 | $+24.25 | $4,622.76 | ▲ +24.25 after sell → book $10,836.51; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 127 | $14.54 | $2.41 | $+10.46 | $6,466.93 | ▲ +10.46 after sell → book $10,834.10; vs 09:30 mark -2.41 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 589 | $2.58 | $7.71 | $-27.09 | $7,978.85 | ▼ -27.09 after sell → book $10,826.40; vs 09:30 mark -7.70 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 37 | $31.15 | $2.12 | $-68.97 | $9,129.28 | ▼ -68.97 after sell → book $10,824.28; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 58 | $14.44 | $2.18 | $-75.11 | $9,964.61 | ▼ -75.11 after sell → book $10,822.09; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 7 | $80.44 | $2.03 | $+3.10 | $10,525.66 | ▲ +3.10 after sell → book $10,820.06; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PYXS` | 92 | $3.20 | $2.29 | $-15.60 | $10,817.77 | ▼ -15.60 after sell → book $10,817.77; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,817.77 | ▲ close $10,817.77 vs 09:30 $10,841.01 (session +0.00) | 16:00 close · cash $10,817.77 · no lots left · equity $10,817.77. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,817.77 | ▲ 09:30 equity $10,817.77 vs yday $10,817.77 (-0.00) | 09:30 open · cash $10,817.77 · no holdings · equity $10,817.77 vs prior close $10,817.77 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,817.77 | ▲ close $10,817.77 vs 09:30 $10,817.77 (session +0.00) | 16:00 close · cash $10,817.77 · no lots left · equity $10,817.77. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,817.77 | ▲ 09:30 equity $10,817.77 vs yday $10,817.77 (-0.00) | 09:30 open · cash $10,817.77 · no holdings · equity $10,817.77 vs prior close $10,817.77 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,817.77 | ▲ close $10,817.77 vs 09:30 $10,817.77 (session +0.00) | 16:00 close · cash $10,817.77 · no lots left · equity $10,817.77. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,817.77 | ▲ 09:30 equity $10,817.77 vs yday $10,817.77 (-0.00) | 09:30 open · cash $10,817.77 · no holdings · equity $10,817.77 vs prior close $10,817.77 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 45 | $52.88 | $2.12 | — | $8,436.04 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $2403.95 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 48 | $42.93 | $2.13 | — | $6,373.27 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $2103.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 496 | $3.63 | $6.40 | — | $4,566.39 | — | rank-weighted leftover; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1802.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 187 | $8.03 | $2.55 | — | $3,062.23 | — | rank-weighted leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1502.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $1,868.16 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1201.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 58 | $15.45 | $2.16 | — | $969.90 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $901.48 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 4 | $145.94 | $2.00 | — | $384.12 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $600.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 17 | $16.77 | $2.04 | — | $96.99 | — | rank-weighted leftover; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $300.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $96.99 | ▼ close $10,588.08 vs 09:30 $10,817.77 (session -208.26) | 16:00 close · cash $96.99 · equity $10,588.08 vs 09:30 $10,817.77 (-229.69; session marks -208.26) · 8 name(s) marked open→close (per-name table). ATRC×45 09:30 $52.88 → close $52.46 -18.90; HRMY×48 09:30 $42.93 → close $41.86 -51.36; CABA×496 09:30 $3.63 → close $3.48 -74.40; VSTM×187 09:30 $8.03 → close $7.98 -9.35; RVTY×9 09:30 $132.45 → close $130.63 -16.38; CRK×58 09:30 $15.45 → close $14.95 -29.00; MRNA×4 09:30 $145.94 → close $148.87 +11.70; ARCT×17 09:30 $16.77 → close $15.56 -20.57 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.99 | ▼ 09:30 equity $10,545.79 vs yday $10,588.08 (-42.29) | 09:30 open · cash $96.99 (unchanged overnight, no fees) · equity $10,545.79 vs prior close $10,588.08 (-42.29) · 8 name(s) re-marked at the open (per-name table). ATRC×45 yday $52.46 → 09:30 $52.03 -19.35; HRMY×48 yday $41.86 → 09:30 $41.50 -17.28; CABA×496 yday $3.48 → 09:30 $3.46 -9.92; VSTM×187 yday $7.98 → 09:30 $7.91 -13.09; RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; CRK×58 yday $14.95 → 09:30 $15.00 +2.90; MRNA×4 yday $148.87 → 09:30 $153.62 +19.00; ARCT×17 yday $15.56 → 09:30 $15.61 +0.85 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 48 | $41.50 | $2.16 | $-72.93 | $2,086.83 | ▼ -72.93 after sell → book $10,543.63; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 187 | $7.91 | $2.59 | $-27.58 | $3,563.40 | ▼ -27.58 after sell → book $10,541.03; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $4,731.64 | ▼ -25.83 after sell → book $10,539.00; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 58 | $15.00 | $2.18 | $-30.45 | $5,599.45 | ▼ -30.45 after sell → book $10,536.81; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 4 | $153.62 | $2.02 | $+26.68 | $6,211.91 | ▲ +26.68 after sell → book $10,534.79; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 17 | $15.61 | $2.06 | $-23.82 | $6,475.22 | ▼ -23.82 after sell → book $10,532.73; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 734 | $2.52 | $9.47 | — | $4,616.07 | — | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1850.06 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 229 | $6.71 | $2.95 | — | $3,076.53 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1541.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 649 | $1.90 | $8.37 | — | $1,835.05 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1233.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 193 | $4.78 | $2.57 | — | $909.95 | — | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $925.03 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 387 | $1.59 | $4.99 | — | $289.62 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $616.69 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 25 | $11.31 | $2.06 | — | $4.81 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $308.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.81 | ▼ close $10,424.67 vs 09:30 $10,545.79 (session -77.64) | 16:00 close · cash $4.81 · equity $10,424.67 vs 09:30 $10,545.79 (-121.12; session marks -77.64) · 8 name(s) marked open→close (per-name table). ATRC×45 09:30 $52.03 → close $51.52 -22.95; CABA×496 09:30 $3.46 → close $3.47 +4.96; ALEC×734 09:30 $2.52 → close $2.46 -44.04; BHC×229 09:30 $6.71 → close $6.56 -34.35; BMEA×649 09:30 $1.90 → close $2.03 +84.37; OABI×193 09:30 $4.78 → close $4.33 -86.85; OPK×387 09:30 $1.59 → close $1.64 +19.35; VIR×25 09:30 $11.31 → close $11.38 +1.87 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.81 | ▲ 09:30 equity $10,440.70 vs yday $10,424.67 (+16.03) | 09:30 open · cash $4.81 (unchanged overnight, no fees) · equity $10,440.70 vs prior close $10,424.67 (+16.03) · 8 name(s) re-marked at the open (per-name table). ATRC×45 yday $51.52 → 09:30 $54.31 +125.55; CABA×496 yday $3.47 → 09:30 $3.43 -19.84; ALEC×734 yday $2.46 → 09:30 $2.38 -58.72; BHC×229 yday $6.56 → 09:30 $6.57 +2.29; BMEA×649 yday $2.03 → 09:30 $2.00 -19.47; OABI×193 yday $4.33 → 09:30 $4.30 -5.79; OPK×387 yday $1.64 → 09:30 $1.63 -3.87; VIR×25 yday $11.38 → 09:30 $11.22 -4.12 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 45 | $54.31 | $2.15 | $+60.07 | $2,446.60 | ▲ +60.07 after sell → book $10,438.54; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 496 | $3.43 | $6.49 | $-112.09 | $4,141.39 | ▼ -112.09 after sell → book $10,432.05; vs 09:30 mark -6.49 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 734 | $2.38 | $9.60 | $-121.83 | $5,878.71 | ▼ -121.83 after sell → book $10,422.45; vs 09:30 mark -9.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 229 | $6.57 | $3.00 | $-38.02 | $7,380.23 | ▼ -38.02 after sell → book $10,419.44; vs 09:30 mark -3.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 649 | $2.00 | $8.49 | $+48.04 | $8,669.74 | ▲ +48.04 after sell → book $10,410.95; vs 09:30 mark -8.49 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 193 | $4.30 | $2.61 | $-97.82 | $9,497.03 | ▼ -97.82 after sell → book $10,408.34; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 387 | $1.63 | $5.07 | $+5.42 | $10,122.77 | ▲ +5.42 after sell → book $10,403.27; vs 09:30 mark -5.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 25 | $11.22 | $2.08 | $-6.40 | $10,401.19 | ▼ -6.40 after sell → book $10,401.19; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,401.19 | ▲ close $10,401.19 vs 09:30 $10,440.70 (session +0.00) | 16:00 close · cash $10,401.19 · no lots left · equity $10,401.19. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,401.19 | ▲ 09:30 equity $10,401.19 vs yday $10,401.19 (-0.00) | 09:30 open · cash $10,401.19 · no holdings · equity $10,401.19 vs prior close $10,401.19 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,401.19 | ▲ close $10,401.19 vs 09:30 $10,401.19 (session +0.00) | 16:00 close · cash $10,401.19 · no lots left · equity $10,401.19. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `MU` | cash | leftover split 306.28 < 1 share @ 967.01 |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CABA` | hard_red | hard-red S=-13.95 sit; no new buys |
