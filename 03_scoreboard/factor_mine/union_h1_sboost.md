# Factor mine action — `union_h1_sboost`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `both` · S≥+5: sizeup + more names

Cash book **+5.00%** ($10,500) · signal-only (no cash/fees) was +3.74%. Starts YES **6/21**. Fills 161 · skips 79 · realized $+516.14.

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
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- On a strong morning (S ≥ +5), spend 1.35× leftover and add 4 extra names — still cash-capped.
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
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8 (S≥+5 may raise this when S-boost is `both`).
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,211.54.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 18 | — | $59.80 | +0.00 | $60.23 | +7.74 | +7.74 | +0.00 | +7.74 |
| 2026-08-13 | `IREN` | 24 | — | $45.98 | +0.00 | $44.76 | -29.28 | -29.28 | +0.00 | -29.28 |
| 2026-08-13 | `TPG` | 21 | — | $50.62 | +0.00 | $54.62 | +83.93 | +83.93 | +0.00 | +83.93 |
| 2026-08-13 | `TGTX` | 22 | — | $49.70 | +0.00 | $47.94 | -38.72 | -38.72 | +0.00 | -38.72 |
| 2026-08-13 | `SLS` | 94 | — | $11.70 | +0.00 | $12.36 | +62.04 | +62.04 | +0.00 | +62.04 |
| 2026-08-13 | `HIMS` | 37 | — | $29.74 | +0.00 | $28.77 | -35.89 | -35.89 | +0.00 | -35.89 |
| 2026-08-13 | `INO` | 1371 | — | $0.81 | +0.00 | $0.90 | +123.39 | +123.39 | +0.00 | +123.39 |
| 2026-08-13 | `TNDM` | 47 | — | $23.33 | +0.00 | $23.13 | -9.40 | -9.40 | +0.00 | -9.40 |
| 2026-08-13 | `VOR` | 50 | — | $22.01 | +0.00 | $23.29 | +64.00 | +64.00 | +0.00 | +64.00 |
| 2026-08-14 | `BTSG` | 18 | $60.23 | $59.65 | -10.44 | — | +0.00 | -10.44 | -2.70 | — |
| 2026-08-14 | `IREN` | 24 | $44.76 | $44.09 | -16.08 | — | +0.00 | -16.08 | -45.36 | — |
| 2026-08-14 | `TPG` | 21 | $54.62 | $55.29 | +14.07 | — | +0.00 | +14.07 | +98.00 | — |
| 2026-08-14 | `TGTX` | 22 | $47.94 | $47.27 | -14.74 | — | +0.00 | -14.74 | -53.46 | — |
| 2026-08-14 | `SLS` | 94 | $12.36 | $12.40 | +3.76 | — | +0.00 | +3.76 | +65.80 | — |
| 2026-08-14 | `HIMS` | 37 | $28.77 | $29.15 | +14.06 | — | +0.00 | +14.06 | -21.83 | — |
| 2026-08-14 | `INO` | 1371 | $0.90 | $0.93 | +41.13 | — | +0.00 | +41.13 | +164.52 | — |
| 2026-08-14 | `TNDM` | 47 | $23.13 | $22.92 | -9.87 | — | +0.00 | -9.87 | -19.27 | — |
| 2026-08-14 | `VOR` | 50 | $23.29 | $23.33 | +2.00 | — | +0.00 | +2.00 | +66.00 | — |
| 2026-08-14 | `TLN` | 2 | — | $359.83 | +0.00 | $362.74 | +5.82 | +5.82 | +0.00 | +5.82 |
| 2026-08-14 | `VST` | 5 | — | $146.90 | +0.00 | $148.13 | +6.15 | +6.15 | +0.00 | +6.15 |
| 2026-08-14 | `NRG` | 7 | — | $120.00 | +0.00 | $126.24 | +43.68 | +43.68 | +0.00 | +43.68 |
| 2026-08-14 | `DAVE` | 2 | — | $330.91 | +0.00 | $334.57 | +7.32 | +7.32 | +0.00 | +7.32 |
| 2026-08-14 | `SLG` | 14 | — | $57.61 | +0.00 | $56.09 | -21.28 | -21.28 | +0.00 | -21.28 |
| 2026-08-14 | `MARA` | 94 | — | $9.01 | +0.00 | $9.20 | +17.86 | +17.86 | +0.00 | +17.86 |
| 2026-08-14 | `LDI` | 905 | — | $0.94 | +0.00 | $0.90 | -36.20 | -36.20 | +0.00 | -36.20 |
| 2026-08-14 | `BTBT` | 565 | — | $1.50 | +0.00 | $1.57 | +39.55 | +39.55 | +0.00 | +39.55 |
| 2026-08-14 | `BETR` | 57 | — | $14.80 | +0.00 | $13.73 | -60.99 | -60.99 | +0.00 | -60.99 |
| 2026-08-14 | `ANGX` | 196 | — | $4.31 | +0.00 | $4.37 | +11.76 | +11.76 | +0.00 | +11.76 |
| 2026-08-14 | `WWW` | 41 | — | $20.60 | +0.00 | $21.03 | +17.63 | +17.63 | +0.00 | +17.63 |
| 2026-08-14 | `HYLN` | 203 | — | $4.18 | +0.00 | $4.06 | -24.36 | -24.36 | +0.00 | -24.36 |
| 2026-08-17 | `TLN` | 2 | $362.74 | $367.88 | +10.28 | — | +0.00 | +10.28 | +16.10 | — |
| 2026-08-17 | `VST` | 5 | $148.13 | $149.37 | +6.20 | — | +0.00 | +6.20 | +12.35 | — |
| 2026-08-17 | `NRG` | 7 | $126.24 | $127.40 | +8.12 | — | +0.00 | +8.12 | +51.80 | — |
| 2026-08-17 | `DAVE` | 2 | $334.57 | $336.94 | +4.74 | — | +0.00 | +4.74 | +12.06 | — |
| 2026-08-17 | `SLG` | 14 | $56.09 | $55.37 | -10.08 | — | +0.00 | -10.08 | -31.36 | — |
| 2026-08-17 | `MARA` | 94 | $9.20 | $9.22 | +1.88 | — | +0.00 | +1.88 | +19.74 | — |
| 2026-08-17 | `LDI` | 905 | $0.90 | $0.91 | +9.05 | — | +0.00 | +9.05 | -27.15 | — |
| 2026-08-17 | `BTBT` | 565 | $1.57 | $1.52 | -28.25 | — | +0.00 | -28.25 | +11.30 | — |
| 2026-08-17 | `BETR` | 57 | $13.73 | $13.67 | -3.42 | — | +0.00 | -3.42 | -64.41 | — |
| 2026-08-17 | `ANGX` | 196 | $4.37 | $4.60 | +45.08 | — | +0.00 | +45.08 | +56.84 | — |
| 2026-08-17 | `WWW` | 41 | $21.03 | $20.98 | -2.05 | — | +0.00 | -2.05 | +15.58 | — |
| 2026-08-17 | `HYLN` | 203 | $4.06 | $4.10 | +8.12 | — | +0.00 | +8.12 | -16.24 | — |
| 2026-08-17 | `DVN` | 27 | — | $46.18 | +0.00 | $47.57 | +37.53 | +37.53 | +0.00 | +37.53 |
| 2026-08-17 | `EOG` | 8 | — | $142.77 | +0.00 | $146.15 | +27.04 | +27.04 | +0.00 | +27.04 |
| 2026-08-17 | `FANG` | 6 | — | $202.70 | +0.00 | $206.29 | +21.54 | +21.54 | +0.00 | +21.54 |
| 2026-08-17 | `TMC` | 313 | — | $4.05 | +0.00 | $3.77 | -87.64 | -87.64 | +0.00 | -87.64 |
| 2026-08-17 | `TGB` | 150 | — | $8.46 | +0.00 | $8.77 | +46.50 | +46.50 | +0.00 | +46.50 |
| 2026-08-17 | `ELF` | 14 | — | $90.54 | +0.00 | $93.66 | +43.68 | +43.68 | +0.00 | +43.68 |
| 2026-08-17 | `DNN` | 392 | — | $3.24 | +0.00 | $3.19 | -19.60 | -19.60 | +0.00 | -19.60 |
| 2026-08-17 | `HNST` | 264 | — | $4.81 | +0.00 | $4.70 | -29.04 | -29.04 | +0.00 | -29.04 |
| 2026-08-18 | `DVN` | 27 | $47.57 | $48.00 | +11.61 | — | +0.00 | +11.61 | +49.14 | — |
| 2026-08-18 | `EOG` | 8 | $146.15 | $148.04 | +15.12 | — | +0.00 | +15.12 | +42.16 | — |
| 2026-08-18 | `FANG` | 6 | $206.29 | $208.93 | +15.84 | — | +0.00 | +15.84 | +37.38 | — |
| 2026-08-18 | `TMC` | 313 | $3.77 | $3.72 | -15.65 | — | +0.00 | -15.65 | -103.29 | — |
| 2026-08-18 | `TGB` | 150 | $8.77 | $8.55 | -33.00 | — | +0.00 | -33.00 | +13.50 | — |
| 2026-08-18 | `ELF` | 14 | $93.66 | $93.44 | -3.08 | — | +0.00 | -3.08 | +40.60 | — |
| 2026-08-18 | `DNN` | 392 | $3.19 | $3.11 | -31.36 | — | +0.00 | -31.36 | -50.96 | — |
| 2026-08-18 | `HNST` | 264 | $4.70 | $4.67 | -7.92 | — | +0.00 | -7.92 | -36.96 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 61 | — | $20.55 | +0.00 | $21.19 | +39.04 | +39.04 | +0.00 | +39.04 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 61 | — | $20.65 | +0.00 | $21.11 | +28.06 | +28.06 | +0.00 | +28.06 |
| 2026-08-20 | `HDSN` | 218 | — | $5.77 | +0.00 | $5.57 | -43.60 | -43.60 | +0.00 | -43.60 |
| 2026-08-20 | `IAG` | 64 | — | $19.63 | +0.00 | $20.50 | +55.68 | +55.68 | +0.00 | +55.68 |
| 2026-08-20 | `KGC` | 42 | — | $29.63 | +0.00 | $31.43 | +75.60 | +75.60 | +0.00 | +75.60 |
| 2026-08-20 | `NFGC` | 721 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 61 | $21.19 | $21.90 | +43.31 | — | +0.00 | +43.31 | +82.35 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `CDE` | 61 | $21.11 | $21.75 | +39.04 | — | +0.00 | +39.04 | +67.10 | — |
| 2026-08-21 | `HDSN` | 218 | $5.57 | $5.67 | +21.80 | — | +0.00 | +21.80 | -21.80 | — |
| 2026-08-21 | `IAG` | 64 | $20.50 | $21.17 | +42.88 | — | +0.00 | +42.88 | +98.56 | — |
| 2026-08-21 | `KGC` | 42 | $31.43 | $32.17 | +31.08 | — | +0.00 | +31.08 | +106.68 | — |
| 2026-08-21 | `NFGC` | 721 | $1.75 | $1.79 | +28.84 | — | +0.00 | +28.84 | +28.84 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 11 | — | $119.43 | +0.00 | $121.22 | +19.69 | +19.69 | +0.00 | +19.69 |
| 2026-08-21 | `AUPH` | 76 | — | $17.20 | +0.00 | $16.65 | -41.80 | -41.80 | +0.00 | -41.80 |
| 2026-08-21 | `AEM` | 6 | — | $216.30 | +0.00 | $216.06 | -1.44 | -1.44 | +0.00 | -1.44 |
| 2026-08-21 | `ARCT` | 118 | — | $11.13 | +0.00 | $13.45 | +273.76 | +273.76 | +0.00 | +273.76 |
| 2026-08-21 | `AUTL` | 534 | — | $2.47 | +0.00 | $2.41 | -32.04 | -32.04 | +0.00 | -32.04 |
| 2026-08-21 | `CRDL` | 683 | — | $1.93 | +0.00 | $1.86 | -47.81 | -47.81 | +0.00 | -47.81 |
| 2026-08-21 | `CRSP` | 22 | — | $59.72 | +0.00 | $59.50 | -4.84 | -4.84 | +0.00 | -4.84 |
| 2026-08-21 | `CYPH` | 1000 | — | $1.32 | +0.00 | $1.42 | +100.00 | +100.00 | +0.00 | +100.00 |
| 2026-08-24 | `AU` | 11 | $121.22 | $120.51 | -7.81 | — | +0.00 | -7.81 | +11.88 | — |
| 2026-08-24 | `AUPH` | 76 | $16.65 | $16.57 | -6.08 | — | +0.00 | -6.08 | -47.88 | — |
| 2026-08-24 | `AEM` | 6 | $216.06 | $217.03 | +5.82 | — | +0.00 | +5.82 | +4.38 | — |
| 2026-08-24 | `ARCT` | 118 | $13.45 | $13.33 | -14.16 | — | +0.00 | -14.16 | +259.60 | — |
| 2026-08-24 | `AUTL` | 534 | $2.41 | $2.40 | -5.34 | — | +0.00 | -5.34 | -37.38 | — |
| 2026-08-24 | `CRDL` | 683 | $1.86 | $1.88 | +13.66 | — | +0.00 | +13.66 | -34.15 | — |
| 2026-08-24 | `CRSP` | 22 | $59.50 | $58.75 | -16.50 | — | +0.00 | -16.50 | -21.34 | — |
| 2026-08-24 | `CYPH` | 1000 | $1.42 | $1.83 | +410.00 | — | +0.00 | +410.00 | +510.00 | — |
| 2026-08-25 | `MOS` | 58 | — | $23.77 | +0.00 | $24.27 | +29.00 | +29.00 | +0.00 | +29.00 |
| 2026-08-25 | `OCUL` | 126 | — | $10.98 | +0.00 | $10.88 | -12.60 | -12.60 | +0.00 | -12.60 |
| 2026-08-25 | `INSP` | 22 | — | $61.19 | +0.00 | $61.07 | -2.64 | -2.64 | +0.00 | -2.64 |
| 2026-08-25 | `CRMD` | 166 | — | $8.35 | +0.00 | $8.56 | +34.86 | +34.86 | +0.00 | +34.86 |
| 2026-08-25 | `RZLT` | 281 | — | $4.94 | +0.00 | $5.01 | +19.67 | +19.67 | +0.00 | +19.67 |
| 2026-08-25 | `HCA` | 3 | — | $426.97 | +0.00 | $428.76 | +5.37 | +5.37 | +0.00 | +5.37 |
| 2026-08-25 | `CAPR` | 191 | — | $7.25 | +0.00 | $8.29 | +198.64 | +198.64 | +0.00 | +198.64 |
| 2026-08-25 | `SAFX` | 3885 | — | $0.36 | +0.00 | $0.35 | -15.54 | -15.54 | +0.00 | -15.54 |
| 2026-08-26 | `MOS` | 58 | $24.27 | $24.84 | +33.06 | $24.16 | -39.44 | -6.38 | +62.06 | +22.62 |
| 2026-08-26 | `OCUL` | 126 | $10.88 | $10.79 | -11.34 | $10.77 | -2.52 | -13.86 | -23.94 | -26.46 |
| 2026-08-26 | `INSP` | 22 | $61.07 | $60.07 | -22.00 | $61.80 | +38.06 | +16.06 | -24.64 | +13.42 |
| 2026-08-26 | `CRMD` | 166 | $8.56 | $8.60 | +6.64 | $8.39 | -34.86 | -28.22 | +41.50 | +6.64 |
| 2026-08-26 | `RZLT` | 281 | $5.01 | $5.01 | +0.00 | $5.04 | +8.43 | +8.43 | +19.67 | +28.10 |
| 2026-08-26 | `HCA` | 3 | $428.76 | $427.50 | -3.78 | $427.16 | -1.02 | -4.80 | +1.59 | +0.57 |
| 2026-08-26 | `CAPR` | 191 | $8.29 | $8.29 | +0.00 | — | +0.00 | +0.00 | +198.64 | — |
| 2026-08-26 | `SAFX` | 3885 | $0.35 | $0.35 | -3.89 | — | +0.00 | -3.89 | -19.43 | — |
| 2026-08-26 | `AVBP` | 49 | — | $31.21 | +0.00 | $31.14 | -3.43 | -3.43 | +0.00 | -3.43 |
| 2026-08-26 | `FLNC` | 138 | — | $11.12 | +0.00 | $11.08 | -5.52 | -5.52 | +0.00 | -5.52 |
| 2026-08-27 | `MOS` | 58 | $24.16 | $24.00 | -9.28 | $23.76 | -13.92 | -23.20 | +13.34 | -0.58 |
| 2026-08-27 | `OCUL` | 126 | $10.77 | $10.63 | -17.64 | — | +0.00 | -17.64 | -44.10 | — |
| 2026-08-27 | `INSP` | 22 | $61.80 | $62.10 | +6.60 | — | +0.00 | +6.60 | +20.02 | — |
| 2026-08-27 | `CRMD` | 166 | $8.39 | $8.49 | +16.60 | — | +0.00 | +16.60 | +23.24 | — |
| 2026-08-27 | `RZLT` | 281 | $5.04 | $5.07 | +8.43 | — | +0.00 | +8.43 | +36.53 | — |
| 2026-08-27 | `HCA` | 3 | $427.16 | $424.61 | -7.65 | — | +0.00 | -7.65 | -7.08 | — |
| 2026-08-27 | `AVBP` | 49 | $31.14 | $30.79 | -17.15 | — | +0.00 | -17.15 | -20.58 | — |
| 2026-08-27 | `FLNC` | 138 | $11.08 | $11.52 | +60.72 | — | +0.00 | +60.72 | +55.20 | — |
| 2026-08-27 | `RRC` | 34 | — | $41.44 | +0.00 | $41.64 | +6.80 | +6.80 | +0.00 | +6.80 |
| 2026-08-27 | `CRK` | 98 | — | $14.42 | +0.00 | $14.62 | +19.60 | +19.60 | +0.00 | +19.60 |
| 2026-08-27 | `SLI` | 543 | — | $2.60 | +0.00 | $2.64 | +21.72 | +21.72 | +0.00 | +21.72 |
| 2026-08-27 | `ACMR` | 17 | — | $81.65 | +0.00 | $80.49 | -19.72 | -19.72 | +0.00 | -19.72 |
| 2026-08-27 | `GGB` | 309 | — | $4.57 | +0.00 | $4.70 | +40.17 | +40.17 | +0.00 | +40.17 |
| 2026-08-27 | `MT` | 18 | — | $74.54 | +0.00 | $74.63 | +1.62 | +1.62 | +0.00 | +1.62 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-28 | `MOS` | 58 | $23.76 | $23.95 | +11.02 | $23.60 | -20.30 | -9.28 | +10.44 | -9.86 |
| 2026-08-28 | `RRC` | 34 | $41.64 | $41.74 | +3.40 | $41.46 | -9.52 | -6.12 | +10.20 | +0.68 |
| 2026-08-28 | `CRK` | 98 | $14.62 | $14.63 | +0.98 | $14.29 | -33.32 | -32.34 | +20.58 | -12.74 |
| 2026-08-28 | `SLI` | 543 | $2.64 | $2.68 | +21.72 | $2.55 | -70.59 | -48.87 | +43.44 | -27.15 |
| 2026-08-28 | `ACMR` | 17 | $80.49 | $79.27 | -20.74 | — | +0.00 | -20.74 | -40.46 | — |
| 2026-08-28 | `GGB` | 309 | $4.70 | $4.67 | -9.27 | — | +0.00 | -9.27 | +30.90 | — |
| 2026-08-28 | `MT` | 18 | $74.63 | $75.39 | +13.68 | — | +0.00 | +13.68 | +15.30 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `SEDG` | 42 | — | $32.90 | +0.00 | $31.41 | -62.58 | -62.58 | +0.00 | -62.58 |
| 2026-08-28 | `GRRR` | 89 | — | $15.66 | +0.00 | $14.41 | -111.25 | -111.25 | +0.00 | -111.25 |
| 2026-08-28 | `URBN` | 17 | — | $79.42 | +0.00 | $81.09 | +28.39 | +28.39 | +0.00 | +28.39 |
| 2026-08-28 | `PYXS` | 420 | — | $3.32 | +0.00 | $3.23 | -37.80 | -37.80 | +0.00 | -37.80 |
| 2026-08-31 | `MOS` | 58 | $23.60 | $23.68 | +4.64 | — | +0.00 | +4.64 | -5.22 | — |
| 2026-08-31 | `RRC` | 34 | $41.46 | $42.00 | +18.36 | — | +0.00 | +18.36 | +19.04 | — |
| 2026-08-31 | `CRK` | 98 | $14.29 | $14.54 | +24.50 | — | +0.00 | +24.50 | +11.76 | — |
| 2026-08-31 | `SLI` | 543 | $2.55 | $2.58 | +16.29 | — | +0.00 | +16.29 | -10.86 | — |
| 2026-08-31 | `SEDG` | 42 | $31.41 | $31.15 | -10.92 | — | +0.00 | -10.92 | -73.50 | — |
| 2026-08-31 | `GRRR` | 89 | $14.41 | $14.44 | +2.67 | — | +0.00 | +2.67 | -108.58 | — |
| 2026-08-31 | `URBN` | 17 | $81.09 | $80.44 | -11.05 | — | +0.00 | -11.05 | +17.34 | — |
| 2026-08-31 | `PYXS` | 420 | $3.23 | $3.20 | -12.60 | — | +0.00 | -12.60 | -50.40 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 25 | — | $52.88 | +0.00 | $52.46 | -10.50 | -10.50 | +0.00 | -10.50 |
| 2026-09-03 | `HRMY` | 31 | — | $42.93 | +0.00 | $41.86 | -33.17 | -33.17 | +0.00 | -33.17 |
| 2026-09-03 | `CABA` | 377 | — | $3.63 | +0.00 | $3.48 | -56.55 | -56.55 | +0.00 | -56.55 |
| 2026-09-03 | `VSTM` | 170 | — | $8.03 | +0.00 | $7.98 | -8.50 | -8.50 | +0.00 | -8.50 |
| 2026-09-03 | `RVTY` | 10 | — | $132.45 | +0.00 | $130.63 | -18.20 | -18.20 | +0.00 | -18.20 |
| 2026-09-03 | `CRK` | 88 | — | $15.45 | +0.00 | $14.95 | -44.00 | -44.00 | +0.00 | -44.00 |
| 2026-09-03 | `MRNA` | 9 | — | $145.94 | +0.00 | $148.87 | +26.33 | +26.33 | +0.00 | +26.33 |
| 2026-09-03 | `ARCT` | 81 | — | $16.77 | +0.00 | $15.56 | -98.01 | -98.01 | +0.00 | -98.01 |
| 2026-09-04 | `ATRC` | 25 | $52.46 | $52.03 | -10.75 | $51.52 | -12.75 | -23.50 | -21.25 | -34.00 |
| 2026-09-04 | `HRMY` | 31 | $41.86 | $41.50 | -11.16 | — | +0.00 | -11.16 | -44.33 | — |
| 2026-09-04 | `CABA` | 377 | $3.48 | $3.46 | -7.54 | $3.47 | +3.77 | -3.77 | -64.09 | -60.32 |
| 2026-09-04 | `VSTM` | 170 | $7.98 | $7.91 | -11.90 | — | +0.00 | -11.90 | -20.40 | — |
| 2026-09-04 | `RVTY` | 10 | $130.63 | $130.03 | -6.00 | — | +0.00 | -6.00 | -24.20 | — |
| 2026-09-04 | `CRK` | 88 | $14.95 | $15.00 | +4.40 | — | +0.00 | +4.40 | -39.60 | — |
| 2026-09-04 | `MRNA` | 9 | $148.87 | $153.62 | +42.75 | — | +0.00 | +42.75 | +69.08 | — |
| 2026-09-04 | `ARCT` | 81 | $15.56 | $15.61 | +4.05 | — | +0.00 | +4.05 | -93.96 | — |
| 2026-09-04 | `ALEC` | 534 | — | $2.52 | +0.00 | $2.46 | -32.04 | -32.04 | +0.00 | -32.04 |
| 2026-09-04 | `BHC` | 200 | — | $6.71 | +0.00 | $6.56 | -30.00 | -30.00 | +0.00 | -30.00 |
| 2026-09-04 | `BMEA` | 709 | — | $1.90 | +0.00 | $2.03 | +92.17 | +92.17 | +0.00 | +92.17 |
| 2026-09-04 | `OABI` | 281 | — | $4.78 | +0.00 | $4.33 | -126.45 | -126.45 | +0.00 | -126.45 |
| 2026-09-04 | `OPK` | 847 | — | $1.59 | +0.00 | $1.64 | +42.35 | +42.35 | +0.00 | +42.35 |
| 2026-09-04 | `VIR` | 117 | — | $11.31 | +0.00 | $11.38 | +8.77 | +8.77 | +0.00 | +8.77 |
| 2026-09-08 | `ATRC` | 25 | $51.52 | $54.31 | +69.75 | — | +0.00 | +69.75 | +35.75 | — |
| 2026-09-08 | `CABA` | 377 | $3.47 | $3.43 | -15.08 | — | +0.00 | -15.08 | -75.40 | — |
| 2026-09-08 | `ALEC` | 534 | $2.46 | $2.38 | -42.72 | — | +0.00 | -42.72 | -74.76 | — |
| 2026-09-08 | `BHC` | 200 | $6.56 | $6.57 | +2.00 | — | +0.00 | +2.00 | -28.00 | — |
| 2026-09-08 | `BMEA` | 709 | $2.03 | $2.00 | -21.27 | — | +0.00 | -21.27 | +70.90 | — |
| 2026-09-08 | `OABI` | 281 | $4.33 | $4.30 | -8.43 | — | +0.00 | -8.43 | -134.88 | — |
| 2026-09-08 | `OPK` | 847 | $1.64 | $1.63 | -8.47 | — | +0.00 | -8.47 | +33.88 | — |
| 2026-09-08 | `VIR` | 117 | $11.38 | $11.22 | -19.30 | — | +0.00 | -19.30 | -10.53 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `AUPH` | 80 | — | $16.28 | +0.00 | $16.10 | -14.40 | -14.40 | +0.00 | -14.40 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +227.81 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM, VOR | — | $123.82 | $10,195.74 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50 |
| 2026-08-14 | +5.50 | $123.82 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50 | $10,219.63 | +23.89 | +6.94 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT, BETR, ANGX, WWW, HYLN | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM, VOR | $458.79 | $10,152.17 | TLN×2, VST×5, NRG×7, DAVE×2, SLG×14, MARA×94, LDI×905, BTBT×565, BETR×57, ANGX×196, WWW×41, HYLN×203 |
| 2026-08-17 | +2.25 | $458.79 | TLN×2, VST×5, NRG×7, DAVE×2, SLG×14, MARA×94, LDI×905, BTBT×565, BETR×57, ANGX×196, WWW×41, HYLN×203 | $10,201.84 | +49.67 | +40.01 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT, BETR, ANGX, WWW, HYLN | $188.92 | $10,178.28 | DVN×27, EOG×8, FANG×6, TMC×313, TGB×150, ELF×14, DNN×392, HNST×264 |
| 2026-08-18 | -6.20 | $188.92 | DVN×27, EOG×8, FANG×6, TMC×313, TGB×150, ELF×14, DNN×392, HNST×264 | $10,129.84 | -48.44 | +0.00 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | $10,106.47 | $10,106.47 | — |
| 2026-08-19 | -7.20 | $10,106.47 | — | $10,106.47 | -0.00 | +0.00 | — | — | $10,106.47 | $10,106.47 | — |
| 2026-08-20 | +1.12 | $10,106.47 | — | $10,106.47 | -0.00 | +234.52 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $208.63 | $10,316.19 | AG×61, BHP×13, CDE×61, HDSN×218, IAG×64, KGC×42, NFGC×721, WPM×8 |
| 2026-08-21 | +3.25 | $208.63 | AG×61, BHP×13, CDE×61, HDSN×218, IAG×64, KGC×42, NFGC×721, WPM×8 | $10,585.91 | +269.72 | +265.52 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $18.48 | $10,787.08 | AU×11, AUPH×76, AEM×6, ARCT×118, AUTL×534, CRDL×683, CRSP×22, CYPH×1000 |
| 2026-08-24 | -5.17 | $18.48 | AU×11, AUPH×76, AEM×6, ARCT×118, AUTL×534, CRDL×683, CRSP×22, CYPH×1000 | $11,166.67 | +379.59 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $11,126.90 | $11,126.90 | — |
| 2026-08-25 | +1.80 | $11,126.90 | — | $11,126.90 | +0.00 | +256.76 | MOS, OCUL, INSP, CRMD, RZLT, HCA, CAPR, SAFX | — | $145.02 | $11,340.83 | MOS×58, OCUL×126, INSP×22, CRMD×166, RZLT×281, HCA×3, CAPR×191, SAFX×3885 |
| 2026-08-26 | +2.02 | $145.02 | MOS×58, OCUL×126, INSP×22, CRMD×166, RZLT×281, HCA×3, CAPR×191, SAFX×3885 | $11,339.53 | -1.30 | -40.30 | AVBP, FLNC | CAPR, SAFX | $2.80 | $11,266.06 | MOS×58, OCUL×126, INSP×22, CRMD×166, RZLT×281, HCA×3, AVBP×49, FLNC×138 |
| 2026-08-27 | — | $2.80 | MOS×58, OCUL×126, INSP×22, CRMD×166, RZLT×281, HCA×3, AVBP×49, FLNC×138 | $11,306.69 | +40.63 | +24.65 | RRC, CRK, SLI, ACMR, GGB, MT, MU | OCUL, INSP, CRMD, RZLT, HCA, AVBP, FLNC | $533.11 | $11,292.59 | MOS×58, RRC×34, CRK×98, SLI×543, ACMR×17, GGB×309, MT×18, MU×1 |
| 2026-08-28 | +0.75 | $533.11 | MOS×58, RRC×34, CRK×98, SLI×543, ACMR×17, GGB×309, MT×18, MU×1 | $11,297.28 | +4.69 | -316.97 | SEDG, GRRR, URBN, PYXS | ACMR, GGB, MT, MU | $57.94 | $10,958.29 | MOS×58, RRC×34, CRK×98, SLI×543, SEDG×42, GRRR×89, URBN×17, PYXS×420 |
| 2026-08-31 | -5.85 | $57.94 | MOS×58, RRC×34, CRK×98, SLI×543, SEDG×42, GRRR×89, URBN×17, PYXS×420 | $10,990.18 | +31.89 | +0.00 | — | MOS, RRC, CRK, SLI, SEDG, GRRR, URBN, PYXS | $10,964.48 | $10,964.48 | — |
| 2026-09-01 | -6.30 | $10,964.48 | — | $10,964.48 | +0.00 | +0.00 | — | — | $10,964.48 | $10,964.48 | — |
| 2026-09-02 | -3.83 | $10,964.48 | — | $10,964.48 | +0.00 | +0.00 | — | — | $10,964.48 | $10,964.48 | — |
| 2026-09-03 | -0.90 | $10,964.48 | — | $10,964.48 | +0.00 | -242.60 | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MRNA, ARCT | — | $202.03 | $10,701.84 | ATRC×25, HRMY×31, CABA×377, VSTM×170, RVTY×10, CRK×88, MRNA×9, ARCT×81 |
| 2026-09-04 | +2.25 | $202.03 | ATRC×25, HRMY×31, CABA×377, VSTM×170, RVTY×10, CRK×88, MRNA×9, ARCT×81 | $10,705.69 | +3.85 | -54.18 | ALEC, BHC, BMEA, OABI, OPK, VIR | HRMY, VSTM, RVTY, CRK, MRNA, ARCT | $3.79 | $10,602.74 | ATRC×25, CABA×377, ALEC×534, BHC×200, BMEA×709, OABI×281, OPK×847, VIR×117 |
| 2026-09-08 | -11.47 | $3.79 | ATRC×25, CABA×377, ALEC×534, BHC×200, BMEA×709, OABI×281, OPK×847, VIR×117 | $10,559.22 | -43.52 | +0.00 | — | ATRC, CABA, ALEC, BHC, BMEA, OABI, OPK, VIR | $10,516.17 | $10,516.17 | — |
| 2026-09-09 | -13.95 | $10,516.17 | — | $10,516.17 | -0.00 | +0.00 | — | — | $10,516.17 | $10,516.17 | — |
| 2026-09-10 | -13.28 | $10,516.17 | — | $10,516.17 | -0.00 | +0.00 | — | — | $10,516.17 | $10,516.17 | — |
| 2026-09-11 | +0.50 | $10,516.17 | — | $10,516.17 | -0.00 | -14.40 | AUPH | — | $9,211.54 | $10,499.54 | AUPH×80 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 18 | $59.80 | $2.04 | — | $8,921.56 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-5.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 24 | $45.98 | $2.06 | — | $7,815.97 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+12.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 21 | $50.62 | $2.05 | — | $6,750.83 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+6.2; leftover $1111.11 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 22 | $49.70 | $2.06 | — | $5,655.38 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-0.8; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $4,553.31 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-0.8; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 37 | $29.74 | $2.10 | — | $3,450.82 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-5.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1371 | $0.81 | $15.22 | — | $2,325.10 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+13.2; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 47 | $23.33 | $2.13 | — | $1,226.46 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+19.7; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 50 | $22.01 | $2.14 | — | $123.82 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+0.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.82 | ▲ close $10,195.74 vs 09:30 $10,000.00 (session +227.81) | 16:00 close · cash $123.82 · equity $10,195.74 vs 09:30 $10,000.00 (+195.74; session marks +227.81) · 9 name(s) marked open→close (per-name table). BTSG×18 09:30 $59.80 → close $60.23 +7.74; IREN×24 09:30 $45.98 → close $44.76 -29.28; TPG×21 09:30 $50.62 → close $54.62 +83.93; TGTX×22 09:30 $49.70 → close $47.94 -38.72; SLS×94 09:30 $11.70 → close $12.36 +62.04; HIMS×37 09:30 $29.74 → close $28.77 -35.89; INO×1371 09:30 $0.81 → close $0.90 +123.39; TNDM×47 09:30 $23.33 → close $23.13 -9.40; VOR×50 09:30 $22.01 → close $23.29 +64.00 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.82 | ▲ 09:30 equity $10,219.63 vs yday $10,195.74 (+23.89) | 09:30 open · cash $123.82 (unchanged overnight, no fees) · equity $10,219.63 vs prior close $10,195.74 (+23.89) · 9 name(s) re-marked at the open (per-name table). BTSG×18 yday $60.23 → 09:30 $59.65 -10.44; IREN×24 yday $44.76 → 09:30 $44.09 -16.08; TPG×21 yday $54.62 → 09:30 $55.29 +14.07; TGTX×22 yday $47.94 → 09:30 $47.27 -14.74; SLS×94 yday $12.36 → 09:30 $12.40 +3.76; HIMS×37 yday $28.77 → 09:30 $29.15 +14.06; INO×1371 yday $0.90 → 09:30 $0.93 +41.13; TNDM×47 yday $23.13 → 09:30 $22.92 -9.87; VOR×50 yday $23.29 → 09:30 $23.33 +2.00 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 18 | $59.65 | $2.06 | $-6.81 | $1,195.45 | ▼ -6.81 after sell → book $10,217.56; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 24 | $44.09 | $2.08 | $-49.50 | $2,251.53 | ▼ -49.50 after sell → book $10,215.48; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 21 | $55.29 | $2.07 | $+93.88 | $3,410.55 | ▲ +93.88 after sell → book $10,213.41; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 22 | $47.27 | $2.08 | $-57.59 | $4,448.41 | ▼ -57.59 after sell → book $10,211.33; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 94 | $12.40 | $2.30 | $+61.23 | $5,611.71 | ▲ +61.23 after sell → book $10,209.03; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 37 | $29.15 | $2.12 | $-26.05 | $6,688.14 | ▼ -26.05 after sell → book $10,206.91; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1371 | $0.93 | $17.10 | $+132.20 | $7,946.07 | ▲ +132.20 after sell → book $10,189.81; vs 09:30 mark -17.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 47 | $22.92 | $2.15 | $-23.55 | $9,021.16 | ▼ -23.55 after sell → book $10,187.66; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 50 | $23.33 | $2.16 | $+61.70 | $10,185.50 | ▲ +61.70 after sell → book $10,185.50; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 2 | $359.83 | $2.00 | — | $9,463.84 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+5.9; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 5 | $146.90 | $2.00 | — | $8,727.34 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+3.6; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 7 | $120.00 | $2.01 | — | $7,885.33 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+0.6; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 2 | $330.91 | $2.00 | — | $7,221.51 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-8.6; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 14 | $57.61 | $2.03 | — | $6,412.94 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+5.7; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 94 | $9.01 | $2.27 | — | $5,563.73 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-13.5; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 905 | $0.94 | $11.19 | — | $4,704.55 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.5; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 565 | $1.50 | $7.29 | — | $3,849.76 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+9.2; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 57 | $14.80 | $2.16 | — | $3,004.00 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-9.9; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 196 | $4.31 | $2.58 | — | $2,156.66 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WWW` | 41 | $20.60 | $2.11 | — | $1,309.95 | — | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=+4.4; leftover $848.79 | join🟢 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 203 | $4.18 | $2.62 | — | $458.79 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $458.79 | ▲ close $10,152.17 vs 09:30 $10,219.63 (session +6.94) | 16:00 close · cash $458.79 · equity $10,152.17 vs 09:30 $10,219.63 (-67.46; session marks +6.94) · 12 name(s) marked open→close (per-name table). TLN×2 09:30 $359.83 → close $362.74 +5.82; VST×5 09:30 $146.90 → close $148.13 +6.15; NRG×7 09:30 $120.00 → close $126.24 +43.68; DAVE×2 09:30 $330.91 → close $334.57 +7.32; SLG×14 09:30 $57.61 → close $56.09 -21.28; MARA×94 09:30 $9.01 → close $9.20 +17.86; LDI×905 09:30 $0.94 → close $0.90 -36.20; BTBT×565 09:30 $1.50 → close $1.57 +39.55; BETR×57 09:30 $14.80 → close $13.73 -60.99; ANGX×196 09:30 $4.31 → close $4.37 +11.76; WWW×41 09:30 $20.60 → close $21.03 +17.63; HYLN×203 09:30 $4.18 → close $4.06 -24.36 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $458.79 | ▲ 09:30 equity $10,201.84 vs yday $10,152.17 (+49.67) | 09:30 open · cash $458.79 (unchanged overnight, no fees) · equity $10,201.84 vs prior close $10,152.17 (+49.67) · 12 name(s) re-marked at the open (per-name table). TLN×2 yday $362.74 → 09:30 $367.88 +10.28; VST×5 yday $148.13 → 09:30 $149.37 +6.20; NRG×7 yday $126.24 → 09:30 $127.40 +8.12; DAVE×2 yday $334.57 → 09:30 $336.94 +4.74; SLG×14 yday $56.09 → 09:30 $55.37 -10.08; MARA×94 yday $9.20 → 09:30 $9.22 +1.88; LDI×905 yday $0.90 → 09:30 $0.91 +9.05; BTBT×565 yday $1.57 → 09:30 $1.52 -28.25; BETR×57 yday $13.73 → 09:30 $13.67 -3.42; ANGX×196 yday $4.37 → 09:30 $4.60 +45.08; WWW×41 yday $21.03 → 09:30 $20.98 -2.05; HYLN×203 yday $4.06 → 09:30 $4.10 +8.12 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 2 | $367.88 | $2.02 | $+12.09 | $1,192.53 | ▲ +12.09 after sell → book $10,199.83; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 5 | $149.37 | $2.02 | $+8.32 | $1,937.36 | ▲ +8.32 after sell → book $10,197.80; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 7 | $127.40 | $2.03 | $+47.76 | $2,827.13 | ▲ +47.76 after sell → book $10,195.77; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 2 | $336.94 | $2.02 | $+8.05 | $3,498.99 | ▲ +8.05 after sell → book $10,193.76; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 14 | $55.37 | $2.05 | $-35.44 | $4,272.12 | ▼ -35.44 after sell → book $10,191.70; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 94 | $9.22 | $2.30 | $+15.17 | $5,136.50 | ▲ +15.17 after sell → book $10,189.41; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 905 | $0.91 | $11.08 | $-49.43 | $5,946.25 | ▼ -49.43 after sell → book $10,178.32; vs 09:30 mark -11.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 565 | $1.52 | $7.39 | $-3.38 | $6,797.66 | ▼ -3.38 after sell → book $10,170.93; vs 09:30 mark -7.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 57 | $13.67 | $2.18 | $-68.75 | $7,574.67 | ▼ -68.75 after sell → book $10,168.75; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 196 | $4.60 | $2.62 | $+51.64 | $8,473.65 | ▲ +51.64 after sell → book $10,166.13; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WWW` | 41 | $20.98 | $2.13 | $+11.33 | $9,331.70 | ▲ +11.33 after sell → book $10,164.00; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 203 | $4.10 | $2.66 | $-21.52 | $10,161.33 | ▼ -21.52 after sell → book $10,161.33; vs 09:30 mark -2.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,912.40 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+6.7; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,768.23 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+5.8; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,550.02 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+8.3; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 313 | $4.05 | $4.04 | — | $5,278.33 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 150 | $8.46 | $2.44 | — | $4,006.89 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $2,737.30 | — | S≥+5: sizeup + more names; list flatten; ret5=-7.2; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 392 | $3.24 | $5.06 | — | $1,462.16 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+0.3; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 264 | $4.81 | $3.41 | — | $188.92 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-11.4; leftover $1270.17 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $188.92 | ▲ close $10,178.28 vs 09:30 $10,201.84 (session +40.01) | 16:00 close · cash $188.92 · equity $10,178.28 vs 09:30 $10,201.84 (-23.56; session marks +40.01) · 8 name(s) marked open→close (per-name table). DVN×27 09:30 $46.18 → close $47.57 +37.53; EOG×8 09:30 $142.77 → close $146.15 +27.04; FANG×6 09:30 $202.70 → close $206.29 +21.54; TMC×313 09:30 $4.05 → close $3.77 -87.64; TGB×150 09:30 $8.46 → close $8.77 +46.50; ELF×14 09:30 $90.54 → close $93.66 +43.68; DNN×392 09:30 $3.24 → close $3.19 -19.60; HNST×264 09:30 $4.81 → close $4.70 -29.04 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $188.92 | ▼ 09:30 equity $10,129.84 vs yday $10,178.28 (-48.44) | 09:30 open · cash $188.92 (unchanged overnight, no fees) · equity $10,129.84 vs prior close $10,178.28 (-48.44) · 8 name(s) re-marked at the open (per-name table). DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; TMC×313 yday $3.77 → 09:30 $3.72 -15.65; TGB×150 yday $8.77 → 09:30 $8.55 -33.00; ELF×14 yday $93.66 → 09:30 $93.44 -3.08; DNN×392 yday $3.19 → 09:30 $3.11 -31.36; HNST×264 yday $4.70 → 09:30 $4.67 -7.92 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,482.83 | ▲ +44.98 after sell → book $10,127.75; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,665.11 | ▲ +38.11 after sell → book $10,125.71; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,916.67 | ▲ +33.34 after sell → book $10,123.69; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 313 | $3.72 | $4.10 | $-111.43 | $5,076.93 | ▼ -111.43 after sell → book $10,119.59; vs 09:30 mark -4.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 150 | $8.55 | $2.48 | $+8.58 | $6,356.95 | ▲ +8.58 after sell → book $10,117.11; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 14 | $93.44 | $2.05 | $+36.52 | $7,663.06 | ▲ +36.52 after sell → book $10,115.06; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 392 | $3.11 | $5.13 | $-61.15 | $8,877.05 | ▼ -61.15 after sell → book $10,109.93; vs 09:30 mark -5.13 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 264 | $4.67 | $3.46 | $-43.83 | $10,106.47 | ▼ -43.83 after sell → book $10,106.47; vs 09:30 mark -3.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,106.47 | ▲ close $10,106.47 vs 09:30 $10,129.84 (session +0.00) | 16:00 close · cash $10,106.47 · no lots left · equity $10,106.47. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,106.47 | ▲ 09:30 equity $10,106.47 vs yday $10,106.47 (-0.00) | 09:30 open · cash $10,106.47 · no holdings · equity $10,106.47 vs prior close $10,106.47 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,106.47 | ▲ close $10,106.47 vs 09:30 $10,106.47 (session +0.00) | 16:00 close · cash $10,106.47 · no lots left · equity $10,106.47. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,106.47 | ▲ 09:30 equity $10,106.47 vs yday $10,106.47 (-0.00) | 09:30 open · cash $10,106.47 · no holdings · equity $10,106.47 vs prior close $10,106.47 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,850.74 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,665.58 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 61 | $20.65 | $2.17 | — | $6,403.76 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 218 | $5.77 | $2.81 | — | $5,143.09 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $3,884.59 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,638.01 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 721 | $1.75 | $9.30 | — | $1,366.96 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $208.63 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $208.63 | ▲ close $10,316.19 vs 09:30 $10,106.47 (session +234.52) | 16:00 close · cash $208.63 · equity $10,316.19 vs 09:30 $10,106.47 (+209.72; session marks +234.52) · 8 name(s) marked open→close (per-name table). AG×61 09:30 $20.55 → close $21.19 +39.04; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×61 09:30 $20.65 → close $21.11 +28.06; HDSN×218 09:30 $5.77 → close $5.57 -43.60; IAG×64 09:30 $19.63 → close $20.50 +55.68; KGC×42 09:30 $29.63 → close $31.43 +75.60; NFGC×721 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $208.63 | ▲ 09:30 equity $10,585.91 vs yday $10,316.19 (+269.72) | 09:30 open · cash $208.63 (unchanged overnight, no fees) · equity $10,585.91 vs prior close $10,316.19 (+269.72) · 8 name(s) re-marked at the open (per-name table). AG×61 yday $21.19 → 09:30 $21.90 +43.31; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×61 yday $21.11 → 09:30 $21.75 +39.04; HDSN×218 yday $5.57 → 09:30 $5.67 +21.80; IAG×64 yday $20.50 → 09:30 $21.17 +42.88; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×721 yday $1.75 → 09:30 $1.79 +28.84; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 61 | $21.90 | $2.19 | $+77.98 | $1,542.33 | ▲ +77.98 after sell → book $10,583.71; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,784.64 | ▲ +57.15 after sell → book $10,581.66; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 61 | $21.75 | $2.19 | $+62.73 | $4,109.20 | ▲ +62.73 after sell → book $10,579.47; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 218 | $5.67 | $2.86 | $-27.47 | $5,342.40 | ▼ -27.47 after sell → book $10,576.61; vs 09:30 mark -2.86 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 64 | $21.17 | $2.20 | $+94.17 | $6,695.08 | ▲ +94.17 after sell → book $10,574.41; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $8,044.08 | ▲ +102.43 after sell → book $10,572.27; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 721 | $1.79 | $9.43 | $+10.11 | $9,325.24 | ▲ +10.11 after sell → book $10,562.84; vs 09:30 mark -9.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,560.81 | ▲ +77.23 after sell → book $10,560.81; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 11 | $119.43 | $2.02 | — | $9,245.05 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 76 | $17.20 | $2.22 | — | $7,935.64 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,635.83 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 118 | $11.13 | $2.34 | — | $5,320.14 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 534 | $2.47 | $6.89 | — | $3,994.27 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 683 | $1.93 | $8.81 | — | $2,667.27 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 22 | $59.72 | $2.06 | — | $1,351.38 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 1000 | $1.32 | $12.90 | — | $18.48 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.48 | ▲ close $10,787.08 vs 09:30 $10,585.91 (session +265.52) | 16:00 close · cash $18.48 · equity $10,787.08 vs 09:30 $10,585.91 (+201.17; session marks +265.52) · 8 name(s) marked open→close (per-name table). AU×11 09:30 $119.43 → close $121.22 +19.69; AUPH×76 09:30 $17.20 → close $16.65 -41.80; AEM×6 09:30 $216.30 → close $216.06 -1.44; ARCT×118 09:30 $11.13 → close $13.45 +273.76; AUTL×534 09:30 $2.47 → close $2.41 -32.04; CRDL×683 09:30 $1.93 → close $1.86 -47.81; CRSP×22 09:30 $59.72 → close $59.50 -4.84; CYPH×1000 09:30 $1.32 → close $1.42 +100.00 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.48 | ▲ 09:30 equity $11,166.67 vs yday $10,787.08 (+379.59) | 09:30 open · cash $18.48 (unchanged overnight, no fees) · equity $11,166.67 vs prior close $10,787.08 (+379.59) · 8 name(s) re-marked at the open (per-name table). AU×11 yday $121.22 → 09:30 $120.51 -7.81; AUPH×76 yday $16.65 → 09:30 $16.57 -6.08; AEM×6 yday $216.06 → 09:30 $217.03 +5.82; ARCT×118 yday $13.45 → 09:30 $13.33 -14.16; AUTL×534 yday $2.41 → 09:30 $2.40 -5.34; CRDL×683 yday $1.86 → 09:30 $1.88 +13.66; CRSP×22 yday $59.50 → 09:30 $58.75 -16.50; CYPH×1000 yday $1.42 → 09:30 $1.83 +410.00 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 11 | $120.51 | $2.04 | $+7.81 | $1,342.04 | ▲ +7.81 after sell → book $11,164.62; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 76 | $16.57 | $2.24 | $-52.34 | $2,599.12 | ▼ -52.34 after sell → book $11,162.38; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,899.28 | ▲ +0.34 after sell → book $11,160.36; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 118 | $13.33 | $2.38 | $+254.88 | $5,469.84 | ▲ +254.88 after sell → book $11,157.98; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 534 | $2.40 | $6.99 | $-51.26 | $6,744.45 | ▼ -51.26 after sell → book $11,150.99; vs 09:30 mark -6.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 683 | $1.88 | $8.93 | $-51.90 | $8,019.56 | ▼ -51.90 after sell → book $11,142.06; vs 09:30 mark -8.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 22 | $58.75 | $2.08 | $-25.47 | $9,309.98 | ▼ -25.47 after sell → book $11,139.98; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1000 | $1.83 | $13.08 | $+484.02 | $11,126.90 | ▲ +484.02 after sell → book $11,126.90; vs 09:30 mark -13.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,126.90 | ▲ close $11,126.90 vs 09:30 $11,166.67 (session +0.00) | 16:00 close · cash $11,126.90 · no lots left · equity $11,126.90. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,126.90 | ▲ 09:30 equity $11,126.90 vs yday $11,126.90 (+0.00) | 09:30 open · cash $11,126.90 · no holdings · equity $11,126.90 vs prior close $11,126.90 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 58 | $23.77 | $2.16 | — | $9,746.08 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+13.0; leftover $1390.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 126 | $10.98 | $2.37 | — | $8,360.23 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+1.2; leftover $1390.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.19 | $2.06 | — | $7,011.99 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+7.4; leftover $1390.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 166 | $8.35 | $2.49 | — | $5,623.40 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1390.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 281 | $4.94 | $3.62 | — | $4,231.64 | — | S≥+5: sizeup + more names; list flatten; ret5=+7.1; leftover $1390.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $2,948.73 | — | S≥+5: sizeup + more names; list flatten; ret5=+6.0; leftover $1390.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 191 | $7.25 | $2.56 | — | $1,561.42 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1390.86 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3885 | $0.36 | $25.56 | — | $145.02 | — | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=-15.6; leftover $1390.86 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $145.02 | ▲ close $11,340.83 vs 09:30 $11,126.90 (session +256.76) | 16:00 close · cash $145.02 · equity $11,340.83 vs 09:30 $11,126.90 (+213.93; session marks +256.76) · 8 name(s) marked open→close (per-name table). MOS×58 09:30 $23.77 → close $24.27 +29.00; OCUL×126 09:30 $10.98 → close $10.88 -12.60; INSP×22 09:30 $61.19 → close $61.07 -2.64; CRMD×166 09:30 $8.35 → close $8.56 +34.86; RZLT×281 09:30 $4.94 → close $5.01 +19.67; HCA×3 09:30 $426.97 → close $428.76 +5.37; CAPR×191 09:30 $7.25 → close $8.29 +198.64; SAFX×3885 09:30 $0.36 → close $0.35 -15.54 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $145.02 | ▼ 09:30 equity $11,339.53 vs yday $11,340.83 (-1.30) | 09:30 open · cash $145.02 (unchanged overnight, no fees) · equity $11,339.53 vs prior close $11,340.83 (-1.30) · 8 name(s) re-marked at the open (per-name table). MOS×58 yday $24.27 → 09:30 $24.84 +33.06; OCUL×126 yday $10.88 → 09:30 $10.79 -11.34; INSP×22 yday $61.07 → 09:30 $60.07 -22.00; CRMD×166 yday $8.56 → 09:30 $8.60 +6.64; RZLT×281 yday $5.01 → 09:30 $5.01 +0.00; HCA×3 yday $428.76 → 09:30 $427.50 -3.78; CAPR×191 yday $8.29 → 09:30 $8.29 +0.00; SAFX×3885 yday $0.35 → 09:30 $0.35 -3.89 | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 191 | $8.29 | $2.61 | $+193.47 | $1,725.81 | ▲ +193.47 after sell → book $11,336.92; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 3885 | $0.35 | $26.02 | $-71.01 | $3,071.19 | ▼ -71.01 after sell → book $11,310.90; vs 09:30 mark -26.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 49 | $31.21 | $2.14 | — | $1,539.76 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1535.59 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 138 | $11.12 | $2.40 | — | $2.80 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1535.59 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.80 | ▼ close $11,266.06 vs 09:30 $11,339.53 (session -40.30) | 16:00 close · cash $2.80 · equity $11,266.06 vs 09:30 $11,339.53 (-73.47; session marks -40.30) · 8 name(s) marked open→close (per-name table). MOS×58 09:30 $24.84 → close $24.16 -39.44; OCUL×126 09:30 $10.79 → close $10.77 -2.52; INSP×22 09:30 $60.07 → close $61.80 +38.06; CRMD×166 09:30 $8.60 → close $8.39 -34.86; RZLT×281 09:30 $5.01 → close $5.04 +8.43; HCA×3 09:30 $427.50 → close $427.16 -1.02; AVBP×49 09:30 $31.21 → close $31.14 -3.43; FLNC×138 09:30 $11.12 → close $11.08 -5.52 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.80 | ▲ 09:30 equity $11,306.69 vs yday $11,266.06 (+40.63) | 09:30 open · cash $2.80 (unchanged overnight, no fees) · equity $11,306.69 vs prior close $11,266.06 (+40.63) · 8 name(s) re-marked at the open (per-name table). MOS×58 yday $24.16 → 09:30 $24.00 -9.28; OCUL×126 yday $10.77 → 09:30 $10.63 -17.64; INSP×22 yday $61.80 → 09:30 $62.10 +6.60; CRMD×166 yday $8.39 → 09:30 $8.49 +16.60; RZLT×281 yday $5.04 → 09:30 $5.07 +8.43; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; AVBP×49 yday $31.14 → 09:30 $30.79 -17.15; FLNC×138 yday $11.08 → 09:30 $11.52 +60.72 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 126 | $10.63 | $2.40 | $-48.87 | $1,339.78 | ▼ -48.87 after sell → book $11,304.29; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 22 | $62.10 | $2.08 | $+15.89 | $2,703.90 | ▲ +15.89 after sell → book $11,302.21; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 166 | $8.49 | $2.53 | $+18.23 | $4,110.71 | ▲ +18.23 after sell → book $11,299.68; vs 09:30 mark -2.53 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 281 | $5.07 | $3.68 | $+29.22 | $5,531.70 | ▲ +29.22 after sell → book $11,296.00; vs 09:30 mark -3.68 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-11.10 | $6,803.51 | ▼ -11.10 after sell → book $11,293.98; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 49 | $30.79 | $2.16 | $-24.88 | $8,310.06 | ▼ -24.88 after sell → book $11,291.82; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 138 | $11.52 | $2.44 | $+50.36 | $9,897.38 | ▲ +50.36 after sell → book $11,289.38; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 34 | $41.44 | $2.09 | — | $8,486.33 | — | S≥+5: sizeup + more names; list flatten; ret5=+3.1; leftover $1413.91 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 98 | $14.42 | $2.28 | — | $7,070.89 | — | S≥+5: sizeup + more names; list flatten; ret5=+7.1; leftover $1413.91 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 543 | $2.60 | $7.00 | — | $5,652.08 | — | S≥+5: sizeup + more names; list flatten; ret5=+13.0; leftover $1413.91 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 17 | $81.65 | $2.04 | — | $4,261.99 | — | S≥+5: sizeup + more names; list mover_buy; 🔵; ret5=+2.0; leftover $1413.91 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 309 | $4.57 | $3.99 | — | $2,845.87 | — | S≥+5: sizeup + more names; list mover_buy; 🔵; ret5=+1.1; leftover $1413.91 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 18 | $74.54 | $2.04 | — | $1,502.11 | — | S≥+5: sizeup + more names; list mover_buy; 🔵; ret5=-0.1; leftover $1413.91 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $533.11 | — | S≥+5: sizeup + more names; list mover_buy; 🔵; ret5=+0.1; leftover $1413.91 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $533.11 | ▲ close $11,292.59 vs 09:30 $11,306.69 (session +24.65) | 16:00 close · cash $533.11 · equity $11,292.59 vs 09:30 $11,306.69 (-14.10; session marks +24.65) · 8 name(s) marked open→close (per-name table). MOS×58 09:30 $24.00 → close $23.76 -13.92; RRC×34 09:30 $41.44 → close $41.64 +6.80; CRK×98 09:30 $14.42 → close $14.62 +19.60; SLI×543 09:30 $2.60 → close $2.64 +21.72; ACMR×17 09:30 $81.65 → close $80.49 -19.72; GGB×309 09:30 $4.57 → close $4.70 +40.17; MT×18 09:30 $74.54 → close $74.63 +1.62; MU×1 09:30 $967.01 → close $935.39 -31.62 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $533.11 | ▲ 09:30 equity $11,297.28 vs yday $11,292.59 (+4.69) | 09:30 open · cash $533.11 (unchanged overnight, no fees) · equity $11,297.28 vs prior close $11,292.59 (+4.69) · 8 name(s) re-marked at the open (per-name table). MOS×58 yday $23.76 → 09:30 $23.95 +11.02; RRC×34 yday $41.64 → 09:30 $41.74 +3.40; CRK×98 yday $14.62 → 09:30 $14.63 +0.98; SLI×543 yday $2.64 → 09:30 $2.68 +21.72; ACMR×17 yday $80.49 → 09:30 $79.27 -20.74; GGB×309 yday $4.70 → 09:30 $4.67 -9.27; MT×18 yday $74.63 → 09:30 $75.39 +13.68; MU×1 yday $935.39 → 09:30 $919.29 -16.10 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 17 | $79.27 | $2.06 | $-44.56 | $1,878.63 | ▼ -44.56 after sell → book $11,295.21; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 309 | $4.67 | $4.05 | $+22.86 | $3,317.62 | ▲ +22.86 after sell → book $11,291.17; vs 09:30 mark -4.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 18 | $75.39 | $2.06 | $+11.19 | $4,672.57 | ▲ +11.19 after sell → book $11,289.10; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $5,589.85 | ▼ -51.73 after sell → book $11,287.09; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 42 | $32.90 | $2.12 | — | $4,205.93 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1397.46 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 89 | $15.66 | $2.26 | — | $2,809.94 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1397.46 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 17 | $79.42 | $2.04 | — | $1,457.75 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1397.46 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 420 | $3.32 | $5.42 | — | $57.94 | — | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=+6.4; leftover $1397.46 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.94 | ▼ close $10,958.29 vs 09:30 $11,297.28 (session -316.97) | 16:00 close · cash $57.94 · equity $10,958.29 vs 09:30 $11,297.28 (-338.99; session marks -316.97) · 8 name(s) marked open→close (per-name table). MOS×58 09:30 $23.95 → close $23.60 -20.30; RRC×34 09:30 $41.74 → close $41.46 -9.52; CRK×98 09:30 $14.63 → close $14.29 -33.32; SLI×543 09:30 $2.68 → close $2.55 -70.59; SEDG×42 09:30 $32.90 → close $31.41 -62.58; GRRR×89 09:30 $15.66 → close $14.41 -111.25; URBN×17 09:30 $79.42 → close $81.09 +28.39; PYXS×420 09:30 $3.32 → close $3.23 -37.80 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.94 | ▲ 09:30 equity $10,990.18 vs yday $10,958.29 (+31.89) | 09:30 open · cash $57.94 (unchanged overnight, no fees) · equity $10,990.18 vs prior close $10,958.29 (+31.89) · 8 name(s) re-marked at the open (per-name table). MOS×58 yday $23.60 → 09:30 $23.68 +4.64; RRC×34 yday $41.46 → 09:30 $42.00 +18.36; CRK×98 yday $14.29 → 09:30 $14.54 +24.50; SLI×543 yday $2.55 → 09:30 $2.58 +16.29; SEDG×42 yday $31.41 → 09:30 $31.15 -10.92; GRRR×89 yday $14.41 → 09:30 $14.44 +2.67; URBN×17 yday $81.09 → 09:30 $80.44 -11.05; PYXS×420 yday $3.23 → 09:30 $3.20 -12.60 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 58 | $23.68 | $2.19 | $-9.57 | $1,429.19 | ▼ -9.57 after sell → book $10,987.99; vs 09:30 mark -2.19 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 34 | $42.00 | $2.11 | $+14.83 | $2,855.08 | ▲ +14.83 after sell → book $10,985.88; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 98 | $14.54 | $2.31 | $+7.16 | $4,277.69 | ▲ +7.16 after sell → book $10,983.57; vs 09:30 mark -2.31 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 543 | $2.58 | $7.11 | $-24.97 | $5,671.52 | ▼ -24.97 after sell → book $10,976.46; vs 09:30 mark -7.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 42 | $31.15 | $2.14 | $-77.75 | $6,977.68 | ▼ -77.75 after sell → book $10,974.32; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 89 | $14.44 | $2.28 | $-113.12 | $8,260.56 | ▼ -113.12 after sell → book $10,972.04; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 17 | $80.44 | $2.06 | $+13.24 | $9,625.98 | ▲ +13.24 after sell → book $10,969.98; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PYXS` | 420 | $3.20 | $5.50 | $-61.32 | $10,964.48 | ▼ -61.32 after sell → book $10,964.48; vs 09:30 mark -5.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,964.48 | ▲ close $10,964.48 vs 09:30 $10,990.18 (session +0.00) | 16:00 close · cash $10,964.48 · no lots left · equity $10,964.48. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,964.48 | ▲ 09:30 equity $10,964.48 vs yday $10,964.48 (+0.00) | 09:30 open · cash $10,964.48 · no holdings · equity $10,964.48 vs prior close $10,964.48 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,964.48 | ▲ close $10,964.48 vs 09:30 $10,964.48 (session +0.00) | 16:00 close · cash $10,964.48 · no lots left · equity $10,964.48. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,964.48 | ▲ 09:30 equity $10,964.48 vs yday $10,964.48 (+0.00) | 09:30 open · cash $10,964.48 · no holdings · equity $10,964.48 vs prior close $10,964.48 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,964.48 | ▲ close $10,964.48 vs 09:30 $10,964.48 (session +0.00) | 16:00 close · cash $10,964.48 · no lots left · equity $10,964.48. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,964.48 | ▲ 09:30 equity $10,964.48 vs yday $10,964.48 (+0.00) | 09:30 open · cash $10,964.48 · no holdings · equity $10,964.48 vs prior close $10,964.48 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,640.42 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1370.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $42.93 | $2.08 | — | $8,307.50 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1370.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 377 | $3.63 | $4.86 | — | $6,934.13 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1370.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 170 | $8.03 | $2.50 | — | $5,566.53 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1370.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,240.01 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1370.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 88 | $15.45 | $2.25 | — | $2,878.16 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1370.56 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,562.63 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1370.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 81 | $16.77 | $2.23 | — | $202.03 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1370.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $202.03 | ▼ close $10,701.84 vs 09:30 $10,964.48 (session -242.60) | 16:00 close · cash $202.03 · equity $10,701.84 vs 09:30 $10,964.48 (-262.64; session marks -242.60) · 8 name(s) marked open→close (per-name table). ATRC×25 09:30 $52.88 → close $52.46 -10.50; HRMY×31 09:30 $42.93 → close $41.86 -33.17; CABA×377 09:30 $3.63 → close $3.48 -56.55; VSTM×170 09:30 $8.03 → close $7.98 -8.50; RVTY×10 09:30 $132.45 → close $130.63 -18.20; CRK×88 09:30 $15.45 → close $14.95 -44.00; MRNA×9 09:30 $145.94 → close $148.87 +26.33; ARCT×81 09:30 $16.77 → close $15.56 -98.01 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $202.03 | ▲ 09:30 equity $10,705.69 vs yday $10,701.84 (+3.85) | 09:30 open · cash $202.03 (unchanged overnight, no fees) · equity $10,705.69 vs prior close $10,701.84 (+3.85) · 8 name(s) re-marked at the open (per-name table). ATRC×25 yday $52.46 → 09:30 $52.03 -10.75; HRMY×31 yday $41.86 → 09:30 $41.50 -11.16; CABA×377 yday $3.48 → 09:30 $3.46 -7.54; VSTM×170 yday $7.98 → 09:30 $7.91 -11.90; RVTY×10 yday $130.63 → 09:30 $130.03 -6.00; CRK×88 yday $14.95 → 09:30 $15.00 +4.40; MRNA×9 yday $148.87 → 09:30 $153.62 +42.75; ARCT×81 yday $15.56 → 09:30 $15.61 +4.05 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 31 | $41.50 | $2.10 | $-48.52 | $1,486.43 | ▼ -48.52 after sell → book $10,703.59; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 170 | $7.91 | $2.54 | $-25.44 | $2,828.59 | ▼ -25.44 after sell → book $10,701.05; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $4,126.85 | ▼ -28.26 after sell → book $10,699.01; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 88 | $15.00 | $2.28 | $-44.13 | $5,444.57 | ▼ -44.13 after sell → book $10,696.73; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 9 | $153.62 | $2.04 | $+65.02 | $6,825.11 | ▲ +65.02 after sell → book $10,694.69; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 81 | $15.61 | $2.26 | $-98.45 | $8,087.26 | ▼ -98.45 after sell → book $10,692.43; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 534 | $2.52 | $6.89 | — | $6,734.70 | — | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1347.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 200 | $6.71 | $2.59 | — | $5,390.11 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1347.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 709 | $1.90 | $9.15 | — | $4,033.86 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1347.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 281 | $4.78 | $3.62 | — | $2,687.05 | — | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1347.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 847 | $1.59 | $10.93 | — | $1,329.40 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1347.88 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 117 | $11.31 | $2.34 | — | $3.79 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1347.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.79 | ▼ close $10,602.74 vs 09:30 $10,705.69 (session -54.18) | 16:00 close · cash $3.79 · equity $10,602.74 vs 09:30 $10,705.69 (-102.95; session marks -54.18) · 8 name(s) marked open→close (per-name table). ATRC×25 09:30 $52.03 → close $51.52 -12.75; CABA×377 09:30 $3.46 → close $3.47 +3.77; ALEC×534 09:30 $2.52 → close $2.46 -32.04; BHC×200 09:30 $6.71 → close $6.56 -30.00; BMEA×709 09:30 $1.90 → close $2.03 +92.17; OABI×281 09:30 $4.78 → close $4.33 -126.45; OPK×847 09:30 $1.59 → close $1.64 +42.35; VIR×117 09:30 $11.31 → close $11.38 +8.77 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.79 | ▼ 09:30 equity $10,559.22 vs yday $10,602.74 (-43.52) | 09:30 open · cash $3.79 (unchanged overnight, no fees) · equity $10,559.22 vs prior close $10,602.74 (-43.52) · 8 name(s) re-marked at the open (per-name table). ATRC×25 yday $51.52 → 09:30 $54.31 +69.75; CABA×377 yday $3.47 → 09:30 $3.43 -15.08; ALEC×534 yday $2.46 → 09:30 $2.38 -42.72; BHC×200 yday $6.56 → 09:30 $6.57 +2.00; BMEA×709 yday $2.03 → 09:30 $2.00 -21.27; OABI×281 yday $4.33 → 09:30 $4.30 -8.43; OPK×847 yday $1.64 → 09:30 $1.63 -8.47; VIR×117 yday $11.38 → 09:30 $11.22 -19.30 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 25 | $54.31 | $2.09 | $+31.60 | $1,359.45 | ▲ +31.60 after sell → book $10,557.13; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 377 | $3.43 | $4.94 | $-85.20 | $2,647.63 | ▼ -85.20 after sell → book $10,552.20; vs 09:30 mark -4.93 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 534 | $2.38 | $6.99 | $-88.64 | $3,911.56 | ▼ -88.64 after sell → book $10,545.21; vs 09:30 mark -6.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 200 | $6.57 | $2.63 | $-33.22 | $5,222.92 | ▼ -33.22 after sell → book $10,542.57; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 709 | $2.00 | $9.28 | $+52.48 | $6,631.65 | ▲ +52.48 after sell → book $10,533.30; vs 09:30 mark -9.27 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 281 | $4.30 | $3.68 | $-142.19 | $7,836.27 | ▼ -142.19 after sell → book $10,529.62; vs 09:30 mark -3.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 847 | $1.63 | $11.08 | $+11.88 | $9,205.80 | ▲ +11.88 after sell → book $10,518.54; vs 09:30 mark -11.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 117 | $11.22 | $2.37 | $-15.24 | $10,516.17 | ▼ -15.24 after sell → book $10,516.17; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,516.17 | ▲ close $10,516.17 vs 09:30 $10,559.22 (session +0.00) | 16:00 close · cash $10,516.17 · no lots left · equity $10,516.17. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,516.17 | ▲ 09:30 equity $10,516.17 vs yday $10,516.17 (-0.00) | 09:30 open · cash $10,516.17 · no holdings · equity $10,516.17 vs prior close $10,516.17 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,516.17 | ▲ close $10,516.17 vs 09:30 $10,516.17 (session +0.00) | 16:00 close · cash $10,516.17 · no lots left · equity $10,516.17. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,516.17 | ▲ 09:30 equity $10,516.17 vs yday $10,516.17 (-0.00) | 09:30 open · cash $10,516.17 · no holdings · equity $10,516.17 vs prior close $10,516.17 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,516.17 | ▲ close $10,516.17 vs 09:30 $10,516.17 (session +0.00) | 16:00 close · cash $10,516.17 · no lots left · equity $10,516.17. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,516.17 | ▲ 09:30 equity $10,516.17 vs yday $10,516.17 (-0.00) | 09:30 open · cash $10,516.17 · no holdings · equity $10,516.17 vs prior close $10,516.17 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 80 | $16.28 | $2.23 | — | $9,211.54 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=-0.2; leftover $1314.52 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,211.54 | ▼ close $10,499.54 vs 09:30 $10,516.17 (session -14.40) | 16:00 close · cash $9,211.54 · equity $10,499.54 vs 09:30 $10,516.17 (-16.63; session marks -14.40) · 1 name(s) marked open→close (per-name table). AUPH×80 09:30 $16.28 → close $16.10 -14.40 | — |

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
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `OVID` | no_price | no 09:30 open |
| 2026-09-11 | `SANM` | no_price | no 09:30 open |
| 2026-09-11 | `ORCL` | no_price | no 09:30 open |
| 2026-09-11 | `NVT` | no_price | no 09:30 open |
| 2026-09-11 | `COHU` | no_price | no 09:30 open |
| 2026-09-11 | `CMRC` | no_price | no 09:30 open |
| 2026-09-11 | `DBI` | no_price | no 09:30 open |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `AUPH` | 80 | 2026-09-11 @ $16.28 | S≥+5: sizeup + more names; list flatten; 🔵; ret5=-0.2; leftover $1314.52 |
