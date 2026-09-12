# Factor mine action — `union_join_present_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ join_present, no 🚨

Cash book **+4.52%** ($10,452) · signal-only (no cash/fees) was +6.51%. Starts YES **6/21**. Fills 151 · skips 79 · realized $+468.89.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the join camera printed something (any color, not blank).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
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

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `join_present=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,164.25.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 20 | — | $59.80 | +0.00 | $60.23 | +8.60 | +8.60 | +0.00 | +8.60 |
| 2026-08-13 | `IREN` | 27 | — | $45.98 | +0.00 | $44.76 | -32.94 | -32.94 | +0.00 | -32.94 |
| 2026-08-13 | `TPG` | 24 | — | $50.62 | +0.00 | $54.62 | +95.92 | +95.92 | +0.00 | +95.92 |
| 2026-08-13 | `TGTX` | 25 | — | $49.70 | +0.00 | $47.94 | -44.00 | -44.00 | +0.00 | -44.00 |
| 2026-08-13 | `SLS` | 106 | — | $11.70 | +0.00 | $12.36 | +69.96 | +69.96 | +0.00 | +69.96 |
| 2026-08-13 | `HIMS` | 42 | — | $29.74 | +0.00 | $28.77 | -40.74 | -40.74 | +0.00 | -40.74 |
| 2026-08-13 | `INO` | 1543 | — | $0.81 | +0.00 | $0.90 | +138.87 | +138.87 | +0.00 | +138.87 |
| 2026-08-13 | `TNDM` | 53 | — | $23.33 | +0.00 | $23.13 | -10.60 | -10.60 | +0.00 | -10.60 |
| 2026-08-14 | `BTSG` | 20 | $60.23 | $59.65 | -11.60 | — | +0.00 | -11.60 | -3.00 | — |
| 2026-08-14 | `IREN` | 27 | $44.76 | $44.09 | -18.09 | — | +0.00 | -18.09 | -51.03 | — |
| 2026-08-14 | `TPG` | 24 | $54.62 | $55.29 | +16.08 | — | +0.00 | +16.08 | +112.00 | — |
| 2026-08-14 | `TGTX` | 25 | $47.94 | $47.27 | -16.75 | — | +0.00 | -16.75 | -60.75 | — |
| 2026-08-14 | `SLS` | 106 | $12.36 | $12.40 | +4.24 | — | +0.00 | +4.24 | +74.20 | — |
| 2026-08-14 | `HIMS` | 42 | $28.77 | $29.15 | +15.96 | — | +0.00 | +15.96 | -24.78 | — |
| 2026-08-14 | `INO` | 1543 | $0.90 | $0.93 | +46.29 | — | +0.00 | +46.29 | +185.16 | — |
| 2026-08-14 | `TNDM` | 53 | $23.13 | $22.92 | -11.13 | — | +0.00 | -11.13 | -21.73 | — |
| 2026-08-14 | `TLN` | 3 | — | $359.83 | +0.00 | $362.74 | +8.73 | +8.73 | +0.00 | +8.73 |
| 2026-08-14 | `VST` | 8 | — | $146.90 | +0.00 | $148.13 | +9.84 | +9.84 | +0.00 | +9.84 |
| 2026-08-14 | `NRG` | 10 | — | $120.00 | +0.00 | $126.24 | +62.40 | +62.40 | +0.00 | +62.40 |
| 2026-08-14 | `DAVE` | 3 | — | $330.91 | +0.00 | $334.57 | +10.98 | +10.98 | +0.00 | +10.98 |
| 2026-08-14 | `SLG` | 22 | — | $57.61 | +0.00 | $56.09 | -33.44 | -33.44 | +0.00 | -33.44 |
| 2026-08-14 | `MARA` | 140 | — | $9.01 | +0.00 | $9.20 | +26.60 | +26.60 | +0.00 | +26.60 |
| 2026-08-14 | `LDI` | 1353 | — | $0.94 | +0.00 | $0.90 | -54.12 | -54.12 | +0.00 | -54.12 |
| 2026-08-14 | `BTBT` | 845 | — | $1.50 | +0.00 | $1.57 | +59.15 | +59.15 | +0.00 | +59.15 |
| 2026-08-17 | `TLN` | 3 | $362.74 | $367.88 | +15.42 | — | +0.00 | +15.42 | +24.15 | — |
| 2026-08-17 | `VST` | 8 | $148.13 | $149.37 | +9.92 | — | +0.00 | +9.92 | +19.76 | — |
| 2026-08-17 | `NRG` | 10 | $126.24 | $127.40 | +11.60 | — | +0.00 | +11.60 | +74.00 | — |
| 2026-08-17 | `DAVE` | 3 | $334.57 | $336.94 | +7.11 | — | +0.00 | +7.11 | +18.09 | — |
| 2026-08-17 | `SLG` | 22 | $56.09 | $55.37 | -15.84 | — | +0.00 | -15.84 | -49.28 | — |
| 2026-08-17 | `MARA` | 140 | $9.20 | $9.22 | +2.80 | — | +0.00 | +2.80 | +29.40 | — |
| 2026-08-17 | `LDI` | 1353 | $0.90 | $0.91 | +13.53 | — | +0.00 | +13.53 | -40.59 | — |
| 2026-08-17 | `BTBT` | 845 | $1.57 | $1.52 | -42.25 | — | +0.00 | -42.25 | +16.90 | — |
| 2026-08-17 | `DVN` | 27 | — | $46.18 | +0.00 | $47.57 | +37.53 | +37.53 | +0.00 | +37.53 |
| 2026-08-17 | `EOG` | 8 | — | $142.77 | +0.00 | $146.15 | +27.04 | +27.04 | +0.00 | +27.04 |
| 2026-08-17 | `FANG` | 6 | — | $202.70 | +0.00 | $206.29 | +21.54 | +21.54 | +0.00 | +21.54 |
| 2026-08-17 | `TMC` | 313 | — | $4.05 | +0.00 | $3.77 | -87.64 | -87.64 | +0.00 | -87.64 |
| 2026-08-17 | `TGB` | 150 | — | $8.46 | +0.00 | $8.77 | +46.50 | +46.50 | +0.00 | +46.50 |
| 2026-08-17 | `ELF` | 14 | — | $90.54 | +0.00 | $93.66 | +43.68 | +43.68 | +0.00 | +43.68 |
| 2026-08-17 | `DNN` | 391 | — | $3.24 | +0.00 | $3.19 | -19.55 | -19.55 | +0.00 | -19.55 |
| 2026-08-17 | `NB` | 250 | — | $5.07 | +0.00 | $4.81 | -65.00 | -65.00 | +0.00 | -65.00 |
| 2026-08-18 | `DVN` | 27 | $47.57 | $48.00 | +11.61 | — | +0.00 | +11.61 | +49.14 | — |
| 2026-08-18 | `EOG` | 8 | $146.15 | $148.04 | +15.12 | — | +0.00 | +15.12 | +42.16 | — |
| 2026-08-18 | `FANG` | 6 | $206.29 | $208.93 | +15.84 | — | +0.00 | +15.84 | +37.38 | — |
| 2026-08-18 | `TMC` | 313 | $3.77 | $3.72 | -15.65 | — | +0.00 | -15.65 | -103.29 | — |
| 2026-08-18 | `TGB` | 150 | $8.77 | $8.55 | -33.00 | — | +0.00 | -33.00 | +13.50 | — |
| 2026-08-18 | `ELF` | 14 | $93.66 | $93.44 | -3.08 | — | +0.00 | -3.08 | +40.60 | — |
| 2026-08-18 | `DNN` | 391 | $3.19 | $3.11 | -31.28 | — | +0.00 | -31.28 | -50.83 | — |
| 2026-08-18 | `NB` | 250 | $4.81 | $4.66 | -37.50 | — | +0.00 | -37.50 | -102.50 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 61 | — | $20.55 | +0.00 | $21.19 | +39.04 | +39.04 | +0.00 | +39.04 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 60 | — | $20.65 | +0.00 | $21.11 | +27.60 | +27.60 | +0.00 | +27.60 |
| 2026-08-20 | `HDSN` | 217 | — | $5.77 | +0.00 | $5.57 | -43.40 | -43.40 | +0.00 | -43.40 |
| 2026-08-20 | `IAG` | 63 | — | $19.63 | +0.00 | $20.50 | +54.81 | +54.81 | +0.00 | +54.81 |
| 2026-08-20 | `KGC` | 42 | — | $29.63 | +0.00 | $31.43 | +75.60 | +75.60 | +0.00 | +75.60 |
| 2026-08-20 | `NFGC` | 716 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 61 | $21.19 | $21.90 | +43.31 | — | +0.00 | +43.31 | +82.35 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `CDE` | 60 | $21.11 | $21.75 | +38.40 | — | +0.00 | +38.40 | +66.00 | — |
| 2026-08-21 | `HDSN` | 217 | $5.57 | $5.67 | +21.70 | — | +0.00 | +21.70 | -21.70 | — |
| 2026-08-21 | `IAG` | 63 | $20.50 | $21.17 | +42.21 | — | +0.00 | +42.21 | +97.02 | — |
| 2026-08-21 | `KGC` | 42 | $31.43 | $32.17 | +31.08 | — | +0.00 | +31.08 | +106.68 | — |
| 2026-08-21 | `NFGC` | 716 | $1.75 | $1.79 | +28.64 | — | +0.00 | +28.64 | +28.64 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 76 | — | $17.20 | +0.00 | $16.65 | -41.80 | -41.80 | +0.00 | -41.80 |
| 2026-08-21 | `AEM` | 6 | — | $216.30 | +0.00 | $216.06 | -1.44 | -1.44 | +0.00 | -1.44 |
| 2026-08-21 | `ARCT` | 117 | — | $11.13 | +0.00 | $13.45 | +271.44 | +271.44 | +0.00 | +271.44 |
| 2026-08-21 | `AUTL` | 530 | — | $2.47 | +0.00 | $2.41 | -31.80 | -31.80 | +0.00 | -31.80 |
| 2026-08-21 | `CRDL` | 679 | — | $1.93 | +0.00 | $1.86 | -47.53 | -47.53 | +0.00 | -47.53 |
| 2026-08-21 | `CRSP` | 21 | — | $59.72 | +0.00 | $59.50 | -4.62 | -4.62 | +0.00 | -4.62 |
| 2026-08-21 | `CYPH` | 993 | — | $1.32 | +0.00 | $1.42 | +99.30 | +99.30 | +0.00 | +99.30 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUPH` | 76 | $16.65 | $16.57 | -6.08 | — | +0.00 | -6.08 | -47.88 | — |
| 2026-08-24 | `AEM` | 6 | $216.06 | $217.03 | +5.82 | — | +0.00 | +5.82 | +4.38 | — |
| 2026-08-24 | `ARCT` | 117 | $13.45 | $13.33 | -14.04 | — | +0.00 | -14.04 | +257.40 | — |
| 2026-08-24 | `AUTL` | 530 | $2.41 | $2.40 | -5.30 | — | +0.00 | -5.30 | -37.10 | — |
| 2026-08-24 | `CRDL` | 679 | $1.86 | $1.88 | +13.58 | — | +0.00 | +13.58 | -33.95 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | — | +0.00 | -15.75 | -20.37 | — |
| 2026-08-24 | `CYPH` | 993 | $1.42 | $1.83 | +407.13 | — | +0.00 | +407.13 | +506.43 | — |
| 2026-08-25 | `MOS` | 58 | — | $23.77 | +0.00 | $24.27 | +29.00 | +29.00 | +0.00 | +29.00 |
| 2026-08-25 | `OCUL` | 125 | — | $10.98 | +0.00 | $10.88 | -12.50 | -12.50 | +0.00 | -12.50 |
| 2026-08-25 | `INSP` | 22 | — | $61.19 | +0.00 | $61.07 | -2.64 | -2.64 | +0.00 | -2.64 |
| 2026-08-25 | `CRMD` | 165 | — | $8.35 | +0.00 | $8.56 | +34.65 | +34.65 | +0.00 | +34.65 |
| 2026-08-25 | `RZLT` | 279 | — | $4.94 | +0.00 | $5.01 | +19.53 | +19.53 | +0.00 | +19.53 |
| 2026-08-25 | `HCA` | 3 | — | $426.97 | +0.00 | $428.76 | +5.37 | +5.37 | +0.00 | +5.37 |
| 2026-08-25 | `CAPR` | 190 | — | $7.25 | +0.00 | $8.29 | +197.60 | +197.60 | +0.00 | +197.60 |
| 2026-08-25 | `SAFX` | 3857 | — | $0.36 | +0.00 | $0.35 | -15.43 | -15.43 | +0.00 | -15.43 |
| 2026-08-26 | `MOS` | 58 | $24.27 | $24.84 | +33.06 | $24.16 | -39.44 | -6.38 | +62.06 | +22.62 |
| 2026-08-26 | `OCUL` | 125 | $10.88 | $10.79 | -11.25 | $10.77 | -2.50 | -13.75 | -23.75 | -26.25 |
| 2026-08-26 | `INSP` | 22 | $61.07 | $60.07 | -22.00 | $61.80 | +38.06 | +16.06 | -24.64 | +13.42 |
| 2026-08-26 | `CRMD` | 165 | $8.56 | $8.60 | +6.60 | $8.39 | -34.65 | -28.05 | +41.25 | +6.60 |
| 2026-08-26 | `RZLT` | 279 | $5.01 | $5.01 | +0.00 | $5.04 | +8.37 | +8.37 | +19.53 | +27.90 |
| 2026-08-26 | `HCA` | 3 | $428.76 | $427.50 | -3.78 | $427.16 | -1.02 | -4.80 | +1.59 | +0.57 |
| 2026-08-26 | `CAPR` | 190 | $8.29 | $8.29 | +0.00 | — | +0.00 | +0.00 | +197.60 | — |
| 2026-08-26 | `SAFX` | 3857 | $0.35 | $0.35 | -3.86 | — | +0.00 | -3.86 | -19.29 | — |
| 2026-08-26 | `AVBP` | 48 | — | $31.21 | +0.00 | $31.14 | -3.36 | -3.36 | +0.00 | -3.36 |
| 2026-08-26 | `FLNC` | 135 | — | $11.12 | +0.00 | $11.08 | -5.40 | -5.40 | +0.00 | -5.40 |
| 2026-08-27 | `MOS` | 58 | $24.16 | $24.00 | -9.28 | $23.76 | -13.92 | -23.20 | +13.34 | -0.58 |
| 2026-08-27 | `OCUL` | 125 | $10.77 | $10.63 | -17.50 | — | +0.00 | -17.50 | -43.75 | — |
| 2026-08-27 | `INSP` | 22 | $61.80 | $62.10 | +6.60 | — | +0.00 | +6.60 | +20.02 | — |
| 2026-08-27 | `CRMD` | 165 | $8.39 | $8.49 | +16.50 | — | +0.00 | +16.50 | +23.10 | — |
| 2026-08-27 | `RZLT` | 279 | $5.04 | $5.07 | +8.37 | — | +0.00 | +8.37 | +36.27 | — |
| 2026-08-27 | `HCA` | 3 | $427.16 | $424.61 | -7.65 | — | +0.00 | -7.65 | -7.08 | — |
| 2026-08-27 | `AVBP` | 48 | $31.14 | $30.79 | -16.80 | — | +0.00 | -16.80 | -20.16 | — |
| 2026-08-27 | `FLNC` | 135 | $11.08 | $11.52 | +59.40 | — | +0.00 | +59.40 | +54.00 | — |
| 2026-08-27 | `RRC` | 33 | — | $41.44 | +0.00 | $41.64 | +6.60 | +6.60 | +0.00 | +6.60 |
| 2026-08-27 | `CRK` | 97 | — | $14.42 | +0.00 | $14.62 | +19.40 | +19.40 | +0.00 | +19.40 |
| 2026-08-27 | `SLI` | 539 | — | $2.60 | +0.00 | $2.64 | +21.56 | +21.56 | +0.00 | +21.56 |
| 2026-08-27 | `ACMR` | 17 | — | $81.65 | +0.00 | $80.49 | -19.72 | -19.72 | +0.00 | -19.72 |
| 2026-08-27 | `GGB` | 306 | — | $4.57 | +0.00 | $4.70 | +39.78 | +39.78 | +0.00 | +39.78 |
| 2026-08-27 | `MT` | 18 | — | $74.54 | +0.00 | $74.63 | +1.62 | +1.62 | +0.00 | +1.62 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-28 | `MOS` | 58 | $23.76 | $23.95 | +11.02 | $23.60 | -20.30 | -9.28 | +10.44 | -9.86 |
| 2026-08-28 | `RRC` | 33 | $41.64 | $41.74 | +3.30 | $41.46 | -9.24 | -5.94 | +9.90 | +0.66 |
| 2026-08-28 | `CRK` | 97 | $14.62 | $14.63 | +0.97 | $14.29 | -32.98 | -32.01 | +20.37 | -12.61 |
| 2026-08-28 | `SLI` | 539 | $2.64 | $2.68 | +21.56 | $2.55 | -70.07 | -48.51 | +43.12 | -26.95 |
| 2026-08-28 | `ACMR` | 17 | $80.49 | $79.27 | -20.74 | — | +0.00 | -20.74 | -40.46 | — |
| 2026-08-28 | `GGB` | 306 | $4.70 | $4.67 | -9.18 | — | +0.00 | -9.18 | +30.60 | — |
| 2026-08-28 | `MT` | 18 | $74.63 | $75.39 | +13.68 | — | +0.00 | +13.68 | +15.30 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `SEDG` | 42 | — | $32.90 | +0.00 | $31.41 | -62.58 | -62.58 | +0.00 | -62.58 |
| 2026-08-28 | `GRRR` | 89 | — | $15.66 | +0.00 | $14.41 | -111.25 | -111.25 | +0.00 | -111.25 |
| 2026-08-28 | `URBN` | 17 | — | $79.42 | +0.00 | $81.09 | +28.39 | +28.39 | +0.00 | +28.39 |
| 2026-08-28 | `SIMO` | 5 | — | $252.24 | +0.00 | $245.81 | -32.15 | -32.15 | +0.00 | -32.15 |
| 2026-08-31 | `MOS` | 58 | $23.60 | $23.68 | +4.64 | — | +0.00 | +4.64 | -5.22 | — |
| 2026-08-31 | `RRC` | 33 | $41.46 | $42.00 | +17.82 | — | +0.00 | +17.82 | +18.48 | — |
| 2026-08-31 | `CRK` | 97 | $14.29 | $14.54 | +24.25 | — | +0.00 | +24.25 | +11.64 | — |
| 2026-08-31 | `SLI` | 539 | $2.55 | $2.58 | +16.17 | — | +0.00 | +16.17 | -10.78 | — |
| 2026-08-31 | `SEDG` | 42 | $31.41 | $31.15 | -10.92 | — | +0.00 | -10.92 | -73.50 | — |
| 2026-08-31 | `GRRR` | 89 | $14.41 | $14.44 | +2.67 | — | +0.00 | +2.67 | -108.58 | — |
| 2026-08-31 | `URBN` | 17 | $81.09 | $80.44 | -11.05 | — | +0.00 | -11.05 | +17.34 | — |
| 2026-08-31 | `SIMO` | 5 | $245.81 | $247.05 | +6.20 | — | +0.00 | +6.20 | -25.95 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 25 | — | $52.88 | +0.00 | $52.46 | -10.50 | -10.50 | +0.00 | -10.50 |
| 2026-09-03 | `HRMY` | 31 | — | $42.93 | +0.00 | $41.86 | -33.17 | -33.17 | +0.00 | -33.17 |
| 2026-09-03 | `CABA` | 375 | — | $3.63 | +0.00 | $3.48 | -56.25 | -56.25 | +0.00 | -56.25 |
| 2026-09-03 | `VSTM` | 169 | — | $8.03 | +0.00 | $7.98 | -8.45 | -8.45 | +0.00 | -8.45 |
| 2026-09-03 | `RVTY` | 10 | — | $132.45 | +0.00 | $130.63 | -18.20 | -18.20 | +0.00 | -18.20 |
| 2026-09-03 | `CRK` | 88 | — | $15.45 | +0.00 | $14.95 | -44.00 | -44.00 | +0.00 | -44.00 |
| 2026-09-03 | `MRNA` | 9 | — | $145.94 | +0.00 | $148.87 | +26.33 | +26.33 | +0.00 | +26.33 |
| 2026-09-03 | `ARCT` | 81 | — | $16.77 | +0.00 | $15.56 | -98.01 | -98.01 | +0.00 | -98.01 |
| 2026-09-04 | `ATRC` | 25 | $52.46 | $52.03 | -10.75 | $51.52 | -12.75 | -23.50 | -21.25 | -34.00 |
| 2026-09-04 | `HRMY` | 31 | $41.86 | $41.50 | -11.16 | — | +0.00 | -11.16 | -44.33 | — |
| 2026-09-04 | `CABA` | 375 | $3.48 | $3.46 | -7.50 | $3.47 | +3.75 | -3.75 | -63.75 | -60.00 |
| 2026-09-04 | `VSTM` | 169 | $7.98 | $7.91 | -11.83 | — | +0.00 | -11.83 | -20.28 | — |
| 2026-09-04 | `RVTY` | 10 | $130.63 | $130.03 | -6.00 | — | +0.00 | -6.00 | -24.20 | — |
| 2026-09-04 | `CRK` | 88 | $14.95 | $15.00 | +4.40 | — | +0.00 | +4.40 | -39.60 | — |
| 2026-09-04 | `MRNA` | 9 | $148.87 | $153.62 | +42.75 | — | +0.00 | +42.75 | +69.08 | — |
| 2026-09-04 | `ARCT` | 81 | $15.56 | $15.61 | +4.05 | — | +0.00 | +4.05 | -93.96 | — |
| 2026-09-04 | `ALEC` | 532 | — | $2.52 | +0.00 | $2.46 | -31.92 | -31.92 | +0.00 | -31.92 |
| 2026-09-04 | `BHC` | 199 | — | $6.71 | +0.00 | $6.56 | -29.85 | -29.85 | +0.00 | -29.85 |
| 2026-09-04 | `BMEA` | 705 | — | $1.90 | +0.00 | $2.03 | +91.65 | +91.65 | +0.00 | +91.65 |
| 2026-09-04 | `OABI` | 280 | — | $4.78 | +0.00 | $4.33 | -126.00 | -126.00 | +0.00 | -126.00 |
| 2026-09-04 | `OPK` | 843 | — | $1.59 | +0.00 | $1.64 | +42.15 | +42.15 | +0.00 | +42.15 |
| 2026-09-04 | `VIR` | 116 | — | $11.31 | +0.00 | $11.38 | +8.70 | +8.70 | +0.00 | +8.70 |
| 2026-09-08 | `ATRC` | 25 | $51.52 | $54.31 | +69.75 | — | +0.00 | +69.75 | +35.75 | — |
| 2026-09-08 | `CABA` | 375 | $3.47 | $3.43 | -15.00 | — | +0.00 | -15.00 | -75.00 | — |
| 2026-09-08 | `ALEC` | 532 | $2.46 | $2.38 | -42.56 | — | +0.00 | -42.56 | -74.48 | — |
| 2026-09-08 | `BHC` | 199 | $6.56 | $6.57 | +1.99 | — | +0.00 | +1.99 | -27.86 | — |
| 2026-09-08 | `BMEA` | 705 | $2.03 | $2.00 | -21.15 | — | +0.00 | -21.15 | +70.50 | — |
| 2026-09-08 | `OABI` | 280 | $4.33 | $4.30 | -8.40 | — | +0.00 | -8.40 | -134.40 | — |
| 2026-09-08 | `OPK` | 843 | $1.64 | $1.63 | -8.43 | — | +0.00 | -8.43 | +33.72 | — |
| 2026-09-08 | `VIR` | 116 | $11.38 | $11.22 | -19.14 | — | +0.00 | -19.14 | -10.44 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `AUPH` | 80 | — | $16.28 | +0.00 | $16.10 | -14.40 | -14.40 | +0.00 | -14.40 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +185.07 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | +90.14 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $592.27 | $10,193.91 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×22, MARA×140, LDI×1353, BTBT×845 |
| 2026-08-17 | +2.25 | $592.27 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×22, MARA×140, LDI×1353, BTBT×845 | $10,196.20 | +2.29 | +4.10 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, NB | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $189.31 | $10,137.18 | DVN×27, EOG×8, FANG×6, TMC×313, TGB×150, ELF×14, DNN×391, NB×250 |
| 2026-08-18 | -6.20 | $189.31 | DVN×27, EOG×8, FANG×6, TMC×313, TGB×150, ELF×14, DNN×391, NB×250 | $10,059.24 | -77.94 | +0.00 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, NB | $10,036.07 | $10,036.07 | — |
| 2026-08-19 | -7.20 | $10,036.07 | — | $10,036.07 | -0.00 | +0.00 | — | — | $10,036.07 | $10,036.07 | — |
| 2026-08-20 | +1.12 | $10,036.07 | — | $10,036.07 | -0.00 | +233.39 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $193.11 | $10,244.74 | AG×61, BHP×13, CDE×60, HDSN×217, IAG×63, KGC×42, NFGC×716, WPM×8 |
| 2026-08-21 | +3.25 | $193.11 | AG×61, BHP×13, CDE×60, HDSN×217, IAG×63, KGC×42, NFGC×716, WPM×8 | $10,512.85 | +268.11 | +261.45 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $162.83 | $10,710.24 | AU×10, AUPH×76, AEM×6, ARCT×117, AUTL×530, CRDL×679, CRSP×21, CYPH×993 |
| 2026-08-24 | -5.17 | $162.83 | AU×10, AUPH×76, AEM×6, ARCT×117, AUTL×530, CRDL×679, CRSP×21, CYPH×993 | $11,088.50 | +378.26 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $11,048.94 | $11,048.94 | — |
| 2026-08-25 | +1.80 | $11,048.94 | — | $11,048.94 | -0.00 | +255.58 | MOS, OCUL, INSP, CRMD, RZLT, HCA, CAPR, SAFX | — | $113.77 | $11,261.91 | MOS×58, OCUL×125, INSP×22, CRMD×165, RZLT×279, HCA×3, CAPR×190, SAFX×3857 |
| 2026-08-26 | +2.02 | $113.77 | MOS×58, OCUL×125, INSP×22, CRMD×165, RZLT×279, HCA×3, CAPR×190, SAFX×3857 | $11,260.69 | -1.22 | -39.94 | AVBP, FLNC | CAPR, SAFX | $18.14 | $11,187.78 | MOS×58, OCUL×125, INSP×22, CRMD×165, RZLT×279, HCA×3, AVBP×48, FLNC×135 |
| 2026-08-27 | — | $18.14 | MOS×58, OCUL×125, INSP×22, CRMD×165, RZLT×279, HCA×3, AVBP×48, FLNC×135 | $11,227.42 | +39.64 | +23.70 | RRC, CRK, SLI, ACMR, GGB, MT, MU | OCUL, INSP, CRMD, RZLT, HCA, AVBP, FLNC | $533.95 | $11,212.51 | MOS×58, RRC×33, CRK×97, SLI×539, ACMR×17, GGB×306, MT×18, MU×1 |
| 2026-08-28 | +0.75 | $533.95 | MOS×58, RRC×33, CRK×97, SLI×539, ACMR×17, GGB×306, MT×18, MU×1 | $11,217.02 | +4.51 | -310.18 | SEDG, GRRR, URBN, SIMO | ACMR, GGB, MT, MU | $181.42 | $10,888.27 | MOS×58, RRC×33, CRK×97, SLI×539, SEDG×42, GRRR×89, URBN×17, SIMO×5 |
| 2026-08-31 | -5.85 | $181.42 | MOS×58, RRC×33, CRK×97, SLI×539, SEDG×42, GRRR×89, URBN×17, SIMO×5 | $10,938.05 | +49.78 | +0.00 | — | MOS, RRC, CRK, SLI, SEDG, GRRR, URBN, SIMO | $10,915.89 | $10,915.89 | — |
| 2026-09-01 | -6.30 | $10,915.89 | — | $10,915.89 | -0.00 | +0.00 | — | — | $10,915.89 | $10,915.89 | — |
| 2026-09-02 | -3.83 | $10,915.89 | — | $10,915.89 | -0.00 | +0.00 | — | — | $10,915.89 | $10,915.89 | — |
| 2026-09-03 | -0.90 | $10,915.89 | — | $10,915.89 | -0.00 | -242.25 | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MRNA, ARCT | — | $168.76 | $10,653.63 | ATRC×25, HRMY×31, CABA×375, VSTM×169, RVTY×10, CRK×88, MRNA×9, ARCT×81 |
| 2026-09-04 | +2.25 | $168.76 | ATRC×25, HRMY×31, CABA×375, VSTM×169, RVTY×10, CRK×88, MRNA×9, ARCT×81 | $10,657.59 | +3.96 | -54.27 | ALEC, BHC, BMEA, OABI, OPK, VIR | HRMY, VSTM, RVTY, CRK, MRNA, ARCT | $4.55 | $10,554.69 | ATRC×25, CABA×375, ALEC×532, BHC×199, BMEA×705, OABI×280, OPK×843, VIR×116 |
| 2026-09-08 | -11.47 | $4.55 | ATRC×25, CABA×375, ALEC×532, BHC×199, BMEA×705, OABI×280, OPK×843, VIR×116 | $10,511.75 | -42.94 | +0.00 | — | ATRC, CABA, ALEC, BHC, BMEA, OABI, OPK, VIR | $10,468.88 | $10,468.88 | — |
| 2026-09-09 | -13.95 | $10,468.88 | — | $10,468.88 | +0.00 | +0.00 | — | — | $10,468.88 | $10,468.88 | — |
| 2026-09-10 | -13.28 | $10,468.88 | — | $10,468.88 | +0.00 | +0.00 | — | — | $10,468.88 | $10,468.88 | — |
| 2026-09-11 | +0.50 | $10,468.88 | — | $10,468.88 | +0.00 | -14.40 | AUPH | — | $9,164.25 | $10,452.25 | AUPH×80 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | 16:00 close · cash $97.53 · equity $10,153.12 vs 09:30 $10,000.00 (+153.12; session marks +185.07) · 8 name(s) marked open→close (per-name table). BTSG×20 09:30 $59.80 → close $60.23 +8.60; IREN×27 09:30 $45.98 → close $44.76 -32.94; TPG×24 09:30 $50.62 → close $54.62 +95.92; TGTX×25 09:30 $49.70 → close $47.94 -44.00; SLS×106 09:30 $11.70 → close $12.36 +69.96; HIMS×42 09:30 $29.74 → close $28.77 -40.74; INO×1543 09:30 $0.81 → close $0.90 +138.87; TNDM×53 09:30 $23.33 → close $23.13 -10.60 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) · 8 name(s) re-marked at the open (per-name table). BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $1,288.46 | ▼ -7.12 after sell → book $10,176.05; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $2,476.80 | ▼ -55.19 after sell → book $10,173.96; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,801.68 | ▲ +107.86 after sell → book $10,171.88; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $4,981.35 | ▼ -64.90 after sell → book $10,169.80; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $6,293.41 | ▲ +69.56 after sell → book $10,167.46; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $7,515.57 | ▼ -29.03 after sell → book $10,165.32; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $8,931.32 | ▲ +148.79 after sell → book $10,146.08; vs 09:30 mark -19.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $10,143.91 | ▼ -26.05 after sell → book $10,143.91; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,062.42 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+5.9; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,885.21 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+3.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,683.19 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+0.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,688.46 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $4,418.98 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $3,155.17 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1353 | $0.94 | $16.74 | — | $1,870.67 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 845 | $1.50 | $10.90 | — | $592.27 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $592.27 | ▲ close $10,193.91 vs 09:30 $10,178.12 (session +90.14) | 16:00 close · cash $592.27 · equity $10,193.91 vs 09:30 $10,178.12 (+15.79; session marks +90.14) · 8 name(s) marked open→close (per-name table). TLN×3 09:30 $359.83 → close $362.74 +8.73; VST×8 09:30 $146.90 → close $148.13 +9.84; NRG×10 09:30 $120.00 → close $126.24 +62.40; DAVE×3 09:30 $330.91 → close $334.57 +10.98; SLG×22 09:30 $57.61 → close $56.09 -33.44; MARA×140 09:30 $9.01 → close $9.20 +26.60; LDI×1353 09:30 $0.94 → close $0.90 -54.12; BTBT×845 09:30 $1.50 → close $1.57 +59.15 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $592.27 | ▲ 09:30 equity $10,196.20 vs yday $10,193.91 (+2.29) | 09:30 open · cash $592.27 (unchanged overnight, no fees) · equity $10,196.20 vs prior close $10,193.91 (+2.29) · 8 name(s) re-marked at the open (per-name table). TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×22 yday $56.09 → 09:30 $55.37 -15.84; MARA×140 yday $9.20 → 09:30 $9.22 +2.80; LDI×1353 yday $0.90 → 09:30 $0.91 +13.53; BTBT×845 yday $1.57 → 09:30 $1.52 -42.25 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,693.89 | ▲ +20.13 after sell → book $10,194.18; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $2,886.82 | ▲ +15.71 after sell → book $10,192.15; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $4,158.78 | ▲ +69.94 after sell → book $10,190.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $5,167.58 | ▲ +14.07 after sell → book $10,188.09; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $6,383.64 | ▼ -53.41 after sell → book $10,186.02; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 140 | $9.22 | $2.44 | $+24.55 | $7,672.00 | ▲ +24.55 after sell → book $10,183.57; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1353 | $0.91 | $16.57 | $-73.89 | $8,882.61 | ▼ -73.89 after sell → book $10,167.01; vs 09:30 mark -16.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 845 | $1.52 | $11.05 | $-5.05 | $10,155.96 | ▼ -5.05 after sell → book $10,155.96; vs 09:30 mark -11.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,907.02 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+6.7; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,762.85 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+5.8; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,544.64 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+8.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 313 | $4.05 | $4.04 | — | $5,272.95 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 150 | $8.46 | $2.44 | — | $4,001.51 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $2,731.92 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=-7.2; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 391 | $3.24 | $5.04 | — | $1,460.04 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=+0.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 250 | $5.07 | $3.23 | — | $189.31 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=-4.7; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $189.31 | ▲ close $10,137.18 vs 09:30 $10,196.20 (session +4.10) | 16:00 close · cash $189.31 · equity $10,137.18 vs 09:30 $10,196.20 (-59.02; session marks +4.10) · 8 name(s) marked open→close (per-name table). DVN×27 09:30 $46.18 → close $47.57 +37.53; EOG×8 09:30 $142.77 → close $146.15 +27.04; FANG×6 09:30 $202.70 → close $206.29 +21.54; TMC×313 09:30 $4.05 → close $3.77 -87.64; TGB×150 09:30 $8.46 → close $8.77 +46.50; ELF×14 09:30 $90.54 → close $93.66 +43.68; DNN×391 09:30 $3.24 → close $3.19 -19.55; NB×250 09:30 $5.07 → close $4.81 -65.00 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $189.31 | ▼ 09:30 equity $10,059.24 vs yday $10,137.18 (-77.94) | 09:30 open · cash $189.31 (unchanged overnight, no fees) · equity $10,059.24 vs prior close $10,137.18 (-77.94) · 8 name(s) re-marked at the open (per-name table). DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; TMC×313 yday $3.77 → 09:30 $3.72 -15.65; TGB×150 yday $8.77 → 09:30 $8.55 -33.00; ELF×14 yday $93.66 → 09:30 $93.44 -3.08; DNN×391 yday $3.19 → 09:30 $3.11 -31.28; NB×250 yday $4.81 → 09:30 $4.66 -37.50 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,483.22 | ▲ +44.98 after sell → book $10,057.15; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,665.51 | ▲ +38.11 after sell → book $10,055.12; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,917.06 | ▲ +33.34 after sell → book $10,053.09; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 313 | $3.72 | $4.10 | $-111.43 | $5,077.32 | ▼ -111.43 after sell → book $10,048.99; vs 09:30 mark -4.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 150 | $8.55 | $2.48 | $+8.58 | $6,357.35 | ▲ +8.58 after sell → book $10,046.52; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 14 | $93.44 | $2.05 | $+36.52 | $7,663.45 | ▲ +36.52 after sell → book $10,044.46; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 391 | $3.11 | $5.12 | $-60.99 | $8,874.34 | ▼ -60.99 after sell → book $10,039.34; vs 09:30 mark -5.12 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `NB` | 250 | $4.66 | $3.28 | $-109.00 | $10,036.07 | ▼ -109.00 after sell → book $10,036.07; vs 09:30 mark -3.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,036.07 | ▲ close $10,036.07 vs 09:30 $10,059.24 (session +0.00) | 16:00 close · cash $10,036.07 · no lots left · equity $10,036.07. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,036.07 | ▲ 09:30 equity $10,036.07 vs yday $10,036.07 (-0.00) | 09:30 open · cash $10,036.07 · no holdings · equity $10,036.07 vs prior close $10,036.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,036.07 | ▲ close $10,036.07 vs 09:30 $10,036.07 (session +0.00) | 16:00 close · cash $10,036.07 · no lots left · equity $10,036.07. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,036.07 | ▲ 09:30 equity $10,036.07 vs yday $10,036.07 (-0.00) | 09:30 open · cash $10,036.07 · no holdings · equity $10,036.07 vs prior close $10,036.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,780.34 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,595.19 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,354.02 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 217 | $5.77 | $2.80 | — | $5,099.13 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,860.26 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,613.68 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 716 | $1.75 | $9.24 | — | $1,351.45 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $193.11 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $193.11 | ▲ close $10,244.74 vs 09:30 $10,036.07 (session +233.39) | 16:00 close · cash $193.11 · equity $10,244.74 vs 09:30 $10,036.07 (+208.67; session marks +233.39) · 8 name(s) marked open→close (per-name table). AG×61 09:30 $20.55 → close $21.19 +39.04; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×60 09:30 $20.65 → close $21.11 +27.60; HDSN×217 09:30 $5.77 → close $5.57 -43.40; IAG×63 09:30 $19.63 → close $20.50 +54.81; KGC×42 09:30 $29.63 → close $31.43 +75.60; NFGC×716 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $193.11 | ▲ 09:30 equity $10,512.85 vs yday $10,244.74 (+268.11) | 09:30 open · cash $193.11 (unchanged overnight, no fees) · equity $10,512.85 vs prior close $10,244.74 (+268.11) · 8 name(s) re-marked at the open (per-name table). AG×61 yday $21.19 → 09:30 $21.90 +43.31; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×60 yday $21.11 → 09:30 $21.75 +38.40; HDSN×217 yday $5.57 → 09:30 $5.67 +21.70; IAG×63 yday $20.50 → 09:30 $21.17 +42.21; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×716 yday $1.75 → 09:30 $1.79 +28.64; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 61 | $21.90 | $2.19 | $+77.98 | $1,526.82 | ▲ +77.98 after sell → book $10,510.66; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,769.13 | ▲ +57.15 after sell → book $10,508.61; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 60 | $21.75 | $2.19 | $+61.64 | $4,071.94 | ▲ +61.64 after sell → book $10,506.42; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 217 | $5.67 | $2.85 | $-27.34 | $5,299.48 | ▼ -27.34 after sell → book $10,503.57; vs 09:30 mark -2.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 63 | $21.17 | $2.20 | $+92.64 | $6,630.99 | ▲ +92.64 after sell → book $10,501.37; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $7,980.00 | ▲ +102.43 after sell → book $10,499.24; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 716 | $1.79 | $9.37 | $+10.04 | $9,252.27 | ▲ +10.04 after sell → book $10,489.87; vs 09:30 mark -9.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,487.84 | ▲ +77.23 after sell → book $10,487.84; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,291.52 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 76 | $17.20 | $2.22 | — | $7,982.10 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,682.29 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 117 | $11.13 | $2.34 | — | $5,377.74 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 530 | $2.47 | $6.84 | — | $4,061.80 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 679 | $1.93 | $8.76 | — | $2,742.57 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,486.40 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 993 | $1.32 | $12.81 | — | $162.83 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $162.83 | ▲ close $10,710.24 vs 09:30 $10,512.85 (session +261.45) | 16:00 close · cash $162.83 · equity $10,710.24 vs 09:30 $10,512.85 (+197.39; session marks +261.45) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×76 09:30 $17.20 → close $16.65 -41.80; AEM×6 09:30 $216.30 → close $216.06 -1.44; ARCT×117 09:30 $11.13 → close $13.45 +271.44; AUTL×530 09:30 $2.47 → close $2.41 -31.80; CRDL×679 09:30 $1.93 → close $1.86 -47.53; CRSP×21 09:30 $59.72 → close $59.50 -4.62; CYPH×993 09:30 $1.32 → close $1.42 +99.30 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $162.83 | ▲ 09:30 equity $11,088.50 vs yday $10,710.24 (+378.26) | 09:30 open · cash $162.83 (unchanged overnight, no fees) · equity $11,088.50 vs prior close $10,710.24 (+378.26) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×76 yday $16.65 → 09:30 $16.57 -6.08; AEM×6 yday $216.06 → 09:30 $217.03 +5.82; ARCT×117 yday $13.45 → 09:30 $13.33 -14.04; AUTL×530 yday $2.41 → 09:30 $2.40 -5.30; CRDL×679 yday $1.86 → 09:30 $1.88 +13.58; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; CYPH×993 yday $1.42 → 09:30 $1.83 +407.13 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,365.89 | ▲ +6.74 after sell → book $11,086.46; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 76 | $16.57 | $2.24 | $-52.34 | $2,622.97 | ▼ -52.34 after sell → book $11,084.22; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,923.12 | ▲ +0.34 after sell → book $11,082.19; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 117 | $13.33 | $2.37 | $+252.69 | $5,480.36 | ▲ +252.69 after sell → book $11,079.82; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 530 | $2.40 | $6.94 | $-50.87 | $6,745.42 | ▼ -50.87 after sell → book $11,072.88; vs 09:30 mark -6.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 679 | $1.88 | $8.88 | $-51.59 | $8,013.06 | ▼ -51.59 after sell → book $11,064.00; vs 09:30 mark -8.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.75 | $2.07 | $-24.50 | $9,244.74 | ▼ -24.50 after sell → book $11,061.93; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 993 | $1.83 | $12.99 | $+480.63 | $11,048.94 | ▲ +480.63 after sell → book $11,048.94; vs 09:30 mark -12.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,048.94 | ▲ close $11,048.94 vs 09:30 $11,088.50 (session +0.00) | 16:00 close · cash $11,048.94 · no lots left · equity $11,048.94. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,048.94 | ▲ 09:30 equity $11,048.94 vs yday $11,048.94 (-0.00) | 09:30 open · cash $11,048.94 · no holdings · equity $11,048.94 vs prior close $11,048.94 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 58 | $23.77 | $2.16 | — | $9,668.12 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=+13.0; leftover $1381.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 125 | $10.98 | $2.37 | — | $8,293.25 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+1.2; leftover $1381.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.19 | $2.06 | — | $6,945.01 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+7.4; leftover $1381.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 165 | $8.35 | $2.48 | — | $5,564.78 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1381.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 279 | $4.94 | $3.60 | — | $4,182.92 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+7.1; leftover $1381.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $2,900.01 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+6.0; leftover $1381.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 190 | $7.25 | $2.56 | — | $1,519.95 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1381.12 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3857 | $0.36 | $25.38 | — | $113.77 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer; ret5=-15.6; leftover $1381.12 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.77 | ▲ close $11,261.91 vs 09:30 $11,048.94 (session +255.58) | 16:00 close · cash $113.77 · equity $11,261.91 vs 09:30 $11,048.94 (+212.97; session marks +255.58) · 8 name(s) marked open→close (per-name table). MOS×58 09:30 $23.77 → close $24.27 +29.00; OCUL×125 09:30 $10.98 → close $10.88 -12.50; INSP×22 09:30 $61.19 → close $61.07 -2.64; CRMD×165 09:30 $8.35 → close $8.56 +34.65; RZLT×279 09:30 $4.94 → close $5.01 +19.53; HCA×3 09:30 $426.97 → close $428.76 +5.37; CAPR×190 09:30 $7.25 → close $8.29 +197.60; SAFX×3857 09:30 $0.36 → close $0.35 -15.43 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.77 | ▼ 09:30 equity $11,260.69 vs yday $11,261.91 (-1.22) | 09:30 open · cash $113.77 (unchanged overnight, no fees) · equity $11,260.69 vs prior close $11,261.91 (-1.22) · 8 name(s) re-marked at the open (per-name table). MOS×58 yday $24.27 → 09:30 $24.84 +33.06; OCUL×125 yday $10.88 → 09:30 $10.79 -11.25; INSP×22 yday $61.07 → 09:30 $60.07 -22.00; CRMD×165 yday $8.56 → 09:30 $8.60 +6.60; RZLT×279 yday $5.01 → 09:30 $5.01 +0.00; HCA×3 yday $428.76 → 09:30 $427.50 -3.78; CAPR×190 yday $8.29 → 09:30 $8.29 +0.00; SAFX×3857 yday $0.35 → 09:30 $0.35 -3.86 | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 190 | $8.29 | $2.60 | $+192.44 | $1,686.26 | ▲ +192.44 after sell → book $11,258.08; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 3857 | $0.35 | $25.84 | $-70.50 | $3,021.95 | ▼ -70.50 after sell → book $11,232.25; vs 09:30 mark -25.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 48 | $31.21 | $2.13 | — | $1,521.73 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1510.97 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 135 | $11.12 | $2.40 | — | $18.14 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1510.97 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.14 | ▼ close $11,187.78 vs 09:30 $11,260.69 (session -39.94) | 16:00 close · cash $18.14 · equity $11,187.78 vs 09:30 $11,260.69 (-72.91; session marks -39.94) · 8 name(s) marked open→close (per-name table). MOS×58 09:30 $24.84 → close $24.16 -39.44; OCUL×125 09:30 $10.79 → close $10.77 -2.50; INSP×22 09:30 $60.07 → close $61.80 +38.06; CRMD×165 09:30 $8.60 → close $8.39 -34.65; RZLT×279 09:30 $5.01 → close $5.04 +8.37; HCA×3 09:30 $427.50 → close $427.16 -1.02; AVBP×48 09:30 $31.21 → close $31.14 -3.36; FLNC×135 09:30 $11.12 → close $11.08 -5.40 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.14 | ▲ 09:30 equity $11,227.42 vs yday $11,187.78 (+39.64) | 09:30 open · cash $18.14 (unchanged overnight, no fees) · equity $11,227.42 vs prior close $11,187.78 (+39.64) · 8 name(s) re-marked at the open (per-name table). MOS×58 yday $24.16 → 09:30 $24.00 -9.28; OCUL×125 yday $10.77 → 09:30 $10.63 -17.50; INSP×22 yday $61.80 → 09:30 $62.10 +6.60; CRMD×165 yday $8.39 → 09:30 $8.49 +16.50; RZLT×279 yday $5.04 → 09:30 $5.07 +8.37; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; AVBP×48 yday $31.14 → 09:30 $30.79 -16.80; FLNC×135 yday $11.08 → 09:30 $11.52 +59.40 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 125 | $10.63 | $2.40 | $-48.51 | $1,344.49 | ▼ -48.51 after sell → book $11,225.02; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 22 | $62.10 | $2.08 | $+15.89 | $2,708.61 | ▲ +15.89 after sell → book $11,222.94; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 165 | $8.49 | $2.52 | $+18.09 | $4,106.94 | ▲ +18.09 after sell → book $11,220.42; vs 09:30 mark -2.52 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 279 | $5.07 | $3.66 | $+29.01 | $5,517.81 | ▲ +29.01 after sell → book $11,216.76; vs 09:30 mark -3.66 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-11.10 | $6,789.62 | ▼ -11.10 after sell → book $11,214.74; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 48 | $30.79 | $2.16 | $-24.45 | $8,265.39 | ▼ -24.45 after sell → book $11,212.59; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 135 | $11.52 | $2.43 | $+49.18 | $9,818.16 | ▲ +49.18 after sell → book $11,210.16; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 33 | $41.44 | $2.09 | — | $8,448.55 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+3.1; leftover $1402.59 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 97 | $14.42 | $2.28 | — | $7,047.53 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+7.1; leftover $1402.59 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 539 | $2.60 | $6.95 | — | $5,639.17 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+13.0; leftover $1402.59 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 17 | $81.65 | $2.04 | — | $4,249.08 | — | union ∩ join_present, no 🚨; gate join_present=True; list mover_buy; 🔵; ret5=+2.0; leftover $1402.59 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 306 | $4.57 | $3.95 | — | $2,846.72 | — | union ∩ join_present, no 🚨; gate join_present=True; list mover_buy; 🔵; ret5=+1.1; leftover $1402.59 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 18 | $74.54 | $2.04 | — | $1,502.95 | — | union ∩ join_present, no 🚨; gate join_present=True; list mover_buy; 🔵; ret5=-0.1; leftover $1402.59 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $533.95 | — | union ∩ join_present, no 🚨; gate join_present=True; list mover_buy; 🔵; ret5=+0.1; leftover $1402.59 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $533.95 | ▲ close $11,212.51 vs 09:30 $11,227.42 (session +23.70) | 16:00 close · cash $533.95 · equity $11,212.51 vs 09:30 $11,227.42 (-14.91; session marks +23.70) · 8 name(s) marked open→close (per-name table). MOS×58 09:30 $24.00 → close $23.76 -13.92; RRC×33 09:30 $41.44 → close $41.64 +6.60; CRK×97 09:30 $14.42 → close $14.62 +19.40; SLI×539 09:30 $2.60 → close $2.64 +21.56; ACMR×17 09:30 $81.65 → close $80.49 -19.72; GGB×306 09:30 $4.57 → close $4.70 +39.78; MT×18 09:30 $74.54 → close $74.63 +1.62; MU×1 09:30 $967.01 → close $935.39 -31.62 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $533.95 | ▲ 09:30 equity $11,217.02 vs yday $11,212.51 (+4.51) | 09:30 open · cash $533.95 (unchanged overnight, no fees) · equity $11,217.02 vs prior close $11,212.51 (+4.51) · 8 name(s) re-marked at the open (per-name table). MOS×58 yday $23.76 → 09:30 $23.95 +11.02; RRC×33 yday $41.64 → 09:30 $41.74 +3.30; CRK×97 yday $14.62 → 09:30 $14.63 +0.97; SLI×539 yday $2.64 → 09:30 $2.68 +21.56; ACMR×17 yday $80.49 → 09:30 $79.27 -20.74; GGB×306 yday $4.70 → 09:30 $4.67 -9.18; MT×18 yday $74.63 → 09:30 $75.39 +13.68; MU×1 yday $935.39 → 09:30 $919.29 -16.10 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 17 | $79.27 | $2.06 | $-44.56 | $1,879.48 | ▼ -44.56 after sell → book $11,214.96; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 306 | $4.67 | $4.01 | $+22.64 | $3,304.49 | ▲ +22.64 after sell → book $11,210.95; vs 09:30 mark -4.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 18 | $75.39 | $2.06 | $+11.19 | $4,659.44 | ▲ +11.19 after sell → book $11,208.88; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $5,576.72 | ▼ -51.73 after sell → book $11,206.87; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 42 | $32.90 | $2.12 | — | $4,192.80 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1394.18 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 89 | $15.66 | $2.26 | — | $2,796.81 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1394.18 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 17 | $79.42 | $2.04 | — | $1,444.63 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1394.18 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $181.42 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1394.18 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $181.42 | ▼ close $10,888.27 vs 09:30 $11,217.02 (session -310.18) | 16:00 close · cash $181.42 · equity $10,888.27 vs 09:30 $11,217.02 (-328.75; session marks -310.18) · 8 name(s) marked open→close (per-name table). MOS×58 09:30 $23.95 → close $23.60 -20.30; RRC×33 09:30 $41.74 → close $41.46 -9.24; CRK×97 09:30 $14.63 → close $14.29 -32.98; SLI×539 09:30 $2.68 → close $2.55 -70.07; SEDG×42 09:30 $32.90 → close $31.41 -62.58; GRRR×89 09:30 $15.66 → close $14.41 -111.25; URBN×17 09:30 $79.42 → close $81.09 +28.39; SIMO×5 09:30 $252.24 → close $245.81 -32.15 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $181.42 | ▲ 09:30 equity $10,938.05 vs yday $10,888.27 (+49.78) | 09:30 open · cash $181.42 (unchanged overnight, no fees) · equity $10,938.05 vs prior close $10,888.27 (+49.78) · 8 name(s) re-marked at the open (per-name table). MOS×58 yday $23.60 → 09:30 $23.68 +4.64; RRC×33 yday $41.46 → 09:30 $42.00 +17.82; CRK×97 yday $14.29 → 09:30 $14.54 +24.25; SLI×539 yday $2.55 → 09:30 $2.58 +16.17; SEDG×42 yday $31.41 → 09:30 $31.15 -10.92; GRRR×89 yday $14.41 → 09:30 $14.44 +2.67; URBN×17 yday $81.09 → 09:30 $80.44 -11.05; SIMO×5 yday $245.81 → 09:30 $247.05 +6.20 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 58 | $23.68 | $2.19 | $-9.57 | $1,552.68 | ▼ -9.57 after sell → book $10,935.87; vs 09:30 mark -2.18 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 33 | $42.00 | $2.11 | $+14.28 | $2,936.57 | ▲ +14.28 after sell → book $10,933.76; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 97 | $14.54 | $2.31 | $+7.05 | $4,344.64 | ▲ +7.05 after sell → book $10,931.45; vs 09:30 mark -2.31 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 539 | $2.58 | $7.05 | $-24.79 | $5,728.20 | ▼ -24.79 after sell → book $10,924.39; vs 09:30 mark -7.06 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 42 | $31.15 | $2.14 | $-77.75 | $7,034.37 | ▼ -77.75 after sell → book $10,922.26; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 89 | $14.44 | $2.28 | $-113.12 | $8,317.25 | ▼ -113.12 after sell → book $10,919.98; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 17 | $80.44 | $2.06 | $+13.24 | $9,682.66 | ▲ +13.24 after sell → book $10,917.91; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $10,915.89 | ▼ -29.98 after sell → book $10,915.89; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,915.89 | ▲ close $10,915.89 vs 09:30 $10,938.05 (session +0.00) | 16:00 close · cash $10,915.89 · no lots left · equity $10,915.89. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,915.89 | ▲ 09:30 equity $10,915.89 vs yday $10,915.89 (-0.00) | 09:30 open · cash $10,915.89 · no holdings · equity $10,915.89 vs prior close $10,915.89 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,915.89 | ▲ close $10,915.89 vs 09:30 $10,915.89 (session +0.00) | 16:00 close · cash $10,915.89 · no lots left · equity $10,915.89. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,915.89 | ▲ 09:30 equity $10,915.89 vs yday $10,915.89 (-0.00) | 09:30 open · cash $10,915.89 · no holdings · equity $10,915.89 vs prior close $10,915.89 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,915.89 | ▲ close $10,915.89 vs 09:30 $10,915.89 (session +0.00) | 16:00 close · cash $10,915.89 · no lots left · equity $10,915.89. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,915.89 | ▲ 09:30 equity $10,915.89 vs yday $10,915.89 (-0.00) | 09:30 open · cash $10,915.89 · no holdings · equity $10,915.89 vs prior close $10,915.89 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,591.82 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1364.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $42.93 | $2.08 | — | $8,258.91 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1364.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 375 | $3.63 | $4.84 | — | $6,892.82 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1364.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 169 | $8.03 | $2.50 | — | $5,533.26 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1364.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,206.74 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1364.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 88 | $15.45 | $2.25 | — | $2,844.88 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1364.49 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,529.36 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1364.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 81 | $16.77 | $2.23 | — | $168.76 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1364.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $168.76 | ▼ close $10,653.63 vs 09:30 $10,915.89 (session -242.25) | 16:00 close · cash $168.76 · equity $10,653.63 vs 09:30 $10,915.89 (-262.26; session marks -242.25) · 8 name(s) marked open→close (per-name table). ATRC×25 09:30 $52.88 → close $52.46 -10.50; HRMY×31 09:30 $42.93 → close $41.86 -33.17; CABA×375 09:30 $3.63 → close $3.48 -56.25; VSTM×169 09:30 $8.03 → close $7.98 -8.45; RVTY×10 09:30 $132.45 → close $130.63 -18.20; CRK×88 09:30 $15.45 → close $14.95 -44.00; MRNA×9 09:30 $145.94 → close $148.87 +26.33; ARCT×81 09:30 $16.77 → close $15.56 -98.01 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $168.76 | ▲ 09:30 equity $10,657.59 vs yday $10,653.63 (+3.96) | 09:30 open · cash $168.76 (unchanged overnight, no fees) · equity $10,657.59 vs prior close $10,653.63 (+3.96) · 8 name(s) re-marked at the open (per-name table). ATRC×25 yday $52.46 → 09:30 $52.03 -10.75; HRMY×31 yday $41.86 → 09:30 $41.50 -11.16; CABA×375 yday $3.48 → 09:30 $3.46 -7.50; VSTM×169 yday $7.98 → 09:30 $7.91 -11.83; RVTY×10 yday $130.63 → 09:30 $130.03 -6.00; CRK×88 yday $14.95 → 09:30 $15.00 +4.40; MRNA×9 yday $148.87 → 09:30 $153.62 +42.75; ARCT×81 yday $15.56 → 09:30 $15.61 +4.05 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 31 | $41.50 | $2.10 | $-48.52 | $1,453.15 | ▼ -48.52 after sell → book $10,655.48; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 169 | $7.91 | $2.54 | $-25.31 | $2,787.41 | ▼ -25.31 after sell → book $10,652.95; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $4,085.67 | ▼ -28.26 after sell → book $10,650.91; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 88 | $15.00 | $2.28 | $-44.13 | $5,403.39 | ▼ -44.13 after sell → book $10,648.63; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 9 | $153.62 | $2.04 | $+65.02 | $6,783.93 | ▲ +65.02 after sell → book $10,646.59; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 81 | $15.61 | $2.26 | $-98.45 | $8,046.08 | ▼ -98.45 after sell → book $10,644.33; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 532 | $2.52 | $6.86 | — | $6,698.58 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1341.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 199 | $6.71 | $2.59 | — | $5,360.70 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1341.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 705 | $1.90 | $9.09 | — | $4,012.11 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1341.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 280 | $4.78 | $3.61 | — | $2,670.10 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1341.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 843 | $1.59 | $10.87 | — | $1,318.85 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1341.01 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 116 | $11.31 | $2.34 | — | $4.55 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1341.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.55 | ▼ close $10,554.69 vs 09:30 $10,657.59 (session -54.27) | 16:00 close · cash $4.55 · equity $10,554.69 vs 09:30 $10,657.59 (-102.90; session marks -54.27) · 8 name(s) marked open→close (per-name table). ATRC×25 09:30 $52.03 → close $51.52 -12.75; CABA×375 09:30 $3.46 → close $3.47 +3.75; ALEC×532 09:30 $2.52 → close $2.46 -31.92; BHC×199 09:30 $6.71 → close $6.56 -29.85; BMEA×705 09:30 $1.90 → close $2.03 +91.65; OABI×280 09:30 $4.78 → close $4.33 -126.00; OPK×843 09:30 $1.59 → close $1.64 +42.15; VIR×116 09:30 $11.31 → close $11.38 +8.70 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.55 | ▼ 09:30 equity $10,511.75 vs yday $10,554.69 (-42.94) | 09:30 open · cash $4.55 (unchanged overnight, no fees) · equity $10,511.75 vs prior close $10,554.69 (-42.94) · 8 name(s) re-marked at the open (per-name table). ATRC×25 yday $51.52 → 09:30 $54.31 +69.75; CABA×375 yday $3.47 → 09:30 $3.43 -15.00; ALEC×532 yday $2.46 → 09:30 $2.38 -42.56; BHC×199 yday $6.56 → 09:30 $6.57 +1.99; BMEA×705 yday $2.03 → 09:30 $2.00 -21.15; OABI×280 yday $4.33 → 09:30 $4.30 -8.40; OPK×843 yday $1.64 → 09:30 $1.63 -8.43; VIR×116 yday $11.38 → 09:30 $11.22 -19.14 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 25 | $54.31 | $2.09 | $+31.60 | $1,360.22 | ▲ +31.60 after sell → book $10,509.67; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 375 | $3.43 | $4.91 | $-84.75 | $2,641.56 | ▼ -84.75 after sell → book $10,504.76; vs 09:30 mark -4.91 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 532 | $2.38 | $6.96 | $-88.30 | $3,900.76 | ▼ -88.30 after sell → book $10,497.80; vs 09:30 mark -6.96 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 199 | $6.57 | $2.63 | $-33.08 | $5,205.56 | ▼ -33.08 after sell → book $10,495.17; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 705 | $2.00 | $9.22 | $+52.18 | $6,606.33 | ▲ +52.18 after sell → book $10,485.94; vs 09:30 mark -9.23 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 280 | $4.30 | $3.67 | $-141.68 | $7,806.67 | ▼ -141.68 after sell → book $10,482.28; vs 09:30 mark -3.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 843 | $1.63 | $11.03 | $+11.82 | $9,169.73 | ▲ +11.82 after sell → book $10,471.25; vs 09:30 mark -11.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 116 | $11.22 | $2.37 | $-15.15 | $10,468.88 | ▼ -15.15 after sell → book $10,468.88; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,468.88 | ▲ close $10,468.88 vs 09:30 $10,511.75 (session +0.00) | 16:00 close · cash $10,468.88 · no lots left · equity $10,468.88. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,468.88 | ▲ 09:30 equity $10,468.88 vs yday $10,468.88 (+0.00) | 09:30 open · cash $10,468.88 · no holdings · equity $10,468.88 vs prior close $10,468.88 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,468.88 | ▲ close $10,468.88 vs 09:30 $10,468.88 (session +0.00) | 16:00 close · cash $10,468.88 · no lots left · equity $10,468.88. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,468.88 | ▲ 09:30 equity $10,468.88 vs yday $10,468.88 (+0.00) | 09:30 open · cash $10,468.88 · no holdings · equity $10,468.88 vs prior close $10,468.88 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,468.88 | ▲ close $10,468.88 vs 09:30 $10,468.88 (session +0.00) | 16:00 close · cash $10,468.88 · no lots left · equity $10,468.88. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,468.88 | ▲ 09:30 equity $10,468.88 vs yday $10,468.88 (+0.00) | 09:30 open · cash $10,468.88 · no holdings · equity $10,468.88 vs prior close $10,468.88 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 80 | $16.28 | $2.23 | — | $9,164.25 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=-0.2; leftover $1308.61 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,164.25 | ▼ close $10,452.25 vs 09:30 $10,468.88 (session -14.40) | 16:00 close · cash $9,164.25 · equity $10,452.25 vs 09:30 $10,468.88 (-16.63; session marks -14.40) · 1 name(s) marked open→close (per-name table). AUPH×80 09:30 $16.28 → close $16.10 -14.40 | — |

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
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
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
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
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
| `AUPH` | 80 | 2026-09-11 @ $16.28 | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=-0.2; leftover $1308.61 |
