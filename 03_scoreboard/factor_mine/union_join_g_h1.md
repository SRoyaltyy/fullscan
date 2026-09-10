# Factor mine action — `union_join_g_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ join_g, no 🚨

Cash book **+4.05%** ($10,406) · signal-only (no cash/fees) was +6.51%. Starts YES **6/19**. Fills 168 · skips 59 · realized $+405.52.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the join camera (do several factors agree?) is green.
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
- **Gate** `join=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,405.53.

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
| 2026-08-25 | `BMEA` | 847 | — | $1.63 | +0.00 | $1.73 | +84.70 | +84.70 | +0.00 | +84.70 |
| 2026-08-25 | `CYPH` | 885 | — | $1.56 | +0.00 | $1.64 | +70.80 | +70.80 | +0.00 | +70.80 |
| 2026-08-26 | `MOS` | 58 | $24.27 | $24.84 | +33.06 | $24.16 | -39.44 | -6.38 | +62.06 | +22.62 |
| 2026-08-26 | `OCUL` | 125 | $10.88 | $10.79 | -11.25 | — | +0.00 | -11.25 | -23.75 | — |
| 2026-08-26 | `INSP` | 22 | $61.07 | $60.07 | -22.00 | — | +0.00 | -22.00 | -24.64 | — |
| 2026-08-26 | `CRMD` | 165 | $8.56 | $8.60 | +6.60 | — | +0.00 | +6.60 | +41.25 | — |
| 2026-08-26 | `RZLT` | 279 | $5.01 | $5.01 | +0.00 | — | +0.00 | +0.00 | +19.53 | — |
| 2026-08-26 | `HCA` | 3 | $428.76 | $427.50 | -3.78 | — | +0.00 | -3.78 | +1.59 | — |
| 2026-08-26 | `BMEA` | 847 | $1.73 | $1.75 | +21.17 | — | +0.00 | +21.17 | +105.88 | — |
| 2026-08-26 | `CYPH` | 885 | $1.64 | $1.60 | -35.40 | — | +0.00 | -35.40 | +35.40 | — |
| 2026-08-26 | `ABX` | 141 | — | $9.83 | +0.00 | $9.78 | -7.05 | -7.05 | +0.00 | -7.05 |
| 2026-08-26 | `AVEX` | 79 | — | $17.51 | +0.00 | $18.34 | +65.57 | +65.57 | +0.00 | +65.57 |
| 2026-08-26 | `BE` | 6 | — | $213.94 | +0.00 | $218.21 | +25.62 | +25.62 | +0.00 | +25.62 |
| 2026-08-26 | `BZ` | 83 | — | $16.77 | +0.00 | $18.84 | +171.81 | +171.81 | +0.00 | +171.81 |
| 2026-08-26 | `MAIR` | 50 | — | $27.59 | +0.00 | $28.51 | +46.00 | +46.00 | +0.00 | +46.00 |
| 2026-08-26 | `BRR` | 633 | — | $2.20 | +0.00 | $2.17 | -18.99 | -18.99 | +0.00 | -18.99 |
| 2026-08-26 | `SLQT` | 2389 | — | $0.58 | +0.00 | $0.55 | -78.84 | -78.84 | +0.00 | -78.84 |
| 2026-08-27 | `MOS` | 58 | $24.16 | $24.00 | -9.28 | $23.76 | -13.92 | -23.20 | +13.34 | -0.58 |
| 2026-08-27 | `ABX` | 141 | $9.78 | $9.68 | -14.10 | — | +0.00 | -14.10 | -21.15 | — |
| 2026-08-27 | `AVEX` | 79 | $18.34 | $18.43 | +7.11 | — | +0.00 | +7.11 | +72.68 | — |
| 2026-08-27 | `BE` | 6 | $218.21 | $227.10 | +53.34 | — | +0.00 | +53.34 | +78.96 | — |
| 2026-08-27 | `BZ` | 83 | $18.84 | $18.50 | -28.22 | — | +0.00 | -28.22 | +143.59 | — |
| 2026-08-27 | `MAIR` | 50 | $28.51 | $28.76 | +12.50 | — | +0.00 | +12.50 | +58.50 | — |
| 2026-08-27 | `BRR` | 633 | $2.17 | $2.19 | +12.66 | — | +0.00 | +12.66 | -6.33 | — |
| 2026-08-27 | `SLQT` | 2389 | $0.55 | $0.53 | -47.78 | — | +0.00 | -47.78 | -126.62 | — |
| 2026-08-27 | `RRC` | 34 | — | $41.44 | +0.00 | $41.64 | +6.80 | +6.80 | +0.00 | +6.80 |
| 2026-08-27 | `CRK` | 97 | — | $14.42 | +0.00 | $14.62 | +19.40 | +19.40 | +0.00 | +19.40 |
| 2026-08-27 | `SLI` | 542 | — | $2.60 | +0.00 | $2.64 | +21.68 | +21.68 | +0.00 | +21.68 |
| 2026-08-27 | `ACMR` | 17 | — | $81.65 | +0.00 | $80.49 | -19.72 | -19.72 | +0.00 | -19.72 |
| 2026-08-27 | `GGB` | 308 | — | $4.57 | +0.00 | $4.70 | +40.04 | +40.04 | +0.00 | +40.04 |
| 2026-08-27 | `MT` | 18 | — | $74.54 | +0.00 | $74.63 | +1.62 | +1.62 | +0.00 | +1.62 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-28 | `MOS` | 58 | $23.76 | $23.95 | +11.02 | — | +0.00 | +11.02 | +10.44 | — |
| 2026-08-28 | `RRC` | 34 | $41.64 | $41.74 | +3.40 | — | +0.00 | +3.40 | +10.20 | — |
| 2026-08-28 | `CRK` | 97 | $14.62 | $14.63 | +0.97 | — | +0.00 | +0.97 | +20.37 | — |
| 2026-08-28 | `SLI` | 542 | $2.64 | $2.68 | +21.68 | — | +0.00 | +21.68 | +43.36 | — |
| 2026-08-28 | `ACMR` | 17 | $80.49 | $79.27 | -20.74 | — | +0.00 | -20.74 | -40.46 | — |
| 2026-08-28 | `GGB` | 308 | $4.70 | $4.67 | -9.24 | — | +0.00 | -9.24 | +30.80 | — |
| 2026-08-28 | `MT` | 18 | $74.63 | $75.39 | +13.68 | — | +0.00 | +13.68 | +15.30 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `SEDG` | 42 | — | $32.90 | +0.00 | $31.41 | -62.58 | -62.58 | +0.00 | -62.58 |
| 2026-08-28 | `GRRR` | 89 | — | $15.66 | +0.00 | $14.41 | -111.25 | -111.25 | +0.00 | -111.25 |
| 2026-08-28 | `URBN` | 17 | — | $79.42 | +0.00 | $81.09 | +28.39 | +28.39 | +0.00 | +28.39 |
| 2026-08-28 | `SIMO` | 5 | — | $252.24 | +0.00 | $245.81 | -32.15 | -32.15 | +0.00 | -32.15 |
| 2026-08-28 | `ANF` | 9 | — | $146.07 | +0.00 | $148.42 | +21.15 | +21.15 | +0.00 | +21.15 |
| 2026-08-28 | `BZ` | 77 | — | $18.15 | +0.00 | $17.80 | -26.95 | -26.95 | +0.00 | -26.95 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `VYX` | 154 | — | $9.13 | +0.00 | $8.78 | -53.90 | -53.90 | +0.00 | -53.90 |
| 2026-08-31 | `SEDG` | 42 | $31.41 | $31.15 | -10.92 | — | +0.00 | -10.92 | -73.50 | — |
| 2026-08-31 | `GRRR` | 89 | $14.41 | $14.44 | +2.67 | — | +0.00 | +2.67 | -108.58 | — |
| 2026-08-31 | `URBN` | 17 | $81.09 | $80.44 | -11.05 | — | +0.00 | -11.05 | +17.34 | — |
| 2026-08-31 | `SIMO` | 5 | $245.81 | $247.05 | +6.20 | — | +0.00 | +6.20 | -25.95 | — |
| 2026-08-31 | `ANF` | 9 | $148.42 | $148.03 | -3.51 | — | +0.00 | -3.51 | +17.64 | — |
| 2026-08-31 | `BZ` | 77 | $17.80 | $17.70 | -7.70 | — | +0.00 | -7.70 | -34.65 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `VYX` | 154 | $8.78 | $8.66 | -18.48 | — | +0.00 | -18.48 | -72.38 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 25 | — | $52.88 | +0.00 | $52.46 | -10.50 | -10.50 | +0.00 | -10.50 |
| 2026-09-03 | `HRMY` | 31 | — | $42.93 | +0.00 | $41.86 | -33.17 | -33.17 | +0.00 | -33.17 |
| 2026-09-03 | `CABA` | 373 | — | $3.63 | +0.00 | $3.48 | -55.95 | -55.95 | +0.00 | -55.95 |
| 2026-09-03 | `VSTM` | 168 | — | $8.03 | +0.00 | $7.98 | -8.40 | -8.40 | +0.00 | -8.40 |
| 2026-09-03 | `RVTY` | 10 | — | $132.45 | +0.00 | $130.63 | -18.20 | -18.20 | +0.00 | -18.20 |
| 2026-09-03 | `CRK` | 87 | — | $15.45 | +0.00 | $14.95 | -43.50 | -43.50 | +0.00 | -43.50 |
| 2026-09-03 | `MRNA` | 9 | — | $145.94 | +0.00 | $148.87 | +26.33 | +26.33 | +0.00 | +26.33 |
| 2026-09-03 | `ARCT` | 80 | — | $16.77 | +0.00 | $15.56 | -96.80 | -96.80 | +0.00 | -96.80 |
| 2026-09-04 | `ATRC` | 25 | $52.46 | $52.03 | -10.75 | $51.52 | -12.75 | -23.50 | -21.25 | -34.00 |
| 2026-09-04 | `HRMY` | 31 | $41.86 | $41.50 | -11.16 | — | +0.00 | -11.16 | -44.33 | — |
| 2026-09-04 | `CABA` | 373 | $3.48 | $3.46 | -7.46 | $3.47 | +3.73 | -3.73 | -63.41 | -59.68 |
| 2026-09-04 | `VSTM` | 168 | $7.98 | $7.91 | -11.76 | — | +0.00 | -11.76 | -20.16 | — |
| 2026-09-04 | `RVTY` | 10 | $130.63 | $130.03 | -6.00 | — | +0.00 | -6.00 | -24.20 | — |
| 2026-09-04 | `CRK` | 87 | $14.95 | $15.00 | +4.35 | — | +0.00 | +4.35 | -39.15 | — |
| 2026-09-04 | `MRNA` | 9 | $148.87 | $153.62 | +42.75 | — | +0.00 | +42.75 | +69.08 | — |
| 2026-09-04 | `ARCT` | 80 | $15.56 | $15.61 | +4.00 | — | +0.00 | +4.00 | -92.80 | — |
| 2026-09-04 | `ALEC` | 528 | — | $2.52 | +0.00 | $2.46 | -31.68 | -31.68 | +0.00 | -31.68 |
| 2026-09-04 | `BHC` | 198 | — | $6.71 | +0.00 | $6.56 | -29.70 | -29.70 | +0.00 | -29.70 |
| 2026-09-04 | `BMEA` | 700 | — | $1.90 | +0.00 | $2.03 | +91.00 | +91.00 | +0.00 | +91.00 |
| 2026-09-04 | `OABI` | 278 | — | $4.78 | +0.00 | $4.33 | -125.10 | -125.10 | +0.00 | -125.10 |
| 2026-09-04 | `OPK` | 837 | — | $1.59 | +0.00 | $1.64 | +41.85 | +41.85 | +0.00 | +41.85 |
| 2026-09-04 | `VIR` | 115 | — | $11.31 | +0.00 | $11.38 | +8.62 | +8.62 | +0.00 | +8.62 |
| 2026-09-08 | `ATRC` | 25 | $51.52 | $54.31 | +69.75 | — | +0.00 | +69.75 | +35.75 | — |
| 2026-09-08 | `CABA` | 373 | $3.47 | $3.43 | -14.92 | — | +0.00 | -14.92 | -74.60 | — |
| 2026-09-08 | `ALEC` | 528 | $2.46 | $2.38 | -42.24 | — | +0.00 | -42.24 | -73.92 | — |
| 2026-09-08 | `BHC` | 198 | $6.56 | $6.57 | +1.98 | — | +0.00 | +1.98 | -27.72 | — |
| 2026-09-08 | `BMEA` | 700 | $2.03 | $2.00 | -21.00 | — | +0.00 | -21.00 | +70.00 | — |
| 2026-09-08 | `OABI` | 278 | $4.33 | $4.30 | -8.34 | — | +0.00 | -8.34 | -133.44 | — |
| 2026-09-08 | `OPK` | 837 | $1.64 | $1.63 | -8.37 | — | +0.00 | -8.37 | +33.48 | — |
| 2026-09-08 | `VIR` | 115 | $11.38 | $11.22 | -18.97 | — | +0.00 | -18.97 | -10.35 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

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
| 2026-08-25 | +1.80 | $11,048.94 | — | $11,048.94 | -0.00 | +228.91 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, CYPH | — | $116.46 | $11,240.84 | MOS×58, OCUL×125, INSP×22, CRMD×165, RZLT×279, HCA×3, BMEA×847, CYPH×885 |
| 2026-08-26 | +2.02 | $116.46 | MOS×58, OCUL×125, INSP×22, CRMD×165, RZLT×279, HCA×3, BMEA×847, CYPH×885 | $11,229.24 | -11.60 | +164.68 | ABX, AVEX, BE, BZ, MAIR, BRR, SLQT | OCUL, INSP, CRMD, RZLT, HCA, BMEA, CYPH | $103.15 | $11,318.31 | MOS×58, ABX×141, AVEX×79, BE×6, BZ×83, MAIR×50, BRR×633, SLQT×2389 |
| 2026-08-27 | — | $103.15 | MOS×58, ABX×141, AVEX×79, BE×6, BZ×83, MAIR×50, BRR×633, SLQT×2389 | $11,304.54 | -13.77 | +24.28 | RRC, CRK, SLI, ACMR, GGB, MT, MU | ABX, AVEX, BE, BZ, MAIR, BRR, SLQT | $530.21 | $11,267.73 | MOS×58, RRC×34, CRK×97, SLI×542, ACMR×17, GGB×308, MT×18, MU×1 |
| 2026-08-28 | +0.75 | $530.21 | MOS×58, RRC×34, CRK×97, SLI×542, ACMR×17, GGB×308, MT×18, MU×1 | $11,272.40 | +4.67 | -332.60 | SEDG, GRRR, URBN, SIMO, ANF, BZ, SMTC, VYX | MOS, RRC, CRK, SLI, ACMR, GGB, MT, MU | $450.48 | $10,898.80 | SEDG×42, GRRR×89, URBN×17, SIMO×5, ANF×9, BZ×77, SMTC×9, VYX×154 |
| 2026-08-31 | -5.85 | $450.48 | SEDG×42, GRRR×89, URBN×17, SIMO×5, ANF×9, BZ×77, SMTC×9, VYX×154 | $10,866.18 | -32.62 | +0.00 | — | SEDG, GRRR, URBN, SIMO, ANF, BZ, SMTC, VYX | $10,848.87 | $10,848.87 | — |
| 2026-09-01 | -6.30 | $10,848.87 | — | $10,848.87 | -0.00 | +0.00 | — | — | $10,848.87 | $10,848.87 | — |
| 2026-09-02 | -3.83 | $10,848.87 | — | $10,848.87 | -0.00 | +0.00 | — | — | $10,848.87 | $10,848.87 | — |
| 2026-09-03 | -0.90 | $10,848.87 | — | $10,848.87 | -0.00 | -240.19 | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MRNA, ARCT | — | $149.28 | $10,588.70 | ATRC×25, HRMY×31, CABA×373, VSTM×168, RVTY×10, CRK×87, MRNA×9, ARCT×80 |
| 2026-09-04 | +2.25 | $149.28 | ATRC×25, HRMY×31, CABA×373, VSTM×168, RVTY×10, CRK×87, MRNA×9, ARCT×80 | $10,592.67 | +3.97 | -54.03 | ALEC, BHC, BMEA, OABI, OPK, VIR | HRMY, VSTM, RVTY, CRK, MRNA, ARCT | $3.50 | $10,490.26 | ATRC×25, CABA×373, ALEC×528, BHC×198, BMEA×700, OABI×278, OPK×837, VIR×115 |
| 2026-09-08 | -11.47 | $3.50 | ATRC×25, CABA×373, ALEC×528, BHC×198, BMEA×700, OABI×278, OPK×837, VIR×115 | $10,448.15 | -42.11 | +0.00 | — | ATRC, CABA, ALEC, BHC, BMEA, OABI, OPK, VIR | $10,405.53 | $10,405.53 | — |
| 2026-09-09 | -13.95 | $10,405.53 | — | $10,405.53 | -0.00 | +0.00 | — | — | $10,405.53 | $10,405.53 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
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
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,062.42 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+5.9; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,885.21 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+3.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,683.19 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+0.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,688.46 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $4,418.98 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $3,155.17 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1353 | $0.94 | $16.74 | — | $1,870.67 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 845 | $1.50 | $10.90 | — | $592.27 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
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
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,907.02 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+6.7; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,762.85 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+5.8; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,544.64 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+8.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 313 | $4.05 | $4.04 | — | $5,272.95 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 150 | $8.46 | $2.44 | — | $4,001.51 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $2,731.92 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=-7.2; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 391 | $3.24 | $5.04 | — | $1,460.04 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+0.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 250 | $5.07 | $3.23 | — | $189.31 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=-4.7; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🔴 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $189.31 | ▲ close $10,137.18 vs 09:30 $10,196.20 (session +4.10) | 16:00 close · cash $189.31 · equity $10,137.18 vs 09:30 $10,196.20 (-59.02; session marks +4.10) · 8 name(s) marked open→close (per-name table). DVN×27 09:30 $46.18 → close $47.57 +37.53; EOG×8 09:30 $142.77 → close $146.15 +27.04; FANG×6 09:30 $202.70 → close $206.29 +21.54; TMC×313 09:30 $4.05 → close $3.77 -87.64; TGB×150 09:30 $8.46 → close $8.77 +46.50; ELF×14 09:30 $90.54 → close $93.66 +43.68; DNN×391 09:30 $3.24 → close $3.19 -19.55; NB×250 09:30 $5.07 → close $4.81 -65.00 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $189.31 | ▼ 09:30 equity $10,059.24 vs yday $10,137.18 (-77.94) | 09:30 open · cash $189.31 (unchanged overnight, no fees) · equity $10,059.24 vs prior close $10,137.18 (-77.94) · 8 name(s) re-marked at the open (per-name table). DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; TMC×313 yday $3.77 → 09:30 $3.72 -15.65; TGB×150 yday $8.77 → 09:30 $8.55 -33.00; ELF×14 yday $93.66 → 09:30 $93.44 -3.08; DNN×391 yday $3.19 → 09:30 $3.11 -31.28; NB×250 yday $4.81 → 09:30 $4.66 -37.50 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,483.22 | ▲ +44.98 after sell → book $10,057.15; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,665.51 | ▲ +38.11 after sell → book $10,055.12; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,917.06 | ▲ +33.34 after sell → book $10,053.09; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 313 | $3.72 | $4.10 | $-111.43 | $5,077.32 | ▼ -111.43 after sell → book $10,048.99; vs 09:30 mark -4.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 150 | $8.55 | $2.48 | $+8.58 | $6,357.35 | ▲ +8.58 after sell → book $10,046.52; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 14 | $93.44 | $2.05 | $+36.52 | $7,663.45 | ▲ +36.52 after sell → book $10,044.46; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 391 | $3.11 | $5.12 | $-60.99 | $8,874.34 | ▼ -60.99 after sell → book $10,039.34; vs 09:30 mark -5.12 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `NB` | 250 | $4.66 | $3.28 | $-109.00 | $10,036.07 | ▼ -109.00 after sell → book $10,036.07; vs 09:30 mark -3.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,036.07 | ▲ close $10,036.07 vs 09:30 $10,059.24 (session +0.00) | 16:00 close · cash $10,036.07 · no lots left · equity $10,036.07. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,036.07 | ▲ 09:30 equity $10,036.07 vs yday $10,036.07 (-0.00) | 09:30 open · cash $10,036.07 · no holdings · equity $10,036.07 vs prior close $10,036.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,036.07 | ▲ close $10,036.07 vs 09:30 $10,036.07 (session +0.00) | 16:00 close · cash $10,036.07 · no lots left · equity $10,036.07. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,036.07 | ▲ 09:30 equity $10,036.07 vs yday $10,036.07 (-0.00) | 09:30 open · cash $10,036.07 · no holdings · equity $10,036.07 vs prior close $10,036.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,780.34 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,595.19 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,354.02 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 217 | $5.77 | $2.80 | — | $5,099.13 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,860.26 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,613.68 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 716 | $1.75 | $9.24 | — | $1,351.45 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $193.11 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
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
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,291.52 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 76 | $17.20 | $2.22 | — | $7,982.10 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,682.29 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 117 | $11.13 | $2.34 | — | $5,377.74 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 530 | $2.47 | $6.84 | — | $4,061.80 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 679 | $1.93 | $8.76 | — | $2,742.57 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,486.40 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 993 | $1.32 | $12.81 | — | $162.83 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
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
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 58 | $23.77 | $2.16 | — | $9,668.12 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+13.0; leftover $1381.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 125 | $10.98 | $2.37 | — | $8,293.25 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+1.2; leftover $1381.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.19 | $2.06 | — | $6,945.01 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+7.4; leftover $1381.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 165 | $8.35 | $2.48 | — | $5,564.78 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1381.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 279 | $4.94 | $3.60 | — | $4,182.92 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+7.1; leftover $1381.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $2,900.01 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+6.0; leftover $1381.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 847 | $1.63 | $10.93 | — | $1,508.47 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1381.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 885 | $1.56 | $11.42 | — | $116.46 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1381.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.46 | ▲ close $11,240.84 vs 09:30 $11,048.94 (session +228.91) | 16:00 close · cash $116.46 · equity $11,240.84 vs 09:30 $11,048.94 (+191.90; session marks +228.91) · 8 name(s) marked open→close (per-name table). MOS×58 09:30 $23.77 → close $24.27 +29.00; OCUL×125 09:30 $10.98 → close $10.88 -12.50; INSP×22 09:30 $61.19 → close $61.07 -2.64; CRMD×165 09:30 $8.35 → close $8.56 +34.65; RZLT×279 09:30 $4.94 → close $5.01 +19.53; HCA×3 09:30 $426.97 → close $428.76 +5.37; BMEA×847 09:30 $1.63 → close $1.73 +84.70; CYPH×885 09:30 $1.56 → close $1.64 +70.80 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.46 | ▼ 09:30 equity $11,229.24 vs yday $11,240.84 (-11.60) | 09:30 open · cash $116.46 (unchanged overnight, no fees) · equity $11,229.24 vs prior close $11,240.84 (-11.60) · 8 name(s) re-marked at the open (per-name table). MOS×58 yday $24.27 → 09:30 $24.84 +33.06; OCUL×125 yday $10.88 → 09:30 $10.79 -11.25; INSP×22 yday $61.07 → 09:30 $60.07 -22.00; CRMD×165 yday $8.56 → 09:30 $8.60 +6.60; RZLT×279 yday $5.01 → 09:30 $5.01 +0.00; HCA×3 yday $428.76 → 09:30 $427.50 -3.78; BMEA×847 yday $1.73 → 09:30 $1.75 +21.17; CYPH×885 yday $1.64 → 09:30 $1.60 -35.40 | — |
| 2026-08-26 09:30 ET | **SELL** | `OCUL` | 125 | $10.79 | $2.40 | $-28.51 | $1,462.81 | ▼ -28.51 after sell → book $11,226.85; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `INSP` | 22 | $60.07 | $2.08 | $-28.77 | $2,782.28 | ▼ -28.77 after sell → book $11,224.77; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `CRMD` | 165 | $8.60 | $2.52 | $+36.24 | $4,198.75 | ▲ +36.24 after sell → book $11,222.25; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `RZLT` | 279 | $5.01 | $3.66 | $+12.27 | $5,592.88 | ▲ +12.27 after sell → book $11,218.59; vs 09:30 mark -3.66 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-2.43 | $6,873.37 | ▼ -2.43 after sell → book $11,216.57; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 847 | $1.75 | $11.08 | $+83.87 | $8,348.77 | ▲ +83.87 after sell → book $11,205.49; vs 09:30 mark -11.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 885 | $1.60 | $11.57 | $+12.41 | $9,753.20 | ▲ +12.41 after sell → book $11,193.92; vs 09:30 mark -11.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 141 | $9.83 | $2.41 | — | $8,364.75 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1393.31 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AVEX` | 79 | $17.51 | $2.23 | — | $6,979.24 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $1393.31 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BE` | 6 | $213.94 | $2.01 | — | $5,693.59 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1393.31 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BZ` | 83 | $16.77 | $2.24 | — | $4,299.44 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $1393.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `MAIR` | 50 | $27.59 | $2.14 | — | $2,917.80 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer; ret5=+2.0; leftover $1393.31 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BRR` | 633 | $2.20 | $8.17 | — | $1,517.03 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer; 🔵; ret5=+17.8; leftover $1393.31 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 2389 | $0.58 | $21.09 | — | $103.15 | — | union ∩ join_g, no 🚨; gate join=good; list yday_mover; 🔵; ret5=-27.5; leftover $1393.31 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $103.15 | ▲ close $11,318.31 vs 09:30 $11,229.24 (session +164.68) | 16:00 close · cash $103.15 · equity $11,318.31 vs 09:30 $11,229.24 (+89.07; session marks +164.68) · 8 name(s) marked open→close (per-name table). MOS×58 09:30 $24.84 → close $24.16 -39.44; ABX×141 09:30 $9.83 → close $9.78 -7.05; AVEX×79 09:30 $17.51 → close $18.34 +65.57; BE×6 09:30 $213.94 → close $218.21 +25.62; BZ×83 09:30 $16.77 → close $18.84 +171.81; MAIR×50 09:30 $27.59 → close $28.51 +46.00; BRR×633 09:30 $2.20 → close $2.17 -18.99; SLQT×2389 09:30 $0.58 → close $0.55 -78.84 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $103.15 | ▼ 09:30 equity $11,304.54 vs yday $11,318.31 (-13.77) | 09:30 open · cash $103.15 (unchanged overnight, no fees) · equity $11,304.54 vs prior close $11,318.31 (-13.77) · 8 name(s) re-marked at the open (per-name table). MOS×58 yday $24.16 → 09:30 $24.00 -9.28; ABX×141 yday $9.78 → 09:30 $9.68 -14.10; AVEX×79 yday $18.34 → 09:30 $18.43 +7.11; BE×6 yday $218.21 → 09:30 $227.10 +53.34; BZ×83 yday $18.84 → 09:30 $18.50 -28.22; MAIR×50 yday $28.51 → 09:30 $28.76 +12.50; BRR×633 yday $2.17 → 09:30 $2.19 +12.66; SLQT×2389 yday $0.55 → 09:30 $0.53 -47.78 | — |
| 2026-08-27 09:30 ET | **SELL** | `ABX` | 141 | $9.68 | $2.45 | $-26.01 | $1,465.59 | ▼ -26.01 after sell → book $11,302.10; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVEX` | 79 | $18.43 | $2.25 | $+68.20 | $2,919.30 | ▲ +68.20 after sell → book $11,299.84; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BE` | 6 | $227.10 | $2.03 | $+74.92 | $4,279.87 | ▲ +74.92 after sell → book $11,297.81; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 83 | $18.50 | $2.27 | $+139.09 | $5,813.11 | ▲ +139.09 after sell → book $11,295.55; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MAIR` | 50 | $28.76 | $2.16 | $+54.20 | $7,248.95 | ▲ +54.20 after sell → book $11,293.39; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BRR` | 633 | $2.19 | $8.28 | $-22.78 | $8,626.94 | ▼ -22.78 after sell → book $11,285.11; vs 09:30 mark -8.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 2389 | $0.53 | $20.24 | $-167.95 | $9,872.87 | ▼ -167.95 after sell → book $11,264.87; vs 09:30 mark -20.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 34 | $41.44 | $2.09 | — | $8,461.82 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+3.1; leftover $1410.41 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 97 | $14.42 | $2.28 | — | $7,060.80 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+7.1; leftover $1410.41 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 542 | $2.60 | $6.99 | — | $5,644.61 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+13.0; leftover $1410.41 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 17 | $81.65 | $2.04 | — | $4,254.51 | — | union ∩ join_g, no 🚨; gate join=good; list mover_buy; 🔵; ret5=+2.0; leftover $1410.41 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 308 | $4.57 | $3.97 | — | $2,842.98 | — | union ∩ join_g, no 🚨; gate join=good; list mover_buy; 🔵; ret5=+1.1; leftover $1410.41 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 18 | $74.54 | $2.04 | — | $1,499.22 | — | union ∩ join_g, no 🚨; gate join=good; list mover_buy; 🔵; ret5=-0.1; leftover $1410.41 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $530.21 | — | union ∩ join_g, no 🚨; gate join=good; list mover_buy; 🔵; ret5=+0.1; leftover $1410.41 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $530.21 | ▲ close $11,267.73 vs 09:30 $11,304.54 (session +24.28) | 16:00 close · cash $530.21 · equity $11,267.73 vs 09:30 $11,304.54 (-36.81; session marks +24.28) · 8 name(s) marked open→close (per-name table). MOS×58 09:30 $24.00 → close $23.76 -13.92; RRC×34 09:30 $41.44 → close $41.64 +6.80; CRK×97 09:30 $14.42 → close $14.62 +19.40; SLI×542 09:30 $2.60 → close $2.64 +21.68; ACMR×17 09:30 $81.65 → close $80.49 -19.72; GGB×308 09:30 $4.57 → close $4.70 +40.04; MT×18 09:30 $74.54 → close $74.63 +1.62; MU×1 09:30 $967.01 → close $935.39 -31.62 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $530.21 | ▲ 09:30 equity $11,272.40 vs yday $11,267.73 (+4.67) | 09:30 open · cash $530.21 (unchanged overnight, no fees) · equity $11,272.40 vs prior close $11,267.73 (+4.67) · 8 name(s) re-marked at the open (per-name table). MOS×58 yday $23.76 → 09:30 $23.95 +11.02; RRC×34 yday $41.64 → 09:30 $41.74 +3.40; CRK×97 yday $14.62 → 09:30 $14.63 +0.97; SLI×542 yday $2.64 → 09:30 $2.68 +21.68; ACMR×17 yday $80.49 → 09:30 $79.27 -20.74; GGB×308 yday $4.70 → 09:30 $4.67 -9.24; MT×18 yday $74.63 → 09:30 $75.39 +13.68; MU×1 yday $935.39 → 09:30 $919.29 -16.10 | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 58 | $23.95 | $2.19 | $+6.09 | $1,917.13 | ▲ +6.09 after sell → book $11,270.22; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 34 | $41.74 | $2.11 | $+5.99 | $3,334.18 | ▲ +5.99 after sell → book $11,268.11; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 97 | $14.63 | $2.31 | $+15.78 | $4,750.98 | ▲ +15.78 after sell → book $11,265.80; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 542 | $2.68 | $7.09 | $+29.27 | $6,196.44 | ▲ +29.27 after sell → book $11,258.70; vs 09:30 mark -7.10 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 17 | $79.27 | $2.06 | $-44.56 | $7,541.97 | ▼ -44.56 after sell → book $11,256.64; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 308 | $4.67 | $4.04 | $+22.79 | $8,976.30 | ▲ +22.79 after sell → book $11,252.61; vs 09:30 mark -4.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 18 | $75.39 | $2.06 | $+11.19 | $10,331.25 | ▲ +11.19 after sell → book $11,250.54; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $11,248.53 | ▼ -51.73 after sell → book $11,248.53; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 42 | $32.90 | $2.12 | — | $9,864.61 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1406.07 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 89 | $15.66 | $2.26 | — | $8,468.62 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1406.07 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 17 | $79.42 | $2.04 | — | $7,116.43 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1406.07 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $5,853.23 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1406.07 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $4,536.58 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1406.07 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 77 | $18.15 | $2.22 | — | $3,136.81 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer,yday_mover; ret5=+14.1; leftover $1406.07 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $1,858.95 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1406.07 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 154 | $9.13 | $2.45 | — | $450.48 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer; 🔵; ret5=+20.0; leftover $1406.07 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $450.48 | ▼ close $10,898.80 vs 09:30 $11,272.40 (session -332.60) | 16:00 close · cash $450.48 · equity $10,898.80 vs 09:30 $11,272.40 (-373.60; session marks -332.60) · 8 name(s) marked open→close (per-name table). SEDG×42 09:30 $32.90 → close $31.41 -62.58; GRRR×89 09:30 $15.66 → close $14.41 -111.25; URBN×17 09:30 $79.42 → close $81.09 +28.39; SIMO×5 09:30 $252.24 → close $245.81 -32.15; ANF×9 09:30 $146.07 → close $148.42 +21.15; BZ×77 09:30 $18.15 → close $17.80 -26.95; SMTC×9 09:30 $141.76 → close $131.17 -95.31; VYX×154 09:30 $9.13 → close $8.78 -53.90 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $450.48 | ▼ 09:30 equity $10,866.18 vs yday $10,898.80 (-32.62) | 09:30 open · cash $450.48 (unchanged overnight, no fees) · equity $10,866.18 vs prior close $10,898.80 (-32.62) · 8 name(s) re-marked at the open (per-name table). SEDG×42 yday $31.41 → 09:30 $31.15 -10.92; GRRR×89 yday $14.41 → 09:30 $14.44 +2.67; URBN×17 yday $81.09 → 09:30 $80.44 -11.05; SIMO×5 yday $245.81 → 09:30 $247.05 +6.20; ANF×9 yday $148.42 → 09:30 $148.03 -3.51; BZ×77 yday $17.80 → 09:30 $17.70 -7.70; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; VYX×154 yday $8.78 → 09:30 $8.66 -18.48 | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 42 | $31.15 | $2.14 | $-77.75 | $1,756.65 | ▼ -77.75 after sell → book $10,864.05; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 89 | $14.44 | $2.28 | $-113.12 | $3,039.52 | ▼ -113.12 after sell → book $10,861.76; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 17 | $80.44 | $2.06 | $+13.24 | $4,404.94 | ▲ +13.24 after sell → book $10,859.70; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $5,638.17 | ▼ -29.98 after sell → book $10,857.68; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $6,968.40 | ▲ +13.59 after sell → book $10,855.64; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 77 | $17.70 | $2.24 | $-39.12 | $8,329.05 | ▼ -39.12 after sell → book $10,853.39; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $9,517.72 | ▼ -89.19 after sell → book $10,851.36; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 154 | $8.66 | $2.49 | $-77.32 | $10,848.87 | ▼ -77.32 after sell → book $10,848.87; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,848.87 | ▲ close $10,848.87 vs 09:30 $10,866.18 (session +0.00) | 16:00 close · cash $10,848.87 · no lots left · equity $10,848.87. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,848.87 | ▲ 09:30 equity $10,848.87 vs yday $10,848.87 (-0.00) | 09:30 open · cash $10,848.87 · no holdings · equity $10,848.87 vs prior close $10,848.87 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,848.87 | ▲ close $10,848.87 vs 09:30 $10,848.87 (session +0.00) | 16:00 close · cash $10,848.87 · no lots left · equity $10,848.87. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,848.87 | ▲ 09:30 equity $10,848.87 vs yday $10,848.87 (-0.00) | 09:30 open · cash $10,848.87 · no holdings · equity $10,848.87 vs prior close $10,848.87 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,848.87 | ▲ close $10,848.87 vs 09:30 $10,848.87 (session +0.00) | 16:00 close · cash $10,848.87 · no lots left · equity $10,848.87. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,848.87 | ▲ 09:30 equity $10,848.87 vs yday $10,848.87 (-0.00) | 09:30 open · cash $10,848.87 · no holdings · equity $10,848.87 vs prior close $10,848.87 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,524.80 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1356.11 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $42.93 | $2.08 | — | $8,191.89 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1356.11 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 373 | $3.63 | $4.81 | — | $6,833.09 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1356.11 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 168 | $8.03 | $2.49 | — | $5,481.56 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1356.11 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,155.04 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1356.11 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 87 | $15.45 | $2.25 | — | $2,808.63 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1356.11 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,493.11 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1356.11 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 80 | $16.77 | $2.23 | — | $149.28 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1356.11 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $149.28 | ▼ close $10,588.70 vs 09:30 $10,848.87 (session -240.19) | 16:00 close · cash $149.28 · equity $10,588.70 vs 09:30 $10,848.87 (-260.17; session marks -240.19) · 8 name(s) marked open→close (per-name table). ATRC×25 09:30 $52.88 → close $52.46 -10.50; HRMY×31 09:30 $42.93 → close $41.86 -33.17; CABA×373 09:30 $3.63 → close $3.48 -55.95; VSTM×168 09:30 $8.03 → close $7.98 -8.40; RVTY×10 09:30 $132.45 → close $130.63 -18.20; CRK×87 09:30 $15.45 → close $14.95 -43.50; MRNA×9 09:30 $145.94 → close $148.87 +26.33; ARCT×80 09:30 $16.77 → close $15.56 -96.80 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.28 | ▲ 09:30 equity $10,592.67 vs yday $10,588.70 (+3.97) | 09:30 open · cash $149.28 (unchanged overnight, no fees) · equity $10,592.67 vs prior close $10,588.70 (+3.97) · 8 name(s) re-marked at the open (per-name table). ATRC×25 yday $52.46 → 09:30 $52.03 -10.75; HRMY×31 yday $41.86 → 09:30 $41.50 -11.16; CABA×373 yday $3.48 → 09:30 $3.46 -7.46; VSTM×168 yday $7.98 → 09:30 $7.91 -11.76; RVTY×10 yday $130.63 → 09:30 $130.03 -6.00; CRK×87 yday $14.95 → 09:30 $15.00 +4.35; MRNA×9 yday $148.87 → 09:30 $153.62 +42.75; ARCT×80 yday $15.56 → 09:30 $15.61 +4.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 31 | $41.50 | $2.10 | $-48.52 | $1,433.68 | ▼ -48.52 after sell → book $10,590.57; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 168 | $7.91 | $2.53 | $-25.19 | $2,760.03 | ▼ -25.19 after sell → book $10,588.04; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $4,058.29 | ▼ -28.26 after sell → book $10,586.00; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 87 | $15.00 | $2.28 | $-43.68 | $5,361.01 | ▼ -43.68 after sell → book $10,583.72; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 9 | $153.62 | $2.04 | $+65.02 | $6,741.55 | ▲ +65.02 after sell → book $10,581.68; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 80 | $15.61 | $2.25 | $-97.28 | $7,988.10 | ▼ -97.28 after sell → book $10,579.43; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 528 | $2.52 | $6.81 | — | $6,650.73 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1331.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 198 | $6.71 | $2.58 | — | $5,319.56 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1331.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 700 | $1.90 | $9.03 | — | $3,980.53 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1331.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 278 | $4.78 | $3.59 | — | $2,648.11 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1331.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 837 | $1.59 | $10.80 | — | $1,306.48 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1331.35 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 115 | $11.31 | $2.33 | — | $3.50 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1331.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.50 | ▼ close $10,490.26 vs 09:30 $10,592.67 (session -54.03) | 16:00 close · cash $3.50 · equity $10,490.26 vs 09:30 $10,592.67 (-102.41; session marks -54.03) · 8 name(s) marked open→close (per-name table). ATRC×25 09:30 $52.03 → close $51.52 -12.75; CABA×373 09:30 $3.46 → close $3.47 +3.73; ALEC×528 09:30 $2.52 → close $2.46 -31.68; BHC×198 09:30 $6.71 → close $6.56 -29.70; BMEA×700 09:30 $1.90 → close $2.03 +91.00; OABI×278 09:30 $4.78 → close $4.33 -125.10; OPK×837 09:30 $1.59 → close $1.64 +41.85; VIR×115 09:30 $11.31 → close $11.38 +8.62 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.50 | ▼ 09:30 equity $10,448.15 vs yday $10,490.26 (-42.11) | 09:30 open · cash $3.50 (unchanged overnight, no fees) · equity $10,448.15 vs prior close $10,490.26 (-42.11) · 8 name(s) re-marked at the open (per-name table). ATRC×25 yday $51.52 → 09:30 $54.31 +69.75; CABA×373 yday $3.47 → 09:30 $3.43 -14.92; ALEC×528 yday $2.46 → 09:30 $2.38 -42.24; BHC×198 yday $6.56 → 09:30 $6.57 +1.98; BMEA×700 yday $2.03 → 09:30 $2.00 -21.00; OABI×278 yday $4.33 → 09:30 $4.30 -8.34; OPK×837 yday $1.64 → 09:30 $1.63 -8.37; VIR×115 yday $11.38 → 09:30 $11.22 -18.97 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 25 | $54.31 | $2.09 | $+31.60 | $1,359.16 | ▲ +31.60 after sell → book $10,446.06; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 373 | $3.43 | $4.88 | $-84.30 | $2,633.67 | ▼ -84.30 after sell → book $10,441.18; vs 09:30 mark -4.88 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 528 | $2.38 | $6.91 | $-87.64 | $3,883.40 | ▼ -87.64 after sell → book $10,434.27; vs 09:30 mark -6.91 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 198 | $6.57 | $2.63 | $-32.93 | $5,181.63 | ▼ -32.93 after sell → book $10,431.64; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 700 | $2.00 | $9.16 | $+51.81 | $6,572.47 | ▲ +51.81 after sell → book $10,422.48; vs 09:30 mark -9.16 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 278 | $4.30 | $3.64 | $-140.67 | $7,764.23 | ▼ -140.67 after sell → book $10,418.84; vs 09:30 mark -3.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 837 | $1.63 | $10.95 | $+11.74 | $9,117.59 | ▲ +11.74 after sell → book $10,407.89; vs 09:30 mark -10.95 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 115 | $11.22 | $2.36 | $-15.05 | $10,405.53 | ▼ -15.05 after sell → book $10,405.53; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,405.53 | ▲ close $10,405.53 vs 09:30 $10,448.15 (session +0.00) | 16:00 close · cash $10,405.53 · no lots left · equity $10,405.53. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,405.53 | ▲ 09:30 equity $10,405.53 vs yday $10,405.53 (-0.00) | 09:30 open · cash $10,405.53 · no holdings · equity $10,405.53 vs prior close $10,405.53 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,405.53 | ▲ close $10,405.53 vs 09:30 $10,405.53 (session +0.00) | 16:00 close · cash $10,405.53 · no lots left · equity $10,405.53. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CNTB` | hard_red | hard-red S=-13.95 sit; no new buys |
