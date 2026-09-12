# Factor mine action — `union_blue_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ blue, no 🚨

Cash book **+0.54%** ($10,054) · signal-only (no cash/fees) was -0.65%. Starts YES **6/21**. Fills 158 · skips 58 · realized $+184.80.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the name is painted 🔵 (a turn higher on a still-red row).
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
- **Gate** `blue=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $144.04.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `TLN` | 3 | — | $359.83 | +0.00 | $362.74 | +8.73 | +8.73 | +0.00 | +8.73 |
| 2026-08-14 | `VST` | 8 | — | $146.90 | +0.00 | $148.13 | +9.84 | +9.84 | +0.00 | +9.84 |
| 2026-08-14 | `NRG` | 10 | — | $120.00 | +0.00 | $126.24 | +62.40 | +62.40 | +0.00 | +62.40 |
| 2026-08-14 | `DAVE` | 3 | — | $330.91 | +0.00 | $334.57 | +10.98 | +10.98 | +0.00 | +10.98 |
| 2026-08-14 | `SLG` | 21 | — | $57.61 | +0.00 | $56.09 | -31.92 | -31.92 | +0.00 | -31.92 |
| 2026-08-14 | `MARA` | 138 | — | $9.01 | +0.00 | $9.20 | +26.22 | +26.22 | +0.00 | +26.22 |
| 2026-08-14 | `LDI` | 1334 | — | $0.94 | +0.00 | $0.90 | -53.36 | -53.36 | +0.00 | -53.36 |
| 2026-08-14 | `BTBT` | 833 | — | $1.50 | +0.00 | $1.57 | +58.31 | +58.31 | +0.00 | +58.31 |
| 2026-08-17 | `TLN` | 3 | $362.74 | $367.88 | +15.42 | — | +0.00 | +15.42 | +24.15 | — |
| 2026-08-17 | `VST` | 8 | $148.13 | $149.37 | +9.92 | — | +0.00 | +9.92 | +19.76 | — |
| 2026-08-17 | `NRG` | 10 | $126.24 | $127.40 | +11.60 | — | +0.00 | +11.60 | +74.00 | — |
| 2026-08-17 | `DAVE` | 3 | $334.57 | $336.94 | +7.11 | — | +0.00 | +7.11 | +18.09 | — |
| 2026-08-17 | `SLG` | 21 | $56.09 | $55.37 | -15.12 | — | +0.00 | -15.12 | -47.04 | — |
| 2026-08-17 | `MARA` | 138 | $9.20 | $9.22 | +2.76 | — | +0.00 | +2.76 | +28.98 | — |
| 2026-08-17 | `LDI` | 1334 | $0.90 | $0.91 | +13.34 | — | +0.00 | +13.34 | -40.02 | — |
| 2026-08-17 | `BTBT` | 833 | $1.57 | $1.52 | -41.65 | — | +0.00 | -41.65 | +16.66 | — |
| 2026-08-17 | `DVN` | 27 | — | $46.18 | +0.00 | $47.57 | +37.53 | +37.53 | +0.00 | +37.53 |
| 2026-08-17 | `EOG` | 8 | — | $142.77 | +0.00 | $146.15 | +27.04 | +27.04 | +0.00 | +27.04 |
| 2026-08-17 | `FANG` | 6 | — | $202.70 | +0.00 | $206.29 | +21.54 | +21.54 | +0.00 | +21.54 |
| 2026-08-17 | `TMC` | 309 | — | $4.05 | +0.00 | $3.77 | -86.52 | -86.52 | +0.00 | -86.52 |
| 2026-08-17 | `TGB` | 147 | — | $8.46 | +0.00 | $8.77 | +45.57 | +45.57 | +0.00 | +45.57 |
| 2026-08-17 | `ABX` | 137 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `ALM` | 77 | — | $16.20 | +0.00 | $16.36 | +12.32 | +12.32 | +0.00 | +12.32 |
| 2026-08-17 | `ALOY` | 85 | — | $14.66 | +0.00 | $13.86 | -68.42 | -68.42 | +0.00 | -68.42 |
| 2026-08-18 | `DVN` | 27 | $47.57 | $48.00 | +11.61 | — | +0.00 | +11.61 | +49.14 | — |
| 2026-08-18 | `EOG` | 8 | $146.15 | $148.04 | +15.12 | — | +0.00 | +15.12 | +42.16 | — |
| 2026-08-18 | `FANG` | 6 | $206.29 | $208.93 | +15.84 | — | +0.00 | +15.84 | +37.38 | — |
| 2026-08-18 | `TMC` | 309 | $3.77 | $3.72 | -15.45 | — | +0.00 | -15.45 | -101.97 | — |
| 2026-08-18 | `TGB` | 147 | $8.77 | $8.55 | -32.34 | — | +0.00 | -32.34 | +13.23 | — |
| 2026-08-18 | `ABX` | 137 | $9.12 | $9.03 | -12.33 | — | +0.00 | -12.33 | -12.33 | — |
| 2026-08-18 | `ALM` | 77 | $16.36 | $15.78 | -44.66 | — | +0.00 | -44.66 | -32.34 | — |
| 2026-08-18 | `ALOY` | 85 | $13.86 | $13.19 | -56.53 | — | +0.00 | -56.53 | -124.95 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 59 | — | $20.55 | +0.00 | $21.19 | +37.76 | +37.76 | +0.00 | +37.76 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 59 | — | $20.65 | +0.00 | $21.11 | +27.14 | +27.14 | +0.00 | +27.14 |
| 2026-08-20 | `HDSN` | 213 | — | $5.77 | +0.00 | $5.57 | -42.60 | -42.60 | +0.00 | -42.60 |
| 2026-08-20 | `IAG` | 62 | — | $19.63 | +0.00 | $20.50 | +53.94 | +53.94 | +0.00 | +53.94 |
| 2026-08-20 | `KGC` | 41 | — | $29.63 | +0.00 | $31.43 | +73.80 | +73.80 | +0.00 | +73.80 |
| 2026-08-20 | `NFGC` | 703 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 59 | $21.19 | $21.90 | +41.89 | — | +0.00 | +41.89 | +79.65 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `CDE` | 59 | $21.11 | $21.75 | +37.76 | — | +0.00 | +37.76 | +64.90 | — |
| 2026-08-21 | `HDSN` | 213 | $5.57 | $5.67 | +21.30 | — | +0.00 | +21.30 | -21.30 | — |
| 2026-08-21 | `IAG` | 62 | $20.50 | $21.17 | +41.54 | — | +0.00 | +41.54 | +95.48 | — |
| 2026-08-21 | `KGC` | 41 | $31.43 | $32.17 | +30.34 | — | +0.00 | +30.34 | +104.14 | — |
| 2026-08-21 | `NFGC` | 703 | $1.75 | $1.79 | +28.12 | — | +0.00 | +28.12 | +28.12 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 74 | — | $17.20 | +0.00 | $16.65 | -40.70 | -40.70 | +0.00 | -40.70 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 115 | — | $11.13 | +0.00 | $13.45 | +266.80 | +266.80 | +0.00 | +266.80 |
| 2026-08-21 | `AUTL` | 520 | — | $2.47 | +0.00 | $2.41 | -31.20 | -31.20 | +0.00 | -31.20 |
| 2026-08-21 | `CRDL` | 666 | — | $1.93 | +0.00 | $1.86 | -46.62 | -46.62 | +0.00 | -46.62 |
| 2026-08-21 | `CRSP` | 21 | — | $59.72 | +0.00 | $59.50 | -4.62 | -4.62 | +0.00 | -4.62 |
| 2026-08-21 | `CYPH` | 974 | — | $1.32 | +0.00 | $1.42 | +97.40 | +97.40 | +0.00 | +97.40 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUPH` | 74 | $16.65 | $16.57 | -5.92 | — | +0.00 | -5.92 | -46.62 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 115 | $13.45 | $13.33 | -13.80 | — | +0.00 | -13.80 | +253.00 | — |
| 2026-08-24 | `AUTL` | 520 | $2.41 | $2.40 | -5.20 | — | +0.00 | -5.20 | -36.40 | — |
| 2026-08-24 | `CRDL` | 666 | $1.86 | $1.88 | +13.32 | — | +0.00 | +13.32 | -33.30 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | — | +0.00 | -15.75 | -20.37 | — |
| 2026-08-24 | `CYPH` | 974 | $1.42 | $1.83 | +399.34 | — | +0.00 | +399.34 | +496.74 | — |
| 2026-08-25 | `OCUL` | 123 | — | $10.98 | +0.00 | $10.88 | -12.30 | -12.30 | +0.00 | -12.30 |
| 2026-08-25 | `INSP` | 22 | — | $61.19 | +0.00 | $61.07 | -2.64 | -2.64 | +0.00 | -2.64 |
| 2026-08-25 | `CRMD` | 162 | — | $8.35 | +0.00 | $8.56 | +34.02 | +34.02 | +0.00 | +34.02 |
| 2026-08-25 | `CAPR` | 186 | — | $7.25 | +0.00 | $8.29 | +193.44 | +193.44 | +0.00 | +193.44 |
| 2026-08-25 | `KURA` | 99 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CCOI` | 142 | — | $9.49 | +0.00 | $9.88 | +55.38 | +55.38 | +0.00 | +55.38 |
| 2026-08-25 | `LIFE` | 36 | — | $36.96 | +0.00 | $38.56 | +57.60 | +57.60 | +0.00 | +57.60 |
| 2026-08-25 | `ZIP` | 297 | — | $4.55 | +0.00 | $4.35 | -59.40 | -59.40 | +0.00 | -59.40 |
| 2026-08-26 | `OCUL` | 123 | $10.88 | $10.79 | -11.07 | $10.77 | -2.46 | -13.53 | -23.37 | -25.83 |
| 2026-08-26 | `INSP` | 22 | $61.07 | $60.07 | -22.00 | — | +0.00 | -22.00 | -24.64 | — |
| 2026-08-26 | `CRMD` | 162 | $8.56 | $8.60 | +6.48 | $8.39 | -34.02 | -27.54 | +40.50 | +6.48 |
| 2026-08-26 | `CAPR` | 186 | $8.29 | $8.29 | +0.00 | — | +0.00 | +0.00 | +193.44 | — |
| 2026-08-26 | `KURA` | 99 | $13.59 | $13.63 | +3.96 | — | +0.00 | +3.96 | +3.96 | — |
| 2026-08-26 | `CCOI` | 142 | $9.88 | $9.89 | +1.42 | — | +0.00 | +1.42 | +56.80 | — |
| 2026-08-26 | `LIFE` | 36 | $38.56 | $38.24 | -11.52 | — | +0.00 | -11.52 | +46.08 | — |
| 2026-08-26 | `ZIP` | 297 | $4.35 | $4.31 | -11.88 | — | +0.00 | -11.88 | -71.28 | — |
| 2026-08-26 | `RZLT` | 276 | — | $5.01 | +0.00 | $5.04 | +8.28 | +8.28 | +0.00 | +8.28 |
| 2026-08-26 | `AVBP` | 44 | — | $31.21 | +0.00 | $31.14 | -3.08 | -3.08 | +0.00 | -3.08 |
| 2026-08-26 | `FLNC` | 124 | — | $11.12 | +0.00 | $11.08 | -4.96 | -4.96 | +0.00 | -4.96 |
| 2026-08-26 | `ABX` | 140 | — | $9.83 | +0.00 | $9.78 | -7.00 | -7.00 | +0.00 | -7.00 |
| 2026-08-26 | `AVEX` | 79 | — | $17.51 | +0.00 | $18.34 | +65.57 | +65.57 | +0.00 | +65.57 |
| 2026-08-26 | `BE` | 6 | — | $213.94 | +0.00 | $218.21 | +25.62 | +25.62 | +0.00 | +25.62 |
| 2026-08-27 | `OCUL` | 123 | $10.77 | $10.63 | -17.22 | — | +0.00 | -17.22 | -43.05 | — |
| 2026-08-27 | `CRMD` | 162 | $8.39 | $8.49 | +16.20 | — | +0.00 | +16.20 | +22.68 | — |
| 2026-08-27 | `RZLT` | 276 | $5.04 | $5.07 | +8.28 | — | +0.00 | +8.28 | +16.56 | — |
| 2026-08-27 | `AVBP` | 44 | $31.14 | $30.79 | -15.40 | — | +0.00 | -15.40 | -18.48 | — |
| 2026-08-27 | `FLNC` | 124 | $11.08 | $11.52 | +54.56 | — | +0.00 | +54.56 | +49.60 | — |
| 2026-08-27 | `ABX` | 140 | $9.78 | $9.68 | -14.00 | — | +0.00 | -14.00 | -21.00 | — |
| 2026-08-27 | `AVEX` | 79 | $18.34 | $18.43 | +7.11 | — | +0.00 | +7.11 | +72.68 | — |
| 2026-08-27 | `BE` | 6 | $218.21 | $227.10 | +53.34 | — | +0.00 | +53.34 | +78.96 | — |
| 2026-08-27 | `ACMR` | 17 | — | $81.65 | +0.00 | $80.49 | -19.72 | -19.72 | +0.00 | -19.72 |
| 2026-08-27 | `GGB` | 304 | — | $4.57 | +0.00 | $4.70 | +39.52 | +39.52 | +0.00 | +39.52 |
| 2026-08-27 | `MT` | 18 | — | $74.54 | +0.00 | $74.63 | +1.62 | +1.62 | +0.00 | +1.62 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `TX` | 25 | — | $55.25 | +0.00 | $55.83 | +14.50 | +14.50 | +0.00 | +14.50 |
| 2026-08-27 | `ANET` | 6 | — | $205.90 | +0.00 | $201.09 | -28.86 | -28.86 | +0.00 | -28.86 |
| 2026-08-27 | `DLO` | 90 | — | $15.33 | +0.00 | $15.14 | -17.10 | -17.10 | +0.00 | -17.10 |
| 2026-08-28 | `ACMR` | 17 | $80.49 | $79.27 | -20.74 | — | +0.00 | -20.74 | -40.46 | — |
| 2026-08-28 | `GGB` | 304 | $4.70 | $4.67 | -9.12 | — | +0.00 | -9.12 | +30.40 | — |
| 2026-08-28 | `MT` | 18 | $74.63 | $75.39 | +13.68 | — | +0.00 | +13.68 | +15.30 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `TX` | 25 | $55.83 | $55.97 | +3.50 | — | +0.00 | +3.50 | +18.00 | — |
| 2026-08-28 | `ANET` | 6 | $201.09 | $200.00 | -6.54 | — | +0.00 | -6.54 | -35.40 | — |
| 2026-08-28 | `DLO` | 90 | $15.14 | $15.19 | +4.50 | — | +0.00 | +4.50 | -12.60 | — |
| 2026-08-28 | `SEDG` | 41 | — | $32.90 | +0.00 | $31.41 | -61.09 | -61.09 | +0.00 | -61.09 |
| 2026-08-28 | `GRRR` | 88 | — | $15.66 | +0.00 | $14.41 | -110.00 | -110.00 | +0.00 | -110.00 |
| 2026-08-28 | `URBN` | 17 | — | $79.42 | +0.00 | $81.09 | +28.39 | +28.39 | +0.00 | +28.39 |
| 2026-08-28 | `ANF` | 9 | — | $146.07 | +0.00 | $148.42 | +21.15 | +21.15 | +0.00 | +21.15 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `VYX` | 150 | — | $9.13 | +0.00 | $8.78 | -52.50 | -52.50 | +0.00 | -52.50 |
| 2026-08-28 | `TTMI` | 11 | — | $122.81 | +0.00 | $118.65 | -45.76 | -45.76 | +0.00 | -45.76 |
| 2026-08-28 | `NVRI` | 60 | — | $22.66 | +0.00 | $22.28 | -22.80 | -22.80 | +0.00 | -22.80 |
| 2026-08-31 | `SEDG` | 41 | $31.41 | $31.15 | -10.66 | — | +0.00 | -10.66 | -71.75 | — |
| 2026-08-31 | `GRRR` | 88 | $14.41 | $14.44 | +2.64 | — | +0.00 | +2.64 | -107.36 | — |
| 2026-08-31 | `URBN` | 17 | $81.09 | $80.44 | -11.05 | — | +0.00 | -11.05 | +17.34 | — |
| 2026-08-31 | `ANF` | 9 | $148.42 | $148.03 | -3.51 | — | +0.00 | -3.51 | +17.64 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `VYX` | 150 | $8.78 | $8.66 | -18.00 | — | +0.00 | -18.00 | -70.50 | — |
| 2026-08-31 | `TTMI` | 11 | $118.65 | $118.83 | +1.98 | — | +0.00 | +1.98 | -43.78 | — |
| 2026-08-31 | `NVRI` | 60 | $22.28 | $22.12 | -9.60 | — | +0.00 | -9.60 | -32.40 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 25 | — | $52.88 | +0.00 | $52.46 | -10.50 | -10.50 | +0.00 | -10.50 |
| 2026-09-03 | `HRMY` | 30 | — | $42.93 | +0.00 | $41.86 | -32.10 | -32.10 | +0.00 | -32.10 |
| 2026-09-03 | `CABA` | 365 | — | $3.63 | +0.00 | $3.48 | -54.75 | -54.75 | +0.00 | -54.75 |
| 2026-09-03 | `VSTM` | 165 | — | $8.03 | +0.00 | $7.98 | -8.25 | -8.25 | +0.00 | -8.25 |
| 2026-09-03 | `RVTY` | 10 | — | $132.45 | +0.00 | $130.63 | -18.20 | -18.20 | +0.00 | -18.20 |
| 2026-09-03 | `CRK` | 85 | — | $15.45 | +0.00 | $14.95 | -42.50 | -42.50 | +0.00 | -42.50 |
| 2026-09-03 | `MRNA` | 9 | — | $145.94 | +0.00 | $148.87 | +26.33 | +26.33 | +0.00 | +26.33 |
| 2026-09-03 | `ARCT` | 79 | — | $16.77 | +0.00 | $15.56 | -95.59 | -95.59 | +0.00 | -95.59 |
| 2026-09-04 | `ATRC` | 25 | $52.46 | $52.03 | -10.75 | $51.52 | -12.75 | -23.50 | -21.25 | -34.00 |
| 2026-09-04 | `HRMY` | 30 | $41.86 | $41.50 | -10.80 | — | +0.00 | -10.80 | -42.90 | — |
| 2026-09-04 | `CABA` | 365 | $3.48 | $3.46 | -7.30 | $3.47 | +3.65 | -3.65 | -62.05 | -58.40 |
| 2026-09-04 | `VSTM` | 165 | $7.98 | $7.91 | -11.55 | — | +0.00 | -11.55 | -19.80 | — |
| 2026-09-04 | `RVTY` | 10 | $130.63 | $130.03 | -6.00 | — | +0.00 | -6.00 | -24.20 | — |
| 2026-09-04 | `CRK` | 85 | $14.95 | $15.00 | +4.25 | — | +0.00 | +4.25 | -38.25 | — |
| 2026-09-04 | `MRNA` | 9 | $148.87 | $153.62 | +42.75 | — | +0.00 | +42.75 | +69.08 | — |
| 2026-09-04 | `ARCT` | 79 | $15.56 | $15.61 | +3.95 | — | +0.00 | +3.95 | -91.64 | — |
| 2026-09-04 | `ALEC` | 515 | — | $2.52 | +0.00 | $2.46 | -30.90 | -30.90 | +0.00 | -30.90 |
| 2026-09-04 | `BHC` | 193 | — | $6.71 | +0.00 | $6.56 | -28.95 | -28.95 | +0.00 | -28.95 |
| 2026-09-04 | `BMEA` | 683 | — | $1.90 | +0.00 | $2.03 | +88.79 | +88.79 | +0.00 | +88.79 |
| 2026-09-04 | `OABI` | 271 | — | $4.78 | +0.00 | $4.33 | -121.95 | -121.95 | +0.00 | -121.95 |
| 2026-09-04 | `OPK` | 816 | — | $1.59 | +0.00 | $1.64 | +40.80 | +40.80 | +0.00 | +40.80 |
| 2026-09-04 | `VIR` | 112 | — | $11.31 | +0.00 | $11.38 | +8.40 | +8.40 | +0.00 | +8.40 |
| 2026-09-08 | `ATRC` | 25 | $51.52 | $54.31 | +69.75 | — | +0.00 | +69.75 | +35.75 | — |
| 2026-09-08 | `CABA` | 365 | $3.47 | $3.43 | -14.60 | — | +0.00 | -14.60 | -73.00 | — |
| 2026-09-08 | `ALEC` | 515 | $2.46 | $2.38 | -41.20 | — | +0.00 | -41.20 | -72.10 | — |
| 2026-09-08 | `BHC` | 193 | $6.56 | $6.57 | +1.93 | — | +0.00 | +1.93 | -27.02 | — |
| 2026-09-08 | `BMEA` | 683 | $2.03 | $2.00 | -20.49 | — | +0.00 | -20.49 | +68.30 | — |
| 2026-09-08 | `OABI` | 271 | $4.33 | $4.30 | -8.13 | — | +0.00 | -8.13 | -130.08 | — |
| 2026-09-08 | `OPK` | 816 | $1.64 | $1.63 | -8.16 | — | +0.00 | -8.16 | +32.64 | — |
| 2026-09-08 | `VIR` | 112 | $11.38 | $11.22 | -18.48 | — | +0.00 | -18.48 | -10.08 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `AUPH` | 78 | — | $16.28 | +0.00 | $16.10 | -14.04 | -14.04 | +0.00 | -14.04 |
| 2026-09-11 | `OVID` | 466 | — | $2.73 | +0.00 | $2.69 | -18.64 | -18.64 | +0.00 | -18.64 |
| 2026-09-11 | `ORCL` | 7 | — | $164.43 | +0.00 | $150.28 | -99.05 | -99.05 | +0.00 | -99.05 |
| 2026-09-11 | `NVT` | 8 | — | $157.78 | +0.00 | $162.38 | +36.80 | +36.80 | +0.00 | +36.80 |
| 2026-09-11 | `COHU` | 22 | — | $56.09 | +0.00 | $57.08 | +21.78 | +21.78 | +0.00 | +21.78 |
| 2026-09-11 | `AMTX` | 624 | — | $2.04 | +0.00 | $2.01 | -18.72 | -18.72 | +0.00 | -18.72 |
| 2026-09-11 | `CLOV` | 268 | — | $4.75 | +0.00 | $4.82 | +18.76 | +18.76 | +0.00 | +18.76 |
| 2026-09-11 | `BAK` | 600 | — | $2.12 | +0.00 | $2.08 | -24.00 | -24.00 | +0.00 | -24.00 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +91.20 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | — | $560.20 | $10,051.46 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 |
| 2026-08-17 | +2.25 | $560.20 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | $10,054.84 | +3.38 | -10.94 | DVN, EOG, FANG, TMC, TGB, ABX, ALM, ALOY | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $152.39 | $9,984.67 | DVN×27, EOG×8, FANG×6, TMC×309, TGB×147, ABX×137, ALM×77, ALOY×85 |
| 2026-08-18 | -6.20 | $152.39 | DVN×27, EOG×8, FANG×6, TMC×309, TGB×147, ABX×137, ALM×77, ALOY×85 | $9,865.94 | -118.73 | +0.00 | — | DVN, EOG, FANG, TMC, TGB, ABX, ALM, ALOY | $9,846.32 | $9,846.32 | — |
| 2026-08-19 | -7.20 | $9,846.32 | — | $9,846.32 | +0.00 | +0.00 | — | — | $9,846.32 | $9,846.32 | — |
| 2026-08-20 | +1.12 | $9,846.32 | — | $9,846.32 | +0.00 | +229.78 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $160.44 | $10,051.62 | AG×59, BHP×13, CDE×59, HDSN×213, IAG×62, KGC×41, NFGC×703, WPM×8 |
| 2026-08-21 | +3.25 | $160.44 | AG×59, BHP×13, CDE×59, HDSN×213, IAG×62, KGC×41, NFGC×703, WPM×8 | $10,315.34 | +263.72 | +257.76 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $313.95 | $10,509.84 | AU×10, AUPH×74, AEM×5, ARCT×115, AUTL×520, CRDL×666, CRSP×21, CYPH×974 |
| 2026-08-24 | -5.17 | $313.95 | AU×10, AUPH×74, AEM×5, ARCT×115, AUTL×520, CRDL×666, CRSP×21, CYPH×974 | $10,879.58 | +369.74 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,840.58 | $10,840.58 | — |
| 2026-08-25 | +1.80 | $10,840.58 | — | $10,840.58 | +0.00 | +266.10 | OCUL, INSP, CRMD, CAPR, KURA, CCOI, LIFE, ZIP | — | $47.69 | $11,086.61 | OCUL×123, INSP×22, CRMD×162, CAPR×186, KURA×99, CCOI×142, LIFE×36, ZIP×297 |
| 2026-08-26 | +2.02 | $47.69 | OCUL×123, INSP×22, CRMD×162, CAPR×186, KURA×99, CCOI×142, LIFE×36, ZIP×297 | $11,042.00 | -44.61 | +47.95 | RZLT, AVBP, FLNC, ABX, AVEX, BE | INSP, CAPR, KURA, CCOI, LIFE, ZIP | $113.49 | $11,059.82 | OCUL×123, CRMD×162, RZLT×276, AVBP×44, FLNC×124, ABX×140, AVEX×79, BE×6 |
| 2026-08-27 | — | $113.49 | OCUL×123, CRMD×162, RZLT×276, AVBP×44, FLNC×124, ABX×140, AVEX×79, BE×6 | $11,152.69 | +92.87 | -41.66 | ACMR, GGB, MT, MU, TX, ANET, DLO | OCUL, CRMD, RZLT, AVBP, FLNC, ABX, AVEX, BE | $2,034.16 | $11,074.91 | ACMR×17, GGB×304, MT×18, MU×1, TX×25, ANET×6, DLO×90 |
| 2026-08-28 | +0.75 | $2,034.16 | ACMR×17, GGB×304, MT×18, MU×1, TX×25, ANET×6, DLO×90 | $11,044.09 | -30.82 | -337.92 | SEDG, GRRR, URBN, ANF, SMTC, VYX, TTMI, NVRI | ACMR, GGB, MT, MU, TX, ANET, DLO | $262.89 | $10,672.57 | SEDG×41, GRRR×88, URBN×17, ANF×9, SMTC×9, VYX×150, TTMI×11, NVRI×60 |
| 2026-08-31 | -5.85 | $262.89 | SEDG×41, GRRR×88, URBN×17, ANF×9, SMTC×9, VYX×150, TTMI×11, NVRI×60 | $10,634.54 | -38.03 | +0.00 | — | SEDG, GRRR, URBN, ANF, SMTC, VYX, TTMI, NVRI | $10,617.29 | $10,617.29 | — |
| 2026-09-01 | -6.30 | $10,617.29 | — | $10,617.29 | -0.00 | +0.00 | — | — | $10,617.29 | $10,617.29 | — |
| 2026-09-02 | -3.83 | $10,617.29 | — | $10,617.29 | -0.00 | +0.00 | — | — | $10,617.29 | $10,617.29 | — |
| 2026-09-03 | -0.90 | $10,617.29 | — | $10,617.29 | -0.00 | -235.56 | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MRNA, ARCT | — | $61.55 | $10,361.87 | ATRC×25, HRMY×30, CABA×365, VSTM×165, RVTY×10, CRK×85, MRNA×9, ARCT×79 |
| 2026-09-04 | +2.25 | $61.55 | ATRC×25, HRMY×30, CABA×365, VSTM×165, RVTY×10, CRK×85, MRNA×9, ARCT×79 | $10,366.42 | +4.55 | -52.91 | ALEC, BHC, BMEA, OABI, OPK, VIR | HRMY, VSTM, RVTY, CRK, MRNA, ARCT | $5.11 | $10,265.92 | ATRC×25, CABA×365, ALEC×515, BHC×193, BMEA×683, OABI×271, OPK×816, VIR×112 |
| 2026-09-08 | -11.47 | $5.11 | ATRC×25, CABA×365, ALEC×515, BHC×193, BMEA×683, OABI×271, OPK×816, VIR×112 | $10,226.54 | -39.38 | +0.00 | — | ATRC, CABA, ALEC, BHC, BMEA, OABI, OPK, VIR | $10,184.81 | $10,184.81 | — |
| 2026-09-09 | -13.95 | $10,184.81 | — | $10,184.81 | +0.00 | +0.00 | — | — | $10,184.81 | $10,184.81 | — |
| 2026-09-10 | -13.28 | $10,184.81 | — | $10,184.81 | +0.00 | +0.00 | — | — | $10,184.81 | $10,184.81 | — |
| 2026-09-11 | +0.50 | $10,184.81 | — | $10,184.81 | +0.00 | -97.11 | AUPH, OVID, ORCL, NVT, COHU, AMTX, CLOV, BAK | — | $144.04 | $10,054.14 | AUPH×78, OVID×466, ORCL×7, NVT×8, COHU×22, AMTX×624, CLOV×268, BAK×600 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,544.55 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $4,332.68 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 138 | $9.01 | $2.40 | — | $3,086.90 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1334 | $0.94 | $16.50 | — | $1,820.44 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $560.20 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $560.20 | ▲ close $10,051.46 vs 09:30 $10,000.00 (session +91.20) | 16:00 close · cash $560.20 · equity $10,051.46 vs 09:30 $10,000.00 (+51.46; session marks +91.20) · 8 name(s) marked open→close (per-name table). TLN×3 09:30 $359.83 → close $362.74 +8.73; VST×8 09:30 $146.90 → close $148.13 +9.84; NRG×10 09:30 $120.00 → close $126.24 +62.40; DAVE×3 09:30 $330.91 → close $334.57 +10.98; SLG×21 09:30 $57.61 → close $56.09 -31.92; MARA×138 09:30 $9.01 → close $9.20 +26.22; LDI×1334 09:30 $0.94 → close $0.90 -53.36; BTBT×833 09:30 $1.50 → close $1.57 +58.31 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $560.20 | ▲ 09:30 equity $10,054.84 vs yday $10,051.46 (+3.38) | 09:30 open · cash $560.20 (unchanged overnight, no fees) · equity $10,054.84 vs prior close $10,051.46 (+3.38) · 8 name(s) re-marked at the open (per-name table). TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×21 yday $56.09 → 09:30 $55.37 -15.12; MARA×138 yday $9.20 → 09:30 $9.22 +2.76; LDI×1334 yday $0.90 → 09:30 $0.91 +13.34; BTBT×833 yday $1.57 → 09:30 $1.52 -41.65 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,661.82 | ▲ +20.13 after sell → book $10,052.82; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $2,854.74 | ▲ +15.71 after sell → book $10,050.79; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $4,126.70 | ▲ +69.94 after sell → book $10,048.75; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $5,135.50 | ▲ +14.07 after sell → book $10,046.73; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 21 | $55.37 | $2.07 | $-51.17 | $6,296.20 | ▼ -51.17 after sell → book $10,044.66; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 138 | $9.22 | $2.44 | $+24.14 | $7,566.12 | ▲ +24.14 after sell → book $10,042.22; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1334 | $0.91 | $16.33 | $-72.85 | $8,759.73 | ▼ -72.85 after sell → book $10,025.89; vs 09:30 mark -16.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 833 | $1.52 | $10.89 | $-4.98 | $10,014.99 | ▼ -4.98 after sell → book $10,014.99; vs 09:30 mark -10.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,766.06 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+6.7; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,621.89 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+5.8; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,403.68 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+8.3; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 309 | $4.05 | $3.99 | — | $5,148.25 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 147 | $8.46 | $2.43 | — | $3,902.19 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 137 | $9.12 | $2.40 | — | $2,650.35 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 77 | $16.20 | $2.22 | — | $1,400.73 | — | union ∩ blue, no 🚨; gate blue=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 85 | $14.66 | $2.25 | — | $152.39 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $152.39 | ▼ close $9,984.67 vs 09:30 $10,054.84 (session -10.94) | 16:00 close · cash $152.39 · equity $9,984.67 vs 09:30 $10,054.84 (-70.17; session marks -10.94) · 8 name(s) marked open→close (per-name table). DVN×27 09:30 $46.18 → close $47.57 +37.53; EOG×8 09:30 $142.77 → close $146.15 +27.04; FANG×6 09:30 $202.70 → close $206.29 +21.54; TMC×309 09:30 $4.05 → close $3.77 -86.52; TGB×147 09:30 $8.46 → close $8.77 +45.57; ABX×137 09:30 $9.12 → close $9.12 +0.00; ALM×77 09:30 $16.20 → close $16.36 +12.32; ALOY×85 09:30 $14.66 → close $13.86 -68.42 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $152.39 | ▼ 09:30 equity $9,865.94 vs yday $9,984.67 (-118.73) | 09:30 open · cash $152.39 (unchanged overnight, no fees) · equity $9,865.94 vs prior close $9,984.67 (-118.73) · 8 name(s) re-marked at the open (per-name table). DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; TMC×309 yday $3.77 → 09:30 $3.72 -15.45; TGB×147 yday $8.77 → 09:30 $8.55 -32.34; ABX×137 yday $9.12 → 09:30 $9.03 -12.33; ALM×77 yday $16.36 → 09:30 $15.78 -44.66; ALOY×85 yday $13.86 → 09:30 $13.19 -56.53 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,446.30 | ▲ +44.98 after sell → book $9,863.85; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,628.58 | ▲ +38.11 after sell → book $9,861.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,880.13 | ▲ +33.34 after sell → book $9,859.78; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 309 | $3.72 | $4.05 | $-110.00 | $5,025.57 | ▼ -110.00 after sell → book $9,855.74; vs 09:30 mark -4.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 147 | $8.55 | $2.47 | $+8.33 | $6,279.95 | ▲ +8.33 after sell → book $9,853.27; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 137 | $9.03 | $2.43 | $-17.16 | $7,514.63 | ▼ -17.16 after sell → book $9,850.84; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 77 | $15.78 | $2.24 | $-36.80 | $8,727.44 | ▼ -36.80 after sell → book $9,848.59; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 85 | $13.19 | $2.27 | $-129.46 | $9,846.32 | ▼ -129.46 after sell → book $9,846.32; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,846.32 | ▲ close $9,846.32 vs 09:30 $9,865.94 (session +0.00) | 16:00 close · cash $9,846.32 · no lots left · equity $9,846.32. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,846.32 | ▲ 09:30 equity $9,846.32 vs yday $9,846.32 (+0.00) | 09:30 open · cash $9,846.32 · no holdings · equity $9,846.32 vs prior close $9,846.32 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,846.32 | ▲ close $9,846.32 vs 09:30 $9,846.32 (session +0.00) | 16:00 close · cash $9,846.32 · no lots left · equity $9,846.32. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,846.32 | ▲ 09:30 equity $9,846.32 vs yday $9,846.32 (+0.00) | 09:30 open · cash $9,846.32 · no holdings · equity $9,846.32 vs prior close $9,846.32 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 59 | $20.55 | $2.17 | — | $8,631.71 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1230.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,446.55 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1230.79 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 59 | $20.65 | $2.17 | — | $6,226.03 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1230.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 213 | $5.77 | $2.75 | — | $4,994.27 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1230.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 62 | $19.63 | $2.18 | — | $3,775.04 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1230.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $2,558.09 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1230.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 703 | $1.75 | $9.07 | — | $1,318.78 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1230.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $160.44 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1230.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.44 | ▲ close $10,051.62 vs 09:30 $9,846.32 (session +229.78) | 16:00 close · cash $160.44 · equity $10,051.62 vs 09:30 $9,846.32 (+205.30; session marks +229.78) · 8 name(s) marked open→close (per-name table). AG×59 09:30 $20.55 → close $21.19 +37.76; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×59 09:30 $20.65 → close $21.11 +27.14; HDSN×213 09:30 $5.77 → close $5.57 -42.60; IAG×62 09:30 $19.63 → close $20.50 +53.94; KGC×41 09:30 $29.63 → close $31.43 +73.80; NFGC×703 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.44 | ▲ 09:30 equity $10,315.34 vs yday $10,051.62 (+263.72) | 09:30 open · cash $160.44 (unchanged overnight, no fees) · equity $10,315.34 vs prior close $10,051.62 (+263.72) · 8 name(s) re-marked at the open (per-name table). AG×59 yday $21.19 → 09:30 $21.90 +41.89; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×59 yday $21.11 → 09:30 $21.75 +37.76; HDSN×213 yday $5.57 → 09:30 $5.67 +21.30; IAG×62 yday $20.50 → 09:30 $21.17 +41.54; KGC×41 yday $31.43 → 09:30 $32.17 +30.34; NFGC×703 yday $1.75 → 09:30 $1.79 +28.12; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 59 | $21.90 | $2.19 | $+75.30 | $1,450.35 | ▲ +75.30 after sell → book $10,313.15; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,692.67 | ▲ +57.15 after sell → book $10,311.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 59 | $21.75 | $2.19 | $+60.55 | $3,973.73 | ▲ +60.55 after sell → book $10,308.92; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 213 | $5.67 | $2.79 | $-26.84 | $5,178.65 | ▼ -26.84 after sell → book $10,306.13; vs 09:30 mark -2.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 62 | $21.17 | $2.20 | $+91.11 | $6,488.99 | ▲ +91.11 after sell → book $10,303.93; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $7,805.82 | ▲ +99.89 after sell → book $10,301.79; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 703 | $1.79 | $9.20 | $+9.86 | $9,055.00 | ▲ +9.86 after sell → book $10,292.60; vs 09:30 mark -9.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,290.57 | ▲ +77.23 after sell → book $10,290.57; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,094.25 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1286.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 74 | $17.20 | $2.21 | — | $7,819.23 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1286.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,735.73 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1286.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 115 | $11.13 | $2.33 | — | $5,453.44 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1286.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 520 | $2.47 | $6.71 | — | $4,162.34 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1286.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 666 | $1.93 | $8.59 | — | $2,868.36 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1286.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,612.19 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1286.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 974 | $1.32 | $12.56 | — | $313.95 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1286.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $313.95 | ▲ close $10,509.84 vs 09:30 $10,315.34 (session +257.76) | 16:00 close · cash $313.95 · equity $10,509.84 vs 09:30 $10,315.34 (+194.50; session marks +257.76) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×74 09:30 $17.20 → close $16.65 -40.70; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×115 09:30 $11.13 → close $13.45 +266.80; AUTL×520 09:30 $2.47 → close $2.41 -31.20; CRDL×666 09:30 $1.93 → close $1.86 -46.62; CRSP×21 09:30 $59.72 → close $59.50 -4.62; CYPH×974 09:30 $1.32 → close $1.42 +97.40 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $313.95 | ▲ 09:30 equity $10,879.58 vs yday $10,509.84 (+369.74) | 09:30 open · cash $313.95 (unchanged overnight, no fees) · equity $10,879.58 vs prior close $10,509.84 (+369.74) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×74 yday $16.65 → 09:30 $16.57 -5.92; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×115 yday $13.45 → 09:30 $13.33 -13.80; AUTL×520 yday $2.41 → 09:30 $2.40 -5.20; CRDL×666 yday $1.86 → 09:30 $1.88 +13.32; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; CYPH×974 yday $1.42 → 09:30 $1.83 +399.34 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,517.01 | ▲ +6.74 after sell → book $10,877.54; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 74 | $16.57 | $2.23 | $-51.07 | $2,740.95 | ▼ -51.07 after sell → book $10,875.30; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,824.08 | ▼ -0.38 after sell → book $10,873.28; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 115 | $13.33 | $2.37 | $+248.30 | $5,354.66 | ▲ +248.30 after sell → book $10,870.91; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 520 | $2.40 | $6.80 | $-49.91 | $6,595.86 | ▼ -49.91 after sell → book $10,864.11; vs 09:30 mark -6.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 666 | $1.88 | $8.71 | $-50.60 | $7,839.22 | ▼ -50.60 after sell → book $10,855.39; vs 09:30 mark -8.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.75 | $2.07 | $-24.50 | $9,070.90 | ▼ -24.50 after sell → book $10,853.32; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 974 | $1.83 | $12.74 | $+471.43 | $10,840.58 | ▲ +471.43 after sell → book $10,840.58; vs 09:30 mark -12.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,840.58 | ▲ close $10,840.58 vs 09:30 $10,879.58 (session +0.00) | 16:00 close · cash $10,840.58 · no lots left · equity $10,840.58. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,840.58 | ▲ 09:30 equity $10,840.58 vs yday $10,840.58 (+0.00) | 09:30 open · cash $10,840.58 · no holdings · equity $10,840.58 vs prior close $10,840.58 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 123 | $10.98 | $2.36 | — | $9,487.68 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+1.2; leftover $1355.07 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.19 | $2.06 | — | $8,139.45 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+7.4; leftover $1355.07 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 162 | $8.35 | $2.48 | — | $6,784.27 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1355.07 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 186 | $7.25 | $2.55 | — | $5,433.22 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1355.07 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 99 | $13.59 | $2.29 | — | $4,085.52 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1355.07 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 142 | $9.49 | $2.42 | — | $2,735.53 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1355.07 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 36 | $36.96 | $2.10 | — | $1,402.87 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1355.07 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 297 | $4.55 | $3.83 | — | $47.69 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1355.07 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.69 | ▲ close $11,086.61 vs 09:30 $10,840.58 (session +266.10) | 16:00 close · cash $47.69 · equity $11,086.61 vs 09:30 $10,840.58 (+246.03; session marks +266.10) · 8 name(s) marked open→close (per-name table). OCUL×123 09:30 $10.98 → close $10.88 -12.30; INSP×22 09:30 $61.19 → close $61.07 -2.64; CRMD×162 09:30 $8.35 → close $8.56 +34.02; CAPR×186 09:30 $7.25 → close $8.29 +193.44; KURA×99 09:30 $13.59 → close $13.59 +0.00; CCOI×142 09:30 $9.49 → close $9.88 +55.38; LIFE×36 09:30 $36.96 → close $38.56 +57.60; ZIP×297 09:30 $4.55 → close $4.35 -59.40 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.69 | ▼ 09:30 equity $11,042.00 vs yday $11,086.61 (-44.61) | 09:30 open · cash $47.69 (unchanged overnight, no fees) · equity $11,042.00 vs prior close $11,086.61 (-44.61) · 8 name(s) re-marked at the open (per-name table). OCUL×123 yday $10.88 → 09:30 $10.79 -11.07; INSP×22 yday $61.07 → 09:30 $60.07 -22.00; CRMD×162 yday $8.56 → 09:30 $8.60 +6.48; CAPR×186 yday $8.29 → 09:30 $8.29 +0.00; KURA×99 yday $13.59 → 09:30 $13.63 +3.96; CCOI×142 yday $9.88 → 09:30 $9.89 +1.42; LIFE×36 yday $38.56 → 09:30 $38.24 -11.52; ZIP×297 yday $4.35 → 09:30 $4.31 -11.88 | — |
| 2026-08-26 09:30 ET | **SELL** | `INSP` | 22 | $60.07 | $2.08 | $-28.77 | $1,367.15 | ▼ -28.77 after sell → book $11,039.92; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 186 | $8.29 | $2.59 | $+188.30 | $2,906.50 | ▲ +188.30 after sell → book $11,037.33; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 99 | $13.63 | $2.31 | $-0.64 | $4,253.56 | ▼ -0.64 after sell → book $11,035.02; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 142 | $9.89 | $2.45 | $+51.93 | $5,655.49 | ▲ +51.93 after sell → book $11,032.57; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 36 | $38.24 | $2.12 | $+41.86 | $7,030.01 | ▲ +41.86 after sell → book $11,030.45; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 297 | $4.31 | $3.89 | $-79.00 | $8,306.19 | ▼ -79.00 after sell → book $11,026.56; vs 09:30 mark -3.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `RZLT` | 276 | $5.01 | $3.56 | — | $6,919.87 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,yday_gainer; 🔵; ret5=+7.5; leftover $1384.36 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 44 | $31.21 | $2.12 | — | $5,544.50 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1384.36 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 124 | $11.12 | $2.36 | — | $4,163.26 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1384.36 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 140 | $9.83 | $2.41 | — | $2,784.65 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1384.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AVEX` | 79 | $17.51 | $2.23 | — | $1,399.14 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $1384.36 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BE` | 6 | $213.94 | $2.01 | — | $113.49 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1384.36 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.49 | ▲ close $11,059.82 vs 09:30 $11,042.00 (session +47.95) | 16:00 close · cash $113.49 · equity $11,059.82 vs 09:30 $11,042.00 (+17.82; session marks +47.95) · 8 name(s) marked open→close (per-name table). OCUL×123 09:30 $10.79 → close $10.77 -2.46; CRMD×162 09:30 $8.60 → close $8.39 -34.02; RZLT×276 09:30 $5.01 → close $5.04 +8.28; AVBP×44 09:30 $31.21 → close $31.14 -3.08; FLNC×124 09:30 $11.12 → close $11.08 -4.96; ABX×140 09:30 $9.83 → close $9.78 -7.00; AVEX×79 09:30 $17.51 → close $18.34 +65.57; BE×6 09:30 $213.94 → close $218.21 +25.62 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.49 | ▲ 09:30 equity $11,152.69 vs yday $11,059.82 (+92.87) | 09:30 open · cash $113.49 (unchanged overnight, no fees) · equity $11,152.69 vs prior close $11,059.82 (+92.87) · 8 name(s) re-marked at the open (per-name table). OCUL×123 yday $10.77 → 09:30 $10.63 -17.22; CRMD×162 yday $8.39 → 09:30 $8.49 +16.20; RZLT×276 yday $5.04 → 09:30 $5.07 +8.28; AVBP×44 yday $31.14 → 09:30 $30.79 -15.40; FLNC×124 yday $11.08 → 09:30 $11.52 +54.56; ABX×140 yday $9.78 → 09:30 $9.68 -14.00; AVEX×79 yday $18.34 → 09:30 $18.43 +7.11; BE×6 yday $218.21 → 09:30 $227.10 +53.34 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 123 | $10.63 | $2.39 | $-47.80 | $1,418.59 | ▼ -47.80 after sell → book $11,150.30; vs 09:30 mark -2.39 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 162 | $8.49 | $2.51 | $+17.69 | $2,791.45 | ▲ +17.69 after sell → book $11,147.78; vs 09:30 mark -2.52 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 276 | $5.07 | $3.62 | $+9.38 | $4,187.16 | ▲ +9.38 after sell → book $11,144.17; vs 09:30 mark -3.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 44 | $30.79 | $2.14 | $-22.74 | $5,539.77 | ▼ -22.74 after sell → book $11,142.02; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 124 | $11.52 | $2.39 | $+44.84 | $6,965.86 | ▲ +44.84 after sell → book $11,139.63; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ABX` | 140 | $9.68 | $2.44 | $-25.85 | $8,318.62 | ▼ -25.85 after sell → book $11,137.19; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVEX` | 79 | $18.43 | $2.25 | $+68.20 | $9,772.33 | ▲ +68.20 after sell → book $11,134.93; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BE` | 6 | $227.10 | $2.03 | $+74.92 | $11,132.90 | ▲ +74.92 after sell → book $11,132.90; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 17 | $81.65 | $2.04 | — | $9,742.81 | — | union ∩ blue, no 🚨; gate blue=True; list mover_buy; 🔵; ret5=+2.0; leftover $1391.61 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 304 | $4.57 | $3.92 | — | $8,349.61 | — | union ∩ blue, no 🚨; gate blue=True; list mover_buy; 🔵; ret5=+1.1; leftover $1391.61 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 18 | $74.54 | $2.04 | — | $7,005.85 | — | union ∩ blue, no 🚨; gate blue=True; list mover_buy; 🔵; ret5=-0.1; leftover $1391.61 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $6,036.85 | — | union ∩ blue, no 🚨; gate blue=True; list mover_buy; 🔵; ret5=+0.1; leftover $1391.61 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 25 | $55.25 | $2.06 | — | $4,653.53 | — | union ∩ blue, no 🚨; gate blue=True; list mover_buy; 🔵; ret5=+2.1; leftover $1391.61 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $205.90 | $2.01 | — | $3,416.12 | — | union ∩ blue, no 🚨; gate blue=True; list mover_buy; 🔵; ret5=+8.5; leftover $1391.61 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 90 | $15.33 | $2.26 | — | $2,034.16 | — | union ∩ blue, no 🚨; gate blue=True; list mover_buy; 🔵; ret5=+7.4; leftover $1391.61 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,034.16 | ▼ close $11,074.91 vs 09:30 $11,152.69 (session -41.66) | 16:00 close · cash $2,034.16 · equity $11,074.91 vs 09:30 $11,152.69 (-77.78; session marks -41.66) · 7 name(s) marked open→close (per-name table). ACMR×17 09:30 $81.65 → close $80.49 -19.72; GGB×304 09:30 $4.57 → close $4.70 +39.52; MT×18 09:30 $74.54 → close $74.63 +1.62; MU×1 09:30 $967.01 → close $935.39 -31.62; TX×25 09:30 $55.25 → close $55.83 +14.50; ANET×6 09:30 $205.90 → close $201.09 -28.86; DLO×90 09:30 $15.33 → close $15.14 -17.10 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,034.16 | ▼ 09:30 equity $11,044.09 vs yday $11,074.91 (-30.82) | 09:30 open · cash $2,034.16 (unchanged overnight, no fees) · equity $11,044.09 vs prior close $11,074.91 (-30.82) · 7 name(s) re-marked at the open (per-name table). ACMR×17 yday $80.49 → 09:30 $79.27 -20.74; GGB×304 yday $4.70 → 09:30 $4.67 -9.12; MT×18 yday $74.63 → 09:30 $75.39 +13.68; MU×1 yday $935.39 → 09:30 $919.29 -16.10; TX×25 yday $55.83 → 09:30 $55.97 +3.50; ANET×6 yday $201.09 → 09:30 $200.00 -6.54; DLO×90 yday $15.14 → 09:30 $15.19 +4.50 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 17 | $79.27 | $2.06 | $-44.56 | $3,379.69 | ▼ -44.56 after sell → book $11,042.03; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 304 | $4.67 | $3.98 | $+22.49 | $4,795.39 | ▲ +22.49 after sell → book $11,038.05; vs 09:30 mark -3.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 18 | $75.39 | $2.06 | $+11.19 | $6,150.34 | ▲ +11.19 after sell → book $11,035.98; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $7,067.62 | ▼ -51.73 after sell → book $11,033.97; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 25 | $55.97 | $2.09 | $+13.85 | $8,464.78 | ▲ +13.85 after sell → book $11,031.88; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $200.00 | $2.03 | $-39.44 | $9,662.75 | ▼ -39.44 after sell → book $11,029.85; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 90 | $15.19 | $2.29 | $-17.15 | $11,027.57 | ▼ -17.15 after sell → book $11,027.57; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 41 | $32.90 | $2.11 | — | $9,676.56 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1378.45 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 88 | $15.66 | $2.25 | — | $8,296.22 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1378.45 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 17 | $79.42 | $2.04 | — | $6,944.04 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1378.45 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $5,627.39 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1378.45 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $4,349.54 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1378.45 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 150 | $9.13 | $2.44 | — | $2,977.60 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer; 🔵; ret5=+20.0; leftover $1378.45 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 11 | $122.81 | $2.02 | — | $1,624.66 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1378.45 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NVRI` | 60 | $22.66 | $2.17 | — | $262.89 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer; 🔵; ret5=+10.6; leftover $1378.45 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $262.89 | ▼ close $10,672.57 vs 09:30 $11,044.09 (session -337.92) | 16:00 close · cash $262.89 · equity $10,672.57 vs 09:30 $11,044.09 (-371.52; session marks -337.92) · 8 name(s) marked open→close (per-name table). SEDG×41 09:30 $32.90 → close $31.41 -61.09; GRRR×88 09:30 $15.66 → close $14.41 -110.00; URBN×17 09:30 $79.42 → close $81.09 +28.39; ANF×9 09:30 $146.07 → close $148.42 +21.15; SMTC×9 09:30 $141.76 → close $131.17 -95.31; VYX×150 09:30 $9.13 → close $8.78 -52.50; TTMI×11 09:30 $122.81 → close $118.65 -45.76; NVRI×60 09:30 $22.66 → close $22.28 -22.80 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $262.89 | ▼ 09:30 equity $10,634.54 vs yday $10,672.57 (-38.03) | 09:30 open · cash $262.89 (unchanged overnight, no fees) · equity $10,634.54 vs prior close $10,672.57 (-38.03) · 8 name(s) re-marked at the open (per-name table). SEDG×41 yday $31.41 → 09:30 $31.15 -10.66; GRRR×88 yday $14.41 → 09:30 $14.44 +2.64; URBN×17 yday $81.09 → 09:30 $80.44 -11.05; ANF×9 yday $148.42 → 09:30 $148.03 -3.51; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; VYX×150 yday $8.78 → 09:30 $8.66 -18.00; TTMI×11 yday $118.65 → 09:30 $118.83 +1.98; NVRI×60 yday $22.28 → 09:30 $22.12 -9.60 | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 41 | $31.15 | $2.13 | $-76.00 | $1,537.91 | ▼ -76.00 after sell → book $10,632.41; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 88 | $14.44 | $2.28 | $-111.89 | $2,806.35 | ▼ -111.89 after sell → book $10,630.13; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 17 | $80.44 | $2.06 | $+13.24 | $4,171.77 | ▲ +13.24 after sell → book $10,628.07; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $5,502.00 | ▲ +13.59 after sell → book $10,626.03; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $6,690.67 | ▼ -89.19 after sell → book $10,624.00; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 150 | $8.66 | $2.48 | $-75.42 | $7,987.19 | ▼ -75.42 after sell → book $10,621.52; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 11 | $118.83 | $2.04 | $-47.85 | $9,292.28 | ▼ -47.85 after sell → book $10,619.48; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NVRI` | 60 | $22.12 | $2.19 | $-36.76 | $10,617.29 | ▼ -36.76 after sell → book $10,617.29; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,617.29 | ▲ close $10,617.29 vs 09:30 $10,634.54 (session +0.00) | 16:00 close · cash $10,617.29 · no lots left · equity $10,617.29. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,617.29 | ▲ 09:30 equity $10,617.29 vs yday $10,617.29 (-0.00) | 09:30 open · cash $10,617.29 · no holdings · equity $10,617.29 vs prior close $10,617.29 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,617.29 | ▲ close $10,617.29 vs 09:30 $10,617.29 (session +0.00) | 16:00 close · cash $10,617.29 · no lots left · equity $10,617.29. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,617.29 | ▲ 09:30 equity $10,617.29 vs yday $10,617.29 (-0.00) | 09:30 open · cash $10,617.29 · no holdings · equity $10,617.29 vs prior close $10,617.29 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,617.29 | ▲ close $10,617.29 vs 09:30 $10,617.29 (session +0.00) | 16:00 close · cash $10,617.29 · no lots left · equity $10,617.29. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,617.29 | ▲ 09:30 equity $10,617.29 vs yday $10,617.29 (-0.00) | 09:30 open · cash $10,617.29 · no holdings · equity $10,617.29 vs prior close $10,617.29 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,293.22 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1327.16 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $42.93 | $2.08 | — | $8,003.24 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1327.16 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 365 | $3.63 | $4.71 | — | $6,673.58 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1327.16 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 165 | $8.03 | $2.48 | — | $5,346.15 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1327.16 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,019.63 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1327.16 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 85 | $15.45 | $2.25 | — | $2,704.13 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1327.16 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,388.61 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1327.16 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 79 | $16.77 | $2.23 | — | $61.55 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1327.16 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.55 | ▼ close $10,361.87 vs 09:30 $10,617.29 (session -235.56) | 16:00 close · cash $61.55 · equity $10,361.87 vs 09:30 $10,617.29 (-255.42; session marks -235.56) · 8 name(s) marked open→close (per-name table). ATRC×25 09:30 $52.88 → close $52.46 -10.50; HRMY×30 09:30 $42.93 → close $41.86 -32.10; CABA×365 09:30 $3.63 → close $3.48 -54.75; VSTM×165 09:30 $8.03 → close $7.98 -8.25; RVTY×10 09:30 $132.45 → close $130.63 -18.20; CRK×85 09:30 $15.45 → close $14.95 -42.50; MRNA×9 09:30 $145.94 → close $148.87 +26.33; ARCT×79 09:30 $16.77 → close $15.56 -95.59 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.55 | ▲ 09:30 equity $10,366.42 vs yday $10,361.87 (+4.55) | 09:30 open · cash $61.55 (unchanged overnight, no fees) · equity $10,366.42 vs prior close $10,361.87 (+4.55) · 8 name(s) re-marked at the open (per-name table). ATRC×25 yday $52.46 → 09:30 $52.03 -10.75; HRMY×30 yday $41.86 → 09:30 $41.50 -10.80; CABA×365 yday $3.48 → 09:30 $3.46 -7.30; VSTM×165 yday $7.98 → 09:30 $7.91 -11.55; RVTY×10 yday $130.63 → 09:30 $130.03 -6.00; CRK×85 yday $14.95 → 09:30 $15.00 +4.25; MRNA×9 yday $148.87 → 09:30 $153.62 +42.75; ARCT×79 yday $15.56 → 09:30 $15.61 +3.95 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 30 | $41.50 | $2.10 | $-47.08 | $1,304.45 | ▼ -47.08 after sell → book $10,364.32; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 165 | $7.91 | $2.52 | $-24.81 | $2,607.08 | ▼ -24.81 after sell → book $10,361.80; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $3,905.34 | ▼ -28.26 after sell → book $10,359.76; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 85 | $15.00 | $2.27 | $-42.76 | $5,178.07 | ▼ -42.76 after sell → book $10,357.49; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 9 | $153.62 | $2.04 | $+65.02 | $6,558.61 | ▲ +65.02 after sell → book $10,355.45; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 79 | $15.61 | $2.25 | $-96.12 | $7,789.55 | ▼ -96.12 after sell → book $10,353.20; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 515 | $2.52 | $6.64 | — | $6,485.11 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1298.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 193 | $6.71 | $2.57 | — | $5,187.51 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1298.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 683 | $1.90 | $8.81 | — | $3,881.00 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1298.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 271 | $4.78 | $3.50 | — | $2,582.12 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1298.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 816 | $1.59 | $10.53 | — | $1,274.16 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1298.26 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 112 | $11.31 | $2.33 | — | $5.11 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1298.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.11 | ▼ close $10,265.92 vs 09:30 $10,366.42 (session -52.91) | 16:00 close · cash $5.11 · equity $10,265.92 vs 09:30 $10,366.42 (-100.50; session marks -52.91) · 8 name(s) marked open→close (per-name table). ATRC×25 09:30 $52.03 → close $51.52 -12.75; CABA×365 09:30 $3.46 → close $3.47 +3.65; ALEC×515 09:30 $2.52 → close $2.46 -30.90; BHC×193 09:30 $6.71 → close $6.56 -28.95; BMEA×683 09:30 $1.90 → close $2.03 +88.79; OABI×271 09:30 $4.78 → close $4.33 -121.95; OPK×816 09:30 $1.59 → close $1.64 +40.80; VIR×112 09:30 $11.31 → close $11.38 +8.40 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.11 | ▼ 09:30 equity $10,226.54 vs yday $10,265.92 (-39.38) | 09:30 open · cash $5.11 (unchanged overnight, no fees) · equity $10,226.54 vs prior close $10,265.92 (-39.38) · 8 name(s) re-marked at the open (per-name table). ATRC×25 yday $51.52 → 09:30 $54.31 +69.75; CABA×365 yday $3.47 → 09:30 $3.43 -14.60; ALEC×515 yday $2.46 → 09:30 $2.38 -41.20; BHC×193 yday $6.56 → 09:30 $6.57 +1.93; BMEA×683 yday $2.03 → 09:30 $2.00 -20.49; OABI×271 yday $4.33 → 09:30 $4.30 -8.13; OPK×816 yday $1.64 → 09:30 $1.63 -8.16; VIR×112 yday $11.38 → 09:30 $11.22 -18.48 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 25 | $54.31 | $2.09 | $+31.60 | $1,360.78 | ▲ +31.60 after sell → book $10,224.46; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 365 | $3.43 | $4.78 | $-82.49 | $2,607.95 | ▼ -82.49 after sell → book $10,219.68; vs 09:30 mark -4.78 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 515 | $2.38 | $6.74 | $-85.48 | $3,826.91 | ▼ -85.48 after sell → book $10,212.94; vs 09:30 mark -6.74 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 193 | $6.57 | $2.61 | $-32.20 | $5,092.31 | ▼ -32.20 after sell → book $10,210.33; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 683 | $2.00 | $8.94 | $+50.55 | $6,449.37 | ▲ +50.55 after sell → book $10,201.39; vs 09:30 mark -8.94 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 271 | $4.30 | $3.55 | $-137.13 | $7,611.12 | ▼ -137.13 after sell → book $10,197.84; vs 09:30 mark -3.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 816 | $1.63 | $10.67 | $+11.44 | $8,930.53 | ▲ +11.44 after sell → book $10,187.17; vs 09:30 mark -10.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 112 | $11.22 | $2.35 | $-14.76 | $10,184.81 | ▼ -14.76 after sell → book $10,184.81; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,184.81 | ▲ close $10,184.81 vs 09:30 $10,226.54 (session +0.00) | 16:00 close · cash $10,184.81 · no lots left · equity $10,184.81. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,184.81 | ▲ 09:30 equity $10,184.81 vs yday $10,184.81 (+0.00) | 09:30 open · cash $10,184.81 · no holdings · equity $10,184.81 vs prior close $10,184.81 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,184.81 | ▲ close $10,184.81 vs 09:30 $10,184.81 (session +0.00) | 16:00 close · cash $10,184.81 · no lots left · equity $10,184.81. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,184.81 | ▲ 09:30 equity $10,184.81 vs yday $10,184.81 (+0.00) | 09:30 open · cash $10,184.81 · no holdings · equity $10,184.81 vs prior close $10,184.81 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,184.81 | ▲ close $10,184.81 vs 09:30 $10,184.81 (session +0.00) | 16:00 close · cash $10,184.81 · no lots left · equity $10,184.81. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,184.81 | ▲ 09:30 equity $10,184.81 vs yday $10,184.81 (+0.00) | 09:30 open · cash $10,184.81 · no holdings · equity $10,184.81 vs prior close $10,184.81 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 78 | $16.28 | $2.22 | — | $8,912.75 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=-1.1; leftover $1273.10 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 466 | $2.73 | $6.01 | — | $7,634.56 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+3.0; leftover $1273.10 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $6,481.54 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,earn_react; 🔵; ⚪; ret5=+9.0; leftover $1273.10 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 8 | $157.78 | $2.01 | — | $5,217.28 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+7.8; leftover $1273.10 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 22 | $56.09 | $2.06 | — | $3,981.25 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+15.3; leftover $1273.10 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 624 | $2.04 | $8.05 | — | $2,700.24 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.8; leftover $1273.10 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 268 | $4.75 | $3.46 | — | $1,423.78 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=+3.4; leftover $1273.10 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 600 | $2.12 | $7.74 | — | $144.04 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1273.10 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $144.04 | ▼ close $10,054.14 vs 09:30 $10,184.81 (session -97.11) | 16:00 close · cash $144.04 · equity $10,054.14 vs 09:30 $10,184.81 (-130.67; session marks -97.11) · 8 name(s) marked open→close (per-name table). AUPH×78 09:30 $16.28 → close $16.10 -14.04; OVID×466 09:30 $2.73 → close $2.69 -18.64; ORCL×7 09:30 $164.43 → close $150.28 -99.05; NVT×8 09:30 $157.78 → close $162.38 +36.80; COHU×22 09:30 $56.09 → close $57.08 +21.78; AMTX×624 09:30 $2.04 → close $2.01 -18.72; CLOV×268 09:30 $4.75 → close $4.82 +18.76; BAK×600 09:30 $2.12 → close $2.08 -24.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1391.61 < 1 share @ 1746.53 |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SRPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OHI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BMRN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `PHM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OBE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `MXL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `AUPH` | 78 | 2026-09-11 @ $16.28 | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=-1.1; leftover $1273.10 |
| `OVID` | 466 | 2026-09-11 @ $2.73 | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+3.0; leftover $1273.10 |
| `ORCL` | 7 | 2026-09-11 @ $164.43 | union ∩ blue, no 🚨; gate blue=True; list flatten,earn_react; 🔵; ⚪; ret5=+9.0; leftover $1273.10 |
| `NVT` | 8 | 2026-09-11 @ $157.78 | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+7.8; leftover $1273.10 |
| `COHU` | 22 | 2026-09-11 @ $56.09 | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+15.3; leftover $1273.10 |
| `AMTX` | 624 | 2026-09-11 @ $2.04 | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.8; leftover $1273.10 |
| `CLOV` | 268 | 2026-09-11 @ $4.75 | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=+3.4; leftover $1273.10 |
| `BAK` | 600 | 2026-09-11 @ $2.12 | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1273.10 |
