# Factor mine action — `union_news_present_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ news_present, no 🚨

Cash book **+3.51%** ($10,351) · signal-only (no cash/fees) was +3.02%. Starts YES **10/21**. Fills 146 · skips 72 · realized $+246.70.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the news camera printed something (any color, not blank).
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
- **Gate** `news_present=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $227.08.

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
| 2026-08-17 | `ELF` | 13 | — | $90.54 | +0.00 | $93.66 | +40.56 | +40.56 | +0.00 | +40.56 |
| 2026-08-17 | `DNN` | 386 | — | $3.24 | +0.00 | $3.19 | -19.30 | -19.30 | +0.00 | -19.30 |
| 2026-08-17 | `NB` | 246 | — | $5.07 | +0.00 | $4.81 | -63.96 | -63.96 | +0.00 | -63.96 |
| 2026-08-18 | `DVN` | 27 | $47.57 | $48.00 | +11.61 | — | +0.00 | +11.61 | +49.14 | — |
| 2026-08-18 | `EOG` | 8 | $146.15 | $148.04 | +15.12 | — | +0.00 | +15.12 | +42.16 | — |
| 2026-08-18 | `FANG` | 6 | $206.29 | $208.93 | +15.84 | — | +0.00 | +15.84 | +37.38 | — |
| 2026-08-18 | `TMC` | 309 | $3.77 | $3.72 | -15.45 | — | +0.00 | -15.45 | -101.97 | — |
| 2026-08-18 | `TGB` | 147 | $8.77 | $8.55 | -32.34 | — | +0.00 | -32.34 | +13.23 | — |
| 2026-08-18 | `ELF` | 13 | $93.66 | $93.44 | -2.86 | — | +0.00 | -2.86 | +37.70 | — |
| 2026-08-18 | `DNN` | 386 | $3.19 | $3.11 | -30.88 | — | +0.00 | -30.88 | -50.18 | — |
| 2026-08-18 | `NB` | 246 | $4.81 | $4.66 | -36.90 | — | +0.00 | -36.90 | -100.86 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 60 | — | $20.55 | +0.00 | $21.19 | +38.40 | +38.40 | +0.00 | +38.40 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 59 | — | $20.65 | +0.00 | $21.11 | +27.14 | +27.14 | +0.00 | +27.14 |
| 2026-08-20 | `HDSN` | 214 | — | $5.77 | +0.00 | $5.57 | -42.80 | -42.80 | +0.00 | -42.80 |
| 2026-08-20 | `IAG` | 63 | — | $19.63 | +0.00 | $20.50 | +54.81 | +54.81 | +0.00 | +54.81 |
| 2026-08-20 | `KGC` | 41 | — | $29.63 | +0.00 | $31.43 | +73.80 | +73.80 | +0.00 | +73.80 |
| 2026-08-20 | `NFGC` | 706 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 60 | $21.19 | $21.90 | +42.60 | — | +0.00 | +42.60 | +81.00 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `CDE` | 59 | $21.11 | $21.75 | +37.76 | — | +0.00 | +37.76 | +64.90 | — |
| 2026-08-21 | `HDSN` | 214 | $5.57 | $5.67 | +21.40 | — | +0.00 | +21.40 | -21.40 | — |
| 2026-08-21 | `IAG` | 63 | $20.50 | $21.17 | +42.21 | — | +0.00 | +42.21 | +97.02 | — |
| 2026-08-21 | `KGC` | 41 | $31.43 | $32.17 | +30.34 | — | +0.00 | +30.34 | +104.14 | — |
| 2026-08-21 | `NFGC` | 706 | $1.75 | $1.79 | +28.24 | — | +0.00 | +28.24 | +28.24 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 75 | — | $17.20 | +0.00 | $16.65 | -41.25 | -41.25 | +0.00 | -41.25 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 116 | — | $11.13 | +0.00 | $13.45 | +269.12 | +269.12 | +0.00 | +269.12 |
| 2026-08-21 | `AUTL` | 523 | — | $2.47 | +0.00 | $2.41 | -31.38 | -31.38 | +0.00 | -31.38 |
| 2026-08-21 | `CRDL` | 669 | — | $1.93 | +0.00 | $1.86 | -46.83 | -46.83 | +0.00 | -46.83 |
| 2026-08-21 | `CRSP` | 21 | — | $59.72 | +0.00 | $59.50 | -4.62 | -4.62 | +0.00 | -4.62 |
| 2026-08-21 | `CYPH` | 979 | — | $1.32 | +0.00 | $1.42 | +97.90 | +97.90 | +0.00 | +97.90 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUPH` | 75 | $16.65 | $16.57 | -6.00 | — | +0.00 | -6.00 | -47.25 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 116 | $13.45 | $13.33 | -13.92 | — | +0.00 | -13.92 | +255.20 | — |
| 2026-08-24 | `AUTL` | 523 | $2.41 | $2.40 | -5.23 | — | +0.00 | -5.23 | -36.61 | — |
| 2026-08-24 | `CRDL` | 669 | $1.86 | $1.88 | +13.38 | — | +0.00 | +13.38 | -33.45 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | — | +0.00 | -15.75 | -20.37 | — |
| 2026-08-24 | `CYPH` | 979 | $1.42 | $1.83 | +401.39 | — | +0.00 | +401.39 | +499.29 | — |
| 2026-08-25 | `MOS` | 57 | — | $23.77 | +0.00 | $24.27 | +28.50 | +28.50 | +0.00 | +28.50 |
| 2026-08-25 | `OCUL` | 124 | — | $10.98 | +0.00 | $10.88 | -12.40 | -12.40 | +0.00 | -12.40 |
| 2026-08-25 | `INSP` | 22 | — | $61.19 | +0.00 | $61.07 | -2.64 | -2.64 | +0.00 | -2.64 |
| 2026-08-25 | `CRMD` | 163 | — | $8.35 | +0.00 | $8.56 | +34.23 | +34.23 | +0.00 | +34.23 |
| 2026-08-25 | `RZLT` | 275 | — | $4.94 | +0.00 | $5.01 | +19.25 | +19.25 | +0.00 | +19.25 |
| 2026-08-25 | `HCA` | 3 | — | $426.97 | +0.00 | $428.76 | +5.37 | +5.37 | +0.00 | +5.37 |
| 2026-08-25 | `CAPR` | 187 | — | $7.25 | +0.00 | $8.29 | +194.48 | +194.48 | +0.00 | +194.48 |
| 2026-08-25 | `SAFX` | 3804 | — | $0.36 | +0.00 | $0.35 | -15.22 | -15.22 | +0.00 | -15.22 |
| 2026-08-26 | `MOS` | 57 | $24.27 | $24.84 | +32.49 | $24.16 | -38.76 | -6.27 | +60.99 | +22.23 |
| 2026-08-26 | `OCUL` | 124 | $10.88 | $10.79 | -11.16 | $10.77 | -2.48 | -13.64 | -23.56 | -26.04 |
| 2026-08-26 | `INSP` | 22 | $61.07 | $60.07 | -22.00 | $61.80 | +38.06 | +16.06 | -24.64 | +13.42 |
| 2026-08-26 | `CRMD` | 163 | $8.56 | $8.60 | +6.52 | $8.39 | -34.23 | -27.71 | +40.75 | +6.52 |
| 2026-08-26 | `RZLT` | 275 | $5.01 | $5.01 | +0.00 | $5.04 | +8.25 | +8.25 | +19.25 | +27.50 |
| 2026-08-26 | `HCA` | 3 | $428.76 | $427.50 | -3.78 | $427.16 | -1.02 | -4.80 | +1.59 | +0.57 |
| 2026-08-26 | `CAPR` | 187 | $8.29 | $8.29 | +0.00 | — | +0.00 | +0.00 | +194.48 | — |
| 2026-08-26 | `SAFX` | 3804 | $0.35 | $0.35 | -3.80 | — | +0.00 | -3.80 | -19.02 | — |
| 2026-08-26 | `AVBP` | 47 | — | $31.21 | +0.00 | $31.14 | -3.29 | -3.29 | +0.00 | -3.29 |
| 2026-08-26 | `FLNC` | 131 | — | $11.12 | +0.00 | $11.08 | -5.24 | -5.24 | +0.00 | -5.24 |
| 2026-08-27 | `MOS` | 57 | $24.16 | $24.00 | -9.12 | — | +0.00 | -9.12 | +13.11 | — |
| 2026-08-27 | `OCUL` | 124 | $10.77 | $10.63 | -17.36 | — | +0.00 | -17.36 | -43.40 | — |
| 2026-08-27 | `INSP` | 22 | $61.80 | $62.10 | +6.60 | — | +0.00 | +6.60 | +20.02 | — |
| 2026-08-27 | `CRMD` | 163 | $8.39 | $8.49 | +16.30 | — | +0.00 | +16.30 | +22.82 | — |
| 2026-08-27 | `RZLT` | 275 | $5.04 | $5.07 | +8.25 | — | +0.00 | +8.25 | +35.75 | — |
| 2026-08-27 | `HCA` | 3 | $427.16 | $424.61 | -7.65 | — | +0.00 | -7.65 | -7.08 | — |
| 2026-08-27 | `AVBP` | 47 | $31.14 | $30.79 | -16.45 | — | +0.00 | -16.45 | -19.74 | — |
| 2026-08-27 | `FLNC` | 131 | $11.08 | $11.52 | +57.64 | — | +0.00 | +57.64 | +52.40 | — |
| 2026-08-27 | `RRC` | 44 | — | $41.44 | +0.00 | $41.64 | +8.80 | +8.80 | +0.00 | +8.80 |
| 2026-08-27 | `ACMR` | 22 | — | $81.65 | +0.00 | $80.49 | -25.52 | -25.52 | +0.00 | -25.52 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `ASML` | 1 | — | $1746.53 | +0.00 | $1735.01 | -11.52 | -11.52 | +0.00 | -11.52 |
| 2026-08-27 | `LRCX` | 5 | — | $318.88 | +0.00 | $318.58 | -1.50 | -1.50 | +0.00 | -1.50 |
| 2026-08-27 | `NVDA` | 8 | — | $222.86 | +0.00 | $227.98 | +40.96 | +40.96 | +0.00 | +40.96 |
| 2026-08-28 | `RRC` | 44 | $41.64 | $41.74 | +4.40 | $41.46 | -12.32 | -7.92 | +13.20 | +0.88 |
| 2026-08-28 | `ACMR` | 22 | $80.49 | $79.27 | -26.84 | — | +0.00 | -26.84 | -52.36 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `ASML` | 1 | $1735.01 | $1734.75 | -0.26 | — | +0.00 | -0.26 | -11.78 | — |
| 2026-08-28 | `LRCX` | 5 | $318.58 | $318.03 | -2.75 | — | +0.00 | -2.75 | -4.25 | — |
| 2026-08-28 | `NVDA` | 8 | $227.98 | $227.36 | -4.96 | — | +0.00 | -4.96 | +36.00 | — |
| 2026-08-28 | `CRK` | 89 | — | $14.63 | +0.00 | $14.29 | -30.26 | -30.26 | +0.00 | -30.26 |
| 2026-08-28 | `MOS` | 54 | — | $23.95 | +0.00 | $23.60 | -18.90 | -18.90 | +0.00 | -18.90 |
| 2026-08-28 | `SLI` | 486 | — | $2.68 | +0.00 | $2.55 | -63.18 | -63.18 | +0.00 | -63.18 |
| 2026-08-28 | `SEDG` | 39 | — | $32.90 | +0.00 | $31.41 | -58.11 | -58.11 | +0.00 | -58.11 |
| 2026-08-28 | `GRRR` | 83 | — | $15.66 | +0.00 | $14.41 | -103.75 | -103.75 | +0.00 | -103.75 |
| 2026-08-28 | `URBN` | 16 | — | $79.42 | +0.00 | $81.09 | +26.72 | +26.72 | +0.00 | +26.72 |
| 2026-08-28 | `SIMO` | 5 | — | $252.24 | +0.00 | $245.81 | -32.15 | -32.15 | +0.00 | -32.15 |
| 2026-08-31 | `RRC` | 44 | $41.46 | $42.00 | +23.76 | — | +0.00 | +23.76 | +24.64 | — |
| 2026-08-31 | `CRK` | 89 | $14.29 | $14.54 | +22.25 | — | +0.00 | +22.25 | -8.01 | — |
| 2026-08-31 | `MOS` | 54 | $23.60 | $23.68 | +4.32 | — | +0.00 | +4.32 | -14.58 | — |
| 2026-08-31 | `SLI` | 486 | $2.55 | $2.58 | +14.58 | — | +0.00 | +14.58 | -48.60 | — |
| 2026-08-31 | `SEDG` | 39 | $31.41 | $31.15 | -10.14 | — | +0.00 | -10.14 | -68.25 | — |
| 2026-08-31 | `GRRR` | 83 | $14.41 | $14.44 | +2.49 | — | +0.00 | +2.49 | -101.26 | — |
| 2026-08-31 | `URBN` | 16 | $81.09 | $80.44 | -10.40 | — | +0.00 | -10.40 | +16.32 | — |
| 2026-08-31 | `SIMO` | 5 | $245.81 | $247.05 | +6.20 | — | +0.00 | +6.20 | -25.95 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 25 | — | $52.88 | +0.00 | $52.46 | -10.50 | -10.50 | +0.00 | -10.50 |
| 2026-09-03 | `HRMY` | 31 | — | $42.93 | +0.00 | $41.86 | -33.17 | -33.17 | +0.00 | -33.17 |
| 2026-09-03 | `CABA` | 367 | — | $3.63 | +0.00 | $3.48 | -55.05 | -55.05 | +0.00 | -55.05 |
| 2026-09-03 | `VSTM` | 166 | — | $8.03 | +0.00 | $7.98 | -8.30 | -8.30 | +0.00 | -8.30 |
| 2026-09-03 | `RVTY` | 10 | — | $132.45 | +0.00 | $130.63 | -18.20 | -18.20 | +0.00 | -18.20 |
| 2026-09-03 | `CRK` | 86 | — | $15.45 | +0.00 | $14.95 | -43.00 | -43.00 | +0.00 | -43.00 |
| 2026-09-03 | `MRNA` | 9 | — | $145.94 | +0.00 | $148.87 | +26.33 | +26.33 | +0.00 | +26.33 |
| 2026-09-03 | `ARCT` | 79 | — | $16.77 | +0.00 | $15.56 | -95.59 | -95.59 | +0.00 | -95.59 |
| 2026-09-04 | `ATRC` | 25 | $52.46 | $52.03 | -10.75 | $51.52 | -12.75 | -23.50 | -21.25 | -34.00 |
| 2026-09-04 | `HRMY` | 31 | $41.86 | $41.50 | -11.16 | — | +0.00 | -11.16 | -44.33 | — |
| 2026-09-04 | `CABA` | 367 | $3.48 | $3.46 | -7.34 | $3.47 | +3.67 | -3.67 | -62.39 | -58.72 |
| 2026-09-04 | `VSTM` | 166 | $7.98 | $7.91 | -11.62 | — | +0.00 | -11.62 | -19.92 | — |
| 2026-09-04 | `RVTY` | 10 | $130.63 | $130.03 | -6.00 | — | +0.00 | -6.00 | -24.20 | — |
| 2026-09-04 | `CRK` | 86 | $14.95 | $15.00 | +4.30 | — | +0.00 | +4.30 | -38.70 | — |
| 2026-09-04 | `MRNA` | 9 | $148.87 | $153.62 | +42.75 | — | +0.00 | +42.75 | +69.08 | — |
| 2026-09-04 | `ARCT` | 79 | $15.56 | $15.61 | +3.95 | — | +0.00 | +3.95 | -91.64 | — |
| 2026-09-04 | `ALEC` | 518 | — | $2.52 | +0.00 | $2.46 | -31.08 | -31.08 | +0.00 | -31.08 |
| 2026-09-04 | `BHC` | 194 | — | $6.71 | +0.00 | $6.56 | -29.10 | -29.10 | +0.00 | -29.10 |
| 2026-09-04 | `BMEA` | 688 | — | $1.90 | +0.00 | $2.03 | +89.44 | +89.44 | +0.00 | +89.44 |
| 2026-09-04 | `OABI` | 273 | — | $4.78 | +0.00 | $4.33 | -122.85 | -122.85 | +0.00 | -122.85 |
| 2026-09-04 | `OPK` | 822 | — | $1.59 | +0.00 | $1.64 | +41.10 | +41.10 | +0.00 | +41.10 |
| 2026-09-04 | `VIR` | 113 | — | $11.31 | +0.00 | $11.38 | +8.47 | +8.47 | +0.00 | +8.47 |
| 2026-09-08 | `ATRC` | 25 | $51.52 | $54.31 | +69.75 | — | +0.00 | +69.75 | +35.75 | — |
| 2026-09-08 | `CABA` | 367 | $3.47 | $3.43 | -14.68 | — | +0.00 | -14.68 | -73.40 | — |
| 2026-09-08 | `ALEC` | 518 | $2.46 | $2.38 | -41.44 | — | +0.00 | -41.44 | -72.52 | — |
| 2026-09-08 | `BHC` | 194 | $6.56 | $6.57 | +1.94 | — | +0.00 | +1.94 | -27.16 | — |
| 2026-09-08 | `BMEA` | 688 | $2.03 | $2.00 | -20.64 | — | +0.00 | -20.64 | +68.80 | — |
| 2026-09-08 | `OABI` | 273 | $4.33 | $4.30 | -8.19 | — | +0.00 | -8.19 | -131.04 | — |
| 2026-09-08 | `OPK` | 822 | $1.64 | $1.63 | -8.22 | — | +0.00 | -8.22 | +32.88 | — |
| 2026-09-08 | `VIR` | 113 | $11.38 | $11.22 | -18.64 | — | +0.00 | -18.64 | -10.17 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `AUPH` | 78 | — | $16.28 | +0.00 | $16.10 | -14.04 | -14.04 | +0.00 | -14.04 |
| 2026-09-11 | `OVID` | 469 | — | $2.73 | +0.00 | $2.69 | -18.76 | -18.76 | +0.00 | -18.76 |
| 2026-09-11 | `SANM` | 6 | — | $206.84 | +0.00 | $216.00 | +54.96 | +54.96 | +0.00 | +54.96 |
| 2026-09-11 | `ORCL` | 7 | — | $164.43 | +0.00 | $150.28 | -99.05 | -99.05 | +0.00 | -99.05 |
| 2026-09-11 | `NVT` | 8 | — | $157.78 | +0.00 | $162.38 | +36.80 | +36.80 | +0.00 | +36.80 |
| 2026-09-11 | `COHU` | 22 | — | $56.09 | +0.00 | $57.08 | +21.78 | +21.78 | +0.00 | +21.78 |
| 2026-09-11 | `CMRC` | 409 | — | $3.13 | +0.00 | $3.50 | +153.38 | +153.38 | +0.00 | +153.38 |
| 2026-09-11 | `DBI` | 216 | — | $5.91 | +0.00 | $5.88 | -6.48 | -6.48 | +0.00 | -6.48 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +91.20 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | — | $560.20 | $10,051.46 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 |
| 2026-08-17 | +2.25 | $560.20 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | $10,054.84 | +3.38 | +2.46 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, NB | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $217.13 | $9,994.76 | DVN×27, EOG×8, FANG×6, TMC×309, TGB×147, ELF×13, DNN×386, NB×246 |
| 2026-08-18 | -6.20 | $217.13 | DVN×27, EOG×8, FANG×6, TMC×309, TGB×147, ELF×13, DNN×386, NB×246 | $9,918.90 | -75.86 | +0.00 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, NB | $9,895.91 | $9,895.91 | — |
| 2026-08-19 | -7.20 | $9,895.91 | — | $9,895.91 | -0.00 | +0.00 | — | — | $9,895.91 | $9,895.91 | — |
| 2026-08-20 | +1.12 | $9,895.91 | — | $9,895.91 | -0.00 | +231.09 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $158.77 | $10,102.46 | AG×60, BHP×13, CDE×59, HDSN×214, IAG×63, KGC×41, NFGC×706, WPM×8 |
| 2026-08-21 | +3.25 | $158.77 | AG×60, BHP×13, CDE×59, HDSN×214, IAG×63, KGC×41, NFGC×706, WPM×8 | $10,367.78 | +265.32 | +259.64 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $318.05 | $10,563.95 | AU×10, AUPH×75, AEM×5, ARCT×116, AUTL×523, CRDL×669, CRSP×21, CYPH×979 |
| 2026-08-24 | -5.17 | $318.05 | AU×10, AUPH×75, AEM×5, ARCT×116, AUTL×523, CRDL×669, CRSP×21, CYPH×979 | $10,935.57 | +371.62 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,896.42 | $10,896.42 | — |
| 2026-08-25 | +1.80 | $10,896.42 | — | $10,896.42 | +0.00 | +251.57 | MOS, OCUL, INSP, CRMD, RZLT, HCA, CAPR, SAFX | — | $73.60 | $11,105.81 | MOS×57, OCUL×124, INSP×22, CRMD×163, RZLT×275, HCA×3, CAPR×187, SAFX×3804 |
| 2026-08-26 | +2.02 | $73.60 | MOS×57, OCUL×124, INSP×22, CRMD×163, RZLT×275, HCA×3, CAPR×187, SAFX×3804 | $11,104.08 | -1.73 | -38.71 | AVBP, FLNC | CAPR, SAFX | $10.47 | $11,032.78 | MOS×57, OCUL×124, INSP×22, CRMD×163, RZLT×275, HCA×3, AVBP×47, FLNC×131 |
| 2026-08-27 | — | $10.47 | MOS×57, OCUL×124, INSP×22, CRMD×163, RZLT×275, HCA×3, AVBP×47, FLNC×131 | $11,070.99 | +38.21 | -20.40 | RRC, ACMR, MU, ASML, LRCX, NVDA | MOS, OCUL, INSP, CRMD, RZLT, HCA, AVBP, FLNC | $1,328.96 | $11,019.04 | RRC×44, ACMR×22, MU×1, ASML×1, LRCX×5, NVDA×8 |
| 2026-08-28 | +0.75 | $1,328.96 | RRC×44, ACMR×22, MU×1, ASML×1, LRCX×5, NVDA×8 | $10,972.53 | -46.51 | -291.95 | CRK, MOS, SLI, SEDG, GRRR, URBN, SIMO | ACMR, MU, ASML, LRCX, NVDA | $94.08 | $10,651.34 | RRC×44, CRK×89, MOS×54, SLI×486, SEDG×39, GRRR×83, URBN×16, SIMO×5 |
| 2026-08-31 | -5.85 | $94.08 | RRC×44, CRK×89, MOS×54, SLI×486, SEDG×39, GRRR×83, URBN×16, SIMO×5 | $10,704.40 | +53.06 | +0.00 | — | RRC, CRK, MOS, SLI, SEDG, GRRR, URBN, SIMO | $10,682.96 | $10,682.96 | — |
| 2026-09-01 | -6.30 | $10,682.96 | — | $10,682.96 | +0.00 | +0.00 | — | — | $10,682.96 | $10,682.96 | — |
| 2026-09-02 | -3.83 | $10,682.96 | — | $10,682.96 | +0.00 | +0.00 | — | — | $10,682.96 | $10,682.96 | — |
| 2026-09-03 | -0.90 | $10,682.96 | — | $10,682.96 | +0.00 | -237.48 | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MRNA, ARCT | — | $53.53 | $10,425.60 | ATRC×25, HRMY×31, CABA×367, VSTM×166, RVTY×10, CRK×86, MRNA×9, ARCT×79 |
| 2026-09-04 | +2.25 | $53.53 | ATRC×25, HRMY×31, CABA×367, VSTM×166, RVTY×10, CRK×86, MRNA×9, ARCT×79 | $10,429.73 | +4.13 | -53.10 | ALEC, BHC, BMEA, OABI, OPK, VIR | HRMY, VSTM, RVTY, CRK, MRNA, ARCT | $7.09 | $10,328.82 | ATRC×25, CABA×367, ALEC×518, BHC×194, BMEA×688, OABI×273, OPK×822, VIR×113 |
| 2026-09-08 | -11.47 | $7.09 | ATRC×25, CABA×367, ALEC×518, BHC×194, BMEA×688, OABI×273, OPK×822, VIR×113 | $10,288.69 | -40.13 | +0.00 | — | ATRC, CABA, ALEC, BHC, BMEA, OABI, OPK, VIR | $10,246.72 | $10,246.72 | — |
| 2026-09-09 | -13.95 | $10,246.72 | — | $10,246.72 | +0.00 | +0.00 | — | — | $10,246.72 | $10,246.72 | — |
| 2026-09-10 | -13.28 | $10,246.72 | — | $10,246.72 | +0.00 | +0.00 | — | — | $10,246.72 | $10,246.72 | — |
| 2026-09-11 | +0.50 | $10,246.72 | — | $10,246.72 | +0.00 | +128.59 | AUPH, OVID, SANM, ORCL, NVT, COHU, CMRC, DBI | — | $227.08 | $10,350.88 | AUPH×78, OVID×469, SANM×6, ORCL×7, NVT×8, COHU×22, CMRC×409, DBI×216 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,544.55 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $4,332.68 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 138 | $9.01 | $2.40 | — | $3,086.90 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1334 | $0.94 | $16.50 | — | $1,820.44 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $560.20 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
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
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,766.06 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+6.7; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,621.89 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+5.8; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,403.68 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+8.3; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 309 | $4.05 | $3.99 | — | $5,148.25 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 147 | $8.46 | $2.43 | — | $3,902.19 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 13 | $90.54 | $2.03 | — | $2,723.15 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=-7.2; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 386 | $3.24 | $4.98 | — | $1,467.53 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ⚪; ret5=+0.3; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 246 | $5.07 | $3.17 | — | $217.13 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=-4.7; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $217.13 | ▲ close $9,994.76 vs 09:30 $10,054.84 (session +2.46) | 16:00 close · cash $217.13 · equity $9,994.76 vs 09:30 $10,054.84 (-60.08; session marks +2.46) · 8 name(s) marked open→close (per-name table). DVN×27 09:30 $46.18 → close $47.57 +37.53; EOG×8 09:30 $142.77 → close $146.15 +27.04; FANG×6 09:30 $202.70 → close $206.29 +21.54; TMC×309 09:30 $4.05 → close $3.77 -86.52; TGB×147 09:30 $8.46 → close $8.77 +45.57; ELF×13 09:30 $90.54 → close $93.66 +40.56; DNN×386 09:30 $3.24 → close $3.19 -19.30; NB×246 09:30 $5.07 → close $4.81 -63.96 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $217.13 | ▼ 09:30 equity $9,918.90 vs yday $9,994.76 (-75.86) | 09:30 open · cash $217.13 (unchanged overnight, no fees) · equity $9,918.90 vs prior close $9,994.76 (-75.86) · 8 name(s) re-marked at the open (per-name table). DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; TMC×309 yday $3.77 → 09:30 $3.72 -15.45; TGB×147 yday $8.77 → 09:30 $8.55 -32.34; ELF×13 yday $93.66 → 09:30 $93.44 -2.86; DNN×386 yday $3.19 → 09:30 $3.11 -30.88; NB×246 yday $4.81 → 09:30 $4.66 -36.90 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,511.04 | ▲ +44.98 after sell → book $9,916.81; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,693.33 | ▲ +38.11 after sell → book $9,914.78; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,944.88 | ▲ +33.34 after sell → book $9,912.75; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 309 | $3.72 | $4.05 | $-110.00 | $5,090.31 | ▼ -110.00 after sell → book $9,908.70; vs 09:30 mark -4.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 147 | $8.55 | $2.47 | $+8.33 | $6,344.70 | ▲ +8.33 after sell → book $9,906.24; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 13 | $93.44 | $2.05 | $+33.62 | $7,557.37 | ▲ +33.62 after sell → book $9,904.19; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 386 | $3.11 | $5.05 | $-60.21 | $8,752.77 | ▼ -60.21 after sell → book $9,899.13; vs 09:30 mark -5.06 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `NB` | 246 | $4.66 | $3.22 | $-107.26 | $9,895.91 | ▼ -107.26 after sell → book $9,895.91; vs 09:30 mark -3.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,895.91 | ▲ close $9,895.91 vs 09:30 $9,918.90 (session +0.00) | 16:00 close · cash $9,895.91 · no lots left · equity $9,895.91. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,895.91 | ▲ 09:30 equity $9,895.91 vs yday $9,895.91 (-0.00) | 09:30 open · cash $9,895.91 · no holdings · equity $9,895.91 vs prior close $9,895.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,895.91 | ▲ close $9,895.91 vs 09:30 $9,895.91 (session +0.00) | 16:00 close · cash $9,895.91 · no lots left · equity $9,895.91. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,895.91 | ▲ 09:30 equity $9,895.91 vs yday $9,895.91 (-0.00) | 09:30 open · cash $9,895.91 · no holdings · equity $9,895.91 vs prior close $9,895.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,660.74 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,475.58 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 59 | $20.65 | $2.17 | — | $6,255.06 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 214 | $5.77 | $2.76 | — | $5,017.52 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,778.65 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $2,561.71 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 706 | $1.75 | $9.11 | — | $1,317.10 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $158.77 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.77 | ▲ close $10,102.46 vs 09:30 $9,895.91 (session +231.09) | 16:00 close · cash $158.77 · equity $10,102.46 vs 09:30 $9,895.91 (+206.55; session marks +231.09) · 8 name(s) marked open→close (per-name table). AG×60 09:30 $20.55 → close $21.19 +38.40; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×59 09:30 $20.65 → close $21.11 +27.14; HDSN×214 09:30 $5.77 → close $5.57 -42.80; IAG×63 09:30 $19.63 → close $20.50 +54.81; KGC×41 09:30 $29.63 → close $31.43 +73.80; NFGC×706 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.77 | ▲ 09:30 equity $10,367.78 vs yday $10,102.46 (+265.32) | 09:30 open · cash $158.77 (unchanged overnight, no fees) · equity $10,367.78 vs prior close $10,102.46 (+265.32) · 8 name(s) re-marked at the open (per-name table). AG×60 yday $21.19 → 09:30 $21.90 +42.60; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×59 yday $21.11 → 09:30 $21.75 +37.76; HDSN×214 yday $5.57 → 09:30 $5.67 +21.40; IAG×63 yday $20.50 → 09:30 $21.17 +42.21; KGC×41 yday $31.43 → 09:30 $32.17 +30.34; NFGC×706 yday $1.75 → 09:30 $1.79 +28.24; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 60 | $21.90 | $2.19 | $+76.64 | $1,470.58 | ▲ +76.64 after sell → book $10,365.59; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,712.89 | ▲ +57.15 after sell → book $10,363.54; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 59 | $21.75 | $2.19 | $+60.55 | $3,993.95 | ▲ +60.55 after sell → book $10,361.35; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 214 | $5.67 | $2.81 | $-26.97 | $5,204.53 | ▼ -26.97 after sell → book $10,358.55; vs 09:30 mark -2.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 63 | $21.17 | $2.20 | $+92.64 | $6,536.04 | ▲ +92.64 after sell → book $10,356.35; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $7,852.87 | ▲ +99.89 after sell → book $10,354.21; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 706 | $1.79 | $9.23 | $+9.90 | $9,107.38 | ▲ +9.90 after sell → book $10,344.98; vs 09:30 mark -9.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,342.94 | ▲ +77.23 after sell → book $10,342.94; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,146.62 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 75 | $17.20 | $2.21 | — | $7,854.41 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,770.90 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 116 | $11.13 | $2.34 | — | $5,477.49 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 523 | $2.47 | $6.75 | — | $4,178.93 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 669 | $1.93 | $8.63 | — | $2,879.13 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,622.96 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 979 | $1.32 | $12.63 | — | $318.05 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $318.05 | ▲ close $10,563.95 vs 09:30 $10,367.78 (session +259.64) | 16:00 close · cash $318.05 · equity $10,563.95 vs 09:30 $10,367.78 (+196.17; session marks +259.64) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×75 09:30 $17.20 → close $16.65 -41.25; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×116 09:30 $11.13 → close $13.45 +269.12; AUTL×523 09:30 $2.47 → close $2.41 -31.38; CRDL×669 09:30 $1.93 → close $1.86 -46.83; CRSP×21 09:30 $59.72 → close $59.50 -4.62; CYPH×979 09:30 $1.32 → close $1.42 +97.90 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $318.05 | ▲ 09:30 equity $10,935.57 vs yday $10,563.95 (+371.62) | 09:30 open · cash $318.05 (unchanged overnight, no fees) · equity $10,935.57 vs prior close $10,563.95 (+371.62) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×75 yday $16.65 → 09:30 $16.57 -6.00; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×116 yday $13.45 → 09:30 $13.33 -13.92; AUTL×523 yday $2.41 → 09:30 $2.40 -5.23; CRDL×669 yday $1.86 → 09:30 $1.88 +13.38; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; CYPH×979 yday $1.42 → 09:30 $1.83 +401.39 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,521.11 | ▲ +6.74 after sell → book $10,933.53; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 75 | $16.57 | $2.24 | $-51.70 | $2,761.62 | ▼ -51.70 after sell → book $10,931.29; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,844.74 | ▼ -0.38 after sell → book $10,929.26; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 116 | $13.33 | $2.37 | $+250.49 | $5,388.66 | ▲ +250.49 after sell → book $10,926.90; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 523 | $2.40 | $6.84 | $-50.20 | $6,637.01 | ▼ -50.20 after sell → book $10,920.05; vs 09:30 mark -6.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 669 | $1.88 | $8.75 | $-50.83 | $7,885.98 | ▼ -50.83 after sell → book $10,911.30; vs 09:30 mark -8.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.75 | $2.07 | $-24.50 | $9,117.66 | ▼ -24.50 after sell → book $10,909.23; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 979 | $1.83 | $12.81 | $+473.86 | $10,896.42 | ▲ +473.86 after sell → book $10,896.42; vs 09:30 mark -12.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,896.42 | ▲ close $10,896.42 vs 09:30 $10,935.57 (session +0.00) | 16:00 close · cash $10,896.42 · no lots left · equity $10,896.42. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,896.42 | ▲ 09:30 equity $10,896.42 vs yday $10,896.42 (+0.00) | 09:30 open · cash $10,896.42 · no holdings · equity $10,896.42 vs prior close $10,896.42 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 57 | $23.77 | $2.16 | — | $9,539.37 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ⚪; ret5=+13.0; leftover $1362.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 124 | $10.98 | $2.36 | — | $8,175.49 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+1.2; leftover $1362.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.19 | $2.06 | — | $6,827.25 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+7.4; leftover $1362.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 163 | $8.35 | $2.48 | — | $5,463.72 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1362.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 275 | $4.94 | $3.55 | — | $4,101.68 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+7.1; leftover $1362.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $2,818.77 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+6.0; leftover $1362.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 187 | $7.25 | $2.55 | — | $1,460.47 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1362.05 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3804 | $0.36 | $25.03 | — | $73.60 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer; ret5=-15.6; leftover $1362.05 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.60 | ▲ close $11,105.81 vs 09:30 $10,896.42 (session +251.57) | 16:00 close · cash $73.60 · equity $11,105.81 vs 09:30 $10,896.42 (+209.39; session marks +251.57) · 8 name(s) marked open→close (per-name table). MOS×57 09:30 $23.77 → close $24.27 +28.50; OCUL×124 09:30 $10.98 → close $10.88 -12.40; INSP×22 09:30 $61.19 → close $61.07 -2.64; CRMD×163 09:30 $8.35 → close $8.56 +34.23; RZLT×275 09:30 $4.94 → close $5.01 +19.25; HCA×3 09:30 $426.97 → close $428.76 +5.37; CAPR×187 09:30 $7.25 → close $8.29 +194.48; SAFX×3804 09:30 $0.36 → close $0.35 -15.22 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.60 | ▼ 09:30 equity $11,104.08 vs yday $11,105.81 (-1.73) | 09:30 open · cash $73.60 (unchanged overnight, no fees) · equity $11,104.08 vs prior close $11,105.81 (-1.73) · 8 name(s) re-marked at the open (per-name table). MOS×57 yday $24.27 → 09:30 $24.84 +32.49; OCUL×124 yday $10.88 → 09:30 $10.79 -11.16; INSP×22 yday $61.07 → 09:30 $60.07 -22.00; CRMD×163 yday $8.56 → 09:30 $8.60 +6.52; RZLT×275 yday $5.01 → 09:30 $5.01 +0.00; HCA×3 yday $428.76 → 09:30 $427.50 -3.78; CAPR×187 yday $8.29 → 09:30 $8.29 +0.00; SAFX×3804 yday $0.35 → 09:30 $0.35 -3.80 | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 187 | $8.29 | $2.59 | $+189.33 | $1,621.24 | ▲ +189.33 after sell → book $11,101.48; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 3804 | $0.35 | $25.48 | $-69.53 | $2,938.57 | ▼ -69.53 after sell → book $11,076.00; vs 09:30 mark -25.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 47 | $31.21 | $2.13 | — | $1,469.57 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1469.28 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 131 | $11.12 | $2.38 | — | $10.47 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1469.28 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.47 | ▼ close $11,032.78 vs 09:30 $11,104.08 (session -38.71) | 16:00 close · cash $10.47 · equity $11,032.78 vs 09:30 $11,104.08 (-71.30; session marks -38.71) · 8 name(s) marked open→close (per-name table). MOS×57 09:30 $24.84 → close $24.16 -38.76; OCUL×124 09:30 $10.79 → close $10.77 -2.48; INSP×22 09:30 $60.07 → close $61.80 +38.06; CRMD×163 09:30 $8.60 → close $8.39 -34.23; RZLT×275 09:30 $5.01 → close $5.04 +8.25; HCA×3 09:30 $427.50 → close $427.16 -1.02; AVBP×47 09:30 $31.21 → close $31.14 -3.29; FLNC×131 09:30 $11.12 → close $11.08 -5.24 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.47 | ▲ 09:30 equity $11,070.99 vs yday $11,032.78 (+38.21) | 09:30 open · cash $10.47 (unchanged overnight, no fees) · equity $11,070.99 vs prior close $11,032.78 (+38.21) · 8 name(s) re-marked at the open (per-name table). MOS×57 yday $24.16 → 09:30 $24.00 -9.12; OCUL×124 yday $10.77 → 09:30 $10.63 -17.36; INSP×22 yday $61.80 → 09:30 $62.10 +6.60; CRMD×163 yday $8.39 → 09:30 $8.49 +16.30; RZLT×275 yday $5.04 → 09:30 $5.07 +8.25; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; AVBP×47 yday $31.14 → 09:30 $30.79 -16.45; FLNC×131 yday $11.08 → 09:30 $11.52 +57.64 | — |
| 2026-08-27 09:30 ET | **SELL** | `MOS` | 57 | $24.00 | $2.18 | $+8.77 | $1,376.28 | ▲ +8.77 after sell → book $11,068.80; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 124 | $10.63 | $2.39 | $-48.16 | $2,692.01 | ▼ -48.16 after sell → book $11,066.41; vs 09:30 mark -2.39 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 22 | $62.10 | $2.08 | $+15.89 | $4,056.13 | ▲ +15.89 after sell → book $11,064.33; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 163 | $8.49 | $2.52 | $+17.82 | $5,437.49 | ▲ +17.82 after sell → book $11,061.82; vs 09:30 mark -2.51 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 275 | $5.07 | $3.60 | $+28.60 | $6,828.13 | ▲ +28.60 after sell → book $11,058.21; vs 09:30 mark -3.61 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-11.10 | $8,099.94 | ▼ -11.10 after sell → book $11,056.19; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 47 | $30.79 | $2.15 | $-24.02 | $9,544.92 | ▼ -24.02 after sell → book $11,054.04; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 131 | $11.52 | $2.42 | $+47.60 | $11,051.62 | ▲ +47.60 after sell → book $11,051.62; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 44 | $41.44 | $2.12 | — | $9,226.14 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+3.1; leftover $1841.94 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 22 | $81.65 | $2.06 | — | $7,427.79 | — | union ∩ news_present, no 🚨; gate news_present=True; list mover_buy; 🔵; ret5=+2.0; leftover $1841.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $6,458.78 | — | union ∩ news_present, no 🚨; gate news_present=True; list mover_buy; 🔵; ret5=+0.1; leftover $1841.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ASML` | 1 | $1746.53 | $1.99 | — | $4,710.26 | — | union ∩ news_present, no 🚨; gate news_present=True; list mover_buy; 🔵; ret5=-0.3; leftover $1841.94 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 5 | $318.88 | $2.00 | — | $3,113.85 | — | union ∩ news_present, no 🚨; gate news_present=True; list mover_buy; 🔵; ret5=+1.9; leftover $1841.94 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 8 | $222.86 | $2.01 | — | $1,328.96 | — | union ∩ news_present, no 🚨; gate news_present=True; list mover_buy; 🔵; ret5=-3.6; leftover $1841.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,328.96 | ▼ close $11,019.04 vs 09:30 $11,070.99 (session -20.40) | 16:00 close · cash $1,328.96 · equity $11,019.04 vs 09:30 $11,070.99 (-51.95; session marks -20.40) · 6 name(s) marked open→close (per-name table). RRC×44 09:30 $41.44 → close $41.64 +8.80; ACMR×22 09:30 $81.65 → close $80.49 -25.52; MU×1 09:30 $967.01 → close $935.39 -31.62; ASML×1 09:30 $1746.53 → close $1735.01 -11.52; LRCX×5 09:30 $318.88 → close $318.58 -1.50; NVDA×8 09:30 $222.86 → close $227.98 +40.96 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,328.96 | ▼ 09:30 equity $10,972.53 vs yday $11,019.04 (-46.51) | 09:30 open · cash $1,328.96 (unchanged overnight, no fees) · equity $10,972.53 vs prior close $11,019.04 (-46.51) · 6 name(s) re-marked at the open (per-name table). RRC×44 yday $41.64 → 09:30 $41.74 +4.40; ACMR×22 yday $80.49 → 09:30 $79.27 -26.84; MU×1 yday $935.39 → 09:30 $919.29 -16.10; ASML×1 yday $1735.01 → 09:30 $1734.75 -0.26; LRCX×5 yday $318.58 → 09:30 $318.03 -2.75; NVDA×8 yday $227.98 → 09:30 $227.36 -4.96 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 22 | $79.27 | $2.08 | $-56.50 | $3,070.82 | ▼ -56.50 after sell → book $10,970.45; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $3,988.10 | ▼ -51.73 after sell → book $10,968.44; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ASML` | 1 | $1734.75 | $2.02 | $-15.79 | $5,720.83 | ▼ -15.79 after sell → book $10,966.42; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 5 | $318.03 | $2.03 | $-8.28 | $7,308.95 | ▼ -8.28 after sell → book $10,964.39; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 8 | $227.36 | $2.04 | $+31.95 | $9,125.79 | ▲ +31.95 after sell → book $10,962.35; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 89 | $14.63 | $2.26 | — | $7,821.47 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+5.8; leftover $1303.68 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `MOS` | 54 | $23.95 | $2.15 | — | $6,526.02 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+1.8; leftover $1303.68 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SLI` | 486 | $2.68 | $6.27 | — | $5,217.27 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,ohlc_hot; ret5=+16.3; leftover $1303.68 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $32.90 | $2.11 | — | $3,932.06 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1303.68 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 83 | $15.66 | $2.24 | — | $2,630.04 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1303.68 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $79.42 | $2.04 | — | $1,357.28 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1303.68 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $94.08 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1303.68 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.08 | ▼ close $10,651.34 vs 09:30 $10,972.53 (session -291.95) | 16:00 close · cash $94.08 · equity $10,651.34 vs 09:30 $10,972.53 (-321.19; session marks -291.95) · 8 name(s) marked open→close (per-name table). RRC×44 09:30 $41.74 → close $41.46 -12.32; CRK×89 09:30 $14.63 → close $14.29 -30.26; MOS×54 09:30 $23.95 → close $23.60 -18.90; SLI×486 09:30 $2.68 → close $2.55 -63.18; SEDG×39 09:30 $32.90 → close $31.41 -58.11; GRRR×83 09:30 $15.66 → close $14.41 -103.75; URBN×16 09:30 $79.42 → close $81.09 +26.72; SIMO×5 09:30 $252.24 → close $245.81 -32.15 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.08 | ▲ 09:30 equity $10,704.40 vs yday $10,651.34 (+53.06) | 09:30 open · cash $94.08 (unchanged overnight, no fees) · equity $10,704.40 vs prior close $10,651.34 (+53.06) · 8 name(s) re-marked at the open (per-name table). RRC×44 yday $41.46 → 09:30 $42.00 +23.76; CRK×89 yday $14.29 → 09:30 $14.54 +22.25; MOS×54 yday $23.60 → 09:30 $23.68 +4.32; SLI×486 yday $2.55 → 09:30 $2.58 +14.58; SEDG×39 yday $31.41 → 09:30 $31.15 -10.14; GRRR×83 yday $14.41 → 09:30 $14.44 +2.49; URBN×16 yday $81.09 → 09:30 $80.44 -10.40; SIMO×5 yday $245.81 → 09:30 $247.05 +6.20 | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 44 | $42.00 | $2.15 | $+20.37 | $1,939.93 | ▲ +20.37 after sell → book $10,702.25; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 89 | $14.54 | $2.28 | $-12.55 | $3,231.71 | ▼ -12.55 after sell → book $10,699.97; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 54 | $23.68 | $2.17 | $-18.90 | $4,508.26 | ▼ -18.90 after sell → book $10,697.80; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 486 | $2.58 | $6.36 | $-61.23 | $5,755.78 | ▼ -61.23 after sell → book $10,691.44; vs 09:30 mark -6.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 39 | $31.15 | $2.13 | $-72.48 | $6,968.50 | ▼ -72.48 after sell → book $10,689.31; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 83 | $14.44 | $2.26 | $-105.76 | $8,164.76 | ▼ -105.76 after sell → book $10,687.05; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $80.44 | $2.06 | $+12.22 | $9,449.74 | ▲ +12.22 after sell → book $10,684.99; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $10,682.96 | ▼ -29.98 after sell → book $10,682.96; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,682.96 | ▲ close $10,682.96 vs 09:30 $10,704.40 (session +0.00) | 16:00 close · cash $10,682.96 · no lots left · equity $10,682.96. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,682.96 | ▲ 09:30 equity $10,682.96 vs yday $10,682.96 (+0.00) | 09:30 open · cash $10,682.96 · no holdings · equity $10,682.96 vs prior close $10,682.96 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,682.96 | ▲ close $10,682.96 vs 09:30 $10,682.96 (session +0.00) | 16:00 close · cash $10,682.96 · no lots left · equity $10,682.96. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,682.96 | ▲ 09:30 equity $10,682.96 vs yday $10,682.96 (+0.00) | 09:30 open · cash $10,682.96 · no holdings · equity $10,682.96 vs prior close $10,682.96 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,682.96 | ▲ close $10,682.96 vs 09:30 $10,682.96 (session +0.00) | 16:00 close · cash $10,682.96 · no lots left · equity $10,682.96. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,682.96 | ▲ 09:30 equity $10,682.96 vs yday $10,682.96 (+0.00) | 09:30 open · cash $10,682.96 · no holdings · equity $10,682.96 vs prior close $10,682.96 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,358.90 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1335.37 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $42.93 | $2.08 | — | $8,025.98 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1335.37 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 367 | $3.63 | $4.73 | — | $6,689.04 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1335.37 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 166 | $8.03 | $2.49 | — | $5,353.57 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1335.37 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,027.05 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1335.37 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 86 | $15.45 | $2.25 | — | $2,696.10 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1335.37 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,380.58 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1335.37 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 79 | $16.77 | $2.23 | — | $53.53 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1335.37 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.53 | ▼ close $10,425.60 vs 09:30 $10,682.96 (session -237.48) | 16:00 close · cash $53.53 · equity $10,425.60 vs 09:30 $10,682.96 (-257.36; session marks -237.48) · 8 name(s) marked open→close (per-name table). ATRC×25 09:30 $52.88 → close $52.46 -10.50; HRMY×31 09:30 $42.93 → close $41.86 -33.17; CABA×367 09:30 $3.63 → close $3.48 -55.05; VSTM×166 09:30 $8.03 → close $7.98 -8.30; RVTY×10 09:30 $132.45 → close $130.63 -18.20; CRK×86 09:30 $15.45 → close $14.95 -43.00; MRNA×9 09:30 $145.94 → close $148.87 +26.33; ARCT×79 09:30 $16.77 → close $15.56 -95.59 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.53 | ▲ 09:30 equity $10,429.73 vs yday $10,425.60 (+4.13) | 09:30 open · cash $53.53 (unchanged overnight, no fees) · equity $10,429.73 vs prior close $10,425.60 (+4.13) · 8 name(s) re-marked at the open (per-name table). ATRC×25 yday $52.46 → 09:30 $52.03 -10.75; HRMY×31 yday $41.86 → 09:30 $41.50 -11.16; CABA×367 yday $3.48 → 09:30 $3.46 -7.34; VSTM×166 yday $7.98 → 09:30 $7.91 -11.62; RVTY×10 yday $130.63 → 09:30 $130.03 -6.00; CRK×86 yday $14.95 → 09:30 $15.00 +4.30; MRNA×9 yday $148.87 → 09:30 $153.62 +42.75; ARCT×79 yday $15.56 → 09:30 $15.61 +3.95 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 31 | $41.50 | $2.10 | $-48.52 | $1,337.92 | ▼ -48.52 after sell → book $10,427.62; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 166 | $7.91 | $2.53 | $-24.93 | $2,648.46 | ▼ -24.93 after sell → book $10,425.10; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $3,946.72 | ▼ -28.26 after sell → book $10,423.06; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 86 | $15.00 | $2.27 | $-43.22 | $5,234.44 | ▼ -43.22 after sell → book $10,420.78; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 9 | $153.62 | $2.04 | $+65.02 | $6,614.98 | ▲ +65.02 after sell → book $10,418.74; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 79 | $15.61 | $2.25 | $-96.12 | $7,845.92 | ▼ -96.12 after sell → book $10,416.49; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 518 | $2.52 | $6.68 | — | $6,533.88 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1307.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 194 | $6.71 | $2.57 | — | $5,229.57 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1307.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 688 | $1.90 | $8.88 | — | $3,913.50 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1307.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 273 | $4.78 | $3.52 | — | $2,605.03 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1307.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 822 | $1.59 | $10.60 | — | $1,287.45 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1307.65 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 113 | $11.31 | $2.33 | — | $7.09 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1307.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.09 | ▼ close $10,328.82 vs 09:30 $10,429.73 (session -53.10) | 16:00 close · cash $7.09 · equity $10,328.82 vs 09:30 $10,429.73 (-100.91; session marks -53.10) · 8 name(s) marked open→close (per-name table). ATRC×25 09:30 $52.03 → close $51.52 -12.75; CABA×367 09:30 $3.46 → close $3.47 +3.67; ALEC×518 09:30 $2.52 → close $2.46 -31.08; BHC×194 09:30 $6.71 → close $6.56 -29.10; BMEA×688 09:30 $1.90 → close $2.03 +89.44; OABI×273 09:30 $4.78 → close $4.33 -122.85; OPK×822 09:30 $1.59 → close $1.64 +41.10; VIR×113 09:30 $11.31 → close $11.38 +8.47 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.09 | ▼ 09:30 equity $10,288.69 vs yday $10,328.82 (-40.13) | 09:30 open · cash $7.09 (unchanged overnight, no fees) · equity $10,288.69 vs prior close $10,328.82 (-40.13) · 8 name(s) re-marked at the open (per-name table). ATRC×25 yday $51.52 → 09:30 $54.31 +69.75; CABA×367 yday $3.47 → 09:30 $3.43 -14.68; ALEC×518 yday $2.46 → 09:30 $2.38 -41.44; BHC×194 yday $6.56 → 09:30 $6.57 +1.94; BMEA×688 yday $2.03 → 09:30 $2.00 -20.64; OABI×273 yday $4.33 → 09:30 $4.30 -8.19; OPK×822 yday $1.64 → 09:30 $1.63 -8.22; VIR×113 yday $11.38 → 09:30 $11.22 -18.64 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 25 | $54.31 | $2.09 | $+31.60 | $1,362.75 | ▲ +31.60 after sell → book $10,286.60; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 367 | $3.43 | $4.81 | $-82.94 | $2,616.76 | ▼ -82.94 after sell → book $10,281.80; vs 09:30 mark -4.80 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 518 | $2.38 | $6.78 | $-85.98 | $3,842.82 | ▼ -85.98 after sell → book $10,275.02; vs 09:30 mark -6.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 194 | $6.57 | $2.61 | $-32.35 | $5,114.79 | ▼ -32.35 after sell → book $10,272.41; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 688 | $2.00 | $9.00 | $+50.92 | $6,481.79 | ▲ +50.92 after sell → book $10,263.41; vs 09:30 mark -9.00 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 273 | $4.30 | $3.58 | $-138.14 | $7,652.11 | ▼ -138.14 after sell → book $10,259.83; vs 09:30 mark -3.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 822 | $1.63 | $10.75 | $+11.53 | $8,981.22 | ▲ +11.53 after sell → book $10,249.08; vs 09:30 mark -10.75 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 113 | $11.22 | $2.36 | $-14.86 | $10,246.72 | ▼ -14.86 after sell → book $10,246.72; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,246.72 | ▲ close $10,246.72 vs 09:30 $10,288.69 (session +0.00) | 16:00 close · cash $10,246.72 · no lots left · equity $10,246.72. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,246.72 | ▲ 09:30 equity $10,246.72 vs yday $10,246.72 (+0.00) | 09:30 open · cash $10,246.72 · no holdings · equity $10,246.72 vs prior close $10,246.72 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,246.72 | ▲ close $10,246.72 vs 09:30 $10,246.72 (session +0.00) | 16:00 close · cash $10,246.72 · no lots left · equity $10,246.72. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,246.72 | ▲ 09:30 equity $10,246.72 vs yday $10,246.72 (+0.00) | 09:30 open · cash $10,246.72 · no holdings · equity $10,246.72 vs prior close $10,246.72 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,246.72 | ▲ close $10,246.72 vs 09:30 $10,246.72 (session +0.00) | 16:00 close · cash $10,246.72 · no lots left · equity $10,246.72. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,246.72 | ▲ 09:30 equity $10,246.72 vs yday $10,246.72 (+0.00) | 09:30 open · cash $10,246.72 · no holdings · equity $10,246.72 vs prior close $10,246.72 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 78 | $16.28 | $2.22 | — | $8,974.66 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=-1.1; leftover $1280.84 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 469 | $2.73 | $6.05 | — | $7,688.24 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+3.0; leftover $1280.84 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $6,445.19 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+4.4; leftover $1280.84 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $5,292.17 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,earn_react; 🔵; ⚪; ret5=+9.0; leftover $1280.84 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 8 | $157.78 | $2.01 | — | $4,027.91 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+7.8; leftover $1280.84 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 22 | $56.09 | $2.06 | — | $2,791.88 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+15.3; leftover $1280.84 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 409 | $3.13 | $5.28 | — | $1,506.43 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+6.2; leftover $1280.84 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 216 | $5.91 | $2.79 | — | $227.08 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-4.9; leftover $1280.84 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $227.08 | ▲ close $10,350.88 vs 09:30 $10,246.72 (session +128.59) | 16:00 close · cash $227.08 · equity $10,350.88 vs 09:30 $10,246.72 (+104.16; session marks +128.59) · 8 name(s) marked open→close (per-name table). AUPH×78 09:30 $16.28 → close $16.10 -14.04; OVID×469 09:30 $2.73 → close $2.69 -18.76; SANM×6 09:30 $206.84 → close $216.00 +54.96; ORCL×7 09:30 $164.43 → close $150.28 -99.05; NVT×8 09:30 $157.78 → close $162.38 +36.80; COHU×22 09:30 $56.09 → close $57.08 +21.78; CMRC×409 09:30 $3.13 → close $3.50 +153.38; DBI×216 09:30 $5.91 → close $5.88 -6.48 | — |

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

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `AUPH` | 78 | 2026-09-11 @ $16.28 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=-1.1; leftover $1280.84 |
| `OVID` | 469 | 2026-09-11 @ $2.73 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+3.0; leftover $1280.84 |
| `SANM` | 6 | 2026-09-11 @ $206.84 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+4.4; leftover $1280.84 |
| `ORCL` | 7 | 2026-09-11 @ $164.43 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,earn_react; 🔵; ⚪; ret5=+9.0; leftover $1280.84 |
| `NVT` | 8 | 2026-09-11 @ $157.78 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+7.8; leftover $1280.84 |
| `COHU` | 22 | 2026-09-11 @ $56.09 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+15.3; leftover $1280.84 |
| `CMRC` | 409 | 2026-09-11 @ $3.13 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+6.2; leftover $1280.84 |
| `DBI` | 216 | 2026-09-11 @ $5.91 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-4.9; leftover $1280.84 |
