# Factor mine action — `union_last_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ last_green, no 🚨

Cash book **+7.30%** ($10,730) · signal-only (no cash/fees) was +15.70%. Starts YES **12/21**. Fills 172 · skips 72 · realized $+474.31.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the last finished bar was green (closed up).
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
- **Gate** `last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $176.28.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 33 | — | $59.80 | +0.00 | $60.23 | +14.19 | +14.19 | +0.00 | +14.19 |
| 2026-08-13 | `IREN` | 43 | — | $45.98 | +0.00 | $44.76 | -52.46 | -52.46 | +0.00 | -52.46 |
| 2026-08-13 | `TPG` | 39 | — | $50.62 | +0.00 | $54.62 | +155.88 | +155.88 | +0.00 | +155.88 |
| 2026-08-13 | `INO` | 2469 | — | $0.81 | +0.00 | $0.90 | +222.21 | +222.21 | +0.00 | +222.21 |
| 2026-08-13 | `TNDM` | 85 | — | $23.33 | +0.00 | $23.13 | -17.00 | -17.00 | +0.00 | -17.00 |
| 2026-08-14 | `BTSG` | 33 | $60.23 | $59.65 | -19.14 | — | +0.00 | -19.14 | -4.95 | — |
| 2026-08-14 | `IREN` | 43 | $44.76 | $44.09 | -28.81 | — | +0.00 | -28.81 | -81.27 | — |
| 2026-08-14 | `TPG` | 39 | $54.62 | $55.29 | +26.13 | — | +0.00 | +26.13 | +182.01 | — |
| 2026-08-14 | `INO` | 2469 | $0.90 | $0.93 | +74.07 | — | +0.00 | +74.07 | +296.28 | — |
| 2026-08-14 | `TNDM` | 85 | $23.13 | $22.92 | -17.85 | — | +0.00 | -17.85 | -34.85 | — |
| 2026-08-14 | `VST` | 8 | — | $146.90 | +0.00 | $148.13 | +9.84 | +9.84 | +0.00 | +9.84 |
| 2026-08-14 | `DAVE` | 3 | — | $330.91 | +0.00 | $334.57 | +10.98 | +10.98 | +0.00 | +10.98 |
| 2026-08-14 | `SLG` | 22 | — | $57.61 | +0.00 | $56.09 | -33.44 | -33.44 | +0.00 | -33.44 |
| 2026-08-14 | `LDI` | 1371 | — | $0.94 | +0.00 | $0.90 | -54.84 | -54.84 | +0.00 | -54.84 |
| 2026-08-14 | `BTBT` | 856 | — | $1.50 | +0.00 | $1.57 | +59.92 | +59.92 | +0.00 | +59.92 |
| 2026-08-14 | `BETR` | 86 | — | $14.80 | +0.00 | $13.73 | -92.02 | -92.02 | +0.00 | -92.02 |
| 2026-08-14 | `ANGX` | 298 | — | $4.31 | +0.00 | $4.37 | +17.88 | +17.88 | +0.00 | +17.88 |
| 2026-08-14 | `HYLN` | 307 | — | $4.18 | +0.00 | $4.06 | -36.84 | -36.84 | +0.00 | -36.84 |
| 2026-08-17 | `VST` | 8 | $148.13 | $149.37 | +9.92 | — | +0.00 | +9.92 | +19.76 | — |
| 2026-08-17 | `DAVE` | 3 | $334.57 | $336.94 | +7.11 | — | +0.00 | +7.11 | +18.09 | — |
| 2026-08-17 | `SLG` | 22 | $56.09 | $55.37 | -15.84 | — | +0.00 | -15.84 | -49.28 | — |
| 2026-08-17 | `LDI` | 1371 | $0.90 | $0.91 | +13.71 | — | +0.00 | +13.71 | -41.13 | — |
| 2026-08-17 | `BTBT` | 856 | $1.57 | $1.52 | -42.80 | — | +0.00 | -42.80 | +17.12 | — |
| 2026-08-17 | `BETR` | 86 | $13.73 | $13.67 | -5.16 | — | +0.00 | -5.16 | -97.18 | — |
| 2026-08-17 | `ANGX` | 298 | $4.37 | $4.60 | +68.54 | — | +0.00 | +68.54 | +86.42 | — |
| 2026-08-17 | `HYLN` | 307 | $4.06 | $4.10 | +12.28 | — | +0.00 | +12.28 | -24.56 | — |
| 2026-08-17 | `DVN` | 27 | — | $46.18 | +0.00 | $47.57 | +37.53 | +37.53 | +0.00 | +37.53 |
| 2026-08-17 | `EOG` | 8 | — | $142.77 | +0.00 | $146.15 | +27.04 | +27.04 | +0.00 | +27.04 |
| 2026-08-17 | `FANG` | 6 | — | $202.70 | +0.00 | $206.29 | +21.54 | +21.54 | +0.00 | +21.54 |
| 2026-08-17 | `NB` | 249 | — | $5.07 | +0.00 | $4.81 | -64.74 | -64.74 | +0.00 | -64.74 |
| 2026-08-17 | `CDNL` | 31 | — | $39.85 | +0.00 | $39.23 | -19.22 | -19.22 | +0.00 | -19.22 |
| 2026-08-17 | `ABX` | 138 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `VERA` | 40 | — | $31.30 | +0.00 | $31.63 | +13.20 | +13.20 | +0.00 | +13.20 |
| 2026-08-17 | `CELC` | 13 | — | $92.99 | +0.00 | $92.44 | -7.15 | -7.15 | +0.00 | -7.15 |
| 2026-08-18 | `DVN` | 27 | $47.57 | $48.00 | +11.61 | — | +0.00 | +11.61 | +49.14 | — |
| 2026-08-18 | `EOG` | 8 | $146.15 | $148.04 | +15.12 | — | +0.00 | +15.12 | +42.16 | — |
| 2026-08-18 | `FANG` | 6 | $206.29 | $208.93 | +15.84 | — | +0.00 | +15.84 | +37.38 | — |
| 2026-08-18 | `NB` | 249 | $4.81 | $4.66 | -37.35 | — | +0.00 | -37.35 | -102.09 | — |
| 2026-08-18 | `CDNL` | 31 | $39.23 | $41.57 | +72.54 | — | +0.00 | +72.54 | +53.32 | — |
| 2026-08-18 | `ABX` | 138 | $9.12 | $9.03 | -12.42 | — | +0.00 | -12.42 | -12.42 | — |
| 2026-08-18 | `VERA` | 40 | $31.63 | $31.31 | -12.80 | — | +0.00 | -12.80 | +0.40 | — |
| 2026-08-18 | `CELC` | 13 | $92.44 | $92.38 | -0.78 | — | +0.00 | -0.78 | -7.93 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 61 | — | $20.55 | +0.00 | $21.19 | +39.04 | +39.04 | +0.00 | +39.04 |
| 2026-08-20 | `CDE` | 61 | — | $20.65 | +0.00 | $21.11 | +28.06 | +28.06 | +0.00 | +28.06 |
| 2026-08-20 | `HDSN` | 219 | — | $5.77 | +0.00 | $5.57 | -43.80 | -43.80 | +0.00 | -43.80 |
| 2026-08-20 | `IAG` | 64 | — | $19.63 | +0.00 | $20.50 | +55.68 | +55.68 | +0.00 | +55.68 |
| 2026-08-20 | `KGC` | 42 | — | $29.63 | +0.00 | $31.43 | +75.60 | +75.60 | +0.00 | +75.60 |
| 2026-08-20 | `NFGC` | 724 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-20 | `ABUS` | 257 | — | $4.92 | +0.00 | $4.77 | -38.55 | -38.55 | +0.00 | -38.55 |
| 2026-08-21 | `AG` | 61 | $21.19 | $21.90 | +43.31 | — | +0.00 | +43.31 | +82.35 | — |
| 2026-08-21 | `CDE` | 61 | $21.11 | $21.75 | +39.04 | — | +0.00 | +39.04 | +67.10 | — |
| 2026-08-21 | `HDSN` | 219 | $5.57 | $5.67 | +21.90 | — | +0.00 | +21.90 | -21.90 | — |
| 2026-08-21 | `IAG` | 64 | $20.50 | $21.17 | +42.88 | — | +0.00 | +42.88 | +98.56 | — |
| 2026-08-21 | `KGC` | 42 | $31.43 | $32.17 | +31.08 | — | +0.00 | +31.08 | +106.68 | — |
| 2026-08-21 | `NFGC` | 724 | $1.75 | $1.79 | +28.96 | — | +0.00 | +28.96 | +28.96 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `ABUS` | 257 | $4.77 | $5.20 | +110.51 | — | +0.00 | +110.51 | +71.96 | — |
| 2026-08-21 | `AU` | 11 | — | $119.43 | +0.00 | $121.22 | +19.69 | +19.69 | +0.00 | +19.69 |
| 2026-08-21 | `AUPH` | 77 | — | $17.20 | +0.00 | $16.65 | -42.35 | -42.35 | +0.00 | -42.35 |
| 2026-08-21 | `AEM` | 6 | — | $216.30 | +0.00 | $216.06 | -1.44 | -1.44 | +0.00 | -1.44 |
| 2026-08-21 | `ARCT` | 119 | — | $11.13 | +0.00 | $13.45 | +276.08 | +276.08 | +0.00 | +276.08 |
| 2026-08-21 | `CYPH` | 1004 | — | $1.32 | +0.00 | $1.42 | +100.40 | +100.40 | +0.00 | +100.40 |
| 2026-08-21 | `BTBT` | 798 | — | $1.66 | +0.00 | $1.53 | -103.74 | -103.74 | +0.00 | -103.74 |
| 2026-08-21 | `DE` | 2 | — | $623.26 | +0.00 | $647.47 | +48.42 | +48.42 | +0.00 | +48.42 |
| 2026-08-21 | `QDEL` | 88 | — | $14.96 | +0.00 | $14.74 | -19.36 | -19.36 | +0.00 | -19.36 |
| 2026-08-24 | `AU` | 11 | $121.22 | $120.51 | -7.81 | — | +0.00 | -7.81 | +11.88 | — |
| 2026-08-24 | `AUPH` | 77 | $16.65 | $16.57 | -6.16 | — | +0.00 | -6.16 | -48.51 | — |
| 2026-08-24 | `AEM` | 6 | $216.06 | $217.03 | +5.82 | — | +0.00 | +5.82 | +4.38 | — |
| 2026-08-24 | `ARCT` | 119 | $13.45 | $13.33 | -14.28 | — | +0.00 | -14.28 | +261.80 | — |
| 2026-08-24 | `CYPH` | 1004 | $1.42 | $1.83 | +411.64 | — | +0.00 | +411.64 | +512.04 | — |
| 2026-08-24 | `BTBT` | 798 | $1.53 | $1.55 | +15.96 | — | +0.00 | +15.96 | -87.78 | — |
| 2026-08-24 | `DE` | 2 | $647.47 | $653.04 | +11.14 | — | +0.00 | +11.14 | +59.56 | — |
| 2026-08-24 | `QDEL` | 88 | $14.74 | $14.74 | +0.00 | — | +0.00 | +0.00 | -19.36 | — |
| 2026-08-25 | `SAFX` | 3921 | — | $0.36 | +0.00 | $0.35 | -15.68 | -15.68 | +0.00 | -15.68 |
| 2026-08-25 | `VITL` | 126 | — | $11.12 | +0.00 | $11.11 | -1.26 | -1.26 | +0.00 | -1.26 |
| 2026-08-25 | `KURA` | 103 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CCOI` | 147 | — | $9.49 | +0.00 | $9.88 | +57.33 | +57.33 | +0.00 | +57.33 |
| 2026-08-25 | `LIFE` | 37 | — | $36.96 | +0.00 | $38.56 | +59.20 | +59.20 | +0.00 | +59.20 |
| 2026-08-25 | `ZIP` | 308 | — | $4.55 | +0.00 | $4.35 | -61.60 | -61.60 | +0.00 | -61.60 |
| 2026-08-25 | `ADIG` | 64 | — | $21.79 | +0.00 | $22.27 | +30.72 | +30.72 | +0.00 | +30.72 |
| 2026-08-25 | `BMEA` | 861 | — | $1.63 | +0.00 | $1.73 | +86.10 | +86.10 | +0.00 | +86.10 |
| 2026-08-26 | `SAFX` | 3921 | $0.35 | $0.35 | -3.92 | — | +0.00 | -3.92 | -19.61 | — |
| 2026-08-26 | `VITL` | 126 | $11.11 | $11.03 | -10.08 | — | +0.00 | -10.08 | -11.34 | — |
| 2026-08-26 | `KURA` | 103 | $13.59 | $13.63 | +4.12 | — | +0.00 | +4.12 | +4.12 | — |
| 2026-08-26 | `CCOI` | 147 | $9.88 | $9.89 | +1.47 | — | +0.00 | +1.47 | +58.80 | — |
| 2026-08-26 | `LIFE` | 37 | $38.56 | $38.24 | -11.84 | — | +0.00 | -11.84 | +47.36 | — |
| 2026-08-26 | `ZIP` | 308 | $4.35 | $4.31 | -12.32 | — | +0.00 | -12.32 | -73.92 | — |
| 2026-08-26 | `ADIG` | 64 | $22.27 | $21.78 | -31.36 | — | +0.00 | -31.36 | -0.64 | — |
| 2026-08-26 | `BMEA` | 861 | $1.73 | $1.75 | +21.52 | — | +0.00 | +21.52 | +107.62 | — |
| 2026-08-26 | `HCA` | 3 | — | $427.50 | +0.00 | $427.16 | -1.02 | -1.02 | +0.00 | -1.02 |
| 2026-08-26 | `MOS` | 56 | — | $24.84 | +0.00 | $24.16 | -38.08 | -38.08 | +0.00 | -38.08 |
| 2026-08-26 | `CRMD` | 163 | — | $8.60 | +0.00 | $8.39 | -34.23 | -34.23 | +0.00 | -34.23 |
| 2026-08-26 | `RZLT` | 280 | — | $5.01 | +0.00 | $5.04 | +8.40 | +8.40 | +0.00 | +8.40 |
| 2026-08-26 | `AVBP` | 45 | — | $31.21 | +0.00 | $31.14 | -3.15 | -3.15 | +0.00 | -3.15 |
| 2026-08-26 | `ABX` | 142 | — | $9.83 | +0.00 | $9.78 | -7.10 | -7.10 | +0.00 | -7.10 |
| 2026-08-26 | `ITG` | 116 | — | $12.04 | +0.00 | $12.45 | +47.56 | +47.56 | +0.00 | +47.56 |
| 2026-08-26 | `SENS` | 148 | — | $9.48 | +0.00 | $9.34 | -20.72 | -20.72 | +0.00 | -20.72 |
| 2026-08-27 | `HCA` | 3 | $427.16 | $424.61 | -7.65 | — | +0.00 | -7.65 | -8.67 | — |
| 2026-08-27 | `MOS` | 56 | $24.16 | $24.00 | -8.96 | — | +0.00 | -8.96 | -47.04 | — |
| 2026-08-27 | `CRMD` | 163 | $8.39 | $8.49 | +16.30 | — | +0.00 | +16.30 | -17.93 | — |
| 2026-08-27 | `RZLT` | 280 | $5.04 | $5.07 | +8.40 | — | +0.00 | +8.40 | +16.80 | — |
| 2026-08-27 | `AVBP` | 45 | $31.14 | $30.79 | -15.75 | — | +0.00 | -15.75 | -18.90 | — |
| 2026-08-27 | `ABX` | 142 | $9.78 | $9.68 | -14.20 | — | +0.00 | -14.20 | -21.30 | — |
| 2026-08-27 | `ITG` | 116 | $12.45 | $12.36 | -10.44 | — | +0.00 | -10.44 | +37.12 | — |
| 2026-08-27 | `SENS` | 148 | $9.34 | $9.33 | -1.48 | — | +0.00 | -1.48 | -22.20 | — |
| 2026-08-27 | `RRC` | 33 | — | $41.44 | +0.00 | $41.64 | +6.60 | +6.60 | +0.00 | +6.60 |
| 2026-08-27 | `CRK` | 96 | — | $14.42 | +0.00 | $14.62 | +19.20 | +19.20 | +0.00 | +19.20 |
| 2026-08-27 | `SLI` | 534 | — | $2.60 | +0.00 | $2.64 | +21.36 | +21.36 | +0.00 | +21.36 |
| 2026-08-27 | `GGB` | 304 | — | $4.57 | +0.00 | $4.70 | +39.52 | +39.52 | +0.00 | +39.52 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `ANET` | 6 | — | $205.90 | +0.00 | $201.09 | -28.86 | -28.86 | +0.00 | -28.86 |
| 2026-08-27 | `GEN` | 46 | — | $29.83 | +0.00 | $30.50 | +30.82 | +30.82 | +0.00 | +30.82 |
| 2026-08-27 | `MRVL` | 5 | — | $253.44 | +0.00 | $241.45 | -59.95 | -59.95 | +0.00 | -59.95 |
| 2026-08-28 | `RRC` | 33 | $41.64 | $41.74 | +3.30 | $41.46 | -9.24 | -5.94 | +9.90 | +0.66 |
| 2026-08-28 | `CRK` | 96 | $14.62 | $14.63 | +0.96 | $14.29 | -32.64 | -31.68 | +20.16 | -12.48 |
| 2026-08-28 | `SLI` | 534 | $2.64 | $2.68 | +21.36 | $2.55 | -69.42 | -48.06 | +42.72 | -26.70 |
| 2026-08-28 | `GGB` | 304 | $4.70 | $4.67 | -9.12 | — | +0.00 | -9.12 | +30.40 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `ANET` | 6 | $201.09 | $200.00 | -6.54 | — | +0.00 | -6.54 | -35.40 | — |
| 2026-08-28 | `GEN` | 46 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +30.82 | — |
| 2026-08-28 | `MRVL` | 5 | $241.45 | $225.26 | -80.95 | — | +0.00 | -80.95 | -140.90 | — |
| 2026-08-28 | `OPTX` | 157 | — | $8.61 | +0.00 | $8.52 | -14.13 | -14.13 | +0.00 | -14.13 |
| 2026-08-28 | `ANF` | 9 | — | $146.07 | +0.00 | $148.42 | +21.15 | +21.15 | +0.00 | +21.15 |
| 2026-08-28 | `CAPR` | 139 | — | $9.73 | +0.00 | $9.59 | -19.46 | -19.46 | +0.00 | -19.46 |
| 2026-08-28 | `VYX` | 148 | — | $9.13 | +0.00 | $8.78 | -51.80 | -51.80 | +0.00 | -51.80 |
| 2026-08-28 | `EQ` | 550 | — | $2.46 | +0.00 | $2.39 | -38.50 | -38.50 | +0.00 | -38.50 |
| 2026-08-31 | `RRC` | 33 | $41.46 | $42.00 | +17.82 | — | +0.00 | +17.82 | +18.48 | — |
| 2026-08-31 | `CRK` | 96 | $14.29 | $14.54 | +24.00 | — | +0.00 | +24.00 | +11.52 | — |
| 2026-08-31 | `SLI` | 534 | $2.55 | $2.58 | +16.02 | — | +0.00 | +16.02 | -10.68 | — |
| 2026-08-31 | `OPTX` | 157 | $8.52 | $8.52 | +0.00 | — | +0.00 | +0.00 | -14.13 | — |
| 2026-08-31 | `ANF` | 9 | $148.42 | $148.03 | -3.51 | — | +0.00 | -3.51 | +17.64 | — |
| 2026-08-31 | `CAPR` | 139 | $9.59 | $9.50 | -12.51 | — | +0.00 | -12.51 | -31.97 | — |
| 2026-08-31 | `VYX` | 148 | $8.78 | $8.66 | -17.76 | — | +0.00 | -17.76 | -69.56 | — |
| 2026-08-31 | `EQ` | 550 | $2.39 | $2.39 | +0.00 | — | +0.00 | +0.00 | -38.50 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 25 | — | $52.88 | +0.00 | $52.46 | -10.50 | -10.50 | +0.00 | -10.50 |
| 2026-09-03 | `HRMY` | 31 | — | $42.93 | +0.00 | $41.86 | -33.17 | -33.17 | +0.00 | -33.17 |
| 2026-09-03 | `CABA` | 370 | — | $3.63 | +0.00 | $3.48 | -55.50 | -55.50 | +0.00 | -55.50 |
| 2026-09-03 | `VSTM` | 167 | — | $8.03 | +0.00 | $7.98 | -8.35 | -8.35 | +0.00 | -8.35 |
| 2026-09-03 | `RVTY` | 10 | — | $132.45 | +0.00 | $130.63 | -18.20 | -18.20 | +0.00 | -18.20 |
| 2026-09-03 | `ARCT` | 80 | — | $16.77 | +0.00 | $15.56 | -96.80 | -96.80 | +0.00 | -96.80 |
| 2026-09-03 | `SLN` | 90 | — | $14.85 | +0.00 | $14.79 | -5.40 | -5.40 | +0.00 | -5.40 |
| 2026-09-03 | `CRDL` | 616 | — | $2.18 | +0.00 | $2.16 | -12.32 | -12.32 | +0.00 | -12.32 |
| 2026-09-04 | `ATRC` | 25 | $52.46 | $52.03 | -10.75 | — | +0.00 | -10.75 | -21.25 | — |
| 2026-09-04 | `HRMY` | 31 | $41.86 | $41.50 | -11.16 | — | +0.00 | -11.16 | -44.33 | — |
| 2026-09-04 | `CABA` | 370 | $3.48 | $3.46 | -7.40 | — | +0.00 | -7.40 | -62.90 | — |
| 2026-09-04 | `VSTM` | 167 | $7.98 | $7.91 | -11.69 | — | +0.00 | -11.69 | -20.04 | — |
| 2026-09-04 | `RVTY` | 10 | $130.63 | $130.03 | -6.00 | — | +0.00 | -6.00 | -24.20 | — |
| 2026-09-04 | `ARCT` | 80 | $15.56 | $15.61 | +4.00 | — | +0.00 | +4.00 | -92.80 | — |
| 2026-09-04 | `SLN` | 90 | $14.79 | $14.63 | -14.40 | — | +0.00 | -14.40 | -19.80 | — |
| 2026-09-04 | `CRDL` | 616 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -12.32 | — |
| 2026-09-04 | `CRM` | 4 | — | $263.36 | +0.00 | $259.23 | -16.52 | -16.52 | +0.00 | -16.52 |
| 2026-09-04 | `DELL` | 2 | — | $513.78 | +0.00 | $524.14 | +20.72 | +20.72 | +0.00 | +20.72 |
| 2026-09-04 | `SLBT` | 412 | — | $3.15 | +0.00 | $2.88 | -111.24 | -111.24 | +0.00 | -111.24 |
| 2026-09-04 | `TARS` | 15 | — | $82.70 | +0.00 | $90.78 | +121.20 | +121.20 | +0.00 | +121.20 |
| 2026-09-04 | `BRR` | 518 | — | $2.51 | +0.00 | $2.66 | +77.70 | +77.70 | +0.00 | +77.70 |
| 2026-09-04 | `FCEL` | 89 | — | $14.52 | +0.00 | $14.95 | +38.27 | +38.27 | +0.00 | +38.27 |
| 2026-09-04 | `MDB` | 3 | — | $378.34 | +0.00 | $368.74 | -28.80 | -28.80 | +0.00 | -28.80 |
| 2026-09-04 | `ASST` | 51 | — | $25.18 | +0.00 | $27.14 | +99.96 | +99.96 | +0.00 | +99.96 |
| 2026-09-08 | `CRM` | 4 | $259.23 | $253.72 | -22.04 | — | +0.00 | -22.04 | -38.56 | — |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | — | +0.00 | -5.98 | +14.74 | — |
| 2026-09-08 | `SLBT` | 412 | $2.88 | $2.88 | +0.00 | — | +0.00 | +0.00 | -111.24 | — |
| 2026-09-08 | `TARS` | 15 | $90.78 | $89.67 | -16.65 | — | +0.00 | -16.65 | +104.55 | — |
| 2026-09-08 | `BRR` | 518 | $2.66 | $2.66 | +0.00 | — | +0.00 | +0.00 | +77.70 | — |
| 2026-09-08 | `FCEL` | 89 | $14.95 | $15.18 | +20.47 | — | +0.00 | +20.47 | +58.74 | — |
| 2026-09-08 | `MDB` | 3 | $368.74 | $360.75 | -23.97 | — | +0.00 | -23.97 | -52.77 | — |
| 2026-09-08 | `ASST` | 51 | $27.14 | $26.44 | -35.70 | — | +0.00 | -35.70 | +64.26 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `SANM` | 6 | — | $206.84 | +0.00 | $216.00 | +54.96 | +54.96 | +0.00 | +54.96 |
| 2026-09-11 | `NVT` | 8 | — | $157.78 | +0.00 | $162.38 | +36.80 | +36.80 | +0.00 | +36.80 |
| 2026-09-11 | `COHU` | 23 | — | $56.09 | +0.00 | $57.08 | +22.77 | +22.77 | +0.00 | +22.77 |
| 2026-09-11 | `CMRC` | 418 | — | $3.13 | +0.00 | $3.50 | +156.75 | +156.75 | +0.00 | +156.75 |
| 2026-09-11 | `AMTX` | 641 | — | $2.04 | +0.00 | $2.01 | -19.23 | -19.23 | +0.00 | -19.23 |
| 2026-09-11 | `CLOV` | 275 | — | $4.75 | +0.00 | $4.82 | +19.25 | +19.25 | +0.00 | +19.25 |
| 2026-09-11 | `BAK` | 617 | — | $2.12 | +0.00 | $2.08 | -24.68 | -24.68 | +0.00 | -24.68 |
| 2026-09-11 | `QRVO` | 11 | — | $112.83 | +0.00 | $116.65 | +41.97 | +41.97 | +0.00 | +41.97 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +322.82 | BTSG, IREN, TPG, INO, TNDM | — | $56.25 | $10,286.85 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85 |
| 2026-08-14 | +5.50 | $56.25 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85 | $10,321.25 | +34.40 | -118.52 | VST, DAVE, SLG, LDI, BTBT, BETR, ANGX, HYLN | BTSG, IREN, TPG, INO, TNDM | $393.24 | $10,119.14 | VST×8, DAVE×3, SLG×22, LDI×1371, BTBT×856, BETR×86, ANGX×298, HYLN×307 |
| 2026-08-17 | +2.25 | $393.24 | VST×8, DAVE×3, SLG×22, LDI×1371, BTBT×856, BETR×86, ANGX×298, HYLN×307 | $10,166.90 | +47.76 | +8.20 | DVN, EOG, FANG, NB, CDNL, ABX, VERA, CELC | VST, DAVE, SLG, LDI, BTBT, BETR, ANGX, HYLN | $282.23 | $10,112.86 | DVN×27, EOG×8, FANG×6, NB×249, CDNL×31, ABX×138, VERA×40, CELC×13 |
| 2026-08-18 | -6.20 | $282.23 | DVN×27, EOG×8, FANG×6, NB×249, CDNL×31, ABX×138, VERA×40, CELC×13 | $10,164.62 | +51.76 | +0.00 | — | DVN, EOG, FANG, NB, CDNL, ABX, VERA, CELC | $10,146.49 | $10,146.49 | — |
| 2026-08-19 | -7.20 | $10,146.49 | — | $10,146.49 | -0.00 | +0.00 | — | — | $10,146.49 | $10,146.49 | — |
| 2026-08-20 | +1.12 | $10,146.49 | — | $10,146.49 | -0.00 | +161.71 | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | — | $154.98 | $10,282.06 | AG×61, CDE×61, HDSN×219, IAG×64, KGC×42, NFGC×724, WPM×8, ABUS×257 |
| 2026-08-21 | +3.25 | $154.98 | AG×61, CDE×61, HDSN×219, IAG×64, KGC×42, NFGC×724, WPM×8, ABUS×257 | $10,635.34 | +353.28 | +277.70 | AU, AUPH, AEM, ARCT, CYPH, BTBT, DE, QDEL | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | $99.41 | $10,850.47 | AU×11, AUPH×77, AEM×6, ARCT×119, CYPH×1004, BTBT×798, DE×2, QDEL×88 |
| 2026-08-24 | -5.17 | $99.41 | AU×11, AUPH×77, AEM×6, ARCT×119, CYPH×1004, BTBT×798, DE×2, QDEL×88 | $11,266.78 | +416.31 | +0.00 | — | AU, AUPH, AEM, ARCT, CYPH, BTBT, DE, QDEL | $11,230.22 | $11,230.22 | — |
| 2026-08-25 | +1.80 | $11,230.22 | — | $11,230.22 | +0.00 | +154.81 | SAFX, VITL, KURA, CCOI, LIFE, ZIP, ADIG, BMEA | — | $11.41 | $11,332.77 | SAFX×3921, VITL×126, KURA×103, CCOI×147, LIFE×37, ZIP×308, ADIG×64, BMEA×861 |
| 2026-08-26 | +2.02 | $11.41 | SAFX×3921, VITL×126, KURA×103, CCOI×147, LIFE×37, ZIP×308, ADIG×64, BMEA×861 | $11,290.36 | -42.41 | -48.34 | HCA, MOS, CRMD, RZLT, AVBP, ABX, ITG, SENS | SAFX, VITL, KURA, CCOI, LIFE, ZIP, ADIG, BMEA | $139.59 | $11,169.38 | HCA×3, MOS×56, CRMD×163, RZLT×280, AVBP×45, ABX×142, ITG×116, SENS×148 |
| 2026-08-27 | — | $139.59 | HCA×3, MOS×56, CRMD×163, RZLT×280, AVBP×45, ABX×142, ITG×116, SENS×148 | $11,135.60 | -33.78 | -2.93 | RRC, CRK, SLI, GGB, MU, ANET, GEN, MRVL | HCA, MOS, CRMD, RZLT, AVBP, ABX, ITG, SENS | $721.15 | $11,089.53 | RRC×33, CRK×96, SLI×534, GGB×304, MU×1, ANET×6, GEN×46, MRVL×5 |
| 2026-08-28 | +0.75 | $721.15 | RRC×33, CRK×96, SLI×534, GGB×304, MU×1, ANET×6, GEN×46, MRVL×5 | $11,002.44 | -87.09 | -214.04 | OPTX, ANF, CAPR, VYX, EQ | GGB, MU, ANET, GEN, MRVL | $37.70 | $10,759.79 | RRC×33, CRK×96, SLI×534, OPTX×157, ANF×9, CAPR×139, VYX×148, EQ×550 |
| 2026-08-31 | -5.85 | $37.70 | RRC×33, CRK×96, SLI×534, OPTX×157, ANF×9, CAPR×139, VYX×148, EQ×550 | $10,783.85 | +24.06 | +0.00 | — | RRC, CRK, SLI, OPTX, ANF, CAPR, VYX, EQ | $10,755.81 | $10,755.81 | — |
| 2026-09-01 | -6.30 | $10,755.81 | — | $10,755.81 | -0.00 | +0.00 | — | — | $10,755.81 | $10,755.81 | — |
| 2026-09-02 | -3.83 | $10,755.81 | — | $10,755.81 | -0.00 | +0.00 | — | — | $10,755.81 | $10,755.81 | — |
| 2026-09-03 | -0.90 | $10,755.81 | — | $10,755.81 | -0.00 | -240.24 | ATRC, HRMY, CABA, VSTM, RVTY, ARCT, SLN, CRDL | — | $47.52 | $10,489.70 | ATRC×25, HRMY×31, CABA×370, VSTM×167, RVTY×10, ARCT×80, SLN×90, CRDL×616 |
| 2026-09-04 | +2.25 | $47.52 | ATRC×25, HRMY×31, CABA×370, VSTM×167, RVTY×10, ARCT×80, SLN×90, CRDL×616 | $10,432.30 | -57.40 | +201.29 | CRM, DELL, SLBT, TARS, BRR, FCEL, MDB, ASST | ATRC, HRMY, CABA, VSTM, RVTY, ARCT, SLN, CRDL | $750.71 | $10,582.96 | CRM×4, DELL×2, SLBT×412, TARS×15, BRR×518, FCEL×89, MDB×3, ASST×51 |
| 2026-09-08 | -11.47 | $750.71 | CRM×4, DELL×2, SLBT×412, TARS×15, BRR×518, FCEL×89, MDB×3, ASST×51 | $10,499.09 | -83.87 | +0.00 | — | CRM, DELL, SLBT, TARS, BRR, FCEL, MDB, ASST | $10,474.36 | $10,474.36 | — |
| 2026-09-09 | -13.95 | $10,474.36 | — | $10,474.36 | -0.00 | +0.00 | — | — | $10,474.36 | $10,474.36 | — |
| 2026-09-10 | -13.28 | $10,474.36 | — | $10,474.36 | -0.00 | +0.00 | — | — | $10,474.36 | $10,474.36 | — |
| 2026-09-11 | +0.50 | $10,474.36 | — | $10,474.36 | -0.00 | +288.59 | SANM, NVT, COHU, CMRC, AMTX, CLOV, BAK, QRVO | — | $176.28 | $10,729.67 | SANM×6, NVT×8, COHU×23, CMRC×418, AMTX×641, CLOV×275, BAK×617, QRVO×11 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 33 | $59.80 | $2.09 | — | $8,024.51 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=-5.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 43 | $45.98 | $2.12 | — | $6,045.25 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+12.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 39 | $50.62 | $2.11 | — | $4,068.84 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+6.2; leftover $2000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 2469 | $0.81 | $27.41 | — | $2,041.54 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+13.2; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 85 | $23.33 | $2.25 | — | $56.25 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+19.7; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.25 | ▲ close $10,286.85 vs 09:30 $10,000.00 (session +322.82) | 16:00 close · cash $56.25 · equity $10,286.85 vs 09:30 $10,000.00 (+286.85; session marks +322.82) · 5 name(s) marked open→close (per-name table). BTSG×33 09:30 $59.80 → close $60.23 +14.19; IREN×43 09:30 $45.98 → close $44.76 -52.46; TPG×39 09:30 $50.62 → close $54.62 +155.88; INO×2469 09:30 $0.81 → close $0.90 +222.21; TNDM×85 09:30 $23.33 → close $23.13 -17.00 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.25 | ▲ 09:30 equity $10,321.25 vs yday $10,286.85 (+34.40) | 09:30 open · cash $56.25 (unchanged overnight, no fees) · equity $10,321.25 vs prior close $10,286.85 (+34.40) · 5 name(s) re-marked at the open (per-name table). BTSG×33 yday $60.23 → 09:30 $59.65 -19.14; IREN×43 yday $44.76 → 09:30 $44.09 -28.81; TPG×39 yday $54.62 → 09:30 $55.29 +26.13; INO×2469 yday $0.90 → 09:30 $0.93 +74.07; TNDM×85 yday $23.13 → 09:30 $22.92 -17.85 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 33 | $59.65 | $2.11 | $-9.15 | $2,022.58 | ▼ -9.15 after sell → book $10,319.13; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 43 | $44.09 | $2.14 | $-85.53 | $3,916.31 | ▼ -85.53 after sell → book $10,316.99; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 39 | $55.29 | $2.13 | $+177.76 | $6,070.49 | ▲ +177.76 after sell → book $10,314.86; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 2469 | $0.93 | $30.80 | $+238.08 | $8,335.86 | ▲ +238.08 after sell → book $10,284.06; vs 09:30 mark -30.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 85 | $22.92 | $2.27 | $-39.37 | $10,281.78 | ▼ -39.37 after sell → book $10,281.78; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $9,104.57 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+3.6; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $8,109.84 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $6,840.37 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1371 | $0.94 | $16.96 | — | $5,538.78 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 856 | $1.50 | $11.04 | — | $4,243.74 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 86 | $14.80 | $2.25 | — | $2,968.69 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 298 | $4.31 | $3.84 | — | $1,680.46 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 307 | $4.18 | $3.96 | — | $393.24 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $393.24 | ▼ close $10,119.14 vs 09:30 $10,321.25 (session -118.52) | 16:00 close · cash $393.24 · equity $10,119.14 vs 09:30 $10,321.25 (-202.11; session marks -118.52) · 8 name(s) marked open→close (per-name table). VST×8 09:30 $146.90 → close $148.13 +9.84; DAVE×3 09:30 $330.91 → close $334.57 +10.98; SLG×22 09:30 $57.61 → close $56.09 -33.44; LDI×1371 09:30 $0.94 → close $0.90 -54.84; BTBT×856 09:30 $1.50 → close $1.57 +59.92; BETR×86 09:30 $14.80 → close $13.73 -92.02; ANGX×298 09:30 $4.31 → close $4.37 +17.88; HYLN×307 09:30 $4.18 → close $4.06 -36.84 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $393.24 | ▲ 09:30 equity $10,166.90 vs yday $10,119.14 (+47.76) | 09:30 open · cash $393.24 (unchanged overnight, no fees) · equity $10,166.90 vs prior close $10,119.14 (+47.76) · 8 name(s) re-marked at the open (per-name table). VST×8 yday $148.13 → 09:30 $149.37 +9.92; DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×22 yday $56.09 → 09:30 $55.37 -15.84; LDI×1371 yday $0.90 → 09:30 $0.91 +13.71; BTBT×856 yday $1.57 → 09:30 $1.52 -42.80; BETR×86 yday $13.73 → 09:30 $13.67 -5.16; ANGX×298 yday $4.37 → 09:30 $4.60 +68.54; HYLN×307 yday $4.06 → 09:30 $4.10 +12.28 | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $1,586.17 | ▲ +15.71 after sell → book $10,164.87; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $2,594.97 | ▲ +14.07 after sell → book $10,162.85; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $3,811.04 | ▼ -53.41 after sell → book $10,160.77; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1371 | $0.91 | $16.79 | $-74.87 | $5,037.75 | ▼ -74.87 after sell → book $10,143.99; vs 09:30 mark -16.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 856 | $1.52 | $11.19 | $-5.12 | $6,327.67 | ▼ -5.12 after sell → book $10,132.79; vs 09:30 mark -11.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 86 | $13.67 | $2.27 | $-101.70 | $7,501.02 | ▼ -101.70 after sell → book $10,130.52; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 298 | $4.60 | $3.90 | $+78.67 | $8,867.91 | ▲ +78.67 after sell → book $10,126.61; vs 09:30 mark -3.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 307 | $4.10 | $4.02 | $-32.54 | $10,122.59 | ▼ -32.54 after sell → book $10,122.59; vs 09:30 mark -4.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,873.66 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+6.7; leftover $1265.32 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,729.49 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+5.8; leftover $1265.32 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,511.28 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+8.3; leftover $1265.32 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 249 | $5.07 | $3.21 | — | $5,245.64 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=-4.7; leftover $1265.32 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $4,008.21 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1265.32 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 138 | $9.12 | $2.40 | — | $2,747.24 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1265.32 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 40 | $31.30 | $2.11 | — | $1,493.13 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-3.8; leftover $1265.32 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 13 | $92.99 | $2.03 | — | $282.23 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-0.8; leftover $1265.32 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $282.23 | ▲ close $10,112.86 vs 09:30 $10,166.90 (session +8.20) | 16:00 close · cash $282.23 · equity $10,112.86 vs 09:30 $10,166.90 (-54.04; session marks +8.20) · 8 name(s) marked open→close (per-name table). DVN×27 09:30 $46.18 → close $47.57 +37.53; EOG×8 09:30 $142.77 → close $146.15 +27.04; FANG×6 09:30 $202.70 → close $206.29 +21.54; NB×249 09:30 $5.07 → close $4.81 -64.74; CDNL×31 09:30 $39.85 → close $39.23 -19.22; ABX×138 09:30 $9.12 → close $9.12 +0.00; VERA×40 09:30 $31.30 → close $31.63 +13.20; CELC×13 09:30 $92.99 → close $92.44 -7.15 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $282.23 | ▲ 09:30 equity $10,164.62 vs yday $10,112.86 (+51.76) | 09:30 open · cash $282.23 (unchanged overnight, no fees) · equity $10,164.62 vs prior close $10,112.86 (+51.76) · 8 name(s) re-marked at the open (per-name table). DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; NB×249 yday $4.81 → 09:30 $4.66 -37.35; CDNL×31 yday $39.23 → 09:30 $41.57 +72.54; ABX×138 yday $9.12 → 09:30 $9.03 -12.42; VERA×40 yday $31.63 → 09:30 $31.31 -12.80; CELC×13 yday $92.44 → 09:30 $92.38 -0.78 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,576.14 | ▲ +44.98 after sell → book $10,162.53; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,758.43 | ▲ +38.11 after sell → book $10,160.50; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $4,009.98 | ▲ +33.34 after sell → book $10,158.47; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NB` | 249 | $4.66 | $3.26 | $-108.57 | $5,167.06 | ▼ -108.57 after sell → book $10,155.21; vs 09:30 mark -3.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $6,453.62 | ▲ +49.13 after sell → book $10,153.10; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 138 | $9.03 | $2.44 | $-17.26 | $7,697.33 | ▼ -17.26 after sell → book $10,150.67; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 40 | $31.31 | $2.13 | $-3.84 | $8,947.60 | ▼ -3.84 after sell → book $10,148.54; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 13 | $92.38 | $2.05 | $-12.01 | $10,146.49 | ▼ -12.01 after sell → book $10,146.49; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,146.49 | ▲ close $10,146.49 vs 09:30 $10,164.62 (session +0.00) | 16:00 close · cash $10,146.49 · no lots left · equity $10,146.49. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,146.49 | ▲ 09:30 equity $10,146.49 vs yday $10,146.49 (-0.00) | 09:30 open · cash $10,146.49 · no holdings · equity $10,146.49 vs prior close $10,146.49 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,146.49 | ▲ close $10,146.49 vs 09:30 $10,146.49 (session +0.00) | 16:00 close · cash $10,146.49 · no lots left · equity $10,146.49. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,146.49 | ▲ 09:30 equity $10,146.49 vs yday $10,146.49 (-0.00) | 09:30 open · cash $10,146.49 · no holdings · equity $10,146.49 vs prior close $10,146.49 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,890.76 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 61 | $20.65 | $2.17 | — | $7,628.94 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 219 | $5.77 | $2.83 | — | $6,362.49 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $5,103.98 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $3,857.41 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 724 | $1.75 | $9.34 | — | $2,581.07 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,422.73 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 257 | $4.92 | $3.32 | — | $154.98 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $154.98 | ▲ close $10,282.06 vs 09:30 $10,146.49 (session +161.71) | 16:00 close · cash $154.98 · equity $10,282.06 vs 09:30 $10,146.49 (+135.57; session marks +161.71) · 8 name(s) marked open→close (per-name table). AG×61 09:30 $20.55 → close $21.19 +39.04; CDE×61 09:30 $20.65 → close $21.11 +28.06; HDSN×219 09:30 $5.77 → close $5.57 -43.80; IAG×64 09:30 $19.63 → close $20.50 +55.68; KGC×42 09:30 $29.63 → close $31.43 +75.60; NFGC×724 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68; ABUS×257 09:30 $4.92 → close $4.77 -38.55 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.98 | ▲ 09:30 equity $10,635.34 vs yday $10,282.06 (+353.28) | 09:30 open · cash $154.98 (unchanged overnight, no fees) · equity $10,635.34 vs prior close $10,282.06 (+353.28) · 8 name(s) re-marked at the open (per-name table). AG×61 yday $21.19 → 09:30 $21.90 +43.31; CDE×61 yday $21.11 → 09:30 $21.75 +39.04; HDSN×219 yday $5.57 → 09:30 $5.67 +21.90; IAG×64 yday $20.50 → 09:30 $21.17 +42.88; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×724 yday $1.75 → 09:30 $1.79 +28.96; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×257 yday $4.77 → 09:30 $5.20 +110.51 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 61 | $21.90 | $2.19 | $+77.98 | $1,488.68 | ▲ +77.98 after sell → book $10,633.14; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 61 | $21.75 | $2.19 | $+62.73 | $2,813.24 | ▲ +62.73 after sell → book $10,630.95; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 219 | $5.67 | $2.87 | $-27.60 | $4,052.10 | ▼ -27.60 after sell → book $10,628.08; vs 09:30 mark -2.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 64 | $21.17 | $2.20 | $+94.17 | $5,404.78 | ▲ +94.17 after sell → book $10,625.88; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $6,753.78 | ▲ +102.43 after sell → book $10,623.74; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 724 | $1.79 | $9.47 | $+10.15 | $8,040.27 | ▲ +10.15 after sell → book $10,614.27; vs 09:30 mark -9.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $9,275.84 | ▲ +77.23 after sell → book $10,612.24; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 257 | $5.20 | $3.37 | $+65.28 | $10,608.87 | ▲ +65.28 after sell → book $10,608.87; vs 09:30 mark -3.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 11 | $119.43 | $2.02 | — | $9,293.11 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 77 | $17.20 | $2.22 | — | $7,966.49 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,666.68 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 119 | $11.13 | $2.35 | — | $5,339.87 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 1004 | $1.32 | $12.95 | — | $4,001.64 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 798 | $1.66 | $10.29 | — | $2,666.66 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $1,418.15 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1326.11 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 88 | $14.96 | $2.25 | — | $99.41 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-1.6; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.41 | ▲ close $10,850.47 vs 09:30 $10,635.34 (session +277.70) | 16:00 close · cash $99.41 · equity $10,850.47 vs 09:30 $10,635.34 (+215.13; session marks +277.70) · 8 name(s) marked open→close (per-name table). AU×11 09:30 $119.43 → close $121.22 +19.69; AUPH×77 09:30 $17.20 → close $16.65 -42.35; AEM×6 09:30 $216.30 → close $216.06 -1.44; ARCT×119 09:30 $11.13 → close $13.45 +276.08; CYPH×1004 09:30 $1.32 → close $1.42 +100.40; BTBT×798 09:30 $1.66 → close $1.53 -103.74; DE×2 09:30 $623.26 → close $647.47 +48.42; QDEL×88 09:30 $14.96 → close $14.74 -19.36 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.41 | ▲ 09:30 equity $11,266.78 vs yday $10,850.47 (+416.31) | 09:30 open · cash $99.41 (unchanged overnight, no fees) · equity $11,266.78 vs prior close $10,850.47 (+416.31) · 8 name(s) re-marked at the open (per-name table). AU×11 yday $121.22 → 09:30 $120.51 -7.81; AUPH×77 yday $16.65 → 09:30 $16.57 -6.16; AEM×6 yday $216.06 → 09:30 $217.03 +5.82; ARCT×119 yday $13.45 → 09:30 $13.33 -14.28; CYPH×1004 yday $1.42 → 09:30 $1.83 +411.64; BTBT×798 yday $1.53 → 09:30 $1.55 +15.96; DE×2 yday $647.47 → 09:30 $653.04 +11.14; QDEL×88 yday $14.74 → 09:30 $14.74 +0.00 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 11 | $120.51 | $2.04 | $+7.81 | $1,422.98 | ▲ +7.81 after sell → book $11,264.74; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 77 | $16.57 | $2.24 | $-52.97 | $2,696.62 | ▼ -52.97 after sell → book $11,262.49; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,996.78 | ▲ +0.34 after sell → book $11,260.47; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 119 | $13.33 | $2.38 | $+257.07 | $5,580.67 | ▲ +257.07 after sell → book $11,258.09; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1004 | $1.83 | $13.13 | $+485.96 | $7,404.85 | ▲ +485.96 after sell → book $11,244.95; vs 09:30 mark -13.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 798 | $1.55 | $10.44 | $-108.51 | $8,631.32 | ▼ -108.51 after sell → book $11,234.52; vs 09:30 mark -10.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $9,935.38 | ▲ +55.55 after sell → book $11,232.50; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 88 | $14.74 | $2.28 | $-23.89 | $11,230.22 | ▼ -23.89 after sell → book $11,230.22; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,230.22 | ▲ close $11,230.22 vs 09:30 $11,266.78 (session +0.00) | 16:00 close · cash $11,230.22 · no lots left · equity $11,230.22. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,230.22 | ▲ 09:30 equity $11,230.22 vs yday $11,230.22 (+0.00) | 09:30 open · cash $11,230.22 · no holdings · equity $11,230.22 vs prior close $11,230.22 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3921 | $0.36 | $25.80 | — | $9,800.70 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-15.6; leftover $1403.78 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 126 | $11.12 | $2.37 | — | $8,397.21 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-0.7; leftover $1403.78 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 103 | $13.59 | $2.30 | — | $6,995.15 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1403.78 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 147 | $9.49 | $2.43 | — | $5,597.68 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1403.78 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 37 | $36.96 | $2.10 | — | $4,228.06 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1403.78 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 308 | $4.55 | $3.97 | — | $2,822.69 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1403.78 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ADIG` | 64 | $21.79 | $2.18 | — | $1,425.95 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable; 🔵; ret5=+3.1; leftover $1403.78 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 861 | $1.63 | $11.11 | — | $11.41 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1403.78 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.41 | ▲ close $11,332.77 vs 09:30 $11,230.22 (session +154.81) | 16:00 close · cash $11.41 · equity $11,332.77 vs 09:30 $11,230.22 (+102.55; session marks +154.81) · 8 name(s) marked open→close (per-name table). SAFX×3921 09:30 $0.36 → close $0.35 -15.68; VITL×126 09:30 $11.12 → close $11.11 -1.26; KURA×103 09:30 $13.59 → close $13.59 +0.00; CCOI×147 09:30 $9.49 → close $9.88 +57.33; LIFE×37 09:30 $36.96 → close $38.56 +59.20; ZIP×308 09:30 $4.55 → close $4.35 -61.60; ADIG×64 09:30 $21.79 → close $22.27 +30.72; BMEA×861 09:30 $1.63 → close $1.73 +86.10 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.41 | ▼ 09:30 equity $11,290.36 vs yday $11,332.77 (-42.41) | 09:30 open · cash $11.41 (unchanged overnight, no fees) · equity $11,290.36 vs prior close $11,332.77 (-42.41) · 8 name(s) re-marked at the open (per-name table). SAFX×3921 yday $0.35 → 09:30 $0.35 -3.92; VITL×126 yday $11.11 → 09:30 $11.03 -10.08; KURA×103 yday $13.59 → 09:30 $13.63 +4.12; CCOI×147 yday $9.88 → 09:30 $9.89 +1.47; LIFE×37 yday $38.56 → 09:30 $38.24 -11.84; ZIP×308 yday $4.35 → 09:30 $4.31 -12.32; ADIG×64 yday $22.27 → 09:30 $21.78 -31.36; BMEA×861 yday $1.73 → 09:30 $1.75 +21.52 | — |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 3921 | $0.35 | $26.27 | $-71.67 | $1,369.26 | ▼ -71.67 after sell → book $11,264.09; vs 09:30 mark -26.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `VITL` | 126 | $11.03 | $2.40 | $-16.11 | $2,756.64 | ▼ -16.11 after sell → book $11,261.69; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 103 | $13.63 | $2.33 | $-0.51 | $4,158.20 | ▼ -0.51 after sell → book $11,259.37; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 147 | $9.89 | $2.47 | $+53.90 | $5,609.56 | ▲ +53.90 after sell → book $11,256.90; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 37 | $38.24 | $2.12 | $+43.14 | $7,022.32 | ▲ +43.14 after sell → book $11,254.78; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 308 | $4.31 | $4.03 | $-81.93 | $8,345.77 | ▼ -81.93 after sell → book $11,250.74; vs 09:30 mark -4.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ADIG` | 64 | $21.78 | $2.20 | $-5.03 | $9,737.48 | ▼ -5.03 after sell → book $11,248.54; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 861 | $1.75 | $11.26 | $+85.26 | $11,237.28 | ▲ +85.26 after sell → book $11,237.28; vs 09:30 mark -11.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `HCA` | 3 | $427.50 | $2.00 | — | $9,952.78 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+4.1; leftover $1404.66 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `MOS` | 56 | $24.84 | $2.16 | — | $8,559.58 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+14.8; leftover $1404.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `CRMD` | 163 | $8.60 | $2.48 | — | $7,155.30 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+4.8; leftover $1404.66 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `RZLT` | 280 | $5.01 | $3.61 | — | $5,748.89 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,yday_gainer; 🔵; ret5=+7.5; leftover $1404.66 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 45 | $31.21 | $2.12 | — | $4,342.31 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1404.66 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 142 | $9.83 | $2.42 | — | $2,944.04 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1404.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ITG` | 116 | $12.04 | $2.34 | — | $1,545.06 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-5.1; leftover $1404.66 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SENS` | 148 | $9.48 | $2.43 | — | $139.59 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $1404.66 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $139.59 | ▼ close $11,169.38 vs 09:30 $11,290.36 (session -48.34) | 16:00 close · cash $139.59 · equity $11,169.38 vs 09:30 $11,290.36 (-120.98; session marks -48.34) · 8 name(s) marked open→close (per-name table). HCA×3 09:30 $427.50 → close $427.16 -1.02; MOS×56 09:30 $24.84 → close $24.16 -38.08; CRMD×163 09:30 $8.60 → close $8.39 -34.23; RZLT×280 09:30 $5.01 → close $5.04 +8.40; AVBP×45 09:30 $31.21 → close $31.14 -3.15; ABX×142 09:30 $9.83 → close $9.78 -7.10; ITG×116 09:30 $12.04 → close $12.45 +47.56; SENS×148 09:30 $9.48 → close $9.34 -20.72 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $139.59 | ▼ 09:30 equity $11,135.60 vs yday $11,169.38 (-33.78) | 09:30 open · cash $139.59 (unchanged overnight, no fees) · equity $11,135.60 vs prior close $11,169.38 (-33.78) · 8 name(s) re-marked at the open (per-name table). HCA×3 yday $427.16 → 09:30 $424.61 -7.65; MOS×56 yday $24.16 → 09:30 $24.00 -8.96; CRMD×163 yday $8.39 → 09:30 $8.49 +16.30; RZLT×280 yday $5.04 → 09:30 $5.07 +8.40; AVBP×45 yday $31.14 → 09:30 $30.79 -15.75; ABX×142 yday $9.78 → 09:30 $9.68 -14.20; ITG×116 yday $12.45 → 09:30 $12.36 -10.44; SENS×148 yday $9.34 → 09:30 $9.33 -1.48 | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-12.69 | $1,411.40 | ▼ -12.69 after sell → book $11,133.58; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MOS` | 56 | $24.00 | $2.18 | $-51.38 | $2,753.22 | ▼ -51.38 after sell → book $11,131.40; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 163 | $8.49 | $2.52 | $-22.93 | $4,134.57 | ▼ -22.93 after sell → book $11,128.88; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 280 | $5.07 | $3.67 | $+9.52 | $5,550.50 | ▲ +9.52 after sell → book $11,125.21; vs 09:30 mark -3.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 45 | $30.79 | $2.15 | $-23.17 | $6,933.90 | ▼ -23.17 after sell → book $11,123.06; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ABX` | 142 | $9.68 | $2.45 | $-26.17 | $8,306.01 | ▼ -26.17 after sell → book $11,120.61; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ITG` | 116 | $12.36 | $2.37 | $+32.41 | $9,737.41 | ▲ +32.41 after sell → book $11,118.25; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SENS` | 148 | $9.33 | $2.47 | $-27.10 | $11,115.78 | ▼ -27.10 after sell → book $11,115.78; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 33 | $41.44 | $2.09 | — | $9,746.17 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+3.1; leftover $1389.47 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 96 | $14.42 | $2.28 | — | $8,359.57 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+7.1; leftover $1389.47 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 534 | $2.60 | $6.89 | — | $6,964.28 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+13.0; leftover $1389.47 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 304 | $4.57 | $3.92 | — | $5,571.08 | — | union ∩ last_green, no 🚨; gate last_green=True; list mover_buy; 🔵; ret5=+1.1; leftover $1389.47 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $4,602.08 | — | union ∩ last_green, no 🚨; gate last_green=True; list mover_buy; 🔵; ret5=+0.1; leftover $1389.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $205.90 | $2.01 | — | $3,364.67 | — | union ∩ last_green, no 🚨; gate last_green=True; list mover_buy; 🔵; ret5=+8.5; leftover $1389.47 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 46 | $29.83 | $2.13 | — | $1,990.36 | — | union ∩ last_green, no 🚨; gate last_green=True; list mover_buy; 🔵; ret5=+7.6; leftover $1389.47 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $253.44 | $2.00 | — | $721.15 | — | union ∩ last_green, no 🚨; gate last_green=True; list mover_buy; 🔵; ret5=+3.3; leftover $1389.47 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $721.15 | ▼ close $11,089.53 vs 09:30 $11,135.60 (session -2.93) | 16:00 close · cash $721.15 · equity $11,089.53 vs 09:30 $11,135.60 (-46.07; session marks -2.93) · 8 name(s) marked open→close (per-name table). RRC×33 09:30 $41.44 → close $41.64 +6.60; CRK×96 09:30 $14.42 → close $14.62 +19.20; SLI×534 09:30 $2.60 → close $2.64 +21.36; GGB×304 09:30 $4.57 → close $4.70 +39.52; MU×1 09:30 $967.01 → close $935.39 -31.62; ANET×6 09:30 $205.90 → close $201.09 -28.86; GEN×46 09:30 $29.83 → close $30.50 +30.82; MRVL×5 09:30 $253.44 → close $241.45 -59.95 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $721.15 | ▼ 09:30 equity $11,002.44 vs yday $11,089.53 (-87.09) | 09:30 open · cash $721.15 (unchanged overnight, no fees) · equity $11,002.44 vs prior close $11,089.53 (-87.09) · 8 name(s) re-marked at the open (per-name table). RRC×33 yday $41.64 → 09:30 $41.74 +3.30; CRK×96 yday $14.62 → 09:30 $14.63 +0.96; SLI×534 yday $2.64 → 09:30 $2.68 +21.36; GGB×304 yday $4.70 → 09:30 $4.67 -9.12; MU×1 yday $935.39 → 09:30 $919.29 -16.10; ANET×6 yday $201.09 → 09:30 $200.00 -6.54; GEN×46 yday $30.50 → 09:30 $30.50 +0.00; MRVL×5 yday $241.45 → 09:30 $225.26 -80.95 | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 304 | $4.67 | $3.98 | $+22.49 | $2,136.85 | ▲ +22.49 after sell → book $10,998.46; vs 09:30 mark -3.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $3,054.13 | ▼ -51.73 after sell → book $10,996.45; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $200.00 | $2.03 | $-39.44 | $4,252.10 | ▼ -39.44 after sell → book $10,994.42; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 46 | $30.50 | $2.15 | $+26.54 | $5,652.95 | ▲ +26.54 after sell → book $10,992.27; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $225.26 | $2.02 | $-144.93 | $6,777.23 | ▼ -144.93 after sell → book $10,990.25; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 157 | $8.61 | $2.46 | — | $5,422.99 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-0.7; leftover $1355.45 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $4,106.35 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1355.45 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 139 | $9.73 | $2.41 | — | $2,751.47 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer,yday_mover; ret5=+47.1; leftover $1355.45 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 148 | $9.13 | $2.43 | — | $1,397.80 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer; 🔵; ret5=+20.0; leftover $1355.45 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `EQ` | 550 | $2.46 | $7.09 | — | $37.70 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer; ret5=+7.9; leftover $1355.45 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.70 | ▼ close $10,759.79 vs 09:30 $11,002.44 (session -214.04) | 16:00 close · cash $37.70 · equity $10,759.79 vs 09:30 $11,002.44 (-242.65; session marks -214.04) · 8 name(s) marked open→close (per-name table). RRC×33 09:30 $41.74 → close $41.46 -9.24; CRK×96 09:30 $14.63 → close $14.29 -32.64; SLI×534 09:30 $2.68 → close $2.55 -69.42; OPTX×157 09:30 $8.61 → close $8.52 -14.13; ANF×9 09:30 $146.07 → close $148.42 +21.15; CAPR×139 09:30 $9.73 → close $9.59 -19.46; VYX×148 09:30 $9.13 → close $8.78 -51.80; EQ×550 09:30 $2.46 → close $2.39 -38.50 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.70 | ▲ 09:30 equity $10,783.85 vs yday $10,759.79 (+24.06) | 09:30 open · cash $37.70 (unchanged overnight, no fees) · equity $10,783.85 vs prior close $10,759.79 (+24.06) · 8 name(s) re-marked at the open (per-name table). RRC×33 yday $41.46 → 09:30 $42.00 +17.82; CRK×96 yday $14.29 → 09:30 $14.54 +24.00; SLI×534 yday $2.55 → 09:30 $2.58 +16.02; OPTX×157 yday $8.52 → 09:30 $8.52 +0.00; ANF×9 yday $148.42 → 09:30 $148.03 -3.51; CAPR×139 yday $9.59 → 09:30 $9.50 -12.51; VYX×148 yday $8.78 → 09:30 $8.66 -17.76; EQ×550 yday $2.39 → 09:30 $2.39 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 33 | $42.00 | $2.11 | $+14.28 | $1,421.59 | ▲ +14.28 after sell → book $10,781.74; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 96 | $14.54 | $2.31 | $+6.94 | $2,815.13 | ▲ +6.94 after sell → book $10,779.44; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 534 | $2.58 | $6.99 | $-24.56 | $4,185.86 | ▼ -24.56 after sell → book $10,772.45; vs 09:30 mark -6.99 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 157 | $8.52 | $2.50 | $-19.09 | $5,521.00 | ▼ -19.09 after sell → book $10,769.95; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $6,851.23 | ▲ +13.59 after sell → book $10,767.91; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 139 | $9.50 | $2.44 | $-36.82 | $8,169.29 | ▼ -36.82 after sell → book $10,765.47; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 148 | $8.66 | $2.47 | $-74.46 | $9,448.50 | ▼ -74.46 after sell → book $10,763.00; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `EQ` | 550 | $2.39 | $7.20 | $-52.79 | $10,755.81 | ▼ -52.79 after sell → book $10,755.81; vs 09:30 mark -7.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,755.81 | ▲ close $10,755.81 vs 09:30 $10,783.85 (session +0.00) | 16:00 close · cash $10,755.81 · no lots left · equity $10,755.81. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,755.81 | ▲ 09:30 equity $10,755.81 vs yday $10,755.81 (-0.00) | 09:30 open · cash $10,755.81 · no holdings · equity $10,755.81 vs prior close $10,755.81 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,755.81 | ▲ close $10,755.81 vs 09:30 $10,755.81 (session +0.00) | 16:00 close · cash $10,755.81 · no lots left · equity $10,755.81. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,755.81 | ▲ 09:30 equity $10,755.81 vs yday $10,755.81 (-0.00) | 09:30 open · cash $10,755.81 · no holdings · equity $10,755.81 vs prior close $10,755.81 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,755.81 | ▲ close $10,755.81 vs 09:30 $10,755.81 (session +0.00) | 16:00 close · cash $10,755.81 · no lots left · equity $10,755.81. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,755.81 | ▲ 09:30 equity $10,755.81 vs yday $10,755.81 (-0.00) | 09:30 open · cash $10,755.81 · no holdings · equity $10,755.81 vs prior close $10,755.81 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,431.74 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1344.48 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $42.93 | $2.08 | — | $8,098.83 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1344.48 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 370 | $3.63 | $4.77 | — | $6,750.96 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1344.48 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 167 | $8.03 | $2.49 | — | $5,407.45 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1344.48 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,080.93 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1344.48 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 80 | $16.77 | $2.23 | — | $2,737.10 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1344.48 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 90 | $14.85 | $2.26 | — | $1,398.34 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1344.48 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 616 | $2.18 | $7.95 | — | $47.52 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1344.48 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.52 | ▼ close $10,489.70 vs 09:30 $10,755.81 (session -240.24) | 16:00 close · cash $47.52 · equity $10,489.70 vs 09:30 $10,755.81 (-266.11; session marks -240.24) · 8 name(s) marked open→close (per-name table). ATRC×25 09:30 $52.88 → close $52.46 -10.50; HRMY×31 09:30 $42.93 → close $41.86 -33.17; CABA×370 09:30 $3.63 → close $3.48 -55.50; VSTM×167 09:30 $8.03 → close $7.98 -8.35; RVTY×10 09:30 $132.45 → close $130.63 -18.20; ARCT×80 09:30 $16.77 → close $15.56 -96.80; SLN×90 09:30 $14.85 → close $14.79 -5.40; CRDL×616 09:30 $2.18 → close $2.16 -12.32 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.52 | ▼ 09:30 equity $10,432.30 vs yday $10,489.70 (-57.40) | 09:30 open · cash $47.52 (unchanged overnight, no fees) · equity $10,432.30 vs prior close $10,489.70 (-57.40) · 8 name(s) re-marked at the open (per-name table). ATRC×25 yday $52.46 → 09:30 $52.03 -10.75; HRMY×31 yday $41.86 → 09:30 $41.50 -11.16; CABA×370 yday $3.48 → 09:30 $3.46 -7.40; VSTM×167 yday $7.98 → 09:30 $7.91 -11.69; RVTY×10 yday $130.63 → 09:30 $130.03 -6.00; ARCT×80 yday $15.56 → 09:30 $15.61 +4.00; SLN×90 yday $14.79 → 09:30 $14.63 -14.40; CRDL×616 yday $2.16 → 09:30 $2.16 +0.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 25 | $52.03 | $2.09 | $-25.40 | $1,346.18 | ▼ -25.40 after sell → book $10,430.21; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 31 | $41.50 | $2.10 | $-48.52 | $2,630.58 | ▼ -48.52 after sell → book $10,428.11; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CABA` | 370 | $3.46 | $4.84 | $-72.52 | $3,905.93 | ▼ -72.52 after sell → book $10,423.26; vs 09:30 mark -4.85 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 167 | $7.91 | $2.53 | $-25.06 | $5,224.38 | ▼ -25.06 after sell → book $10,420.74; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $6,522.64 | ▼ -28.26 after sell → book $10,418.69; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 80 | $15.61 | $2.25 | $-97.28 | $7,769.18 | ▼ -97.28 after sell → book $10,416.44; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 90 | $14.63 | $2.29 | $-24.35 | $9,083.60 | ▼ -24.35 after sell → book $10,414.16; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 616 | $2.16 | $8.06 | $-28.33 | $10,406.10 | ▼ -28.33 after sell → book $10,406.10; vs 09:30 mark -8.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $9,350.65 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1300.76 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $8,321.10 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1300.76 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 412 | $3.15 | $5.31 | — | $7,017.98 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+19.3; leftover $1300.76 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 15 | $82.70 | $2.04 | — | $5,775.45 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1300.76 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 518 | $2.51 | $6.68 | — | $4,468.59 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $1300.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `FCEL` | 89 | $14.52 | $2.26 | — | $3,174.05 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_mover; ret5=-24.1; leftover $1300.76 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MDB` | 3 | $378.34 | $2.00 | — | $2,037.03 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_mover; ret5=-12.7; leftover $1300.76 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 51 | $25.18 | $2.14 | — | $750.71 | — | union ∩ last_green, no 🚨; gate last_green=True; list ohlc_hot; 🔵; ret5=+16.0; leftover $1300.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $750.71 | ▲ close $10,582.96 vs 09:30 $10,432.30 (session +201.29) | 16:00 close · cash $750.71 · equity $10,582.96 vs 09:30 $10,432.30 (+150.66; session marks +201.29) · 8 name(s) marked open→close (per-name table). CRM×4 09:30 $263.36 → close $259.23 -16.52; DELL×2 09:30 $513.78 → close $524.14 +20.72; SLBT×412 09:30 $3.15 → close $2.88 -111.24; TARS×15 09:30 $82.70 → close $90.78 +121.20; BRR×518 09:30 $2.51 → close $2.66 +77.70; FCEL×89 09:30 $14.52 → close $14.95 +38.27; MDB×3 09:30 $378.34 → close $368.74 -28.80; ASST×51 09:30 $25.18 → close $27.14 +99.96 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $750.71 | ▼ 09:30 equity $10,499.09 vs yday $10,582.96 (-83.87) | 09:30 open · cash $750.71 (unchanged overnight, no fees) · equity $10,499.09 vs prior close $10,582.96 (-83.87) · 8 name(s) re-marked at the open (per-name table). CRM×4 yday $259.23 → 09:30 $253.72 -22.04; DELL×2 yday $524.14 → 09:30 $521.15 -5.98; SLBT×412 yday $2.88 → 09:30 $2.88 +0.00; TARS×15 yday $90.78 → 09:30 $89.67 -16.65; BRR×518 yday $2.66 → 09:30 $2.66 +0.00; FCEL×89 yday $14.95 → 09:30 $15.18 +20.47; MDB×3 yday $368.74 → 09:30 $360.75 -23.97; ASST×51 yday $27.14 → 09:30 $26.44 -35.70 | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $1,763.57 | ▼ -42.58 after sell → book $10,497.07; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $2,803.85 | ▲ +10.73 after sell → book $10,495.05; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `SLBT` | 412 | $2.88 | $5.39 | $-121.95 | $3,985.02 | ▼ -121.95 after sell → book $10,489.66; vs 09:30 mark -5.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 15 | $89.67 | $2.06 | $+100.46 | $5,328.01 | ▲ +100.46 after sell → book $10,487.60; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 518 | $2.66 | $6.78 | $+64.24 | $6,699.11 | ▲ +64.24 after sell → book $10,480.82; vs 09:30 mark -6.78 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `FCEL` | 89 | $15.18 | $2.28 | $+54.20 | $8,047.85 | ▲ +54.20 after sell → book $10,478.54; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MDB` | 3 | $360.75 | $2.02 | $-56.79 | $9,128.08 | ▼ -56.79 after sell → book $10,476.52; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 51 | $26.44 | $2.16 | $+59.95 | $10,474.36 | ▲ +59.95 after sell → book $10,474.36; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,474.36 | ▲ close $10,474.36 vs 09:30 $10,499.09 (session +0.00) | 16:00 close · cash $10,474.36 · no lots left · equity $10,474.36. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,474.36 | ▲ 09:30 equity $10,474.36 vs yday $10,474.36 (-0.00) | 09:30 open · cash $10,474.36 · no holdings · equity $10,474.36 vs prior close $10,474.36 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,474.36 | ▲ close $10,474.36 vs 09:30 $10,474.36 (session +0.00) | 16:00 close · cash $10,474.36 · no lots left · equity $10,474.36. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,474.36 | ▲ 09:30 equity $10,474.36 vs yday $10,474.36 (-0.00) | 09:30 open · cash $10,474.36 · no holdings · equity $10,474.36 vs prior close $10,474.36 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,474.36 | ▲ close $10,474.36 vs 09:30 $10,474.36 (session +0.00) | 16:00 close · cash $10,474.36 · no lots left · equity $10,474.36. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,474.36 | ▲ 09:30 equity $10,474.36 vs yday $10,474.36 (-0.00) | 09:30 open · cash $10,474.36 · no holdings · equity $10,474.36 vs prior close $10,474.36 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $9,231.31 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+4.4; leftover $1309.29 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 8 | $157.78 | $2.01 | — | $7,967.05 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+7.8; leftover $1309.29 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 23 | $56.09 | $2.06 | — | $6,674.93 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+15.3; leftover $1309.29 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 418 | $3.13 | $5.39 | — | $5,361.19 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+6.2; leftover $1309.29 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 641 | $2.04 | $8.27 | — | $4,045.28 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.8; leftover $1309.29 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 275 | $4.75 | $3.55 | — | $2,735.49 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+3.4; leftover $1309.29 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 617 | $2.12 | $7.96 | — | $1,419.49 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1309.29 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `QRVO` | 11 | $112.83 | $2.02 | — | $176.28 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1309.29 | join🟢 sector🟢 gen🟡 news🔴 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $176.28 | ▲ close $10,729.67 vs 09:30 $10,474.36 (session +288.59) | 16:00 close · cash $176.28 · equity $10,729.67 vs 09:30 $10,474.36 (+255.31; session marks +288.59) · 8 name(s) marked open→close (per-name table). SANM×6 09:30 $206.84 → close $216.00 +54.96; NVT×8 09:30 $157.78 → close $162.38 +36.80; COHU×23 09:30 $56.09 → close $57.08 +22.77; CMRC×418 09:30 $3.13 → close $3.50 +156.75; AMTX×641 09:30 $2.04 → close $2.01 -19.23; CLOV×275 09:30 $4.75 → close $4.82 +19.25; BAK×617 09:30 $2.12 → close $2.08 -24.68; QRVO×11 09:30 $112.83 → close $116.65 +41.97 | — |

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
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WEN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BTBT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SSL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `WDS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HELP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XHLD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HELP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `SANM` | 6 | 2026-09-11 @ $206.84 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+4.4; leftover $1309.29 |
| `NVT` | 8 | 2026-09-11 @ $157.78 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+7.8; leftover $1309.29 |
| `COHU` | 23 | 2026-09-11 @ $56.09 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+15.3; leftover $1309.29 |
| `CMRC` | 418 | 2026-09-11 @ $3.13 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+6.2; leftover $1309.29 |
| `AMTX` | 641 | 2026-09-11 @ $2.04 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.8; leftover $1309.29 |
| `CLOV` | 275 | 2026-09-11 @ $4.75 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+3.4; leftover $1309.29 |
| `BAK` | 617 | 2026-09-11 @ $2.12 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1309.29 |
| `QRVO` | 11 | 2026-09-11 @ $112.83 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1309.29 |
