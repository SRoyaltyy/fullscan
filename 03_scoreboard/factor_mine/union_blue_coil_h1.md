# Factor mine action — `union_blue_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-6.29%** ($9,371) · signal-only (no cash/fees) was -6.33%. Starts YES **0/18**. Fills 148 · skips 36 · realized $-629.03.

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
- Must-have: prior 5-session return is at most 10% (not already exploded).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

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
- **Gate** `blue=True,ret_5_max=10.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,371.00.

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
| 2026-08-17 | `INV` | 772 | — | $1.62 | +0.00 | $1.39 | -181.42 | -181.42 | +0.00 | -181.42 |
| 2026-08-18 | `DVN` | 27 | $47.57 | $48.00 | +11.61 | — | +0.00 | +11.61 | +49.14 | — |
| 2026-08-18 | `EOG` | 8 | $146.15 | $148.04 | +15.12 | — | +0.00 | +15.12 | +42.16 | — |
| 2026-08-18 | `FANG` | 6 | $206.29 | $208.93 | +15.84 | — | +0.00 | +15.84 | +37.38 | — |
| 2026-08-18 | `TMC` | 309 | $3.77 | $3.72 | -15.45 | — | +0.00 | -15.45 | -101.97 | — |
| 2026-08-18 | `TGB` | 147 | $8.77 | $8.55 | -32.34 | — | +0.00 | -32.34 | +13.23 | — |
| 2026-08-18 | `ABX` | 137 | $9.12 | $9.03 | -12.33 | — | +0.00 | -12.33 | -12.33 | — |
| 2026-08-18 | `ALM` | 77 | $16.36 | $15.78 | -44.66 | — | +0.00 | -44.66 | -32.34 | — |
| 2026-08-18 | `INV` | 772 | $1.39 | $1.32 | -46.32 | — | +0.00 | -46.32 | -227.74 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 59 | — | $20.55 | +0.00 | $21.19 | +37.76 | +37.76 | +0.00 | +37.76 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `HDSN` | 210 | — | $5.77 | +0.00 | $5.57 | -42.00 | -42.00 | +0.00 | -42.00 |
| 2026-08-20 | `IAG` | 61 | — | $19.63 | +0.00 | $20.50 | +53.07 | +53.07 | +0.00 | +53.07 |
| 2026-08-20 | `KGC` | 41 | — | $29.63 | +0.00 | $31.43 | +73.80 | +73.80 | +0.00 | +73.80 |
| 2026-08-20 | `NFGC` | 694 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-20 | `ABUS` | 247 | — | $4.92 | +0.00 | $4.77 | -37.05 | -37.05 | +0.00 | -37.05 |
| 2026-08-21 | `AG` | 59 | $21.19 | $21.90 | +41.89 | — | +0.00 | +41.89 | +79.65 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `HDSN` | 210 | $5.57 | $5.67 | +21.00 | — | +0.00 | +21.00 | -21.00 | — |
| 2026-08-21 | `IAG` | 61 | $20.50 | $21.17 | +40.87 | — | +0.00 | +40.87 | +93.94 | — |
| 2026-08-21 | `KGC` | 41 | $31.43 | $32.17 | +30.34 | — | +0.00 | +30.34 | +104.14 | — |
| 2026-08-21 | `NFGC` | 694 | $1.75 | $1.79 | +27.76 | — | +0.00 | +27.76 | +27.76 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `ABUS` | 247 | $4.77 | $5.20 | +106.21 | — | +0.00 | +106.21 | +69.16 | — |
| 2026-08-21 | `CRSP` | 21 | — | $59.72 | +0.00 | $59.50 | -4.62 | -4.62 | +0.00 | -4.62 |
| 2026-08-21 | `FUTU` | 11 | — | $115.18 | +0.00 | $123.64 | +93.06 | +93.06 | +0.00 | +93.06 |
| 2026-08-21 | `GMAB` | 38 | — | $33.36 | +0.00 | $33.45 | +3.42 | +3.42 | +0.00 | +3.42 |
| 2026-08-21 | `BTBT` | 766 | — | $1.66 | +0.00 | $1.53 | -99.58 | -99.58 | +0.00 | -99.58 |
| 2026-08-21 | `DE` | 2 | — | $623.26 | +0.00 | $647.47 | +48.42 | +48.42 | +0.00 | +48.42 |
| 2026-08-21 | `CF` | 9 | — | $127.43 | +0.00 | $129.60 | +19.53 | +19.53 | +0.00 | +19.53 |
| 2026-08-21 | `WOLF` | 47 | — | $26.86 | +0.00 | $25.76 | -51.70 | -51.70 | +0.00 | -51.70 |
| 2026-08-21 | `AMRC` | 56 | — | $22.51 | +0.00 | $21.38 | -63.28 | -63.28 | +0.00 | -63.28 |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | — | +0.00 | -15.75 | -20.37 | — |
| 2026-08-24 | `FUTU` | 11 | $123.64 | $121.00 | -29.04 | — | +0.00 | -29.04 | +64.02 | — |
| 2026-08-24 | `GMAB` | 38 | $33.45 | $32.82 | -23.94 | — | +0.00 | -23.94 | -20.52 | — |
| 2026-08-24 | `BTBT` | 766 | $1.53 | $1.55 | +15.32 | — | +0.00 | +15.32 | -84.26 | — |
| 2026-08-24 | `DE` | 2 | $647.47 | $653.04 | +11.14 | — | +0.00 | +11.14 | +59.56 | — |
| 2026-08-24 | `CF` | 9 | $129.60 | $129.99 | +3.51 | — | +0.00 | +3.51 | +23.04 | — |
| 2026-08-24 | `WOLF` | 47 | $25.76 | $25.00 | -35.72 | — | +0.00 | -35.72 | -87.42 | — |
| 2026-08-24 | `AMRC` | 56 | $21.38 | $21.19 | -10.64 | — | +0.00 | -10.64 | -73.92 | — |
| 2026-08-25 | `OCUL` | 113 | — | $10.98 | +0.00 | $10.88 | -11.30 | -11.30 | +0.00 | -11.30 |
| 2026-08-25 | `INSP` | 20 | — | $61.19 | +0.00 | $61.07 | -2.40 | -2.40 | +0.00 | -2.40 |
| 2026-08-25 | `CRMD` | 149 | — | $8.35 | +0.00 | $8.56 | +31.29 | +31.29 | +0.00 | +31.29 |
| 2026-08-25 | `CAPR` | 172 | — | $7.25 | +0.00 | $8.29 | +178.88 | +178.88 | +0.00 | +178.88 |
| 2026-08-25 | `KURA` | 91 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CCOI` | 131 | — | $9.49 | +0.00 | $9.88 | +51.09 | +51.09 | +0.00 | +51.09 |
| 2026-08-25 | `LIFE` | 33 | — | $36.96 | +0.00 | $38.56 | +52.80 | +52.80 | +0.00 | +52.80 |
| 2026-08-25 | `ZIP` | 274 | — | $4.55 | +0.00 | $4.35 | -54.80 | -54.80 | +0.00 | -54.80 |
| 2026-08-26 | `OCUL` | 113 | $10.88 | $10.79 | -10.17 | $10.77 | -2.26 | -12.43 | -21.47 | -23.73 |
| 2026-08-26 | `INSP` | 20 | $61.07 | $60.07 | -20.00 | — | +0.00 | -20.00 | -22.40 | — |
| 2026-08-26 | `CRMD` | 149 | $8.56 | $8.60 | +5.96 | $8.39 | -31.29 | -25.33 | +37.25 | +5.96 |
| 2026-08-26 | `CAPR` | 172 | $8.29 | $8.29 | +0.00 | — | +0.00 | +0.00 | +178.88 | — |
| 2026-08-26 | `KURA` | 91 | $13.59 | $13.63 | +3.64 | — | +0.00 | +3.64 | +3.64 | — |
| 2026-08-26 | `CCOI` | 131 | $9.88 | $9.89 | +1.31 | — | +0.00 | +1.31 | +52.40 | — |
| 2026-08-26 | `LIFE` | 33 | $38.56 | $38.24 | -10.56 | — | +0.00 | -10.56 | +42.24 | — |
| 2026-08-26 | `ZIP` | 274 | $4.35 | $4.31 | -10.96 | — | +0.00 | -10.96 | -65.76 | — |
| 2026-08-26 | `RZLT` | 254 | — | $5.01 | +0.00 | $5.04 | +7.62 | +7.62 | +0.00 | +7.62 |
| 2026-08-26 | `AVBP` | 40 | — | $31.21 | +0.00 | $31.14 | -2.80 | -2.80 | +0.00 | -2.80 |
| 2026-08-26 | `FLNC` | 114 | — | $11.12 | +0.00 | $11.08 | -4.56 | -4.56 | +0.00 | -4.56 |
| 2026-08-26 | `ABX` | 129 | — | $9.83 | +0.00 | $9.78 | -6.45 | -6.45 | +0.00 | -6.45 |
| 2026-08-26 | `AVEX` | 72 | — | $17.51 | +0.00 | $18.34 | +59.76 | +59.76 | +0.00 | +59.76 |
| 2026-08-26 | `AXTI` | 19 | — | $65.34 | +0.00 | $65.18 | -3.04 | -3.04 | +0.00 | -3.04 |
| 2026-08-27 | `OCUL` | 113 | $10.77 | $10.63 | -15.82 | — | +0.00 | -15.82 | -39.55 | — |
| 2026-08-27 | `CRMD` | 149 | $8.39 | $8.49 | +14.90 | — | +0.00 | +14.90 | +20.86 | — |
| 2026-08-27 | `RZLT` | 254 | $5.04 | $5.07 | +7.62 | — | +0.00 | +7.62 | +15.24 | — |
| 2026-08-27 | `AVBP` | 40 | $31.14 | $30.79 | -14.00 | — | +0.00 | -14.00 | -16.80 | — |
| 2026-08-27 | `FLNC` | 114 | $11.08 | $11.52 | +50.16 | — | +0.00 | +50.16 | +45.60 | — |
| 2026-08-27 | `ABX` | 129 | $9.78 | $9.68 | -12.90 | — | +0.00 | -12.90 | -19.35 | — |
| 2026-08-27 | `AVEX` | 72 | $18.34 | $18.43 | +6.48 | — | +0.00 | +6.48 | +66.24 | — |
| 2026-08-27 | `AXTI` | 19 | $65.18 | $70.30 | +97.28 | — | +0.00 | +97.28 | +94.24 | — |
| 2026-08-27 | `ACMR` | 15 | — | $81.65 | +0.00 | $80.49 | -17.40 | -17.40 | +0.00 | -17.40 |
| 2026-08-27 | `GGB` | 280 | — | $4.57 | +0.00 | $4.70 | +36.40 | +36.40 | +0.00 | +36.40 |
| 2026-08-27 | `MT` | 17 | — | $74.54 | +0.00 | $74.63 | +1.53 | +1.53 | +0.00 | +1.53 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `TX` | 23 | — | $55.25 | +0.00 | $55.83 | +13.34 | +13.34 | +0.00 | +13.34 |
| 2026-08-27 | `ANET` | 6 | — | $205.90 | +0.00 | $201.09 | -28.86 | -28.86 | +0.00 | -28.86 |
| 2026-08-27 | `DLO` | 83 | — | $15.33 | +0.00 | $15.14 | -15.77 | -15.77 | +0.00 | -15.77 |
| 2026-08-28 | `ACMR` | 15 | $80.49 | $79.27 | -18.30 | — | +0.00 | -18.30 | -35.70 | — |
| 2026-08-28 | `GGB` | 280 | $4.70 | $4.67 | -8.40 | — | +0.00 | -8.40 | +28.00 | — |
| 2026-08-28 | `MT` | 17 | $74.63 | $75.39 | +12.92 | — | +0.00 | +12.92 | +14.45 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `TX` | 23 | $55.83 | $55.97 | +3.22 | — | +0.00 | +3.22 | +16.56 | — |
| 2026-08-28 | `ANET` | 6 | $201.09 | $200.00 | -6.54 | — | +0.00 | -6.54 | -35.40 | — |
| 2026-08-28 | `DLO` | 83 | $15.14 | $15.19 | +4.15 | — | +0.00 | +4.15 | -11.62 | — |
| 2026-08-28 | `SEDG` | 38 | — | $32.90 | +0.00 | $31.41 | -56.62 | -56.62 | +0.00 | -56.62 |
| 2026-08-28 | `GRRR` | 81 | — | $15.66 | +0.00 | $14.41 | -101.25 | -101.25 | +0.00 | -101.25 |
| 2026-08-28 | `URBN` | 16 | — | $79.42 | +0.00 | $81.09 | +26.72 | +26.72 | +0.00 | +26.72 |
| 2026-08-28 | `TTMI` | 10 | — | $122.81 | +0.00 | $118.65 | -41.60 | -41.60 | +0.00 | -41.60 |
| 2026-08-28 | `TLS` | 263 | — | $4.82 | +0.00 | $4.79 | -7.89 | -7.89 | +0.00 | -7.89 |
| 2026-08-28 | `KEYS` | 3 | — | $324.41 | +0.00 | $319.97 | -13.32 | -13.32 | +0.00 | -13.32 |
| 2026-08-28 | `AVT` | 13 | — | $91.49 | +0.00 | $88.63 | -37.18 | -37.18 | +0.00 | -37.18 |
| 2026-08-28 | `CGNX` | 20 | — | $62.82 | +0.00 | $60.46 | -47.20 | -47.20 | +0.00 | -47.20 |
| 2026-08-31 | `SEDG` | 38 | $31.41 | $31.15 | -9.88 | — | +0.00 | -9.88 | -66.50 | — |
| 2026-08-31 | `GRRR` | 81 | $14.41 | $14.44 | +2.43 | — | +0.00 | +2.43 | -98.82 | — |
| 2026-08-31 | `URBN` | 16 | $81.09 | $80.44 | -10.40 | — | +0.00 | -10.40 | +16.32 | — |
| 2026-08-31 | `TTMI` | 10 | $118.65 | $118.83 | +1.80 | — | +0.00 | +1.80 | -39.80 | — |
| 2026-08-31 | `TLS` | 263 | $4.79 | $4.81 | +5.26 | — | +0.00 | +5.26 | -2.63 | — |
| 2026-08-31 | `KEYS` | 3 | $319.97 | $322.49 | +7.56 | — | +0.00 | +7.56 | -5.76 | — |
| 2026-08-31 | `AVT` | 13 | $88.63 | $89.39 | +9.88 | — | +0.00 | +9.88 | -27.30 | — |
| 2026-08-31 | `CGNX` | 20 | $60.46 | $60.46 | +0.00 | — | +0.00 | +0.00 | -47.20 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 23 | — | $52.88 | +0.00 | $52.46 | -9.66 | -9.66 | +0.00 | -9.66 |
| 2026-09-03 | `HRMY` | 28 | — | $42.93 | +0.00 | $41.86 | -29.96 | -29.96 | +0.00 | -29.96 |
| 2026-09-03 | `CABA` | 339 | — | $3.63 | +0.00 | $3.48 | -50.85 | -50.85 | +0.00 | -50.85 |
| 2026-09-03 | `VSTM` | 153 | — | $8.03 | +0.00 | $7.98 | -7.65 | -7.65 | +0.00 | -7.65 |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `CRK` | 79 | — | $15.45 | +0.00 | $14.95 | -39.50 | -39.50 | +0.00 | -39.50 |
| 2026-09-03 | `MRNA` | 8 | — | $145.94 | +0.00 | $148.87 | +23.40 | +23.40 | +0.00 | +23.40 |
| 2026-09-03 | `ARCT` | 73 | — | $16.77 | +0.00 | $15.56 | -88.33 | -88.33 | +0.00 | -88.33 |
| 2026-09-04 | `ATRC` | 23 | $52.46 | $52.03 | -9.89 | $51.52 | -11.73 | -21.62 | -19.55 | -31.28 |
| 2026-09-04 | `HRMY` | 28 | $41.86 | $41.50 | -10.08 | $42.25 | +21.00 | +10.92 | -40.04 | -19.04 |
| 2026-09-04 | `CABA` | 339 | $3.48 | $3.46 | -6.78 | $3.47 | +3.39 | -3.39 | -57.63 | -54.24 |
| 2026-09-04 | `VSTM` | 153 | $7.98 | $7.91 | -10.71 | — | +0.00 | -10.71 | -18.36 | — |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `CRK` | 79 | $14.95 | $15.00 | +3.95 | — | +0.00 | +3.95 | -35.55 | — |
| 2026-09-04 | `MRNA` | 8 | $148.87 | $153.62 | +38.00 | — | +0.00 | +38.00 | +61.40 | — |
| 2026-09-04 | `ARCT` | 73 | $15.56 | $15.61 | +3.65 | — | +0.00 | +3.65 | -84.68 | — |
| 2026-09-04 | `ALEC` | 482 | — | $2.52 | +0.00 | $2.46 | -28.92 | -28.92 | +0.00 | -28.92 |
| 2026-09-04 | `BHC` | 181 | — | $6.71 | +0.00 | $6.56 | -27.15 | -27.15 | +0.00 | -27.15 |
| 2026-09-04 | `OABI` | 254 | — | $4.78 | +0.00 | $4.33 | -114.30 | -114.30 | +0.00 | -114.30 |
| 2026-09-04 | `VIR` | 107 | — | $11.31 | +0.00 | $11.38 | +8.02 | +8.02 | +0.00 | +8.02 |
| 2026-09-04 | `CRM` | 4 | — | $263.36 | +0.00 | $259.23 | -16.52 | -16.52 | +0.00 | -16.52 |
| 2026-09-08 | `ATRC` | 23 | $51.52 | $54.31 | +64.17 | — | +0.00 | +64.17 | +32.89 | — |
| 2026-09-08 | `HRMY` | 28 | $42.25 | $42.20 | -1.40 | — | +0.00 | -1.40 | -20.44 | — |
| 2026-09-08 | `CABA` | 339 | $3.47 | $3.43 | -13.56 | — | +0.00 | -13.56 | -67.80 | — |
| 2026-09-08 | `ALEC` | 482 | $2.46 | $2.38 | -38.56 | — | +0.00 | -38.56 | -67.48 | — |
| 2026-09-08 | `BHC` | 181 | $6.56 | $6.57 | +1.81 | — | +0.00 | +1.81 | -25.34 | — |
| 2026-09-08 | `OABI` | 254 | $4.33 | $4.30 | -7.62 | — | +0.00 | -7.62 | -121.92 | — |
| 2026-09-08 | `VIR` | 107 | $11.38 | $11.22 | -17.65 | — | +0.00 | -17.65 | -9.63 | — |
| 2026-09-08 | `CRM` | 4 | $259.23 | $253.72 | -22.04 | — | +0.00 | -22.04 | -38.56 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +91.20 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | — | $560.20 | $10,051.46 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 |
| 2026-08-17 | +2.25 | $560.20 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | $10,054.84 | +3.38 | -123.94 | DVN, EOG, FANG, TMC, TGB, ABX, ALM, INV | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $140.13 | $9,863.96 | DVN×27, EOG×8, FANG×6, TMC×309, TGB×147, ABX×137, ALM×77, INV×772 |
| 2026-08-18 | -6.20 | $140.13 | DVN×27, EOG×8, FANG×6, TMC×309, TGB×147, ABX×137, ALM×77, INV×772 | $9,755.43 | -108.53 | +0.00 | — | DVN, EOG, FANG, TMC, TGB, ABX, ALM, INV | $9,727.99 | $9,727.99 | — |
| 2026-08-19 | -7.20 | $9,727.99 | — | $9,727.99 | +0.00 | +0.00 | — | — | $9,727.99 | $9,727.99 | — |
| 2026-08-20 | +1.12 | $9,727.99 | — | $9,727.99 | +0.00 | +165.32 | AG, BHP, HDSN, IAG, KGC, NFGC, WPM, ABUS | — | $97.05 | $9,867.97 | AG×59, BHP×13, HDSN×210, IAG×61, KGC×41, NFGC×694, WPM×8, ABUS×247 |
| 2026-08-21 | +3.25 | $97.05 | AG×59, BHP×13, HDSN×210, IAG×61, KGC×41, NFGC×694, WPM×8, ABUS×247 | $10,198.81 | +330.84 | -54.75 | CRSP, FUTU, GMAB, BTBT, DE, CF, WOLF, AMRC | AG, BHP, HDSN, IAG, KGC, NFGC, WPM, ABUS | $172.07 | $10,094.03 | CRSP×21, FUTU×11, GMAB×38, BTBT×766, DE×2, CF×9, WOLF×47, AMRC×56 |
| 2026-08-24 | -5.17 | $172.07 | CRSP×21, FUTU×11, GMAB×38, BTBT×766, DE×2, CF×9, WOLF×47, AMRC×56 | $10,008.91 | -85.12 | +0.00 | — | CRSP, FUTU, GMAB, BTBT, DE, CF, WOLF, AMRC | $9,984.27 | $9,984.27 | — |
| 2026-08-25 | +1.80 | $9,984.27 | — | $9,984.27 | -0.00 | +245.56 | OCUL, INSP, CRMD, CAPR, KURA, CCOI, LIFE, ZIP | — | $62.73 | $10,210.24 | OCUL×113, INSP×20, CRMD×149, CAPR×172, KURA×91, CCOI×131, LIFE×33, ZIP×274 |
| 2026-08-26 | +2.02 | $62.73 | OCUL×113, INSP×20, CRMD×149, CAPR×172, KURA×91, CCOI×131, LIFE×33, ZIP×274 | $10,169.46 | -40.78 | +16.98 | RZLT, AVBP, FLNC, ABX, AVEX, AXTI | INSP, CAPR, KURA, CCOI, LIFE, ZIP | $80.55 | $10,157.07 | OCUL×113, CRMD×149, RZLT×254, AVBP×40, FLNC×114, ABX×129, AVEX×72, AXTI×19 |
| 2026-08-27 | — | $80.55 | OCUL×113, CRMD×149, RZLT×254, AVBP×40, FLNC×114, ABX×129, AVEX×72, AXTI×19 | $10,290.79 | +133.72 | -42.38 | ACMR, GGB, MT, MU, TX, ANET, DLO | OCUL, CRMD, RZLT, AVBP, FLNC, ABX, AVEX, AXTI | $1,738.37 | $10,213.07 | ACMR×15, GGB×280, MT×17, MU×1, TX×23, ANET×6, DLO×83 |
| 2026-08-28 | +0.75 | $1,738.37 | ACMR×15, GGB×280, MT×17, MU×1, TX×23, ANET×6, DLO×83 | $10,184.02 | -29.05 | -278.34 | SEDG, GRRR, URBN, TTMI, TLS, KEYS, AVT, CGNX | ACMR, GGB, MT, MU, TX, ANET, DLO | $445.84 | $9,871.64 | SEDG×38, GRRR×81, URBN×16, TTMI×10, TLS×263, KEYS×3, AVT×13, CGNX×20 |
| 2026-08-31 | -5.85 | $445.84 | SEDG×38, GRRR×81, URBN×16, TTMI×10, TLS×263, KEYS×3, AVT×13, CGNX×20 | $9,878.29 | +6.65 | +0.00 | — | SEDG, GRRR, URBN, TTMI, TLS, KEYS, AVT, CGNX | $9,860.23 | $9,860.23 | — |
| 2026-09-01 | -6.30 | $9,860.23 | — | $9,860.23 | -0.00 | +0.00 | — | — | $9,860.23 | $9,860.23 | — |
| 2026-09-02 | -3.83 | $9,860.23 | — | $9,860.23 | -0.00 | +0.00 | — | — | $9,860.23 | $9,860.23 | — |
| 2026-09-03 | -0.90 | $9,860.23 | — | $9,860.23 | -0.00 | -218.93 | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MRNA, ARCT | — | $159.00 | $9,621.88 | ATRC×23, HRMY×28, CABA×339, VSTM×153, RVTY×9, CRK×79, MRNA×8, ARCT×73 |
| 2026-09-04 | +2.25 | $159.00 | ATRC×23, HRMY×28, CABA×339, VSTM×153, RVTY×9, CRK×79, MRNA×8, ARCT×73 | $9,624.62 | +2.74 | -166.21 | ALEC, BHC, OABI, VIR, CRM | VSTM, RVTY, CRK, MRNA, ARCT | $158.73 | $9,431.04 | ATRC×23, HRMY×28, CABA×339, ALEC×482, BHC×181, OABI×254, VIR×107, CRM×4 |
| 2026-09-08 | -11.47 | $158.73 | ATRC×23, HRMY×28, CABA×339, ALEC×482, BHC×181, OABI×254, VIR×107, CRM×4 | $9,396.18 | -34.86 | +0.00 | — | ATRC, HRMY, CABA, ALEC, BHC, OABI, VIR, CRM | $9,371.00 | $9,371.00 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,544.55 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $4,332.68 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+5.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 138 | $9.01 | $2.40 | — | $3,086.90 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1334 | $0.94 | $16.50 | — | $1,820.44 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $560.20 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
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
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,766.06 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+6.7; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,621.89 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+5.8; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,403.68 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+8.3; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 309 | $4.05 | $3.99 | — | $5,148.25 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 147 | $8.46 | $2.43 | — | $3,902.19 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 137 | $9.12 | $2.40 | — | $2,650.35 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 77 | $16.20 | $2.22 | — | $1,400.73 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 772 | $1.62 | $9.96 | — | $140.13 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $140.13 | ▼ close $9,863.96 vs 09:30 $10,054.84 (session -123.94) | 16:00 close · cash $140.13 · equity $9,863.96 vs 09:30 $10,054.84 (-190.88; session marks -123.94) · 8 name(s) marked open→close (per-name table). DVN×27 09:30 $46.18 → close $47.57 +37.53; EOG×8 09:30 $142.77 → close $146.15 +27.04; FANG×6 09:30 $202.70 → close $206.29 +21.54; TMC×309 09:30 $4.05 → close $3.77 -86.52; TGB×147 09:30 $8.46 → close $8.77 +45.57; ABX×137 09:30 $9.12 → close $9.12 +0.00; ALM×77 09:30 $16.20 → close $16.36 +12.32; INV×772 09:30 $1.62 → close $1.39 -181.42 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $140.13 | ▼ 09:30 equity $9,755.43 vs yday $9,863.96 (-108.53) | 09:30 open · cash $140.13 (unchanged overnight, no fees) · equity $9,755.43 vs prior close $9,863.96 (-108.53) · 8 name(s) re-marked at the open (per-name table). DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; TMC×309 yday $3.77 → 09:30 $3.72 -15.45; TGB×147 yday $8.77 → 09:30 $8.55 -32.34; ABX×137 yday $9.12 → 09:30 $9.03 -12.33; ALM×77 yday $16.36 → 09:30 $15.78 -44.66; INV×772 yday $1.39 → 09:30 $1.32 -46.32 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,434.04 | ▲ +44.98 after sell → book $9,753.34; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,616.33 | ▲ +38.11 after sell → book $9,751.31; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,867.88 | ▲ +33.34 after sell → book $9,749.28; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 309 | $3.72 | $4.05 | $-110.00 | $5,013.31 | ▼ -110.00 after sell → book $9,745.23; vs 09:30 mark -4.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 147 | $8.55 | $2.47 | $+8.33 | $6,267.70 | ▲ +8.33 after sell → book $9,742.77; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 137 | $9.03 | $2.43 | $-17.16 | $7,502.37 | ▼ -17.16 after sell → book $9,740.33; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 77 | $15.78 | $2.24 | $-36.80 | $8,715.19 | ▼ -36.80 after sell → book $9,738.09; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `INV` | 772 | $1.32 | $10.10 | $-247.80 | $9,727.99 | ▼ -247.80 after sell → book $9,727.99; vs 09:30 mark -10.10 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,727.99 | ▲ close $9,727.99 vs 09:30 $9,755.43 (session +0.00) | 16:00 close · cash $9,727.99 · no lots left · equity $9,727.99. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,727.99 | ▲ 09:30 equity $9,727.99 vs yday $9,727.99 (+0.00) | 09:30 open · cash $9,727.99 · no holdings · equity $9,727.99 vs prior close $9,727.99 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,727.99 | ▲ close $9,727.99 vs 09:30 $9,727.99 (session +0.00) | 16:00 close · cash $9,727.99 · no lots left · equity $9,727.99. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,727.99 | ▲ 09:30 equity $9,727.99 vs yday $9,727.99 (+0.00) | 09:30 open · cash $9,727.99 · no holdings · equity $9,727.99 vs prior close $9,727.99 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 59 | $20.55 | $2.17 | — | $8,513.38 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1216.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,328.22 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1216.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 210 | $5.77 | $2.71 | — | $6,113.81 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1216.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $4,914.20 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1216.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $3,697.26 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1216.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 694 | $1.75 | $8.95 | — | $2,473.81 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1216.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,315.48 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1216.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 247 | $4.92 | $3.19 | — | $97.05 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1216.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.05 | ▲ close $9,867.97 vs 09:30 $9,727.99 (session +165.32) | 16:00 close · cash $97.05 · equity $9,867.97 vs 09:30 $9,727.99 (+139.98; session marks +165.32) · 8 name(s) marked open→close (per-name table). AG×59 09:30 $20.55 → close $21.19 +37.76; BHP×13 09:30 $91.01 → close $93.63 +34.06; HDSN×210 09:30 $5.77 → close $5.57 -42.00; IAG×61 09:30 $19.63 → close $20.50 +53.07; KGC×41 09:30 $29.63 → close $31.43 +73.80; NFGC×694 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68; ABUS×247 09:30 $4.92 → close $4.77 -37.05 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.05 | ▲ 09:30 equity $10,198.81 vs yday $9,867.97 (+330.84) | 09:30 open · cash $97.05 (unchanged overnight, no fees) · equity $10,198.81 vs prior close $9,867.97 (+330.84) · 8 name(s) re-marked at the open (per-name table). AG×59 yday $21.19 → 09:30 $21.90 +41.89; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; HDSN×210 yday $5.57 → 09:30 $5.67 +21.00; IAG×61 yday $20.50 → 09:30 $21.17 +40.87; KGC×41 yday $31.43 → 09:30 $32.17 +30.34; NFGC×694 yday $1.75 → 09:30 $1.79 +27.76; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×247 yday $4.77 → 09:30 $5.20 +106.21 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 59 | $21.90 | $2.19 | $+75.30 | $1,386.96 | ▲ +75.30 after sell → book $10,196.62; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,629.27 | ▲ +57.15 after sell → book $10,194.57; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 210 | $5.67 | $2.75 | $-26.46 | $3,817.22 | ▼ -26.46 after sell → book $10,191.82; vs 09:30 mark -2.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 61 | $21.17 | $2.19 | $+89.57 | $5,106.40 | ▲ +89.57 after sell → book $10,189.63; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $6,423.23 | ▲ +99.89 after sell → book $10,187.49; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 694 | $1.79 | $9.08 | $+9.73 | $7,656.41 | ▲ +9.73 after sell → book $10,178.41; vs 09:30 mark -9.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $8,891.98 | ▲ +77.23 after sell → book $10,176.38; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 247 | $5.20 | $3.24 | $+62.74 | $10,173.14 | ▲ +62.74 after sell → book $10,173.14; vs 09:30 mark -3.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $8,916.97 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1271.64 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $7,647.97 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1271.64 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 38 | $33.36 | $2.10 | — | $6,378.18 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $1271.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 766 | $1.66 | $9.88 | — | $5,096.74 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1271.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $3,848.22 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1271.64 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 9 | $127.43 | $2.02 | — | $2,699.34 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable; 🔵; ⚪; ret5=+7.9; leftover $1271.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `WOLF` | 47 | $26.86 | $2.13 | — | $1,434.79 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_mover; 🔵; ret5=-16.4; leftover $1271.64 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AMRC` | 56 | $22.51 | $2.16 | — | $172.07 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_mover; 🔵; ret5=-20.2; leftover $1271.64 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $172.07 | ▼ close $10,094.03 vs 09:30 $10,198.81 (session -54.75) | 16:00 close · cash $172.07 · equity $10,094.03 vs 09:30 $10,198.81 (-104.78; session marks -54.75) · 8 name(s) marked open→close (per-name table). CRSP×21 09:30 $59.72 → close $59.50 -4.62; FUTU×11 09:30 $115.18 → close $123.64 +93.06; GMAB×38 09:30 $33.36 → close $33.45 +3.42; BTBT×766 09:30 $1.66 → close $1.53 -99.58; DE×2 09:30 $623.26 → close $647.47 +48.42; CF×9 09:30 $127.43 → close $129.60 +19.53; WOLF×47 09:30 $26.86 → close $25.76 -51.70; AMRC×56 09:30 $22.51 → close $21.38 -63.28 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $172.07 | ▼ 09:30 equity $10,008.91 vs yday $10,094.03 (-85.12) | 09:30 open · cash $172.07 (unchanged overnight, no fees) · equity $10,008.91 vs prior close $10,094.03 (-85.12) · 8 name(s) re-marked at the open (per-name table). CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; FUTU×11 yday $123.64 → 09:30 $121.00 -29.04; GMAB×38 yday $33.45 → 09:30 $32.82 -23.94; BTBT×766 yday $1.53 → 09:30 $1.55 +15.32; DE×2 yday $647.47 → 09:30 $653.04 +11.14; CF×9 yday $129.60 → 09:30 $129.99 +3.51; WOLF×47 yday $25.76 → 09:30 $25.00 -35.72; AMRC×56 yday $21.38 → 09:30 $21.19 -10.64 | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.75 | $2.07 | $-24.50 | $1,403.75 | ▼ -24.50 after sell → book $10,006.84; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $121.00 | $2.04 | $+59.95 | $2,732.70 | ▲ +59.95 after sell → book $10,004.79; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 38 | $32.82 | $2.12 | $-24.75 | $3,977.74 | ▼ -24.75 after sell → book $10,002.67; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 766 | $1.55 | $10.02 | $-104.16 | $5,155.02 | ▼ -104.16 after sell → book $9,992.65; vs 09:30 mark -10.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $6,459.08 | ▲ +55.55 after sell → book $9,990.63; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 9 | $129.99 | $2.04 | $+18.99 | $7,626.96 | ▲ +18.99 after sell → book $9,988.60; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WOLF` | 47 | $25.00 | $2.15 | $-91.70 | $8,799.81 | ▼ -91.70 after sell → book $9,986.45; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AMRC` | 56 | $21.19 | $2.18 | $-78.26 | $9,984.27 | ▼ -78.26 after sell → book $9,984.27; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,984.27 | ▲ close $9,984.27 vs 09:30 $10,008.91 (session +0.00) | 16:00 close · cash $9,984.27 · no lots left · equity $9,984.27. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,984.27 | ▲ 09:30 equity $9,984.27 vs yday $9,984.27 (-0.00) | 09:30 open · cash $9,984.27 · no holdings · equity $9,984.27 vs prior close $9,984.27 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 113 | $10.98 | $2.33 | — | $8,741.20 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+1.2; leftover $1248.03 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 20 | $61.19 | $2.05 | — | $7,515.35 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+7.4; leftover $1248.03 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 149 | $8.35 | $2.44 | — | $6,268.76 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1248.03 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 172 | $7.25 | $2.51 | — | $5,019.26 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1248.03 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 91 | $13.59 | $2.26 | — | $3,780.30 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1248.03 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 131 | $9.49 | $2.38 | — | $2,534.73 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1248.03 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 33 | $36.96 | $2.09 | — | $1,312.96 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1248.03 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 274 | $4.55 | $3.53 | — | $62.73 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1248.03 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $62.73 | ▲ close $10,210.24 vs 09:30 $9,984.27 (session +245.56) | 16:00 close · cash $62.73 · equity $10,210.24 vs 09:30 $9,984.27 (+225.97; session marks +245.56) · 8 name(s) marked open→close (per-name table). OCUL×113 09:30 $10.98 → close $10.88 -11.30; INSP×20 09:30 $61.19 → close $61.07 -2.40; CRMD×149 09:30 $8.35 → close $8.56 +31.29; CAPR×172 09:30 $7.25 → close $8.29 +178.88; KURA×91 09:30 $13.59 → close $13.59 +0.00; CCOI×131 09:30 $9.49 → close $9.88 +51.09; LIFE×33 09:30 $36.96 → close $38.56 +52.80; ZIP×274 09:30 $4.55 → close $4.35 -54.80 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.73 | ▼ 09:30 equity $10,169.46 vs yday $10,210.24 (-40.78) | 09:30 open · cash $62.73 (unchanged overnight, no fees) · equity $10,169.46 vs prior close $10,210.24 (-40.78) · 8 name(s) re-marked at the open (per-name table). OCUL×113 yday $10.88 → 09:30 $10.79 -10.17; INSP×20 yday $61.07 → 09:30 $60.07 -20.00; CRMD×149 yday $8.56 → 09:30 $8.60 +5.96; CAPR×172 yday $8.29 → 09:30 $8.29 +0.00; KURA×91 yday $13.59 → 09:30 $13.63 +3.64; CCOI×131 yday $9.88 → 09:30 $9.89 +1.31; LIFE×33 yday $38.56 → 09:30 $38.24 -10.56; ZIP×274 yday $4.35 → 09:30 $4.31 -10.96 | — |
| 2026-08-26 09:30 ET | **SELL** | `INSP` | 20 | $60.07 | $2.07 | $-26.52 | $1,262.06 | ▼ -26.52 after sell → book $10,167.39; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 172 | $8.29 | $2.55 | $+173.83 | $2,685.39 | ▲ +173.83 after sell → book $10,164.84; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 91 | $13.63 | $2.29 | $-0.91 | $3,923.43 | ▼ -0.91 after sell → book $10,162.55; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 131 | $9.89 | $2.42 | $+47.60 | $5,216.61 | ▲ +47.60 after sell → book $10,160.14; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 33 | $38.24 | $2.11 | $+38.04 | $6,476.42 | ▲ +38.04 after sell → book $10,158.03; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 274 | $4.31 | $3.59 | $-72.88 | $7,653.77 | ▼ -72.88 after sell → book $10,154.44; vs 09:30 mark -3.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `RZLT` | 254 | $5.01 | $3.28 | — | $6,377.95 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,yday_gainer; 🔵; ret5=+7.5; leftover $1275.63 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 40 | $31.21 | $2.11 | — | $5,127.44 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1275.63 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 114 | $11.12 | $2.33 | — | $3,857.43 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1275.63 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 129 | $9.83 | $2.38 | — | $2,586.98 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1275.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AVEX` | 72 | $17.51 | $2.21 | — | $1,324.06 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $1275.63 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AXTI` | 19 | $65.34 | $2.05 | — | $80.55 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1275.63 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $80.55 | ▲ close $10,157.07 vs 09:30 $10,169.46 (session +16.98) | 16:00 close · cash $80.55 · equity $10,157.07 vs 09:30 $10,169.46 (-12.39; session marks +16.98) · 8 name(s) marked open→close (per-name table). OCUL×113 09:30 $10.79 → close $10.77 -2.26; CRMD×149 09:30 $8.60 → close $8.39 -31.29; RZLT×254 09:30 $5.01 → close $5.04 +7.62; AVBP×40 09:30 $31.21 → close $31.14 -2.80; FLNC×114 09:30 $11.12 → close $11.08 -4.56; ABX×129 09:30 $9.83 → close $9.78 -6.45; AVEX×72 09:30 $17.51 → close $18.34 +59.76; AXTI×19 09:30 $65.34 → close $65.18 -3.04 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.55 | ▲ 09:30 equity $10,290.79 vs yday $10,157.07 (+133.72) | 09:30 open · cash $80.55 (unchanged overnight, no fees) · equity $10,290.79 vs prior close $10,157.07 (+133.72) · 8 name(s) re-marked at the open (per-name table). OCUL×113 yday $10.77 → 09:30 $10.63 -15.82; CRMD×149 yday $8.39 → 09:30 $8.49 +14.90; RZLT×254 yday $5.04 → 09:30 $5.07 +7.62; AVBP×40 yday $31.14 → 09:30 $30.79 -14.00; FLNC×114 yday $11.08 → 09:30 $11.52 +50.16; ABX×129 yday $9.78 → 09:30 $9.68 -12.90; AVEX×72 yday $18.34 → 09:30 $18.43 +6.48; AXTI×19 yday $65.18 → 09:30 $70.30 +97.28 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 113 | $10.63 | $2.36 | $-44.24 | $1,279.38 | ▼ -44.24 after sell → book $10,288.43; vs 09:30 mark -2.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 149 | $8.49 | $2.47 | $+15.95 | $2,541.92 | ▲ +15.95 after sell → book $10,285.96; vs 09:30 mark -2.47 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 254 | $5.07 | $3.33 | $+8.63 | $3,826.37 | ▲ +8.63 after sell → book $10,282.63; vs 09:30 mark -3.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 40 | $30.79 | $2.13 | $-21.04 | $5,055.84 | ▼ -21.04 after sell → book $10,280.50; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 114 | $11.52 | $2.36 | $+40.91 | $6,366.76 | ▲ +40.91 after sell → book $10,278.14; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ABX` | 129 | $9.68 | $2.41 | $-24.14 | $7,613.07 | ▼ -24.14 after sell → book $10,275.73; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVEX` | 72 | $18.43 | $2.23 | $+61.81 | $8,937.80 | ▲ +61.81 after sell → book $10,273.50; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AXTI` | 19 | $70.30 | $2.07 | $+90.13 | $10,271.43 | ▲ +90.13 after sell → book $10,271.43; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 15 | $81.65 | $2.04 | — | $9,044.65 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=+2.0; leftover $1283.93 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 280 | $4.57 | $3.61 | — | $7,761.44 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=+1.1; leftover $1283.93 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 17 | $74.54 | $2.04 | — | $6,492.22 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=-0.1; leftover $1283.93 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $5,523.21 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=+0.1; leftover $1283.93 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 23 | $55.25 | $2.06 | — | $4,250.40 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=+2.1; leftover $1283.93 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $205.90 | $2.01 | — | $3,013.00 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=+8.5; leftover $1283.93 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 83 | $15.33 | $2.24 | — | $1,738.37 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=+7.4; leftover $1283.93 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,738.37 | ▼ close $10,213.07 vs 09:30 $10,290.79 (session -42.38) | 16:00 close · cash $1,738.37 · equity $10,213.07 vs 09:30 $10,290.79 (-77.72; session marks -42.38) · 7 name(s) marked open→close (per-name table). ACMR×15 09:30 $81.65 → close $80.49 -17.40; GGB×280 09:30 $4.57 → close $4.70 +36.40; MT×17 09:30 $74.54 → close $74.63 +1.53; MU×1 09:30 $967.01 → close $935.39 -31.62; TX×23 09:30 $55.25 → close $55.83 +13.34; ANET×6 09:30 $205.90 → close $201.09 -28.86; DLO×83 09:30 $15.33 → close $15.14 -15.77 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,738.37 | ▼ 09:30 equity $10,184.02 vs yday $10,213.07 (-29.05) | 09:30 open · cash $1,738.37 (unchanged overnight, no fees) · equity $10,184.02 vs prior close $10,213.07 (-29.05) · 7 name(s) re-marked at the open (per-name table). ACMR×15 yday $80.49 → 09:30 $79.27 -18.30; GGB×280 yday $4.70 → 09:30 $4.67 -8.40; MT×17 yday $74.63 → 09:30 $75.39 +12.92; MU×1 yday $935.39 → 09:30 $919.29 -16.10; TX×23 yday $55.83 → 09:30 $55.97 +3.22; ANET×6 yday $201.09 → 09:30 $200.00 -6.54; DLO×83 yday $15.14 → 09:30 $15.19 +4.15 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 15 | $79.27 | $2.06 | $-39.79 | $2,925.36 | ▼ -39.79 after sell → book $10,181.96; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 280 | $4.67 | $3.67 | $+20.72 | $4,229.29 | ▲ +20.72 after sell → book $10,178.29; vs 09:30 mark -3.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 17 | $75.39 | $2.06 | $+10.35 | $5,508.86 | ▲ +10.35 after sell → book $10,176.23; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $6,426.14 | ▼ -51.73 after sell → book $10,174.22; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 23 | $55.97 | $2.08 | $+12.42 | $7,711.37 | ▲ +12.42 after sell → book $10,172.14; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $200.00 | $2.03 | $-39.44 | $8,909.34 | ▼ -39.44 after sell → book $10,170.11; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 83 | $15.19 | $2.26 | $-16.12 | $10,167.85 | ▼ -16.12 after sell → book $10,167.85; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 38 | $32.90 | $2.10 | — | $8,915.54 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1270.98 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 81 | $15.66 | $2.23 | — | $7,644.85 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1270.98 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $79.42 | $2.04 | — | $6,372.09 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1270.98 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $122.81 | $2.02 | — | $5,141.97 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1270.98 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 263 | $4.82 | $3.39 | — | $3,870.92 | — | combo gate; gate blue=True,ret_5_max=10.0; list ohlc_hot; 🔵; ret5=+8.8; leftover $1270.98 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $2,895.69 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1270.98 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 13 | $91.49 | $2.03 | — | $1,704.29 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1270.98 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 20 | $62.82 | $2.05 | — | $445.84 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1270.98 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $445.84 | ▼ close $9,871.64 vs 09:30 $10,184.02 (session -278.34) | 16:00 close · cash $445.84 · equity $9,871.64 vs 09:30 $10,184.02 (-312.38; session marks -278.34) · 8 name(s) marked open→close (per-name table). SEDG×38 09:30 $32.90 → close $31.41 -56.62; GRRR×81 09:30 $15.66 → close $14.41 -101.25; URBN×16 09:30 $79.42 → close $81.09 +26.72; TTMI×10 09:30 $122.81 → close $118.65 -41.60; TLS×263 09:30 $4.82 → close $4.79 -7.89; KEYS×3 09:30 $324.41 → close $319.97 -13.32; AVT×13 09:30 $91.49 → close $88.63 -37.18; CGNX×20 09:30 $62.82 → close $60.46 -47.20 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $445.84 | ▲ 09:30 equity $9,878.29 vs yday $9,871.64 (+6.65) | 09:30 open · cash $445.84 (unchanged overnight, no fees) · equity $9,878.29 vs prior close $9,871.64 (+6.65) · 8 name(s) re-marked at the open (per-name table). SEDG×38 yday $31.41 → 09:30 $31.15 -9.88; GRRR×81 yday $14.41 → 09:30 $14.44 +2.43; URBN×16 yday $81.09 → 09:30 $80.44 -10.40; TTMI×10 yday $118.65 → 09:30 $118.83 +1.80; TLS×263 yday $4.79 → 09:30 $4.81 +5.26; KEYS×3 yday $319.97 → 09:30 $322.49 +7.56; AVT×13 yday $88.63 → 09:30 $89.39 +9.88; CGNX×20 yday $60.46 → 09:30 $60.46 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 38 | $31.15 | $2.12 | $-70.73 | $1,627.42 | ▼ -70.73 after sell → book $9,876.17; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 81 | $14.44 | $2.26 | $-103.31 | $2,794.80 | ▼ -103.31 after sell → book $9,873.91; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $80.44 | $2.06 | $+12.22 | $4,079.78 | ▲ +12.22 after sell → book $9,871.85; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 10 | $118.83 | $2.04 | $-43.86 | $5,266.04 | ▼ -43.86 after sell → book $9,869.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 263 | $4.81 | $3.45 | $-9.47 | $6,527.63 | ▼ -9.47 after sell → book $9,866.37; vs 09:30 mark -3.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 3 | $322.49 | $2.02 | $-9.78 | $7,493.08 | ▼ -9.78 after sell → book $9,864.35; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 13 | $89.39 | $2.05 | $-31.38 | $8,653.10 | ▼ -31.38 after sell → book $9,862.30; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 20 | $60.46 | $2.07 | $-51.32 | $9,860.23 | ▼ -51.32 after sell → book $9,860.23; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,860.23 | ▲ close $9,860.23 vs 09:30 $9,878.29 (session +0.00) | 16:00 close · cash $9,860.23 · no lots left · equity $9,860.23. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,860.23 | ▲ 09:30 equity $9,860.23 vs yday $9,860.23 (-0.00) | 09:30 open · cash $9,860.23 · no holdings · equity $9,860.23 vs prior close $9,860.23 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,860.23 | ▲ close $9,860.23 vs 09:30 $9,860.23 (session +0.00) | 16:00 close · cash $9,860.23 · no lots left · equity $9,860.23. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,860.23 | ▲ 09:30 equity $9,860.23 vs yday $9,860.23 (-0.00) | 09:30 open · cash $9,860.23 · no holdings · equity $9,860.23 vs prior close $9,860.23 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,860.23 | ▲ close $9,860.23 vs 09:30 $9,860.23 (session +0.00) | 16:00 close · cash $9,860.23 · no lots left · equity $9,860.23. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,860.23 | ▲ 09:30 equity $9,860.23 vs yday $9,860.23 (-0.00) | 09:30 open · cash $9,860.23 · no holdings · equity $9,860.23 vs prior close $9,860.23 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $8,641.93 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1232.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 28 | $42.93 | $2.07 | — | $7,437.82 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1232.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 339 | $3.63 | $4.37 | — | $6,202.87 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1232.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 153 | $8.03 | $2.45 | — | $4,971.83 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1232.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $3,777.77 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1232.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 79 | $15.45 | $2.23 | — | $2,554.99 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1232.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $1,385.42 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1232.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 73 | $16.77 | $2.21 | — | $159.00 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1232.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $159.00 | ▼ close $9,621.88 vs 09:30 $9,860.23 (session -218.93) | 16:00 close · cash $159.00 · equity $9,621.88 vs 09:30 $9,860.23 (-238.35; session marks -218.93) · 8 name(s) marked open→close (per-name table). ATRC×23 09:30 $52.88 → close $52.46 -9.66; HRMY×28 09:30 $42.93 → close $41.86 -29.96; CABA×339 09:30 $3.63 → close $3.48 -50.85; VSTM×153 09:30 $8.03 → close $7.98 -7.65; RVTY×9 09:30 $132.45 → close $130.63 -16.38; CRK×79 09:30 $15.45 → close $14.95 -39.50; MRNA×8 09:30 $145.94 → close $148.87 +23.40; ARCT×73 09:30 $16.77 → close $15.56 -88.33 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $159.00 | ▲ 09:30 equity $9,624.62 vs yday $9,621.88 (+2.74) | 09:30 open · cash $159.00 (unchanged overnight, no fees) · equity $9,624.62 vs prior close $9,621.88 (+2.74) · 8 name(s) re-marked at the open (per-name table). ATRC×23 yday $52.46 → 09:30 $52.03 -9.89; HRMY×28 yday $41.86 → 09:30 $41.50 -10.08; CABA×339 yday $3.48 → 09:30 $3.46 -6.78; VSTM×153 yday $7.98 → 09:30 $7.91 -10.71; RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; CRK×79 yday $14.95 → 09:30 $15.00 +3.95; MRNA×8 yday $148.87 → 09:30 $153.62 +38.00; ARCT×73 yday $15.56 → 09:30 $15.61 +3.65 | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 153 | $7.91 | $2.48 | $-23.29 | $1,366.74 | ▼ -23.29 after sell → book $9,622.13; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $2,534.98 | ▼ -25.83 after sell → book $9,620.10; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 79 | $15.00 | $2.25 | $-40.03 | $3,717.73 | ▼ -40.03 after sell → book $9,617.85; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $153.62 | $2.03 | $+57.35 | $4,944.65 | ▲ +57.35 after sell → book $9,615.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 73 | $15.61 | $2.23 | $-89.12 | $6,081.95 | ▼ -89.12 after sell → book $9,613.58; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 482 | $2.52 | $6.22 | — | $4,861.09 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1216.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 181 | $6.71 | $2.53 | — | $3,644.05 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1216.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 254 | $4.78 | $3.28 | — | $2,426.65 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1216.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 107 | $11.31 | $2.31 | — | $1,214.17 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1216.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $158.73 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1216.39 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.73 | ▼ close $9,431.04 vs 09:30 $9,624.62 (session -166.21) | 16:00 close · cash $158.73 · equity $9,431.04 vs 09:30 $9,624.62 (-193.58; session marks -166.21) · 8 name(s) marked open→close (per-name table). ATRC×23 09:30 $52.03 → close $51.52 -11.73; HRMY×28 09:30 $41.50 → close $42.25 +21.00; CABA×339 09:30 $3.46 → close $3.47 +3.39; ALEC×482 09:30 $2.52 → close $2.46 -28.92; BHC×181 09:30 $6.71 → close $6.56 -27.15; OABI×254 09:30 $4.78 → close $4.33 -114.30; VIR×107 09:30 $11.31 → close $11.38 +8.02; CRM×4 09:30 $263.36 → close $259.23 -16.52 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.73 | ▼ 09:30 equity $9,396.18 vs yday $9,431.04 (-34.86) | 09:30 open · cash $158.73 (unchanged overnight, no fees) · equity $9,396.18 vs prior close $9,431.04 (-34.86) · 8 name(s) re-marked at the open (per-name table). ATRC×23 yday $51.52 → 09:30 $54.31 +64.17; HRMY×28 yday $42.25 → 09:30 $42.20 -1.40; CABA×339 yday $3.47 → 09:30 $3.43 -13.56; ALEC×482 yday $2.46 → 09:30 $2.38 -38.56; BHC×181 yday $6.56 → 09:30 $6.57 +1.81; OABI×254 yday $4.33 → 09:30 $4.30 -7.62; VIR×107 yday $11.38 → 09:30 $11.22 -17.65; CRM×4 yday $259.23 → 09:30 $253.72 -22.04 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 23 | $54.31 | $2.08 | $+28.75 | $1,405.78 | ▲ +28.75 after sell → book $9,394.10; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HRMY` | 28 | $42.20 | $2.09 | $-24.61 | $2,585.29 | ▼ -24.61 after sell → book $9,392.01; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 339 | $3.43 | $4.44 | $-76.61 | $3,743.62 | ▼ -76.61 after sell → book $9,387.57; vs 09:30 mark -4.44 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 482 | $2.38 | $6.31 | $-80.01 | $4,884.47 | ▼ -80.01 after sell → book $9,381.26; vs 09:30 mark -6.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 181 | $6.57 | $2.57 | $-30.45 | $6,071.07 | ▼ -30.45 after sell → book $9,378.69; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 254 | $4.30 | $3.33 | $-128.53 | $7,159.94 | ▼ -128.53 after sell → book $9,375.36; vs 09:30 mark -3.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 107 | $11.22 | $2.34 | $-14.28 | $8,358.14 | ▼ -14.28 after sell → book $9,373.02; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $9,371.00 | ▼ -42.58 after sell → book $9,371.00; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,371.00 | ▲ close $9,371.00 vs 09:30 $9,396.18 (session +0.00) | 16:00 close · cash $9,371.00 · no lots left · equity $9,371.00. | — |

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
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BJ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1283.93 < 1 share @ 1746.53 |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ZJYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RCKT` | hard_red | hard-red S=-5.85 sit; no new buys |
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
