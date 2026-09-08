# Factor mine action — `union_blue_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-4.02%** ($9,598) · signal-only (no cash/fees) was +1.99%. Starts YES **0/18**. Fills 150 · skips 47 · realized $-306.21.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $39.50.

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
| 2026-08-21 | `MRVI` | 155 | — | $8.20 | +0.00 | $8.70 | +77.50 | +77.50 | +0.00 | +77.50 |
| 2026-08-21 | `DE` | 2 | — | $623.26 | +0.00 | $647.47 | +48.42 | +48.42 | +0.00 | +48.42 |
| 2026-08-21 | `WOLF` | 47 | — | $26.86 | +0.00 | $25.76 | -51.70 | -51.70 | +0.00 | -51.70 |
| 2026-08-21 | `AMRC` | 56 | — | $22.51 | +0.00 | $21.38 | -63.28 | -63.28 | +0.00 | -63.28 |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.79 | -14.91 | — | +0.00 | -14.91 | -19.53 | — |
| 2026-08-24 | `FUTU` | 11 | $123.64 | $120.87 | -30.47 | — | +0.00 | -30.47 | +62.59 | — |
| 2026-08-24 | `GMAB` | 38 | $33.45 | $32.82 | -23.94 | — | +0.00 | -23.94 | -20.52 | — |
| 2026-08-24 | `BTBT` | 766 | $1.53 | $1.55 | +15.32 | — | +0.00 | +15.32 | -84.26 | — |
| 2026-08-24 | `MRVI` | 155 | $8.70 | $8.59 | -17.05 | — | +0.00 | -17.05 | +60.45 | — |
| 2026-08-24 | `DE` | 2 | $647.47 | $653.62 | +12.30 | — | +0.00 | +12.30 | +60.72 | — |
| 2026-08-24 | `WOLF` | 47 | $25.76 | $25.07 | -32.43 | — | +0.00 | -32.43 | -84.13 | — |
| 2026-08-24 | `AMRC` | 56 | $21.38 | $21.19 | -10.64 | — | +0.00 | -10.64 | -73.92 | — |
| 2026-08-25 | `INSP` | 20 | — | $61.47 | +0.00 | $61.47 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CRMD` | 151 | — | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `BMEA` | 773 | — | $1.62 | +0.00 | $1.61 | -7.73 | -7.73 | +0.00 | -7.73 |
| 2026-08-25 | `NPWR` | 626 | — | $2.00 | +0.00 | $2.02 | +12.52 | +12.52 | +0.00 | +12.52 |
| 2026-08-25 | `PUSA` | 338 | — | $3.70 | +0.00 | $3.91 | +70.98 | +70.98 | +0.00 | +70.98 |
| 2026-08-25 | `ALVO` | 240 | — | $5.22 | +0.00 | $5.25 | +7.20 | +7.20 | +0.00 | +7.20 |
| 2026-08-25 | `CAPR` | 184 | — | $6.79 | +0.00 | $7.19 | +73.60 | +73.60 | +0.00 | +73.60 |
| 2026-08-25 | `ALIT` | 84 | — | $14.86 | +0.00 | $14.87 | +0.84 | +0.84 | +0.00 | +0.84 |
| 2026-08-26 | `INSP` | 20 | $61.47 | $61.47 | +0.00 | $61.47 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `CRMD` | 151 | $8.28 | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `BMEA` | 773 | $1.61 | $1.61 | +0.00 | $1.61 | +0.00 | +0.00 | -7.73 | -7.73 |
| 2026-08-26 | `NPWR` | 626 | $2.02 | $2.02 | +0.00 | $2.02 | +0.00 | +0.00 | +12.52 | +12.52 |
| 2026-08-26 | `PUSA` | 338 | $3.91 | $3.91 | +0.00 | $3.91 | +0.00 | +0.00 | +70.98 | +70.98 |
| 2026-08-26 | `ALVO` | 240 | $5.25 | $5.25 | +0.00 | $5.25 | +0.00 | +0.00 | +7.20 | +7.20 |
| 2026-08-26 | `CAPR` | 184 | $7.19 | $7.19 | +0.00 | $7.19 | +0.00 | +0.00 | +73.60 | +73.60 |
| 2026-08-26 | `ALIT` | 84 | $14.87 | $14.87 | +0.00 | $14.87 | +0.00 | +0.00 | +0.84 | +0.84 |
| 2026-08-27 | `INSP` | 20 | $61.47 | $60.07 | -28.00 | — | +0.00 | -28.00 | -28.00 | — |
| 2026-08-27 | `CRMD` | 151 | $8.28 | $8.60 | +48.32 | — | +0.00 | +48.32 | +48.32 | — |
| 2026-08-27 | `BMEA` | 773 | $1.61 | $1.75 | +108.22 | — | +0.00 | +108.22 | +100.49 | — |
| 2026-08-27 | `NPWR` | 626 | $2.02 | $1.93 | -56.34 | — | +0.00 | -56.34 | -43.82 | — |
| 2026-08-27 | `PUSA` | 338 | $3.91 | $3.84 | -23.66 | — | +0.00 | -23.66 | +47.32 | — |
| 2026-08-27 | `ALVO` | 240 | $5.25 | $4.98 | -64.80 | — | +0.00 | -64.80 | -57.60 | — |
| 2026-08-27 | `CAPR` | 184 | $7.19 | $8.29 | +202.40 | — | +0.00 | +202.40 | +276.00 | — |
| 2026-08-27 | `ALIT` | 84 | $14.87 | $14.85 | -1.68 | — | +0.00 | -1.68 | -0.84 | — |
| 2026-08-27 | `ACMR` | 15 | — | $80.97 | +0.00 | $79.11 | -27.90 | -27.90 | +0.00 | -27.90 |
| 2026-08-27 | `MU` | 1 | — | $925.74 | +0.00 | $938.40 | +12.66 | +12.66 | +0.00 | +12.66 |
| 2026-08-27 | `ANET` | 6 | — | $190.90 | +0.00 | $202.25 | +68.10 | +68.10 | +0.00 | +68.10 |
| 2026-08-27 | `DLO` | 82 | — | $15.60 | +0.00 | $15.36 | -19.68 | -19.68 | +0.00 | -19.68 |
| 2026-08-27 | `GEN` | 44 | — | $28.89 | +0.00 | $29.64 | +33.00 | +33.00 | +0.00 | +33.00 |
| 2026-08-27 | `LRCX` | 4 | — | $314.61 | +0.00 | $312.88 | -6.92 | -6.92 | +0.00 | -6.92 |
| 2026-08-27 | `MRVL` | 5 | — | $240.00 | +0.00 | $245.11 | +25.55 | +25.55 | +0.00 | +25.55 |
| 2026-08-28 | `ACMR` | 15 | $79.11 | $81.65 | +38.10 | — | +0.00 | +38.10 | +10.20 | — |
| 2026-08-28 | `MU` | 1 | $938.40 | $967.01 | +28.61 | — | +0.00 | +28.61 | +41.27 | — |
| 2026-08-28 | `ANET` | 6 | $202.25 | $205.90 | +21.90 | — | +0.00 | +21.90 | +90.00 | — |
| 2026-08-28 | `DLO` | 82 | $15.36 | $15.33 | -2.46 | — | +0.00 | -2.46 | -22.14 | — |
| 2026-08-28 | `GEN` | 44 | $29.64 | $29.83 | +8.36 | — | +0.00 | +8.36 | +41.36 | — |
| 2026-08-28 | `LRCX` | 4 | $312.88 | $318.88 | +24.00 | — | +0.00 | +24.00 | +17.08 | — |
| 2026-08-28 | `MRVL` | 5 | $245.11 | $253.44 | +41.65 | — | +0.00 | +41.65 | +67.20 | — |
| 2026-08-28 | `ANF` | 9 | — | $144.70 | +0.00 | $145.75 | +9.45 | +9.45 | +0.00 | +9.45 |
| 2026-08-28 | `SEDG` | 38 | — | $33.78 | +0.00 | $33.51 | -10.26 | -10.26 | +0.00 | -10.26 |
| 2026-08-28 | `SMTC` | 8 | — | $149.40 | +0.00 | $142.43 | -55.76 | -55.76 | +0.00 | -55.76 |
| 2026-08-28 | `GRRR` | 82 | — | $15.94 | +0.00 | $15.66 | -22.96 | -22.96 | +0.00 | -22.96 |
| 2026-08-28 | `URBN` | 15 | — | $82.70 | +0.00 | $78.79 | -58.65 | -58.65 | +0.00 | -58.65 |
| 2026-08-28 | `VYX` | 146 | — | $8.95 | +0.00 | $9.18 | +33.58 | +33.58 | +0.00 | +33.58 |
| 2026-08-28 | `TTMI` | 10 | — | $127.07 | +0.00 | $124.73 | -23.40 | -23.40 | +0.00 | -23.40 |
| 2026-08-28 | `NVRI` | 56 | — | $23.11 | +0.00 | $22.47 | -35.84 | -35.84 | +0.00 | -35.84 |
| 2026-08-31 | `ANF` | 9 | $145.75 | $148.67 | +26.28 | — | +0.00 | +26.28 | +35.73 | — |
| 2026-08-31 | `SEDG` | 38 | $33.51 | $31.50 | -76.38 | — | +0.00 | -76.38 | -86.64 | — |
| 2026-08-31 | `SMTC` | 8 | $142.43 | $133.04 | -75.12 | — | +0.00 | -75.12 | -130.88 | — |
| 2026-08-31 | `GRRR` | 82 | $15.66 | $14.32 | -109.88 | — | +0.00 | -109.88 | -132.84 | — |
| 2026-08-31 | `URBN` | 15 | $78.79 | $81.09 | +34.50 | — | +0.00 | +34.50 | -24.15 | — |
| 2026-08-31 | `VYX` | 146 | $9.18 | $9.06 | -17.52 | — | +0.00 | -17.52 | +16.06 | — |
| 2026-08-31 | `TTMI` | 10 | $124.73 | $117.20 | -75.30 | — | +0.00 | -75.30 | -98.70 | — |
| 2026-08-31 | `NVRI` | 56 | $22.47 | $22.28 | -10.64 | — | +0.00 | -10.64 | -46.48 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `HRMY` | 30 | — | $41.31 | +0.00 | $42.86 | +46.50 | +46.50 | +0.00 | +46.50 |
| 2026-09-03 | `VSTM` | 162 | — | $7.70 | +0.00 | $8.02 | +51.84 | +51.84 | +0.00 | +51.84 |
| 2026-09-03 | `RVTY` | 9 | — | $125.94 | +0.00 | $130.94 | +45.00 | +45.00 | +0.00 | +45.00 |
| 2026-09-03 | `CRK` | 79 | — | $15.70 | +0.00 | $15.54 | -12.64 | -12.64 | +0.00 | -12.64 |
| 2026-09-03 | `MMED` | 54 | — | $22.78 | +0.00 | $23.76 | +52.92 | +52.92 | +0.00 | +52.92 |
| 2026-09-03 | `CTMX` | 336 | — | $3.72 | +0.00 | $3.72 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `CRDL` | 579 | — | $2.16 | +0.00 | $2.17 | +5.79 | +5.79 | +0.00 | +5.79 |
| 2026-09-03 | `CLYM` | 84 | — | $14.79 | +0.00 | $15.05 | +21.84 | +21.84 | +0.00 | +21.84 |
| 2026-09-04 | `HRMY` | 30 | $42.86 | $42.93 | +2.10 | — | +0.00 | +2.10 | +48.60 | — |
| 2026-09-04 | `VSTM` | 162 | $8.02 | $8.03 | +1.62 | — | +0.00 | +1.62 | +53.46 | — |
| 2026-09-04 | `RVTY` | 9 | $130.94 | $132.45 | +13.59 | — | +0.00 | +13.59 | +58.59 | — |
| 2026-09-04 | `CRK` | 79 | $15.54 | $15.45 | -7.11 | — | +0.00 | -7.11 | -19.75 | — |
| 2026-09-04 | `MMED` | 54 | $23.76 | $23.88 | +6.48 | — | +0.00 | +6.48 | +59.40 | — |
| 2026-09-04 | `CTMX` | 336 | $3.72 | $3.73 | +3.36 | — | +0.00 | +3.36 | +3.36 | — |
| 2026-09-04 | `CRDL` | 579 | $2.17 | $2.18 | +5.79 | — | +0.00 | +5.79 | +11.58 | — |
| 2026-09-04 | `CLYM` | 84 | $15.05 | $13.96 | -91.56 | — | +0.00 | -91.56 | -69.72 | — |
| 2026-09-04 | `OSCR` | 41 | — | $30.65 | +0.00 | $32.24 | +65.19 | +65.19 | +0.00 | +65.19 |
| 2026-09-04 | `BVS` | 87 | — | $14.50 | +0.00 | $14.36 | -12.18 | -12.18 | +0.00 | -12.18 |
| 2026-09-04 | `GPRO` | 709 | — | $1.78 | +0.00 | $1.39 | -276.51 | -276.51 | +0.00 | -276.51 |
| 2026-09-04 | `EOSE` | 353 | — | $3.57 | +0.00 | $3.50 | -24.71 | -24.71 | +0.00 | -24.71 |
| 2026-09-04 | `SLBT` | 411 | — | $3.07 | +0.00 | $3.15 | +32.88 | +32.88 | +0.00 | +32.88 |
| 2026-09-04 | `MLYS` | 43 | — | $29.15 | +0.00 | $28.27 | -37.84 | -37.84 | +0.00 | -37.84 |
| 2026-09-04 | `CCOI` | 123 | — | $10.22 | +0.00 | $9.98 | -29.52 | -29.52 | +0.00 | -29.52 |
| 2026-09-04 | `SION` | 172 | — | $7.31 | +0.00 | $6.75 | -96.32 | -96.32 | +0.00 | -96.32 |
| 2026-09-07 | `OSCR` | 41 | $32.24 | $32.19 | -2.05 | — | +0.00 | -2.05 | +63.14 | — |
| 2026-09-07 | `BVS` | 87 | $14.36 | $14.11 | -21.75 | — | +0.00 | -21.75 | -33.93 | — |
| 2026-09-07 | `GPRO` | 709 | $1.39 | $1.48 | +63.81 | — | +0.00 | +63.81 | -212.70 | — |
| 2026-09-07 | `EOSE` | 353 | $3.50 | $3.52 | +7.06 | — | +0.00 | +7.06 | -17.65 | — |
| 2026-09-07 | `SLBT` | 411 | $3.15 | $3.15 | +0.00 | — | +0.00 | +0.00 | +32.88 | — |
| 2026-09-07 | `MLYS` | 43 | $28.27 | $28.00 | -11.61 | — | +0.00 | -11.61 | -49.45 | — |
| 2026-09-07 | `CCOI` | 123 | $9.98 | $10.02 | +4.92 | — | +0.00 | +4.92 | -24.60 | — |
| 2026-09-07 | `SION` | 172 | $6.75 | $6.68 | -12.04 | — | +0.00 | -12.04 | -108.36 | — |
| 2026-09-07 | `BSX` | 25 | — | $46.92 | +0.00 | $47.80 | +22.00 | +22.00 | +0.00 | +22.00 |
| 2026-09-07 | `UGP` | 168 | — | $7.18 | +0.00 | $7.28 | +16.80 | +16.80 | +0.00 | +16.80 |
| 2026-09-07 | `LPG` | 22 | — | $53.90 | +0.00 | $55.20 | +28.60 | +28.60 | +0.00 | +28.60 |
| 2026-09-07 | `CHPT` | 130 | — | $9.28 | +0.00 | $9.89 | +79.30 | +79.30 | +0.00 | +79.30 |
| 2026-09-07 | `ABTC` | 135 | — | $8.95 | +0.00 | $7.99 | -129.60 | -129.60 | +0.00 | -129.60 |
| 2026-09-07 | `CHGG` | 1275 | — | $0.95 | +0.00 | $0.85 | -127.50 | -127.50 | +0.00 | -127.50 |
| 2026-09-07 | `MRLN` | 369 | — | $3.28 | +0.00 | $3.35 | +25.83 | +25.83 | +0.00 | +25.83 |
| 2026-09-07 | `BTBT` | 757 | — | $1.60 | +0.00 | $1.64 | +30.28 | +30.28 | +0.00 | +30.28 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +91.20 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | — | $560.20 | $10,051.46 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 |
| 2026-08-17 | +2.25 | $560.20 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | $10,054.84 | +3.38 | -123.94 | DVN, EOG, FANG, TMC, TGB, ABX, ALM, INV | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $140.13 | $9,863.96 | DVN×27, EOG×8, FANG×6, TMC×309, TGB×147, ABX×137, ALM×77, INV×772 |
| 2026-08-18 | -6.20 | $140.13 | DVN×27, EOG×8, FANG×6, TMC×309, TGB×147, ABX×137, ALM×77, INV×772 | $9,755.43 | -108.53 | +0.00 | — | DVN, EOG, FANG, TMC, TGB, ABX, ALM, INV | $9,727.99 | $9,727.99 | — |
| 2026-08-19 | -7.20 | $9,727.99 | — | $9,727.99 | +0.00 | +0.00 | — | — | $9,727.99 | $9,727.99 | — |
| 2026-08-20 | +1.12 | $9,727.99 | — | $9,727.99 | +0.00 | +165.32 | AG, BHP, HDSN, IAG, KGC, NFGC, WPM, ABUS | — | $97.05 | $9,867.97 | AG×59, BHP×13, HDSN×210, IAG×61, KGC×41, NFGC×694, WPM×8, ABUS×247 |
| 2026-08-21 | +3.25 | $97.05 | AG×59, BHP×13, HDSN×210, IAG×61, KGC×41, NFGC×694, WPM×8, ABUS×247 | $10,198.81 | +330.84 | +3.22 | CRSP, FUTU, GMAB, BTBT, MRVI, DE, WOLF, AMRC | AG, BHP, HDSN, IAG, KGC, NFGC, WPM, ABUS | $47.50 | $10,151.56 | CRSP×21, FUTU×11, GMAB×38, BTBT×766, MRVI×155, DE×2, WOLF×47, AMRC×56 |
| 2026-08-24 | -5.17 | $47.50 | CRSP×21, FUTU×11, GMAB×38, BTBT×766, MRVI×155, DE×2, WOLF×47, AMRC×56 | $10,049.74 | -101.82 | +0.00 | — | CRSP, FUTU, GMAB, BTBT, MRVI, DE, WOLF, AMRC | $10,024.64 | $10,024.64 | — |
| 2026-08-25 | +1.80 | $10,024.64 | — | $10,024.64 | +0.00 | +157.41 | INSP, CRMD, BMEA, NPWR, PUSA, ALVO, CAPR, ALIT | — | $4.92 | $10,147.27 | INSP×20, CRMD×151, BMEA×773, NPWR×626, PUSA×338, ALVO×240, CAPR×184, ALIT×84 |
| 2026-08-26 | +2.02 | $4.92 | INSP×20, CRMD×151, BMEA×773, NPWR×626, PUSA×338, ALVO×240, CAPR×184, ALIT×84 | $10,147.27 | +0.00 | +0.00 | — | — | $4.92 | $10,147.27 | INSP×20, CRMD×151, BMEA×773, NPWR×626, PUSA×338, ALVO×240, CAPR×184, ALIT×84 |
| 2026-08-27 | — | $4.92 | INSP×20, CRMD×151, BMEA×773, NPWR×626, PUSA×338, ALVO×240, CAPR×184, ALIT×84 | $10,331.73 | +184.46 | +84.81 | ACMR, MU, ANET, DLO, GEN, LRCX, MRVL | INSP, CRMD, BMEA, NPWR, PUSA, ALVO, CAPR, ALIT | $1,987.57 | $10,366.87 | ACMR×15, MU×1, ANET×6, DLO×82, GEN×44, LRCX×4, MRVL×5 |
| 2026-08-28 | +0.75 | $1,987.57 | ACMR×15, MU×1, ANET×6, DLO×82, GEN×44, LRCX×4, MRVL×5 | $10,527.03 | +160.16 | -163.84 | ANF, SEDG, SMTC, GRRR, URBN, VYX, TTMI, NVRI | ACMR, MU, ANET, DLO, GEN, LRCX, MRVL | $295.19 | $10,331.63 | ANF×9, SEDG×38, SMTC×8, GRRR×82, URBN×15, VYX×146, TTMI×10, NVRI×56 |
| 2026-08-31 | -5.85 | $295.19 | ANF×9, SEDG×38, SMTC×8, GRRR×82, URBN×15, VYX×146, TTMI×10, NVRI×56 | $10,027.57 | -304.06 | +0.00 | — | ANF, SEDG, SMTC, GRRR, URBN, VYX, TTMI, NVRI | $10,010.38 | $10,010.38 | — |
| 2026-09-01 | -6.30 | $10,010.38 | — | $10,010.38 | +0.00 | +0.00 | — | — | $10,010.38 | $10,010.38 | — |
| 2026-09-02 | -3.83 | $10,010.38 | — | $10,010.38 | +0.00 | +0.00 | — | — | $10,010.38 | $10,010.38 | — |
| 2026-09-03 | -0.90 | $10,010.38 | — | $10,010.38 | +0.00 | +211.25 | HRMY, VSTM, RVTY, CRK, MMED, CTMX, CRDL, CLYM | — | $151.89 | $10,196.64 | HRMY×30, VSTM×162, RVTY×9, CRK×79, MMED×54, CTMX×336, CRDL×579, CLYM×84 |
| 2026-09-04 | — | $151.89 | HRMY×30, VSTM×162, RVTY×9, CRK×79, MMED×54, CTMX×336, CRDL×579, CLYM×84 | $10,130.91 | -65.73 | -379.01 | OSCR, BVS, GPRO, EOSE, SLBT, MLYS, CCOI, SION | HRMY, VSTM, RVTY, CRK, MMED, CTMX, CRDL, CLYM | $5.26 | $9,696.23 | OSCR×41, BVS×87, GPRO×709, EOSE×353, SLBT×411, MLYS×43, CCOI×123, SION×172 |
| 2026-09-07 | — | $5.26 | OSCR×41, BVS×87, GPRO×709, EOSE×353, SLBT×411, MLYS×43, CCOI×123, SION×172 | $9,724.57 | +28.34 | -54.29 | BSX, UGP, LPG, CHPT, ABTC, CHGG, MRLN, BTBT | OSCR, BVS, GPRO, EOSE, SLBT, MLYS, CCOI, SION | $39.50 | $9,597.67 | BSX×25, UGP×168, LPG×22, CHPT×130, ABTC×135, CHGG×1275, MRLN×369, BTBT×757 |

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
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 155 | $8.20 | $2.46 | — | $3,823.29 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1271.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $2,574.77 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1271.64 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `WOLF` | 47 | $26.86 | $2.13 | — | $1,310.22 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_mover; 🔵; ret5=-16.4; leftover $1271.64 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AMRC` | 56 | $22.51 | $2.16 | — | $47.50 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_mover; 🔵; ret5=-20.2; leftover $1271.64 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.50 | ▲ close $10,151.56 vs 09:30 $10,198.81 (session +3.22) | 16:00 close · cash $47.50 · equity $10,151.56 vs 09:30 $10,198.81 (-47.25; session marks +3.22) · 8 name(s) marked open→close (per-name table). CRSP×21 09:30 $59.72 → close $59.50 -4.62; FUTU×11 09:30 $115.18 → close $123.64 +93.06; GMAB×38 09:30 $33.36 → close $33.45 +3.42; BTBT×766 09:30 $1.66 → close $1.53 -99.58; MRVI×155 09:30 $8.20 → close $8.70 +77.50; DE×2 09:30 $623.26 → close $647.47 +48.42; WOLF×47 09:30 $26.86 → close $25.76 -51.70; AMRC×56 09:30 $22.51 → close $21.38 -63.28 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.50 | ▼ 09:30 equity $10,049.74 vs yday $10,151.56 (-101.82) | 09:30 open · cash $47.50 (unchanged overnight, no fees) · equity $10,049.74 vs prior close $10,151.56 (-101.82) · 8 name(s) re-marked at the open (per-name table). CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; FUTU×11 yday $123.64 → 09:30 $120.87 -30.47; GMAB×38 yday $33.45 → 09:30 $32.82 -23.94; BTBT×766 yday $1.53 → 09:30 $1.55 +15.32; MRVI×155 yday $8.70 → 09:30 $8.59 -17.05; DE×2 yday $647.47 → 09:30 $653.62 +12.30; WOLF×47 yday $25.76 → 09:30 $25.07 -32.43; AMRC×56 yday $21.38 → 09:30 $21.19 -10.64 | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.79 | $2.07 | $-23.66 | $1,280.02 | ▼ -23.66 after sell → book $10,047.67; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $120.87 | $2.04 | $+58.52 | $2,607.54 | ▲ +58.52 after sell → book $10,045.62; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 38 | $32.82 | $2.12 | $-24.75 | $3,852.58 | ▼ -24.75 after sell → book $10,043.50; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 766 | $1.55 | $10.02 | $-104.16 | $5,029.86 | ▼ -104.16 after sell → book $10,033.48; vs 09:30 mark -10.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 155 | $8.59 | $2.49 | $+55.50 | $6,358.82 | ▲ +55.50 after sell → book $10,030.99; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.62 | $2.02 | $+56.71 | $7,664.04 | ▲ +56.71 after sell → book $10,028.97; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WOLF` | 47 | $25.07 | $2.15 | $-88.41 | $8,840.18 | ▼ -88.41 after sell → book $10,026.82; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AMRC` | 56 | $21.19 | $2.18 | $-78.26 | $10,024.64 | ▼ -78.26 after sell → book $10,024.64; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,024.64 | ▲ close $10,024.64 vs 09:30 $10,049.74 (session +0.00) | 16:00 close · cash $10,024.64 · no lots left · equity $10,024.64. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,024.64 | ▲ 09:30 equity $10,024.64 vs yday $10,024.64 (+0.00) | 09:30 open · cash $10,024.64 · no holdings · equity $10,024.64 vs prior close $10,024.64 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 20 | $61.47 | $2.05 | — | $8,793.19 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+9.2; leftover $1253.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 151 | $8.28 | $2.44 | — | $7,540.47 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1253.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 773 | $1.62 | $9.97 | — | $6,278.24 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1253.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 626 | $2.00 | $8.08 | — | $5,018.16 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1253.08 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 338 | $3.70 | $4.36 | — | $3,763.20 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1253.08 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 240 | $5.22 | $3.10 | — | $2,507.31 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1253.08 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 184 | $6.79 | $2.54 | — | $1,255.41 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1253.08 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 84 | $14.86 | $2.24 | — | $4.92 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1253.08 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.92 | ▲ close $10,147.27 vs 09:30 $10,024.64 (session +157.41) | 16:00 close · cash $4.92 · equity $10,147.27 vs 09:30 $10,024.64 (+122.63; session marks +157.41) · 8 name(s) marked open→close (per-name table). INSP×20 09:30 $61.47 → close $61.47 +0.00; CRMD×151 09:30 $8.28 → close $8.28 +0.00; BMEA×773 09:30 $1.62 → close $1.61 -7.73; NPWR×626 09:30 $2.00 → close $2.02 +12.52; PUSA×338 09:30 $3.70 → close $3.91 +70.98; ALVO×240 09:30 $5.22 → close $5.25 +7.20; CAPR×184 09:30 $6.79 → close $7.19 +73.60; ALIT×84 09:30 $14.86 → close $14.87 +0.84 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.92 | ▲ 09:30 equity $10,147.27 vs yday $10,147.27 (+0.00) | 09:30 open · cash $4.92 (unchanged overnight, no fees) · equity $10,147.27 vs prior close $10,147.27 (+0.00) · 8 name(s) re-marked at the open (per-name table). INSP×20 yday $61.47 → 09:30 $61.47 +0.00; CRMD×151 yday $8.28 → 09:30 $8.28 +0.00; BMEA×773 yday $1.61 → 09:30 $1.61 +0.00; NPWR×626 yday $2.02 → 09:30 $2.02 +0.00; PUSA×338 yday $3.91 → 09:30 $3.91 +0.00; ALVO×240 yday $5.25 → 09:30 $5.25 +0.00; CAPR×184 yday $7.19 → 09:30 $7.19 +0.00; ALIT×84 yday $14.87 → 09:30 $14.87 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.92 | ▲ close $10,147.27 vs 09:30 $10,147.27 (session +0.00) | 16:00 close · cash $4.92 · equity $10,147.27 vs 09:30 $10,147.27 (+0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). INSP×20 09:30 $61.47 → close $61.47 +0.00; CRMD×151 09:30 $8.28 → close $8.28 +0.00; BMEA×773 09:30 $1.61 → close $1.61 +0.00; NPWR×626 09:30 $2.02 → close $2.02 +0.00; PUSA×338 09:30 $3.91 → close $3.91 +0.00; ALVO×240 09:30 $5.25 → close $5.25 +0.00; CAPR×184 09:30 $7.19 → close $7.19 +0.00; ALIT×84 09:30 $14.87 → close $14.87 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.92 | ▲ 09:30 equity $10,331.73 vs yday $10,147.27 (+184.46) | 09:30 open · cash $4.92 (unchanged overnight, no fees) · equity $10,331.73 vs prior close $10,147.27 (+184.46) · 8 name(s) re-marked at the open (per-name table). INSP×20 yday $61.47 → 09:30 $60.07 -28.00; CRMD×151 yday $8.28 → 09:30 $8.60 +48.32; BMEA×773 yday $1.61 → 09:30 $1.75 +108.22; NPWR×626 yday $2.02 → 09:30 $1.93 -56.34; PUSA×338 yday $3.91 → 09:30 $3.84 -23.66; ALVO×240 yday $5.25 → 09:30 $4.98 -64.80; CAPR×184 yday $7.19 → 09:30 $8.29 +202.40; ALIT×84 yday $14.87 → 09:30 $14.85 -1.68 | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 20 | $60.07 | $2.07 | $-32.12 | $1,204.25 | ▼ -32.12 after sell → book $10,329.66; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 151 | $8.60 | $2.48 | $+43.40 | $2,500.38 | ▲ +43.40 after sell → book $10,327.19; vs 09:30 mark -2.47 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 773 | $1.75 | $10.11 | $+80.41 | $3,843.02 | ▲ +80.41 after sell → book $10,317.08; vs 09:30 mark -10.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 626 | $1.93 | $8.19 | $-60.08 | $5,043.01 | ▼ -60.08 after sell → book $10,308.89; vs 09:30 mark -8.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PUSA` | 338 | $3.84 | $4.43 | $+38.53 | $6,336.50 | ▲ +38.53 after sell → book $10,304.46; vs 09:30 mark -4.43 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 240 | $4.98 | $3.15 | $-63.84 | $7,528.55 | ▼ -63.84 after sell → book $10,301.31; vs 09:30 mark -3.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 184 | $8.29 | $2.58 | $+270.87 | $9,051.33 | ▲ +270.87 after sell → book $10,298.73; vs 09:30 mark -2.58 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALIT` | 84 | $14.85 | $2.27 | $-5.35 | $10,296.46 | ▼ -5.35 after sell → book $10,296.46; vs 09:30 mark -2.27 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 15 | $80.97 | $2.04 | — | $9,079.88 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=-1.3; leftover $1287.06 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $8,152.14 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=-0.5; leftover $1287.06 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $190.90 | $2.01 | — | $7,004.74 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=-5.1; leftover $1287.06 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 82 | $15.60 | $2.24 | — | $5,723.30 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=+7.1; leftover $1287.06 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 44 | $28.89 | $2.12 | — | $4,450.02 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=+1.6; leftover $1287.06 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 4 | $314.61 | $2.00 | — | $3,189.58 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=-5.5; leftover $1287.06 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $240.00 | $2.00 | — | $1,987.57 | — | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=+6.8; leftover $1287.06 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,987.57 | ▲ close $10,366.87 vs 09:30 $10,331.73 (session +84.81) | 16:00 close · cash $1,987.57 · equity $10,366.87 vs 09:30 $10,331.73 (+35.14; session marks +84.81) · 7 name(s) marked open→close (per-name table). ACMR×15 09:30 $80.97 → close $79.11 -27.90; MU×1 09:30 $925.74 → close $938.40 +12.66; ANET×6 09:30 $190.90 → close $202.25 +68.10; DLO×82 09:30 $15.60 → close $15.36 -19.68; GEN×44 09:30 $28.89 → close $29.64 +33.00; LRCX×4 09:30 $314.61 → close $312.88 -6.92; MRVL×5 09:30 $240.00 → close $245.11 +25.55 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,987.57 | ▲ 09:30 equity $10,527.03 vs yday $10,366.87 (+160.16) | 09:30 open · cash $1,987.57 (unchanged overnight, no fees) · equity $10,527.03 vs prior close $10,366.87 (+160.16) · 7 name(s) re-marked at the open (per-name table). ACMR×15 yday $79.11 → 09:30 $81.65 +38.10; MU×1 yday $938.40 → 09:30 $967.01 +28.61; ANET×6 yday $202.25 → 09:30 $205.90 +21.90; DLO×82 yday $15.36 → 09:30 $15.33 -2.46; GEN×44 yday $29.64 → 09:30 $29.83 +8.36; LRCX×4 yday $312.88 → 09:30 $318.88 +24.00; MRVL×5 yday $245.11 → 09:30 $253.44 +41.65 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 15 | $81.65 | $2.06 | $+6.11 | $3,210.27 | ▲ +6.11 after sell → book $10,524.98; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $4,175.26 | ▲ +37.26 after sell → book $10,522.96; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $205.90 | $2.03 | $+85.96 | $5,408.64 | ▲ +85.96 after sell → book $10,520.94; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 82 | $15.33 | $2.26 | $-26.64 | $6,663.44 | ▼ -26.64 after sell → book $10,518.68; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 44 | $29.83 | $2.14 | $+37.10 | $7,973.81 | ▲ +37.10 after sell → book $10,516.53; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 4 | $318.88 | $2.02 | $+13.06 | $9,247.31 | ▲ +13.06 after sell → book $10,514.51; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $253.44 | $2.03 | $+63.17 | $10,512.49 | ▲ +63.17 after sell → book $10,512.49; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $9,208.17 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1314.06 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 38 | $33.78 | $2.10 | — | $7,922.43 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1314.06 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $6,725.21 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1314.06 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 82 | $15.94 | $2.24 | — | $5,415.90 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1314.06 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 15 | $82.70 | $2.04 | — | $4,173.36 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $1314.06 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 146 | $8.95 | $2.43 | — | $2,864.23 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=-3.1; leftover $1314.06 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $127.07 | $2.02 | — | $1,591.51 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $1314.06 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NVRI` | 56 | $23.11 | $2.16 | — | $295.19 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=+0.3; leftover $1314.06 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $295.19 | ▼ close $10,331.63 vs 09:30 $10,527.03 (session -163.84) | 16:00 close · cash $295.19 · equity $10,331.63 vs 09:30 $10,527.03 (-195.40; session marks -163.84) · 8 name(s) marked open→close (per-name table). ANF×9 09:30 $144.70 → close $145.75 +9.45; SEDG×38 09:30 $33.78 → close $33.51 -10.26; SMTC×8 09:30 $149.40 → close $142.43 -55.76; GRRR×82 09:30 $15.94 → close $15.66 -22.96; URBN×15 09:30 $82.70 → close $78.79 -58.65; VYX×146 09:30 $8.95 → close $9.18 +33.58; TTMI×10 09:30 $127.07 → close $124.73 -23.40; NVRI×56 09:30 $23.11 → close $22.47 -35.84 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $295.19 | ▼ 09:30 equity $10,027.57 vs yday $10,331.63 (-304.06) | 09:30 open · cash $295.19 (unchanged overnight, no fees) · equity $10,027.57 vs prior close $10,331.63 (-304.06) · 8 name(s) re-marked at the open (per-name table). ANF×9 yday $145.75 → 09:30 $148.67 +26.28; SEDG×38 yday $33.51 → 09:30 $31.50 -76.38; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; GRRR×82 yday $15.66 → 09:30 $14.32 -109.88; URBN×15 yday $78.79 → 09:30 $81.09 +34.50; VYX×146 yday $9.18 → 09:30 $9.06 -17.52; TTMI×10 yday $124.73 → 09:30 $117.20 -75.30; NVRI×56 yday $22.47 → 09:30 $22.28 -10.64 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.67 | $2.04 | $+31.68 | $1,631.19 | ▲ +31.68 after sell → book $10,025.54; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 38 | $31.50 | $2.12 | $-90.87 | $2,826.06 | ▼ -90.87 after sell → book $10,023.41; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $133.04 | $2.03 | $-134.93 | $3,888.35 | ▼ -134.93 after sell → book $10,021.38; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 82 | $14.32 | $2.26 | $-137.34 | $5,060.33 | ▼ -137.34 after sell → book $10,019.12; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 15 | $81.09 | $2.06 | $-28.24 | $6,274.62 | ▼ -28.24 after sell → book $10,017.06; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 146 | $9.06 | $2.46 | $+11.17 | $7,594.92 | ▲ +11.17 after sell → book $10,014.60; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 10 | $117.20 | $2.04 | $-102.76 | $8,764.88 | ▼ -102.76 after sell → book $10,012.56; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NVRI` | 56 | $22.28 | $2.18 | $-50.82 | $10,010.38 | ▼ -50.82 after sell → book $10,010.38; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,010.38 | ▲ close $10,010.38 vs 09:30 $10,027.57 (session +0.00) | 16:00 close · cash $10,010.38 · no lots left · equity $10,010.38. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,010.38 | ▲ 09:30 equity $10,010.38 vs yday $10,010.38 (+0.00) | 09:30 open · cash $10,010.38 · no holdings · equity $10,010.38 vs prior close $10,010.38 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,010.38 | ▲ close $10,010.38 vs 09:30 $10,010.38 (session +0.00) | 16:00 close · cash $10,010.38 · no lots left · equity $10,010.38. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,010.38 | ▲ 09:30 equity $10,010.38 vs yday $10,010.38 (+0.00) | 09:30 open · cash $10,010.38 · no holdings · equity $10,010.38 vs prior close $10,010.38 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,010.38 | ▲ close $10,010.38 vs 09:30 $10,010.38 (session +0.00) | 16:00 close · cash $10,010.38 · no lots left · equity $10,010.38. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,010.38 | ▲ 09:30 equity $10,010.38 vs yday $10,010.38 (+0.00) | 09:30 open · cash $10,010.38 · no holdings · equity $10,010.38 vs prior close $10,010.38 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $41.31 | $2.08 | — | $8,769.00 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1251.30 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 162 | $7.70 | $2.48 | — | $7,519.13 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1251.30 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $6,383.65 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1251.30 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 79 | $15.70 | $2.23 | — | $5,141.12 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1251.30 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 54 | $22.78 | $2.15 | — | $3,908.85 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1251.30 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 336 | $3.72 | $4.33 | — | $2,654.60 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1251.30 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 579 | $2.16 | $7.47 | — | $1,396.49 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1251.30 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 84 | $14.79 | $2.24 | — | $151.89 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=+5.8; leftover $1251.30 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.89 | ▲ close $10,196.64 vs 09:30 $10,010.38 (session +211.25) | 16:00 close · cash $151.89 · equity $10,196.64 vs 09:30 $10,010.38 (+186.26; session marks +211.25) · 8 name(s) marked open→close (per-name table). HRMY×30 09:30 $41.31 → close $42.86 +46.50; VSTM×162 09:30 $7.70 → close $8.02 +51.84; RVTY×9 09:30 $125.94 → close $130.94 +45.00; CRK×79 09:30 $15.70 → close $15.54 -12.64; MMED×54 09:30 $22.78 → close $23.76 +52.92; CTMX×336 09:30 $3.72 → close $3.72 +0.00; CRDL×579 09:30 $2.16 → close $2.17 +5.79; CLYM×84 09:30 $14.79 → close $15.05 +21.84 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.89 | ▼ 09:30 equity $10,130.91 vs yday $10,196.64 (-65.73) | 09:30 open · cash $151.89 (unchanged overnight, no fees) · equity $10,130.91 vs prior close $10,196.64 (-65.73) · 8 name(s) re-marked at the open (per-name table). HRMY×30 yday $42.86 → 09:30 $42.93 +2.10; VSTM×162 yday $8.02 → 09:30 $8.03 +1.62; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; CRK×79 yday $15.54 → 09:30 $15.45 -7.11; MMED×54 yday $23.76 → 09:30 $23.88 +6.48; CTMX×336 yday $3.72 → 09:30 $3.73 +3.36; CRDL×579 yday $2.17 → 09:30 $2.18 +5.79; CLYM×84 yday $15.05 → 09:30 $13.96 -91.56 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 30 | $42.93 | $2.10 | $+44.42 | $1,437.69 | ▲ +44.42 after sell → book $10,128.81; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 162 | $8.03 | $2.51 | $+48.47 | $2,736.03 | ▲ +48.47 after sell → book $10,126.29; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $132.45 | $2.04 | $+54.54 | $3,926.04 | ▲ +54.54 after sell → book $10,124.25; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 79 | $15.45 | $2.25 | $-24.23 | $5,144.34 | ▼ -24.23 after sell → book $10,122.00; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 54 | $23.88 | $2.17 | $+55.08 | $6,431.69 | ▲ +55.08 after sell → book $10,119.83; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTMX` | 336 | $3.73 | $4.40 | $-5.37 | $7,680.57 | ▼ -5.37 after sell → book $10,115.43; vs 09:30 mark -4.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 579 | $2.18 | $7.58 | $-3.46 | $8,935.22 | ▼ -3.46 after sell → book $10,107.86; vs 09:30 mark -7.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CLYM` | 84 | $13.96 | $2.27 | $-74.23 | $10,105.59 | ▼ -74.23 after sell → book $10,105.59; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 41 | $30.65 | $2.11 | — | $8,846.83 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=-2.2; leftover $1263.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 87 | $14.50 | $2.25 | — | $7,583.08 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1263.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GPRO` | 709 | $1.78 | $9.15 | — | $6,311.91 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.9; leftover $1263.20 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 353 | $3.57 | $4.55 | — | $5,047.15 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1263.20 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 411 | $3.07 | $5.30 | — | $3,780.08 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1263.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 43 | $29.15 | $2.12 | — | $2,524.51 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1263.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 123 | $10.22 | $2.36 | — | $1,265.09 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1263.20 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SION` | 172 | $7.31 | $2.51 | — | $5.26 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1263.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.26 | ▼ close $9,696.23 vs 09:30 $10,130.91 (session -379.01) | 16:00 close · cash $5.26 · equity $9,696.23 vs 09:30 $10,130.91 (-434.68; session marks -379.01) · 8 name(s) marked open→close (per-name table). OSCR×41 09:30 $30.65 → close $32.24 +65.19; BVS×87 09:30 $14.50 → close $14.36 -12.18; GPRO×709 09:30 $1.78 → close $1.39 -276.51; EOSE×353 09:30 $3.57 → close $3.50 -24.71; SLBT×411 09:30 $3.07 → close $3.15 +32.88; MLYS×43 09:30 $29.15 → close $28.27 -37.84; CCOI×123 09:30 $10.22 → close $9.98 -29.52; SION×172 09:30 $7.31 → close $6.75 -96.32 | — |
| 2026-09-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.26 | ▲ 09:30 equity $9,724.57 vs yday $9,696.23 (+28.34) | 09:30 open · cash $5.26 (unchanged overnight, no fees) · equity $9,724.57 vs prior close $9,696.23 (+28.34) · 8 name(s) re-marked at the open (per-name table). OSCR×41 yday $32.24 → 09:30 $32.19 -2.05; BVS×87 yday $14.36 → 09:30 $14.11 -21.75; GPRO×709 yday $1.39 → 09:30 $1.48 +63.81; EOSE×353 yday $3.50 → 09:30 $3.52 +7.06; SLBT×411 yday $3.15 → 09:30 $3.15 +0.00; MLYS×43 yday $28.27 → 09:30 $28.00 -11.61; CCOI×123 yday $9.98 → 09:30 $10.02 +4.92; SION×172 yday $6.75 → 09:30 $6.68 -12.04 | — |
| 2026-09-07 09:30 ET | **SELL** | `OSCR` | 41 | $32.19 | $2.13 | $+58.89 | $1,322.92 | ▲ +58.89 after sell → book $9,722.44; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `BVS` | 87 | $14.11 | $2.28 | $-38.46 | $2,548.21 | ▼ -38.46 after sell → book $9,720.16; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `GPRO` | 709 | $1.48 | $9.27 | $-231.12 | $3,588.26 | ▼ -231.12 after sell → book $9,710.89; vs 09:30 mark -9.27 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **SELL** | `EOSE` | 353 | $3.52 | $4.62 | $-26.83 | $4,826.20 | ▼ -26.83 after sell → book $9,706.27; vs 09:30 mark -4.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `SLBT` | 411 | $3.15 | $5.38 | $+22.20 | $6,115.47 | ▲ +22.20 after sell → book $9,700.89; vs 09:30 mark -5.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `MLYS` | 43 | $28.00 | $2.14 | $-53.71 | $7,317.33 | ▼ -53.71 after sell → book $9,698.75; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `CCOI` | 123 | $10.02 | $2.39 | $-29.35 | $8,547.40 | ▼ -29.35 after sell → book $9,696.36; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `SION` | 172 | $6.68 | $2.54 | $-113.41 | $9,693.81 | ▼ -113.41 after sell → book $9,693.81; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **BUY** | `BSX` | 25 | $46.92 | $2.06 | — | $8,518.75 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=-2.8; leftover $1211.73 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `UGP` | 168 | $7.18 | $2.49 | — | $7,310.01 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+9.4; leftover $1211.73 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `LPG` | 22 | $53.90 | $2.06 | — | $6,122.16 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+7.9; leftover $1211.73 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `CHPT` | 130 | $9.28 | $2.38 | — | $4,913.38 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.1; leftover $1211.73 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `ABTC` | 135 | $8.95 | $2.40 | — | $3,702.73 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1211.73 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `CHGG` | 1275 | $0.95 | $15.94 | — | $2,475.55 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $1211.73 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `MRLN` | 369 | $3.28 | $4.76 | — | $1,260.47 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.6; leftover $1211.73 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `BTBT` | 757 | $1.60 | $9.77 | — | $39.50 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-2.5; leftover $1211.73 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.50 | ▼ close $9,597.67 vs 09:30 $9,724.57 (session -54.29) | 16:00 close · cash $39.50 · equity $9,597.67 vs 09:30 $9,724.57 (-126.90; session marks -54.29) · 8 name(s) marked open→close (per-name table). BSX×25 09:30 $46.92 → close $47.80 +22.00; UGP×168 09:30 $7.18 → close $7.28 +16.80; LPG×22 09:30 $53.90 → close $55.20 +28.60; CHPT×130 09:30 $9.28 → close $9.89 +79.30; ABTC×135 09:30 $8.95 → close $7.99 -129.60; CHGG×1275 09:30 $0.95 → close $0.85 -127.50; MRLN×369 09:30 $3.28 → close $3.35 +25.83; BTBT×757 09:30 $1.60 → close $1.64 +30.28 | — |

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
| 2026-08-26 | `INSP` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NPWR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `PUSA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALIT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `FLNC` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `BZ` | no_price | no 09:30 open |
| 2026-08-27 | `ASML` | cash | leftover split 1287.06 < 1 share @ 1746.33 |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SRPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ZJYL` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `BSX` | 25 | 2026-09-07 @ $46.92 | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=-2.8; leftover $1211.73 |
| `UGP` | 168 | 2026-09-07 @ $7.18 | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+9.4; leftover $1211.73 |
| `LPG` | 22 | 2026-09-07 @ $53.90 | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+7.9; leftover $1211.73 |
| `CHPT` | 130 | 2026-09-07 @ $9.28 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.1; leftover $1211.73 |
| `ABTC` | 135 | 2026-09-07 @ $8.95 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1211.73 |
| `CHGG` | 1275 | 2026-09-07 @ $0.95 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $1211.73 |
| `MRLN` | 369 | 2026-09-07 @ $3.28 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.6; leftover $1211.73 |
| `BTBT` | 757 | 2026-09-07 @ $1.60 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-2.5; leftover $1211.73 |
