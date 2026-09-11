# Factor mine action — `combo_hn_7030_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_hot_n4_h1/union_news_g_h1 w=0.7,0.3 net=priority

Cash book **+14.75%** ($11,475) · signal-only (no cash/fees) was —. Starts YES **11/20**. Fills 192 · skips 96 · realized $+1475.43.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_hot_n4_h1 70%, union_news_g_h1 30%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_hot_n4_h1 70%, union_news_g_h1 30%.
- Member: union_hot_n4_h1 (70% · long · hold 1).
- Member: union_news_g_h1 (30% · long · hold 1).
- Each lot remembers the owner kid, so that kid’s min-hold and list-drop rule apply. A hold-3 fresh-E lot is not sold because the heat kid only holds 1 day.

### When it buys

- At 09:30, each member runs its own pick_day on its own list and gates. Nobody mashes the names into one ranked list first.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- One ticker, one side. Claim order: fresh-E, then heat, then the other longs, then shorts. A name already held cannot be opened on the other side.
- Shared pile: leftover cash is offered in claim order (fresh-E, then heat, then other longs, then shorts). Each kid splits their room equally across *their* new names (leftover, whole shares, fees out of cash). Unused room spills to the next kid. A short fill adds cash; that cash can later fund a long, still capped by the cover rule (equity ≥ 2× notional).
- Skip a name if the slice cannot buy 1 share after fees.
- Skip a name if there is no official 09:30 open.
- Long lots buy shares (want the price up). Short lots borrow (want the price down) and are marked as a liability.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is the owner kid’s hold — the buy morning counts as 1.
- No extra panic button unless that owner recipe has one (🚨 / last-red / news🔴).
- List-drop: after the owner’s min-hold, sell at the 09:30 open if the name is no longer on *that owner’s* list today. The heat kid falling off does not sell a fresh-E lot.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `combo` — each member keeps its own 09:30 list (not a mashed shopping list).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **owner mix**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,475.44.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `IREN` | 54 | — | $45.98 | +0.00 | $44.76 | -65.88 | -65.88 | +0.00 | -65.88 |
| 2026-08-13 | `TNDM` | 107 | — | $23.33 | +0.00 | $23.13 | -21.40 | -21.40 | +0.00 | -21.40 |
| 2026-08-13 | `TPG` | 49 | — | $50.62 | +0.00 | $54.62 | +195.84 | +195.84 | +0.00 | +195.84 |
| 2026-08-13 | `INO` | 3085 | — | $0.81 | +0.00 | $0.90 | +277.65 | +277.65 | +0.00 | +277.65 |
| 2026-08-14 | `IREN` | 54 | $44.76 | $44.09 | -36.18 | — | +0.00 | -36.18 | -102.06 | — |
| 2026-08-14 | `TNDM` | 107 | $23.13 | $22.92 | -22.47 | — | +0.00 | -22.47 | -43.87 | — |
| 2026-08-14 | `TPG` | 49 | $54.62 | $55.29 | +32.83 | — | +0.00 | +32.83 | +228.67 | — |
| 2026-08-14 | `INO` | 3085 | $0.90 | $0.93 | +92.55 | — | +0.00 | +92.55 | +370.20 | — |
| 2026-08-14 | `QMCO` | 73 | — | $24.68 | +0.00 | $26.11 | +104.39 | +104.39 | +0.00 | +104.39 |
| 2026-08-14 | `ARX` | 92 | — | $19.57 | +0.00 | $19.58 | +0.92 | +0.92 | +0.00 | +0.92 |
| 2026-08-14 | `ZENA` | 824 | — | $2.20 | +0.00 | $2.14 | -49.44 | -49.44 | +0.00 | -49.44 |
| 2026-08-14 | `AIRO` | 163 | — | $11.12 | +0.00 | $9.57 | -252.65 | -252.65 | +0.00 | -252.65 |
| 2026-08-14 | `TLN` | 1 | — | $359.83 | +0.00 | $362.74 | +2.91 | +2.91 | +0.00 | +2.91 |
| 2026-08-14 | `VST` | 3 | — | $146.90 | +0.00 | $148.13 | +3.69 | +3.69 | +0.00 | +3.69 |
| 2026-08-14 | `NRG` | 3 | — | $120.00 | +0.00 | $126.24 | +18.72 | +18.72 | +0.00 | +18.72 |
| 2026-08-14 | `ANGX` | 103 | — | $4.31 | +0.00 | $4.37 | +6.18 | +6.18 | +0.00 | +6.18 |
| 2026-08-14 | `MH` | 32 | — | $13.55 | +0.00 | $13.10 | -14.40 | -14.40 | +0.00 | -14.40 |
| 2026-08-14 | `HLIT` | 33 | — | $13.18 | +0.00 | $13.92 | +24.42 | +24.42 | +0.00 | +24.42 |
| 2026-08-17 | `QMCO` | 73 | $26.11 | $24.83 | -93.44 | — | +0.00 | -93.44 | +10.95 | — |
| 2026-08-17 | `ARX` | 92 | $19.58 | $19.57 | -0.92 | — | +0.00 | -0.92 | +0.00 | — |
| 2026-08-17 | `ZENA` | 824 | $2.14 | $2.08 | -45.32 | — | +0.00 | -45.32 | -94.76 | — |
| 2026-08-17 | `AIRO` | 163 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -252.65 | — |
| 2026-08-17 | `TLN` | 1 | $362.74 | $367.88 | +5.14 | — | +0.00 | +5.14 | +8.05 | — |
| 2026-08-17 | `VST` | 3 | $148.13 | $149.37 | +3.72 | — | +0.00 | +3.72 | +7.41 | — |
| 2026-08-17 | `NRG` | 3 | $126.24 | $127.40 | +3.48 | — | +0.00 | +3.48 | +22.20 | — |
| 2026-08-17 | `ANGX` | 103 | $4.37 | $4.60 | +23.69 | — | +0.00 | +23.69 | +29.87 | — |
| 2026-08-17 | `MH` | 32 | $13.10 | $13.16 | +1.92 | — | +0.00 | +1.92 | -12.48 | — |
| 2026-08-17 | `HLIT` | 33 | $13.92 | $13.84 | -2.64 | — | +0.00 | -2.64 | +21.78 | — |
| 2026-08-17 | `XHG` | 419 | — | $4.19 | +0.00 | $3.91 | -117.32 | -117.32 | +0.00 | -117.32 |
| 2026-08-17 | `CAPR` | 255 | — | $6.87 | +0.00 | $7.45 | +147.90 | +147.90 | +0.00 | +147.90 |
| 2026-08-17 | `STDN` | 128 | — | $13.64 | +0.00 | $13.31 | -42.24 | -42.24 | +0.00 | -42.24 |
| 2026-08-17 | `HTFL` | 42 | — | $41.23 | +0.00 | $41.94 | +29.82 | +29.82 | +0.00 | +29.82 |
| 2026-08-17 | `DVN` | 13 | — | $46.18 | +0.00 | $47.57 | +18.07 | +18.07 | +0.00 | +18.07 |
| 2026-08-17 | `EOG` | 4 | — | $142.77 | +0.00 | $146.15 | +13.52 | +13.52 | +0.00 | +13.52 |
| 2026-08-17 | `FANG` | 3 | — | $202.70 | +0.00 | $206.29 | +10.77 | +10.77 | +0.00 | +10.77 |
| 2026-08-17 | `CELC` | 6 | — | $92.99 | +0.00 | $92.44 | -3.30 | -3.30 | +0.00 | -3.30 |
| 2026-08-17 | `OUST` | 12 | — | $49.00 | +0.00 | $48.13 | -10.44 | -10.44 | +0.00 | -10.44 |
| 2026-08-18 | `XHG` | 419 | $3.91 | $3.94 | +12.57 | — | +0.00 | +12.57 | -104.75 | — |
| 2026-08-18 | `CAPR` | 255 | $7.45 | $7.50 | +12.75 | $7.08 | -107.10 | -94.35 | +160.65 | +53.55 |
| 2026-08-18 | `STDN` | 128 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -42.24 | — |
| 2026-08-18 | `HTFL` | 42 | $41.94 | $41.50 | -18.48 | — | +0.00 | -18.48 | +11.34 | — |
| 2026-08-18 | `DVN` | 13 | $47.57 | $48.00 | +5.59 | — | +0.00 | +5.59 | +23.66 | — |
| 2026-08-18 | `EOG` | 4 | $146.15 | $148.04 | +7.56 | — | +0.00 | +7.56 | +21.08 | — |
| 2026-08-18 | `FANG` | 3 | $206.29 | $208.93 | +7.92 | — | +0.00 | +7.92 | +18.69 | — |
| 2026-08-18 | `CELC` | 6 | $92.44 | $92.38 | -0.36 | — | +0.00 | -0.36 | -3.66 | — |
| 2026-08-18 | `OUST` | 12 | $48.13 | $45.09 | -36.48 | — | +0.00 | -36.48 | -46.92 | — |
| 2026-08-19 | `CAPR` | 255 | $7.08 | $7.19 | +28.05 | — | +0.00 | +28.05 | +81.60 | — |
| 2026-08-20 | `MRNA` | 11 | — | $150.14 | +0.00 | $133.32 | -185.02 | -185.02 | +0.00 | -185.02 |
| 2026-08-20 | `CYPH` | 1515 | — | $1.15 | +0.00 | $1.19 | +60.60 | +60.60 | +0.00 | +60.60 |
| 2026-08-20 | `ABCL` | 147 | — | $11.81 | +0.00 | $11.57 | -36.01 | -36.01 | +0.00 | -36.01 |
| 2026-08-20 | `AZI` | 1272 | — | $1.37 | +0.00 | $1.44 | +89.04 | +89.04 | +0.00 | +89.04 |
| 2026-08-20 | `BHP` | 4 | — | $91.01 | +0.00 | $93.63 | +10.48 | +10.48 | +0.00 | +10.48 |
| 2026-08-20 | `HUMA` | 615 | — | $0.71 | +0.00 | $0.68 | -15.99 | -15.99 | +0.00 | -15.99 |
| 2026-08-20 | `BTGO` | 65 | — | $6.61 | +0.00 | $6.60 | -0.33 | -0.33 | +0.00 | -0.33 |
| 2026-08-20 | `ASST` | 27 | — | $16.00 | +0.00 | $16.13 | +3.51 | +3.51 | +0.00 | +3.51 |
| 2026-08-20 | `ZLAB` | 16 | — | $26.57 | +0.00 | $26.02 | -8.80 | -8.80 | +0.00 | -8.80 |
| 2026-08-20 | `CRSP` | 7 | — | $58.73 | +0.00 | $58.12 | -4.27 | -4.27 | +0.00 | -4.27 |
| 2026-08-20 | `APA` | 9 | — | $44.76 | +0.00 | $44.39 | -3.33 | -3.33 | +0.00 | -3.33 |
| 2026-08-21 | `MRNA` | 11 | $133.32 | $133.11 | -2.31 | $145.13 | +132.22 | +129.91 | -187.33 | -55.11 |
| 2026-08-21 | `CYPH` | 1515 | $1.19 | $1.32 | +196.95 | $1.42 | +151.50 | +348.45 | +257.55 | +409.05 |
| 2026-08-21 | `ABCL` | 147 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -36.01 | — |
| 2026-08-21 | `AZI` | 1272 | $1.44 | $1.46 | +25.44 | — | +0.00 | +25.44 | +114.48 | — |
| 2026-08-21 | `BHP` | 4 | $93.63 | $95.72 | +8.36 | — | +0.00 | +8.36 | +18.84 | — |
| 2026-08-21 | `HUMA` | 615 | $0.68 | $0.67 | -4.31 | — | +0.00 | -4.31 | -20.29 | — |
| 2026-08-21 | `BTGO` | 65 | $6.60 | $6.95 | +22.75 | — | +0.00 | +22.75 | +22.42 | — |
| 2026-08-21 | `ASST` | 27 | $16.13 | $17.66 | +41.31 | — | +0.00 | +41.31 | +44.82 | — |
| 2026-08-21 | `ZLAB` | 16 | $26.02 | $26.25 | +3.68 | — | +0.00 | +3.68 | -5.12 | — |
| 2026-08-21 | `CRSP` | 7 | $58.12 | $59.72 | +11.20 | $59.50 | -1.54 | +9.66 | +6.93 | +5.39 |
| 2026-08-21 | `APA` | 9 | $44.39 | $44.52 | +1.17 | — | +0.00 | +1.17 | -2.16 | — |
| 2026-08-21 | `XHG` | 483 | — | $4.49 | +0.00 | $4.41 | -38.64 | -38.64 | +0.00 | -38.64 |
| 2026-08-21 | `CAPR` | 318 | — | $6.81 | +0.00 | $6.29 | -165.36 | -165.36 | +0.00 | -165.36 |
| 2026-08-21 | `AU` | 2 | — | $119.43 | +0.00 | $121.22 | +3.58 | +3.58 | +0.00 | +3.58 |
| 2026-08-21 | `AUTL` | 107 | — | $2.47 | +0.00 | $2.41 | -6.42 | -6.42 | +0.00 | -6.42 |
| 2026-08-21 | `FUTU` | 2 | — | $115.18 | +0.00 | $123.64 | +16.92 | +16.92 | +0.00 | +16.92 |
| 2026-08-21 | `MARA` | 22 | — | $11.70 | +0.00 | $11.26 | -9.68 | -9.68 | +0.00 | -9.68 |
| 2026-08-21 | `BTDR` | 23 | — | $11.10 | +0.00 | $11.37 | +6.32 | +6.32 | +0.00 | +6.32 |
| 2026-08-21 | `HIVE` | 81 | — | $3.24 | +0.00 | $3.03 | -17.01 | -17.01 | +0.00 | -17.01 |
| 2026-08-24 | `MRNA` | 11 | $145.13 | $142.70 | -26.73 | — | +0.00 | -26.73 | -81.84 | — |
| 2026-08-24 | `CYPH` | 1515 | $1.42 | $1.83 | +621.15 | — | +0.00 | +621.15 | +1030.20 | — |
| 2026-08-24 | `CRSP` | 7 | $59.50 | $58.75 | -5.25 | $57.08 | -11.72 | -16.97 | +0.14 | -11.58 |
| 2026-08-24 | `XHG` | 483 | $4.41 | $4.32 | -43.47 | — | +0.00 | -43.47 | -82.11 | — |
| 2026-08-24 | `CAPR` | 318 | $6.29 | $8.03 | +553.32 | — | +0.00 | +553.32 | +387.96 | — |
| 2026-08-24 | `AU` | 2 | $121.22 | $120.51 | -1.42 | — | +0.00 | -1.42 | +2.16 | — |
| 2026-08-24 | `AUTL` | 107 | $2.41 | $2.40 | -1.07 | — | +0.00 | -1.07 | -7.49 | — |
| 2026-08-24 | `FUTU` | 2 | $123.64 | $121.00 | -5.28 | — | +0.00 | -5.28 | +11.64 | — |
| 2026-08-24 | `MARA` | 22 | $11.26 | $11.17 | -1.98 | — | +0.00 | -1.98 | -11.66 | — |
| 2026-08-24 | `BTDR` | 23 | $11.37 | $11.48 | +2.53 | — | +0.00 | +2.53 | +8.85 | — |
| 2026-08-24 | `HIVE` | 81 | $3.03 | $2.99 | -3.24 | — | +0.00 | -3.24 | -20.25 | — |
| 2026-08-25 | `CRSP` | 7 | $57.08 | $57.93 | +5.98 | — | +0.00 | +5.98 | -5.60 | — |
| 2026-08-25 | `REAX` | 81 | — | $24.11 | +0.00 | $28.43 | +349.92 | +349.92 | +0.00 | +349.92 |
| 2026-08-25 | `CYPH` | 1252 | — | $1.56 | +0.00 | $1.64 | +100.16 | +100.16 | +0.00 | +100.16 |
| 2026-08-25 | `XHG` | 479 | — | $4.07 | +0.00 | $4.02 | -23.95 | -23.95 | +0.00 | -23.95 |
| 2026-08-25 | `ASST` | 102 | — | $19.04 | +0.00 | $21.39 | +239.70 | +239.70 | +0.00 | +239.70 |
| 2026-08-25 | `RUM` | 59 | — | $9.42 | +0.00 | $10.23 | +47.79 | +47.79 | +0.00 | +47.79 |
| 2026-08-25 | `EZPW` | 15 | — | $35.05 | +0.00 | $35.23 | +2.70 | +2.70 | +0.00 | +2.70 |
| 2026-08-25 | `ZYME` | 19 | — | $28.86 | +0.00 | $27.47 | -26.41 | -26.41 | +0.00 | -26.41 |
| 2026-08-25 | `EOLS` | 63 | — | $8.72 | +0.00 | $8.97 | +16.06 | +16.06 | +0.00 | +16.06 |
| 2026-08-25 | `AU` | 4 | — | $118.52 | +0.00 | $123.39 | +19.48 | +19.48 | +0.00 | +19.48 |
| 2026-08-25 | `FCX` | 7 | — | $77.13 | +0.00 | $79.91 | +19.46 | +19.46 | +0.00 | +19.46 |
| 2026-08-26 | `REAX` | 81 | $28.43 | $26.61 | -147.42 | — | +0.00 | -147.42 | +202.50 | — |
| 2026-08-26 | `CYPH` | 1252 | $1.64 | $1.60 | -50.08 | — | +0.00 | -50.08 | +50.08 | — |
| 2026-08-26 | `XHG` | 479 | $4.02 | $3.81 | -100.59 | $4.06 | +119.75 | +19.16 | -124.54 | -4.79 |
| 2026-08-26 | `ASST` | 102 | $21.39 | $20.72 | -68.34 | — | +0.00 | -68.34 | +171.36 | — |
| 2026-08-26 | `RUM` | 59 | $10.23 | $10.07 | -9.44 | — | +0.00 | -9.44 | +38.35 | — |
| 2026-08-26 | `EZPW` | 15 | $35.23 | $35.70 | +7.05 | — | +0.00 | +7.05 | +9.75 | — |
| 2026-08-26 | `ZYME` | 19 | $27.47 | $27.56 | +1.71 | — | +0.00 | +1.71 | -24.70 | — |
| 2026-08-26 | `EOLS` | 63 | $8.97 | $8.86 | -7.25 | — | +0.00 | -7.25 | +8.82 | — |
| 2026-08-26 | `AU` | 4 | $123.39 | $119.80 | -14.36 | — | +0.00 | -14.36 | +5.12 | — |
| 2026-08-26 | `FCX` | 7 | $79.91 | $79.34 | -3.99 | — | +0.00 | -3.99 | +15.47 | — |
| 2026-08-26 | `BYND` | 159 | — | $14.11 | +0.00 | $14.25 | +22.26 | +22.26 | +0.00 | +22.26 |
| 2026-08-26 | `USDE` | 386 | — | $5.81 | +0.00 | $5.98 | +65.62 | +65.62 | +0.00 | +65.62 |
| 2026-08-26 | `SUJA` | 238 | — | $9.39 | +0.00 | $9.44 | +11.90 | +11.90 | +0.00 | +11.90 |
| 2026-08-26 | `FLNC` | 51 | — | $11.12 | +0.00 | $11.08 | -2.04 | -2.04 | +0.00 | -2.04 |
| 2026-08-26 | `CAPR` | 69 | — | $8.29 | +0.00 | $9.36 | +73.83 | +73.83 | +0.00 | +73.83 |
| 2026-08-26 | `FWRD` | 33 | — | $17.41 | +0.00 | $17.63 | +7.26 | +7.26 | +0.00 | +7.26 |
| 2026-08-26 | `TRLV` | 51 | — | $11.22 | +0.00 | $11.43 | +10.71 | +10.71 | +0.00 | +10.71 |
| 2026-08-26 | `FNV` | 2 | — | $267.02 | +0.00 | $267.37 | +0.70 | +0.70 | +0.00 | +0.70 |
| 2026-08-27 | `XHG` | 479 | $4.06 | $4.06 | +0.00 | — | +0.00 | +0.00 | -4.79 | — |
| 2026-08-27 | `BYND` | 159 | $14.25 | $14.20 | -7.95 | — | +0.00 | -7.95 | +14.31 | — |
| 2026-08-27 | `USDE` | 386 | $5.98 | $6.50 | +200.72 | — | +0.00 | +200.72 | +266.34 | — |
| 2026-08-27 | `SUJA` | 238 | $9.44 | $9.41 | -7.14 | — | +0.00 | -7.14 | +4.76 | — |
| 2026-08-27 | `FLNC` | 51 | $11.08 | $11.52 | +22.44 | — | +0.00 | +22.44 | +20.40 | — |
| 2026-08-27 | `CAPR` | 69 | $9.36 | $9.19 | -11.73 | — | +0.00 | -11.73 | +62.10 | — |
| 2026-08-27 | `FWRD` | 33 | $17.63 | $17.60 | -0.99 | — | +0.00 | -0.99 | +6.27 | — |
| 2026-08-27 | `TRLV` | 51 | $11.43 | $11.38 | -2.55 | — | +0.00 | -2.55 | +8.16 | — |
| 2026-08-27 | `FNV` | 2 | $267.37 | $267.23 | -0.28 | — | +0.00 | -0.28 | +0.42 | — |
| 2026-08-27 | `SLI` | 800 | — | $2.60 | +0.00 | $2.64 | +32.00 | +32.00 | +0.00 | +32.00 |
| 2026-08-27 | `RRC` | 50 | — | $41.44 | +0.00 | $41.64 | +10.00 | +10.00 | +0.00 | +10.00 |
| 2026-08-27 | `PGY` | 90 | — | $22.93 | +0.00 | $23.26 | +29.70 | +29.70 | +0.00 | +29.70 |
| 2026-08-27 | `CRK` | 144 | — | $14.42 | +0.00 | $14.62 | +28.80 | +28.80 | +0.00 | +28.80 |
| 2026-08-27 | `ACMR` | 8 | — | $81.65 | +0.00 | $80.49 | -9.28 | -9.28 | +0.00 | -9.28 |
| 2026-08-27 | `LRCX` | 2 | — | $318.88 | +0.00 | $318.58 | -0.60 | -0.60 | +0.00 | -0.60 |
| 2026-08-27 | `NVDA` | 3 | — | $222.86 | +0.00 | $227.98 | +15.36 | +15.36 | +0.00 | +15.36 |
| 2026-08-28 | `SLI` | 800 | $2.64 | $2.68 | +32.00 | — | +0.00 | +32.00 | +64.00 | — |
| 2026-08-28 | `RRC` | 12 | $41.64 | $41.74 | +5.00 | $41.46 | -3.36 | +1.64 | +15.00 | -3.36 |
| 2026-08-28 | `PGY` | 90 | $23.26 | $23.21 | -4.50 | — | +0.00 | -4.50 | +25.20 | — |
| 2026-08-28 | `CRK` | 144 | $14.62 | $14.63 | +1.44 | — | +0.00 | +1.44 | +30.24 | — |
| 2026-08-28 | `ACMR` | 8 | $80.49 | $79.27 | -9.76 | — | +0.00 | -9.76 | -19.04 | — |
| 2026-08-28 | `LRCX` | 2 | $318.58 | $318.03 | -1.10 | — | +0.00 | -1.10 | -1.70 | — |
| 2026-08-28 | `NVDA` | 3 | $227.98 | $227.36 | -1.86 | — | +0.00 | -1.86 | +13.50 | — |
| 2026-08-28 | `BYND` | 149 | — | $14.00 | +0.00 | $13.86 | -20.86 | -20.86 | +0.00 | -20.86 |
| 2026-08-28 | `CAPR` | 215 | — | $9.73 | +0.00 | $9.59 | -30.10 | -30.10 | +0.00 | -30.10 |
| 2026-08-28 | `MRNA` | 15 | — | $137.19 | +0.00 | $137.99 | +12.00 | +12.00 | +0.00 | +12.00 |
| 2026-08-28 | `ANF` | 14 | — | $146.07 | +0.00 | $148.42 | +32.90 | +32.90 | +0.00 | +32.90 |
| 2026-08-28 | `SEDG` | 16 | — | $32.90 | +0.00 | $31.41 | -23.84 | -23.84 | +0.00 | -23.84 |
| 2026-08-28 | `OPTX` | 61 | — | $8.61 | +0.00 | $8.52 | -5.49 | -5.49 | +0.00 | -5.49 |
| 2026-08-28 | `SMTC` | 3 | — | $141.76 | +0.00 | $131.17 | -31.77 | -31.77 | +0.00 | -31.77 |
| 2026-08-28 | `ERAS` | 27 | — | $19.25 | +0.00 | $18.03 | -32.94 | -32.94 | +0.00 | -32.94 |
| 2026-08-28 | `BBWI` | 28 | — | $18.75 | +0.00 | $19.22 | +13.16 | +13.16 | +0.00 | +13.16 |
| 2026-08-28 | `ZYME` | 18 | — | $28.91 | +0.00 | $28.27 | -11.52 | -11.52 | +0.00 | -11.52 |
| 2026-08-31 | `BYND` | 149 | $13.86 | $13.81 | -7.45 | $13.30 | -75.99 | -83.44 | -28.31 | -104.30 |
| 2026-08-31 | `CAPR` | 215 | $9.59 | $9.50 | -19.35 | — | +0.00 | -19.35 | -49.45 | — |
| 2026-08-31 | `MRNA` | 15 | $137.99 | $134.10 | -58.35 | — | +0.00 | -58.35 | -46.35 | — |
| 2026-08-31 | `ANF` | 14 | $148.42 | $148.03 | -5.46 | — | +0.00 | -5.46 | +27.44 | — |
| 2026-08-31 | `RRC` | 12 | $41.46 | $42.00 | +6.48 | — | +0.00 | +6.48 | +3.12 | — |
| 2026-08-31 | `SEDG` | 16 | $31.41 | $31.15 | -4.16 | — | +0.00 | -4.16 | -28.00 | — |
| 2026-08-31 | `OPTX` | 61 | $8.52 | $8.52 | +0.00 | — | +0.00 | +0.00 | -5.49 | — |
| 2026-08-31 | `SMTC` | 3 | $131.17 | $132.30 | +3.39 | — | +0.00 | +3.39 | -28.38 | — |
| 2026-08-31 | `ERAS` | 27 | $18.03 | $17.87 | -4.32 | — | +0.00 | -4.32 | -37.26 | — |
| 2026-08-31 | `BBWI` | 28 | $19.22 | $19.25 | +0.84 | — | +0.00 | +0.84 | +14.00 | — |
| 2026-08-31 | `ZYME` | 18 | $28.27 | $28.06 | -3.78 | — | +0.00 | -3.78 | -15.30 | — |
| 2026-09-01 | `BYND` | 149 | $13.30 | $13.04 | -38.74 | — | +0.00 | -38.74 | -143.04 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `GPRO` | 1142 | — | $1.78 | +0.00 | $1.39 | -445.38 | -445.38 | +0.00 | -445.38 |
| 2026-09-03 | `REAX` | 110 | — | $18.40 | +0.00 | $18.40 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `CNH` | 148 | — | $13.71 | +0.00 | $13.84 | +19.24 | +19.24 | +0.00 | +19.24 |
| 2026-09-03 | `MMED` | 85 | — | $23.88 | +0.00 | $23.84 | -3.40 | -3.40 | +0.00 | -3.40 |
| 2026-09-03 | `CNXC` | 15 | — | $32.88 | +0.00 | $32.85 | -0.45 | -0.45 | +0.00 | -0.45 |
| 2026-09-03 | `OPTX` | 65 | — | $7.59 | +0.00 | $7.76 | +11.05 | +11.05 | +0.00 | +11.05 |
| 2026-09-03 | `FRNM` | 31 | — | $15.87 | +0.00 | $16.90 | +31.93 | +31.93 | +0.00 | +31.93 |
| 2026-09-03 | `AVGO` | 1 | — | $351.74 | +0.00 | $357.16 | +5.42 | +5.42 | +0.00 | +5.42 |
| 2026-09-03 | `CIEN` | 1 | — | $354.49 | +0.00 | $317.46 | -37.03 | -37.03 | +0.00 | -37.03 |
| 2026-09-03 | `HPE` | 10 | — | $47.60 | +0.00 | $54.44 | +68.40 | +68.40 | +0.00 | +68.40 |
| 2026-09-04 | `GPRO` | 1142 | $1.39 | $1.48 | +102.78 | $1.70 | +251.24 | +354.02 | -342.60 | -91.36 |
| 2026-09-04 | `REAX` | 110 | $18.40 | $18.15 | -27.50 | — | +0.00 | -27.50 | -27.50 | — |
| 2026-09-04 | `CNH` | 148 | $13.84 | $13.89 | +7.40 | — | +0.00 | +7.40 | +26.64 | — |
| 2026-09-04 | `MMED` | 85 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -3.40 | — |
| 2026-09-04 | `CNXC` | 15 | $32.85 | $32.48 | -5.55 | — | +0.00 | -5.55 | -6.00 | — |
| 2026-09-04 | `OPTX` | 65 | $7.76 | $7.79 | +1.95 | — | +0.00 | +1.95 | +13.00 | — |
| 2026-09-04 | `FRNM` | 31 | $16.90 | $16.40 | -15.50 | $16.31 | -2.79 | -18.29 | +16.43 | +13.64 |
| 2026-09-04 | `AVGO` | 1 | $357.16 | $359.70 | +2.54 | — | +0.00 | +2.54 | +7.96 | — |
| 2026-09-04 | `CIEN` | 1 | $317.46 | $321.67 | +4.21 | — | +0.00 | +4.21 | -32.82 | — |
| 2026-09-04 | `HPE` | 10 | $54.44 | $53.85 | -5.90 | — | +0.00 | -5.90 | +62.50 | — |
| 2026-09-04 | `ASST` | 84 | — | $25.18 | +0.00 | $27.14 | +164.64 | +164.64 | +0.00 | +164.64 |
| 2026-09-04 | `USDE` | 269 | — | $7.87 | +0.00 | $7.93 | +16.14 | +16.14 | +0.00 | +16.14 |
| 2026-09-04 | `DFDV` | 366 | — | $5.79 | +0.00 | $5.87 | +29.28 | +29.28 | +0.00 | +29.28 |
| 2026-09-04 | `CRM` | 2 | — | $263.36 | +0.00 | $259.23 | -8.26 | -8.26 | +0.00 | -8.26 |
| 2026-09-04 | `BAK` | 280 | — | $1.94 | +0.00 | $1.89 | -14.00 | -14.00 | +0.00 | -14.00 |
| 2026-09-04 | `MSTR` | 3 | — | $137.35 | +0.00 | $142.80 | +16.35 | +16.35 | +0.00 | +16.35 |
| 2026-09-04 | `BE` | 2 | — | $236.82 | +0.00 | $252.87 | +32.10 | +32.10 | +0.00 | +32.10 |
| 2026-09-04 | `MRX` | 7 | — | $75.65 | +0.00 | $78.27 | +18.34 | +18.34 | +0.00 | +18.34 |
| 2026-09-08 | `GPRO` | 1142 | $1.70 | $1.56 | -154.17 | — | +0.00 | -154.17 | -245.53 | — |
| 2026-09-08 | `FRNM` | 31 | $16.31 | $16.74 | +13.33 | — | +0.00 | +13.33 | +26.97 | — |
| 2026-09-08 | `ASST` | 84 | $27.14 | $26.44 | -58.80 | — | +0.00 | -58.80 | +105.84 | — |
| 2026-09-08 | `USDE` | 269 | $7.93 | $7.76 | -45.73 | — | +0.00 | -45.73 | -29.59 | — |
| 2026-09-08 | `DFDV` | 366 | $5.87 | $5.81 | -21.96 | — | +0.00 | -21.96 | +7.32 | — |
| 2026-09-08 | `CRM` | 2 | $259.23 | $253.72 | -11.02 | — | +0.00 | -11.02 | -19.28 | — |
| 2026-09-08 | `BAK` | 280 | $1.89 | $1.94 | +14.00 | — | +0.00 | +14.00 | +0.00 | — |
| 2026-09-08 | `MSTR` | 3 | $142.80 | $137.62 | -15.54 | $136.52 | -3.30 | -18.84 | +0.81 | -2.49 |
| 2026-09-08 | `BE` | 2 | $252.87 | $267.76 | +29.78 | — | +0.00 | +29.78 | +61.88 | — |
| 2026-09-08 | `MRX` | 7 | $78.27 | $78.84 | +3.99 | $76.71 | -14.91 | -10.92 | +22.33 | +7.42 |
| 2026-09-09 | `MSTR` | 3 | $136.52 | $141.82 | +15.90 | — | +0.00 | +15.90 | +13.41 | — |
| 2026-09-09 | `MRX` | 7 | $76.71 | $76.60 | -0.77 | — | +0.00 | -0.77 | +6.65 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +386.21 | IREN, TNDM, TPG, INO | — | $0.54 | $10,345.37 | IREN×54, TNDM×107, TPG×49, INO×3085 |
| 2026-08-14 | +5.50 | $0.54 | IREN×54, TNDM×107, TPG×49, INO×3085 | $10,412.10 | +66.73 | -155.26 | QMCO, ARX, ZENA, AIRO, TLN, VST, NRG, ANGX, MH, HLIT | IREN, TNDM, TPG, INO | $636.43 | $10,181.61 | QMCO×73, ARX×92, ZENA×824, AIRO×163, TLN×1, VST×3, NRG×3, ANGX×103, MH×32, HLIT×33 |
| 2026-08-17 | +2.25 | $636.43 | QMCO×73, ARX×92, ZENA×824, AIRO×163, TLN×1, VST×3, NRG×3, ANGX×103, MH×32, HLIT×33 | $10,077.24 | -104.37 | +46.78 | XHG, CAPR, STDN, HTFL, DVN, EOG, FANG, CELC, OUST | QMCO, ARX, ZENA, AIRO, TLN, VST, NRG, ANGX, MH, HLIT | $113.07 | $10,070.35 | XHG×419, CAPR×255, STDN×128, HTFL×42, DVN×13, EOG×4, FANG×3, CELC×6, OUST×12 |
| 2026-08-18 | -6.20 | $113.07 | XHG×419, CAPR×255, STDN×128, HTFL×42, DVN×13, EOG×4, FANG×3, CELC×6, OUST×12 | $10,061.42 | -8.93 | -107.10 | — | XHG, STDN, HTFL, DVN, EOG, FANG, CELC, OUST | $8,128.72 | $9,934.12 | CAPR×255 |
| 2026-08-19 | -7.20 | $8,128.72 | CAPR×255 | $9,962.17 | +28.05 | +0.00 | — | CAPR | $9,958.82 | $9,958.82 | — |
| 2026-08-20 | +1.12 | $9,958.82 | — | $9,958.82 | +0.00 | -90.12 | MRNA, CYPH, ABCL, AZI, BHP, HUMA, BTGO, ASST, ZLAB, CRSP, APA | — | $127.43 | $9,809.78 | MRNA×11, CYPH×1515, ABCL×147, AZI×1272, BHP×4, HUMA×615, BTGO×65, ASST×27, ZLAB×16, CRSP×7, APA×9 |
| 2026-08-21 | +3.25 | $127.43 | MRNA×11, CYPH×1515, ABCL×147, AZI×1272, BHP×4, HUMA×615, BTGO×65, ASST×27, ZLAB×16, CRSP×7, APA×9 | $10,114.03 | +304.25 | +71.89 | XHG, CAPR, AU, AUTL, FUTU, MARA, BTDR, HIVE | ABCL, AZI, BHP, HUMA, BTGO, ASST, ZLAB, APA | $330.59 | $10,127.32 | MRNA×11, CYPH×1515, CRSP×7, XHG×483, CAPR×318, AU×2, AUTL×107, FUTU×2, MARA×22, BTDR×23, HIVE×81 |
| 2026-08-24 | -5.17 | $330.59 | MRNA×11, CYPH×1515, CRSP×7, XHG×483, CAPR×318, AU×2, AUTL×107, FUTU×2, MARA×22, BTDR×23, HIVE×81 | $11,215.88 | +1,088.56 | -11.72 | — | MRNA, CYPH, XHG, CAPR, AU, AUTL, FUTU, MARA, BTDR, HIVE | $10,759.48 | $11,159.00 | CRSP×7 |
| 2026-08-25 | +1.80 | $10,759.48 | CRSP×7 | $11,164.99 | +5.99 | +744.91 | REAX, CYPH, XHG, ASST, RUM, EZPW, ZYME, EOLS, AU, FCX | CRSP | $132.80 | $11,868.57 | REAX×81, CYPH×1252, XHG×479, ASST×102, RUM×59, EZPW×15, ZYME×19, EOLS×63, AU×4, FCX×7 |
| 2026-08-26 | +2.02 | $132.80 | REAX×81, CYPH×1252, XHG×479, ASST×102, RUM×59, EZPW×15, ZYME×19, EOLS×63, AU×4, FCX×7 | $11,475.87 | -392.70 | +309.99 | BYND, USDE, SUJA, FLNC, CAPR, FWRD, TRLV, FNV | REAX, CYPH, ASST, RUM, EZPW, ZYME, EOLS, AU, FCX | $55.37 | $11,731.24 | XHG×479, BYND×159, USDE×386, SUJA×238, FLNC×51, CAPR×69, FWRD×33, TRLV×51, FNV×2 |
| 2026-08-27 | — | $55.37 | XHG×479, BYND×159, USDE×386, SUJA×238, FLNC×51, CAPR×69, FWRD×33, TRLV×51, FNV×2 | $11,923.76 | +192.52 | +105.98 | SLI, RRC, PGY, CRK, ACMR, LRCX, NVDA | XHG, BYND, USDE, SUJA, FLNC, CAPR, FWRD, TRLV, FNV | $1,621.25 | $11,978.95 | SLI×800, RRC×50, PGY×90, CRK×144, ACMR×8, LRCX×2, NVDA×3 |
| 2026-08-28 | +0.75 | $1,621.25 | SLI×800, RRC×50, PGY×90, CRK×144, ACMR×8, LRCX×2, NVDA×3 | $12,000.17 | +21.22 | -101.82 | BYND, CAPR, MRNA, ANF, RRC, SEDG, OPTX, SMTC, ERAS, BBWI, ZYME | SLI, RRC, PGY, CRK, ACMR, LRCX, NVDA | $129.32 | $11,851.18 | BYND×149, CAPR×215, MRNA×15, ANF×14, RRC×12, SEDG×16, OPTX×61, SMTC×3, ERAS×27, BBWI×28, ZYME×18 |
| 2026-08-31 | -5.85 | $129.32 | BYND×149, CAPR×215, MRNA×15, ANF×14, RRC×12, SEDG×16, OPTX×61, SMTC×3, ERAS×27, BBWI×28, ZYME×18 | $11,759.02 | -92.16 | -75.99 | — | CAPR, MRNA, ANF, RRC, SEDG, OPTX, SMTC, ERAS, BBWI, ZYME | $9,679.82 | $11,661.52 | BYND×149 |
| 2026-09-01 | -6.30 | $9,679.82 | BYND×149 | $11,622.78 | -38.74 | +0.00 | — | BYND | $11,620.31 | $11,620.31 | — |
| 2026-09-02 | -3.83 | $11,620.31 | — | $11,620.31 | -0.00 | +0.00 | — | — | $11,620.31 | $11,620.31 | — |
| 2026-09-03 | -0.90 | $11,620.31 | — | $11,620.31 | -0.00 | -350.22 | GPRO, REAX, CNH, MMED, CNXC, OPTX, FRNM, AVGO, CIEN, HPE | — | $809.88 | $11,236.05 | GPRO×1142, REAX×110, CNH×148, MMED×85, CNXC×15, OPTX×65, FRNM×31, AVGO×1, CIEN×1, HPE×10 |
| 2026-09-04 | +2.25 | $809.88 | GPRO×1142, REAX×110, CNH×148, MMED×85, CNXC×15, OPTX×65, FRNM×31, AVGO×1, CIEN×1, HPE×10 | $11,300.48 | +64.43 | +503.04 | ASST, USDE, DFDV, CRM, BAK, MSTR, BE, MRX | REAX, CNH, MMED, CNXC, OPTX, AVGO, CIEN, HPE | $225.99 | $11,764.04 | GPRO×1142, FRNM×31, ASST×84, USDE×269, DFDV×366, CRM×2, BAK×280, MSTR×3, BE×2, MRX×7 |
| 2026-09-08 | -11.47 | $225.99 | GPRO×1142, FRNM×31, ASST×84, USDE×269, DFDV×366, CRM×2, BAK×280, MSTR×3, BE×2, MRX×7 | $11,517.92 | -246.12 | -18.21 | — | GPRO, FRNM, ASST, USDE, DFDV, CRM, BAK, BE | $10,517.83 | $11,464.36 | MSTR×3, MRX×7 |
| 2026-09-09 | -13.95 | $10,517.83 | MSTR×3, MRX×7 | $11,479.49 | +15.13 | +0.00 | — | MSTR, MRX | $11,475.44 | $11,475.44 | — |
| 2026-09-10 | -13.28 | $11,475.44 | — | $11,475.44 | +0.00 | +0.00 | — | — | $11,475.44 | $11,475.44 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 (unchanged overnight, no fees) · equity $10,000.00 vs prior close $10,000.00 (+0.00) | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $7,514.93 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; combo leftover $2500.00; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 107 | $23.33 | $2.31 | — | $5,016.31 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; combo leftover $2500.00; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 49 | $50.62 | $2.14 | — | $2,533.63 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; combo leftover $2500.00; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3085 | $0.81 | $34.24 | — | $0.54 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+13.2; combo leftover $2500.00; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.54 | ▲ close $10,345.37 vs 09:30 $10,000.00 (session +386.21) | 16:00 close · cash $0.54 · equity $10,345.37 vs 09:30 $10,000.00 (+345.37; session marks +386.21) · 4 name(s) marked open→close (per-name table). IREN×54 09:30 $45.98 → close $44.76 -65.88; TNDM×107 09:30 $23.33 → close $23.13 -21.40; TPG×49 09:30 $50.62 → close $54.62 +195.84; INO×3085 09:30 $0.81 → close $0.90 +277.65 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.54 | ▲ 09:30 equity $10,412.10 vs yday $10,345.37 (+66.73) | 09:30 open · cash $0.54 (unchanged overnight, no fees) · equity $10,412.10 vs prior close $10,345.37 (+66.73) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 54 | $44.09 | $2.18 | $-106.39 | $2,379.22 | ▼ -106.39 after sell → book $10,409.92; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 107 | $22.92 | $2.35 | $-48.53 | $4,829.31 | ▼ -48.53 after sell → book $10,407.57; vs 09:30 mark -2.35 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 49 | $55.29 | $2.17 | $+224.37 | $7,536.35 | ▲ +224.37 after sell → book $10,405.40; vs 09:30 mark -2.17 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 3085 | $0.93 | $38.48 | $+297.48 | $10,366.92 | ▲ +297.48 after sell → book $10,366.92; vs 09:30 mark -38.48 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 73 | $24.68 | $2.21 | — | $8,563.07 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; combo leftover $1814.21; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 92 | $19.57 | $2.27 | — | $6,760.37 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $1814.21; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 824 | $2.20 | $10.63 | — | $4,936.94 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; combo leftover $1814.21; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 163 | $11.12 | $2.48 | — | $3,121.90 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $1814.21; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 1 | $359.83 | $1.99 | — | $2,760.07 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; combo leftover $445.99; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 3 | $146.90 | $2.00 | — | $2,317.38 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; combo leftover $445.99; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 3 | $120.00 | $2.00 | — | $1,955.38 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; combo leftover $445.99; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 103 | $4.31 | $2.30 | — | $1,509.15 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; combo leftover $445.99; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 32 | $13.55 | $2.09 | — | $1,073.46 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $445.99; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 33 | $13.18 | $2.09 | — | $636.43 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; combo leftover $445.99; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $636.43 | ▼ close $10,181.61 vs 09:30 $10,412.10 (session -155.26) | 16:00 close · cash $636.43 · equity $10,181.61 vs 09:30 $10,412.10 (-230.49; session marks -155.26) · 10 name(s) marked open→close (per-name table). QMCO×73 09:30 $24.68 → close $26.11 +104.39; ARX×92 09:30 $19.57 → close $19.58 +0.92; ZENA×824 09:30 $2.20 → close $2.14 -49.44; AIRO×163 09:30 $11.12 → close $9.57 -252.65; TLN×1 09:30 $359.83 → close $362.74 +2.91; VST×3 09:30 $146.90 → close $148.13 +3.69; NRG×3 09:30 $120.00 → close $126.24 +18.72; ANGX×103 09:30 $4.31 → close $4.37 +6.18; MH×32 09:30 $13.55 → close $13.10 -14.40; HLIT×33 09:30 $13.18 → close $13.92 +24.42 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $636.43 | ▼ 09:30 equity $10,077.24 vs yday $10,181.61 (-104.37) | 09:30 open · cash $636.43 (unchanged overnight, no fees) · equity $10,077.24 vs prior close $10,181.61 (-104.37) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 73 | $24.83 | $2.24 | $+6.51 | $2,446.79 | ▲ +6.51 after sell → book $10,075.01; vs 09:30 mark -2.23 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 92 | $19.57 | $2.30 | $-4.56 | $4,244.93 | ▼ -4.56 after sell → book $10,072.71; vs 09:30 mark -2.30 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 824 | $2.08 | $10.78 | $-116.17 | $5,952.19 | ▼ -116.17 after sell → book $10,061.93; vs 09:30 mark -10.78 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 163 | $9.57 | $2.52 | $-257.65 | $7,509.58 | ▼ -257.65 after sell → book $10,059.41; vs 09:30 mark -2.52 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 1 | $367.88 | $2.01 | $+4.04 | $7,875.45 | ▲ +4.04 after sell → book $10,057.40; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 3 | $149.37 | $2.02 | $+3.39 | $8,321.54 | ▲ +3.39 after sell → book $10,055.38; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 3 | $127.40 | $2.02 | $+18.18 | $8,701.72 | ▲ +18.18 after sell → book $10,053.36; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 103 | $4.60 | $2.33 | $+25.24 | $9,173.20 | ▲ +25.24 after sell → book $10,051.04; vs 09:30 mark -2.32 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 32 | $13.16 | $2.11 | $-16.67 | $9,592.21 | ▼ -16.67 after sell → book $10,048.93; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 33 | $13.84 | $2.11 | $+17.58 | $10,046.82 | ▲ +17.58 after sell → book $10,046.82; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 419 | $4.19 | $5.41 | — | $8,285.81 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $1758.19; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 255 | $6.87 | $3.29 | — | $6,530.67 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $1758.19; owner union_hot_n4_h1 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 128 | $13.64 | $2.37 | — | $4,782.37 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $1758.19; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 42 | $41.23 | $2.12 | — | $3,048.60 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $1758.19; owner union_hot_n4_h1 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 13 | $46.18 | $2.03 | — | $2,446.23 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; combo leftover $609.72; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 4 | $142.77 | $2.00 | — | $1,873.14 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; combo leftover $609.72; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 3 | $202.70 | $2.00 | — | $1,263.05 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; combo leftover $609.72; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 6 | $92.99 | $2.01 | — | $703.10 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; combo leftover $609.72; owner union_news_g_h1 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 12 | $49.00 | $2.03 | — | $113.07 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; combo leftover $609.72; owner union_news_g_h1 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.07 | ▲ close $10,070.35 vs 09:30 $10,077.24 (session +46.78) | 16:00 close · cash $113.07 · equity $10,070.35 vs 09:30 $10,077.24 (-6.89; session marks +46.78) · 9 name(s) marked open→close (per-name table). XHG×419 09:30 $4.19 → close $3.91 -117.32; CAPR×255 09:30 $6.87 → close $7.45 +147.90; STDN×128 09:30 $13.64 → close $13.31 -42.24; HTFL×42 09:30 $41.23 → close $41.94 +29.82; DVN×13 09:30 $46.18 → close $47.57 +18.07; EOG×4 09:30 $142.77 → close $146.15 +13.52; FANG×3 09:30 $202.70 → close $206.29 +10.77; CELC×6 09:30 $92.99 → close $92.44 -3.30; OUST×12 09:30 $49.00 → close $48.13 -10.44 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.07 | ▼ 09:30 equity $10,061.42 vs yday $10,070.35 (-8.93) | 09:30 open · cash $113.07 (unchanged overnight, no fees) · equity $10,061.42 vs prior close $10,070.35 (-8.93) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 419 | $3.94 | $5.49 | $-115.64 | $1,758.44 | ▼ -115.64 after sell → book $10,055.93; vs 09:30 mark -5.49 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 128 | $13.31 | $2.41 | $-47.02 | $3,459.72 | ▼ -47.02 after sell → book $10,053.52; vs 09:30 mark -2.41 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 42 | $41.50 | $2.14 | $+7.08 | $5,200.58 | ▲ +7.08 after sell → book $10,051.39; vs 09:30 mark -2.14 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 13 | $48.00 | $2.05 | $+19.58 | $5,822.53 | ▲ +19.58 after sell → book $10,049.34; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 4 | $148.04 | $2.02 | $+17.06 | $6,412.66 | ▲ +17.06 after sell → book $10,047.31; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 3 | $208.93 | $2.02 | $+14.67 | $7,037.44 | ▲ +14.67 after sell → book $10,045.30; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 6 | $92.38 | $2.03 | $-7.70 | $7,589.69 | ▼ -7.70 after sell → book $10,043.27; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 12 | $45.09 | $2.05 | $-50.99 | $8,128.72 | ▼ -50.99 after sell → book $10,041.22; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,128.72 | ▼ close $9,934.12 vs 09:30 $10,061.42 (session -107.10) | 16:00 close · cash $8,128.72 · equity $9,934.12 vs 09:30 $10,061.42 (-127.30; session marks -107.10) · 1 name(s) marked open→close (per-name table). CAPR×255 09:30 $7.50 → close $7.08 -107.10 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,128.72 | ▲ 09:30 equity $9,962.17 vs yday $9,934.12 (+28.05) | 09:30 open · cash $8,128.72 (unchanged overnight, no fees) · equity $9,962.17 vs prior close $9,934.12 (+28.05) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 255 | $7.19 | $3.35 | $+74.96 | $9,958.82 | ▲ +74.96 after sell → book $9,958.82; vs 09:30 mark -3.35 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,958.82 | ▲ close $9,958.82 vs 09:30 $9,962.17 (session +0.00) | 16:00 close · cash $9,958.82 · no lots left · equity $9,958.82. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,958.82 | ▲ 09:30 equity $9,958.82 vs yday $9,958.82 (+0.00) | 09:30 open · cash $9,958.82 (unchanged overnight, no fees) · equity $9,958.82 vs prior close $9,958.82 (+0.00) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 11 | $150.14 | $2.02 | — | $8,305.26 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $1742.79; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1515 | $1.15 | $19.54 | — | $6,543.47 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $1742.79; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 147 | $11.81 | $2.43 | — | $4,804.23 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1742.79; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 1272 | $1.37 | $16.41 | — | $3,045.18 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $1742.79; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 4 | $91.01 | $2.00 | — | $2,679.14 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; combo leftover $435.03; owner union_news_g_h1 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 615 | $0.71 | $6.19 | — | $2,238.14 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; combo leftover $435.03; owner union_news_g_h1 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 65 | $6.61 | $2.19 | — | $1,806.63 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; combo leftover $435.03; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 27 | $16.00 | $2.07 | — | $1,372.56 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; combo leftover $435.03; owner union_news_g_h1 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 16 | $26.57 | $2.04 | — | $945.40 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; combo leftover $435.03; owner union_news_g_h1 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 7 | $58.73 | $2.01 | — | $532.28 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; combo leftover $435.03; owner union_news_g_h1 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 9 | $44.76 | $2.02 | — | $127.43 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; combo leftover $435.03; owner union_news_g_h1 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.43 | ▼ close $9,809.78 vs 09:30 $9,958.82 (session -90.12) | 16:00 close · cash $127.43 · equity $9,809.78 vs 09:30 $9,958.82 (-149.04; session marks -90.12) · 11 name(s) marked open→close (per-name table). MRNA×11 09:30 $150.14 → close $133.32 -185.02; CYPH×1515 09:30 $1.15 → close $1.19 +60.60; ABCL×147 09:30 $11.81 → close $11.57 -36.01; AZI×1272 09:30 $1.37 → close $1.44 +89.04; BHP×4 09:30 $91.01 → close $93.63 +10.48; HUMA×615 09:30 $0.71 → close $0.68 -15.99; BTGO×65 09:30 $6.61 → close $6.60 -0.33; ASST×27 09:30 $16.00 → close $16.13 +3.51; ZLAB×16 09:30 $26.57 → close $26.02 -8.80; CRSP×7 09:30 $58.73 → close $58.12 -4.27; APA×9 09:30 $44.76 → close $44.39 -3.33 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.43 | ▲ 09:30 equity $10,114.03 vs yday $9,809.78 (+304.25) | 09:30 open · cash $127.43 (unchanged overnight, no fees) · equity $10,114.03 vs prior close $9,809.78 (+304.25) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 147 | $11.57 | $2.47 | $-40.91 | $1,825.75 | ▼ -40.91 after sell → book $10,111.56; vs 09:30 mark -2.47 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 1272 | $1.46 | $16.63 | $+81.44 | $3,666.23 | ▲ +81.44 after sell → book $10,094.92; vs 09:30 mark -16.64 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 4 | $95.72 | $2.02 | $+14.82 | $4,047.09 | ▲ +14.82 after sell → book $10,092.90; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 615 | $0.67 | $6.10 | $-32.59 | $4,455.50 | ▼ -32.59 after sell → book $10,086.80; vs 09:30 mark -6.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 65 | $6.95 | $2.21 | $+18.03 | $4,905.04 | ▲ +18.03 after sell → book $10,084.59; vs 09:30 mark -2.21 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 27 | $17.66 | $2.09 | $+40.66 | $5,379.77 | ▲ +40.66 after sell → book $10,082.50; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 16 | $26.25 | $2.06 | $-9.22 | $5,797.71 | ▼ -9.22 after sell → book $10,080.44; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 9 | $44.52 | $2.04 | $-6.21 | $6,196.36 | ▼ -6.21 after sell → book $10,078.41; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 483 | $4.49 | $6.23 | — | $4,021.46 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $2168.72; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 318 | $6.81 | $4.10 | — | $1,851.77 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $2168.72; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 2 | $119.43 | $2.00 | — | $1,610.92 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $264.54; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 107 | $2.47 | $2.31 | — | $1,344.32 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; combo leftover $264.54; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 2 | $115.18 | $2.00 | — | $1,111.96 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $264.54; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 22 | $11.70 | $2.06 | — | $852.50 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; combo leftover $264.54; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 23 | $11.10 | $2.06 | — | $595.26 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; combo leftover $264.54; owner union_news_g_h1 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 81 | $3.24 | $2.23 | — | $330.59 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; combo leftover $264.54; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $330.59 | ▲ close $10,127.32 vs 09:30 $10,114.03 (session +71.89) | 16:00 close · cash $330.59 · equity $10,127.32 vs 09:30 $10,114.03 (+13.29; session marks +71.89) · 11 name(s) marked open→close (per-name table). MRNA×11 09:30 $133.11 → close $145.13 +132.22; CYPH×1515 09:30 $1.32 → close $1.42 +151.50; CRSP×7 09:30 $59.72 → close $59.50 -1.54; XHG×483 09:30 $4.49 → close $4.41 -38.64; CAPR×318 09:30 $6.81 → close $6.29 -165.36; AU×2 09:30 $119.43 → close $121.22 +3.58; AUTL×107 09:30 $2.47 → close $2.41 -6.42; FUTU×2 09:30 $115.18 → close $123.64 +16.92; MARA×22 09:30 $11.70 → close $11.26 -9.68; BTDR×23 09:30 $11.10 → close $11.37 +6.32; HIVE×81 09:30 $3.24 → close $3.03 -17.01 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $330.59 | ▲ 09:30 equity $11,215.88 vs yday $10,127.32 (+1,088.56) | 09:30 open · cash $330.59 (unchanged overnight, no fees) · equity $11,215.88 vs prior close $10,127.32 (+1088.56) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 11 | $142.70 | $2.05 | $-85.91 | $1,898.24 | ▼ -85.91 after sell → book $11,213.83; vs 09:30 mark -2.05 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1515 | $1.83 | $19.82 | $+990.84 | $4,650.87 | ▲ +990.84 after sell → book $11,194.01; vs 09:30 mark -19.82 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 483 | $4.32 | $6.33 | $-94.67 | $6,731.11 | ▼ -94.67 after sell → book $11,187.69; vs 09:30 mark -6.32 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 318 | $8.03 | $4.18 | $+379.68 | $9,280.47 | ▲ +379.68 after sell → book $11,183.51; vs 09:30 mark -4.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 2 | $120.51 | $2.02 | $-1.85 | $9,519.48 | ▼ -1.85 after sell → book $11,181.50; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 107 | $2.40 | $2.34 | $-12.14 | $9,773.94 | ▼ -12.14 after sell → book $11,179.16; vs 09:30 mark -2.34 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 2 | $121.00 | $2.02 | $+7.63 | $10,013.92 | ▲ +7.63 after sell → book $11,177.14; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 22 | $11.17 | $2.08 | $-15.79 | $10,257.59 | ▼ -15.79 after sell → book $11,175.07; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 23 | $11.48 | $2.08 | $+4.72 | $10,519.55 | ▲ +4.72 after sell → book $11,172.99; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 81 | $2.99 | $2.26 | $-24.74 | $10,759.48 | ▼ -24.74 after sell → book $11,170.73; vs 09:30 mark -2.26 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,759.48 | ▼ close $11,159.00 vs 09:30 $11,215.88 (session -11.72) | 16:00 close · cash $10,759.48 · equity $11,159.00 vs 09:30 $11,215.88 (-56.88; session marks -11.72) · 1 name(s) marked open→close (per-name table). CRSP×7 09:30 $58.75 → close $57.08 -11.72 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,759.48 | ▲ 09:30 equity $11,164.99 vs yday $11,159.00 (+5.99) | 09:30 open · cash $10,759.48 (unchanged overnight, no fees) · equity $11,164.99 vs prior close $11,159.00 (+5.99) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 7 | $57.93 | $2.03 | $-9.64 | $11,162.96 | ▼ -9.64 after sell → book $11,162.96; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 81 | $24.11 | $2.23 | — | $9,207.82 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $1953.52; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 1252 | $1.56 | $16.15 | — | $7,238.54 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $1953.52; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 479 | $4.07 | $6.18 | — | $5,282.84 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $1953.52; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 102 | $19.04 | $2.30 | — | $3,338.46 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $1953.52; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 59 | $9.42 | $2.17 | — | $2,780.51 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; combo leftover $556.41; owner union_news_g_h1 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 15 | $35.05 | $2.04 | — | $2,252.73 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $556.41; owner union_news_g_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 19 | $28.86 | $2.05 | — | $1,702.34 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.7; combo leftover $556.41; owner union_news_g_h1 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 63 | $8.72 | $2.18 | — | $1,150.80 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.0; combo leftover $556.41; owner union_news_g_h1 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 4 | $118.52 | $2.00 | — | $674.72 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+21.7; combo leftover $556.41; owner union_news_g_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 7 | $77.13 | $2.01 | — | $132.80 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; ⚪; ret5=+13.8; combo leftover $556.41; owner union_news_g_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.80 | ▲ close $11,868.57 vs 09:30 $11,164.99 (session +744.91) | 16:00 close · cash $132.80 · equity $11,868.57 vs 09:30 $11,164.99 (+703.58; session marks +744.91) · 10 name(s) marked open→close (per-name table). REAX×81 09:30 $24.11 → close $28.43 +349.92; CYPH×1252 09:30 $1.56 → close $1.64 +100.16; XHG×479 09:30 $4.07 → close $4.02 -23.95; ASST×102 09:30 $19.04 → close $21.39 +239.70; RUM×59 09:30 $9.42 → close $10.23 +47.79; EZPW×15 09:30 $35.05 → close $35.23 +2.70; ZYME×19 09:30 $28.86 → close $27.47 -26.41; EOLS×63 09:30 $8.72 → close $8.97 +16.06; AU×4 09:30 $118.52 → close $123.39 +19.48; FCX×7 09:30 $77.13 → close $79.91 +19.46 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.80 | ▼ 09:30 equity $11,475.87 vs yday $11,868.57 (-392.70) | 09:30 open · cash $132.80 (unchanged overnight, no fees) · equity $11,475.87 vs prior close $11,868.57 (-392.70) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 81 | $26.61 | $2.26 | $+198.00 | $2,285.94 | ▲ +198.00 after sell → book $11,473.60; vs 09:30 mark -2.27 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 1252 | $1.60 | $16.37 | $+17.55 | $4,272.77 | ▲ +17.55 after sell → book $11,457.23; vs 09:30 mark -16.38 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 102 | $20.72 | $2.33 | $+166.73 | $6,383.88 | ▲ +166.73 after sell → book $11,454.90; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 59 | $10.07 | $2.19 | $+34.00 | $6,975.82 | ▲ +34.00 after sell → book $11,452.71; vs 09:30 mark -2.19 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 15 | $35.70 | $2.06 | $+5.66 | $7,509.27 | ▲ +5.66 after sell → book $11,450.66; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 19 | $27.56 | $2.07 | $-28.81 | $8,030.84 | ▼ -28.81 after sell → book $11,448.59; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 63 | $8.86 | $2.20 | $+4.44 | $8,586.82 | ▲ +4.44 after sell → book $11,446.39; vs 09:30 mark -2.20 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 4 | $119.80 | $2.02 | $+1.10 | $9,064.00 | ▲ +1.10 after sell → book $11,444.37; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 7 | $79.34 | $2.03 | $+11.43 | $9,617.35 | ▲ +11.43 after sell → book $11,442.34; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 159 | $14.11 | $2.47 | — | $7,371.39 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $2244.05; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 386 | $5.81 | $4.98 | — | $5,123.75 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $2244.05; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SUJA` | 238 | $9.39 | $3.07 | — | $2,885.86 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+39.0; combo leftover $2244.05; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 51 | $11.12 | $2.14 | — | $2,316.60 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; combo leftover $577.17; owner union_news_g_h1 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 69 | $8.29 | $2.20 | — | $1,742.39 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; combo leftover $577.17; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 33 | $17.41 | $2.09 | — | $1,165.77 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-9.2; combo leftover $577.17; owner union_news_g_h1 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 51 | $11.22 | $2.14 | — | $591.41 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.8; combo leftover $577.17; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 2 | $267.02 | $2.00 | — | $55.37 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.7; combo leftover $577.17; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.37 | ▲ close $11,731.24 vs 09:30 $11,475.87 (session +309.99) | 16:00 close · cash $55.37 · equity $11,731.24 vs 09:30 $11,475.87 (+255.37; session marks +309.99) · 9 name(s) marked open→close (per-name table). XHG×479 09:30 $3.81 → close $4.06 +119.75; BYND×159 09:30 $14.11 → close $14.25 +22.26; USDE×386 09:30 $5.81 → close $5.98 +65.62; SUJA×238 09:30 $9.39 → close $9.44 +11.90; FLNC×51 09:30 $11.12 → close $11.08 -2.04; CAPR×69 09:30 $8.29 → close $9.36 +73.83; FWRD×33 09:30 $17.41 → close $17.63 +7.26; TRLV×51 09:30 $11.22 → close $11.43 +10.71; FNV×2 09:30 $267.02 → close $267.37 +0.70 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.37 | ▲ 09:30 equity $11,923.76 vs yday $11,731.24 (+192.52) | 09:30 open · cash $55.37 (unchanged overnight, no fees) · equity $11,923.76 vs prior close $11,731.24 (+192.52) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 479 | $4.06 | $6.27 | $-17.24 | $1,993.84 | ▼ -17.24 after sell → book $11,917.49; vs 09:30 mark -6.27 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 159 | $14.20 | $2.51 | $+9.33 | $4,249.13 | ▲ +9.33 after sell → book $11,914.98; vs 09:30 mark -2.51 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 386 | $6.50 | $5.06 | $+256.30 | $6,753.07 | ▲ +256.30 after sell → book $11,909.92; vs 09:30 mark -5.06 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 238 | $9.41 | $3.13 | $-1.44 | $8,989.52 | ▼ -1.44 after sell → book $11,906.79; vs 09:30 mark -3.13 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 51 | $11.52 | $2.16 | $+16.09 | $9,574.87 | ▲ +16.09 after sell → book $11,904.62; vs 09:30 mark -2.17 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 69 | $9.19 | $2.22 | $+57.68 | $10,206.77 | ▲ +57.68 after sell → book $11,902.41; vs 09:30 mark -2.21 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FWRD` | 33 | $17.60 | $2.11 | $+2.07 | $10,785.46 | ▲ +2.07 after sell → book $11,900.30; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 51 | $11.38 | $2.16 | $+3.85 | $11,363.67 | ▲ +3.85 after sell → book $11,898.13; vs 09:30 mark -2.17 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 2 | $267.23 | $2.02 | $-3.59 | $11,896.12 | ▼ -3.59 after sell → book $11,896.12; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 800 | $2.60 | $10.32 | — | $9,805.80 | — | top 4 by hot; rank hot_score; list flatten; ret5=+13.0; combo leftover $2081.82; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 50 | $41.44 | $2.14 | — | $7,731.66 | — | top 4 by hot; rank hot_score; list flatten; ret5=+3.1; combo leftover $2081.82; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 90 | $22.93 | $2.26 | — | $5,665.70 | — | top 4 by hot; rank hot_score; list mover_buy; 🔵; ret5=+9.5; combo leftover $2081.82; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 144 | $14.42 | $2.42 | — | $3,586.80 | — | top 4 by hot; rank hot_score; list flatten; ret5=+7.1; combo leftover $2081.82; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 8 | $81.65 | $2.01 | — | $2,931.58 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+2.0; combo leftover $717.36; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 2 | $318.88 | $2.00 | — | $2,291.83 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+1.9; combo leftover $717.36; owner union_news_g_h1 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 3 | $222.86 | $2.00 | — | $1,621.25 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=-3.6; combo leftover $717.36; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,621.25 | ▲ close $11,978.95 vs 09:30 $11,923.76 (session +105.98) | 16:00 close · cash $1,621.25 · equity $11,978.95 vs 09:30 $11,923.76 (+55.19; session marks +105.98) · 7 name(s) marked open→close (per-name table). SLI×800 09:30 $2.60 → close $2.64 +32.00; RRC×50 09:30 $41.44 → close $41.64 +10.00; PGY×90 09:30 $22.93 → close $23.26 +29.70; CRK×144 09:30 $14.42 → close $14.62 +28.80; ACMR×8 09:30 $81.65 → close $80.49 -9.28; LRCX×2 09:30 $318.88 → close $318.58 -0.60; NVDA×3 09:30 $222.86 → close $227.98 +15.36 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,621.25 | ▲ 09:30 equity $12,000.17 vs yday $11,978.95 (+21.22) | 09:30 open · cash $1,621.25 (unchanged overnight, no fees) · equity $12,000.17 vs prior close $11,978.95 (+21.22) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 800 | $2.68 | $10.47 | $+43.21 | $3,754.78 | ▲ +43.21 after sell → book $11,989.70; vs 09:30 mark -10.47 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 50 | $41.74 | $2.17 | $+10.69 | $5,839.61 | ▲ +10.69 after sell → book $11,987.53; vs 09:30 mark -2.17 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 90 | $23.21 | $2.29 | $+20.65 | $7,926.22 | ▲ +20.65 after sell → book $11,985.24; vs 09:30 mark -2.29 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 144 | $14.63 | $2.46 | $+25.36 | $10,030.48 | ▲ +25.36 after sell → book $11,982.78; vs 09:30 mark -2.46 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 8 | $79.27 | $2.03 | $-23.09 | $10,662.60 | ▼ -23.09 after sell → book $11,980.74; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 2 | $318.03 | $2.02 | $-5.71 | $11,296.65 | ▼ -5.71 after sell → book $11,978.73; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 3 | $227.36 | $2.02 | $+9.48 | $11,976.71 | ▲ +9.48 after sell → book $11,976.71; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 149 | $14.00 | $2.44 | — | $9,888.27 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $2095.92; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 215 | $9.73 | $2.77 | — | $7,793.55 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+47.1; combo leftover $2095.92; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 15 | $137.19 | $2.04 | — | $5,733.66 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+7.1; combo leftover $2095.92; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 14 | $146.07 | $2.03 | — | $3,686.65 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $2095.92; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 12 | $41.74 | $2.03 | — | $3,183.74 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+2.4; combo leftover $526.66; owner union_news_g_h1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 16 | $32.90 | $2.04 | — | $2,655.31 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; combo leftover $526.66; owner union_news_g_h1 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 61 | $8.61 | $2.17 | — | $2,127.92 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.7; combo leftover $526.66; owner union_news_g_h1 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 3 | $141.76 | $2.00 | — | $1,700.64 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; combo leftover $526.66; owner union_news_g_h1 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 27 | $19.25 | $2.07 | — | $1,178.82 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+14.1; combo leftover $526.66; owner union_news_g_h1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 28 | $18.75 | $2.07 | — | $651.75 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-5.0; combo leftover $526.66; owner union_news_g_h1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 18 | $28.91 | $2.04 | — | $129.32 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+9.2; combo leftover $526.66; owner union_news_g_h1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.32 | ▼ close $11,851.18 vs 09:30 $12,000.17 (session -101.82) | 16:00 close · cash $129.32 · equity $11,851.18 vs 09:30 $12,000.17 (-148.99; session marks -101.82) · 11 name(s) marked open→close (per-name table). RRC×12 09:30 $41.74 → close $41.46 -3.36; BYND×149 09:30 $14.00 → close $13.86 -20.86; CAPR×215 09:30 $9.73 → close $9.59 -30.10; MRNA×15 09:30 $137.19 → close $137.99 +12.00; ANF×14 09:30 $146.07 → close $148.42 +32.90; SEDG×16 09:30 $32.90 → close $31.41 -23.84; OPTX×61 09:30 $8.61 → close $8.52 -5.49; SMTC×3 09:30 $141.76 → close $131.17 -31.77; ERAS×27 09:30 $19.25 → close $18.03 -32.94; BBWI×28 09:30 $18.75 → close $19.22 +13.16; ZYME×18 09:30 $28.91 → close $28.27 -11.52 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.32 | ▼ 09:30 equity $11,759.02 vs yday $11,851.18 (-92.16) | 09:30 open · cash $129.32 (unchanged overnight, no fees) · equity $11,759.02 vs prior close $11,851.18 (-92.16) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 215 | $9.50 | $2.83 | $-55.05 | $2,169.00 | ▼ -55.05 after sell → book $11,756.20; vs 09:30 mark -2.82 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 15 | $134.10 | $2.06 | $-50.45 | $4,178.44 | ▼ -50.45 after sell → book $11,754.14; vs 09:30 mark -2.06 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 14 | $148.03 | $2.06 | $+23.35 | $6,248.80 | ▲ +23.35 after sell → book $11,752.08; vs 09:30 mark -2.06 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 12 | $42.00 | $2.05 | $-0.95 | $6,750.75 | ▼ -0.95 after sell → book $11,750.03; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 16 | $31.15 | $2.06 | $-32.10 | $7,247.10 | ▼ -32.10 after sell → book $11,747.98; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 61 | $8.52 | $2.19 | $-9.86 | $7,764.62 | ▼ -9.86 after sell → book $11,745.78; vs 09:30 mark -2.20 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 3 | $132.30 | $2.02 | $-32.40 | $8,159.50 | ▼ -32.40 after sell → book $11,743.76; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 27 | $17.87 | $2.09 | $-41.42 | $8,639.90 | ▼ -41.42 after sell → book $11,741.67; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 28 | $19.25 | $2.09 | $+9.83 | $9,176.81 | ▲ +9.83 after sell → book $11,739.58; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 18 | $28.06 | $2.06 | $-19.41 | $9,679.82 | ▼ -19.41 after sell → book $11,737.51; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,679.82 | ▼ close $11,661.52 vs 09:30 $11,759.02 (session -75.99) | 16:00 close · cash $9,679.82 · equity $11,661.52 vs 09:30 $11,759.02 (-97.50; session marks -75.99) · 1 name(s) marked open→close (per-name table). BYND×149 09:30 $13.81 → close $13.30 -75.99 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,679.82 | ▼ 09:30 equity $11,622.78 vs yday $11,661.52 (-38.74) | 09:30 open · cash $9,679.82 (unchanged overnight, no fees) · equity $11,622.78 vs prior close $11,661.52 (-38.74) | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 149 | $13.04 | $2.48 | $-147.95 | $11,620.31 | ▼ -147.95 after sell → book $11,620.31; vs 09:30 mark -2.47 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,620.31 | ▲ close $11,620.31 vs 09:30 $11,622.78 (session +0.00) | 16:00 close · cash $11,620.31 · no lots left · equity $11,620.31. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,620.31 | ▲ 09:30 equity $11,620.31 vs yday $11,620.31 (-0.00) | 09:30 open · cash $11,620.31 (unchanged overnight, no fees) · equity $11,620.31 vs prior close $11,620.31 (-0.00) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,620.31 | ▲ close $11,620.31 vs 09:30 $11,620.31 (session +0.00) | 16:00 close · cash $11,620.31 · no lots left · equity $11,620.31. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,620.31 | ▲ 09:30 equity $11,620.31 vs yday $11,620.31 (-0.00) | 09:30 open · cash $11,620.31 (unchanged overnight, no fees) · equity $11,620.31 vs prior close $11,620.31 (-0.00) | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1142 | $1.78 | $14.73 | — | $9,572.82 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $2033.55; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 110 | $18.40 | $2.32 | — | $7,546.50 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $2033.55; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 148 | $13.71 | $2.43 | — | $5,514.98 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $2033.55; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 85 | $23.88 | $2.25 | — | $3,482.94 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $2033.55; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 15 | $32.88 | $2.04 | — | $2,987.70 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+16.2; combo leftover $497.56; owner union_news_g_h1 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 65 | $7.59 | $2.19 | — | $2,492.17 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.5; combo leftover $497.56; owner union_news_g_h1 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 31 | $15.87 | $2.08 | — | $1,998.11 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $497.56; owner union_news_g_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $1,644.38 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+3.3; combo leftover $497.56; owner union_news_g_h1 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $1,287.90 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-12.3; combo leftover $497.56; owner union_news_g_h1 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 10 | $47.60 | $2.02 | — | $809.88 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; combo leftover $497.56; owner union_news_g_h1 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $809.88 | ▼ close $11,236.05 vs 09:30 $11,620.31 (session -350.22) | 16:00 close · cash $809.88 · equity $11,236.05 vs 09:30 $11,620.31 (-384.26; session marks -350.22) · 10 name(s) marked open→close (per-name table). GPRO×1142 09:30 $1.78 → close $1.39 -445.38; REAX×110 09:30 $18.40 → close $18.40 +0.00; CNH×148 09:30 $13.71 → close $13.84 +19.24; MMED×85 09:30 $23.88 → close $23.84 -3.40; CNXC×15 09:30 $32.88 → close $32.85 -0.45; OPTX×65 09:30 $7.59 → close $7.76 +11.05; FRNM×31 09:30 $15.87 → close $16.90 +31.93; AVGO×1 09:30 $351.74 → close $357.16 +5.42; CIEN×1 09:30 $354.49 → close $317.46 -37.03; HPE×10 09:30 $47.60 → close $54.44 +68.40 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $809.88 | ▲ 09:30 equity $11,300.48 vs yday $11,236.05 (+64.43) | 09:30 open · cash $809.88 (unchanged overnight, no fees) · equity $11,300.48 vs prior close $11,236.05 (+64.43) | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 110 | $18.15 | $2.35 | $-32.17 | $2,804.02 | ▼ -32.17 after sell → book $11,298.12; vs 09:30 mark -2.36 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 148 | $13.89 | $2.48 | $+21.73 | $4,857.27 | ▲ +21.73 after sell → book $11,295.65; vs 09:30 mark -2.47 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 85 | $23.84 | $2.28 | $-7.92 | $6,881.39 | ▼ -7.92 after sell → book $11,293.37; vs 09:30 mark -2.28 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 15 | $32.48 | $2.06 | $-10.09 | $7,366.54 | ▼ -10.09 after sell → book $11,291.32; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 65 | $7.79 | $2.21 | $+8.61 | $7,870.68 | ▲ +8.61 after sell → book $11,289.11; vs 09:30 mark -2.21 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 1 | $359.70 | $2.01 | $+3.95 | $8,228.37 | ▲ +3.95 after sell → book $11,287.10; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 1 | $321.67 | $2.01 | $-36.83 | $8,548.03 | ▼ -36.83 after sell → book $11,285.09; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 10 | $53.85 | $2.04 | $+58.44 | $9,084.49 | ▲ +58.44 after sell → book $11,283.05; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 84 | $25.18 | $2.24 | — | $6,967.12 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $2119.71; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 269 | $7.87 | $3.47 | — | $4,846.62 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $2119.71; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 366 | $5.79 | $4.72 | — | $2,722.76 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $2119.71; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 2 | $263.36 | $2.00 | — | $2,194.05 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; combo leftover $544.55; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 280 | $1.94 | $3.61 | — | $1,647.23 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+18.3; combo leftover $544.55; owner union_news_g_h1 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 3 | $137.35 | $2.00 | — | $1,233.19 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+5.4; combo leftover $544.55; owner union_news_g_h1 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 2 | $236.82 | $2.00 | — | $757.55 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.1; combo leftover $544.55; owner union_news_g_h1 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 7 | $75.65 | $2.01 | — | $225.99 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+6.0; combo leftover $544.55; owner union_news_g_h1 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $225.99 | ▲ close $11,764.04 vs 09:30 $11,300.48 (session +503.04) | 16:00 close · cash $225.99 · equity $11,764.04 vs 09:30 $11,300.48 (+463.56; session marks +503.04) · 10 name(s) marked open→close (per-name table). GPRO×1142 09:30 $1.48 → close $1.70 +251.24; FRNM×31 09:30 $16.40 → close $16.31 -2.79; ASST×84 09:30 $25.18 → close $27.14 +164.64; USDE×269 09:30 $7.87 → close $7.93 +16.14; DFDV×366 09:30 $5.79 → close $5.87 +29.28; CRM×2 09:30 $263.36 → close $259.23 -8.26; BAK×280 09:30 $1.94 → close $1.89 -14.00; MSTR×3 09:30 $137.35 → close $142.80 +16.35; BE×2 09:30 $236.82 → close $252.87 +32.10; MRX×7 09:30 $75.65 → close $78.27 +18.34 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $225.99 | ▼ 09:30 equity $11,517.92 vs yday $11,764.04 (-246.12) | 09:30 open · cash $225.99 (unchanged overnight, no fees) · equity $11,517.92 vs prior close $11,764.04 (-246.12) | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 1142 | $1.56 | $14.94 | $-275.20 | $1,998.28 | ▼ -275.20 after sell → book $11,502.98; vs 09:30 mark -14.94 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 31 | $16.74 | $2.10 | $+22.78 | $2,515.12 | ▲ +22.78 after sell → book $11,500.88; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 84 | $26.44 | $2.27 | $+101.32 | $4,733.81 | ▲ +101.32 after sell → book $11,498.61; vs 09:30 mark -2.27 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 269 | $7.76 | $3.53 | $-36.59 | $6,817.71 | ▼ -36.59 after sell → book $11,495.07; vs 09:30 mark -3.54 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 366 | $5.81 | $4.80 | $-2.20 | $8,939.38 | ▼ -2.20 after sell → book $11,490.28; vs 09:30 mark -4.79 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 2 | $253.72 | $2.02 | $-23.29 | $9,444.80 | ▼ -23.29 after sell → book $11,488.26; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 280 | $1.94 | $3.67 | $-7.28 | $9,984.33 | ▼ -7.28 after sell → book $11,484.59; vs 09:30 mark -3.67 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 2 | $267.76 | $2.02 | $+57.87 | $10,517.83 | ▲ +57.87 after sell → book $11,482.57; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,517.83 | ▼ close $11,464.36 vs 09:30 $11,517.92 (session -18.21) | 16:00 close · cash $10,517.83 · equity $11,464.36 vs 09:30 $11,517.92 (-53.56; session marks -18.21) · 2 name(s) marked open→close (per-name table). MSTR×3 09:30 $137.62 → close $136.52 -3.30; MRX×7 09:30 $78.84 → close $76.71 -14.91 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,517.83 | ▲ 09:30 equity $11,479.49 vs yday $11,464.36 (+15.13) | 09:30 open · cash $10,517.83 (unchanged overnight, no fees) · equity $11,479.49 vs prior close $11,464.36 (+15.13) | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 3 | $141.82 | $2.02 | $+9.39 | $10,941.28 | ▲ +9.39 after sell → book $11,477.48; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 7 | $76.60 | $2.03 | $+2.61 | $11,475.44 | ▲ +2.61 after sell → book $11,475.44; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,475.44 | ▲ close $11,475.44 vs 09:30 $11,479.49 (session +0.00) | 16:00 close · cash $11,475.44 · no lots left · equity $11,475.44. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,475.44 | ▲ 09:30 equity $11,475.44 vs yday $11,475.44 (+0.00) | 09:30 open · cash $11,475.44 (unchanged overnight, no fees) · equity $11,475.44 vs prior close $11,475.44 (+0.00) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,475.44 | ▲ close $11,475.44 vs 09:30 $11,475.44 (session +0.00) | 16:00 close · cash $11,475.44 · no lots left · equity $11,475.44. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 445.99 < 1 share @ 1646.93 |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new union_hot_n4_h1 |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new union_hot_n4_h1 |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new union_hot_n4_h1 |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new union_news_g_h1 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new union_news_g_h1 |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new union_news_g_h1 |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new union_news_g_h1 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new union_news_g_h1 |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new union_news_g_h1 |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new union_news_g_h1 |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new union_news_g_h1 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new union_hot_n4_h1 |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new union_hot_n4_h1 |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new union_hot_n4_h1 |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new union_hot_n4_h1 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new union_news_g_h1 |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new union_news_g_h1 |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new union_news_g_h1 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new union_news_g_h1 |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new union_news_g_h1 |
| 2026-08-21 | `DE` | cash | leftover split 264.54 < 1 share @ 623.26 |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new union_news_g_h1 |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new union_news_g_h1 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new union_news_g_h1 |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new union_news_g_h1 |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new union_news_g_h1 |
| 2026-08-27 | `MU` | cash | leftover split 717.36 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 717.36 < 1 share @ 1746.53 |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new union_hot_n4_h1 |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new union_hot_n4_h1 |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new union_hot_n4_h1 |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new union_news_g_h1 |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new union_news_g_h1 |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new union_news_g_h1 |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new union_news_g_h1 |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new union_news_g_h1 |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new union_news_g_h1 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new union_news_g_h1 |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new union_news_g_h1 |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new union_hot_n4_h1 |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new union_hot_n4_h1 |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new union_hot_n4_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new union_hot_n4_h1 |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new union_news_g_h1 |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new union_news_g_h1 |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new union_news_g_h1 |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new union_news_g_h1 |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new union_news_g_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new union_news_g_h1 |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new union_news_g_h1 |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new union_news_g_h1 |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new union_hot_n4_h1 |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new union_hot_n4_h1 |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new union_hot_n4_h1 |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new union_hot_n4_h1 |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new union_news_g_h1 |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new union_news_g_h1 |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new union_news_g_h1 |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new union_news_g_h1 |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new union_news_g_h1 |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new union_news_g_h1 |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new union_news_g_h1 |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new union_news_g_h1 |
| 2026-09-03 | `DE` | cash | leftover split 497.56 < 1 share @ 703.25 |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new union_hot_n4_h1 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new union_hot_n4_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new union_hot_n4_h1 |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new union_hot_n4_h1 |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new union_news_g_h1 |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new union_news_g_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new union_news_g_h1 |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new union_news_g_h1 |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new union_hot_n4_h1 |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new union_hot_n4_h1 |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new union_hot_n4_h1 |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new union_hot_n4_h1 |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new union_news_g_h1 |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new union_news_g_h1 |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new union_news_g_h1 |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new union_hot_n4_h1 |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new union_hot_n4_h1 |
| 2026-09-10 | `PAYP` | hard_red | hard-red S=-13.28 sit; no new union_hot_n4_h1 |
| 2026-09-10 | `CRWV` | hard_red | hard-red S=-13.28 sit; no new union_hot_n4_h1 |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new union_news_g_h1 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new union_news_g_h1 |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new union_news_g_h1 |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new union_news_g_h1 |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new union_news_g_h1 |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new union_news_g_h1 |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new union_news_g_h1 |
| 2026-09-10 | `CLSK` | hard_red | hard-red S=-13.28 sit; no new union_news_g_h1 |
