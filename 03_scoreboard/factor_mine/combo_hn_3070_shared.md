# Factor mine action — `combo_hn_3070_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_hot_n4_h1/union_news_g_h1 w=0.3,0.7 net=priority

Cash book **+11.89%** ($11,189) · signal-only (no cash/fees) was —. Starts YES **15/18**. Fills 196 · skips 74 · realized $+1182.06.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_hot_n4_h1 30%, union_news_g_h1 70%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_hot_n4_h1 30%, union_news_g_h1 70%.
- Member: union_hot_n4_h1 (30% · long · hold 1).
- Member: union_news_g_h1 (70% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,655.82.

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
| 2026-08-14 | `QMCO` | 31 | — | $24.68 | +0.00 | $26.11 | +44.33 | +44.33 | +0.00 | +44.33 |
| 2026-08-14 | `ARX` | 39 | — | $19.57 | +0.00 | $19.58 | +0.39 | +0.39 | +0.00 | +0.39 |
| 2026-08-14 | `ZENA` | 353 | — | $2.20 | +0.00 | $2.14 | -21.18 | -21.18 | +0.00 | -21.18 |
| 2026-08-14 | `AIRO` | 69 | — | $11.12 | +0.00 | $9.57 | -106.95 | -106.95 | +0.00 | -106.95 |
| 2026-08-14 | `TLN` | 2 | — | $359.83 | +0.00 | $362.74 | +5.82 | +5.82 | +0.00 | +5.82 |
| 2026-08-14 | `VST` | 7 | — | $146.90 | +0.00 | $148.13 | +8.61 | +8.61 | +0.00 | +8.61 |
| 2026-08-14 | `NRG` | 8 | — | $120.00 | +0.00 | $126.24 | +49.92 | +49.92 | +0.00 | +49.92 |
| 2026-08-14 | `ANGX` | 241 | — | $4.31 | +0.00 | $4.37 | +14.46 | +14.46 | +0.00 | +14.46 |
| 2026-08-14 | `MH` | 76 | — | $13.55 | +0.00 | $13.10 | -34.20 | -34.20 | +0.00 | -34.20 |
| 2026-08-14 | `HLIT` | 78 | — | $13.18 | +0.00 | $13.92 | +57.72 | +57.72 | +0.00 | +57.72 |
| 2026-08-17 | `QMCO` | 31 | $26.11 | $24.83 | -39.68 | — | +0.00 | -39.68 | +4.65 | — |
| 2026-08-17 | `ARX` | 39 | $19.58 | $19.57 | -0.39 | — | +0.00 | -0.39 | +0.00 | — |
| 2026-08-17 | `ZENA` | 353 | $2.14 | $2.08 | -19.42 | — | +0.00 | -19.42 | -40.60 | — |
| 2026-08-17 | `AIRO` | 69 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -106.95 | — |
| 2026-08-17 | `TLN` | 2 | $362.74 | $367.88 | +10.28 | — | +0.00 | +10.28 | +16.10 | — |
| 2026-08-17 | `VST` | 7 | $148.13 | $149.37 | +8.68 | — | +0.00 | +8.68 | +17.29 | — |
| 2026-08-17 | `NRG` | 8 | $126.24 | $127.40 | +9.28 | — | +0.00 | +9.28 | +59.20 | — |
| 2026-08-17 | `ANGX` | 241 | $4.37 | $4.60 | +55.43 | — | +0.00 | +55.43 | +69.89 | — |
| 2026-08-17 | `MH` | 76 | $13.10 | $13.16 | +4.56 | — | +0.00 | +4.56 | -29.64 | — |
| 2026-08-17 | `HLIT` | 78 | $13.92 | $13.84 | -6.24 | — | +0.00 | -6.24 | +51.48 | — |
| 2026-08-17 | `XHG` | 185 | — | $4.19 | +0.00 | $3.91 | -51.80 | -51.80 | +0.00 | -51.80 |
| 2026-08-17 | `CAPR` | 113 | — | $6.87 | +0.00 | $7.45 | +65.54 | +65.54 | +0.00 | +65.54 |
| 2026-08-17 | `STDN` | 56 | — | $13.64 | +0.00 | $13.31 | -18.48 | -18.48 | +0.00 | -18.48 |
| 2026-08-17 | `HTFL` | 18 | — | $41.23 | +0.00 | $41.94 | +12.78 | +12.78 | +0.00 | +12.78 |
| 2026-08-17 | `DVN` | 31 | — | $46.18 | +0.00 | $47.57 | +43.09 | +43.09 | +0.00 | +43.09 |
| 2026-08-17 | `EOG` | 10 | — | $142.77 | +0.00 | $146.15 | +33.80 | +33.80 | +0.00 | +33.80 |
| 2026-08-17 | `FANG` | 7 | — | $202.70 | +0.00 | $206.29 | +25.13 | +25.13 | +0.00 | +25.13 |
| 2026-08-17 | `CELC` | 15 | — | $92.99 | +0.00 | $92.44 | -8.25 | -8.25 | +0.00 | -8.25 |
| 2026-08-17 | `OUST` | 29 | — | $49.00 | +0.00 | $48.13 | -25.23 | -25.23 | +0.00 | -25.23 |
| 2026-08-18 | `XHG` | 185 | $3.91 | $3.94 | +5.55 | — | +0.00 | +5.55 | -46.25 | — |
| 2026-08-18 | `CAPR` | 113 | $7.45 | $7.50 | +5.65 | $7.08 | -47.46 | -41.81 | +71.19 | +23.73 |
| 2026-08-18 | `STDN` | 56 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -18.48 | — |
| 2026-08-18 | `HTFL` | 18 | $41.94 | $41.50 | -7.92 | — | +0.00 | -7.92 | +4.86 | — |
| 2026-08-18 | `DVN` | 31 | $47.57 | $48.00 | +13.33 | — | +0.00 | +13.33 | +56.42 | — |
| 2026-08-18 | `EOG` | 10 | $146.15 | $148.04 | +18.90 | — | +0.00 | +18.90 | +52.70 | — |
| 2026-08-18 | `FANG` | 7 | $206.29 | $208.93 | +18.48 | — | +0.00 | +18.48 | +43.61 | — |
| 2026-08-18 | `CELC` | 15 | $92.44 | $92.38 | -0.90 | — | +0.00 | -0.90 | -9.15 | — |
| 2026-08-18 | `OUST` | 29 | $48.13 | $45.09 | -88.16 | — | +0.00 | -88.16 | -113.39 | — |
| 2026-08-19 | `CAPR` | 113 | $7.08 | $7.19 | +12.43 | — | +0.00 | +12.43 | +36.16 | — |
| 2026-08-20 | `MRNA` | 5 | — | $150.14 | +0.00 | $133.32 | -84.10 | -84.10 | +0.00 | -84.10 |
| 2026-08-20 | `CYPH` | 673 | — | $1.15 | +0.00 | $1.19 | +26.92 | +26.92 | +0.00 | +26.92 |
| 2026-08-20 | `ABCL` | 65 | — | $11.81 | +0.00 | $11.57 | -15.92 | -15.92 | +0.00 | -15.92 |
| 2026-08-20 | `AZI` | 565 | — | $1.37 | +0.00 | $1.44 | +39.55 | +39.55 | +0.00 | +39.55 |
| 2026-08-20 | `BHP` | 11 | — | $91.01 | +0.00 | $93.63 | +28.82 | +28.82 | +0.00 | +28.82 |
| 2026-08-20 | `HUMA` | 1462 | — | $0.71 | +0.00 | $0.68 | -38.01 | -38.01 | +0.00 | -38.01 |
| 2026-08-20 | `BTGO` | 156 | — | $6.61 | +0.00 | $6.60 | -0.78 | -0.78 | +0.00 | -0.78 |
| 2026-08-20 | `ASST` | 64 | — | $16.00 | +0.00 | $16.13 | +8.32 | +8.32 | +0.00 | +8.32 |
| 2026-08-20 | `ZLAB` | 38 | — | $26.57 | +0.00 | $26.02 | -20.90 | -20.90 | +0.00 | -20.90 |
| 2026-08-20 | `CRSP` | 17 | — | $58.73 | +0.00 | $58.12 | -10.37 | -10.37 | +0.00 | -10.37 |
| 2026-08-20 | `APA` | 23 | — | $44.76 | +0.00 | $44.39 | -8.51 | -8.51 | +0.00 | -8.51 |
| 2026-08-21 | `MRNA` | 5 | $133.32 | $133.11 | -1.05 | $145.13 | +60.10 | +59.05 | -85.15 | -25.05 |
| 2026-08-21 | `CYPH` | 673 | $1.19 | $1.32 | +87.49 | $1.42 | +67.30 | +154.79 | +114.41 | +181.71 |
| 2026-08-21 | `ABCL` | 65 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -15.92 | — |
| 2026-08-21 | `AZI` | 565 | $1.44 | $1.46 | +11.30 | — | +0.00 | +11.30 | +50.85 | — |
| 2026-08-21 | `BHP` | 11 | $93.63 | $95.72 | +22.99 | — | +0.00 | +22.99 | +51.81 | — |
| 2026-08-21 | `HUMA` | 1462 | $0.68 | $0.67 | -10.23 | — | +0.00 | -10.23 | -48.25 | — |
| 2026-08-21 | `BTGO` | 156 | $6.60 | $6.95 | +54.60 | — | +0.00 | +54.60 | +53.82 | — |
| 2026-08-21 | `ASST` | 64 | $16.13 | $17.66 | +97.92 | — | +0.00 | +97.92 | +106.24 | — |
| 2026-08-21 | `ZLAB` | 38 | $26.02 | $26.25 | +8.74 | — | +0.00 | +8.74 | -12.16 | — |
| 2026-08-21 | `CRSP` | 17 | $58.12 | $59.72 | +27.20 | $59.50 | -3.74 | +23.46 | +16.83 | +13.09 |
| 2026-08-21 | `APA` | 23 | $44.39 | $44.52 | +2.99 | — | +0.00 | +2.99 | -5.52 | — |
| 2026-08-21 | `XHG` | 263 | — | $4.49 | +0.00 | $4.41 | -21.04 | -21.04 | +0.00 | -21.04 |
| 2026-08-21 | `CAPR` | 174 | — | $6.81 | +0.00 | $6.29 | -90.48 | -90.48 | +0.00 | -90.48 |
| 2026-08-21 | `AU` | 6 | — | $119.43 | +0.00 | $121.22 | +10.74 | +10.74 | +0.00 | +10.74 |
| 2026-08-21 | `AUTL` | 319 | — | $2.47 | +0.00 | $2.41 | -19.14 | -19.14 | +0.00 | -19.14 |
| 2026-08-21 | `FUTU` | 6 | — | $115.18 | +0.00 | $123.64 | +50.76 | +50.76 | +0.00 | +50.76 |
| 2026-08-21 | `DE` | 1 | — | $623.26 | +0.00 | $647.47 | +24.21 | +24.21 | +0.00 | +24.21 |
| 2026-08-21 | `MARA` | 67 | — | $11.70 | +0.00 | $11.26 | -29.48 | -29.48 | +0.00 | -29.48 |
| 2026-08-21 | `BTDR` | 71 | — | $11.10 | +0.00 | $11.37 | +19.52 | +19.52 | +0.00 | +19.52 |
| 2026-08-21 | `HIVE` | 243 | — | $3.24 | +0.00 | $3.03 | -51.03 | -51.03 | +0.00 | -51.03 |
| 2026-08-24 | `MRNA` | 5 | $145.13 | $142.70 | -12.15 | — | +0.00 | -12.15 | -37.20 | — |
| 2026-08-24 | `CYPH` | 673 | $1.42 | $1.83 | +275.93 | — | +0.00 | +275.93 | +457.64 | — |
| 2026-08-24 | `CRSP` | 17 | $59.50 | $58.75 | -12.75 | $57.08 | -28.47 | -41.22 | +0.34 | -28.13 |
| 2026-08-24 | `XHG` | 263 | $4.41 | $4.32 | -23.67 | — | +0.00 | -23.67 | -44.71 | — |
| 2026-08-24 | `CAPR` | 174 | $6.29 | $8.03 | +302.76 | — | +0.00 | +302.76 | +212.28 | — |
| 2026-08-24 | `AU` | 6 | $121.22 | $120.51 | -4.26 | — | +0.00 | -4.26 | +6.48 | — |
| 2026-08-24 | `AUTL` | 319 | $2.41 | $2.40 | -3.19 | — | +0.00 | -3.19 | -22.33 | — |
| 2026-08-24 | `FUTU` | 6 | $123.64 | $121.00 | -15.84 | — | +0.00 | -15.84 | +34.92 | — |
| 2026-08-24 | `DE` | 1 | $647.47 | $653.04 | +5.57 | — | +0.00 | +5.57 | +29.78 | — |
| 2026-08-24 | `MARA` | 67 | $11.26 | $11.17 | -6.03 | — | +0.00 | -6.03 | -35.51 | — |
| 2026-08-24 | `BTDR` | 71 | $11.37 | $11.48 | +7.81 | — | +0.00 | +7.81 | +27.33 | — |
| 2026-08-24 | `HIVE` | 243 | $3.03 | $2.99 | -9.72 | — | +0.00 | -9.72 | -60.75 | — |
| 2026-08-25 | `CRSP` | 17 | $57.08 | $57.93 | +14.53 | — | +0.00 | +14.53 | -13.60 | — |
| 2026-08-25 | `REAX` | 33 | — | $24.11 | +0.00 | $28.43 | +142.56 | +142.56 | +0.00 | +142.56 |
| 2026-08-25 | `CYPH` | 524 | — | $1.56 | +0.00 | $1.64 | +41.92 | +41.92 | +0.00 | +41.92 |
| 2026-08-25 | `XHG` | 201 | — | $4.07 | +0.00 | $4.02 | -10.05 | -10.05 | +0.00 | -10.05 |
| 2026-08-25 | `ASST` | 43 | — | $19.04 | +0.00 | $21.39 | +101.05 | +101.05 | +0.00 | +101.05 |
| 2026-08-25 | `RUM` | 135 | — | $9.42 | +0.00 | $10.23 | +109.35 | +109.35 | +0.00 | +109.35 |
| 2026-08-25 | `EZPW` | 36 | — | $35.05 | +0.00 | $35.23 | +6.48 | +6.48 | +0.00 | +6.48 |
| 2026-08-25 | `ZYME` | 44 | — | $28.86 | +0.00 | $27.47 | -61.16 | -61.16 | +0.00 | -61.16 |
| 2026-08-25 | `EOLS` | 146 | — | $8.72 | +0.00 | $8.97 | +37.23 | +37.23 | +0.00 | +37.23 |
| 2026-08-25 | `AU` | 10 | — | $118.52 | +0.00 | $123.39 | +48.70 | +48.70 | +0.00 | +48.70 |
| 2026-08-25 | `FCX` | 16 | — | $77.13 | +0.00 | $79.91 | +44.48 | +44.48 | +0.00 | +44.48 |
| 2026-08-26 | `REAX` | 33 | $28.43 | $26.61 | -60.06 | — | +0.00 | -60.06 | +82.50 | — |
| 2026-08-26 | `CYPH` | 524 | $1.64 | $1.60 | -20.96 | — | +0.00 | -20.96 | +20.96 | — |
| 2026-08-26 | `XHG` | 201 | $4.02 | $3.81 | -42.21 | $4.06 | +50.25 | +8.04 | -52.26 | -2.01 |
| 2026-08-26 | `ASST` | 43 | $21.39 | $20.72 | -28.81 | — | +0.00 | -28.81 | +72.24 | — |
| 2026-08-26 | `RUM` | 135 | $10.23 | $10.07 | -21.60 | — | +0.00 | -21.60 | +87.75 | — |
| 2026-08-26 | `EZPW` | 36 | $35.23 | $35.70 | +16.92 | — | +0.00 | +16.92 | +23.40 | — |
| 2026-08-26 | `ZYME` | 44 | $27.47 | $27.56 | +3.96 | — | +0.00 | +3.96 | -57.20 | — |
| 2026-08-26 | `EOLS` | 146 | $8.97 | $8.86 | -16.79 | — | +0.00 | -16.79 | +20.44 | — |
| 2026-08-26 | `AU` | 10 | $123.39 | $119.80 | -35.90 | — | +0.00 | -35.90 | +12.80 | — |
| 2026-08-26 | `FCX` | 16 | $79.91 | $79.34 | -9.12 | — | +0.00 | -9.12 | +35.36 | — |
| 2026-08-26 | `BYND` | 73 | — | $14.11 | +0.00 | $14.25 | +10.22 | +10.22 | +0.00 | +10.22 |
| 2026-08-26 | `USDE` | 178 | — | $5.81 | +0.00 | $5.98 | +30.26 | +30.26 | +0.00 | +30.26 |
| 2026-08-26 | `SUJA` | 110 | — | $9.39 | +0.00 | $9.44 | +5.50 | +5.50 | +0.00 | +5.50 |
| 2026-08-26 | `FLNC` | 130 | — | $11.12 | +0.00 | $11.08 | -5.20 | -5.20 | +0.00 | -5.20 |
| 2026-08-26 | `CAPR` | 174 | — | $8.29 | +0.00 | $9.36 | +186.18 | +186.18 | +0.00 | +186.18 |
| 2026-08-26 | `FWRD` | 83 | — | $17.41 | +0.00 | $17.63 | +18.26 | +18.26 | +0.00 | +18.26 |
| 2026-08-26 | `TRLV` | 129 | — | $11.22 | +0.00 | $11.43 | +27.09 | +27.09 | +0.00 | +27.09 |
| 2026-08-26 | `FNV` | 5 | — | $267.02 | +0.00 | $267.37 | +1.75 | +1.75 | +0.00 | +1.75 |
| 2026-08-27 | `XHG` | 201 | $4.06 | $4.06 | +0.00 | — | +0.00 | +0.00 | -2.01 | — |
| 2026-08-27 | `BYND` | 73 | $14.25 | $14.20 | -3.65 | — | +0.00 | -3.65 | +6.57 | — |
| 2026-08-27 | `USDE` | 178 | $5.98 | $6.50 | +92.56 | — | +0.00 | +92.56 | +122.82 | — |
| 2026-08-27 | `SUJA` | 110 | $9.44 | $9.41 | -3.30 | — | +0.00 | -3.30 | +2.20 | — |
| 2026-08-27 | `FLNC` | 130 | $11.08 | $11.52 | +57.20 | — | +0.00 | +57.20 | +52.00 | — |
| 2026-08-27 | `CAPR` | 174 | $9.36 | $9.19 | -29.58 | — | +0.00 | -29.58 | +156.60 | — |
| 2026-08-27 | `FWRD` | 83 | $17.63 | $17.60 | -2.49 | — | +0.00 | -2.49 | +15.77 | — |
| 2026-08-27 | `TRLV` | 129 | $11.43 | $11.38 | -6.45 | — | +0.00 | -6.45 | +20.64 | — |
| 2026-08-27 | `FNV` | 5 | $267.37 | $267.23 | -0.70 | — | +0.00 | -0.70 | +1.05 | — |
| 2026-08-27 | `SLI` | 331 | — | $2.60 | +0.00 | $2.64 | +13.24 | +13.24 | +0.00 | +13.24 |
| 2026-08-27 | `RRC` | 20 | — | $41.44 | +0.00 | $41.64 | +4.00 | +4.00 | +0.00 | +4.00 |
| 2026-08-27 | `PGY` | 37 | — | $22.93 | +0.00 | $23.26 | +12.21 | +12.21 | +0.00 | +12.21 |
| 2026-08-27 | `CRK` | 59 | — | $14.42 | +0.00 | $14.62 | +11.80 | +11.80 | +0.00 | +11.80 |
| 2026-08-27 | `ACMR` | 19 | — | $81.65 | +0.00 | $80.49 | -22.04 | -22.04 | +0.00 | -22.04 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `LRCX` | 5 | — | $318.88 | +0.00 | $318.58 | -1.50 | -1.50 | +0.00 | -1.50 |
| 2026-08-27 | `NVDA` | 7 | — | $222.86 | +0.00 | $227.98 | +35.84 | +35.84 | +0.00 | +35.84 |
| 2026-08-28 | `SLI` | 331 | $2.64 | $2.68 | +13.24 | — | +0.00 | +13.24 | +26.48 | — |
| 2026-08-28 | `RRC` | 28 | $41.64 | $41.74 | +2.00 | $41.46 | -7.84 | -5.84 | +6.00 | -7.84 |
| 2026-08-28 | `PGY` | 37 | $23.26 | $23.21 | -1.85 | — | +0.00 | -1.85 | +10.36 | — |
| 2026-08-28 | `CRK` | 59 | $14.62 | $14.63 | +0.59 | — | +0.00 | +0.59 | +12.39 | — |
| 2026-08-28 | `ACMR` | 19 | $80.49 | $79.27 | -23.18 | — | +0.00 | -23.18 | -45.22 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `LRCX` | 5 | $318.58 | $318.03 | -2.75 | — | +0.00 | -2.75 | -4.25 | — |
| 2026-08-28 | `NVDA` | 7 | $227.98 | $227.36 | -4.34 | — | +0.00 | -4.34 | +31.50 | — |
| 2026-08-28 | `BYND` | 61 | — | $14.00 | +0.00 | $13.86 | -8.54 | -8.54 | +0.00 | -8.54 |
| 2026-08-28 | `CAPR` | 88 | — | $9.73 | +0.00 | $9.59 | -12.32 | -12.32 | +0.00 | -12.32 |
| 2026-08-28 | `MRNA` | 6 | — | $137.19 | +0.00 | $137.99 | +4.80 | +4.80 | +0.00 | +4.80 |
| 2026-08-28 | `ANF` | 5 | — | $146.07 | +0.00 | $148.42 | +11.75 | +11.75 | +0.00 | +11.75 |
| 2026-08-28 | `SEDG` | 35 | — | $32.90 | +0.00 | $31.41 | -52.15 | -52.15 | +0.00 | -52.15 |
| 2026-08-28 | `OPTX` | 135 | — | $8.61 | +0.00 | $8.52 | -12.15 | -12.15 | +0.00 | -12.15 |
| 2026-08-28 | `SMTC` | 8 | — | $141.76 | +0.00 | $131.17 | -84.72 | -84.72 | +0.00 | -84.72 |
| 2026-08-28 | `ERAS` | 60 | — | $19.25 | +0.00 | $18.03 | -73.20 | -73.20 | +0.00 | -73.20 |
| 2026-08-28 | `BBWI` | 62 | — | $18.75 | +0.00 | $19.22 | +29.14 | +29.14 | +0.00 | +29.14 |
| 2026-08-28 | `ZYME` | 40 | — | $28.91 | +0.00 | $28.27 | -25.60 | -25.60 | +0.00 | -25.60 |
| 2026-08-31 | `BYND` | 61 | $13.86 | $13.81 | -3.05 | $13.30 | -31.11 | -34.16 | -11.59 | -42.70 |
| 2026-08-31 | `CAPR` | 88 | $9.59 | $9.50 | -7.92 | — | +0.00 | -7.92 | -20.24 | — |
| 2026-08-31 | `MRNA` | 6 | $137.99 | $134.10 | -23.34 | — | +0.00 | -23.34 | -18.54 | — |
| 2026-08-31 | `ANF` | 5 | $148.42 | $148.03 | -1.95 | — | +0.00 | -1.95 | +9.80 | — |
| 2026-08-31 | `RRC` | 28 | $41.46 | $42.00 | +15.12 | — | +0.00 | +15.12 | +7.28 | — |
| 2026-08-31 | `SEDG` | 35 | $31.41 | $31.15 | -9.10 | — | +0.00 | -9.10 | -61.25 | — |
| 2026-08-31 | `OPTX` | 135 | $8.52 | $8.52 | +0.00 | — | +0.00 | +0.00 | -12.15 | — |
| 2026-08-31 | `SMTC` | 8 | $131.17 | $132.30 | +9.04 | — | +0.00 | +9.04 | -75.68 | — |
| 2026-08-31 | `ERAS` | 60 | $18.03 | $17.87 | -9.60 | — | +0.00 | -9.60 | -82.80 | — |
| 2026-08-31 | `BBWI` | 62 | $19.22 | $19.25 | +1.86 | — | +0.00 | +1.86 | +31.00 | — |
| 2026-08-31 | `ZYME` | 40 | $28.27 | $28.06 | -8.40 | — | +0.00 | -8.40 | -34.00 | — |
| 2026-09-01 | `BYND` | 61 | $13.30 | $13.04 | -15.86 | — | +0.00 | -15.86 | -58.56 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `GPRO` | 467 | — | $1.78 | +0.00 | $1.39 | -182.13 | -182.13 | +0.00 | -182.13 |
| 2026-09-03 | `REAX` | 45 | — | $18.40 | +0.00 | $18.40 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `CNH` | 60 | — | $13.71 | +0.00 | $13.84 | +7.80 | +7.80 | +0.00 | +7.80 |
| 2026-09-03 | `MMED` | 34 | — | $23.88 | +0.00 | $23.84 | -1.36 | -1.36 | +0.00 | -1.36 |
| 2026-09-03 | `CNXC` | 33 | — | $32.88 | +0.00 | $32.85 | -0.99 | -0.99 | +0.00 | -0.99 |
| 2026-09-03 | `OPTX` | 146 | — | $7.59 | +0.00 | $7.76 | +24.82 | +24.82 | +0.00 | +24.82 |
| 2026-09-03 | `DE` | 1 | — | $703.25 | +0.00 | $694.41 | -8.84 | -8.84 | +0.00 | -8.84 |
| 2026-09-03 | `FRNM` | 70 | — | $15.87 | +0.00 | $16.90 | +72.10 | +72.10 | +0.00 | +72.10 |
| 2026-09-03 | `AVGO` | 3 | — | $351.74 | +0.00 | $357.16 | +16.26 | +16.26 | +0.00 | +16.26 |
| 2026-09-03 | `CIEN` | 3 | — | $354.49 | +0.00 | $317.46 | -111.09 | -111.09 | +0.00 | -111.09 |
| 2026-09-03 | `HPE` | 23 | — | $47.60 | +0.00 | $54.44 | +157.32 | +157.32 | +0.00 | +157.32 |
| 2026-09-04 | `GPRO` | 467 | $1.39 | $1.48 | +42.03 | $1.70 | +102.74 | +144.77 | -140.10 | -37.36 |
| 2026-09-04 | `REAX` | 45 | $18.40 | $18.15 | -11.25 | — | +0.00 | -11.25 | -11.25 | — |
| 2026-09-04 | `CNH` | 60 | $13.84 | $13.89 | +3.00 | — | +0.00 | +3.00 | +10.80 | — |
| 2026-09-04 | `MMED` | 34 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -1.36 | — |
| 2026-09-04 | `CNXC` | 33 | $32.85 | $32.48 | -12.21 | — | +0.00 | -12.21 | -13.20 | — |
| 2026-09-04 | `OPTX` | 146 | $7.76 | $7.79 | +4.38 | — | +0.00 | +4.38 | +29.20 | — |
| 2026-09-04 | `DE` | 1 | $694.41 | $692.03 | -2.38 | — | +0.00 | -2.38 | -11.22 | — |
| 2026-09-04 | `FRNM` | 70 | $16.90 | $16.40 | -35.00 | $16.31 | -6.30 | -41.30 | +37.10 | +30.80 |
| 2026-09-04 | `AVGO` | 3 | $357.16 | $359.70 | +7.62 | — | +0.00 | +7.62 | +23.88 | — |
| 2026-09-04 | `CIEN` | 3 | $317.46 | $321.67 | +12.63 | — | +0.00 | +12.63 | -98.46 | — |
| 2026-09-04 | `HPE` | 23 | $54.44 | $53.85 | -13.57 | — | +0.00 | -13.57 | +143.75 | — |
| 2026-09-04 | `ASST` | 36 | — | $25.18 | +0.00 | $27.14 | +70.56 | +70.56 | +0.00 | +70.56 |
| 2026-09-04 | `USDE` | 116 | — | $7.87 | +0.00 | $7.93 | +6.96 | +6.96 | +0.00 | +6.96 |
| 2026-09-04 | `DFDV` | 158 | — | $5.79 | +0.00 | $5.87 | +12.64 | +12.64 | +0.00 | +12.64 |
| 2026-09-04 | `CRM` | 4 | — | $263.36 | +0.00 | $259.23 | -16.52 | -16.52 | +0.00 | -16.52 |
| 2026-09-04 | `BAK` | 663 | — | $1.94 | +0.00 | $1.89 | -33.15 | -33.15 | +0.00 | -33.15 |
| 2026-09-04 | `MSTR` | 9 | — | $137.35 | +0.00 | $142.80 | +49.05 | +49.05 | +0.00 | +49.05 |
| 2026-09-04 | `BE` | 5 | — | $236.82 | +0.00 | $252.87 | +80.25 | +80.25 | +0.00 | +80.25 |
| 2026-09-04 | `MRX` | 17 | — | $75.65 | +0.00 | $78.27 | +44.54 | +44.54 | +0.00 | +44.54 |
| 2026-09-08 | `GPRO` | 467 | $1.70 | $1.56 | -63.05 | — | +0.00 | -63.05 | -100.41 | — |
| 2026-09-08 | `FRNM` | 70 | $16.31 | $16.74 | +30.10 | — | +0.00 | +30.10 | +60.90 | — |
| 2026-09-08 | `ASST` | 36 | $27.14 | $26.44 | -25.20 | — | +0.00 | -25.20 | +45.36 | — |
| 2026-09-08 | `USDE` | 116 | $7.93 | $7.76 | -19.72 | — | +0.00 | -19.72 | -12.76 | — |
| 2026-09-08 | `DFDV` | 158 | $5.87 | $5.81 | -9.48 | — | +0.00 | -9.48 | +3.16 | — |
| 2026-09-08 | `CRM` | 4 | $259.23 | $253.72 | -22.04 | — | +0.00 | -22.04 | -38.56 | — |
| 2026-09-08 | `BAK` | 663 | $1.89 | $1.94 | +33.15 | — | +0.00 | +33.15 | +0.00 | — |
| 2026-09-08 | `MSTR` | 9 | $142.80 | $137.62 | -46.62 | $136.52 | -9.90 | -56.52 | +2.43 | -7.47 |
| 2026-09-08 | `BE` | 5 | $252.87 | $267.76 | +74.45 | — | +0.00 | +74.45 | +154.70 | — |
| 2026-09-08 | `MRX` | 17 | $78.27 | $78.84 | +9.69 | $76.71 | -36.21 | -26.52 | +54.23 | +18.02 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +386.21 | IREN, TNDM, TPG, INO | — | $0.54 | $10,345.37 | IREN×54, TNDM×107, TPG×49, INO×3085 |
| 2026-08-14 | +5.50 | $0.54 | IREN×54, TNDM×107, TPG×49, INO×3085 | $10,412.10 | +66.73 | +18.92 | QMCO, ARX, ZENA, AIRO, TLN, VST, NRG, ANGX, MH, HLIT | IREN, TNDM, TPG, INO | $1,465.71 | $10,361.33 | QMCO×31, ARX×39, ZENA×353, AIRO×69, TLN×2, VST×7, NRG×8, ANGX×241, MH×76, HLIT×78 |
| 2026-08-17 | +2.25 | $1,465.71 | QMCO×31, ARX×39, ZENA×353, AIRO×69, TLN×2, VST×7, NRG×8, ANGX×241, MH×76, HLIT×78 | $10,383.83 | +22.50 | +76.58 | XHG, CAPR, STDN, HTFL, DVN, EOG, FANG, CELC, OUST | QMCO, ARX, ZENA, AIRO, TLN, VST, NRG, ANGX, MH, HLIT | $188.26 | $10,416.31 | XHG×185, CAPR×113, STDN×56, HTFL×18, DVN×31, EOG×10, FANG×7, CELC×15, OUST×29 |
| 2026-08-18 | -6.20 | $188.26 | XHG×185, CAPR×113, STDN×56, HTFL×18, DVN×31, EOG×10, FANG×7, CELC×15, OUST×29 | $10,381.24 | -35.07 | -47.46 | — | XHG, STDN, HTFL, DVN, EOG, FANG, CELC, OUST | $9,516.58 | $10,316.62 | CAPR×113 |
| 2026-08-19 | -7.20 | $9,516.58 | CAPR×113 | $10,329.05 | +12.43 | +0.00 | — | CAPR | $10,326.69 | $10,326.69 | — |
| 2026-08-20 | +1.12 | $10,326.69 | — | $10,326.69 | +0.00 | -74.98 | MRNA, CYPH, ABCL, AZI, BHP, HUMA, BTGO, ASST, ZLAB, CRSP, APA | — | $85.60 | $10,203.96 | MRNA×5, CYPH×673, ABCL×65, AZI×565, BHP×11, HUMA×1462, BTGO×156, ASST×64, ZLAB×38, CRSP×17, APA×23 |
| 2026-08-21 | +3.25 | $85.60 | MRNA×5, CYPH×673, ABCL×65, AZI×565, BHP×11, HUMA×1462, BTGO×156, ASST×64, ZLAB×38, CRSP×17, APA×23 | $10,505.90 | +301.94 | +17.72 | XHG, CAPR, AU, AUTL, FUTU, DE, MARA, BTDR, HIVE | ABCL, AZI, BHP, HUMA, BTGO, ASST, ZLAB, APA | $334.54 | $10,465.04 | MRNA×5, CYPH×673, CRSP×17, XHG×263, CAPR×174, AU×6, AUTL×319, FUTU×6, DE×1, MARA×67, BTDR×71, HIVE×243 |
| 2026-08-24 | -5.17 | $334.54 | MRNA×5, CYPH×673, CRSP×17, XHG×263, CAPR×174, AU×6, AUTL×319, FUTU×6, DE×1, MARA×67, BTDR×71, HIVE×243 | $10,969.50 | +504.46 | -28.47 | — | MRNA, CYPH, XHG, CAPR, AU, AUTL, FUTU, DE, MARA, BTDR, HIVE | $9,936.05 | $10,906.33 | CRSP×17 |
| 2026-08-25 | +1.80 | $9,936.05 | CRSP×17 | $10,920.86 | +14.53 | +460.56 | REAX, CYPH, XHG, ASST, RUM, EZPW, ZYME, EOLS, AU, FCX | CRSP | $146.54 | $11,352.69 | REAX×33, CYPH×524, XHG×201, ASST×43, RUM×135, EZPW×36, ZYME×44, EOLS×146, AU×10, FCX×16 |
| 2026-08-26 | +2.02 | $146.54 | REAX×33, CYPH×524, XHG×201, ASST×43, RUM×135, EZPW×36, ZYME×44, EOLS×146, AU×10, FCX×16 | $11,138.13 | -214.56 | +324.31 | BYND, USDE, SUJA, FLNC, CAPR, FWRD, TRLV, FNV | REAX, CYPH, ASST, RUM, EZPW, ZYME, EOLS, AU, FCX | $116.71 | $11,419.51 | XHG×201, BYND×73, USDE×178, SUJA×110, FLNC×130, CAPR×174, FWRD×83, TRLV×129, FNV×5 |
| 2026-08-27 | — | $116.71 | XHG×201, BYND×73, USDE×178, SUJA×110, FLNC×130, CAPR×174, FWRD×83, TRLV×129, FNV×5 | $11,523.10 | +103.59 | +21.93 | SLI, RRC, PGY, CRK, ACMR, MU, LRCX, NVDA | XHG, BYND, USDE, SUJA, FLNC, CAPR, FWRD, TRLV, FNV | $2,421.64 | $11,504.94 | SLI×331, RRC×20, PGY×37, CRK×59, ACMR×19, MU×1, LRCX×5, NVDA×7 |
| 2026-08-28 | +0.75 | $2,421.64 | SLI×331, RRC×20, PGY×37, CRK×59, ACMR×19, MU×1, LRCX×5, NVDA×7 | $11,472.55 | -32.39 | -230.83 | BYND, CAPR, MRNA, ANF, RRC, SEDG, OPTX, SMTC, ERAS, BBWI, ZYME | SLI, RRC, PGY, CRK, ACMR, MU, LRCX, NVDA | $75.94 | $11,199.39 | BYND×61, CAPR×88, MRNA×6, ANF×5, RRC×28, SEDG×35, OPTX×135, SMTC×8, ERAS×60, BBWI×62, ZYME×40 |
| 2026-08-31 | -5.85 | $75.94 | BYND×61, CAPR×88, MRNA×6, ANF×5, RRC×28, SEDG×35, OPTX×135, SMTC×8, ERAS×60, BBWI×62, ZYME×40 | $11,162.05 | -37.34 | -31.11 | — | CAPR, MRNA, ANF, RRC, SEDG, OPTX, SMTC, ERAS, BBWI, ZYME | $10,298.12 | $11,109.42 | BYND×61 |
| 2026-09-01 | -6.30 | $10,298.12 | BYND×61 | $11,093.56 | -15.86 | +0.00 | — | BYND | $11,091.37 | $11,091.37 | — |
| 2026-09-02 | -3.83 | $11,091.37 | — | $11,091.37 | -0.00 | +0.00 | — | — | $11,091.37 | $11,091.37 | — |
| 2026-09-03 | -0.90 | $11,091.37 | — | $11,091.37 | -0.00 | -26.11 | GPRO, REAX, CNH, MMED, CNXC, OPTX, DE, FRNM, AVGO, CIEN, HPE | — | $549.59 | $11,038.08 | GPRO×467, REAX×45, CNH×60, MMED×34, CNXC×33, OPTX×146, DE×1, FRNM×70, AVGO×3, CIEN×3, HPE×23 |
| 2026-09-04 | +2.25 | $549.59 | GPRO×467, REAX×45, CNH×60, MMED×34, CNXC×33, OPTX×146, DE×1, FRNM×70, AVGO×3, CIEN×3, HPE×23 | $11,033.33 | -4.75 | +310.77 | ASST, USDE, DFDV, CRM, BAK, MSTR, BE, MRX | REAX, CNH, MMED, CNXC, OPTX, DE, AVGO, CIEN, HPE | $371.32 | $11,301.43 | GPRO×467, FRNM×70, ASST×36, USDE×116, DFDV×158, CRM×4, BAK×663, MSTR×9, BE×5, MRX×17 |
| 2026-09-08 | -11.47 | $371.32 | GPRO×467, FRNM×70, ASST×36, USDE×116, DFDV×158, CRM×4, BAK×663, MSTR×9, BE×5, MRX×17 | $11,262.72 | -38.71 | -46.11 | — | GPRO, FRNM, ASST, USDE, DFDV, CRM, BAK, BE | $8,655.82 | $11,188.57 | MSTR×9, MRX×17 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 (unchanged overnight, no fees) · equity $10,000.00 vs prior close $10,000.00 (+0.00) | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $7,514.93 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; combo leftover $2500.00; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 107 | $23.33 | $2.31 | — | $5,016.31 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; combo leftover $2500.00; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 49 | $50.62 | $2.14 | — | $2,533.63 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; combo leftover $2500.00; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3085 | $0.81 | $34.24 | — | $0.54 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+13.2; combo leftover $2500.00; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.54 | ▲ close $10,345.37 vs 09:30 $10,000.00 (session +386.21) | 16:00 close · cash $0.54 · equity $10,345.37 vs 09:30 $10,000.00 (+345.37; session marks +386.21) · 4 name(s) marked open→close (per-name table). IREN×54 09:30 $45.98 → close $44.76 -65.88; TNDM×107 09:30 $23.33 → close $23.13 -21.40; TPG×49 09:30 $50.62 → close $54.62 +195.84; INO×3085 09:30 $0.81 → close $0.90 +277.65 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.54 | ▲ 09:30 equity $10,412.10 vs yday $10,345.37 (+66.73) | 09:30 open · cash $0.54 (unchanged overnight, no fees) · equity $10,412.10 vs prior close $10,345.37 (+66.73) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 54 | $44.09 | $2.18 | $-106.39 | $2,379.22 | ▼ -106.39 after sell → book $10,409.92; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 107 | $22.92 | $2.35 | $-48.53 | $4,829.31 | ▼ -48.53 after sell → book $10,407.57; vs 09:30 mark -2.35 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 49 | $55.29 | $2.17 | $+224.37 | $7,536.35 | ▲ +224.37 after sell → book $10,405.40; vs 09:30 mark -2.17 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 3085 | $0.93 | $38.48 | $+297.48 | $10,366.92 | ▲ +297.48 after sell → book $10,366.92; vs 09:30 mark -38.48 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 31 | $24.68 | $2.08 | — | $9,599.76 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; combo leftover $777.52; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 39 | $19.57 | $2.11 | — | $8,834.42 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $777.52; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 353 | $2.20 | $4.55 | — | $8,053.27 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; combo leftover $777.52; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 69 | $11.12 | $2.20 | — | $7,283.79 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $777.52; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 2 | $359.83 | $2.00 | — | $6,562.13 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; combo leftover $1040.54; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 7 | $146.90 | $2.01 | — | $5,531.82 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; combo leftover $1040.54; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 8 | $120.00 | $2.01 | — | $4,569.81 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; combo leftover $1040.54; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 241 | $4.31 | $3.11 | — | $3,527.99 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; combo leftover $1040.54; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 76 | $13.55 | $2.22 | — | $2,495.97 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $1040.54; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 78 | $13.18 | $2.22 | — | $1,465.71 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; combo leftover $1040.54; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,465.71 | ▲ close $10,361.33 vs 09:30 $10,412.10 (session +18.92) | 16:00 close · cash $1,465.71 · equity $10,361.33 vs 09:30 $10,412.10 (-50.77; session marks +18.92) · 10 name(s) marked open→close (per-name table). QMCO×31 09:30 $24.68 → close $26.11 +44.33; ARX×39 09:30 $19.57 → close $19.58 +0.39; ZENA×353 09:30 $2.20 → close $2.14 -21.18; AIRO×69 09:30 $11.12 → close $9.57 -106.95; TLN×2 09:30 $359.83 → close $362.74 +5.82; VST×7 09:30 $146.90 → close $148.13 +8.61; NRG×8 09:30 $120.00 → close $126.24 +49.92; ANGX×241 09:30 $4.31 → close $4.37 +14.46; MH×76 09:30 $13.55 → close $13.10 -34.20; HLIT×78 09:30 $13.18 → close $13.92 +57.72 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,465.71 | ▲ 09:30 equity $10,383.83 vs yday $10,361.33 (+22.50) | 09:30 open · cash $1,465.71 (unchanged overnight, no fees) · equity $10,383.83 vs prior close $10,361.33 (+22.50) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 31 | $24.83 | $2.10 | $+0.46 | $2,233.34 | ▲ +0.46 after sell → book $10,381.73; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 39 | $19.57 | $2.13 | $-4.23 | $2,994.44 | ▼ -4.23 after sell → book $10,379.60; vs 09:30 mark -2.13 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 353 | $2.08 | $4.62 | $-49.77 | $3,725.82 | ▼ -49.77 after sell → book $10,374.98; vs 09:30 mark -4.62 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 69 | $9.57 | $2.22 | $-111.37 | $4,383.93 | ▼ -111.37 after sell → book $10,372.76; vs 09:30 mark -2.22 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 2 | $367.88 | $2.02 | $+12.09 | $5,117.68 | ▲ +12.09 after sell → book $10,370.75; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 7 | $149.37 | $2.03 | $+13.25 | $6,161.24 | ▲ +13.25 after sell → book $10,368.72; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 8 | $127.40 | $2.03 | $+55.15 | $7,178.40 | ▲ +55.15 after sell → book $10,366.68; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 241 | $4.60 | $3.16 | $+63.62 | $8,283.84 | ▲ +63.62 after sell → book $10,363.52; vs 09:30 mark -3.16 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 76 | $13.16 | $2.24 | $-34.10 | $9,281.76 | ▼ -34.10 after sell → book $10,361.28; vs 09:30 mark -2.24 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 78 | $13.84 | $2.25 | $+47.01 | $10,359.04 | ▲ +47.01 after sell → book $10,359.04; vs 09:30 mark -2.24 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 185 | $4.19 | $2.54 | — | $9,581.34 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $776.93; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 113 | $6.87 | $2.33 | — | $8,802.70 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $776.93; owner union_hot_n4_h1 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 56 | $13.64 | $2.16 | — | $8,036.70 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $776.93; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 18 | $41.23 | $2.04 | — | $7,292.52 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $776.93; owner union_hot_n4_h1 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 31 | $46.18 | $2.08 | — | $5,858.86 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; combo leftover $1458.50; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 10 | $142.77 | $2.02 | — | $4,429.14 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; combo leftover $1458.50; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 7 | $202.70 | $2.01 | — | $3,008.23 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; combo leftover $1458.50; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 15 | $92.99 | $2.04 | — | $1,611.34 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; combo leftover $1458.50; owner union_news_g_h1 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 29 | $49.00 | $2.08 | — | $188.26 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; combo leftover $1458.50; owner union_news_g_h1 | join🟡 sector🟢 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $188.26 | ▲ close $10,416.31 vs 09:30 $10,383.83 (session +76.58) | 16:00 close · cash $188.26 · equity $10,416.31 vs 09:30 $10,383.83 (+32.48; session marks +76.58) · 9 name(s) marked open→close (per-name table). XHG×185 09:30 $4.19 → close $3.91 -51.80; CAPR×113 09:30 $6.87 → close $7.45 +65.54; STDN×56 09:30 $13.64 → close $13.31 -18.48; HTFL×18 09:30 $41.23 → close $41.94 +12.78; DVN×31 09:30 $46.18 → close $47.57 +43.09; EOG×10 09:30 $142.77 → close $146.15 +33.80; FANG×7 09:30 $202.70 → close $206.29 +25.13; CELC×15 09:30 $92.99 → close $92.44 -8.25; OUST×29 09:30 $49.00 → close $48.13 -25.23 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $188.26 | ▼ 09:30 equity $10,381.24 vs yday $10,416.31 (-35.07) | 09:30 open · cash $188.26 (unchanged overnight, no fees) · equity $10,381.24 vs prior close $10,416.31 (-35.07) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 185 | $3.94 | $2.59 | $-51.38 | $914.58 | ▼ -51.38 after sell → book $10,378.66; vs 09:30 mark -2.58 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 56 | $13.31 | $2.18 | $-22.82 | $1,657.76 | ▼ -22.82 after sell → book $10,376.48; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 18 | $41.50 | $2.06 | $+0.75 | $2,402.70 | ▲ +0.75 after sell → book $10,374.42; vs 09:30 mark -2.06 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 31 | $48.00 | $2.10 | $+52.23 | $3,888.59 | ▲ +52.23 after sell → book $10,372.31; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 10 | $148.04 | $2.04 | $+48.64 | $5,366.95 | ▲ +48.64 after sell → book $10,370.27; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 7 | $208.93 | $2.03 | $+39.57 | $6,827.43 | ▲ +39.57 after sell → book $10,368.24; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 15 | $92.38 | $2.06 | $-13.24 | $8,211.07 | ▼ -13.24 after sell → book $10,366.18; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 29 | $45.09 | $2.10 | $-117.56 | $9,516.58 | ▼ -117.56 after sell → book $10,364.08; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,516.58 | ▼ close $10,316.62 vs 09:30 $10,381.24 (session -47.46) | 16:00 close · cash $9,516.58 · equity $10,316.62 vs 09:30 $10,381.24 (-64.62; session marks -47.46) · 1 name(s) marked open→close (per-name table). CAPR×113 09:30 $7.50 → close $7.08 -47.46 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,516.58 | ▲ 09:30 equity $10,329.05 vs yday $10,316.62 (+12.43) | 09:30 open · cash $9,516.58 (unchanged overnight, no fees) · equity $10,329.05 vs prior close $10,316.62 (+12.43) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 113 | $7.19 | $2.36 | $+31.47 | $10,326.69 | ▲ +31.47 after sell → book $10,326.69; vs 09:30 mark -2.36 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,326.69 | ▲ close $10,326.69 vs 09:30 $10,329.05 (session +0.00) | 16:00 close · cash $10,326.69 · no lots left · equity $10,326.69. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,326.69 | ▲ 09:30 equity $10,326.69 vs yday $10,326.69 (+0.00) | 09:30 open · cash $10,326.69 (unchanged overnight, no fees) · equity $10,326.69 vs prior close $10,326.69 (+0.00) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 5 | $150.14 | $2.00 | — | $9,573.99 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $774.50; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 673 | $1.15 | $8.68 | — | $8,791.36 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $774.50; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 65 | $11.81 | $2.19 | — | $8,021.20 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $774.50; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 565 | $1.37 | $7.29 | — | $7,239.86 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $774.50; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 11 | $91.01 | $2.02 | — | $6,236.73 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; combo leftover $1034.27; owner union_news_g_h1 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1462 | $0.71 | $14.72 | — | $5,188.37 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; combo leftover $1034.27; owner union_news_g_h1 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 156 | $6.61 | $2.46 | — | $4,155.53 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; combo leftover $1034.27; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 64 | $16.00 | $2.18 | — | $3,129.35 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; combo leftover $1034.27; owner union_news_g_h1 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 38 | $26.57 | $2.10 | — | $2,117.59 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; combo leftover $1034.27; owner union_news_g_h1 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 17 | $58.73 | $2.04 | — | $1,117.14 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; combo leftover $1034.27; owner union_news_g_h1 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 23 | $44.76 | $2.06 | — | $85.60 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; combo leftover $1034.27; owner union_news_g_h1 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.60 | ▼ close $10,203.96 vs 09:30 $10,326.69 (session -74.98) | 16:00 close · cash $85.60 · equity $10,203.96 vs 09:30 $10,326.69 (-122.73; session marks -74.98) · 11 name(s) marked open→close (per-name table). MRNA×5 09:30 $150.14 → close $133.32 -84.10; CYPH×673 09:30 $1.15 → close $1.19 +26.92; ABCL×65 09:30 $11.81 → close $11.57 -15.92; AZI×565 09:30 $1.37 → close $1.44 +39.55; BHP×11 09:30 $91.01 → close $93.63 +28.82; HUMA×1462 09:30 $0.71 → close $0.68 -38.01; BTGO×156 09:30 $6.61 → close $6.60 -0.78; ASST×64 09:30 $16.00 → close $16.13 +8.32; ZLAB×38 09:30 $26.57 → close $26.02 -20.90; CRSP×17 09:30 $58.73 → close $58.12 -10.37; APA×23 09:30 $44.76 → close $44.39 -8.51 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.60 | ▲ 09:30 equity $10,505.90 vs yday $10,203.96 (+301.94) | 09:30 open · cash $85.60 (unchanged overnight, no fees) · equity $10,505.90 vs prior close $10,203.96 (+301.94) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 65 | $11.57 | $2.21 | $-20.32 | $835.44 | ▼ -20.32 after sell → book $10,503.70; vs 09:30 mark -2.20 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 565 | $1.46 | $7.39 | $+36.17 | $1,652.95 | ▲ +36.17 after sell → book $10,496.31; vs 09:30 mark -7.39 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 11 | $95.72 | $2.04 | $+47.74 | $2,703.83 | ▲ +47.74 after sell → book $10,494.26; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1462 | $0.67 | $14.49 | $-77.46 | $3,674.72 | ▼ -77.46 after sell → book $10,479.77; vs 09:30 mark -14.49 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 156 | $6.95 | $2.49 | $+48.87 | $4,756.43 | ▲ +48.87 after sell → book $10,477.28; vs 09:30 mark -2.49 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 64 | $17.66 | $2.20 | $+101.86 | $5,884.46 | ▲ +101.86 after sell → book $10,475.07; vs 09:30 mark -2.21 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 38 | $26.25 | $2.12 | $-16.39 | $6,879.84 | ▼ -16.39 after sell → book $10,472.95; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 23 | $44.52 | $2.08 | $-9.66 | $7,901.72 | ▼ -9.66 after sell → book $10,470.87; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 263 | $4.49 | $3.39 | — | $6,717.46 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $1185.26; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 174 | $6.81 | $2.51 | — | $5,530.01 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $1185.26; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 6 | $119.43 | $2.01 | — | $4,811.42 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $790.00; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 319 | $2.47 | $4.12 | — | $4,019.37 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; combo leftover $790.00; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 6 | $115.18 | $2.01 | — | $3,326.29 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $790.00; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $2,701.03 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.4; combo leftover $790.00; owner union_news_g_h1 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 67 | $11.70 | $2.19 | — | $1,914.94 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; combo leftover $790.00; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 71 | $11.10 | $2.20 | — | $1,124.99 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; combo leftover $790.00; owner union_news_g_h1 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 243 | $3.24 | $3.13 | — | $334.54 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; combo leftover $790.00; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $334.54 | ▲ close $10,465.04 vs 09:30 $10,505.90 (session +17.72) | 16:00 close · cash $334.54 · equity $10,465.04 vs 09:30 $10,505.90 (-40.86; session marks +17.72) · 12 name(s) marked open→close (per-name table). MRNA×5 09:30 $133.11 → close $145.13 +60.10; CYPH×673 09:30 $1.32 → close $1.42 +67.30; CRSP×17 09:30 $59.72 → close $59.50 -3.74; XHG×263 09:30 $4.49 → close $4.41 -21.04; CAPR×174 09:30 $6.81 → close $6.29 -90.48; AU×6 09:30 $119.43 → close $121.22 +10.74; AUTL×319 09:30 $2.47 → close $2.41 -19.14; FUTU×6 09:30 $115.18 → close $123.64 +50.76; DE×1 09:30 $623.26 → close $647.47 +24.21; MARA×67 09:30 $11.70 → close $11.26 -29.48; BTDR×71 09:30 $11.10 → close $11.37 +19.52; HIVE×243 09:30 $3.24 → close $3.03 -51.03 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $334.54 | ▲ 09:30 equity $10,969.50 vs yday $10,465.04 (+504.46) | 09:30 open · cash $334.54 (unchanged overnight, no fees) · equity $10,969.50 vs prior close $10,465.04 (+504.46) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 5 | $142.70 | $2.02 | $-41.23 | $1,046.01 | ▼ -41.23 after sell → book $10,967.47; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 673 | $1.83 | $8.80 | $+440.15 | $2,268.80 | ▲ +440.15 after sell → book $10,958.67; vs 09:30 mark -8.80 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 263 | $4.32 | $3.45 | $-51.55 | $3,401.51 | ▼ -51.55 after sell → book $10,955.22; vs 09:30 mark -3.45 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 174 | $8.03 | $2.55 | $+207.22 | $4,796.18 | ▲ +207.22 after sell → book $10,952.67; vs 09:30 mark -2.55 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 6 | $120.51 | $2.03 | $+2.44 | $5,517.21 | ▲ +2.44 after sell → book $10,950.64; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 319 | $2.40 | $4.18 | $-30.62 | $6,278.64 | ▼ -30.62 after sell → book $10,946.47; vs 09:30 mark -4.17 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 6 | $121.00 | $2.03 | $+30.88 | $7,002.61 | ▲ +30.88 after sell → book $10,944.44; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 1 | $653.04 | $2.01 | $+25.77 | $7,653.63 | ▲ +25.77 after sell → book $10,942.42; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 67 | $11.17 | $2.21 | $-39.91 | $8,399.81 | ▼ -39.91 after sell → book $10,940.21; vs 09:30 mark -2.21 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 71 | $11.48 | $2.22 | $+22.91 | $9,212.67 | ▲ +22.91 after sell → book $10,937.99; vs 09:30 mark -2.22 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 243 | $2.99 | $3.19 | $-67.07 | $9,936.05 | ▼ -67.07 after sell → book $10,934.80; vs 09:30 mark -3.19 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,936.05 | ▼ close $10,906.33 vs 09:30 $10,969.50 (session -28.47) | 16:00 close · cash $9,936.05 · equity $10,906.33 vs 09:30 $10,969.50 (-63.17; session marks -28.47) · 1 name(s) marked open→close (per-name table). CRSP×17 09:30 $58.75 → close $57.08 -28.47 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,936.05 | ▲ 09:30 equity $10,920.86 vs yday $10,906.33 (+14.53) | 09:30 open · cash $9,936.05 (unchanged overnight, no fees) · equity $10,920.86 vs prior close $10,906.33 (+14.53) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 17 | $57.93 | $2.06 | $-17.70 | $10,918.80 | ▼ -17.70 after sell → book $10,918.80; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 33 | $24.11 | $2.09 | — | $10,121.08 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $818.91; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 524 | $1.56 | $6.76 | — | $9,296.88 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $818.91; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 201 | $4.07 | $2.60 | — | $8,476.22 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $818.91; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 43 | $19.04 | $2.12 | — | $7,655.38 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $818.91; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 135 | $9.42 | $2.40 | — | $6,381.28 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; combo leftover $1275.90; owner union_news_g_h1 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 36 | $35.05 | $2.10 | — | $5,117.38 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $1275.90; owner union_news_g_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 44 | $28.86 | $2.12 | — | $3,845.42 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.7; combo leftover $1275.90; owner union_news_g_h1 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 146 | $8.72 | $2.43 | — | $2,569.87 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.0; combo leftover $1275.90; owner union_news_g_h1 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 10 | $118.52 | $2.02 | — | $1,382.65 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+21.7; combo leftover $1275.90; owner union_news_g_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 16 | $77.13 | $2.04 | — | $146.54 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; ⚪; ret5=+13.8; combo leftover $1275.90; owner union_news_g_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $146.54 | ▲ close $11,352.69 vs 09:30 $10,920.86 (session +460.56) | 16:00 close · cash $146.54 · equity $11,352.69 vs 09:30 $10,920.86 (+431.83; session marks +460.56) · 10 name(s) marked open→close (per-name table). REAX×33 09:30 $24.11 → close $28.43 +142.56; CYPH×524 09:30 $1.56 → close $1.64 +41.92; XHG×201 09:30 $4.07 → close $4.02 -10.05; ASST×43 09:30 $19.04 → close $21.39 +101.05; RUM×135 09:30 $9.42 → close $10.23 +109.35; EZPW×36 09:30 $35.05 → close $35.23 +6.48; ZYME×44 09:30 $28.86 → close $27.47 -61.16; EOLS×146 09:30 $8.72 → close $8.97 +37.23; AU×10 09:30 $118.52 → close $123.39 +48.70; FCX×16 09:30 $77.13 → close $79.91 +44.48 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $146.54 | ▼ 09:30 equity $11,138.13 vs yday $11,352.69 (-214.56) | 09:30 open · cash $146.54 (unchanged overnight, no fees) · equity $11,138.13 vs prior close $11,352.69 (-214.56) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 33 | $26.61 | $2.11 | $+78.30 | $1,022.56 | ▲ +78.30 after sell → book $11,136.02; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 524 | $1.60 | $6.86 | $+7.34 | $1,854.10 | ▲ +7.34 after sell → book $11,129.16; vs 09:30 mark -6.86 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 43 | $20.72 | $2.14 | $+67.98 | $2,742.92 | ▲ +67.98 after sell → book $11,127.02; vs 09:30 mark -2.14 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 135 | $10.07 | $2.43 | $+82.93 | $4,099.94 | ▲ +82.93 after sell → book $11,124.59; vs 09:30 mark -2.43 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 36 | $35.70 | $2.12 | $+19.18 | $5,383.02 | ▲ +19.18 after sell → book $11,122.47; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 44 | $27.56 | $2.14 | $-61.46 | $6,593.52 | ▼ -61.46 after sell → book $11,120.33; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 146 | $8.86 | $2.46 | $+15.55 | $7,884.62 | ▲ +15.55 after sell → book $11,117.87; vs 09:30 mark -2.46 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 10 | $119.80 | $2.04 | $+8.74 | $9,080.58 | ▲ +8.74 after sell → book $11,115.83; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 16 | $79.34 | $2.06 | $+31.26 | $10,347.96 | ▲ +31.26 after sell → book $11,113.77; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 73 | $14.11 | $2.21 | — | $9,315.72 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $1034.80; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 178 | $5.81 | $2.52 | — | $8,279.02 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $1034.80; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SUJA` | 110 | $9.39 | $2.32 | — | $7,243.80 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+39.0; combo leftover $1034.80; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 130 | $11.12 | $2.38 | — | $5,795.82 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; combo leftover $1448.76; owner union_news_g_h1 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 174 | $8.29 | $2.51 | — | $4,350.85 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; combo leftover $1448.76; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 83 | $17.41 | $2.24 | — | $2,903.58 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-9.2; combo leftover $1448.76; owner union_news_g_h1 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 129 | $11.22 | $2.38 | — | $1,453.82 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.8; combo leftover $1448.76; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 5 | $267.02 | $2.00 | — | $116.71 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.7; combo leftover $1448.76; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.71 | ▲ close $11,419.51 vs 09:30 $11,138.13 (session +324.31) | 16:00 close · cash $116.71 · equity $11,419.51 vs 09:30 $11,138.13 (+281.39; session marks +324.31) · 9 name(s) marked open→close (per-name table). XHG×201 09:30 $3.81 → close $4.06 +50.25; BYND×73 09:30 $14.11 → close $14.25 +10.22; USDE×178 09:30 $5.81 → close $5.98 +30.26; SUJA×110 09:30 $9.39 → close $9.44 +5.50; FLNC×130 09:30 $11.12 → close $11.08 -5.20; CAPR×174 09:30 $8.29 → close $9.36 +186.18; FWRD×83 09:30 $17.41 → close $17.63 +18.26; TRLV×129 09:30 $11.22 → close $11.43 +27.09; FNV×5 09:30 $267.02 → close $267.37 +1.75 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.71 | ▲ 09:30 equity $11,523.10 vs yday $11,419.51 (+103.59) | 09:30 open · cash $116.71 (unchanged overnight, no fees) · equity $11,523.10 vs prior close $11,419.51 (+103.59) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 201 | $4.06 | $2.64 | $-7.25 | $930.13 | ▼ -7.25 after sell → book $11,520.46; vs 09:30 mark -2.65 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 73 | $14.20 | $2.23 | $+2.13 | $1,964.50 | ▲ +2.13 after sell → book $11,518.23; vs 09:30 mark -2.23 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 178 | $6.50 | $2.56 | $+117.73 | $3,118.94 | ▲ +117.73 after sell → book $11,515.67; vs 09:30 mark -2.56 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 110 | $9.41 | $2.35 | $-2.47 | $4,151.69 | ▼ -2.47 after sell → book $11,513.32; vs 09:30 mark -2.35 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 130 | $11.52 | $2.41 | $+47.21 | $5,646.88 | ▲ +47.21 after sell → book $11,510.91; vs 09:30 mark -2.41 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 174 | $9.19 | $2.55 | $+151.53 | $7,243.38 | ▲ +151.53 after sell → book $11,508.35; vs 09:30 mark -2.56 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FWRD` | 83 | $17.60 | $2.26 | $+11.27 | $8,701.92 | ▲ +11.27 after sell → book $11,506.09; vs 09:30 mark -2.26 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 129 | $11.38 | $2.41 | $+15.85 | $10,167.53 | ▲ +15.85 after sell → book $11,503.68; vs 09:30 mark -2.41 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 5 | $267.23 | $2.03 | $-2.98 | $11,501.65 | ▼ -2.98 after sell → book $11,501.65; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 331 | $2.60 | $4.27 | — | $10,636.78 | — | top 4 by hot; rank hot_score; list flatten; ret5=+13.0; combo leftover $862.62; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 20 | $41.44 | $2.05 | — | $9,805.93 | — | top 4 by hot; rank hot_score; list flatten; ret5=+3.1; combo leftover $862.62; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 37 | $22.93 | $2.10 | — | $8,955.42 | — | top 4 by hot; rank hot_score; list mover_buy; 🔵; ret5=+9.5; combo leftover $862.62; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 59 | $14.42 | $2.17 | — | $8,102.48 | — | top 4 by hot; rank hot_score; list flatten; ret5=+7.1; combo leftover $862.62; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 19 | $81.65 | $2.05 | — | $6,549.08 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+2.0; combo leftover $1620.50; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $5,580.08 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+0.1; combo leftover $1620.50; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 5 | $318.88 | $2.00 | — | $3,983.67 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+1.9; combo leftover $1620.50; owner union_news_g_h1 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 7 | $222.86 | $2.01 | — | $2,421.64 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=-3.6; combo leftover $1620.50; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,421.64 | ▲ close $11,504.94 vs 09:30 $11,523.10 (session +21.93) | 16:00 close · cash $2,421.64 · equity $11,504.94 vs 09:30 $11,523.10 (-18.16; session marks +21.93) · 8 name(s) marked open→close (per-name table). SLI×331 09:30 $2.60 → close $2.64 +13.24; RRC×20 09:30 $41.44 → close $41.64 +4.00; PGY×37 09:30 $22.93 → close $23.26 +12.21; CRK×59 09:30 $14.42 → close $14.62 +11.80; ACMR×19 09:30 $81.65 → close $80.49 -22.04; MU×1 09:30 $967.01 → close $935.39 -31.62; LRCX×5 09:30 $318.88 → close $318.58 -1.50; NVDA×7 09:30 $222.86 → close $227.98 +35.84 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,421.64 | ▼ 09:30 equity $11,472.55 vs yday $11,504.94 (-32.39) | 09:30 open · cash $2,421.64 (unchanged overnight, no fees) · equity $11,472.55 vs prior close $11,504.94 (-32.39) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 331 | $2.68 | $4.33 | $+17.88 | $3,304.38 | ▲ +17.88 after sell → book $11,468.21; vs 09:30 mark -4.34 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 20 | $41.74 | $2.07 | $+1.88 | $4,137.11 | ▲ +1.88 after sell → book $11,466.14; vs 09:30 mark -2.07 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 37 | $23.21 | $2.12 | $+6.14 | $4,993.76 | ▲ +6.14 after sell → book $11,464.02; vs 09:30 mark -2.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 59 | $14.63 | $2.19 | $+8.04 | $5,854.75 | ▲ +8.04 after sell → book $11,461.84; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 19 | $79.27 | $2.07 | $-49.34 | $7,358.81 | ▼ -49.34 after sell → book $11,459.77; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $8,276.08 | ▼ -51.73 after sell → book $11,457.75; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 5 | $318.03 | $2.03 | $-8.28 | $9,864.21 | ▼ -8.28 after sell → book $11,455.73; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 7 | $227.36 | $2.03 | $+27.46 | $11,453.69 | ▲ +27.46 after sell → book $11,453.69; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 61 | $14.00 | $2.17 | — | $10,597.52 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $859.03; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 88 | $9.73 | $2.25 | — | $9,739.03 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+47.1; combo leftover $859.03; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 6 | $137.19 | $2.01 | — | $8,913.88 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+7.1; combo leftover $859.03; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 5 | $146.07 | $2.00 | — | $8,181.52 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $859.03; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 28 | $41.74 | $2.07 | — | $7,010.73 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+2.4; combo leftover $1168.79; owner union_news_g_h1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 35 | $32.90 | $2.10 | — | $5,857.13 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; combo leftover $1168.79; owner union_news_g_h1 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 135 | $8.61 | $2.40 | — | $4,692.39 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.7; combo leftover $1168.79; owner union_news_g_h1 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $3,556.29 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; combo leftover $1168.79; owner union_news_g_h1 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 60 | $19.25 | $2.17 | — | $2,399.12 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+14.1; combo leftover $1168.79; owner union_news_g_h1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 62 | $18.75 | $2.18 | — | $1,234.45 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-5.0; combo leftover $1168.79; owner union_news_g_h1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 40 | $28.91 | $2.11 | — | $75.94 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+9.2; combo leftover $1168.79; owner union_news_g_h1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.94 | ▼ close $11,199.39 vs 09:30 $11,472.55 (session -230.83) | 16:00 close · cash $75.94 · equity $11,199.39 vs 09:30 $11,472.55 (-273.16; session marks -230.83) · 11 name(s) marked open→close (per-name table). RRC×28 09:30 $41.74 → close $41.46 -7.84; BYND×61 09:30 $14.00 → close $13.86 -8.54; CAPR×88 09:30 $9.73 → close $9.59 -12.32; MRNA×6 09:30 $137.19 → close $137.99 +4.80; ANF×5 09:30 $146.07 → close $148.42 +11.75; SEDG×35 09:30 $32.90 → close $31.41 -52.15; OPTX×135 09:30 $8.61 → close $8.52 -12.15; SMTC×8 09:30 $141.76 → close $131.17 -84.72; ERAS×60 09:30 $19.25 → close $18.03 -73.20; BBWI×62 09:30 $18.75 → close $19.22 +29.14; ZYME×40 09:30 $28.91 → close $28.27 -25.60 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.94 | ▼ 09:30 equity $11,162.05 vs yday $11,199.39 (-37.34) | 09:30 open · cash $75.94 (unchanged overnight, no fees) · equity $11,162.05 vs prior close $11,199.39 (-37.34) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 88 | $9.50 | $2.28 | $-24.77 | $909.66 | ▼ -24.77 after sell → book $11,159.77; vs 09:30 mark -2.28 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 6 | $134.10 | $2.03 | $-22.58 | $1,712.23 | ▼ -22.58 after sell → book $11,157.74; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 5 | $148.03 | $2.02 | $+5.77 | $2,450.36 | ▲ +5.77 after sell → book $11,155.72; vs 09:30 mark -2.02 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 28 | $42.00 | $2.09 | $+3.11 | $3,624.26 | ▲ +3.11 after sell → book $11,153.62; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 35 | $31.15 | $2.12 | $-65.46 | $4,712.40 | ▼ -65.46 after sell → book $11,151.51; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 135 | $8.52 | $2.43 | $-16.97 | $5,860.17 | ▼ -16.97 after sell → book $11,149.08; vs 09:30 mark -2.43 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $132.30 | $2.03 | $-79.73 | $6,916.54 | ▼ -79.73 after sell → book $11,147.05; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 60 | $17.87 | $2.19 | $-87.16 | $7,986.55 | ▼ -87.16 after sell → book $11,144.86; vs 09:30 mark -2.19 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 62 | $19.25 | $2.20 | $+26.63 | $9,177.85 | ▲ +26.63 after sell → book $11,142.66; vs 09:30 mark -2.20 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 40 | $28.06 | $2.13 | $-38.24 | $10,298.12 | ▼ -38.24 after sell → book $11,140.53; vs 09:30 mark -2.13 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,298.12 | ▼ close $11,109.42 vs 09:30 $11,162.05 (session -31.11) | 16:00 close · cash $10,298.12 · equity $11,109.42 vs 09:30 $11,162.05 (-52.63; session marks -31.11) · 1 name(s) marked open→close (per-name table). BYND×61 09:30 $13.81 → close $13.30 -31.11 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,298.12 | ▼ 09:30 equity $11,093.56 vs yday $11,109.42 (-15.86) | 09:30 open · cash $10,298.12 (unchanged overnight, no fees) · equity $11,093.56 vs prior close $11,109.42 (-15.86) | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 61 | $13.04 | $2.19 | $-62.93 | $11,091.37 | ▼ -62.93 after sell → book $11,091.37; vs 09:30 mark -2.19 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,091.37 | ▲ close $11,091.37 vs 09:30 $11,093.56 (session +0.00) | 16:00 close · cash $11,091.37 · no lots left · equity $11,091.37. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,091.37 | ▲ 09:30 equity $11,091.37 vs yday $11,091.37 (-0.00) | 09:30 open · cash $11,091.37 (unchanged overnight, no fees) · equity $11,091.37 vs prior close $11,091.37 (-0.00) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,091.37 | ▲ close $11,091.37 vs 09:30 $11,091.37 (session +0.00) | 16:00 close · cash $11,091.37 · no lots left · equity $11,091.37. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,091.37 | ▲ 09:30 equity $11,091.37 vs yday $11,091.37 (-0.00) | 09:30 open · cash $11,091.37 (unchanged overnight, no fees) · equity $11,091.37 vs prior close $11,091.37 (-0.00) | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 467 | $1.78 | $6.02 | — | $10,254.08 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $831.85; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 45 | $18.40 | $2.12 | — | $9,423.96 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $831.85; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 60 | $13.71 | $2.17 | — | $8,599.19 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $831.85; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 34 | $23.88 | $2.09 | — | $7,785.18 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $831.85; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 33 | $32.88 | $2.09 | — | $6,698.05 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+16.2; combo leftover $1112.17; owner union_news_g_h1 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 146 | $7.59 | $2.43 | — | $5,587.48 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.5; combo leftover $1112.17; owner union_news_g_h1 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $4,882.24 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.1; combo leftover $1112.17; owner union_news_g_h1 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 70 | $15.87 | $2.20 | — | $3,769.14 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $1112.17; owner union_news_g_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $2,711.92 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+3.3; combo leftover $1112.17; owner union_news_g_h1 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 3 | $354.49 | $2.00 | — | $1,646.45 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-12.3; combo leftover $1112.17; owner union_news_g_h1 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 23 | $47.60 | $2.06 | — | $549.59 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; combo leftover $1112.17; owner union_news_g_h1 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $549.59 | ▼ close $11,038.08 vs 09:30 $11,091.37 (session -26.11) | 16:00 close · cash $549.59 · equity $11,038.08 vs 09:30 $11,091.37 (-53.29; session marks -26.11) · 11 name(s) marked open→close (per-name table). GPRO×467 09:30 $1.78 → close $1.39 -182.13; REAX×45 09:30 $18.40 → close $18.40 +0.00; CNH×60 09:30 $13.71 → close $13.84 +7.80; MMED×34 09:30 $23.88 → close $23.84 -1.36; CNXC×33 09:30 $32.88 → close $32.85 -0.99; OPTX×146 09:30 $7.59 → close $7.76 +24.82; DE×1 09:30 $703.25 → close $694.41 -8.84; FRNM×70 09:30 $15.87 → close $16.90 +72.10; AVGO×3 09:30 $351.74 → close $357.16 +16.26; CIEN×3 09:30 $354.49 → close $317.46 -111.09; HPE×23 09:30 $47.60 → close $54.44 +157.32 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $549.59 | ▼ 09:30 equity $11,033.33 vs yday $11,038.08 (-4.75) | 09:30 open · cash $549.59 (unchanged overnight, no fees) · equity $11,033.33 vs prior close $11,038.08 (-4.75) | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 45 | $18.15 | $2.15 | $-15.52 | $1,364.19 | ▼ -15.52 after sell → book $11,031.18; vs 09:30 mark -2.15 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 60 | $13.89 | $2.19 | $+6.44 | $2,195.40 | ▲ +6.44 after sell → book $11,028.99; vs 09:30 mark -2.19 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 34 | $23.84 | $2.11 | $-5.56 | $3,003.85 | ▼ -5.56 after sell → book $11,026.88; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 33 | $32.48 | $2.11 | $-17.40 | $4,073.58 | ▼ -17.40 after sell → book $11,024.77; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 146 | $7.79 | $2.46 | $+24.31 | $5,208.46 | ▲ +24.31 after sell → book $11,022.31; vs 09:30 mark -2.46 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $5,898.48 | ▼ -15.23 after sell → book $11,020.30; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $6,975.56 | ▲ +19.86 after sell → book $11,018.28; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 3 | $321.67 | $2.02 | $-102.48 | $7,938.55 | ▼ -102.48 after sell → book $11,016.26; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 23 | $53.85 | $2.08 | $+139.61 | $9,175.02 | ▲ +139.61 after sell → book $11,014.18; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 36 | $25.18 | $2.10 | — | $8,266.44 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $917.50; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 116 | $7.87 | $2.34 | — | $7,351.18 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $917.50; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 158 | $5.79 | $2.46 | — | $6,433.90 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $917.50; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $5,378.46 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; combo leftover $1286.78; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 663 | $1.94 | $8.55 | — | $4,083.69 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+18.3; combo leftover $1286.78; owner union_news_g_h1 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 9 | $137.35 | $2.02 | — | $2,845.52 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+5.4; combo leftover $1286.78; owner union_news_g_h1 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 5 | $236.82 | $2.00 | — | $1,659.41 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.1; combo leftover $1286.78; owner union_news_g_h1 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 17 | $75.65 | $2.04 | — | $371.32 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+6.0; combo leftover $1286.78; owner union_news_g_h1 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $371.32 | ▲ close $11,301.43 vs 09:30 $11,033.33 (session +310.77) | 16:00 close · cash $371.32 · equity $11,301.43 vs 09:30 $11,033.33 (+268.10; session marks +310.77) · 10 name(s) marked open→close (per-name table). GPRO×467 09:30 $1.48 → close $1.70 +102.74; FRNM×70 09:30 $16.40 → close $16.31 -6.30; ASST×36 09:30 $25.18 → close $27.14 +70.56; USDE×116 09:30 $7.87 → close $7.93 +6.96; DFDV×158 09:30 $5.79 → close $5.87 +12.64; CRM×4 09:30 $263.36 → close $259.23 -16.52; BAK×663 09:30 $1.94 → close $1.89 -33.15; MSTR×9 09:30 $137.35 → close $142.80 +49.05; BE×5 09:30 $236.82 → close $252.87 +80.25; MRX×17 09:30 $75.65 → close $78.27 +44.54 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $371.32 | ▼ 09:30 equity $11,262.72 vs yday $11,301.43 (-38.71) | 09:30 open · cash $371.32 (unchanged overnight, no fees) · equity $11,262.72 vs prior close $11,301.43 (-38.71) | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 467 | $1.56 | $6.11 | $-112.54 | $1,096.07 | ▼ -112.54 after sell → book $11,256.61; vs 09:30 mark -6.11 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 70 | $16.74 | $2.22 | $+56.48 | $2,265.64 | ▲ +56.48 after sell → book $11,254.38; vs 09:30 mark -2.23 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 36 | $26.44 | $2.12 | $+41.14 | $3,215.37 | ▲ +41.14 after sell → book $11,252.27; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 116 | $7.76 | $2.37 | $-17.47 | $4,113.16 | ▼ -17.47 after sell → book $11,249.90; vs 09:30 mark -2.37 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 158 | $5.81 | $2.50 | $-1.80 | $5,028.64 | ▼ -1.80 after sell → book $11,247.40; vs 09:30 mark -2.50 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $6,041.50 | ▼ -42.58 after sell → book $11,245.38; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 663 | $1.94 | $8.67 | $-17.23 | $7,319.04 | ▼ -17.23 after sell → book $11,236.70; vs 09:30 mark -8.68 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 5 | $267.76 | $2.03 | $+150.67 | $8,655.82 | ▲ +150.67 after sell → book $11,234.68; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,655.82 | ▼ close $11,188.57 vs 09:30 $11,262.72 (session -46.11) | 16:00 close · cash $8,655.82 · equity $11,188.57 vs 09:30 $11,262.72 (-74.15; session marks -46.11) · 2 name(s) marked open→close (per-name table). MSTR×9 09:30 $137.62 → close $136.52 -9.90; MRX×17 09:30 $78.84 → close $76.71 -36.21 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1040.54 < 1 share @ 1646.93 |
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
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new union_news_g_h1 |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new union_news_g_h1 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new union_news_g_h1 |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new union_news_g_h1 |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new union_news_g_h1 |
| 2026-08-27 | `ASML` | cash | leftover split 1620.50 < 1 share @ 1746.53 |
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
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new union_hot_n4_h1 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new union_hot_n4_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new union_hot_n4_h1 |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new union_hot_n4_h1 |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new union_news_g_h1 |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new union_news_g_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new union_news_g_h1 |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new union_news_g_h1 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `MSTR` | 9 | 2026-09-04 @ $137.35 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+5.4; combo leftover $1286.78; owner union_news_g_h1 |
| `MRX` | 17 | 2026-09-04 @ $75.65 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+6.0; combo leftover $1286.78; owner union_news_g_h1 |
