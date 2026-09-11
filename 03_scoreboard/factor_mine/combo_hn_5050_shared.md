# Factor mine action — `combo_hn_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_hot_n4_h1/union_news_g_h1 w=0.5,0.5 net=priority

Cash book **+13.36%** ($11,336) · signal-only (no cash/fees) was —. Starts YES **10/20**. Fills 196 · skips 94 · realized $+1335.53.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_hot_n4_h1 50%, union_news_g_h1 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_hot_n4_h1 50%, union_news_g_h1 50%.
- Member: union_hot_n4_h1 (50% · long · hold 1).
- Member: union_news_g_h1 (50% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,335.52.

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
| 2026-08-14 | `QMCO` | 52 | — | $24.68 | +0.00 | $26.11 | +74.36 | +74.36 | +0.00 | +74.36 |
| 2026-08-14 | `ARX` | 66 | — | $19.57 | +0.00 | $19.58 | +0.66 | +0.66 | +0.00 | +0.66 |
| 2026-08-14 | `ZENA` | 589 | — | $2.20 | +0.00 | $2.14 | -35.34 | -35.34 | +0.00 | -35.34 |
| 2026-08-14 | `AIRO` | 116 | — | $11.12 | +0.00 | $9.57 | -179.80 | -179.80 | +0.00 | -179.80 |
| 2026-08-14 | `TLN` | 2 | — | $359.83 | +0.00 | $362.74 | +5.82 | +5.82 | +0.00 | +5.82 |
| 2026-08-14 | `VST` | 5 | — | $146.90 | +0.00 | $148.13 | +6.15 | +6.15 | +0.00 | +6.15 |
| 2026-08-14 | `NRG` | 6 | — | $120.00 | +0.00 | $126.24 | +37.44 | +37.44 | +0.00 | +37.44 |
| 2026-08-14 | `ANGX` | 172 | — | $4.31 | +0.00 | $4.37 | +10.32 | +10.32 | +0.00 | +10.32 |
| 2026-08-14 | `MH` | 54 | — | $13.55 | +0.00 | $13.10 | -24.30 | -24.30 | +0.00 | -24.30 |
| 2026-08-14 | `HLIT` | 56 | — | $13.18 | +0.00 | $13.92 | +41.44 | +41.44 | +0.00 | +41.44 |
| 2026-08-17 | `QMCO` | 52 | $26.11 | $24.83 | -66.56 | — | +0.00 | -66.56 | +7.80 | — |
| 2026-08-17 | `ARX` | 66 | $19.58 | $19.57 | -0.66 | — | +0.00 | -0.66 | +0.00 | — |
| 2026-08-17 | `ZENA` | 589 | $2.14 | $2.08 | -32.40 | — | +0.00 | -32.40 | -67.74 | — |
| 2026-08-17 | `AIRO` | 116 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -179.80 | — |
| 2026-08-17 | `TLN` | 2 | $362.74 | $367.88 | +10.28 | — | +0.00 | +10.28 | +16.10 | — |
| 2026-08-17 | `VST` | 5 | $148.13 | $149.37 | +6.20 | — | +0.00 | +6.20 | +12.35 | — |
| 2026-08-17 | `NRG` | 6 | $126.24 | $127.40 | +6.96 | — | +0.00 | +6.96 | +44.40 | — |
| 2026-08-17 | `ANGX` | 172 | $4.37 | $4.60 | +39.56 | — | +0.00 | +39.56 | +49.88 | — |
| 2026-08-17 | `MH` | 54 | $13.10 | $13.16 | +3.24 | — | +0.00 | +3.24 | -21.06 | — |
| 2026-08-17 | `HLIT` | 56 | $13.92 | $13.84 | -4.48 | — | +0.00 | -4.48 | +36.96 | — |
| 2026-08-17 | `XHG` | 304 | — | $4.19 | +0.00 | $3.91 | -85.12 | -85.12 | +0.00 | -85.12 |
| 2026-08-17 | `CAPR` | 185 | — | $6.87 | +0.00 | $7.45 | +107.30 | +107.30 | +0.00 | +107.30 |
| 2026-08-17 | `STDN` | 93 | — | $13.64 | +0.00 | $13.31 | -30.69 | -30.69 | +0.00 | -30.69 |
| 2026-08-17 | `HTFL` | 30 | — | $41.23 | +0.00 | $41.94 | +21.30 | +21.30 | +0.00 | +21.30 |
| 2026-08-17 | `DVN` | 22 | — | $46.18 | +0.00 | $47.57 | +30.58 | +30.58 | +0.00 | +30.58 |
| 2026-08-17 | `EOG` | 7 | — | $142.77 | +0.00 | $146.15 | +23.66 | +23.66 | +0.00 | +23.66 |
| 2026-08-17 | `FANG` | 5 | — | $202.70 | +0.00 | $206.29 | +17.95 | +17.95 | +0.00 | +17.95 |
| 2026-08-17 | `CELC` | 11 | — | $92.99 | +0.00 | $92.44 | -6.05 | -6.05 | +0.00 | -6.05 |
| 2026-08-17 | `OUST` | 21 | — | $49.00 | +0.00 | $48.13 | -18.27 | -18.27 | +0.00 | -18.27 |
| 2026-08-18 | `XHG` | 304 | $3.91 | $3.94 | +9.12 | — | +0.00 | +9.12 | -76.00 | — |
| 2026-08-18 | `CAPR` | 185 | $7.45 | $7.50 | +9.25 | $7.08 | -77.70 | -68.45 | +116.55 | +38.85 |
| 2026-08-18 | `STDN` | 93 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -30.69 | — |
| 2026-08-18 | `HTFL` | 30 | $41.94 | $41.50 | -13.20 | — | +0.00 | -13.20 | +8.10 | — |
| 2026-08-18 | `DVN` | 22 | $47.57 | $48.00 | +9.46 | — | +0.00 | +9.46 | +40.04 | — |
| 2026-08-18 | `EOG` | 7 | $146.15 | $148.04 | +13.23 | — | +0.00 | +13.23 | +36.89 | — |
| 2026-08-18 | `FANG` | 5 | $206.29 | $208.93 | +13.20 | — | +0.00 | +13.20 | +31.15 | — |
| 2026-08-18 | `CELC` | 11 | $92.44 | $92.38 | -0.66 | — | +0.00 | -0.66 | -6.71 | — |
| 2026-08-18 | `OUST` | 21 | $48.13 | $45.09 | -63.84 | — | +0.00 | -63.84 | -82.11 | — |
| 2026-08-19 | `CAPR` | 185 | $7.08 | $7.19 | +20.35 | — | +0.00 | +20.35 | +59.20 | — |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `CYPH` | 1103 | — | $1.15 | +0.00 | $1.19 | +44.12 | +44.12 | +0.00 | +44.12 |
| 2026-08-20 | `ABCL` | 107 | — | $11.81 | +0.00 | $11.57 | -26.21 | -26.21 | +0.00 | -26.21 |
| 2026-08-20 | `AZI` | 926 | — | $1.37 | +0.00 | $1.44 | +64.82 | +64.82 | +0.00 | +64.82 |
| 2026-08-20 | `BHP` | 8 | — | $91.01 | +0.00 | $93.63 | +20.96 | +20.96 | +0.00 | +20.96 |
| 2026-08-20 | `HUMA` | 1033 | — | $0.71 | +0.00 | $0.68 | -26.86 | -26.86 | +0.00 | -26.86 |
| 2026-08-20 | `BTGO` | 110 | — | $6.61 | +0.00 | $6.60 | -0.55 | -0.55 | +0.00 | -0.55 |
| 2026-08-20 | `ASST` | 45 | — | $16.00 | +0.00 | $16.13 | +5.85 | +5.85 | +0.00 | +5.85 |
| 2026-08-20 | `ZLAB` | 27 | — | $26.57 | +0.00 | $26.02 | -14.85 | -14.85 | +0.00 | -14.85 |
| 2026-08-20 | `CRSP` | 12 | — | $58.73 | +0.00 | $58.12 | -7.32 | -7.32 | +0.00 | -7.32 |
| 2026-08-20 | `APA` | 16 | — | $44.76 | +0.00 | $44.39 | -5.92 | -5.92 | +0.00 | -5.92 |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | $145.13 | +96.16 | +94.48 | -136.24 | -40.08 |
| 2026-08-21 | `CYPH` | 1103 | $1.19 | $1.32 | +143.39 | $1.42 | +110.30 | +253.69 | +187.51 | +297.81 |
| 2026-08-21 | `ABCL` | 107 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -26.21 | — |
| 2026-08-21 | `AZI` | 926 | $1.44 | $1.46 | +18.52 | — | +0.00 | +18.52 | +83.34 | — |
| 2026-08-21 | `BHP` | 8 | $93.63 | $95.72 | +16.72 | — | +0.00 | +16.72 | +37.68 | — |
| 2026-08-21 | `HUMA` | 1033 | $0.68 | $0.67 | -7.23 | — | +0.00 | -7.23 | -34.09 | — |
| 2026-08-21 | `BTGO` | 110 | $6.60 | $6.95 | +38.50 | — | +0.00 | +38.50 | +37.95 | — |
| 2026-08-21 | `ASST` | 45 | $16.13 | $17.66 | +68.85 | — | +0.00 | +68.85 | +74.70 | — |
| 2026-08-21 | `ZLAB` | 27 | $26.02 | $26.25 | +6.21 | — | +0.00 | +6.21 | -8.64 | — |
| 2026-08-21 | `CRSP` | 12 | $58.12 | $59.72 | +19.20 | $59.50 | -2.64 | +16.56 | +11.88 | +9.24 |
| 2026-08-21 | `APA` | 16 | $44.39 | $44.52 | +2.08 | — | +0.00 | +2.08 | -3.84 | — |
| 2026-08-21 | `XHG` | 392 | — | $4.49 | +0.00 | $4.41 | -31.36 | -31.36 | +0.00 | -31.36 |
| 2026-08-21 | `CAPR` | 258 | — | $6.81 | +0.00 | $6.29 | -134.16 | -134.16 | +0.00 | -134.16 |
| 2026-08-21 | `AU` | 4 | — | $119.43 | +0.00 | $121.22 | +7.16 | +7.16 | +0.00 | +7.16 |
| 2026-08-21 | `AUTL` | 203 | — | $2.47 | +0.00 | $2.41 | -12.18 | -12.18 | +0.00 | -12.18 |
| 2026-08-21 | `FUTU` | 4 | — | $115.18 | +0.00 | $123.64 | +33.84 | +33.84 | +0.00 | +33.84 |
| 2026-08-21 | `MARA` | 42 | — | $11.70 | +0.00 | $11.26 | -18.48 | -18.48 | +0.00 | -18.48 |
| 2026-08-21 | `BTDR` | 45 | — | $11.10 | +0.00 | $11.37 | +12.37 | +12.37 | +0.00 | +12.37 |
| 2026-08-21 | `HIVE` | 155 | — | $3.24 | +0.00 | $3.03 | -32.55 | -32.55 | +0.00 | -32.55 |
| 2026-08-24 | `MRNA` | 8 | $145.13 | $142.70 | -19.44 | — | +0.00 | -19.44 | -59.52 | — |
| 2026-08-24 | `CYPH` | 1103 | $1.42 | $1.83 | +452.23 | — | +0.00 | +452.23 | +750.04 | — |
| 2026-08-24 | `CRSP` | 12 | $59.50 | $58.75 | -9.00 | $57.08 | -20.10 | -29.10 | +0.24 | -19.86 |
| 2026-08-24 | `XHG` | 392 | $4.41 | $4.32 | -35.28 | — | +0.00 | -35.28 | -66.64 | — |
| 2026-08-24 | `CAPR` | 258 | $6.29 | $8.03 | +448.92 | — | +0.00 | +448.92 | +314.76 | — |
| 2026-08-24 | `AU` | 4 | $121.22 | $120.51 | -2.84 | — | +0.00 | -2.84 | +4.32 | — |
| 2026-08-24 | `AUTL` | 203 | $2.41 | $2.40 | -2.03 | — | +0.00 | -2.03 | -14.21 | — |
| 2026-08-24 | `FUTU` | 4 | $123.64 | $121.00 | -10.56 | — | +0.00 | -10.56 | +23.28 | — |
| 2026-08-24 | `MARA` | 42 | $11.26 | $11.17 | -3.78 | — | +0.00 | -3.78 | -22.26 | — |
| 2026-08-24 | `BTDR` | 45 | $11.37 | $11.48 | +4.95 | — | +0.00 | +4.95 | +17.32 | — |
| 2026-08-24 | `HIVE` | 155 | $3.03 | $2.99 | -6.20 | — | +0.00 | -6.20 | -38.75 | — |
| 2026-08-25 | `CRSP` | 12 | $57.08 | $57.93 | +10.26 | — | +0.00 | +10.26 | -9.60 | — |
| 2026-08-25 | `REAX` | 57 | — | $24.11 | +0.00 | $28.43 | +246.24 | +246.24 | +0.00 | +246.24 |
| 2026-08-25 | `CYPH` | 886 | — | $1.56 | +0.00 | $1.64 | +70.88 | +70.88 | +0.00 | +70.88 |
| 2026-08-25 | `XHG` | 339 | — | $4.07 | +0.00 | $4.02 | -16.95 | -16.95 | +0.00 | -16.95 |
| 2026-08-25 | `ASST` | 72 | — | $19.04 | +0.00 | $21.39 | +169.20 | +169.20 | +0.00 | +169.20 |
| 2026-08-25 | `RUM` | 97 | — | $9.42 | +0.00 | $10.23 | +78.57 | +78.57 | +0.00 | +78.57 |
| 2026-08-25 | `EZPW` | 26 | — | $35.05 | +0.00 | $35.23 | +4.68 | +4.68 | +0.00 | +4.68 |
| 2026-08-25 | `ZYME` | 31 | — | $28.86 | +0.00 | $27.47 | -43.09 | -43.09 | +0.00 | -43.09 |
| 2026-08-25 | `EOLS` | 105 | — | $8.72 | +0.00 | $8.97 | +26.77 | +26.77 | +0.00 | +26.77 |
| 2026-08-25 | `AU` | 7 | — | $118.52 | +0.00 | $123.39 | +34.09 | +34.09 | +0.00 | +34.09 |
| 2026-08-25 | `FCX` | 11 | — | $77.13 | +0.00 | $79.91 | +30.58 | +30.58 | +0.00 | +30.58 |
| 2026-08-26 | `REAX` | 57 | $28.43 | $26.61 | -103.74 | — | +0.00 | -103.74 | +142.50 | — |
| 2026-08-26 | `CYPH` | 886 | $1.64 | $1.60 | -35.44 | — | +0.00 | -35.44 | +35.44 | — |
| 2026-08-26 | `XHG` | 339 | $4.02 | $3.81 | -71.19 | $4.06 | +84.75 | +13.56 | -88.14 | -3.39 |
| 2026-08-26 | `ASST` | 72 | $21.39 | $20.72 | -48.24 | — | +0.00 | -48.24 | +120.96 | — |
| 2026-08-26 | `RUM` | 97 | $10.23 | $10.07 | -15.52 | — | +0.00 | -15.52 | +63.05 | — |
| 2026-08-26 | `EZPW` | 26 | $35.23 | $35.70 | +12.22 | — | +0.00 | +12.22 | +16.90 | — |
| 2026-08-26 | `ZYME` | 31 | $27.47 | $27.56 | +2.79 | — | +0.00 | +2.79 | -40.30 | — |
| 2026-08-26 | `EOLS` | 105 | $8.97 | $8.86 | -12.08 | — | +0.00 | -12.08 | +14.70 | — |
| 2026-08-26 | `AU` | 7 | $123.39 | $119.80 | -25.13 | — | +0.00 | -25.13 | +8.96 | — |
| 2026-08-26 | `FCX` | 11 | $79.91 | $79.34 | -6.27 | — | +0.00 | -6.27 | +24.31 | — |
| 2026-08-26 | `BYND` | 118 | — | $14.11 | +0.00 | $14.25 | +16.52 | +16.52 | +0.00 | +16.52 |
| 2026-08-26 | `USDE` | 286 | — | $5.81 | +0.00 | $5.98 | +48.62 | +48.62 | +0.00 | +48.62 |
| 2026-08-26 | `SUJA` | 177 | — | $9.39 | +0.00 | $9.44 | +8.85 | +8.85 | +0.00 | +8.85 |
| 2026-08-26 | `FLNC` | 90 | — | $11.12 | +0.00 | $11.08 | -3.60 | -3.60 | +0.00 | -3.60 |
| 2026-08-26 | `CAPR` | 120 | — | $8.29 | +0.00 | $9.36 | +128.40 | +128.40 | +0.00 | +128.40 |
| 2026-08-26 | `FWRD` | 57 | — | $17.41 | +0.00 | $17.63 | +12.54 | +12.54 | +0.00 | +12.54 |
| 2026-08-26 | `TRLV` | 89 | — | $11.22 | +0.00 | $11.43 | +18.69 | +18.69 | +0.00 | +18.69 |
| 2026-08-26 | `FNV` | 3 | — | $267.02 | +0.00 | $267.37 | +1.05 | +1.05 | +0.00 | +1.05 |
| 2026-08-27 | `XHG` | 339 | $4.06 | $4.06 | +0.00 | — | +0.00 | +0.00 | -3.39 | — |
| 2026-08-27 | `BYND` | 118 | $14.25 | $14.20 | -5.90 | — | +0.00 | -5.90 | +10.62 | — |
| 2026-08-27 | `USDE` | 286 | $5.98 | $6.50 | +148.72 | — | +0.00 | +148.72 | +197.34 | — |
| 2026-08-27 | `SUJA` | 177 | $9.44 | $9.41 | -5.31 | — | +0.00 | -5.31 | +3.54 | — |
| 2026-08-27 | `FLNC` | 90 | $11.08 | $11.52 | +39.60 | — | +0.00 | +39.60 | +36.00 | — |
| 2026-08-27 | `CAPR` | 120 | $9.36 | $9.19 | -20.40 | — | +0.00 | -20.40 | +108.00 | — |
| 2026-08-27 | `FWRD` | 57 | $17.63 | $17.60 | -1.71 | — | +0.00 | -1.71 | +10.83 | — |
| 2026-08-27 | `TRLV` | 89 | $11.43 | $11.38 | -4.45 | — | +0.00 | -4.45 | +14.24 | — |
| 2026-08-27 | `FNV` | 3 | $267.37 | $267.23 | -0.42 | — | +0.00 | -0.42 | +0.63 | — |
| 2026-08-27 | `SLI` | 563 | — | $2.60 | +0.00 | $2.64 | +22.52 | +22.52 | +0.00 | +22.52 |
| 2026-08-27 | `RRC` | 35 | — | $41.44 | +0.00 | $41.64 | +7.00 | +7.00 | +0.00 | +7.00 |
| 2026-08-27 | `PGY` | 63 | — | $22.93 | +0.00 | $23.26 | +20.79 | +20.79 | +0.00 | +20.79 |
| 2026-08-27 | `CRK` | 101 | — | $14.42 | +0.00 | $14.62 | +20.20 | +20.20 | +0.00 | +20.20 |
| 2026-08-27 | `ACMR` | 14 | — | $81.65 | +0.00 | $80.49 | -16.24 | -16.24 | +0.00 | -16.24 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `LRCX` | 3 | — | $318.88 | +0.00 | $318.58 | -0.90 | -0.90 | +0.00 | -0.90 |
| 2026-08-27 | `NVDA` | 5 | — | $222.86 | +0.00 | $227.98 | +25.60 | +25.60 | +0.00 | +25.60 |
| 2026-08-28 | `SLI` | 563 | $2.64 | $2.68 | +22.52 | — | +0.00 | +22.52 | +45.04 | — |
| 2026-08-28 | `RRC` | 20 | $41.64 | $41.74 | +3.50 | $41.46 | -5.60 | -2.10 | +10.50 | -5.60 |
| 2026-08-28 | `PGY` | 63 | $23.26 | $23.21 | -3.15 | — | +0.00 | -3.15 | +17.64 | — |
| 2026-08-28 | `CRK` | 101 | $14.62 | $14.63 | +1.01 | — | +0.00 | +1.01 | +21.21 | — |
| 2026-08-28 | `ACMR` | 14 | $80.49 | $79.27 | -17.08 | — | +0.00 | -17.08 | -33.32 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `LRCX` | 3 | $318.58 | $318.03 | -1.65 | — | +0.00 | -1.65 | -2.55 | — |
| 2026-08-28 | `NVDA` | 5 | $227.98 | $227.36 | -3.10 | — | +0.00 | -3.10 | +22.50 | — |
| 2026-08-28 | `BYND` | 104 | — | $14.00 | +0.00 | $13.86 | -14.56 | -14.56 | +0.00 | -14.56 |
| 2026-08-28 | `CAPR` | 150 | — | $9.73 | +0.00 | $9.59 | -21.00 | -21.00 | +0.00 | -21.00 |
| 2026-08-28 | `MRNA` | 10 | — | $137.19 | +0.00 | $137.99 | +8.00 | +8.00 | +0.00 | +8.00 |
| 2026-08-28 | `ANF` | 10 | — | $146.07 | +0.00 | $148.42 | +23.50 | +23.50 | +0.00 | +23.50 |
| 2026-08-28 | `SEDG` | 25 | — | $32.90 | +0.00 | $31.41 | -37.25 | -37.25 | +0.00 | -37.25 |
| 2026-08-28 | `OPTX` | 98 | — | $8.61 | +0.00 | $8.52 | -8.82 | -8.82 | +0.00 | -8.82 |
| 2026-08-28 | `SMTC` | 5 | — | $141.76 | +0.00 | $131.17 | -52.95 | -52.95 | +0.00 | -52.95 |
| 2026-08-28 | `ERAS` | 44 | — | $19.25 | +0.00 | $18.03 | -53.68 | -53.68 | +0.00 | -53.68 |
| 2026-08-28 | `BBWI` | 45 | — | $18.75 | +0.00 | $19.22 | +21.15 | +21.15 | +0.00 | +21.15 |
| 2026-08-28 | `ZYME` | 29 | — | $28.91 | +0.00 | $28.27 | -18.56 | -18.56 | +0.00 | -18.56 |
| 2026-08-31 | `BYND` | 104 | $13.86 | $13.81 | -5.20 | $13.30 | -53.04 | -58.24 | -19.76 | -72.80 |
| 2026-08-31 | `CAPR` | 150 | $9.59 | $9.50 | -13.50 | — | +0.00 | -13.50 | -34.50 | — |
| 2026-08-31 | `MRNA` | 10 | $137.99 | $134.10 | -38.90 | — | +0.00 | -38.90 | -30.90 | — |
| 2026-08-31 | `ANF` | 10 | $148.42 | $148.03 | -3.90 | — | +0.00 | -3.90 | +19.60 | — |
| 2026-08-31 | `RRC` | 20 | $41.46 | $42.00 | +10.80 | — | +0.00 | +10.80 | +5.20 | — |
| 2026-08-31 | `SEDG` | 25 | $31.41 | $31.15 | -6.50 | — | +0.00 | -6.50 | -43.75 | — |
| 2026-08-31 | `OPTX` | 98 | $8.52 | $8.52 | +0.00 | — | +0.00 | +0.00 | -8.82 | — |
| 2026-08-31 | `SMTC` | 5 | $131.17 | $132.30 | +5.65 | — | +0.00 | +5.65 | -47.30 | — |
| 2026-08-31 | `ERAS` | 44 | $18.03 | $17.87 | -7.04 | — | +0.00 | -7.04 | -60.72 | — |
| 2026-08-31 | `BBWI` | 45 | $19.22 | $19.25 | +1.35 | — | +0.00 | +1.35 | +22.50 | — |
| 2026-08-31 | `ZYME` | 29 | $28.27 | $28.06 | -6.09 | — | +0.00 | -6.09 | -24.65 | — |
| 2026-09-01 | `BYND` | 104 | $13.30 | $13.04 | -27.04 | — | +0.00 | -27.04 | -99.84 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `GPRO` | 797 | — | $1.78 | +0.00 | $1.39 | -310.83 | -310.83 | +0.00 | -310.83 |
| 2026-09-03 | `REAX` | 77 | — | $18.40 | +0.00 | $18.40 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `CNH` | 103 | — | $13.71 | +0.00 | $13.84 | +13.39 | +13.39 | +0.00 | +13.39 |
| 2026-09-03 | `MMED` | 59 | — | $23.88 | +0.00 | $23.84 | -2.36 | -2.36 | +0.00 | -2.36 |
| 2026-09-03 | `CNXC` | 24 | — | $32.88 | +0.00 | $32.85 | -0.72 | -0.72 | +0.00 | -0.72 |
| 2026-09-03 | `OPTX` | 106 | — | $7.59 | +0.00 | $7.76 | +18.02 | +18.02 | +0.00 | +18.02 |
| 2026-09-03 | `DE` | 1 | — | $703.25 | +0.00 | $694.41 | -8.84 | -8.84 | +0.00 | -8.84 |
| 2026-09-03 | `FRNM` | 51 | — | $15.87 | +0.00 | $16.90 | +52.53 | +52.53 | +0.00 | +52.53 |
| 2026-09-03 | `AVGO` | 2 | — | $351.74 | +0.00 | $357.16 | +10.84 | +10.84 | +0.00 | +10.84 |
| 2026-09-03 | `CIEN` | 2 | — | $354.49 | +0.00 | $317.46 | -74.06 | -74.06 | +0.00 | -74.06 |
| 2026-09-03 | `HPE` | 17 | — | $47.60 | +0.00 | $54.44 | +116.28 | +116.28 | +0.00 | +116.28 |
| 2026-09-04 | `GPRO` | 797 | $1.39 | $1.48 | +71.73 | $1.70 | +175.34 | +247.07 | -239.10 | -63.76 |
| 2026-09-04 | `REAX` | 77 | $18.40 | $18.15 | -19.25 | — | +0.00 | -19.25 | -19.25 | — |
| 2026-09-04 | `CNH` | 103 | $13.84 | $13.89 | +5.15 | — | +0.00 | +5.15 | +18.54 | — |
| 2026-09-04 | `MMED` | 59 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.36 | — |
| 2026-09-04 | `CNXC` | 24 | $32.85 | $32.48 | -8.88 | — | +0.00 | -8.88 | -9.60 | — |
| 2026-09-04 | `OPTX` | 106 | $7.76 | $7.79 | +3.18 | — | +0.00 | +3.18 | +21.20 | — |
| 2026-09-04 | `DE` | 1 | $694.41 | $692.03 | -2.38 | — | +0.00 | -2.38 | -11.22 | — |
| 2026-09-04 | `FRNM` | 51 | $16.90 | $16.40 | -25.50 | $16.31 | -4.59 | -30.09 | +27.03 | +22.44 |
| 2026-09-04 | `AVGO` | 2 | $357.16 | $359.70 | +5.08 | — | +0.00 | +5.08 | +15.92 | — |
| 2026-09-04 | `CIEN` | 2 | $317.46 | $321.67 | +8.42 | — | +0.00 | +8.42 | -65.64 | — |
| 2026-09-04 | `HPE` | 17 | $54.44 | $53.85 | -10.03 | — | +0.00 | -10.03 | +106.25 | — |
| 2026-09-04 | `ASST` | 60 | — | $25.18 | +0.00 | $27.14 | +117.60 | +117.60 | +0.00 | +117.60 |
| 2026-09-04 | `USDE` | 193 | — | $7.87 | +0.00 | $7.93 | +11.58 | +11.58 | +0.00 | +11.58 |
| 2026-09-04 | `DFDV` | 262 | — | $5.79 | +0.00 | $5.87 | +20.96 | +20.96 | +0.00 | +20.96 |
| 2026-09-04 | `CRM` | 3 | — | $263.36 | +0.00 | $259.23 | -12.39 | -12.39 | +0.00 | -12.39 |
| 2026-09-04 | `BAK` | 471 | — | $1.94 | +0.00 | $1.89 | -23.55 | -23.55 | +0.00 | -23.55 |
| 2026-09-04 | `MSTR` | 6 | — | $137.35 | +0.00 | $142.80 | +32.70 | +32.70 | +0.00 | +32.70 |
| 2026-09-04 | `BE` | 3 | — | $236.82 | +0.00 | $252.87 | +48.15 | +48.15 | +0.00 | +48.15 |
| 2026-09-04 | `MRX` | 12 | — | $75.65 | +0.00 | $78.27 | +31.44 | +31.44 | +0.00 | +31.44 |
| 2026-09-08 | `GPRO` | 797 | $1.70 | $1.56 | -107.60 | — | +0.00 | -107.60 | -171.36 | — |
| 2026-09-08 | `FRNM` | 51 | $16.31 | $16.74 | +21.93 | — | +0.00 | +21.93 | +44.37 | — |
| 2026-09-08 | `ASST` | 60 | $27.14 | $26.44 | -42.00 | — | +0.00 | -42.00 | +75.60 | — |
| 2026-09-08 | `USDE` | 193 | $7.93 | $7.76 | -32.81 | — | +0.00 | -32.81 | -21.23 | — |
| 2026-09-08 | `DFDV` | 262 | $5.87 | $5.81 | -15.72 | — | +0.00 | -15.72 | +5.24 | — |
| 2026-09-08 | `CRM` | 3 | $259.23 | $253.72 | -16.53 | — | +0.00 | -16.53 | -28.92 | — |
| 2026-09-08 | `BAK` | 471 | $1.89 | $1.94 | +23.55 | — | +0.00 | +23.55 | +0.00 | — |
| 2026-09-08 | `MSTR` | 6 | $142.80 | $137.62 | -31.08 | $136.52 | -6.60 | -37.68 | +1.62 | -4.98 |
| 2026-09-08 | `BE` | 3 | $252.87 | $267.76 | +44.67 | — | +0.00 | +44.67 | +92.82 | — |
| 2026-09-08 | `MRX` | 12 | $78.27 | $78.84 | +6.84 | $76.71 | -25.56 | -18.72 | +38.28 | +12.72 |
| 2026-09-09 | `MSTR` | 6 | $136.52 | $141.82 | +31.80 | — | +0.00 | +31.80 | +26.82 | — |
| 2026-09-09 | `MRX` | 12 | $76.71 | $76.60 | -1.32 | — | +0.00 | -1.32 | +11.40 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +386.21 | IREN, TNDM, TPG, INO | — | $0.54 | $10,345.37 | IREN×54, TNDM×107, TPG×49, INO×3085 |
| 2026-08-14 | +5.50 | $0.54 | IREN×54, TNDM×107, TPG×49, INO×3085 | $10,412.10 | +66.73 | -63.25 | QMCO, ARX, ZENA, AIRO, TLN, VST, NRG, ANGX, MH, HLIT | IREN, TNDM, TPG, INO | $793.87 | $10,276.58 | QMCO×52, ARX×66, ZENA×589, AIRO×116, TLN×2, VST×5, NRG×6, ANGX×172, MH×54, HLIT×56 |
| 2026-08-17 | +2.25 | $793.87 | QMCO×52, ARX×66, ZENA×589, AIRO×116, TLN×2, VST×5, NRG×6, ANGX×172, MH×54, HLIT×56 | $10,238.72 | -37.86 | +60.66 | XHG, CAPR, STDN, HTFL, DVN, EOG, FANG, CELC, OUST | QMCO, ARX, ZENA, AIRO, TLN, VST, NRG, ANGX, MH, HLIT | $59.47 | $10,251.00 | XHG×304, CAPR×185, STDN×93, HTFL×30, DVN×22, EOG×7, FANG×5, CELC×11, OUST×21 |
| 2026-08-18 | -6.20 | $59.47 | XHG×304, CAPR×185, STDN×93, HTFL×30, DVN×22, EOG×7, FANG×5, CELC×11, OUST×21 | $10,227.56 | -23.44 | -77.70 | — | XHG, STDN, HTFL, DVN, EOG, FANG, CELC, OUST | $8,821.44 | $10,131.24 | CAPR×185 |
| 2026-08-19 | -7.20 | $8,821.44 | CAPR×185 | $10,151.59 | +20.35 | +0.00 | — | CAPR | $10,149.00 | $10,149.00 | — |
| 2026-08-20 | +1.12 | $10,149.00 | — | $10,149.00 | +0.00 | -80.52 | MRNA, CYPH, ABCL, AZI, BHP, HUMA, BTGO, ASST, ZLAB, CRSP, APA | — | $49.84 | $10,014.99 | MRNA×8, CYPH×1103, ABCL×107, AZI×926, BHP×8, HUMA×1033, BTGO×110, ASST×45, ZLAB×27, CRSP×12, APA×16 |
| 2026-08-21 | +3.25 | $49.84 | MRNA×8, CYPH×1103, ABCL×107, AZI×926, BHP×8, HUMA×1033, BTGO×110, ASST×45, ZLAB×27, CRSP×12, APA×16 | $10,319.54 | +304.55 | +28.46 | XHG, CAPR, AU, AUTL, FUTU, MARA, BTDR, HIVE | ABCL, AZI, BHP, HUMA, BTGO, ASST, ZLAB, APA | $575.21 | $10,290.94 | MRNA×8, CYPH×1103, CRSP×12, XHG×392, CAPR×258, AU×4, AUTL×203, FUTU×4, MARA×42, BTDR×45, HIVE×155 |
| 2026-08-24 | -5.17 | $575.21 | MRNA×8, CYPH×1103, CRSP×12, XHG×392, CAPR×258, AU×4, AUTL×203, FUTU×4, MARA×42, BTDR×45, HIVE×155 | $11,107.91 | +816.97 | -20.10 | — | MRNA, CYPH, XHG, CAPR, AU, AUTL, FUTU, MARA, BTDR, HIVE | $10,364.44 | $11,049.34 | CRSP×12 |
| 2026-08-25 | +1.80 | $10,364.44 | CRSP×12 | $11,059.60 | +10.26 | +600.97 | REAX, CYPH, XHG, ASST, RUM, EZPW, ZYME, EOLS, AU, FCX | CRSP | $204.21 | $11,625.59 | REAX×57, CYPH×886, XHG×339, ASST×72, RUM×97, EZPW×26, ZYME×31, EOLS×105, AU×7, FCX×11 |
| 2026-08-26 | +2.02 | $204.21 | REAX×57, CYPH×886, XHG×339, ASST×72, RUM×97, EZPW×26, ZYME×31, EOLS×105, AU×7, FCX×11 | $11,323.00 | -302.59 | +315.82 | BYND, USDE, SUJA, FLNC, CAPR, FWRD, TRLV, FNV | REAX, CYPH, ASST, RUM, EZPW, ZYME, EOLS, AU, FCX | $206.64 | $11,590.33 | XHG×339, BYND×118, USDE×286, SUJA×177, FLNC×90, CAPR×120, FWRD×57, TRLV×89, FNV×3 |
| 2026-08-27 | — | $206.64 | XHG×339, BYND×118, USDE×286, SUJA×177, FLNC×90, CAPR×120, FWRD×57, TRLV×89, FNV×3 | $11,740.46 | +150.13 | +47.35 | SLI, RRC, PGY, CRK, ACMR, MU, LRCX, NVDA | XHG, BYND, USDE, SUJA, FLNC, CAPR, FWRD, TRLV, FNV | $1,698.06 | $11,741.67 | SLI×563, RRC×35, PGY×63, CRK×101, ACMR×14, MU×1, LRCX×3, NVDA×5 |
| 2026-08-28 | +0.75 | $1,698.06 | SLI×563, RRC×35, PGY×63, CRK×101, ACMR×14, MU×1, LRCX×3, NVDA×5 | $11,727.62 | -14.05 | -159.77 | BYND, CAPR, MRNA, ANF, RRC, SEDG, OPTX, SMTC, ERAS, BBWI, ZYME | SLI, RRC, PGY, CRK, ACMR, MU, LRCX, NVDA | $194.88 | $11,522.23 | BYND×104, CAPR×150, MRNA×10, ANF×10, RRC×20, SEDG×25, OPTX×98, SMTC×5, ERAS×44, BBWI×45, ZYME×29 |
| 2026-08-31 | -5.85 | $194.88 | BYND×104, CAPR×150, MRNA×10, ANF×10, RRC×20, SEDG×25, OPTX×98, SMTC×5, ERAS×44, BBWI×45, ZYME×29 | $11,458.90 | -63.33 | -53.04 | — | CAPR, MRNA, ANF, RRC, SEDG, OPTX, SMTC, ERAS, BBWI, ZYME | $10,001.22 | $11,384.42 | BYND×104 |
| 2026-09-01 | -6.30 | $10,001.22 | BYND×104 | $11,357.38 | -27.04 | +0.00 | — | BYND | $11,355.05 | $11,355.05 | — |
| 2026-09-02 | -3.83 | $11,355.05 | — | $11,355.05 | +0.00 | +0.00 | — | — | $11,355.05 | $11,355.05 | — |
| 2026-09-03 | -0.90 | $11,355.05 | — | $11,355.05 | +0.00 | -185.75 | GPRO, REAX, CNH, MMED, CNXC, OPTX, DE, FRNM, AVGO, CIEN, HPE | — | $339.10 | $11,137.80 | GPRO×797, REAX×77, CNH×103, MMED×59, CNXC×24, OPTX×106, DE×1, FRNM×51, AVGO×2, CIEN×2, HPE×17 |
| 2026-09-04 | +2.25 | $339.10 | GPRO×797, REAX×77, CNH×103, MMED×59, CNXC×24, OPTX×106, DE×1, FRNM×51, AVGO×2, CIEN×2, HPE×17 | $11,165.32 | +27.52 | +397.24 | ASST, USDE, DFDV, CRM, BAK, MSTR, BE, MRX | REAX, CNH, MMED, CNXC, OPTX, DE, AVGO, CIEN, HPE | $414.98 | $11,521.05 | GPRO×797, FRNM×51, ASST×60, USDE×193, DFDV×262, CRM×3, BAK×471, MSTR×6, BE×3, MRX×12 |
| 2026-09-08 | -11.47 | $414.98 | GPRO×797, FRNM×51, ASST×60, USDE×193, DFDV×262, CRM×3, BAK×471, MSTR×6, BE×3, MRX×12 | $11,372.30 | -148.75 | -32.16 | — | GPRO, FRNM, ASST, USDE, DFDV, CRM, BAK, BE | $9,569.47 | $11,309.11 | MSTR×6, MRX×12 |
| 2026-09-09 | -13.95 | $9,569.47 | MSTR×6, MRX×12 | $11,339.59 | +30.48 | +0.00 | — | MSTR, MRX | $11,335.52 | $11,335.52 | — |
| 2026-09-10 | -13.28 | $11,335.52 | — | $11,335.52 | -0.00 | +0.00 | — | — | $11,335.52 | $11,335.52 | — |

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
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 52 | $24.68 | $2.15 | — | $9,081.41 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; combo leftover $1295.87; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 66 | $19.57 | $2.19 | — | $7,787.61 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $1295.87; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 589 | $2.20 | $7.60 | — | $6,484.21 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; combo leftover $1295.87; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 116 | $11.12 | $2.34 | — | $5,191.95 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $1295.87; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 2 | $359.83 | $2.00 | — | $4,470.29 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; combo leftover $741.71; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 5 | $146.90 | $2.00 | — | $3,733.79 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; combo leftover $741.71; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 6 | $120.00 | $2.01 | — | $3,011.78 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; combo leftover $741.71; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 172 | $4.31 | $2.51 | — | $2,267.96 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; combo leftover $741.71; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 54 | $13.55 | $2.15 | — | $1,534.10 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $741.71; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 56 | $13.18 | $2.16 | — | $793.87 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; combo leftover $741.71; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $793.87 | ▼ close $10,276.58 vs 09:30 $10,412.10 (session -63.25) | 16:00 close · cash $793.87 · equity $10,276.58 vs 09:30 $10,412.10 (-135.52; session marks -63.25) · 10 name(s) marked open→close (per-name table). QMCO×52 09:30 $24.68 → close $26.11 +74.36; ARX×66 09:30 $19.57 → close $19.58 +0.66; ZENA×589 09:30 $2.20 → close $2.14 -35.34; AIRO×116 09:30 $11.12 → close $9.57 -179.80; TLN×2 09:30 $359.83 → close $362.74 +5.82; VST×5 09:30 $146.90 → close $148.13 +6.15; NRG×6 09:30 $120.00 → close $126.24 +37.44; ANGX×172 09:30 $4.31 → close $4.37 +10.32; MH×54 09:30 $13.55 → close $13.10 -24.30; HLIT×56 09:30 $13.18 → close $13.92 +41.44 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $793.87 | ▼ 09:30 equity $10,238.72 vs yday $10,276.58 (-37.86) | 09:30 open · cash $793.87 (unchanged overnight, no fees) · equity $10,238.72 vs prior close $10,276.58 (-37.86) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $2,082.86 | ▲ +3.49 after sell → book $10,236.55; vs 09:30 mark -2.17 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 66 | $19.57 | $2.21 | $-4.40 | $3,372.27 | ▼ -4.40 after sell → book $10,234.35; vs 09:30 mark -2.20 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 589 | $2.08 | $7.71 | $-83.04 | $4,592.63 | ▼ -83.04 after sell → book $10,226.64; vs 09:30 mark -7.71 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 116 | $9.57 | $2.37 | $-184.51 | $5,700.38 | ▼ -184.51 after sell → book $10,224.27; vs 09:30 mark -2.37 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 2 | $367.88 | $2.02 | $+12.09 | $6,434.13 | ▲ +12.09 after sell → book $10,222.26; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 5 | $149.37 | $2.02 | $+8.32 | $7,178.95 | ▲ +8.32 after sell → book $10,220.23; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 6 | $127.40 | $2.03 | $+40.36 | $7,941.32 | ▲ +40.36 after sell → book $10,218.20; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 172 | $4.60 | $2.54 | $+44.83 | $8,729.98 | ▲ +44.83 after sell → book $10,215.66; vs 09:30 mark -2.54 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 54 | $13.16 | $2.17 | $-25.38 | $9,438.45 | ▼ -25.38 after sell → book $10,213.49; vs 09:30 mark -2.17 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 56 | $13.84 | $2.18 | $+32.62 | $10,211.31 | ▲ +32.62 after sell → book $10,211.31; vs 09:30 mark -2.18 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 304 | $4.19 | $3.92 | — | $8,933.63 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $1276.41; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 185 | $6.87 | $2.54 | — | $7,660.13 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $1276.41; owner union_hot_n4_h1 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 93 | $13.64 | $2.27 | — | $6,389.34 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $1276.41; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 30 | $41.23 | $2.08 | — | $5,150.36 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $1276.41; owner union_hot_n4_h1 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 22 | $46.18 | $2.06 | — | $4,132.35 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; combo leftover $1030.07; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 7 | $142.77 | $2.01 | — | $3,130.95 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; combo leftover $1030.07; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 5 | $202.70 | $2.00 | — | $2,115.44 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; combo leftover $1030.07; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 11 | $92.99 | $2.02 | — | $1,090.53 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; combo leftover $1030.07; owner union_news_g_h1 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 21 | $49.00 | $2.05 | — | $59.47 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; combo leftover $1030.07; owner union_news_g_h1 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.47 | ▲ close $10,251.00 vs 09:30 $10,238.72 (session +60.66) | 16:00 close · cash $59.47 · equity $10,251.00 vs 09:30 $10,238.72 (+12.28; session marks +60.66) · 9 name(s) marked open→close (per-name table). XHG×304 09:30 $4.19 → close $3.91 -85.12; CAPR×185 09:30 $6.87 → close $7.45 +107.30; STDN×93 09:30 $13.64 → close $13.31 -30.69; HTFL×30 09:30 $41.23 → close $41.94 +21.30; DVN×22 09:30 $46.18 → close $47.57 +30.58; EOG×7 09:30 $142.77 → close $146.15 +23.66; FANG×5 09:30 $202.70 → close $206.29 +17.95; CELC×11 09:30 $92.99 → close $92.44 -6.05; OUST×21 09:30 $49.00 → close $48.13 -18.27 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.47 | ▼ 09:30 equity $10,227.56 vs yday $10,251.00 (-23.44) | 09:30 open · cash $59.47 (unchanged overnight, no fees) · equity $10,227.56 vs prior close $10,251.00 (-23.44) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 304 | $3.94 | $3.98 | $-83.90 | $1,253.25 | ▼ -83.90 after sell → book $10,223.58; vs 09:30 mark -3.98 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 93 | $13.31 | $2.29 | $-35.25 | $2,488.79 | ▼ -35.25 after sell → book $10,221.29; vs 09:30 mark -2.29 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 30 | $41.50 | $2.10 | $+3.92 | $3,731.69 | ▲ +3.92 after sell → book $10,219.19; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 22 | $48.00 | $2.08 | $+35.91 | $4,785.61 | ▲ +35.91 after sell → book $10,217.11; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 7 | $148.04 | $2.03 | $+32.85 | $5,819.86 | ▲ +32.85 after sell → book $10,215.08; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 5 | $208.93 | $2.02 | $+27.12 | $6,862.49 | ▲ +27.12 after sell → book $10,213.06; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 11 | $92.38 | $2.04 | $-10.78 | $7,876.62 | ▼ -10.78 after sell → book $10,211.01; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 21 | $45.09 | $2.07 | $-86.24 | $8,821.44 | ▼ -86.24 after sell → book $10,208.94; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,821.44 | ▼ close $10,131.24 vs 09:30 $10,227.56 (session -77.70) | 16:00 close · cash $8,821.44 · equity $10,131.24 vs 09:30 $10,227.56 (-96.32; session marks -77.70) · 1 name(s) marked open→close (per-name table). CAPR×185 09:30 $7.50 → close $7.08 -77.70 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,821.44 | ▲ 09:30 equity $10,151.59 vs yday $10,131.24 (+20.35) | 09:30 open · cash $8,821.44 (unchanged overnight, no fees) · equity $10,151.59 vs prior close $10,131.24 (+20.35) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 185 | $7.19 | $2.59 | $+54.07 | $10,149.00 | ▲ +54.07 after sell → book $10,149.00; vs 09:30 mark -2.59 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,149.00 | ▲ close $10,149.00 vs 09:30 $10,151.59 (session +0.00) | 16:00 close · cash $10,149.00 · no lots left · equity $10,149.00. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,149.00 | ▲ 09:30 equity $10,149.00 vs yday $10,149.00 (+0.00) | 09:30 open · cash $10,149.00 (unchanged overnight, no fees) · equity $10,149.00 vs prior close $10,149.00 (+0.00) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $8,945.87 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $1268.63; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1103 | $1.15 | $14.23 | — | $7,663.19 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $1268.63; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 107 | $11.81 | $2.31 | — | $6,396.68 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1268.63; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 926 | $1.37 | $11.95 | — | $5,116.11 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $1268.63; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 8 | $91.01 | $2.01 | — | $4,386.02 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; combo leftover $730.87; owner union_news_g_h1 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1033 | $0.71 | $10.40 | — | $3,645.28 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; combo leftover $730.87; owner union_news_g_h1 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 110 | $6.61 | $2.32 | — | $2,916.41 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; combo leftover $730.87; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 45 | $16.00 | $2.12 | — | $2,194.29 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; combo leftover $730.87; owner union_news_g_h1 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 27 | $26.57 | $2.07 | — | $1,474.83 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; combo leftover $730.87; owner union_news_g_h1 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 12 | $58.73 | $2.03 | — | $768.04 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; combo leftover $730.87; owner union_news_g_h1 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 16 | $44.76 | $2.04 | — | $49.84 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; combo leftover $730.87; owner union_news_g_h1 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.84 | ▼ close $10,014.99 vs 09:30 $10,149.00 (session -80.52) | 16:00 close · cash $49.84 · equity $10,014.99 vs 09:30 $10,149.00 (-134.01; session marks -80.52) · 11 name(s) marked open→close (per-name table). MRNA×8 09:30 $150.14 → close $133.32 -134.56; CYPH×1103 09:30 $1.15 → close $1.19 +44.12; ABCL×107 09:30 $11.81 → close $11.57 -26.21; AZI×926 09:30 $1.37 → close $1.44 +64.82; BHP×8 09:30 $91.01 → close $93.63 +20.96; HUMA×1033 09:30 $0.71 → close $0.68 -26.86; BTGO×110 09:30 $6.61 → close $6.60 -0.55; ASST×45 09:30 $16.00 → close $16.13 +5.85; ZLAB×27 09:30 $26.57 → close $26.02 -14.85; CRSP×12 09:30 $58.73 → close $58.12 -7.32; APA×16 09:30 $44.76 → close $44.39 -5.92 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.84 | ▲ 09:30 equity $10,319.54 vs yday $10,014.99 (+304.55) | 09:30 open · cash $49.84 (unchanged overnight, no fees) · equity $10,319.54 vs prior close $10,014.99 (+304.55) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 107 | $11.57 | $2.34 | $-30.86 | $1,285.49 | ▼ -30.86 after sell → book $10,317.21; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 926 | $1.46 | $12.11 | $+59.28 | $2,625.34 | ▲ +59.28 after sell → book $10,305.10; vs 09:30 mark -12.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 8 | $95.72 | $2.03 | $+33.63 | $3,389.07 | ▲ +33.63 after sell → book $10,303.06; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1033 | $0.67 | $10.24 | $-54.73 | $4,075.07 | ▼ -54.73 after sell → book $10,292.82; vs 09:30 mark -10.24 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 110 | $6.95 | $2.35 | $+33.28 | $4,837.22 | ▲ +33.28 after sell → book $10,290.47; vs 09:30 mark -2.35 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 45 | $17.66 | $2.15 | $+70.43 | $5,629.78 | ▲ +70.43 after sell → book $10,288.33; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 27 | $26.25 | $2.09 | $-12.80 | $6,336.43 | ▼ -12.80 after sell → book $10,286.23; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 16 | $44.52 | $2.06 | $-7.94 | $7,046.70 | ▼ -7.94 after sell → book $10,284.18; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 392 | $4.49 | $5.06 | — | $5,281.56 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $1761.67; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 258 | $6.81 | $3.33 | — | $3,521.25 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $1761.67; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 4 | $119.43 | $2.00 | — | $3,041.53 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $503.04; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 203 | $2.47 | $2.62 | — | $2,537.50 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; combo leftover $503.04; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 4 | $115.18 | $2.00 | — | $2,074.78 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $503.04; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 42 | $11.70 | $2.12 | — | $1,581.26 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; combo leftover $503.04; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 45 | $11.10 | $2.12 | — | $1,079.86 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; combo leftover $503.04; owner union_news_g_h1 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 155 | $3.24 | $2.46 | — | $575.21 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; combo leftover $503.04; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $575.21 | ▲ close $10,290.94 vs 09:30 $10,319.54 (session +28.46) | 16:00 close · cash $575.21 · equity $10,290.94 vs 09:30 $10,319.54 (-28.60; session marks +28.46) · 11 name(s) marked open→close (per-name table). MRNA×8 09:30 $133.11 → close $145.13 +96.16; CYPH×1103 09:30 $1.32 → close $1.42 +110.30; CRSP×12 09:30 $59.72 → close $59.50 -2.64; XHG×392 09:30 $4.49 → close $4.41 -31.36; CAPR×258 09:30 $6.81 → close $6.29 -134.16; AU×4 09:30 $119.43 → close $121.22 +7.16; AUTL×203 09:30 $2.47 → close $2.41 -12.18; FUTU×4 09:30 $115.18 → close $123.64 +33.84; MARA×42 09:30 $11.70 → close $11.26 -18.48; BTDR×45 09:30 $11.10 → close $11.37 +12.37; HIVE×155 09:30 $3.24 → close $3.03 -32.55 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $575.21 | ▲ 09:30 equity $11,107.91 vs yday $10,290.94 (+816.97) | 09:30 open · cash $575.21 (unchanged overnight, no fees) · equity $11,107.91 vs prior close $10,290.94 (+816.97) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 8 | $142.70 | $2.03 | $-63.57 | $1,714.77 | ▼ -63.57 after sell → book $11,105.87; vs 09:30 mark -2.04 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1103 | $1.83 | $14.43 | $+721.38 | $3,718.84 | ▲ +721.38 after sell → book $11,091.45; vs 09:30 mark -14.42 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 392 | $4.32 | $5.14 | $-76.83 | $5,407.14 | ▼ -76.83 after sell → book $11,086.31; vs 09:30 mark -5.14 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 258 | $8.03 | $3.39 | $+308.04 | $7,475.49 | ▲ +308.04 after sell → book $11,082.92; vs 09:30 mark -3.39 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 4 | $120.51 | $2.02 | $+0.30 | $7,955.51 | ▲ +0.30 after sell → book $11,080.90; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 203 | $2.40 | $2.66 | $-19.49 | $8,440.05 | ▼ -19.49 after sell → book $11,078.24; vs 09:30 mark -2.66 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 4 | $121.00 | $2.02 | $+19.26 | $8,922.03 | ▲ +19.26 after sell → book $11,076.22; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 42 | $11.17 | $2.14 | $-26.51 | $9,389.03 | ▼ -26.51 after sell → book $11,074.08; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 45 | $11.48 | $2.15 | $+13.06 | $9,903.49 | ▲ +13.06 after sell → book $11,071.94; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 155 | $2.99 | $2.49 | $-43.70 | $10,364.44 | ▼ -43.70 after sell → book $11,069.44; vs 09:30 mark -2.50 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,364.44 | ▼ close $11,049.34 vs 09:30 $11,107.91 (session -20.10) | 16:00 close · cash $10,364.44 · equity $11,049.34 vs 09:30 $11,107.91 (-58.57; session marks -20.10) · 1 name(s) marked open→close (per-name table). CRSP×12 09:30 $58.75 → close $57.08 -20.10 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,364.44 | ▲ 09:30 equity $11,059.60 vs yday $11,049.34 (+10.26) | 09:30 open · cash $10,364.44 (unchanged overnight, no fees) · equity $11,059.60 vs prior close $11,049.34 (+10.26) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 12 | $57.93 | $2.05 | $-13.67 | $11,057.56 | ▼ -13.67 after sell → book $11,057.56; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 57 | $24.11 | $2.16 | — | $9,681.13 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $1382.19; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 886 | $1.56 | $11.43 | — | $8,287.54 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $1382.19; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 339 | $4.07 | $4.37 | — | $6,903.44 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $1382.19; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 72 | $19.04 | $2.21 | — | $5,530.35 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $1382.19; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 97 | $9.42 | $2.28 | — | $4,614.33 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; combo leftover $921.72; owner union_news_g_h1 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 26 | $35.05 | $2.07 | — | $3,700.96 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $921.72; owner union_news_g_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 31 | $28.86 | $2.08 | — | $2,804.22 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.7; combo leftover $921.72; owner union_news_g_h1 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 105 | $8.72 | $2.31 | — | $1,886.31 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.0; combo leftover $921.72; owner union_news_g_h1 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 7 | $118.52 | $2.01 | — | $1,054.66 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+21.7; combo leftover $921.72; owner union_news_g_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 11 | $77.13 | $2.02 | — | $204.21 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; ⚪; ret5=+13.8; combo leftover $921.72; owner union_news_g_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $204.21 | ▲ close $11,625.59 vs 09:30 $11,059.60 (session +600.97) | 16:00 close · cash $204.21 · equity $11,625.59 vs 09:30 $11,059.60 (+565.99; session marks +600.97) · 10 name(s) marked open→close (per-name table). REAX×57 09:30 $24.11 → close $28.43 +246.24; CYPH×886 09:30 $1.56 → close $1.64 +70.88; XHG×339 09:30 $4.07 → close $4.02 -16.95; ASST×72 09:30 $19.04 → close $21.39 +169.20; RUM×97 09:30 $9.42 → close $10.23 +78.57; EZPW×26 09:30 $35.05 → close $35.23 +4.68; ZYME×31 09:30 $28.86 → close $27.47 -43.09; EOLS×105 09:30 $8.72 → close $8.97 +26.77; AU×7 09:30 $118.52 → close $123.39 +34.09; FCX×11 09:30 $77.13 → close $79.91 +30.58 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $204.21 | ▼ 09:30 equity $11,323.00 vs yday $11,625.59 (-302.59) | 09:30 open · cash $204.21 (unchanged overnight, no fees) · equity $11,323.00 vs prior close $11,625.59 (-302.59) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 57 | $26.61 | $2.18 | $+138.16 | $1,718.80 | ▲ +138.16 after sell → book $11,320.82; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 886 | $1.60 | $11.59 | $+12.42 | $3,124.81 | ▲ +12.42 after sell → book $11,309.23; vs 09:30 mark -11.59 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 72 | $20.72 | $2.23 | $+116.52 | $4,614.42 | ▲ +116.52 after sell → book $11,307.00; vs 09:30 mark -2.23 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 97 | $10.07 | $2.31 | $+58.46 | $5,588.90 | ▲ +58.46 after sell → book $11,304.69; vs 09:30 mark -2.31 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 26 | $35.70 | $2.09 | $+12.74 | $6,515.01 | ▲ +12.74 after sell → book $11,302.60; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 31 | $27.56 | $2.10 | $-44.49 | $7,367.27 | ▼ -44.49 after sell → book $11,300.50; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 105 | $8.86 | $2.33 | $+10.06 | $8,295.24 | ▲ +10.06 after sell → book $11,298.17; vs 09:30 mark -2.33 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 7 | $119.80 | $2.03 | $+4.92 | $9,131.81 | ▲ +4.92 after sell → book $11,296.14; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 11 | $79.34 | $2.04 | $+20.24 | $10,002.50 | ▲ +20.24 after sell → book $11,294.09; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 118 | $14.11 | $2.34 | — | $8,335.18 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $1667.08; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 286 | $5.81 | $3.69 | — | $6,669.83 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $1667.08; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SUJA` | 177 | $9.39 | $2.52 | — | $5,005.28 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+39.0; combo leftover $1667.08; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 90 | $11.12 | $2.26 | — | $4,002.22 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; combo leftover $1001.06; owner union_news_g_h1 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 120 | $8.29 | $2.35 | — | $3,005.07 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; combo leftover $1001.06; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 57 | $17.41 | $2.16 | — | $2,010.54 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-9.2; combo leftover $1001.06; owner union_news_g_h1 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 89 | $11.22 | $2.26 | — | $1,009.70 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.8; combo leftover $1001.06; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 3 | $267.02 | $2.00 | — | $206.64 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.7; combo leftover $1001.06; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $206.64 | ▲ close $11,590.33 vs 09:30 $11,323.00 (session +315.82) | 16:00 close · cash $206.64 · equity $11,590.33 vs 09:30 $11,323.00 (+267.33; session marks +315.82) · 9 name(s) marked open→close (per-name table). XHG×339 09:30 $3.81 → close $4.06 +84.75; BYND×118 09:30 $14.11 → close $14.25 +16.52; USDE×286 09:30 $5.81 → close $5.98 +48.62; SUJA×177 09:30 $9.39 → close $9.44 +8.85; FLNC×90 09:30 $11.12 → close $11.08 -3.60; CAPR×120 09:30 $8.29 → close $9.36 +128.40; FWRD×57 09:30 $17.41 → close $17.63 +12.54; TRLV×89 09:30 $11.22 → close $11.43 +18.69; FNV×3 09:30 $267.02 → close $267.37 +1.05 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $206.64 | ▲ 09:30 equity $11,740.46 vs yday $11,590.33 (+150.13) | 09:30 open · cash $206.64 (unchanged overnight, no fees) · equity $11,740.46 vs prior close $11,590.33 (+150.13) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 339 | $4.06 | $4.44 | $-12.20 | $1,578.54 | ▼ -12.20 after sell → book $11,736.02; vs 09:30 mark -4.44 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 118 | $14.20 | $2.38 | $+5.90 | $3,251.76 | ▲ +5.90 after sell → book $11,733.64; vs 09:30 mark -2.38 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 286 | $6.50 | $3.75 | $+189.90 | $5,107.01 | ▲ +189.90 after sell → book $11,729.89; vs 09:30 mark -3.75 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 177 | $9.41 | $2.56 | $-1.54 | $6,770.02 | ▼ -1.54 after sell → book $11,727.33; vs 09:30 mark -2.56 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 90 | $11.52 | $2.28 | $+31.46 | $7,804.53 | ▲ +31.46 after sell → book $11,725.04; vs 09:30 mark -2.29 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 120 | $9.19 | $2.38 | $+103.27 | $8,904.95 | ▲ +103.27 after sell → book $11,722.66; vs 09:30 mark -2.38 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FWRD` | 57 | $17.60 | $2.18 | $+6.49 | $9,905.97 | ▲ +6.49 after sell → book $11,720.48; vs 09:30 mark -2.18 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 89 | $11.38 | $2.28 | $+9.70 | $10,916.51 | ▲ +9.70 after sell → book $11,718.20; vs 09:30 mark -2.28 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 3 | $267.23 | $2.02 | $-3.39 | $11,716.18 | ▼ -3.39 after sell → book $11,716.18; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 563 | $2.60 | $7.26 | — | $10,245.12 | — | top 4 by hot; rank hot_score; list flatten; ret5=+13.0; combo leftover $1464.52; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 35 | $41.44 | $2.10 | — | $8,792.62 | — | top 4 by hot; rank hot_score; list flatten; ret5=+3.1; combo leftover $1464.52; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 63 | $22.93 | $2.18 | — | $7,345.86 | — | top 4 by hot; rank hot_score; list mover_buy; 🔵; ret5=+9.5; combo leftover $1464.52; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 101 | $14.42 | $2.29 | — | $5,887.14 | — | top 4 by hot; rank hot_score; list flatten; ret5=+7.1; combo leftover $1464.52; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 14 | $81.65 | $2.03 | — | $4,742.01 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+2.0; combo leftover $1177.43; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $3,773.01 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+0.1; combo leftover $1177.43; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 3 | $318.88 | $2.00 | — | $2,814.37 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+1.9; combo leftover $1177.43; owner union_news_g_h1 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 5 | $222.86 | $2.00 | — | $1,698.06 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=-3.6; combo leftover $1177.43; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,698.06 | ▲ close $11,741.67 vs 09:30 $11,740.46 (session +47.35) | 16:00 close · cash $1,698.06 · equity $11,741.67 vs 09:30 $11,740.46 (+1.21; session marks +47.35) · 8 name(s) marked open→close (per-name table). SLI×563 09:30 $2.60 → close $2.64 +22.52; RRC×35 09:30 $41.44 → close $41.64 +7.00; PGY×63 09:30 $22.93 → close $23.26 +20.79; CRK×101 09:30 $14.42 → close $14.62 +20.20; ACMR×14 09:30 $81.65 → close $80.49 -16.24; MU×1 09:30 $967.01 → close $935.39 -31.62; LRCX×3 09:30 $318.88 → close $318.58 -0.90; NVDA×5 09:30 $222.86 → close $227.98 +25.60 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,698.06 | ▼ 09:30 equity $11,727.62 vs yday $11,741.67 (-14.05) | 09:30 open · cash $1,698.06 (unchanged overnight, no fees) · equity $11,727.62 vs prior close $11,741.67 (-14.05) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 563 | $2.68 | $7.37 | $+30.41 | $3,199.54 | ▲ +30.41 after sell → book $11,720.26; vs 09:30 mark -7.36 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 35 | $41.74 | $2.12 | $+6.29 | $4,658.32 | ▲ +6.29 after sell → book $11,718.14; vs 09:30 mark -2.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 63 | $23.21 | $2.20 | $+13.26 | $6,118.35 | ▲ +13.26 after sell → book $11,715.94; vs 09:30 mark -2.20 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 101 | $14.63 | $2.32 | $+16.60 | $7,593.66 | ▲ +16.60 after sell → book $11,713.62; vs 09:30 mark -2.32 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 14 | $79.27 | $2.05 | $-37.40 | $8,701.38 | ▼ -37.40 after sell → book $11,711.56; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $9,618.66 | ▼ -51.73 after sell → book $11,709.55; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 3 | $318.03 | $2.02 | $-6.57 | $10,570.73 | ▼ -6.57 after sell → book $11,707.53; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 5 | $227.36 | $2.02 | $+18.47 | $11,705.51 | ▲ +18.47 after sell → book $11,705.51; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 104 | $14.00 | $2.30 | — | $10,247.21 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $1463.19; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 150 | $9.73 | $2.44 | — | $8,785.27 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+47.1; combo leftover $1463.19; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 10 | $137.19 | $2.02 | — | $7,411.35 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+7.1; combo leftover $1463.19; owner union_hot_n4_h1 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 10 | $146.07 | $2.02 | — | $5,948.63 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $1463.19; owner union_hot_n4_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 20 | $41.74 | $2.05 | — | $5,111.78 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+2.4; combo leftover $849.80; owner union_news_g_h1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 25 | $32.90 | $2.06 | — | $4,287.21 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; combo leftover $849.80; owner union_news_g_h1 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 98 | $8.61 | $2.28 | — | $3,441.15 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.7; combo leftover $849.80; owner union_news_g_h1 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 5 | $141.76 | $2.00 | — | $2,730.34 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; combo leftover $849.80; owner union_news_g_h1 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 44 | $19.25 | $2.12 | — | $1,881.22 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+14.1; combo leftover $849.80; owner union_news_g_h1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 45 | $18.75 | $2.12 | — | $1,035.34 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-5.0; combo leftover $849.80; owner union_news_g_h1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 29 | $28.91 | $2.08 | — | $194.88 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+9.2; combo leftover $849.80; owner union_news_g_h1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $194.88 | ▼ close $11,522.23 vs 09:30 $11,727.62 (session -159.77) | 16:00 close · cash $194.88 · equity $11,522.23 vs 09:30 $11,727.62 (-205.39; session marks -159.77) · 11 name(s) marked open→close (per-name table). RRC×20 09:30 $41.74 → close $41.46 -5.60; BYND×104 09:30 $14.00 → close $13.86 -14.56; CAPR×150 09:30 $9.73 → close $9.59 -21.00; MRNA×10 09:30 $137.19 → close $137.99 +8.00; ANF×10 09:30 $146.07 → close $148.42 +23.50; SEDG×25 09:30 $32.90 → close $31.41 -37.25; OPTX×98 09:30 $8.61 → close $8.52 -8.82; SMTC×5 09:30 $141.76 → close $131.17 -52.95; ERAS×44 09:30 $19.25 → close $18.03 -53.68; BBWI×45 09:30 $18.75 → close $19.22 +21.15; ZYME×29 09:30 $28.91 → close $28.27 -18.56 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $194.88 | ▼ 09:30 equity $11,458.90 vs yday $11,522.23 (-63.33) | 09:30 open · cash $194.88 (unchanged overnight, no fees) · equity $11,458.90 vs prior close $11,522.23 (-63.33) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 150 | $9.50 | $2.48 | $-39.42 | $1,617.40 | ▼ -39.42 after sell → book $11,456.42; vs 09:30 mark -2.48 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 10 | $134.10 | $2.04 | $-34.96 | $2,956.36 | ▼ -34.96 after sell → book $11,454.38; vs 09:30 mark -2.04 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 10 | $148.03 | $2.04 | $+15.54 | $4,434.62 | ▲ +15.54 after sell → book $11,452.34; vs 09:30 mark -2.04 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 20 | $42.00 | $2.07 | $+1.08 | $5,272.55 | ▲ +1.08 after sell → book $11,450.27; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 25 | $31.15 | $2.08 | $-47.90 | $6,049.21 | ▼ -47.90 after sell → book $11,448.18; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 98 | $8.52 | $2.31 | $-13.41 | $6,881.86 | ▼ -13.41 after sell → book $11,445.87; vs 09:30 mark -2.31 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 5 | $132.30 | $2.02 | $-51.33 | $7,541.34 | ▼ -51.33 after sell → book $11,443.85; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 44 | $17.87 | $2.14 | $-64.98 | $8,325.48 | ▼ -64.98 after sell → book $11,441.71; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 45 | $19.25 | $2.15 | $+18.23 | $9,189.58 | ▲ +18.23 after sell → book $11,439.56; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 29 | $28.06 | $2.10 | $-28.82 | $10,001.22 | ▼ -28.82 after sell → book $11,437.46; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,001.22 | ▼ close $11,384.42 vs 09:30 $11,458.90 (session -53.04) | 16:00 close · cash $10,001.22 · equity $11,384.42 vs 09:30 $11,458.90 (-74.48; session marks -53.04) · 1 name(s) marked open→close (per-name table). BYND×104 09:30 $13.81 → close $13.30 -53.04 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,001.22 | ▼ 09:30 equity $11,357.38 vs yday $11,384.42 (-27.04) | 09:30 open · cash $10,001.22 (unchanged overnight, no fees) · equity $11,357.38 vs prior close $11,384.42 (-27.04) | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 104 | $13.04 | $2.33 | $-104.47 | $11,355.05 | ▼ -104.47 after sell → book $11,355.05; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,355.05 | ▲ close $11,355.05 vs 09:30 $11,357.38 (session +0.00) | 16:00 close · cash $11,355.05 · no lots left · equity $11,355.05. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,355.05 | ▲ 09:30 equity $11,355.05 vs yday $11,355.05 (+0.00) | 09:30 open · cash $11,355.05 (unchanged overnight, no fees) · equity $11,355.05 vs prior close $11,355.05 (+0.00) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,355.05 | ▲ close $11,355.05 vs 09:30 $11,355.05 (session +0.00) | 16:00 close · cash $11,355.05 · no lots left · equity $11,355.05. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,355.05 | ▲ 09:30 equity $11,355.05 vs yday $11,355.05 (+0.00) | 09:30 open · cash $11,355.05 (unchanged overnight, no fees) · equity $11,355.05 vs prior close $11,355.05 (+0.00) | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 797 | $1.78 | $10.28 | — | $9,926.11 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $1419.38; owner union_hot_n4_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 77 | $18.40 | $2.22 | — | $8,507.09 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $1419.38; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 103 | $13.71 | $2.30 | — | $7,092.66 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $1419.38; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 59 | $23.88 | $2.17 | — | $5,681.58 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $1419.38; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 24 | $32.88 | $2.06 | — | $4,890.39 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+16.2; combo leftover $811.65; owner union_news_g_h1 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 106 | $7.59 | $2.31 | — | $4,083.55 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.5; combo leftover $811.65; owner union_news_g_h1 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $3,378.30 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.1; combo leftover $811.65; owner union_news_g_h1 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 51 | $15.87 | $2.14 | — | $2,566.79 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $811.65; owner union_news_g_h1 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 2 | $351.74 | $2.00 | — | $1,861.31 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+3.3; combo leftover $811.65; owner union_news_g_h1 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 2 | $354.49 | $2.00 | — | $1,150.34 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-12.3; combo leftover $811.65; owner union_news_g_h1 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 17 | $47.60 | $2.04 | — | $339.10 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; combo leftover $811.65; owner union_news_g_h1 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $339.10 | ▼ close $11,137.80 vs 09:30 $11,355.05 (session -185.75) | 16:00 close · cash $339.10 · equity $11,137.80 vs 09:30 $11,355.05 (-217.25; session marks -185.75) · 11 name(s) marked open→close (per-name table). GPRO×797 09:30 $1.78 → close $1.39 -310.83; REAX×77 09:30 $18.40 → close $18.40 +0.00; CNH×103 09:30 $13.71 → close $13.84 +13.39; MMED×59 09:30 $23.88 → close $23.84 -2.36; CNXC×24 09:30 $32.88 → close $32.85 -0.72; OPTX×106 09:30 $7.59 → close $7.76 +18.02; DE×1 09:30 $703.25 → close $694.41 -8.84; FRNM×51 09:30 $15.87 → close $16.90 +52.53; AVGO×2 09:30 $351.74 → close $357.16 +10.84; CIEN×2 09:30 $354.49 → close $317.46 -74.06; HPE×17 09:30 $47.60 → close $54.44 +116.28 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $339.10 | ▲ 09:30 equity $11,165.32 vs yday $11,137.80 (+27.52) | 09:30 open · cash $339.10 (unchanged overnight, no fees) · equity $11,165.32 vs prior close $11,137.80 (+27.52) | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 77 | $18.15 | $2.25 | $-23.72 | $1,734.40 | ▼ -23.72 after sell → book $11,163.07; vs 09:30 mark -2.25 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 103 | $13.89 | $2.33 | $+13.91 | $3,162.74 | ▲ +13.91 after sell → book $11,160.74; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 59 | $23.84 | $2.19 | $-6.72 | $4,567.12 | ▼ -6.72 after sell → book $11,158.56; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 24 | $32.48 | $2.08 | $-13.74 | $5,344.55 | ▼ -13.74 after sell → book $11,156.47; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 106 | $7.79 | $2.34 | $+16.56 | $6,167.96 | ▲ +16.56 after sell → book $11,154.14; vs 09:30 mark -2.33 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $6,857.98 | ▼ -15.23 after sell → book $11,152.13; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 2 | $359.70 | $2.02 | $+11.91 | $7,575.36 | ▲ +11.91 after sell → book $11,150.11; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 2 | $321.67 | $2.02 | $-69.65 | $8,216.68 | ▼ -69.65 after sell → book $11,148.09; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 17 | $53.85 | $2.06 | $+102.15 | $9,130.07 | ▲ +102.15 after sell → book $11,146.03; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 60 | $25.18 | $2.17 | — | $7,617.10 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $1521.68; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 193 | $7.87 | $2.57 | — | $6,095.62 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $1521.68; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 262 | $5.79 | $3.38 | — | $4,575.26 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $1521.68; owner union_hot_n4_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 3 | $263.36 | $2.00 | — | $3,783.18 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; combo leftover $915.05; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 471 | $1.94 | $6.08 | — | $2,863.37 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+18.3; combo leftover $915.05; owner union_news_g_h1 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 6 | $137.35 | $2.01 | — | $2,037.26 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+5.4; combo leftover $915.05; owner union_news_g_h1 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 3 | $236.82 | $2.00 | — | $1,324.80 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.1; combo leftover $915.05; owner union_news_g_h1 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 12 | $75.65 | $2.03 | — | $414.98 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+6.0; combo leftover $915.05; owner union_news_g_h1 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $414.98 | ▲ close $11,521.05 vs 09:30 $11,165.32 (session +397.24) | 16:00 close · cash $414.98 · equity $11,521.05 vs 09:30 $11,165.32 (+355.73; session marks +397.24) · 10 name(s) marked open→close (per-name table). GPRO×797 09:30 $1.48 → close $1.70 +175.34; FRNM×51 09:30 $16.40 → close $16.31 -4.59; ASST×60 09:30 $25.18 → close $27.14 +117.60; USDE×193 09:30 $7.87 → close $7.93 +11.58; DFDV×262 09:30 $5.79 → close $5.87 +20.96; CRM×3 09:30 $263.36 → close $259.23 -12.39; BAK×471 09:30 $1.94 → close $1.89 -23.55; MSTR×6 09:30 $137.35 → close $142.80 +32.70; BE×3 09:30 $236.82 → close $252.87 +48.15; MRX×12 09:30 $75.65 → close $78.27 +31.44 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $414.98 | ▼ 09:30 equity $11,372.30 vs yday $11,521.05 (-148.75) | 09:30 open · cash $414.98 (unchanged overnight, no fees) · equity $11,372.30 vs prior close $11,521.05 (-148.75) | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 797 | $1.56 | $10.42 | $-192.06 | $1,651.86 | ▼ -192.06 after sell → book $11,361.88; vs 09:30 mark -10.42 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 51 | $16.74 | $2.16 | $+40.06 | $2,503.43 | ▲ +40.06 after sell → book $11,359.71; vs 09:30 mark -2.17 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 60 | $26.44 | $2.19 | $+71.24 | $4,087.64 | ▲ +71.24 after sell → book $11,357.52; vs 09:30 mark -2.19 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 193 | $7.76 | $2.61 | $-26.41 | $5,582.71 | ▼ -26.41 after sell → book $11,354.91; vs 09:30 mark -2.61 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 262 | $5.81 | $3.44 | $-1.58 | $7,101.49 | ▼ -1.58 after sell → book $11,351.47; vs 09:30 mark -3.44 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 3 | $253.72 | $2.02 | $-32.94 | $7,860.63 | ▼ -32.94 after sell → book $11,349.45; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 471 | $1.94 | $6.16 | $-12.24 | $8,768.21 | ▼ -12.24 after sell → book $11,343.29; vs 09:30 mark -6.16 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 3 | $267.76 | $2.02 | $+88.80 | $9,569.47 | ▲ +88.80 after sell → book $11,341.27; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,569.47 | ▼ close $11,309.11 vs 09:30 $11,372.30 (session -32.16) | 16:00 close · cash $9,569.47 · equity $11,309.11 vs 09:30 $11,372.30 (-63.19; session marks -32.16) · 2 name(s) marked open→close (per-name table). MSTR×6 09:30 $137.62 → close $136.52 -6.60; MRX×12 09:30 $78.84 → close $76.71 -25.56 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,569.47 | ▲ 09:30 equity $11,339.59 vs yday $11,309.11 (+30.48) | 09:30 open · cash $9,569.47 (unchanged overnight, no fees) · equity $11,339.59 vs prior close $11,309.11 (+30.48) | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 6 | $141.82 | $2.03 | $+22.78 | $10,418.36 | ▲ +22.78 after sell → book $11,337.56; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 12 | $76.60 | $2.05 | $+7.33 | $11,335.52 | ▲ +7.33 after sell → book $11,335.52; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,335.52 | ▲ close $11,335.52 vs 09:30 $11,339.59 (session +0.00) | 16:00 close · cash $11,335.52 · no lots left · equity $11,335.52. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,335.52 | ▲ 09:30 equity $11,335.52 vs yday $11,335.52 (-0.00) | 09:30 open · cash $11,335.52 (unchanged overnight, no fees) · equity $11,335.52 vs prior close $11,335.52 (-0.00) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,335.52 | ▲ close $11,335.52 vs 09:30 $11,335.52 (session +0.00) | 16:00 close · cash $11,335.52 · no lots left · equity $11,335.52. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 741.71 < 1 share @ 1646.93 |
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
| 2026-08-21 | `DE` | cash | leftover split 503.04 < 1 share @ 623.26 |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new union_hot_n4_h1 |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new union_news_g_h1 |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new union_news_g_h1 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new union_news_g_h1 |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new union_news_g_h1 |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new union_news_g_h1 |
| 2026-08-27 | `ASML` | cash | leftover split 1177.43 < 1 share @ 1746.53 |
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
